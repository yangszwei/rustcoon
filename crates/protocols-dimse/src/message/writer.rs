use dicom_dictionary_std::tags;
use dicom_object::InMemDicomObject;
use dicom_transfer_syntax_registry::entries::IMPLICIT_VR_LITTLE_ENDIAN;
use dicom_ul::pdu::{PDataValue, PDataValueType, Pdu, PresentationContextResultReason};
use rustcoon_ul::UlAssociation;

use crate::error::DimseError;

const PDV_ITEM_OVERHEAD_BYTES: usize = 6;
const PDATA_PDU_HEADER_BYTES: usize = 6;

/// Stateless DIMSE writer.
#[derive(Debug, Default)]
pub struct DimseWriter {
    bytes_out: u64,
}

impl DimseWriter {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn send_command_object(
        &mut self,
        association: &mut UlAssociation,
        presentation_context_id: u8,
        command: &InMemDicomObject,
    ) -> Result<(), DimseError> {
        validate_presentation_context(association, presentation_context_id)?;
        validate_command_object(command)?;

        let mut bytes = Vec::new();
        command.write_dataset_with_ts(&mut bytes, &IMPLICIT_VR_LITTLE_ENDIAN.erased())?;

        self.send_pdv(
            association,
            PDataValue {
                presentation_context_id,
                value_type: PDataValueType::Command,
                is_last: true,
                data: bytes,
            },
        )
    }

    pub fn send_data_pdv(
        &mut self,
        association: &mut UlAssociation,
        pdv: PDataValue,
    ) -> Result<(), DimseError> {
        if pdv.value_type != PDataValueType::Data {
            return Err(DimseError::protocol("send_data_pdv expects a data PDV"));
        }
        self.send_pdv(association, pdv)
    }

    fn send_pdv(
        &mut self,
        association: &mut UlAssociation,
        pdv: PDataValue,
    ) -> Result<(), DimseError> {
        validate_presentation_context(association, pdv.presentation_context_id)?;

        let peer_max_pdu_length = association.peer_max_pdu_length() as usize;
        let max_pdv_data_len = max_pdv_data_len_for_peer(peer_max_pdu_length)?;

        if pdv.data.is_empty() {
            if !pdv.is_last {
                return Err(DimseError::protocol(
                    "empty PDV payload must be the last fragment",
                ));
            }

            association.send_pdu(&Pdu::PData { data: vec![pdv] })?;
            self.bytes_out = self
                .bytes_out
                .saturating_add((PDATA_PDU_HEADER_BYTES + PDV_ITEM_OVERHEAD_BYTES) as u64);
            return Ok(());
        }

        let total_len = pdv.data.len();
        let mut offset = 0;
        while offset < total_len {
            let end = offset.saturating_add(max_pdv_data_len).min(total_len);
            let is_fragment_last = end == total_len && pdv.is_last;
            let fragment_data = pdv.data[offset..end].to_vec();

            association.send_pdu(&Pdu::PData {
                data: vec![PDataValue {
                    presentation_context_id: pdv.presentation_context_id,
                    value_type: pdv.value_type.clone(),
                    is_last: is_fragment_last,
                    data: fragment_data,
                }],
            })?;
            self.bytes_out = self
                .bytes_out
                .saturating_add((PDATA_PDU_HEADER_BYTES + PDV_ITEM_OVERHEAD_BYTES) as u64)
                .saturating_add((end - offset) as u64);

            offset = end;
        }

        Ok(())
    }

    pub fn bytes_out(&self) -> u64 {
        self.bytes_out
    }
}

fn max_pdv_data_len_for_peer(peer_max_pdu_length: usize) -> Result<usize, DimseError> {
    if peer_max_pdu_length == 0 {
        return Ok(usize::MAX);
    }

    peer_max_pdu_length
        .checked_sub(PDATA_PDU_HEADER_BYTES + PDV_ITEM_OVERHEAD_BYTES)
        .filter(|value| *value > 0)
        .ok_or_else(|| {
            DimseError::protocol(format!(
                "peer max PDU length {} too small for PDV payload",
                peer_max_pdu_length
            ))
        })
}

fn validate_presentation_context(
    association: &UlAssociation,
    presentation_context_id: u8,
) -> Result<(), DimseError> {
    let negotiated = association
        .presentation_contexts()
        .iter()
        .find(|pc| pc.id == presentation_context_id)
        .ok_or_else(|| {
            DimseError::protocol(format!(
                "presentation context {} was not negotiated",
                presentation_context_id
            ))
        })?;

    if negotiated.reason != PresentationContextResultReason::Acceptance {
        return Err(DimseError::protocol(format!(
            "presentation context {} is not accepted",
            presentation_context_id
        )));
    }
    Ok(())
}

fn validate_command_object(command: &InMemDicomObject) -> Result<(), DimseError> {
    for tag in command.tags() {
        if tag.group() != 0x0000 {
            return Err(DimseError::protocol(format!(
                "command set contains non-command element {}",
                tag
            )));
        }
    }

    let _ = command
        .element(tags::COMMAND_FIELD)
        .map_err(|_| DimseError::protocol("missing Command Field"))?
        .to_int::<u16>()
        .map_err(|_| DimseError::protocol("invalid Command Field"))?;

    let _ = command
        .element(tags::COMMAND_DATA_SET_TYPE)
        .map_err(|_| DimseError::protocol("missing Command Data Set Type"))?
        .to_int::<u16>()
        .map_err(|_| DimseError::protocol("invalid Command Data Set Type"))?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::io::ErrorKind;
    use std::net::SocketAddr;
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    use dicom_core::{DataElement, PrimitiveValue, VR};
    use dicom_dictionary_std::tags;
    use dicom_object::InMemDicomObject;
    use dicom_ul::pdu::{PDataValue, PDataValueType};
    use rustcoon_application_entity::ApplicationEntityRegistry;
    use rustcoon_config::application_entity::{
        ApplicationEntitiesConfig, LocalApplicationEntityConfig, RemoteApplicationEntityConfig,
    };
    use rustcoon_ul::{OutboundAssociationRequest, UlAssociation, UlListener};

    use super::{DimseWriter, max_pdv_data_len_for_peer};
    use crate::{DimseError, DimseReader};

    const VERIFICATION_SOP_CLASS: &str = "1.2.840.10008.1.1";
    const UNKNOWN_SOP_CLASS: &str = "1.2.840.10008.5.1.4.999.1";

    fn local(title: &str, bind: SocketAddr, max_pdu_length: u32) -> LocalApplicationEntityConfig {
        LocalApplicationEntityConfig {
            title: title.to_string(),
            bind_address: bind,
            read_timeout_seconds: Some(1),
            write_timeout_seconds: Some(1),
            max_pdu_length,
        }
    }

    fn remote(title: &str, address: SocketAddr) -> RemoteApplicationEntityConfig {
        RemoteApplicationEntityConfig {
            title: title.to_string(),
            address,
            connect_timeout_seconds: Some(1),
            read_timeout_seconds: Some(1),
            write_timeout_seconds: Some(1),
            max_pdu_length: 16_384,
        }
    }

    fn setup_ul_pair(
        server_max_pdu: u32,
    ) -> Option<(UlAssociation, UlAssociation, u8, Option<u8>)> {
        let registry = Arc::new(
            ApplicationEntityRegistry::try_from_config(&ApplicationEntitiesConfig {
                local: vec![local(
                    "REMOTE_SCP",
                    "127.0.0.1:0".parse().ok()?,
                    server_max_pdu,
                )],
                remote: vec![remote("LOCAL_SCU", "127.0.0.1:11112".parse().ok()?)],
            })
            .ok()?,
        );

        let listener = match UlListener::bind_from_registry(Arc::clone(&registry), "REMOTE_SCP") {
            Ok(listener) => listener.with_abstract_syntax(VERIFICATION_SOP_CLASS),
            Err(rustcoon_ul::UlError::Io(error)) if error.kind() == ErrorKind::PermissionDenied => {
                return None;
            }
            Err(error) => panic!("listener bind should succeed: {error}"),
        };
        let addr = listener.local_addr().expect("listener address");
        let server = thread::spawn(move || listener.accept().expect("server accept").0);

        let client = OutboundAssociationRequest::new("LOCAL_SCU", "REMOTE_SCP", addr)
            .connect_timeout(Duration::from_secs(1))
            .read_timeout(Duration::from_secs(1))
            .write_timeout(Duration::from_secs(1))
            .with_abstract_syntax(VERIFICATION_SOP_CLASS)
            .with_abstract_syntax(UNKNOWN_SOP_CLASS)
            .establish()
            .expect("client establish");

        let accepted_context_id = client
            .presentation_contexts()
            .iter()
            .find(|pc| pc.abstract_syntax == VERIFICATION_SOP_CLASS)
            .map(|pc| pc.id)
            .expect("accepted context");
        let rejected_context_id = client
            .presentation_contexts()
            .iter()
            .find(|pc| pc.abstract_syntax == UNKNOWN_SOP_CLASS)
            .map(|pc| pc.id);
        let server_association = server.join().expect("server join");

        Some((
            server_association,
            client,
            accepted_context_id,
            rejected_context_id,
        ))
    }

    fn valid_command(has_data_set: bool) -> InMemDicomObject {
        let mut command = InMemDicomObject::new_empty();
        command.put(DataElement::new(
            tags::COMMAND_FIELD,
            VR::US,
            PrimitiveValue::from(0x0030_u16),
        ));
        command.put(DataElement::new(
            tags::COMMAND_DATA_SET_TYPE,
            VR::US,
            PrimitiveValue::from(if has_data_set { 0x0000_u16 } else { 0x0101_u16 }),
        ));
        command
    }

    #[test]
    fn writer_rejects_unnegotiated_and_non_accepted_contexts() {
        let Some((_server, mut client, accepted_id, rejected_id)) = setup_ul_pair(16_384) else {
            return;
        };
        let mut writer = DimseWriter::new();
        let command = valid_command(false);

        let missing = writer.send_command_object(&mut client, accepted_id + 100, &command);
        assert!(matches!(missing, Err(DimseError::Protocol(_))));

        if let Some(rejected_id) = rejected_id {
            let rejected = writer.send_command_object(&mut client, rejected_id, &command);
            assert!(matches!(rejected, Err(DimseError::Protocol(_))));
        }
    }

    #[test]
    fn writer_rejects_invalid_command_objects() {
        let Some((_server, mut client, context_id, _)) = setup_ul_pair(16_384) else {
            return;
        };
        let mut writer = DimseWriter::new();

        let missing = InMemDicomObject::new_empty();
        let result = writer.send_command_object(&mut client, context_id, &missing);
        assert!(matches!(result, Err(DimseError::Protocol(_))));

        let mut non_command = valid_command(false);
        non_command.put(DataElement::new(
            dicom_core::Tag(0x0010, 0x0010),
            VR::PN,
            "DOE^JOHN",
        ));
        let result = writer.send_command_object(&mut client, context_id, &non_command);
        assert!(matches!(result, Err(DimseError::Protocol(_))));
    }

    #[test]
    fn writer_rejects_invalid_data_pdv_inputs() {
        let Some((_server, mut client, context_id, _)) = setup_ul_pair(16_384) else {
            return;
        };
        let mut writer = DimseWriter::new();

        let wrong_type = writer.send_data_pdv(
            &mut client,
            PDataValue {
                presentation_context_id: context_id,
                value_type: PDataValueType::Command,
                is_last: true,
                data: vec![1],
            },
        );
        assert!(matches!(wrong_type, Err(DimseError::Protocol(_))));

        let empty_non_last = writer.send_data_pdv(
            &mut client,
            PDataValue {
                presentation_context_id: context_id,
                value_type: PDataValueType::Data,
                is_last: false,
                data: vec![],
            },
        );
        assert!(matches!(empty_non_last, Err(DimseError::Protocol(_))));
    }

    #[test]
    fn writer_splits_data_by_peer_max_pdu_and_reader_reassembles() {
        let Some((mut server, mut client, context_id, _)) = setup_ul_pair(4096) else {
            return;
        };
        let mut writer = DimseWriter::new();
        let mut reader = DimseReader::new();

        writer
            .send_command_object(&mut client, context_id, &valid_command(true))
            .expect("send command");
        writer
            .send_data_pdv(
                &mut client,
                PDataValue {
                    presentation_context_id: context_id,
                    value_type: PDataValueType::Data,
                    is_last: true,
                    data: vec![7; 10_000],
                },
            )
            .expect("send fragmented dataset");

        let _ = reader
            .read_command_object(&mut server)
            .expect("read command");
        let mut total = 0usize;
        while let Some(pdv) = reader.read_data_pdv(&mut server).expect("read dataset") {
            total += pdv.data.len();
        }
        assert_eq!(total, 10_000);
    }

    #[test]
    fn max_pdv_data_len_for_peer_handles_edge_values() {
        assert_eq!(max_pdv_data_len_for_peer(0).unwrap(), usize::MAX);
        assert!(max_pdv_data_len_for_peer(8).is_err());
        assert!(max_pdv_data_len_for_peer(12).is_err());
        assert_eq!(max_pdv_data_len_for_peer(13).unwrap(), 1);
    }
}
