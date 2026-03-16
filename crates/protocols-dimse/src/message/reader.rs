use std::collections::VecDeque;
use std::io::Cursor;

use dicom_dictionary_std::tags;
use dicom_object::InMemDicomObject;
use dicom_transfer_syntax_registry::entries::IMPLICIT_VR_LITTLE_ENDIAN;
use dicom_ul::pdu::{PDataValue, PDataValueType, Pdu, PresentationContextResultReason};
use rustcoon_ul::UlAssociation;

use crate::error::DimseError;
use crate::message::CommandObject;

#[derive(Debug)]
struct ActiveDataSet {
    presentation_context_id: u8,
    finished: bool,
}

/// Incremental DIMSE reader over UL `P-DATA` PDUs.
/// Reads full commands while keeping datasets streamable PDV-by-PDV.
#[derive(Debug, Default)]
pub struct DimseReader {
    pending_pdvs: VecDeque<PDataValue>,
    active_data_set: Option<ActiveDataSet>,
    bytes_in: u64,
}

impl DimseReader {
    /// Create an empty reader.
    pub fn new() -> Self {
        Self::default()
    }

    /// Read the next command set and decode it into a `CommandObject`.
    /// Fails if the previous command dataset is not yet fully consumed.
    pub fn read_command_object(
        &mut self,
        association: &mut UlAssociation,
    ) -> Result<CommandObject, DimseError> {
        if self.active_data_set.is_some() {
            return Err(DimseError::protocol(
                "cannot read next command before data set is finished",
            ));
        }

        let (context_id, command_bytes) = self.read_command_fragments(association)?;
        validate_presentation_context(association, context_id)?;

        let command = InMemDicomObject::read_dataset_with_ts(
            Cursor::new(command_bytes),
            &IMPLICIT_VR_LITTLE_ENDIAN.erased(),
        )?;

        validate_command_object(&command)?;

        let has_data_set =
            element_u16(&command, tags::COMMAND_DATA_SET_TYPE)? != COMMAND_DATA_SET_MISSING;

        self.active_data_set = if has_data_set {
            Some(ActiveDataSet {
                presentation_context_id: context_id,
                finished: false,
            })
        } else {
            None
        };

        Ok(CommandObject {
            presentation_context_id: context_id,
            command,
        })
    }

    /// Read one dataset PDV for the currently active command.
    /// Returns `Ok(None)` when no dataset is expected or already finished.
    pub fn read_data_pdv(
        &mut self,
        association: &mut UlAssociation,
    ) -> Result<Option<PDataValue>, DimseError> {
        let (expected_context_id, finished) = match self.active_data_set.as_ref() {
            Some(active) => (active.presentation_context_id, active.finished),
            None => return Ok(None),
        };

        if finished {
            self.active_data_set = None;
            return Ok(None);
        }

        let pdv = self.next_pdv(association)?;
        if pdv.value_type != PDataValueType::Data {
            return Err(DimseError::protocol(
                "received command PDV while data set was expected",
            ));
        }
        if pdv.presentation_context_id != expected_context_id {
            return Err(DimseError::protocol(
                "data set fragments use multiple presentation contexts",
            ));
        }
        if pdv.is_last
            && let Some(active) = &mut self.active_data_set
        {
            active.finished = true;
        }
        Ok(Some(pdv))
    }

    /// Return `true` when a dataset stream is active and not yet complete.
    pub fn has_unfinished_data_set(&self) -> bool {
        self.active_data_set
            .as_ref()
            .map(|active| !active.finished)
            .unwrap_or(false)
    }

    pub fn bytes_in(&self) -> u64 {
        self.bytes_in
    }

    fn read_command_fragments(
        &mut self,
        association: &mut UlAssociation,
    ) -> Result<(u8, Vec<u8>), DimseError> {
        let mut command_bytes = Vec::new();
        let mut context_id: Option<u8> = None;

        loop {
            let pdv = self.next_pdv(association)?;
            match pdv.value_type {
                PDataValueType::Command => {
                    match context_id {
                        Some(id) if id != pdv.presentation_context_id => {
                            return Err(DimseError::protocol(
                                "command fragments use multiple presentation contexts",
                            ));
                        }
                        Some(_) => {}
                        None => context_id = Some(pdv.presentation_context_id),
                    }

                    command_bytes.extend_from_slice(&pdv.data);
                    if pdv.is_last {
                        let id = context_id
                            .ok_or_else(|| DimseError::protocol("missing presentation context"))?;
                        return Ok((id, command_bytes));
                    }
                }
                PDataValueType::Data => {
                    return Err(DimseError::protocol(
                        "received data PDV before command was complete",
                    ));
                }
            }
        }
    }

    fn next_pdv(&mut self, association: &mut UlAssociation) -> Result<PDataValue, DimseError> {
        if let Some(pdv) = self.pending_pdvs.pop_front() {
            return Ok(pdv);
        }

        loop {
            match association.receive_pdu()? {
                Pdu::PData { data } => {
                    if data.is_empty() {
                        continue;
                    }
                    const PDATA_PDU_HEADER_BYTES: u64 = 6;
                    const PDV_ITEM_OVERHEAD_BYTES: u64 = 6;
                    let payload_len = data.iter().map(|pdv| pdv.data.len() as u64).sum::<u64>();
                    self.bytes_in = self
                        .bytes_in
                        .saturating_add(PDATA_PDU_HEADER_BYTES)
                        .saturating_add(PDV_ITEM_OVERHEAD_BYTES.saturating_mul(data.len() as u64))
                        .saturating_add(payload_len);
                    self.pending_pdvs.extend(data);
                    return self
                        .pending_pdvs
                        .pop_front()
                        .ok_or_else(|| DimseError::protocol("missing PDV in P-DATA"));
                }
                Pdu::AbortRQ { .. } => {
                    return Err(DimseError::Ul(rustcoon_ul::UlError::Aborted));
                }
                Pdu::ReleaseRQ => {
                    return Err(DimseError::PeerReleaseRequested);
                }
                Pdu::ReleaseRP => {
                    return Err(DimseError::Ul(rustcoon_ul::UlError::Closed));
                }
                other => {
                    return Err(DimseError::protocol(format!(
                        "unexpected PDU during DIMSE read: {:?}",
                        other
                    )));
                }
            }
        }
    }
}

const COMMAND_DATA_SET_MISSING: u16 = 0x0101;

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

    let _ = element_u16(command, tags::COMMAND_FIELD)?;
    let _ = element_u16(command, tags::COMMAND_DATA_SET_TYPE)?;
    Ok(())
}

fn element_u16(command: &InMemDicomObject, tag: dicom_core::Tag) -> Result<u16, DimseError> {
    command
        .element(tag)
        .map_err(|_| DimseError::protocol(format!("missing {}", tag)))?
        .to_int::<u16>()
        .map_err(|_| DimseError::protocol(format!("invalid {}", tag)))
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
    use dicom_transfer_syntax_registry::entries::IMPLICIT_VR_LITTLE_ENDIAN;
    use dicom_ul::pdu::{AbortRQSource, PDataValue, PDataValueType, Pdu};
    use rustcoon_application_entity::ApplicationEntityRegistry;
    use rustcoon_config::application_entity::{
        ApplicationEntitiesConfig, LocalApplicationEntityConfig, RemoteApplicationEntityConfig,
    };
    use rustcoon_ul::{OutboundAssociationRequest, UlAssociation, UlListener};

    use super::DimseReader;
    use crate::DimseError;

    const VERIFICATION_SOP_CLASS: &str = "1.2.840.10008.1.1";

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

    fn setup_ul_pair() -> Option<(UlAssociation, UlAssociation, u8)> {
        let registry = Arc::new(
            ApplicationEntityRegistry::try_from_config(&ApplicationEntitiesConfig {
                local: vec![local("REMOTE_SCP", "127.0.0.1:0".parse().ok()?, 16_384)],
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
            .establish()
            .expect("client establish");
        let context_id = client.presentation_contexts()[0].id;
        let server_association = server.join().expect("server join");
        Some((server_association, client, context_id))
    }

    fn command_bytes(has_data_set: bool) -> Vec<u8> {
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
        command.put(DataElement::new(
            tags::MESSAGE_ID,
            VR::US,
            PrimitiveValue::from(1_u16),
        ));
        command.put(DataElement::new(
            tags::AFFECTED_SOP_CLASS_UID,
            VR::UI,
            VERIFICATION_SOP_CLASS,
        ));
        let mut bytes = Vec::new();
        command
            .write_dataset_with_ts(&mut bytes, &IMPLICIT_VR_LITTLE_ENDIAN.erased())
            .expect("encode command");
        bytes
    }

    #[test]
    fn read_data_pdv_returns_none_without_active_dataset() {
        let Some((mut server, _client, _context_id)) = setup_ul_pair() else {
            return;
        };
        let mut reader = DimseReader::new();
        assert!(reader.read_data_pdv(&mut server).expect("read").is_none());
    }

    #[test]
    fn reader_handles_pending_pdv_queue_and_finished_dataset_state() {
        let Some((mut server, mut client, context_id)) = setup_ul_pair() else {
            return;
        };
        let mut reader = DimseReader::new();

        let bytes = command_bytes(true);
        client
            .send_pdu(&Pdu::PData {
                data: vec![
                    PDataValue {
                        presentation_context_id: context_id,
                        value_type: PDataValueType::Command,
                        is_last: false,
                        data: bytes[..bytes.len() / 2].to_vec(),
                    },
                    PDataValue {
                        presentation_context_id: context_id,
                        value_type: PDataValueType::Command,
                        is_last: true,
                        data: bytes[bytes.len() / 2..].to_vec(),
                    },
                ],
            })
            .expect("send command fragments");
        client
            .send_pdu(&Pdu::PData {
                data: vec![PDataValue {
                    presentation_context_id: context_id,
                    value_type: PDataValueType::Data,
                    is_last: true,
                    data: vec![1, 2, 3],
                }],
            })
            .expect("send data");

        let _ = reader.read_command_object(&mut server).expect("command");
        assert!(reader.has_unfinished_data_set());
        assert!(reader.read_data_pdv(&mut server).expect("data").is_some());
        assert!(!reader.has_unfinished_data_set());
        assert!(
            reader
                .read_data_pdv(&mut server)
                .expect("finished")
                .is_none()
        );
    }

    #[test]
    fn reader_rejects_command_before_consuming_dataset() {
        let Some((mut server, mut client, context_id)) = setup_ul_pair() else {
            return;
        };
        let mut reader = DimseReader::new();
        let bytes = command_bytes(true);
        client
            .send_pdu(&Pdu::PData {
                data: vec![PDataValue {
                    presentation_context_id: context_id,
                    value_type: PDataValueType::Command,
                    is_last: true,
                    data: bytes,
                }],
            })
            .expect("send command");

        let _ = reader.read_command_object(&mut server).expect("command");
        let result = reader.read_command_object(&mut server);
        assert!(matches!(result, Err(DimseError::Protocol(_))));
    }

    #[test]
    fn reader_rejects_invalid_dataset_stream_shapes() {
        let Some((mut server, mut client, context_id)) = setup_ul_pair() else {
            return;
        };
        let mut reader = DimseReader::new();
        let bytes = command_bytes(true);

        client
            .send_pdu(&Pdu::PData {
                data: vec![PDataValue {
                    presentation_context_id: context_id,
                    value_type: PDataValueType::Command,
                    is_last: true,
                    data: bytes,
                }],
            })
            .expect("send command");
        client
            .send_pdu(&Pdu::PData {
                data: vec![PDataValue {
                    presentation_context_id: context_id,
                    value_type: PDataValueType::Command,
                    is_last: true,
                    data: command_bytes(false),
                }],
            })
            .expect("send wrong type");

        let _ = reader.read_command_object(&mut server).expect("command");
        let result = reader.read_data_pdv(&mut server);
        assert!(matches!(result, Err(DimseError::Protocol(_))));

        let Some((mut server, mut client, context_id)) = setup_ul_pair() else {
            return;
        };
        let mut reader = DimseReader::new();
        client
            .send_pdu(&Pdu::PData {
                data: vec![PDataValue {
                    presentation_context_id: context_id,
                    value_type: PDataValueType::Command,
                    is_last: true,
                    data: command_bytes(true),
                }],
            })
            .expect("send command");
        client
            .send_pdu(&Pdu::PData {
                data: vec![PDataValue {
                    presentation_context_id: context_id + 2,
                    value_type: PDataValueType::Data,
                    is_last: true,
                    data: vec![1],
                }],
            })
            .expect("send wrong context");

        let _ = reader.read_command_object(&mut server).expect("command");
        let result = reader.read_data_pdv(&mut server);
        assert!(matches!(result, Err(DimseError::Protocol(_))));
    }

    #[test]
    fn reader_maps_abort_and_release_pdus_to_expected_errors() {
        let Some((mut server, mut client, _context_id)) = setup_ul_pair() else {
            return;
        };
        let mut reader = DimseReader::new();

        client
            .send_pdu(&Pdu::AbortRQ {
                source: AbortRQSource::ServiceUser,
            })
            .expect("send abort");
        let abort = reader.read_command_object(&mut server);
        assert!(matches!(
            abort,
            Err(DimseError::Ul(rustcoon_ul::UlError::Aborted))
        ));

        let Some((mut server, mut client, _context_id)) = setup_ul_pair() else {
            return;
        };
        let mut reader = DimseReader::new();
        client.send_pdu(&Pdu::ReleaseRQ).expect("send release rq");
        let rq = reader.read_command_object(&mut server);
        assert!(matches!(rq, Err(DimseError::PeerReleaseRequested)));

        let Some((mut server, mut client, _context_id)) = setup_ul_pair() else {
            return;
        };
        let mut reader = DimseReader::new();
        client.send_pdu(&Pdu::ReleaseRP).expect("send release rp");
        let rp = reader.read_command_object(&mut server);
        assert!(matches!(
            rp,
            Err(DimseError::Ul(rustcoon_ul::UlError::Closed))
        ));
    }
}
