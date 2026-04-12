use std::fs::File;
use std::io::{BufReader, Seek, SeekFrom, Write};
use std::sync::Arc;

use async_trait::async_trait;
use dicom_core::Tag;
use dicom_dictionary_std::{tags, uids};
use dicom_encoding::transfer_syntax::TransferSyntaxIndex;
use dicom_object::DicomCollectorOptions;
use dicom_object::InMemDicomObject;
use dicom_object::file::ReadPreamble;
use dicom_transfer_syntax_registry::TransferSyntaxRegistry;
use dicom_ul::pdu::PDataValue;
use rustcoon_dicom::{
    DicomInstanceIdentity, DicomInstanceMetadata, DicomInstanceRecord, DicomPatient,
    DicomSeriesMetadata, DicomStudyMetadata, SeriesInstanceUid, SopClassUid, SopInstanceUid,
    StudyInstanceUid, TransferSyntaxUid,
};
use rustcoon_ingest::{IngestError, IngestRequest, IngestService};
use tempfile::NamedTempFile;

use crate::context::AssociationContext;
use crate::error::DimseError;
use crate::instrumentation::DimseErrorClass;
use crate::service::store::{CStoreRequest, CStoreResponse, CStoreStatus};
use crate::service::{
    CommandField, DescribedServiceClassProvider, ServiceBinding, ServiceClassProvider,
};

/// Storage Service Class (C-STORE SCP) provider backed by the ingest application layer.
pub struct StorageServiceProvider {
    ingest: Arc<IngestService>,
    bindings: Vec<ServiceBinding>,
}

impl StorageServiceProvider {
    pub const DEFAULT_STORAGE_SOP_CLASS_UIDS: &[&str] = &[
        uids::COMPUTED_RADIOGRAPHY_IMAGE_STORAGE,
        uids::DIGITAL_X_RAY_IMAGE_STORAGE_FOR_PRESENTATION,
        uids::DIGITAL_X_RAY_IMAGE_STORAGE_FOR_PROCESSING,
        uids::DIGITAL_MAMMOGRAPHY_X_RAY_IMAGE_STORAGE_FOR_PRESENTATION,
        uids::DIGITAL_MAMMOGRAPHY_X_RAY_IMAGE_STORAGE_FOR_PROCESSING,
        uids::DIGITAL_INTRA_ORAL_X_RAY_IMAGE_STORAGE_FOR_PRESENTATION,
        uids::DIGITAL_INTRA_ORAL_X_RAY_IMAGE_STORAGE_FOR_PROCESSING,
        uids::CT_IMAGE_STORAGE,
        uids::ENHANCED_CT_IMAGE_STORAGE,
        uids::LEGACY_CONVERTED_ENHANCED_CT_IMAGE_STORAGE,
        uids::MR_IMAGE_STORAGE,
        uids::ENHANCED_MR_IMAGE_STORAGE,
        uids::ENHANCED_MR_COLOR_IMAGE_STORAGE,
        uids::LEGACY_CONVERTED_ENHANCED_MR_IMAGE_STORAGE,
        uids::ULTRASOUND_IMAGE_STORAGE,
        uids::ULTRASOUND_MULTI_FRAME_IMAGE_STORAGE,
        uids::SECONDARY_CAPTURE_IMAGE_STORAGE,
        uids::MULTI_FRAME_SINGLE_BIT_SECONDARY_CAPTURE_IMAGE_STORAGE,
        uids::MULTI_FRAME_GRAYSCALE_BYTE_SECONDARY_CAPTURE_IMAGE_STORAGE,
        uids::MULTI_FRAME_GRAYSCALE_WORD_SECONDARY_CAPTURE_IMAGE_STORAGE,
        uids::MULTI_FRAME_TRUE_COLOR_SECONDARY_CAPTURE_IMAGE_STORAGE,
        uids::X_RAY_ANGIOGRAPHIC_IMAGE_STORAGE,
        uids::ENHANCED_XA_IMAGE_STORAGE,
        uids::X_RAY_RADIOFLUOROSCOPIC_IMAGE_STORAGE,
        uids::ENHANCED_XRF_IMAGE_STORAGE,
        uids::NUCLEAR_MEDICINE_IMAGE_STORAGE,
        uids::POSITRON_EMISSION_TOMOGRAPHY_IMAGE_STORAGE,
        uids::ENHANCED_PET_IMAGE_STORAGE,
        uids::LEGACY_CONVERTED_ENHANCED_PET_IMAGE_STORAGE,
    ];

    pub fn new(
        ingest: Arc<IngestService>,
        sop_class_uids: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        Self {
            ingest,
            bindings: sop_class_uids
                .into_iter()
                .map(|uid| ServiceBinding::owned(CommandField::CStoreRq, uid.into()))
                .collect(),
        }
    }

    pub fn with_default_storage_sop_classes(ingest: Arc<IngestService>) -> Self {
        Self::new(ingest, Self::DEFAULT_STORAGE_SOP_CLASS_UIDS.iter().copied())
    }
}

#[async_trait]
impl ServiceClassProvider for StorageServiceProvider {
    async fn handle(&self, ctx: &mut AssociationContext) -> Result<(), DimseError> {
        let request = CStoreRequest::from_command(&ctx.read_command().await?)?;
        tracing::debug!(stage = "validate", "C-STORE request validated");
        let failure = match receive_data_set_to_temp_file(ctx).await {
            Ok(payload_file) => {
                tracing::debug!(stage = "dataset_received", "C-STORE data set received");
                match build_ingest_request(ctx, &request, payload_file.as_file()) {
                    Ok(ingest_request) => match payload_file.reopen() {
                        Ok(std_file) => {
                            let mut reader = tokio::fs::File::from_std(std_file);
                            tracing::debug!(
                                stage = "backend_call",
                                backend = "ingest",
                                "C-STORE ingest started"
                            );
                            match self.ingest.ingest(ingest_request, &mut reader).await {
                                Ok(_) => None,
                                Err(error) => {
                                    tracing::warn!(
                                        stage = "backend_failure",
                                        backend = "ingest",
                                        error = %error,
                                        "C-STORE ingest failed"
                                    );
                                    Some(map_ingest_error_status(&error))
                                }
                            }
                        }
                        Err(_) => Some(StoreFailure::out_of_resources(
                            "failed to reopen temporary payload storage",
                        )),
                    },
                    Err(failure) => Some(failure),
                }
            }
            Err(ReceiveDataSetError::Dimse(error)) => return Err(error),
            Err(ReceiveDataSetError::Status(status)) => Some(StoreFailure::new(status)),
        };

        let response = if let Some(failure) = failure {
            let mut response = CStoreResponse::for_request(&request, failure.status);
            ctx.record_response_error_class(store_status_error_class(failure.status));
            if let Some(comment) = failure.error_comment {
                response = response.with_error_comment(comment);
            }
            for tag in failure.offending_elements {
                response = response.with_offending_element(tag);
            }
            response
        } else {
            CStoreResponse::success_for(&request)
        };
        let status = response.status.code();
        let response = response.to_command_object();
        ctx.send_command_object(request.presentation_context_id, &response)
            .await?;
        ctx.record_response_status(status);
        tracing::debug!(
            stage = "response",
            status = format!("0x{status:04X}"),
            "C-STORE response sent"
        );
        Ok(())
    }
}

impl DescribedServiceClassProvider for StorageServiceProvider {
    fn bindings(&self) -> &[ServiceBinding] {
        &self.bindings
    }
}

enum ReceiveDataSetError {
    Dimse(DimseError),
    Status(CStoreStatus),
}

#[derive(Debug)]
struct StoreFailure {
    status: CStoreStatus,
    offending_elements: Vec<Tag>,
    error_comment: Option<String>,
}

impl StoreFailure {
    fn new(status: CStoreStatus) -> Self {
        Self {
            status,
            offending_elements: Vec::new(),
            error_comment: None,
        }
    }

    fn cannot_understand(comment: impl Into<String>) -> Self {
        Self {
            status: CStoreStatus::CannotUnderstand,
            offending_elements: Vec::new(),
            error_comment: Some(comment.into()),
        }
    }

    fn out_of_resources(comment: impl Into<String>) -> Self {
        Self {
            status: CStoreStatus::OutOfResources,
            offending_elements: Vec::new(),
            error_comment: Some(comment.into()),
        }
    }

    fn with_offending_element(mut self, tag: Tag) -> Self {
        self.offending_elements.push(tag);
        self
    }
}

async fn drain_remaining_data_set(ctx: &mut AssociationContext) -> Result<(), DimseError> {
    while ctx.read_data_pdv().await?.is_some() {}
    Ok(())
}

async fn receive_data_set_to_temp_file(
    ctx: &mut AssociationContext,
) -> Result<NamedTempFile, ReceiveDataSetError> {
    let mut file = match NamedTempFile::new() {
        Ok(file) => file,
        Err(_) => {
            drain_remaining_data_set(ctx).await?;
            return Err(ReceiveDataSetError::Status(CStoreStatus::OutOfResources));
        }
    };

    while let Some(PDataValue { data, .. }) = ctx.read_data_pdv().await? {
        if file.write_all(&data).is_err() {
            drain_remaining_data_set(ctx).await?;
            return Err(ReceiveDataSetError::Status(CStoreStatus::OutOfResources));
        }
    }
    if file.flush().is_err() {
        return Err(ReceiveDataSetError::Status(CStoreStatus::OutOfResources));
    }
    Ok(file)
}

impl From<DimseError> for ReceiveDataSetError {
    fn from(error: DimseError) -> Self {
        Self::Dimse(error)
    }
}

fn build_ingest_request(
    ctx: &AssociationContext,
    request: &CStoreRequest,
    payload: &File,
) -> Result<IngestRequest, StoreFailure> {
    let presentation_context = ctx
        .association()
        .presentation_contexts()
        .iter()
        .find(|pc| pc.id == request.presentation_context_id)
        .ok_or_else(|| StoreFailure::cannot_understand("presentation context not negotiated"))?;
    if presentation_context.abstract_syntax != request.affected_sop_class_uid {
        return Err(StoreFailure::cannot_understand(
            "presentation context abstract syntax does not match command Affected SOP Class UID",
        )
        .with_offending_element(tags::AFFECTED_SOP_CLASS_UID));
    }
    let transfer_syntax_uid = presentation_context.transfer_syntax.clone();
    let transfer_syntax = TransferSyntaxRegistry
        .get(&transfer_syntax_uid)
        .ok_or_else(|| {
            StoreFailure::cannot_understand("negotiated transfer syntax is not recognized")
        })?;

    if !transfer_syntax.can_decode_dataset() {
        return Err(StoreFailure::cannot_understand(
            "negotiated transfer syntax cannot decode data sets",
        ));
    }

    let mut reader = payload
        .try_clone()
        .map_err(|_| StoreFailure::out_of_resources("failed to clone temporary payload storage"))?;
    reader
        .seek(SeekFrom::Start(0))
        .map_err(|_| StoreFailure::out_of_resources("failed to seek temporary payload storage"))?;
    let mut collector = DicomCollectorOptions::new()
        .expected_ts(transfer_syntax_uid.clone())
        .read_preamble(ReadPreamble::Never)
        .from_reader(BufReader::new(reader));
    let mut data_set = InMemDicomObject::new_empty();
    collector
        .read_dataset_up_to_pixeldata(&mut data_set)
        .map_err(|_| StoreFailure::cannot_understand("failed to decode C-STORE data set"))?;

    let data_set_sop_class_uid =
        required_string(&data_set, tags::SOP_CLASS_UID).map_err(|tag| {
            StoreFailure::cannot_understand("missing or invalid SOP Class UID in data set")
                .with_offending_element(tag)
        })?;
    let data_set_sop_instance_uid =
        required_string(&data_set, tags::SOP_INSTANCE_UID).map_err(|tag| {
            StoreFailure::cannot_understand("missing or invalid SOP Instance UID in data set")
                .with_offending_element(tag)
        })?;
    if data_set_sop_class_uid != request.affected_sop_class_uid {
        let mut failure = StoreFailure::new(CStoreStatus::DataSetDoesNotMatchSopClass)
            .with_offending_element(tags::SOP_CLASS_UID);
        failure.error_comment = Some("data set SOP Class UID does not match command".to_string());
        return Err(failure);
    }
    if data_set_sop_instance_uid != request.affected_sop_instance_uid {
        let mut failure = StoreFailure::new(CStoreStatus::CannotUnderstand)
            .with_offending_element(tags::SOP_INSTANCE_UID);
        failure.error_comment =
            Some("data set SOP Instance UID does not match command".to_string());
        return Err(failure);
    }

    let record = DicomInstanceRecord::new(
        DicomInstanceIdentity::new(
            StudyInstanceUid::new(
                required_string(&data_set, tags::STUDY_INSTANCE_UID).map_err(|tag| {
                    StoreFailure::cannot_understand(
                        "missing or invalid Study Instance UID in data set",
                    )
                    .with_offending_element(tag)
                })?,
            )
            .map_err(|_| {
                StoreFailure::cannot_understand("invalid Study Instance UID in data set")
                    .with_offending_element(tags::STUDY_INSTANCE_UID)
            })?,
            SeriesInstanceUid::new(
                required_string(&data_set, tags::SERIES_INSTANCE_UID).map_err(|tag| {
                    StoreFailure::cannot_understand(
                        "missing or invalid Series Instance UID in data set",
                    )
                    .with_offending_element(tag)
                })?,
            )
            .map_err(|_| {
                StoreFailure::cannot_understand("invalid Series Instance UID in data set")
                    .with_offending_element(tags::SERIES_INSTANCE_UID)
            })?,
            SopInstanceUid::new(request.affected_sop_instance_uid.clone()).map_err(|_| {
                StoreFailure::cannot_understand("invalid Affected SOP Instance UID in command")
                    .with_offending_element(tags::AFFECTED_SOP_INSTANCE_UID)
            })?,
            SopClassUid::new(request.affected_sop_class_uid.clone()).map_err(|_| {
                StoreFailure::cannot_understand("invalid Affected SOP Class UID in command")
                    .with_offending_element(tags::AFFECTED_SOP_CLASS_UID)
            })?,
        ),
        DicomPatient::new(
            optional_string(&data_set, tags::PATIENT_ID).map_err(|tag| {
                StoreFailure::cannot_understand("invalid Patient ID in data set")
                    .with_offending_element(tag)
            })?,
            optional_string(&data_set, tags::PATIENT_NAME).map_err(|tag| {
                StoreFailure::cannot_understand("invalid Patient Name in data set")
                    .with_offending_element(tag)
            })?,
        ),
        DicomStudyMetadata::new(
            optional_string(&data_set, tags::ACCESSION_NUMBER).map_err(|tag| {
                StoreFailure::cannot_understand("invalid Accession Number in data set")
                    .with_offending_element(tag)
            })?,
            optional_string(&data_set, tags::STUDY_ID).map_err(|tag| {
                StoreFailure::cannot_understand("invalid Study ID in data set")
                    .with_offending_element(tag)
            })?,
        ),
        DicomSeriesMetadata::new(
            optional_string(&data_set, tags::MODALITY).map_err(|tag| {
                StoreFailure::cannot_understand("invalid Modality in data set")
                    .with_offending_element(tag)
            })?,
            optional_u32(&data_set, tags::SERIES_NUMBER).map_err(|tag| {
                StoreFailure::cannot_understand("invalid Series Number in data set")
                    .with_offending_element(tag)
            })?,
        ),
        DicomInstanceMetadata::new(
            optional_u32(&data_set, tags::INSTANCE_NUMBER).map_err(|tag| {
                StoreFailure::cannot_understand("invalid Instance Number in data set")
                    .with_offending_element(tag)
            })?,
            Some(TransferSyntaxUid::new(transfer_syntax_uid).map_err(|_| {
                StoreFailure::cannot_understand("invalid negotiated transfer syntax UID")
            })?),
        ),
    );

    Ok(IngestRequest::new(record).with_attributes(data_set))
}

fn required_string(data_set: &InMemDicomObject, tag: Tag) -> Result<String, Tag> {
    data_set
        .element(tag)
        .map_err(|_| tag)?
        .to_str()
        .map(|value| value.to_string())
        .map_err(|_| tag)
}

fn optional_string(data_set: &InMemDicomObject, tag: Tag) -> Result<Option<String>, Tag> {
    let element = match data_set.element(tag) {
        Ok(element) => element,
        Err(_) => return Ok(None),
    };

    element
        .to_str()
        .map(|value| Some(value.to_string()))
        .map_err(|_| tag)
}

fn optional_u32(data_set: &InMemDicomObject, tag: Tag) -> Result<Option<u32>, Tag> {
    let element = match data_set.element(tag) {
        Ok(element) => element,
        Err(_) => return Ok(None),
    };

    element.to_int::<u32>().map(Some).map_err(|_| tag)
}

fn map_ingest_error_status(error: &IngestError) -> StoreFailure {
    match error {
        IngestError::BeginWrite(_)
        | IngestError::CommitWrite(_)
        | IngestError::HeadBlob(_)
        | IngestError::CatalogUpdate { .. } => {
            StoreFailure::out_of_resources("failed to persist received instance")
        }
        IngestError::BlobKey(_)
        | IngestError::ReadPayload(_)
        | IngestError::WritePayload(_)
        | IngestError::AbortWrite(_) => {
            let mut failure = StoreFailure::new(CStoreStatus::OutOfResources);
            failure.error_comment =
                Some("failed while processing received instance payload".to_string());
            failure
        }
    }
}

fn store_status_error_class(status: CStoreStatus) -> DimseErrorClass {
    match status {
        CStoreStatus::Success => DimseErrorClass::new("service", "unknown"),
        CStoreStatus::OutOfResources => DimseErrorClass::new("backend", "out_of_resources"),
        CStoreStatus::DataSetDoesNotMatchSopClass => {
            DimseErrorClass::new("service", "invalid_dataset")
        }
        CStoreStatus::CannotUnderstand => DimseErrorClass::new("service", "unable_to_process"),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::io::ErrorKind;
    use std::io::Write;
    use std::net::SocketAddr;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use async_trait::async_trait;
    use dicom_core::{DataElement, PrimitiveValue, VR};
    use dicom_dictionary_std::{tags, uids};
    use dicom_encoding::TransferSyntaxIndex;
    use dicom_object::InMemDicomObject;
    use dicom_transfer_syntax_registry::TransferSyntaxRegistry;
    use dicom_ul::pdu::{PDataValue, PDataValueType};
    use rustcoon_application_entity::ApplicationEntityRegistry;
    use rustcoon_config::application_entity::{
        ApplicationEntitiesConfig, LocalApplicationEntityConfig, RemoteApplicationEntityConfig,
    };
    use rustcoon_index::{
        CatalogInstanceEntry, CatalogQuery, CatalogQueryEntry, CatalogReadStore,
        CatalogSeriesEntry, CatalogStudyEntry, CatalogUpsertOutcome, CatalogWriteStore, IndexError,
        Page, Paging, StoredObjectRef,
    };
    use rustcoon_ingest::{HierarchicalInstanceKeyResolver, IngestError, IngestService};
    use rustcoon_storage::{
        BlobDeleteStore, BlobKey, BlobMetadata, BlobReadRange, BlobReadStore, BlobReader,
        BlobStore, BlobWriteRequest, BlobWriteSession, BlobWriteStore, StorageError,
        StorageOperation,
    };
    use rustcoon_ul::{OutboundAssociationRequest, UlAssociation, UlListener};
    use tempfile::NamedTempFile;

    use super::{
        CStoreRequest, CStoreStatus, StorageServiceProvider, build_ingest_request,
        drain_remaining_data_set, map_ingest_error_status, optional_string, optional_u32,
        required_string,
    };
    use crate::service::{CommandField, DescribedServiceClassProvider, DimseCommand};
    use crate::{AssociationContext, DimseError, DimseReader, DimseWriter, ServiceClassProvider};

    #[derive(Default)]
    struct State {
        blobs: HashMap<String, Vec<u8>>,
        metadata: HashMap<String, BlobMetadata>,
        requests: Vec<rustcoon_index::InstanceUpsertRequest>,
    }

    struct BlobStoreMock {
        state: Arc<Mutex<State>>,
    }

    struct SessionMock {
        key: BlobKey,
        buffer: Vec<u8>,
        state: Arc<Mutex<State>>,
    }

    #[async_trait]
    impl BlobWriteSession for SessionMock {
        async fn write_chunk(&mut self, chunk: &[u8]) -> Result<(), StorageError> {
            self.buffer.extend_from_slice(chunk);
            Ok(())
        }

        async fn commit(self: Box<Self>) -> Result<(), StorageError> {
            let mut state = self.state.lock().expect("state lock");
            state
                .blobs
                .insert(self.key.to_string(), self.buffer.clone());
            state.metadata.insert(
                self.key.to_string(),
                BlobMetadata {
                    key: self.key,
                    size_bytes: self.buffer.len() as u64,
                    content_type: Some("application/dicom".to_string()),
                    version: None,
                    created_at: None,
                    updated_at: None,
                },
            );
            Ok(())
        }

        async fn abort(self: Box<Self>) -> Result<(), StorageError> {
            Ok(())
        }
    }

    #[async_trait]
    impl BlobWriteStore for BlobStoreMock {
        async fn begin_write(
            &self,
            request: BlobWriteRequest,
        ) -> Result<Box<dyn BlobWriteSession>, StorageError> {
            Ok(Box::new(SessionMock {
                key: request.key,
                buffer: Vec::new(),
                state: Arc::clone(&self.state),
            }))
        }
    }

    #[async_trait]
    impl BlobReadStore for BlobStoreMock {
        async fn head(&self, key: &BlobKey) -> Result<BlobMetadata, StorageError> {
            self.state
                .lock()
                .expect("state lock")
                .metadata
                .get(key.as_str())
                .cloned()
                .ok_or_else(|| StorageError::not_found(key.clone()))
        }

        async fn open(&self, _key: &BlobKey) -> Result<BlobReader, StorageError> {
            unreachable!()
        }

        async fn open_range(
            &self,
            _key: &BlobKey,
            _range: BlobReadRange,
        ) -> Result<BlobReader, StorageError> {
            unreachable!()
        }
    }

    #[async_trait]
    impl BlobDeleteStore for BlobStoreMock {
        async fn delete(&self, _key: &BlobKey) -> Result<(), StorageError> {
            Ok(())
        }
    }

    struct CatalogMock {
        state: Arc<Mutex<State>>,
    }

    fn local(title: &str, bind: SocketAddr) -> LocalApplicationEntityConfig {
        LocalApplicationEntityConfig {
            title: title.to_string(),
            bind_address: bind,
            read_timeout_seconds: Some(1),
            write_timeout_seconds: Some(1),
            max_pdu_length: 16_384,
            max_concurrent_associations: 64,
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

    async fn setup_ul_pair(abstract_syntax_uid: &str) -> Option<(UlAssociation, UlAssociation)> {
        let registry = Arc::new(
            ApplicationEntityRegistry::try_from_config(&ApplicationEntitiesConfig {
                local: vec![local(
                    "REMOTE_SCP",
                    "127.0.0.1:0".parse().expect("valid addr"),
                )],
                remote: vec![remote(
                    "LOCAL_SCU",
                    "127.0.0.1:11112".parse().expect("valid addr"),
                )],
            })
            .expect("valid registry"),
        );

        let listener = match UlListener::bind_from_registry(Arc::clone(&registry), "REMOTE_SCP")
            .await
        {
            Ok(listener) => listener.with_abstract_syntax(abstract_syntax_uid),
            Err(rustcoon_ul::UlError::Io(error)) if error.kind() == ErrorKind::PermissionDenied => {
                return None;
            }
            Err(error) => panic!("listener should bind: {error}"),
        };
        let addr = listener.local_addr().expect("listener addr");
        let server = tokio::spawn(async move { listener.accept().await.expect("server accept").0 });

        let client = OutboundAssociationRequest::new("LOCAL_SCU", "REMOTE_SCP", addr)
            .connect_timeout(Duration::from_secs(1))
            .read_timeout(Duration::from_secs(1))
            .write_timeout(Duration::from_secs(1))
            .with_abstract_syntax(abstract_syntax_uid)
            .establish()
            .await
            .expect("client establish");

        let server_association = server.await.expect("server join");
        Some((server_association, client))
    }

    #[async_trait]
    impl CatalogReadStore for CatalogMock {
        async fn get_study(
            &self,
            _study_instance_uid: &rustcoon_dicom::StudyInstanceUid,
        ) -> Result<Option<CatalogStudyEntry>, IndexError> {
            Ok(None)
        }

        async fn get_series(
            &self,
            _series_instance_uid: &rustcoon_dicom::SeriesInstanceUid,
        ) -> Result<Option<CatalogSeriesEntry>, IndexError> {
            Ok(None)
        }

        async fn get_instance(
            &self,
            _sop_instance_uid: &rustcoon_dicom::SopInstanceUid,
        ) -> Result<Option<CatalogInstanceEntry>, IndexError> {
            Ok(None)
        }

        async fn query(&self, _query: CatalogQuery) -> Result<Page<CatalogQueryEntry>, IndexError> {
            Ok(Page::new(
                Vec::new(),
                Some(Paging::new(0, 10).unwrap()),
                Some(0),
            ))
        }
    }

    #[async_trait]
    impl CatalogWriteStore for CatalogMock {
        async fn upsert_instance(
            &self,
            request: rustcoon_index::InstanceUpsertRequest,
        ) -> Result<CatalogUpsertOutcome, IndexError> {
            self.state
                .lock()
                .expect("state lock")
                .requests
                .push(request);
            Ok(CatalogUpsertOutcome::Created)
        }

        async fn attach_blob(
            &self,
            _identity: &rustcoon_dicom::DicomInstanceIdentity,
            _blob: StoredObjectRef,
        ) -> Result<(), IndexError> {
            Ok(())
        }
    }

    fn c_store_rq_command() -> InMemDicomObject {
        let mut command = InMemDicomObject::new_empty();
        command.put(DataElement::new(
            tags::COMMAND_FIELD,
            VR::US,
            PrimitiveValue::from(0x0001_u16),
        ));
        command.put(DataElement::new(
            tags::COMMAND_DATA_SET_TYPE,
            VR::US,
            PrimitiveValue::from(0x0000_u16),
        ));
        command.put(DataElement::new(
            tags::MESSAGE_ID,
            VR::US,
            PrimitiveValue::from(7_u16),
        ));
        command.put(DataElement::new(
            tags::PRIORITY,
            VR::US,
            PrimitiveValue::from(0_u16),
        ));
        command.put(DataElement::new(
            tags::AFFECTED_SOP_CLASS_UID,
            VR::UI,
            uids::CT_IMAGE_STORAGE,
        ));
        command.put(DataElement::new(
            tags::AFFECTED_SOP_INSTANCE_UID,
            VR::UI,
            "1.2.3.4",
        ));
        command
    }

    fn data_set() -> InMemDicomObject {
        let mut data_set = InMemDicomObject::new_empty();
        data_set.put(DataElement::new(
            tags::SOP_CLASS_UID,
            VR::UI,
            uids::CT_IMAGE_STORAGE,
        ));
        data_set.put(DataElement::new(tags::SOP_INSTANCE_UID, VR::UI, "1.2.3.4"));
        data_set.put(DataElement::new(tags::STUDY_INSTANCE_UID, VR::UI, "1.2.3"));
        data_set.put(DataElement::new(
            tags::SERIES_INSTANCE_UID,
            VR::UI,
            "1.2.3.1",
        ));
        data_set.put(DataElement::new(tags::PATIENT_ID, VR::LO, "PAT-001"));
        data_set.put(DataElement::new(tags::PATIENT_NAME, VR::PN, "Jane^Doe"));
        data_set.put(DataElement::new(tags::MODALITY, VR::CS, "CT"));
        data_set
    }

    fn store_request(context_id: u8) -> CStoreRequest {
        CStoreRequest {
            presentation_context_id: context_id,
            message_id: 7,
            priority: crate::Priority::Medium,
            affected_sop_class_uid: uids::CT_IMAGE_STORAGE.to_string(),
            affected_sop_instance_uid: "1.2.3.4".to_string(),
            move_originator_ae_title: Some("MOVE_SCU".to_string()),
            move_originator_message_id: Some(99),
        }
    }

    fn serialize_data_set(
        association: &UlAssociation,
        presentation_context_id: u8,
        data_set: &InMemDicomObject,
    ) -> Vec<u8> {
        let transfer_syntax_uid = association
            .presentation_contexts()
            .iter()
            .find(|pc| pc.id == presentation_context_id)
            .expect("presentation context")
            .transfer_syntax
            .clone();
        let transfer_syntax = TransferSyntaxRegistry
            .get(&transfer_syntax_uid)
            .expect("negotiated transfer syntax");
        let mut bytes = Vec::new();
        data_set
            .write_dataset_with_ts(&mut bytes, transfer_syntax)
            .expect("serialize data set");
        bytes
    }

    fn data_set_file(
        association: &UlAssociation,
        presentation_context_id: u8,
        data_set: &InMemDicomObject,
    ) -> NamedTempFile {
        let bytes = serialize_data_set(association, presentation_context_id, data_set);
        let mut file = NamedTempFile::new().expect("temp file");
        file.write_all(&bytes).expect("write temp file");
        file.flush().expect("flush temp file");
        file
    }

    #[test]
    fn bindings_cover_configured_sop_classes() {
        let state = Arc::new(Mutex::new(State::default()));
        let storage: Arc<dyn BlobStore> = Arc::new(BlobStoreMock {
            state: Arc::clone(&state),
        });
        let catalog = Arc::new(CatalogMock { state });
        let provider = StorageServiceProvider::new(
            Arc::new(IngestService::new(
                storage,
                catalog.clone(),
                catalog,
                Arc::new(HierarchicalInstanceKeyResolver::new()),
            )),
            [uids::CT_IMAGE_STORAGE, uids::MR_IMAGE_STORAGE],
        );

        let bindings = provider.bindings();
        assert_eq!(bindings.len(), 2);
        assert_eq!(bindings[0].command_field, CommandField::CStoreRq);
        assert_eq!(bindings[0].sop_class_uid.as_ref(), uids::CT_IMAGE_STORAGE);
        assert_eq!(bindings[1].sop_class_uid.as_ref(), uids::MR_IMAGE_STORAGE);
    }

    #[tokio::test]
    async fn storage_provider_handles_store_and_returns_success_response() {
        let Some((server_association, mut client_association)) =
            setup_ul_pair(uids::CT_IMAGE_STORAGE).await
        else {
            return;
        };
        let context_id = client_association.presentation_contexts()[0].id;

        let state = Arc::new(Mutex::new(State::default()));
        let storage: Arc<dyn BlobStore> = Arc::new(BlobStoreMock {
            state: Arc::clone(&state),
        });
        let catalog = Arc::new(CatalogMock {
            state: Arc::clone(&state),
        });
        let provider = StorageServiceProvider::new(
            Arc::new(IngestService::new(
                storage,
                catalog.clone(),
                catalog,
                Arc::new(HierarchicalInstanceKeyResolver::new()),
            )),
            [uids::CT_IMAGE_STORAGE],
        );

        DimseWriter::new()
            .send_command_object(&mut client_association, context_id, &c_store_rq_command())
            .await
            .expect("send C-STORE-RQ command");

        let bytes = serialize_data_set(&client_association, context_id, &data_set());
        DimseWriter::new()
            .send_data_pdv(
                &mut client_association,
                PDataValue {
                    presentation_context_id: context_id,
                    value_type: PDataValueType::Data,
                    is_last: true,
                    data: bytes,
                },
            )
            .await
            .expect("send data set");

        let mut server_context = AssociationContext::new(server_association);
        provider
            .handle(&mut server_context)
            .await
            .expect("handle C-STORE-RQ");

        let response = DimseReader::new()
            .read_command_object(&mut client_association)
            .await
            .expect("read C-STORE-RSP");
        let response = DimseCommand::from_command_object(&response).expect("parse C-STORE-RSP");
        assert_eq!(response.command_field, CommandField::CStoreRsp);
        assert_eq!(response.message_id_being_responded_to, Some(7));
        assert_eq!(response.status, Some(0x0000));
        assert!(!response.has_data_set);

        let state = state.lock().expect("state lock");
        assert_eq!(state.requests.len(), 1);
        assert_eq!(
            state.requests[0]
                .record
                .identity()
                .sop_instance_uid()
                .as_str(),
            "1.2.3.4"
        );
    }

    #[test]
    fn store_request_parser_requires_dataset_and_priority() {
        let mut command = DimseCommand {
            presentation_context_id: 1,
            command_field: CommandField::CStoreRq,
            sop_class_uid: Some(uids::CT_IMAGE_STORAGE.to_string()),
            sop_instance_uid: Some("1.2.3.4".to_string()),
            message_id: Some(7),
            message_id_being_responded_to: None,
            priority: None,
            status: None,
            move_destination: None,
            move_originator_ae_title: None,
            move_originator_message_id: None,
            has_data_set: true,
        };

        let error =
            crate::service::CStoreRequest::from_command(&command).expect_err("priority required");
        assert!(
            matches!(error, DimseError::Protocol(message) if message.contains("missing Priority"))
        );

        command.priority = Some(crate::Priority::Medium);
        command.has_data_set = false;
        let error =
            crate::service::CStoreRequest::from_command(&command).expect_err("dataset required");
        assert!(
            matches!(error, DimseError::Protocol(message) if message.contains("must include a data set"))
        );
    }

    #[tokio::test]
    async fn storage_provider_returns_sop_class_mismatch_status() {
        let Some((server_association, mut client_association)) =
            setup_ul_pair(uids::CT_IMAGE_STORAGE).await
        else {
            return;
        };
        let context_id = client_association.presentation_contexts()[0].id;

        let state = Arc::new(Mutex::new(State::default()));
        let storage: Arc<dyn BlobStore> = Arc::new(BlobStoreMock {
            state: Arc::clone(&state),
        });
        let catalog = Arc::new(CatalogMock {
            state: Arc::clone(&state),
        });
        let provider = StorageServiceProvider::new(
            Arc::new(IngestService::new(
                storage,
                catalog.clone(),
                catalog,
                Arc::new(HierarchicalInstanceKeyResolver::new()),
            )),
            [uids::CT_IMAGE_STORAGE],
        );

        DimseWriter::new()
            .send_command_object(&mut client_association, context_id, &c_store_rq_command())
            .await
            .expect("send C-STORE-RQ command");

        let mut mismatched_data_set = data_set();
        mismatched_data_set.put(DataElement::new(
            tags::SOP_CLASS_UID,
            VR::UI,
            uids::MR_IMAGE_STORAGE,
        ));
        let bytes = serialize_data_set(&client_association, context_id, &mismatched_data_set);
        DimseWriter::new()
            .send_data_pdv(
                &mut client_association,
                PDataValue {
                    presentation_context_id: context_id,
                    value_type: PDataValueType::Data,
                    is_last: true,
                    data: bytes,
                },
            )
            .await
            .expect("send data set");

        let mut server_context = AssociationContext::new(server_association);
        provider
            .handle(&mut server_context)
            .await
            .expect("handle C-STORE-RQ");

        let response = DimseReader::new()
            .read_command_object(&mut client_association)
            .await
            .expect("read C-STORE-RSP");
        let response = DimseCommand::from_command_object(&response).expect("parse C-STORE-RSP");
        assert_eq!(response.status, Some(0xA900));

        let state = state.lock().expect("state lock");
        assert!(state.requests.is_empty());
    }

    #[tokio::test]
    async fn storage_provider_returns_cxxx_for_sop_instance_uid_mismatch() {
        let Some((server_association, mut client_association)) =
            setup_ul_pair(uids::CT_IMAGE_STORAGE).await
        else {
            return;
        };
        let context_id = client_association.presentation_contexts()[0].id;

        let state = Arc::new(Mutex::new(State::default()));
        let storage: Arc<dyn BlobStore> = Arc::new(BlobStoreMock {
            state: Arc::clone(&state),
        });
        let catalog = Arc::new(CatalogMock {
            state: Arc::clone(&state),
        });
        let provider = StorageServiceProvider::new(
            Arc::new(IngestService::new(
                storage,
                catalog.clone(),
                catalog,
                Arc::new(HierarchicalInstanceKeyResolver::new()),
            )),
            [uids::CT_IMAGE_STORAGE],
        );

        DimseWriter::new()
            .send_command_object(&mut client_association, context_id, &c_store_rq_command())
            .await
            .expect("send C-STORE-RQ command");

        let mut mismatched_data_set = data_set();
        mismatched_data_set.put(DataElement::new(tags::SOP_INSTANCE_UID, VR::UI, "9.9.9.9"));
        let bytes = serialize_data_set(&client_association, context_id, &mismatched_data_set);
        DimseWriter::new()
            .send_data_pdv(
                &mut client_association,
                PDataValue {
                    presentation_context_id: context_id,
                    value_type: PDataValueType::Data,
                    is_last: true,
                    data: bytes,
                },
            )
            .await
            .expect("send data set");

        let mut server_context = AssociationContext::new(server_association);
        provider
            .handle(&mut server_context)
            .await
            .expect("handle C-STORE-RQ");

        let response_object = DimseReader::new()
            .read_command_object(&mut client_association)
            .await
            .expect("read C-STORE-RSP");
        let response = DimseCommand::from_command_object(&response_object).expect("parse response");
        assert_eq!(response.status, Some(0xC000));
        assert_eq!(
            response_object
                .command
                .element(tags::ERROR_COMMENT)
                .expect("error comment")
                .to_str()
                .expect("error comment string"),
            "data set SOP Instance UID does not match command"
        );

        let state = state.lock().expect("state lock");
        assert!(state.requests.is_empty());
    }

    #[tokio::test]
    async fn storage_provider_rejects_abstract_syntax_mismatch_with_command() {
        let Some((server_association, mut client_association)) =
            setup_ul_pair(uids::CT_IMAGE_STORAGE).await
        else {
            return;
        };
        let context_id = client_association.presentation_contexts()[0].id;

        let state = Arc::new(Mutex::new(State::default()));
        let storage: Arc<dyn BlobStore> = Arc::new(BlobStoreMock {
            state: Arc::clone(&state),
        });
        let catalog = Arc::new(CatalogMock {
            state: Arc::clone(&state),
        });
        let provider = StorageServiceProvider::new(
            Arc::new(IngestService::new(
                storage,
                catalog.clone(),
                catalog,
                Arc::new(HierarchicalInstanceKeyResolver::new()),
            )),
            [uids::CT_IMAGE_STORAGE],
        );

        let mut command = c_store_rq_command();
        command.put(DataElement::new(
            tags::AFFECTED_SOP_CLASS_UID,
            VR::UI,
            uids::MR_IMAGE_STORAGE,
        ));
        DimseWriter::new()
            .send_command_object(&mut client_association, context_id, &command)
            .await
            .expect("send C-STORE-RQ command");

        let bytes = serialize_data_set(&client_association, context_id, &data_set());
        DimseWriter::new()
            .send_data_pdv(
                &mut client_association,
                PDataValue {
                    presentation_context_id: context_id,
                    value_type: PDataValueType::Data,
                    is_last: true,
                    data: bytes,
                },
            )
            .await
            .expect("send data set");

        let mut server_context = AssociationContext::new(server_association);
        provider
            .handle(&mut server_context)
            .await
            .expect("handle C-STORE-RQ");

        let response_object = DimseReader::new()
            .read_command_object(&mut client_association)
            .await
            .expect("read C-STORE-RSP");
        let response = DimseCommand::from_command_object(&response_object).expect("parse response");
        assert_eq!(response.status, Some(0xC000));
        assert_eq!(
            response_object
                .command
                .element(tags::ERROR_COMMENT)
                .expect("error comment")
                .to_str()
                .expect("error comment string"),
            "presentation context abstract syntax does not match command Affe"
        );
    }

    #[tokio::test]
    async fn build_ingest_request_extracts_metadata_and_rejects_invalid_datasets() {
        let Some((server_association, client_association)) =
            setup_ul_pair(uids::CT_IMAGE_STORAGE).await
        else {
            return;
        };
        let context_id = client_association.presentation_contexts()[0].id;
        let request = store_request(context_id);

        let mut valid_data_set = data_set();
        valid_data_set.put(DataElement::new(tags::ACCESSION_NUMBER, VR::SH, "ACC-001"));
        valid_data_set.put(DataElement::new(tags::STUDY_ID, VR::SH, "STUDY-1"));
        valid_data_set.put(DataElement::new(
            tags::SERIES_NUMBER,
            VR::IS,
            PrimitiveValue::from(42_i32),
        ));
        valid_data_set.put(DataElement::new(
            tags::INSTANCE_NUMBER,
            VR::IS,
            PrimitiveValue::from(7_i32),
        ));
        let payload = data_set_file(&client_association, context_id, &valid_data_set);
        let server_context = AssociationContext::new(server_association);
        let ingest_request = build_ingest_request(&server_context, &request, payload.as_file())
            .expect("ingest request");

        assert_eq!(
            ingest_request.record.study().accession_number(),
            Some("ACC-001")
        );
        assert_eq!(ingest_request.record.study().study_id(), Some("STUDY-1"));
        assert_eq!(ingest_request.record.series().series_number(), Some(42));
        assert_eq!(ingest_request.record.instance().instance_number(), Some(7));
        assert_eq!(
            ingest_request
                .record
                .instance()
                .transfer_syntax_uid()
                .map(|uid| uid.as_str()),
            Some(
                client_association.presentation_contexts()[0]
                    .transfer_syntax
                    .as_str()
            )
        );
        assert!(ingest_request.attributes.element(tags::PIXEL_DATA).is_err());
        assert_eq!(
            ingest_request
                .attributes
                .element(tags::PATIENT_ID)
                .expect("patient id")
                .to_str()
                .expect("patient id string"),
            "PAT-001"
        );

        let Some((server_association, client_association)) =
            setup_ul_pair(uids::CT_IMAGE_STORAGE).await
        else {
            return;
        };
        let context_id = client_association.presentation_contexts()[0].id;
        let request = store_request(context_id);
        let mut invalid_data_set = data_set();
        invalid_data_set.put(DataElement::new(tags::SERIES_NUMBER, VR::IS, "abc"));
        let payload = data_set_file(&client_association, context_id, &invalid_data_set);
        let server_context = AssociationContext::new(server_association);
        let failure = build_ingest_request(&server_context, &request, payload.as_file())
            .expect_err("invalid dataset");
        assert_eq!(failure.status, CStoreStatus::CannotUnderstand);
        assert!(failure.offending_elements.contains(&tags::SERIES_NUMBER));

        let Some((server_association, client_association)) =
            setup_ul_pair(uids::CT_IMAGE_STORAGE).await
        else {
            return;
        };
        let context_id = client_association.presentation_contexts()[0].id;
        let request = store_request(context_id);
        let mut missing_identity_data_set = data_set();
        missing_identity_data_set.remove_element(tags::STUDY_INSTANCE_UID);
        let payload = data_set_file(&client_association, context_id, &missing_identity_data_set);
        let server_context = AssociationContext::new(server_association);
        let failure = build_ingest_request(&server_context, &request, payload.as_file())
            .expect_err("missing study uid");
        assert_eq!(failure.status, CStoreStatus::CannotUnderstand);
        assert!(
            failure
                .offending_elements
                .contains(&tags::STUDY_INSTANCE_UID)
        );

        let Some((server_association, client_association)) =
            setup_ul_pair(uids::CT_IMAGE_STORAGE).await
        else {
            return;
        };
        let context_id = client_association.presentation_contexts()[0].id;
        let request = store_request(context_id);
        let mut invalid_uid_data_set = data_set();
        invalid_uid_data_set.put(DataElement::new(
            tags::SERIES_INSTANCE_UID,
            VR::UI,
            "bad uid",
        ));
        let payload = data_set_file(&client_association, context_id, &invalid_uid_data_set);
        let server_context = AssociationContext::new(server_association);
        let failure = build_ingest_request(&server_context, &request, payload.as_file())
            .expect_err("invalid series uid");
        assert_eq!(failure.status, CStoreStatus::CannotUnderstand);
        assert!(
            failure
                .offending_elements
                .contains(&tags::SERIES_INSTANCE_UID)
        );
    }

    #[test]
    fn helper_functions_cover_validation_error_paths_and_status_mapping() {
        let mut invalid_data_set = InMemDicomObject::new_empty();
        invalid_data_set.put(DataElement::new(tags::PATIENT_NAME, VR::PN, "Jane^Doe"));
        invalid_data_set.put(DataElement::new(tags::INSTANCE_NUMBER, VR::IS, "oops"));

        assert_eq!(
            required_string(&invalid_data_set, tags::STUDY_INSTANCE_UID),
            Err(tags::STUDY_INSTANCE_UID)
        );
        assert_eq!(
            optional_u32(&invalid_data_set, tags::INSTANCE_NUMBER),
            Err(tags::INSTANCE_NUMBER)
        );
        assert_eq!(
            optional_string(&invalid_data_set, tags::ACCESSION_NUMBER),
            Ok(None)
        );

        let begin_write = IngestError::BeginWrite(StorageError::backend(
            "filesystem",
            StorageOperation::BeginWrite,
            std::io::Error::other("disk full"),
        ));
        let blob_key = IngestError::BlobKey(BlobKey::new("").expect_err("invalid blob key"));
        let catalog_update = IngestError::CatalogUpdate {
            source: IndexError::backend(
                "postgres",
                rustcoon_index::IndexOperation::UpsertInstance,
                std::io::Error::other("db down"),
            ),
            rollback_failed: None,
        };
        assert_eq!(
            map_ingest_error_status(&begin_write).status,
            CStoreStatus::OutOfResources
        );
        assert_eq!(
            map_ingest_error_status(&catalog_update).status,
            CStoreStatus::OutOfResources
        );
        assert_eq!(
            map_ingest_error_status(&blob_key).status,
            CStoreStatus::OutOfResources
        );
    }

    #[tokio::test]
    async fn drain_remaining_data_set_consumes_pending_store_payload() {
        let Some((server_association, mut client_association)) =
            setup_ul_pair(uids::CT_IMAGE_STORAGE).await
        else {
            return;
        };
        let context_id = client_association.presentation_contexts()[0].id;

        DimseWriter::new()
            .send_command_object(&mut client_association, context_id, &c_store_rq_command())
            .await
            .expect("send C-STORE-RQ command");

        let bytes = serialize_data_set(&client_association, context_id, &data_set());
        let split_at = bytes.len().max(2) / 2;
        for (index, chunk) in [bytes[..split_at].to_vec(), bytes[split_at..].to_vec()]
            .into_iter()
            .enumerate()
        {
            DimseWriter::new()
                .send_data_pdv(
                    &mut client_association,
                    PDataValue {
                        presentation_context_id: context_id,
                        value_type: PDataValueType::Data,
                        is_last: index == 1,
                        data: chunk,
                    },
                )
                .await
                .expect("send data set fragment");
        }

        let mut server_context = AssociationContext::new(server_association);
        let _request =
            CStoreRequest::from_command(&server_context.read_command().await.expect("command"))
                .expect("parse request");
        assert!(server_context.has_unfinished_data_set());

        drain_remaining_data_set(&mut server_context)
            .await
            .expect("drain remaining dataset");
        assert!(!server_context.has_unfinished_data_set());
        server_context
            .complete_message_cycle()
            .expect("message cycle complete after drain");
    }
}
