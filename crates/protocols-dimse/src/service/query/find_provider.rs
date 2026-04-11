use std::io::Cursor;
use std::sync::Arc;

use dicom_core::Tag;
use dicom_dictionary_std::{tags, uids};
use dicom_encoding::transfer_syntax::TransferSyntaxIndex;
use dicom_object::InMemDicomObject;
use dicom_transfer_syntax_registry::TransferSyntaxRegistry;
use dicom_ul::pdu::{PDataValue, PDataValueType};
use rustcoon_query::{
    CFindQueryModel as AppCFindQueryModel, CFindRequest as AppCFindRequest,
    CFindResponseLocation as AppCFindResponseLocation, QueryError, QueryService,
};
use tokio::runtime::{Builder, Handle};

use crate::context::AssociationContext;
use crate::error::DimseError;
use crate::service::query::{CFindRequest, CFindResponse, CFindStatus};
use crate::service::{
    CommandField, DescribedServiceClassProvider, ServiceBinding, ServiceClassProvider,
};

#[derive(Debug, Clone)]
struct CFindFailure {
    status: CFindStatus,
    offending_elements: Vec<Tag>,
    error_comment: Option<String>,
}

impl CFindFailure {
    fn new(status: CFindStatus) -> Self {
        Self {
            status,
            offending_elements: Vec::new(),
            error_comment: None,
        }
    }

    fn with_offending_element(mut self, tag: Tag) -> Self {
        self.offending_elements.push(tag);
        self
    }

    fn with_error_comment(mut self, comment: impl Into<String>) -> Self {
        self.error_comment = Some(comment.into());
        self
    }
}

/// Query/Retrieve FIND SCP provider backed by the application query service.
pub struct QueryServiceProvider {
    query: Arc<QueryService>,
    default_retrieve_ae_title: String,
}

impl QueryServiceProvider {
    pub const STUDY_ROOT_FIND_SOP_CLASS_UID: &'static str =
        uids::STUDY_ROOT_QUERY_RETRIEVE_INFORMATION_MODEL_FIND;
    pub const PATIENT_ROOT_FIND_SOP_CLASS_UID: &'static str =
        uids::PATIENT_ROOT_QUERY_RETRIEVE_INFORMATION_MODEL_FIND;

    pub fn new(query: Arc<QueryService>, default_retrieve_ae_title: impl Into<String>) -> Self {
        Self {
            query,
            default_retrieve_ae_title: default_retrieve_ae_title.into(),
        }
    }

    fn find_model_for_sop_class_uid(
        sop_class_uid: &str,
    ) -> Result<AppCFindQueryModel, CFindFailure> {
        match sop_class_uid {
            Self::STUDY_ROOT_FIND_SOP_CLASS_UID => Ok(AppCFindQueryModel::StudyRoot),
            Self::PATIENT_ROOT_FIND_SOP_CLASS_UID => Ok(AppCFindQueryModel::PatientRoot),
            _ => Err(
                CFindFailure::new(CFindStatus::IdentifierDoesNotMatchSopClass)
                    .with_offending_element(tags::AFFECTED_SOP_CLASS_UID)
                    .with_error_comment("unsupported Query/Retrieve FIND SOP Class UID"),
            ),
        }
    }

    fn retrieve_ae_title(&self, ctx: &AssociationContext) -> String {
        ctx.route()
            .map(|route| route.called_ae_title.as_str().to_string())
            .unwrap_or_else(|| self.default_retrieve_ae_title.clone())
    }
}

impl ServiceClassProvider for QueryServiceProvider {
    fn handle(&self, ctx: &mut AssociationContext) -> Result<(), DimseError> {
        let request = CFindRequest::from_command(&ctx.read_command()?)?;
        let query_model = match Self::find_model_for_sop_class_uid(&request.affected_sop_class_uid)
        {
            Ok(model) => model,
            Err(failure) => {
                send_failure_response(ctx, &request, failure)?;
                return Ok(());
            }
        };

        let identifier = match read_identifier_data_set(ctx, &request) {
            Ok(identifier) => identifier,
            Err(failure) => {
                send_failure_response(ctx, &request, failure)?;
                return Ok(());
            }
        };

        let app_request = AppCFindRequest {
            model: query_model,
            identifier,
            response_location: AppCFindResponseLocation::RetrieveAeTitle(
                self.retrieve_ae_title(ctx),
            ),
            paging: None,
        };

        let result = block_on_query(self.query.find(app_request));
        let result = match result {
            Ok(result) => result,
            Err(error) => {
                send_failure_response(ctx, &request, map_query_error(error))?;
                return Ok(());
            }
        };

        for matched in result.matches.items {
            let response = CFindResponse::pending_for(&request).to_command_object();
            ctx.send_command_object(request.presentation_context_id, &response)?;
            send_identifier_data_set(
                ctx,
                request.presentation_context_id,
                &request.affected_sop_class_uid,
                &matched.identifier,
            )?;
        }

        let response = CFindResponse::success_for(&request).to_command_object();
        ctx.send_command_object(request.presentation_context_id, &response)?;
        Ok(())
    }
}

impl DescribedServiceClassProvider for QueryServiceProvider {
    fn bindings(&self) -> &[ServiceBinding] {
        static BINDINGS: [ServiceBinding; 2] = [
            ServiceBinding::new(
                CommandField::CFindRq,
                QueryServiceProvider::STUDY_ROOT_FIND_SOP_CLASS_UID,
            ),
            ServiceBinding::new(
                CommandField::CFindRq,
                QueryServiceProvider::PATIENT_ROOT_FIND_SOP_CLASS_UID,
            ),
        ];
        &BINDINGS
    }
}

fn send_failure_response(
    ctx: &mut AssociationContext,
    request: &CFindRequest,
    failure: CFindFailure,
) -> Result<(), DimseError> {
    let mut response = CFindResponse::for_request(request, failure.status);
    for tag in failure.offending_elements {
        response = response.with_offending_element(tag);
    }
    if let Some(comment) = failure.error_comment {
        response = response.with_error_comment(comment);
    }
    ctx.send_command_object(
        request.presentation_context_id,
        &response.to_command_object(),
    )?;
    Ok(())
}

fn map_query_error(error: QueryError) -> CFindFailure {
    match error {
        QueryError::MissingQueryRetrieveLevel => {
            CFindFailure::new(CFindStatus::IdentifierDoesNotMatchSopClass)
                .with_offending_element(tags::QUERY_RETRIEVE_LEVEL)
                .with_error_comment("missing Query/Retrieve Level")
        }
        QueryError::UnsupportedQueryRetrieveLevel { .. } => {
            CFindFailure::new(CFindStatus::IdentifierDoesNotMatchSopClass)
                .with_offending_element(tags::QUERY_RETRIEVE_LEVEL)
                .with_error_comment("unsupported Query/Retrieve Level")
        }
        QueryError::MissingUniqueKey { key, .. } => {
            CFindFailure::new(CFindStatus::IdentifierDoesNotMatchSopClass)
                .with_offending_element(key)
                .with_error_comment("required unique key is missing")
        }
        QueryError::InvalidBaselineHierarchyKey { tag, .. } => {
            CFindFailure::new(CFindStatus::IdentifierDoesNotMatchSopClass)
                .with_offending_element(tag)
                .with_error_comment("identifier hierarchy keys are invalid for baseline query")
        }
        QueryError::UnsupportedQueryKey { tag, .. } => {
            CFindFailure::new(CFindStatus::IdentifierDoesNotMatchSopClass)
                .with_offending_element(tag)
                .with_error_comment("identifier contains an unsupported query key")
        }
        QueryError::InvalidIdentifierElement { tag, .. } => {
            CFindFailure::new(CFindStatus::IdentifierDoesNotMatchSopClass)
                .with_offending_element(tag)
                .with_error_comment("identifier contains an invalid element")
        }
        QueryError::Catalog(_) => CFindFailure::new(CFindStatus::OutOfResources)
            .with_error_comment("catalog query backend is unavailable"),
        QueryError::InvalidCatalogQuery(_) | QueryError::InvalidResponseLocation(_) => {
            CFindFailure::new(CFindStatus::UnableToProcess)
                .with_error_comment("query could not be processed")
        }
    }
}

fn read_identifier_data_set(
    ctx: &mut AssociationContext,
    request: &CFindRequest,
) -> Result<InMemDicomObject, CFindFailure> {
    let transfer_syntax_uid = negotiated_transfer_syntax_uid(
        ctx,
        request.presentation_context_id,
        &request.affected_sop_class_uid,
    )?;
    let transfer_syntax = TransferSyntaxRegistry
        .get(&transfer_syntax_uid)
        .ok_or_else(|| {
            CFindFailure::new(CFindStatus::UnableToProcess)
                .with_error_comment("negotiated transfer syntax is not recognized")
        })?;
    if !transfer_syntax.can_decode_dataset() {
        return Err(CFindFailure::new(CFindStatus::UnableToProcess)
            .with_error_comment("negotiated transfer syntax cannot decode data sets"));
    }

    let mut bytes = Vec::new();
    while let Some(pdv) = ctx.read_data_pdv().map_err(|_| {
        CFindFailure::new(CFindStatus::UnableToProcess)
            .with_error_comment("failed while reading C-FIND identifier")
    })? {
        bytes.extend_from_slice(&pdv.data);
    }
    if bytes.is_empty() {
        return Err(
            CFindFailure::new(CFindStatus::IdentifierDoesNotMatchSopClass)
                .with_error_comment("C-FIND identifier data set is missing"),
        );
    }

    InMemDicomObject::read_dataset_with_ts(Cursor::new(bytes), transfer_syntax).map_err(|_| {
        CFindFailure::new(CFindStatus::UnableToProcess)
            .with_error_comment("failed to decode C-FIND identifier")
    })
}

fn send_identifier_data_set(
    ctx: &mut AssociationContext,
    presentation_context_id: u8,
    affected_sop_class_uid: &str,
    identifier: &InMemDicomObject,
) -> Result<(), DimseError> {
    let transfer_syntax_uid =
        negotiated_transfer_syntax_uid(ctx, presentation_context_id, affected_sop_class_uid)
            .map_err(|failure| {
                DimseError::protocol(failure.error_comment.unwrap_or_else(|| {
                    "failed to resolve transfer syntax for response".to_string()
                }))
            })?;
    let transfer_syntax = TransferSyntaxRegistry
        .get(&transfer_syntax_uid)
        .ok_or_else(|| {
            DimseError::protocol("negotiated transfer syntax is not recognized for C-FIND response")
        })?;
    let mut bytes = Vec::new();
    identifier.write_dataset_with_ts(&mut bytes, transfer_syntax)?;
    ctx.send_data_pdv(PDataValue {
        presentation_context_id,
        value_type: PDataValueType::Data,
        is_last: true,
        data: bytes,
    })
}

fn negotiated_transfer_syntax_uid(
    ctx: &AssociationContext,
    presentation_context_id: u8,
    expected_sop_class_uid: &str,
) -> Result<String, CFindFailure> {
    let presentation_context = ctx
        .association()
        .presentation_contexts()
        .iter()
        .find(|pc| pc.id == presentation_context_id)
        .ok_or_else(|| {
            CFindFailure::new(CFindStatus::UnableToProcess)
                .with_error_comment("presentation context was not negotiated")
        })?;
    if presentation_context.abstract_syntax != expected_sop_class_uid {
        return Err(CFindFailure::new(CFindStatus::IdentifierDoesNotMatchSopClass)
            .with_offending_element(tags::AFFECTED_SOP_CLASS_UID)
            .with_error_comment(
                "presentation context abstract syntax does not match command Affected SOP Class UID",
            ));
    }

    Ok(presentation_context.transfer_syntax.clone())
}

fn block_on_query<T>(future: impl std::future::Future<Output = T>) -> T {
    if let Ok(handle) = Handle::try_current() {
        return handle.block_on(future);
    }

    Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime for C-FIND provider")
        .block_on(future)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use async_trait::async_trait;
    use dicom_dictionary_std::tags;
    use rustcoon_dicom::{SeriesInstanceUid, SopInstanceUid, StudyInstanceUid};
    use rustcoon_index::{
        CatalogInstanceEntry, CatalogQuery, CatalogQueryEntry, CatalogReadStore,
        CatalogSeriesEntry, CatalogStudyEntry, IndexError, Page, Paging,
    };
    use rustcoon_query::QueryService;

    use super::QueryServiceProvider;
    use crate::service::{CommandField, DescribedServiceClassProvider};

    struct NullCatalogReadStore;

    #[async_trait]
    impl CatalogReadStore for NullCatalogReadStore {
        async fn get_study(
            &self,
            _study_instance_uid: &StudyInstanceUid,
        ) -> Result<Option<CatalogStudyEntry>, IndexError> {
            Ok(None)
        }

        async fn get_series(
            &self,
            _series_instance_uid: &SeriesInstanceUid,
        ) -> Result<Option<CatalogSeriesEntry>, IndexError> {
            Ok(None)
        }

        async fn get_instance(
            &self,
            _sop_instance_uid: &SopInstanceUid,
        ) -> Result<Option<CatalogInstanceEntry>, IndexError> {
            Ok(None)
        }

        async fn query(&self, _query: CatalogQuery) -> Result<Page<CatalogQueryEntry>, IndexError> {
            Ok(Page::new(
                Vec::new(),
                Some(Paging::new(0, 1).expect("paging")),
                Some(0),
            ))
        }
    }

    #[test]
    fn bindings_cover_patient_and_study_root_find_models() {
        let provider = QueryServiceProvider::new(
            Arc::new(QueryService::new(Arc::new(NullCatalogReadStore))),
            "RUSTCOON",
        );
        let bindings = provider.bindings();

        assert_eq!(bindings.len(), 2);
        assert_eq!(bindings[0].command_field, CommandField::CFindRq);
        assert_eq!(
            bindings[0].sop_class_uid.as_ref(),
            QueryServiceProvider::STUDY_ROOT_FIND_SOP_CLASS_UID
        );
        assert_eq!(bindings[1].command_field, CommandField::CFindRq);
        assert_eq!(
            bindings[1].sop_class_uid.as_ref(),
            QueryServiceProvider::PATIENT_ROOT_FIND_SOP_CLASS_UID
        );
    }

    #[test]
    fn map_find_response_status_codes_match_expected_values() {
        assert_eq!(super::CFindStatus::Pending.code(), 0xFF00);
        assert_eq!(super::CFindStatus::Success.code(), 0x0000);
        assert_eq!(super::CFindStatus::OutOfResources.code(), 0xA700);
        assert_eq!(
            super::CFindStatus::IdentifierDoesNotMatchSopClass.code(),
            0xA900
        );
        assert_eq!(super::CFindStatus::UnableToProcess.code(), 0xC000);
    }

    #[test]
    fn query_error_mapping_marks_identifier_element_as_offending() {
        let failure =
            super::map_query_error(rustcoon_query::QueryError::InvalidIdentifierElement {
                tag: tags::QUERY_RETRIEVE_LEVEL,
                message: "bad".to_string(),
            });

        assert_eq!(
            failure.status,
            super::CFindStatus::IdentifierDoesNotMatchSopClass
        );
        assert_eq!(failure.offending_elements, vec![tags::QUERY_RETRIEVE_LEVEL]);
        assert!(failure.error_comment.is_some());
    }
}
