use std::sync::Arc;

use async_trait::async_trait;
use dicom_dictionary_std::tags;
use rustcoon_retrieve::{RetrieveError, RetrieveQueryModel, RetrieveService};

use crate::context::AssociationContext;
use crate::error::DimseError;
use crate::instrumentation::{DimseErrorClass, record_suboperation};
use crate::service::retrieve::common::{
    StoreSubOperationStatus, build_retrieve_request, read_identifier_data_set,
    send_store_sub_operation,
};
use crate::service::retrieve::{CGetRequest, CGetResponse, CGetStatus};
use crate::service::{
    CommandField, DescribedServiceClassProvider, ServiceBinding, ServiceClassProvider,
};

const STUDY_ROOT_GET_SOP_CLASS_UID: &str = "1.2.840.10008.5.1.4.1.2.2.3";
const PATIENT_ROOT_GET_SOP_CLASS_UID: &str = "1.2.840.10008.5.1.4.1.2.1.3";

pub struct CGetServiceProvider {
    retrieve: Arc<RetrieveService>,
}

impl CGetServiceProvider {
    pub fn new(retrieve: Arc<RetrieveService>) -> Self {
        Self { retrieve }
    }
}

#[async_trait]
impl ServiceClassProvider for CGetServiceProvider {
    async fn handle(&self, ctx: &mut AssociationContext) -> Result<(), DimseError> {
        let request = CGetRequest::from_command(&ctx.read_command().await?)?;
        tracing::debug!(stage = "validate", "C-GET request validated");

        let Some(model) = retrieve_model_for_get_sop_class_uid(&request.affected_sop_class_uid)
        else {
            let response =
                CGetResponse::for_request(&request, CGetStatus::IdentifierDoesNotMatchSopClass)
                    .with_offending_element(tags::AFFECTED_SOP_CLASS_UID)
                    .with_error_comment("unsupported Query/Retrieve GET SOP Class UID");
            ctx.send_command_object(
                request.presentation_context_id,
                &response.to_command_object(),
            )
            .await?;
            ctx.record_response_status(CGetStatus::IdentifierDoesNotMatchSopClass.code());
            ctx.record_response_error_class(DimseErrorClass::new(
                "service",
                "unsupported_sop_class",
            ));
            return Ok(());
        };

        let identifier = match read_identifier_data_set(
            ctx,
            request.presentation_context_id,
            &request.affected_sop_class_uid,
        )
        .await
        {
            Ok(identifier) => {
                tracing::debug!(stage = "identifier_decoded", "C-GET identifier decoded");
                identifier
            }
            Err(_) => {
                let response = CGetResponse::for_request(&request, CGetStatus::UnableToProcess)
                    .with_error_comment("failed to decode C-GET identifier");
                ctx.send_command_object(
                    request.presentation_context_id,
                    &response.to_command_object(),
                )
                .await?;
                ctx.record_response_status(CGetStatus::UnableToProcess.code());
                ctx.record_response_error_class(DimseErrorClass::new("service", "invalid_dataset"));
                return Ok(());
            }
        };

        let app_request = match build_retrieve_request(model, &identifier) {
            Ok(app_request) => app_request,
            Err(error) => {
                let mut response =
                    CGetResponse::for_request(&request, CGetStatus::IdentifierDoesNotMatchSopClass)
                        .with_error_comment(error.message);
                if let Some(tag) = error.tag {
                    response = response.with_offending_element(tag);
                }
                ctx.send_command_object(
                    request.presentation_context_id,
                    &response.to_command_object(),
                )
                .await?;
                ctx.record_response_status(CGetStatus::IdentifierDoesNotMatchSopClass.code());
                ctx.record_response_error_class(DimseErrorClass::new("service", "invalid_dataset"));
                return Ok(());
            }
        };

        tracing::debug!(
            stage = "backend_call",
            backend = "retrieve",
            "C-GET retrieve plan started"
        );
        let plan = self.retrieve.plan(app_request).await;
        let response = match plan {
            Ok(plan) if plan.total_suboperations == 0 => {
                tracing::debug!(
                    stage = "backend_complete",
                    backend = "retrieve",
                    suboperations = 0_u64,
                    "C-GET retrieve plan completed"
                );
                CGetResponse::for_request(&request, CGetStatus::Success)
                    .with_suboperation_counts(0, 0, 0, 0)
            }
            Ok(plan) => {
                tracing::debug!(
                    stage = "backend_complete",
                    backend = "retrieve",
                    suboperations = plan.total_suboperations as u64,
                    "C-GET retrieve plan completed"
                );
                let mut completed = 0_u16;
                let mut failed = 0_u16;
                let mut warning = 0_u16;
                let total = u16::try_from(plan.total_suboperations).unwrap_or(u16::MAX);

                for (index, candidate) in plan.instances.iter().enumerate() {
                    let message_id = u16::try_from(index + 1).unwrap_or(u16::MAX);
                    match send_store_sub_operation(
                        ctx,
                        self.retrieve.as_ref(),
                        candidate,
                        message_id,
                        None,
                    )
                    .await?
                    {
                        StoreSubOperationStatus::Completed => {
                            record_suboperation("c_get_store", "completed");
                            completed = completed.saturating_add(1)
                        }
                        StoreSubOperationStatus::Failed => {
                            record_suboperation("c_get_store", "failed");
                            failed = failed.saturating_add(1)
                        }
                        StoreSubOperationStatus::Warning => {
                            record_suboperation("c_get_store", "warning");
                            warning = warning.saturating_add(1)
                        }
                    }

                    let done = completed.saturating_add(failed).saturating_add(warning);
                    let remaining = total.saturating_sub(done);
                    if remaining > 0 {
                        let pending = CGetResponse::for_request(&request, CGetStatus::Pending)
                            .with_suboperation_counts(remaining, completed, failed, warning);
                        ctx.send_command_object(
                            request.presentation_context_id,
                            &pending.to_command_object(),
                        )
                        .await?;
                    }
                }

                let status = if failed > 0 || warning > 0 {
                    CGetStatus::Warning
                } else {
                    CGetStatus::Success
                };
                CGetResponse::for_request(&request, status)
                    .with_suboperation_counts(0, completed, failed, warning)
            }
            Err(error) => {
                tracing::warn!(
                    stage = "backend_failure",
                    backend = "retrieve",
                    error = %error,
                    "C-GET retrieve plan failed"
                );
                let response = map_retrieve_error_to_get_response(&request, error);
                ctx.record_response_error_class(c_get_status_error_class(response.status));
                response
            }
        };

        let status = response.status.code();
        if !matches!(response.status, CGetStatus::Success | CGetStatus::Pending) {
            ctx.record_response_error_class(c_get_status_error_class(response.status));
        }
        ctx.send_command_object(
            request.presentation_context_id,
            &response.to_command_object(),
        )
        .await?;
        ctx.record_response_status(status);
        tracing::debug!(
            stage = "response",
            status = format!("0x{status:04X}"),
            "C-GET response sent"
        );
        Ok(())
    }
}

impl DescribedServiceClassProvider for CGetServiceProvider {
    fn bindings(&self) -> &[ServiceBinding] {
        static BINDINGS: [ServiceBinding; 2] = [
            ServiceBinding::new(CommandField::CGetRq, STUDY_ROOT_GET_SOP_CLASS_UID),
            ServiceBinding::new(CommandField::CGetRq, PATIENT_ROOT_GET_SOP_CLASS_UID),
        ];
        &BINDINGS
    }
}

fn c_get_status_error_class(status: CGetStatus) -> DimseErrorClass {
    match status {
        CGetStatus::Pending | CGetStatus::Success => DimseErrorClass::new("service", "unknown"),
        CGetStatus::Warning => DimseErrorClass::new("service", "unable_to_process"),
        CGetStatus::OutOfResources => DimseErrorClass::new("backend", "out_of_resources"),
        CGetStatus::IdentifierDoesNotMatchSopClass => {
            DimseErrorClass::new("service", "invalid_dataset")
        }
        CGetStatus::UnableToProcess => DimseErrorClass::new("service", "unable_to_process"),
    }
}

fn retrieve_model_for_get_sop_class_uid(sop_class_uid: &str) -> Option<RetrieveQueryModel> {
    match sop_class_uid {
        STUDY_ROOT_GET_SOP_CLASS_UID => Some(RetrieveQueryModel::StudyRoot),
        PATIENT_ROOT_GET_SOP_CLASS_UID => Some(RetrieveQueryModel::PatientRoot),
        _ => None,
    }
}

fn map_retrieve_error_to_get_response(request: &CGetRequest, error: RetrieveError) -> CGetResponse {
    match error {
        RetrieveError::UnsupportedQueryRetrieveLevel { .. }
        | RetrieveError::MissingUniqueKey { .. }
        | RetrieveError::InvalidHierarchy { .. }
        | RetrieveError::InvalidCatalogProjection { .. } => {
            CGetResponse::for_request(request, CGetStatus::IdentifierDoesNotMatchSopClass)
                .with_error_comment("identifier is invalid for baseline retrieval")
        }
        RetrieveError::Catalog(_) | RetrieveError::ResolveInstance { .. } => {
            CGetResponse::for_request(request, CGetStatus::OutOfResources)
                .with_error_comment("catalog backend is unavailable")
        }
        RetrieveError::InvalidCatalogQuery(_)
        | RetrieveError::MissingCatalogInstance { .. }
        | RetrieveError::MissingBlobReference { .. }
        | RetrieveError::OpenBlob(_)
        | RetrieveError::OpenBlobRange(_) => {
            CGetResponse::for_request(request, CGetStatus::UnableToProcess)
                .with_error_comment("retrieve request could not be processed")
        }
    }
}
