use std::sync::Arc;

use dicom_dictionary_std::tags;
use rustcoon_application_entity::{AeTitle, ApplicationEntityRegistry};
use rustcoon_retrieve::{RetrieveError, RetrieveQueryModel, RetrieveService};
use rustcoon_ul::OutboundAssociationRequest;

use crate::context::AssociationContext;
use crate::error::DimseError;
use crate::instrumentation::{DimseErrorClass, record_suboperation};
use crate::service::retrieve::common::{
    StoreSubOperationStatus, block_on_retrieve, build_retrieve_request, read_identifier_data_set,
    send_store_sub_operation,
};
use crate::service::retrieve::{CMoveRequest, CMoveResponse, CMoveStatus};
use crate::service::{
    CommandField, DescribedServiceClassProvider, ServiceBinding, ServiceClassProvider,
};

const STUDY_ROOT_MOVE_SOP_CLASS_UID: &str = "1.2.840.10008.5.1.4.1.2.2.2";
const PATIENT_ROOT_MOVE_SOP_CLASS_UID: &str = "1.2.840.10008.5.1.4.1.2.1.2";

pub struct CMoveServiceProvider {
    retrieve: Arc<RetrieveService>,
    ae_registry: Arc<ApplicationEntityRegistry>,
}

impl CMoveServiceProvider {
    pub fn new(
        retrieve: Arc<RetrieveService>,
        ae_registry: Arc<ApplicationEntityRegistry>,
    ) -> Self {
        Self {
            retrieve,
            ae_registry,
        }
    }
}

impl ServiceClassProvider for CMoveServiceProvider {
    fn handle(&self, ctx: &mut AssociationContext) -> Result<(), DimseError> {
        let request = CMoveRequest::from_command(&ctx.read_command()?)?;
        tracing::debug!(stage = "validate", "C-MOVE request validated");

        let Some(model) = retrieve_model_for_move_sop_class_uid(&request.affected_sop_class_uid)
        else {
            let response =
                CMoveResponse::for_request(&request, CMoveStatus::IdentifierDoesNotMatchSopClass)
                    .with_offending_element(tags::AFFECTED_SOP_CLASS_UID)
                    .with_error_comment("unsupported Query/Retrieve MOVE SOP Class UID");
            ctx.send_command_object(
                request.presentation_context_id,
                &response.to_command_object(),
            )?;
            ctx.record_response_status(CMoveStatus::IdentifierDoesNotMatchSopClass.code());
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
        ) {
            Ok(identifier) => {
                tracing::debug!(stage = "identifier_decoded", "C-MOVE identifier decoded");
                identifier
            }
            Err(_) => {
                let response = CMoveResponse::for_request(&request, CMoveStatus::UnableToProcess)
                    .with_error_comment("failed to decode C-MOVE identifier");
                ctx.send_command_object(
                    request.presentation_context_id,
                    &response.to_command_object(),
                )?;
                ctx.record_response_status(CMoveStatus::UnableToProcess.code());
                ctx.record_response_error_class(DimseErrorClass::new("service", "invalid_dataset"));
                return Ok(());
            }
        };

        let app_request = match build_retrieve_request(model, &identifier) {
            Ok(app_request) => app_request,
            Err(error) => {
                let mut response = CMoveResponse::for_request(
                    &request,
                    CMoveStatus::IdentifierDoesNotMatchSopClass,
                )
                .with_error_comment(error.message);
                if let Some(tag) = error.tag {
                    response = response.with_offending_element(tag);
                }
                ctx.send_command_object(
                    request.presentation_context_id,
                    &response.to_command_object(),
                )?;
                ctx.record_response_status(CMoveStatus::IdentifierDoesNotMatchSopClass.code());
                ctx.record_response_error_class(DimseErrorClass::new("service", "invalid_dataset"));
                return Ok(());
            }
        };

        tracing::debug!(
            stage = "backend_call",
            backend = "retrieve",
            "C-MOVE retrieve plan started"
        );
        let plan = block_on_retrieve(self.retrieve.plan(app_request));
        let response = match plan {
            Ok(plan) if plan.total_suboperations == 0 => {
                tracing::debug!(
                    stage = "backend_complete",
                    backend = "retrieve",
                    suboperations = 0_u64,
                    "C-MOVE retrieve plan completed"
                );
                CMoveResponse::for_request(&request, CMoveStatus::Success)
                    .with_suboperation_counts(0, 0, 0, 0)
            }
            Ok(plan) => {
                tracing::debug!(
                    stage = "backend_complete",
                    backend = "retrieve",
                    suboperations = plan.total_suboperations as u64,
                    "C-MOVE retrieve plan completed"
                );
                let Some(route_context) = ctx.route().cloned() else {
                    let response =
                        CMoveResponse::for_request(&request, CMoveStatus::UnableToProcess)
                            .with_error_comment("association route context is unavailable");
                    ctx.send_command_object(
                        request.presentation_context_id,
                        &response.to_command_object(),
                    )?;
                    ctx.record_response_status(CMoveStatus::UnableToProcess.code());
                    ctx.record_response_error_class(DimseErrorClass::new(
                        "service",
                        "unable_to_process",
                    ));
                    return Ok(());
                };
                let move_destination = match request.move_destination.parse::<AeTitle>() {
                    Ok(title) => title,
                    Err(_) => {
                        let response = CMoveResponse::for_request(
                            &request,
                            CMoveStatus::MoveDestinationUnknown,
                        )
                        .with_error_comment("Move Destination is not a valid AE title");
                        ctx.send_command_object(
                            request.presentation_context_id,
                            &response.to_command_object(),
                        )?;
                        ctx.record_response_status(CMoveStatus::MoveDestinationUnknown.code());
                        ctx.record_response_error_class(DimseErrorClass::new(
                            "service",
                            "invalid_ae_title",
                        ));
                        return Ok(());
                    }
                };
                if self.ae_registry.local(&move_destination).is_none()
                    && self.ae_registry.remote(&move_destination).is_none()
                {
                    let response =
                        CMoveResponse::for_request(&request, CMoveStatus::MoveDestinationUnknown)
                            .with_error_comment("Move Destination is unknown");
                    ctx.send_command_object(
                        request.presentation_context_id,
                        &response.to_command_object(),
                    )?;
                    ctx.record_response_status(CMoveStatus::MoveDestinationUnknown.code());
                    ctx.record_response_error_class(DimseErrorClass::new(
                        "service",
                        "unknown_move_destination",
                    ));
                    return Ok(());
                }
                let route = match self
                    .ae_registry
                    .plan_outbound(&route_context.called_ae_title, &move_destination)
                {
                    Ok(route) => route,
                    Err(_) => {
                        let response = CMoveResponse::for_request(
                            &request,
                            CMoveStatus::MoveDestinationUnknown,
                        )
                        .with_error_comment("Move Destination is unknown");
                        ctx.send_command_object(
                            request.presentation_context_id,
                            &response.to_command_object(),
                        )?;
                        ctx.record_response_status(CMoveStatus::MoveDestinationUnknown.code());
                        ctx.record_response_error_class(DimseErrorClass::new(
                            "service",
                            "unknown_move_destination",
                        ));
                        return Ok(());
                    }
                };
                let storage_sop_classes = plan
                    .instances
                    .iter()
                    .map(|candidate| candidate.identity.sop_class_uid().as_str().to_string())
                    .collect::<std::collections::BTreeSet<_>>();
                let association = match OutboundAssociationRequest::establish_from_route(
                    &route,
                    self.ae_registry.as_ref(),
                    storage_sop_classes,
                ) {
                    Ok(association) => association,
                    Err(_) => {
                        let response = CMoveResponse::for_request(
                            &request,
                            CMoveStatus::MoveDestinationUnknown,
                        )
                        .with_error_comment("failed to establish Move Destination association");
                        ctx.send_command_object(
                            request.presentation_context_id,
                            &response.to_command_object(),
                        )?;
                        ctx.record_response_status(CMoveStatus::MoveDestinationUnknown.code());
                        ctx.record_response_error_class(DimseErrorClass::new(
                            "ul",
                            "association_rejected",
                        ));
                        return Ok(());
                    }
                };
                let mut move_ctx = AssociationContext::new(association).with_route_plan(&route);
                let move_originator_ae_title = route_context
                    .calling_ae_title
                    .as_ref()
                    .map(|title| title.as_str().to_string())
                    .unwrap_or_else(|| "UNKNOWN".to_string());
                let mut completed = 0_u16;
                let mut failed = 0_u16;
                let mut warning = 0_u16;
                let total = u16::try_from(plan.total_suboperations).unwrap_or(u16::MAX);

                for (index, candidate) in plan.instances.iter().enumerate() {
                    let message_id = u16::try_from(index + 1).unwrap_or(u16::MAX);
                    match send_store_sub_operation(
                        &mut move_ctx,
                        self.retrieve.as_ref(),
                        candidate,
                        message_id,
                        Some((move_originator_ae_title.as_str(), request.message_id)),
                    )? {
                        StoreSubOperationStatus::Completed => {
                            record_suboperation("c_move_store", "completed");
                            completed = completed.saturating_add(1)
                        }
                        StoreSubOperationStatus::Failed => {
                            record_suboperation("c_move_store", "failed");
                            failed = failed.saturating_add(1)
                        }
                        StoreSubOperationStatus::Warning => {
                            record_suboperation("c_move_store", "warning");
                            warning = warning.saturating_add(1)
                        }
                    }

                    let done = completed.saturating_add(failed).saturating_add(warning);
                    let remaining = total.saturating_sub(done);
                    if remaining > 0 {
                        let pending = CMoveResponse::for_request(&request, CMoveStatus::Pending)
                            .with_suboperation_counts(remaining, completed, failed, warning);
                        ctx.send_command_object(
                            request.presentation_context_id,
                            &pending.to_command_object(),
                        )?;
                    }
                }

                let _ = move_ctx.into_association().release();
                let status = if failed > 0 || warning > 0 {
                    CMoveStatus::Warning
                } else {
                    CMoveStatus::Success
                };
                CMoveResponse::for_request(&request, status)
                    .with_suboperation_counts(0, completed, failed, warning)
            }
            Err(error) => {
                tracing::warn!(
                    stage = "backend_failure",
                    backend = "retrieve",
                    error = %error,
                    "C-MOVE retrieve plan failed"
                );
                let response = map_retrieve_error_to_move_response(&request, error);
                ctx.record_response_error_class(c_move_status_error_class(response.status));
                response
            }
        };

        let status = response.status.code();
        if !matches!(response.status, CMoveStatus::Success | CMoveStatus::Pending) {
            ctx.record_response_error_class(c_move_status_error_class(response.status));
        }
        ctx.send_command_object(
            request.presentation_context_id,
            &response.to_command_object(),
        )?;
        ctx.record_response_status(status);
        tracing::debug!(
            stage = "response",
            status = format!("0x{status:04X}"),
            "C-MOVE response sent"
        );
        Ok(())
    }
}

impl DescribedServiceClassProvider for CMoveServiceProvider {
    fn bindings(&self) -> &[ServiceBinding] {
        static BINDINGS: [ServiceBinding; 2] = [
            ServiceBinding::new(CommandField::CMoveRq, STUDY_ROOT_MOVE_SOP_CLASS_UID),
            ServiceBinding::new(CommandField::CMoveRq, PATIENT_ROOT_MOVE_SOP_CLASS_UID),
        ];
        &BINDINGS
    }
}

fn c_move_status_error_class(status: CMoveStatus) -> DimseErrorClass {
    match status {
        CMoveStatus::Pending | CMoveStatus::Success => DimseErrorClass::new("service", "unknown"),
        CMoveStatus::Warning => DimseErrorClass::new("service", "unable_to_process"),
        CMoveStatus::MoveDestinationUnknown => {
            DimseErrorClass::new("service", "unknown_move_destination")
        }
        CMoveStatus::OutOfResources => DimseErrorClass::new("backend", "out_of_resources"),
        CMoveStatus::IdentifierDoesNotMatchSopClass => {
            DimseErrorClass::new("service", "invalid_dataset")
        }
        CMoveStatus::UnableToProcess => DimseErrorClass::new("service", "unable_to_process"),
    }
}

fn retrieve_model_for_move_sop_class_uid(sop_class_uid: &str) -> Option<RetrieveQueryModel> {
    match sop_class_uid {
        STUDY_ROOT_MOVE_SOP_CLASS_UID => Some(RetrieveQueryModel::StudyRoot),
        PATIENT_ROOT_MOVE_SOP_CLASS_UID => Some(RetrieveQueryModel::PatientRoot),
        _ => None,
    }
}

fn map_retrieve_error_to_move_response(
    request: &CMoveRequest,
    error: RetrieveError,
) -> CMoveResponse {
    match error {
        RetrieveError::UnsupportedQueryRetrieveLevel { .. }
        | RetrieveError::MissingUniqueKey { .. }
        | RetrieveError::InvalidHierarchy { .. }
        | RetrieveError::InvalidCatalogProjection { .. } => {
            CMoveResponse::for_request(request, CMoveStatus::IdentifierDoesNotMatchSopClass)
                .with_error_comment("identifier is invalid for baseline retrieval")
        }
        RetrieveError::Catalog(_) | RetrieveError::ResolveInstance { .. } => {
            CMoveResponse::for_request(request, CMoveStatus::OutOfResources)
                .with_error_comment("catalog backend is unavailable")
        }
        RetrieveError::InvalidCatalogQuery(_)
        | RetrieveError::MissingCatalogInstance { .. }
        | RetrieveError::MissingBlobReference { .. }
        | RetrieveError::OpenBlob(_)
        | RetrieveError::OpenBlobRange(_) => {
            CMoveResponse::for_request(request, CMoveStatus::UnableToProcess)
                .with_error_comment("retrieve request could not be processed")
        }
    }
}
