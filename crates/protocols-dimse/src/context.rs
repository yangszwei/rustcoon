use dicom_object::InMemDicomObject;
use dicom_ul::pdu::PDataValue;
use rustcoon_application_entity::{AeTitle, AssociationRoutePlan};
use rustcoon_ul::UlAssociation;

use crate::error::DimseError;
use crate::instrumentation::DimseErrorClass;
use crate::message::{CommandObject, DimseReader, DimseWriter};
use crate::service::DimseCommand;

/// Association-level AE route metadata (optional but useful for providers).
/// Useful when provider behavior depends on call/called AE identity.
#[derive(Debug, Clone)]
pub struct AeRouteContext {
    /// Calling AE title when known from route planning.
    pub calling_ae_title: Option<AeTitle>,
    /// Called/local AE title for the accepted association.
    pub called_ae_title: AeTitle,
}

impl AeRouteContext {
    pub fn from_route(route: &AssociationRoutePlan) -> Self {
        Self {
            calling_ae_title: Some(route.calling_ae_title.clone()),
            called_ae_title: route.called_ae_title.clone(),
        }
    }
}

/// DIMSE I/O context scoped to one established UL association.
/// Bundles reader/writer state, command cache, and one UL association.
#[derive(Debug)]
pub struct AssociationContext {
    association: UlAssociation,
    route: Option<AeRouteContext>,
    association_id: u64,
    next_request_id: u64,
    response_status: Option<u16>,
    response_error_class: Option<DimseErrorClass>,
    reader: DimseReader,
    writer: DimseWriter,
    cached_command_object: Option<CommandObject>,
    cached_command: Option<DimseCommand>,
}

impl AssociationContext {
    /// Create a message context bound to one established UL association.
    pub fn new(association: UlAssociation) -> Self {
        Self {
            association,
            route: None,
            association_id: 0,
            next_request_id: 1,
            response_status: None,
            response_error_class: None,
            reader: DimseReader::new(),
            writer: DimseWriter::new(),
            cached_command_object: None,
            cached_command: None,
        }
    }

    /// Attach route metadata for service logic.
    pub fn with_route(mut self, route: AeRouteContext) -> Self {
        self.route = Some(route);
        self
    }

    /// Attach route metadata from a route plan.
    pub fn with_route_plan(self, route: &AssociationRoutePlan) -> Self {
        self.with_route(AeRouteContext::from_route(route))
    }

    /// Attach observability metadata for logs, traces, and metrics.
    pub fn with_association_id(mut self, association_id: u64) -> Self {
        self.association_id = association_id;
        self
    }

    /// Access optional route metadata.
    pub fn route(&self) -> Option<&AeRouteContext> {
        self.route.as_ref()
    }

    pub(crate) fn association_id(&self) -> u64 {
        self.association_id
    }

    pub(crate) fn next_request_id(&mut self) -> u64 {
        let request_id = self.next_request_id;
        self.next_request_id = self.next_request_id.saturating_add(1);
        self.response_status = None;
        self.response_error_class = None;
        request_id
    }

    pub(crate) fn record_response_status(&mut self, status: u16) {
        self.response_status = Some(status);
    }

    pub(crate) fn record_response_error_class(&mut self, class: DimseErrorClass) {
        self.response_error_class = Some(class);
    }

    pub(crate) fn response_status(&self) -> Option<u16> {
        self.response_status
    }

    pub(crate) fn response_error_class(&self) -> Option<DimseErrorClass> {
        self.response_error_class
    }

    pub(crate) fn cached_command(&self) -> Option<&DimseCommand> {
        self.cached_command.as_ref()
    }

    /// Borrow the underlying UL association.
    pub fn association(&self) -> &UlAssociation {
        &self.association
    }

    /// Borrow the underlying UL association mutably.
    pub fn association_mut(&mut self) -> &mut UlAssociation {
        &mut self.association
    }

    /// Read and cache the command object for this message cycle.
    pub async fn read_command_object(&mut self) -> Result<CommandObject, DimseError> {
        if let Some(command_object) = &self.cached_command_object {
            return Ok(command_object.clone());
        }

        let command_object = self
            .reader
            .read_command_object(&mut self.association)
            .await?;
        self.cached_command_object = Some(command_object.clone());
        Ok(command_object)
    }

    /// Read and cache a parsed `DimseCommand`.
    pub async fn read_command(&mut self) -> Result<DimseCommand, DimseError> {
        if let Some(command) = &self.cached_command {
            return Ok(command.clone());
        }

        let command = DimseCommand::from_command_object(&self.read_command_object().await?)?;
        self.cached_command = Some(command.clone());
        Ok(command)
    }

    /// Read one dataset PDV fragment for the active command.
    pub async fn read_data_pdv(&mut self) -> Result<Option<PDataValue>, DimseError> {
        self.reader.read_data_pdv(&mut self.association).await
    }

    /// Clear cached command state.
    pub fn clear_cached_command(&mut self) {
        self.cached_command_object = None;
        self.cached_command = None;
    }

    /// Return `true` if a command-declared dataset is still streaming.
    pub fn has_unfinished_data_set(&self) -> bool {
        self.reader.has_unfinished_data_set()
    }

    /// Ensure no unfinished dataset remains, then reset command caches.
    pub fn complete_message_cycle(&mut self) -> Result<(), DimseError> {
        if self.has_unfinished_data_set() {
            return Err(DimseError::protocol(
                "provider returned before consuming the full DIMSE data set",
            ));
        }
        self.clear_cached_command();
        Ok(())
    }

    /// Serialize and send one DIMSE command set.
    pub async fn send_command_object(
        &mut self,
        presentation_context_id: u8,
        command: &InMemDicomObject,
    ) -> Result<(), DimseError> {
        self.writer
            .send_command_object(&mut self.association, presentation_context_id, command)
            .await
    }

    /// Send one dataset PDV fragment.
    pub async fn send_data_pdv(&mut self, pdv: PDataValue) -> Result<(), DimseError> {
        self.writer.send_data_pdv(&mut self.association, pdv).await
    }

    pub fn bytes_in(&self) -> u64 {
        self.reader.bytes_in()
    }

    pub fn bytes_out(&self) -> u64 {
        self.writer.bytes_out()
    }

    /// Consume the context and return the UL association.
    pub fn into_association(self) -> UlAssociation {
        self.association
    }
}
