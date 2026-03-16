use dicom_object::InMemDicomObject;
use dicom_ul::pdu::PDataValue;
use rustcoon_application_entity::{AeTitle, AssociationRoutePlan};
use rustcoon_ul::UlAssociation;

use crate::error::DimseError;
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

    /// Access optional route metadata.
    pub fn route(&self) -> Option<&AeRouteContext> {
        self.route.as_ref()
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
    pub fn read_command_object(&mut self) -> Result<CommandObject, DimseError> {
        if let Some(command_object) = &self.cached_command_object {
            return Ok(command_object.clone());
        }

        let command_object = self.reader.read_command_object(&mut self.association)?;
        self.cached_command_object = Some(command_object.clone());
        Ok(command_object)
    }

    /// Read and cache a parsed `DimseCommand`.
    pub fn read_command(&mut self) -> Result<DimseCommand, DimseError> {
        if let Some(command) = &self.cached_command {
            return Ok(command.clone());
        }

        let command = DimseCommand::from_command_object(&self.read_command_object()?)?;
        self.cached_command = Some(command.clone());
        Ok(command)
    }

    /// Read one dataset PDV fragment for the active command.
    pub fn read_data_pdv(&mut self) -> Result<Option<PDataValue>, DimseError> {
        self.reader.read_data_pdv(&mut self.association)
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
    pub fn send_command_object(
        &mut self,
        presentation_context_id: u8,
        command: &InMemDicomObject,
    ) -> Result<(), DimseError> {
        self.writer
            .send_command_object(&mut self.association, presentation_context_id, command)
    }

    /// Send one dataset PDV fragment.
    pub fn send_data_pdv(&mut self, pdv: PDataValue) -> Result<(), DimseError> {
        self.writer.send_data_pdv(&mut self.association, pdv)
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
