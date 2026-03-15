mod reader;
mod writer;

use dicom_object::InMemDicomObject;
pub use reader::DimseReader;
pub use writer::DimseWriter;

/// One decoded DIMSE command set and its presentation context.
/// Keeps command metadata and routing context together for service handlers.
#[derive(Debug, Clone)]
pub struct CommandObject {
    /// Negotiated presentation context id for this command set.
    pub presentation_context_id: u8,

    /// Decoded DIMSE command set (`0000,xxxx` group).
    pub command: InMemDicomObject,
}
