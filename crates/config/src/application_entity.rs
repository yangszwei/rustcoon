use serde::Deserialize;

/// Default AE Title for the local DICOM Application Entity.
const DEFAULT_LOCAL_AE_TITLE: &str = "RUSTCOON";

/// Default host address to bind the local DICOM listener.
const DEFAULT_LOCAL_AE_HOST: &str = "127.0.0.1";

/// Default TCP port for the local DICOM listener.
const DEFAULT_LOCAL_AE_PORT: u16 = 11112;

/// Configuration grouping for all DICOM Application Entities known to the system.
///
/// This struct serves as the top-level container for AE configuration,
/// separating **local Application Entities** hosted by this PACS
/// from **remote Application Entities** representing external peers.
///
/// In typical deployments:
/// - One or more local AEs may be defined to support multiple listeners
///   or AE Titles on different network endpoints.
/// - Remote AEs are used for outbound associations (e.g. C-STORE SCU,
///   C-MOVE destinations) and for validating inbound calling AEs.
#[derive(Debug, Deserialize)]
#[serde(default)]
pub struct ApplicationEntitiesConfig {
    /// Local DICOM Application Entities hosted by this PACS.
    ///
    /// Each local AE defines an AE Title and a network endpoint on which
    /// the server listens for incoming DIMSE associations.
    pub local: Vec<LocalApplicationEntity>,

    /// Remote DICOM Application Entities known to this PACS.
    ///
    /// These entries describe peer AEs that may initiate or receive
    /// associations, such as modalities, workstations, or other PACS.
    pub remote: Vec<RemoteApplicationEntity>,
}

impl Default for ApplicationEntitiesConfig {
    /// Provides a default configuration with one local AE and no remotes.
    fn default() -> Self {
        Self {
            local: vec![LocalApplicationEntity::default()],
            remote: Vec::new(),
        }
    }
}

/// Configuration for the local DICOM Application Entity.
///
/// This struct represents the **server-side** AE configuration:
/// the AE Title presented to peers and the network endpoint
/// on which the PACS listens for incoming DIMSE associations.
#[derive(Debug, Deserialize)]
#[serde(default)]
pub struct LocalApplicationEntity {
    /// DICOM AE Title presented during association negotiation.
    ///
    /// Must be 1–16 characters and is typically uppercase ASCII.
    pub title: String,

    /// Host address to bind for incoming DICOM associations.
    ///
    /// Examples: `"0.0.0.0"`, `"127.0.0.1"`, `"::"`.
    pub host: String,

    /// TCP port to bind for incoming DICOM associations.
    pub port: u16,
}

impl Default for LocalApplicationEntity {
    fn default() -> Self {
        Self {
            title: DEFAULT_LOCAL_AE_TITLE.to_string(),
            host: DEFAULT_LOCAL_AE_HOST.to_string(),
            port: DEFAULT_LOCAL_AE_PORT,
        }
    }
}

/// Configuration for a remote DICOM Application Entity.
///
/// This struct represents a **peer AE** known to the system,
/// such as a modality or another PACS. It is typically used
/// for outbound associations (e.g. C-STORE SCU, C-MOVE destination)
/// and for validating calling AEs on inbound associations.
#[derive(Debug, Deserialize)]
pub struct RemoteApplicationEntity {
    /// DICOM AE Title of the remote peer.
    pub title: String,

    /// Hostname or IP address of the remote peer.
    pub host: String,

    /// TCP port on which the remote peer accepts DICOM associations.
    pub port: u16,
}
