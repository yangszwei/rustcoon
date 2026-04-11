use std::sync::Arc;

use rustcoon_ingest::{HierarchicalInstanceKeyResolver, IngestService};
use rustcoon_storage::BlobStore;

use crate::infrastructure::index::CatalogPorts;

/// Builds ingest service from shared infrastructure handles.
pub fn build_ingest_service(
    blob_store: Arc<dyn BlobStore>,
    catalog_ports: &CatalogPorts,
) -> Arc<IngestService> {
    Arc::new(IngestService::new(
        blob_store,
        Arc::clone(&catalog_ports.0),
        Arc::clone(&catalog_ports.1),
        Arc::new(HierarchicalInstanceKeyResolver::new()),
    ))
}
