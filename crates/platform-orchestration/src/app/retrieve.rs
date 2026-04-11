use std::sync::Arc;

use rustcoon_retrieve::RetrieveService;
use rustcoon_storage::BlobStore;

use crate::infrastructure::index::CatalogPorts;

/// Builds retrieve service from shared infrastructure handles.
pub fn build_retrieve_service(
    blob_store: Arc<dyn BlobStore>,
    catalog_ports: &CatalogPorts,
) -> Arc<RetrieveService> {
    let blob_read_store: Arc<dyn rustcoon_storage::BlobReadStore> = blob_store;
    Arc::new(RetrieveService::new(
        Arc::clone(&catalog_ports.0),
        blob_read_store,
    ))
}
