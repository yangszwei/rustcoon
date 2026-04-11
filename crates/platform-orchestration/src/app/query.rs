use std::sync::Arc;

use rustcoon_query::QueryService;

use crate::infrastructure::index::CatalogPorts;

/// Builds query service from shared catalog infrastructure handles.
pub fn build_query_service(catalog_ports: &CatalogPorts) -> Arc<QueryService> {
    Arc::new(QueryService::new(Arc::clone(&catalog_ports.0)))
}
