mod retrieve;
mod search;
mod store;

use axum::Router;

/// The studies web service.
pub fn routes() -> Router<crate::AppState> {
    Router::new()
        .merge(retrieve::routes())
        .merge(search::routes())
        .merge(store::routes())
}
