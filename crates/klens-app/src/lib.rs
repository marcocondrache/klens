use std::sync::Arc;

use axum::Router;
use klens_kafka::ClusterRegistry;

mod api_error;
mod graphql;
mod health;

pub use api_error::ApiError;

#[derive(Clone)]
pub struct AppState {
    clusters: Arc<ClusterRegistry>,
}

impl AppState {
    pub fn new(clusters: Arc<ClusterRegistry>) -> Self {
        Self { clusters }
    }
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .merge(graphql::router())
        .with_state(state)
        .merge(health::router())
        .fallback(klens_server::web::serve)
}
