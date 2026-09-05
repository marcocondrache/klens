use std::sync::Arc;

use crate::kafka::ClusterRegistry;
use axum::Router;

mod graphql;
mod health;

#[derive(Clone)]
pub struct AppState {
    clusters: Arc<ClusterRegistry>,
}

impl AppState {
    pub fn new(clusters: Arc<ClusterRegistry>) -> Self {
        Self { clusters }
    }
}

pub fn schema_sdl() -> String {
    graphql::schema_sdl()
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .merge(graphql::router())
        .with_state(state)
        .merge(health::router())
        .fallback(crate::server::web::serve)
}
