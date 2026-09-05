use std::sync::Arc;

use axum::{Json, Router, extract::State, routing::get};
use klens_kafka::ClusterRegistry;
use serde::Serialize;

mod api_error;
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
        .route("/api/clusters", get(list_clusters))
        .with_state(state)
        .merge(health::router())
        .fallback(klens_server::web::serve)
}

#[derive(Debug, Serialize)]
struct ClusterSummary {
    name: String,
    bootstrap_servers: Vec<String>,
}

async fn list_clusters(State(state): State<AppState>) -> Json<Vec<ClusterSummary>> {
    let clusters = state
        .clusters
        .list()
        .into_iter()
        .map(|client| ClusterSummary {
            name: client.name().to_owned(),
            bootstrap_servers: client.config().bootstrap_servers.clone(),
        })
        .collect();

    Json(clusters)
}
