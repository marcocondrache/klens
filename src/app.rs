use std::sync::Arc;

use crate::kafka::QueryEngine;
use axum::Router;

mod graphql;
mod health;

#[derive(Clone)]
pub struct AppState {
    pub(crate) query: Arc<QueryEngine>,
}

impl AppState {
    pub fn new(query: Arc<QueryEngine>) -> Self {
        Self { query }
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
