use std::sync::Arc;

use crate::kafka::{QueryEngine, RateStore};
use axum::Router;
use axum::middleware;

mod auth;
mod graphql;
mod health;

pub use auth::AuthState;

#[derive(Clone)]
pub struct AppState {
    pub(crate) query: Arc<QueryEngine>,
    pub(crate) rates: RateStore,
    pub(crate) auth: AuthState,
}

impl AppState {
    pub fn new(query: Arc<QueryEngine>) -> Self {
        Self {
            query,
            rates: RateStore::new(),
            auth: AuthState::disabled(),
        }
    }

    pub fn with_auth(query: Arc<QueryEngine>, auth: AuthState) -> Self {
        Self {
            query,
            rates: RateStore::new(),
            auth,
        }
    }
}

pub use graphql::{Schema, schema};

pub fn router(state: AppState) -> Router {
    let graphql = graphql::router().route_layer(middleware::from_fn_with_state(
        state.clone(),
        auth::require_session,
    ));

    Router::new()
        .merge(graphql)
        .merge(auth::router())
        .with_state(state)
        .merge(health::router())
        .fallback(crate::server::web::serve)
}
