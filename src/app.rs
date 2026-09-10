use std::sync::Arc;

use crate::kafka::{ClusterSession, LagStore, QueryEngine, RateStore};
use axum::Router;
use axum::middleware;

mod auth;
mod graphql;
mod health;
mod sampler;

pub use auth::AuthState;

use graphql::Samplers;

#[derive(Clone)]
pub struct AppState {
    pub(crate) query: Arc<QueryEngine<dyn ClusterSession>>,
    pub(crate) rates: RateStore,
    pub(crate) lags: LagStore,
    pub(crate) samplers: Arc<Samplers>,
    pub(crate) auth: AuthState,
}

impl AppState {
    pub fn new(query: Arc<QueryEngine<dyn ClusterSession>>) -> Self {
        Self::build(query, AuthState::disabled())
    }

    pub fn with_auth(query: Arc<QueryEngine<dyn ClusterSession>>, auth: AuthState) -> Self {
        Self::build(query, auth)
    }

    fn build(query: Arc<QueryEngine<dyn ClusterSession>>, auth: AuthState) -> Self {
        Self {
            query,
            rates: RateStore::new(),
            lags: LagStore::new(),
            samplers: Arc::new(Samplers::default()),
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
