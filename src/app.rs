use std::sync::Arc;
use std::time::Duration;

use crate::kafka::{
    CatalogCache, CatalogPoller, ClusterSession, ClusterSnapshot, LagStore, QueryEngine, RateStore,
};
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
    pub(crate) catalog: CatalogCache,
    pub(crate) rates: RateStore,
    pub(crate) lags: LagStore,
    pub(crate) samplers: Arc<Samplers>,
    pub(crate) auth: AuthState,
    _poller: Option<Arc<CatalogPoller>>,
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
            catalog: CatalogCache::new(),
            rates: RateStore::new(),
            lags: LagStore::new(),
            samplers: Arc::new(Samplers::default()),
            auth,
            _poller: None,
        }
    }

    pub fn with_catalog_poller(self, interval: Duration) -> Self {
        let poller = CatalogPoller::start(self.catalog.clone(), Arc::clone(&self.query), interval);
        Self {
            _poller: Some(Arc::new(poller)),
            ..self
        }
    }

    pub(crate) async fn catalog_snapshot(
        &self,
        cluster: &str,
    ) -> Result<ClusterSnapshot, crate::kafka::KafkaError> {
        if let Some(snapshot) = self.catalog.snapshot(cluster) {
            return Ok(snapshot);
        }

        let (topics, groups) = tokio::try_join!(
            self.query.topics(cluster),
            self.query.consumer_groups(cluster, None),
        )?;
        let snapshot = ClusterSnapshot::from_catalog(topics, groups);
        self.catalog.seed(cluster, snapshot.clone());
        Ok(snapshot)
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
