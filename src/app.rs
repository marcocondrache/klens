use std::sync::Arc;
use std::time::Duration;

use crate::environment::{CONFIG_POLL_INTERVAL, OVERVIEW_BUDGET, SUBJECT_POLL_INTERVAL};
use crate::kafka::model::SchemaSubject;
use crate::kafka::{
    CatalogCache, CatalogHealth, CatalogPoller, ClusterIdentity, ClusterOverview, ClusterSession,
    ClusterSnapshot, LagStore, QueryEngine, RateStore, SubjectCache,
};
use axum::Router;
use axum::middleware;
use futures::future::join_all;
use tokio::time::timeout;

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
    pub(crate) subjects: SubjectCache,
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
            subjects: SubjectCache::new(),
            rates: RateStore::new(),
            lags: LagStore::new(),
            samplers: Arc::new(Samplers::default()),
            auth,
            _poller: None,
        }
    }

    pub fn with_catalog_poller(self, interval: Duration) -> Self {
        let poller = CatalogPoller::start(
            self.catalog.clone(),
            self.subjects.clone(),
            Arc::clone(&self.query),
            self.rates.clone(),
            interval,
            *SUBJECT_POLL_INTERVAL,
            *CONFIG_POLL_INTERVAL,
        );
        Self {
            _poller: Some(Arc::new(poller)),
            ..self
        }
    }

    pub fn kick_catalog(&self, cluster: &str) {
        if let Some(poller) = &self._poller {
            poller.kick(cluster);
        }
    }

    pub fn invalidate_catalog(&self, cluster: &str) {
        self.catalog.invalidate(cluster);
        self.subjects.invalidate(cluster);
    }

    pub(crate) fn is_ready(&self) -> bool {
        self.query
            .names()
            .into_iter()
            .all(|name| self.catalog.snapshot(name).is_some())
    }

    pub(crate) fn catalog_health(&self, cluster: &str) -> CatalogHealth {
        let snapshot = self.catalog.snapshot(cluster);
        let subjects = self.subjects.snapshot(cluster);
        let catalog = self.catalog.poll_lane(cluster);
        let subjects_lane = self.subjects.poll_lane(cluster);
        CatalogHealth {
            cluster: cluster.to_owned(),
            updated_at: snapshot.as_ref().map(|snapshot| snapshot.updated_at),
            subjects_updated_at: subjects_lane.updated_at,
            last_error: catalog.last_error,
            last_poll_duration_ms: catalog.last_poll_duration_ms,
            topic_count: snapshot
                .as_ref()
                .map(|snapshot| snapshot.topics.len() as i32)
                .unwrap_or(0),
            group_count: snapshot
                .as_ref()
                .map(|snapshot| snapshot.groups.len() as i32)
                .unwrap_or(0),
            broker_count: snapshot
                .as_ref()
                .map(|snapshot| snapshot.brokers.len() as i32)
                .unwrap_or(0),
            subject_count: subjects
                .as_ref()
                .map(|subjects| subjects.len() as i32)
                .unwrap_or(0),
        }
    }

    pub(crate) async fn catalog_snapshot(
        &self,
        cluster: &str,
    ) -> Result<Arc<ClusterSnapshot>, crate::kafka::KafkaError> {
        if let Some(snapshot) = self.catalog.snapshot(cluster) {
            return Ok(snapshot);
        }

        let snapshot = Arc::new(self.query.catalog(cluster).await?);
        self.catalog.seed(cluster, Arc::clone(&snapshot));
        Ok(snapshot)
    }

    pub(crate) async fn subject_snapshot(
        &self,
        cluster: &str,
    ) -> Result<Arc<Vec<SchemaSubject>>, crate::kafka::KafkaError> {
        if let Some(subjects) = self.subjects.snapshot(cluster) {
            return Ok(subjects);
        }

        let subjects = Arc::new(self.query.schema_subjects(cluster).await?);
        self.subjects.seed(cluster, Arc::clone(&subjects));
        Ok(subjects)
    }

    pub(crate) async fn cluster_overviews(&self) -> Vec<ClusterOverview> {
        let identities = self.query.identities();
        join_all(
            identities
                .iter()
                .map(|identity| self.overview_or_offline(identity)),
        )
        .await
    }

    pub(crate) async fn cluster_overview(&self, name: &str) -> Option<ClusterOverview> {
        let identity = self.query.session(name).ok()?.identity().clone();
        Some(self.overview_or_offline(&identity).await)
    }

    async fn overview_or_offline(&self, identity: &ClusterIdentity) -> ClusterOverview {
        if let Some(snapshot) = self.catalog.snapshot(&identity.name) {
            return snapshot.overview.clone();
        }

        match timeout(*OVERVIEW_BUDGET, self.catalog_snapshot(&identity.name)).await {
            Ok(Ok(snapshot)) => snapshot.overview.clone(),
            Ok(Err(_)) | Err(_) => ClusterOverview::offline(identity.clone()),
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
        .merge(health::router())
        .with_state(state)
        .fallback(crate::server::web::serve)
}
