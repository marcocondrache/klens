use std::sync::Arc;

use axum::Router;
use axum::middleware;

use crate::config::{ClusterIngestConfig, Config};
use crate::kafka::ingest::Ingest;
use crate::kafka::model::{AclListing, RegisteredSchema};
use crate::kafka::store::{ClusterStore, StoreSet};
use crate::kafka::{
    ConfigEntry, KafkaError, RecordLimits, RecordPage, RecordQuery, SessionSet, read_page,
};

mod api;
pub(crate) mod auth;
mod health;

pub use auth::AuthState;

#[derive(Clone)]
pub struct AppState {
    pub(crate) sessions: Arc<SessionSet>,
    pub(crate) stores: Arc<StoreSet>,
    pub(crate) auth: AuthState,
    limits: RecordLimits,
    _ingest: Option<Arc<Ingest>>,
}

impl AppState {
    pub fn new(sessions: Arc<SessionSet>) -> Self {
        Self::build(sessions, AuthState::disabled())
    }

    pub fn with_auth(sessions: Arc<SessionSet>, auth: AuthState) -> Self {
        Self::build(sessions, auth)
    }

    fn build(sessions: Arc<SessionSet>, auth: AuthState) -> Self {
        Self {
            stores: Arc::new(StoreSet::new(sessions.identities())),
            sessions,
            auth,
            limits: RecordLimits::from_env(),
            _ingest: None,
        }
    }

    pub fn with_ingest(self) -> Self {
        self.ingest_with(|_| ClusterIngestConfig::default())
    }

    pub fn with_ingest_from(self, config: &Config) -> Self {
        self.ingest_with(|name| {
            config
                .clusters
                .iter()
                .find(|cluster| cluster.name.trim() == name)
                .map(|cluster| cluster.ingest)
                .unwrap_or_default()
        })
    }

    fn ingest_with(self, ingest: impl Fn(&str) -> ClusterIngestConfig) -> Self {
        let clusters = self
            .sessions
            .sessions()
            .into_iter()
            .filter_map(|session| {
                let store = self.stores.get(&session.identity().name)?;
                let ingest = ingest(&session.identity().name);
                Some((Arc::clone(store), session, ingest))
            })
            .collect::<Vec<_>>();

        Self {
            _ingest: Some(Arc::new(Ingest::start(clusters))),
            ..self
        }
    }

    pub(crate) fn cluster(&self, name: &str) -> Result<&Arc<ClusterStore>, KafkaError> {
        self.stores.cluster(name)
    }

    pub(crate) fn is_ready(&self) -> bool {
        self.stores.ready()
    }

    pub(crate) async fn live_records(
        &self,
        cluster: &str,
        query: RecordQuery,
    ) -> Result<RecordPage, KafkaError> {
        read_page(
            self.sessions.session(cluster)?,
            self.cluster(cluster)?,
            query,
            self.limits,
        )
        .await
    }

    pub(crate) async fn live_broker_configs(
        &self,
        cluster: &str,
        id: i32,
    ) -> Result<Vec<ConfigEntry>, KafkaError> {
        let store = self.cluster(cluster)?;
        if let Some(topology) = store.topology.load()
            && !topology.brokers.contains_key(&id)
        {
            return Err(KafkaError::UnknownBroker {
                cluster: cluster.to_owned(),
                id,
            });
        }

        self.sessions.session(cluster)?.broker_configs(id).await
    }

    pub(crate) async fn live_acls(&self, cluster: &str) -> Result<AclListing, KafkaError> {
        self.sessions.session(cluster)?.acls().await
    }

    pub(crate) async fn live_subject_schema(
        &self,
        cluster: &str,
        subject: &str,
        version: i32,
    ) -> Result<RegisteredSchema, KafkaError> {
        self.sessions
            .session(cluster)?
            .subject_schema(subject, version)
            .await
    }
}

pub use api::typescript;

pub fn router(state: AppState) -> Router {
    let api = api::router().route_layer(middleware::from_fn_with_state(
        state.clone(),
        auth::require_session,
    ));
    let auth_layer = state.auth.layer();

    Router::new()
        .merge(api)
        .nest("/api/auth", auth::router())
        .merge(health::router())
        .with_state(state)
        .fallback(crate::server::web::serve)
        .layer(auth_layer)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kafka::FakeCluster;

    async fn wait_until(predicate: impl Fn() -> bool) {
        for _ in 0..1_000 {
            if predicate() {
                return;
            }
            tokio::task::yield_now().await;
        }
        panic!("condition not met");
    }

    #[tokio::test]
    async fn ingestion_fills_the_store_the_api_projects_from() {
        let state = AppState::new(Arc::new(SessionSet::from_sessions(vec![
            FakeCluster::local(),
        ])))
        .with_ingest();

        wait_until(|| {
            state.is_ready() && !state.cluster("local").unwrap().subject_rows().is_empty()
        })
        .await;

        let store = state.cluster("local").unwrap();
        assert_eq!(store.topic_rows()[0].name.as_ref(), "orders.created");
        assert_eq!(
            store.subject_rows()[0].subject.as_ref(),
            "orders.created-value"
        );
    }

    #[tokio::test]
    async fn ingestion_never_describes_acls() {
        let session = FakeCluster::local();
        let state =
            AppState::new(Arc::new(SessionSet::from_sessions(vec![session.clone()]))).with_ingest();

        wait_until(|| state.is_ready()).await;

        assert!(
            session.calls().metadata() > 0,
            "topology lane never called metadata"
        );
        assert_eq!(session.calls().acls(), 0);
    }

    #[tokio::test]
    async fn a_state_without_ingestion_never_becomes_ready() {
        let state = AppState::new(Arc::new(SessionSet::from_sessions(vec![
            FakeCluster::local(),
        ])));

        assert!(!state.is_ready());
        assert!(state.cluster("ghost").is_err());
    }
}
