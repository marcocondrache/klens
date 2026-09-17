use std::sync::Arc;

use axum::Router;
use axum::middleware;

use crate::kafka::ingest::{Ingest, LaneIntervals};
use crate::kafka::model::{AclListing, RegisteredSchema};
use crate::kafka::store::{ClusterStore, StoreSet};
use crate::kafka::{ClusterSession, ConfigEntry, KafkaError, QueryEngine, RecordPage, RecordQuery};

pub(crate) mod auth;
mod graphql;
mod health;

pub use auth::AuthState;

#[derive(Clone)]
pub struct AppState {
    pub(crate) query: Arc<QueryEngine<dyn ClusterSession>>,
    pub(crate) stores: Arc<StoreSet>,
    pub(crate) auth: AuthState,
    _ingest: Option<Arc<Ingest>>,
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
            stores: Arc::new(StoreSet::new(query.identities())),
            query,
            auth,
            _ingest: None,
        }
    }

    pub fn with_ingest(self, intervals: LaneIntervals) -> Self {
        let clusters = self
            .query
            .sessions()
            .into_iter()
            .filter_map(|session| {
                let store = self.stores.get(&session.identity().name)?;
                Some((Arc::clone(store), session))
            })
            .collect::<Vec<_>>();

        Self {
            _ingest: Some(Arc::new(Ingest::start(clusters, intervals))),
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
        self.query.records(cluster, query).await
    }

    pub(crate) async fn live_broker_configs(
        &self,
        cluster: &str,
        id: i32,
    ) -> Result<Vec<ConfigEntry>, KafkaError> {
        self.query.broker_configs(cluster, id).await
    }

    pub(crate) async fn live_acls(&self, cluster: &str) -> Result<AclListing, KafkaError> {
        self.query.session(cluster)?.acls().await
    }

    pub(crate) async fn live_subject_schema(
        &self,
        cluster: &str,
        subject: &str,
        version: i32,
    ) -> Result<RegisteredSchema, KafkaError> {
        self.query
            .session(cluster)?
            .subject_schema(subject, version)
            .await
    }
}

pub use graphql::{Schema, schema};

pub fn router(state: AppState) -> Router {
    let graphql = graphql::router().route_layer(middleware::from_fn_with_state(
        state.clone(),
        auth::require_session,
    ));
    let auth_layer = state.auth.layer();

    Router::new()
        .merge(graphql)
        .merge(auth::router())
        .merge(health::router())
        .with_state(state)
        .fallback(crate::server::web::serve)
        .layer(auth_layer)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::kafka::FakeCluster;

    fn intervals() -> LaneIntervals {
        LaneIntervals {
            topology: Duration::from_secs(60),
            watermarks: Duration::from_secs(60),
            offsets_tick: Duration::from_secs(60),
            fast_offsets: Duration::from_secs(60),
            slow_offsets: Duration::from_secs(60),
            configs: Duration::from_secs(60),
            subjects: Duration::from_secs(60),
        }
    }

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
        let state = AppState::new(Arc::new(QueryEngine::from_sessions(vec![
            FakeCluster::local(),
        ])))
        .with_ingest(intervals());

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
        let state = AppState::new(Arc::new(QueryEngine::from_sessions(vec![session.clone()])))
            .with_ingest(intervals());

        wait_until(|| state.is_ready()).await;

        assert!(
            session.calls().metadata() > 0,
            "topology lane never called metadata"
        );
        assert_eq!(session.calls().acls(), 0);
    }

    #[tokio::test]
    async fn a_state_without_ingestion_never_becomes_ready() {
        let state = AppState::new(Arc::new(QueryEngine::from_sessions(vec![
            FakeCluster::local(),
        ])));

        assert!(!state.is_ready());
        assert!(state.cluster("ghost").is_err());
    }
}
