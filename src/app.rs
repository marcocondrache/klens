use std::sync::Arc;

use axum::Router;
use axum::middleware;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::environment::MAX_LIVE_TAILS;
use crate::kafka::ingest::Ingest;
use crate::kafka::model::{AclListing, RegisteredSchema};
use crate::kafka::store::ClusterStore;
use crate::kafka::{
    Clusters, ConfigEntry, KafkaError, RecordLimits, RecordPage, RecordQuery, Tail, TailLimits,
    TailQuery, read_page,
};

mod acls;
pub(crate) mod auth;
mod brokers;
mod clusters;
mod configs;
mod context;
mod error;
mod extract;
mod groups;
mod health;
mod int64;
mod paging;
mod records;
mod search;
mod subjects;
mod topics;
#[cfg(feature = "typescript")]
pub mod typescript;
mod updates;
mod whoami;

#[cfg(test)]
mod harness;

pub use auth::AuthState;

#[derive(Clone)]
pub struct AppState {
    pub(crate) clusters: Arc<Clusters>,
    pub(crate) auth: AuthState,
    limits: RecordLimits,
    tail_limits: TailLimits,
    tails: Arc<Semaphore>,
    _ingest: Option<Arc<Ingest>>,
}

impl AppState {
    pub fn new(clusters: Arc<Clusters>) -> Self {
        Self::build(clusters, AuthState::disabled())
    }

    pub fn with_auth(clusters: Arc<Clusters>, auth: AuthState) -> Self {
        Self::build(clusters, auth)
    }

    fn build(clusters: Arc<Clusters>, auth: AuthState) -> Self {
        Self {
            clusters,
            auth,
            limits: RecordLimits::from_env(),
            tail_limits: TailLimits::from_env(),
            tails: Arc::new(Semaphore::new(*MAX_LIVE_TAILS)),
            _ingest: None,
        }
    }

    #[cfg(test)]
    pub(crate) fn with_tail_capacity(self, tails: usize) -> Self {
        Self {
            tails: Arc::new(Semaphore::new(tails)),
            ..self
        }
    }

    pub fn with_ingest(self) -> Self {
        Self {
            _ingest: Some(Arc::new(Ingest::start(&self.clusters))),
            ..self
        }
    }

    pub(crate) fn cluster(&self, name: &str) -> Result<&Arc<ClusterStore>, KafkaError> {
        Ok(&self.clusters.get(name)?.store)
    }

    pub(crate) fn is_ready(&self) -> bool {
        self.clusters.ready()
    }

    pub(crate) async fn live_records(
        &self,
        cluster: &str,
        query: RecordQuery,
    ) -> Result<RecordPage, KafkaError> {
        let cluster = self.clusters.get(cluster)?;
        read_page(cluster.session.as_ref(), &cluster.store, query, self.limits).await
    }

    pub(crate) fn tail_permit(&self) -> Option<OwnedSemaphorePermit> {
        Arc::clone(&self.tails).try_acquire_owned().ok()
    }

    pub(crate) async fn live_tail(
        &self,
        cluster: &str,
        query: TailQuery,
    ) -> Result<Tail, KafkaError> {
        let cluster = self.clusters.get(cluster)?;
        Tail::open(
            cluster.session.as_ref(),
            &cluster.store,
            query,
            self.tail_limits,
        )
        .await
    }

    pub(crate) async fn live_broker_configs(
        &self,
        cluster: &str,
        id: i32,
    ) -> Result<Vec<ConfigEntry>, KafkaError> {
        let cluster = self.clusters.get(cluster)?;
        if let Some(topology) = cluster.store.topology.load()
            && !topology.brokers.contains_key(&id)
        {
            return Err(KafkaError::UnknownBroker {
                cluster: cluster.name().to_owned(),
                id,
            });
        }

        cluster.session.broker_configs(id).await
    }

    pub(crate) async fn live_acls(&self, cluster: &str) -> Result<AclListing, KafkaError> {
        self.clusters.get(cluster)?.session.acls().await
    }

    pub(crate) async fn live_subject_schema(
        &self,
        cluster: &str,
        subject: &str,
        version: i32,
    ) -> Result<RegisteredSchema, KafkaError> {
        self.clusters
            .get(cluster)?
            .session
            .subject_schema(subject, version)
            .await
    }
}

fn resources() -> Router<AppState> {
    Router::new()
        .merge(whoami::router())
        .nest("/clusters", clusters::router())
}

fn auth_routes() -> Router<AppState> {
    Router::new().nest("/auth", auth::router())
}

#[cfg(test)]
fn api() -> Router<AppState> {
    health::router().merge(auth_routes()).merge(resources())
}

pub fn router(state: AppState) -> Router {
    let resources = resources().route_layer(middleware::from_fn_with_state(
        state.clone(),
        auth::require_session,
    ));
    let auth_layer = state.auth.layer();

    Router::new()
        .merge(health::router())
        .nest("/api", auth_routes().merge(resources))
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
        let state = AppState::new(Arc::new(Clusters::from_sessions(
            vec![FakeCluster::local()],
        )))
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
            AppState::new(Arc::new(Clusters::from_sessions(vec![session.clone()]))).with_ingest();

        wait_until(|| state.is_ready()).await;

        assert!(
            session.calls().metadata() > 0,
            "topology lane never called metadata"
        );
        assert_eq!(session.calls().acls(), 0);
    }

    #[tokio::test]
    async fn a_state_without_ingestion_never_becomes_ready() {
        let state = AppState::new(Arc::new(Clusters::from_sessions(
            vec![FakeCluster::local()],
        )));

        assert!(!state.is_ready());
        assert!(state.cluster("ghost").is_err());
    }
}
