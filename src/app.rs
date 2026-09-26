use std::sync::Arc;

use axum::Router;
use axum::middleware;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::environment::MAX_LIVE_TAILS;
use crate::kafka::ingest::Ingest;
use crate::kafka::{Clusters, TailLimits};

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
    clusters: Arc<Clusters>,
    auth: AuthState,
    /// Sizes live tails; its `records` also bounds one-shot record pages.
    limits: TailLimits,
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
            limits: TailLimits::from_env(),
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

    pub(crate) fn tail_permit(&self) -> Option<OwnedSemaphorePermit> {
        Arc::clone(&self.tails).try_acquire_owned().ok()
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
            state.clusters.ready()
                && !state
                    .clusters
                    .get("local")
                    .unwrap()
                    .store
                    .subject_rows()
                    .is_empty()
        })
        .await;

        let store = &state.clusters.get("local").unwrap().store;
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

        wait_until(|| state.clusters.ready()).await;

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

        assert!(!state.clusters.ready());
        assert!(state.clusters.get("ghost").is_err());
    }
}
