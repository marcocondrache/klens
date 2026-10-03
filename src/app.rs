use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::middleware;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::config::Tuning;
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
mod quotas;
mod records;
mod search;
mod subjects;
mod topics;
#[cfg(feature = "typescript")]
pub mod typescript;
mod updates;
mod whoami;

#[cfg(test)]
pub(crate) mod testing;
#[cfg(test)]
mod tests;

pub use auth::AuthState;

pub(crate) const SSE_KEEP_ALIVE: Duration = Duration::from_secs(15);

#[derive(Clone)]
pub struct AppState {
    clusters: Arc<Clusters>,
    auth: AuthState,
    limits: TailLimits,
    tails: Arc<Semaphore>,
}

impl AppState {
    pub fn new(clusters: Clusters, auth: AuthState, limits: Limits) -> Self {
        Self {
            clusters: Arc::new(clusters),
            auth,
            limits: limits.tail,
            tails: Arc::new(Semaphore::new(limits.live_tails)),
        }
    }

    pub(crate) fn tail_permit(&self) -> Option<OwnedSemaphorePermit> {
        Arc::clone(&self.tails).try_acquire_owned().ok()
    }
}

/// How much one request may read, and how many live tails run at once.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    /// Sizes live tails; its `records` also bounds one-shot record pages.
    pub tail: TailLimits,
    /// Live tails served at once, across every cluster.
    pub live_tails: usize,
}

impl Limits {
    pub fn new(tuning: &Tuning) -> Self {
        Self {
            tail: TailLimits {
                batch: tuning.tail.batch_limit.get(),
                interval: tuning.tail.interval,
                poll_wait: tuning.tail.poll_wait,
                heartbeat: SSE_KEEP_ALIVE,
                records: tuning.records,
            },
            live_tails: tuning.tail.max_live,
        }
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
        .merge(crate::server::web::router())
        .layer(auth_layer)
}
