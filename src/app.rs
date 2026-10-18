use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::http::{HeaderValue, StatusCode, header};
use axum::middleware;
use axum::response::Response;
use governor::{DefaultDirectRateLimiter, Quota, RateLimiter};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::config::{AllowedHost, Mcp, Tuning};
use crate::kafka::{Clusters, TailLimits};
use crate::server::hosts::AllowedHostsLayer;
use error::ApiError;

pub(crate) mod auth;
mod context;
pub(crate) mod error;
mod extract;
mod health;
pub mod mcp;
mod resources;
#[cfg(feature = "typescript")]
pub mod typescript;

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
    mcp_calls: Arc<Semaphore>,
    mcp_live_calls: Arc<DefaultDirectRateLimiter>,
}

impl AppState {
    pub fn new(clusters: Clusters, auth: AuthState, limits: Limits) -> Self {
        Self {
            clusters: Arc::new(clusters),
            auth,
            limits: limits.tail,
            tails: Arc::new(Semaphore::new(limits.live_tails)),
            mcp_calls: Arc::new(Semaphore::new(limits.mcp_calls)),
            mcp_live_calls: Arc::new(RateLimiter::direct(Quota::per_minute(
                limits.mcp_live_calls_per_minute,
            ))),
        }
    }

    pub(crate) fn tail_permit(&self) -> Option<OwnedSemaphorePermit> {
        Arc::clone(&self.tails).try_acquire_owned().ok()
    }

    pub(crate) fn mcp_permit(&self) -> Option<OwnedSemaphorePermit> {
        Arc::clone(&self.mcp_calls).try_acquire_owned().ok()
    }

    pub(crate) fn mcp_live_call(&self) -> bool {
        self.mcp_live_calls.check().is_ok()
    }
}

/// How much one request may read, how many live tails and MCP tool calls run
/// at once, and how often MCP tools may read more from Kafka.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    /// Sizes live tails; its `records` also bounds one-shot record pages.
    pub tail: TailLimits,
    /// Live tails served at once, across every cluster.
    pub live_tails: usize,
    /// MCP tool calls served at once, across every client.
    pub mcp_calls: usize,
    pub mcp_live_calls_per_minute: NonZeroU32,
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
            mcp_calls: tuning.mcp.max_concurrent_calls.get(),
            mcp_live_calls_per_minute: tuning.mcp.live_calls_per_minute,
        }
    }
}

fn auth_routes() -> Router<AppState> {
    Router::new().nest("/auth", auth::router())
}

fn api_routes(state: AppState, resources: Router<AppState>) -> Router<AppState> {
    let api = auth_routes()
        .merge(resources)
        .fallback(not_found)
        .method_not_allowed_fallback(method_not_allowed)
        .with_state(state);

    Router::new().nest_service("/api", api)
}

async fn not_found() -> ApiError {
    ApiError::NotFound
}

async fn method_not_allowed() -> ApiError {
    ApiError::InvalidRequest {
        status: StatusCode::METHOD_NOT_ALLOWED,
        message: "method not allowed".to_owned(),
    }
}

pub fn router(state: AppState, allowed_hosts: &[AllowedHost]) -> Router {
    let resources = resources::router().route_layer(middleware::from_fn_with_state(
        state.clone(),
        auth::require_session,
    ));
    let auth_layer = state.auth.layer();

    let mut app = Router::new()
        .merge(api_routes(state.clone(), resources))
        .with_state(state.clone())
        .merge(crate::server::web::router());
    if !state.auth.is_enabled() {
        app = app.layer(AllowedHostsLayer::new(allowed_hosts));
    }

    // Kubernetes probes name the pod IP as the host, so health skips the
    // check. When neither router sets a fallback, merge keeps the one from
    // `app`, which carries the check.
    health::router()
        .with_state(state)
        .merge(app)
        .layer(auth_layer)
        .layer(middleware::map_response(refuse_framing))
}

/// Any page the browser opens could otherwise frame the UI and steer a click
/// onto a write, and with `auth` off nothing else would stop it.
async fn refuse_framing(mut response: Response) -> Response {
    let headers = response.headers_mut();
    headers.insert(
        header::CONTENT_SECURITY_POLICY,
        HeaderValue::from_static("frame-ancestors 'none'"),
    );
    headers.insert(header::X_FRAME_OPTIONS, HeaderValue::from_static("DENY"));
    response
}

/// Serves `/mcp` alone, at `mcp.bind`, so the session cookie is never read or
/// set on it.
pub fn mcp_router(state: AppState, mcp: &Mcp) -> Router {
    mcp::router(state, mcp)
}
