use std::sync::Arc;

use axum::extract::{Request, State};
use axum::http::header;
use axum::http::uri::Authority;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};

use super::error::ApiError;
use crate::config::AllowedHost;

#[cfg(test)]
mod tests;

pub async fn require_allowed_host(
    State(allowed): State<Arc<[AllowedHost]>>,
    request: Request,
    next: Next,
) -> Response {
    let authority = match request.headers().get(header::HOST) {
        Some(host) => Authority::try_from(host.as_bytes()).ok(),
        // An absolute-form request target names the host in the URI instead.
        None => request.uri().authority().cloned(),
    };
    if authority
        .as_ref()
        .is_some_and(|authority| allowed.iter().any(|host| host.allows(authority)))
    {
        return next.run(request).await;
    }
    tracing::debug!(
        host = authority.as_ref().map(Authority::as_str),
        "host not in allowed_hosts"
    );
    ApiError::HostNotAllowed.into_response()
}
