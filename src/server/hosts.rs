use std::sync::Arc;
use std::task::{Context, Poll};

use axum::extract::Request;
use axum::http::header;
use axum::http::uri::Authority;
use axum::response::{IntoResponse, Response};
use futures::future::{Either, Ready, ready};
use tower::{Layer, Service};

use crate::app::error::ApiError;
use crate::config::AllowedHost;

#[cfg(test)]
mod tests;

/// Refuses with 403 any request whose host is not in `allowed_hosts`.
#[derive(Clone)]
pub struct AllowedHostsLayer {
    allowed: Arc<[AllowedHost]>,
}

impl AllowedHostsLayer {
    pub fn new(allowed: &[AllowedHost]) -> Self {
        Self {
            allowed: Arc::from(allowed),
        }
    }
}

impl<S> Layer<S> for AllowedHostsLayer {
    type Service = AllowedHosts<S>;

    fn layer(&self, inner: S) -> Self::Service {
        AllowedHosts {
            inner,
            allowed: Arc::clone(&self.allowed),
        }
    }
}

#[derive(Clone)]
pub struct AllowedHosts<S> {
    inner: S,
    allowed: Arc<[AllowedHost]>,
}

impl<S> AllowedHosts<S> {
    fn allows(&self, authority: Option<&Authority>) -> bool {
        authority.is_some_and(|authority| self.allowed.iter().any(|host| host.allows(authority)))
    }
}

impl<S> Service<Request> for AllowedHosts<S>
where
    S: Service<Request, Response = Response>,
{
    type Response = Response;
    type Error = S::Error;
    type Future = Either<S::Future, Ready<Result<Response, S::Error>>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: Request) -> Self::Future {
        let authority = match request.headers().get(header::HOST) {
            Some(host) => Authority::try_from(host.as_bytes()).ok(),
            // An absolute-form request target names the host in the URI instead.
            None => request.uri().authority().cloned(),
        };
        if self.allows(authority.as_ref()) {
            return Either::Left(self.inner.call(request));
        }

        tracing::debug!(
            host = authority.as_ref().map(Authority::as_str),
            "host not in allowed_hosts"
        );
        Either::Right(ready(Ok(ApiError::HostNotAllowed.into_response())))
    }
}
