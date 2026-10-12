use axum::extract::{Request, State};
use axum::http::{HeaderMap, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use sha2::{Digest, Sha256};

use crate::app::error::ApiError;
use crate::config::KeyMaterial;

/// The secret a gateway such as agentgateway adds to every call it lets
/// through, as `Authorization: Bearer`.
#[derive(Clone)]
pub(crate) struct GatewaySecret([u8; 32]);

impl GatewaySecret {
    pub(crate) fn new(secret: &KeyMaterial) -> Self {
        Self(Sha256::digest(secret.as_bytes()).into())
    }

    /// Comparing digests keeps the time a comparison takes from telling
    /// anything about the secret.
    fn admits(&self, headers: &HeaderMap) -> bool {
        let mut values = headers.get_all(header::AUTHORIZATION).iter();
        let (Some(value), None) = (values.next(), values.next()) else {
            return false;
        };
        let Some((scheme, token)) = value.to_str().ok().and_then(|value| value.split_once(' '))
        else {
            return false;
        };
        let presented: [u8; 32] = Sha256::digest(token.trim().as_bytes()).into();
        scheme.eq_ignore_ascii_case("bearer") && presented == self.0
    }
}

/// A 403 rather than a 401, since a 401 sends the client back through the
/// gateway's sign-in, which cannot fix a missing secret.
pub(crate) async fn require_secret(
    State(secret): State<GatewaySecret>,
    mut request: Request,
    next: Next,
) -> Response {
    if !secret.admits(request.headers()) {
        tracing::warn!("refused an mcp call that lacks the gateway's secret");
        return ApiError::NotFromGateway.into_response();
    }
    // Nothing behind `/mcp` can echo the secret once it stops here.
    request.headers_mut().remove(header::AUTHORIZATION);
    next.run(request).await
}
