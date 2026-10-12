use std::borrow::Cow;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::rejection::JsonRejection;
use axum::extract::{DefaultBodyLimit, State};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use jiff::Timestamp;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use url::Url;

use super::access::Ceiling;
use super::bearer::Caller;
use crate::AppState;
use crate::app::error::ApiError;

mod client;
mod consent;
#[cfg(test)]
mod tests;
mod token;

use client::Client;
pub(super) use consent::{Authorization, signed_in};
#[cfg(feature = "typescript")]
pub use consent::{ConsentRequest, Decided, RedirectTarget};
use token::Grants;
pub(super) use token::TOKEN_PREFIX;

const METADATA_PATH: &str = "/.well-known/oauth-authorization-server";

/// A registration needs a name and a few redirect URIs, and a token request
/// a few short fields.
const MAX_REGISTRATION_BYTES: usize = 8 * 1024;
const MAX_TOKEN_REQUEST_BYTES: usize = 4 * 1024;

/// klens as the OAuth authorization server of its own `/mcp`.
pub(crate) struct OAuth {
    issuer: String,
    /// The issuer's path, empty when the issuer is an origin.
    path: String,
    origin: String,
    resource: Url,
    metadata: Value,
    ceiling: Ceiling,
    /// How long a token lasts from the person's approval.
    lifetime: Duration,
    grants: Grants,
}

impl OAuth {
    pub(super) fn new(resource: &Url, ceiling: Ceiling, lifetime: Duration) -> Self {
        let path = resource.path().trim_end_matches('/');
        let path = path.rsplit_once('/').map_or("", |(parent, _)| parent);
        // The resource without its last segment, so the endpoints sit beside
        // `/mcp` under whatever prefix a proxy adds.
        let issuer = format!("{}{path}", resource.origin().ascii_serialization());
        let metadata = json!({
            "issuer": issuer,
            "authorization_endpoint": format!("{issuer}/api/oauth/authorize"),
            "token_endpoint": format!("{issuer}/api/oauth/token"),
            "registration_endpoint": format!("{issuer}/api/oauth/register"),
            "response_types_supported": ["code"],
            "grant_types_supported": ["authorization_code"],
            "code_challenge_methods_supported": ["S256"],
            "token_endpoint_auth_methods_supported": ["none"],
            "authorization_response_iss_parameter_supported": true,
        });
        Self {
            path: path.to_owned(),
            origin: resource.origin().ascii_serialization(),
            resource: resource.clone(),
            issuer,
            metadata,
            ceiling,
            lifetime,
            grants: Grants::new(lifetime),
        }
    }

    /// RFC 8707: a client that names a resource names `/mcp`.
    fn serves(&self, resource: &str) -> bool {
        Url::parse(resource).is_ok_and(|resource| resource == self.resource)
    }

    pub(super) fn caller(&self, token: &str) -> Option<Arc<Caller>> {
        self.grants.caller(token)
    }

    /// Sends the browser back to the client with RFC 9207's `iss`, so a
    /// client that talks to several servers can tell which one answered.
    fn back(&self, redirect_uri: &Url, state: Option<&str>, params: &[(&str, &str)]) -> Url {
        let mut url = redirect_uri.clone();
        {
            let mut query = url.query_pairs_mut();
            query.extend_pairs(params);
            if let Some(state) = state {
                query.append_pair("state", state);
            }
            query.append_pair("iss", &self.issuer);
        }
        url
    }

    /// RFC 8414 3.1 puts the issuer's path after the well-known segment.
    /// Clients that find nothing there fall back to the bare path.
    pub(crate) fn metadata(&self) -> Router {
        let document = Json(self.metadata.clone());
        let serve = get(move || async move { document });
        let router = Router::new().route(METADATA_PATH, serve.clone());
        match self.path.as_str() {
            "" => router,
            path => router.route(&format!("{METADATA_PATH}{path}"), serve),
        }
    }
}

pub(crate) fn router() -> Router<AppState> {
    Router::new()
        .route(
            "/register",
            post(register).layer(DefaultBodyLimit::max(MAX_REGISTRATION_BYTES)),
        )
        .route("/authorize", get(consent::authorize))
        .route("/consent/{id}", get(consent::show).post(consent::decide))
        .route(
            "/token",
            post(token::token).layer(DefaultBodyLimit::max(MAX_TOKEN_REQUEST_BYTES)),
        )
}

#[derive(Deserialize)]
struct Registration {
    redirect_uris: Vec<String>,
    client_name: Option<String>,
}

#[derive(Serialize)]
struct Registered {
    client_id: String,
    client_id_issued_at: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    client_name: Option<String>,
    redirect_uris: Vec<String>,
    grant_types: [&'static str; 1],
    response_types: [&'static str; 1],
    token_endpoint_auth_method: &'static str,
}

/// RFC 7591 registration, open to anyone. klens keeps no registry, because
/// the client id carries the registration.
async fn register(
    State(state): State<AppState>,
    registration: Result<Json<Registration>, JsonRejection>,
) -> Response {
    if state.auth.oauth().is_none() {
        return ApiError::NotFound.into_response();
    }
    let Json(registration) = match registration {
        Ok(registration) => registration,
        Err(rejection) => {
            return OAuthError::new("invalid_client_metadata", rejection.body_text())
                .into_response();
        }
    };
    let Some(client) = Client::register(
        registration.client_name.as_deref(),
        &registration.redirect_uris,
    ) else {
        return OAuthError::new(
            "invalid_redirect_uri",
            "klens takes an http redirect URI only on a loopback address, and otherwise only \
             the URI scheme of a known app or the callback of a known hosted client",
        )
        .into_response();
    };
    let registered = Registered {
        client_id: client.id(),
        client_id_issued_at: Timestamp::now().as_second(),
        client_name: client.name,
        redirect_uris: client.redirect_uris,
        grant_types: ["authorization_code"],
        response_types: ["code"],
        token_endpoint_auth_method: "none",
    };
    (
        StatusCode::CREATED,
        [(header::CACHE_CONTROL, "no-store")],
        Json(registered),
    )
        .into_response()
}

/// An error in the shape of RFC 6749 5.2, which RFC 7591 3.2.2 reuses.
struct OAuthError {
    error: &'static str,
    description: Cow<'static, str>,
}

impl OAuthError {
    fn new(error: &'static str, description: impl Into<Cow<'static, str>>) -> Self {
        Self {
            error,
            description: description.into(),
        }
    }
}

impl IntoResponse for OAuthError {
    fn into_response(self) -> Response {
        (
            StatusCode::BAD_REQUEST,
            [(header::CACHE_CONTROL, "no-store")],
            Json(json!({
                "error": self.error,
                "error_description": self.description,
            })),
        )
            .into_response()
    }
}
