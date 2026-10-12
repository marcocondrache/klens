use std::borrow::Cow;

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

use crate::AppState;
use crate::app::error::ApiError;

mod client;
#[cfg(test)]
mod tests;

use client::Client;

const METADATA_PATH: &str = "/.well-known/oauth-authorization-server";

/// A registration needs a name and a few redirect URIs.
const MAX_REGISTRATION_BYTES: usize = 8 * 1024;

/// klens as the OAuth authorization server of its own `/mcp`.
pub(crate) struct OAuth {
    /// The issuer's path, empty when the issuer is an origin.
    path: String,
    metadata: Value,
}

impl OAuth {
    pub(super) fn new(resource: &Url) -> Self {
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
            metadata,
        }
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
    Router::new().route(
        "/register",
        post(register).layer(DefaultBodyLimit::max(MAX_REGISTRATION_BYTES)),
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
