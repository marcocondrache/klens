use std::sync::Arc;

use anyhow::anyhow;
use async_trait::async_trait;
use axum::Json;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use jiff::Timestamp;
use openidconnect::{CsrfToken, Nonce, PkceCodeChallenge, PkceCodeVerifier};
use tower_sessions::cookie::Key;
use tower_sessions::cookie::time::Duration;

use super::access::AccessPolicy;
use super::backend::AuthBackend;
use super::oidc::OidcFlow;
use super::{AuthSession, AuthState, SessionGuard, SessionUser, session_layer};
use crate::AppState;

impl AuthState {
    pub fn enabled_for_tests() -> Self {
        Self::enabled_for_tests_with(FakeOidc::default(), AccessPolicy::Open)
    }

    pub fn enabled_for_tests_with(flow: FakeOidc, policy: AccessPolicy) -> Self {
        Self {
            backend: AuthBackend::enabled(Arc::new(flow)),
            policy: Arc::new(policy),
            session_layer: session_layer(false, Key::generate()),
            login_timeout: Duration::minutes(10),
        }
    }
}

impl SessionGuard {
    pub fn open() -> Self {
        Self {
            auth: AuthState::disabled(),
            subject: None,
        }
    }

    pub fn expired() -> Self {
        Self {
            auth: AuthState::enabled_for_tests(),
            subject: Some("gone".to_owned()),
        }
    }
}

#[derive(Clone, Default)]
pub struct FakeOidc {
    pub groups: Vec<String>,
}

#[async_trait]
impl OidcFlow for FakeOidc {
    fn authorize_url(
        &self,
        csrf: CsrfToken,
        _nonce: Nonce,
        _pkce_challenge: PkceCodeChallenge,
    ) -> url::Url {
        let mut url = url::Url::parse("https://idp.example/authorize").expect("stub authorize url");
        url.query_pairs_mut().append_pair("state", csrf.secret());
        url
    }

    async fn authenticate(
        &self,
        code: String,
        _pkce_verifier: PkceCodeVerifier,
        _nonce: Nonce,
    ) -> anyhow::Result<SessionUser> {
        if code != "test-code" {
            return Err(anyhow!("invalid authorization code"));
        }

        Ok(SessionUser::new(
            "user-1",
            Some("user@example.com".into()),
            Some("Test User".into()),
            self.groups.clone(),
            Timestamp::now().as_second() + 3600,
        ))
    }
}

pub(super) async fn impersonate(
    State(state): State<AppState>,
    mut auth_session: AuthSession,
    Json(mut user): Json<SessionUser>,
) -> Response {
    user.refresh_auth_hash();
    state.auth.backend.remember(user.clone());
    if let Err(error) = auth_session.login(&user).await {
        tracing::error!(%error, "failed to impersonate");
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }
    StatusCode::NO_CONTENT.into_response()
}
