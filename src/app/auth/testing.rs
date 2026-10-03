use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::anyhow;
use async_trait::async_trait;
use axum::body::{Body, Bytes, to_bytes};
use axum::extract::State;
use axum::http::{HeaderMap, Request, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::{Json, Router};
use jiff::Timestamp;
use openidconnect::{CsrfToken, Nonce, PkceCodeChallenge, PkceCodeVerifier};
use serde_json::Value;
use tower::ServiceExt as _;
use tower_sessions::cookie::Key;
use tower_sessions::cookie::time::Duration;

use super::access::AccessPolicy;
use super::backend::AuthBackend;
use super::oidc::OidcFlow;
use super::{AuthSession, AuthState, SessionGuard, SessionUser, session_layer};
use crate::AppState;
use crate::app::Limits;
use crate::config::Tuning;
use crate::kafka::Clusters;
use crate::testing::FakeCluster;

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

pub struct Browser {
    app: Router,
    jar: BTreeMap<String, String>,
}

impl Browser {
    pub fn new(auth: AuthState) -> Self {
        let state = AppState::new(
            Clusters::from_sessions(vec![FakeCluster::local()]),
            auth,
            Limits::new(&Tuning::default()),
        );
        Self {
            app: crate::app::router(state),
            jar: BTreeMap::new(),
        }
    }

    pub async fn get(&mut self, path: &str) -> Page {
        self.send(Request::get(path).body(Body::empty()).expect("request"))
            .await
    }

    pub async fn impersonate(&mut self, user: &SessionUser) {
        let body = serde_json::to_vec(user).expect("user json");
        let page = self
            .send(
                Request::post("/api/auth/impersonate")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(body))
                    .expect("request"),
            )
            .await;
        assert_eq!(page.status, StatusCode::NO_CONTENT);
    }

    pub async fn log_in(&mut self, code: &str) -> Page {
        let login = self.get("/api/auth/login").await;
        let state = url::Url::parse(login.location())
            .expect("the login redirect is a url")
            .query_pairs()
            .find(|(key, _)| key == "state")
            .map(|(_, value)| value.into_owned())
            .expect("the login redirect carries a state");
        self.get(&format!("/api/auth/callback?code={code}&state={state}"))
            .await
    }

    async fn send(&mut self, mut request: Request<Body>) -> Page {
        if !self.jar.is_empty() {
            let cookies = self
                .jar
                .iter()
                .map(|(name, value)| format!("{name}={value}"))
                .collect::<Vec<_>>()
                .join("; ");
            request
                .headers_mut()
                .insert(header::COOKIE, cookies.parse().expect("cookie header"));
        }

        let response = self.app.clone().oneshot(request).await.expect("response");
        for set_cookie in response.headers().get_all(header::SET_COOKIE) {
            let set_cookie = set_cookie.to_str().expect("ascii set-cookie");
            let pair = set_cookie.split(';').next().unwrap_or(set_cookie);
            if let Some((name, value)) = pair.trim().split_once('=') {
                self.jar.insert(name.to_owned(), value.to_owned());
            }
        }

        let status = response.status();
        let headers = response.headers().clone();
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        Page {
            status,
            headers,
            body,
        }
    }
}

pub struct Page {
    pub status: StatusCode,
    pub headers: HeaderMap,
    body: Bytes,
}

impl Page {
    #[track_caller]
    pub fn location(&self) -> &str {
        self.headers
            .get(header::LOCATION)
            .and_then(|location| location.to_str().ok())
            .unwrap_or_else(|| panic!("a {} page with no location", self.status))
    }

    #[track_caller]
    pub fn set_cookie(&self) -> &str {
        self.headers
            .get(header::SET_COOKIE)
            .and_then(|cookie| cookie.to_str().ok())
            .unwrap_or_else(|| panic!("a {} page set no cookie", self.status))
    }

    #[track_caller]
    pub fn json(&self) -> Value {
        serde_json::from_slice(&self.body).unwrap_or_else(|error| {
            panic!(
                "body is not json ({error}): {}",
                String::from_utf8_lossy(&self.body)
            )
        })
    }
}
