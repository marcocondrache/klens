use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use axum::body::{Body, Bytes, to_bytes};
use axum::extract::State;
use axum::http::{HeaderMap, HeaderValue, Request, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::{Json, Router};
use jiff::Timestamp;
use openidconnect::{CsrfToken, Nonce, PkceCodeChallenge, PkceCodeVerifier, RefreshToken};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tower::ServiceExt as _;
use tower_sessions::cookie::Key;
use tower_sessions::session::Id;

use super::access::{AccessPolicy, ClusterScope, EffectiveAccess, Grant, PrivilegeSet};
use super::backend::AuthBackend;
use super::login::LoginCookie;
use super::oidc::{OidcError, OidcFlow};
use super::sign_in::{Renewal, SignIn};
use super::store::ExpiringStore;
use super::{AuthSession, AuthState, Holder, SessionGuard, SessionUser, session_layer};
use crate::AppState;
use crate::app::Limits;
use crate::app::testing::Live;
use crate::config::{Config, Tuning};
use crate::kafka::Clusters;
use crate::testing::FakeCluster;

impl AuthState {
    pub fn enabled_for_tests() -> Self {
        Self::enabled_for_tests_with(FakeOidc::default(), AccessPolicy::Open)
    }

    pub fn enabled_for_tests_with(flow: FakeOidc, policy: AccessPolicy) -> Self {
        let sessions = ExpiringStore::default();
        Self {
            backend: AuthBackend::enabled(Arc::new(flow)),
            policy: Arc::new(policy),
            session_layer: session_layer(sessions.clone(), false, Key::generate()),
            sessions,
            login_cookie: LoginCookie::new(false, Duration::from_secs(10 * 60)),
        }
    }
}

impl SessionGuard {
    pub fn open() -> Self {
        Self {
            auth: AuthState::disabled(),
            holder: Holder::Nobody,
        }
    }

    pub fn expired() -> Self {
        Self {
            auth: AuthState::enabled_for_tests(),
            holder: Holder::Session {
                id: Id::default(),
                subject: "gone".to_owned(),
            },
        }
    }
}

#[derive(Clone, Default)]
pub struct FakeOidc {
    pub groups: Vec<String>,
    /// Set, a sign-in comes with a refresh token and an ID token that has
    /// already expired, so the next request has to renew it.
    pub refresh: Option<FakeRefresh>,
}

/// A provider that rotates its refresh tokens and takes each one once.
#[derive(Clone, Default)]
pub struct FakeRefresh {
    /// The groups in a renewed ID token.
    pub groups: Vec<String>,
    /// How long a renewed ID token lasts. At zero every request renews.
    pub id_token_ttl: i64,
    pub fails: bool,
    pub(super) issued: Arc<AtomicU32>,
}

impl FakeRefresh {
    pub fn renewals(&self) -> u32 {
        self.issued.load(Ordering::SeqCst).saturating_sub(1)
    }

    fn sign_in(&self, user: SessionUser) -> SignIn {
        let issued = self.issued.fetch_add(1, Ordering::SeqCst) + 1;
        let renewal = Renewal {
            refresh_token: RefreshToken::new(format!("refresh-{issued}")),
            deadline: Timestamp::now().as_second() + 3600,
        };
        SignIn::new(user, Some(renewal))
    }

    fn takes(&self, refresh_token: &RefreshToken) -> bool {
        let latest = format!("refresh-{}", self.issued.load(Ordering::SeqCst));
        !self.fails && *refresh_token.secret() == latest
    }
}

impl FakeOidc {
    pub fn refreshing(refresh: &FakeRefresh) -> Self {
        Self {
            groups: Vec::new(),
            refresh: Some(refresh.clone()),
        }
    }

    fn user(&self, groups: &[String], ttl: i64) -> SessionUser {
        SessionUser::new(
            "user-1",
            Some("user@example.com".into()),
            Some("Test User".into()),
            groups.to_vec(),
            Timestamp::now().as_second() + ttl,
        )
    }
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
    ) -> Result<SignIn, OidcError> {
        if code != "test-code" {
            return Err(OidcError::Exchange("invalid authorization code".to_owned()));
        }

        Ok(match &self.refresh {
            Some(refresh) => refresh.sign_in(self.user(&self.groups, 0)),
            None => SignIn::new(self.user(&self.groups, 3600), None),
        })
    }

    async fn refresh(&self, user: &SessionUser, renewal: &Renewal) -> Result<SignIn, OidcError> {
        let refresh = self
            .refresh
            .as_ref()
            .filter(|refresh| refresh.takes(&renewal.refresh_token))
            .ok_or_else(|| OidcError::Exchange("invalid refresh token".to_owned()))?;

        // Lets every request that found the token expired reach the renewal.
        tokio::task::yield_now().await;

        let renewed = self.user(&refresh.groups, refresh.id_token_ttl);
        Ok(refresh.sign_in(renewed.renewing(user)))
    }
}

#[derive(Serialize, Deserialize)]
pub(super) struct Impersonated {
    sub: String,
    email: Option<String>,
    name: Option<String>,
    exp: i64,
    groups: Vec<String>,
}

pub(super) async fn impersonate(
    State(state): State<AppState>,
    mut auth_session: AuthSession,
    Json(impersonated): Json<Impersonated>,
) -> Response {
    let user = SessionUser::new(
        impersonated.sub,
        impersonated.email,
        impersonated.name,
        impersonated.groups,
        impersonated.exp,
    );
    state
        .auth
        .backend
        .remember(SignIn::new(user.clone(), None))
        .await;
    if let Err(error) = auth_session.login(&user).await {
        tracing::error!(%error, "failed to impersonate");
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }
    StatusCode::NO_CONTENT.into_response()
}

pub fn access(grants: impl IntoIterator<Item = Grant>) -> EffectiveAccess {
    EffectiveAccess::Granted(grants.into_iter().collect())
}

pub fn admin() -> Grant {
    role("admin", PrivilegeSet::all())
}

pub fn viewer() -> Grant {
    role("viewer", PrivilegeSet::empty())
}

pub fn role(name: &str, privileges: PrivilegeSet) -> Grant {
    Grant {
        role_name: Arc::from(name),
        privileges,
        scope: ClusterScope::All,
    }
}

impl Grant {
    pub fn on(self, clusters: &[&str]) -> Self {
        Self {
            scope: ClusterScope::Only(Arc::new(
                clusters.iter().map(|name| (*name).to_owned()).collect(),
            )),
            ..self
        }
    }
}

pub struct Browser {
    app: Router,
    jar: BTreeMap<String, String>,
}

impl Browser {
    pub fn new(auth: AuthState) -> Self {
        Self::serving(AppState::new(
            Clusters::from_sessions(vec![FakeCluster::local()]),
            auth,
            Limits::new(&Tuning::default()),
        ))
    }

    pub fn serving(state: AppState) -> Self {
        Self {
            app: crate::app::router(state, &Config::default().allowed_hosts),
            jar: BTreeMap::new(),
        }
    }

    /// Keeps the cookies, as a browser does when klens restarts.
    pub fn restart(&mut self, auth: AuthState) {
        self.app = Self::new(auth).app;
    }

    pub async fn get(&mut self, path: &str) -> Page {
        self.send(Request::get(path).body(Body::empty()).expect("request"))
            .await
    }

    pub async fn post(&mut self, path: &str) -> Page {
        self.send(Request::post(path).body(Body::empty()).expect("request"))
            .await
    }

    pub async fn open(&mut self, path: &str) -> Live {
        let request = Request::get(path).body(Body::empty()).expect("request");
        Live::open(path, self.respond(request).await)
    }

    pub async fn impersonate(&mut self, user: &SessionUser) {
        let body = serde_json::to_vec(&Impersonated {
            sub: user.sub.clone(),
            email: user.email.clone(),
            name: user.name.clone(),
            exp: user.exp,
            groups: user.groups.clone(),
        })
        .expect("user json");
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
        self.return_from_provider(&login, code).await
    }

    pub async fn return_from_provider(&mut self, login: &Page, code: &str) -> Page {
        let state = url::Url::parse(login.location())
            .expect("the login redirect is a url")
            .query_pairs()
            .find(|(key, _)| key == "state")
            .map(|(_, value)| value.into_owned())
            .expect("the login redirect carries a state");
        self.get(&format!("/api/auth/callback?code={code}&state={state}"))
            .await
    }

    pub async fn send(&mut self, request: Request<Body>) -> Page {
        let response = self.respond(request).await;
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

    async fn respond(&mut self, mut request: Request<Body>) -> Response {
        request
            .headers_mut()
            .entry(header::HOST)
            .or_insert(HeaderValue::from_static("localhost:8080"));
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

        response
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

    pub fn cookie(&self, name: &str) -> Option<&str> {
        self.headers
            .get_all(header::SET_COOKIE)
            .iter()
            .filter_map(|cookie| cookie.to_str().ok())
            .find(|cookie| {
                cookie
                    .strip_prefix(name)
                    .is_some_and(|rest| rest.starts_with('='))
            })
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
