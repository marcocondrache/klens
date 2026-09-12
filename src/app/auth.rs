use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::extract::{FromRef, Query, State};
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::{IntoResponse, Redirect, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use axum_extra::extract::cookie::{Cookie, Key, PrivateCookieJar, SameSite};
use cookie::time::Duration;
use openidconnect::{CsrfToken, Nonce, PkceCodeChallenge, PkceCodeVerifier};
use serde::{Deserialize, Serialize};

use crate::AppState;
use crate::config::AuthConfig;
use crate::environment::{
    COOKIE_KEY_MIN_LEN, LOGIN_COOKIE, LOGIN_MAX_AGE_SECS, SESSION_COOKIE, SESSION_COOKIE_KEY_PREFIX,
};

mod oidc;

use oidc::{Oidc, OidcFlow};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct SessionUser {
    pub sub: String,
    pub email: Option<String>,
    pub name: Option<String>,
    pub exp: i64,
}

#[derive(Clone)]
pub struct AuthState {
    inner: Inner,
    key: Key,
}

#[derive(Clone)]
enum Inner {
    Disabled,
    Enabled {
        flow: Arc<dyn OidcFlow>,
        cookie_secure: bool,
    },
}

impl AuthState {
    pub fn disabled() -> Self {
        Self {
            inner: Inner::Disabled,
            key: Key::generate(),
        }
    }

    pub async fn from_config(auth: Option<&AuthConfig>) -> anyhow::Result<Self> {
        match auth {
            None => Ok(Self::disabled()),
            Some(config) => {
                let flow = Oidc::discover(&config.oidc).await?;
                Ok(Self::enabled(Arc::new(flow), &config.oidc))
            }
        }
    }

    fn enabled(flow: Arc<dyn OidcFlow>, oidc: &crate::config::OidcConfig) -> Self {
        Self {
            inner: Inner::Enabled {
                flow,
                cookie_secure: oidc.cookie_secure(),
            },
            key: derive_cookie_key(&oidc.issuer, &oidc.client_secret),
        }
    }

    pub fn is_enabled(&self) -> bool {
        matches!(self.inner, Inner::Enabled { .. })
    }

    fn flow(&self) -> Option<&dyn OidcFlow> {
        match &self.inner {
            Inner::Enabled { flow, .. } => Some(flow.as_ref()),
            Inner::Disabled => None,
        }
    }

    fn cookie_secure(&self) -> bool {
        match &self.inner {
            Inner::Enabled { cookie_secure, .. } => *cookie_secure,
            Inner::Disabled => false,
        }
    }

    fn session_from_jar(&self, jar: &PrivateCookieJar) -> Option<SessionUser> {
        let cookie = jar.get(SESSION_COOKIE)?;
        let user: SessionUser = serde_json::from_str(cookie.value()).ok()?;
        if user.exp <= unix_now() {
            return None;
        }
        Some(user)
    }
}

impl FromRef<AppState> for Key {
    fn from_ref(state: &AppState) -> Self {
        state.auth.key.clone()
    }
}

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/auth/me", get(me))
        .route("/auth/login", get(login))
        .route("/auth/callback", get(callback))
        .route("/auth/logout", post(logout))
}

pub async fn require_session(
    State(state): State<AppState>,
    jar: PrivateCookieJar,
    request: axum::extract::Request,
    next: Next,
) -> Response {
    if !state.auth.is_enabled() || state.auth.session_from_jar(&jar).is_some() {
        return next.run(request).await;
    }

    (
        StatusCode::UNAUTHORIZED,
        Json(serde_json::json!({ "error": "unauthorized" })),
    )
        .into_response()
}

#[derive(Serialize)]
struct AuthMeResponse {
    enabled: bool,
    user: Option<AuthUserResponse>,
}

#[derive(Serialize)]
struct AuthUserResponse {
    sub: String,
    email: Option<String>,
    name: Option<String>,
}

impl From<SessionUser> for AuthUserResponse {
    fn from(user: SessionUser) -> Self {
        Self {
            sub: user.sub,
            email: user.email,
            name: user.name,
        }
    }
}

async fn me(State(state): State<AppState>, jar: PrivateCookieJar) -> impl IntoResponse {
    let user = state.auth.session_from_jar(&jar);
    Json(AuthMeResponse {
        enabled: state.auth.is_enabled(),
        user: user.map(AuthUserResponse::from),
    })
}

async fn login(State(state): State<AppState>, jar: PrivateCookieJar) -> Response {
    let Some(flow) = state.auth.flow() else {
        return StatusCode::NOT_FOUND.into_response();
    };

    let (pkce_challenge, pkce_verifier) = PkceCodeChallenge::new_random_sha256();
    let csrf = CsrfToken::new_random();
    let nonce = Nonce::new_random();
    let authorize_url = flow.authorize_url(csrf.clone(), nonce.clone(), pkce_challenge);

    let pending = LoginPending {
        state: csrf.secret().to_owned(),
        nonce: nonce.secret().to_owned(),
        pkce_verifier: pkce_verifier.secret().to_owned(),
    };

    let jar = jar.add(build_cookie(
        LOGIN_COOKIE,
        serde_json::to_string(&pending).expect("login pending json"),
        *LOGIN_MAX_AGE_SECS,
        state.auth.cookie_secure(),
    ));

    (jar, Redirect::to(authorize_url.as_str())).into_response()
}

#[derive(Debug, Deserialize)]
struct CallbackQuery {
    code: Option<String>,
    state: Option<String>,
    error: Option<String>,
}

async fn callback(
    State(state): State<AppState>,
    jar: PrivateCookieJar,
    Query(query): Query<CallbackQuery>,
) -> Response {
    let Some(flow) = state.auth.flow() else {
        return StatusCode::NOT_FOUND.into_response();
    };

    if query.error.is_some() {
        return login_error(jar, state.auth.cookie_secure()).into_response();
    }

    let Some(pending) = login_pending(&jar) else {
        tracing::warn!("oidc callback missing login state");
        return login_error(jar, state.auth.cookie_secure()).into_response();
    };

    let Some(state_param) = query.state.as_deref() else {
        return login_error(jar, state.auth.cookie_secure()).into_response();
    };

    if pending.state != state_param {
        tracing::warn!("oidc callback rejected: state mismatch");
        return login_error(jar, state.auth.cookie_secure()).into_response();
    }

    let Some(code) = query.code else {
        return login_error(jar, state.auth.cookie_secure()).into_response();
    };

    let user = match flow
        .authenticate(
            code,
            PkceCodeVerifier::new(pending.pkce_verifier),
            Nonce::new(pending.nonce),
        )
        .await
    {
        Ok(user) => user,
        Err(error) => {
            tracing::warn!(%error, "oidc callback failed");
            return login_error(jar, state.auth.cookie_secure()).into_response();
        }
    };

    tracing::info!(sub = %user.sub, "oidc login succeeded");

    let ttl = (user.exp - unix_now()).max(0);
    let jar = jar
        .remove(removal_cookie(LOGIN_COOKIE, state.auth.cookie_secure()))
        .add(build_cookie(
            SESSION_COOKIE,
            serde_json::to_string(&user).expect("session json"),
            ttl,
            state.auth.cookie_secure(),
        ));

    (jar, Redirect::to("/")).into_response()
}

async fn logout(State(state): State<AppState>, jar: PrivateCookieJar) -> impl IntoResponse {
    let secure = state.auth.cookie_secure();
    let jar = jar
        .remove(removal_cookie(SESSION_COOKIE, secure))
        .remove(removal_cookie(LOGIN_COOKIE, secure));
    (jar, StatusCode::NO_CONTENT)
}

fn login_error(jar: PrivateCookieJar, secure: bool) -> impl IntoResponse {
    (
        jar.remove(removal_cookie(LOGIN_COOKIE, secure)),
        Redirect::to("/login?error=auth"),
    )
}

#[derive(Debug, Serialize, Deserialize)]
struct LoginPending {
    state: String,
    nonce: String,
    pkce_verifier: String,
}

fn login_pending(jar: &PrivateCookieJar) -> Option<LoginPending> {
    let cookie = jar.get(LOGIN_COOKIE)?;
    serde_json::from_str(cookie.value()).ok()
}

fn build_cookie(
    name: &'static str,
    value: String,
    max_age_secs: i64,
    secure: bool,
) -> Cookie<'static> {
    Cookie::build((name, value))
        .http_only(true)
        .same_site(SameSite::Lax)
        .path("/")
        .secure(secure)
        .max_age(Duration::seconds(max_age_secs.max(0)))
        .build()
}

fn removal_cookie(name: &'static str, secure: bool) -> Cookie<'static> {
    Cookie::build(name).path("/").secure(secure).build()
}

fn derive_cookie_key(issuer: &str, client_secret: &str) -> Key {
    let mut material = format!("{SESSION_COOKIE_KEY_PREFIX}|{issuer}|{client_secret}").into_bytes();
    if material.len() < COOKIE_KEY_MIN_LEN {
        material.resize(COOKIE_KEY_MIN_LEN, 0);
    }
    Key::derive_from(&material)
}

pub(crate) fn unix_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

#[cfg(test)]
mod tests {
    use axum::body::{Body, to_bytes};
    use axum::http::StatusCode;
    use axum::http::{Request, header};
    use tower::ServiceExt;

    use super::oidc::FakeOidc;
    use super::*;
    use crate::kafka::{FakeCluster, QueryEngine};

    impl AuthState {
        pub(crate) fn enabled_for_tests() -> Self {
            Self {
                inner: Inner::Enabled {
                    flow: Arc::new(FakeOidc),
                    cookie_secure: false,
                },
                key: Key::derive_from(b"klens-test-session-cookie-key-32b!!"),
            }
        }

        fn session_cookie_header(&self, user: &SessionUser) -> String {
            let mut jar = cookie::CookieJar::new();
            jar.private_mut(&self.key).add(Cookie::new(
                SESSION_COOKIE,
                serde_json::to_string(user).expect("session json"),
            ));
            let cookie = jar.get(SESSION_COOKIE).expect("session cookie");
            format!("{SESSION_COOKIE}={}", cookie.value())
        }
    }

    fn app(auth: AuthState) -> axum::Router {
        crate::app::router(AppState::with_auth(
            Arc::new(QueryEngine::from_sessions(vec![FakeCluster::local()])),
            auth,
        ))
    }

    async fn send(app: axum::Router, request: Request<Body>) -> axum::http::Response<Body> {
        app.oneshot(request).await.unwrap()
    }

    fn graphql_request() -> Request<Body> {
        Request::builder()
            .method("POST")
            .uri("/graphql")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(r#"{"query":"{ clusters { name } }"}"#))
            .unwrap()
    }

    fn cookie_header(response: &axum::http::Response<Body>) -> String {
        response
            .headers()
            .get_all(header::SET_COOKIE)
            .iter()
            .filter_map(|value| value.to_str().ok())
            .map(|set_cookie| {
                set_cookie
                    .split(';')
                    .next()
                    .unwrap_or(set_cookie)
                    .trim()
                    .to_owned()
            })
            .collect::<Vec<_>>()
            .join("; ")
    }

    #[tokio::test]
    async fn health_stays_public_when_oidc_enabled() {
        let response = send(
            app(AuthState::enabled_for_tests()),
            Request::builder()
                .uri("/health")
                .body(Body::empty())
                .unwrap(),
        )
        .await;

        assert_eq!(response.status(), StatusCode::NO_CONTENT);
    }

    #[tokio::test]
    async fn ready_stays_public_when_oidc_enabled() {
        let response = send(
            app(AuthState::enabled_for_tests()),
            Request::builder()
                .uri("/ready")
                .body(Body::empty())
                .unwrap(),
        )
        .await;

        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn graphql_is_open_when_oidc_disabled() {
        let response = send(app(AuthState::disabled()), graphql_request()).await;
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn graphql_unauthorized_without_session() {
        let response = send(app(AuthState::enabled_for_tests()), graphql_request()).await;
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["error"], "unauthorized");
    }

    #[tokio::test]
    async fn graphql_allows_valid_session() {
        let auth = AuthState::enabled_for_tests();
        let user = SessionUser {
            sub: "user-1".into(),
            email: Some("user@example.com".into()),
            name: Some("Test User".into()),
            exp: unix_now() + 3600,
        };
        let cookie = auth.session_cookie_header(&user);

        let mut request = graphql_request();
        request
            .headers_mut()
            .insert(header::COOKIE, cookie.parse().unwrap());

        let response = send(app(auth), request).await;
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn me_reports_disabled_auth() {
        let response = send(
            app(AuthState::disabled()),
            Request::builder()
                .uri("/auth/me")
                .body(Body::empty())
                .unwrap(),
        )
        .await;

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["enabled"], false);
        assert!(json["user"].is_null());
    }

    #[tokio::test]
    async fn me_reports_enabled_auth_without_user() {
        let response = send(
            app(AuthState::enabled_for_tests()),
            Request::builder()
                .uri("/auth/me")
                .body(Body::empty())
                .unwrap(),
        )
        .await;

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["enabled"], true);
        assert!(json["user"].is_null());
    }

    #[tokio::test]
    async fn login_is_not_found_when_disabled() {
        let response = send(
            app(AuthState::disabled()),
            Request::builder()
                .uri("/auth/login")
                .body(Body::empty())
                .unwrap(),
        )
        .await;

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn login_redirects_to_identity_provider() {
        let response = send(
            app(AuthState::enabled_for_tests()),
            Request::builder()
                .uri("/auth/login")
                .body(Body::empty())
                .unwrap(),
        )
        .await;

        assert!(response.status().is_redirection());
        let location = response
            .headers()
            .get(header::LOCATION)
            .unwrap()
            .to_str()
            .unwrap();
        assert!(location.starts_with("https://idp.example/authorize"));
        assert!(cookie_header(&response).contains(LOGIN_COOKIE));
    }

    #[tokio::test]
    async fn callback_rejects_missing_and_mismatched_state() {
        let router = app(AuthState::enabled_for_tests());

        let missing = send(
            router.clone(),
            Request::builder()
                .uri("/auth/callback?code=test-code&state=nope")
                .body(Body::empty())
                .unwrap(),
        )
        .await;
        assert!(missing.status().is_redirection());
        assert_eq!(
            missing.headers().get(header::LOCATION).unwrap(),
            "/login?error=auth"
        );

        let login = send(
            router.clone(),
            Request::builder()
                .uri("/auth/login")
                .body(Body::empty())
                .unwrap(),
        )
        .await;
        let cookies = cookie_header(&login);

        let mismatched = send(
            router,
            Request::builder()
                .uri("/auth/callback?code=test-code&state=wrong")
                .header(header::COOKIE, cookies)
                .body(Body::empty())
                .unwrap(),
        )
        .await;
        assert!(mismatched.status().is_redirection());
        assert_eq!(
            mismatched.headers().get(header::LOCATION).unwrap(),
            "/login?error=auth"
        );
    }

    #[tokio::test]
    async fn callback_sets_session_when_state_matches() {
        let router = app(AuthState::enabled_for_tests());

        let login = send(
            router.clone(),
            Request::builder()
                .uri("/auth/login")
                .body(Body::empty())
                .unwrap(),
        )
        .await;
        let location = login
            .headers()
            .get(header::LOCATION)
            .unwrap()
            .to_str()
            .unwrap()
            .to_owned();
        let state = url::Url::parse(&location)
            .unwrap()
            .query_pairs()
            .find(|(key, _)| key == "state")
            .map(|(_, value)| value.into_owned())
            .unwrap();
        let cookies = cookie_header(&login);

        let callback = send(
            router.clone(),
            Request::builder()
                .uri(format!("/auth/callback?code=test-code&state={state}"))
                .header(header::COOKIE, cookies)
                .body(Body::empty())
                .unwrap(),
        )
        .await;

        assert!(callback.status().is_redirection());
        assert_eq!(callback.headers().get(header::LOCATION).unwrap(), "/");
        let session_cookie = cookie_header(&callback);
        assert!(session_cookie.contains(SESSION_COOKIE));

        let mut request = graphql_request();
        request
            .headers_mut()
            .insert(header::COOKIE, session_cookie.parse().unwrap());
        let graphql = send(router, request).await;
        assert_eq!(graphql.status(), StatusCode::OK);
    }
}
