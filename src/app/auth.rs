use std::sync::Arc;

use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::{IntoResponse, Redirect, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use axum_login::AuthManagerLayerBuilder;
use base64::Engine as _;
use jiff::Timestamp;
use openidconnect::{CsrfToken, Nonce, PkceCodeChallenge};
use serde::{Deserialize, Serialize};
use tower_sessions::cookie::time::Duration;
use tower_sessions::cookie::{Key, SameSite};
use tower_sessions::service::SignedCookie;
use tower_sessions::{Expiry, MemoryStore, SessionManagerLayer};

use crate::AppState;
use crate::config::AuthConfig;
use crate::environment::{
    LOGIN_MAX_AGE_SECS, MIN_SESSION_KEY_BYTES, SESSION_COOKIE, SESSION_COOKIE_KEY_PREFIX,
    SESSION_KEY,
};

pub(crate) mod access;
mod backend;
mod oidc;

use access::{AccessPolicy, EffectiveAccess, Identity};
use backend::{AuthBackend, OidcCredentials};
use oidc::{Oidc, OidcFlow};

const LOGIN_PENDING_KEY: &str = "klens.login_pending";

type AuthSession = axum_login::AuthSession<AuthBackend>;
type SessionLayer = SessionManagerLayer<MemoryStore, SignedCookie>;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct SessionUser {
    pub sub: String,
    pub email: Option<String>,
    pub name: Option<String>,
    pub exp: i64,
    #[serde(default)]
    pub groups: Vec<String>,
    #[serde(skip)]
    auth_hash: Vec<u8>,
}

impl SessionUser {
    pub(crate) fn new(
        sub: impl Into<String>,
        email: Option<String>,
        name: Option<String>,
        groups: Vec<String>,
        exp: i64,
    ) -> Self {
        let mut user = Self {
            sub: sub.into(),
            email,
            name,
            groups,
            exp,
            auth_hash: Vec::new(),
        };
        user.refresh_auth_hash();
        user
    }

    fn refresh_auth_hash(&mut self) {
        self.auth_hash = format!(
            "{SESSION_COOKIE_KEY_PREFIX}|{}|{}|{}",
            self.sub,
            self.exp,
            self.groups.join("\0")
        )
        .into_bytes();
    }
}

#[derive(Clone)]
pub struct AuthState {
    backend: AuthBackend,
    policy: AccessPolicy,
    session_layer: SessionLayer,
}

impl AuthState {
    pub fn disabled() -> Self {
        Self {
            backend: AuthBackend::disabled(),
            policy: AccessPolicy::disabled(),
            session_layer: session_layer(false, Key::generate()),
        }
    }

    pub async fn from_config(auth: Option<&AuthConfig>) -> anyhow::Result<Self> {
        match auth {
            None => Ok(Self::disabled()),
            Some(config) => {
                let policy = AccessPolicy::from_roles(config.roles.as_ref());
                let key = signing_key(SESSION_KEY.as_deref().or(config.session_key.as_deref()))?;
                let flow = Oidc::discover(&config.oidc, policy.groups_claim()).await?;
                Ok(Self::enabled(Arc::new(flow), &config.oidc, policy, key))
            }
        }
    }

    fn enabled(
        flow: Arc<dyn OidcFlow>,
        oidc: &crate::config::OidcConfig,
        policy: AccessPolicy,
        key: Key,
    ) -> Self {
        Self {
            backend: AuthBackend::enabled(flow),
            policy,
            session_layer: session_layer(oidc.cookie_secure(), key),
        }
    }

    pub fn is_enabled(&self) -> bool {
        self.backend.is_enabled()
    }

    fn flow(&self) -> Option<&dyn OidcFlow> {
        self.backend.flow()
    }

    pub(crate) fn layer(
        &self,
    ) -> axum_login::AuthManagerLayer<AuthBackend, MemoryStore, SignedCookie> {
        AuthManagerLayerBuilder::new(self.backend.clone(), self.session_layer.clone()).build()
    }

    fn access_from_user(&self, user: &SessionUser) -> Option<EffectiveAccess> {
        self.policy.admit(&Identity {
            groups: &user.groups,
        })
    }

    fn access_from_session(&self, session: &AuthSession) -> Option<EffectiveAccess> {
        if !self.is_enabled() {
            return Some(EffectiveAccess::Unrestricted);
        }
        let user = session.user.as_ref()?;
        self.access_from_user(user)
    }

    fn guard(&self, session: &AuthSession) -> SessionGuard {
        SessionGuard {
            auth: self.clone(),
            subject: self
                .is_enabled()
                .then(|| session.user.as_ref().map(|user| user.sub.clone()))
                .flatten(),
        }
    }
}

#[derive(Clone)]
pub struct SessionGuard {
    auth: AuthState,
    subject: Option<String>,
}

impl SessionGuard {
    pub fn revalidate(&self) -> Option<EffectiveAccess> {
        let Some(subject) = &self.subject else {
            return (!self.auth.is_enabled()).then_some(EffectiveAccess::Unrestricted);
        };
        let user = self.auth.backend.live_user(subject)?;
        self.auth.access_from_user(&user)
    }

    pub fn subject(&self) -> Option<&str> {
        self.subject.as_deref()
    }

    #[cfg(test)]
    pub(crate) fn open() -> Self {
        Self {
            auth: AuthState::disabled(),
            subject: None,
        }
    }

    #[cfg(test)]
    pub(crate) fn expired() -> Self {
        Self {
            auth: AuthState::enabled_for_tests(),
            subject: Some("gone".to_owned()),
        }
    }
}

pub fn router() -> Router<AppState> {
    let router = Router::new()
        .route("/me", get(me))
        .route("/login", get(login))
        .route("/callback", get(callback))
        .route("/logout", post(logout));

    #[cfg(test)]
    let router = router.route("/impersonate", post(impersonate));

    router
}

pub async fn require_session(
    State(state): State<AppState>,
    auth_session: AuthSession,
    request: axum::extract::Request,
    next: Next,
) -> Response {
    if let Some(access) = state.auth.access_from_session(&auth_session) {
        let mut request = request;
        request.extensions_mut().insert(access);
        request
            .extensions_mut()
            .insert(state.auth.guard(&auth_session));
        return next.run(request).await;
    }

    unauthorized()
}

fn unauthorized() -> Response {
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

async fn me(State(state): State<AppState>, auth_session: AuthSession) -> impl IntoResponse {
    let user = auth_session.user.clone().and_then(|user| {
        state.auth.access_from_user(&user)?;
        Some(AuthUserResponse::from(user))
    });
    Json(AuthMeResponse {
        enabled: state.auth.is_enabled(),
        user,
    })
}

async fn login(State(state): State<AppState>, auth_session: AuthSession) -> Response {
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

    auth_session
        .session
        .set_expiry(Some(Expiry::OnInactivity(Duration::seconds(
            *LOGIN_MAX_AGE_SECS,
        ))));

    if let Err(error) = auth_session
        .session
        .insert(LOGIN_PENDING_KEY, pending)
        .await
    {
        tracing::error!(%error, "failed to store oidc login state");
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }

    Redirect::to(authorize_url.as_str()).into_response()
}

#[derive(Debug, Deserialize)]
struct CallbackQuery {
    code: Option<String>,
    state: Option<String>,
    error: Option<String>,
}

async fn callback(
    State(state): State<AppState>,
    mut auth_session: AuthSession,
    Query(query): Query<CallbackQuery>,
) -> Response {
    if state.auth.flow().is_none() {
        return StatusCode::NOT_FOUND.into_response();
    }

    if query.error.is_some() {
        return login_error(&mut auth_session, LoginFail::Auth).await;
    }

    let pending = match auth_session
        .session
        .get::<LoginPending>(LOGIN_PENDING_KEY)
        .await
    {
        Ok(Some(pending)) => pending,
        Ok(None) => {
            tracing::warn!("oidc callback missing login state");
            return login_error(&mut auth_session, LoginFail::Auth).await;
        }
        Err(error) => {
            tracing::error!(%error, "failed to read oidc login state");
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    };

    let Some(state_param) = query.state.as_deref() else {
        return login_error(&mut auth_session, LoginFail::Auth).await;
    };

    if pending.state != state_param {
        tracing::warn!("oidc callback rejected: state mismatch");
        return login_error(&mut auth_session, LoginFail::Auth).await;
    }

    let Some(code) = query.code else {
        return login_error(&mut auth_session, LoginFail::Auth).await;
    };

    let user = match auth_session
        .authenticate(OidcCredentials {
            code,
            pkce_verifier: pending.pkce_verifier,
            nonce: pending.nonce,
        })
        .await
    {
        Ok(Some(user)) => user,
        Ok(None) => return login_error(&mut auth_session, LoginFail::Auth).await,
        Err(error) => {
            tracing::error!(%error, "oidc authenticate failed");
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    };

    if state.auth.access_from_user(&user).is_none() {
        tracing::info!(sub = %user.sub, "oidc login refused: no matching role");
        return login_error(&mut auth_session, LoginFail::Forbidden).await;
    }

    if let Err(error) = auth_session
        .session
        .remove::<LoginPending>(LOGIN_PENDING_KEY)
        .await
    {
        tracing::error!(%error, "failed to clear oidc login state");
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }

    let ttl = (user.exp - Timestamp::now().as_second()).max(1);
    auth_session
        .session
        .set_expiry(Some(Expiry::OnInactivity(Duration::seconds(ttl))));

    auth_session.backend.remember(user.clone());

    if let Err(error) = auth_session.login(&user).await {
        tracing::error!(%error, "failed to establish session");
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }

    tracing::info!(sub = %user.sub, "oidc login succeeded");
    Redirect::to("/").into_response()
}

async fn logout(mut auth_session: AuthSession) -> impl IntoResponse {
    if let Err(error) = auth_session.logout().await {
        tracing::error!(%error, "failed to log out");
        return StatusCode::INTERNAL_SERVER_ERROR;
    }
    StatusCode::NO_CONTENT
}

enum LoginFail {
    Auth,
    Forbidden,
}

async fn login_error(auth_session: &mut AuthSession, fail: LoginFail) -> Response {
    if let Err(error) = auth_session.logout().await {
        tracing::error!(%error, "failed to clear session after login error");
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }
    let location = match fail {
        LoginFail::Auth => "/login?error=auth",
        LoginFail::Forbidden => "/login?error=forbidden",
    };
    Redirect::to(location).into_response()
}

#[cfg(test)]
async fn impersonate(
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

#[derive(Debug, Serialize, Deserialize)]
struct LoginPending {
    state: String,
    nonce: String,
    pkce_verifier: String,
}

fn session_layer(secure: bool, key: Key) -> SessionLayer {
    SessionManagerLayer::new(MemoryStore::default())
        .with_name(SESSION_COOKIE)
        .with_http_only(true)
        // Lax so the IdP redirect back to /auth/callback still sends the session.
        .with_same_site(SameSite::Lax)
        .with_secure(secure)
        .with_path("/")
        .with_signed(key)
}

fn signing_key(configured: Option<&str>) -> anyhow::Result<Key> {
    let Some(secret) = configured.map(str::trim).filter(|value| !value.is_empty()) else {
        tracing::warn!(
            "no session key configured; sessions will not survive a restart. \
             set KLENS_SESSION_KEY or auth.session_key"
        );
        return Ok(Key::generate());
    };

    let bytes = match base64::engine::general_purpose::STANDARD.decode(secret) {
        Ok(decoded) if decoded.len() >= MIN_SESSION_KEY_BYTES => decoded,
        _ => secret.as_bytes().to_vec(),
    };

    anyhow::ensure!(
        bytes.len() >= MIN_SESSION_KEY_BYTES,
        "session key must decode to at least {MIN_SESSION_KEY_BYTES} bytes, got {}",
        bytes.len()
    );

    Ok(Key::derive_from(&bytes))
}

#[cfg(test)]
mod tests {
    use axum::body::{Body, to_bytes};
    use axum::http::StatusCode;
    use axum::http::{Request, header};
    use tower::ServiceExt;

    use super::oidc::FakeOidc;
    use super::*;
    use crate::kafka::{FakeCluster, SessionSet};

    impl AuthState {
        pub(crate) fn enabled_for_tests() -> Self {
            Self::enabled_for_tests_with(FakeOidc::default(), AccessPolicy::open())
        }

        pub(crate) fn enabled_for_tests_with(flow: FakeOidc, policy: AccessPolicy) -> Self {
            Self {
                backend: AuthBackend::enabled(Arc::new(flow)),
                policy,
                session_layer: session_layer(false, Key::generate()),
            }
        }
    }

    fn app(auth: AuthState) -> axum::Router {
        crate::app::router(AppState::with_auth(
            Arc::new(SessionSet::from_sessions(vec![FakeCluster::local()])),
            auth,
        ))
    }

    async fn send(app: axum::Router, request: Request<Body>) -> axum::http::Response<Body> {
        app.oneshot(request).await.unwrap()
    }

    async fn send_ui(app: axum::Router, request: Request<Body>) -> axum::http::Response<Body> {
        tower::ServiceBuilder::new()
            .map_request(crate::app::apply_ui_prefix)
            .service(app)
            .oneshot(request)
            .await
            .unwrap()
    }

    fn api_request() -> Request<Body> {
        Request::builder()
            .uri("/clusters")
            .body(Body::empty())
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

    async fn impersonate_cookie(router: &axum::Router, user: &SessionUser) -> String {
        let response = send(
            router.clone(),
            Request::builder()
                .method("POST")
                .uri("/auth/impersonate")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(serde_json::to_vec(user).expect("user json")))
                .unwrap(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        let cookies = cookie_header(&response);
        assert!(
            cookies.contains(SESSION_COOKIE),
            "impersonate must set {SESSION_COOKIE}: {cookies}"
        );
        cookies
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
    async fn api_is_open_when_oidc_disabled() {
        let response = send(app(AuthState::disabled()), api_request()).await;
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn api_unauthorized_without_session() {
        let router = app(AuthState::enabled_for_tests());
        let response = send(router.clone(), api_request()).await;
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        let prefixed = send_ui(
            router,
            Request::builder()
                .uri("/api/clusters")
                .body(Body::empty())
                .unwrap(),
        )
        .await;
        assert_eq!(prefixed.status(), StatusCode::UNAUTHORIZED);

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["error"], "unauthorized");
    }

    #[tokio::test]
    async fn api_allows_valid_session() {
        let router = app(AuthState::enabled_for_tests());
        let user = SessionUser::new(
            "user-1",
            Some("user@example.com".into()),
            Some("Test User".into()),
            vec![],
            Timestamp::now().as_second() + 3600,
        );
        let cookie = impersonate_cookie(&router, &user).await;

        let mut request = api_request();
        request
            .headers_mut()
            .insert(header::COOKIE, cookie.parse().unwrap());

        let response = send(router, request).await;
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
    async fn the_ui_prefix_strips_to_the_session_route() {
        let response = send_ui(
            app(AuthState::disabled()),
            Request::builder()
                .uri("/api/auth/me")
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
        assert!(cookie_header(&response).contains(SESSION_COOKIE));
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

        let mut request = api_request();
        request
            .headers_mut()
            .insert(header::COOKIE, session_cookie.parse().unwrap());
        let api = send(router, request).await;
        assert_eq!(api.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn callback_rejects_a_bad_authorization_code() {
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
            router,
            Request::builder()
                .uri(format!("/auth/callback?code=wrong&state={state}"))
                .header(header::COOKIE, cookies)
                .body(Body::empty())
                .unwrap(),
        )
        .await;

        assert!(callback.status().is_redirection());
        assert_eq!(
            callback.headers().get(header::LOCATION).unwrap(),
            "/login?error=auth"
        );
    }

    fn bound_admins() -> AccessPolicy {
        use crate::config::PrivilegeName::{Acls, Configs, Records, SchemaText};
        bound(
            "admin",
            &[Records, Configs, SchemaText, Acls],
            "klens-admins",
        )
    }

    fn bound_viewers() -> AccessPolicy {
        bound("viewer", &[], "klens-viewers")
    }

    fn bound(role: &str, privileges: &[crate::config::PrivilegeName], group: &str) -> AccessPolicy {
        AccessPolicy::from_roles(Some(&crate::config::RolesConfig {
            claim: "groups".into(),
            definitions: [(role.to_owned(), privileges.to_vec())]
                .into_iter()
                .collect(),
            bindings: vec![crate::config::RoleBinding {
                groups: vec![group.to_owned()],
                role: role.to_owned(),
                clusters: None,
            }],
        }))
    }

    async fn login_and_callback(
        auth: AuthState,
        code: &str,
    ) -> (axum::Router, axum::http::Response<Body>) {
        let router = app(auth);
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
                .uri(format!("/auth/callback?code={code}&state={state}"))
                .header(header::COOKIE, cookies)
                .body(Body::empty())
                .unwrap(),
        )
        .await;
        (router, callback)
    }

    #[tokio::test]
    async fn callback_refuses_an_unmatched_group() {
        let (router, callback) = login_and_callback(
            AuthState::enabled_for_tests_with(
                FakeOidc {
                    groups: vec!["other".into()],
                },
                bound_admins(),
            ),
            "test-code",
        )
        .await;

        assert!(callback.status().is_redirection());
        assert_eq!(
            callback.headers().get(header::LOCATION).unwrap(),
            "/login?error=forbidden"
        );

        let mut request = api_request();
        if let Ok(cookies) = cookie_header(&callback).parse() {
            request.headers_mut().insert(header::COOKIE, cookies);
        }
        let api = send(router, request).await;
        assert_eq!(api.status(), StatusCode::UNAUTHORIZED);
    }

    #[test]
    fn the_same_session_key_derives_the_same_signing_key_across_restarts() {
        let encoded = base64::engine::general_purpose::STANDARD.encode([7u8; 32]);

        assert_eq!(
            signing_key(Some(&encoded)).expect("key").signing(),
            signing_key(Some(&encoded)).expect("key").signing()
        );
    }

    #[test]
    fn a_passphrase_that_happens_to_be_base64_is_taken_as_written() {
        let passphrase = "p".repeat(MIN_SESSION_KEY_BYTES);
        let key = signing_key(Some(&passphrase)).expect("passphrase");

        assert_ne!(
            key.signing(),
            signing_key(Some(
                &base64::engine::general_purpose::STANDARD.encode([7u8; 32])
            ))
            .expect("key")
            .signing()
        );
    }

    #[test]
    fn a_short_session_key_is_rejected_rather_than_silently_padded() {
        let error = signing_key(Some("too-short")).expect_err("short key");
        assert!(error.to_string().contains("at least"), "{error}");
    }

    #[test]
    fn an_absent_session_key_falls_back_to_a_generated_one() {
        let first = signing_key(None).expect("generated");
        let second = signing_key(Some("   ")).expect("blank counts as absent");
        assert_ne!(first.signing(), second.signing());
    }

    #[tokio::test]
    async fn me_reports_identity_only() {
        let router = app(AuthState::enabled_for_tests_with(
            FakeOidc::default(),
            bound_viewers(),
        ));
        let user = SessionUser::new(
            "user-1",
            Some("user@example.com".into()),
            Some("Test User".into()),
            vec!["klens-viewers".into()],
            Timestamp::now().as_second() + 3600,
        );
        let cookie = impersonate_cookie(&router, &user).await;

        let response = send(
            router,
            Request::builder()
                .uri("/auth/me")
                .header(header::COOKIE, cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await;

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["enabled"], true);
        assert_eq!(json["user"]["sub"], "user-1");
        assert_eq!(json["user"]["email"], "user@example.com");
        assert!(json["user"]["role"].is_null());
        assert!(json["user"]["clusters"].is_null());
    }

    #[tokio::test]
    async fn whoami_reports_the_bound_roles_per_cluster() {
        let router = app(AuthState::enabled_for_tests_with(
            FakeOidc::default(),
            bound_viewers(),
        ));
        let user = SessionUser::new(
            "user-1",
            None,
            None,
            vec!["klens-viewers".into()],
            Timestamp::now().as_second() + 3600,
        );
        let cookie = impersonate_cookie(&router, &user).await;

        let request = Request::builder()
            .uri("/whoami")
            .header(header::COOKIE, cookie)
            .body(Body::empty())
            .unwrap();

        let response = send(router, request).await;
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let whoami: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(whoami["subject"], "user-1");
        assert_eq!(whoami["clusters"][0]["cluster"], "local");
        assert_eq!(
            whoami["clusters"][0]["roles"],
            serde_json::json!(["viewer"])
        );
        assert_eq!(whoami["clusters"][0]["privileges"], serde_json::json!([]));
    }

    #[tokio::test]
    async fn api_rejects_a_session_without_a_matching_role() {
        let router = app(AuthState::enabled_for_tests_with(
            FakeOidc::default(),
            bound_admins(),
        ));
        let user = SessionUser::new(
            "user-1",
            None,
            None,
            vec![],
            Timestamp::now().as_second() + 3600,
        );
        let cookie = impersonate_cookie(&router, &user).await;

        let mut request = api_request();
        request
            .headers_mut()
            .insert(header::COOKIE, cookie.parse().unwrap());

        let response = send(router, request).await;
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn api_forbids_records_for_a_viewer() {
        let router = app(AuthState::enabled_for_tests_with(
            FakeOidc::default(),
            bound_viewers(),
        ));
        let user = SessionUser::new(
            "user-1",
            None,
            None,
            vec!["klens-viewers".into()],
            Timestamp::now().as_second() + 3600,
        );
        let cookie = impersonate_cookie(&router, &user).await;

        let request = Request::builder()
            .uri("/clusters/local/topics/orders.created/records?limit=1&order=OLDEST")
            .header(header::COOKIE, cookie)
            .body(Body::empty())
            .unwrap();

        let response = send(router, request).await;
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["code"], "FORBIDDEN");
    }
}
