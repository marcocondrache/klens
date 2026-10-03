use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::{IntoResponse, Redirect, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use axum_extra::extract::Query;
use axum_login::AuthManagerLayerBuilder;
use jiff::Timestamp;
use openidconnect::{CsrfToken, Nonce, PkceCodeChallenge};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tower_sessions::cookie::time::Duration;
use tower_sessions::cookie::{Key, SameSite};
use tower_sessions::service::SignedCookie;
use tower_sessions::{Expiry, SessionManagerLayer};

use crate::AppState;
use crate::config::{self, KeyMaterial};

pub(crate) mod access;
mod backend;
mod oidc;
mod store;
#[cfg(test)]
pub(crate) mod testing;

use access::{AccessPolicy, EffectiveAccess, Identity};
use backend::{AuthBackend, OidcCredentials};
use oidc::{Oidc, OidcFlow};
use store::ExpiringStore;

const LOGIN_PENDING_KEY: &str = "klens.login_pending";

const SESSION_COOKIE: &str = "klens_session";

const SESSION_COOKIE_KEY_PREFIX: &str = "klens-session-v1";

type AuthSession = axum_login::AuthSession<AuthBackend>;
type SessionLayer = SessionManagerLayer<ExpiringStore, SignedCookie>;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct SessionUser {
    pub sub: String,
    pub email: Option<String>,
    pub name: Option<String>,
    pub exp: i64,
    #[serde(default)]
    pub groups: Vec<String>,
    #[serde(skip)]
    auth_hash: [u8; 32],
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
            auth_hash: [0; 32],
        };
        user.refresh_auth_hash();
        user
    }

    fn refresh_auth_hash(&mut self) {
        self.auth_hash = Sha256::digest(format!(
            "{SESSION_COOKIE_KEY_PREFIX}|{}|{}|{}",
            self.sub,
            self.exp,
            self.groups.join("\0")
        ))
        .into();
    }
}

#[derive(Clone)]
pub struct AuthState {
    backend: AuthBackend,
    policy: Arc<AccessPolicy>,
    session_layer: SessionLayer,
    login_timeout: Duration,
}

impl AuthState {
    pub fn disabled() -> Self {
        Self {
            backend: AuthBackend::disabled(),
            policy: Arc::new(AccessPolicy::disabled()),
            session_layer: session_layer(false, Key::generate()),
            login_timeout: Duration::ZERO,
        }
    }

    pub async fn from_config(auth: Option<&config::Auth>) -> anyhow::Result<Self> {
        let Some(auth) = auth else {
            return Ok(Self::disabled());
        };
        let flow = Oidc::discover(&auth.oidc, auth.session.max_age).await?;
        Ok(Self::enabled(Arc::new(flow), auth))
    }

    fn enabled(flow: Arc<dyn OidcFlow>, auth: &config::Auth) -> Self {
        Self {
            backend: AuthBackend::enabled(flow),
            policy: Arc::new(AccessPolicy::from_roles(auth.roles.as_ref())),
            session_layer: session_layer(
                auth.oidc.redirect_uri.url().scheme() == "https",
                signing_key(auth.session.key.as_ref()),
            ),
            login_timeout: Duration::try_from(auth.session.login_timeout).unwrap_or(Duration::MAX),
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
    ) -> axum_login::AuthManagerLayer<AuthBackend, ExpiringStore, SignedCookie> {
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
        self.auth
            .backend
            .with_live_user(subject, |user| self.auth.access_from_user(user))
            .flatten()
    }

    pub fn subject(&self) -> Option<&str> {
        self.subject.as_deref()
    }
}

pub fn router() -> Router<AppState> {
    let router = Router::new()
        .route("/me", get(me))
        .route("/login", get(login))
        .route("/callback", get(callback))
        .route("/logout", post(logout));

    #[cfg(test)]
    let router = router.route("/impersonate", post(testing::impersonate));

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
    let user = auth_session.user.and_then(|user| {
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
        .set_expiry(Some(Expiry::OnInactivity(state.auth.login_timeout)));

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
    Redirect::to("/login?from=callback").into_response()
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

#[derive(Debug, Serialize, Deserialize)]
struct LoginPending {
    state: String,
    nonce: String,
    pkce_verifier: String,
}

fn session_layer(secure: bool, key: Key) -> SessionLayer {
    SessionManagerLayer::new(ExpiringStore::default())
        .with_name(SESSION_COOKIE)
        .with_http_only(true)
        // Lax so the IdP redirect back to /api/auth/callback still sends the session.
        .with_same_site(SameSite::Lax)
        .with_secure(secure)
        .with_path("/")
        .with_signed(key)
}

fn signing_key(configured: Option<&KeyMaterial>) -> Key {
    let Some(secret) = configured else {
        tracing::warn!(
            "no session key configured; sessions will not survive a restart. \
             set auth.session.key"
        );
        return Key::generate();
    };

    Key::derive_from(secret.as_bytes())
}

#[cfg(test)]
mod tests {
    use axum::http::StatusCode;

    use super::testing::{Browser, FakeOidc};
    use super::*;
    use crate::testing::yaml;

    #[test]
    fn the_session_hash_follows_every_claim_it_binds() {
        use axum_login::AuthUser as _;

        let user = |sub: &str, groups: &[&str], exp| {
            SessionUser::new(
                sub,
                None,
                None,
                groups.iter().map(|g| g.to_string()).collect(),
                exp,
            )
        };
        let base = user("alice", &["ops"], 100);

        assert_eq!(
            base.session_auth_hash(),
            user("alice", &["ops"], 100).session_auth_hash()
        );
        for changed in [
            user("bob", &["ops"], 100),
            user("alice", &["ops", "admin"], 100),
            user("alice", &["ops"], 200),
        ] {
            assert_ne!(base.session_auth_hash(), changed.session_auth_hash());
        }
    }

    fn user(groups: &[&str]) -> SessionUser {
        SessionUser::new(
            "user-1",
            Some("user@example.com".into()),
            Some("Test User".into()),
            groups.iter().map(|group| (*group).to_owned()).collect(),
            Timestamp::now().as_second() + 3600,
        )
    }

    fn bound(role_name: &str, privileges: &[config::Privilege], group: &str) -> AccessPolicy {
        let role = config::Role {
            privileges: privileges.to_vec(),
            bindings: vec![config::Binding {
                groups: vec![group.to_owned()],
                clusters: None,
            }],
        };
        AccessPolicy::from_roles(Some(&[(role_name.to_owned(), role)].into_iter().collect()))
    }

    fn bound_admins() -> AccessPolicy {
        use config::Privilege::{Acls, Configs, Records, SchemaText};
        bound(
            "admin",
            &[Records, Configs, SchemaText, Acls],
            "klens-admins",
        )
    }

    fn bound_viewers() -> AccessPolicy {
        bound("viewer", &[], "klens-viewers")
    }

    #[tokio::test]
    async fn health_and_ready_stay_public_when_oidc_enabled() {
        let mut browser = Browser::new(AuthState::enabled_for_tests());

        assert_eq!(browser.get("/health").await.status, StatusCode::NO_CONTENT);
        assert_eq!(
            browser.get("/ready").await.status,
            StatusCode::SERVICE_UNAVAILABLE
        );
    }

    #[tokio::test]
    async fn api_is_open_when_oidc_disabled() {
        let mut browser = Browser::new(AuthState::disabled());

        assert_eq!(browser.get("/api/clusters").await.status, StatusCode::OK);
    }

    #[tokio::test]
    async fn api_unauthorized_without_session() {
        let mut browser = Browser::new(AuthState::enabled_for_tests());

        let page = browser.get("/api/clusters").await;

        assert_eq!(page.status, StatusCode::UNAUTHORIZED);
        assert_eq!(page.json()["error"], "unauthorized");
    }

    #[tokio::test]
    async fn api_allows_valid_session() {
        let mut browser = Browser::new(AuthState::enabled_for_tests());
        browser.impersonate(&user(&[])).await;

        assert_eq!(browser.get("/api/clusters").await.status, StatusCode::OK);
    }

    #[tokio::test]
    async fn me_reports_disabled_auth() {
        let mut browser = Browser::new(AuthState::disabled());

        let me = browser.get("/api/auth/me").await.json();

        assert_eq!(me["enabled"], false);
        assert!(me["user"].is_null());
    }

    #[tokio::test]
    async fn me_reports_enabled_auth_without_user() {
        let mut browser = Browser::new(AuthState::enabled_for_tests());

        let me = browser.get("/api/auth/me").await.json();

        assert_eq!(me["enabled"], true);
        assert!(me["user"].is_null());
    }

    #[tokio::test]
    async fn login_is_not_found_when_disabled() {
        let mut browser = Browser::new(AuthState::disabled());

        assert_eq!(
            browser.get("/api/auth/login").await.status,
            StatusCode::NOT_FOUND
        );
    }

    #[tokio::test]
    async fn login_redirects_to_identity_provider() {
        let mut browser = Browser::new(AuthState::enabled_for_tests());

        let login = browser.get("/api/auth/login").await;

        assert!(login.status.is_redirection());
        assert!(
            login
                .location()
                .starts_with("https://idp.example/authorize")
        );
        assert!(login.set_cookie().starts_with(SESSION_COOKIE));
    }

    async fn login_cookie(redirect_uri: &str) -> String {
        let auth: config::Auth = yaml(&format!(
            "
            oidc:
              issuer: https://idp.example
              client_id: klens
              client_secret: {{value: secret}}
              redirect_uri: {redirect_uri}
            session:
              login_timeout: 90s
            "
        ));
        let mut browser = Browser::new(AuthState::enabled(Arc::new(FakeOidc::default()), &auth));

        browser.get("/api/auth/login").await.set_cookie().to_owned()
    }

    #[tokio::test]
    async fn the_session_cookie_follows_the_auth_config() {
        let https = login_cookie("https://klens.example/api/auth/callback").await;
        let http = login_cookie("http://localhost:8080/api/auth/callback").await;

        assert!(https.contains("; Secure"), "{https}");
        assert!(!http.contains("; Secure"), "{http}");
        assert!(https.contains("Max-Age=90"), "{https}");
    }

    #[tokio::test]
    async fn callback_rejects_missing_and_mismatched_state() {
        let mut browser = Browser::new(AuthState::enabled_for_tests());

        let missing = browser
            .get("/api/auth/callback?code=test-code&state=nope")
            .await;
        assert_eq!(missing.location(), "/login?error=auth");

        browser.get("/api/auth/login").await;
        let mismatched = browser
            .get("/api/auth/callback?code=test-code&state=wrong")
            .await;
        assert_eq!(mismatched.location(), "/login?error=auth");
    }

    #[tokio::test]
    async fn callback_sets_session_when_state_matches() {
        let mut browser = Browser::new(AuthState::enabled_for_tests());

        let callback = browser.log_in("test-code").await;

        assert_eq!(callback.location(), "/login?from=callback");
        assert!(callback.set_cookie().starts_with(SESSION_COOKIE));
        assert_eq!(browser.get("/api/clusters").await.status, StatusCode::OK);
    }

    #[tokio::test]
    async fn callback_rejects_a_bad_authorization_code() {
        let mut browser = Browser::new(AuthState::enabled_for_tests());

        let callback = browser.log_in("wrong").await;

        assert_eq!(callback.location(), "/login?error=auth");
    }

    #[tokio::test]
    async fn callback_refuses_an_unmatched_group() {
        let mut browser = Browser::new(AuthState::enabled_for_tests_with(
            FakeOidc {
                groups: vec!["other".into()],
            },
            bound_admins(),
        ));

        let callback = browser.log_in("test-code").await;

        assert_eq!(callback.location(), "/login?error=forbidden");
        assert_eq!(
            browser.get("/api/clusters").await.status,
            StatusCode::UNAUTHORIZED
        );
    }

    fn key(text: &str) -> KeyMaterial {
        yaml(&format!("{{value: '{text}'}}"))
    }

    #[test]
    fn a_session_key_derives_the_signing_key_as_written() {
        let text = "0123456789abcdef0123456789abcdef";

        assert_eq!(
            signing_key(Some(&key(text))).signing(),
            Key::derive_from(text.as_bytes()).signing()
        );
        assert_ne!(
            signing_key(Some(&key(text))).signing(),
            signing_key(Some(&key(&format!("{text} ")))).signing()
        );
    }

    #[tokio::test]
    async fn me_reports_identity_only() {
        let mut browser = Browser::new(AuthState::enabled_for_tests_with(
            FakeOidc::default(),
            bound_viewers(),
        ));
        browser.impersonate(&user(&["klens-viewers"])).await;

        let me = browser.get("/api/auth/me").await.json();

        assert_eq!(me["enabled"], true);
        assert_eq!(me["user"]["sub"], "user-1");
        assert_eq!(me["user"]["email"], "user@example.com");
        assert!(me["user"]["role"].is_null());
        assert!(me["user"]["clusters"].is_null());
    }

    #[tokio::test]
    async fn whoami_reports_the_bound_roles_per_cluster() {
        let mut browser = Browser::new(AuthState::enabled_for_tests_with(
            FakeOidc::default(),
            bound_viewers(),
        ));
        browser.impersonate(&user(&["klens-viewers"])).await;

        let whoami = browser.get("/api/whoami").await.json();

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
        let mut browser = Browser::new(AuthState::enabled_for_tests_with(
            FakeOidc::default(),
            bound_admins(),
        ));
        browser.impersonate(&user(&[])).await;

        assert_eq!(
            browser.get("/api/clusters").await.status,
            StatusCode::UNAUTHORIZED
        );
    }
}
