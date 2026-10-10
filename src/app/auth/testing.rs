use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::anyhow;
use async_trait::async_trait;
use axum::body::{Body, Bytes, to_bytes};
use axum::extract::State;
use axum::http::{HeaderMap, HeaderValue, Request, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::{Json, Router};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use jiff::Timestamp;
use jsonwebtoken::jwk::{Jwk, PublicKeyUse};
use jsonwebtoken::{Algorithm, EncodingKey, Header};
use openidconnect::{CsrfToken, Nonce, PkceCodeChallenge, PkceCodeVerifier};
use serde_json::{Value, json};
use tower::ServiceExt as _;
use tower_sessions::cookie::Key;
use tower_sessions::cookie::time::Duration;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use super::access::{AccessPolicy, ClusterScope, EffectiveAccess, Grant, PrivilegeSet};
use super::backend::AuthBackend;
use super::bearer::Caller;
use super::oidc::OidcFlow;
use super::{AuthSession, AuthState, Holder, SessionGuard, SessionUser, session_layer};
use crate::AppState;
use crate::app::Limits;
use crate::config::{self, Config, Mcp, Tuning};
use crate::kafka::Clusters;
use crate::testing::{FakeCluster, yaml};

pub const RESOURCE: &str = "https://klens.example.com/mcp";

pub const RESOURCE_HOST: &str = "klens.example.com";

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
            bearer: None,
        }
    }
}

impl SessionGuard {
    pub fn open() -> Self {
        Self {
            auth: AuthState::disabled(),
            holder: Holder::Nobody,
            ceiling: None,
        }
    }

    pub fn expired() -> Self {
        Self {
            auth: AuthState::enabled_for_tests(),
            holder: Holder::Session("gone".to_owned()),
            ceiling: None,
        }
    }

    pub fn token(user: &str, exp: i64) -> Self {
        Self {
            auth: AuthState::enabled_for_tests(),
            holder: Holder::Token(Arc::new(Caller {
                user: user.to_owned(),
                client: Some("claude-code".to_owned()),
                groups: Vec::new(),
                exp,
            })),
            ceiling: None,
        }
    }
}

pub struct Signer {
    pub kid: &'static str,
    alg: Algorithm,
    key: EncodingKey,
}

impl Signer {
    pub fn a() -> Self {
        Self::ec("a", include_str!("testing/es256-a.pem"))
    }

    pub fn b() -> Self {
        Self::ec("b", include_str!("testing/es256-b.pem"))
    }

    pub fn ec(kid: &'static str, pem: &str) -> Self {
        Self {
            kid,
            alg: Algorithm::ES256,
            key: EncodingKey::from_ec_der(&der(pem)),
        }
    }

    pub fn rsa() -> Self {
        Self {
            kid: "rsa",
            alg: Algorithm::RS256,
            key: EncodingKey::from_rsa_der(&der(include_str!("testing/rs256.pem"))),
        }
    }

    pub fn ed25519() -> Self {
        Self {
            kid: "ed25519",
            alg: Algorithm::EdDSA,
            key: EncodingKey::from_ed_der(&der(include_str!("testing/ed25519.pem"))),
        }
    }

    pub fn jwk(&self) -> Value {
        let mut jwk = Jwk::from_encoding_key(&self.key, self.alg).expect("a public jwk");
        jwk.common.key_id = Some(self.kid.to_owned());
        jwk.common.public_key_use = Some(PublicKeyUse::Signature);
        serde_json::to_value(jwk).expect("jwk json")
    }

    pub fn sign(&self, claims: &Value) -> String {
        self.sign_with(
            Header {
                kid: Some(self.kid.to_owned()),
                ..Header::new(self.alg)
            },
            claims,
        )
    }

    pub fn sign_with(&self, header: Header, claims: &Value) -> String {
        jsonwebtoken::encode(&header, claims, &self.key).expect("a signed token")
    }
}

fn der(pem: &str) -> Vec<u8> {
    let body: String = pem
        .lines()
        .filter(|line| !line.starts_with("-----"))
        .collect();
    STANDARD.decode(body).expect("a pem body")
}

pub fn key_set(signers: &[&Signer]) -> Value {
    let keys: Vec<Value> = signers.iter().map(|signer| signer.jwk()).collect();
    json!({ "keys": keys })
}

pub struct Idp {
    server: MockServer,
}

impl Idp {
    pub async fn start(boot: &[&Signer], later: &[&Signer]) -> Self {
        Self::serving(
            key_set(boot),
            ResponseTemplate::new(200).set_body_json(key_set(later)),
        )
        .await
    }

    pub async fn serving(boot: Value, later: ResponseTemplate) -> Self {
        let server = MockServer::start().await;
        let issuer = server.uri();
        Mock::given(method("GET"))
            .and(path("/.well-known/openid-configuration"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "issuer": issuer,
                "authorization_endpoint": format!("{issuer}/authorize"),
                "token_endpoint": format!("{issuer}/token"),
                "jwks_uri": format!("{issuer}/jwks"),
                "response_types_supported": ["code"],
                "subject_types_supported": ["public"],
                "id_token_signing_alg_values_supported": ["ES256"],
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/jwks"))
            .respond_with(ResponseTemplate::new(200).set_body_json(boot))
            .up_to_n_times(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/jwks"))
            .respond_with(later)
            .mount(&server)
            .await;
        Self { server }
    }

    pub fn issuer(&self) -> String {
        self.server.uri()
    }

    pub async fn key_fetches(&self) -> usize {
        self.server
            .received_requests()
            .await
            .expect("recorded requests")
            .iter()
            .filter(|request| request.url.path() == "/jwks")
            .count()
    }

    pub async fn auth(&self, mcp: &Mcp) -> AuthState {
        let auth: config::Auth = yaml(&format!(
            "
            oidc:
              issuer: {}
              client_id: klens
              client_secret: {{value: secret}}
              redirect_uri: http://localhost:8080/api/auth/callback
            roles:
              operator:
                privileges: [records, topic_configs, acls]
                bindings: [{{groups: [ops]}}]
            ",
            self.issuer()
        ));
        AuthState::from_config(Some(&auth), Some(mcp))
            .await
            .expect("discovery")
    }

    pub fn claims(&self) -> Value {
        let now = Timestamp::now().as_second();
        json!({
            "iss": self.issuer(),
            "sub": "user-1",
            "aud": RESOURCE,
            "azp": "claude-code",
            "iat": now,
            "exp": now + 300,
            "groups": ["ops"],
        })
    }
}

pub fn mcp(extra: &str) -> Mcp {
    yaml(&format!("resource: {RESOURCE}\n{extra}"))
}

pub fn bearing(mut request: Request<Body>, token: &str) -> Request<Body> {
    let headers = request.headers_mut();
    headers.insert(header::HOST, HeaderValue::from_static(RESOURCE_HOST));
    headers.insert(
        header::AUTHORIZATION,
        format!("Bearer {token}")
            .parse()
            .expect("an authorization header"),
    );
    request
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

pub fn access(grants: impl IntoIterator<Item = Grant>) -> EffectiveAccess {
    EffectiveAccess::Granted(grants.into_iter().collect())
}

pub fn admin() -> Grant {
    role("admin", PrivilegeSet::ALL)
}

pub fn viewer() -> Grant {
    role("viewer", PrivilegeSet::NONE)
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
        Self::serving(auth, None)
    }

    pub fn serving(auth: AuthState, mcp: Option<&Mcp>) -> Self {
        let state = AppState::new(
            Clusters::from_sessions(vec![FakeCluster::local()]),
            auth,
            Limits::new(&Tuning::default()),
        );
        Self {
            app: crate::app::router(state, &Config::default().allowed_hosts, mcp),
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

    pub async fn send(&mut self, mut request: Request<Body>) -> Page {
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
