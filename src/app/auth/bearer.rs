use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context as _, bail};
use arc_swap::ArcSwap;
use axum::extract::{Request, State};
use axum::http::{HeaderMap, HeaderValue, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use jiff::Timestamp;
use jsonwebtoken::jwk::{AlgorithmParameters, Jwk, PublicKeyUse};
use jsonwebtoken::{Algorithm, DecodingKey, Validation};
use openidconnect::core::CoreJsonWebKey;
use openidconnect::{JsonWebKeySet, reqwest};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::sync::Mutex;
use tokio::time::Instant;
use url::Url;

use super::access::groups_from_json;
use super::{Holder, SessionGuard};
use crate::app::error::ApiError;
use crate::config;

#[cfg(test)]
mod tests;

const ASYMMETRIC: [Algorithm; 9] = [
    Algorithm::RS256,
    Algorithm::RS384,
    Algorithm::RS512,
    Algorithm::PS256,
    Algorithm::PS384,
    Algorithm::PS512,
    Algorithm::ES256,
    Algorithm::ES384,
    Algorithm::EdDSA,
];

const PROVIDER_CLOCK_AHEAD_SECS: i64 = 30;

const MISS_REFETCH: Duration = Duration::from_secs(30);

const REFRESH: Duration = Duration::from_secs(15 * 60);

const FETCH_TIMEOUT: Duration = Duration::from_secs(10);

const MAX_KEY_SET_BYTES: usize = 64 * 1024;

const METADATA_PATH: &str = "/.well-known/oauth-protected-resource";

pub(crate) struct Bearer {
    keys: Arc<Jwks>,
    validation: Validation,
    max_age: i64,
    clients: Vec<String>,
    groups_claim: String,
    user_claim: String,
    resource: Url,
    metadata: Value,
    challenge: HeaderValue,
    invalid: HeaderValue,
}

pub(crate) struct Caller {
    pub(super) user: String,
    pub(super) client: Option<String>,
    pub(super) groups: Vec<String>,
    pub(super) exp: i64,
}

enum Refusal {
    /// RFC 6750 3.1: a request with no token gets a challenge with no error.
    Missing,
    Invalid(Cow<'static, str>),
}

impl From<&'static str> for Refusal {
    fn from(reason: &'static str) -> Self {
        Self::Invalid(Cow::Borrowed(reason))
    }
}

impl From<jsonwebtoken::errors::Error> for Refusal {
    fn from(error: jsonwebtoken::errors::Error) -> Self {
        Self::Invalid(Cow::Owned(error.to_string()))
    }
}

impl Bearer {
    pub(super) fn new(oidc: &config::Oidc, mcp: &config::Mcp, keys: Jwks) -> anyhow::Result<Self> {
        let resource = mcp
            .resource
            .clone()
            .context("mcp.resource is required with auth")?;
        let mut validation = Validation::new(Algorithm::ES256);
        validation.leeway = PROVIDER_CLOCK_AHEAD_SECS.unsigned_abs();
        validation.validate_exp = false;
        validation.validate_nbf = true;
        validation.set_issuer(&[oidc.issuer.as_str()]);
        validation.set_audience(&mcp.audiences());
        // jsonwebtoken checks the issuer and the audience only when the token
        // names them.
        validation.set_required_spec_claims(&["exp", "iss", "aud", "sub"]);

        let mut metadata = json!({
            "resource": resource.as_str(),
            "authorization_servers": [oidc.issuer.as_str()],
            "bearer_methods_supported": ["header"],
            "resource_name": "klens",
        });
        let mut challenge = format!(
            r#"Bearer resource_metadata="{}{}""#,
            resource.origin().ascii_serialization(),
            metadata_path(&resource)
        );
        if !mcp.token.scopes.is_empty() {
            metadata["scopes_supported"] = json!(mcp.token.scopes);
            challenge += &format!(r#", scope="{}""#, mcp.token.scopes.join(" "));
        }
        let invalid = format!(r#"{challenge}, error="invalid_token""#);

        let keys = Arc::new(keys);
        keys.refresh_every(REFRESH);
        Ok(Self {
            keys,
            validation,
            max_age: i64::try_from(mcp.token.max_age.as_secs()).unwrap_or(i64::MAX),
            clients: mcp.token.clients.clone(),
            groups_claim: mcp
                .token
                .groups_claim
                .clone()
                .unwrap_or_else(|| oidc.groups_claim.clone()),
            user_claim: mcp.token.user_claim.clone(),
            resource,
            metadata,
            challenge: HeaderValue::try_from(challenge)?,
            invalid: HeaderValue::try_from(invalid)?,
        })
    }

    pub(crate) fn resource(&self) -> &Url {
        &self.resource
    }

    /// MCP clients that get no `resource_metadata` fall back to the bare
    /// well-known path.
    pub(crate) fn metadata(&self) -> Router {
        let document = Json(self.metadata.clone());
        let serve = get(move || async move { document });
        let router = Router::new().route(METADATA_PATH, serve.clone());
        match metadata_path(&self.resource) {
            path if path == METADATA_PATH => router,
            path => router.route(&path, serve),
        }
    }

    async fn authenticate(&self, headers: &HeaderMap) -> Result<Caller, Refusal> {
        let mut values = headers.get_all(header::AUTHORIZATION).iter();
        let (Some(value), None) = (values.next(), values.next()) else {
            return Err(Refusal::Missing);
        };
        let token = value
            .to_str()
            .ok()
            .and_then(|value| value.split_once(' '))
            .filter(|(scheme, _)| scheme.eq_ignore_ascii_case("bearer"))
            .map(|(_, token)| token.trim())
            .ok_or(Refusal::Missing)?;
        self.verify(token).await
    }

    async fn verify(&self, token: &str) -> Result<Caller, Refusal> {
        let header = jsonwebtoken::decode_header(token)?;
        if !ASYMMETRIC.contains(&header.alg) {
            return Err("the token is not signed with a public key".into());
        }
        if !header.typ.as_deref().is_none_or(names_an_access_token) {
            return Err("the token is typed as something other than an access token".into());
        }
        if header.crit.is_some() {
            return Err("the token names critical header parameters".into());
        }
        let key = self
            .keys
            .key(header.kid.as_deref())
            .await
            .ok_or("no key the provider publishes matches the token")?;
        if key.alg.is_some_and(|alg| alg != header.alg) {
            return Err("the key is for another algorithm".into());
        }
        // jsonwebtoken refuses a list of algorithms that spans key families,
        // so each token is checked against the one it names.
        let validation = Validation {
            algorithms: vec![header.alg],
            ..self.validation.clone()
        };
        let claims = jsonwebtoken::decode::<Value>(token, &key.key, &validation)?.claims;

        let now = Timestamp::now().as_second();
        let exp = seconds(&claims, "exp").ok_or("the token names no expiry")?;
        let iat = seconds(&claims, "iat").ok_or("the token names no issue time")?;
        if exp <= now {
            return Err("the token has expired".into());
        }
        if iat > now + PROVIDER_CLOCK_AHEAD_SECS {
            return Err("the token was issued in the future".into());
        }
        if exp.saturating_sub(iat) > self.max_age {
            return Err("the token lasts longer than mcp.token.max_age".into());
        }
        let client = ["azp", "client_id"]
            .into_iter()
            .find_map(|name| claims.get(name).and_then(Value::as_str))
            .map(ToOwned::to_owned);
        if !self.clients.is_empty()
            && !client
                .as_ref()
                .is_some_and(|client| self.clients.contains(client))
        {
            return Err("the token is for a client mcp.token.clients does not list".into());
        }
        let sub = claims
            .get("sub")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let user = claims
            .get(&self.user_claim)
            .and_then(Value::as_str)
            .unwrap_or(sub);
        Ok(Caller {
            user: user.to_owned(),
            client,
            groups: groups_from_json(&claims, &self.groups_claim),
            exp,
        })
    }

    fn refuse(&self, refusal: Refusal) -> Response {
        let challenge = match refusal {
            Refusal::Missing => &self.challenge,
            Refusal::Invalid(reason) => {
                tracing::debug!(reason = &*reason, "refused a bearer token");
                &self.invalid
            }
        };
        let mut response = ApiError::Unauthorized.into_response();
        response
            .headers_mut()
            .insert(header::WWW_AUTHENTICATE, challenge.clone());
        response
    }
}

impl Caller {
    pub(super) fn is_live(&self) -> bool {
        Timestamp::now().as_second() < self.exp
    }
}

pub(crate) async fn require_bearer(
    State((bearer, capped)): State<(Arc<Bearer>, SessionGuard)>,
    mut request: Request,
    next: Next,
) -> Response {
    let caller = match bearer.authenticate(request.headers()).await {
        Ok(caller) => caller,
        Err(refusal) => return bearer.refuse(refusal),
    };
    request.headers_mut().remove(header::AUTHORIZATION);
    let span = tracing::Span::current();
    span.record("user", caller.user.as_str());
    if let Some(client) = &caller.client {
        span.record("client", client.as_str());
    }
    let guard = SessionGuard {
        holder: Holder::Token(Arc::new(caller)),
        ..capped
    };
    // A 401 would send the client back through the provider for a token
    // with the same groups.
    let Some(access) = guard.narrowed() else {
        tracing::info!("bearer token refused: no matching role");
        return ApiError::NoRole.into_response();
    };
    request.extensions_mut().insert(access);
    request.extensions_mut().insert(guard);
    next.run(request).await
}

/// RFC 8725 3.11: a token typed as anything else, such as a logout token,
/// was never meant for a resource server.
fn names_an_access_token(typ: &str) -> bool {
    let typ = typ
        .get(.."application/".len())
        .filter(|prefix| prefix.eq_ignore_ascii_case("application/"))
        .map_or(typ, |prefix| &typ[prefix.len()..]);
    typ.eq_ignore_ascii_case("jwt") || typ.eq_ignore_ascii_case("at+jwt")
}

/// RFC 9728 3.1: the well-known segment goes between the host and the path.
fn metadata_path(resource: &Url) -> String {
    match resource.path() {
        "/" => METADATA_PATH.to_owned(),
        path => format!("{METADATA_PATH}{path}"),
    }
}

fn seconds(claims: &Value, name: &str) -> Option<i64> {
    claims.get(name)?.as_f64().map(|seconds| seconds as i64)
}

#[derive(Clone)]
struct Key {
    key: DecodingKey,
    alg: Option<Algorithm>,
}

pub(crate) struct Jwks {
    http: reqwest::Client,
    uri: Url,
    keys: ArcSwap<HashMap<String, Key>>,
    missed_at: Arc<Mutex<Option<Instant>>>,
}

#[derive(Deserialize)]
struct KeySet {
    keys: Vec<Value>,
}

impl Jwks {
    pub(super) fn discovered(
        http: reqwest::Client,
        uri: Url,
        set: &JsonWebKeySet<CoreJsonWebKey>,
    ) -> Self {
        let keys = set
            .keys()
            .iter()
            .filter_map(|key| serde_json::to_value(key).ok())
            .collect();
        Self {
            http,
            uri,
            keys: ArcSwap::from_pointee(usable(keys)),
            missed_at: Arc::default(),
        }
    }

    async fn key(self: &Arc<Self>, kid: Option<&str>) -> Option<Key> {
        let Some(kid) = kid else {
            let keys = self.keys.load();
            return match keys.len() {
                1 => keys.values().next().cloned(),
                _ => None,
            };
        };
        if let Some(key) = self.cached(kid) {
            return Some(key);
        }
        let mut missed_at = Arc::clone(&self.missed_at).lock_owned().await;
        if let Some(key) = self.cached(kid) {
            return Some(key);
        }
        if missed_at.is_some_and(|at| at.elapsed() < MISS_REFETCH) {
            return None;
        }
        *missed_at = Some(Instant::now());
        let keys = Arc::clone(self);
        // The fetch keeps the lock to its end even when this caller hangs up,
        // so the callers waiting on the lock never find the window spent and
        // the new key missing.
        tokio::spawn(async move {
            keys.add_fetched().await;
            drop(missed_at);
        })
        .await
        .ok()?;
        self.cached(kid)
    }

    fn cached(&self, kid: &str) -> Option<Key> {
        self.keys.load().get(kid).cloned()
    }

    async fn add_fetched(&self) {
        match self.fetch().await {
            Ok(fetched) => {
                self.keys.rcu(|keys| {
                    let mut keys = HashMap::clone(keys);
                    keys.extend(fetched.clone());
                    keys
                });
            }
            Err(error) => tracing::warn!(
                %error,
                uri = self.uri.as_str(),
                "failed to fetch the signing keys of the oidc provider"
            ),
        }
    }

    fn refresh_every(self: &Arc<Self>, period: Duration) {
        let keys = Arc::downgrade(self);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(period).await;
                let Some(keys) = keys.upgrade() else {
                    return;
                };
                keys.refresh().await;
            }
        });
    }

    async fn refresh(&self) {
        match self.fetch().await {
            Ok(fetched) => self.keys.store(Arc::new(fetched)),
            Err(error) => tracing::warn!(
                %error,
                uri = self.uri.as_str(),
                "failed to refresh the signing keys of the oidc provider"
            ),
        }
    }

    async fn fetch(&self) -> anyhow::Result<HashMap<String, Key>> {
        let fetch = async {
            let mut response = self
                .http
                .get(self.uri.clone())
                .header(header::ACCEPT, "application/json")
                .send()
                .await?
                .error_for_status()?;
            let mut body = Vec::new();
            while let Some(chunk) = response.chunk().await? {
                if body.len() + chunk.len() > MAX_KEY_SET_BYTES {
                    bail!("the key set is larger than {MAX_KEY_SET_BYTES} bytes");
                }
                body.extend_from_slice(&chunk);
            }
            let set: KeySet = serde_json::from_slice(&body)?;
            Ok(usable(set.keys))
        };
        tokio::time::timeout(FETCH_TIMEOUT, fetch)
            .await
            .context("the key set did not arrive in time")?
    }
}

fn usable(keys: Vec<Value>) -> HashMap<String, Key> {
    keys.into_iter()
        .filter_map(|key| serde_json::from_value::<Jwk>(key).ok())
        .filter(|jwk| {
            jwk.common
                .public_key_use
                .as_ref()
                .is_none_or(|used| *used == PublicKeyUse::Signature)
                && !matches!(jwk.algorithm, AlgorithmParameters::OctetKey(_))
        })
        .filter_map(|jwk| {
            let alg = jwk
                .common
                .key_algorithm
                .map(Algorithm::try_from)
                .transpose()
                .ok()?;
            let key = DecodingKey::from_jwk(&jwk).ok()?;
            Some((jwk.common.key_id?, Key { key, alg }))
        })
        .collect()
}
