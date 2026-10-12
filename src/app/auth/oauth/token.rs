use std::sync::Arc;
use std::time::Duration;

use axum::Json;
use axum::extract::State;
use axum::http::header;
use axum::response::{IntoResponse, Response};
use axum_extra::extract::{Form, FormRejection};
use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use jiff::Timestamp;
use moka::sync::Cache;
use openidconnect::{PkceCodeChallenge, PkceCodeVerifier};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use url::Url;

use super::OAuthError;
use crate::AppState;
use crate::app::auth::bearer::Caller;
use crate::app::error::ApiError;

const CODE_PREFIX: &str = "klens_code_";
pub(in crate::app::auth) const TOKEN_PREFIX: &str = "klens_mcp_";

/// A client trades its code as soon as the browser comes back.
const CODE_TTL: Duration = Duration::from_secs(60);

/// A code used twice revokes the token of its first use, for as long as this
/// remembers that use.
const REDEEMED_TTL: Duration = Duration::from_secs(10 * 60);

const MAX_CODES: u64 = 10_000;
const MAX_TOKENS: u64 = 100_000;

type Digest32 = [u8; 32];

/// What the person approved, until the client trades the code for it.
pub(super) struct Approval {
    pub client_id: String,
    pub client: Option<String>,
    pub redirect_uri: Url,
    pub challenge: String,
    pub user: String,
    pub groups: Vec<String>,
    pub exp: i64,
}

/// Codes and tokens live in memory, as sessions do, so a restart signs every
/// agent out.
pub(super) struct Grants {
    codes: Cache<Digest32, Arc<Approval>>,
    redeemed: Cache<Digest32, Digest32>,
    tokens: Cache<Digest32, Arc<Caller>>,
}

impl Grants {
    /// A token outlives its `exp` by up to [`CODE_TTL`] here, and
    /// [`Caller::is_live`] refuses it meanwhile.
    pub(super) fn new(lifetime: Duration) -> Self {
        Self {
            codes: Cache::builder()
                .max_capacity(MAX_CODES)
                .time_to_live(CODE_TTL)
                .build(),
            redeemed: Cache::builder()
                .max_capacity(MAX_CODES)
                .time_to_live(REDEEMED_TTL)
                .build(),
            tokens: Cache::builder()
                .max_capacity(MAX_TOKENS)
                .time_to_live(lifetime)
                .build(),
        }
    }

    pub(super) fn approve(&self, approval: Approval) -> String {
        let (code, key) = secret(CODE_PREFIX);
        self.codes.insert(key, Arc::new(approval));
        code
    }

    /// `remove` hands a code to one of two racing requests only.
    fn redeem(&self, code: &str) -> Option<(Digest32, Arc<Approval>)> {
        let key = digest(code);
        if let Some(approval) = self.codes.remove(&key) {
            return Some((key, approval));
        }
        if let Some(token) = self.redeemed.remove(&key) {
            self.tokens.invalidate(&token);
            tracing::warn!("an mcp authorization code came back, so its token is revoked");
        }
        None
    }

    fn issue(&self, code: Digest32, approval: &Approval) -> String {
        let (token, key) = secret(TOKEN_PREFIX);
        self.tokens.insert(
            key,
            Arc::new(Caller {
                user: approval.user.clone(),
                client: approval.client.clone(),
                groups: approval.groups.clone(),
                exp: approval.exp,
            }),
        );
        self.redeemed.insert(code, key);
        token
    }

    pub(in crate::app::auth) fn caller(&self, token: &str) -> Option<Arc<Caller>> {
        self.tokens
            .get(&digest(token))
            .filter(|caller| caller.is_live())
    }
}

fn secret(prefix: &str) -> (String, Digest32) {
    let secret = format!(
        "{prefix}{}",
        URL_SAFE_NO_PAD.encode(rand::random::<[u8; 32]>())
    );
    let key = digest(&secret);
    (secret, key)
}

fn digest(secret: &str) -> Digest32 {
    Sha256::digest(secret.as_bytes()).into()
}

#[derive(Deserialize)]
pub(super) struct TokenRequest {
    grant_type: Option<String>,
    code: Option<String>,
    redirect_uri: Option<String>,
    client_id: Option<String>,
    code_verifier: Option<String>,
    resource: Option<String>,
}

#[derive(Serialize)]
struct Issued {
    access_token: String,
    token_type: &'static str,
    expires_in: i64,
}

/// A code is spent by its first use, even one that fails a check.
pub(super) async fn token(
    State(state): State<AppState>,
    request: Result<Form<TokenRequest>, FormRejection>,
) -> Response {
    let Some(oauth) = state.auth.oauth() else {
        return ApiError::NotFound.into_response();
    };
    let Ok(Form(request)) = request else {
        return OAuthError::new("invalid_request", "send a form body").into_response();
    };
    if request.grant_type.as_deref() != Some("authorization_code") {
        return OAuthError::new("unsupported_grant_type", "klens trades only codes")
            .into_response();
    }
    let Some((code, approval)) = request.code.as_deref().and_then(|c| oauth.grants.redeem(c))
    else {
        return OAuthError::new("invalid_grant", "the code is unknown, used or expired")
            .into_response();
    };
    let redirect_uri = request
        .redirect_uri
        .as_deref()
        .and_then(|uri| Url::parse(uri).ok());
    if request.client_id.as_deref() != Some(approval.client_id.as_str())
        || redirect_uri.as_ref() != Some(&approval.redirect_uri)
    {
        return OAuthError::new("invalid_grant", "the code was issued to another client")
            .into_response();
    }
    if !request
        .code_verifier
        .is_some_and(|verifier| verifies(&verifier, &approval.challenge))
    {
        return OAuthError::new("invalid_grant", "the code verifier does not match")
            .into_response();
    }
    if request
        .resource
        .is_some_and(|resource| !oauth.serves(&resource))
    {
        return OAuthError::new(
            "invalid_target",
            "klens issues tokens only for its own /mcp",
        )
        .into_response();
    }
    let access_token = oauth.grants.issue(code, &approval);
    tracing::info!(
        user = approval.user.as_str(),
        client = approval.client.as_deref().unwrap_or_default(),
        "issued an mcp token"
    );
    (
        [(header::CACHE_CONTROL, "no-store")],
        Json(Issued {
            access_token,
            token_type: "Bearer",
            expires_in: approval.exp - Timestamp::now().as_second(),
        }),
    )
        .into_response()
}

/// RFC 7636 4.1: 43 to 128 unreserved characters.
fn verifies(verifier: &str, challenge: &str) -> bool {
    (43..=128).contains(&verifier.len())
        && verifier
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"-._~".contains(&byte))
        && PkceCodeChallenge::from_code_verifier_sha256(&PkceCodeVerifier::new(verifier.to_owned()))
            .as_str()
            == challenge
}
