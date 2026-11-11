use std::fmt::Display;
use std::time::Duration;

use anyhow::{Context, anyhow};
use async_trait::async_trait;
use jiff::Timestamp;
use openidconnect::core::{
    CoreAuthenticationFlow, CoreClient, CoreProviderMetadata, CoreTokenResponse,
};
use openidconnect::reqwest;
use openidconnect::{
    AccessTokenHash, AuthorizationCode, ClientId, ClientSecret, CsrfToken, EndpointMaybeSet,
    EndpointNotSet, EndpointSet, Nonce, NonceVerifier, OAuth2TokenResponse, PkceCodeChallenge,
    PkceCodeVerifier, Scope, TokenResponse,
};
use secrecy::ExposeSecret;

type DiscoveredClient = CoreClient<
    EndpointSet,
    EndpointNotSet,
    EndpointNotSet,
    EndpointNotSet,
    EndpointMaybeSet,
    EndpointMaybeSet,
>;

use super::SessionUser;
use super::access::groups_from_json;
use super::sign_in::{Renewal, SignIn};
use crate::config;

#[async_trait]
pub(crate) trait OidcFlow: Send + Sync {
    fn authorize_url(
        &self,
        csrf: CsrfToken,
        nonce: Nonce,
        pkce_challenge: PkceCodeChallenge,
    ) -> url::Url;

    async fn authenticate(
        &self,
        code: String,
        pkce_verifier: PkceCodeVerifier,
        nonce: Nonce,
    ) -> Result<SignIn, OidcError>;

    /// Trades the refresh token for the claims the provider vouches for now.
    async fn refresh(&self, user: &SessionUser, renewal: &Renewal) -> Result<SignIn, OidcError>;
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum OidcError {
    #[error("oidc token exchange failed: {0}")]
    Exchange(String),
    #[error("oidc provider did not return an ID token")]
    NoIdToken,
    #[error("oidc ID token is invalid: {0}")]
    InvalidIdToken(String),
    #[error("oidc ID token has expired")]
    Expired,
}

impl OidcError {
    fn exchange(error: impl Display) -> Self {
        Self::Exchange(error.to_string())
    }

    fn invalid(error: impl Display) -> Self {
        Self::InvalidIdToken(error.to_string())
    }
}

pub(crate) struct Oidc {
    http: reqwest::Client,
    client: DiscoveredClient,
    scopes: Vec<Scope>,
    groups_claim: String,
    max_session_secs: i64,
}

impl Oidc {
    pub(crate) async fn discover(
        config: &config::Oidc,
        max_session: Duration,
    ) -> anyhow::Result<Self> {
        let http = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .context("failed to build oidc http client")?;

        tracing::info!(issuer = %config.issuer, "discovering oidc provider");

        let metadata = CoreProviderMetadata::discover_async(config.issuer.clone(), &http)
            .await
            .map_err(|error| anyhow!("oidc provider discovery failed: {error}"))?;

        let client = CoreClient::from_provider_metadata(
            metadata,
            ClientId::new(config.client_id.clone()),
            Some(ClientSecret::new(
                config.client_secret.expose_secret().to_owned(),
            )),
        )
        .set_redirect_uri(config.redirect_uri.clone());

        Ok(Self {
            client,
            http,
            scopes: config.scopes.iter().cloned().map(Scope::new).collect(),
            groups_claim: config.groups_claim.clone(),
            max_session_secs: i64::try_from(max_session.as_secs()).unwrap_or(i64::MAX),
        })
    }
}

#[async_trait]
impl OidcFlow for Oidc {
    fn authorize_url(
        &self,
        csrf: CsrfToken,
        nonce: Nonce,
        pkce_challenge: PkceCodeChallenge,
    ) -> url::Url {
        let mut request = self
            .client
            .authorize_url(
                CoreAuthenticationFlow::AuthorizationCode,
                move || csrf,
                move || nonce,
            )
            .set_pkce_challenge(pkce_challenge);

        for scope in &self.scopes {
            if scope.as_str() != "openid" {
                request = request.add_scope(scope.clone());
            }
        }

        request.url().0
    }

    async fn authenticate(
        &self,
        code: String,
        pkce_verifier: PkceCodeVerifier,
        nonce: Nonce,
    ) -> Result<SignIn, OidcError> {
        let response = self
            .client
            .exchange_code(AuthorizationCode::new(code))
            .map_err(OidcError::exchange)?
            .set_pkce_verifier(pkce_verifier)
            .request_async(&self.http)
            .await
            .map_err(OidcError::exchange)?;

        let deadline = Timestamp::now()
            .as_second()
            .saturating_add(self.max_session_secs);
        let user = self.user(&response, &nonce, deadline)?;
        let renewal = response.refresh_token().map(|refresh_token| Renewal {
            refresh_token: refresh_token.clone(),
            deadline,
        });

        Ok(SignIn::new(user, renewal))
    }

    async fn refresh(&self, user: &SessionUser, renewal: &Renewal) -> Result<SignIn, OidcError> {
        let response = self
            .client
            .exchange_refresh_token(&renewal.refresh_token)
            .map_err(OidcError::exchange)?
            .request_async(&self.http)
            .await
            .map_err(OidcError::exchange)?;

        // A refreshed ID token answers no authorization request, so it has
        // no nonce to match.
        let any_nonce = |_: Option<&Nonce>| Ok(());
        let renewed = self.user(&response, any_nonce, renewal.deadline)?;

        // A provider that does not rotate refresh tokens returns none.
        let refresh_token = response
            .refresh_token()
            .unwrap_or(&renewal.refresh_token)
            .clone();

        Ok(SignIn::new(
            renewed.renewing(user),
            Some(Renewal {
                refresh_token,
                deadline: renewal.deadline,
            }),
        ))
    }
}

impl Oidc {
    /// Reads the user out of the response's ID token, whose claims hold
    /// until the token expires or `deadline`, whichever comes first.
    fn user(
        &self,
        response: &CoreTokenResponse,
        nonce: impl NonceVerifier,
        deadline: i64,
    ) -> Result<SessionUser, OidcError> {
        let id_token = response.id_token().ok_or(OidcError::NoIdToken)?;
        let verifier = self.client.id_token_verifier();
        let claims = id_token
            .claims(&verifier, nonce)
            .map_err(OidcError::invalid)?;

        if let Some(expected_hash) = claims.access_token_hash() {
            let signing_alg = id_token.signing_alg().map_err(OidcError::invalid)?;
            let signing_key = id_token
                .signing_key(&verifier)
                .map_err(OidcError::invalid)?;
            let actual_hash =
                AccessTokenHash::from_token(response.access_token(), signing_alg, signing_key)
                    .map_err(OidcError::invalid)?;

            if actual_hash != *expected_hash {
                return Err(OidcError::invalid("access token hash mismatch"));
            }
        }

        let exp = claims_end(
            claims.expiration().timestamp(),
            deadline,
            Timestamp::now().as_second(),
        )?;

        let name = claims.name().and_then(|localized| {
            localized
                .get(None)
                .or_else(|| localized.iter().next().map(|(_, value)| value))
                .map(|name| name.as_str().to_owned())
        });

        Ok(SessionUser::new(
            claims.subject().to_string(),
            claims.email().map(|email| email.to_string()),
            name,
            groups_from_id_token(&id_token.to_string(), &self.groups_claim)?,
            exp,
        ))
    }
}

/// The second the claims of an ID token stop holding, which is never past
/// the session's deadline.
fn claims_end(id_token_exp: i64, deadline: i64, now: i64) -> Result<i64, OidcError> {
    let end = id_token_exp.min(deadline);
    if end <= now {
        return Err(OidcError::Expired);
    }
    Ok(end)
}

fn groups_from_id_token(id_token: &str, claim: &str) -> Result<Vec<String>, OidcError> {
    let payload = id_token
        .split('.')
        .nth(1)
        .ok_or_else(|| OidcError::invalid("it has no payload"))?;
    let decoded =
        base64::Engine::decode(&base64::engine::general_purpose::URL_SAFE_NO_PAD, payload)
            .or_else(|_| {
                base64::Engine::decode(&base64::engine::general_purpose::URL_SAFE, payload)
            })
            .map_err(OidcError::invalid)?;
    let value: serde_json::Value = serde_json::from_slice(&decoded).map_err(OidcError::invalid)?;
    Ok(groups_from_json(&value, claim))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn claims_hold_until_the_id_token_or_the_deadline_ends() {
        assert_eq!(claims_end(200, 300, 100).unwrap(), 200);
        assert_eq!(claims_end(400, 300, 100).unwrap(), 300);

        assert!(matches!(claims_end(100, 300, 100), Err(OidcError::Expired)));
        assert!(matches!(claims_end(400, 100, 100), Err(OidcError::Expired)));
    }

    #[test]
    fn groups_from_id_token_reads_the_payload_claim() {
        let payload = base64::Engine::encode(
            &base64::engine::general_purpose::URL_SAFE_NO_PAD,
            br#"{"groups":["ops","platform"]}"#,
        );
        let token = format!("header.{payload}.sig");
        assert_eq!(
            groups_from_id_token(&token, "groups").unwrap(),
            ["ops", "platform"]
        );
    }
}
