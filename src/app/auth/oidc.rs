use std::fmt::Display;
use std::time::Duration;

use anyhow::{Context, anyhow};
use async_trait::async_trait;
use jiff::Timestamp;
use openidconnect::core::{CoreAuthenticationFlow, CoreClient, CoreProviderMetadata};
use openidconnect::reqwest;
use openidconnect::{
    AccessTokenHash, AuthorizationCode, ClientId, ClientSecret, CsrfToken, EndpointMaybeSet,
    EndpointNotSet, EndpointSet, Nonce, OAuth2TokenResponse, PkceCodeChallenge, PkceCodeVerifier,
    Scope, TokenResponse,
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
use super::bearer::Jwks;
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
    ) -> Result<SessionUser, OidcError>;
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

/// A provider that stops answering would otherwise hang startup and every
/// login callback. Key set fetches stop after the same time.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

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
    ) -> anyhow::Result<(Self, Jwks)> {
        let http = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(REQUEST_TIMEOUT)
            .build()
            .context("failed to build oidc http client")?;

        tracing::info!(issuer = %config.issuer, "discovering oidc provider");

        let metadata = CoreProviderMetadata::discover_async(config.issuer.clone(), &http)
            .await
            .map_err(|error| anyhow!("oidc provider discovery failed: {error}"))?;

        let keys = Jwks::discovered(
            http.clone(),
            metadata.jwks_uri().url().clone(),
            metadata.jwks(),
        );
        let client = CoreClient::from_provider_metadata(
            metadata,
            ClientId::new(config.client_id.clone()),
            Some(ClientSecret::new(
                config.client_secret.expose_secret().to_owned(),
            )),
        )
        .set_redirect_uri(config.redirect_uri.clone());

        let oidc = Self {
            client,
            http,
            scopes: config.scopes.iter().cloned().map(Scope::new).collect(),
            groups_claim: config.groups_claim.clone(),
            max_session_secs: i64::try_from(max_session.as_secs()).unwrap_or(i64::MAX),
        };
        Ok((oidc, keys))
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
    ) -> Result<SessionUser, OidcError> {
        let token_response = self
            .client
            .exchange_code(AuthorizationCode::new(code))
            .map_err(OidcError::exchange)?
            .set_pkce_verifier(pkce_verifier)
            .request_async(&self.http)
            .await
            .map_err(OidcError::exchange)?;

        let id_token = token_response.id_token().ok_or(OidcError::NoIdToken)?;
        let verifier = self.client.id_token_verifier();
        let claims = id_token
            .claims(&verifier, &nonce)
            .map_err(OidcError::invalid)?;

        if let Some(expected_hash) = claims.access_token_hash() {
            let signing_alg = id_token.signing_alg().map_err(OidcError::invalid)?;
            let signing_key = id_token
                .signing_key(&verifier)
                .map_err(OidcError::invalid)?;
            let actual_hash = AccessTokenHash::from_token(
                token_response.access_token(),
                signing_alg,
                signing_key,
            )
            .map_err(OidcError::invalid)?;

            if actual_hash != *expected_hash {
                return Err(OidcError::invalid("access token hash mismatch"));
            }
        }

        let now = Timestamp::now().as_second();
        let exp = claims
            .expiration()
            .timestamp()
            .min(now.saturating_add(self.max_session_secs));
        if exp <= now {
            return Err(OidcError::Expired);
        }

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
    use tokio::time::Instant;
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::*;
    use crate::testing::yaml;

    #[tokio::test(start_paused = true)]
    async fn discovery_gives_up_on_a_provider_that_stops_answering() {
        let provider = MockServer::start().await;
        Mock::given(wiremock::matchers::any())
            .respond_with(ResponseTemplate::new(200).set_delay(REQUEST_TIMEOUT * 2))
            .mount(&provider)
            .await;
        let oidc: config::Oidc = yaml(&format!(
            "
            issuer: {}
            client_id: klens
            client_secret: {{value: secret}}
            redirect_uri: http://localhost:8080/api/auth/callback
            ",
            provider.uri()
        ));
        let started = Instant::now();

        let discovered = Oidc::discover(&oidc, Duration::from_secs(3600)).await;

        assert!(discovered.is_err());
        assert_eq!(started.elapsed(), REQUEST_TIMEOUT);
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
