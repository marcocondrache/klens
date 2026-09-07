use anyhow::{Context, anyhow};
use async_trait::async_trait;
use openidconnect::core::{CoreAuthenticationFlow, CoreClient, CoreProviderMetadata};
use openidconnect::reqwest;
use openidconnect::{
    AccessTokenHash, AuthorizationCode, ClientId, ClientSecret, CsrfToken, EndpointMaybeSet,
    EndpointNotSet, EndpointSet, IssuerUrl, Nonce, OAuth2TokenResponse, PkceCodeChallenge,
    PkceCodeVerifier, RedirectUrl, Scope, TokenResponse,
};

type DiscoveredClient = CoreClient<
    EndpointSet,
    EndpointNotSet,
    EndpointNotSet,
    EndpointNotSet,
    EndpointMaybeSet,
    EndpointMaybeSet,
>;
use thiserror::Error;

use super::SessionUser;
use crate::config::OidcConfig;

#[derive(Debug, Error)]
pub(crate) enum OidcError {
    #[error("oidc token exchange failed: {0}")]
    TokenExchange(String),
    #[error("oidc provider did not return an ID token")]
    MissingIdToken,
    #[error("oidc ID token is invalid: {0}")]
    InvalidIdToken(String),
    #[error("oidc ID token has expired")]
    ExpiredToken,
}

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

pub(crate) struct RealOidc {
    client: DiscoveredClient,
    http: reqwest::Client,
    scopes: Vec<Scope>,
}

impl RealOidc {
    pub(crate) async fn discover(config: &OidcConfig) -> anyhow::Result<Self> {
        let http = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .context("failed to build oidc http client")?;

        let issuer = IssuerUrl::new(config.issuer.clone())
            .map_err(|error| anyhow!("invalid oidc issuer: {error}"))?;

        tracing::info!(issuer = %config.issuer, "discovering oidc provider");

        let metadata = CoreProviderMetadata::discover_async(issuer, &http)
            .await
            .map_err(|error| anyhow!("oidc provider discovery failed: {error}"))?;

        let redirect = RedirectUrl::new(config.redirect_uri.clone())
            .map_err(|error| anyhow!("invalid oidc redirect_uri: {error}"))?;

        let client = CoreClient::from_provider_metadata(
            metadata,
            ClientId::new(config.client_id.clone()),
            Some(ClientSecret::new(config.client_secret.clone())),
        )
        .set_redirect_uri(redirect);

        Ok(Self {
            client,
            http,
            scopes: config
                .effective_scopes()
                .into_iter()
                .map(Scope::new)
                .collect(),
        })
    }
}

#[async_trait]
impl OidcFlow for RealOidc {
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
                move || csrf.clone(),
                move || nonce.clone(),
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
            .map_err(|error| OidcError::TokenExchange(error.to_string()))?
            .set_pkce_verifier(pkce_verifier)
            .request_async(&self.http)
            .await
            .map_err(|error| OidcError::TokenExchange(error.to_string()))?;

        let id_token = token_response.id_token().ok_or(OidcError::MissingIdToken)?;
        let verifier = self.client.id_token_verifier();
        let claims = id_token
            .claims(&verifier, &nonce)
            .map_err(|error| OidcError::InvalidIdToken(error.to_string()))?;

        if let Some(expected_hash) = claims.access_token_hash() {
            let signing_alg = id_token
                .signing_alg()
                .map_err(|error| OidcError::InvalidIdToken(error.to_string()))?;
            let signing_key = id_token
                .signing_key(&verifier)
                .map_err(|error| OidcError::InvalidIdToken(error.to_string()))?;
            let actual_hash = AccessTokenHash::from_token(
                token_response.access_token(),
                signing_alg,
                signing_key,
            )
            .map_err(|error| OidcError::InvalidIdToken(error.to_string()))?;

            if actual_hash != *expected_hash {
                return Err(OidcError::InvalidIdToken(
                    "access token hash mismatch".into(),
                ));
            }
        }

        let now = super::unix_now();
        let exp = claims
            .expiration()
            .timestamp()
            .min(now + super::MAX_SESSION_SECS);
        if exp <= now {
            return Err(OidcError::ExpiredToken);
        }

        let name = claims.name().and_then(|localized| {
            localized
                .get(None)
                .or_else(|| localized.iter().next().map(|(_, value)| value))
                .map(|name| name.as_str().to_owned())
        });

        Ok(SessionUser {
            sub: claims.subject().to_string(),
            email: claims.email().map(|email| email.to_string()),
            name,
            exp,
        })
    }
}

#[cfg(test)]
pub(crate) struct StubOidc;

#[cfg(test)]
#[async_trait]
impl OidcFlow for StubOidc {
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
    ) -> Result<SessionUser, OidcError> {
        if code != "test-code" {
            return Err(OidcError::TokenExchange(
                "invalid authorization code".into(),
            ));
        }

        Ok(SessionUser {
            sub: "user-1".into(),
            email: Some("user@example.com".into()),
            name: Some("Test User".into()),
            exp: super::unix_now() + 3600,
        })
    }
}
