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

use super::SessionUser;
use crate::config::OidcConfig;

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
    ) -> anyhow::Result<SessionUser>;
}

pub(crate) struct Oidc {
    http: reqwest::Client,
    client: DiscoveredClient,
    scopes: Vec<Scope>,
}

impl Oidc {
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
    ) -> anyhow::Result<SessionUser> {
        let token_response = self
            .client
            .exchange_code(AuthorizationCode::new(code))
            .map_err(|error| anyhow!("oidc token exchange failed: {error}"))?
            .set_pkce_verifier(pkce_verifier)
            .request_async(&self.http)
            .await
            .map_err(|error| anyhow!("oidc token exchange failed: {error}"))?;

        let id_token = token_response
            .id_token()
            .ok_or_else(|| anyhow!("oidc provider did not return an ID token"))?;
        let verifier = self.client.id_token_verifier();
        let claims = id_token
            .claims(&verifier, &nonce)
            .map_err(|error| anyhow!("oidc ID token is invalid: {error}"))?;

        if let Some(expected_hash) = claims.access_token_hash() {
            let signing_alg = id_token
                .signing_alg()
                .map_err(|error| anyhow!("oidc ID token is invalid: {error}"))?;
            let signing_key = id_token
                .signing_key(&verifier)
                .map_err(|error| anyhow!("oidc ID token is invalid: {error}"))?;
            let actual_hash = AccessTokenHash::from_token(
                token_response.access_token(),
                signing_alg,
                signing_key,
            )
            .map_err(|error| anyhow!("oidc ID token is invalid: {error}"))?;

            if actual_hash != *expected_hash {
                return Err(anyhow!(
                    "oidc ID token is invalid: access token hash mismatch"
                ));
            }
        }

        let now = super::unix_now();
        let exp = claims
            .expiration()
            .timestamp()
            .min(now + *crate::environment::MAX_SESSION_SECS);
        if exp <= now {
            return Err(anyhow!("oidc ID token has expired"));
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
pub(crate) struct FakeOidc;

#[cfg(test)]
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

        Ok(SessionUser {
            sub: "user-1".into(),
            email: Some("user@example.com".into()),
            name: Some("Test User".into()),
            exp: super::unix_now() + 3600,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn unused_pkce() -> PkceCodeVerifier {
        PkceCodeVerifier::new("verifier".into())
    }

    fn unused_nonce() -> Nonce {
        Nonce::new("nonce".into())
    }

    #[tokio::test]
    async fn fake_oidc_rejects_a_bad_code_with_a_literal() {
        let error = FakeOidc
            .authenticate("nope".into(), unused_pkce(), unused_nonce())
            .await
            .unwrap_err();
        assert_eq!(error.to_string(), "invalid authorization code");
    }

    #[tokio::test]
    async fn fake_oidc_accepts_the_test_code() {
        let user = FakeOidc
            .authenticate("test-code".into(), unused_pkce(), unused_nonce())
            .await
            .unwrap();
        assert_eq!(user.sub, "user-1");
        assert_eq!(user.email.as_deref(), Some("user@example.com"));
        assert_eq!(user.name.as_deref(), Some("Test User"));
    }
}
