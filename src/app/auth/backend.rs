use std::convert::Infallible;
use std::future::ready;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum_login::{AuthUser, AuthnBackend, UserId};
use jiff::Timestamp;
use moka::Expiry;
use moka::future::Cache;
use moka::ops::compute::{CompResult, Op};
use openidconnect::{Nonce, PkceCodeVerifier};

use super::SessionUser;
use super::oidc::OidcFlow;
use super::sign_in::SignIn;

#[derive(Clone)]
pub(crate) struct AuthBackend {
    flow: Option<Arc<dyn OidcFlow>>,
    sign_ins: Cache<String, Arc<SignIn>>,
}

struct UntilEnd;

impl Expiry<String, Arc<SignIn>> for UntilEnd {
    fn expire_after_create(
        &self,
        _: &String,
        sign_in: &Arc<SignIn>,
        _: Instant,
    ) -> Option<Duration> {
        let left = sign_in
            .ends_at()
            .saturating_sub(Timestamp::now().as_second());
        Some(Duration::from_secs(left.try_into().unwrap_or_default()))
    }

    fn expire_after_update(
        &self,
        subject: &String,
        sign_in: &Arc<SignIn>,
        updated_at: Instant,
        _: Option<Duration>,
    ) -> Option<Duration> {
        self.expire_after_create(subject, sign_in, updated_at)
    }
}

fn sign_ins() -> Cache<String, Arc<SignIn>> {
    Cache::builder().expire_after(UntilEnd).build()
}

pub(crate) struct OidcCredentials {
    pub code: String,
    pub pkce_verifier: String,
    pub nonce: String,
}

impl AuthBackend {
    pub(crate) fn disabled() -> Self {
        Self {
            flow: None,
            sign_ins: sign_ins(),
        }
    }

    pub(crate) fn enabled(flow: Arc<dyn OidcFlow>) -> Self {
        Self {
            flow: Some(flow),
            sign_ins: sign_ins(),
        }
    }

    pub(crate) fn is_enabled(&self) -> bool {
        self.flow.is_some()
    }

    pub(crate) fn flow(&self) -> Option<&dyn OidcFlow> {
        self.flow.as_deref()
    }

    pub(crate) async fn exchange(&self, creds: OidcCredentials) -> Option<SignIn> {
        let flow = self.flow()?;

        flow.authenticate(
            creds.code,
            PkceCodeVerifier::new(creds.pkce_verifier),
            Nonce::new(creds.nonce),
        )
        .await
        .inspect_err(|error| tracing::warn!(%error, "oidc callback failed"))
        .ok()
    }

    pub(crate) async fn remember(&self, sign_in: SignIn) {
        // An upsert waits for a renewal in flight, which would otherwise
        // replace this sign-in with the one it renewed.
        self.sign_ins
            .entry(sign_in.user.sub.clone())
            .and_upsert_with(|_| ready(Arc::new(sign_in)))
            .await;
    }

    pub(crate) async fn forget(&self, subject: &str) {
        // A plain removal could land before a renewal in flight puts the
        // sign-in back.
        self.sign_ins
            .entry_by_ref(subject)
            .and_compute_with(|_| ready(Op::Remove))
            .await;
    }

    /// The subject's sign-in, renewed first when its ID token has expired.
    pub(crate) async fn live(&self, subject: &str) -> Option<Arc<SignIn>> {
        let sign_in = self.sign_ins.get(subject).await?;
        if sign_in.is_fresh(Timestamp::now()) {
            return Some(sign_in);
        }

        self.renew(subject).await
    }

    async fn renew(&self, subject: &str) -> Option<Arc<SignIn>> {
        // Computes on one subject run in turn. Providers that rotate refresh
        // tokens reject a second use, so concurrent requests must spend the
        // token once and share the result.
        let outcome = self
            .sign_ins
            .entry_by_ref(subject)
            .and_compute_with(|entry| async move {
                let Some(current) = entry.map(|entry| entry.into_value()) else {
                    return Op::Nop;
                };
                if current.is_fresh(Timestamp::now()) {
                    return Op::Nop;
                }

                match self.renewed(&current).await {
                    Some(sign_in) => Op::Put(Arc::new(sign_in)),
                    None => Op::Remove,
                }
            })
            .await;

        match outcome {
            CompResult::Unchanged(entry)
            | CompResult::Inserted(entry)
            | CompResult::ReplacedWith(entry) => Some(entry.into_value()),
            CompResult::Removed(_) | CompResult::StillNone(_) => None,
        }
    }

    async fn renewed(&self, stale: &SignIn) -> Option<SignIn> {
        let flow = self.flow()?;
        let renewal = stale.renewal()?;

        flow.refresh(&stale.user, renewal)
            .await
            .inspect_err(|error| {
                tracing::warn!(%error, sub = stale.user.sub.as_str(), "oidc refresh failed");
            })
            .ok()
    }
}

impl std::fmt::Debug for AuthBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AuthBackend")
            .field("enabled", &self.is_enabled())
            .finish_non_exhaustive()
    }
}

impl AuthUser for SessionUser {
    type Id = String;

    fn id(&self) -> Self::Id {
        self.sub.clone()
    }

    fn session_auth_hash(&self) -> &[u8] {
        &self.auth_hash
    }
}

impl AuthnBackend for AuthBackend {
    type User = SessionUser;
    type Credentials = OidcCredentials;
    type Error = Infallible;

    async fn authenticate(
        &self,
        creds: Self::Credentials,
    ) -> Result<Option<Self::User>, Self::Error> {
        Ok(self.exchange(creds).await.map(|sign_in| sign_in.user))
    }

    async fn get_user(&self, user_id: &UserId<Self>) -> Result<Option<Self::User>, Self::Error> {
        Ok(self.live(user_id).await.map(|sign_in| sign_in.user.clone()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::auth::testing::{FakeOidc, FakeRefresh};

    fn user(exp: i64) -> SessionUser {
        SessionUser::new("alice", None, None, Vec::new(), exp)
    }

    fn once(exp: i64) -> SignIn {
        SignIn::new(user(exp), None)
    }

    async fn live_user(backend: &AuthBackend) -> Option<SessionUser> {
        let sign_in = backend.live("alice").await?;
        Some(sign_in.user.clone())
    }

    #[tokio::test]
    async fn an_expired_user_is_forgotten() {
        let backend = AuthBackend::disabled();
        backend.remember(once(Timestamp::now().as_second())).await;

        assert_eq!(live_user(&backend).await, None);
    }

    #[tokio::test]
    async fn remembering_again_moves_the_expiry() {
        let backend = AuthBackend::disabled();
        let now = Timestamp::now().as_second();
        backend.remember(once(now + 60)).await;
        backend.remember(once(now + 120)).await;
        assert_eq!(live_user(&backend).await, Some(user(now + 120)));

        backend.remember(once(now)).await;
        assert_eq!(live_user(&backend).await, None);
    }

    #[tokio::test]
    async fn concurrent_requests_spend_a_refresh_token_once() {
        let refresh = FakeRefresh {
            id_token_ttl: 3600,
            ..FakeRefresh::default()
        };
        let flow = FakeOidc::refreshing(&refresh);
        let backend = AuthBackend::enabled(Arc::new(flow.clone()));
        let expired = flow
            .authenticate(
                "test-code".to_owned(),
                PkceCodeVerifier::new("verifier".to_owned()),
                Nonce::new_random(),
            )
            .await
            .expect("a sign-in");
        backend.remember(expired).await;

        let (first, second, third) = tokio::join!(
            backend.live("user-1"),
            backend.live("user-1"),
            backend.live("user-1")
        );

        assert!(first.is_some() && second.is_some() && third.is_some());
        assert_eq!(refresh.renewals(), 1);
    }
}
