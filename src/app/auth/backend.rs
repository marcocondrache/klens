use std::convert::Infallible;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum_login::{AuthUser, AuthnBackend, UserId};
use jiff::Timestamp;
use moka::Expiry;
use moka::sync::Cache;
use openidconnect::{Nonce, PkceCodeVerifier};

use super::SessionUser;
use super::oidc::OidcFlow;

#[derive(Clone)]
pub(crate) struct AuthBackend {
    flow: Option<Arc<dyn OidcFlow>>,
    users: Cache<String, Arc<SessionUser>>,
}

struct UntilExp;

impl Expiry<String, Arc<SessionUser>> for UntilExp {
    fn expire_after_create(
        &self,
        _: &String,
        user: &Arc<SessionUser>,
        _: Instant,
    ) -> Option<Duration> {
        let left = user.exp.saturating_sub(Timestamp::now().as_second());
        Some(Duration::from_secs(left.try_into().unwrap_or_default()))
    }

    fn expire_after_update(
        &self,
        subject: &String,
        user: &Arc<SessionUser>,
        updated_at: Instant,
        _: Option<Duration>,
    ) -> Option<Duration> {
        self.expire_after_create(subject, user, updated_at)
    }
}

fn users() -> Cache<String, Arc<SessionUser>> {
    Cache::builder().expire_after(UntilExp).build()
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
            users: users(),
        }
    }

    pub(crate) fn enabled(flow: Arc<dyn OidcFlow>) -> Self {
        Self {
            flow: Some(flow),
            users: users(),
        }
    }

    pub(crate) fn is_enabled(&self) -> bool {
        self.flow.is_some()
    }

    pub(crate) fn flow(&self) -> Option<&dyn OidcFlow> {
        self.flow.as_deref()
    }

    pub(crate) fn remember(&self, user: SessionUser) {
        self.users.insert(user.sub.clone(), Arc::new(user));
    }

    pub(crate) fn live_user(&self, subject: &str) -> Option<SessionUser> {
        self.with_live_user(subject, SessionUser::clone)
    }

    pub(crate) fn with_live_user<T>(
        &self,
        subject: &str,
        read: impl FnOnce(&SessionUser) -> T,
    ) -> Option<T> {
        self.users.get(subject).map(|user| read(&user))
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
        let Some(flow) = self.flow() else {
            return Ok(None);
        };

        match flow
            .authenticate(
                creds.code,
                PkceCodeVerifier::new(creds.pkce_verifier),
                Nonce::new(creds.nonce),
            )
            .await
        {
            Ok(user) => Ok(Some(user)),
            Err(error) => {
                tracing::warn!(%error, "oidc callback failed");
                Ok(None)
            }
        }
    }

    async fn get_user(&self, user_id: &UserId<Self>) -> Result<Option<Self::User>, Self::Error> {
        Ok(self.live_user(user_id))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn user(exp: i64) -> SessionUser {
        SessionUser::new("alice", None, None, Vec::new(), exp)
    }

    #[test]
    fn a_live_user_is_read_in_place() {
        let backend = AuthBackend::disabled();
        let exp = Timestamp::now().as_second() + 60;
        backend.remember(user(exp));

        assert_eq!(backend.with_live_user("alice", |user| user.exp), Some(exp));
        assert_eq!(backend.live_user("alice"), Some(user(exp)));
    }

    #[test]
    fn an_expired_user_is_forgotten() {
        let backend = AuthBackend::disabled();
        backend.remember(user(Timestamp::now().as_second()));

        assert_eq!(backend.live_user("alice"), None);
    }

    #[test]
    fn remembering_again_moves_the_expiry() {
        let backend = AuthBackend::disabled();
        let now = Timestamp::now().as_second();
        backend.remember(user(now + 60));
        backend.remember(user(now + 120));
        assert_eq!(backend.live_user("alice"), Some(user(now + 120)));

        backend.remember(user(now));
        assert_eq!(backend.live_user("alice"), None);
    }
}
