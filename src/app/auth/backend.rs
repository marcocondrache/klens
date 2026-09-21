use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::{Arc, Mutex};

use axum_login::{AuthUser, AuthnBackend, UserId};
use jiff::Timestamp;
use openidconnect::{Nonce, PkceCodeVerifier};

use super::SessionUser;
use super::oidc::OidcFlow;

#[derive(Clone)]
pub(crate) struct AuthBackend {
    flow: Option<Arc<dyn OidcFlow>>,
    users: Arc<Mutex<HashMap<String, SessionUser>>>,
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
            users: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub(crate) fn enabled(flow: Arc<dyn OidcFlow>) -> Self {
        Self {
            flow: Some(flow),
            users: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub(crate) fn is_enabled(&self) -> bool {
        self.flow.is_some()
    }

    pub(crate) fn flow(&self) -> Option<&dyn OidcFlow> {
        self.flow.as_deref()
    }

    pub(crate) fn remember(&self, user: SessionUser) {
        self.users
            .lock()
            .expect("auth user store")
            .insert(user.sub.clone(), user);
    }

    pub(crate) fn live_user(&self, subject: &str) -> Option<SessionUser> {
        let mut users = self.users.lock().expect("auth user store");
        match users.get(subject) {
            Some(user) if user.exp > Timestamp::now().as_second() => Some(user.clone()),
            Some(_) => {
                users.remove(subject);
                None
            }
            None => None,
        }
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
