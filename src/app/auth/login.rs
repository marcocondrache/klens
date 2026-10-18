use std::time::Duration;

use axum::http::HeaderMap;
use axum_extra::extract::cookie::{Cookie, Key, SameSite, SignedCookieJar};
use jiff::Timestamp;
use openidconnect::{CsrfToken, Nonce, PkceCodeVerifier};
use serde::{Deserialize, Serialize};
use tower_sessions::cookie::time;

pub(super) const LOGIN_COOKIE: &str = "klens_login";

const LOGIN_COOKIE_PATH: &str = "/api/auth/callback";

/// A login waiting on the provider. The browser holds it in a signed cookie,
/// so an anonymous visitor costs the server no memory.
#[derive(Serialize, Deserialize)]
pub(super) struct LoginPending {
    pub state: String,
    pub nonce: String,
    pub pkce_verifier: String,
    expires_at: Timestamp,
}

#[derive(Clone)]
pub(super) struct LoginCookie {
    key: Key,
    secure: bool,
    timeout: Duration,
}

impl LoginCookie {
    pub(super) fn new(secure: bool, timeout: Duration) -> Self {
        Self {
            key: Key::generate(),
            secure,
            timeout,
        }
    }

    /// Returns the jar that hands the login to the browser until the
    /// provider sends it back to the callback.
    pub(super) fn start(
        &self,
        now: Timestamp,
        state: &CsrfToken,
        nonce: &Nonce,
        pkce_verifier: &PkceCodeVerifier,
    ) -> SignedCookieJar {
        let pending = LoginPending {
            state: state.secret().to_owned(),
            nonce: nonce.secret().to_owned(),
            pkce_verifier: pkce_verifier.secret().to_owned(),
            expires_at: now.checked_add(self.timeout).unwrap_or(Timestamp::MAX),
        };
        let value = serde_json::to_string(&pending).expect("a pending login is plain strings");

        let cookie = Cookie::build((LOGIN_COOKIE, value))
            .path(LOGIN_COOKIE_PATH)
            .http_only(true)
            // Lax so the provider's redirect to the callback still sends it.
            .same_site(SameSite::Lax)
            .secure(self.secure)
            .max_age(time::Duration::try_from(self.timeout).unwrap_or(time::Duration::MAX));
        SignedCookieJar::new(self.key.clone()).add(cookie)
    }

    /// Returns the login the browser brought back, when klens signed it and
    /// it is on time, with the jar that clears it.
    pub(super) fn finish(
        &self,
        headers: &HeaderMap,
        now: Timestamp,
    ) -> (SignedCookieJar, Option<LoginPending>) {
        let jar = SignedCookieJar::from_headers(headers, self.key.clone());
        let pending = jar
            .get(LOGIN_COOKIE)
            .and_then(|cookie| serde_json::from_str::<LoginPending>(cookie.value()).ok())
            .filter(|pending| now < pending.expires_at);

        let jar = jar.remove(Cookie::build(LOGIN_COOKIE).path(LOGIN_COOKIE_PATH));
        (jar, pending)
    }
}

#[cfg(test)]
mod tests {
    use axum::http::header::{COOKIE, SET_COOKIE};
    use axum::response::IntoResponse;
    use jiff::SignedDuration;

    use super::*;

    fn sent_back(jar: SignedCookieJar) -> HeaderMap {
        let response = jar.into_response();
        let set_cookie = response.headers()[SET_COOKIE]
            .to_str()
            .expect("ascii set-cookie");
        let pair = set_cookie.split(';').next().unwrap_or(set_cookie);
        HeaderMap::from_iter([(COOKIE, pair.parse().expect("a cookie header"))])
    }

    #[test]
    fn a_login_lasts_until_its_timeout() {
        let cookie = LoginCookie::new(false, Duration::from_secs(90));
        let started = Timestamp::now();
        let headers = sent_back(cookie.start(
            started,
            &CsrfToken::new_random(),
            &Nonce::new_random(),
            &PkceCodeVerifier::new("verifier".to_owned()),
        ));

        let live_after = |seconds| {
            let now = started + SignedDuration::from_secs(seconds);
            cookie.finish(&headers, now).1.is_some()
        };
        assert!(live_after(89));
        assert!(!live_after(90));
    }
}
