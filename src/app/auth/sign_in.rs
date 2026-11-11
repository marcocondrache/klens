use jiff::Timestamp;
use openidconnect::RefreshToken;

use super::SessionUser;

/// What a login at the provider yielded. The claims in `user` hold until
/// their ID token expires, and a renewal trades them for fresh ones.
pub(crate) struct SignIn {
    pub user: SessionUser,
    renewal: Option<Renewal>,
}

pub(crate) struct Renewal {
    pub refresh_token: RefreshToken,
    /// No renewal carries the session past this second.
    pub deadline: i64,
}

impl SignIn {
    pub(crate) fn new(user: SessionUser, renewal: Option<Renewal>) -> Self {
        Self { user, renewal }
    }

    pub(crate) fn renewal(&self) -> Option<&Renewal> {
        self.renewal.as_ref()
    }

    pub(crate) fn is_fresh(&self, now: Timestamp) -> bool {
        now.as_second() < self.user.exp
    }

    /// The second the session ends, which without a renewal is the second
    /// the ID token expires.
    pub(crate) fn ends_at(&self) -> i64 {
        self.renewal
            .as_ref()
            .map_or(self.user.exp, |renewal| renewal.deadline)
    }
}
