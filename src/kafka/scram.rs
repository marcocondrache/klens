use std::fmt::{self, Display, Formatter};
use std::ops::RangeInclusive;

use krafka::auth::ScramMechanism as WireMechanism;
use krafka::protocol::ScramCredentialUpsertion;
use pbkdf2::pbkdf2_hmac;
use secrecy::{ExposeSecret, SecretString};
use sha2::{Sha256, Sha512};
use zeroize::Zeroizing;

use crate::kafka::error::{KafkaError, is_cluster_authorization_text};

/// The iteration counts Kafka accepts for a credential.
pub const ITERATIONS: RangeInclusive<i32> = 4096..=16384;

const SALT_LEN: usize = 32;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum ScramMechanism {
    Sha256,
    Sha512,
}

impl ScramMechanism {
    pub(crate) fn wire(self) -> WireMechanism {
        match self {
            Self::Sha256 => WireMechanism::Sha256,
            Self::Sha512 => WireMechanism::Sha512,
        }
    }

    // krafka fails the whole describe on a mechanism code it does not know,
    // so `None` only answers krafka's non-exhaustive enum.
    fn from_wire(mechanism: WireMechanism) -> Option<Self> {
        match mechanism {
            WireMechanism::Sha256 => Some(Self::Sha256),
            WireMechanism::Sha512 => Some(Self::Sha512),
            _ => None,
        }
    }
}

impl Display for ScramMechanism {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(self.wire().mechanism_name())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScramCredential {
    pub mechanism: ScramMechanism,
    pub iterations: i32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScramUser {
    pub name: String,
    pub credentials: Vec<ScramCredential>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScramListing {
    Described(Vec<ScramUser>),
    /// klens's own Kafka user lacks `DESCRIBE` on the cluster.
    Denied,
}

pub struct DescribedUser {
    pub name: String,
    pub credentials: Vec<(WireMechanism, i32)>,
}

impl ScramListing {
    pub fn iterations(&self, user: &str, mechanism: ScramMechanism) -> Option<i32> {
        let Self::Described(users) = self else {
            return None;
        };
        users
            .iter()
            .find(|described| described.name == user)?
            .credentials
            .iter()
            .find(|credential| credential.mechanism == mechanism)
            .map(|credential| credential.iterations)
    }

    pub fn from_describe(
        error: Option<&str>,
        users: impl IntoIterator<Item = DescribedUser>,
    ) -> Result<Self, KafkaError> {
        if let Some(message) = error {
            if is_cluster_authorization_text(message) {
                return Ok(Self::Denied);
            }
            return Err(KafkaError::Admin(message.to_owned()));
        }

        let mut users: Vec<ScramUser> = users.into_iter().map(ScramUser::from).collect();
        users.sort_by(|left, right| left.name.cmp(&right.name));
        Ok(Self::Described(users))
    }
}

impl From<DescribedUser> for ScramUser {
    fn from(described: DescribedUser) -> Self {
        let mut credentials: Vec<ScramCredential> = described
            .credentials
            .into_iter()
            .filter_map(|(mechanism, iterations)| {
                Some(ScramCredential {
                    mechanism: ScramMechanism::from_wire(mechanism)?,
                    iterations,
                })
            })
            .collect();
        credentials.sort_by_key(|credential| credential.mechanism);
        Self {
            name: described.name,
            credentials,
        }
    }
}

/// A password to store as `user`'s credential for `mechanism`, in place of
/// any it has.
#[derive(Debug, Clone)]
pub struct NewScramCredential {
    pub user: String,
    pub mechanism: ScramMechanism,
    /// Within [`ITERATIONS`].
    pub iterations: i32,
    pub password: SecretString,
}

impl NewScramCredential {
    pub(crate) fn upsertion(&self) -> ScramCredentialUpsertion {
        self.salted_with(Zeroizing::new(rand::random::<[u8; SALT_LEN]>().to_vec()))
    }

    // Kafka hashes the password's UTF-8 bytes without SASLprep, so klens does too.
    fn salted_with(&self, salt: Zeroizing<Vec<u8>>) -> ScramCredentialUpsertion {
        let password = self.password.expose_secret().as_bytes();
        let rounds = self.iterations.cast_unsigned();
        let mechanism = self.mechanism.wire();
        let mut salted_password = Zeroizing::new(vec![0; mechanism.hash_length()]);
        match self.mechanism {
            ScramMechanism::Sha256 => {
                pbkdf2_hmac::<Sha256>(password, &salt, rounds, &mut salted_password);
            }
            ScramMechanism::Sha512 => {
                pbkdf2_hmac::<Sha512>(password, &salt, rounds, &mut salted_password);
            }
        }
        ScramCredentialUpsertion {
            name: self.user.clone(),
            mechanism,
            iterations: self.iterations,
            salt,
            salted_password,
        }
    }
}

#[cfg(test)]
mod tests;
