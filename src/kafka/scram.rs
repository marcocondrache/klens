use krafka::auth::ScramMechanism as WireMechanism;

use crate::kafka::error::{KafkaError, is_cluster_authorization_text};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum ScramMechanism {
    Sha256,
    Sha512,
}

impl ScramMechanism {
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

#[cfg(test)]
mod tests;
