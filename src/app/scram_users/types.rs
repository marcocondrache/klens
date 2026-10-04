use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};
use ts_rs::TS;

use crate::kafka::model as domain;
use crate::r#macro::from_same_variants;

use super::super::clusters::LaneHealth;
use super::super::error::ApiError;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, TS)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ScramStatus {
    /// The lane has not described the users yet.
    Pending,
    Described,
    Denied,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ScramMechanism {
    Sha256,
    Sha512,
}

from_same_variants!(domain::ScramMechanism => ScramMechanism { Sha256, Sha512 });
from_same_variants!(ScramMechanism => domain::ScramMechanism { Sha256, Sha512 });

#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct ScramCredential {
    pub mechanism: ScramMechanism,
    pub iterations: i32,
}

#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct ScramUser {
    pub name: String,
    pub credentials: Vec<ScramCredential>,
}

impl From<&domain::ScramUser> for ScramUser {
    fn from(user: &domain::ScramUser) -> Self {
        Self {
            name: user.name.clone(),
            credentials: user
                .credentials
                .iter()
                .map(|credential| ScramCredential {
                    mechanism: credential.mechanism.into(),
                    iterations: credential.iterations,
                })
                .collect(),
        }
    }
}

#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct ScramListing {
    pub status: ScramStatus,
    pub users: Vec<ScramUser>,
    pub source_health: LaneHealth,
}

impl ScramListing {
    pub(crate) fn new(listing: Option<&domain::ScramListing>, source_health: LaneHealth) -> Self {
        let (status, users) = match listing {
            None => (ScramStatus::Pending, Vec::new()),
            Some(domain::ScramListing::Described(users)) => (
                ScramStatus::Described,
                users.iter().map(ScramUser::from).collect(),
            ),
            Some(domain::ScramListing::Denied) => (ScramStatus::Denied, Vec::new()),
        };
        Self {
            status,
            users,
            source_health,
        }
    }
}

/// Replaces the user's credential for `mechanism`, or adds one.
#[derive(Debug, Clone, Deserialize, TS)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SetScramCredential {
    pub mechanism: ScramMechanism,
    #[ts(type = "string")]
    pub password: SecretString,
    /// When omitted, Kafka's minimum of 4096 applies.
    #[ts(optional)]
    pub iterations: Option<i32>,
}

impl SetScramCredential {
    pub(crate) fn into_credential(
        self,
        user: String,
    ) -> Result<domain::NewScramCredential, ApiError> {
        let range = domain::SCRAM_ITERATIONS;
        let iterations = self.iterations.unwrap_or(*range.start());
        if !range.contains(&iterations) {
            return Err(ApiError::unprocessable(format!(
                "iterations must be between {} and {}",
                range.start(),
                range.end()
            )));
        }
        if self.password.expose_secret().is_empty() {
            return Err(ApiError::unprocessable("the password is empty"));
        }
        Ok(domain::NewScramCredential {
            user,
            mechanism: self.mechanism.into(),
            iterations,
            password: self.password,
        })
    }
}

#[derive(Debug, Deserialize)]
pub(crate) struct DeleteScramCredential {
    pub mechanism: ScramMechanism,
}
