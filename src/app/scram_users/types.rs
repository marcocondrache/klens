use serde::Serialize;
use ts_rs::TS;

use crate::kafka::model as domain;
use crate::r#macro::from_same_variants;

use super::super::clusters::LaneHealth;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, TS)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ScramStatus {
    /// The lane has not described the users yet.
    Pending,
    Described,
    Denied,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, TS)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ScramMechanism {
    Sha256,
    Sha512,
}

from_same_variants!(domain::ScramMechanism => ScramMechanism { Sha256, Sha512 });

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
