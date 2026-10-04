use itertools::Itertools as _;
use serde::{Deserialize, Serialize};
use ts_rs::TS;

use crate::kafka::model as domain;
use crate::r#macro::from_same_variants;

use super::super::clusters::LaneHealth;
use super::super::error::ApiError;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, TS)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum QuotaStatus {
    /// The lane has not described the quotas yet.
    Pending,
    Described,
    Denied,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum QuotaEntityType {
    User,
    ClientId,
    Ip,
}

from_same_variants!(domain::QuotaEntityType => QuotaEntityType { User, ClientId, Ip });
from_same_variants!(QuotaEntityType => domain::QuotaEntityType { User, ClientId, Ip });

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct QuotaEntity {
    pub entity_type: QuotaEntityType,
    pub name: Option<String>,
}

/// Also the body that sets a quota: every value left null is removed.
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ClientQuota {
    pub entity: Vec<QuotaEntity>,
    pub producer_byte_rate: Option<f64>,
    pub consumer_byte_rate: Option<f64>,
    pub request_percentage: Option<f64>,
    pub controller_mutation_rate: Option<f64>,
    pub connection_creation_rate: Option<f64>,
}

impl From<&domain::ClientQuota> for ClientQuota {
    fn from(quota: &domain::ClientQuota) -> Self {
        Self {
            entity: quota
                .entity
                .iter()
                .map(|part| QuotaEntity {
                    entity_type: part.entity_type.into(),
                    name: part.name.clone(),
                })
                .collect(),
            producer_byte_rate: quota.values.producer_byte_rate,
            consumer_byte_rate: quota.values.consumer_byte_rate,
            request_percentage: quota.values.request_percentage,
            controller_mutation_rate: quota.values.controller_mutation_rate,
            connection_creation_rate: quota.values.connection_creation_rate,
        }
    }
}

impl ClientQuota {
    pub(crate) fn into_quota(self) -> Result<domain::ClientQuota, ApiError> {
        if self.entity.is_empty() {
            return Err(ApiError::unprocessable(
                "name the entity the quota applies to",
            ));
        }
        let entity: Vec<domain::QuotaEntity> = self
            .entity
            .into_iter()
            .map(|part| domain::QuotaEntity {
                entity_type: part.entity_type.into(),
                name: part.name,
            })
            .sorted_by_key(|part| part.entity_type)
            .collect();
        if let Some((part, _)) = entity
            .iter()
            .tuple_windows()
            .find(|(part, next)| part.entity_type == next.entity_type)
        {
            return Err(ApiError::unprocessable(format!(
                "the entity names {} twice",
                part.entity_type.wire()
            )));
        }
        Ok(domain::ClientQuota {
            entity,
            values: domain::QuotaValues {
                producer_byte_rate: self.producer_byte_rate,
                consumer_byte_rate: self.consumer_byte_rate,
                request_percentage: self.request_percentage,
                controller_mutation_rate: self.controller_mutation_rate,
                connection_creation_rate: self.connection_creation_rate,
            },
        })
    }
}

#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct QuotaListing {
    pub status: QuotaStatus,
    pub quotas: Vec<ClientQuota>,
    pub source_health: LaneHealth,
}

impl QuotaListing {
    pub(crate) fn new(listing: Option<&domain::QuotaListing>, source_health: LaneHealth) -> Self {
        let (status, quotas) = match listing {
            None => (QuotaStatus::Pending, Vec::new()),
            Some(domain::QuotaListing::Described(quotas)) => (
                QuotaStatus::Described,
                quotas.iter().map(ClientQuota::from).collect(),
            ),
            Some(domain::QuotaListing::Denied) => (QuotaStatus::Denied, Vec::new()),
        };
        Self {
            status,
            quotas,
            source_health,
        }
    }
}
