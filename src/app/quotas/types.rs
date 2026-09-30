use serde::Serialize;
use ts_rs::TS;

use crate::kafka::model as domain;
use crate::r#macro::from_same_variants;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, TS)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum QuotaAccess {
    Allowed,
    Denied,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, TS)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum QuotaEntityType {
    User,
    ClientId,
    Ip,
}

from_same_variants!(domain::QuotaEntityType => QuotaEntityType { User, ClientId, Ip });

#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct QuotaEntity {
    pub entity_type: QuotaEntityType,
    pub name: Option<String>,
}

#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
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

#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct QuotaListing {
    pub access: QuotaAccess,
    pub quotas: Vec<ClientQuota>,
}

impl From<&domain::QuotaListing> for QuotaListing {
    fn from(listing: &domain::QuotaListing) -> Self {
        match listing {
            domain::QuotaListing::Described(quotas) => Self {
                access: QuotaAccess::Allowed,
                quotas: quotas.iter().map(ClientQuota::from).collect(),
            },
            domain::QuotaListing::Denied => Self {
                access: QuotaAccess::Denied,
                quotas: Vec::new(),
            },
        }
    }
}
