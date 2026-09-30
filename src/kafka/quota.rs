use tracing::warn;

use crate::kafka::error::KafkaError;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum QuotaEntityType {
    User,
    ClientId,
    Ip,
}

impl QuotaEntityType {
    fn from_wire(name: &str) -> Option<Self> {
        match name {
            "user" => Some(Self::User),
            "client-id" => Some(Self::ClientId),
            "ip" => Some(Self::Ip),
            _ => None,
        }
    }
}

/// One part of the entity a quota applies to. `None` is the default entity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuotaEntity {
    pub entity_type: QuotaEntityType,
    pub name: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct QuotaValues {
    pub producer_byte_rate: Option<f64>,
    pub consumer_byte_rate: Option<f64>,
    pub request_percentage: Option<f64>,
    pub controller_mutation_rate: Option<f64>,
    pub connection_creation_rate: Option<f64>,
}

impl QuotaValues {
    fn set(&mut self, key: &str, value: f64) {
        let slot = match key {
            "producer_byte_rate" => &mut self.producer_byte_rate,
            "consumer_byte_rate" => &mut self.consumer_byte_rate,
            "request_percentage" => &mut self.request_percentage,
            "controller_mutation_rate" => &mut self.controller_mutation_rate,
            "connection_creation_rate" => &mut self.connection_creation_rate,
            _ => return,
        };
        *slot = Some(value);
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ClientQuota {
    pub entity: Vec<QuotaEntity>,
    pub values: QuotaValues,
}

#[derive(Debug, Clone, PartialEq)]
pub enum QuotaListing {
    Described(Vec<ClientQuota>),
    /// klens's own Kafka user lacks `DESCRIBE_CONFIGS` on the cluster.
    Denied,
}

pub struct DescribedQuota {
    pub entity: Vec<(String, Option<String>)>,
    pub values: Vec<(String, f64)>,
}

impl QuotaListing {
    pub fn from_describe(
        cluster: &str,
        error: Option<&str>,
        entries: impl IntoIterator<Item = DescribedQuota>,
    ) -> Result<Self, KafkaError> {
        if let Some(message) = error {
            if is_cluster_authorization_text(message) {
                return Ok(Self::Denied);
            }
            return Err(KafkaError::Admin(message.to_owned()));
        }

        Ok(Self::Described(
            entries
                .into_iter()
                .filter_map(|entry| ClientQuota::from_described(cluster, entry))
                .collect(),
        ))
    }
}

impl ClientQuota {
    fn from_described(cluster: &str, described: DescribedQuota) -> Option<Self> {
        let mut entity = Vec::with_capacity(described.entity.len());
        for (entity_type, name) in described.entity {
            let Some(entity_type) = QuotaEntityType::from_wire(&entity_type) else {
                warn!(
                    cluster,
                    entity_type, "dropping quota for an unknown entity type"
                );
                return None;
            };
            entity.push(QuotaEntity { entity_type, name });
        }
        entity.sort_by_key(|part| part.entity_type);

        let mut values = QuotaValues::default();
        for (key, value) in described.values {
            values.set(&key, value);
        }
        Some(Self { entity, values })
    }
}

fn is_cluster_authorization_text(message: &str) -> bool {
    let lower = message.to_ascii_lowercase();
    lower.contains("clusterauthorizationfailed") || lower.contains("cluster authorization failed")
}

#[cfg(test)]
mod tests;
