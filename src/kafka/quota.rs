use std::fmt::{self, Display, Formatter};

use itertools::Itertools as _;
use tracing::warn;

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    strum::EnumString,
    strum::IntoStaticStr,
)]
#[strum(serialize_all = "kebab-case")]
pub enum QuotaEntityType {
    User,
    ClientId,
    Ip,
}

impl QuotaEntityType {
    pub fn wire(self) -> &'static str {
        self.into()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuotaEntity {
    pub entity_type: QuotaEntityType,
    pub name: Option<String>,
}

impl Display for QuotaEntity {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let name = self.name.as_deref().unwrap_or("<default>");
        write!(f, "{}={name}", self.entity_type.wire())
    }
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

    pub fn entries(&self) -> [(&'static str, Option<f64>); 5] {
        [
            ("producer_byte_rate", self.producer_byte_rate),
            ("consumer_byte_rate", self.consumer_byte_rate),
            ("request_percentage", self.request_percentage),
            ("controller_mutation_rate", self.controller_mutation_rate),
            ("connection_creation_rate", self.connection_creation_rate),
        ]
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ClientQuota {
    pub entity: Vec<QuotaEntity>,
    pub values: QuotaValues,
}

impl Display for ClientQuota {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let values = self
            .values
            .entries()
            .into_iter()
            .filter_map(|(key, value)| value.map(|value| format!("{key}={value}")))
            .join(" ");
        write!(f, "{}: ", self.entity.iter().join(" "))?;
        f.write_str(if values.is_empty() { "none" } else { &values })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum QuotaListing {
    Described(Vec<ClientQuota>),
    Denied,
}

pub struct DescribedQuota {
    pub entity: Vec<(String, Option<String>)>,
    pub values: Vec<(String, f64)>,
}

impl QuotaListing {
    pub fn values(&self, entity: &[QuotaEntity]) -> Option<&QuotaValues> {
        match self {
            Self::Described(quotas) => quotas
                .iter()
                .find(|quota| quota.entity == entity)
                .map(|quota| &quota.values),
            Self::Denied => None,
        }
    }

    pub fn from_describe(cluster: &str, entries: impl IntoIterator<Item = DescribedQuota>) -> Self {
        Self::Described(
            entries
                .into_iter()
                .filter_map(|entry| ClientQuota::from_described(cluster, entry))
                .collect(),
        )
    }
}

impl ClientQuota {
    fn from_described(cluster: &str, described: DescribedQuota) -> Option<Self> {
        let mut entity = Vec::with_capacity(described.entity.len());
        for (entity_type, name) in described.entity {
            let Ok(entity_type) = entity_type.parse::<QuotaEntityType>() else {
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

#[cfg(test)]
mod tests;
