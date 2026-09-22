//! Topic and broker config rows share this wire shape.

use serde::{Deserialize, Serialize};
use ts_rs::TS;

use crate::kafka::model as domain;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
#[allow(clippy::enum_variant_names)]
pub(crate) enum ConfigSource {
    DynamicTopicConfig,
    DynamicBrokerConfig,
    StaticBrokerConfig,
    DefaultConfig,
}

impl From<domain::ConfigSource> for ConfigSource {
    fn from(source: domain::ConfigSource) -> Self {
        match source {
            domain::ConfigSource::DynamicTopic => Self::DynamicTopicConfig,
            domain::ConfigSource::DynamicBroker => Self::DynamicBrokerConfig,
            domain::ConfigSource::StaticBroker => Self::StaticBrokerConfig,
            domain::ConfigSource::Default => Self::DefaultConfig,
        }
    }
}

#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ConfigEntry {
    pub name: String,
    pub value: Option<String>,
    pub source: ConfigSource,
    pub read_only: bool,
    pub sensitive: bool,
}

impl From<domain::ConfigEntry> for ConfigEntry {
    fn from(entry: domain::ConfigEntry) -> Self {
        Self {
            name: entry.name,
            value: entry.value,
            source: entry.source.into(),
            read_only: entry.read_only,
            sensitive: entry.sensitive,
        }
    }
}
