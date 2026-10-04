use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use ts_rs::TS;

use crate::kafka::model as domain;

use super::error::ApiError;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
#[allow(clippy::enum_variant_names)]
pub enum ConfigSource {
    DynamicTopicConfig,
    DynamicBrokerConfig,
    DynamicDefaultBrokerConfig,
    StaticBrokerConfig,
    DefaultConfig,
}

impl From<domain::ConfigSource> for ConfigSource {
    fn from(source: domain::ConfigSource) -> Self {
        match source {
            domain::ConfigSource::DynamicTopic => Self::DynamicTopicConfig,
            domain::ConfigSource::DynamicBroker => Self::DynamicBrokerConfig,
            domain::ConfigSource::DynamicDefaultBroker => Self::DynamicDefaultBrokerConfig,
            domain::ConfigSource::StaticBroker => Self::StaticBrokerConfig,
            domain::ConfigSource::Default => Self::DefaultConfig,
        }
    }
}

#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct ConfigEntry {
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

#[derive(Debug, Clone, Deserialize, TS)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct EditConfigs {
    /// Overrides to write, by config name.
    #[serde(default)]
    pub set: BTreeMap<String, String>,
    /// Overrides to drop, so each falls back to the level below.
    #[serde(default)]
    pub reset: BTreeSet<String>,
}

impl EditConfigs {
    pub(crate) fn into_edit(self) -> Result<domain::ConfigEdit, ApiError> {
        if self.set.is_empty() && self.reset.is_empty() {
            return Err(ApiError::unprocessable("name a config to set or reset"));
        }
        if let Some(name) = self.reset.iter().find(|name| self.set.contains_key(*name)) {
            return Err(ApiError::unprocessable(format!(
                "{name} cannot be both set and reset"
            )));
        }
        Ok(domain::ConfigEdit {
            set: self.set,
            reset: self.reset,
        })
    }
}
