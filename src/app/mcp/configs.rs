use serde::Serialize;

use crate::app::auth::access::{AccessError, Privilege};
use crate::app::error::ApiError;
use crate::app::resources::configs::ConfigEntry;
use crate::kafka::model as domain;

use super::MAX_CONFIG_CHARS;
use super::reply::{Cut, Lists, Rows};
use super::untrusted::{Boundary, clip};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) enum Section {
    Configs,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct ConfigSection {
    configs: Option<Rows<ConfigRow>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    omitted: Option<Omitted>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Omitted {
    section: Section,
    #[serde(flatten)]
    reason: Reason,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase", rename_all_fields = "camelCase")]
enum Reason {
    Needs(Privilege),
    NotRead { last_error: Option<String> },
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ConfigRow {
    #[serde(flatten)]
    entry: ConfigEntry,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    cut: bool,
}

impl ConfigSection {
    pub fn overrides(entries: Vec<domain::ConfigEntry>) -> Self {
        let overrides = entries
            .into_iter()
            .filter(|entry| entry.source != domain::ConfigSource::Default)
            .map(ConfigRow::new)
            .collect();
        Self {
            configs: Some(Rows::new("configs", overrides)),
            omitted: None,
        }
    }

    pub fn withheld(error: AccessError) -> Result<Self, ApiError> {
        match error {
            AccessError::Forbidden { privilege, .. } => Ok(Self::omitted(Reason::Needs(privilege))),
            error => Err(error.into()),
        }
    }

    pub fn not_read(last_error: Option<&str>, boundary: &Boundary) -> Self {
        Self::omitted(Reason::NotRead {
            last_error: boundary.lane_error(last_error),
        })
    }

    pub fn explained_by_error(&self) -> bool {
        matches!(
            self.omitted,
            Some(Omitted {
                reason: Reason::NotRead {
                    last_error: Some(_)
                },
                ..
            })
        )
    }

    fn omitted(reason: Reason) -> Self {
        Self {
            configs: None,
            omitted: Some(Omitted {
                section: Section::Configs,
                reason,
            }),
        }
    }
}

impl Lists for ConfigSection {
    fn push_lists<'a>(&'a mut self, lists: &mut Vec<&'a mut dyn Cut>) {
        if let Some(configs) = &mut self.configs {
            configs.push_lists(lists);
        }
    }
}

impl ConfigRow {
    fn new(entry: domain::ConfigEntry) -> Self {
        let mut entry = ConfigEntry::from(entry);
        let cut = match &mut entry.value {
            Some(value) => {
                let (kept, cut) = clip(value, MAX_CONFIG_CHARS);
                value.truncate(kept.len());
                cut
            }
            None => false,
        };
        Self { entry, cut }
    }
}
