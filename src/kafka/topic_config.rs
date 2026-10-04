use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigSource {
    DynamicTopic,
    DynamicBroker,
    StaticBroker,
    Default,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigEntry {
    pub name: String,
    pub value: Option<String>,
    pub source: ConfigSource,
    pub read_only: bool,
    pub sensitive: bool,
}

/// Overrides to set on a topic, and overrides to drop so the topic follows
/// the broker again.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ConfigEdit {
    pub set: BTreeMap<String, String>,
    pub reset: BTreeSet<String>,
}

impl ConfigEdit {
    pub fn shows_in(&self, entries: &[ConfigEntry]) -> bool {
        let entry = |name: &str| entries.iter().find(|entry| entry.name == name);
        self.set.iter().all(|(name, value)| {
            entry(name).is_some_and(|entry| {
                entry.source == ConfigSource::DynamicTopic
                    && (entry.sensitive || entry.value.as_deref() == Some(value))
            })
        }) && self
            .reset
            .iter()
            .all(|name| entry(name).is_none_or(|entry| entry.source != ConfigSource::DynamicTopic))
    }
}

impl ConfigEntry {
    pub fn lookup<'a>(entries: &'a [Self], name: &str) -> Option<&'a str> {
        entries
            .iter()
            .find(|entry| entry.name == name)
            .and_then(|entry| entry.value.as_deref())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CleanupPolicy {
    Delete,
    Compact,
    CompactDelete,
}

impl CleanupPolicy {
    pub fn parse(value: &str) -> Self {
        let mut compact = false;
        let mut delete = false;

        for part in value.split(',') {
            match part.trim() {
                "compact" => compact = true,
                "delete" => delete = true,
                _ => {}
            }
        }

        match (compact, delete) {
            (true, true) => Self::CompactDelete,
            (true, false) => Self::Compact,
            _ => Self::Delete,
        }
    }
}

pub fn topic_config_values(entries: Option<&[ConfigEntry]>) -> (CleanupPolicy, Option<i64>) {
    let entries = entries.unwrap_or(&[]);
    let cleanup_policy = ConfigEntry::lookup(entries, "cleanup.policy")
        .map(CleanupPolicy::parse)
        .unwrap_or(CleanupPolicy::Delete);
    let retention_ms =
        ConfigEntry::lookup(entries, "retention.ms").and_then(|value| value.parse().ok());
    (cleanup_policy, retention_ms)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(name: &str, value: &str, source: ConfigSource) -> ConfigEntry {
        ConfigEntry {
            name: name.to_owned(),
            value: Some(value.to_owned()),
            source,
            read_only: false,
            sensitive: false,
        }
    }

    fn edit(set: &[(&str, &str)], reset: &[&str]) -> ConfigEdit {
        ConfigEdit {
            set: set
                .iter()
                .map(|(name, value)| ((*name).to_owned(), (*value).to_owned()))
                .collect(),
            reset: reset.iter().map(|name| (*name).to_owned()).collect(),
        }
    }

    #[test]
    fn a_set_shows_once_the_topic_overrides_that_value() {
        let set = edit(&[("retention.ms", "60000")], &[]);

        assert!(set.shows_in(&[entry("retention.ms", "60000", ConfigSource::DynamicTopic)]));
        assert!(!set.shows_in(&[entry("retention.ms", "1", ConfigSource::DynamicTopic)]));
        assert!(!set.shows_in(&[entry("retention.ms", "60000", ConfigSource::Default)]));
        assert!(!set.shows_in(&[]));

        let mut hidden = entry("sasl.jaas.config", "", ConfigSource::DynamicTopic);
        hidden.value = None;
        hidden.sensitive = true;
        assert!(edit(&[("sasl.jaas.config", "secret")], &[]).shows_in(&[hidden]));
    }

    #[test]
    fn a_reset_shows_once_the_topic_stops_overriding() {
        let reset = edit(&[], &["cleanup.policy"]);

        assert!(reset.shows_in(&[entry("cleanup.policy", "delete", ConfigSource::Default)]));
        assert!(reset.shows_in(&[]));
        assert!(!reset.shows_in(&[entry(
            "cleanup.policy",
            "compact",
            ConfigSource::DynamicTopic
        )]));
    }

    #[test]
    fn an_edit_shows_only_when_every_change_does() {
        let both = edit(&[("retention.ms", "60000")], &["cleanup.policy"]);
        let set = entry("retention.ms", "60000", ConfigSource::DynamicTopic);
        let kept = entry("cleanup.policy", "compact", ConfigSource::DynamicTopic);

        assert!(both.shows_in(std::slice::from_ref(&set)));
        assert!(!both.shows_in(&[set, kept.clone()]));
        assert!(!both.shows_in(&[kept]));
    }

    #[test]
    fn cleanup_policy_parses_combined_values() {
        assert_eq!(
            CleanupPolicy::parse("compact,delete"),
            CleanupPolicy::CompactDelete
        );
        assert_eq!(CleanupPolicy::parse("compact"), CleanupPolicy::Compact);
        assert_eq!(CleanupPolicy::parse("delete"), CleanupPolicy::Delete);
    }

    #[test]
    fn topic_config_values_default_to_delete_and_unknown_retention() {
        assert_eq!(
            topic_config_values(None),
            (CleanupPolicy::Delete, None),
            "no config reported"
        );

        let entries = [ConfigEntry {
            name: "retention.ms".into(),
            value: Some("604800000".into()),
            source: ConfigSource::DynamicTopic,
            read_only: false,
            sensitive: false,
        }];
        assert_eq!(
            topic_config_values(Some(&entries)),
            (CleanupPolicy::Delete, Some(604_800_000))
        );
    }
}
