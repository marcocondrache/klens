use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

/// Where a config value comes from, the source that wins first.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum ConfigSource {
    DynamicTopic,
    DynamicBroker,
    DynamicDefaultBroker,
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

/// Overrides to set, and overrides to drop so the value falls back to the
/// level below.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ConfigEdit {
    pub set: BTreeMap<String, String>,
    pub reset: BTreeSet<String>,
}

impl ConfigEdit {
    /// Whether `entries` read back every change made at `source`. A value
    /// from a source that wins over it hides the change, which then counts as
    /// shown.
    pub fn shows_in(&self, entries: &[ConfigEntry], source: ConfigSource) -> bool {
        let entry = |name: &str| entries.iter().find(|entry| entry.name == name);
        self.set.iter().all(|(name, value)| {
            entry(name).is_some_and(|entry| {
                entry.source < source
                    || entry.source == source
                        && (entry.sensitive || entry.value.as_deref() == Some(value))
            })
        }) && self
            .reset
            .iter()
            .all(|name| entry(name).is_none_or(|entry| entry.source != source))
    }
}

/// The brokers a config edit applies to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BrokerScope {
    Broker(i32),
    /// The default every broker without a value of its own follows.
    Cluster,
}

impl BrokerScope {
    /// The source a value set here reads back with.
    pub fn source(self) -> ConfigSource {
        match self {
            Self::Broker(_) => ConfigSource::DynamicBroker,
            Self::Cluster => ConfigSource::DynamicDefaultBroker,
        }
    }
}

impl fmt::Display for BrokerScope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Broker(id) => write!(f, "broker {id}"),
            Self::Cluster => f.write_str("every broker"),
        }
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
    use std::slice;

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

        assert!(set.shows_in(
            &[entry("retention.ms", "60000", ConfigSource::DynamicTopic)],
            ConfigSource::DynamicTopic
        ));
        assert!(!set.shows_in(
            &[entry("retention.ms", "1", ConfigSource::DynamicTopic)],
            ConfigSource::DynamicTopic
        ));
        assert!(!set.shows_in(
            &[entry("retention.ms", "60000", ConfigSource::Default)],
            ConfigSource::DynamicTopic
        ));
        assert!(!set.shows_in(&[], ConfigSource::DynamicTopic));

        let mut hidden = entry("sasl.jaas.config", "", ConfigSource::DynamicTopic);
        hidden.value = None;
        hidden.sensitive = true;
        assert!(
            edit(&[("sasl.jaas.config", "secret")], &[])
                .shows_in(&[hidden], ConfigSource::DynamicTopic)
        );
    }

    #[test]
    fn a_reset_shows_once_the_topic_stops_overriding() {
        let reset = edit(&[], &["cleanup.policy"]);

        assert!(reset.shows_in(
            &[entry("cleanup.policy", "delete", ConfigSource::Default)],
            ConfigSource::DynamicTopic
        ));
        assert!(reset.shows_in(&[], ConfigSource::DynamicTopic));
        assert!(!reset.shows_in(
            &[entry(
                "cleanup.policy",
                "compact",
                ConfigSource::DynamicTopic
            )],
            ConfigSource::DynamicTopic
        ));
    }

    #[test]
    fn an_edit_shows_only_when_every_change_does() {
        let both = edit(&[("retention.ms", "60000")], &["cleanup.policy"]);
        let set = entry("retention.ms", "60000", ConfigSource::DynamicTopic);
        let kept = entry("cleanup.policy", "compact", ConfigSource::DynamicTopic);

        assert!(both.shows_in(slice::from_ref(&set), ConfigSource::DynamicTopic));
        assert!(!both.shows_in(&[set, kept.clone()], ConfigSource::DynamicTopic));
        assert!(!both.shows_in(&[kept], ConfigSource::DynamicTopic));
    }

    #[test]
    fn a_value_from_a_winning_source_hides_a_change_beneath_it() {
        let set = edit(&[("log.retention.ms", "60000")], &[]);
        let reset = edit(&[], &["log.retention.ms"]);
        let own = entry("log.retention.ms", "1", ConfigSource::DynamicBroker);
        let shared = entry(
            "log.retention.ms",
            "60000",
            ConfigSource::DynamicDefaultBroker,
        );
        let fixed = entry("log.retention.ms", "60000", ConfigSource::StaticBroker);

        assert!(set.shows_in(slice::from_ref(&shared), ConfigSource::DynamicDefaultBroker));
        assert!(set.shows_in(slice::from_ref(&own), ConfigSource::DynamicDefaultBroker));
        assert!(!set.shows_in(slice::from_ref(&fixed), ConfigSource::DynamicDefaultBroker));
        assert!(!set.shows_in(slice::from_ref(&shared), ConfigSource::DynamicBroker));

        assert!(reset.shows_in(&[own], ConfigSource::DynamicDefaultBroker));
        assert!(reset.shows_in(&[fixed], ConfigSource::DynamicDefaultBroker));
        assert!(!reset.shows_in(&[shared], ConfigSource::DynamicDefaultBroker));
    }

    #[test]
    fn a_broker_scope_reads_back_at_its_own_source() {
        assert_eq!(BrokerScope::Broker(1).source(), ConfigSource::DynamicBroker);
        assert_eq!(
            BrokerScope::Cluster.source(),
            ConfigSource::DynamicDefaultBroker
        );
        assert_eq!(BrokerScope::Broker(1).to_string(), "broker 1");
        assert_eq!(BrokerScope::Cluster.to_string(), "every broker");
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
