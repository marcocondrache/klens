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
    /// Parses Kafka's comma-separated `cleanup.policy`.
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

/// Falls back to Kafka's defaults when the broker did not report them.
pub fn topic_config_values(entries: Option<&[ConfigEntry]>) -> (CleanupPolicy, i64) {
    let entries = entries.unwrap_or(&[]);
    let cleanup_policy = ConfigEntry::lookup(entries, "cleanup.policy")
        .map(CleanupPolicy::parse)
        .unwrap_or(CleanupPolicy::Delete);
    let retention_ms = ConfigEntry::lookup(entries, "retention.ms")
        .and_then(|value| value.parse().ok())
        .unwrap_or(0);
    (cleanup_policy, retention_ms)
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn topic_config_values_fall_back_to_kafka_defaults() {
        assert_eq!(
            topic_config_values(None),
            (CleanupPolicy::Delete, 0),
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
            (CleanupPolicy::Delete, 604_800_000)
        );
    }
}
