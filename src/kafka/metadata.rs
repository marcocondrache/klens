#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataSnapshot {
    pub cluster_id: Option<String>,
    pub brokers: Vec<BrokerMetadata>,
    pub topics: Vec<TopicMetadata>,
}

impl MetadataSnapshot {
    pub fn topic(&self, name: &str) -> Option<&TopicMetadata> {
        self.topics.iter().find(|topic| topic.name == name)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrokerMetadata {
    pub id: i32,
    pub host: String,
    pub port: i32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicMetadata {
    pub name: String,
    pub internal: bool,
    pub partitions: Vec<PartitionMetadata>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionMetadata {
    pub id: i32,
    pub leader: i32,
    pub replicas: Vec<i32>,
    pub isr: Vec<i32>,
}

impl PartitionMetadata {
    pub fn under_replicated(&self) -> bool {
        self.isr.len() < self.replicas.len()
    }

    pub fn offline(&self) -> bool {
        self.leader < 0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Watermarks {
    pub low: i64,
    pub high: i64,
}

pub fn is_internal_topic(name: &str) -> bool {
    name.starts_with('_') || name.starts_with('.')
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kafka::topic_config::{ConfigEntry, ConfigSource};

    #[test]
    fn topic_looks_up_by_name() {
        let meta = MetadataSnapshot {
            cluster_id: None,
            brokers: Vec::new(),
            topics: vec![TopicMetadata {
                name: "orders.created".into(),
                internal: false,
                partitions: Vec::new(),
            }],
        };
        assert_eq!(meta.topic("orders.created"), Some(&meta.topics[0]));
        assert!(meta.topic("missing").is_none());
    }

    #[test]
    fn config_lookup_reads_entries_by_name() {
        let entries = [ConfigEntry {
            name: "cleanup.policy".into(),
            value: Some("compact".into()),
            source: ConfigSource::Default,
            read_only: false,
            sensitive: false,
        }];
        assert_eq!(
            ConfigEntry::lookup(&entries, "cleanup.policy"),
            Some("compact")
        );
        assert_eq!(ConfigEntry::lookup(&entries, "retention.ms"), None);
    }
}
