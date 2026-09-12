use crate::kafka::topic::Partition;
use crate::kafka::watermarks::Watermarks;

/// Raw cluster metadata as reported by the broker, before watermarks,
/// configs or consumer groups are folded in.
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

    pub fn broker(&self, id: i32) -> Option<&BrokerMetadata> {
        self.brokers.iter().find(|broker| broker.id == id)
    }

    pub fn topic_names(&self) -> Vec<&str> {
        self.topics
            .iter()
            .map(|topic| topic.name.as_str())
            .collect()
    }

    pub fn topic_partition_pairs<S: AsRef<str>>(&self, names: &[S]) -> Vec<(String, i32)> {
        names
            .iter()
            .flat_map(|name| {
                let name = name.as_ref();
                self.topic(name).into_iter().flat_map(|topic| {
                    topic
                        .partitions
                        .iter()
                        .map(|partition| (name.to_owned(), partition.id))
                })
            })
            .collect()
    }

    pub fn partitions(&self) -> impl Iterator<Item = &PartitionMetadata> {
        self.topics.iter().flat_map(|topic| topic.partitions.iter())
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

impl TopicMetadata {
    pub fn partition(&self, id: i32) -> Option<&PartitionMetadata> {
        self.partitions.iter().find(|partition| partition.id == id)
    }

    pub fn partition_ids(&self) -> Vec<i32> {
        self.partitions
            .iter()
            .map(|partition| partition.id)
            .collect()
    }
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

    pub fn with_watermarks(&self, marks: Watermarks) -> Partition {
        Partition {
            id: self.id,
            leader: self.leader,
            replicas: self.replicas.clone(),
            isr: self.isr.clone(),
            low_watermark: marks.low,
            high_watermark: marks.high,
        }
    }
}

pub fn is_internal_topic(name: &str) -> bool {
    name.starts_with('_') || name.starts_with('.')
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kafka::topic_config::{ConfigEntry, ConfigSource};

    fn snapshot() -> MetadataSnapshot {
        MetadataSnapshot {
            cluster_id: None,
            brokers: Vec::new(),
            topics: vec![TopicMetadata {
                name: "orders.created".into(),
                internal: false,
                partitions: vec![
                    PartitionMetadata {
                        id: 0,
                        leader: 1,
                        replicas: vec![1],
                        isr: vec![1],
                    },
                    PartitionMetadata {
                        id: 2,
                        leader: 1,
                        replicas: vec![1],
                        isr: vec![1],
                    },
                ],
            }],
        }
    }

    #[test]
    fn topic_partition_pairs_skips_unknown_topics() {
        let meta = snapshot();
        assert_eq!(
            meta.topic("orders.created").unwrap().partition_ids(),
            vec![0, 2]
        );
        assert_eq!(
            meta.topic_partition_pairs(&["missing", "orders.created"]),
            vec![("orders.created".into(), 0), ("orders.created".into(), 2),]
        );
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
