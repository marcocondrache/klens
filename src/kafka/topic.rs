use std::collections::HashMap;

use crate::kafka::group::GroupSnapshot;
use crate::kafka::metadata::TopicMetadata;
use crate::kafka::topic_config::{CleanupPolicy, ConfigEntry, topic_config_values};
use crate::kafka::watermarks::Watermarks;

/// A partition with its watermarks resolved.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Partition {
    pub id: i32,
    pub leader: i32,
    pub replicas: Vec<i32>,
    pub isr: Vec<i32>,
    pub low_watermark: i64,
    pub high_watermark: i64,
}

impl Partition {
    pub fn available(&self) -> i64 {
        (self.high_watermark - self.low_watermark).max(0)
    }

    pub fn under_replicated(&self) -> bool {
        self.isr.len() < self.replicas.len()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Topic {
    pub name: String,
    pub internal: bool,
    pub partitions: Vec<Partition>,
    pub replication_factor: i32,
    pub message_count: u64,
    pub cleanup_policy: CleanupPolicy,
    pub retention_ms: i64,
    pub consumer_groups: Vec<String>,
    pub under_replicated: bool,
}

impl Topic {
    pub fn assemble(
        topic: &TopicMetadata,
        watermarks: &HashMap<i32, Watermarks>,
        config: Option<&[ConfigEntry]>,
        consumer_groups: Vec<String>,
    ) -> Self {
        let partitions: Vec<Partition> = topic
            .partitions
            .iter()
            .map(|partition| {
                partition
                    .with_watermarks(watermarks.get(&partition.id).copied().unwrap_or_default())
            })
            .collect();

        let replication_factor = partitions
            .first()
            .map(|partition| partition.replicas.len() as i32)
            .unwrap_or(0);
        let message_count = partitions
            .iter()
            .map(|partition| partition.available() as u64)
            .sum();
        let under_replicated = partitions.iter().any(Partition::under_replicated);
        let (cleanup_policy, retention_ms) = topic_config_values(config);

        Self {
            name: topic.name.clone(),
            internal: topic.internal,
            partitions,
            replication_factor,
            message_count,
            cleanup_policy,
            retention_ms,
            consumer_groups,
            under_replicated,
        }
    }

    pub fn with_watermarks(&self, watermarks: &HashMap<i32, Watermarks>) -> Self {
        let partitions: Vec<Partition> = self
            .partitions
            .iter()
            .map(|partition| {
                let marks = watermarks
                    .get(&partition.id)
                    .copied()
                    .unwrap_or(Watermarks {
                        low: partition.low_watermark,
                        high: partition.high_watermark,
                    });
                Partition {
                    id: partition.id,
                    leader: partition.leader,
                    replicas: partition.replicas.clone(),
                    isr: partition.isr.clone(),
                    low_watermark: marks.low,
                    high_watermark: marks.high,
                }
            })
            .collect();
        let message_count = partitions
            .iter()
            .map(|partition| partition.available() as u64)
            .sum();
        let under_replicated = partitions.iter().any(Partition::under_replicated);
        Self {
            name: self.name.clone(),
            internal: self.internal,
            partitions,
            replication_factor: self.replication_factor,
            message_count,
            cleanup_policy: self.cleanup_policy,
            retention_ms: self.retention_ms,
            consumer_groups: self.consumer_groups.clone(),
            under_replicated,
        }
    }

    pub fn with_config(&self, config: Option<&[ConfigEntry]>) -> Self {
        let Some(entries) = config else {
            return self.clone();
        };
        let (cleanup_policy, retention_ms) = topic_config_values(Some(entries));
        Self {
            cleanup_policy,
            retention_ms,
            ..self.clone()
        }
    }
}

pub fn groups_for_topic(topic: &str, groups: &[GroupSnapshot]) -> Vec<String> {
    groups
        .iter()
        .filter(|group| group.consumes_topic(topic))
        .map(|group| group.id.clone())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kafka::group::{CommittedOffset, GroupMember, GroupState, MemberAssignment};

    #[test]
    fn groups_for_topic_does_not_need_a_materialized_topic_list() {
        let assigned = GroupSnapshot {
            id: "assigned".into(),
            state: GroupState::Stable,
            protocol: "range".into(),
            coordinator: 1,
            members: vec![GroupMember {
                id: "m1".into(),
                client_id: "c1".into(),
                host: "127.0.0.1".into(),
                assignments: vec![MemberAssignment {
                    topic: "orders".into(),
                    partitions: vec![0],
                }],
            }],
            committed: Vec::new(),
        };
        let committed = GroupSnapshot {
            id: "committed".into(),
            state: GroupState::Stable,
            protocol: "range".into(),
            coordinator: 1,
            members: Vec::new(),
            committed: vec![CommittedOffset {
                topic: "orders".into(),
                partition: 0,
                offset: 1,
            }],
        };
        let other = GroupSnapshot {
            id: "other".into(),
            state: GroupState::Empty,
            protocol: String::new(),
            coordinator: 1,
            members: vec![GroupMember {
                id: "m2".into(),
                client_id: "c2".into(),
                host: "127.0.0.1".into(),
                assignments: vec![MemberAssignment {
                    topic: "payments".into(),
                    partitions: vec![0],
                }],
            }],
            committed: Vec::new(),
        };

        assert_eq!(
            groups_for_topic("orders", &[assigned, committed, other]),
            vec!["assigned", "committed"]
        );
    }

    #[test]
    fn assemble_folds_watermarks_and_config_into_the_topic() {
        let meta = TopicMetadata {
            name: "orders".into(),
            internal: false,
            partitions: vec![
                crate::kafka::metadata::PartitionMetadata {
                    id: 0,
                    leader: 1,
                    replicas: vec![1, 2],
                    isr: vec![1, 2],
                },
                crate::kafka::metadata::PartitionMetadata {
                    id: 1,
                    leader: 2,
                    replicas: vec![1, 2],
                    isr: vec![2],
                },
            ],
        };
        let watermarks = HashMap::from([
            (0, Watermarks { low: 0, high: 10 }),
            (1, Watermarks { low: 4, high: 9 }),
        ]);
        let config = [ConfigEntry {
            name: "cleanup.policy".into(),
            value: Some("compact".into()),
            source: crate::kafka::topic_config::ConfigSource::DynamicTopic,
            read_only: false,
            sensitive: false,
        }];

        let topic = Topic::assemble(&meta, &watermarks, Some(&config), vec!["g1".into()]);
        assert_eq!(topic.replication_factor, 2);
        assert_eq!(topic.message_count, 15, "10 available + 5 available");
        assert!(topic.under_replicated, "partition 1 has a shrunken isr");
        assert_eq!(topic.cleanup_policy, CleanupPolicy::Compact);
        assert_eq!(topic.consumer_groups, vec!["g1"]);

        let patched = topic.with_watermarks(&HashMap::from([
            (0, Watermarks { low: 0, high: 20 }),
            (1, Watermarks { low: 4, high: 9 }),
        ]));
        assert_eq!(patched.message_count, 25);
        assert_eq!(patched.cleanup_policy, CleanupPolicy::Compact);
        assert_eq!(patched.consumer_groups, vec!["g1"]);
        assert_eq!(patched.partitions[0].high_watermark, 20);

        let compact_delete = [ConfigEntry {
            name: "cleanup.policy".into(),
            value: Some("compact,delete".into()),
            source: crate::kafka::topic_config::ConfigSource::DynamicTopic,
            read_only: false,
            sensitive: false,
        }];
        let retargeted = patched.with_config(Some(&compact_delete));
        assert_eq!(retargeted.cleanup_policy, CleanupPolicy::CompactDelete);
        assert_eq!(retargeted.message_count, 25);
        assert_eq!(
            patched.with_config(None).cleanup_policy,
            CleanupPolicy::Compact
        );
    }

    #[test]
    fn partitions_without_watermarks_default_to_empty() {
        let meta = TopicMetadata {
            name: "orders".into(),
            internal: false,
            partitions: vec![crate::kafka::metadata::PartitionMetadata {
                id: 0,
                leader: 1,
                replicas: vec![1],
                isr: vec![1],
            }],
        };

        let topic = Topic::assemble(&meta, &HashMap::new(), None, Vec::new());
        assert_eq!(topic.message_count, 0);
        assert_eq!(topic.partitions[0].available(), 0);
    }
}
