//! The single place krafka types cross into the Kafka domain model.

use krafka::admin::{
    ConfigEntry as KrafkaConfigEntry, ConsumerGroupDescription, ConsumerGroupMember,
    GroupOffsetEntry, TopicPartitionAssignment,
};
use krafka::metadata::{ClusterMetadata, TopicInfo as KrafkaTopicInfo};

use crate::kafka::group::{
    CommittedOffset, GroupMember, GroupSnapshot, GroupState, MemberAssignment,
};
use crate::kafka::metadata::{
    BrokerMetadata, MetadataSnapshot, PartitionMetadata, TopicMetadata, is_internal_topic,
};
use crate::kafka::topic_config::{ConfigEntry, ConfigSource};

impl MetadataSnapshot {
    pub(super) fn from_krafka(cache: &ClusterMetadata) -> Self {
        Self {
            cluster_id: cache.cluster_id(),
            brokers: cache
                .brokers()
                .into_iter()
                .map(|broker| BrokerMetadata {
                    id: broker.id(),
                    host: broker.host().to_owned(),
                    port: broker.port(),
                })
                .collect(),
            topics: cache
                .topics()
                .into_iter()
                .map(TopicMetadata::from_krafka)
                .collect(),
        }
    }
}

impl TopicMetadata {
    pub(super) fn from_krafka(topic: KrafkaTopicInfo) -> Self {
        let mut partitions: Vec<PartitionMetadata> = topic
            .partitions
            .into_values()
            .map(|partition| PartitionMetadata {
                id: partition.partition,
                leader: partition.leader,
                replicas: partition.replicas,
                isr: partition.isr,
            })
            .collect();
        partitions.sort_unstable_by_key(|partition| partition.id);

        Self {
            internal: topic.is_internal || is_internal_topic(&topic.name),
            name: topic.name,
            partitions,
        }
    }
}

impl GroupSnapshot {
    pub(super) fn from_krafka(description: ConsumerGroupDescription) -> Self {
        Self {
            id: description.group_id,
            state: GroupState::parse(&description.state),
            protocol: description.assignor.unwrap_or_default(),
            coordinator: 0,
            members: description
                .members
                .into_iter()
                .map(GroupMember::from_krafka)
                .collect(),
            committed: Vec::new(),
        }
    }
}

impl GroupMember {
    fn from_krafka(member: ConsumerGroupMember) -> Self {
        Self {
            id: member.member_id,
            client_id: member.client_id,
            host: member.client_host.trim_start_matches('/').to_owned(),
            assignments: member
                .assignment
                .map(assignments_from_krafka)
                .unwrap_or_default(),
        }
    }
}

fn assignments_from_krafka(assigned: Vec<TopicPartitionAssignment>) -> Vec<MemberAssignment> {
    assigned
        .into_iter()
        .filter(|assignment| !assignment.topic_name.is_empty())
        .map(|assignment| MemberAssignment {
            topic: assignment.topic_name,
            partitions: assignment.partitions,
        })
        .collect()
}

pub(super) fn committed_from_krafka(entries: Vec<GroupOffsetEntry>) -> Vec<CommittedOffset> {
    entries
        .into_iter()
        .filter_map(|entry| {
            (entry.committed_offset >= 0).then_some(CommittedOffset {
                topic: entry.topic,
                partition: entry.partition,
                offset: entry.committed_offset,
            })
        })
        .collect()
}

impl From<KrafkaConfigEntry> for ConfigEntry {
    fn from(entry: KrafkaConfigEntry) -> Self {
        Self {
            name: entry.name,
            value: entry.value,
            source: ConfigSource::from_krafka(entry.config_source),
            read_only: entry.read_only,
            sensitive: entry.is_sensitive,
        }
    }
}

impl ConfigSource {
    /// Kafka `DescribeConfigs` `config_source` (v1+).
    fn from_krafka(source: i8) -> Self {
        match source {
            1 => Self::DynamicTopic,
            3 => Self::DynamicBroker,
            5 => Self::StaticBroker,
            _ => Self::Default,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_source_maps_kafka_describe_codes() {
        assert_eq!(ConfigSource::from_krafka(1), ConfigSource::DynamicTopic);
        assert_eq!(ConfigSource::from_krafka(3), ConfigSource::DynamicBroker);
        assert_eq!(ConfigSource::from_krafka(5), ConfigSource::StaticBroker);
        assert_eq!(ConfigSource::from_krafka(0), ConfigSource::Default);
        assert_eq!(ConfigSource::from_krafka(6), ConfigSource::Default);
        assert_eq!(ConfigSource::from_krafka(-1), ConfigSource::Default);
    }
}
