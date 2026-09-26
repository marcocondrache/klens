//! Collected `Vec`s are shrunk because `collect` reuses the wider krafka
//! allocation in place, and the store keeps them for as long as they live.

use krafka::admin::{
    ConfigEntry as KrafkaConfigEntry, ConsumerGroupDescription, ConsumerGroupMember,
    GroupOffsetEntry, TopicPartitionAssignment,
};
use krafka::metadata::{ClusterMetadata, TopicInfo as KrafkaTopicInfo};
use krafka::protocol::validate_topic_name;

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
        let mut members: Vec<GroupMember> = description
            .members
            .into_iter()
            .map(GroupMember::from_krafka)
            .collect();
        members.shrink_to_fit();

        Self {
            id: description.group_id,
            state: GroupState::parse(&description.state),
            protocol: description.assignor.unwrap_or_default(),
            coordinator: 0,
            members,
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
    let mut assignments: Vec<MemberAssignment> = assigned
        .into_iter()
        .filter(|assignment| !assignment.topic_name.is_empty())
        .map(|assignment| MemberAssignment {
            topic: assignment.topic_name,
            partitions: assignment.partitions,
        })
        .collect();
    assignments.shrink_to_fit();
    assignments
}

pub(super) fn committed_from_krafka(entries: Vec<GroupOffsetEntry>) -> Vec<CommittedOffset> {
    let mut committed: Vec<CommittedOffset> = entries
        .into_iter()
        .filter_map(|entry| committed_offset(entry.topic, entry.partition, entry.committed_offset))
        .collect();
    committed.shrink_to_fit();
    committed
}

fn committed_offset(topic: String, partition: i32, offset: i64) -> Option<CommittedOffset> {
    (offset >= 0 && validate_topic_name(&topic).is_ok()).then_some(CommittedOffset {
        topic,
        partition,
        offset,
    })
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
    fn from_krafka(source: i8) -> Self {
        match source {
            1 => Self::DynamicTopic,
            2 | 3 => Self::DynamicBroker,
            4 => Self::StaticBroker,
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
        assert_eq!(ConfigSource::from_krafka(2), ConfigSource::DynamicBroker);
        assert_eq!(ConfigSource::from_krafka(3), ConfigSource::DynamicBroker);
        assert_eq!(ConfigSource::from_krafka(4), ConfigSource::StaticBroker);
        assert_eq!(ConfigSource::from_krafka(5), ConfigSource::Default);
        assert_eq!(ConfigSource::from_krafka(0), ConfigSource::Default);
        assert_eq!(ConfigSource::from_krafka(6), ConfigSource::Default);
        assert_eq!(ConfigSource::from_krafka(-1), ConfigSource::Default);
    }

    #[test]
    fn committed_offsets_drop_unset_offsets_and_illegal_topic_names() {
        assert_eq!(
            committed_offset("orders".into(), 1, 0),
            Some(CommittedOffset {
                topic: "orders".into(),
                partition: 1,
                offset: 0,
            })
        );
        assert_eq!(committed_offset("orders".into(), 1, -1), None);
        assert_eq!(committed_offset(String::new(), 1, 7), None);
        assert_eq!(committed_offset("bad/name".into(), 1, 7), None);
    }
}
