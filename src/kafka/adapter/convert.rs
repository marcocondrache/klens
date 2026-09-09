//! The single place rdkafka types cross into the Kafka domain model.

use kafka_protocol::messages::consumer_protocol_assignment::ConsumerProtocolAssignment;
use kafka_protocol::protocol::Decodable;
use rdkafka::admin::ConfigSource as RdConfigSource;
use rdkafka::metadata::Metadata;

use crate::kafka::group::{GroupMember, GroupSnapshot, GroupState, MemberAssignment};
use crate::kafka::metadata::{
    BrokerMetadata, MetadataSnapshot, PartitionMetadata, TopicMetadata, is_internal_topic,
};
use crate::kafka::topic_config::{ConfigEntry, ConfigSource};

impl MetadataSnapshot {
    pub(super) fn from_rdkafka(metadata: &Metadata, cluster_id: Option<String>) -> Self {
        Self {
            cluster_id,
            brokers: metadata
                .brokers()
                .iter()
                .map(|broker| BrokerMetadata {
                    id: broker.id(),
                    host: broker.host().to_owned(),
                    port: broker.port(),
                })
                .collect(),
            topics: metadata
                .topics()
                .iter()
                .map(|topic| {
                    let name = topic.name().to_owned();
                    TopicMetadata {
                        internal: is_internal_topic(&name),
                        name,
                        partitions: topic
                            .partitions()
                            .iter()
                            .map(|partition| PartitionMetadata {
                                id: partition.id(),
                                leader: partition.leader(),
                                replicas: partition.replicas().to_vec(),
                                isr: partition.isr().to_vec(),
                            })
                            .collect(),
                    }
                })
                .collect(),
        }
    }
}

impl GroupSnapshot {
    pub(super) fn from_rdkafka(info: &rdkafka::groups::GroupInfo) -> Self {
        Self {
            id: info.name().to_owned(),
            state: GroupState::parse(info.state()),
            protocol: info.protocol().to_owned(),
            coordinator: 0,
            members: info
                .members()
                .iter()
                .map(|member| GroupMember {
                    id: member.id().to_owned(),
                    client_id: member.client_id().to_owned(),
                    host: member.client_host().trim_start_matches('/').to_owned(),
                    assignments: member
                        .assignment()
                        .map(member_assignments)
                        .unwrap_or_default(),
                })
                .collect(),
            committed: Vec::new(),
        }
    }
}

/// Decodes the version-prefixed `ConsumerProtocolAssignment` blob a member
/// publishes. Malformed or truncated blobs yield no assignments rather than
/// failing the whole group listing.
pub(super) fn member_assignments(bytes: &[u8]) -> Vec<MemberAssignment> {
    let Some((version_bytes, rest)) = bytes.split_first_chunk() else {
        return Vec::new();
    };
    let version = i16::from_be_bytes(*version_bytes);
    let mut buf = rest;
    let Ok(assignment) = ConsumerProtocolAssignment::decode(&mut buf, version) else {
        return Vec::new();
    };

    assignment
        .assigned_partitions
        .into_iter()
        .map(|assigned| MemberAssignment {
            topic: assigned.topic.as_str().to_owned(),
            partitions: assigned.partitions,
        })
        .collect()
}

impl From<rdkafka::admin::ConfigEntry> for ConfigEntry {
    fn from(entry: rdkafka::admin::ConfigEntry) -> Self {
        Self {
            name: entry.name,
            value: entry.value,
            source: ConfigSource::from(entry.source),
            read_only: entry.is_read_only,
            sensitive: entry.is_sensitive,
        }
    }
}

impl From<RdConfigSource> for ConfigSource {
    fn from(source: RdConfigSource) -> Self {
        match source {
            RdConfigSource::DynamicTopic => Self::DynamicTopic,
            RdConfigSource::DynamicBroker => Self::DynamicBroker,
            RdConfigSource::StaticBroker => Self::StaticBroker,
            RdConfigSource::Unknown
            | RdConfigSource::DynamicDefaultBroker
            | RdConfigSource::Default => Self::Default,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn encode_assignment(assignment: ConsumerProtocolAssignment) -> Vec<u8> {
        use kafka_protocol::protocol::Encodable;

        let version = 0i16;
        let mut buf = Vec::from(version.to_be_bytes());
        assignment.encode(&mut buf, version).unwrap();
        buf
    }

    #[test]
    fn member_assignments_decodes_version_prefixed_blob() {
        use kafka_protocol::messages::TopicName;
        use kafka_protocol::messages::consumer_protocol_assignment::TopicPartition;
        use kafka_protocol::protocol::StrBytes;

        let bytes = encode_assignment(
            ConsumerProtocolAssignment::default().with_assigned_partitions(vec![
                TopicPartition::default()
                    .with_topic(TopicName(StrBytes::from_static_str("orders.created")))
                    .with_partitions(vec![0, 2]),
                TopicPartition::default()
                    .with_topic(TopicName(StrBytes::from_static_str("payments.captured")))
                    .with_partitions(vec![1]),
            ]),
        );

        assert_eq!(
            member_assignments(&bytes),
            vec![
                MemberAssignment {
                    topic: "orders.created".into(),
                    partitions: vec![0, 2],
                },
                MemberAssignment {
                    topic: "payments.captured".into(),
                    partitions: vec![1],
                },
            ]
        );
    }

    #[test]
    fn member_assignments_ignores_empty_and_truncated_blobs() {
        assert!(member_assignments(&[]).is_empty());
        assert!(member_assignments(&[0, 0, 0]).is_empty());
    }
}
