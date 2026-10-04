//! Collected `Vec`s are shrunk because `collect` reuses the wider krafka
//! allocation in place, and the store keeps them for as long as they live.

use bytes::Bytes;
use std::collections::HashMap;

use krafka::admin::{
    ConfigEntry as KrafkaConfigEntry, ConsumerGroupDescription, ConsumerGroupMember,
    GroupOffsetEntry, LogDirInfo, TopicPartitionAssignment,
};
use krafka::error::{ErrorCode, KrafkaError};
use krafka::metadata::{ClusterMetadata, TopicInfo as KrafkaTopicInfo};
use krafka::producer::ProducerRecord;
use krafka::protocol::{
    AlterConfigOp, AlterableConfig, DescribeConfigsEntry, IncrementalAlterConfigsResponse,
    validate_topic_name,
};

use crate::kafka::error::KafkaError;
use crate::kafka::group::{
    CommittedOffset, GroupMember, GroupSnapshot, GroupState, MemberAssignment,
};
use crate::kafka::metadata::{
    BrokerMetadata, MetadataSnapshot, NewTopic, PartitionMetadata, RecordDeletion, TopicMetadata,
    is_internal_topic,
};
use crate::kafka::produce::NewRecord;
use crate::kafka::storage::{LogDir, ReplicaLog, volume_bytes};
use crate::kafka::topic_config::{ConfigEdit, ConfigEntry, ConfigSource};

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

pub(super) fn refused(error: Option<String>) -> Result<(), KafkaError> {
    error.map_or(Ok(()), |message| Err(KafkaError::Refused(message)))
}

/// DeleteRecords reads this offset as the partition's high watermark.
const HIGH_WATERMARK: i64 = -1;

impl RecordDeletion {
    pub(super) fn to_krafka(&self) -> HashMap<(String, i32), i64> {
        self.before
            .iter()
            .map(|(&partition, before)| {
                (
                    (self.topic.clone(), partition),
                    before.unwrap_or(HIGH_WATERMARK),
                )
            })
            .collect()
    }
}

pub(super) fn produce_refusal(error: KrafkaError) -> KafkaError {
    match error {
        KrafkaError::Broker { code, .. } if !code.is_retriable() => {
            KafkaError::Refused(format!("{code:?}"))
        }
        error => error.into(),
    }
}

impl NewRecord {
    pub(super) fn to_krafka(&self) -> ProducerRecord {
        let mut record = ProducerRecord::new(self.topic.as_str(), Bytes::new());
        record.partition = self.partition;
        record.key = self.key.clone();
        record.value = self.value.clone();
        record.headers = self
            .headers
            .iter()
            .map(|header| (header.key.clone(), Some(Bytes::from(header.value.clone()))))
            .collect();
        record
    }
}

pub(super) fn altered(response: IncrementalAlterConfigsResponse) -> Result<(), KafkaError> {
    response
        .results
        .into_iter()
        .try_for_each(|result| answered(result.error_code, result.error_message))
}

pub(super) fn answered(code: ErrorCode, message: Option<String>) -> Result<(), KafkaError> {
    if code.is_ok() {
        Ok(())
    } else {
        Err(KafkaError::Refused(
            message.unwrap_or_else(|| format!("{code:?}")),
        ))
    }
}

impl ConfigEdit {
    pub(super) fn to_krafka(&self) -> Vec<AlterableConfig> {
        let set = self.set.iter().map(|(name, value)| AlterableConfig {
            name: name.clone(),
            config_operation: AlterConfigOp::Set,
            value: Some(value.clone()),
        });
        let reset = self.reset.iter().map(|name| AlterableConfig {
            name: name.clone(),
            config_operation: AlterConfigOp::Delete,
            value: None,
        });
        set.chain(reset).collect()
    }
}

const BROKER_DEFAULT: i16 = -1;

impl NewTopic {
    pub(super) fn to_krafka(&self) -> Result<krafka::admin::NewTopic, KafkaError> {
        let topic = krafka::admin::NewTopic::new(
            &self.name,
            self.partitions
                .map_or(BROKER_DEFAULT.into(), |count| count.get().into()),
            self.replication_factor
                .map_or(BROKER_DEFAULT, |factor| factor.get().into()),
        )?;
        Ok(self
            .configs
            .iter()
            .fold(topic, |topic, (name, value)| topic.with_config(name, value)))
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

impl LogDir {
    pub(super) fn from_krafka(dir: LogDirInfo) -> Self {
        Self {
            broker: dir.broker_id,
            path: dir.log_dir,
            error: dir.error,
            total_bytes: volume_bytes(dir.total_bytes),
            usable_bytes: volume_bytes(dir.usable_bytes),
            cordoned: dir.is_cordoned,
            replicas: dir
                .topics
                .into_iter()
                .flat_map(|topic| {
                    topic
                        .partitions
                        .into_iter()
                        .map(move |partition| ReplicaLog {
                            topic: topic.name.clone(),
                            partition: partition.partition_index,
                            size_bytes: partition.partition_size,
                            future: partition.is_future_key,
                        })
                })
                .collect(),
        }
    }
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

impl From<DescribeConfigsEntry> for ConfigEntry {
    fn from(entry: DescribeConfigsEntry) -> Self {
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
    use std::collections::BTreeMap;
    use std::num::{NonZeroU8, NonZeroU16};

    use krafka::admin::ConfigResourceType;
    use krafka::error::ErrorCode;
    use krafka::protocol::IncrementalAlterConfigsResult;

    use super::*;
    use crate::kafka::scan::RecordHeader;

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

    #[test]
    fn a_new_topic_leaves_unset_counts_to_the_broker() {
        let topic = NewTopic {
            name: "orders".into(),
            partitions: None,
            replication_factor: None,
            configs: [("cleanup.policy".into(), "compact".into())].into(),
        }
        .to_krafka()
        .expect("valid topic");

        assert_eq!(topic.name, "orders");
        assert_eq!((topic.num_partitions, topic.replication_factor), (-1, -1));
        assert_eq!(topic.configs["cleanup.policy"], "compact");
    }

    #[test]
    fn a_new_topic_sends_the_counts_it_sets() {
        let topic = NewTopic {
            name: "orders".into(),
            partitions: NonZeroU16::new(6),
            replication_factor: NonZeroU8::new(3),
            configs: BTreeMap::new(),
        }
        .to_krafka()
        .expect("valid topic");

        assert_eq!((topic.num_partitions, topic.replication_factor), (6, 3));
        assert!(topic.configs.is_empty());
    }

    #[test]
    fn a_new_record_keeps_a_null_value_null_and_sends_text_headers() {
        let record = NewRecord {
            topic: "orders".into(),
            partition: Some(2),
            key: Some(Bytes::from_static(b"order-1")),
            value: None,
            headers: vec![RecordHeader {
                key: "trace".into(),
                value: "abc".into(),
            }],
        }
        .to_krafka();

        assert_eq!(record.topic, "orders");
        assert_eq!(record.partition, Some(2));
        assert_eq!(record.key.as_deref(), Some(&b"order-1"[..]));
        assert!(record.is_tombstone());
        assert_eq!(
            record.headers,
            vec![("trace".to_owned(), Some(Bytes::from_static(b"abc")))]
        );
    }

    #[test]
    fn a_broker_refusal_names_its_code_but_a_retriable_one_stays_a_client_error() {
        let refused = produce_refusal(KrafkaError::broker(ErrorCode::MessageTooLarge, "batch"));
        assert!(
            matches!(&refused, KafkaError::Refused(code) if code == "MessageTooLarge"),
            "{refused}"
        );

        let retriable = produce_refusal(KrafkaError::broker(ErrorCode::NotEnoughReplicas, "batch"));
        assert!(matches!(retriable, KafkaError::Krafka(_)), "{retriable}");
    }

    #[test]
    fn a_deletion_without_an_offset_reaches_the_high_watermark() {
        let deletion = RecordDeletion {
            topic: "orders".to_owned(),
            before: [(0, Some(42)), (1, None)].into(),
        };

        assert_eq!(
            deletion.to_krafka(),
            HashMap::from([
                (("orders".to_owned(), 0), 42),
                (("orders".to_owned(), 1), -1)
            ])
        );
    }

    #[test]
    fn a_config_edit_sets_values_and_deletes_resets() {
        let edit = ConfigEdit {
            set: [("retention.ms".to_owned(), "60000".to_owned())].into(),
            reset: ["cleanup.policy".to_owned()].into(),
        };

        let ops: Vec<_> = edit
            .to_krafka()
            .into_iter()
            .map(|config| (config.name, config.config_operation, config.value))
            .collect();

        assert_eq!(
            ops,
            [
                (
                    "retention.ms".to_owned(),
                    AlterConfigOp::Set,
                    Some("60000".to_owned())
                ),
                ("cleanup.policy".to_owned(), AlterConfigOp::Delete, None),
            ]
        );
    }

    fn alter_result(code: ErrorCode, message: Option<&str>) -> IncrementalAlterConfigsResponse {
        IncrementalAlterConfigsResponse {
            throttle_time_ms: 0,
            results: vec![IncrementalAlterConfigsResult {
                error_code: code,
                error_message: message.map(str::to_owned),
                resource_type: ConfigResourceType::Topic,
                resource_name: "orders".to_owned(),
            }],
        }
    }

    #[test]
    fn an_altered_resource_is_ok_and_a_refused_one_carries_the_broker_reason() {
        assert!(altered(alter_result(ErrorCode::None, None)).is_ok());

        let invalid = altered(alter_result(
            ErrorCode::InvalidConfig,
            Some("Invalid value -5 for configuration retention.ms"),
        ))
        .unwrap_err();
        assert!(
            matches!(&invalid, KafkaError::Refused(reason) if reason.starts_with("Invalid value -5")),
            "{invalid}"
        );

        let bare = altered(alter_result(ErrorCode::PolicyViolation, None)).unwrap_err();
        assert!(
            matches!(&bare, KafkaError::Refused(reason) if reason == "PolicyViolation"),
            "{bare}"
        );
    }
}
