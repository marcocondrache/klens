//! Collected `Vec`s are shrunk because `collect` reuses the wider krafka
//! allocation in place, and the store keeps them for as long as they live.

use krafka::admin::{
    ConfigEntry as KrafkaConfigEntry, ConsumerGroupDescription, ConsumerGroupMember,
    DescribeProducersPartitionInfo, GroupOffsetEntry, LogDirInfo, ProducerStateInfo,
    TopicPartitionAssignment, TransactionDescription as KrafkaTransactionDescription,
};
use krafka::metadata::{ClusterMetadata, TopicInfo as KrafkaTopicInfo};
use krafka::protocol::validate_topic_name;

use crate::kafka::error::KafkaError;
use crate::kafka::group::{
    CommittedOffset, GroupMember, GroupSnapshot, GroupState, MemberAssignment,
};
use crate::kafka::metadata::{
    BrokerMetadata, MetadataSnapshot, PartitionMetadata, TopicMetadata, is_internal_topic,
};
use crate::kafka::storage::{LogDir, ReplicaLog, volume_bytes};
use crate::kafka::topic_config::{ConfigEntry, ConfigSource};
use crate::kafka::transaction::{
    ActiveProducer, ListedTransaction, PartitionProducers, TransactionDescription,
    TransactionState, reported,
};

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

/// A coordinator that failed to list sets `error` and leaves its transactions
/// out, so the rest would pass for the whole cluster.
pub(super) fn listed_transactions(
    error: Option<String>,
    entries: impl IntoIterator<Item = (String, i64, String)>,
) -> Result<Vec<ListedTransaction>, KafkaError> {
    if let Some(error) = error {
        return Err(KafkaError::Admin(error));
    }
    Ok(entries
        .into_iter()
        .map(|(transactional_id, producer_id, state)| ListedTransaction {
            transactional_id,
            producer_id,
            state: TransactionState::parse(&state),
        })
        .collect())
}

impl TransactionDescription {
    pub(super) fn from_krafka(description: KrafkaTransactionDescription) -> Self {
        Self {
            transactional_id: description.transactional_id,
            error: description.error,
            state: TransactionState::parse(&description.state),
            producer_id: description.producer_id,
            producer_epoch: description.producer_epoch,
            timeout_ms: description.timeout_ms,
            started_at_ms: reported(description.start_time_ms),
            partitions: transaction_partitions(
                description
                    .topics
                    .into_iter()
                    .map(|topic| (topic.topic, topic.partitions)),
            ),
        }
    }
}

fn transaction_partitions(
    topics: impl IntoIterator<Item = (String, Vec<i32>)>,
) -> Vec<(String, i32)> {
    let mut partitions: Vec<(String, i32)> = topics
        .into_iter()
        .flat_map(|(topic, ids)| ids.into_iter().map(move |id| (topic.clone(), id)))
        .collect();
    partitions.sort();
    partitions
}

impl PartitionProducers {
    pub(super) fn from_krafka(topic: String, partition: DescribeProducersPartitionInfo) -> Self {
        Self {
            topic,
            partition: partition.partition_index,
            error: partition.error,
            producers: partition
                .active_producers
                .into_iter()
                .map(ActiveProducer::from_krafka)
                .collect(),
        }
    }
}

impl ActiveProducer {
    fn from_krafka(producer: ProducerStateInfo) -> Self {
        active_producer(
            producer.producer_id,
            producer.producer_epoch,
            producer.last_timestamp,
            producer.current_txn_start_offset,
        )
    }
}

fn active_producer(
    producer_id: i64,
    producer_epoch: i32,
    last_timestamp: i64,
    open_offset: i64,
) -> ActiveProducer {
    ActiveProducer {
        producer_id,
        producer_epoch,
        last_timestamp_ms: reported(last_timestamp),
        open_offset: reported(open_offset),
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
    fn a_listing_error_fails_the_whole_listing() {
        let error = listed_transactions(
            Some("CoordinatorLoadInProgress".into()),
            [("payments-1".into(), 7, "Ongoing".into())],
        )
        .unwrap_err();

        assert!(
            matches!(error, KafkaError::Admin(message) if message == "CoordinatorLoadInProgress")
        );
    }

    #[test]
    fn listed_transactions_read_their_state() {
        let listed = listed_transactions(
            None,
            [
                ("payments-1".into(), 7, "PrepareCommit".into()),
                ("payments-2".into(), 8, "CompleteAbort".into()),
            ],
        )
        .unwrap();

        assert_eq!(
            listed,
            [
                ListedTransaction {
                    transactional_id: "payments-1".into(),
                    producer_id: 7,
                    state: TransactionState::PrepareCommit,
                },
                ListedTransaction {
                    transactional_id: "payments-2".into(),
                    producer_id: 8,
                    state: TransactionState::CompleteAbort,
                },
            ]
        );
    }

    #[test]
    fn transaction_partitions_flatten_and_sort() {
        assert_eq!(
            transaction_partitions([("payments".into(), vec![1, 0]), ("orders".into(), vec![3]),]),
            [
                ("orders".to_owned(), 3),
                ("payments".to_owned(), 0),
                ("payments".to_owned(), 1),
            ]
        );
    }

    #[test]
    fn a_producer_without_an_open_transaction_has_no_open_offset() {
        assert_eq!(
            active_producer(7, 2, 1_700_000_000_000, 42),
            ActiveProducer {
                producer_id: 7,
                producer_epoch: 2,
                last_timestamp_ms: Some(1_700_000_000_000),
                open_offset: Some(42),
            }
        );
        assert_eq!(
            active_producer(7, 2, -1, -1),
            ActiveProducer {
                producer_id: 7,
                producer_epoch: 2,
                last_timestamp_ms: None,
                open_offset: None,
            }
        );
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
