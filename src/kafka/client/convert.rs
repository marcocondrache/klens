use std::collections::HashMap;

use bytes::Bytes;
use krafka::TopicInfo;
use krafka::admin::{
    ConfigEntry as KrafkaConfigEntry, ConfigOp, ConfigResource, ConsumerGroupDescription,
    ConsumerGroupMember, GroupOffset, LogDirDescription, TopicPartition, TopicPartitionAssignment,
};
use krafka::consumer::FetchMetadataResult;
use krafka::error::KrafkaError;
use krafka::producer::Record;
use tracing::warn;

use crate::kafka::error::KafkaError;
use crate::kafka::group::{
    CommittedOffset, GroupMember, GroupSnapshot, GroupState, MemberAssignment,
};
use crate::kafka::metadata::{
    BrokerMetadata, MetadataSnapshot, NewTopic, PartitionMetadata, RecordDeletion, TopicMetadata,
    is_internal_topic, validate_topic_name,
};
use crate::kafka::produce::NewRecord;
use crate::kafka::storage::{LogDir, ReplicaLog};
use crate::kafka::topic_config::{BrokerScope, ConfigEdit, ConfigEntry, ConfigSource};

impl MetadataSnapshot {
    pub(super) fn from_krafka(cluster_id: Option<String>, cached: FetchMetadataResult) -> Self {
        Self {
            cluster_id,
            brokers: cached
                .brokers
                .into_iter()
                .map(|broker| BrokerMetadata {
                    id: broker.id(),
                    host: broker.host().to_owned(),
                    port: broker.port(),
                })
                .collect(),
            topics: cached
                .topics
                .into_iter()
                .map(TopicMetadata::from_krafka)
                .collect(),
        }
    }
}

pub(super) fn refused<T>(result: Result<T, KrafkaError>) -> Result<T, KafkaError> {
    result.map_err(|error| match error {
        KrafkaError::Broker { message, .. } => KafkaError::Refused(message),
        error => error.into(),
    })
}

const HIGH_WATERMARK: i64 = -1;

impl RecordDeletion {
    pub(super) fn to_krafka(&self) -> impl Iterator<Item = (TopicPartition, i64)> + '_ {
        self.before.iter().map(|(&partition, before)| {
            (
                TopicPartition::new(self.topic.as_str(), partition),
                before.unwrap_or(HIGH_WATERMARK),
            )
        })
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
    pub(super) fn to_krafka(&self) -> Record {
        let mut record = Record::new(self.topic.as_str(), Bytes::new());
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

impl ConfigEdit {
    pub(super) fn to_krafka(&self) -> Vec<ConfigOp> {
        let set = self
            .set
            .iter()
            .map(|(name, value)| ConfigOp::set(name, value));
        let reset = self.reset.iter().map(ConfigOp::delete);
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
    pub(super) fn from_krafka(topic: TopicInfo) -> Self {
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
                .map(|assigned| {
                    let mut assignments: Vec<MemberAssignment> = assigned
                        .into_iter()
                        .filter_map(MemberAssignment::from_krafka)
                        .collect();
                    assignments.shrink_to_fit();
                    assignments
                })
                .unwrap_or_default(),
        }
    }
}

impl MemberAssignment {
    fn from_krafka(assignment: TopicPartitionAssignment) -> Option<Self> {
        (!assignment.topic_name.is_empty()).then_some(Self {
            topic: assignment.topic_name,
            partitions: assignment.partitions,
        })
    }
}

pub(super) fn committed_from_krafka(
    group: &str,
    offsets: HashMap<TopicPartition, Result<GroupOffset, KrafkaError>>,
) -> Vec<CommittedOffset> {
    let mut committed: Vec<CommittedOffset> = offsets
        .into_iter()
        .filter_map(|(partition, offset)| match offset {
            Ok(offset) => CommittedOffset::from_krafka(partition, offset.offset),
            Err(error) => {
                warn!(
                    group,
                    topic = partition.topic,
                    partition = partition.partition,
                    %error,
                    "skipping a committed offset Kafka would not return"
                );
                None
            }
        })
        .collect();
    committed.shrink_to_fit();
    committed
}

impl CommittedOffset {
    fn from_krafka(partition: TopicPartition, offset: Option<i64>) -> Option<Self> {
        let offset = offset?;
        validate_topic_name(&partition.topic).ok()?;
        Some(Self {
            topic: partition.topic,
            partition: partition.partition,
            offset,
        })
    }
}

impl LogDir {
    pub(super) fn from_krafka(
        broker: i32,
        path: String,
        dir: Result<LogDirDescription, KrafkaError>,
    ) -> Self {
        let dir = match dir {
            Ok(dir) => dir,
            Err(error) => {
                return Self {
                    broker,
                    path,
                    error: Some(error.to_string()),
                    total_bytes: None,
                    usable_bytes: None,
                    cordoned: false,
                    replicas: Vec::new(),
                };
            }
        };

        Self {
            broker,
            path,
            error: None,
            total_bytes: dir.total_bytes,
            usable_bytes: dir.usable_bytes,
            cordoned: dir.is_cordoned,
            replicas: dir
                .replicas
                .into_iter()
                .map(|(partition, replica)| ReplicaLog {
                    topic: partition.topic,
                    partition: partition.partition,
                    size_bytes: replica.size,
                    future: replica.is_future,
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

impl BrokerScope {
    pub(super) fn to_krafka(self) -> ConfigResource {
        match self {
            Self::Broker(id) => ConfigResource::broker(id),
            Self::Cluster => ConfigResource::broker_default(),
        }
    }
}

impl ConfigSource {
    fn from_krafka(source: i8) -> Self {
        match source {
            1 => Self::DynamicTopic,
            2 => Self::DynamicBroker,
            3 => Self::DynamicDefaultBroker,
            4 => Self::StaticBroker,
            _ => Self::Default,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::num::{NonZeroU8, NonZeroU16};

    use krafka::error::ErrorCode;

    use super::*;
    use crate::kafka::scan::RecordHeader;

    #[test]
    fn config_source_maps_kafka_describe_codes() {
        assert_eq!(ConfigSource::from_krafka(1), ConfigSource::DynamicTopic);
        assert_eq!(ConfigSource::from_krafka(2), ConfigSource::DynamicBroker);
        assert_eq!(
            ConfigSource::from_krafka(3),
            ConfigSource::DynamicDefaultBroker
        );
        assert_eq!(ConfigSource::from_krafka(4), ConfigSource::StaticBroker);
        assert_eq!(ConfigSource::from_krafka(5), ConfigSource::Default);
        assert_eq!(ConfigSource::from_krafka(0), ConfigSource::Default);
        assert_eq!(ConfigSource::from_krafka(6), ConfigSource::Default);
        assert_eq!(ConfigSource::from_krafka(-1), ConfigSource::Default);
    }

    #[test]
    fn a_broker_scope_names_its_broker_and_the_cluster_default_names_none() {
        assert_eq!(
            BrokerScope::Broker(12).to_krafka(),
            ConfigResource::broker(12)
        );
        assert_eq!(BrokerScope::Cluster.to_krafka().name, "");
    }

    #[test]
    fn committed_offsets_drop_unset_offsets_and_illegal_topic_names() {
        let at = |topic: &str| TopicPartition::new(topic, 1);

        assert_eq!(
            CommittedOffset::from_krafka(at("orders"), Some(0)),
            Some(CommittedOffset {
                topic: "orders".into(),
                partition: 1,
                offset: 0,
            })
        );
        assert_eq!(CommittedOffset::from_krafka(at("orders"), None), None);
        assert_eq!(CommittedOffset::from_krafka(at(""), Some(7)), None);
        assert_eq!(CommittedOffset::from_krafka(at("bad/name"), Some(7)), None);
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
            deletion.to_krafka().collect::<HashMap<_, _>>(),
            HashMap::from([
                (TopicPartition::new("orders", 0), 42),
                (TopicPartition::new("orders", 1), -1)
            ])
        );
    }

    #[test]
    fn a_config_edit_sets_values_and_deletes_resets() {
        let edit = ConfigEdit {
            set: [("retention.ms".to_owned(), "60000".to_owned())].into(),
            reset: ["cleanup.policy".to_owned()].into(),
        };

        assert_eq!(
            edit.to_krafka(),
            [
                ConfigOp::set("retention.ms", "60000"),
                ConfigOp::delete("cleanup.policy"),
            ]
        );
    }

    #[test]
    fn a_refused_item_carries_the_broker_reason_but_a_timeout_stays_a_client_error() {
        let invalid = refused::<()>(Err(KrafkaError::broker(
            ErrorCode::InvalidConfig,
            "Invalid value -5 for configuration retention.ms",
        )))
        .unwrap_err();
        assert!(
            matches!(&invalid, KafkaError::Refused(reason) if reason.starts_with("Invalid value -5")),
            "{invalid}"
        );

        let timeout = refused::<()>(Err(KrafkaError::timeout("CreateTopics"))).unwrap_err();
        assert!(matches!(timeout, KafkaError::Krafka(_)), "{timeout}");
    }
}
