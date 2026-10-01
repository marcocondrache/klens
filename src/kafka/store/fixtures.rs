use std::sync::Arc;

use foldhash::{HashMap, HashMapExt};

use super::tables::{GroupOffsets, Interner, Topology, WatermarkTable};
use super::transactions::{DEFAULT_MAX_TIMEOUT_MS, OpenPartition};
use crate::kafka::cluster::ClusterIdentity;
use crate::kafka::group::{
    CommittedOffset, GroupMember, GroupSnapshot, GroupState, MemberAssignment,
};
use crate::kafka::metadata::{
    BrokerMetadata, MetadataSnapshot, PartitionMetadata, TopicMetadata, Watermarks,
};
use crate::kafka::registry::{SchemaCompatibility, SchemaSubject, SchemaType};
use crate::kafka::storage::{LogDir, ReplicaLog};
use crate::kafka::topic_config::{ConfigEntry, ConfigSource};
use crate::kafka::transaction::{
    ActiveProducer, PartitionProducers, TransactionDescription, TransactionState,
};

pub fn identity(name: &str) -> ClusterIdentity {
    ClusterIdentity::new(name)
}

pub fn partition(id: i32, replicas: Vec<i32>, isr: Vec<i32>) -> PartitionMetadata {
    PartitionMetadata {
        id,
        leader: replicas.first().copied().unwrap_or(-1),
        replicas,
        isr,
    }
}

pub fn offline_partition(id: i32, replicas: Vec<i32>) -> PartitionMetadata {
    PartitionMetadata {
        id,
        leader: -1,
        replicas,
        isr: Vec::new(),
    }
}

pub fn topic(name: &str, partitions: Vec<PartitionMetadata>) -> TopicMetadata {
    TopicMetadata {
        name: name.into(),
        internal: name.starts_with('_'),
        partitions,
    }
}

pub fn metadata(topics: Vec<TopicMetadata>) -> MetadataSnapshot {
    MetadataSnapshot {
        cluster_id: Some("test-cluster".into()),
        brokers: vec![BrokerMetadata {
            id: 1,
            host: "localhost".into(),
            port: 9092,
        }],
        topics,
    }
}

pub fn group(id: &str, topic: &str, partitions: Vec<i32>) -> GroupSnapshot {
    GroupSnapshot {
        id: id.into(),
        state: GroupState::Stable,
        protocol: "range".into(),
        members: vec![GroupMember {
            id: format!("{id}-m1"),
            client_id: "c1".into(),
            host: "127.0.0.1".into(),
            assignments: vec![MemberAssignment {
                topic: topic.into(),
                partitions,
            }],
        }],
        committed: Vec::new(),
    }
}

pub fn topology(topics: Vec<TopicMetadata>, groups: Vec<GroupSnapshot>) -> Topology {
    Topology::assemble(metadata(topics), groups, &mut Interner::default())
}

pub fn watermarks(marks: &[(&str, i32, i64, i64)]) -> WatermarkTable {
    let mut table: HashMap<Arc<str>, HashMap<i32, Watermarks>> = HashMap::new();
    for (topic, partition, low, high) in marks {
        table.entry(Arc::from(*topic)).or_default().insert(
            *partition,
            Watermarks {
                low: *low,
                high: *high,
            },
        );
    }
    WatermarkTable { marks: table }
}

pub fn offsets(committed: &[(&str, i32, i64)]) -> GroupOffsets {
    GroupOffsets {
        committed: committed
            .iter()
            .map(|(topic, partition, offset)| CommittedOffset {
                topic: (*topic).to_owned(),
                partition: *partition,
                offset: *offset,
            })
            .collect(),
    }
}

pub fn config(name: &str, value: &str) -> ConfigEntry {
    ConfigEntry {
        name: name.to_owned(),
        value: Some(value.to_owned()),
        source: ConfigSource::DynamicTopic,
        read_only: false,
        sensitive: false,
    }
}

pub fn subject(name: &str, id: i32, latest: i32) -> SchemaSubject {
    SchemaSubject {
        subject: name.to_owned(),
        id,
        schema_type: SchemaType::Avro,
        latest_version: latest,
        versions: (1..=latest).collect(),
        compatibility: SchemaCompatibility::Backward,
    }
}

pub fn log_dir(broker: i32, path: &str, replicas: &[(&str, i32, i64)]) -> LogDir {
    LogDir {
        broker,
        path: path.to_owned(),
        error: None,
        total_bytes: None,
        usable_bytes: None,
        cordoned: false,
        replicas: replicas
            .iter()
            .map(|(topic, partition, size_bytes)| ReplicaLog {
                topic: (*topic).to_owned(),
                partition: *partition,
                size_bytes: *size_bytes,
                future: false,
            })
            .collect(),
    }
}

pub fn transaction(
    id: &str,
    producer_id: i64,
    started_at_ms: i64,
    partitions: &[(&str, i32)],
) -> TransactionDescription {
    TransactionDescription {
        transactional_id: id.to_owned(),
        error: None,
        state: TransactionState::Ongoing,
        producer_id,
        producer_epoch: 0,
        timeout_ms: 60_000,
        started_at_ms: Some(started_at_ms),
        partitions: partitions
            .iter()
            .map(|(topic, partition)| ((*topic).to_owned(), *partition))
            .collect(),
    }
}

pub fn open_producer(producer_id: i64, last_timestamp_ms: i64, open_offset: i64) -> ActiveProducer {
    ActiveProducer {
        producer_id,
        producer_epoch: 0,
        last_timestamp_ms: Some(last_timestamp_ms),
        open_offset: Some(open_offset),
    }
}

pub fn producers(
    topic: &str,
    partition: i32,
    producers: Vec<ActiveProducer>,
) -> PartitionProducers {
    PartitionProducers {
        topic: topic.to_owned(),
        partition,
        error: None,
        producers,
    }
}

pub fn open_partition(
    topic: &str,
    partition: i32,
    producer_id: i64,
    open_offset: i64,
) -> OpenPartition {
    OpenPartition {
        topic: Arc::from(topic),
        partition,
        leader: 1,
        producer_id,
        producer_epoch: 0,
        open_offset,
        last_timestamp_ms: None,
        max_timeout_ms: DEFAULT_MAX_TIMEOUT_MS,
        seen_before: true,
    }
}
