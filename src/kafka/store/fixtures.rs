use std::sync::Arc;

use foldhash::{HashMap, HashMapExt};
use jiff::Timestamp;

use super::tables::{GroupOffsets, Interner, Topology, WatermarkTable};
use crate::config::SecurityProtocol;
use crate::kafka::cluster::ClusterIdentity;
use crate::kafka::group::{
    CommittedOffset, GroupMember, GroupSnapshot, GroupState, MemberAssignment,
};
use crate::kafka::metadata::{BrokerMetadata, MetadataSnapshot, PartitionMetadata, TopicMetadata};
use crate::kafka::registry::{SchemaCompatibility, SchemaSubject, SchemaType};
use crate::kafka::topic_config::{ConfigEntry, ConfigSource};
use crate::kafka::watermarks::Watermarks;

pub fn identity(name: &str) -> ClusterIdentity {
    ClusterIdentity {
        name: name.to_owned(),
        bootstrap_servers: vec!["localhost:9092".into()],
        security_protocol: SecurityProtocol::Plaintext,
    }
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

/// A single-broker cluster carrying the given topics.
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
        coordinator: 1,
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

pub fn at(millis: i64) -> Timestamp {
    Timestamp::from_millisecond(millis).unwrap_or(Timestamp::UNIX_EPOCH)
}

pub fn watermarks(sampled_at: Timestamp, marks: &[(&str, i32, i64, i64)]) -> WatermarkTable {
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
    WatermarkTable::new(sampled_at, table)
}

pub fn offsets(sampled_at: Timestamp, committed: &[(&str, i32, i64)]) -> GroupOffsets {
    GroupOffsets {
        sampled_at,
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
