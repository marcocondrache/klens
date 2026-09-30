use std::sync::Arc;

use foldhash::{HashMap, HashMapExt};

use super::tables::{GroupOffsets, Interner, Topology, WatermarkTable};
use crate::kafka::cluster::ClusterIdentity;
use crate::kafka::group::{
    CommittedOffset, GroupMember, GroupSnapshot, GroupState, MemberAssignment,
};
use crate::kafka::metadata::{
    BrokerMetadata, MetadataSnapshot, PartitionMetadata, TopicMetadata, Watermarks,
};
use crate::kafka::registry::{SchemaCompatibility, SchemaSubject, SchemaType};
use crate::kafka::topic_config::{ConfigEntry, ConfigSource};

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
