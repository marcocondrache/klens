//! Catalog clone-cost harness.
//!
//! ```sh
//! cargo test --release --lib kafka::clone_cost::harness -- --ignored --nocapture
//! ```

use std::collections::HashMap;
use std::hint::black_box;
use std::sync::Arc;
use std::time::{Duration, Instant};

use juniper::{Variables, execute};

use crate::app::{AppState, schema};
use crate::kafka::metadata::{MetadataSnapshot, PartitionMetadata, TopicMetadata};
use crate::kafka::model::{
    CleanupPolicy, ClusterIdentity, ClusterOverview, CommittedOffset, Compression, GroupMember,
    GroupOffset, GroupSnapshot, GroupState, MemberAssignment, Partition, Record, RecordHeader,
    SchemaCompatibility, SchemaSubject, SchemaType, Watermarks,
};
use crate::kafka::{
    Broker, ClusterHealth, ClusterSnapshot, ConsumerGroup, FakeCluster, QueryEngine, RateStore,
    Topic,
};

const WARMUP: u32 = 8;
const SAMPLES: u32 = 25;
const EXEC_SAMPLES: u32 = 9;

struct Shape {
    label: &'static str,
    topics: usize,
    partitions: usize,
    groups: usize,
}

const SHAPES: &[Shape] = &[
    Shape {
        label: "20×6p",
        topics: 20,
        partitions: 6,
        groups: 4,
    },
    Shape {
        label: "200×12p",
        topics: 200,
        partitions: 12,
        groups: 20,
    },
    Shape {
        label: "2000×12p",
        topics: 2_000,
        partitions: 12,
        groups: 200,
    },
];

struct Row {
    shape: &'static str,
    op: &'static str,
    us: f64,
    bytes: usize,
    allocs: usize,
}

#[tokio::test]
#[ignore]
async fn harness() {
    let mut rows = Vec::new();
    for shape in SHAPES {
        measure_shape(shape, &mut rows).await;
    }
    print_table(&rows);
}

async fn measure_shape(shape: &Shape, rows: &mut Vec<Row>) {
    let snapshot = build_snapshot(shape);
    let topics = snapshot.topics.clone();
    let groups = snapshot.groups.clone();
    let topic_bytes = topics.iter().map(topic_heap).sum::<usize>();
    let topic_allocs = topics.iter().map(topic_allocs).sum::<usize>() + 1;
    let group_bytes = groups.iter().map(group_heap).sum::<usize>();
    let group_allocs = groups.iter().map(group_allocs).sum::<usize>() + 1;
    let list_bytes = topics.iter().map(topic_list_heap).sum::<usize>();
    let list_allocs = topics.iter().map(topic_list_allocs).sum::<usize>() + 1;

    let snapshot = Arc::new(snapshot);
    let topics_arc = Arc::new(topics.clone());

    rows.push(timed(
        shape.label,
        "Arc<ClusterSnapshot> clone",
        0,
        0,
        || Arc::clone(&snapshot),
    ));
    rows.push(timed(shape.label, "Arc<Vec<Topic>> clone", 0, 0, || {
        Arc::clone(&topics_arc)
    }));
    rows.push(timed(
        shape.label,
        "topics.clone() (domain)",
        topic_bytes,
        topic_allocs,
        || topics.clone(),
    ));
    rows.push(timed(
        shape.label,
        "map_topics eager (clone+GraphQL From)",
        topic_bytes,
        topic_allocs,
        || map_topics_eager(&topics),
    ));
    rows.push(timed(
        shape.label,
        "map_topics from &Topic (no temp domain)",
        topic_bytes,
        topic_allocs,
        || map_topics_from_ref(&topics),
    ));
    rows.push(timed(
        shape.label,
        "list fields only (no partitions)",
        list_bytes,
        list_allocs,
        || map_topic_list_fields(&topics),
    ));
    rows.push(timed(
        shape.label,
        "groups.clone() (domain)",
        group_bytes,
        group_allocs,
        || groups.clone(),
    ));
    rows.push(timed(
        shape.label,
        "groups_for_topic(None) + map_groups (double)",
        group_bytes * 2,
        group_allocs * 2,
        || map_groups_eager(&snapshot.groups_for_topic(None)),
    ));
    rows.push(timed(
        shape.label,
        "map_groups from &group (single)",
        group_bytes,
        group_allocs,
        || map_groups_eager(&groups),
    ));
    rows.push(timed(
        shape.label,
        "group list fields only (no members/offsets)",
        groups.iter().map(group_list_heap).sum(),
        groups.iter().map(group_list_allocs).sum::<usize>() + 1,
        || map_group_list_fields(&groups),
    ));

    let watermarks = topic_watermarks(&topics);
    rows.push(timed(
        shape.label,
        "Topic::with_watermarks (..self.clone() waste)",
        topic_bytes,
        topic_allocs,
        || {
            topics
                .iter()
                .map(|topic| {
                    with_watermarks_clone_all(
                        topic,
                        watermarks.get(&topic.name).unwrap_or(&HashMap::new()),
                    )
                })
                .collect::<Vec<_>>()
        },
    ));
    rows.push(timed(
        shape.label,
        "Topic::with_watermarks (current)",
        list_bytes
            + topics
                .iter()
                .map(|topic| {
                    topic
                        .partitions
                        .iter()
                        .map(|partition| partition.replicas.len() * 4 + partition.isr.len() * 4)
                        .sum::<usize>()
                })
                .sum::<usize>(),
        topics
            .iter()
            .map(|topic| 2 + topic.partitions.len() * 2 + topic.consumer_groups.len())
            .sum::<usize>()
            + 1,
        || {
            topics
                .iter()
                .map(|topic| {
                    topic.with_watermarks(watermarks.get(&topic.name).unwrap_or(&HashMap::new()))
                })
                .collect::<Vec<_>>()
        },
    ));

    let meta = metadata_for(&topics);
    let empty_wm = HashMap::new();
    rows.push(timed(
        shape.label,
        "Topic::assemble (raw → domain)",
        topic_bytes,
        topic_allocs,
        || {
            meta.topics
                .iter()
                .map(|topic| Topic::assemble(topic, &empty_wm, None, vec!["group-0000".into()]))
                .collect::<Vec<_>>()
        },
    ));

    let raw_groups = raw_groups_for(shape);
    let ends = ends_for(&groups);
    rows.push(timed(
        shape.label,
        "ConsumerGroup::assemble (raw → domain)",
        group_bytes,
        group_allocs,
        || {
            raw_groups
                .iter()
                .map(|group| ConsumerGroup::assemble(group, &ends))
                .collect::<Vec<_>>()
        },
    ));

    rows.push(timed(
        shape.label,
        "search_snapshot(\"topic-1\")",
        0,
        0,
        || snapshot.search("topic-1", &[]),
    ));
    rows.push(timed(
        shape.label,
        "message_counts HashMap",
        topics.iter().map(|topic| topic.name.len()).sum(),
        topics.len() + 1,
        || snapshot.message_counts(),
    ));
    rows.push(timed(
        shape.label,
        "overview.clone()",
        snapshot.overview.identity.name.len()
            + snapshot
                .overview
                .identity
                .bootstrap_servers
                .iter()
                .map(String::len)
                .sum::<usize>(),
        3,
        || snapshot.overview.clone(),
    ));

    let subjects = subjects_for(shape.topics.min(200));
    let subject_bytes = subjects
        .iter()
        .map(|subject| subject.subject.len() + subject.schema.len())
        .sum();
    rows.push(timed(
        shape.label,
        "subjects.clone() (schema text)",
        subject_bytes,
        subjects.len() * 3 + 1,
        || subjects.clone(),
    ));

    let records = records_for(100);
    let record_bytes = records
        .iter()
        .map(|record| {
            record.topic.len()
                + record.key.as_ref().map(String::len).unwrap_or(0)
                + record.value.as_ref().map(String::len).unwrap_or(0)
        })
        .sum();
    rows.push(timed(
        shape.label,
        "100-record page clone (2KiB values)",
        record_bytes,
        records.len() * 3,
        || records.clone(),
    ));

    let rates = RateStore::new();
    rates.observe(
        "local",
        topics
            .iter()
            .map(|topic| (topic.name.clone(), topic.message_count))
            .collect(),
    );
    rows.push(timed(
        shape.label,
        "RateStore::topic_rates (clone+sort)",
        topics.iter().map(|topic| topic.name.len()).sum(),
        topics.len() + 1,
        || rates.topic_rates("local"),
    ));

    let state = seeded_state(Arc::clone(&snapshot));
    let gql = schema();
    let list_query = r#"{
        clusterCatalog(cluster: "local") {
            updatedAt
            topics {
                name internal partitionCount replicationFactor messageCount
                sizeBytes cleanupPolicy retentionMs consumerGroups
                bytesInPerSec messagesPerSec underReplicated
            }
        }
    }"#;
    let full_query = r#"{
        clusterCatalog(cluster: "local") {
            updatedAt
            topics {
                name internal partitionCount replicationFactor messageCount
                sizeBytes cleanupPolicy retentionMs consumerGroups
                bytesInPerSec messagesPerSec underReplicated
                partitions { id leader replicas isr lowWatermark highWatermark sizeBytes }
            }
        }
    }"#;
    let group_list_query = r#"{
        clusterCatalog(cluster: "local") {
            consumerGroups { id state protocol coordinator memberCount topics lag assignedPartitionCount }
        }
    }"#;
    let group_full_query = r#"{
        clusterCatalog(cluster: "local") {
            consumerGroups {
                id state protocol coordinator memberCount topics lag assignedPartitionCount
                members { id clientId host assignments { topic partitions } }
                offsets { topic partition currentOffset endOffset lag memberId }
            }
        }
    }"#;

    let vars = Variables::new();
    warmup_execute(&gql, &state, list_query).await;
    rows.push(
        timed_async(
            shape.label,
            "Juniper execute topics list (no partitions)",
            list_bytes,
            list_allocs,
            || execute(list_query, None, &gql, &vars, &state),
        )
        .await,
    );
    rows.push(
        timed_async(
            shape.label,
            "Juniper execute topics + partitions",
            topic_bytes,
            topic_allocs,
            || execute(full_query, None, &gql, &vars, &state),
        )
        .await,
    );
    rows.push(
        timed_async(
            shape.label,
            "Juniper execute groups list (no members/offsets)",
            groups.iter().map(group_list_heap).sum(),
            groups.iter().map(group_list_allocs).sum::<usize>() + 1,
            || execute(group_list_query, None, &gql, &vars, &state),
        )
        .await,
    );
    rows.push(
        timed_async(
            shape.label,
            "Juniper execute groups + members/offsets",
            group_bytes,
            group_allocs,
            || execute(group_full_query, None, &gql, &vars, &state),
        )
        .await,
    );
}

fn seeded_state(snapshot: Arc<ClusterSnapshot>) -> AppState {
    let state = AppState::new(Arc::new(QueryEngine::from_sessions(vec![
        FakeCluster::local(),
    ])));
    state.catalog.store("local", snapshot);
    state
}

async fn warmup_execute(gql: &crate::app::Schema, state: &AppState, query: &str) {
    for _ in 0..3 {
        let _ = execute(query, None, gql, &Variables::new(), state).await;
    }
}

fn timed<T>(
    shape: &'static str,
    op: &'static str,
    bytes: usize,
    allocs: usize,
    mut f: impl FnMut() -> T,
) -> Row {
    for _ in 0..WARMUP {
        black_box(f());
    }
    let mut samples = Vec::with_capacity(SAMPLES as usize);
    for _ in 0..SAMPLES {
        let start = Instant::now();
        black_box(f());
        samples.push(start.elapsed());
    }
    Row {
        shape,
        op,
        us: median_us(&samples),
        bytes,
        allocs,
    }
}

async fn timed_async<Fut, T>(
    shape: &'static str,
    op: &'static str,
    bytes: usize,
    allocs: usize,
    mut f: impl FnMut() -> Fut,
) -> Row
where
    Fut: std::future::Future<Output = T>,
{
    for _ in 0..3 {
        black_box(f().await);
    }
    let mut samples = Vec::with_capacity(EXEC_SAMPLES as usize);
    for _ in 0..EXEC_SAMPLES {
        let start = Instant::now();
        black_box(f().await);
        samples.push(start.elapsed());
    }
    Row {
        shape,
        op,
        us: median_us(&samples),
        bytes,
        allocs,
    }
}

fn median_us(samples: &[Duration]) -> f64 {
    let mut samples = samples.to_vec();
    samples.sort();
    samples[samples.len() / 2].as_secs_f64() * 1_000_000.0
}

fn print_table(rows: &[Row]) {
    println!(
        "{:<10}  {:>10}  {:>10}  {:>8}  {}",
        "shape", "median µs", "est heap", "allocs", "op"
    );
    for row in rows {
        println!(
            "{:<10}  {:>10.1}  {:>10}  {:>8}  {}",
            row.shape, row.us, row.bytes, row.allocs, row.op
        );
    }
}

fn build_snapshot(shape: &Shape) -> ClusterSnapshot {
    let topics: Vec<Topic> = (0..shape.topics)
        .map(|index| topic(index, shape.partitions))
        .collect();
    let groups: Vec<ConsumerGroup> = (0..shape.groups)
        .map(|index| group(index, shape.partitions, shape.topics))
        .collect();
    let brokers = vec![Broker {
        id: 1,
        host: "localhost".into(),
        port: 9092,
        rack: None,
        controller: true,
        partition_count: (shape.topics * shape.partitions) as i32,
        leader_count: (shape.topics * shape.partitions) as i32,
    }];
    let overview = ClusterOverview {
        identity: ClusterIdentity {
            name: "local".into(),
            bootstrap_servers: vec!["localhost:9092".into()],
            security_protocol: crate::config::SecurityProtocol::Plaintext,
        },
        cluster_id: "bench".into(),
        health: ClusterHealth::Healthy,
        broker_count: 1,
        topic_count: shape.topics as i32,
        partition_count: (shape.topics * shape.partitions) as i32,
        consumer_group_count: shape.groups as i32,
        under_replicated_partitions: 0,
        offline_partitions: 0,
        message_count: 0,
    };
    ClusterSnapshot::assemble(topics, groups, brokers, overview)
}

fn topic(index: usize, partitions: usize) -> Topic {
    Topic {
        name: format!("topic-{index:04}"),
        internal: false,
        partitions: (0..partitions)
            .map(|id| Partition {
                id: id as i32,
                leader: 1,
                replicas: vec![1, 2, 3],
                isr: vec![1, 2, 3],
                low_watermark: 0,
                high_watermark: 1_000,
            })
            .collect(),
        replication_factor: 3,
        message_count: 1_000 * partitions as u64,
        cleanup_policy: CleanupPolicy::Delete,
        retention_ms: 604_800_000,
        consumer_groups: vec![format!("group-{:04}", index % 20)],
        under_replicated: false,
    }
}

fn group(index: usize, partitions: usize, topic_count: usize) -> ConsumerGroup {
    let topic = format!("topic-{:04}", index % topic_count.max(1));
    let member_id = format!("member-{index}");
    ConsumerGroup {
        id: format!("group-{index:04}"),
        state: GroupState::Stable,
        protocol: "range".into(),
        coordinator: 1,
        members: vec![GroupMember {
            id: member_id.clone(),
            client_id: "bench".into(),
            host: "127.0.0.1".into(),
            assignments: vec![MemberAssignment {
                topic: topic.clone(),
                partitions: (0..partitions as i32).collect(),
            }],
        }],
        topics: vec![topic.clone()],
        lag: 12,
        offsets: (0..partitions as i32)
            .map(|partition| GroupOffset {
                topic: topic.clone(),
                partition,
                current_offset: 988,
                end_offset: 1_000,
                lag: 12,
                member_id: Some(member_id.clone()),
            })
            .collect(),
    }
}

fn topic_watermarks(topics: &[Topic]) -> HashMap<String, HashMap<i32, Watermarks>> {
    topics
        .iter()
        .map(|topic| {
            let marks = topic
                .partitions
                .iter()
                .map(|partition| {
                    (
                        partition.id,
                        Watermarks {
                            low: partition.low_watermark,
                            high: partition.high_watermark + 1,
                        },
                    )
                })
                .collect();
            (topic.name.clone(), marks)
        })
        .collect()
}

fn with_watermarks_clone_all(topic: &Topic, watermarks: &HashMap<i32, Watermarks>) -> Topic {
    let partitions: Vec<Partition> = topic
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
    Topic {
        partitions,
        message_count,
        under_replicated,
        ..topic.clone()
    }
}

fn metadata_for(topics: &[Topic]) -> MetadataSnapshot {
    MetadataSnapshot {
        cluster_id: Some("bench".into()),
        brokers: Vec::new(),
        topics: topics
            .iter()
            .map(|topic| TopicMetadata {
                name: topic.name.clone(),
                internal: topic.internal,
                partitions: topic
                    .partitions
                    .iter()
                    .map(|partition| PartitionMetadata {
                        id: partition.id,
                        leader: partition.leader,
                        replicas: partition.replicas.clone(),
                        isr: partition.isr.clone(),
                    })
                    .collect(),
            })
            .collect(),
    }
}

fn raw_groups_for(shape: &Shape) -> Vec<GroupSnapshot> {
    (0..shape.groups)
        .map(|index| {
            let topic = format!("topic-{:04}", index % shape.topics.max(1));
            GroupSnapshot {
                id: format!("group-{index:04}"),
                state: GroupState::Stable,
                protocol: "range".into(),
                coordinator: 1,
                members: vec![GroupMember {
                    id: format!("member-{index}"),
                    client_id: "bench".into(),
                    host: "127.0.0.1".into(),
                    assignments: vec![MemberAssignment {
                        topic: topic.clone(),
                        partitions: (0..shape.partitions as i32).collect(),
                    }],
                }],
                committed: (0..shape.partitions as i32)
                    .map(|partition| CommittedOffset {
                        topic: topic.clone(),
                        partition,
                        offset: 988,
                    })
                    .collect(),
            }
        })
        .collect()
}

fn ends_for(groups: &[ConsumerGroup]) -> HashMap<(String, i32), i64> {
    groups
        .iter()
        .flat_map(|group| {
            group
                .offsets
                .iter()
                .map(|offset| ((offset.topic.clone(), offset.partition), offset.end_offset))
        })
        .collect()
}

fn subjects_for(count: usize) -> Vec<SchemaSubject> {
    (0..count)
        .map(|index| SchemaSubject {
            subject: format!("topic-{index:04}-value"),
            id: index as i32,
            schema_type: SchemaType::Avro,
            latest_version: 1,
            versions: vec![1],
            compatibility: SchemaCompatibility::Backward,
            schema: "{\"type\":\"record\",\"name\":\"V\",\"fields\":[]}".repeat(4),
        })
        .collect()
}

fn records_for(count: usize) -> Vec<Record> {
    let value = "x".repeat(2048);
    (0..count)
        .map(|index| Record {
            topic: "topic-0000".into(),
            partition: 0,
            offset: index as i64,
            timestamp: 1_700_000_000_000,
            key: Some(format!("k{index}")),
            value: Some(value.clone()),
            schema_id: None,
            headers: vec![RecordHeader {
                key: "h".into(),
                value: "1".into(),
            }],
            size_bytes: 2056,
            compression: Compression::None,
        })
        .collect()
}

#[allow(dead_code)]
struct GraphTopic {
    name: String,
    partitions: Vec<GraphPartition>,
    consumer_groups: Vec<String>,
}

#[allow(dead_code)]
struct GraphPartition {
    replicas: Vec<i32>,
    isr: Vec<i32>,
}

#[allow(dead_code)]
struct GraphGroup {
    id: String,
    members: Vec<GroupMember>,
    topics: Vec<String>,
    offsets: Vec<GroupOffset>,
}

#[allow(dead_code)]
struct GraphTopicList {
    name: String,
    consumer_groups: Vec<String>,
}

#[allow(dead_code)]
struct GraphGroupList {
    id: String,
    topics: Vec<String>,
}

fn map_topics_eager(topics: &[Topic]) -> Vec<GraphTopic> {
    topics
        .iter()
        .cloned()
        .map(|topic| GraphTopic {
            name: topic.name,
            partitions: topic
                .partitions
                .into_iter()
                .map(|partition| GraphPartition {
                    replicas: partition.replicas,
                    isr: partition.isr,
                })
                .collect(),
            consumer_groups: topic.consumer_groups,
        })
        .collect()
}

fn map_topics_from_ref(topics: &[Topic]) -> Vec<GraphTopic> {
    topics
        .iter()
        .map(|topic| GraphTopic {
            name: topic.name.clone(),
            partitions: topic
                .partitions
                .iter()
                .map(|partition| GraphPartition {
                    replicas: partition.replicas.clone(),
                    isr: partition.isr.clone(),
                })
                .collect(),
            consumer_groups: topic.consumer_groups.clone(),
        })
        .collect()
}

fn map_topic_list_fields(topics: &[Topic]) -> Vec<GraphTopicList> {
    topics
        .iter()
        .map(|topic| GraphTopicList {
            name: topic.name.clone(),
            consumer_groups: topic.consumer_groups.clone(),
        })
        .collect()
}

fn map_groups_eager(groups: &[ConsumerGroup]) -> Vec<GraphGroup> {
    groups
        .iter()
        .cloned()
        .map(|group| GraphGroup {
            id: group.id,
            members: group.members,
            topics: group.topics,
            offsets: group.offsets,
        })
        .collect()
}

fn map_group_list_fields(groups: &[ConsumerGroup]) -> Vec<GraphGroupList> {
    groups
        .iter()
        .map(|group| GraphGroupList {
            id: group.id.clone(),
            topics: group.topics.clone(),
        })
        .collect()
}

fn topic_heap(topic: &Topic) -> usize {
    topic.name.len()
        + topic
            .partitions
            .iter()
            .map(|partition| partition.replicas.len() * 4 + partition.isr.len() * 4)
            .sum::<usize>()
        + topic.consumer_groups.iter().map(String::len).sum::<usize>()
}

fn topic_allocs(topic: &Topic) -> usize {
    2 + topic.partitions.len() * 2 + topic.consumer_groups.len()
}

fn topic_list_heap(topic: &Topic) -> usize {
    topic.name.len() + topic.consumer_groups.iter().map(String::len).sum::<usize>()
}

fn topic_list_allocs(topic: &Topic) -> usize {
    2 + topic.consumer_groups.len()
}

fn group_heap(group: &ConsumerGroup) -> usize {
    group.id.len()
        + group.protocol.len()
        + group.topics.iter().map(String::len).sum::<usize>()
        + group
            .members
            .iter()
            .map(|member| {
                member.id.len()
                    + member.client_id.len()
                    + member.host.len()
                    + member
                        .assignments
                        .iter()
                        .map(|assignment| assignment.topic.len() + assignment.partitions.len() * 4)
                        .sum::<usize>()
            })
            .sum::<usize>()
        + group
            .offsets
            .iter()
            .map(|offset| {
                offset.topic.len() + offset.member_id.as_ref().map(String::len).unwrap_or(0)
            })
            .sum::<usize>()
}

fn group_allocs(group: &ConsumerGroup) -> usize {
    4 + group.topics.len()
        + group
            .members
            .iter()
            .map(|member| 3 + member.assignments.len() * 2)
            .sum::<usize>()
        + group
            .offsets
            .iter()
            .map(|offset| 1 + usize::from(offset.member_id.is_some()))
            .sum::<usize>()
}

fn group_list_heap(group: &ConsumerGroup) -> usize {
    group.id.len() + group.protocol.len() + group.topics.iter().map(String::len).sum::<usize>()
}

fn group_list_allocs(group: &ConsumerGroup) -> usize {
    3 + group.topics.len()
}
