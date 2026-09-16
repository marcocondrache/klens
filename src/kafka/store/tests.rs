use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use super::Lane;
use super::bus::{Change, ChangeBus};
use super::cluster::ClusterStore;
use super::interest::InterestRegistry;
use super::series::{Point, SeriesStore};
use super::tables::{
    ConfigTable, GroupOffsets, OffsetTable, SubjectInfo, SubjectTable, Topology, WatermarkTable,
};
use crate::kafka::cluster::ClusterIdentity;
use crate::kafka::group::{
    CommittedOffset, GroupMember, GroupSnapshot, GroupState, MemberAssignment,
};
use crate::kafka::metadata::{BrokerMetadata, MetadataSnapshot, PartitionMetadata, TopicMetadata};
use crate::kafka::registry::{SchemaCompatibility, SchemaType};
use crate::kafka::search::SearchKind;
use crate::kafka::topic_config::{CleanupPolicy, ConfigEntry, ConfigSource};
use crate::kafka::watermarks::Watermarks;
use crate::utils::utc_now;

fn identity() -> ClusterIdentity {
    ClusterIdentity {
        name: "local".into(),
        bootstrap_servers: vec!["localhost:9092".into()],
        security_protocol: crate::config::SecurityProtocol::Plaintext,
    }
}

fn partition(id: i32, replicas: Vec<i32>, isr: Vec<i32>) -> PartitionMetadata {
    PartitionMetadata {
        id,
        leader: replicas.first().copied().unwrap_or(1),
        replicas,
        isr,
    }
}

fn snapshot() -> (MetadataSnapshot, Vec<GroupSnapshot>) {
    let meta = MetadataSnapshot {
        cluster_id: Some("abc".into()),
        brokers: vec![BrokerMetadata {
            id: 1,
            host: "broker-a".into(),
            port: 9092,
        }],
        topics: vec![TopicMetadata {
            name: "orders".into(),
            internal: false,
            partitions: vec![
                partition(0, vec![1, 2], vec![1, 2]),
                partition(1, vec![1, 2], vec![1]),
            ],
        }],
    };
    let groups = vec![GroupSnapshot {
        id: "billing".into(),
        state: GroupState::Stable,
        protocol: "range".into(),
        coordinator: 1,
        members: vec![GroupMember {
            id: "m1".into(),
            client_id: "c1".into(),
            host: "127.0.0.1".into(),
            assignments: vec![MemberAssignment {
                topic: "orders".into(),
                partitions: vec![0, 1],
            }],
        }],
        committed: vec![
            CommittedOffset {
                topic: "orders".into(),
                partition: 0,
                offset: 4,
            },
            CommittedOffset {
                topic: "orders".into(),
                partition: 1,
                offset: 8,
            },
        ],
    }];
    (meta, groups)
}

fn seed_store() -> ClusterStore {
    let store = ClusterStore::new(identity());
    let (meta, groups) = snapshot();
    let topology = Topology::from_snapshots(meta, groups);
    let topic = topology.intern_topic("orders");
    let group = topology.intern_group("billing");
    store.topology.commit(Arc::new(topology));

    let mut marks = HashMap::new();
    marks.insert(
        Arc::clone(&topic),
        HashMap::from([
            (0, Watermarks { low: 2, high: 10 }),
            (1, Watermarks { low: 0, high: 12 }),
        ]),
    );
    store.watermarks.commit(Arc::new(WatermarkTable {
        sampled_at: utc_now(),
        marks,
    }));

    let mut groups = HashMap::new();
    groups.insert(
        Arc::clone(&group),
        Arc::new(GroupOffsets {
            sampled_at: utc_now(),
            committed: vec![
                CommittedOffset {
                    topic: "orders".into(),
                    partition: 0,
                    offset: 4,
                },
                CommittedOffset {
                    topic: "orders".into(),
                    partition: 1,
                    offset: 8,
                },
            ],
        }),
    );
    store.offsets.commit(Arc::new(OffsetTable { groups }));

    let mut topics = HashMap::new();
    topics.insert(
        topic,
        Arc::new(vec![
            ConfigEntry {
                name: "cleanup.policy".into(),
                value: Some("compact".into()),
                source: ConfigSource::DynamicTopic,
                read_only: false,
                sensitive: false,
            },
            ConfigEntry {
                name: "retention.ms".into(),
                value: Some("60000".into()),
                source: ConfigSource::DynamicTopic,
                read_only: false,
                sensitive: false,
            },
        ]),
    );
    store.configs.commit(Arc::new(ConfigTable { topics }));
    store.rebuild_search();
    store
}

#[test]
fn lane_commit_bumps_version_and_load_is_pointer_clone() {
    let lane = Lane::new();
    assert_eq!(lane.version(), 0);
    assert!(lane.load().is_none());
    let first = Arc::new(7u32);
    assert_eq!(lane.commit(Arc::clone(&first)), 1);
    assert!(Arc::ptr_eq(&lane.load().unwrap(), &first));
    let second = Arc::new(8u32);
    assert_eq!(lane.commit(Arc::clone(&second)), 2);
    assert_eq!(*lane.load().unwrap(), 8);
}

#[test]
fn no_change_poll_updates_health_only() {
    let lane = Lane::new();
    lane.commit(Arc::new(1u32));
    lane.record_health(Duration::from_millis(12), None);
    assert_eq!(lane.version(), 1);
    assert_eq!(lane.health().last_poll_ms, Some(12));
    assert!(lane.health().last_error.is_none());
    lane.record_health(Duration::from_millis(9), Some("down".into()));
    assert_eq!(lane.version(), 1);
    assert_eq!(lane.health().last_error.as_deref(), Some("down"));
}

#[test]
fn topic_row_joins_watermarks_configs_and_group_index() {
    let store = seed_store();
    let rows = store.topic_rows().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].name.as_ref(), "orders");
    assert_eq!(rows[0].partition_count, 2);
    assert_eq!(rows[0].replication_factor, 2);
    assert_eq!(rows[0].retained_messages, 20, " (10-2) + (12-0) ");
    assert_eq!(rows[0].produced_total, 22);
    assert_eq!(rows[0].cleanup_policy, CleanupPolicy::Compact);
    assert_eq!(rows[0].retention_ms, 60_000);
    assert_eq!(rows[0].group_count, 1);
    assert!(rows[0].under_replicated);
}

#[test]
fn group_row_lag_is_a_join_and_missing_watermarks_are_incomplete() {
    let store = seed_store();
    let rows = store.group_rows().unwrap();
    assert_eq!(rows[0].id.as_ref(), "billing");
    assert_eq!(rows[0].total_lag, 10, " (10-4) + (12-8) ");
    assert!(rows[0].lag_complete);
    assert_eq!(rows[0].topic_names, vec!["orders"]);

    let detail = store.group_detail("billing").unwrap();
    assert_eq!(detail.offsets.len(), 2);
    assert_eq!(detail.offsets[0].lag, 6);

    store.watermarks.commit(Arc::new(WatermarkTable {
        sampled_at: utc_now(),
        marks: HashMap::new(),
    }));
    let incomplete = store.group_rows().unwrap();
    assert!(!incomplete[0].lag_complete);
    assert_eq!(incomplete[0].total_lag, 0);
}

#[test]
fn unfetched_group_offsets_report_no_lag_and_incomplete() {
    let store = ClusterStore::new(identity());
    let (meta, groups) = snapshot();
    let topology = Topology::from_snapshots(meta, groups);
    let topic = topology.intern_topic("orders");
    store.topology.commit(Arc::new(topology));
    store.watermarks.commit(Arc::new(WatermarkTable {
        sampled_at: utc_now(),
        marks: HashMap::from([(
            topic,
            HashMap::from([
                (0, Watermarks { low: 2, high: 10 }),
                (1, Watermarks { low: 0, high: 12 }),
            ]),
        )]),
    }));

    // No offsets committed yet: don't guess "committed = 0" from watermarks.
    let rows = store.group_rows().unwrap();
    assert_eq!(rows[0].total_lag, 0);
    assert!(!rows[0].lag_complete);
    assert_eq!(rows[0].topic_names, vec!["orders"]);

    let detail = store.group_detail("billing").unwrap();
    assert!(detail.offsets.is_empty());
    assert!(!detail.row.lag_complete);
}

#[test]
fn committed_only_groups_join_topic_views() {
    let store = ClusterStore::new(identity());
    let (mut meta, mut groups) = snapshot();
    meta.topics.push(TopicMetadata {
        name: "logs".into(),
        internal: false,
        partitions: vec![partition(0, vec![1], vec![1])],
    });
    // An Empty group with no member assignments; its only link to "orders"
    // is a committed offset, which lives in the offsets lane.
    groups.push(GroupSnapshot {
        id: "archiver".into(),
        state: GroupState::Empty,
        protocol: String::new(),
        coordinator: 1,
        members: Vec::new(),
        committed: Vec::new(),
    });
    let topology = Topology::from_snapshots(meta, groups);
    let archiver = topology.intern_group("archiver");
    store.topology.commit(Arc::new(topology));
    store.offsets.commit(Arc::new(OffsetTable {
        groups: HashMap::from([(
            archiver,
            Arc::new(GroupOffsets {
                sampled_at: utc_now(),
                committed: vec![CommittedOffset {
                    topic: "orders".into(),
                    partition: 0,
                    offset: 4,
                }],
            }),
        )]),
    }));

    let rows = store.topic_rows().unwrap();
    let orders = rows
        .iter()
        .find(|row| row.name.as_ref() == "orders")
        .unwrap();
    assert_eq!(
        orders.group_count, 2,
        "billing (assigned) + archiver (committed only)"
    );

    let consumers = store.topic_groups("orders").unwrap();
    assert!(consumers.iter().any(|row| row.id.as_ref() == "archiver"));
    assert!(consumers.iter().any(|row| row.id.as_ref() == "billing"));

    assert_eq!(store.topic_groups("logs"), Some(Vec::new()));
    assert!(store.topic_groups("unknown").is_none());
}

#[test]
fn topic_groups_projects_lag_on_that_topic() {
    let store = seed_store();
    let rows = store.topic_groups("orders").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].id.as_ref(), "billing");
    assert_eq!(rows[0].lag_on_topic, 10);
    assert_eq!(rows[0].member_count, 1);
}

#[test]
fn broker_rows_count_replicas_and_leaders() {
    let store = seed_store();
    let rows = store.broker_rows().unwrap();
    assert_eq!(rows[0].id, 1);
    assert_eq!(rows[0].partition_count, 2);
    assert_eq!(rows[0].leader_count, 2);
}

#[test]
fn projections_over_missing_tables_return_none() {
    let store = ClusterStore::new(identity());
    assert!(store.topic_rows().is_none());
    assert!(store.group_rows().is_none());
    assert!(!store.ready());
    assert!(!store.health().ready);
}

#[test]
fn search_index_matches_names() {
    let store = seed_store();
    let hits = store.search("ord");
    assert!(hits.iter().any(|hit| hit.kind == SearchKind::Topic));
    store.subjects.commit(Arc::new(SubjectTable {
        subjects: [(
            Arc::from("orders-value"),
            SubjectInfo {
                id: 1,
                schema_type: SchemaType::Avro,
                latest_version: 2,
                versions: vec![1, 2],
                compatibility: SchemaCompatibility::Backward,
                degraded: false,
                error: None,
            },
        )]
        .into_iter()
        .collect(),
    }));
    store.rebuild_search();
    let hits = store.search("orders");
    assert!(hits.iter().any(|hit| hit.kind == SearchKind::Subject));
    assert!(store.search("   ").is_empty());
}

#[test]
fn series_store_caps_and_prunes() {
    let series = SeriesStore::new(3);
    let at = utc_now();
    series.push_topic_rate(Arc::from("orders"), at, 1.0);
    series.push_topic_rate(Arc::from("orders"), at, 2.0);
    series.push_topic_rate(Arc::from("orders"), at, 3.0);
    series.push_topic_rate(Arc::from("orders"), at, 4.0);
    let history = series.topic_history("orders");
    assert_eq!(history.len(), 3);
    assert_eq!(history[0].value, 2.0);
    assert_eq!(series.last_topic_rate("orders"), Some(4.0));
    series.prune_topics(|name| name == "missing");
    assert!(series.topic_history("orders").is_empty());
}

#[test]
fn change_bus_delivers_per_subscriber() {
    let bus = ChangeBus::new();
    let mut rx = bus.subscribe();
    bus.publish(Change::Subjects { version: 3 });
    let got = rx.try_recv().unwrap();
    assert!(matches!(got, Change::Subjects { version: 3 }));
}

#[tokio::test(start_paused = true)]
async fn interest_lease_and_touch_drive_hot_set() {
    let registry = InterestRegistry::new(Duration::from_secs(30));
    assert!(registry.hot_groups().is_empty());
    {
        let _lease = registry.lease_group("billing");
        assert!(registry.hot_groups().contains("billing"));
        registry.touch_group("orders");
        assert!(registry.hot_groups().contains("orders"));
    }
    assert!(!registry.hot_groups().contains("billing"));
    assert!(registry.hot_groups().contains("orders"));
    tokio::time::advance(Duration::from_secs(31)).await;
    assert!(!registry.hot_groups().contains("orders"));
}

#[test]
fn offset_table_patches_one_wave() {
    let prev = OffsetTable::default();
    let updated = prev.patch(
        None,
        HashMap::from([(
            Arc::from("g1"),
            Arc::new(GroupOffsets {
                sampled_at: utc_now(),
                committed: Vec::new(),
            }),
        )]),
    );
    assert!(updated.groups.contains_key("g1"));
    let topology = Topology::from_snapshots(
        MetadataSnapshot {
            cluster_id: None,
            brokers: Vec::new(),
            topics: Vec::new(),
        },
        Vec::new(),
    );
    let pruned = updated.patch(Some(&topology), HashMap::new());
    assert!(pruned.groups.is_empty());
}

#[test]
fn point_history_is_server_timestamped() {
    let at = utc_now();
    let point = Point { at, value: 1.5 };
    assert_eq!(point.value, 1.5);
    assert_eq!(point.at, at);
}
