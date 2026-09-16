use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use super::configs::ConfigSource;
use super::runner::spawn_lane;
use super::subjects::SubjectSource;
use super::topology::TopologySource;
use super::watermarks::WatermarkSource;
use crate::kafka::group::CommittedOffset;
use crate::kafka::ingest::IngestSet;
use crate::kafka::registry::{SchemaCompatibility, SchemaSubject, SchemaType};
use crate::kafka::session::ClusterSession;
use crate::kafka::store::{Change, ClusterStore, StoreSet, Topology};
use crate::kafka::testing::FakeCluster;
use crate::kafka::topic_config::{ConfigEntry, ConfigSource as EntrySource};
use crate::kafka::watermarks::Watermarks;

async fn wait_until(mut predicate: impl FnMut() -> bool) {
    for _ in 0..400 {
        if predicate() {
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!("condition not met");
}

fn start(cluster: FakeCluster) -> (FakeCluster, Arc<ClusterStore>, IngestSet) {
    let identity = cluster.identity().clone();
    let session: Arc<dyn ClusterSession> = Arc::new(cluster.clone());
    let store = ClusterStore::new(identity);
    let ingest = IngestSet::start(vec![session], StoreSet::new([store]));
    let store = Arc::clone(ingest.stores().get("local").expect("store"));
    (cluster, store, ingest)
}

#[tokio::test]
async fn ingest_set_fills_tables_from_fake_cluster() {
    let (_cluster, store, _ingest) = start(FakeCluster::local());
    wait_until(|| {
        store.ready()
            && store
                .watermarks
                .load()
                .is_some_and(|table| table.marks.contains_key("orders.created"))
            && store
                .offsets
                .load()
                .is_some_and(|table| table.groups.contains_key("order-processor"))
    })
    .await;

    let rows = store.topic_rows().unwrap();
    assert_eq!(rows[0].name.as_ref(), "orders.created");
    assert_eq!(rows[0].retained_messages, 16);
    assert_eq!(rows[0].produced_total, 16);
    assert_eq!(rows[0].group_count, 1);

    let groups = store.group_rows().unwrap();
    assert_eq!(groups[0].id.as_ref(), "order-processor");
    assert_eq!(groups[0].total_lag, 5);
    assert!(groups[0].lag_complete);

    let subjects = store.subject_rows().unwrap();
    assert_eq!(subjects[0].name.as_ref(), "orders.created-value");
    assert!(!subjects[0].degraded);

    let health = store.health();
    assert!(health.ready);
    assert_eq!(health.topic_count, 1);
    assert_eq!(health.group_count, 1);
    assert_eq!(health.broker_count, 1);
}

#[tokio::test]
async fn topology_diff_emits_added_and_removed_topics() {
    let (cluster, store, ingest) = start(FakeCluster::local());
    wait_until(|| store.ready()).await;
    let mut rx = store.bus.subscribe();

    cluster.add_topic("payments", 1, 4);
    ingest.kick_all("local");
    wait_until(|| {
        store
            .topology
            .load()
            .is_some_and(|topology| topology.topics.contains_key("payments"))
    })
    .await;

    let mut saw_add = false;
    while let Ok(change) = rx.try_recv() {
        if let Change::Topology(delta) = change
            && delta
                .added_topics
                .iter()
                .any(|name| name.as_ref() == "payments")
        {
            saw_add = true;
        }
    }
    assert!(saw_add);

    cluster.remove_topic("payments");
    ingest.kick_all("local");
    wait_until(|| {
        store
            .topology
            .load()
            .is_some_and(|topology| !topology.topics.contains_key("payments"))
    })
    .await;
}

#[tokio::test]
async fn failed_poll_keeps_the_last_table() {
    let cluster = FakeCluster::local().with_configs_error("denied");
    let (_cluster, store, _ingest) = start(cluster);
    wait_until(|| store.ready()).await;
    assert!(store.configs.load().is_none());
    assert!(store.configs.health().last_error.is_some());
    assert!(store.topic_rows().is_some());
}

#[tokio::test]
async fn unchanged_topology_does_not_bump_version() {
    let (_cluster, store, ingest) = start(FakeCluster::local());
    wait_until(|| store.ready()).await;
    let version = store.topology.version();
    ingest.kick_all("local");
    for _ in 0..80 {
        tokio::task::yield_now().await;
    }
    assert_eq!(store.topology.version(), version);
}

#[tokio::test(start_paused = true)]
async fn offsets_fast_tier_follows_interest() {
    let (cluster, store, _ingest) = start(FakeCluster::local());
    wait_until(|| store.offsets.load().is_some()).await;
    let _lease = store.interest.lease_group("order-processor");
    cluster.set_committed(
        "order-processor",
        vec![
            CommittedOffset {
                topic: "orders.created".into(),
                partition: 0,
                offset: 7,
            },
            CommittedOffset {
                topic: "orders.created".into(),
                partition: 1,
                offset: 7,
            },
        ],
    );
    tokio::time::advance(Duration::from_secs(2)).await;
    wait_until(|| {
        store
            .group_rows()
            .is_some_and(|rows| rows.first().is_some_and(|row| row.total_lag == 2))
    })
    .await;
}

#[tokio::test]
async fn search_follows_topology_and_subjects() {
    let (cluster, store, _ingest) = start(FakeCluster::local());
    wait_until(|| store.ready()).await;
    let hits = store.search("orders");
    assert!(hits.iter().any(|hit| hit.id == "orders.created"));
    cluster.set_subjects(vec![SchemaSubject {
        subject: "payments-value".into(),
        id: 9,
        schema_type: SchemaType::Json,
        latest_version: 1,
        versions: vec![1],
        compatibility: SchemaCompatibility::None,
        schema: "{}".into(),
    }]);
    store.subjects.kick();
    wait_until(|| {
        store
            .search("payments")
            .iter()
            .any(|hit| hit.kind == crate::kafka::search::SearchKind::Subject)
    })
    .await;
}

#[test]
fn topology_map_diff_is_granular() {
    let mut prev = BTreeMap::new();
    prev.insert(Arc::from("a"), 1);
    prev.insert(Arc::from("b"), 2);
    let mut next = BTreeMap::new();
    next.insert(Arc::from("b"), 3);
    next.insert(Arc::from("c"), 4);
    let (added, removed, changed) = super::topology::diff_maps(&prev, &next);
    assert_eq!(added, vec![Arc::from("c")]);
    assert_eq!(removed, vec![Arc::from("a")]);
    assert_eq!(changed, vec![Arc::from("b")]);
    let (added, removed, changed) = super::topology::diff_maps(&prev, &prev);
    assert!(added.is_empty() && removed.is_empty() && changed.is_empty());
}

#[tokio::test]
async fn spawn_lane_records_errors_without_commit() {
    let cluster = FakeCluster::local().unreachable();
    let store = Arc::new(ClusterStore::new(cluster.identity().clone()));
    let session: Arc<dyn ClusterSession> = Arc::new(cluster);
    let _task = spawn_lane(
        TopologySource::new(session, Arc::clone(&store)),
        Arc::clone(&store),
        |store| &store.topology,
    );
    wait_until(|| store.topology.health().last_error.is_some()).await;
    assert!(store.topology.load().is_none());
}

#[tokio::test]
async fn watermark_rates_use_high_delta() {
    let cluster = FakeCluster::local();
    let store = Arc::new(ClusterStore::new(cluster.identity().clone()));
    let session: Arc<dyn ClusterSession> = Arc::new(cluster.clone());
    let topology = Topology::from_snapshots(
        session.metadata().await.unwrap(),
        session.groups().await.unwrap(),
    );
    store.topology.commit(Arc::new(topology));
    let _task = spawn_lane(
        WatermarkSource::new(Arc::clone(&session), Arc::clone(&store)),
        Arc::clone(&store),
        |store| &store.watermarks,
    );
    wait_until(|| store.watermarks.load().is_some()).await;
    cluster.set_watermark("orders.created", 0, Watermarks { low: 0, high: 18 });
    store.watermarks.kick();
    wait_until(|| {
        store
            .series
            .last_topic_rate("orders.created")
            .is_some_and(|rate| rate > 0.0)
            || store.watermarks.version() >= 2
    })
    .await;
}

#[tokio::test]
async fn config_and_subject_sources_commit_list_projections() {
    let cluster = FakeCluster::local();
    cluster.set_topic_configs(
        "orders.created",
        vec![ConfigEntry {
            name: "cleanup.policy".into(),
            value: Some("compact".into()),
            source: EntrySource::DynamicTopic,
            read_only: false,
            sensitive: false,
        }],
    );
    let store = Arc::new(ClusterStore::new(cluster.identity().clone()));
    let session: Arc<dyn ClusterSession> = Arc::new(cluster);
    store.topology.commit(Arc::new(Topology::from_snapshots(
        session.metadata().await.unwrap(),
        session.groups().await.unwrap(),
    )));
    let _configs = spawn_lane(
        ConfigSource::new(Arc::clone(&session), Arc::clone(&store)),
        Arc::clone(&store),
        |store| &store.configs,
    );
    let _subjects = spawn_lane(
        SubjectSource::new(Arc::clone(&session), Arc::clone(&store)),
        Arc::clone(&store),
        |store| &store.subjects,
    );
    wait_until(|| store.configs.load().is_some() && store.subjects.load().is_some()).await;
    let rows = store.topic_rows().unwrap();
    assert_eq!(
        rows[0].cleanup_policy,
        crate::kafka::topic_config::CleanupPolicy::Compact
    );
}

#[test]
fn store_set_ready_requires_every_topology() {
    let local = ClusterStore::new(FakeCluster::local().identity().clone());
    let set = StoreSet::new([local]);
    assert!(!set.ready());
}
