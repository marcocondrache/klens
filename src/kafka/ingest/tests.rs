use std::sync::Arc;
use std::time::Duration;

use tokio::sync::broadcast::error::TryRecvError;
use tokio::task::JoinSet;

use super::*;
use crate::config::ClusterIngestConfig;
use crate::kafka::group::{
    CommittedOffset, GroupMember, GroupSnapshot, GroupState, MemberAssignment,
};
use crate::kafka::session::ClusterSession;
use crate::kafka::store::fixtures::identity;
use crate::kafka::store::{Change, ClusterStore};
use crate::kafka::testing::FakeCluster;

/// Long enough that no lane ever fires on its own; tests drive polls with
/// `kick`.
const IDLE: Duration = Duration::from_secs(600);

fn store(session: &FakeCluster) -> Arc<ClusterStore> {
    Arc::new(ClusterStore::new(session.identity().clone()))
}

fn port(session: &FakeCluster) -> Arc<dyn ClusterSession> {
    Arc::new(session.clone())
}

fn catalog_lanes(store: &Arc<ClusterStore>, session: &FakeCluster) -> JoinSet<()> {
    let mut lanes = JoinSet::new();
    lanes.spawn(run(
        Arc::clone(store),
        TopologyLane::with_interval(port(session), IDLE),
    ));
    lanes.spawn(run(
        Arc::clone(store),
        WatermarkLane::with_interval(port(session), IDLE),
    ));
    lanes
}

fn idle_lanes(store: &Arc<ClusterStore>, session: &FakeCluster) -> JoinSet<()> {
    let mut lanes = catalog_lanes(store, session);
    lanes.spawn(run(
        Arc::clone(store),
        ConfigLane::with_interval(port(session), IDLE),
    ));
    lanes.spawn(run(
        Arc::clone(store),
        SubjectLane::with_interval(port(session), IDLE),
    ));
    lanes.spawn(
        OffsetLane::new(port(session))
            .with_tiers(IDLE, Duration::from_secs(2), Duration::from_secs(20))
            .run(Arc::clone(store)),
    );
    lanes
}

fn group(id: &str, topic: &str, partitions: Vec<i32>, committed: &[(i32, i64)]) -> GroupSnapshot {
    GroupSnapshot {
        id: id.to_owned(),
        state: GroupState::Stable,
        protocol: "range".into(),
        coordinator: 1,
        members: vec![GroupMember {
            id: format!("{id}-m1"),
            client_id: "c1".into(),
            host: "127.0.0.1".into(),
            assignments: vec![MemberAssignment {
                topic: topic.to_owned(),
                partitions,
            }],
        }],
        committed: committed
            .iter()
            .map(|(partition, offset)| CommittedOffset {
                topic: topic.to_owned(),
                partition: *partition,
                offset: *offset,
            })
            .collect(),
    }
}

async fn settle<T>(mut ready: impl FnMut() -> Option<T>, what: &str) -> T {
    for _ in 0..2_000 {
        if let Some(value) = ready() {
            return value;
        }
        tokio::task::yield_now().await;
    }
    panic!("{what} never happened");
}

async fn wait_for(ready: impl Fn() -> bool, what: &str) {
    settle(|| ready().then_some(()), what).await;
}

#[tokio::test(start_paused = true)]
async fn the_topology_lane_assembles_brokers_topics_and_groups() {
    let session = FakeCluster::local();
    let store = store(&session);
    let _lanes = idle_lanes(&store, &session);

    wait_for(|| store.ready(), "topology commit").await;

    let topology = store.topology.load().expect("topology");
    assert_eq!(topology.cluster_id.as_deref(), Some("test-cluster"));
    assert_eq!(topology.brokers.len(), 1);
    assert_eq!(
        topology.topic("orders.created").unwrap().partitions.len(),
        2
    );
    assert_eq!(
        topology.groups_for_topic("orders.created"),
        [Arc::from("order-processor")],
        "the reverse index is built at commit time"
    );
    assert_eq!(store.topic_rows().len(), 1);
    assert_eq!(store.broker_rows()[0].host, "localhost");
}

#[tokio::test(start_paused = true)]
async fn a_topology_poll_that_changes_nothing_does_not_bump_the_version() {
    let session = FakeCluster::local();
    let store = store(&session);
    let _lanes = idle_lanes(&store, &session);

    wait_for(|| store.ready(), "topology commit").await;
    let polls = session.calls().metadata();

    store.topology.kick();
    wait_for(
        || session.calls().metadata() > polls,
        "second topology poll",
    )
    .await;

    assert_eq!(
        store.topology.version(),
        1,
        "an unchanged cluster costs a hash-equal check and nothing else"
    );
}

#[tokio::test(start_paused = true)]
async fn a_new_topic_is_published_as_a_granular_delta() {
    let session = FakeCluster::local();
    let store = store(&session);
    let _lanes = idle_lanes(&store, &session);

    wait_for(|| store.ready(), "topology commit").await;
    let mut events = store.bus.subscribe();

    session.add_partition("orders.created", 2, Default::default());
    store.topology.kick();

    let delta = settle(
        || match events.try_recv() {
            Ok(Change::Topology(delta)) => Some(delta),
            _ => None,
        },
        "topology delta",
    )
    .await;

    assert_eq!(delta.changed_topics, [Arc::from("orders.created")]);
    assert!(delta.added_topics.is_empty());
    assert!(delta.removed_topics.is_empty());
    assert_eq!(delta.version, 2);
}

#[tokio::test(start_paused = true)]
async fn the_topology_lane_keeps_the_search_index_current() {
    let session = FakeCluster::local();
    let store = store(&session);
    let _lanes = idle_lanes(&store, &session);

    wait_for(|| store.ready(), "topology commit").await;
    wait_for(|| store.subjects.version() > 0, "subject commit").await;

    let hits = store.search("order");
    let ids: Vec<&str> = hits.iter().map(|hit| hit.id.as_str()).collect();
    assert_eq!(
        ids,
        vec!["orders.created", "order-processor", "orders.created-value"],
        "topics, groups and subjects all reach the index"
    );
}

#[tokio::test(start_paused = true)]
async fn the_watermark_lane_feeds_latest_rates() {
    let session = FakeCluster::local().with_growing_watermarks(20);
    let store = store(&session);
    let _lanes = idle_lanes(&store, &session);

    wait_for(|| store.watermarks.version() > 0, "first watermark tick").await;
    assert_eq!(store.rates.get("orders.created"), Some(0.0));

    tokio::time::advance(Duration::from_secs(2)).await;
    store.watermarks.kick();
    wait_for(|| store.watermarks.version() > 1, "second watermark tick").await;

    assert!(store.rates.get("orders.created").unwrap() > 0.0);
}

#[tokio::test(start_paused = true)]
async fn a_watermark_tick_matches_the_rate_store() {
    let session = FakeCluster::local();
    let store = store(&session);
    let _lanes = idle_lanes(&store, &session);

    wait_for(|| store.watermarks.version() > 0, "watermark commit").await;
    let mut events = store.bus.subscribe();

    tokio::time::advance(Duration::from_secs(20)).await;
    store.watermarks.kick();

    let tick = settle(
        || match events.try_recv() {
            Ok(Change::Watermarks(tick)) => Some(tick),
            _ => None,
        },
        "watermark tick",
    )
    .await;

    assert_eq!(tick.rate("orders.created"), Some(0.0));
    assert_eq!(store.rates.get("orders.created"), Some(0.0));
}

#[tokio::test(start_paused = true)]
async fn an_idle_cluster_still_zeros_the_latest_rate() {
    let session = FakeCluster::local();
    let store = store(&session);
    let _lanes = idle_lanes(&store, &session);

    wait_for(|| store.watermarks.version() > 0, "watermark commit").await;

    store.watermarks.kick();
    wait_for(
        || session.calls().watermarks() >= 2,
        "second watermark poll",
    )
    .await;
    assert_eq!(
        store.watermarks.version(),
        1,
        "an unmoved log does not tick"
    );

    tokio::time::advance(Duration::from_secs(20)).await;
    store.watermarks.kick();
    wait_for(|| store.watermarks.version() > 1, "heartbeat tick").await;

    assert_eq!(store.rates.get("orders.created"), Some(0.0));
}

#[tokio::test(start_paused = true)]
async fn the_config_lane_populates_the_config_table() {
    let session = FakeCluster::local();
    let store = store(&session);
    let _lanes = idle_lanes(&store, &session);

    wait_for(|| store.configs.version() > 0, "config commit").await;

    let row = store.topic_row("orders.created").expect("orders exists");
    assert_eq!(row.retention_ms, 604_800_000);
    assert_eq!(
        store.topic_configs("orders.created").unwrap().len(),
        2,
        "configs are served from the table, not a live describe"
    );
}

#[tokio::test(start_paused = true)]
async fn a_changed_config_names_only_the_topic_that_moved() {
    let session = FakeCluster::local().extra_topic("payments", 1, 4);
    let store = store(&session);
    let _lanes = idle_lanes(&store, &session);

    wait_for(|| store.configs.version() > 0, "config commit").await;
    let mut events = store.bus.subscribe();

    session.set_topic_configs(
        "orders.created",
        vec![crate::kafka::store::fixtures::config(
            "cleanup.policy",
            "compact",
        )],
    );
    store.configs.kick();

    let delta = settle(
        || match events.try_recv() {
            Ok(Change::Configs(delta)) => Some(delta),
            _ => None,
        },
        "config delta",
    )
    .await;

    assert_eq!(delta.topics, [Arc::from("orders.created")]);
}

#[tokio::test(start_paused = true)]
async fn the_subject_lane_stores_the_list_projection_only() {
    let session = FakeCluster::local();
    let store = store(&session);
    let _lanes = idle_lanes(&store, &session);

    wait_for(|| store.subjects.version() > 0, "subject commit").await;

    let rows = store.subject_rows();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].subject.as_ref(), "orders.created-value");
    assert_eq!(rows[0].info.versions, vec![1, 2]);
    assert!(
        !store.search("orders.created-value").is_empty(),
        "subjects join the search index"
    );
}

#[tokio::test(start_paused = true)]
async fn a_registry_outage_degrades_only_the_subject_lane() {
    let session = FakeCluster::local().with_subjects_error("registry down");
    let store = store(&session);
    let _lanes = idle_lanes(&store, &session);

    wait_for(|| store.ready(), "topology commit").await;
    wait_for(
        || store.subjects.health().last_error.is_some(),
        "subject failure",
    )
    .await;

    let health = store.health();
    assert!(health.topology.healthy());
    assert!(!health.subjects.healthy());
    assert!(
        store.subjects.load().is_none(),
        "an unavailable source is not an empty listing"
    );
    assert_eq!(store.topic_rows().len(), 1, "the catalog still serves");
}

#[tokio::test(start_paused = true)]
async fn a_background_group_refreshes_on_the_slow_tier() {
    let session = FakeCluster::local();
    let store = store(&session);
    let lane = OffsetLane::new(port(&session)).with_tiers(
        Duration::from_secs(1),
        Duration::from_secs(2),
        Duration::from_secs(20),
    );
    let _lanes = catalog_lanes(&store, &session);
    wait_for(|| store.ready(), "topology commit").await;

    let first = lane.sweep(&store).await;
    assert_eq!(first.refreshed, [Arc::from("order-processor")]);

    tokio::time::advance(Duration::from_secs(5)).await;
    assert!(
        lane.sweep(&store).await.is_empty(),
        "a group nobody is watching waits out the slow tier"
    );

    tokio::time::advance(Duration::from_secs(20)).await;
    assert_eq!(
        lane.sweep(&store).await.refreshed,
        [Arc::from("order-processor")]
    );
}

#[tokio::test(start_paused = true)]
async fn interest_promotes_a_group_to_the_fast_tier() {
    let session = FakeCluster::local();
    let store = store(&session);
    let lane = OffsetLane::new(port(&session)).with_tiers(
        Duration::from_secs(1),
        Duration::from_secs(2),
        Duration::from_secs(20),
    );
    let _lanes = catalog_lanes(&store, &session);
    wait_for(|| store.ready(), "topology commit").await;
    lane.sweep(&store).await;

    let lease = store.interest.lease_group("order-processor");
    tokio::time::advance(Duration::from_secs(3)).await;
    assert_eq!(
        lane.sweep(&store).await.refreshed,
        [Arc::from("order-processor")],
        "a watched group refreshes on the fast tier"
    );

    drop(lease);
    tokio::time::advance(Duration::from_secs(3)).await;
    assert!(
        lane.sweep(&store).await.is_empty(),
        "dropping the lease releases the fast tier immediately"
    );
}

#[tokio::test(start_paused = true)]
async fn a_wave_commits_once_and_publishes_once() {
    let mut session = FakeCluster::local();
    for index in 0..12 {
        session = session.extra_group(group(
            &format!("group-{index:02}"),
            "orders.created",
            vec![0],
            &[(0, 2)],
        ));
    }
    let store = store(&session);
    let lane = OffsetLane::new(port(&session));
    let _lanes = catalog_lanes(&store, &session);
    wait_for(|| store.ready(), "topology commit").await;
    let mut events = store.bus.subscribe();

    let wave = lane.sweep(&store).await;

    assert_eq!(wave.refreshed.len(), 13);
    assert_eq!(
        store.offsets.version(),
        1,
        "a wave touching thirteen groups is one map rebuild, not thirteen"
    );
    match events.try_recv() {
        Ok(Change::GroupOffsets(published)) => assert_eq!(published.groups.len(), 13),
        other => panic!("expected one wave, got {other:?}"),
    }
    assert!(matches!(events.try_recv(), Err(TryRecvError::Empty)));
}

#[tokio::test(start_paused = true)]
async fn offset_fetches_respect_the_concurrency_cap() {
    let mut session = FakeCluster::local().with_offsets_delay(Duration::from_millis(50));
    for index in 0..16 {
        session = session.extra_group(group(
            &format!("group-{index:02}"),
            "orders.created",
            vec![0],
            &[(0, 2)],
        ));
    }
    let store = store(&session);
    let lane = OffsetLane::new(port(&session)).with_concurrency(4);
    let _lanes = catalog_lanes(&store, &session);
    wait_for(|| store.ready(), "topology commit").await;

    lane.sweep(&store).await;

    assert_eq!(session.calls().committed_offsets(), 17);
    assert_eq!(
        session.calls().committed_offsets_peak(),
        4,
        "the wave is wide but bounded"
    );
}

#[tokio::test(start_paused = true)]
async fn lag_is_computed_from_the_tables() {
    let session = FakeCluster::local();
    let store = store(&session);
    let lane = OffsetLane::new(port(&session));
    let _lanes = catalog_lanes(&store, &session);
    wait_for(|| store.watermarks.version() > 0, "watermark commit").await;

    lane.sweep(&store).await;

    assert_eq!(store.group_row("order-processor").unwrap().total_lag, 5);
    assert_eq!(
        session.calls().committed_offsets(),
        1,
        "lag never costs an extra broker call"
    );
}

#[tokio::test(start_paused = true)]
async fn a_failed_group_degrades_alone_and_keeps_its_last_offsets() {
    let session = FakeCluster::local();
    let store = store(&session);
    let lane = OffsetLane::new(port(&session));
    let _lanes = catalog_lanes(&store, &session);
    wait_for(|| store.ready(), "topology commit").await;
    lane.sweep(&store).await;
    let sampled_at = store
        .offsets
        .load()
        .unwrap()
        .get("order-processor")
        .unwrap()
        .sampled_at;

    session.set_offsets_error(Some("coordinator not available"));
    tokio::time::advance(Duration::from_secs(30)).await;
    let wave = lane.sweep(&store).await;

    assert_eq!(wave.failed, [Arc::from("order-processor")]);
    assert!(wave.refreshed.is_empty());
    let offsets = store.offsets.load().unwrap();
    let group = offsets.get("order-processor").expect("stale value kept");
    assert_eq!(group.committed.len(), 2);
    assert_eq!(
        group.sampled_at, sampled_at,
        "sampled_at exposes the staleness instead of hiding it"
    );
}

#[tokio::test(start_paused = true)]
async fn a_removed_group_is_dropped_from_the_offset_table() {
    let session = FakeCluster::local();
    let store = store(&session);
    let lane = OffsetLane::new(port(&session));
    let _lanes = catalog_lanes(&store, &session);
    wait_for(|| store.ready(), "topology commit").await;
    lane.sweep(&store).await;

    session.remove_group("order-processor");
    store.topology.kick();
    wait_for(
        || store.topology.load().is_some_and(|t| t.groups.is_empty()),
        "group removal",
    )
    .await;

    let wave = lane.sweep(&store).await;

    assert_eq!(wave.dropped, [Arc::from("order-processor")]);
    assert!(store.offsets.load().unwrap().groups.is_empty());
    assert!(store.group_rows().is_empty());

    session.put_group(group(
        "order-processor",
        "orders.created",
        vec![0],
        &[(0, 1)],
    ));
    store.topology.kick();
    wait_for(
        || {
            store
                .topology
                .load()
                .is_some_and(|topology| topology.group("order-processor").is_some())
        },
        "group returning",
    )
    .await;

    assert_eq!(
        lane.sweep(&store).await.refreshed,
        [Arc::from("order-processor")],
        "a group that comes back is due immediately, not on its old schedule"
    );
}

#[tokio::test(start_paused = true)]
async fn a_deleted_topic_loses_its_rate() {
    let session = FakeCluster::local().extra_topic("payments", 1, 4);
    let store = store(&session);
    let _lanes = idle_lanes(&store, &session);

    wait_for(|| store.watermarks.version() > 0, "watermark commit").await;
    assert_eq!(store.rates.get("payments"), Some(0.0));

    session.remove_topic("payments");
    store.topology.kick();
    wait_for(
        || {
            store
                .topology
                .load()
                .is_some_and(|topology| topology.topic("payments").is_none())
        },
        "topic removal",
    )
    .await;

    assert_eq!(store.rates.get("payments"), None);
    assert_eq!(store.rates.get("orders.created"), Some(0.0));
}

#[tokio::test(start_paused = true)]
async fn downstream_lanes_wait_for_topology_instead_of_committing_nothing() {
    let session = FakeCluster::local();
    let store = store(&session);
    let mut lanes = JoinSet::new();
    lanes.spawn(run(
        Arc::clone(&store),
        WatermarkLane::with_interval(port(&session), Duration::from_secs(600)),
    ));
    lanes.spawn(run(
        Arc::clone(&store),
        ConfigLane::with_interval(port(&session), Duration::from_secs(600)),
    ));

    wait_for(
        || store.watermarks.health().checked_at.is_some(),
        "watermark poll",
    )
    .await;

    assert!(
        store.watermarks.load().is_none(),
        "an empty table would read as a cluster with no partitions"
    );
    assert!(store.configs.load().is_none());
    assert_eq!(
        session.calls().watermarks(),
        0,
        "and it costs no broker call"
    );
}

#[tokio::test(start_paused = true)]
async fn every_lane_runs_per_cluster_and_stops_with_the_ingest() {
    let prod = FakeCluster::named("prod");
    let staging = FakeCluster::named("staging");
    let (stores, lanes) = Ingest::bootstrap(vec![port(&prod), port(&staging)]);

    assert_eq!(lanes.lane_count(), 10, "five lanes per cluster");
    wait_for(|| stores.ready(), "both clusters ready").await;
    assert_eq!(stores.names().collect::<Vec<_>>(), vec!["prod", "staging"]);

    drop(lanes);
    tokio::task::yield_now().await;
    let polls = prod.calls().metadata();
    tokio::time::advance(Duration::from_secs(3_600)).await;
    tokio::task::yield_now().await;

    assert_eq!(
        prod.calls().metadata(),
        polls,
        "dropping the ingest aborts every lane task"
    );
}

#[tokio::test(start_paused = true)]
async fn one_cluster_never_wakes_another() {
    let prod = FakeCluster::named("prod");
    let staging = FakeCluster::named("staging");
    let prod_store = Arc::new(ClusterStore::new(identity("prod")));
    let staging_store = Arc::new(ClusterStore::new(identity("staging")));
    let _lanes = Ingest::start([
        (
            Arc::clone(&prod_store),
            port(&prod),
            ClusterIngestConfig::default(),
        ),
        (
            Arc::clone(&staging_store),
            port(&staging),
            ClusterIngestConfig::default(),
        ),
    ]);

    wait_for(|| prod_store.ready() && staging_store.ready(), "both ready").await;
    let mut staging_events = staging_store.bus.subscribe();
    while staging_events.try_recv().is_ok() {}

    prod.add_partition("orders.created", 2, Default::default());
    prod_store.topology.kick();
    wait_for(|| prod_store.topology.version() > 1, "prod delta").await;

    assert!(
        matches!(staging_events.try_recv(), Err(TryRecvError::Empty)),
        "per-cluster buses keep the blast radius at one cluster"
    );
}
