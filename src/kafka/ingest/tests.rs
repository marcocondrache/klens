use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::broadcast::error::TryRecvError;
use tokio::task::JoinSet;

use super::*;
use crate::kafka::acl::AclListing;
use crate::kafka::group::{
    CommittedOffset, GroupMember, GroupSnapshot, GroupState, MemberAssignment,
};
use crate::kafka::metadata::Watermarks;
use crate::kafka::model::QuotaListing;
use crate::kafka::session::ClusterSession;
use crate::kafka::store::{Change, ClusterStore};
use crate::testing::{Api, FakeCluster, LogCapture, config_entry, quiesce, settle, subject, until};

const IDLE: Duration = Duration::from_secs(600);

fn store(session: &FakeCluster) -> Arc<ClusterStore> {
    Arc::new(ClusterStore::new(
        session.identity().clone(),
        IngestTuning::default().interest_ttl,
    ))
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
        WatermarkLane::with_interval(port(session), IDLE, &IngestTuning::default()),
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

fn log_dir_lanes(
    store: &Arc<ClusterStore>,
    session: &FakeCluster,
    interval: Duration,
) -> JoinSet<()> {
    let mut lanes = JoinSet::new();
    lanes.spawn(run(
        Arc::clone(store),
        TopologyLane::with_interval(port(session), IDLE),
    ));
    lanes.spawn(run(
        Arc::clone(store),
        LogDirLane::with_interval(port(session), interval),
    ));
    lanes
}

fn quota_lane(store: &Arc<ClusterStore>, session: &FakeCluster, interval: Duration) -> JoinSet<()> {
    let mut lanes = JoinSet::new();
    lanes.spawn(run(
        Arc::clone(store),
        QuotaLane::with_interval(port(session), interval),
    ));
    lanes
}

fn group(id: &str, topic: &str, partitions: Vec<i32>, committed: &[(i32, i64)]) -> GroupSnapshot {
    GroupSnapshot {
        id: id.to_owned(),
        state: GroupState::Stable,
        protocol: "range".into(),
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

async fn sweep(lane: &OffsetLane, store: &ClusterStore) -> Wave {
    let topology = store.topology.load().expect("topology commit");
    lane.sweep(store, &topology).await
}

#[tokio::test(start_paused = true)]
async fn the_topology_lane_assembles_brokers_topics_and_groups() {
    let session = FakeCluster::local();
    let store = store(&session);
    let _lanes = idle_lanes(&store, &session);

    until("topology commit", || store.ready()).await;

    let topology = store.topology.load().expect("topology");
    assert_eq!(topology.cluster_id.as_deref(), Some("test-cluster"));
    assert_eq!(topology.brokers.len(), 1);
    assert_eq!(topology.topics["orders.created"].partitions.len(), 2);
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

    until("topology commit", || store.ready()).await;
    let polls = session.calls(Api::Metadata);

    store.topology.kick();
    until("second topology poll", || {
        session.calls(Api::Metadata) > polls
    })
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

    until("topology commit", || store.ready()).await;
    let mut events = store.bus.subscribe();

    session.add_partition("orders.created", 2);
    store.topology.kick();

    let delta = settle("topology delta", || match events.try_recv() {
        Ok(Change::Topology(delta)) => Some(delta),
        _ => None,
    })
    .await;

    assert_eq!(delta.changed_topics, [Arc::from("orders.created")]);
    assert!(delta.added_topics.is_empty());
    assert!(delta.removed_topics.is_empty());
}

#[tokio::test(start_paused = true)]
async fn the_topology_lane_keeps_search_current() {
    let session = FakeCluster::local();
    let store = store(&session);
    let _lanes = idle_lanes(&store, &session);

    until("topology commit", || store.ready()).await;
    until("subject commit", || store.subjects.version() > 0).await;

    let hits = store.search("order");
    let ids: Vec<&str> = hits.iter().map(|hit| hit.id.as_str()).collect();
    assert_eq!(
        ids,
        vec!["orders.created", "order-processor", "orders.created-value"],
        "topics, groups and subjects are all searchable"
    );
}

#[tokio::test(start_paused = true)]
async fn the_watermark_lane_feeds_latest_rates() {
    let session = FakeCluster::local().with_growing_watermarks(20);
    let store = store(&session);
    let _lanes = idle_lanes(&store, &session);

    until("first watermark tick", || store.watermarks.version() > 0).await;
    assert_eq!(store.rates.get("orders.created"), Some(0.0));

    tokio::time::advance(Duration::from_secs(2)).await;
    store.watermarks.kick();
    until("second watermark tick", || store.watermarks.version() > 1).await;

    assert!(store.rates.get("orders.created").unwrap() > 0.0);
}

#[tokio::test(start_paused = true)]
async fn a_watermark_tick_matches_the_rate_store() {
    let session = FakeCluster::local().with_growing_watermarks(20);
    let store = store(&session);
    let _lanes = idle_lanes(&store, &session);

    until("watermark commit", || store.watermarks.version() > 0).await;
    let mut events = store.bus.subscribe();

    tokio::time::advance(Duration::from_secs(2)).await;
    store.watermarks.kick();

    let tick = settle("watermark tick", || match events.try_recv() {
        Ok(Change::Watermarks(tick)) => Some(tick),
        _ => None,
    })
    .await;

    let rate = tick
        .rate("orders.created")
        .expect("a moving topic is in the tick");
    assert!(rate > 0.0);
    assert_eq!(store.rates.get("orders.created"), Some(rate));
}

#[tokio::test(start_paused = true)]
async fn an_idle_cluster_still_zeros_the_latest_rate() {
    let session = FakeCluster::local();
    let store = store(&session);
    let _lanes = idle_lanes(&store, &session);

    until("watermark commit", || store.watermarks.version() > 0).await;

    store.watermarks.kick();
    until("second watermark poll", || {
        session.calls(Api::HighWatermarks) >= 1
    })
    .await;
    assert_eq!(
        store.watermarks.version(),
        1,
        "an unmoved log does not tick"
    );

    tokio::time::advance(Duration::from_secs(20)).await;
    store.watermarks.kick();
    until("heartbeat tick", || store.watermarks.version() > 1).await;

    assert_eq!(store.rates.get("orders.created"), Some(0.0));
}

#[tokio::test(start_paused = true)]
async fn the_config_lane_populates_the_config_table() {
    let session = FakeCluster::local();
    let store = store(&session);
    let _lanes = idle_lanes(&store, &session);

    until("config commit", || store.configs.version() > 0).await;

    let row = store.topic_row("orders.created").expect("orders exists");
    assert_eq!(row.retention_ms, Some(604_800_000));
    assert_eq!(
        store.topic_configs("orders.created").unwrap().len(),
        2,
        "configs are served from the table, not a live describe"
    );
}

#[tokio::test(start_paused = true)]
async fn a_topic_whose_configs_were_never_fetched_has_unknown_retention() {
    let session = FakeCluster::local();
    let store = store(&session);
    let _lanes = catalog_lanes(&store, &session);

    until("topology commit", || store.ready()).await;

    let row = store.topic_row("orders.created").expect("orders exists");
    assert_eq!(row.retention_ms, None);
    assert_eq!(store.topic_configs("orders.created"), None);
}

#[tokio::test(start_paused = true)]
async fn a_changed_config_names_only_the_topic_that_moved() {
    let session = FakeCluster::local().with_topic("payments", 1, 4);
    session.set_topic_configs("payments", vec![config_entry("cleanup.policy", "delete")]);
    let store = store(&session);
    let _lanes = idle_lanes(&store, &session);

    until("config commit", || store.configs.version() > 0).await;
    let unchanged = Arc::clone(&store.configs.load().unwrap().topics["payments"]);
    let mut events = store.bus.subscribe();

    session.set_topic_configs(
        "orders.created",
        vec![config_entry("cleanup.policy", "compact")],
    );
    store.configs.kick();

    let delta = settle("config delta", || match events.try_recv() {
        Ok(Change::Configs(delta)) => Some(delta),
        _ => None,
    })
    .await;

    assert_eq!(delta.topics, [Arc::from("orders.created")]);
    assert!(
        Arc::ptr_eq(
            &store.configs.load().unwrap().topics["payments"],
            &unchanged
        ),
        "an unchanged topic keeps sharing its entries"
    );
}

#[tokio::test(start_paused = true)]
async fn the_subject_lane_stores_the_list_projection_only() {
    let session = FakeCluster::local();
    let store = store(&session);
    let _lanes = idle_lanes(&store, &session);

    until("subject commit", || store.subjects.version() > 0).await;

    let rows = store.subject_rows();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].subject.as_ref(), "orders.created-value");
    assert_eq!(rows[0].info.versions, vec![1, 2]);
    assert!(
        !store.search("orders.created-value").is_empty(),
        "subjects are searchable"
    );
}

#[tokio::test(start_paused = true)]
async fn a_new_subject_is_published_as_a_delta() {
    let session = FakeCluster::local();
    let store = store(&session);
    let _lanes = idle_lanes(&store, &session);

    until("subject commit", || store.subjects.version() > 0).await;
    let mut events = store.bus.subscribe();

    session.set_subjects(vec![
        subject("orders.created-value", 1, 2),
        subject("payments-value", 2, 1),
    ]);
    store.subjects.kick();

    let delta = settle("subjects delta", || match events.try_recv() {
        Ok(Change::Subjects(delta)) => Some(delta),
        _ => None,
    })
    .await;

    assert_eq!(delta.added, [Arc::from("payments-value")]);
}

#[tokio::test(start_paused = true)]
async fn a_registry_outage_degrades_only_the_subject_lane() {
    let session = FakeCluster::local();
    session.fail(Api::SchemaSubjects, "registry down");
    let store = store(&session);
    let _lanes = idle_lanes(&store, &session);

    until("topology commit", || store.ready()).await;
    until("subject failure", || {
        store.subjects.health().last_error.is_some()
    })
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
    until("topology commit", || store.ready()).await;

    let first = sweep(&lane, &store).await;
    assert_eq!(first.refreshed, [Arc::from("order-processor")]);

    tokio::time::advance(Duration::from_secs(5)).await;
    assert!(
        sweep(&lane, &store).await.is_empty(),
        "a group nobody is watching waits out the slow tier"
    );

    tokio::time::advance(Duration::from_secs(20)).await;
    assert_eq!(
        sweep(&lane, &store).await.refreshed,
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
    until("topology commit", || store.ready()).await;
    sweep(&lane, &store).await;

    let lease = store.interest.lease_group("order-processor");
    tokio::time::advance(Duration::from_secs(3)).await;
    assert_eq!(
        sweep(&lane, &store).await.refreshed,
        [Arc::from("order-processor")],
        "a watched group refreshes on the fast tier"
    );

    drop(lease);
    tokio::time::advance(Duration::from_secs(3)).await;
    assert!(
        sweep(&lane, &store).await.is_empty(),
        "dropping the lease releases the fast tier immediately"
    );
}

#[tokio::test(start_paused = true)]
async fn a_wave_commits_once_and_publishes_once() {
    let session = FakeCluster::local().with_groups((0..12).map(|index| {
        group(
            &format!("group-{index:02}"),
            "orders.created",
            vec![0],
            &[(0, 2)],
        )
    }));
    let store = store(&session);
    let lane = OffsetLane::new(port(&session));
    let _lanes = catalog_lanes(&store, &session);
    until("topology commit", || store.ready()).await;
    let mut events = store.bus.subscribe();

    let wave = sweep(&lane, &store).await;

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

fn published_lags(events: &mut tokio::sync::broadcast::Receiver<Change>) -> Vec<(String, i64)> {
    let mut lags = Vec::new();
    loop {
        match events.try_recv() {
            Ok(Change::GroupOffsets(wave)) => lags.extend(
                wave.groups
                    .iter()
                    .map(|update| (update.group.to_string(), update.total_lag)),
            ),
            Ok(_) => {}
            Err(TryRecvError::Empty) => return lags,
            Err(other) => panic!("bus closed or lagged: {other:?}"),
        }
    }
}

#[tokio::test(start_paused = true)]
async fn a_refresh_that_changes_nothing_publishes_nothing() {
    let session = FakeCluster::local();
    let store = store(&session);
    let lane = OffsetLane::new(port(&session));
    let _lanes = catalog_lanes(&store, &session);
    until("watermark commit", || store.watermarks.version() > 0).await;
    let mut events = store.bus.subscribe();

    sweep(&lane, &store).await;
    assert_eq!(
        published_lags(&mut events),
        [("order-processor".to_owned(), 5)]
    );

    tokio::time::advance(Duration::from_secs(30)).await;
    let wave = sweep(&lane, &store).await;
    assert_eq!(wave.refreshed, [Arc::from("order-processor")]);
    assert_eq!(store.offsets.version(), 2, "the table still commits");
    assert!(
        published_lags(&mut events).is_empty(),
        "an unchanged group is not republished"
    );

    session.commit_offsets(
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
                offset: 5,
            },
        ],
    );
    tokio::time::advance(Duration::from_secs(30)).await;
    sweep(&lane, &store).await;
    assert_eq!(
        published_lags(&mut events),
        [("order-processor".to_owned(), 4)],
        "a moved commit publishes again"
    );
}

#[tokio::test(start_paused = true)]
async fn a_moved_high_watermark_republishes_the_lag() {
    let session = FakeCluster::local().with_growing_watermarks(10);
    let store = store(&session);
    let lane = OffsetLane::new(port(&session));
    let _lanes = catalog_lanes(&store, &session);
    until("watermark commit", || store.watermarks.version() > 0).await;
    let mut events = store.bus.subscribe();
    sweep(&lane, &store).await;
    let first = published_lags(&mut events);

    let version = store.watermarks.version();
    store.watermarks.kick();
    until("watermark poll", || store.watermarks.version() > version).await;
    tokio::time::advance(Duration::from_secs(30)).await;
    sweep(&lane, &store).await;

    let second = published_lags(&mut events);
    assert_eq!(
        second.len(),
        1,
        "the group's offsets did not move but its lag did"
    );
    assert!(second[0].1 > first[0].1);
}

#[tokio::test(start_paused = true)]
async fn a_returning_group_publishes_its_lag_again() {
    let returning = group("returning", "orders.created", vec![0], &[(0, 2)]);
    let session = FakeCluster::local().with_groups([returning.clone()]);
    let store = store(&session);
    let lane = OffsetLane::new(port(&session));
    let _lanes = catalog_lanes(&store, &session);
    until("watermark commit", || store.watermarks.version() > 0).await;
    let mut events = store.bus.subscribe();
    sweep(&lane, &store).await;
    let first = published_lags(&mut events);

    session.remove_group("returning");
    store.topology.kick();
    until("group removal", || {
        store
            .topology
            .load()
            .is_some_and(|topology| topology.group("returning").is_none())
    })
    .await;
    sweep(&lane, &store).await;

    session.put_group(returning);
    store.topology.kick();
    until("group returning", || {
        store
            .topology
            .load()
            .is_some_and(|topology| topology.group("returning").is_some())
    })
    .await;
    sweep(&lane, &store).await;

    let returned = first
        .iter()
        .find(|(group, _)| group == "returning")
        .cloned()
        .expect("first wave published the group");
    assert_eq!(
        published_lags(&mut events),
        [returned],
        "a group that left and came back is published as new"
    );
}

#[tokio::test(start_paused = true)]
async fn offset_fetches_respect_the_concurrency_cap() {
    let session = FakeCluster::local()
        .with_delay(Api::CommittedOffsets, Duration::from_millis(50))
        .with_groups((0..16).map(|index| {
            group(
                &format!("group-{index:02}"),
                "orders.created",
                vec![0],
                &[(0, 2)],
            )
        }));
    let store = store(&session);
    let lane = OffsetLane::new(port(&session)).with_concurrency(NonZeroUsize::new(4).unwrap());
    let _lanes = catalog_lanes(&store, &session);
    until("topology commit", || store.ready()).await;

    sweep(&lane, &store).await;

    assert_eq!(session.calls(Api::CommittedOffsets), 17);
    assert_eq!(
        session.peak(Api::CommittedOffsets),
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
    until("watermark commit", || store.watermarks.version() > 0).await;

    sweep(&lane, &store).await;

    assert_eq!(
        store.group_row("order-processor").unwrap().total_lag,
        Some(5)
    );
    assert_eq!(
        session.calls(Api::CommittedOffsets),
        1,
        "lag never costs an extra broker call"
    );
}

#[tokio::test(start_paused = true)]
async fn an_empty_group_reports_the_lag_it_left_behind() {
    let mut stopped = group(
        "stopped-consumer",
        "orders.created",
        Vec::new(),
        &[(0, 2), (1, 5)],
    );
    stopped.state = GroupState::Empty;
    stopped.members.clear();
    let session = FakeCluster::local().with_groups([stopped]);
    let store = store(&session);
    let lane = OffsetLane::new(port(&session));
    let _lanes = catalog_lanes(&store, &session);
    until("watermark commit", || store.watermarks.version() > 0).await;

    sweep(&lane, &store).await;

    let row = store.group_row("stopped-consumer").unwrap();
    assert_eq!(
        (row.total_lag, row.lag_complete),
        (Some(6 + 3), true),
        "a group with no members still has committed offsets to measure"
    );
    assert_eq!(row.topic_names, ["orders.created"]);
}

#[tokio::test(start_paused = true)]
async fn an_active_group_fetches_only_what_it_consumes() {
    let session = FakeCluster::local();
    session.commit_offsets(
        "order-processor",
        vec![
            CommittedOffset {
                topic: "orders.created".into(),
                partition: 0,
                offset: 6,
            },
            CommittedOffset {
                topic: "payments.settled".into(),
                partition: 0,
                offset: 3,
            },
        ],
    );
    let store = store(&session);
    let lane = OffsetLane::new(port(&session));
    let _lanes = catalog_lanes(&store, &session);
    until("watermark commit", || store.watermarks.version() > 0).await;

    sweep(&lane, &store).await;

    assert_eq!(
        store.group_row("order-processor").unwrap().topic_names,
        ["orders.created"],
        "a commit left behind on a topic the group no longer consumes is not fetched"
    );
}

#[tokio::test(start_paused = true)]
async fn a_failed_group_degrades_alone_and_keeps_its_last_offsets() {
    let session = FakeCluster::local();
    let store = store(&session);
    let lane = OffsetLane::new(port(&session));
    let _lanes = catalog_lanes(&store, &session);
    until("topology commit", || store.ready()).await;
    sweep(&lane, &store).await;
    let before = Arc::clone(
        store
            .offsets
            .load()
            .unwrap()
            .get("order-processor")
            .unwrap(),
    );

    session.fail(Api::CommittedOffsets, "coordinator not available");
    tokio::time::advance(Duration::from_secs(30)).await;
    let wave = sweep(&lane, &store).await;

    assert_eq!(wave.failed, [Arc::from("order-processor")]);
    assert!(wave.refreshed.is_empty());
    let offsets = store.offsets.load().unwrap();
    let group = offsets.get("order-processor").expect("stale value kept");
    assert_eq!(group.committed.len(), 2);
    assert!(
        Arc::ptr_eq(group, &before),
        "the failed group keeps its last snapshot instead of a rebuilt one"
    );
}

#[tokio::test(start_paused = true)]
async fn a_removed_group_is_dropped_from_the_offset_table() {
    let session = FakeCluster::local();
    let store = store(&session);
    let lane = OffsetLane::new(port(&session));
    let _lanes = catalog_lanes(&store, &session);
    until("topology commit", || store.ready()).await;
    sweep(&lane, &store).await;

    session.remove_group("order-processor");
    store.topology.kick();
    until("group removal", || {
        store.topology.load().is_some_and(|t| t.groups.is_empty())
    })
    .await;

    let wave = sweep(&lane, &store).await;

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
    until("group returning", || {
        store
            .topology
            .load()
            .is_some_and(|topology| topology.group("order-processor").is_some())
    })
    .await;

    assert_eq!(
        sweep(&lane, &store).await.refreshed,
        [Arc::from("order-processor")],
        "a group that comes back is due immediately, not on its old schedule"
    );
}

#[tokio::test(start_paused = true)]
async fn a_deleted_topic_loses_its_rate() {
    let session = FakeCluster::local().with_topic("payments", 1, 4);
    let store = store(&session);
    let _lanes = idle_lanes(&store, &session);

    until("watermark commit", || store.watermarks.version() > 0).await;
    assert_eq!(store.rates.get("payments"), Some(0.0));

    session.remove_topic("payments");
    store.topology.kick();
    until("topic removal", || {
        store
            .topology
            .load()
            .is_some_and(|topology| !topology.topics.contains_key("payments"))
    })
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
        WatermarkLane::with_interval(
            port(&session),
            Duration::from_secs(600),
            &IngestTuning::default(),
        ),
    ));
    lanes.spawn(run(
        Arc::clone(&store),
        ConfigLane::with_interval(port(&session), Duration::from_secs(600)),
    ));

    tokio::time::advance(Duration::from_secs(3_600)).await;

    assert!(
        store.watermarks.load().is_none(),
        "an empty table would read as a cluster with no partitions"
    );
    assert!(store.configs.load().is_none());
    assert!(store.watermarks.health().checked_at.is_none());
    assert_eq!(
        session.calls(Api::LowWatermarks) + session.calls(Api::HighWatermarks),
        0,
        "and it costs no broker call"
    );
}

#[tokio::test(start_paused = true)]
async fn lanes_waiting_for_topology_start_as_soon_as_it_commits() {
    let session = FakeCluster::local();
    let store = store(&session);
    let mut lanes = JoinSet::new();
    lanes.spawn(run(
        Arc::clone(&store),
        ConfigLane::with_interval(port(&session), IDLE),
    ));
    lanes.spawn(run(
        Arc::clone(&store),
        LogDirLane::with_interval(port(&session), IDLE),
    ));
    tokio::time::advance(IDLE).await;

    lanes.spawn(run(
        Arc::clone(&store),
        TopologyLane::with_interval(port(&session), IDLE),
    ));

    until("config and log dir commits", || {
        store.configs.ready() && store.log_dirs.ready()
    })
    .await;
}

#[tokio::test(start_paused = true)]
async fn the_log_dirs_lane_sums_sizes_per_topic_and_directory() {
    let session = FakeCluster::local();
    let store = store(&session);
    let mut events = store.bus.subscribe();
    let _lanes = log_dir_lanes(&store, &session, IDLE);

    until("log dirs commit", || store.log_dirs.ready()).await;

    let topology = store.topology.load().unwrap();
    let log_dirs = store.log_dirs.load().unwrap();
    assert_eq!(
        log_dirs
            .topic("orders.created")
            .map(|topic| topic.size_bytes),
        Some(4_096 + 2_048)
    );
    assert_eq!(log_dirs.broker(1).unwrap()[0].size_bytes, 4_096 + 2_048);
    let (topic, _) = topology.topics.get_key_value("orders.created").unwrap();
    let (sized, _) = log_dirs.partitions.get_key_value("orders.created").unwrap();
    assert!(Arc::ptr_eq(topic, sized), "topic names are shared");

    let delta = std::iter::from_fn(|| events.try_recv().ok())
        .find_map(|change| match change {
            Change::LogDirs(delta) => Some(delta),
            _ => None,
        })
        .expect("a log dirs change");
    assert_eq!(delta.topics, [Arc::from("orders.created")]);
    assert!(delta.brokers_changed);
}

#[tokio::test(start_paused = true)]
async fn a_failing_log_dirs_poll_names_its_lane_and_waits_out_the_interval() {
    let logs = LogCapture::at(tracing::Level::WARN);
    let session = FakeCluster::local();
    session.fail(Api::LogDirs, "DescribeLogDirs is not supported");
    let store = store(&session);
    let _lanes = log_dir_lanes(&store, &session, Duration::from_secs(60));

    until("log dirs error", || {
        store.log_dirs.health().last_error.is_some()
    })
    .await;

    assert!(store.log_dirs.load().is_none());
    assert_eq!(
        store.log_dirs.health().last_error.as_deref(),
        Some("kafka admin request failed: DescribeLogDirs is not supported")
    );
    logs.assert_contains(r#"lane="log_dirs""#);

    let calls = session.calls(Api::LogDirs);
    tokio::time::advance(Duration::from_secs(59)).await;
    tokio::task::yield_now().await;
    assert_eq!(session.calls(Api::LogDirs), calls);
    tokio::time::advance(Duration::from_secs(2)).await;
    until("second log dirs call", || {
        session.calls(Api::LogDirs) > calls
    })
    .await;
}

#[tokio::test(start_paused = true)]
async fn the_quota_lane_commits_and_publishes_only_a_changed_listing() {
    let session = FakeCluster::local();
    let store = store(&session);
    let mut events = store.bus.subscribe();
    let _lanes = quota_lane(&store, &session, IDLE);

    until("quotas commit", || store.quotas.ready()).await;
    assert!(matches!(
        store.quotas.load().as_deref(),
        Some(QuotaListing::Described(quotas)) if quotas.len() == 5
    ));
    assert!(matches!(events.try_recv(), Ok(Change::Quotas)));

    store.quotas.kick();
    until("second quotas call", || {
        session.calls(Api::ClientQuotas) == 2
    })
    .await;
    assert_eq!(store.quotas.version(), 1);
    assert!(matches!(events.try_recv(), Err(TryRecvError::Empty)));

    session.set_quotas(QuotaListing::Denied);
    store.quotas.kick();
    until("denied commit", || store.quotas.version() == 2).await;
    assert_eq!(store.quotas.load().as_deref(), Some(&QuotaListing::Denied));
    assert!(matches!(events.try_recv(), Ok(Change::Quotas)));
    assert!(store.quotas.health().healthy());
}

#[tokio::test(start_paused = true)]
async fn a_failing_quota_poll_names_its_lane_and_waits_out_the_interval() {
    let logs = LogCapture::at(tracing::Level::WARN);
    let session = FakeCluster::local();
    session.fail(Api::ClientQuotas, "DescribeClientQuotas is not supported");
    let store = store(&session);
    let _lanes = quota_lane(&store, &session, Duration::from_secs(60));

    until("quotas error", || {
        store.quotas.health().last_error.is_some()
    })
    .await;

    assert!(store.quotas.load().is_none());
    logs.assert_contains(r#"lane="quotas""#);

    tokio::time::advance(Duration::from_secs(59)).await;
    tokio::task::yield_now().await;
    assert_eq!(session.calls(Api::ClientQuotas), 1);
    tokio::time::advance(Duration::from_secs(2)).await;
    until("second quotas call", || {
        session.calls(Api::ClientQuotas) == 2
    })
    .await;
}

#[tokio::test(start_paused = true)]
async fn every_lane_runs_per_cluster_and_stops_with_the_ingest() {
    let prod = FakeCluster::named("prod");
    let staging = FakeCluster::named("staging");
    let clusters = Clusters::from_sessions(vec![prod.clone(), staging]);
    let lanes = Ingest::start(&clusters, &IngestTuning::default());

    assert_eq!(lanes.lane_count(), 16, "eight lanes per cluster");
    until("both clusters ready", || clusters.ready()).await;

    drop(lanes);
    tokio::task::yield_now().await;
    let polls = prod.calls(Api::Metadata);
    tokio::time::advance(Duration::from_secs(3_600)).await;
    tokio::task::yield_now().await;

    assert_eq!(
        prod.calls(Api::Metadata),
        polls,
        "dropping the ingest aborts every lane task"
    );
}

#[tokio::test]
async fn ingestion_fills_the_stores_the_api_projects_from() {
    let clusters = Clusters::from_sessions(vec![FakeCluster::local()]);
    let _lanes = Ingest::start(&clusters, &IngestTuning::default());
    let store = &clusters.get("local").unwrap().store;

    until("catalog and subjects", || {
        clusters.ready() && !store.subject_rows().is_empty()
    })
    .await;

    assert_eq!(store.topic_rows()[0].name.as_ref(), "orders.created");
    assert_eq!(
        store.subject_rows()[0].subject.as_ref(),
        "orders.created-value"
    );
}

fn acl_lane(store: &Arc<ClusterStore>, session: &FakeCluster, interval: Duration) -> JoinSet<()> {
    let mut lanes = JoinSet::new();
    lanes.spawn(run(
        Arc::clone(store),
        AclLane::with_interval(port(session), interval),
    ));
    lanes
}

#[tokio::test(start_paused = true)]
async fn the_acl_lane_stores_the_listing_and_publishes_each_change() {
    let session = FakeCluster::local();
    let store = store(&session);
    let mut events = store.bus.subscribe();
    let _lanes = acl_lane(&store, &session, IDLE);

    until("acls commit", || store.acls.ready()).await;
    assert!(matches!(
        store.acls.load().as_deref(),
        Some(AclListing::Enabled(rows)) if rows.len() == 3
    ));
    assert!(matches!(events.try_recv(), Ok(Change::Acls)));

    store.acls.kick();
    until("second describe", || session.calls(Api::Acls) == 2).await;
    tokio::task::yield_now().await;
    assert_eq!(
        store.acls.version(),
        1,
        "an unchanged listing is not a commit"
    );
    assert!(matches!(events.try_recv(), Err(TryRecvError::Empty)));

    session.set_acls(AclListing::Disabled);
    store.acls.kick();
    until("acls recommit", || store.acls.version() == 2).await;
    assert!(matches!(events.try_recv(), Ok(Change::Acls)));
}

#[tokio::test(start_paused = true)]
async fn a_failing_acl_poll_names_its_lane_and_waits_out_the_interval() {
    let logs = LogCapture::at(tracing::Level::WARN);
    let session = FakeCluster::local();
    session.fail(Api::Acls, "broker down");
    let store = store(&session);
    let _lanes = acl_lane(&store, &session, Duration::from_secs(60));

    until("acls error", || store.acls.health().last_error.is_some()).await;

    assert!(store.acls.load().is_none());
    logs.assert_contains(r#"lane="acls""#);

    tokio::time::advance(Duration::from_secs(59)).await;
    tokio::task::yield_now().await;
    assert_eq!(session.calls(Api::Acls), 1);
    tokio::time::advance(Duration::from_secs(2)).await;
    until("second acls call", || session.calls(Api::Acls) == 2).await;
}

#[tokio::test(start_paused = true)]
async fn one_cluster_never_wakes_another() {
    let prod = FakeCluster::named("prod");
    let staging = FakeCluster::named("staging");
    let clusters = Clusters::from_sessions(vec![prod.clone(), staging]);
    let _lanes = Ingest::start(&clusters, &IngestTuning::default());
    let prod_store = &clusters.get("prod").unwrap().store;
    let staging_store = &clusters.get("staging").unwrap().store;

    until("both ready", || prod_store.ready() && staging_store.ready()).await;
    let mut staging_events = staging_store.bus.subscribe();
    while staging_events.try_recv().is_ok() {}

    prod.add_partition("orders.created", 2);
    prod_store.topology.kick();
    until("prod delta", || prod_store.topology.version() > 1).await;

    assert!(
        matches!(staging_events.try_recv(), Err(TryRecvError::Empty)),
        "per-cluster buses keep the blast radius at one cluster"
    );
}

#[tokio::test(start_paused = true)]
async fn a_new_topic_gets_its_configs_without_waiting_out_the_config_interval() {
    let session = FakeCluster::local();
    let store = store(&session);
    let _lanes = idle_lanes(&store, &session);
    until("first config poll", || store.configs.ready()).await;

    session.add_topic("payments", 1, 0);
    session.set_topic_configs("payments", vec![config_entry("retention.ms", "1000")]);
    store.topology.kick();

    until("configs for the new topic", || {
        store.topic_configs("payments").is_some()
    })
    .await;
}

#[tokio::test(start_paused = true)]
async fn a_topology_change_without_new_topics_leaves_configs_to_their_interval() {
    let session = FakeCluster::local();
    let store = store(&session);
    let _lanes = idle_lanes(&store, &session);
    until("first config poll", || store.configs.ready()).await;
    let polls = session.calls(Api::TopicConfigs);

    session.add_partition("orders.created", 7);
    store.topology.kick();
    until("topology commit", || store.topology.version() > 1).await;
    quiesce().await;

    assert_eq!(session.calls(Api::TopicConfigs), polls);
}

#[tokio::test(start_paused = true)]
async fn a_new_topic_waits_for_the_next_watermark_poll() {
    let session = FakeCluster::local();
    let store = store(&session);
    let _lanes = catalog_lanes(&store, &session);
    until("first watermark poll", || store.watermarks.ready()).await;
    let polls = session.calls(Api::LowWatermarks);

    session.add_topic("payments", 1, 0);
    store.topology.kick();
    until("topology commit", || store.topology.version() > 1).await;
    quiesce().await;

    assert_eq!(session.calls(Api::LowWatermarks), polls);
}

#[tokio::test(start_paused = true)]
async fn between_low_reads_the_watermark_lane_lists_only_high_watermarks() {
    let session = FakeCluster::local();
    session.set_watermarks("orders.created", 1, Watermarks { low: 8, high: 8 });
    let store = store(&session);
    let _lanes = catalog_lanes(&store, &session);
    until("first watermark poll", || store.watermarks.ready()).await;
    assert_eq!(session.calls(Api::LowWatermarks), 1);
    assert_eq!(session.calls(Api::HighWatermarks), 1);

    session.set_watermarks("orders.created", 0, Watermarks { low: 5, high: 12 });
    store.watermarks.kick();
    until("end-only commit", || store.watermarks.version() > 1).await;

    assert_eq!(session.calls(Api::HighWatermarks), 2);
    assert_eq!(
        session.calls(Api::LowWatermarks),
        1,
        "an emptied log keeps its cached low watermark too"
    );
    let marks = store.watermarks.load().expect("watermarks");
    assert_eq!(
        marks.get("orders.created", 0),
        Some(Watermarks { low: 0, high: 12 })
    );
    assert_eq!(
        marks.get("orders.created", 1),
        Some(Watermarks { low: 8, high: 8 })
    );
}

#[tokio::test(start_paused = true)]
async fn the_watermark_lane_rereads_low_watermarks_once_their_interval_passes() {
    let session = FakeCluster::local();
    let store = store(&session);
    let _lanes = catalog_lanes(&store, &session);
    until("first watermark poll", || store.watermarks.ready()).await;
    let low_watermark = IngestTuning::default().low_watermark;

    tokio::time::advance(low_watermark - Duration::from_millis(1)).await;
    store.watermarks.kick();
    until("end-only poll", || session.calls(Api::HighWatermarks) == 2).await;
    assert_eq!(session.calls(Api::LowWatermarks), 1);

    session.set_watermarks("orders.created", 0, Watermarks { low: 5, high: 12 });
    tokio::time::advance(Duration::from_millis(1)).await;
    store.watermarks.kick();
    until("low watermark reread", || {
        store
            .watermarks
            .load()
            .and_then(|marks| marks.get("orders.created", 0))
            == Some(Watermarks { low: 5, high: 12 })
    })
    .await;
    assert_eq!(session.calls(Api::LowWatermarks), 2);
    assert_eq!(session.calls(Api::HighWatermarks), 3);
}

#[tokio::test(start_paused = true)]
async fn a_new_partition_reads_its_low_watermark_on_the_next_poll() {
    let session = FakeCluster::local();
    let store = store(&session);
    let _lanes = catalog_lanes(&store, &session);
    until("first watermark poll", || store.watermarks.ready()).await;

    session.set_watermarks("orders.created", 0, Watermarks { low: 5, high: 12 });
    session.add_partition("orders.created", 7);
    session.set_watermarks("orders.created", 7, Watermarks { low: 3, high: 9 });
    store.topology.kick();
    until("topology commit", || store.topology.version() > 1).await;
    store.watermarks.kick();
    until("watermark commit", || store.watermarks.version() > 1).await;

    let marks = store.watermarks.load().expect("watermarks");
    assert_eq!(
        marks.get("orders.created", 7),
        Some(Watermarks { low: 3, high: 9 })
    );
    assert_eq!(
        marks.get("orders.created", 0),
        Some(Watermarks { low: 0, high: 12 }),
        "only the new partition rereads its low watermark"
    );
    assert_eq!(session.calls(Api::HighWatermarks), 2);
    assert_eq!(session.calls(Api::LowWatermarks), 2);
}

#[tokio::test(start_paused = true)]
async fn a_high_watermark_below_the_cached_low_rereads_the_low() {
    let session = FakeCluster::local();
    session.set_watermarks("orders.created", 0, Watermarks { low: 6, high: 8 });
    let store = store(&session);
    let _lanes = catalog_lanes(&store, &session);
    until("first watermark poll", || store.watermarks.ready()).await;

    session.set_watermarks("orders.created", 0, Watermarks { low: 0, high: 2 });
    store.watermarks.kick();
    until("watermark commit", || store.watermarks.version() > 1).await;

    assert_eq!(
        store
            .watermarks
            .load()
            .and_then(|marks| marks.get("orders.created", 0)),
        Some(Watermarks { low: 0, high: 2 })
    );
    assert_eq!(session.calls(Api::LowWatermarks), 2);
}

#[tokio::test(start_paused = true)]
async fn the_offset_lane_sweeps_nothing_until_topology_commits() {
    let session = FakeCluster::local();
    let store = store(&session);
    let mut lanes = JoinSet::new();
    lanes.spawn(OffsetLane::new(port(&session)).run(Arc::clone(&store)));
    tokio::time::advance(IDLE).await;

    assert!(store.offsets.health().checked_at.is_none());

    lanes.spawn(run(
        Arc::clone(&store),
        TopologyLane::with_interval(port(&session), IDLE),
    ));
    until("first offset wave", || store.offsets.ready()).await;
}

#[tokio::test(start_paused = true)]
async fn a_new_topic_or_broker_gets_its_log_dirs_without_waiting_out_the_interval() {
    let session = FakeCluster::local();
    let store = store(&session);
    let _lanes = log_dir_lanes(&store, &session, IDLE);
    until("first log dirs poll", || store.log_dirs.ready()).await;

    let calls = session.calls(Api::LogDirs);
    session.add_topic("payments", 1, 0);
    store.topology.kick();
    until("log dirs for the new topic", || {
        session.calls(Api::LogDirs) > calls
    })
    .await;

    let calls = session.calls(Api::LogDirs);
    session.add_broker(2);
    store.topology.kick();
    until("log dirs for the new broker", || {
        session.calls(Api::LogDirs) > calls
    })
    .await;
}

#[tokio::test(start_paused = true)]
async fn a_topology_change_without_new_topics_or_brokers_leaves_log_dirs_to_their_interval() {
    let session = FakeCluster::local();
    let store = store(&session);
    let _lanes = log_dir_lanes(&store, &session, IDLE);
    until("first log dirs poll", || store.log_dirs.ready()).await;
    let calls = session.calls(Api::LogDirs);

    session.add_partition("orders.created", 7);
    store.topology.kick();
    until("topology commit", || store.topology.version() > 1).await;
    quiesce().await;

    assert_eq!(session.calls(Api::LogDirs), calls);
}
