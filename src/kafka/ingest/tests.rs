use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use tokio::time::advance;
use tracing::Level;

use super::*;
use crate::kafka::acl::AclListing;
use crate::kafka::group::GroupState;
use crate::kafka::metadata::Watermarks;
use crate::kafka::model::QuotaListing;
use crate::kafka::store::{Change, TopologyDelta};
use crate::testing::{
    Api, BusProbe, FakeCluster, IDLE, LogCapture, Rig, config_entry, group, quiesce, subject, until,
};

#[tokio::test]
async fn the_topology_lane_assembles_brokers_topics_and_groups() {
    let rig = Rig::local();

    rig.poll(&rig.topology()).await;

    let topology = rig.store.topology.load().expect("topology");
    assert_eq!(topology.cluster_id.as_deref(), Some("test-cluster"));
    assert_eq!(topology.brokers.len(), 1);
    assert_eq!(topology.topics["orders.created"].partitions.len(), 2);
    assert_eq!(
        topology.groups_for_topic("orders.created"),
        [Arc::from("order-processor")],
        "the reverse index is built at commit time"
    );
    assert_eq!(rig.store.topic_rows().len(), 1);
    assert_eq!(rig.store.broker_rows()[0].host, "localhost");
}

#[tokio::test]
async fn a_topology_poll_that_changes_nothing_commits_and_publishes_nothing() {
    let rig = Rig::local();
    let lane = rig.topology();
    let mut bus = rig.store.bus.probe();

    rig.poll(&lane).await;
    rig.poll(&lane).await;

    assert_eq!(rig.cluster.calls(Api::Metadata), 2);
    assert_eq!(
        rig.store.topology.version(),
        1,
        "an unchanged cluster costs a hash-equal check and nothing else"
    );
    bus.next(Change::topology);
    bus.assert_quiet();
}

#[tokio::test]
async fn a_new_partition_is_published_as_a_changed_topic() {
    let rig = Rig::local();
    let lane = rig.topology();
    rig.poll(&lane).await;
    let mut bus = rig.store.bus.probe();

    rig.cluster.add_partition("orders.created", 2);
    rig.poll(&lane).await;

    assert_eq!(
        *bus.next(Change::topology),
        TopologyDelta {
            changed_topics: vec![Arc::from("orders.created")],
            ..TopologyDelta::default()
        }
    );
}

#[tokio::test]
async fn the_topology_lane_keeps_search_current() {
    let rig = Rig::local();

    rig.poll(&rig.topology()).await;
    rig.poll(&rig.subjects()).await;

    let hits = rig.store.search("order");
    let ids: Vec<&str> = hits.iter().map(|hit| hit.id.as_str()).collect();
    assert_eq!(
        ids,
        ["orders.created", "order-processor", "orders.created-value"],
        "topics, groups and subjects are all searchable"
    );
}

#[tokio::test(start_paused = true)]
async fn a_deleted_topic_loses_its_rate() {
    let rig = Rig::new(FakeCluster::local().with_topic("payments", 1, 4));
    let lane = rig.topology();
    rig.poll(&lane).await;
    rig.poll(&rig.watermarks()).await;
    assert_eq!(rig.store.rates.get("payments"), Some(0.0));

    rig.cluster.remove_topic("payments");
    rig.poll(&lane).await;

    assert_eq!(rig.store.rates.get("payments"), None);
    assert_eq!(rig.store.rates.get("orders.created"), Some(0.0));
}

#[tokio::test(start_paused = true)]
async fn a_watermark_tick_publishes_the_rate_it_stores() {
    let rig = Rig::new(FakeCluster::local().with_growing_watermarks(20));
    let lane = rig.watermarks();
    rig.poll(&rig.topology()).await;
    rig.poll(&lane).await;
    assert_eq!(rig.store.rates.get("orders.created"), Some(0.0));
    let mut bus = rig.store.bus.probe();

    advance(Duration::from_secs(2)).await;
    rig.poll(&lane).await;

    let tick = bus.next(Change::watermarks);
    assert_eq!(tick.rate("orders.created"), Some(10.0));
    assert_eq!(rig.store.rates.get("orders.created"), Some(10.0));
}

#[tokio::test(start_paused = true)]
async fn a_quiet_topic_falls_back_to_a_zero_rate_on_the_heartbeat() {
    let rig = Rig::local();
    let lane = rig.watermarks();
    rig.poll(&rig.topology()).await;
    rig.poll(&lane).await;

    advance(Duration::from_secs(1)).await;
    rig.cluster
        .set_watermarks("orders.created", 0, Watermarks { low: 0, high: 18 });
    rig.poll(&lane).await;
    assert_eq!(rig.store.rates.get("orders.created"), Some(10.0));

    rig.poll(&lane).await;
    assert_eq!(
        rig.store.watermarks.version(),
        2,
        "an unmoved log does not tick"
    );

    advance(IngestTuning::default().idle_heartbeat).await;
    rig.poll(&lane).await;
    assert_eq!(rig.store.watermarks.version(), 3);
    assert_eq!(rig.store.rates.get("orders.created"), Some(0.0));
}

#[tokio::test(start_paused = true)]
async fn between_low_reads_the_watermark_lane_lists_only_high_watermarks() {
    let rig = Rig::local();
    rig.cluster
        .set_watermarks("orders.created", 1, Watermarks { low: 8, high: 8 });
    let lane = rig.watermarks();
    rig.poll(&rig.topology()).await;
    rig.poll(&lane).await;
    assert_eq!(rig.cluster.calls(Api::LowWatermarks), 1);
    assert_eq!(rig.cluster.calls(Api::HighWatermarks), 1);

    rig.cluster
        .set_watermarks("orders.created", 0, Watermarks { low: 5, high: 12 });
    rig.poll(&lane).await;

    assert_eq!(rig.cluster.calls(Api::HighWatermarks), 2);
    assert_eq!(
        rig.cluster.calls(Api::LowWatermarks),
        1,
        "an emptied log keeps its cached low watermark too"
    );
    let marks = rig.store.watermarks.load().expect("watermarks");
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
    let rig = Rig::local();
    let lane = rig.watermarks();
    rig.poll(&rig.topology()).await;
    rig.poll(&lane).await;
    let low_watermark = IngestTuning::default().low_watermark;

    advance(low_watermark - Duration::from_millis(1)).await;
    rig.poll(&lane).await;
    assert_eq!(rig.cluster.calls(Api::HighWatermarks), 2);
    assert_eq!(rig.cluster.calls(Api::LowWatermarks), 1);

    rig.cluster
        .set_watermarks("orders.created", 0, Watermarks { low: 5, high: 12 });
    advance(Duration::from_millis(1)).await;
    rig.poll(&lane).await;

    assert_eq!(
        rig.store
            .watermarks
            .load()
            .and_then(|marks| marks.get("orders.created", 0)),
        Some(Watermarks { low: 5, high: 12 })
    );
    assert_eq!(rig.cluster.calls(Api::LowWatermarks), 2);
    assert_eq!(rig.cluster.calls(Api::HighWatermarks), 3);
}

#[tokio::test(start_paused = true)]
async fn a_new_partition_reads_its_low_watermark_on_the_next_poll() {
    let rig = Rig::local();
    let topology = rig.topology();
    let lane = rig.watermarks();
    rig.poll(&topology).await;
    rig.poll(&lane).await;

    rig.cluster
        .set_watermarks("orders.created", 0, Watermarks { low: 5, high: 12 });
    rig.cluster.add_partition("orders.created", 7);
    rig.cluster
        .set_watermarks("orders.created", 7, Watermarks { low: 3, high: 9 });
    rig.poll(&topology).await;
    rig.poll(&lane).await;

    let marks = rig.store.watermarks.load().expect("watermarks");
    assert_eq!(
        marks.get("orders.created", 7),
        Some(Watermarks { low: 3, high: 9 })
    );
    assert_eq!(
        marks.get("orders.created", 0),
        Some(Watermarks { low: 0, high: 12 }),
        "only the new partition rereads its low watermark"
    );
    assert_eq!(rig.cluster.calls(Api::HighWatermarks), 2);
    assert_eq!(rig.cluster.calls(Api::LowWatermarks), 2);
}

#[tokio::test(start_paused = true)]
async fn a_high_watermark_below_the_cached_low_rereads_the_low() {
    let rig = Rig::local();
    rig.cluster
        .set_watermarks("orders.created", 0, Watermarks { low: 6, high: 8 });
    let lane = rig.watermarks();
    rig.poll(&rig.topology()).await;
    rig.poll(&lane).await;

    rig.cluster
        .set_watermarks("orders.created", 0, Watermarks { low: 0, high: 2 });
    rig.poll(&lane).await;

    assert_eq!(
        rig.store
            .watermarks
            .load()
            .and_then(|marks| marks.get("orders.created", 0)),
        Some(Watermarks { low: 0, high: 2 })
    );
    assert_eq!(rig.cluster.calls(Api::LowWatermarks), 2);
}

#[tokio::test(start_paused = true)]
async fn a_new_topic_waits_for_the_next_watermark_poll() {
    let mut rig = Rig::local();
    let topology = rig.topology();
    rig.poll(&topology).await;
    rig.spawn(rig.watermarks());
    rig.store.watermarks.committed().await;

    rig.cluster.add_topic("payments", 1, 0);
    rig.poll(&topology).await;
    quiesce().await;

    assert_eq!(rig.cluster.calls(Api::LowWatermarks), 1);
    assert_eq!(rig.cluster.calls(Api::HighWatermarks), 1);
}

#[tokio::test]
async fn the_config_lane_populates_the_config_table() {
    let rig = Rig::local();

    rig.poll(&rig.topology()).await;
    rig.poll(&rig.configs()).await;

    let row = rig
        .store
        .topic_row("orders.created")
        .expect("orders exists");
    assert_eq!(row.retention_ms, Some(604_800_000));
    assert_eq!(
        rig.store.topic_configs("orders.created").unwrap().len(),
        2,
        "configs are served from the table, not a live describe"
    );
}

#[tokio::test]
async fn a_topic_whose_configs_were_never_fetched_has_unknown_retention() {
    let rig = Rig::local();

    rig.poll(&rig.topology()).await;

    let row = rig
        .store
        .topic_row("orders.created")
        .expect("orders exists");
    assert_eq!(row.retention_ms, None);
    assert_eq!(rig.store.topic_configs("orders.created"), None);
}

#[tokio::test]
async fn a_changed_config_names_only_the_topic_that_moved() {
    let rig = Rig::new(FakeCluster::local().with_topic("payments", 1, 4));
    rig.cluster
        .set_topic_configs("payments", vec![config_entry("cleanup.policy", "delete")]);
    let lane = rig.configs();
    rig.poll(&rig.topology()).await;
    rig.poll(&lane).await;
    let unchanged = Arc::clone(&rig.store.configs.load().unwrap().topics["payments"]);
    let mut bus = rig.store.bus.probe();

    rig.cluster.set_topic_configs(
        "orders.created",
        vec![config_entry("cleanup.policy", "compact")],
    );
    rig.poll(&lane).await;

    assert_eq!(
        bus.next(Change::configs).topics,
        [Arc::from("orders.created")]
    );
    assert!(
        Arc::ptr_eq(
            &rig.store.configs.load().unwrap().topics["payments"],
            &unchanged
        ),
        "an unchanged topic keeps sharing its entries"
    );
}

#[tokio::test(start_paused = true)]
async fn a_new_topic_gets_its_configs_without_waiting_out_the_config_interval() {
    let mut rig = Rig::local();
    let topology = rig.topology();
    rig.poll(&topology).await;
    rig.spawn(rig.configs());
    rig.store.configs.committed().await;

    rig.cluster.add_topic("payments", 1, 0);
    rig.cluster
        .set_topic_configs("payments", vec![config_entry("retention.ms", "1000")]);
    rig.poll(&topology).await;

    until("configs for the new topic", || {
        rig.store.topic_configs("payments").is_some()
    })
    .await;
}

#[tokio::test(start_paused = true)]
async fn a_topology_change_without_new_topics_leaves_configs_to_their_interval() {
    let mut rig = Rig::local();
    let topology = rig.topology();
    rig.poll(&topology).await;
    rig.spawn(rig.configs());
    rig.store.configs.committed().await;

    rig.cluster.add_partition("orders.created", 7);
    rig.poll(&topology).await;
    quiesce().await;

    assert_eq!(rig.cluster.calls(Api::TopicConfigs), 1);
}

#[tokio::test]
async fn the_subject_lane_stores_the_list_projection_only() {
    let rig = Rig::local();

    rig.poll(&rig.subjects()).await;

    let rows = rig.store.subject_rows();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].subject.as_ref(), "orders.created-value");
    assert_eq!(rows[0].info.versions, vec![1, 2]);
    assert!(
        !rig.store.search("orders.created-value").is_empty(),
        "subjects are searchable"
    );
}

#[tokio::test]
async fn a_new_subject_is_published_as_a_delta() {
    let rig = Rig::local();
    let lane = rig.subjects();
    rig.poll(&lane).await;
    let mut bus = rig.store.bus.probe();

    rig.cluster.set_subjects(vec![
        subject("orders.created-value", 1, 2),
        subject("payments-value", 2, 1),
    ]);
    rig.poll(&lane).await;

    assert_eq!(
        bus.next(Change::subjects).added,
        [Arc::from("payments-value")]
    );
}

#[tokio::test]
async fn a_registry_outage_degrades_only_the_subject_lane() {
    let rig = Rig::local();
    rig.cluster.fail(Api::SchemaSubjects, "registry down");

    rig.poll(&rig.topology()).await;
    rig.poll(&rig.subjects()).await;

    let health = rig.store.health();
    assert!(health.topology.healthy());
    assert!(!health.subjects.healthy());
    assert!(
        rig.store.subjects.load().is_none(),
        "an unavailable source is not an empty listing"
    );
    assert_eq!(rig.store.topic_rows().len(), 1, "the catalog still serves");
}

fn lags(bus: &mut BusProbe) -> Vec<(String, i64)> {
    bus.drain()
        .into_iter()
        .filter_map(Change::group_offsets)
        .flat_map(|wave| wave.groups.clone())
        .map(|update| (update.group.to_string(), update.total_lag))
        .collect()
}

fn consumers(count: usize) -> FakeCluster {
    FakeCluster::local().with_groups((0..count).map(|index| {
        group(&format!("group-{index:02}"), "orders.created", vec![0]).with_committed(&[(
            "orders.created",
            0,
            2,
        )])
    }))
}

#[tokio::test(start_paused = true)]
async fn a_background_group_refreshes_on_the_slow_tier() {
    let rig = Rig::local();
    let lane = rig.offsets().with_tiers(
        Duration::from_secs(1),
        Duration::from_secs(2),
        Duration::from_secs(20),
    );
    rig.poll(&rig.topology()).await;

    assert_eq!(
        rig.sweep(&lane).await.refreshed,
        [Arc::from("order-processor")]
    );

    advance(Duration::from_secs(5)).await;
    assert!(
        rig.sweep(&lane).await.is_empty(),
        "a group nobody is watching waits out the slow tier"
    );

    advance(Duration::from_secs(20)).await;
    assert_eq!(
        rig.sweep(&lane).await.refreshed,
        [Arc::from("order-processor")]
    );
}

#[tokio::test(start_paused = true)]
async fn interest_promotes_a_group_to_the_fast_tier() {
    let rig = Rig::local();
    let lane = rig.offsets().with_tiers(
        Duration::from_secs(1),
        Duration::from_secs(2),
        Duration::from_secs(20),
    );
    rig.poll(&rig.topology()).await;
    rig.sweep(&lane).await;

    let lease = rig.store.interest.lease_group("order-processor");
    advance(Duration::from_secs(3)).await;
    assert_eq!(
        rig.sweep(&lane).await.refreshed,
        [Arc::from("order-processor")],
        "a watched group refreshes on the fast tier"
    );

    drop(lease);
    advance(Duration::from_secs(3)).await;
    assert!(
        rig.sweep(&lane).await.is_empty(),
        "dropping the lease releases the fast tier immediately"
    );
}

#[tokio::test(start_paused = true)]
async fn a_wave_commits_once_and_publishes_once() {
    let rig = Rig::new(consumers(12));
    let lane = rig.offsets();
    rig.poll(&rig.topology()).await;
    let mut bus = rig.store.bus.probe();

    let wave = rig.sweep(&lane).await;

    assert_eq!(wave.refreshed.len(), 13);
    assert_eq!(
        rig.store.offsets.version(),
        1,
        "a wave touching thirteen groups is one map rebuild, not thirteen"
    );
    assert_eq!(bus.next(Change::group_offsets).groups.len(), 13);
    bus.assert_quiet();
}

#[tokio::test(start_paused = true)]
async fn a_refresh_that_changes_nothing_publishes_nothing() {
    let rig = Rig::local();
    let lane = rig.offsets();
    rig.poll_catalog().await;
    let mut bus = rig.store.bus.probe();

    rig.sweep(&lane).await;
    assert_eq!(lags(&mut bus), [("order-processor".to_owned(), 5)]);

    advance(Duration::from_secs(30)).await;
    let wave = rig.sweep(&lane).await;
    assert_eq!(wave.refreshed, [Arc::from("order-processor")]);
    assert_eq!(rig.store.offsets.version(), 2, "the table still commits");
    assert!(
        lags(&mut bus).is_empty(),
        "an unchanged group is not republished"
    );

    rig.cluster.commit_offsets(
        "order-processor",
        &[("orders.created", 0, 7), ("orders.created", 1, 5)],
    );
    advance(Duration::from_secs(30)).await;
    rig.sweep(&lane).await;
    assert_eq!(
        lags(&mut bus),
        [("order-processor".to_owned(), 4)],
        "a moved commit publishes again"
    );
}

#[tokio::test(start_paused = true)]
async fn a_moved_high_watermark_republishes_the_lag() {
    let rig = Rig::new(FakeCluster::local().with_growing_watermarks(10));
    let watermarks = rig.watermarks();
    let lane = rig.offsets();
    rig.poll(&rig.topology()).await;
    rig.poll(&watermarks).await;
    let mut bus = rig.store.bus.probe();
    rig.sweep(&lane).await;
    assert_eq!(lags(&mut bus), [("order-processor".to_owned(), 5)]);

    rig.poll(&watermarks).await;
    advance(Duration::from_secs(30)).await;
    rig.sweep(&lane).await;

    assert_eq!(
        lags(&mut bus),
        [("order-processor".to_owned(), 15)],
        "the group's offsets did not move but its lag did"
    );
}

#[tokio::test(start_paused = true)]
async fn a_returning_group_publishes_its_lag_again() {
    let returning =
        group("returning", "orders.created", vec![0]).with_committed(&[("orders.created", 0, 2)]);
    let rig = Rig::new(FakeCluster::local().with_groups([returning.clone()]));
    let topology = rig.topology();
    let lane = rig.offsets();
    rig.poll(&topology).await;
    rig.poll(&rig.watermarks()).await;
    let mut bus = rig.store.bus.probe();
    rig.sweep(&lane).await;
    assert!(lags(&mut bus).contains(&("returning".to_owned(), 6)));

    rig.cluster.remove_group("returning");
    rig.poll(&topology).await;
    rig.sweep(&lane).await;

    rig.cluster.put_group(returning);
    rig.poll(&topology).await;
    rig.sweep(&lane).await;

    assert_eq!(
        lags(&mut bus),
        [("returning".to_owned(), 6)],
        "a group that left and came back is published as new"
    );
}

#[tokio::test(start_paused = true)]
async fn offset_fetches_respect_the_concurrency_cap() {
    let rig = Rig::new(consumers(16).with_delay(Api::CommittedOffsets, Duration::from_millis(50)));
    let lane = rig
        .offsets()
        .with_concurrency(NonZeroUsize::new(4).unwrap());
    rig.poll(&rig.topology()).await;

    rig.sweep(&lane).await;

    assert_eq!(rig.cluster.calls(Api::CommittedOffsets), 17);
    assert_eq!(
        rig.cluster.peak(Api::CommittedOffsets),
        4,
        "the wave is wide but bounded"
    );
}

#[tokio::test(start_paused = true)]
async fn lag_is_computed_from_the_tables() {
    let rig = Rig::local();
    let lane = rig.offsets();
    rig.poll_catalog().await;

    rig.sweep(&lane).await;

    assert_eq!(
        rig.store.group_row("order-processor").unwrap().total_lag,
        Some(5)
    );
    assert_eq!(
        rig.cluster.calls(Api::CommittedOffsets),
        1,
        "lag never costs an extra broker call"
    );
}

#[tokio::test(start_paused = true)]
async fn an_empty_group_reports_the_lag_it_left_behind() {
    let mut stopped = group("stopped-consumer", "orders.created", Vec::new())
        .with_committed(&[("orders.created", 0, 2), ("orders.created", 1, 5)]);
    stopped.state = GroupState::Empty;
    stopped.members.clear();
    let rig = Rig::new(FakeCluster::local().with_groups([stopped]));
    let lane = rig.offsets();
    rig.poll_catalog().await;

    rig.sweep(&lane).await;

    let row = rig.store.group_row("stopped-consumer").unwrap();
    assert_eq!(
        (row.total_lag, row.lag_complete),
        (Some(6 + 3), true),
        "a group with no members still has committed offsets to measure"
    );
    assert_eq!(row.topic_names, ["orders.created"]);
}

#[tokio::test(start_paused = true)]
async fn an_active_group_fetches_only_what_it_consumes() {
    let rig = Rig::local();
    rig.cluster.commit_offsets(
        "order-processor",
        &[("orders.created", 0, 6), ("payments.settled", 0, 3)],
    );
    let lane = rig.offsets();
    rig.poll_catalog().await;

    rig.sweep(&lane).await;

    assert_eq!(
        rig.store.group_row("order-processor").unwrap().topic_names,
        ["orders.created"],
        "a commit left behind on a topic the group no longer consumes is not fetched"
    );
}

#[tokio::test(start_paused = true)]
async fn a_failed_group_degrades_alone_and_keeps_its_last_offsets() {
    let rig = Rig::local();
    let lane = rig.offsets();
    rig.poll(&rig.topology()).await;
    rig.sweep(&lane).await;
    let before = Arc::clone(
        rig.store
            .offsets
            .load()
            .unwrap()
            .get("order-processor")
            .unwrap(),
    );

    rig.cluster
        .fail(Api::CommittedOffsets, "coordinator not available");
    advance(Duration::from_secs(30)).await;
    let wave = rig.sweep(&lane).await;

    assert_eq!(wave.failed, [Arc::from("order-processor")]);
    assert!(wave.refreshed.is_empty());
    let offsets = rig.store.offsets.load().unwrap();
    let group = offsets.get("order-processor").expect("stale value kept");
    assert_eq!(group.committed.len(), 2);
    assert!(
        Arc::ptr_eq(group, &before),
        "the failed group keeps its last snapshot instead of a rebuilt one"
    );
}

#[tokio::test(start_paused = true)]
async fn a_removed_group_is_dropped_from_the_offset_table() {
    let rig = Rig::local();
    let topology = rig.topology();
    let lane = rig.offsets();
    rig.poll(&topology).await;
    rig.sweep(&lane).await;

    rig.cluster.remove_group("order-processor");
    rig.poll(&topology).await;
    let wave = rig.sweep(&lane).await;

    assert_eq!(wave.dropped, [Arc::from("order-processor")]);
    assert!(rig.store.offsets.load().unwrap().groups.is_empty());
    assert!(rig.store.group_rows().is_empty());

    rig.cluster.put_group(
        group("order-processor", "orders.created", vec![0]).with_committed(&[(
            "orders.created",
            0,
            1,
        )]),
    );
    rig.poll(&topology).await;

    assert_eq!(
        rig.sweep(&lane).await.refreshed,
        [Arc::from("order-processor")],
        "a group that comes back is due immediately, not on its old schedule"
    );
}

#[tokio::test(start_paused = true)]
async fn the_offset_lane_sweeps_nothing_until_topology_commits() {
    let mut rig = Rig::local();
    rig.spawn_offsets(rig.offsets());
    advance(IDLE).await;

    assert!(rig.store.offsets.health().checked_at.is_none());

    rig.poll(&rig.topology()).await;
    rig.store.offsets.committed().await;
}

#[tokio::test]
async fn the_log_dirs_lane_sums_sizes_per_topic_and_directory() {
    let rig = Rig::local();
    rig.poll(&rig.topology()).await;
    let mut bus = rig.store.bus.probe();

    rig.poll(&rig.log_dirs()).await;

    let topology = rig.store.topology.load().unwrap();
    let log_dirs = rig.store.log_dirs.load().unwrap();
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

    let delta = bus.next(Change::log_dirs);
    assert_eq!(delta.topics, [Arc::from("orders.created")]);
    assert!(delta.brokers_changed);
}

#[tokio::test]
async fn a_failed_log_dirs_poll_names_its_lane_and_commits_nothing() {
    let logs = LogCapture::at(Level::WARN);
    let rig = Rig::local();
    rig.cluster
        .fail(Api::LogDirs, "DescribeLogDirs is not supported");
    rig.poll(&rig.topology()).await;

    rig.poll(&rig.log_dirs()).await;

    assert!(rig.store.log_dirs.load().is_none());
    assert_eq!(
        rig.store.log_dirs.health().last_error.as_deref(),
        Some("kafka admin request failed: DescribeLogDirs is not supported")
    );
    logs.assert_contains(r#"lane="log_dirs""#);
}

#[tokio::test(start_paused = true)]
async fn a_new_topic_or_broker_gets_its_log_dirs_without_waiting_out_the_interval() {
    let mut rig = Rig::local();
    let topology = rig.topology();
    rig.poll(&topology).await;
    rig.spawn(rig.log_dirs());
    rig.store.log_dirs.committed().await;

    rig.cluster.add_topic("payments", 1, 0);
    rig.poll(&topology).await;
    until("log dirs for the new topic", || {
        rig.cluster.calls(Api::LogDirs) == 2
    })
    .await;

    rig.cluster.add_broker(2);
    rig.poll(&topology).await;
    until("log dirs for the new broker", || {
        rig.cluster.calls(Api::LogDirs) == 3
    })
    .await;
}

#[tokio::test(start_paused = true)]
async fn a_topology_change_without_new_topics_or_brokers_leaves_log_dirs_to_their_interval() {
    let mut rig = Rig::local();
    let topology = rig.topology();
    rig.poll(&topology).await;
    rig.spawn(rig.log_dirs());
    rig.store.log_dirs.committed().await;

    rig.cluster.add_partition("orders.created", 7);
    rig.poll(&topology).await;
    quiesce().await;

    assert_eq!(rig.cluster.calls(Api::LogDirs), 1);
}

#[tokio::test]
async fn the_acl_lane_stores_the_listing_and_publishes_each_change() {
    let rig = Rig::local();
    let lane = rig.acls();
    let mut bus = rig.store.bus.probe();

    rig.poll(&lane).await;
    assert!(matches!(
        rig.store.acls.load().as_deref(),
        Some(AclListing::Enabled(rows)) if rows.len() == 3
    ));
    bus.next(Change::acls);

    rig.poll(&lane).await;
    assert_eq!(
        rig.store.acls.version(),
        1,
        "an unchanged listing is not a commit"
    );
    bus.assert_quiet();

    rig.cluster.set_acls(AclListing::Disabled);
    rig.poll(&lane).await;
    assert_eq!(rig.store.acls.version(), 2);
    bus.next(Change::acls);
}

#[tokio::test]
async fn a_failed_acl_poll_names_its_lane_and_commits_nothing() {
    let logs = LogCapture::at(Level::WARN);
    let rig = Rig::local();
    rig.cluster.fail(Api::Acls, "broker down");

    rig.poll(&rig.acls()).await;

    assert!(rig.store.acls.load().is_none());
    assert_eq!(
        rig.store.acls.health().last_error.as_deref(),
        Some("kafka admin request failed: broker down")
    );
    logs.assert_contains(r#"lane="acls""#);
}

#[tokio::test]
async fn the_quota_lane_commits_and_publishes_only_a_changed_listing() {
    let rig = Rig::local();
    let lane = rig.quotas();
    let mut bus = rig.store.bus.probe();

    rig.poll(&lane).await;
    assert!(matches!(
        rig.store.quotas.load().as_deref(),
        Some(QuotaListing::Described(quotas)) if quotas.len() == 5
    ));
    bus.next(Change::quotas);

    rig.poll(&lane).await;
    assert_eq!(rig.store.quotas.version(), 1);
    bus.assert_quiet();

    rig.cluster.set_quotas(QuotaListing::Denied);
    rig.poll(&lane).await;
    assert_eq!(
        rig.store.quotas.load().as_deref(),
        Some(&QuotaListing::Denied)
    );
    bus.next(Change::quotas);
    assert!(rig.store.quotas.health().healthy());
}

#[tokio::test]
async fn a_failed_quota_poll_names_its_lane_and_commits_nothing() {
    let logs = LogCapture::at(Level::WARN);
    let rig = Rig::local();
    rig.cluster
        .fail(Api::ClientQuotas, "DescribeClientQuotas is not supported");

    rig.poll(&rig.quotas()).await;

    assert!(rig.store.quotas.load().is_none());
    assert_eq!(
        rig.store.quotas.health().last_error.as_deref(),
        Some("kafka admin request failed: DescribeClientQuotas is not supported")
    );
    logs.assert_contains(r#"lane="quotas""#);
}

#[tokio::test(start_paused = true)]
async fn downstream_lanes_wait_for_topology_instead_of_committing_nothing() {
    let mut rig = Rig::local();
    rig.spawn(rig.watermarks());
    rig.spawn(rig.configs());

    advance(Duration::from_secs(3_600)).await;

    assert!(
        rig.store.watermarks.load().is_none(),
        "an empty table would read as a cluster with no partitions"
    );
    assert!(rig.store.configs.load().is_none());
    assert!(rig.store.watermarks.health().checked_at.is_none());
    assert_eq!(
        rig.cluster.calls(Api::LowWatermarks) + rig.cluster.calls(Api::HighWatermarks),
        0,
        "and it costs no broker call"
    );
}

#[tokio::test(start_paused = true)]
async fn lanes_waiting_for_topology_start_as_soon_as_it_commits() {
    let mut rig = Rig::local();
    rig.spawn(rig.configs());
    rig.spawn(rig.log_dirs());
    advance(IDLE).await;

    rig.poll(&rig.topology()).await;

    rig.store.configs.committed().await;
    rig.store.log_dirs.committed().await;
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
    advance(Duration::from_secs(3_600)).await;
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

#[tokio::test(start_paused = true)]
async fn one_cluster_never_wakes_another() {
    let prod = FakeCluster::named("prod");
    let staging = FakeCluster::named("staging");
    let clusters = Clusters::from_sessions(vec![prod.clone(), staging]);
    let _lanes = Ingest::start(&clusters, &IngestTuning::default());
    let prod_store = &clusters.get("prod").unwrap().store;
    let staging_store = &clusters.get("staging").unwrap().store;

    until("both ready", || prod_store.ready() && staging_store.ready()).await;
    let mut staging_bus = staging_store.bus.probe();
    staging_bus.drain();

    prod.add_partition("orders.created", 2);
    prod_store.topology.kick();
    prod_store.topology.reaches(2).await;

    staging_bus.assert_quiet();
}
