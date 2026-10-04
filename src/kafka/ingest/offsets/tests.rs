use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use tokio::time::{Instant, advance};

use crate::kafka::group::GroupState;
use crate::kafka::store::{Change, OffsetTable};
use crate::testing::{Api, BusProbe, FakeCluster, IDLE, Rig, group, quiesce};

fn lags(bus: &mut BusProbe) -> Vec<(String, i64)> {
    bus.drain()
        .into_iter()
        .filter_map(Change::group_offsets)
        .flat_map(|wave| wave.groups.clone())
        .map(|update| (update.group.to_string(), update.total_lag))
        .collect()
}

fn committed(table: &OffsetTable, group: &str) -> Vec<i64> {
    table
        .get(group)
        .map(|offsets| {
            offsets
                .committed
                .iter()
                .map(|offset| offset.offset)
                .collect()
        })
        .unwrap_or_default()
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

#[tokio::test(start_paused = true)]
async fn a_refresh_rereads_the_watched_groups_without_waiting_for_their_tier() {
    let mut rig = Rig::new(consumers(2));
    rig.poll(&rig.topology()).await;
    rig.spawn_offsets(rig.offsets());
    quiesce().await;
    let _lease = rig.store.interest.lease_group("group-00");
    for id in ["group-00", "group-01"] {
        rig.cluster.commit_offsets(id, &[("orders.created", 0, 7)]);
    }
    let started = Instant::now();

    rig.store
        .offsets
        .refresh_until(|table| committed(table, "group-00") == [7])
        .await;

    assert!(started.elapsed() < Duration::from_secs(1));
    let table = rig.store.offsets.load().expect("offsets");
    assert_eq!(
        committed(&table, "group-01"),
        [2],
        "a group nobody watches keeps its tier"
    );
}
