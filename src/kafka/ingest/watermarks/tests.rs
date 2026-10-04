use std::sync::Arc;
use std::time::Duration;

use tokio::time::advance;

use super::*;
use crate::testing::{Api, FakeCluster, Rig, quiesce, watermarks};

fn pairs(rates: &[TopicRate]) -> Vec<(String, f64)> {
    rates
        .iter()
        .map(|rate| (rate.topic.to_string(), rate.rate))
        .collect()
}

fn rates(
    previous: Option<&WatermarkTable>,
    next: &WatermarkTable,
    elapsed: Option<Duration>,
) -> Vec<(String, f64)> {
    pairs(&rates_between(previous, next, elapsed))
}

fn secs(seconds: u64) -> Option<Duration> {
    Some(Duration::from_secs(seconds))
}

#[test]
fn the_first_sample_has_no_rate_to_report() {
    let next = watermarks(&[("orders", 0, 0, 100)]);
    assert_eq!(rates(None, &next, None), vec![("orders".into(), 0.0)]);
}

#[test]
fn the_rate_is_the_high_watermark_delta_over_elapsed_time() {
    let previous = watermarks(&[("orders", 0, 0, 10), ("orders", 1, 0, 10)]);
    let next = watermarks(&[("orders", 0, 0, 30), ("orders", 1, 0, 20)]);

    assert_eq!(
        rates(Some(&previous), &next, secs(2)),
        vec![("orders".into(), 15.0)]
    );
}

#[test]
fn a_truncated_log_reports_zero_rather_than_a_negative_rate() {
    let previous = watermarks(&[("orders", 0, 0, 800)]);
    let next = watermarks(&[("orders", 0, 700, 800)]);

    assert_eq!(
        rates(Some(&previous), &next, secs(2)),
        vec![("orders".into(), 0.0)]
    );
}

#[test]
fn a_stale_previous_sample_is_not_divided_by() {
    let previous = watermarks(&[("orders", 0, 0, 10)]);
    let next = watermarks(&[("orders", 0, 0, 100_000)]);

    assert_eq!(
        rates(Some(&previous), &next, None),
        vec![("orders".into(), 0.0)],
        "the caller drops a gap longer than MAX_SAMPLE_GAP"
    );
}

#[test]
fn a_new_partition_contributes_nothing_on_its_first_appearance() {
    let previous = watermarks(&[("orders", 0, 0, 10)]);
    let next = watermarks(&[("orders", 0, 0, 10), ("orders", 1, 0, 5_000)]);

    assert_eq!(
        rates(Some(&previous), &next, secs(2)),
        vec![("orders".into(), 0.0)],
        "a partition first seen now has no baseline to measure against"
    );
}

#[test]
fn rates_are_reported_per_topic_in_name_order() {
    let previous = watermarks(&[("payments", 0, 0, 0), ("orders", 0, 0, 0)]);
    let next = watermarks(&[("payments", 0, 0, 4), ("orders", 0, 0, 2)]);

    assert_eq!(
        rates(Some(&previous), &next, secs(1)),
        vec![("orders".into(), 2.0), ("payments".into(), 4.0)]
    );
}

#[test]
fn rates_round_to_thousandths() {
    let previous = watermarks(&[("orders", 0, 0, 0)]);
    let next = watermarks(&[("orders", 0, 0, 1)]);

    assert_eq!(
        rates(Some(&previous), &next, secs(3)),
        vec![("orders".into(), 0.333)]
    );
}

#[tokio::test(start_paused = true)]
async fn a_tick_carries_only_the_rates_that_changed() {
    let rig = Rig::local();
    let lane = rig.watermarks();
    let mut bus = rig.store.bus.probe();
    let step = |marks: &[(&str, i32, i64, i64)]| Arc::new(watermarks(marks));

    let first = step(&[("orders", 0, 0, 10), ("payments", 0, 0, 10)]);
    lane.publish(&rig.store, None, &first, ());
    assert_eq!(
        pairs(&bus.next(Change::watermarks).rates),
        [("orders".into(), 0.0), ("payments".into(), 0.0)],
        "the first tick seeds every topic"
    );

    advance(Duration::from_secs(1)).await;
    let busy = step(&[("orders", 0, 0, 14), ("payments", 0, 0, 10)]);
    lane.publish(&rig.store, Some(&first), &busy, ());
    assert_eq!(
        pairs(&bus.next(Change::watermarks).rates),
        [("orders".into(), 4.0)]
    );

    advance(Duration::from_secs(1)).await;
    lane.publish(&rig.store, Some(&busy), &busy, ());
    assert_eq!(
        pairs(&bus.next(Change::watermarks).rates),
        [("orders".into(), 0.0)],
        "a rate that falls to zero is still sent"
    );
    assert_eq!(rig.store.rates.get("orders"), Some(0.0));

    advance(Duration::from_secs(1)).await;
    lane.publish(&rig.store, Some(&busy), &busy, ());
    bus.assert_quiet();
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
async fn a_refresh_rereads_cached_low_watermarks() {
    let rig = Rig::local();
    let lane = rig.watermarks();
    rig.poll(&rig.topology()).await;
    rig.poll(&lane).await;
    rig.cluster
        .set_watermarks("orders.created", 0, Watermarks { low: 5, high: 12 });
    let store = Arc::clone(&rig.store);
    let waiter = tokio::spawn(async move {
        store
            .watermarks
            .refresh_until(|marks| {
                marks.get("orders.created", 0) == Some(Watermarks { low: 5, high: 12 })
            })
            .await;
    });
    quiesce().await;

    rig.poll(&lane).await;

    assert_eq!(rig.cluster.calls(Api::LowWatermarks), 2);
    tokio::time::timeout(Duration::from_secs(1), waiter)
        .await
        .expect("the refresh sees the new low watermark")
        .expect("the waiter");
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
