use std::sync::Arc;

use tracing::Level;

use crate::kafka::store::Change;
use crate::testing::{Api, LogCapture, Rig, quiesce, until};

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
