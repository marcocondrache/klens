use std::sync::Arc;

use crate::kafka::store::Change;
use crate::testing::{Api, FakeCluster, Rig, config_entry, quiesce, until};

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
