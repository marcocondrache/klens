use std::sync::Arc;

use crate::kafka::store::fixtures::{at, group, partition, topic, topology, watermarks};

use super::super::harness::{ok, ok_as, seeded, state, store_of, viewer_everywhere};

#[tokio::test]
async fn group_rows_join_commits_against_watermarks() {
    let state = seeded();
    let data = ok(&state, "/clusters/local/groups").await;
    let row = &data["rows"][0];

    assert_eq!(row["id"], "order-processor");
    assert_eq!(row["state"], "STABLE");
    assert_eq!(row["memberCount"], 1);
    assert_eq!(row["topicNames"], serde_json::json!(["orders.created"]));
    assert_eq!(row["totalLag"], "15");
    assert_eq!(row["lagComplete"], true);
}

#[tokio::test]
async fn opening_a_group_registers_interest_so_its_offsets_poll_faster() {
    let state = seeded();
    let store = store_of(&state, "local");
    assert!(!store.interest.is_hot("order-processor"));

    let group = ok(&state, "/clusters/local/groups/order-processor").await;

    assert_eq!(group["totalLag"], "15");
    assert_eq!(group["members"][0]["clientId"], "c1");
    assert_eq!(group["offsets"][0]["currentOffset"], "90");
    assert_eq!(group["offsets"][0]["endOffset"], "100");
    assert_eq!(group["offsets"][0]["lag"], "10");
    assert!(store.interest.is_hot("order-processor"));
}

#[tokio::test]
async fn a_group_id_with_a_slash_is_one_resource() {
    let state = state();
    let store = store_of(&state, "local");
    store.topology.commit(Arc::new(topology(
        vec![topic(
            "orders.created",
            vec![partition(0, vec![1], vec![1])],
        )],
        vec![group("billing/nightly", "orders.created", vec![0])],
    )));

    let group = ok(&state, "/clusters/local/groups/billing/nightly").await;

    assert_eq!(group["id"], "billing/nightly");
}

#[tokio::test]
async fn group_rows_stay_open_to_a_viewer() {
    let groups = ok_as(&seeded(), "/clusters/local/groups", viewer_everywhere()).await;

    assert_eq!(groups["total"], 1);
}

#[tokio::test]
async fn a_group_whose_offsets_were_never_fetched_has_unknown_lag() {
    let state = state();
    let store = store_of(&state, "local");
    store.topology.commit(Arc::new(topology(
        vec![topic(
            "orders.created",
            vec![partition(0, vec![1], vec![1])],
        )],
        vec![group("order-processor", "orders.created", vec![0])],
    )));
    store.watermarks.commit(Arc::new(watermarks(
        at(1_000),
        &[("orders.created", 0, 0, 100)],
    )));

    let rows = ok(&state, "/clusters/local/groups").await;
    let group = ok(&state, "/clusters/local/groups/order-processor").await;
    let topic_groups = ok(&state, "/clusters/local/topics/orders.created/groups").await;

    assert_eq!(rows["rows"][0]["totalLag"], serde_json::Value::Null);
    assert_eq!(group["totalLag"], serde_json::Value::Null);
    assert_eq!(
        group["offsets"][0]["currentOffset"],
        serde_json::Value::Null
    );
    assert_eq!(group["offsets"][0]["endOffset"], "100");
    assert_eq!(group["offsets"][0]["lag"], serde_json::Value::Null);
    assert_eq!(topic_groups[0]["lagOnTopic"], serde_json::Value::Null);
}
