use std::sync::Arc;

use crate::kafka::store::fixtures::{group, partition, topic, topology};

use super::super::harness::{ok, ok_as, seeded, state, viewer_everywhere};

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
    let store = state.cluster("local").expect("local cluster");
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
    let store = state.cluster("local").expect("local cluster");
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
