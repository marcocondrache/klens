use axum::http::StatusCode;
use serde_json::{Value, json};

use crate::testing::{Api, FakeCluster, TestApp, group};

#[tokio::test]
async fn group_rows_join_commits_against_watermarks() {
    let groups = TestApp::local()
        .await
        .get("/clusters/local/groups")
        .await
        .ok();
    let row = &groups[0];

    assert_eq!(row["id"], "order-processor");
    assert_eq!(row["state"], "STABLE");
    assert_eq!(row["memberCount"], 1);
    assert_eq!(row["topicNames"], json!(["orders.created"]));
    assert_eq!(row["totalLag"], 5);
    assert_eq!(row["lagComplete"], true);
}

#[tokio::test]
async fn opening_a_group_registers_interest_so_its_offsets_poll_faster() {
    let app = TestApp::local().await;
    assert!(!app.store().interest.is_hot("order-processor"));

    let group = app.get("/clusters/local/groups/order-processor").await.ok();

    assert_eq!(group["totalLag"], 5);
    assert_eq!(group["members"][0]["clientId"], "orders");
    assert_eq!(group["offsets"][0]["currentOffset"], 6);
    assert_eq!(group["offsets"][0]["endOffset"], 8);
    assert_eq!(group["offsets"][0]["lag"], 2);
    assert!(app.store().interest.is_hot("order-processor"));
}

#[tokio::test]
async fn a_missing_group_is_not_found() {
    TestApp::local()
        .await
        .get("/clusters/local/groups/ghost")
        .await
        .assert_error(StatusCode::NOT_FOUND, "UNKNOWN_GROUP");
}

#[tokio::test]
async fn a_group_id_with_a_slash_is_one_resource() {
    let billing = group("billing/nightly", "orders.created", vec![0]);
    let app = TestApp::over(FakeCluster::local().with_groups([billing])).await;

    let group = app.get("/clusters/local/groups/billing/nightly").await.ok();

    assert_eq!(group["id"], "billing/nightly");
}

#[tokio::test]
async fn a_group_whose_offsets_were_never_fetched_has_unknown_lag() {
    let cluster = FakeCluster::local();
    cluster.fail(Api::CommittedOffsets, "coordinator not available");
    let app = TestApp::over(cluster).await;

    let rows = app.get("/clusters/local/groups").await.ok();
    let group = app.get("/clusters/local/groups/order-processor").await.ok();
    let topic_groups = app
        .get("/clusters/local/topics/orders.created/groups")
        .await
        .ok();

    assert_eq!(rows[0]["totalLag"], Value::Null);
    assert_eq!(group["totalLag"], Value::Null);
    assert_eq!(group["offsets"][0]["currentOffset"], Value::Null);
    assert_eq!(group["offsets"][0]["endOffset"], 8);
    assert_eq!(group["offsets"][0]["lag"], Value::Null);
    assert_eq!(topic_groups[0]["lagOnTopic"], Value::Null);
}
