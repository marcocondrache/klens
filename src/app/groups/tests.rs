use std::time::Duration;

use axum::http::StatusCode;
use serde_json::{Value, json};
use tokio::time::Instant;
use tracing::Level;

use crate::testing::{Api, FakeCluster, LogCapture, TestApp, group, quiesce};

const ORDERS: &str = "orders.created";
const BILLING: &str = "/clusters/local/group-offsets/billing";

/// A stopped group that committed partition 0 of the two in `orders.created`,
/// whose watermarks are 0 and 8.
async fn billing() -> TestApp {
    let billing = group("billing", ORDERS, vec![0, 1])
        .with_committed(&[(ORDERS, 0, 6)])
        .stopped();
    TestApp::of([FakeCluster::local().with_groups([billing])])
        .writable(&["local"])
        .ingested()
        .await
}

fn moved(partition: i32, current: Option<i64>, new: i64) -> Value {
    json!({
        "topic": ORDERS,
        "partition": partition,
        "currentOffset": current,
        "newOffset": new,
        "endOffset": 8,
    })
}

fn new_offsets(plan: &Value) -> Vec<i64> {
    plan.as_array()
        .expect("plan")
        .iter()
        .map(|moved| moved["newOffset"].as_i64().expect("new offset"))
        .collect()
}

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

#[tokio::test]
async fn a_dry_run_plans_every_committed_partition_and_commits_nothing() {
    let app = billing().await;

    let plan = app
        .patch(
            BILLING,
            &json!({ "to": { "kind": "EARLIEST" }, "dryRun": true }),
        )
        .await
        .ok();

    assert_eq!(plan, json!([moved(0, Some(6), 0)]));
    assert_eq!(app.cluster().calls(Api::AlterGroupOffsets), 0);
}

#[tokio::test(start_paused = true)]
async fn a_reset_shows_on_the_group_before_it_answers() {
    let app = billing().await;
    let mut rig = app.rig();
    rig.spawn_offsets(rig.offsets());
    quiesce().await;
    let logs = LogCapture::at(Level::INFO);
    let started = Instant::now();

    let plan = app
        .patch(
            BILLING,
            &json!({ "topic": ORDERS, "to": { "kind": "LATEST" } }),
        )
        .await
        .ok();

    assert!(started.elapsed() < Duration::from_secs(1));
    assert_eq!(plan, json!([moved(0, Some(6), 8), moved(1, None, 8)]));
    let group = app.get("/clusters/local/groups/billing").await.ok();
    assert_eq!(group["totalLag"], 0);
    assert_eq!(app.cluster().calls(Api::AlterGroupOffsets), 1);
    logs.assert_contains(
        r#"reset group offsets cluster=local group=billing topic=Some("orders.created") partitions=2 to=Latest"#,
    );
}

#[tokio::test]
async fn a_topic_scopes_the_reset_to_the_partitions_it_names() {
    let app = billing().await;

    let plan = app
        .patch(
            BILLING,
            &json!({ "topic": ORDERS, "partitions": [1], "to": { "kind": "OFFSET", "offset": 3 }, "dryRun": true }),
        )
        .await
        .ok();

    assert_eq!(plan, json!([moved(1, None, 3)]));
}

#[tokio::test]
async fn every_target_lands_inside_the_log() {
    let app = billing().await;
    let all = |to: Value| json!({ "topic": ORDERS, "to": to, "dryRun": true });

    for (to, expected) in [
        (json!({ "kind": "OFFSET", "offset": 20 }), [8, 8]),
        (
            json!({ "kind": "TIMESTAMP", "timestamp": 1_700_000_003_500_i64 }),
            [5, 4],
        ),
        (
            json!({ "kind": "TIMESTAMP", "timestamp": 1_800_000_000_000_i64 }),
            [8, 8],
        ),
    ] {
        let plan = app.patch(BILLING, &all(to.clone())).await.ok();
        assert_eq!(new_offsets(&plan), expected, "{to}");
    }
    let shifted = app
        .patch(
            BILLING,
            &json!({ "to": { "kind": "SHIFT", "by": -10 }, "dryRun": true }),
        )
        .await
        .ok();
    assert_eq!(new_offsets(&shifted), [0]);
}

#[tokio::test]
async fn a_shift_needs_a_committed_offset_on_every_partition() {
    billing()
        .await
        .patch(
            BILLING,
            &json!({ "topic": ORDERS, "to": { "kind": "SHIFT", "by": -1 }, "dryRun": true }),
        )
        .await
        .assert_error(StatusCode::UNPROCESSABLE_ENTITY, "NO_COMMITTED_OFFSET");
}

#[tokio::test]
async fn a_group_with_members_plans_but_never_commits() {
    let app = billing().await;
    let path = "/clusters/local/group-offsets/order-processor";

    app.patch(path, &json!({ "to": { "kind": "LATEST" }, "dryRun": true }))
        .await
        .ok();
    let reply = app
        .patch(path, &json!({ "to": { "kind": "LATEST" } }))
        .await;

    reply.assert_error(StatusCode::CONFLICT, "ACTIVE_GROUP");
    assert_eq!(
        reply.body["error"],
        "group 'order-processor' has members; stop its consumers first"
    );
    assert_eq!(app.cluster().calls(Api::AlterGroupOffsets), 0);
}

#[tokio::test]
async fn a_reset_klens_would_reject_never_reaches_kafka() {
    let app = billing().await;
    let latest = json!({ "kind": "LATEST" });

    for (path, body, status, code) in [
        (
            "/clusters/local/group-offsets/ghost",
            json!({ "to": latest }),
            StatusCode::NOT_FOUND,
            "UNKNOWN_GROUP",
        ),
        (
            BILLING,
            json!({ "topic": "ghost", "to": latest }),
            StatusCode::NOT_FOUND,
            "UNKNOWN_TOPIC",
        ),
        (
            BILLING,
            json!({ "topic": ORDERS, "partitions": [0, 2], "to": latest }),
            StatusCode::NOT_FOUND,
            "UNKNOWN_PARTITION",
        ),
        (
            BILLING,
            json!({ "partitions": [0], "to": latest }),
            StatusCode::UNPROCESSABLE_ENTITY,
            "INVALID_REQUEST",
        ),
    ] {
        app.patch(path, &body).await.assert_error(status, code);
    }

    assert_eq!(app.cluster().calls(Api::AlterGroupOffsets), 0);
}

#[tokio::test(start_paused = true)]
async fn a_group_with_nothing_committed_resets_nothing() {
    let idle = group("idle", ORDERS, vec![0]).stopped();
    let app = TestApp::of([FakeCluster::local().with_groups([idle])])
        .writable(&["local"])
        .ingested()
        .await;

    let plan = app
        .patch(
            "/clusters/local/group-offsets/idle",
            &json!({ "to": { "kind": "EARLIEST" } }),
        )
        .await
        .ok();

    assert_eq!(plan, json!([]));
    assert_eq!(app.cluster().calls(Api::AlterGroupOffsets), 0);
}

#[tokio::test(start_paused = true)]
async fn a_group_id_with_a_slash_resets_as_one_group() {
    let nightly = group("billing/nightly", ORDERS, vec![0])
        .with_committed(&[(ORDERS, 0, 2)])
        .stopped();
    let app = TestApp::of([FakeCluster::local().with_groups([nightly])])
        .writable(&["local"])
        .ingested()
        .await;

    let plan = app
        .patch(
            "/clusters/local/group-offsets/billing/nightly",
            &json!({ "to": { "kind": "EARLIEST" }, "dryRun": true }),
        )
        .await
        .ok();

    assert_eq!(plan, json!([moved(0, Some(2), 0)]));
}

#[tokio::test(start_paused = true)]
async fn a_deleted_group_is_gone_before_the_delete_answers() {
    let app = billing().await;
    let mut rig = app.rig();
    let lane = rig.topology();
    rig.spawn(lane);
    quiesce().await;
    let logs = LogCapture::at(Level::INFO);
    let started = Instant::now();

    app.delete("/clusters/local/groups/billing")
        .await
        .expect(StatusCode::NO_CONTENT);

    assert!(started.elapsed() < Duration::from_secs(1));
    app.get("/clusters/local/groups/billing")
        .await
        .assert_error(StatusCode::NOT_FOUND, "UNKNOWN_GROUP");
    assert_eq!(app.cluster().calls(Api::DeleteGroup), 1);
    logs.assert_contains("deleted group cluster=local group=billing");
}

#[tokio::test]
async fn a_group_with_members_is_never_deleted() {
    let app = billing().await;

    let reply = app.delete("/clusters/local/groups/order-processor").await;

    reply.assert_error(StatusCode::CONFLICT, "ACTIVE_GROUP");
    assert_eq!(app.cluster().calls(Api::DeleteGroup), 0);
}

#[tokio::test]
async fn deleting_a_group_the_store_does_not_know_never_reaches_kafka() {
    let app = billing().await;

    app.delete("/clusters/local/groups/ghost")
        .await
        .assert_error(StatusCode::NOT_FOUND, "UNKNOWN_GROUP");

    assert_eq!(app.cluster().calls(Api::DeleteGroup), 0);
}

#[tokio::test(start_paused = true)]
async fn a_group_id_with_a_slash_deletes_as_one_group() {
    let nightly = group("billing/nightly", ORDERS, vec![0]).stopped();
    let billing = group("billing", ORDERS, vec![0]).stopped();
    let app = TestApp::of([FakeCluster::local().with_groups([nightly, billing])])
        .writable(&["local"])
        .ingested()
        .await;
    let mut rig = app.rig();
    let lane = rig.topology();
    rig.spawn(lane);
    quiesce().await;

    app.delete("/clusters/local/groups/billing/nightly")
        .await
        .expect(StatusCode::NO_CONTENT);

    app.get("/clusters/local/groups/billing/nightly")
        .await
        .assert_error(StatusCode::NOT_FOUND, "UNKNOWN_GROUP");
    app.get("/clusters/local/groups/billing").await.ok();
}
