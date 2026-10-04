use std::time::Duration;

use axum::http::StatusCode;
use serde_json::{Value, json};
use tokio::time::Instant;
use tracing::Level;

use crate::kafka::model::AclListing;
use crate::testing::{Api, FakeCluster, LogCapture, TestApp, quiesce};

#[tokio::test]
async fn acls_are_served_from_the_store_without_calling_kafka() {
    let app = TestApp::local().await;
    let acls = app.get("/clusters/local/acls").await.ok();

    assert_eq!(acls["status"], "ENABLED");
    assert_eq!(acls["bindings"].as_array().expect("bindings").len(), 3);
    assert_eq!(app.cluster().calls(Api::Acls), 0);
}

#[tokio::test]
async fn acls_are_pending_until_the_lane_first_reads_them() {
    let cluster = FakeCluster::local();
    cluster.fail(Api::Acls, "broker down");
    let app = TestApp::over(cluster).await;

    let acls = app.get("/clusters/local/acls").await.ok();

    assert_eq!(acls["status"], "PENDING");
    assert!(acls["bindings"].as_array().expect("bindings").is_empty());
    assert_eq!(
        acls["sourceHealth"]["lastError"],
        "kafka admin request failed: broker down"
    );
}

#[tokio::test]
async fn a_denied_describe_says_so_instead_of_failing() {
    let cluster = FakeCluster::local();
    cluster.set_acls(AclListing::Denied);
    let app = TestApp::over(cluster).await;

    let acls = app.get("/clusters/local/acls").await.ok();

    assert_eq!(acls["status"], "DENIED");
    assert!(acls["bindings"].as_array().expect("bindings").is_empty());
}

const ALICE_READS_ORDERS: &str = "/clusters/local/acls?resourceType=TOPIC&resourceName=orders.created&patternType=LITERAL&principal=User:alice&host=*&operation=READ&permission=ALLOW";

async fn writable(cluster: FakeCluster) -> TestApp {
    TestApp::of([cluster]).writable(&["local"]).ingested().await
}

fn bob_reads_orders() -> Value {
    json!({
        "bindings": [
            {
                "resourceType": "TOPIC",
                "resourceName": "orders.created",
                "patternType": "LITERAL",
                "principal": "User:bob",
                "host": "*",
                "operation": "READ",
                "permission": "ALLOW",
            },
            {
                "resourceType": "GROUP",
                "resourceName": "order-",
                "patternType": "PREFIXED",
                "principal": "User:bob",
                "host": "*",
                "operation": "READ",
                "permission": "ALLOW",
            },
        ]
    })
}

fn principals(listing: &Value) -> Vec<&str> {
    listing["bindings"]
        .as_array()
        .expect("bindings")
        .iter()
        .map(|binding| binding["principal"].as_str().expect("principal"))
        .collect()
}

#[tokio::test(start_paused = true)]
async fn created_acls_are_listed_before_the_create_answers() {
    let app = writable(FakeCluster::local()).await;
    let mut rig = app.rig();
    let lane = rig.acls();
    rig.spawn(lane);
    quiesce().await;
    let logs = LogCapture::at(Level::INFO);
    let started = Instant::now();

    app.post("/clusters/local/acls", &bob_reads_orders())
        .await
        .expect(StatusCode::CREATED);

    assert!(started.elapsed() < Duration::from_secs(1));
    let listing = app.get("/clusters/local/acls").await.ok();
    let created = &bob_reads_orders()["bindings"];
    let bindings = listing["bindings"].as_array().expect("bindings");
    assert!(bindings.contains(&created[0]));
    assert!(bindings.contains(&created[1]));
    logs.assert_contains(
        "created acl cluster=local acl=Allow User:bob from * to Read Literal Topic orders.created",
    );
    logs.assert_contains(
        "created acl cluster=local acl=Allow User:bob from * to Read Prefixed Group order-",
    );
}

#[tokio::test(start_paused = true)]
async fn a_deleted_acl_leaves_the_listing_before_the_delete_answers() {
    let app = writable(FakeCluster::local()).await;
    let mut rig = app.rig();
    let lane = rig.acls();
    rig.spawn(lane);
    quiesce().await;
    let logs = LogCapture::at(Level::INFO);
    let started = Instant::now();

    app.delete(ALICE_READS_ORDERS)
        .await
        .expect(StatusCode::NO_CONTENT);

    assert!(started.elapsed() < Duration::from_secs(1));
    let listing = app.get("/clusters/local/acls").await.ok();
    assert_eq!(
        principals(&listing),
        vec!["User:eve", "User:order-processor"]
    );
    logs.assert_contains(
        "deleted acl cluster=local acl=Allow User:alice from * to Read Literal Topic orders.created",
    );
}

#[tokio::test]
async fn creating_no_binding_is_refused_before_kafka() {
    let app = writable(FakeCluster::local()).await;

    app.post("/clusters/local/acls", &json!({ "bindings": [] }))
        .await
        .assert_error(StatusCode::UNPROCESSABLE_ENTITY, "INVALID_REQUEST");

    assert_eq!(app.cluster().calls(Api::CreateAcls), 0);
}

#[tokio::test(start_paused = true)]
async fn a_cluster_without_an_authorizer_refuses_acl_changes_with_kafkas_reason() {
    let cluster = FakeCluster::local();
    cluster.set_acls(AclListing::Disabled);
    let app = writable(cluster).await;

    for reply in [
        app.post("/clusters/local/acls", &bob_reads_orders()).await,
        app.delete(ALICE_READS_ORDERS).await,
    ] {
        reply.assert_error(StatusCode::UNPROCESSABLE_ENTITY, "REFUSED");
        assert_eq!(
            reply.body["error"],
            "kafka refused the change: No Authorizer is configured."
        );
    }
}
