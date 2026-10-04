use std::time::Duration;

use axum::http::StatusCode;
use serde_json::{Value, json};
use tokio::time::Instant;
use tracing::Level;

use crate::testing::{Api, FakeCluster, LogCapture, TestApp, quiesce};

const ORDER: &str = r#"{"type":"record","name":"Order","fields":[{"name":"id","type":"long"}]}"#;

async fn writable() -> TestApp {
    TestApp::of([FakeCluster::local()])
        .writable(&["local"])
        .ingested()
        .await
}

fn avro(schema: &str) -> Value {
    json!({ "type": "AVRO", "schema": schema })
}

fn versions(listing: &Value, subject: &str) -> Value {
    listing["rows"]
        .as_array()
        .expect("rows")
        .iter()
        .find(|row| row["subject"] == subject)
        .map_or(Value::Null, |row| row["versions"].clone())
}

#[tokio::test]
async fn a_schema_body_is_fetched_on_demand_rather_than_kept_in_the_lane() {
    let app = TestApp::local().await;
    let subject = app
        .get("/clusters/local/subjects/orders.created-value")
        .await
        .ok();

    assert_eq!(subject["subject"], "orders.created-value");
    assert_eq!(
        subject["version"], 2,
        "no version asked resolves to the latest"
    );
    assert_eq!(subject["type"], "AVRO");
    assert!(
        subject["schema"]
            .as_str()
            .expect("schema body")
            .contains("orderId")
    );
    assert_eq!(app.cluster().calls(Api::SubjectSchema), 1);
}

#[tokio::test]
async fn an_unknown_subject_is_a_typed_error() {
    TestApp::local()
        .await
        .get("/clusters/local/subjects/ghost-value")
        .await
        .assert_error(StatusCode::NOT_FOUND, "UNKNOWN_SUBJECT");
}

#[tokio::test(start_paused = true)]
async fn a_registered_schema_is_listed_before_the_register_answers() {
    let app = writable().await;
    let mut rig = app.rig();
    let lane = rig.subjects();
    rig.spawn(lane);
    quiesce().await;
    let logs = LogCapture::at(Level::INFO);
    let started = Instant::now();

    let registered = app
        .post(
            "/clusters/local/subjects/orders.created-value",
            &avro(ORDER),
        )
        .await
        .ok();

    assert!(started.elapsed() < Duration::from_secs(1));
    assert_eq!(registered, json!({ "id": 2, "version": 3 }));
    let listing = app.get("/clusters/local/subjects").await.ok();
    assert_eq!(versions(&listing, "orders.created-value"), json!([1, 2, 3]));
    let read = app
        .get("/clusters/local/subjects/orders.created-value?version=3")
        .await
        .ok();
    assert_eq!(read["schema"], ORDER);
    logs.assert_contains(
        "registered schema cluster=local subject=orders.created-value id=2 version=3",
    );
}

#[tokio::test(start_paused = true)]
async fn registering_a_schema_the_subject_holds_answers_its_version() {
    let app = writable().await;
    let route = "/clusters/local/subjects/orders.created-value";

    let first = app.post(route, &avro(ORDER)).await.ok();
    let again = app.post(route, &avro(ORDER)).await.ok();

    assert_eq!(again, first);
    assert_eq!(app.cluster().calls(Api::RegisterSchema), 2);
}

#[tokio::test(start_paused = true)]
async fn a_subject_with_a_slash_registers_as_one_subject() {
    let app = writable().await;
    let schema = json!({
        "type": "PROTOBUF",
        "schema": "syntax = \"proto3\";",
        "references": [{ "name": "order.proto", "subject": "orders.created-value", "version": 2 }],
    });

    let registered = app
        .post("/clusters/local/subjects/team/orders-value", &schema)
        .await
        .ok();

    assert_eq!(registered["version"], 1);
    let read = app
        .get("/clusters/local/subjects/team/orders-value?version=1")
        .await
        .ok();
    assert_eq!(read["subject"], "team/orders-value");
    assert_eq!(read["type"], "PROTOBUF");
    assert_eq!(read["references"], schema["references"]);
}

#[tokio::test]
async fn a_schema_the_registry_refuses_carries_its_message() {
    let app = writable().await;

    let reply = app
        .post("/clusters/local/subjects/orders.created-value", &avro("{"))
        .await;

    reply.assert_error(StatusCode::UNPROCESSABLE_ENTITY, "REGISTRY_REFUSED");
    assert_eq!(
        reply.body["error"],
        "the schema registry refused the change: Invalid schema {"
    );
}

#[tokio::test]
async fn a_cluster_without_a_registry_says_so_and_registers_nothing() {
    let app = TestApp::of([FakeCluster::local().without_schema_registry()])
        .writable(&["local"])
        .ingested()
        .await;

    let listing = app.get("/clusters/local/subjects").await.ok();
    assert_eq!(listing["hasRegistry"], false);
    app.post("/clusters/local/subjects/orders-value", &avro(ORDER))
        .await
        .assert_error(StatusCode::NOT_FOUND, "NO_SCHEMA_REGISTRY");
}

#[tokio::test]
async fn a_cluster_with_a_registry_says_so() {
    let listing = TestApp::local()
        .await
        .get("/clusters/local/subjects")
        .await
        .ok();

    assert_eq!(listing["hasRegistry"], true);
}

#[tokio::test(start_paused = true)]
async fn a_deleted_version_leaves_the_subject_before_the_delete_answers() {
    let app = writable().await;
    let mut rig = app.rig();
    let lane = rig.subjects();
    rig.spawn(lane);
    quiesce().await;
    let logs = LogCapture::at(Level::INFO);
    let started = Instant::now();

    app.delete("/clusters/local/subjects/orders.created-value?version=1")
        .await
        .expect(StatusCode::NO_CONTENT);

    assert!(started.elapsed() < Duration::from_secs(1));
    let listing = app.get("/clusters/local/subjects").await.ok();
    assert_eq!(versions(&listing, "orders.created-value"), json!([2]));
    logs.assert_contains(
        "deleted schema cluster=local subject=orders.created-value version=1 permanent=false",
    );
}

#[tokio::test(start_paused = true)]
async fn a_deleted_subject_is_gone_before_the_delete_answers() {
    let app = writable().await;
    let mut rig = app.rig();
    let lane = rig.subjects();
    rig.spawn(lane);
    quiesce().await;
    let started = Instant::now();

    app.delete("/clusters/local/subjects/orders.created-value?permanent=true")
        .await
        .expect(StatusCode::NO_CONTENT);

    assert!(started.elapsed() < Duration::from_secs(1));
    let listing = app.get("/clusters/local/subjects").await.ok();
    assert_eq!(listing["rows"], json!([]));
    app.get("/clusters/local/subjects/orders.created-value")
        .await
        .assert_error(StatusCode::NOT_FOUND, "UNKNOWN_SUBJECT");
}

#[tokio::test]
async fn deleting_a_schema_the_store_does_not_know_never_reaches_the_registry() {
    let app = writable().await;

    for route in [
        "/clusters/local/subjects/ghost-value",
        "/clusters/local/subjects/orders.created-value?version=9",
    ] {
        app.delete(route)
            .await
            .assert_error(StatusCode::NOT_FOUND, "UNKNOWN_SUBJECT");
    }

    assert_eq!(app.cluster().calls(Api::DeleteSchema), 0);
}
