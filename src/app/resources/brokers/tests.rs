use std::time::Duration;

use axum::http::StatusCode;
use serde_json::{Value, json};
use tokio::time::Instant;
use tracing::Level;

use crate::testing::{Api, FakeCluster, LogCapture, TestApp};

#[tokio::test]
async fn broker_rows_count_the_partitions_each_node_carries() {
    let brokers = TestApp::local()
        .await
        .get("/clusters/local/brokers")
        .await
        .ok();

    assert_eq!(brokers[0]["host"], "localhost");
    assert_eq!(brokers[0]["partitionCount"], 2);
    assert_eq!(brokers[0]["leaderCount"], 2);
}

#[tokio::test]
async fn broker_rows_carry_the_log_dirs_on_each_node() {
    let brokers = TestApp::local()
        .await
        .get("/clusters/local/brokers")
        .await
        .ok();

    assert_eq!(brokers[0]["sizeBytes"], 6_144);
    assert_eq!(
        brokers[0]["logDirs"],
        json!([{
            "path": "/var/lib/kafka/data",
            "error": null,
            "totalBytes": 1_000_000,
            "usableBytes": 750_000,
            "cordoned": false,
            "sizeBytes": 6_144,
            "replicaCount": 2
        }])
    );
}

#[tokio::test]
async fn a_broker_has_no_size_before_the_log_dirs_lane_reports() {
    let cluster = FakeCluster::local();
    cluster.fail(Api::LogDirs, "log dirs unavailable");
    let app = TestApp::over(cluster).await;

    let brokers = app.get("/clusters/local/brokers").await.ok();

    assert_eq!(brokers[0]["sizeBytes"], Value::Null);
    assert_eq!(brokers[0]["logDirs"], json!([]));
}

#[tokio::test]
async fn broker_configs_stay_live_because_no_lane_sweeps_them() {
    let app = TestApp::local().await;
    let configs = app.get("/clusters/local/brokers/1/configs").await.ok();

    assert_eq!(configs[0]["name"], "log.retention.hours");
    assert_eq!(app.cluster().calls(Api::BrokerConfigs), 1);
}

#[tokio::test]
async fn configs_for_a_broker_the_topology_does_not_list_are_not_asked_for() {
    let app = TestApp::local().await;

    app.get("/clusters/local/brokers/9/configs")
        .await
        .assert_error(StatusCode::NOT_FOUND, "UNKNOWN_BROKER");
    assert_eq!(app.cluster().calls(Api::BrokerConfigs), 0);
}

#[tokio::test]
async fn a_non_numeric_broker_id_is_an_invalid_request() {
    TestApp::local()
        .await
        .get("/clusters/local/brokers/one/configs")
        .await
        .assert_error(StatusCode::BAD_REQUEST, "INVALID_REQUEST");
}

async fn writable(cluster: FakeCluster) -> TestApp {
    TestApp::of([cluster]).writable(&["local"]).ingested().await
}

fn retention(entries: &Value) -> &Value {
    entries
        .as_array()
        .expect("configs")
        .iter()
        .find(|entry| entry["name"] == "log.retention.hours")
        .expect("log.retention.hours")
}

#[tokio::test(start_paused = true)]
async fn a_broker_config_edit_shows_on_the_broker_before_the_edit_answers() {
    let app = writable(FakeCluster::local()).await;
    let logs = LogCapture::at(Level::INFO);
    let started = Instant::now();

    app.patch(
        "/clusters/local/brokers/1/configs",
        &json!({ "set": { "log.retention.hours": "72" }, "reset": ["log.retention.ms"] }),
    )
    .await
    .expect(StatusCode::NO_CONTENT);

    assert!(started.elapsed() < Duration::from_secs(1));
    assert_eq!(app.cluster().calls(Api::BrokerConfigs), 1);
    let configs = app.get("/clusters/local/brokers/1/configs").await.ok();
    assert_eq!(retention(&configs)["value"], "72");
    assert_eq!(retention(&configs)["source"], "DYNAMIC_BROKER_CONFIG");
    logs.assert_contains(r#"altered broker configs cluster=local scope=broker 1 set=["log.retention.hours"] reset={"log.retention.ms"}"#);
}

#[tokio::test(start_paused = true)]
async fn a_cluster_default_shows_on_every_broker_before_the_edit_answers() {
    let app = writable(FakeCluster::local().with_broker(2)).await;
    let logs = LogCapture::at(Level::INFO);
    let started = Instant::now();

    app.patch(
        "/clusters/local/brokers/configs",
        &json!({ "set": { "log.retention.hours": "72" } }),
    )
    .await
    .expect(StatusCode::NO_CONTENT);

    assert!(started.elapsed() < Duration::from_secs(1));
    assert_eq!(app.cluster().calls(Api::BrokerConfigs), 2);
    for broker in [1, 2] {
        let configs = app
            .get(&format!("/clusters/local/brokers/{broker}/configs"))
            .await
            .ok();
        assert_eq!(retention(&configs)["value"], "72");
        assert_eq!(
            retention(&configs)["source"],
            "DYNAMIC_DEFAULT_BROKER_CONFIG"
        );
    }
    logs.assert_contains(r#"altered broker configs cluster=local scope=every broker set=["log.retention.hours"] reset={}"#);
}

#[tokio::test]
async fn a_broker_config_edit_kafka_would_reject_never_reaches_the_broker() {
    let app = writable(FakeCluster::local()).await;

    for (route, body, status, code, error) in [
        (
            "/clusters/local/brokers/configs",
            json!({}),
            StatusCode::UNPROCESSABLE_ENTITY,
            "INVALID_REQUEST",
            "name a config to set or reset",
        ),
        (
            "/clusters/local/brokers/1/configs",
            json!({ "set": { "log.retention.hours": "1" }, "reset": ["log.retention.hours"] }),
            StatusCode::UNPROCESSABLE_ENTITY,
            "INVALID_REQUEST",
            "log.retention.hours cannot be both set and reset",
        ),
        (
            "/clusters/local/brokers/9/configs",
            json!({ "reset": ["log.retention.hours"] }),
            StatusCode::NOT_FOUND,
            "UNKNOWN_BROKER",
            "unknown broker 9 in cluster 'local'",
        ),
    ] {
        let reply = app.patch(route, &body).await;
        reply.assert_error(status, code);
        assert_eq!(reply.body["error"], error);
    }

    assert_eq!(app.cluster().calls(Api::AlterBrokerConfigs), 0);
}
