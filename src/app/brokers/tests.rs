use axum::http::StatusCode;
use serde_json::{Value, json};

use crate::testing::{Api, FakeCluster, TestApp};

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
