use std::sync::Arc;

use axum::http::StatusCode;
use serde_json::Value;

use crate::testing::{
    Api, FakeCluster, TestApp, access, config_entry, offline_partition, partition, topic, viewer,
};

fn compacted() -> FakeCluster {
    let cluster = FakeCluster::local();
    cluster.set_topic_configs(
        "orders.created",
        vec![
            config_entry("cleanup.policy", "compact"),
            config_entry("retention.ms", "86400000"),
        ],
    );
    cluster
}

fn with_payments() -> FakeCluster {
    FakeCluster::local().with_topic("payments.settled", 1, 5)
}

#[tokio::test]
async fn topic_rows_project_counts_and_configs_without_touching_the_broker() {
    let app = TestApp::over(compacted()).await;
    let rows = app.get("/clusters/local/topics").await.ok();

    assert_eq!(rows.as_array().map(Vec::len), Some(1));
    assert_eq!(rows[0]["name"], "orders.created");
    assert_eq!(rows[0]["partitionCount"], 2);
    assert_eq!(rows[0]["retainedMessages"], 16);
    assert_eq!(rows[0]["cleanupPolicy"], "COMPACT");
    assert_eq!(rows[0]["retentionMs"], 86_400_000);
    assert_eq!(rows[0]["groupCount"], 1);
    assert_eq!(app.cluster().calls(Api::Metadata), 0);
}

#[tokio::test]
async fn topic_rows_carry_the_size_the_log_dirs_report() {
    let rows = TestApp::over(with_payments())
        .await
        .get("/clusters/local/topics")
        .await
        .ok();

    assert_eq!(rows[0]["sizeBytes"], 6_144);
    assert_eq!(
        rows[1]["sizeBytes"],
        Value::Null,
        "no log dir reported payments.settled"
    );
}

#[tokio::test]
async fn topic_detail_sizes_the_topic_and_each_partition() {
    let topic = TestApp::local()
        .await
        .get("/clusters/local/topics/orders.created")
        .await
        .ok();

    assert_eq!(topic["sizeBytes"], 6_144);
    assert_eq!(topic["diskBytes"], 6_144);
    assert_eq!(topic["partitions"][0]["sizeBytes"], 4_096);
    assert_eq!(topic["partitions"][1]["sizeBytes"], 2_048);
}

#[tokio::test]
async fn topic_rows_expose_the_latest_rate() {
    let app = TestApp::over(with_payments()).await;
    let orders: Arc<str> = Arc::from("orders.created");
    app.store().rates.set(&orders, 12.5);
    app.store().rates.set(&orders, 13.5);

    let rows = app.get("/clusters/local/topics").await.ok();

    assert_eq!(rows[0]["name"], "orders.created");
    assert_eq!(rows[0]["rate"], 13.5);
    assert_eq!(rows[1]["name"], "payments.settled");
    assert_eq!(rows[1]["rate"], 0.0);
}

#[tokio::test]
async fn topic_detail_flags_under_replication_per_partition() {
    let cluster = FakeCluster::local();
    cluster.put_topic(topic(
        "orders.created",
        vec![
            partition(0, vec![1, 2], vec![1, 2]),
            offline_partition(1, vec![1, 2]),
        ],
    ));
    let app = TestApp::over(cluster).await;

    let topic = app.get("/clusters/local/topics/orders.created").await.ok();

    assert_eq!(topic["replicationFactor"], 2);
    assert_eq!(topic["underReplicated"], true);
    assert_eq!(topic["rate"], 0.0);
    assert_eq!(topic["partitions"][0]["underReplicated"], false);
    assert_eq!(topic["partitions"][1]["underReplicated"], true);
    assert_eq!(topic["partitions"][1]["leader"], -1);
}

#[tokio::test]
async fn a_topic_has_no_retention_until_the_configs_lane_reads_it() {
    let cluster = FakeCluster::local();
    cluster.fail(Api::TopicConfigs, "describe configs timed out");
    let app = TestApp::over(cluster).await;

    let topic = app.get("/clusters/local/topics/orders.created").await.ok();

    assert_eq!(topic["retentionMs"], Value::Null);
    assert_eq!(topic["cleanupPolicy"], "DELETE");
}

#[tokio::test]
async fn a_missing_topic_is_not_found() {
    TestApp::local()
        .await
        .get("/clusters/local/topics/ghost")
        .await
        .assert_error(StatusCode::NOT_FOUND, "UNKNOWN_TOPIC");
}

#[tokio::test]
async fn topic_groups_report_lag_on_that_topic_alone() {
    let groups = TestApp::over(with_payments())
        .await
        .get("/clusters/local/topics/orders.created/groups")
        .await
        .ok();

    assert_eq!(groups.as_array().map(Vec::len), Some(1));
    assert_eq!(groups[0]["id"], "order-processor");
    assert_eq!(groups[0]["lagOnTopic"], 5);
}

#[tokio::test]
async fn topic_configs_come_from_the_lane_not_the_broker() {
    let app = TestApp::over(compacted()).await;
    app.cluster().set_topic_configs(
        "orders.created",
        vec![config_entry("cleanup.policy", "delete")],
    );

    let configs = app
        .get("/clusters/local/topics/orders.created/configs")
        .await
        .ok();

    assert_eq!(configs[0]["name"], "cleanup.policy");
    assert_eq!(configs[0]["value"], "compact");
    assert_eq!(configs[0]["source"], "DYNAMIC_TOPIC_CONFIG");
    assert_eq!(app.cluster().calls(Api::TopicConfigs), 0);
}

#[tokio::test]
async fn topic_configs_for_an_unknown_topic_are_an_error() {
    TestApp::local()
        .await
        .get("/clusters/local/topics/ghost/configs")
        .await
        .assert_error(StatusCode::NOT_FOUND, "UNKNOWN_TOPIC");
}

#[tokio::test]
async fn topic_configs_are_forbidden_without_the_configs_privilege() {
    TestApp::local()
        .await
        .with_access(access([viewer()]))
        .get("/clusters/local/topics/orders.created/configs")
        .await
        .assert_error(StatusCode::FORBIDDEN, "FORBIDDEN");
}

#[tokio::test]
async fn topic_rows_stay_open_to_a_viewer() {
    let topics = TestApp::local()
        .await
        .with_access(access([viewer()]))
        .get("/clusters/local/topics")
        .await
        .ok();

    assert_eq!(topics.as_array().map(Vec::len), Some(1));
}
