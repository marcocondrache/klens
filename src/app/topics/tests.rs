use std::sync::Arc;
use std::time::Duration;

use axum::http::StatusCode;
use serde_json::{Value, json};
use tokio::time::Instant;
use tracing::Level;

use crate::testing::{
    Api, FakeCluster, LogCapture, TestApp, access, config_entry, offline_partition, partition,
    quiesce, topic, viewer,
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

async fn writable() -> TestApp {
    TestApp::of([FakeCluster::local()])
        .writable(&["local"])
        .ingested()
        .await
}

#[tokio::test]
async fn a_created_topic_shows_before_the_create_answers() {
    let app = writable().await;
    let mut rig = app.rig();
    let lane = rig.topology();
    rig.spawn(lane);
    quiesce().await;
    let logs = LogCapture::at(Level::INFO);

    app.post(
        "/clusters/local/topics",
        &json!({ "name": "invoices", "partitions": 3, "configs": { "cleanup.policy": "compact" } }),
    )
    .await
    .expect(StatusCode::CREATED);

    let topic = app.get("/clusters/local/topics/invoices").await.ok();
    assert_eq!(topic["partitions"].as_array().map(Vec::len), Some(3));
    rig.poll(&rig.configs()).await;
    let configs = app
        .get("/clusters/local/topics/invoices/configs")
        .await
        .ok();
    assert_eq!(configs[0]["name"], "cleanup.policy");
    assert_eq!(configs[0]["value"], "compact");
    assert_eq!(app.cluster().calls(Api::CreateTopic), 1);
    logs.assert_contains("created topic");
}

#[tokio::test(start_paused = true)]
async fn a_create_answers_even_when_the_store_never_shows_it() {
    let app = writable().await;

    app.post("/clusters/local/topics", &json!({ "name": "invoices" }))
        .await
        .expect(StatusCode::CREATED);

    app.get("/clusters/local/topics/invoices")
        .await
        .assert_error(StatusCode::NOT_FOUND, "UNKNOWN_TOPIC");
}

#[tokio::test]
async fn a_create_without_the_privilege_names_the_one_it_needs() {
    let reply = writable()
        .await
        .with_access(access([viewer()]))
        .post("/clusters/local/topics", &json!({ "name": "invoices" }))
        .await;

    reply.assert_error(StatusCode::FORBIDDEN, "FORBIDDEN");
    assert_eq!(
        reply.body["error"],
        "'manageTopics' is not permitted on cluster 'local'"
    );
}

#[tokio::test]
async fn creating_a_topic_that_exists_carries_the_broker_refusal() {
    let reply = writable()
        .await
        .post(
            "/clusters/local/topics",
            &json!({ "name": "orders.created" }),
        )
        .await;

    reply.assert_error(StatusCode::UNPROCESSABLE_ENTITY, "REFUSED");
    assert!(
        reply.body["error"]
            .as_str()
            .is_some_and(|error| error.contains("Topic 'orders.created' already exists.")),
        "{}",
        reply.body
    );
}

#[tokio::test]
async fn a_create_kafka_would_reject_never_reaches_the_broker() {
    let app = writable().await;

    for body in [
        json!({ "name": "bad name" }),
        json!({ "name": "" }),
        json!({ "name": "invoices", "partitions": 0 }),
        json!({ "name": "invoices", "replicationFactor": 0 }),
        json!({ "name": "invoices", "compacted": true }),
    ] {
        app.post("/clusters/local/topics", &body)
            .await
            .assert_error(StatusCode::UNPROCESSABLE_ENTITY, "INVALID_REQUEST");
    }

    assert_eq!(app.cluster().calls(Api::CreateTopic), 0);
}

#[tokio::test]
async fn a_deleted_topic_is_gone_before_the_delete_answers() {
    let app = writable().await;
    let mut rig = app.rig();
    let lane = rig.topology();
    rig.spawn(lane);
    quiesce().await;
    let logs = LogCapture::at(Level::INFO);

    app.delete("/clusters/local/topics/orders.created")
        .await
        .expect(StatusCode::NO_CONTENT);

    app.get("/clusters/local/topics/orders.created")
        .await
        .assert_error(StatusCode::NOT_FOUND, "UNKNOWN_TOPIC");
    assert_eq!(app.cluster().calls(Api::DeleteTopic), 1);
    logs.assert_contains("deleted topic");
}

#[tokio::test]
async fn deleting_a_topic_the_store_does_not_know_never_reaches_the_broker() {
    let app = writable().await;

    app.delete("/clusters/local/topics/ghost")
        .await
        .assert_error(StatusCode::NOT_FOUND, "UNKNOWN_TOPIC");

    assert_eq!(app.cluster().calls(Api::DeleteTopic), 0);
}

#[tokio::test]
async fn an_internal_topic_is_never_deleted() {
    let app = TestApp::of([FakeCluster::local().with_topic("__consumer_offsets", 1, 0)])
        .writable(&["local"])
        .ingested()
        .await;

    let reply = app
        .delete("/clusters/local/topics/__consumer_offsets")
        .await;

    reply.assert_error(StatusCode::UNPROCESSABLE_ENTITY, "INTERNAL_TOPIC");
    assert_eq!(
        reply.body["error"],
        "klens leaves the internal topic '__consumer_offsets' alone"
    );
    assert_eq!(app.cluster().calls(Api::DeleteTopic), 0);
}

async fn writable_compacted() -> TestApp {
    TestApp::of([compacted()])
        .writable(&["local"])
        .ingested()
        .await
}

#[tokio::test(start_paused = true)]
async fn an_edited_config_shows_before_the_edit_answers() {
    let app = writable_compacted().await;
    let mut rig = app.rig();
    let lane = rig.configs();
    rig.spawn(lane);
    quiesce().await;
    let logs = LogCapture::at(Level::INFO);
    let started = Instant::now();

    app.patch(
        "/clusters/local/topics/orders.created/configs",
        &json!({ "set": { "retention.ms": "60000" }, "reset": ["cleanup.policy"] }),
    )
    .await
    .expect(StatusCode::NO_CONTENT);

    assert!(
        started.elapsed() < Duration::from_secs(1),
        "the edit answers as soon as the lane shows it"
    );
    let configs = app
        .get("/clusters/local/topics/orders.created/configs")
        .await
        .ok();
    assert_eq!(
        configs,
        json!([{
            "name": "retention.ms",
            "value": "60000",
            "source": "DYNAMIC_TOPIC_CONFIG",
            "readOnly": false,
            "sensitive": false
        }])
    );
    assert_eq!(app.cluster().calls(Api::AlterTopicConfigs), 1);
    logs.assert_contains(r#"altered topic configs cluster=local topic="orders.created" set=["retention.ms"] reset={"cleanup.policy"}"#);
}

#[tokio::test]
async fn a_config_edit_kafka_would_reject_never_reaches_the_broker() {
    let app = writable_compacted().await;

    for (topic, body, status, code, error) in [
        (
            "orders.created",
            json!({}),
            StatusCode::UNPROCESSABLE_ENTITY,
            "INVALID_REQUEST",
            "name a config to set or reset",
        ),
        (
            "orders.created",
            json!({ "set": { "retention.ms": "1" }, "reset": ["retention.ms"] }),
            StatusCode::UNPROCESSABLE_ENTITY,
            "INVALID_REQUEST",
            "retention.ms cannot be both set and reset",
        ),
        (
            "ghost",
            json!({ "reset": ["retention.ms"] }),
            StatusCode::NOT_FOUND,
            "UNKNOWN_TOPIC",
            "unknown topic 'ghost' in cluster 'local'",
        ),
    ] {
        let reply = app
            .patch(&format!("/clusters/local/topics/{topic}/configs"), &body)
            .await;
        reply.assert_error(status, code);
        assert_eq!(reply.body["error"], error);
    }

    assert_eq!(app.cluster().calls(Api::AlterTopicConfigs), 0);
}

#[tokio::test]
async fn an_internal_topic_keeps_its_configs() {
    let app = TestApp::of([FakeCluster::local().with_topic("__consumer_offsets", 1, 0)])
        .writable(&["local"])
        .ingested()
        .await;

    app.patch(
        "/clusters/local/topics/__consumer_offsets/configs",
        &json!({ "set": { "retention.ms": "1" } }),
    )
    .await
    .assert_error(StatusCode::UNPROCESSABLE_ENTITY, "INTERNAL_TOPIC");

    assert_eq!(app.cluster().calls(Api::AlterTopicConfigs), 0);
}

#[tokio::test(start_paused = true)]
async fn added_partitions_show_before_the_add_answers() {
    let app = writable().await;
    let mut rig = app.rig();
    let lane = rig.topology();
    rig.spawn(lane);
    quiesce().await;
    let logs = LogCapture::at(Level::INFO);

    app.post(
        "/clusters/local/topics/orders.created/partitions",
        &json!({ "count": 5 }),
    )
    .await
    .expect(StatusCode::NO_CONTENT);

    let topic = app.get("/clusters/local/topics/orders.created").await.ok();
    assert_eq!(topic["partitions"].as_array().map(Vec::len), Some(5));
    assert_eq!(app.cluster().calls(Api::AddPartitions), 1);
    logs.assert_contains(r#"added partitions cluster=local topic="orders.created" count=5"#);
}

#[tokio::test]
async fn fewer_partitions_carry_the_broker_refusal() {
    let reply = writable()
        .await
        .post(
            "/clusters/local/topics/orders.created/partitions",
            &json!({ "count": 1 }),
        )
        .await;

    reply.assert_error(StatusCode::UNPROCESSABLE_ENTITY, "REFUSED");
    assert_eq!(
        reply.body["error"],
        "kafka refused the change: Topic currently has 2 partitions, which is higher than the requested 1."
    );
}

#[tokio::test]
async fn partitions_for_an_unknown_topic_never_reach_the_broker() {
    let app = writable().await;

    app.post(
        "/clusters/local/topics/ghost/partitions",
        &json!({ "count": 3 }),
    )
    .await
    .assert_error(StatusCode::NOT_FOUND, "UNKNOWN_TOPIC");

    assert_eq!(app.cluster().calls(Api::AddPartitions), 0);
}
