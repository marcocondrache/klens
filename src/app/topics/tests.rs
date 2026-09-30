use std::sync::Arc;

use axum::http::StatusCode;
use serde_json::Value;

use crate::app::auth::access::EffectiveAccess;
use crate::kafka::FakeCluster;
use crate::kafka::store::fixtures::{offline_partition, partition, topic, topology};

use super::super::harness::{
    failure, ok, ok_as, seeded, seeded_with, state, store_of, viewer_everywhere,
};

#[tokio::test]
async fn topic_rows_project_counts_and_configs_without_touching_the_broker() {
    let (state, session) = seeded_with(FakeCluster::local());
    let rows = ok(&state, "/clusters/local/topics").await;

    assert_eq!(rows.as_array().map(Vec::len), Some(2));
    assert_eq!(rows[0]["name"], "orders.created");
    assert_eq!(rows[0]["partitionCount"], 2);
    assert_eq!(rows[0]["retainedMessages"], 150);
    assert_eq!(rows[0]["cleanupPolicy"], "COMPACT");
    assert_eq!(rows[0]["retentionMs"], 604_800_000);
    assert_eq!(rows[0]["groupCount"], 1);
    assert_eq!(session.calls().metadata(), 0);
}

#[tokio::test]
async fn topic_rows_carry_the_size_the_log_dirs_report() {
    let rows = ok(&seeded(), "/clusters/local/topics").await;

    assert_eq!(rows[0]["sizeBytes"], 5_120);
    assert_eq!(
        rows[1]["sizeBytes"],
        Value::Null,
        "no log dir reported payments.settled"
    );
}

#[tokio::test]
async fn topic_detail_sizes_the_topic_and_each_partition() {
    let topic = ok(&seeded(), "/clusters/local/topics/orders.created").await;

    assert_eq!(topic["sizeBytes"], 5_120);
    assert_eq!(topic["diskBytes"], 5_120);
    assert_eq!(topic["partitions"][0]["sizeBytes"], 4_096);
    assert_eq!(topic["partitions"][1]["sizeBytes"], 1_024);
}

#[tokio::test]
async fn topic_rows_expose_the_latest_rate() {
    let state = seeded();
    let store = store_of(&state, "local");
    let topic: Arc<str> = Arc::from("orders.created");
    store.rates.set(&topic, 12.5);
    store.rates.set(&topic, 13.5);

    let rows = ok(&state, "/clusters/local/topics").await;

    assert_eq!(rows[0]["name"], "orders.created");
    assert_eq!(rows[0]["rate"], 13.5);
    assert_eq!(rows[1]["name"], "payments.settled");
    assert_eq!(rows[1]["rate"], 0.0);
}

#[tokio::test]
async fn topic_detail_flags_under_replication_per_partition() {
    let state = state();
    let store = store_of(&state, "local");
    store.topology.commit(Arc::new(topology(
        vec![topic(
            "orders.created",
            vec![
                partition(0, vec![1, 2], vec![1, 2]),
                offline_partition(1, vec![1, 2]),
            ],
        )],
        Vec::new(),
    )));

    let topic = ok(&state, "/clusters/local/topics/orders.created").await;

    assert_eq!(topic["replicationFactor"], 2);
    assert_eq!(topic["underReplicated"], true);
    assert_eq!(topic["rate"], 0.0);
    assert_eq!(
        topic["retentionMs"],
        Value::Null,
        "retention is unknown before the configs lane fetches this topic"
    );
    assert_eq!(topic["cleanupPolicy"], "DELETE");
    assert_eq!(topic["partitions"][0]["underReplicated"], false);
    assert_eq!(topic["partitions"][1]["underReplicated"], true);
    assert_eq!(topic["partitions"][1]["leader"], -1);
}

#[tokio::test]
async fn a_missing_topic_is_not_found() {
    let (status, code) = failure(
        &seeded(),
        "/clusters/local/topics/ghost",
        EffectiveAccess::Unrestricted,
    )
    .await;

    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(code, "UNKNOWN_TOPIC");
}

#[tokio::test]
async fn topic_groups_report_lag_on_that_topic_alone() {
    let state = seeded();
    let data = ok(&state, "/clusters/local/topics/orders.created/groups").await;

    assert_eq!(data[0]["id"], "order-processor");
    assert_eq!(data[0]["lagOnTopic"], 15);
}

#[tokio::test]
async fn topic_configs_come_from_the_lane_not_the_broker() {
    let (state, session) = seeded_with(FakeCluster::local());
    let configs = ok(&state, "/clusters/local/topics/orders.created/configs").await;

    assert_eq!(configs[0]["name"], "cleanup.policy");
    assert_eq!(configs[0]["value"], "compact");
    assert_eq!(configs[0]["source"], "DYNAMIC_TOPIC_CONFIG");
    assert_eq!(session.calls().topic_configs(), 0);
}

#[tokio::test]
async fn topic_configs_for_an_unknown_topic_are_an_error() {
    let (status, code) = failure(
        &seeded(),
        "/clusters/local/topics/ghost/configs",
        EffectiveAccess::Unrestricted,
    )
    .await;

    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(code, "UNKNOWN_TOPIC");
}

#[tokio::test]
async fn topic_configs_are_forbidden_without_the_configs_privilege() {
    let (status, code) = failure(
        &seeded(),
        "/clusters/local/topics/orders.created/configs",
        viewer_everywhere(),
    )
    .await;

    assert_eq!(
        (status, code.as_str()),
        (StatusCode::FORBIDDEN, "FORBIDDEN")
    );
}

#[tokio::test]
async fn topic_rows_stay_open_to_a_viewer() {
    let topics = ok_as(&seeded(), "/clusters/local/topics", viewer_everywhere()).await;

    assert_eq!(topics.as_array().map(Vec::len), Some(2));
}
