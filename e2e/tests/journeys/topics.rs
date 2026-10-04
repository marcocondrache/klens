use e2e::{Kafka, Klens};
use krafka::admin::NewTopic;
use reqwest::StatusCode;
use serde_json::json;

use crate::row;

const TOPICS: &str = "/api/clusters/local/topics";

#[tokio::test]
async fn a_topic_lists_its_partitions_messages_and_size_on_disk() {
    let kafka = Kafka::start().await;
    kafka.topic("orders", 3).await;
    for partition in 0..3 {
        kafka.fill("orders", partition, &["a", "b", "c", "d"]).await;
    }
    let klens = Klens::over(&kafka).await;

    let rows = klens
        .eventually(TOPICS, |rows| {
            let orders = row(rows, "name", "orders");
            orders["retainedMessages"] == 12
                && orders["sizeBytes"].as_u64() > Some(0)
                && orders["retentionMs"] == 604_800_000
        })
        .await;
    let orders = row(&rows, "name", "orders");

    assert_eq!(orders["partitionCount"], 3);
    assert_eq!(orders["cleanupPolicy"], "DELETE");
    let detail = klens.get(&format!("{TOPICS}/orders")).await;
    assert_eq!(detail["replicationFactor"], 1);
    assert_eq!(detail["underReplicated"], false);
    for partition in detail["partitions"].as_array().expect("partitions") {
        assert_eq!(partition["leader"], 1);
        assert!(partition["sizeBytes"].as_u64() > Some(0), "{partition}");
    }
}

#[tokio::test]
async fn topic_configs_set_at_creation_show_on_the_row_and_read_live() {
    let kafka = Kafka::start().await;
    kafka
        .create(
            NewTopic::new("audit", 1, 1)
                .expect("a valid topic")
                .with_config("cleanup.policy", "compact")
                .with_config("retention.ms", "3600000"),
        )
        .await;
    let klens = Klens::over(&kafka).await;

    let rows = klens
        .eventually(TOPICS, |rows| {
            row(rows, "name", "audit")["retentionMs"] == 3_600_000
        })
        .await;

    assert_eq!(row(&rows, "name", "audit")["cleanupPolicy"], "COMPACT");
    let configs = klens.get(&format!("{TOPICS}/audit/configs")).await;
    assert_eq!(
        row(&configs, "name", "cleanup.policy"),
        &json!({
            "name": "cleanup.policy",
            "value": "compact",
            "source": "DYNAMIC_TOPIC_CONFIG",
            "readOnly": false,
            "sensitive": false
        })
    );
    assert_eq!(
        row(&configs, "name", "segment.bytes")["source"],
        "DEFAULT_CONFIG"
    );
}

#[tokio::test]
async fn an_edited_topic_config_reads_back_once_the_edit_answers() {
    let kafka = Kafka::start().await;
    kafka
        .create(
            NewTopic::new("audit", 1, 1)
                .expect("a valid topic")
                .with_config("cleanup.policy", "compact"),
        )
        .await;
    let klens = Klens::over(&kafka).await;
    let path = format!("{TOPICS}/audit/configs");
    klens
        .eventually(&path, |configs| {
            row(configs, "name", "cleanup.policy")["value"] == "compact"
        })
        .await;

    let (status, body) = klens
        .patch(
            &path,
            &json!({ "set": { "retention.ms": "3600000" }, "reset": ["cleanup.policy"] }),
        )
        .await;

    assert_eq!(status, StatusCode::NO_CONTENT, "{body}");
    let configs = klens.get(&path).await;
    assert_eq!(
        row(&configs, "name", "retention.ms"),
        &json!({
            "name": "retention.ms",
            "value": "3600000",
            "source": "DYNAMIC_TOPIC_CONFIG",
            "readOnly": false,
            "sensitive": false
        })
    );
    assert_eq!(row(&configs, "name", "cleanup.policy")["value"], "delete");
    assert_eq!(
        row(&configs, "name", "cleanup.policy")["source"],
        "DEFAULT_CONFIG"
    );
}

#[tokio::test]
async fn a_config_value_kafka_refuses_carries_the_broker_reason() {
    let kafka = Kafka::start().await;
    kafka.topic("audit", 1).await;
    let klens = Klens::over(&kafka).await;
    let path = format!("{TOPICS}/audit/configs");
    klens.eventually(&path, |configs| configs.is_array()).await;

    let (status, body) = klens
        .patch(&path, &json!({ "set": { "retention.ms": "soon" } }))
        .await;

    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert_eq!(body["code"], "REFUSED");
    assert!(
        body["error"].as_str().is_some_and(
            |error| error.contains("Invalid value soon for configuration retention.ms")
        ),
        "{body}"
    );
}

#[tokio::test]
async fn added_partitions_show_once_the_add_answers() {
    let kafka = Kafka::start().await;
    kafka.topic("orders", 1).await;
    let klens = Klens::over(&kafka).await;
    let path = format!("{TOPICS}/orders");
    klens.eventually(&path, |topic| topic.is_object()).await;

    let (status, body) = klens
        .post(&format!("{path}/partitions"), &json!({ "count": 3 }))
        .await;

    assert_eq!(status, StatusCode::NO_CONTENT, "{body}");
    let topic = klens.get(&path).await;
    assert_eq!(topic["partitions"].as_array().map(Vec::len), Some(3));

    let (status, body) = klens
        .post(&format!("{path}/partitions"), &json!({ "count": 2 }))
        .await;

    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert_eq!(body["code"], "REFUSED");
}
