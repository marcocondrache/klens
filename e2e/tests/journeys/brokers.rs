use e2e::{Kafka, Klens};
use reqwest::StatusCode;
use serde_json::{Value, json};

use crate::row;

const BROKERS: &str = "/api/clusters/local/brokers";

#[tokio::test]
async fn the_broker_reports_its_log_dirs_and_serves_its_configs_live() {
    let kafka = Kafka::start().await;
    kafka.topic("orders", 2).await;
    let klens = Klens::over(&kafka).await;

    let brokers = klens
        .eventually(BROKERS, |brokers| {
            brokers[0]["partitionCount"] == 2 && brokers[0]["logDirs"][0]["replicaCount"] == 2
        })
        .await;
    let broker = &brokers[0];
    let log_dir = &broker["logDirs"][0];

    assert_eq!(broker["id"], 1);
    assert_eq!(log_dir["error"], Value::Null);
    assert!(
        log_dir["path"]
            .as_str()
            .is_some_and(|path| !path.is_empty())
    );
    assert!(log_dir["totalBytes"].as_u64() > Some(0), "{log_dir}");
    assert!(log_dir["usableBytes"].as_u64() > Some(0), "{log_dir}");
    let configs = klens.get(&format!("{BROKERS}/1/configs")).await;
    assert_eq!(row(&configs, "name", "log.retention.hours")["value"], "168");
    assert_eq!(
        row(&configs, "name", "process.roles")["value"],
        "broker,controller"
    );
}

#[tokio::test]
async fn broker_config_edits_read_back_as_soon_as_klens_answers() {
    let kafka = Kafka::start().await;
    let klens = Klens::over(&kafka).await;
    klens
        .eventually(BROKERS, |brokers| brokers[0]["id"] == 1)
        .await;
    let broker = format!("{BROKERS}/1/configs");
    let defaults = format!("{BROKERS}/configs");
    let backoff = async || {
        let configs = klens.get(&broker).await;
        let entry = row(&configs, "name", "log.cleaner.backoff.ms");
        (entry["value"].clone(), entry["source"].clone())
    };

    for (path, edit, value, source) in [
        (
            &broker,
            json!({ "set": { "log.cleaner.backoff.ms": "20000" } }),
            "20000",
            "DYNAMIC_BROKER_CONFIG",
        ),
        (
            &defaults,
            json!({ "set": { "log.cleaner.backoff.ms": "30000" } }),
            "20000",
            "DYNAMIC_BROKER_CONFIG",
        ),
        (
            &broker,
            json!({ "reset": ["log.cleaner.backoff.ms"] }),
            "30000",
            "DYNAMIC_DEFAULT_BROKER_CONFIG",
        ),
        (
            &defaults,
            json!({ "reset": ["log.cleaner.backoff.ms"] }),
            "15000",
            "DEFAULT_CONFIG",
        ),
    ] {
        let (status, body) = klens.patch(path, &edit).await;
        assert_eq!(status, StatusCode::NO_CONTENT, "{edit} answered {body}");
        assert_eq!(
            backoff().await,
            (json!(value), json!(source)),
            "after {edit}"
        );
    }
}

#[tokio::test]
async fn a_broker_config_kafka_cannot_change_live_carries_the_broker_reason() {
    let kafka = Kafka::start().await;
    let klens = Klens::over(&kafka).await;
    klens
        .eventually(BROKERS, |brokers| brokers[0]["id"] == 1)
        .await;

    let (status, body) = klens
        .patch(
            &format!("{BROKERS}/1/configs"),
            &json!({ "set": { "node.id": "2" } }),
        )
        .await;

    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(body["code"], "REFUSED");
    assert_eq!(
        body["error"],
        "kafka refused the change: Cannot update these configs dynamically: Set(node.id)"
    );
}
