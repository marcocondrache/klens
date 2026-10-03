use e2e::{Kafka, Klens};
use serde_json::Value;

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
