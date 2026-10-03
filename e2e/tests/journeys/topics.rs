use e2e::{Kafka, Klens};
use krafka::admin::NewTopic;
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
