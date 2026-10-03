use e2e::{Kafka, Klens};
use serde_json::json;

#[tokio::test]
async fn the_updates_stream_announces_a_topic_created_on_the_broker() {
    let kafka = Kafka::start().await;
    let klens = Klens::over(&kafka).await;
    klens
        .eventually("/api/clusters", |clusters| clusters[0]["ready"] == true)
        .await;
    let mut updates = klens.open("/api/clusters/local/updates").await;

    kafka.topic("payments", 2).await;
    let topology = updates
        .find(|event| event.name == "topology" && event.data["addedTopics"] != json!([]))
        .await;

    assert_eq!(topology.data["addedTopics"], json!(["payments"]));
}
