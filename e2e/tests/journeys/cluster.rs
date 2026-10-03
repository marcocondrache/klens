use e2e::{Kafka, Klens};
use reqwest::StatusCode;

#[tokio::test]
async fn klens_turns_ready_once_it_has_read_the_cluster() {
    let kafka = Kafka::start().await;
    let klens = Klens::over(&kafka).await;

    assert_eq!(klens.status("/health").await, StatusCode::NO_CONTENT);
    let clusters = klens
        .eventually("/api/clusters", |clusters| clusters[0]["ready"] == true)
        .await;

    assert_eq!(klens.status("/ready").await, StatusCode::NO_CONTENT);
    assert_eq!(clusters[0]["cluster"], "local");
    assert_eq!(clusters[0]["brokerCount"], 1);
}
