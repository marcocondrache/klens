use e2e::{Kafka, Klens};
use serde_json::json;

#[tokio::test]
async fn acls_on_a_broker_without_an_authorizer_are_disabled_not_failing() {
    let kafka = Kafka::start().await;
    let klens = Klens::over(&kafka).await;

    let acls = klens
        .eventually("/api/clusters/local/acls", |acls| {
            acls["status"] != "PENDING"
        })
        .await;

    assert_eq!(acls["status"], "DISABLED");
    assert_eq!(acls["bindings"], json!([]));
}

#[tokio::test]
async fn a_client_quota_set_on_the_broker_is_listed() {
    let kafka = Kafka::start().await;
    kafka
        .set_user_quota("alice", "producer_byte_rate", 1_048_576.0)
        .await;
    let klens = Klens::over(&kafka).await;

    let quotas = klens
        .eventually("/api/clusters/local/quotas", |quotas| {
            quotas["quotas"]
                .as_array()
                .is_some_and(|quotas| !quotas.is_empty())
        })
        .await;

    assert_eq!(quotas["status"], "DESCRIBED");
    assert_eq!(
        quotas["quotas"],
        json!([{
            "entity": [{ "entityType": "USER", "name": "alice" }],
            "producerByteRate": 1_048_576.0,
            "consumerByteRate": null,
            "requestPercentage": null,
            "controllerMutationRate": null,
            "connectionCreationRate": null,
        }])
    );
}
