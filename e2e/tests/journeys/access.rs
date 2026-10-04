use e2e::{Kafka, Klens};
use reqwest::StatusCode;
use serde_json::{Value, json};

const ACLS: &str = "/api/clusters/local/acls";
const QUOTAS: &str = "/api/clusters/local/quotas";

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

fn binding(principal: &str, pattern: &str, name: &str) -> Value {
    json!({
        "resourceType": "TOPIC",
        "resourceName": name,
        "patternType": pattern,
        "principal": principal,
        "host": "*",
        "operation": "READ",
        "permission": "ALLOW",
    })
}

#[tokio::test]
async fn acls_created_through_klens_are_listed_and_deleted_again() {
    let kafka = Kafka::with_authorizer().await;
    let klens = Klens::over(&kafka).await;
    let literal = binding("User:alice", "LITERAL", "orders");
    let prefixed = binding("User:alice", "PREFIXED", "orders.");

    let (status, body) = klens
        .post(ACLS, &json!({ "bindings": [literal, prefixed] }))
        .await;

    assert_eq!(status, StatusCode::CREATED, "{body}");
    let acls = klens.get(ACLS).await;
    assert_eq!(acls["status"], "ENABLED");
    let bindings = acls["bindings"].as_array().expect("bindings");
    assert_eq!(bindings.len(), 2, "{acls}");
    assert!(bindings.contains(&literal) && bindings.contains(&prefixed));

    let (status, body) = klens
        .delete(&format!(
            "{ACLS}?resourceType=TOPIC&resourceName=orders.&patternType=PREFIXED&principal=User:alice&host=*&operation=READ&permission=ALLOW"
        ))
        .await;

    assert_eq!(status, StatusCode::NO_CONTENT, "{body}");
    assert_eq!(klens.get(ACLS).await["bindings"], json!([literal]));
}

#[tokio::test]
async fn a_binding_kafka_refuses_carries_the_broker_reason() {
    let kafka = Kafka::with_authorizer().await;
    let klens = Klens::over(&kafka).await;

    let (status, body) = klens
        .post(
            ACLS,
            &json!({ "bindings": [binding("alice", "LITERAL", "orders")] }),
        )
        .await;

    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert_eq!(body["code"], "REFUSED");
    assert!(
        body["error"]
            .as_str()
            .is_some_and(|error| error.contains("alice")),
        "{body}"
    );
}

#[tokio::test]
async fn a_client_quota_set_through_klens_is_listed_and_removed_again() {
    let kafka = Kafka::start().await;
    let klens = Klens::over(&kafka).await;
    let bob = json!([{ "entityType": "USER", "name": "bob" }]);

    let (status, body) = klens
        .put(
            QUOTAS,
            &json!({ "entity": bob, "consumerByteRate": 2048.0, "requestPercentage": 50.0 }),
        )
        .await;

    assert_eq!(status, StatusCode::NO_CONTENT, "{body}");
    assert_eq!(
        klens.get(QUOTAS).await["quotas"],
        json!([{
            "entity": bob,
            "producerByteRate": null,
            "consumerByteRate": 2048.0,
            "requestPercentage": 50.0,
            "controllerMutationRate": null,
            "connectionCreationRate": null,
        }])
    );

    let (status, body) = klens.put(QUOTAS, &json!({ "entity": bob })).await;

    assert_eq!(status, StatusCode::NO_CONTENT, "{body}");
    assert_eq!(klens.get(QUOTAS).await["quotas"], json!([]));
}

#[tokio::test]
async fn a_quota_kafka_refuses_carries_the_broker_reason() {
    let kafka = Kafka::start().await;
    let klens = Klens::over(&kafka).await;

    let (status, body) = klens
        .put(
            QUOTAS,
            &json!({
                "entity": [{ "entityType": "IP", "name": "10.0.0.7" }],
                "producerByteRate": 1024.0,
            }),
        )
        .await;

    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert_eq!(body["code"], "REFUSED");
}
