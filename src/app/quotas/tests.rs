use axum::http::StatusCode;
use serde_json::json;

use crate::kafka::FakeCluster;
use crate::kafka::model::QuotaListing;

use super::super::harness::{failure, ok, viewer_everywhere, with};

#[tokio::test]
async fn quotas_list_every_entity_type_with_its_values() {
    let quotas = ok(&with(vec![FakeCluster::local()]), "/clusters/local/quotas").await;

    assert_eq!(quotas["access"], "ALLOWED");
    assert_eq!(
        quotas["quotas"][0],
        json!({
            "entity": [{ "entityType": "USER", "name": "alice" }],
            "producerByteRate": 1_048_576.0,
            "consumerByteRate": 2_097_152.0,
            "requestPercentage": null,
            "controllerMutationRate": null,
            "connectionCreationRate": null,
        })
    );
    assert_eq!(
        quotas["quotas"][1]["entity"],
        json!([
            { "entityType": "USER", "name": "alice" },
            { "entityType": "CLIENT_ID", "name": "checkout" },
        ])
    );
    assert_eq!(quotas["quotas"][4]["entity"][0]["entityType"], "IP");
    assert_eq!(quotas["quotas"][4]["connectionCreationRate"], 20.0);
}

#[tokio::test]
async fn default_entities_carry_no_name() {
    let quotas = ok(&with(vec![FakeCluster::local()]), "/clusters/local/quotas").await;

    assert_eq!(
        quotas["quotas"][2]["entity"],
        json!([{ "entityType": "USER", "name": null }])
    );
    assert_eq!(quotas["quotas"][2]["requestPercentage"], 50.0);
    assert_eq!(quotas["quotas"][2]["controllerMutationRate"], 10.0);
    assert_eq!(
        quotas["quotas"][3]["entity"],
        json!([{ "entityType": "CLIENT_ID", "name": null }])
    );
}

#[tokio::test]
async fn a_denied_describe_says_so_instead_of_failing() {
    let cluster = FakeCluster::local();
    cluster.set_quotas(QuotaListing::Denied);

    let quotas = ok(&with(vec![cluster]), "/clusters/local/quotas").await;

    assert_eq!(quotas, json!({ "access": "DENIED", "quotas": [] }));
}

#[tokio::test]
async fn quotas_are_forbidden_without_the_configs_privilege() {
    let cluster = FakeCluster::local();
    let state = with(vec![cluster.clone()]);

    let (status, code) = failure(&state, "/clusters/local/quotas", viewer_everywhere()).await;

    assert_eq!(
        (status, code.as_str()),
        (StatusCode::FORBIDDEN, "FORBIDDEN")
    );
    assert_eq!(cluster.calls().quotas(), 0);
}

#[tokio::test]
async fn quotas_are_read_live_and_reused_briefly() {
    let cluster = FakeCluster::local();
    let state = with(vec![cluster.clone()]);

    ok(&state, "/clusters/local/quotas").await;
    ok(&state, "/clusters/local/quotas").await;

    assert_eq!(cluster.calls().quotas(), 1);
}
