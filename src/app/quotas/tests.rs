use std::sync::Arc;

use axum::http::StatusCode;
use serde_json::json;

use crate::app::AppState;
use crate::kafka::model::QuotaListing;
use crate::kafka::{ClusterSession, FakeCluster};

use super::super::harness::{failure, ok, store_of, viewer_everywhere, with};

async fn polled(cluster: &FakeCluster) -> AppState {
    let state = with(vec![cluster.clone()]);
    let listing = cluster.client_quotas().await.expect("fake quotas");
    store_of(&state, "local").quotas.commit(Arc::new(listing));
    state
}

#[tokio::test]
async fn quotas_list_every_entity_type_with_its_values() {
    let quotas = ok(
        &polled(&FakeCluster::local()).await,
        "/clusters/local/quotas",
    )
    .await;

    assert_eq!(quotas["status"], "DESCRIBED");
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
    let quotas = ok(
        &polled(&FakeCluster::local()).await,
        "/clusters/local/quotas",
    )
    .await;

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
    cluster.set_quotas(Ok(QuotaListing::Denied));

    let quotas = ok(&polled(&cluster).await, "/clusters/local/quotas").await;

    assert_eq!(quotas["status"], "DENIED");
    assert_eq!(quotas["quotas"], json!([]));
}

#[tokio::test]
async fn quotas_are_served_from_the_lane_with_its_health() {
    let cluster = FakeCluster::local();
    let state = polled(&cluster).await;
    cluster.set_quotas(Ok(QuotaListing::Denied));

    let quotas = ok(&state, "/clusters/local/quotas").await;

    assert_eq!(quotas["status"], "DESCRIBED");
    assert_eq!(quotas["quotas"].as_array().map(Vec::len), Some(5));
    assert!(quotas["sourceHealth"]["updatedAt"].is_string());
    assert_eq!(
        cluster.calls().quotas(),
        1,
        "only the lane asks the cluster"
    );
}

#[tokio::test]
async fn quotas_are_empty_and_pending_until_the_lane_commits() {
    let quotas = ok(&with(vec![FakeCluster::local()]), "/clusters/local/quotas").await;

    assert_eq!(quotas["status"], "PENDING");
    assert_eq!(quotas["quotas"], json!([]));
    assert!(quotas["sourceHealth"]["updatedAt"].is_null());
    assert!(quotas["sourceHealth"]["lastError"].is_null());
}

#[tokio::test]
async fn quotas_are_forbidden_without_the_configs_privilege() {
    let state = polled(&FakeCluster::local()).await;

    let (status, code) = failure(&state, "/clusters/local/quotas", viewer_everywhere()).await;

    assert_eq!(
        (status, code.as_str()),
        (StatusCode::FORBIDDEN, "FORBIDDEN")
    );
}
