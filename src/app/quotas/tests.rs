use axum::http::StatusCode;
use serde_json::json;

use crate::kafka::model::QuotaListing;
use crate::testing::{Api, FakeCluster, TestApp, access, viewer};

#[tokio::test]
async fn quotas_list_every_entity_type_with_its_values() {
    let quotas = TestApp::local()
        .await
        .get("/clusters/local/quotas")
        .await
        .ok();

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
    let quotas = TestApp::local()
        .await
        .get("/clusters/local/quotas")
        .await
        .ok();

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

    let quotas = TestApp::over(cluster)
        .await
        .get("/clusters/local/quotas")
        .await
        .ok();

    assert_eq!(quotas["status"], "DENIED");
    assert_eq!(quotas["quotas"], json!([]));
}

#[tokio::test]
async fn quotas_are_served_from_the_lane_with_its_health() {
    let app = TestApp::local().await;
    app.cluster().set_quotas(QuotaListing::Denied);

    let quotas = app.get("/clusters/local/quotas").await.ok();

    assert_eq!(quotas["status"], "DESCRIBED");
    assert_eq!(quotas["quotas"].as_array().map(Vec::len), Some(5));
    assert!(quotas["sourceHealth"]["updatedAt"].is_string());
    assert_eq!(
        app.cluster().calls(Api::ClientQuotas),
        0,
        "only the lane asks the cluster"
    );
}

#[tokio::test]
async fn quotas_are_empty_and_pending_until_the_lane_commits() {
    let quotas = TestApp::of([FakeCluster::local()])
        .build()
        .get("/clusters/local/quotas")
        .await
        .ok();

    assert_eq!(quotas["status"], "PENDING");
    assert_eq!(quotas["quotas"], json!([]));
    assert!(quotas["sourceHealth"]["updatedAt"].is_null());
    assert!(quotas["sourceHealth"]["lastError"].is_null());
}

#[tokio::test]
async fn quotas_are_forbidden_without_the_configs_privilege() {
    TestApp::local()
        .await
        .with_access(access([viewer()]))
        .get("/clusters/local/quotas")
        .await
        .assert_error(StatusCode::FORBIDDEN, "FORBIDDEN");
}
