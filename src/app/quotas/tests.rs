use std::time::Duration;

use axum::http::StatusCode;
use serde_json::{Value, json};
use tokio::time::Instant;
use tracing::Level;

use crate::kafka::model::QuotaListing;
use crate::testing::{Api, FakeCluster, LogCapture, TestApp, quiesce};

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

async fn writable() -> TestApp {
    TestApp::of([FakeCluster::local()])
        .writable(&["local"])
        .ingested()
        .await
}

fn quota_of(listing: &Value, entity: &Value) -> Option<Value> {
    listing["quotas"]
        .as_array()
        .expect("quotas")
        .iter()
        .find(|quota| quota["entity"] == *entity)
        .cloned()
}

#[tokio::test(start_paused = true)]
async fn a_set_quota_is_listed_before_the_put_answers() {
    let app = writable().await;
    let mut rig = app.rig();
    let lane = rig.quotas();
    rig.spawn(lane);
    quiesce().await;
    let logs = LogCapture::at(Level::INFO);
    let started = Instant::now();
    let entity = json!([
        { "entityType": "CLIENT_ID", "name": "checkout" },
        { "entityType": "USER", "name": null },
    ]);

    app.put(
        "/clusters/local/quotas",
        &json!({ "entity": entity, "producerByteRate": 1024.0, "requestPercentage": 12.5 }),
    )
    .await
    .expect(StatusCode::NO_CONTENT);

    assert!(started.elapsed() < Duration::from_secs(1));
    let listing = app.get("/clusters/local/quotas").await.ok();
    let sorted = json!([
        { "entityType": "USER", "name": null },
        { "entityType": "CLIENT_ID", "name": "checkout" },
    ]);
    assert_eq!(
        quota_of(&listing, &sorted),
        Some(json!({
            "entity": sorted,
            "producerByteRate": 1024.0,
            "consumerByteRate": null,
            "requestPercentage": 12.5,
            "controllerMutationRate": null,
            "connectionCreationRate": null,
        }))
    );
    logs.assert_contains(
        "set client quota cluster=local quota=user=<default> client-id=checkout: producer_byte_rate=1024 request_percentage=12.5",
    );
}

#[tokio::test(start_paused = true)]
async fn a_quota_left_with_no_values_leaves_the_listing_before_the_put_answers() {
    let app = writable().await;
    let mut rig = app.rig();
    let lane = rig.quotas();
    rig.spawn(lane);
    quiesce().await;
    let started = Instant::now();
    let alice = json!([{ "entityType": "USER", "name": "alice" }]);

    app.put("/clusters/local/quotas", &json!({ "entity": alice }))
        .await
        .expect(StatusCode::NO_CONTENT);

    assert!(started.elapsed() < Duration::from_secs(1));
    let listing = app.get("/clusters/local/quotas").await.ok();
    assert_eq!(quota_of(&listing, &alice), None);
}

#[tokio::test]
async fn a_quota_entity_names_each_type_once_and_at_least_one() {
    let app = writable().await;

    for entity in [
        json!([]),
        json!([
            { "entityType": "USER", "name": "alice" },
            { "entityType": "USER", "name": "bob" },
        ]),
    ] {
        app.put(
            "/clusters/local/quotas",
            &json!({ "entity": entity, "producerByteRate": 1.0 }),
        )
        .await
        .assert_error(StatusCode::UNPROCESSABLE_ENTITY, "INVALID_REQUEST");
    }

    assert_eq!(app.cluster().calls(Api::AlterClientQuota), 0);
}
