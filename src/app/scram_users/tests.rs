use serde_json::json;

use crate::kafka::model::ScramListing;
use crate::testing::{Api, FakeCluster, TestApp};

const USERS: &str = "/clusters/local/scram-users";

#[tokio::test]
async fn users_list_each_credential_with_its_mechanism_and_iterations() {
    let users = TestApp::local().await.get(USERS).await.ok();

    assert_eq!(users["status"], "DESCRIBED");
    assert_eq!(
        users["users"],
        json!([
            {
                "name": "alice",
                "credentials": [
                    { "mechanism": "SHA256", "iterations": 4096 },
                    { "mechanism": "SHA512", "iterations": 8192 },
                ],
            },
            {
                "name": "bob",
                "credentials": [{ "mechanism": "SHA512", "iterations": 4096 }],
            },
        ])
    );
}

#[tokio::test]
async fn a_denied_describe_says_so_instead_of_failing() {
    let cluster = FakeCluster::local();
    cluster.set_scram_users(ScramListing::Denied);

    let users = TestApp::over(cluster).await.get(USERS).await.ok();

    assert_eq!(users["status"], "DENIED");
    assert_eq!(users["users"], json!([]));
}

#[tokio::test]
async fn users_are_served_from_the_lane_with_its_health() {
    let app = TestApp::local().await;
    app.cluster().set_scram_users(ScramListing::Denied);

    let users = app.get(USERS).await.ok();

    assert_eq!(users["status"], "DESCRIBED");
    assert!(users["sourceHealth"]["updatedAt"].is_string());
    assert_eq!(
        app.cluster().calls(Api::ScramUsers),
        0,
        "only the lane asks the cluster"
    );
}

#[tokio::test]
async fn users_are_empty_and_pending_until_the_lane_commits() {
    let users = TestApp::of([FakeCluster::local()])
        .build()
        .get(USERS)
        .await
        .ok();

    assert_eq!(users["status"], "PENDING");
    assert_eq!(users["users"], json!([]));
    assert!(users["sourceHealth"]["updatedAt"].is_null());
}
