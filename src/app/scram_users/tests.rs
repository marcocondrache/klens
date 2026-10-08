use std::time::Duration;

use axum::http::StatusCode;
use serde_json::{Value, json};
use tokio::time::Instant;
use tracing::Level;

use crate::kafka::model::ScramListing;
use crate::testing::{Api, FakeCluster, LogCapture, Rig, TestApp, quiesce};

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

/// Keep the rig: dropping it stops the lane a write settles on.
async fn writable() -> (TestApp, Rig) {
    let app = TestApp::of([FakeCluster::local()])
        .writable(&["local"])
        .ingested()
        .await;
    let mut rig = app.rig();
    let lane = rig.scram_users();
    rig.spawn(lane);
    quiesce().await;
    (app, rig)
}

async fn credentials_of(app: &TestApp, user: &str) -> Option<Value> {
    app.get(USERS).await.ok()["users"]
        .as_array()
        .expect("users")
        .iter()
        .find(|listed| listed["name"] == user)
        .map(|listed| listed["credentials"].clone())
}

#[tokio::test(start_paused = true)]
async fn a_set_credential_is_listed_before_the_put_answers() {
    let (app, _rig) = writable().await;
    let logs = LogCapture::at(Level::INFO);
    let started = Instant::now();

    app.put(
        "/clusters/local/scram-users/carol",
        &json!({ "mechanism": "SHA512", "password": "s3cret", "iterations": 8192 }),
    )
    .await
    .expect(StatusCode::NO_CONTENT);

    assert!(started.elapsed() < Duration::from_secs(1));
    assert_eq!(
        credentials_of(&app, "carol").await,
        Some(json!([{ "mechanism": "SHA512", "iterations": 8192 }]))
    );
    logs.assert_contains(
        "set scram credential cluster=local scram_user=\"carol\" mechanism=SCRAM-SHA-512 iterations=8192",
    );
    logs.assert_lacks("s3cret");
}

#[tokio::test(start_paused = true)]
async fn a_credential_without_iterations_takes_kafkas_minimum_beside_the_users_others() {
    let (app, _rig) = writable().await;

    app.put(
        "/clusters/local/scram-users/bob",
        &json!({ "mechanism": "SHA256", "password": "s3cret" }),
    )
    .await
    .expect(StatusCode::NO_CONTENT);

    assert_eq!(
        credentials_of(&app, "bob").await,
        Some(json!([
            { "mechanism": "SHA256", "iterations": 4096 },
            { "mechanism": "SHA512", "iterations": 4096 },
        ]))
    );
}

#[tokio::test(start_paused = true)]
async fn a_user_name_may_hold_a_slash() {
    let (app, _rig) = writable().await;

    app.put(
        "/clusters/local/scram-users/team/orders",
        &json!({ "mechanism": "SHA256", "password": "s3cret" }),
    )
    .await
    .expect(StatusCode::NO_CONTENT);

    assert!(credentials_of(&app, "team/orders").await.is_some());
}

#[tokio::test(start_paused = true)]
async fn a_user_name_with_a_newline_stays_on_its_audit_line() {
    let (app, _rig) = writable().await;
    let logs = LogCapture::at(Level::INFO);

    app.put(
        "/clusters/local/scram-users/carol%0Auser=mallory",
        &json!({ "mechanism": "SHA256", "password": "s3cret" }),
    )
    .await
    .expect(StatusCode::NO_CONTENT);

    logs.assert_contains(r#"scram_user="carol\nuser=mallory""#);
    logs.assert_lacks("\nuser=mallory");
}

#[tokio::test(start_paused = true)]
async fn a_deleted_credential_leaves_the_listing_before_the_delete_answers() {
    let (app, _rig) = writable().await;
    let logs = LogCapture::at(Level::INFO);
    let started = Instant::now();

    app.delete("/clusters/local/scram-users/alice?mechanism=SHA256")
        .await
        .expect(StatusCode::NO_CONTENT);

    assert!(started.elapsed() < Duration::from_secs(1));
    assert_eq!(
        credentials_of(&app, "alice").await,
        Some(json!([{ "mechanism": "SHA512", "iterations": 8192 }]))
    );
    logs.assert_contains(
        "deleted scram credential cluster=local scram_user=\"alice\" mechanism=SCRAM-SHA-256",
    );
}

#[tokio::test(start_paused = true)]
async fn a_user_whose_last_credential_is_deleted_leaves_the_listing() {
    let (app, _rig) = writable().await;

    app.delete("/clusters/local/scram-users/bob?mechanism=SHA512")
        .await
        .expect(StatusCode::NO_CONTENT);

    assert_eq!(credentials_of(&app, "bob").await, None);
}

#[tokio::test]
async fn a_credential_kafka_would_refuse_never_reaches_it() {
    let app = TestApp::of([FakeCluster::local()])
        .writable(&["local"])
        .ingested()
        .await;

    for body in [
        json!({ "mechanism": "SHA256", "password": "s3cret", "iterations": 4095 }),
        json!({ "mechanism": "SHA256", "password": "s3cret", "iterations": 16385 }),
        json!({ "mechanism": "SHA256", "password": "" }),
    ] {
        app.put("/clusters/local/scram-users/carol", &body)
            .await
            .assert_error(StatusCode::UNPROCESSABLE_ENTITY, "INVALID_REQUEST");
    }

    assert_eq!(app.cluster().calls(Api::SetScramCredential), 0);
}
