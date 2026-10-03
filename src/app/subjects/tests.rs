use axum::http::StatusCode;

use crate::testing::{Api, TestApp, access, viewer};

#[tokio::test]
async fn a_schema_body_is_fetched_on_demand_rather_than_kept_in_the_lane() {
    let app = TestApp::local().await;
    let subject = app
        .get("/clusters/local/subjects/orders.created-value")
        .await
        .ok();

    assert_eq!(subject["subject"], "orders.created-value");
    assert_eq!(
        subject["version"], 2,
        "no version asked resolves to the latest"
    );
    assert_eq!(subject["type"], "AVRO");
    assert!(
        subject["schema"]
            .as_str()
            .expect("schema body")
            .contains("orderId")
    );
    assert_eq!(app.cluster().calls(Api::SubjectSchema), 1);
}

#[tokio::test]
async fn an_unknown_subject_is_a_typed_error() {
    TestApp::local()
        .await
        .get("/clusters/local/subjects/ghost-value")
        .await
        .assert_error(StatusCode::NOT_FOUND, "UNKNOWN_SUBJECT");
}

#[tokio::test]
async fn a_subject_body_is_forbidden_without_schema_text() {
    TestApp::local()
        .await
        .with_access(access([viewer()]))
        .get("/clusters/local/subjects/orders.created-value")
        .await
        .assert_error(StatusCode::FORBIDDEN, "FORBIDDEN");
}

#[tokio::test]
async fn subject_rows_stay_open_to_a_viewer() {
    let subjects = TestApp::local()
        .await
        .with_access(access([viewer()]))
        .get("/clusters/local/subjects")
        .await
        .ok();

    assert_eq!(subjects["rows"][0]["subject"], "orders.created-value");
}
