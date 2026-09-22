use axum::http::StatusCode;

use crate::app::auth::access::EffectiveAccess;

use super::super::harness::{failure, ok, ok_as, seeded, viewer_everywhere};

#[tokio::test]
async fn a_schema_body_is_fetched_on_demand_rather_than_kept_in_the_lane() {
    let state = seeded();
    let subject = ok(&state, "/clusters/local/subjects/orders.created-value").await;

    assert_eq!(subject["subject"], "orders.created-value");
    assert_eq!(subject["type"], "AVRO");
    assert!(
        subject["schema"]
            .as_str()
            .expect("schema body")
            .contains("orderId")
    );
}

#[tokio::test]
async fn an_unknown_subject_is_a_typed_error() {
    let (status, code) = failure(
        &seeded(),
        "/clusters/local/subjects/ghost-value",
        EffectiveAccess::Unrestricted,
    )
    .await;

    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(code, "UNKNOWN_SUBJECT");
}

#[tokio::test]
async fn a_subject_body_is_forbidden_without_schema_text() {
    let (status, code) = failure(
        &seeded(),
        "/clusters/local/subjects/orders.created-value",
        viewer_everywhere(),
    )
    .await;

    assert_eq!(
        (status, code.as_str()),
        (StatusCode::FORBIDDEN, "FORBIDDEN")
    );
}

#[tokio::test]
async fn subject_rows_stay_open_to_a_viewer() {
    let subjects = ok_as(&seeded(), "/clusters/local/subjects", viewer_everywhere()).await;

    assert_eq!(subjects["rows"][0]["subject"], "orders.created-value");
}
