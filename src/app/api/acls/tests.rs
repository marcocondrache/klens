use axum::http::StatusCode;

use super::super::harness::{failure, ok, seeded, viewer_everywhere};

#[tokio::test]
async fn acls_stay_live_and_carry_the_authorizer_state() {
    let state = seeded();
    let acls = ok(&state, "/api/clusters/local/acls").await;

    assert_eq!(acls["authorizer"], "ENABLED");
    assert!(!acls["bindings"].as_array().expect("bindings").is_empty());
}

#[tokio::test]
async fn acls_are_forbidden_without_the_acls_privilege() {
    let (status, code) = failure(&seeded(), "/api/clusters/local/acls", viewer_everywhere()).await;

    assert_eq!(
        (status, code.as_str()),
        (StatusCode::FORBIDDEN, "FORBIDDEN")
    );
}
