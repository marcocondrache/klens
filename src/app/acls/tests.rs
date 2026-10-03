use std::sync::Arc;
use std::time::Duration;

use axum::http::StatusCode;

use crate::kafka::model::AclListing;
use crate::testing::{Api, FakeCluster};

use super::super::harness::{failure, ok, seeded, seeded_with, state, store_of, viewer_everywhere};

#[tokio::test]
async fn acls_are_served_from_the_store_without_calling_kafka() {
    let (state, session) = seeded_with(FakeCluster::local());
    let acls = ok(&state, "/clusters/local/acls").await;

    assert_eq!(acls["status"], "ENABLED");
    assert_eq!(acls["bindings"].as_array().expect("bindings").len(), 3);
    assert_eq!(session.calls(Api::Acls), 0);
}

#[tokio::test]
async fn acls_are_pending_until_the_lane_first_reads_them() {
    let state = state();
    store_of(&state, "local")
        .acls
        .record_poll(Duration::from_millis(4), Some("broker down".into()));

    let acls = ok(&state, "/clusters/local/acls").await;

    assert_eq!(acls["status"], "PENDING");
    assert!(acls["bindings"].as_array().expect("bindings").is_empty());
    assert_eq!(acls["sourceHealth"]["lastError"], "broker down");
}

#[tokio::test]
async fn a_denied_describe_says_so_instead_of_failing() {
    let state = seeded();
    store_of(&state, "local")
        .acls
        .commit(Arc::new(AclListing::Denied));

    let acls = ok(&state, "/clusters/local/acls").await;

    assert_eq!(acls["status"], "DENIED");
    assert!(acls["bindings"].as_array().expect("bindings").is_empty());
}

#[tokio::test]
async fn acls_are_forbidden_without_the_acls_privilege() {
    let (status, code) = failure(&seeded(), "/clusters/local/acls", viewer_everywhere()).await;

    assert_eq!(
        (status, code.as_str()),
        (StatusCode::FORBIDDEN, "FORBIDDEN")
    );
}
