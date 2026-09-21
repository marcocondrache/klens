use axum::http::StatusCode;

use crate::app::auth::access::EffectiveAccess;

use super::super::harness::{admin, failure, granted, ok, only, seeded, state, two_clusters};

#[tokio::test]
async fn an_invisible_cluster_is_not_found_rather_than_forbidden() {
    let state = two_clusters();
    let (status, code) = failure(
        &state,
        "/api/clusters/payments",
        granted(vec![admin(only(&["local"]))]),
    )
    .await;

    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(code, "UNKNOWN_CLUSTER");
}

#[tokio::test]
async fn a_cluster_nobody_configured_is_not_found() {
    let (status, code) = failure(
        &state(),
        "/api/clusters/nope",
        EffectiveAccess::Unrestricted,
    )
    .await;

    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(code, "UNKNOWN_CLUSTER");
}

#[tokio::test]
async fn cluster_health_reports_per_lane_freshness_and_counts() {
    let state = seeded();
    let health = &ok(&state, "/api/clusters").await[0];

    assert_eq!(health["cluster"], "local");
    assert_eq!(health["ready"], true);
    assert_eq!(health["topology"]["healthy"], true);
    health["topology"]["updatedAt"]
        .as_str()
        .expect("updatedAt")
        .parse::<jiff::Timestamp>()
        .expect("RFC 3339");
    assert_eq!(health["topicCount"], 2);
    assert_eq!(health["partitionCount"], 3);
    assert_eq!(health["groupCount"], 1);
    assert_eq!(health["brokerCount"], 1);
    assert_eq!(health["subjectCount"], 1);
    assert_eq!(health["underReplicatedPartitions"], 0);
}

#[tokio::test]
async fn a_cluster_with_no_commits_yet_is_visible_but_not_ready() {
    let health = &ok(&state(), "/api/clusters").await[0];

    assert_eq!(health["cluster"], "local");
    assert_eq!(health["ready"], false);
    assert_eq!(health["topicCount"], 0);
}
