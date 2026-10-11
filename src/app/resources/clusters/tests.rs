use axum::http::StatusCode;

use crate::testing::{FakeCluster, TestApp};

#[tokio::test]
async fn a_cluster_nobody_configured_is_not_found() {
    TestApp::local()
        .await
        .get("/clusters/nope")
        .await
        .assert_error(StatusCode::NOT_FOUND, "UNKNOWN_CLUSTER");
}

#[tokio::test]
async fn cluster_health_reports_per_lane_freshness_and_counts() {
    let clusters = TestApp::local().await.get("/clusters").await.ok();
    let health = &clusters[0];

    assert_eq!(health["cluster"], "local");
    assert_eq!(health["ready"], true);
    assert_eq!(health["topology"]["healthy"], true);
    assert_eq!(health["logDirs"]["healthy"], true);
    health["topology"]["updatedAt"]
        .as_str()
        .expect("updatedAt")
        .parse::<jiff::Timestamp>()
        .expect("RFC 3339");
    assert_eq!(health["topicCount"], 1);
    assert_eq!(health["partitionCount"], 2);
    assert_eq!(health["groupCount"], 1);
    assert_eq!(health["brokerCount"], 1);
    assert_eq!(health["subjectCount"], 1);
    assert_eq!(health["underReplicatedPartitions"], 0);
}

#[tokio::test]
async fn a_cluster_with_no_commits_yet_is_visible_but_not_ready() {
    let app = TestApp::of([FakeCluster::local()]).build();
    let clusters = app.get("/clusters").await.ok();
    let health = &clusters[0];

    assert_eq!(health["cluster"], "local");
    assert_eq!(health["ready"], false);
    assert_eq!(health["topicCount"], 0);
}
