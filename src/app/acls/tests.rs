use crate::kafka::model::AclListing;
use crate::testing::{Api, FakeCluster, TestApp};

#[tokio::test]
async fn acls_are_served_from_the_store_without_calling_kafka() {
    let app = TestApp::local().await;
    let acls = app.get("/clusters/local/acls").await.ok();

    assert_eq!(acls["status"], "ENABLED");
    assert_eq!(acls["bindings"].as_array().expect("bindings").len(), 3);
    assert_eq!(app.cluster().calls(Api::Acls), 0);
}

#[tokio::test]
async fn acls_are_pending_until_the_lane_first_reads_them() {
    let cluster = FakeCluster::local();
    cluster.fail(Api::Acls, "broker down");
    let app = TestApp::over(cluster).await;

    let acls = app.get("/clusters/local/acls").await.ok();

    assert_eq!(acls["status"], "PENDING");
    assert!(acls["bindings"].as_array().expect("bindings").is_empty());
    assert_eq!(
        acls["sourceHealth"]["lastError"],
        "kafka admin request failed: broker down"
    );
}

#[tokio::test]
async fn a_denied_describe_says_so_instead_of_failing() {
    let cluster = FakeCluster::local();
    cluster.set_acls(AclListing::Denied);
    let app = TestApp::over(cluster).await;

    let acls = app.get("/clusters/local/acls").await.ok();

    assert_eq!(acls["status"], "DENIED");
    assert!(acls["bindings"].as_array().expect("bindings").is_empty());
}
