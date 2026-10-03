use tracing::Level;

use crate::kafka::acl::AclListing;
use crate::kafka::store::Change;
use crate::testing::{Api, LogCapture, Rig};

#[tokio::test]
async fn the_acl_lane_stores_the_listing_and_publishes_each_change() {
    let rig = Rig::local();
    let lane = rig.acls();
    let mut bus = rig.store.bus.probe();

    rig.poll(&lane).await;
    assert!(matches!(
        rig.store.acls.load().as_deref(),
        Some(AclListing::Enabled(rows)) if rows.len() == 3
    ));
    bus.next(Change::acls);

    rig.poll(&lane).await;
    assert_eq!(
        rig.store.acls.version(),
        1,
        "an unchanged listing is not a commit"
    );
    bus.assert_quiet();

    rig.cluster.set_acls(AclListing::Disabled);
    rig.poll(&lane).await;
    assert_eq!(rig.store.acls.version(), 2);
    bus.next(Change::acls);
}

#[tokio::test]
async fn a_failed_acl_poll_names_its_lane_and_commits_nothing() {
    let logs = LogCapture::at(Level::WARN);
    let rig = Rig::local();
    rig.cluster.fail(Api::Acls, "broker down");

    rig.poll(&rig.acls()).await;

    assert!(rig.store.acls.load().is_none());
    assert_eq!(
        rig.store.acls.health().last_error.as_deref(),
        Some("kafka admin request failed: broker down")
    );
    logs.assert_contains(r#"lane="acls""#);
}
