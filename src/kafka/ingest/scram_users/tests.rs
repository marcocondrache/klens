use std::time::Duration;

use tokio::time::advance;
use tracing::Level;

use super::ScramUserLane;
use crate::kafka::model::ScramListing;
use crate::kafka::store::Change;
use crate::testing::{Api, LogCapture, Rig, quiesce, until};

#[tokio::test]
async fn the_scram_user_lane_commits_and_publishes_only_a_changed_listing() {
    let rig = Rig::local();
    let lane = rig.scram_users();
    let mut bus = rig.store.bus.probe();

    rig.poll(&lane).await;
    assert!(matches!(
        rig.store.scram_users.load().as_deref(),
        Some(ScramListing::Described(users)) if users.len() == 2
    ));
    bus.next(Change::scram_users);

    rig.poll(&lane).await;
    assert_eq!(rig.store.scram_users.version(), 1);
    bus.assert_quiet();

    rig.cluster.set_scram_users(ScramListing::Denied);
    rig.poll(&lane).await;
    assert_eq!(
        rig.store.scram_users.load().as_deref(),
        Some(&ScramListing::Denied)
    );
    bus.next(Change::scram_users);
    assert!(rig.store.scram_users.health().healthy());
}

#[tokio::test]
async fn a_failed_scram_user_poll_names_its_lane_and_commits_nothing() {
    let logs = LogCapture::at(Level::WARN);
    let rig = Rig::local();
    rig.cluster.fail(
        Api::ScramUsers,
        "DescribeUserScramCredentials is not supported",
    );

    rig.poll(&rig.scram_users()).await;

    assert!(rig.store.scram_users.load().is_none());
    assert_eq!(
        rig.store.scram_users.health().last_error.as_deref(),
        Some("kafka admin request failed: DescribeUserScramCredentials is not supported")
    );
    logs.assert_contains(r#"lane="scram_users""#);
}

#[tokio::test(start_paused = true)]
async fn the_scram_user_lane_waits_out_its_interval_between_polls() {
    let mut rig = Rig::local();
    rig.spawn(ScramUserLane::with_interval(
        rig.port(),
        Duration::from_secs(60),
    ));
    rig.store.scram_users.committed().await;

    advance(Duration::from_secs(59)).await;
    quiesce().await;
    assert_eq!(rig.cluster.calls(Api::ScramUsers), 1);

    advance(Duration::from_secs(1)).await;
    until("the second poll", || {
        rig.cluster.calls(Api::ScramUsers) == 2
    })
    .await;
}
