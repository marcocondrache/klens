use tracing::Level;

use crate::kafka::model::QuotaListing;
use crate::kafka::store::Change;
use crate::testing::{Api, LogCapture, Rig};

#[tokio::test]
async fn the_quota_lane_commits_and_publishes_only_a_changed_listing() {
    let rig = Rig::local();
    let lane = rig.quotas();
    let mut bus = rig.store.bus.probe();

    rig.poll(&lane).await;
    assert!(matches!(
        rig.store.quotas.load().as_deref(),
        Some(QuotaListing::Described(quotas)) if quotas.len() == 5
    ));
    bus.next(Change::quotas);

    rig.poll(&lane).await;
    assert_eq!(rig.store.quotas.version(), 1);
    bus.assert_quiet();

    rig.cluster.set_quotas(QuotaListing::Denied);
    rig.poll(&lane).await;
    assert_eq!(
        rig.store.quotas.load().as_deref(),
        Some(&QuotaListing::Denied)
    );
    bus.next(Change::quotas);
    assert!(rig.store.quotas.health().healthy());
}

#[tokio::test]
async fn a_failed_quota_poll_names_its_lane_and_commits_nothing() {
    let logs = LogCapture::at(Level::WARN);
    let rig = Rig::local();
    rig.cluster
        .fail(Api::ClientQuotas, "DescribeClientQuotas is not supported");

    rig.poll(&rig.quotas()).await;

    assert!(rig.store.quotas.load().is_none());
    assert_eq!(
        rig.store.quotas.health().last_error.as_deref(),
        Some("kafka admin request failed: DescribeClientQuotas is not supported")
    );
    logs.assert_contains(r#"lane="quotas""#);
}
