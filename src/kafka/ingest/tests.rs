use std::time::Duration;

use tokio::time::advance;

use super::*;
use crate::testing::{Api, FakeCluster, IDLE, Rig, until};

#[tokio::test(start_paused = true)]
async fn downstream_lanes_wait_for_topology_instead_of_committing_nothing() {
    let mut rig = Rig::local();
    rig.spawn(rig.watermarks());
    rig.spawn(rig.configs());

    advance(Duration::from_secs(3_600)).await;

    assert!(
        rig.store.watermarks.load().is_none(),
        "an empty table would read as a cluster with no partitions"
    );
    assert!(rig.store.configs.load().is_none());
    assert!(rig.store.watermarks.health().checked_at.is_none());
    assert_eq!(
        rig.cluster.calls(Api::LowWatermarks) + rig.cluster.calls(Api::HighWatermarks),
        0,
        "and it costs no broker call"
    );
}

#[tokio::test(start_paused = true)]
async fn lanes_waiting_for_topology_start_as_soon_as_it_commits() {
    let mut rig = Rig::local();
    rig.spawn(rig.configs());
    rig.spawn(rig.log_dirs());
    advance(IDLE).await;

    rig.poll(&rig.topology()).await;

    rig.store.configs.committed().await;
    rig.store.log_dirs.committed().await;
}

#[tokio::test(start_paused = true)]
async fn every_lane_runs_per_cluster_and_stops_with_the_ingest() {
    let prod = FakeCluster::named("prod");
    let staging = FakeCluster::named("staging");
    let clusters = Clusters::from_sessions(vec![prod.clone(), staging]);
    let lanes = Ingest::start(&clusters, &IngestTuning::default());

    assert_eq!(lanes.lane_count(), 20, "ten lanes per cluster");
    until("both clusters ready", || clusters.ready()).await;

    drop(lanes);
    tokio::task::yield_now().await;
    let polls = prod.calls(Api::Metadata);
    advance(Duration::from_secs(3_600)).await;
    tokio::task::yield_now().await;

    assert_eq!(
        prod.calls(Api::Metadata),
        polls,
        "dropping the ingest aborts every lane task"
    );
}

#[tokio::test]
async fn ingestion_fills_the_stores_the_api_projects_from() {
    let clusters = Clusters::from_sessions(vec![FakeCluster::local()]);
    let _lanes = Ingest::start(&clusters, &IngestTuning::default());
    let store = &clusters.get("local").unwrap().store;

    until("catalog and subjects", || {
        clusters.ready() && !store.subject_rows().is_empty()
    })
    .await;

    assert_eq!(store.topic_rows()[0].name.as_ref(), "orders.created");
    assert_eq!(
        store.subject_rows()[0].subject.as_ref(),
        "orders.created-value"
    );
}

#[tokio::test(start_paused = true)]
async fn one_cluster_never_wakes_another() {
    let prod = FakeCluster::named("prod");
    let staging = FakeCluster::named("staging");
    let clusters = Clusters::from_sessions(vec![prod.clone(), staging]);
    let _lanes = Ingest::start(&clusters, &IngestTuning::default());
    let prod_store = &clusters.get("prod").unwrap().store;
    let staging_store = &clusters.get("staging").unwrap().store;

    until("both ready", || prod_store.ready() && staging_store.ready()).await;
    let mut staging_bus = staging_store.bus.probe();
    staging_bus.drain();

    prod.add_partition("orders.created", 2);
    prod_store.topology.kick();
    prod_store.topology.reaches(2).await;

    staging_bus.assert_quiet();
}
