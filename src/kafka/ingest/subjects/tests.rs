use std::sync::Arc;

use crate::kafka::store::Change;
use crate::testing::{Api, Rig, subject};

#[tokio::test]
async fn the_subject_lane_stores_the_list_projection_only() {
    let rig = Rig::local();

    rig.poll(&rig.subjects()).await;

    let rows = rig.store.subject_rows();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].subject.as_ref(), "orders.created-value");
    assert_eq!(rows[0].info.versions, vec![1, 2]);
    assert!(
        !rig.store.search("orders.created-value").is_empty(),
        "subjects are searchable"
    );
}

#[tokio::test]
async fn a_new_subject_is_published_as_a_delta() {
    let rig = Rig::local();
    let lane = rig.subjects();
    rig.poll(&lane).await;
    let mut bus = rig.store.bus.probe();

    rig.cluster.set_subjects(vec![
        subject("orders.created-value", 1, 2),
        subject("payments-value", 2, 1),
    ]);
    rig.poll(&lane).await;

    assert_eq!(
        bus.next(Change::subjects).added,
        [Arc::from("payments-value")]
    );
}

#[tokio::test]
async fn a_registry_outage_degrades_only_the_subject_lane() {
    let rig = Rig::local();
    rig.cluster.fail(Api::SchemaSubjects, "registry down");

    rig.poll(&rig.topology()).await;
    rig.poll(&rig.subjects()).await;

    let health = rig.store.health();
    assert!(health.topology.healthy());
    assert!(!health.subjects.healthy());
    assert!(
        rig.store.subjects.load().is_none(),
        "an unavailable source is not an empty listing"
    );
    assert_eq!(rig.store.topic_rows().len(), 1, "the catalog still serves");
}
