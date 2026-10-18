use std::sync::Arc;

use super::SubjectLane;
use crate::kafka::client::testing::Broker;
use crate::kafka::model::SchemaType;
use crate::kafka::registry::testing::{FakeRegistry, Schema};
use crate::kafka::store::Change;
use crate::testing::{Api, IDLE, Rig, subject};

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

#[tokio::test]
async fn a_poll_reads_a_schema_only_for_a_latest_version_it_has_not_seen() {
    let registry = FakeRegistry::start().await;
    registry.register("orders-value", 1, 5, Schema::avro(r#""string""#));
    let broker = Broker::start().await;
    let session = broker.client_with_registry(&registry).await;
    let lane = SubjectLane::with_interval(Arc::new(session), IDLE);
    let rig = Rig::local();

    rig.poll(&lane).await;
    rig.poll(&lane).await;

    assert_eq!(
        registry.hits("/subjects/orders-value/versions/1").await,
        1,
        "a version already in the table is not read again"
    );

    registry.register("orders-value", 2, 6, Schema::json("{}"));
    rig.poll(&lane).await;

    assert_eq!(registry.hits("/subjects/orders-value/versions/2").await, 1);
    let table = rig.store.subjects.load().expect("a subject table");
    let info = table.get("orders-value").expect("the subject");
    assert_eq!(
        (info.latest_version, info.id, info.schema_type),
        (2, 6, SchemaType::Json)
    );
}
