use std::sync::Arc;
use std::time::Duration;

use tokio::time::advance;
use tracing::Level;

use super::SchemaIdLane;
use crate::kafka::ingest::LaneSource;
use crate::kafka::store::Change;
use crate::kafka::store::projections::SubjectVersion;
use crate::testing::{Api, LogCapture, Rig, quiesce, subject, until};

const UNRESOLVED: &str = "schema versions left without an id";

fn versions(rig: &Rig, name: &str) -> Vec<(i32, Option<i32>)> {
    rig.store
        .subject_rows()
        .into_iter()
        .find(|row| &*row.subject == name)
        .expect("a listed subject")
        .versions
        .into_iter()
        .map(|SubjectVersion { version, id }| (version, id))
        .collect()
}

fn bare() -> Rig {
    let rig = Rig::local();
    rig.cluster.set_subjects(Vec::new());
    rig
}

async fn poll(rig: &Rig) {
    rig.poll(&rig.subjects()).await;
    rig.poll(&rig.schema_ids()).await;
}

#[tokio::test]
async fn the_lane_learns_the_id_of_every_version() {
    let logs = LogCapture::at(Level::WARN);
    let rig = bare();
    rig.cluster.put_subject("orders-value", &[5, 6, 7]);

    poll(&rig).await;

    assert_eq!(rig.schema_ids().name(), "schema_ids");
    assert_eq!(
        versions(&rig, "orders-value"),
        [(1, Some(5)), (2, Some(6)), (3, Some(7))]
    );
    assert_eq!(rig.cluster.calls(Api::SchemaVersionIds), 1);
    logs.assert_lacks(UNRESOLVED);
}

#[tokio::test]
async fn a_known_id_is_not_read_again() {
    let rig = bare();
    rig.cluster.put_subject("orders-value", &[5, 6]);
    poll(&rig).await;

    rig.cluster.put_subject("orders-value", &[5, 6, 7]);
    poll(&rig).await;

    assert_eq!(
        versions(&rig, "orders-value"),
        [(1, Some(5)), (2, Some(6)), (3, Some(7))]
    );
    assert_eq!(rig.cluster.calls(Api::SchemaVersionIds), 1);
}

#[tokio::test]
async fn a_latest_version_that_changed_id_has_its_older_ids_read_again() {
    let rig = bare();
    rig.cluster.put_subject("orders-value", &[5, 6, 7]);
    poll(&rig).await;

    rig.cluster.put_subject("orders-value", &[1, 2, 3]);
    poll(&rig).await;

    assert_eq!(
        versions(&rig, "orders-value"),
        [(1, Some(1)), (2, Some(2)), (3, Some(3))]
    );
    assert_eq!(rig.cluster.calls(Api::SchemaVersionIds), 2);
}

#[tokio::test]
async fn a_version_the_registry_does_not_answer_for_is_asked_again_next_poll() {
    let logs = LogCapture::at(Level::WARN);
    let rig = bare();
    rig.cluster
        .set_subjects(vec![subject("orders-value", 6, 2)]);
    poll(&rig).await;

    assert_eq!(versions(&rig, "orders-value"), [(1, None), (2, Some(6))]);
    logs.assert_contains(&format!("{UNRESOLVED} cluster=local missing=1"));

    rig.cluster.put_subject("orders-value", &[5, 6]);
    poll(&rig).await;

    assert_eq!(versions(&rig, "orders-value"), [(1, Some(5)), (2, Some(6))]);
}

#[tokio::test]
async fn learned_ids_are_published_as_a_subject_change() {
    let rig = bare();
    rig.cluster.put_subject("orders-value", &[5, 6]);
    rig.poll(&rig.subjects()).await;
    let mut bus = rig.store.bus.probe();

    rig.poll(&rig.schema_ids()).await;

    assert_eq!(
        bus.next(Change::subjects).added,
        [Arc::from("orders-value")]
    );
}

#[tokio::test]
async fn a_failed_read_keeps_the_ids_already_held() {
    let rig = bare();
    rig.cluster.put_subject("orders-value", &[5, 6]);
    poll(&rig).await;
    rig.cluster.put_subject("orders-value", &[5, 6, 7, 8]);
    rig.cluster.fail(Api::SchemaVersionIds, "registry down");

    poll(&rig).await;

    assert_eq!(
        versions(&rig, "orders-value"),
        [(1, Some(5)), (2, Some(6)), (3, None), (4, Some(8))]
    );
    assert!(!rig.store.schema_ids.health().healthy());
}

#[tokio::test(start_paused = true)]
async fn a_subjects_commit_wakes_the_lane() {
    let mut rig = Rig::local();
    let subjects = rig.subjects();
    rig.poll(&subjects).await;
    rig.spawn(rig.schema_ids());
    rig.store.schema_ids.committed().await;

    rig.cluster.put_subject("orders-value", &[5, 6]);
    rig.poll(&subjects).await;

    until("the new subject's ids", || {
        rig.store
            .schema_ids
            .load()
            .is_some_and(|table| table.get("orders-value", 1) == Some(5))
    })
    .await;
}

#[tokio::test(start_paused = true)]
async fn the_lane_waits_out_its_interval_between_polls() {
    let mut rig = Rig::local();
    rig.poll(&rig.subjects()).await;
    rig.spawn(SchemaIdLane::with_interval(
        rig.port(),
        Duration::from_secs(60),
    ));
    rig.store.schema_ids.committed().await;

    advance(Duration::from_secs(59)).await;
    quiesce().await;
    assert_eq!(rig.cluster.calls(Api::SchemaVersionIds), 1);

    advance(Duration::from_secs(1)).await;
    until("the second poll", || {
        rig.cluster.calls(Api::SchemaVersionIds) == 2
    })
    .await;
}
