use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use serde_json::{Value, json};

use crate::AppState;
use crate::app::auth::access::{ClusterScope, EffectiveAccess, Privilege, PrivilegeSet};
use crate::kafka::model::{CommittedOffset, GroupSnapshot, GroupState};
use crate::kafka::store::fixtures::{group, partition, topic, topology};
use crate::kafka::{FakeCluster, KafkaError};

use super::super::harness::{granted, ok, post, send, viewer, with};

const RESET: &str = "/clusters/local/group-offsets/reset";
const DELETE: &str = "/clusters/local/group-offsets/delete";
const GROUP: &str = "order-processor";
const TOPIC: &str = "orders.created";

fn committed(partition: i32, offset: i64) -> CommittedOffset {
    CommittedOffset {
        topic: TOPIC.into(),
        partition,
        offset,
    }
}

fn idle_group() -> GroupSnapshot {
    GroupSnapshot {
        id: GROUP.into(),
        state: GroupState::Empty,
        protocol: String::new(),
        coordinator: 1,
        members: Vec::new(),
        committed: vec![
            committed(0, 6),
            committed(1, 5),
            CommittedOffset {
                topic: "payments.settled".into(),
                partition: 0,
                offset: 2,
            },
        ],
    }
}

fn writable(group: GroupSnapshot) -> (AppState, FakeCluster) {
    let cluster = FakeCluster::local();
    cluster.put_group(group.clone());
    let state = with(vec![cluster.clone()]).with_writes(
        "local",
        &[Privilege::ResetOffsets, Privilege::DeleteGroupOffsets],
    );
    state
        .cluster("local")
        .expect("local cluster")
        .topology
        .commit(Arc::new(topology(
            vec![topic(
                TOPIC,
                vec![
                    partition(0, vec![1], vec![1]),
                    partition(1, vec![1], vec![1]),
                ],
            )],
            vec![group],
        )));
    (state, cluster)
}

async fn reset(state: &AppState, body: Value) -> (StatusCode, Value) {
    post(state, RESET, body, EffectiveAccess::Unrestricted).await
}

async fn delete(state: &AppState, body: Value) -> (StatusCode, Value) {
    post(state, DELETE, body, EffectiveAccess::Unrestricted).await
}

fn offsets(cluster: &FakeCluster) -> Vec<(String, i32, i64)> {
    cluster
        .committed(GROUP)
        .into_iter()
        .map(|offset| (offset.topic, offset.partition, offset.offset))
        .collect()
}

fn role(privileges: &[Privilege]) -> EffectiveAccess {
    granted(vec![(
        "operator",
        PrivilegeSet::from_privileges(privileges.iter().copied()),
        ClusterScope::All,
    )])
}

#[tokio::test]
async fn a_dry_run_plans_the_reset_and_writes_nothing() {
    let (state, cluster) = writable(idle_group());

    let (status, body) = reset(
        &state,
        json!({ "group": GROUP, "topic": TOPIC, "to": { "kind": "earliest" }, "dryRun": true }),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        body,
        json!({
            "group": GROUP,
            "applied": false,
            "partitions": [
                { "topic": TOPIC, "partition": 0, "current": "6", "target": "0" },
                { "topic": TOPIC, "partition": 1, "current": "5", "target": "0" },
            ],
        })
    );
    assert_eq!(cluster.calls().group_writes(), 0);
    assert_eq!(offsets(&cluster)[0], (TOPIC.into(), 0, 6));
}

#[tokio::test]
async fn a_reset_commits_the_plan() {
    let (state, cluster) = writable(idle_group());

    let (status, body) = reset(
        &state,
        json!({ "group": GROUP, "topic": TOPIC, "partitions": [1], "to": { "kind": "shift", "by": "-2" } }),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["applied"], true);
    assert_eq!(
        body["partitions"],
        json!([{ "topic": TOPIC, "partition": 1, "current": "5", "target": "3" }])
    );
    assert_eq!(
        offsets(&cluster),
        vec![
            (TOPIC.into(), 0, 6),
            (TOPIC.into(), 1, 3),
            ("payments.settled".into(), 0, 2),
        ],
        "only the named partition moved"
    );
}

#[tokio::test]
async fn a_reset_without_a_topic_covers_every_committed_partition() {
    let (state, cluster) = writable(idle_group());
    cluster.commit_offsets(GROUP, vec![committed(0, 6), committed(1, 5)]);

    let (status, body) = reset(
        &state,
        json!({ "group": GROUP, "to": { "kind": "offset", "offset": 7 } }),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        offsets(&cluster),
        vec![(TOPIC.into(), 0, 7), (TOPIC.into(), 1, 7)]
    );
}

#[tokio::test]
async fn a_reset_refuses_a_group_the_store_sees_consuming() {
    let (state, cluster) = writable(group(GROUP, TOPIC, vec![0, 1]));

    let (status, body) = reset(
        &state,
        json!({ "group": GROUP, "topic": TOPIC, "to": { "kind": "latest" } }),
    )
    .await;

    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["code"], "GROUP_NOT_EMPTY");
    assert_eq!(cluster.calls().group_writes(), 0);
}

#[tokio::test]
async fn a_dry_run_still_plans_for_a_consuming_group() {
    let (state, _) = writable(group(GROUP, TOPIC, vec![0, 1]));

    let (status, body) = reset(
        &state,
        json!({ "group": GROUP, "topic": TOPIC, "to": { "kind": "latest" }, "dryRun": true }),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["partitions"][0]["target"], "8");
}

#[tokio::test]
async fn a_consumer_that_joined_after_the_store_looked_is_still_refused() {
    let (state, cluster) = writable(idle_group());
    cluster.reject_writes(|cluster, group| KafkaError::GroupNotEmpty {
        cluster: cluster.to_owned(),
        group: group.to_owned(),
    });

    let (status, body) = reset(
        &state,
        json!({ "group": GROUP, "topic": TOPIC, "to": { "kind": "latest" } }),
    )
    .await;

    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["code"], "GROUP_NOT_EMPTY");
}

#[tokio::test]
async fn writes_stay_off_on_a_cluster_that_did_not_opt_in() {
    let cluster = FakeCluster::local();
    let state = with(vec![cluster.clone()]);

    let (status, body) = reset(
        &state,
        json!({ "group": GROUP, "topic": TOPIC, "to": { "kind": "earliest" }, "dryRun": true }),
    )
    .await;

    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
    assert_eq!(body["code"], "FORBIDDEN");
    assert_eq!(
        body["error"],
        "'resetOffsets' is not permitted on cluster 'local'"
    );
    assert_eq!(cluster.calls().group_writes(), 0);
}

#[tokio::test]
async fn an_opt_in_does_not_open_the_other_write() {
    let (state, cluster) = writable(idle_group());
    let state = state.with_writes("local", &[Privilege::ResetOffsets]);

    let (status, body) = delete(
        &state,
        json!({ "group": GROUP, "topic": TOPIC, "confirm": GROUP }),
    )
    .await;

    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
    assert_eq!(cluster.calls().group_writes(), 0);
}

#[tokio::test]
async fn a_role_needs_the_privilege_even_where_the_cluster_accepts_it() {
    let (state, cluster) = writable(idle_group());
    let body = json!({ "group": GROUP, "topic": TOPIC, "to": { "kind": "earliest" } });

    let (records_only, _) = post(&state, RESET, body.clone(), role(&[Privilege::Records])).await;
    let (viewer, _) = post(
        &state,
        RESET,
        body.clone(),
        granted(vec![viewer(ClusterScope::All)]),
    )
    .await;
    let (resetter, _) = post(&state, RESET, body, role(&[Privilege::ResetOffsets])).await;

    assert_eq!(records_only, StatusCode::FORBIDDEN);
    assert_eq!(viewer, StatusCode::FORBIDDEN);
    assert_eq!(resetter, StatusCode::OK);
    assert_eq!(cluster.calls().group_writes(), 1);
}

#[tokio::test]
async fn an_unknown_group_is_not_created_by_a_reset() {
    let (state, cluster) = writable(idle_group());

    let (status, body) = reset(
        &state,
        json!({ "group": "order-auditor", "topic": TOPIC, "to": { "kind": "earliest" } }),
    )
    .await;

    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["code"], "UNKNOWN_GROUP");
    assert_eq!(cluster.calls().group_writes(), 0);
}

#[tokio::test]
async fn partitions_without_a_topic_are_refused() {
    let (state, _) = writable(idle_group());

    let (status, body) = reset(
        &state,
        json!({ "group": GROUP, "partitions": [0], "to": { "kind": "earliest" } }),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["code"], "INVALID_REQUEST");
}

#[tokio::test]
async fn a_misspelled_field_is_refused_rather_than_ignored() {
    let (state, cluster) = writable(idle_group());

    let (status, body) = reset(
        &state,
        json!({ "group": GROUP, "topic": TOPIC, "to": { "kind": "earliest" }, "dryrun": true }),
    )
    .await;

    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(body["code"], "INVALID_BODY");
    assert!(
        body["error"]
            .as_str()
            .is_some_and(|error| error.contains("unknown field `dryrun`")),
        "{body}"
    );
    assert_eq!(cluster.calls().group_writes(), 0);
}

#[tokio::test]
async fn a_body_that_is_not_json_answers_in_the_api_error_shape() {
    let (state, _) = writable(idle_group());

    let (status, body) = send(
        &state,
        Request::builder()
            .method("POST")
            .uri(RESET)
            .body(Body::from("group=order-processor"))
            .expect("request"),
        EffectiveAccess::Unrestricted,
    )
    .await;

    assert_eq!(status, StatusCode::UNSUPPORTED_MEDIA_TYPE);
    assert_eq!(body["code"], "INVALID_BODY");
}

#[tokio::test]
async fn a_cross_site_request_changes_nothing() {
    let (state, cluster) = writable(idle_group());

    let (status, body) = send(
        &state,
        Request::builder()
            .method("POST")
            .uri(RESET)
            .header("content-type", "application/json")
            .header("sec-fetch-site", "cross-site")
            .body(Body::from(
                json!({ "group": GROUP, "topic": TOPIC, "to": { "kind": "earliest" } }).to_string(),
            ))
            .expect("request"),
        EffectiveAccess::Unrestricted,
    )
    .await;

    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(body["code"], "CROSS_SITE");
    assert_eq!(cluster.calls().group_writes(), 0);
}

#[tokio::test]
async fn deleting_offsets_needs_the_group_id_repeated() {
    let (state, cluster) = writable(idle_group());

    let (status, body) = delete(
        &state,
        json!({ "group": GROUP, "topic": TOPIC, "confirm": "yes" }),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["code"], "CONFIRMATION_REQUIRED");
    assert_eq!(cluster.calls().group_writes(), 0);
}

#[tokio::test]
async fn deleting_offsets_clears_one_topic() {
    let (state, cluster) = writable(idle_group());

    let (status, body) = delete(
        &state,
        json!({ "group": GROUP, "topic": TOPIC, "confirm": GROUP }),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        body,
        json!({ "group": GROUP, "topic": TOPIC, "partitions": [0, 1] })
    );
    assert_eq!(offsets(&cluster), vec![("payments.settled".into(), 0, 2)]);
}

#[tokio::test]
async fn deleting_offsets_refuses_an_empty_partition_list() {
    let (state, cluster) = writable(idle_group());

    let (status, body) = delete(
        &state,
        json!({ "group": GROUP, "topic": TOPIC, "partitions": [], "confirm": GROUP }),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["code"], "INVALID_REQUEST");
    assert_eq!(cluster.calls().group_writes(), 0);
}

#[tokio::test]
async fn deleting_offsets_can_name_partitions() {
    let (state, cluster) = writable(idle_group());

    let (status, body) = delete(
        &state,
        json!({ "group": GROUP, "topic": TOPIC, "partitions": [1, 1], "confirm": GROUP }),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["partitions"], json!([1]));
    assert_eq!(
        offsets(&cluster),
        vec![(TOPIC.into(), 0, 6), ("payments.settled".into(), 0, 2)]
    );
}

#[tokio::test]
async fn whoami_reports_writes_only_where_the_cluster_accepts_them() {
    let state = with(vec![FakeCluster::local(), FakeCluster::named("payments")])
        .with_writes("local", &[Privilege::ResetOffsets]);

    let data = ok(&state, "/whoami").await;

    assert_eq!(
        data["clusters"][0]["privileges"],
        json!(["RECORDS", "CONFIGS", "SCHEMA_TEXT", "ACLS", "RESET_OFFSETS"])
    );
    assert_eq!(
        data["clusters"][1]["privileges"],
        json!(["RECORDS", "CONFIGS", "SCHEMA_TEXT", "ACLS"])
    );
}
