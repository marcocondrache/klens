use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use serde_json::{Value, json};

use crate::app::auth::SessionGuard;
use crate::app::auth::access::{EffectiveAccess, PrivilegeSet};
use crate::kafka::store::fixtures::{
    at, offline_partition, partition, topic, topology, watermarks,
};
use crate::kafka::{ClusterSession, FakeCluster};

use super::super::harness::{
    failure, granted, ok, ok_as, post, seeded, seeded_with, send, state, viewer_everywhere,
    writable,
};

#[tokio::test]
async fn sixty_four_bit_counters_cross_the_wire_as_strings() {
    let state = state();
    let store = state.cluster("local").expect("local cluster");
    let huge = 9_007_199_254_740_993_i64;

    store.topology.commit(Arc::new(topology(
        vec![topic("wide", vec![partition(0, vec![1], vec![1])])],
        Vec::new(),
    )));
    store
        .watermarks
        .commit(Arc::new(watermarks(at(1_000), &[("wide", 0, 0, huge)])));

    let data = ok(&state, "/clusters/local/topics").await;
    let row = &data["rows"][0];

    assert_eq!(row["retainedMessages"], huge.to_string());
    assert_eq!(row["producedTotal"], huge.to_string());
}

#[tokio::test]
async fn topic_rows_project_counts_and_configs_without_touching_the_broker() {
    let (state, session) = seeded_with(FakeCluster::local());
    let data = ok(&state, "/clusters/local/topics?sort=NAME").await;
    let rows = &data["rows"];

    assert_eq!(data["total"], 2);
    assert_eq!(rows[0]["name"], "orders.created");
    assert_eq!(rows[0]["partitionCount"], 2);
    assert_eq!(rows[0]["retainedMessages"], "150");
    assert_eq!(rows[0]["cleanupPolicy"], "COMPACT");
    assert_eq!(rows[0]["retentionMs"], "604800000");
    assert_eq!(rows[0]["groupCount"], 1);
    assert_eq!(session.calls().metadata(), 0);
}

#[tokio::test]
async fn topic_rows_expose_the_latest_rate() {
    let state = seeded();
    let store = state.cluster("local").expect("local cluster");
    let topic: Arc<str> = Arc::from("orders.created");
    store.rates.set(&topic, 12.5);
    store.rates.set(&topic, 13.5);

    let data = ok(&state, "/clusters/local/topics?sort=NAME").await;
    let rows = &data["rows"];

    assert_eq!(rows[0]["name"], "orders.created");
    assert_eq!(rows[0]["rate"], 13.5);
    assert_eq!(rows[1]["name"], "payments.settled");
    assert_eq!(rows[1]["rate"], 0.0);
}

#[tokio::test]
async fn topic_rows_filter_by_name_before_paging() {
    let state = seeded();
    let data = ok(&state, "/clusters/local/topics?contains=PAY").await;

    assert_eq!(data["total"], 1);
    assert_eq!(data["rows"][0]["name"], "payments.settled");
}

#[tokio::test]
async fn topic_rows_page_by_key_and_report_the_unpaged_total() {
    let state = seeded();
    let first = ok(&state, "/clusters/local/topics?limit=1").await;

    assert_eq!(first["total"], 2);
    assert_eq!(first["rows"][0]["name"], "orders.created");
    assert_eq!(first["nextCursor"], "orders.created");

    let second = ok(
        &state,
        "/clusters/local/topics?limit=1&after=orders.created",
    )
    .await;

    assert_eq!(second["rows"][0]["name"], "payments.settled");
    assert_eq!(second["nextCursor"], Value::Null);
}

#[tokio::test]
async fn topic_rows_sort_descending_on_the_requested_column() {
    let state = seeded();
    let data = ok(
        &state,
        "/clusters/local/topics?sort=RETAINED_MESSAGES&desc=true",
    )
    .await;

    assert_eq!(data["rows"][0]["name"], "orders.created");
    assert_eq!(data["rows"][1]["name"], "payments.settled");
}

#[tokio::test]
async fn topic_detail_flags_under_replication_per_partition() {
    let state = state();
    let store = state.cluster("local").expect("local cluster");
    store.topology.commit(Arc::new(topology(
        vec![topic(
            "orders.created",
            vec![
                partition(0, vec![1, 2], vec![1, 2]),
                offline_partition(1, vec![1, 2]),
            ],
        )],
        Vec::new(),
    )));

    let topic = ok(&state, "/clusters/local/topics/orders.created").await;

    assert_eq!(topic["replicationFactor"], 2);
    assert_eq!(topic["underReplicated"], true);
    assert_eq!(topic["rate"], 0.0);
    assert_eq!(topic["retentionMs"], "0");
    assert_eq!(topic["cleanupPolicy"], "DELETE");
    assert_eq!(topic["partitions"][0]["underReplicated"], false);
    assert_eq!(topic["partitions"][1]["underReplicated"], true);
    assert_eq!(topic["partitions"][1]["leader"], -1);
}

#[tokio::test]
async fn a_missing_topic_is_not_found() {
    let (status, code) = failure(
        &seeded(),
        "/clusters/local/topics/ghost",
        EffectiveAccess::Unrestricted,
    )
    .await;

    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(code, "UNKNOWN_TOPIC");
}

#[tokio::test]
async fn a_malformed_topic_query_is_an_invalid_request() {
    let (status, code) = failure(
        &seeded(),
        "/clusters/local/topics?limit=many",
        EffectiveAccess::Unrestricted,
    )
    .await;

    assert_eq!(
        (status, code.as_str()),
        (StatusCode::BAD_REQUEST, "INVALID_REQUEST")
    );
}

#[tokio::test]
async fn topic_groups_report_lag_on_that_topic_alone() {
    let state = seeded();
    let data = ok(&state, "/clusters/local/topics/orders.created/groups").await;

    assert_eq!(data[0]["id"], "order-processor");
    assert_eq!(data[0]["lagOnTopic"], "15");
}

#[tokio::test]
async fn topic_configs_come_from_the_lane_not_the_broker() {
    let (state, session) = seeded_with(FakeCluster::local());
    let configs = ok(&state, "/clusters/local/topics/orders.created/configs").await;

    assert_eq!(configs[0]["name"], "cleanup.policy");
    assert_eq!(configs[0]["value"], "compact");
    assert_eq!(configs[0]["source"], "DYNAMIC_TOPIC_CONFIG");
    assert_eq!(session.calls().topic_configs(), 0);
}

#[tokio::test]
async fn topic_configs_for_an_unknown_topic_are_an_error() {
    let (status, code) = failure(
        &seeded(),
        "/clusters/local/topics/ghost/configs",
        EffectiveAccess::Unrestricted,
    )
    .await;

    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(code, "UNKNOWN_TOPIC");
}

#[tokio::test]
async fn topic_configs_are_forbidden_without_the_configs_privilege() {
    let (status, code) = failure(
        &seeded(),
        "/clusters/local/topics/orders.created/configs",
        viewer_everywhere(),
    )
    .await;

    assert_eq!(
        (status, code.as_str()),
        (StatusCode::FORBIDDEN, "FORBIDDEN")
    );
}

#[tokio::test]
async fn topic_rows_stay_open_to_a_viewer() {
    let topics = ok_as(&seeded(), "/clusters/local/topics", viewer_everywhere()).await;

    assert_eq!(topics["total"], 2);
}

async fn create(
    state: &crate::AppState,
    request: Request<Body>,
    access: EffectiveAccess,
) -> (StatusCode, Value) {
    send(state, request, access, SessionGuard::open()).await
}

async fn broker_topics(session: &FakeCluster) -> Vec<(String, usize)> {
    let mut topics: Vec<_> = session
        .metadata()
        .await
        .expect("metadata")
        .topics
        .into_iter()
        .map(|topic| (topic.name, topic.partitions.len()))
        .collect();
    topics.sort();
    topics
}

#[tokio::test]
async fn creating_a_topic_reaches_the_broker_and_wakes_the_topology_lane() {
    let (state, session) = writable();
    let body = json!({
        "name": "orders.refunded",
        "partitions": 3,
        "replicationFactor": 1,
        "configs": { "retention.ms": "86400000" },
    });

    let (status, json) = create(
        &state,
        post("/clusters/local/topics", body),
        EffectiveAccess::Unrestricted,
    )
    .await;

    assert_eq!((status, json), (StatusCode::CREATED, Value::Null));
    assert_eq!(
        broker_topics(&session).await,
        vec![
            ("orders.created".to_owned(), 2),
            ("orders.refunded".to_owned(), 3)
        ]
    );
    assert_eq!(
        session.topic_configs(&["orders.refunded"]).await.unwrap()["orders.refunded"][0].value,
        Some("86400000".to_owned())
    );
    let lane = &state.cluster("local").unwrap().topology;
    assert!(
        tokio::time::timeout(Duration::from_secs(1), lane.wait(Duration::from_secs(3600)))
            .await
            .is_ok(),
        "the topology lane was kicked"
    );
}

#[tokio::test]
async fn a_read_only_cluster_refuses_topic_creation_even_for_an_unrestricted_session() {
    let (state, session) = seeded_with(FakeCluster::local());

    let (status, json) = create(
        &state,
        post(
            "/clusters/local/topics",
            json!({ "name": "orders.refunded" }),
        ),
        EffectiveAccess::Unrestricted,
    )
    .await;

    assert_eq!(
        (status, json["code"].as_str()),
        (StatusCode::FORBIDDEN, Some("FORBIDDEN"))
    );
    assert_eq!(
        broker_topics(&session).await,
        vec![("orders.created".to_owned(), 2)]
    );
}

#[tokio::test]
async fn topic_creation_needs_the_manage_topics_privilege() {
    let (state, session) = writable();
    let reader = granted(vec![(
        "reader",
        PrivilegeSet::READ,
        crate::app::auth::access::ClusterScope::All,
    )]);

    let (status, json) = create(
        &state,
        post(
            "/clusters/local/topics",
            json!({ "name": "orders.refunded" }),
        ),
        reader,
    )
    .await;

    assert_eq!(
        (status, json["code"].as_str()),
        (StatusCode::FORBIDDEN, Some("FORBIDDEN"))
    );
    assert_eq!(
        broker_topics(&session).await,
        vec![("orders.created".to_owned(), 2)]
    );
}

#[tokio::test]
async fn an_existing_topic_comes_back_as_the_broker_refusal() {
    let (state, _) = writable();

    let (status, json) = create(
        &state,
        post(
            "/clusters/local/topics",
            json!({ "name": "orders.created" }),
        ),
        EffectiveAccess::Unrestricted,
    )
    .await;

    assert_eq!(
        (status, json),
        (
            StatusCode::UNPROCESSABLE_ENTITY,
            json!({
                "code": "REJECTED",
                "error": "kafka rejected the change: Topic 'orders.created' already exists.",
            })
        )
    );
}

#[tokio::test]
async fn a_topic_create_request_is_parsed_before_the_broker_sees_it() {
    let (state, session) = writable();
    let cases = [
        (
            json!({ "name": "orders created" }),
            StatusCode::BAD_REQUEST,
            "INVALID_TOPIC_NAME",
        ),
        (
            json!({ "name": "orders.refunded", "partitions": 0 }),
            StatusCode::UNPROCESSABLE_ENTITY,
            "INVALID_REQUEST",
        ),
        (
            json!({ "name": "orders.refunded", "replicationFactor": 256 }),
            StatusCode::UNPROCESSABLE_ENTITY,
            "INVALID_REQUEST",
        ),
        (
            json!({ "name": "orders.refunded", "replicas": 3 }),
            StatusCode::UNPROCESSABLE_ENTITY,
            "INVALID_REQUEST",
        ),
    ];

    for (body, expected_status, expected_code) in cases {
        let (status, json) = create(
            &state,
            post("/clusters/local/topics", body.clone()),
            EffectiveAccess::Unrestricted,
        )
        .await;
        assert_eq!(
            (status, json["code"].as_str()),
            (expected_status, Some(expected_code)),
            "{body}"
        );
    }

    let without_content_type = Request::builder()
        .method("POST")
        .uri("/clusters/local/topics")
        .body(Body::from(json!({ "name": "orders.refunded" }).to_string()))
        .unwrap();
    let (status, json) = create(&state, without_content_type, EffectiveAccess::Unrestricted).await;
    assert_eq!(
        (status, json["code"].as_str()),
        (StatusCode::UNSUPPORTED_MEDIA_TYPE, Some("INVALID_REQUEST"))
    );

    assert_eq!(
        broker_topics(&session).await,
        vec![("orders.created".to_owned(), 2)]
    );
}

#[tokio::test]
async fn a_cross_site_write_is_refused_but_a_cross_site_read_is_not() {
    let (state, session) = writable();
    let mut request = post(
        "/clusters/local/topics",
        json!({ "name": "orders.refunded" }),
    );
    request
        .headers_mut()
        .insert("sec-fetch-site", "cross-site".parse().unwrap());

    let (status, json) = create(&state, request, EffectiveAccess::Unrestricted).await;

    assert_eq!(
        (status, json["code"].as_str()),
        (StatusCode::FORBIDDEN, Some("CROSS_ORIGIN"))
    );
    assert_eq!(
        broker_topics(&session).await,
        vec![("orders.created".to_owned(), 2)]
    );

    let read = Request::builder()
        .uri("/clusters/local/topics")
        .header("sec-fetch-site", "cross-site")
        .body(Body::empty())
        .unwrap();
    let (status, _) = create(&state, read, EffectiveAccess::Unrestricted).await;
    assert_eq!(status, StatusCode::OK);
}
