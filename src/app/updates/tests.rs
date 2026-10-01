use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use axum::response::Response;
use futures::StreamExt as _;
use serde_json::Value;
use tower::ServiceExt as _;

use crate::app::auth::SessionGuard;
use crate::app::auth::access::EffectiveAccess;
use crate::kafka::store::bus::BUS_CAPACITY;
use crate::kafka::store::{
    Change, ConfigsDelta, GroupLagUpdate, GroupOffsetsWave, LogDirsDelta, SubjectsDelta, TopicRate,
    TopologyDelta, WatermarksTick,
};

use super::super::harness::{
    failure, granted, only, open_stream, read_frames, seed, seeded, store_of, two_clusters, viewer,
};

async fn read_events(response: Response, count: usize) -> Vec<Value> {
    read_frames(response, count)
        .await
        .into_iter()
        .map(|(_, data)| data)
        .collect()
}

fn tick(topics: &[(&str, f64)]) -> Change {
    Change::Watermarks(Arc::new(WatermarksTick {
        rates: topics
            .iter()
            .map(|(topic, rate)| TopicRate {
                topic: Arc::from(*topic),
                rate: *rate,
            })
            .collect(),
    }))
}

fn wave(groups: &[(&str, i64)]) -> Change {
    Change::GroupOffsets(Arc::new(GroupOffsetsWave {
        groups: groups
            .iter()
            .map(|(group, lag)| GroupLagUpdate {
                group: Arc::from(*group),
                total_lag: *lag,
                lag_complete: true,
                offsets: Vec::new(),
            })
            .collect(),
    }))
}

#[tokio::test]
async fn an_unscoped_subscriber_gets_the_whole_cluster_firehose() {
    let state = seeded();
    let store = Arc::clone(store_of(&state, "local"));
    let response = open_stream(
        &state,
        "/clusters/local/updates",
        EffectiveAccess::Unrestricted,
        SessionGuard::open(),
    )
    .await;
    store
        .bus
        .publish(tick(&[("orders.created", 10.0), ("payments.settled", 2.0)]));
    let events = read_events(response, 1).await;

    assert_eq!(events[0]["type"], "watermarks");
    assert_eq!(
        events[0]["topics"],
        serde_json::json!([
            { "topic": "orders.created", "rate": 10.0 },
            { "topic": "payments.settled", "rate": 2.0 }
        ])
    );
}

#[tokio::test]
async fn a_topic_scoped_subscriber_pays_only_for_its_own_topic() {
    let state = seeded();
    let store = Arc::clone(store_of(&state, "local"));
    let response = open_stream(
        &state,
        "/clusters/local/updates?topic=orders.created",
        EffectiveAccess::Unrestricted,
        SessionGuard::open(),
    )
    .await;
    store
        .bus
        .publish(tick(&[("orders.created", 10.0), ("payments.settled", 2.0)]));
    let events = read_events(response, 1).await;

    assert_eq!(
        events[0]["topics"],
        serde_json::json!([{ "topic": "orders.created", "rate": 10.0 }])
    );
}

#[tokio::test]
async fn an_event_outside_the_scope_never_reaches_the_socket() {
    let state = seeded();
    let store = Arc::clone(store_of(&state, "local"));
    let response = open_stream(
        &state,
        "/clusters/local/updates?topic=payments.settled",
        EffectiveAccess::Unrestricted,
        SessionGuard::open(),
    )
    .await;
    store.bus.publish(Change::Configs(Arc::new(ConfigsDelta {
        topics: vec![Arc::from("orders.created")],
    })));
    store.bus.publish(Change::Subjects(Arc::new(SubjectsDelta {
        added: vec![Arc::from("payments.settled-value")],
        removed: Vec::new(),
        changed: Vec::new(),
    })));
    let events = read_events(response, 1).await;

    assert_eq!(events[0]["type"], "subjects");
}

fn log_dirs_moved(topics: &[&str]) -> Change {
    Change::LogDirs(Arc::new(LogDirsDelta {
        topics: topics.iter().map(|topic| Arc::from(*topic)).collect(),
        brokers_changed: true,
    }))
}

#[tokio::test]
async fn an_unscoped_subscriber_hears_every_size_that_moved() {
    let state = seeded();
    let store = Arc::clone(store_of(&state, "local"));
    let response = open_stream(
        &state,
        "/clusters/local/updates",
        EffectiveAccess::Unrestricted,
        SessionGuard::open(),
    )
    .await;
    store
        .bus
        .publish(log_dirs_moved(&["orders.created", "payments.settled"]));
    let frames = read_frames(response, 1).await;

    assert_eq!(frames[0].0, "logDirs");
    assert_eq!(
        frames[0].1,
        serde_json::json!({
            "type": "logDirs",
            "topics": ["orders.created", "payments.settled"],
            "brokersChanged": true
        })
    );
}

#[tokio::test]
async fn an_acl_change_tells_the_client_to_refetch() {
    let state = seeded();
    let store = Arc::clone(store_of(&state, "local"));
    let response = open_stream(
        &state,
        "/clusters/local/updates",
        EffectiveAccess::Unrestricted,
        SessionGuard::open(),
    )
    .await;
    store.bus.publish(Change::Acls);
    let frames = read_frames(response, 1).await;

    assert_eq!(frames[0].0, "acls");
    assert_eq!(frames[0].1, serde_json::json!({ "type": "acls" }));
}

#[tokio::test]
async fn a_topic_scoped_subscriber_hears_only_its_own_size() {
    let state = seeded();
    let store = Arc::clone(store_of(&state, "local"));
    let response = open_stream(
        &state,
        "/clusters/local/updates?topic=orders.created",
        EffectiveAccess::Unrestricted,
        SessionGuard::open(),
    )
    .await;
    store.bus.publish(log_dirs_moved(&["payments.settled"]));
    store.bus.publish(Change::Subjects(Arc::new(SubjectsDelta {
        added: Vec::new(),
        removed: Vec::new(),
        changed: Vec::new(),
    })));
    store
        .bus
        .publish(log_dirs_moved(&["orders.created", "payments.settled"]));
    let events = read_events(response, 2).await;

    assert_eq!(
        events[0]["type"], "subjects",
        "another topic's size stays out"
    );
    assert_eq!(
        events[1],
        serde_json::json!({
            "type": "logDirs",
            "topics": ["orders.created"],
            "brokersChanged": false
        })
    );
}

#[tokio::test]
async fn a_quota_change_is_sent_as_a_quotas_event() {
    let state = seeded();
    let store = Arc::clone(store_of(&state, "local"));
    let response = open_stream(
        &state,
        "/clusters/local/updates",
        EffectiveAccess::Unrestricted,
        SessionGuard::open(),
    )
    .await;
    store.bus.publish(Change::Quotas);
    let frames = read_frames(response, 1).await;

    assert_eq!(frames[0].0, "quotas");
    assert_eq!(frames[0].1, serde_json::json!({ "type": "quotas" }));
}

#[tokio::test]
async fn an_unscoped_lag_wave_fans_out_one_update_per_group_without_offsets() {
    let state = seeded();
    let store = Arc::clone(store_of(&state, "local"));
    let response = open_stream(
        &state,
        "/clusters/local/updates",
        EffectiveAccess::Unrestricted,
        SessionGuard::open(),
    )
    .await;
    store
        .bus
        .publish(wave(&[("order-processor", 15), ("audit", 3)]));
    let events = read_events(response, 2).await;

    assert_eq!(events[0]["type"], "groupLag");
    assert_eq!(events[0]["group"], "order-processor");
    assert_eq!(events[0]["lag"], 15);
    assert_eq!(events[0]["offsets"], serde_json::json!([]));
    assert_eq!(events[1]["group"], "audit");
    assert_eq!(events[1]["lag"], 3);
    assert_eq!(events[1]["offsets"], serde_json::json!([]));
}

#[tokio::test]
async fn a_group_scoped_subscriber_holds_an_interest_lease_for_the_stream() {
    let state = seeded();
    let store = Arc::clone(store_of(&state, "local"));
    let response = open_stream(
        &state,
        "/clusters/local/updates?group=order-processor",
        EffectiveAccess::Unrestricted,
        SessionGuard::open(),
    )
    .await;
    store
        .bus
        .publish(wave(&[("order-processor", 15), ("audit", 3)]));
    let mut stream = response.into_body().into_data_stream();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    let chunk = tokio::time::timeout_at(deadline, stream.next())
        .await
        .expect("timeout")
        .expect("frame")
        .expect("bytes");
    let text = String::from_utf8_lossy(&chunk);
    assert!(text.contains("order-processor"), "{text}");
    assert!(store.interest.is_hot("order-processor"));

    drop(stream);
    assert!(!store.interest.is_hot("order-processor"));
}

#[tokio::test]
async fn a_topology_delta_reaches_a_scoped_subscriber_only_when_it_names_its_topic() {
    let state = seeded();
    let store = Arc::clone(store_of(&state, "local"));
    let response = open_stream(
        &state,
        "/clusters/local/updates?topic=orders.created",
        EffectiveAccess::Unrestricted,
        SessionGuard::open(),
    )
    .await;
    store.bus.publish(Change::Topology(Arc::new(TopologyDelta {
        added_topics: vec![Arc::from("unrelated")],
        removed_topics: Vec::new(),
        changed_topics: Vec::new(),
        added_groups: Vec::new(),
        removed_groups: Vec::new(),
        changed_groups: Vec::new(),
        brokers_changed: false,
    })));
    store.bus.publish(Change::Topology(Arc::new(TopologyDelta {
        added_topics: Vec::new(),
        removed_topics: Vec::new(),
        changed_topics: vec![Arc::from("orders.created")],
        added_groups: Vec::new(),
        removed_groups: Vec::new(),
        changed_groups: Vec::new(),
        brokers_changed: false,
    })));
    let events = read_events(response, 1).await;

    assert_eq!(events[0]["type"], "topology");
    assert_eq!(events[0]["addedTopics"], serde_json::json!([]));
    assert_eq!(
        events[0]["changedTopics"],
        serde_json::json!(["orders.created"])
    );
}

#[tokio::test]
async fn falling_behind_the_bus_asks_the_client_to_refetch_instead_of_dropping_it() {
    let state = seeded();
    let store = Arc::clone(store_of(&state, "local"));
    let response = open_stream(
        &state,
        "/clusters/local/updates",
        EffectiveAccess::Unrestricted,
        SessionGuard::open(),
    )
    .await;
    for index in 0..(2 * BUS_CAPACITY) {
        store.bus.publish(tick(&[("orders.created", index as f64)]));
    }
    let frames = read_frames(response, 1).await;

    assert_eq!(frames[0].0, "resync");
    assert_eq!(frames[0].1, serde_json::json!({ "type": "resync" }));
}

#[tokio::test]
async fn a_cluster_the_session_cannot_see_is_never_subscribable() {
    let state = two_clusters();
    seed(store_of(&state, "payments"));
    let (status, code) = failure(
        &state,
        "/clusters/payments/updates",
        granted(vec![viewer(only(&["local"]))]),
    )
    .await;

    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(code, "UNKNOWN_CLUSTER");
}

#[tokio::test]
async fn a_session_that_expires_mid_stream_terminates_it() {
    let state = seeded();
    let store = Arc::clone(store_of(&state, "local"));
    let response = open_stream(
        &state,
        "/clusters/local/updates",
        EffectiveAccess::Unrestricted,
        SessionGuard::expired(),
    )
    .await;
    store.bus.publish(tick(&[("orders.created", 1.0)]));
    let events = read_events(response, 1).await;

    assert_eq!(events[0]["code"], "SESSION_EXPIRED");
    assert!(
        events[0]["error"]
            .as_str()
            .expect("message")
            .contains("session is no longer valid")
    );
}

#[tokio::test]
async fn the_updates_route_is_wired_with_the_session_extensions() {
    let response = crate::app::router(seeded())
        .oneshot(
            Request::builder()
                .uri("/api/clusters/local/updates")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");

    assert_eq!(response.status(), StatusCode::OK);
    let content_type = response
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("");
    assert!(
        content_type.starts_with("text/event-stream"),
        "{content_type}"
    );
}
