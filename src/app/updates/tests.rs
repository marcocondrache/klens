use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use serde_json::json;
use tower::ServiceExt as _;

use crate::app::auth::SessionGuard;
use crate::kafka::model::{AclListing, QuotaListing};
use crate::kafka::store::bus::BUS_CAPACITY;
use crate::kafka::store::{
    Change, ConfigsDelta, GroupLagUpdate, GroupOffsetsWave, LogDirsDelta, SubjectsDelta, TopicRate,
    WatermarksTick,
};
use crate::testing::{FakeCluster, TestApp, access, viewer};

const UPDATES: &str = "/clusters/local/updates";

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

fn log_dirs_moved(topics: &[&str]) -> Change {
    Change::LogDirs(Arc::new(LogDirsDelta {
        topics: topics.iter().map(|topic| Arc::from(*topic)).collect(),
        brokers_changed: true,
    }))
}

fn subjects_added(subjects: &[&str]) -> Change {
    Change::Subjects(Arc::new(SubjectsDelta {
        added: subjects.iter().map(|subject| Arc::from(*subject)).collect(),
        removed: Vec::new(),
        changed: Vec::new(),
    }))
}

#[tokio::test]
async fn an_unscoped_subscriber_gets_the_whole_cluster_firehose() {
    let app = TestApp::local().await;
    let mut live = app.open(UPDATES).await;
    app.store()
        .bus
        .publish(tick(&[("orders.created", 10.0), ("payments.settled", 2.0)]));

    let event = live.next().await;

    assert_eq!(event.data["type"], "watermarks");
    assert_eq!(
        event.data["topics"],
        json!([
            { "topic": "orders.created", "rate": 10.0 },
            { "topic": "payments.settled", "rate": 2.0 }
        ])
    );
}

#[tokio::test]
async fn a_topic_scoped_subscriber_pays_only_for_its_own_topic() {
    let app = TestApp::local().await;
    let mut live = app.open(&format!("{UPDATES}?topic=orders.created")).await;
    app.store()
        .bus
        .publish(tick(&[("orders.created", 10.0), ("payments.settled", 2.0)]));

    let event = live.next().await;

    assert_eq!(
        event.data["topics"],
        json!([{ "topic": "orders.created", "rate": 10.0 }])
    );
}

#[tokio::test]
async fn an_event_outside_the_scope_never_reaches_the_socket() {
    let app = TestApp::local().await;
    let mut live = app.open(&format!("{UPDATES}?topic=payments.settled")).await;
    app.store()
        .bus
        .publish(Change::Configs(Arc::new(ConfigsDelta {
            topics: vec![Arc::from("orders.created")],
        })));
    app.store()
        .bus
        .publish(subjects_added(&["payments.settled-value"]));

    let event = live.next().await;

    assert_eq!(event.data["type"], "subjects");
}

#[tokio::test]
async fn an_unscoped_subscriber_hears_every_size_that_moved() {
    let app = TestApp::local().await;
    let mut live = app.open(UPDATES).await;
    app.store()
        .bus
        .publish(log_dirs_moved(&["orders.created", "payments.settled"]));

    let event = live.next().await;

    assert_eq!(event.name, "logDirs");
    assert_eq!(
        event.data,
        json!({
            "type": "logDirs",
            "topics": ["orders.created", "payments.settled"],
            "brokersChanged": true
        })
    );
}

#[tokio::test]
async fn a_topic_scoped_subscriber_hears_only_its_own_size() {
    let app = TestApp::local().await;
    let mut live = app.open(&format!("{UPDATES}?topic=orders.created")).await;
    app.store()
        .bus
        .publish(log_dirs_moved(&["payments.settled"]));
    app.store().bus.publish(subjects_added(&[]));
    app.store()
        .bus
        .publish(log_dirs_moved(&["orders.created", "payments.settled"]));

    let events = live.take(2).await;

    assert_eq!(
        events[0].data["type"], "subjects",
        "another topic's size stays out"
    );
    assert_eq!(
        events[1].data,
        json!({
            "type": "logDirs",
            "topics": ["orders.created"],
            "brokersChanged": false
        })
    );
}

#[tokio::test]
async fn an_acl_change_tells_the_client_to_refetch() {
    let app = TestApp::local().await;
    let mut live = app.open(UPDATES).await;
    app.cluster().set_acls(AclListing::Enabled(Vec::new()));
    let rig = app.rig();
    rig.poll(&rig.acls()).await;

    let event = live.next().await;

    assert_eq!(event.name, "acls");
    assert_eq!(event.data, json!({ "type": "acls" }));
}

#[tokio::test]
async fn a_quota_change_is_sent_as_a_quotas_event() {
    let app = TestApp::local().await;
    let mut live = app.open(UPDATES).await;
    app.cluster().set_quotas(QuotaListing::Denied);
    let rig = app.rig();
    rig.poll(&rig.quotas()).await;

    let event = live.next().await;

    assert_eq!(event.name, "quotas");
    assert_eq!(event.data, json!({ "type": "quotas" }));
}

#[tokio::test]
async fn an_unscoped_lag_wave_fans_out_one_update_per_group_without_offsets() {
    let app = TestApp::local().await;
    let mut live = app.open(UPDATES).await;
    app.store()
        .bus
        .publish(wave(&[("order-processor", 15), ("audit", 3)]));

    let events = live.take(2).await;

    assert_eq!(events[0].data["type"], "groupLag");
    assert_eq!(events[0].data["group"], "order-processor");
    assert_eq!(events[0].data["lag"], 15);
    assert_eq!(events[0].data["offsets"], json!([]));
    assert_eq!(events[1].data["group"], "audit");
    assert_eq!(events[1].data["lag"], 3);
    assert_eq!(events[1].data["offsets"], json!([]));
}

#[tokio::test]
async fn a_group_scoped_subscriber_holds_an_interest_lease_for_the_stream() {
    let app = TestApp::local().await;
    let interest = &app.store().interest;
    assert!(!interest.is_hot("order-processor"));

    let mut live = app.open(&format!("{UPDATES}?group=order-processor")).await;
    app.store()
        .bus
        .publish(wave(&[("order-processor", 15), ("audit", 3)]));

    assert_eq!(live.next().await.data["group"], "order-processor");
    assert!(interest.is_hot("order-processor"));

    drop(live);
    assert!(!interest.is_hot("order-processor"));
}

#[tokio::test]
async fn a_topology_change_reaches_a_scoped_subscriber_only_when_it_names_its_topic() {
    let app = TestApp::local().await;
    let mut live = app.open(&format!("{UPDATES}?topic=orders.created")).await;
    let rig = app.rig();
    app.cluster().add_topic("unrelated", 1, 0);
    rig.poll(&rig.topology()).await;
    app.cluster().add_partition("orders.created", 2);
    rig.poll(&rig.topology()).await;

    let event = live.next().await;

    assert_eq!(event.data["type"], "topology");
    assert_eq!(event.data["addedTopics"], json!([]));
    assert_eq!(event.data["changedTopics"], json!(["orders.created"]));
}

#[tokio::test]
async fn falling_behind_the_bus_asks_the_client_to_refetch_instead_of_dropping_it() {
    let app = TestApp::local().await;
    let mut live = app.open(UPDATES).await;
    for index in 0..(2 * BUS_CAPACITY) {
        app.store()
            .bus
            .publish(tick(&[("orders.created", index as f64)]));
    }

    let event = live.next().await;

    assert_eq!(event.name, "resync");
    assert_eq!(event.data, json!({ "type": "resync" }));
}

#[tokio::test]
async fn a_cluster_the_session_cannot_see_is_never_subscribable() {
    TestApp::of([FakeCluster::local(), FakeCluster::named("payments")])
        .ingested()
        .await
        .with_access(access([viewer().on(&["local"])]))
        .get("/clusters/payments/updates")
        .await
        .assert_error(StatusCode::NOT_FOUND, "UNKNOWN_CLUSTER");
}

#[tokio::test]
async fn a_session_that_expires_mid_stream_terminates_it() {
    let app = TestApp::local().await;
    let mut live = app.with_guard(SessionGuard::expired()).open(UPDATES).await;
    app.store().bus.publish(tick(&[("orders.created", 1.0)]));

    let event = live.next().await;

    assert_eq!(event.data["code"], "SESSION_EXPIRED");
    assert!(
        event.data["error"]
            .as_str()
            .expect("message")
            .contains("session is no longer valid")
    );
}

#[tokio::test]
async fn the_updates_route_is_wired_with_the_session_extensions() {
    let app = TestApp::local().await;
    let response = crate::app::router(app.state().clone())
        .oneshot(
            Request::get("/api/clusters/local/updates")
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
