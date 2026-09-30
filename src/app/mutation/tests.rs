use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::http::{Method, Request, StatusCode, header};
use axum::routing::delete;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::AppState;
use crate::app::auth::SessionGuard;
use crate::app::auth::access::{EffectiveAccess, Privilege};
use crate::app::context::Session;
use crate::app::error::ApiError;
use crate::app::extract::{Json, Path};
use crate::kafka::ingest::{TopologyLane, run};
use crate::kafka::store::LaneId;
use crate::kafka::{ClusterSession, FakeCluster, KafkaError};
use crate::telemetry::capture::subscriber as capture;

use super::super::harness::{
    admin, granted, json, respond, serve, store_of, viewer_everywhere, with, with_writable,
};
use super::Mutation;

const ORDERS: &str = "/clusters/local/topics/orders.created";

#[derive(Debug, Serialize, Deserialize)]
struct Removal {
    reason: String,
}

/// A stand-in for a real change: removes a topic from the fake cluster,
/// unless the reason is `refuse`, which the cluster rejects.
async fn remove_topic(
    fake: FakeCluster,
    session: Session,
    Path((cluster, topic)): Path<(String, String)>,
    Json(removal): Json<Removal>,
) -> Result<Json<Removal>, ApiError> {
    let cluster = session.cluster(&cluster)?;
    let removed = cluster
        .write(
            Mutation::new(Privilege::Configs, "topic.delete", &topic).kicks(&[LaneId::Topology]),
        )?
        .apply(|kafka| async move {
            kafka.topic_metadata(&topic).await?;
            if removal.reason == "refuse" {
                return Err(KafkaError::Rejected("a policy forbids it".into()));
            }
            fake.remove_topic(&topic);
            Ok(removal)
        })
        .await?;
    Ok(Json(removed))
}

fn routes(fake: &FakeCluster) -> Router<AppState> {
    let fake = fake.clone();
    Router::new().route(
        "/clusters/{cluster}/topics/{topic}",
        delete(move |session, path, body| remove_topic(fake.clone(), session, path, body)),
    )
}

async fn remove(
    state: &AppState,
    fake: &FakeCluster,
    access: EffectiveAccess,
    guard: SessionGuard,
    reason: &str,
) -> (StatusCode, Value) {
    respond(
        serve(routes(fake), state.clone(), access, guard),
        json(Method::DELETE, ORDERS, &json!({ "reason": reason })),
    )
    .await
}

async fn still_there(fake: &FakeCluster) -> bool {
    fake.topic_metadata("orders.created").await.is_ok()
}

async fn settle(ready: impl Fn() -> bool, what: &str) {
    for _ in 0..2_000 {
        if ready() {
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!("{what}");
}

#[tokio::test]
async fn a_read_only_cluster_refuses_every_change() {
    let fake = FakeCluster::local();
    let state = with(vec![fake.clone()]);

    let (status, body) = remove(
        &state,
        &fake,
        EffectiveAccess::Unrestricted,
        SessionGuard::open(),
        "cleanup",
    )
    .await;

    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
    assert_eq!(body["code"], "READ_ONLY_CLUSTER");
    assert_eq!(body["error"], "cluster 'local' is read-only");
    assert_eq!(fake.calls().topic_metadata(), 0, "the change never ran");
    assert!(still_there(&fake).await);
}

#[tokio::test]
async fn a_change_needs_its_privilege() {
    let fake = FakeCluster::local();
    let state = with_writable(vec![fake.clone()], &["local"]);

    let (status, body) = remove(
        &state,
        &fake,
        viewer_everywhere(),
        SessionGuard::open(),
        "cleanup",
    )
    .await;

    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
    assert_eq!(body["code"], "FORBIDDEN");
    assert_eq!(fake.calls().topic_metadata(), 0, "the change never ran");
    assert!(still_there(&fake).await);
}

#[tokio::test]
async fn a_read_only_cluster_is_the_reason_given_before_a_missing_privilege() {
    let fake = FakeCluster::local();
    let state = with(vec![fake.clone()]);

    let (_, body) = remove(
        &state,
        &fake,
        viewer_everywhere(),
        SessionGuard::open(),
        "cleanup",
    )
    .await;

    assert_eq!(body["code"], "READ_ONLY_CLUSTER");
}

#[tokio::test(start_paused = true)]
async fn an_applied_change_kicks_the_lanes_it_names() {
    let fake = FakeCluster::local();
    let state = with_writable(vec![fake.clone()], &["local"]);
    let store = Arc::clone(store_of(&state, "local"));
    let lane = tokio::spawn(run(
        Arc::clone(&store),
        TopologyLane::with_interval(Arc::new(fake.clone()), Duration::from_secs(600)),
    ));
    settle(|| store.ready(), "the topology lane never polled").await;

    let (status, body) = remove(
        &state,
        &fake,
        granted(vec![admin(crate::app::auth::access::ClusterScope::All)]),
        SessionGuard::open(),
        "cleanup",
    )
    .await;

    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body, json!({ "reason": "cleanup" }), "the handler's answer");
    assert!(!still_there(&fake).await, "the change ran");
    settle(
        || store.topic_detail("orders.created").is_none(),
        "the topology lane waited out its 600s interval",
    )
    .await;
    lane.abort();
}

#[tokio::test]
async fn a_refused_change_answers_the_brokers_reason() {
    let fake = FakeCluster::local();
    let state = with_writable(vec![fake.clone()], &["local"]);

    let (status, body) = remove(
        &state,
        &fake,
        EffectiveAccess::Unrestricted,
        SessionGuard::open(),
        "refuse",
    )
    .await;

    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert_eq!(body["code"], "REJECTED");
    assert_eq!(
        body["error"],
        "the cluster refused the change: a policy forbids it"
    );
    assert!(still_there(&fake).await);
}

#[tokio::test(start_paused = true)]
async fn every_outcome_is_audited_with_who_what_and_where() {
    let (logs, _guard) = capture(tracing::Level::INFO);
    let fake = FakeCluster::local();
    let state = with_writable(vec![fake.clone()], &["local"]);

    remove(
        &state,
        &fake,
        viewer_everywhere(),
        SessionGuard::signed_in("bob"),
        "cleanup",
    )
    .await;
    remove(
        &state,
        &fake,
        EffectiveAccess::Unrestricted,
        SessionGuard::signed_in("alice"),
        "refuse",
    )
    .await;
    remove(
        &state,
        &fake,
        EffectiveAccess::Unrestricted,
        SessionGuard::open(),
        "cleanup",
    )
    .await;

    let text = logs.as_string();
    let lines: Vec<&str> = text.lines().collect();
    assert_eq!(lines.len(), 3, "{text}");
    for line in &lines {
        assert!(line.contains("klens::audit"), "{line}");
        assert!(line.contains("cluster=\"local\""), "{line}");
        assert!(line.contains("action=\"topic.delete\""), "{line}");
        assert!(line.contains("resource=\"orders.created\""), "{line}");
    }
    assert!(lines[0].contains("change refused"), "{text}");
    assert!(lines[0].contains("actor=\"bob\""), "{text}");
    assert!(lines[0].contains("code=\"FORBIDDEN\""), "{text}");
    assert!(lines[1].contains("change failed"), "{text}");
    assert!(lines[1].contains("actor=\"alice\""), "{text}");
    assert!(lines[1].contains("a policy forbids it"), "{text}");
    assert!(lines[2].contains("change applied"), "{text}");
    assert!(
        lines[2].contains("actor=\"anonymous\""),
        "no login, no subject: {text}"
    );
}

#[tokio::test]
async fn a_body_that_does_not_parse_answers_like_any_other_error() {
    let fake = FakeCluster::local();
    let state = with_writable(vec![fake.clone()], &["local"]);
    let app = || {
        serve(
            routes(&fake),
            state.clone(),
            EffectiveAccess::Unrestricted,
            SessionGuard::open(),
        )
    };

    let malformed = Request::builder()
        .method(Method::DELETE)
        .uri(ORDERS)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from("{\"reason\":"))
        .unwrap();
    let (status, body) = respond(app(), malformed).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["code"], "INVALID_REQUEST");

    let (status, body) = respond(
        app(),
        json(Method::DELETE, ORDERS, &json!({ "why": "cleanup" })),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert_eq!(body["code"], "INVALID_REQUEST");
    assert!(
        body["error"]
            .as_str()
            .unwrap()
            .contains("missing field `reason`"),
        "{body}"
    );

    let untyped = Request::builder()
        .method(Method::DELETE)
        .uri(ORDERS)
        .body(Body::from("{\"reason\":\"cleanup\"}"))
        .unwrap();
    let (status, body) = respond(app(), untyped).await;
    assert_eq!(status, StatusCode::UNSUPPORTED_MEDIA_TYPE, "{body}");
    assert_eq!(body["code"], "INVALID_REQUEST");

    assert!(still_there(&fake).await, "no bad body reached the cluster");
}

#[allow(dead_code)]
fn probe_send(
    fake: FakeCluster,
    session: Session,
    path: Path<(String, String)>,
    body: Json<Removal>,
) {
    fn is_send<T: Send>(_: T) {}
    is_send(remove_topic(fake, session, path, body));
}
