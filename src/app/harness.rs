use std::sync::Arc;

use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::middleware::{self, Next};
use axum::response::Response;
use foldhash::HashMap;
use futures::StreamExt as _;
use serde_json::Value;
use tower::ServiceExt as _;

use crate::AppState;
use crate::app::auth::SessionGuard;
use crate::app::auth::access::{ClusterScope, EffectiveAccess, Grant, PrivilegeSet};
use crate::kafka::store::fixtures::{
    at, config, group, offsets, partition, subject, topic, topology, watermarks,
};
use crate::kafka::store::{ClusterStore, ConfigTable, Interner, OffsetTable, SubjectTable};
use crate::kafka::{FakeCluster, SessionSet};

pub(super) fn with(sessions: Vec<FakeCluster>) -> AppState {
    AppState::new(Arc::new(SessionSet::from_sessions(sessions)))
}

pub(super) fn state() -> AppState {
    with(vec![FakeCluster::local()])
}

pub(super) fn two_clusters() -> AppState {
    with(vec![FakeCluster::local(), FakeCluster::named("payments")])
}

pub(super) fn only(clusters: &[&str]) -> ClusterScope {
    ClusterScope::Only(Arc::new(
        clusters.iter().map(|name| (*name).to_owned()).collect(),
    ))
}

pub(super) fn granted(grants: Vec<(&str, PrivilegeSet, ClusterScope)>) -> EffectiveAccess {
    EffectiveAccess::Granted(
        grants
            .into_iter()
            .map(|(role_name, privileges, scope)| Grant {
                role_name: Arc::from(role_name),
                privileges,
                scope,
            })
            .collect(),
    )
}

pub(super) fn admin(scope: ClusterScope) -> (&'static str, PrivilegeSet, ClusterScope) {
    ("admin", PrivilegeSet::ALL, scope)
}

pub(super) fn viewer(scope: ClusterScope) -> (&'static str, PrivilegeSet, ClusterScope) {
    ("viewer", PrivilegeSet::NONE, scope)
}

pub(super) fn viewer_everywhere() -> EffectiveAccess {
    granted(vec![viewer(ClusterScope::All)])
}

pub(super) fn api(state: AppState, access: EffectiveAccess, guard: SessionGuard) -> Router {
    super::api()
        .layer(middleware::from_fn(
            move |mut request: Request<Body>, next: Next| {
                let access = access.clone();
                let guard = guard.clone();
                async move {
                    request.extensions_mut().insert(access);
                    request.extensions_mut().insert(guard);
                    next.run(request).await
                }
            },
        ))
        .with_state(state)
}

pub(super) async fn call(
    state: &AppState,
    path: &str,
    access: EffectiveAccess,
    guard: SessionGuard,
) -> (StatusCode, Value) {
    send(
        state,
        Request::builder()
            .uri(path)
            .body(Body::empty())
            .expect("request"),
        access,
        guard,
    )
    .await
}

pub(super) fn post(path: &str, body: Value) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri(path)
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .expect("request")
}

pub(super) async fn send(
    state: &AppState,
    request: Request<Body>,
    access: EffectiveAccess,
    guard: SessionGuard,
) -> (StatusCode, Value) {
    let response = api(state.clone(), access, guard)
        .oneshot(request)
        .await
        .expect("response");
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    let json = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap_or_else(|error| {
            panic!(
                "body is not json ({error}): {}",
                String::from_utf8_lossy(&bytes)
            )
        })
    };
    (status, json)
}

pub(super) async fn open_stream(
    state: &AppState,
    path: &str,
    access: EffectiveAccess,
    guard: SessionGuard,
) -> Response {
    api(state.clone(), access, guard)
        .oneshot(
            Request::builder()
                .uri(path)
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response")
}

pub(super) async fn read_frames(response: Response, count: usize) -> Vec<(String, Value)> {
    assert_eq!(response.status(), StatusCode::OK, "stream did not open");
    let mut stream = response.into_body().into_data_stream();
    let mut buffer = String::new();
    let mut events = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);

    while events.len() < count {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        assert!(
            !remaining.is_zero(),
            "timed out with {events:?} buffered {buffer}"
        );
        let chunk = tokio::time::timeout(remaining, stream.next())
            .await
            .expect("timeout")
            .unwrap_or_else(|| panic!("stream ended after {} events: {buffer}", events.len()))
            .expect("frame");
        buffer.push_str(&String::from_utf8_lossy(&chunk));
        while let Some(index) = buffer.find("\n\n") {
            let frame = buffer[..index].to_owned();
            buffer.drain(..index + 2);
            let Some(data) = frame.lines().find_map(|line| line.strip_prefix("data:")) else {
                continue;
            };
            let name = frame
                .lines()
                .find_map(|line| line.strip_prefix("event:"))
                .unwrap_or("message")
                .trim()
                .to_owned();
            events.push((
                name,
                serde_json::from_str(data.trim())
                    .unwrap_or_else(|error| panic!("sse data is not json ({error}): {data}")),
            ));
        }
    }
    events
}

pub(super) async fn ok(state: &AppState, path: &str) -> Value {
    ok_as(state, path, EffectiveAccess::Unrestricted).await
}

pub(super) async fn ok_as(state: &AppState, path: &str, access: EffectiveAccess) -> Value {
    let (status, json) = call(state, path, access, SessionGuard::open()).await;
    assert_eq!(status, StatusCode::OK, "{path} {json}");
    json
}

pub(super) async fn failure(
    state: &AppState,
    path: &str,
    access: EffectiveAccess,
) -> (StatusCode, String) {
    let (status, json) = call(state, path, access, SessionGuard::open()).await;
    let code = json["code"]
        .as_str()
        .unwrap_or_else(|| panic!("error has no code: {json}"))
        .to_owned();
    (status, code)
}

pub(super) fn seed(store: &ClusterStore) {
    store.topology.commit(Arc::new(topology(
        vec![
            topic(
                "orders.created",
                vec![
                    partition(0, vec![1], vec![1]),
                    partition(1, vec![1], vec![1]),
                ],
            ),
            topic("payments.settled", vec![partition(0, vec![1], vec![1])]),
        ],
        vec![group("order-processor", "orders.created", vec![0, 1])],
    )));

    store.watermarks.commit(Arc::new(watermarks(
        at(1_000),
        &[
            ("orders.created", 0, 0, 100),
            ("orders.created", 1, 10, 60),
            ("payments.settled", 0, 0, 5),
        ],
    )));

    store.offsets.commit(Arc::new(OffsetTable {
        groups: HashMap::from_iter([(
            Arc::from("order-processor"),
            Arc::new(offsets(
                at(1_000),
                &[("orders.created", 0, 90), ("orders.created", 1, 55)],
            )),
        )]),
    }));

    store.configs.commit(Arc::new(ConfigTable {
        topics: HashMap::from_iter([(
            Arc::from("orders.created"),
            Arc::from([
                config("cleanup.policy", "compact"),
                config("retention.ms", "604800000"),
            ]),
        )]),
    }));

    store.subjects.commit(Arc::new(SubjectTable::assemble(
        &[subject("orders.created-value", 1, 2)],
        &mut Interner::default(),
    )));
}

pub(super) fn writable() -> (AppState, FakeCluster) {
    seeded_with(FakeCluster::local().writable())
}

pub(super) fn seeded() -> AppState {
    seeded_with(FakeCluster::local()).0
}

pub(super) fn seeded_with(session: FakeCluster) -> (AppState, FakeCluster) {
    let state = with(vec![session.clone()]);
    seed(state.cluster("local").expect("local cluster"));
    (state, session)
}
