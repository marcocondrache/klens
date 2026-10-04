use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::body::{Body, BodyDataStream, to_bytes};
use axum::http::{HeaderMap, Method, Request, StatusCode, header};
use axum::middleware::{self, Next};
use axum::response::Response;
use futures::StreamExt as _;
use serde_json::Value;
use tokio::time::timeout;
use tower::ServiceExt as _;

use super::auth::access::EffectiveAccess;
use super::auth::{AuthState, SessionGuard};
use super::{AppState, Limits, auth_routes, health, resources};
use crate::config::Tuning;
use crate::kafka::store::ClusterStore;
use crate::kafka::{ClusterSession, Clusters};
use crate::testing::{FakeCluster, Rig};

const WAIT: Duration = Duration::from_secs(2);

#[derive(Clone)]
pub struct TestApp {
    state: AppState,
    clusters: Vec<FakeCluster>,
    access: EffectiveAccess,
    guard: SessionGuard,
}

pub struct Setup {
    clusters: Vec<FakeCluster>,
    limits: Limits,
    writable: Vec<String>,
}

impl TestApp {
    pub async fn local() -> Self {
        Self::over(FakeCluster::local()).await
    }

    pub async fn over(cluster: FakeCluster) -> Self {
        Self::of([cluster]).ingested().await
    }

    pub fn of(clusters: impl IntoIterator<Item = FakeCluster>) -> Setup {
        Setup {
            clusters: clusters.into_iter().collect(),
            limits: Limits::new(&Tuning::default()),
            writable: Vec::new(),
        }
    }

    pub async fn ingest(&self) {
        for cluster in &self.clusters {
            self.rig_of(cluster).ingest().await;
            cluster.reset_calls();
        }
    }

    pub fn rig(&self) -> Rig {
        self.rig_of(self.cluster())
    }

    pub fn cluster(&self) -> &FakeCluster {
        &self.clusters[0]
    }

    pub fn store(&self) -> &Arc<ClusterStore> {
        self.store_of(&self.cluster().identity().name)
    }

    pub fn state(&self) -> &AppState {
        &self.state
    }

    pub fn with_access(&self, access: EffectiveAccess) -> Self {
        Self {
            access,
            ..self.clone()
        }
    }

    pub fn with_guard(&self, guard: SessionGuard) -> Self {
        Self {
            guard,
            ..self.clone()
        }
    }

    pub async fn get(&self, path: &str) -> Reply {
        self.reply(get_request(path)).await
    }

    pub async fn post(&self, path: &str, body: &Value) -> Reply {
        self.reply(json_request(Method::POST, path, body.to_string()))
            .await
    }

    pub async fn patch(&self, path: &str, body: &Value) -> Reply {
        self.reply(json_request(Method::PATCH, path, body.to_string()))
            .await
    }

    pub async fn delete(&self, path: &str) -> Reply {
        self.reply(Request::delete(path).body(Body::empty()).expect("request"))
            .await
    }

    pub async fn reply(&self, request: Request<Body>) -> Reply {
        let request_line = format!("{} {}", request.method(), request.uri());
        let response = self.send(request).await;
        let status = response.status();
        let body = timeout(WAIT, to_bytes(response.into_body(), usize::MAX))
            .await
            .unwrap_or_else(|_| panic!("{request_line} never finished its body"))
            .expect("body");
        let body = if body.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(&body).unwrap_or_else(|error| {
                panic!(
                    "{request_line} answered with no json ({error}): {}",
                    String::from_utf8_lossy(&body)
                )
            })
        };
        Reply {
            request: request_line,
            status,
            body,
        }
    }

    pub async fn status(&self, path: &str) -> StatusCode {
        self.send(get_request(path)).await.status()
    }

    pub async fn open(&self, path: &str) -> Live {
        let response = self.send(get_request(path)).await;
        assert_eq!(response.status(), StatusCode::OK, "GET {path} did not open");
        Live {
            path: path.to_owned(),
            headers: response.headers().clone(),
            body: response.into_body().into_data_stream(),
            buffer: String::new(),
        }
    }

    fn rig_of(&self, cluster: &FakeCluster) -> Rig {
        let store = self.store_of(&cluster.identity().name);
        Rig::over(cluster.clone(), Arc::clone(store))
    }

    fn store_of(&self, cluster: &str) -> &Arc<ClusterStore> {
        &self
            .state
            .clusters
            .get(cluster)
            .expect("configured cluster")
            .store
    }

    async fn send(&self, request: Request<Body>) -> Response {
        let access = self.access.clone();
        let guard = self.guard.clone();
        api()
            .layer(middleware::from_fn(
                move |mut request: Request<Body>, next: Next| {
                    request.extensions_mut().insert(access.clone());
                    request.extensions_mut().insert(guard.clone());
                    next.run(request)
                },
            ))
            .with_state(self.state.clone())
            .oneshot(request)
            .await
            .expect("response")
    }
}

impl Setup {
    pub fn limits(self, limits: Limits) -> Self {
        Self { limits, ..self }
    }

    pub fn writable(self, clusters: &[&str]) -> Self {
        Self {
            writable: clusters.iter().map(|name| (*name).to_owned()).collect(),
            ..self
        }
    }

    pub fn build(self) -> TestApp {
        let writable: Vec<&str> = self.writable.iter().map(String::as_str).collect();
        let clusters = Clusters::from_sessions(self.clusters.clone()).writable(&writable);
        TestApp {
            state: AppState::new(clusters, AuthState::disabled(), self.limits),
            clusters: self.clusters,
            access: EffectiveAccess::Unrestricted,
            guard: SessionGuard::open(),
        }
    }

    pub async fn ingested(self) -> TestApp {
        let app = self.build();
        app.ingest().await;
        app
    }
}

fn get_request(path: &str) -> Request<Body> {
    Request::get(path).body(Body::empty()).expect("request")
}

pub fn json_request(method: Method, path: &str, body: impl Into<Body>) -> Request<Body> {
    Request::builder()
        .method(method)
        .uri(path)
        .header(header::CONTENT_TYPE, "application/json")
        .body(body.into())
        .expect("request")
}

fn api() -> Router<AppState> {
    health::router().merge(auth_routes()).merge(resources())
}

pub struct Reply {
    request: String,
    pub status: StatusCode,
    pub body: Value,
}

impl Reply {
    #[track_caller]
    pub fn ok(self) -> Value {
        self.expect(StatusCode::OK)
    }

    #[track_caller]
    pub fn expect(self, status: StatusCode) -> Value {
        assert_eq!(
            self.status, status,
            "{} answered {}",
            self.request, self.body
        );
        self.body
    }

    #[track_caller]
    pub fn assert_error(&self, status: StatusCode, code: &str) {
        assert_eq!(
            (self.status, self.body["code"].as_str()),
            (status, Some(code)),
            "{} answered {}",
            self.request,
            self.body
        );
    }
}

pub struct Live {
    path: String,
    pub headers: HeaderMap,
    body: BodyDataStream,
    buffer: String,
}

#[derive(Debug)]
pub struct Event {
    pub name: String,
    pub data: Value,
}

impl Live {
    pub async fn next(&mut self) -> Event {
        loop {
            if let Some(event) = self.parse() {
                return event;
            }
            let chunk = timeout(WAIT, self.body.next())
                .await
                .unwrap_or_else(|_| {
                    panic!("{} sent no event, holding {:?}", self.path, self.buffer)
                })
                .unwrap_or_else(|| panic!("{} ended, holding {:?}", self.path, self.buffer))
                .expect("frame");
            self.buffer.push_str(&String::from_utf8_lossy(&chunk));
        }
    }

    pub async fn take(&mut self, count: usize) -> Vec<Event> {
        let mut events = Vec::with_capacity(count);
        for _ in 0..count {
            events.push(self.next().await);
        }
        events
    }

    pub async fn ndjson(mut self) -> Result<Vec<Value>, String> {
        let mut text = String::new();
        let read = async {
            while let Some(chunk) = self.body.next().await {
                let chunk = chunk.map_err(|error| error.to_string())?;
                text.push_str(std::str::from_utf8(&chunk).expect("utf-8"));
            }
            Ok::<_, String>(())
        };
        timeout(WAIT, read)
            .await
            .map_err(|_| format!("{} never ended", self.path))??;
        assert!(text.is_empty() || text.ends_with('\n'), "{text}");
        Ok(text
            .lines()
            .map(|line| serde_json::from_str(line).expect("one json value per line"))
            .collect())
    }

    fn parse(&mut self) -> Option<Event> {
        while let Some(end) = self.buffer.find("\n\n") {
            let frame: String = self.buffer.drain(..end + 2).collect();
            let Some(data) = frame.lines().find_map(|line| line.strip_prefix("data:")) else {
                continue;
            };
            let name = frame
                .lines()
                .find_map(|line| line.strip_prefix("event:"))
                .unwrap_or("message")
                .trim()
                .to_owned();
            let data = serde_json::from_str(data.trim())
                .unwrap_or_else(|error| panic!("sse data is not json ({error}): {data}"));
            return Some(Event { name, data });
        }
        None
    }
}
