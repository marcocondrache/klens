use std::io::Write as _;
use std::net::{SocketAddr, TcpListener};

use klens::kafka::ingest::Ingest;
use klens::{AppState, AuthState, Clusters, Config, Limits, router};
use reqwest::header::CONTENT_TYPE;
use reqwest::{Method, Response, StatusCode};
use serde_json::Value;
use tempfile::NamedTempFile;
use tokio::net::TcpStream;
use tokio::task::AbortHandle;
use tokio::time::{Instant, sleep};

use crate::stream::Stream;
use crate::{Kafka, PATIENCE, POLL};

pub struct Klens {
    base: String,
    http: reqwest::Client,
    server: AbortHandle,
    _ingest: Ingest,
}

impl Klens {
    pub async fn over(kafka: &Kafka) -> Self {
        let bind = free_port();
        let config = config(&format!(
            "
            bind: {bind}
            clusters:
              local:
                bootstrap_servers: ['{}']
                writable: true
            tuning:
              ingest:
                topology: 1s
                high_watermark: 1s
                low_watermark: 1s
                config: 1s
                log_dirs: 1s
                acls: 1s
                quotas: 1s
                scram_users: 1s
                fast_offset: 1s
                slow_offset: 1s
            ",
            kafka.bootstrap_servers()
        ));
        let clusters = Clusters::connect(&config.clusters, &config.tuning)
            .await
            .expect("klens connects to kafka");
        let ingest = Ingest::start(&clusters, &config.tuning.ingest);
        let state = AppState::new(clusters, AuthState::disabled(), Limits::new(&config.tuning));
        let server = tokio::spawn(klens::serve(
            router(state, &config.allowed_hosts),
            config.bind,
        ));

        let deadline = Instant::now() + PATIENCE;
        while TcpStream::connect(bind).await.is_err() {
            if server.is_finished() {
                panic!("klens stopped serving: {:?}", server.await);
            }
            assert!(Instant::now() < deadline, "klens never started serving");
            sleep(POLL).await;
        }

        Self {
            base: format!("http://{bind}"),
            http: reqwest::Client::new(),
            server: server.abort_handle(),
            _ingest: ingest,
        }
    }

    pub async fn fetch(&self, path: &str) -> Response {
        self.http
            .get(format!("{}{path}", self.base))
            .send()
            .await
            .unwrap_or_else(|error| panic!("GET {path}: {error}"))
    }

    pub async fn status(&self, path: &str) -> StatusCode {
        self.fetch(path).await.status()
    }

    pub async fn get(&self, path: &str) -> Value {
        let response = self.fetch(path).await;
        let status = response.status();
        let body = response.text().await.expect("response body");
        assert_eq!(status, StatusCode::OK, "GET {path} answered {body}");
        serde_json::from_str(&body).unwrap_or_else(|error| panic!("GET {path}: {error}: {body}"))
    }

    pub async fn post(&self, path: &str, body: &Value) -> (StatusCode, Value) {
        self.send(Method::POST, path, Some(body)).await
    }

    pub async fn put(&self, path: &str, body: &Value) -> (StatusCode, Value) {
        self.send(Method::PUT, path, Some(body)).await
    }

    pub async fn patch(&self, path: &str, body: &Value) -> (StatusCode, Value) {
        self.send(Method::PATCH, path, Some(body)).await
    }

    pub async fn delete(&self, path: &str) -> (StatusCode, Value) {
        self.send(Method::DELETE, path, None).await
    }

    async fn send(&self, method: Method, path: &str, body: Option<&Value>) -> (StatusCode, Value) {
        let mut request = self
            .http
            .request(method.clone(), format!("{}{path}", self.base));
        if let Some(body) = body {
            request = request
                .header(CONTENT_TYPE, "application/json")
                .body(body.to_string());
        }
        let response = request
            .send()
            .await
            .unwrap_or_else(|error| panic!("{method} {path}: {error}"));
        let status = response.status();
        let body = response.text().await.expect("response body");
        (status, serde_json::from_str(&body).unwrap_or(Value::Null))
    }

    pub async fn eventually(&self, path: &str, done: impl Fn(&Value) -> bool) -> Value {
        let deadline = Instant::now() + PATIENCE;
        loop {
            let response = self.fetch(path).await;
            let status = response.status();
            let body = response.text().await.expect("response body");
            let value = serde_json::from_str(&body).unwrap_or(Value::Null);
            if status == StatusCode::OK && done(&value) {
                return value;
            }
            assert!(
                Instant::now() < deadline,
                "GET {path} never got there, last answering {status} {body}"
            );
            sleep(POLL).await;
        }
    }

    pub async fn open(&self, path: &str) -> Stream {
        let response = self.fetch(path).await;
        assert_eq!(response.status(), StatusCode::OK, "GET {path}");
        Stream::new(response)
    }
}

impl Drop for Klens {
    fn drop(&mut self) {
        self.server.abort();
    }
}

fn free_port() -> SocketAddr {
    TcpListener::bind("127.0.0.1:0")
        .and_then(|listener| listener.local_addr())
        .expect("a free port")
}

fn config(yaml: &str) -> Config {
    let mut file = NamedTempFile::new().expect("config file");
    file.write_all(yaml.as_bytes()).expect("write config");
    Config::load(file.path()).expect("the e2e config loads")
}
