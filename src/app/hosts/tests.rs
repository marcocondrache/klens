use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use tower::ServiceExt as _;
use tracing::Level;

use crate::app::{AppState, AuthState, Limits, router};
use crate::config::{Config, Tuning};
use crate::kafka::Clusters;
use crate::testing::{FakeCluster, LogCapture, TestApp};

fn get(host: Option<&str>, uri: &str) -> Request<Body> {
    let request = Request::get(uri);
    let request = match host {
        Some(host) => request.header(header::HOST, host),
        None => request,
    };
    request.body(Body::empty()).expect("request")
}

#[tokio::test]
async fn a_listed_host_is_served_on_any_port() {
    let app = TestApp::local().await;

    for host in [
        "localhost",
        "localhost:8080",
        "LocalHost:3000",
        "127.0.0.1:8080",
        "[::1]:8080",
    ] {
        let me = app
            .reply_through_router(get(Some(host), "/api/auth/me"))
            .await
            .ok();

        assert_eq!(me["enabled"], false, "{host}");
    }
}

#[tokio::test]
async fn any_other_host_is_refused_on_the_api_the_auth_routes_and_the_ui() {
    let app = TestApp::local().await;

    for host in [
        "attacker.example",
        "attacker.example:8080",
        "localhost.attacker.example",
        "127.0.0.2",
        "[::2]:8080",
    ] {
        for path in [
            "/api/clusters",
            "/api/auth/me",
            "/api/auth/login",
            "/",
            "/clusters/local/topics",
            "/health/",
        ] {
            let reply = app.reply_through_router(get(Some(host), path)).await;

            reply.assert_error(StatusCode::FORBIDDEN, "HOST_NOT_ALLOWED");
            assert_eq!(reply.body["error"], "host is not in allowed_hosts");
        }
    }
}

#[tokio::test]
async fn a_request_that_names_no_host_or_a_malformed_one_is_refused() {
    let app = TestApp::local().await;

    for host in [None, Some("local host"), Some("localhost:8080:8080")] {
        app.reply_through_router(get(host, "/api/auth/me"))
            .await
            .assert_error(StatusCode::FORBIDDEN, "HOST_NOT_ALLOWED");
    }
}

#[tokio::test]
async fn a_refused_host_is_logged_at_debug() {
    let app = TestApp::local().await;
    let logs = LogCapture::at(Level::DEBUG);

    app.reply_through_router(get(Some("attacker.example:8080"), "/"))
        .await
        .assert_error(StatusCode::FORBIDDEN, "HOST_NOT_ALLOWED");

    logs.assert_contains(r#"host not in allowed_hosts host="attacker.example:8080""#);
}

#[tokio::test]
async fn the_uri_names_the_host_only_when_no_header_does() {
    let app = TestApp::local().await;

    let me = app
        .reply_through_router(get(None, "http://localhost:8080/api/auth/me"))
        .await
        .ok();
    assert_eq!(me["enabled"], false);

    for (host, uri) in [
        (None, "http://attacker.example/api/auth/me"),
        (
            Some("attacker.example"),
            "http://localhost:8080/api/auth/me",
        ),
    ] {
        app.reply_through_router(get(host, uri))
            .await
            .assert_error(StatusCode::FORBIDDEN, "HOST_NOT_ALLOWED");
    }
}

#[tokio::test]
async fn health_and_ready_answer_any_host() {
    let app = TestApp::local().await;

    for host in [Some("10.0.0.7:8080"), Some("attacker.example"), None] {
        for path in ["/health", "/ready"] {
            assert_eq!(
                app.reply_through_router(get(host, path)).await.status,
                StatusCode::NO_CONTENT,
                "{path} for {host:?}"
            );
        }
    }
}

#[tokio::test]
async fn with_auth_on_the_list_is_not_consulted() {
    let state = AppState::new(
        Clusters::from_sessions(vec![FakeCluster::local()]),
        AuthState::enabled_for_tests(),
        Limits::new(&Tuning::default()),
    );

    let response = router(state, &Config::default().allowed_hosts, None)
        .oneshot(get(Some("attacker.example"), "/api/auth/me"))
        .await
        .expect("response");

    assert_eq!(response.status(), StatusCode::OK);
}
