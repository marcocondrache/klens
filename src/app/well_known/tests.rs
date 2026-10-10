use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use serde_json::json;

use crate::app::AuthState;
use crate::testing::{FakeCluster, TestApp};

const PROBES: &[&str] = &[
    "/.well-known/oauth-protected-resource/mcp",
    "/.well-known/oauth-protected-resource",
    "/.well-known/oauth-authorization-server",
    "/.well-known/openid-configuration",
    "/.well-known/",
    "/.well-known",
];

fn get(host: &str, path: &str) -> Request<Body> {
    Request::get(path)
        .header(header::HOST, host)
        .body(Body::empty())
        .expect("request")
}

async fn assert_not_found(auth: AuthState) {
    let app = TestApp::of([FakeCluster::local()]).auth(auth).build();

    for path in PROBES {
        let body = app
            .reply_through_router(get("localhost", path))
            .await
            .expect(StatusCode::NOT_FOUND);

        assert_eq!(body, json!({ "error": "not found", "code": "NOT_FOUND" }));
    }
}

#[tokio::test]
async fn every_discovery_path_is_a_json_404_with_auth_off() {
    assert_not_found(AuthState::disabled()).await;
}

#[tokio::test]
async fn every_discovery_path_is_a_json_404_with_auth_on() {
    assert_not_found(AuthState::enabled_for_tests()).await;
}

#[tokio::test]
async fn every_discovery_path_refuses_a_host_off_the_list_while_auth_is_off() {
    let app = TestApp::of([FakeCluster::local()]).build();

    for path in PROBES {
        app.reply_through_router(get("attacker.example", path))
            .await
            .assert_error(StatusCode::FORBIDDEN, "HOST_NOT_ALLOWED");
    }
}
