use axum::body::{Body, to_bytes};
use axum::http::{HeaderMap, Method, Request};
use serde_json::{Value, json};
use tower::ServiceExt as _;

use super::*;
use crate::app::auth::AuthState;
use crate::app::auth::testing::{Idp, RESOURCE_HOST, Signer, mcp};
use crate::config::Mcp;
use crate::testing::{FakeCluster, TestApp};

mod flow;

const METADATA: &str = "/.well-known/oauth-authorization-server";

fn server(resource: &str) -> OAuth {
    OAuth::new(
        &Url::parse(resource).expect("a url"),
        crate::app::mcp::ceiling(&Mcp::default()),
        Duration::from_secs(60),
    )
}

struct Answer {
    status: StatusCode,
    headers: HeaderMap,
    body: Value,
}

async fn answer(response: Response) -> Answer {
    let status = response.status();
    let headers = response.headers().clone();
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("a body");
    Answer {
        status,
        headers,
        body: serde_json::from_slice(&body).unwrap_or(Value::Null),
    }
}

async fn served() -> (Idp, TestApp) {
    let a = Signer::a();
    let idp = Idp::start(&[&a], &[&a]).await;
    let mcp = mcp("");
    let app = TestApp::of([FakeCluster::local()])
        .auth(idp.auth(&mcp).await)
        .build()
        .serving_mcp(mcp);
    (idp, app)
}

fn get(path: &str) -> Request<Body> {
    Request::get(path)
        .header(header::HOST, RESOURCE_HOST)
        .body(Body::empty())
        .expect("request")
}

fn register(host: &str, body: impl Into<Body>) -> Request<Body> {
    Request::builder()
        .method(Method::POST)
        .uri("/api/oauth/register")
        .header(header::HOST, host)
        .header(header::CONTENT_TYPE, "application/json")
        .body(body.into())
        .expect("request")
}

#[tokio::test]
async fn the_server_metadata_names_the_endpoints_beside_the_resource() {
    let (_idp, app) = served().await;

    let answer = answer(app.send_through_router(get(METADATA)).await).await;

    assert_eq!(answer.status, StatusCode::OK, "{}", answer.body);
    assert_eq!(
        answer.body,
        json!({
            "issuer": "https://klens.example.com",
            "authorization_endpoint": "https://klens.example.com/api/oauth/authorize",
            "token_endpoint": "https://klens.example.com/api/oauth/token",
            "registration_endpoint": "https://klens.example.com/api/oauth/register",
            "response_types_supported": ["code"],
            "grant_types_supported": ["authorization_code"],
            "code_challenge_methods_supported": ["S256"],
            "token_endpoint_auth_methods_supported": ["none"],
            "authorization_response_iss_parameter_supported": true,
        })
    );
}

#[tokio::test]
async fn a_resource_under_a_prefix_puts_the_issuer_under_it() {
    let oauth = server("https://klens.example.com/kafka/mcp");

    for path in [METADATA, &format!("{METADATA}/kafka")] {
        let response = oauth.metadata().oneshot(get(path)).await.expect("response");
        let answer = answer(response).await;

        assert_eq!(answer.status, StatusCode::OK, "{path}");
        assert_eq!(answer.body["issuer"], "https://klens.example.com/kafka");
        assert_eq!(
            answer.body["token_endpoint"],
            "https://klens.example.com/kafka/api/oauth/token"
        );
    }
}

#[test]
fn the_issuer_drops_the_last_segment_of_the_resource() {
    for (resource, issuer) in [
        ("https://klens.example.com/mcp", "https://klens.example.com"),
        (
            "https://klens.example.com/mcp/",
            "https://klens.example.com",
        ),
        ("https://klens.example.com/", "https://klens.example.com"),
        ("http://localhost:8080/mcp", "http://localhost:8080"),
        (
            "https://klens.example.com/a/b/mcp",
            "https://klens.example.com/a/b",
        ),
    ] {
        let oauth = server(resource);

        assert_eq!(oauth.metadata["issuer"], issuer, "{resource}");
    }
}

#[tokio::test]
async fn a_client_registers_with_the_redirect_uris_klens_takes() {
    let (_idp, app) = served().await;
    let body = json!({
        "client_name": "Claude Code (klens)",
        "redirect_uris": ["http://localhost:54321/callback", "https://evil.example/callback"],
        "grant_types": ["authorization_code", "refresh_token"],
        "response_types": ["code"],
        "token_endpoint_auth_method": "client_secret_post",
        "scope": "everything",
    });

    let answer = answer(
        app.send_through_router(register(RESOURCE_HOST, body.to_string()))
            .await,
    )
    .await;

    assert_eq!(answer.status, StatusCode::CREATED, "{}", answer.body);
    assert_eq!(answer.headers[header::CACHE_CONTROL], "no-store");
    let client_id = answer.body["client_id"].as_str().expect("a client id");
    assert!(client_id.starts_with("klens_client_"), "{client_id}");
    assert!(answer.body["client_id_issued_at"].is_i64());
    let mut rest = answer.body.clone();
    for field in ["client_id", "client_id_issued_at"] {
        rest.as_object_mut().expect("an object").remove(field);
    }
    assert_eq!(
        rest,
        json!({
            "client_name": "Claude Code (klens)",
            "redirect_uris": ["http://localhost:54321/callback"],
            "grant_types": ["authorization_code"],
            "response_types": ["code"],
            "token_endpoint_auth_method": "none",
        })
    );
}

#[tokio::test]
async fn a_registration_may_fill_most_of_the_body_cap() {
    let (_idp, app) = served().await;
    let body = json!({
        "client_name": "x".repeat(4_000),
        "redirect_uris": ["http://localhost/"],
    });

    let answer = answer(
        app.send_through_router(register(RESOURCE_HOST, body.to_string()))
            .await,
    )
    .await;

    assert_eq!(answer.status, StatusCode::CREATED, "{}", answer.body);
    assert_eq!(answer.body["client_name"], "x".repeat(64));
}

#[tokio::test]
async fn a_registration_with_no_redirect_uri_klens_takes_is_refused() {
    let (_idp, app) = served().await;
    let body = json!({ "redirect_uris": ["https://evil.example/callback"] });

    let answer = answer(
        app.send_through_router(register(RESOURCE_HOST, body.to_string()))
            .await,
    )
    .await;

    assert_eq!(answer.status, StatusCode::BAD_REQUEST);
    assert_eq!(answer.body["error"], "invalid_redirect_uri");
    assert_eq!(answer.headers[header::CACHE_CONTROL], "no-store");
}

#[tokio::test]
async fn a_malformed_registration_is_refused_as_invalid_metadata() {
    let (_idp, app) = served().await;
    let too_big = json!({
        "client_name": "x".repeat(MAX_REGISTRATION_BYTES),
        "redirect_uris": ["http://localhost/"],
    });

    for body in [
        "{}".to_owned(),
        "not json".to_owned(),
        json!({ "redirect_uris": "http://localhost/" }).to_string(),
        json!({ "redirect_uris": ["http://localhost/"], "client_name": 7 }).to_string(),
        too_big.to_string(),
    ] {
        let answer = answer(
            app.send_through_router(register(RESOURCE_HOST, body.clone()))
                .await,
        )
        .await;

        assert_eq!(answer.status, StatusCode::BAD_REQUEST, "{body:.40}");
        assert_eq!(
            answer.body["error"], "invalid_client_metadata",
            "{body:.40}"
        );
    }
}

#[tokio::test]
async fn registration_is_not_found_without_mcp_or_without_auth() {
    for auth in [AuthState::enabled_for_tests(), AuthState::disabled()] {
        let app = TestApp::of([FakeCluster::local()]).auth(auth).build();
        let body = json!({ "redirect_uris": ["http://localhost/"] });

        app.reply_through_router(register("localhost", body.to_string()))
            .await
            .assert_error(StatusCode::NOT_FOUND, "NOT_FOUND");
    }
}
