use super::*;

#[tokio::test]
async fn a_2025_client_initializes_and_calls_with_no_session() {
    let app = TestApp::local().await.serving_mcp(Mcp::default());

    let initialize = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {
            "protocolVersion": "2025-06-18",
            "capabilities": {},
            "clientInfo": { "name": "probe", "version": "1.0.0" },
        },
    });
    let response = app.send_through_router(mcp_request(&initialize)).await;
    assert_eq!(response.status(), StatusCode::OK);
    assert!(response.headers().get("mcp-session-id").is_none());
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    let initialized: Value = serde_json::from_slice(&body).expect("json");
    assert_eq!(initialized["result"]["protocolVersion"], "2025-06-18");
    assert_eq!(initialized["result"]["serverInfo"]["name"], "klens");
    assert!(initialized["result"]["capabilities"]["tools"].is_object());
    let instructions = initialized["result"]["instructions"]
        .as_str()
        .expect("instructions");
    assert!(instructions.len() < 2048, "{}", instructions.len());

    let notified = app
        .send_through_router(mcp_request(
            &json!({ "jsonrpc": "2.0", "method": "notifications/initialized" }),
        ))
        .await;
    assert_eq!(notified.status(), StatusCode::ACCEPTED);

    let clusters = structured(&call_through_router(&app, "klens_clusters", json!({})).await);
    assert_eq!(clusters["clusters"][0]["cluster"], "local");
}

#[tokio::test]
async fn a_2026_client_discovers_and_calls_with_no_handshake() {
    let app = TestApp::local().await.serving_mcp(Mcp::default());

    let discovered = app
        .reply_through_router(request_2026(
            "server/discover",
            None,
            json!({ "_meta": meta_2026() }),
        ))
        .await
        .ok();
    let result = &discovered["result"];
    assert_eq!(
        result["_meta"]["io.modelcontextprotocol/serverInfo"]["name"],
        "klens"
    );
    assert!(
        result["supportedVersions"]
            .as_array()
            .expect("versions")
            .contains(&json!("2026-07-28")),
        "{result}"
    );

    let params = json!({
        "name": "klens_search",
        "arguments": { "query": "orders" },
        "_meta": meta_2026(),
    });
    let found = app
        .reply_through_router(request_2026("tools/call", Some("klens_search"), params))
        .await
        .ok();
    let found = structured(&found["result"]);
    assert_eq!(found["hits"][0]["cluster"], "local");
}

#[tokio::test]
async fn a_foreign_host_or_any_browser_origin_is_refused() {
    let app = TestApp::local().await.serving_mcp(Mcp::default());

    for (host, origin) in [
        ("attacker.example", None),
        ("localhost.attacker.example:8080", None),
        ("localhost:8080", Some("http://localhost:8080")),
        ("localhost:8080", Some("https://attacker.example")),
        ("localhost:8080", Some("null")),
    ] {
        let mut request = mcp_request(&call_body("klens_clusters", json!({})));
        request
            .headers_mut()
            .insert(header::HOST, host.parse().expect("host"));
        if let Some(origin) = origin {
            request
                .headers_mut()
                .insert(header::ORIGIN, origin.parse().expect("origin"));
        }

        let response = app.send_through_router(request).await;

        assert_eq!(
            response.status(),
            StatusCode::FORBIDDEN,
            "{host} {origin:?}"
        );
    }
}

#[tokio::test]
async fn mcp_answers_the_configured_hosts_and_no_others() {
    let app = TestApp::local().await;
    let hosts: Vec<AllowedHost> = yaml("[klens.internal]");
    let served = router(app.state().clone(), &hosts, Some(&Mcp::default()));

    for (host, status, code) in [
        ("klens.internal", StatusCode::OK, Value::Null),
        ("klens.internal:8080", StatusCode::OK, Value::Null),
        (
            "localhost",
            StatusCode::FORBIDDEN,
            json!("HOST_NOT_ALLOWED"),
        ),
    ] {
        let mut request = mcp_request(&call_body("klens_clusters", json!({})));
        request
            .headers_mut()
            .insert(header::HOST, host.parse().expect("host"));

        let response = served.clone().oneshot(request).await.expect("response");

        assert_eq!(response.status(), status, "{host}");
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        let body: Value = serde_json::from_slice(&body).expect("json");
        assert_eq!(body["code"], code, "{host}");
    }
}

#[tokio::test]
async fn a_body_past_the_limit_is_refused_before_any_tool_runs() {
    let app = TestApp::local().await.serving_mcp(Mcp::default());
    let body = call_body(
        "klens_search",
        json!({ "query": "o".repeat(MAX_REQUEST_BYTES) }),
    );

    let response = app.send_through_router(mcp_request(&body)).await;

    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
}

#[tokio::test]
async fn a_request_that_skips_admission_is_a_wiring_error() {
    let app = TestApp::local().await;
    let unadmitted = Router::new().nest_service(
        "/mcp",
        service(
            app.state().clone(),
            browser_checks(
                Config::default()
                    .allowed_hosts
                    .iter()
                    .map(ToString::to_string),
                [],
            ),
        ),
    );
    let list = json!({ "jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {} });

    for body in [call_body("klens_clusters", json!({})), list] {
        let response = unadmitted
            .clone()
            .oneshot(mcp_request(&body))
            .await
            .expect("response");

        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        let reply: Value = serde_json::from_slice(&body).expect("json");
        assert_eq!(reply["error"]["code"], -32603, "{reply}");
    }
}

#[tokio::test]
async fn only_post_is_served() {
    let app = TestApp::local().await.serving_mcp(Mcp::default());

    for request in [Request::get("/mcp"), Request::delete("/mcp")] {
        let request = request
            .header(header::HOST, "localhost")
            .header(header::ACCEPT, "application/json, text/event-stream")
            .body(Body::empty())
            .expect("request");

        let response = app.send_through_router(request).await;

        assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
        assert_eq!(response.headers()[header::ALLOW], "POST");
    }
}

#[tokio::test]
async fn without_an_mcp_block_only_the_ui_answers_at_mcp() {
    let app = TestApp::local().await;

    let response = app
        .send_through_router(mcp_request(&call_body("klens_clusters", json!({}))))
        .await;

    assert!(
        response.headers()[header::CONTENT_TYPE]
            .to_str()
            .is_ok_and(|kind| kind.starts_with("text/html")),
        "{:?}",
        response.headers()
    );
}

#[tokio::test]
async fn an_auth_state_without_a_token_check_admits_nobody_at_mcp() {
    let state = AppState::new(
        Clusters::from_sessions(vec![FakeCluster::local()]),
        AuthState::enabled_for_tests(),
        Limits::new(&Tuning::default()),
    );

    let response = router(
        state,
        &Config::default().allowed_hosts,
        Some(&Mcp::default()),
    )
    .oneshot(mcp_request(&call_body("klens_clusters", json!({}))))
    .await
    .expect("response");

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn with_auth_on_mcp_answers_only_the_resource_host_and_listed_origins() {
    let a = Signer::a();
    let idp = Idp::start(&[&a], &[&a]).await;
    let mcp = for_resource("allowed_origins: ['https://claude.ai:443']");
    let app = TestApp::of([FakeCluster::local()])
        .auth(idp.auth(&mcp).await)
        .build()
        .serving_mcp(mcp);
    let token = a.sign(&idp.claims());
    let list = json!({ "jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {} });

    for (host, origin, status) in [
        (RESOURCE_HOST, None, StatusCode::OK),
        (RESOURCE_HOST, Some("https://claude.ai"), StatusCode::OK),
        ("localhost", None, StatusCode::FORBIDDEN),
        (
            RESOURCE_HOST,
            Some("http://claude.ai"),
            StatusCode::FORBIDDEN,
        ),
        (
            RESOURCE_HOST,
            Some("https://evil.example"),
            StatusCode::FORBIDDEN,
        ),
    ] {
        let mut request = bearing(mcp_request(&list), &token);
        let headers = request.headers_mut();
        headers.insert(header::HOST, host.parse().expect("a host"));
        if let Some(origin) = origin {
            headers.insert(header::ORIGIN, origin.parse().expect("an origin"));
        }

        let response = app.send_through_router(request).await;

        assert_eq!(response.status(), status, "{host} {origin:?}");
    }
}

const GATEWAY_SECRET: &str = "gateway-secret-of-at-least-32-bytes";

fn behind_gateway() -> Mcp {
    yaml(&format!("secret: {{value: {GATEWAY_SECRET}}}"))
}

fn from_gateway(authorization: &[&str]) -> Request<Body> {
    let mut request = mcp_request(&call_body("klens_clusters", json!({})));
    for value in authorization {
        request
            .headers_mut()
            .append(header::AUTHORIZATION, value.parse().expect("a header"));
    }
    request
}

#[tokio::test]
async fn with_a_secret_only_a_call_that_carries_it_reaches_a_tool() {
    let app = TestApp::local().await.serving_mcp(behind_gateway());
    let bearer = format!("Bearer {GATEWAY_SECRET}");

    for authorization in [
        vec![],
        vec!["Bearer not-the-gateway-secret-of-32-bytes"],
        vec![&format!("Basic {GATEWAY_SECRET}")],
        vec![&bearer, &bearer],
    ] {
        let reply = app.reply_through_router(from_gateway(&authorization)).await;

        assert_eq!(reply.status, StatusCode::FORBIDDEN, "{authorization:?}");
        assert_eq!(reply.body["code"], "NOT_FROM_GATEWAY", "{authorization:?}");
    }
    let called = app
        .reply_through_router(from_gateway(&[&format!("bearer  {GATEWAY_SECRET}")]))
        .await
        .ok();
    assert_eq!(
        structured(&called["result"])["clusters"][0]["cluster"],
        "local"
    );
}

#[tokio::test]
async fn with_a_secret_any_host_and_browser_origin_pass_through_the_gateway() {
    let app = TestApp::local().await.serving_mcp(behind_gateway());
    let mut request = from_gateway(&[&format!("Bearer {GATEWAY_SECRET}")]);
    let headers = request.headers_mut();
    headers.insert(header::HOST, "klens.example.com".parse().expect("host"));
    headers.insert(header::ORIGIN, "https://claude.ai".parse().expect("origin"));

    let response = app.send_through_router(request).await;

    assert_eq!(response.status(), StatusCode::OK);
}
