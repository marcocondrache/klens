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
    let response = app.send_to_mcp(mcp_request(&initialize)).await;
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
        .send_to_mcp(mcp_request(
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
        .reply_to_mcp(request_2026(
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
        .reply_to_mcp(request_2026("tools/call", Some("klens_search"), params))
        .await
        .ok();
    let found = structured(&found["result"]);
    assert_eq!(found["hits"][0]["cluster"], "local");
}

#[tokio::test]
async fn any_host_is_served_and_any_browser_origin_refused() {
    let app = TestApp::local().await.serving_mcp(Mcp::default());

    for (host, origin, status) in [
        ("klens.internal:8090", None, StatusCode::OK),
        ("attacker.example", None, StatusCode::OK),
        (
            "localhost:8090",
            Some("http://localhost:8090"),
            StatusCode::FORBIDDEN,
        ),
        (
            "attacker.example",
            Some("https://attacker.example"),
            StatusCode::FORBIDDEN,
        ),
        ("localhost:8090", Some("null"), StatusCode::FORBIDDEN),
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

        let response = app.send_to_mcp(request).await;

        assert_eq!(response.status(), status, "{host} {origin:?}");
    }
}

#[tokio::test]
async fn a_body_past_the_limit_is_refused_before_any_tool_runs() {
    let app = TestApp::local().await.serving_mcp(Mcp::default());
    let body = call_body(
        "klens_search",
        json!({ "query": "o".repeat(MAX_REQUEST_BYTES) }),
    );

    let response = app.send_to_mcp(mcp_request(&body)).await;

    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
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

        let response = app.send_to_mcp(request).await;

        assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
        assert_eq!(response.headers()[header::ALLOW], "POST");
    }
}

#[tokio::test]
async fn the_ui_router_never_answers_at_mcp() {
    let app = TestApp::local().await.serving_mcp(Mcp::default());

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
async fn with_auth_on_and_no_secret_mcp_still_answers_calls() {
    let app = TestApp::of([FakeCluster::local()])
        .auth(AuthState::enabled_for_tests())
        .build()
        .serving_mcp(Mcp::default());

    let reply = app
        .reply_to_mcp(mcp_request(&call_body("klens_clusters", json!({}))))
        .await;

    assert_eq!(reply.status, StatusCode::OK);
}

fn carrying(authorization: &[&str]) -> Request<Body> {
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
    let app = TestApp::local()
        .await
        .serving_mcp(behind_gateway(Mcp::default()));
    let bearer = format!("Bearer {GATEWAY_SECRET}");

    for authorization in [
        vec![],
        vec!["Bearer not-the-gateway-secret-of-32-bytes"],
        vec![&format!("Basic {GATEWAY_SECRET}")],
        vec![&bearer, &bearer],
    ] {
        let reply = app.reply_to_mcp(carrying(&authorization)).await;

        assert_eq!(reply.status, StatusCode::FORBIDDEN, "{authorization:?}");
        assert_eq!(reply.body["code"], "NOT_FROM_GATEWAY", "{authorization:?}");
    }
    let called = app
        .reply_to_mcp(carrying(&[&format!("bearer  {GATEWAY_SECRET}")]))
        .await
        .ok();
    assert_eq!(
        structured(&called["result"])["clusters"][0]["cluster"],
        "local"
    );
}

#[tokio::test]
async fn with_a_secret_any_host_and_browser_origin_pass_through_the_gateway() {
    let app = TestApp::local()
        .await
        .serving_mcp(behind_gateway(Mcp::default()));
    let mut request = from_gateway(mcp_request(&call_body("klens_clusters", json!({}))));
    let headers = request.headers_mut();
    headers.insert(header::HOST, "klens.example.com".parse().expect("host"));
    headers.insert(header::ORIGIN, "https://claude.ai".parse().expect("origin"));

    let response = app.send_to_mcp(request).await;

    assert_eq!(response.status(), StatusCode::OK);
}
