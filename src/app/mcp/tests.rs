use axum::Router;
use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode, header};
use rmcp::model::Tool;
use serde_json::{Value, json};
use tower::ServiceExt as _;
use tracing::Level;

use super::{
    KlensMcp, MAX_QUERY_CHARS, MAX_REQUEST_BYTES, RESULT_BYTES, TOOLS, service, tool_list,
    tool_rights,
};
use crate::app::auth::access::{Privilege, PrivilegeSet};
use crate::app::{AppState, AuthState, Limits, router};
use crate::config::{AllowedHost, Config, Mcp, Tuning};
use crate::kafka::Clusters;
use crate::testing::{
    Api, FakeCluster, LogCapture, TestApp, access, mcp_request, offline_partition, partition, role,
    topic, viewer, yaml,
};

fn call_body(tool: &str, arguments: Value) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "tools/call",
        "params": { "name": tool, "arguments": arguments },
    })
}

async fn call(app: &TestApp, tool: &str, arguments: Value) -> Value {
    let params = json!({ "name": tool, "arguments": arguments });
    app.mcp("tools/call", params).await.ok()["result"].take()
}

async fn call_through_router(app: &TestApp, tool: &str, arguments: Value) -> Value {
    app.reply_through_router(mcp_request(&call_body(tool, arguments)))
        .await
        .ok()["result"]
        .take()
}

#[track_caller]
fn structured(result: &Value) -> Value {
    assert_eq!(result["isError"], false, "{result}");
    let text = result["content"][0]["text"].as_str().expect("a text copy");
    let text: Value = serde_json::from_str(text).expect("a json text copy");
    assert_eq!(text, result["structuredContent"]);
    text
}

#[track_caller]
fn refusal(result: &Value) -> Value {
    assert_eq!(result["isError"], true, "{result}");
    assert!(result.get("structuredContent").is_none(), "{result}");
    let text = result["content"][0]["text"].as_str().expect("a text copy");
    serde_json::from_str(text).expect("a json refusal")
}

fn naming(tool: &Tool, cluster: &str) -> Value {
    let mut arguments = json!({});
    let required = tool.input_schema.get("required").and_then(Value::as_array);
    for name in required.into_iter().flatten() {
        arguments[name.as_str().expect("a property name")] = json!("x");
    }
    arguments["cluster"] = json!(cluster);
    arguments
}

fn request_2026(method: &str, name: Option<&str>, params: Value) -> Request<Body> {
    let request = Request::post("/mcp")
        .header(header::HOST, "localhost:8080")
        .header(header::ACCEPT, "application/json, text/event-stream")
        .header(header::CONTENT_TYPE, "application/json")
        .header("mcp-protocol-version", "2026-07-28")
        .header("mcp-method", method);
    let request = match name {
        Some(name) => request.header("mcp-name", name),
        None => request,
    };
    let body = json!({ "jsonrpc": "2.0", "id": 1, "method": method, "params": params });
    request.body(Body::from(body.to_string())).expect("request")
}

fn meta_2026() -> Value {
    json!({
        "io.modelcontextprotocol/protocolVersion": "2026-07-28",
        "io.modelcontextprotocol/clientCapabilities": {},
        "io.modelcontextprotocol/clientInfo": { "name": "probe", "version": "1.0.0" },
    })
}

#[test]
fn the_tool_table_names_every_tool_the_router_serves() {
    let served: Vec<String> = KlensMcp::tools()
        .list_all()
        .into_iter()
        .map(|tool| tool.name.into_owned())
        .collect();

    assert_eq!(
        served,
        TOOLS.iter().map(|&(name, _)| name).collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn every_tool_opens_to_exactly_the_privilege_it_names() {
    let app = TestApp::local().await;
    let tools = KlensMcp::tools().list_all();
    let mut wrong = Vec::new();

    for held in std::iter::once(None).chain(Privilege::ALL.map(Some)) {
        let session = app.with_access(access([role("probe", PrivilegeSet::from_privileges(held))]));
        for &(name, needs) in TOOLS {
            let tool = tools.iter().find(|tool| tool.name == name).expect("a tool");
            let expected = if needs.is_none() || needs == held {
                Value::Null
            } else {
                json!("FORBIDDEN")
            };
            let result = call(&session, name, naming(tool, "local")).await;
            let code = match result["isError"].as_bool() {
                Some(true) => refusal(&result)["code"].take(),
                _ => Value::Null,
            };
            if code != expected {
                wrong.push(format!(
                    "{name} holding {held:?} answered {code}, not {expected}"
                ));
            }
        }
    }

    assert!(wrong.is_empty(), "\n{}", wrong.join("\n"));
}

#[test]
fn a_tool_names_the_privilege_it_lacks_on_a_cluster() {
    let tool = ("klens_probe", Some(Privilege::Records));
    let viewer = access([viewer()]);
    let reader = access([role(
        "reader",
        PrivilegeSet::from_privileges([Privilege::Records]),
    )]);

    let lacking = tool_rights(&viewer.cluster("local").expect("visible"), &tool);
    let holding = tool_rights(&reader.cluster("local").expect("visible"), &tool);

    assert_eq!(
        json!(lacking),
        json!({ "name": "klens_probe", "available": false, "needs": "RECORDS" })
    );
    assert_eq!(
        json!(holding),
        json!({ "name": "klens_probe", "available": true })
    );
}

#[test]
fn every_tool_is_a_titled_read_a_client_can_show() {
    for tool in KlensMcp::tools().list_all() {
        let name = &*tool.name;
        let annotations = tool.annotations.as_ref().expect("annotations");
        let description = tool.description.as_deref().expect("a description");

        assert!(name.starts_with("klens_") && name.len() <= 64, "{name}");
        assert!(
            name.bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte == b'_'),
            "{name}"
        );
        assert!(tool.title.is_some(), "{name}");
        assert_eq!(
            (
                annotations.read_only_hint,
                annotations.destructive_hint,
                annotations.idempotent_hint,
                annotations.open_world_hint,
            ),
            (Some(true), Some(false), Some(true), Some(false)),
            "{name}"
        );
        assert!(description.len() < 2048, "{name}");
        assert!(tool.output_schema.is_none(), "{name}");
    }
}

#[tokio::test]
async fn tools_list_serves_the_checked_in_snapshot() {
    let listed = TestApp::local()
        .await
        .mcp("tools/list", json!({}))
        .await
        .ok();
    let snapshot: Value = serde_json::from_str(&tool_list()).expect("json");

    assert_eq!(listed["result"]["tools"], snapshot["tools"]);
}

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
    assert!(initialized["result"]["instructions"].is_string());

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
async fn a_tool_call_that_skips_admission_is_a_wiring_error() {
    let app = TestApp::local().await;
    let unadmitted = Router::new().nest_service(
        "/mcp",
        service(app.state().clone(), &Config::default().allowed_hosts),
    );

    let response = unadmitted
        .oneshot(mcp_request(&call_body("klens_clusters", json!({}))))
        .await
        .expect("response");

    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    let reply: Value = serde_json::from_slice(&body).expect("json");
    assert_eq!(reply["error"]["code"], -32603, "{reply}");
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
async fn with_auth_on_mcp_admits_nobody() {
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
async fn the_ceiling_holds_through_the_router() {
    let app = TestApp::of([FakeCluster::local(), FakeCluster::named("other")])
        .ingested()
        .await
        .serving_mcp(yaml("{privileges: [acls], clusters: [other]}"));

    let explained = structured(&call_through_router(&app, "klens_access_explain", json!({})).await);

    let clusters = explained["clusters"].as_array().expect("clusters");
    assert_eq!(clusters.len(), 1, "{explained}");
    assert_eq!(clusters[0]["cluster"], "other");
    assert_eq!(clusters[0]["privileges"], json!(["ACLS"]));
    for tool in KlensMcp::tools().list_all() {
        let refused = refusal(&call_through_router(&app, &tool.name, naming(&tool, "local")).await);
        assert_eq!(refused["code"], "UNKNOWN_CLUSTER", "{}", tool.name);
    }
}

#[tokio::test]
async fn every_tool_hides_a_cluster_the_caller_cannot_see() {
    let app = TestApp::of([FakeCluster::local(), FakeCluster::named("prod")])
        .ingested()
        .await
        .with_access(access([viewer().on(&["local"])]));

    for tool in KlensMcp::tools().list_all() {
        let refused = refusal(&call(&app, &tool.name, naming(&tool, "prod")).await);
        assert_eq!(refused["code"], "UNKNOWN_CLUSTER", "{}", tool.name);
    }
}

#[tokio::test]
async fn the_clusters_tool_reads_health_from_the_snapshot() {
    let app = TestApp::local().await;

    let listed = structured(&call(&app, "klens_clusters", json!({})).await);

    assert_eq!(
        listed,
        json!({ "clusters": [{
            "cluster": "local",
            "ready": true,
            "brokerCount": 1,
            "topicCount": 1,
            "partitionCount": 2,
            "groupCount": 1,
            "subjectCount": 1,
            "underReplicatedPartitions": 0,
            "offlinePartitions": 0,
            "unhealthyLanes": [],
        }]})
    );
    assert_eq!(app.cluster().calls(Api::Metadata), 0);
}

#[tokio::test]
async fn naming_a_cluster_lists_its_unhealthy_partitions_offline_first() {
    let cluster = FakeCluster::local();
    cluster.put_topic(topic(
        "payments",
        vec![
            partition(0, vec![1, 2], vec![1]),
            offline_partition(1, vec![1, 2]),
            partition(2, vec![1, 2], vec![1, 2]),
        ],
    ));
    let app = TestApp::over(cluster).await;

    let detail = structured(&call(&app, "klens_clusters", json!({ "cluster": "local" })).await);

    assert_eq!(detail["cluster"], "local");
    assert_eq!(detail["offlinePartitions"], 1);
    assert_eq!(
        detail["unhealthyPartitions"],
        json!([
            { "topic": "payments", "partition": 1, "leader": null, "replicas": [1, 2], "isr": [], "offline": true },
            { "topic": "payments", "partition": 0, "leader": 1, "replicas": [1, 2], "isr": [1], "offline": false },
        ])
    );
    assert!(detail.get("truncated").is_none());
}

#[tokio::test]
async fn a_result_past_the_budget_keeps_what_fits_and_says_so() {
    let cluster = FakeCluster::local();
    cluster.put_topic(topic(
        "payments",
        (0..2000)
            .map(|id| offline_partition(id, vec![1, 2, 3]))
            .collect(),
    ));
    let app = TestApp::over(cluster).await;

    let result = call(&app, "klens_clusters", json!({ "cluster": "local" })).await;

    let detail = structured(&result);
    let shown = detail["unhealthyPartitions"]
        .as_array()
        .expect("partitions")
        .len();
    assert!(0 < shown && shown < 2000, "{shown}");
    assert_eq!(
        detail["truncated"],
        format!(
            "{} of 2000 left out to fit the result; the counts above cover every partition",
            2000 - shown
        )
    );
    assert_eq!(detail["offlinePartitions"], 2000);
    assert!(serde_json::to_vec(&result).expect("json").len() <= RESULT_BYTES);
}

#[tokio::test]
async fn a_cluster_klens_has_not_read_is_not_ready() {
    let cluster = FakeCluster::local();
    let app = TestApp::of([cluster.clone()]).build();

    let unread = refusal(&call(&app, "klens_clusters", json!({ "cluster": "local" })).await);
    assert_eq!(unread["code"], "NOT_READY");
    assert_eq!(unread["error"], "klens has not read cluster 'local' yet");

    cluster.fail(Api::Metadata, "brokers unreachable");
    let rig = app.rig();
    rig.poll(&rig.topology()).await;

    let failed = refusal(&call(&app, "klens_clusters", json!({ "cluster": "local" })).await);
    let listed = structured(&call(&app, "klens_clusters", json!({})).await);
    assert_eq!(failed["code"], "NOT_READY");
    let error = failed["error"].as_str().expect("error");
    assert!(
        error.starts_with("klens has not read cluster 'local' yet; its last attempt failed: ")
            && error.contains("brokers unreachable"),
        "{error}"
    );
    let row = &listed["clusters"][0];
    assert_eq!(row["ready"], false);
    assert_eq!(row["topicCount"], Value::Null);
    assert_eq!(row["subjectCount"], Value::Null);
    assert_eq!(row["unhealthyLanes"][0]["lane"], "topology");
    assert!(
        row["unhealthyLanes"][0]["lastError"]
            .as_str()
            .is_some_and(|error| error.contains("brokers unreachable")),
        "{row}"
    );
}

#[tokio::test]
async fn search_names_a_cluster_it_has_not_read_instead_of_matching_nothing() {
    let app = TestApp::of([FakeCluster::local(), FakeCluster::named("unread")]).build();
    app.rig().ingest().await;

    let found = structured(&call(&app, "klens_search", json!({ "query": "orders" })).await);
    let refused = refusal(
        &call(
            &app,
            "klens_search",
            json!({ "query": "orders", "cluster": "unread" }),
        )
        .await,
    );

    let hits = found["hits"].as_array().expect("hits");
    assert!(!hits.is_empty());
    assert!(hits.iter().all(|hit| hit["cluster"] == "local"), "{found}");
    assert!(hits.iter().any(|hit| hit["kind"] == "TOPIC"), "{found}");
    assert_eq!(
        found["notReady"],
        json!([{ "cluster": "unread", "lane": "topology", "lastError": null }])
    );
    assert_eq!(refused["code"], "NOT_READY");
}

#[tokio::test]
async fn search_names_schema_subjects_it_has_not_read() {
    let app = TestApp::of([FakeCluster::local()]).build();
    let rig = app.rig();
    rig.poll(&rig.topology()).await;

    let everywhere = structured(&call(&app, "klens_search", json!({ "query": "orders" })).await);
    let named = structured(
        &call(
            &app,
            "klens_search",
            json!({ "query": "orders", "cluster": "local" }),
        )
        .await,
    );
    let listed = structured(&call(&app, "klens_clusters", json!({})).await);
    rig.poll(&rig.subjects()).await;
    let read = structured(&call(&app, "klens_search", json!({ "query": "orders" })).await);

    let kinds = |found: &Value| -> Vec<Value> {
        found["hits"]
            .as_array()
            .expect("hits")
            .iter()
            .map(|hit| hit["kind"].clone())
            .collect()
    };
    let unread = json!([{ "cluster": "local", "lane": "subjects", "lastError": null }]);
    assert!(kinds(&everywhere).contains(&json!("TOPIC")), "{everywhere}");
    assert!(
        !kinds(&everywhere).contains(&json!("SUBJECT")),
        "{everywhere}"
    );
    assert_eq!(everywhere["notReady"], unread);
    assert_eq!(named["notReady"], unread);
    assert_eq!(listed["clusters"][0]["topicCount"], 1);
    assert_eq!(listed["clusters"][0]["subjectCount"], Value::Null);
    assert!(kinds(&read).contains(&json!("SUBJECT")), "{read}");
    assert!(read.get("notReady").is_none(), "{read}");
}

#[tokio::test]
async fn a_query_past_the_cap_is_refused() {
    let app = TestApp::local().await;

    let longest = call(
        &app,
        "klens_search",
        json!({ "query": "ö".repeat(MAX_QUERY_CHARS) }),
    )
    .await;
    let longer = call(
        &app,
        "klens_search",
        json!({ "query": "o".repeat(MAX_QUERY_CHARS + 1) }),
    )
    .await;

    structured(&longest);
    let refused = refusal(&longer);
    assert_eq!(refused["code"], "INVALID_REQUEST");
    assert_eq!(refused["error"], "a query holds at most 256 characters");
}

#[tokio::test]
async fn access_explain_reports_privileges_under_the_ceiling() {
    let operator = PrivilegeSet::from_privileges([Privilege::Records, Privilege::Produce]);
    let app = TestApp::of([FakeCluster::local(), FakeCluster::named("prod")])
        .writable(&["local"])
        .ingested()
        .await
        .with_access(access([
            role("operator", operator).on(&["local"]),
            viewer().on(&["prod"]),
        ]));

    let explained = structured(&call(&app, "klens_access_explain", json!({})).await);
    let one = structured(&call(&app, "klens_access_explain", json!({ "cluster": "prod" })).await);

    let tools = json!([
        { "name": "klens_access_explain", "available": true },
        { "name": "klens_clusters", "available": true },
        { "name": "klens_search", "available": true },
    ]);
    assert_eq!(
        explained,
        json!({ "clusters": [
            { "cluster": "local", "writable": true, "privileges": ["RECORDS"], "tools": tools },
            { "cluster": "prod", "writable": false, "privileges": [], "tools": tools },
        ]})
    );
    assert_eq!(one["clusters"], json!([explained["clusters"][1]]));
}

#[tokio::test]
async fn a_refusal_carries_its_code_and_the_tool_that_helps() {
    let app = TestApp::local().await;

    let refused = refusal(
        &call(
            &app,
            "klens_search",
            json!({ "query": "x", "cluster": "nope" }),
        )
        .await,
    );

    assert_eq!(refused["code"], "UNKNOWN_CLUSTER");
    assert!(
        refused["error"]
            .as_str()
            .is_some_and(|error| error.contains("nope"))
    );
    assert_eq!(
        refused["hint"],
        "Call klens_clusters for the names of the clusters you can see."
    );
}

#[tokio::test]
async fn arguments_that_miss_the_schema_are_refused_where_the_model_sees_them() {
    let app = TestApp::local().await;

    for arguments in [
        json!({}),
        json!({ "query": 7 }),
        json!({ "query": "orders", "clusters": "local" }),
    ] {
        let refused = refusal(&call(&app, "klens_search", arguments).await);

        assert_eq!(refused["code"], "INVALID_REQUEST", "{refused}");
        assert_eq!(
            refused["hint"],
            "Fix the arguments to match the tool's input schema, then call again."
        );
    }
}

#[tokio::test]
async fn an_unknown_tool_is_a_protocol_error() {
    let reply = TestApp::local()
        .await
        .mcp(
            "tools/call",
            json!({ "name": "klens_nope", "arguments": {} }),
        )
        .await
        .ok();

    assert_eq!(reply["error"]["code"], -32602, "{reply}");
}

#[tokio::test]
async fn a_call_past_the_limit_is_refused_rather_than_queued() {
    let app = TestApp::of([FakeCluster::local()])
        .limits(Limits {
            mcp_calls: 1,
            ..Limits::new(&Tuning::default())
        })
        .ingested()
        .await;

    let held = app.state().mcp_permit().expect("a free call");
    let refused = refusal(&call(&app, "klens_clusters", json!({})).await);
    drop(held);
    let served = call(&app, "klens_clusters", json!({})).await;

    assert_eq!(refused["code"], "RATE_LIMITED");
    structured(&served);
}

#[tokio::test]
async fn a_refused_call_logs_its_tool_and_code_but_not_its_arguments() {
    let app = TestApp::local().await.serving_mcp(Mcp::default());
    let logs = LogCapture::at(Level::INFO);

    let params = json!({
        "name": "klens_search",
        "arguments": { "query": "card 4111", "cluster": "nope" },
        "_meta": meta_2026(),
    });
    let reply = app
        .reply_through_router(request_2026("tools/call", Some("klens_search"), params))
        .await
        .ok();
    refusal(&reply["result"]);

    logs.assert_contains(r#"mcp.tool{tool="klens_search" client="unverified:probe"}"#);
    logs.assert_contains(r#"refused a tool call code="UNKNOWN_CLUSTER""#);
    logs.assert_lacks("4111");
}

#[test]
fn mcp_reaches_clusters_only_through_the_session() {
    for (file, source) in [
        ("mcp.rs", include_str!("../mcp.rs")),
        ("mcp/types.rs", include_str!("types.rs")),
    ] {
        for unchecked in [concat!("state", ".clusters"), concat!("Cluster", "Session")] {
            assert!(!source.contains(unchecked), "{file} names {unchecked}");
        }
    }
}
