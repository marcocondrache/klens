use std::num::NonZeroU32;
use std::path::Path;

use axum::Router;
use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode, header};
use rmcp::model::Tool;
use serde_json::{Value, json};
use tower::ServiceExt as _;
use tracing::Level;
use walkdir::WalkDir;

use super::{
    CLIENT_VALUES_NOTICE, KlensMcp, MAX_QUERY_CHARS, MAX_REQUEST_BYTES, RESULT_BYTES, TOOLS, limit,
    listed, service, tool_list, tool_rights,
};
use crate::app::auth::access::{Privilege, PrivilegeSet};
use crate::app::{AppState, AuthState, Limits, router};
use crate::config::{AllowedHost, Config, Mcp, Tuning};
use crate::kafka::Clusters;
use crate::kafka::model::{GroupSnapshot, Watermarks};
use crate::testing::{
    Api, FakeCluster, LogCapture, TestApp, access, group, mcp_request, offline_partition,
    partition, role, subject, topic, viewer, yaml,
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

/// Names what the local world holds, so each tool reaches its result.
fn naming(tool: &Tool, cluster: &str) -> Value {
    let mut arguments = json!({});
    let required = tool.input_schema.get("required").and_then(Value::as_array);
    for name in required.into_iter().flatten() {
        let name = name.as_str().expect("a property name");
        arguments[name] = match name {
            "topic" => json!("orders.created"),
            "group" => json!("order-processor"),
            _ => json!("x"),
        };
    }
    arguments["cluster"] = json!(cluster);
    arguments
}

fn names(listed: &Value, rows: &str, key: &str) -> Vec<String> {
    listed[rows]
        .as_array()
        .expect("rows")
        .iter()
        .map(|row| row[key].as_str().expect("a name").to_owned())
        .collect()
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
    assert_eq!(
        unread["error"],
        "klens has not read the topology of cluster 'local' yet"
    );

    cluster.fail(Api::Metadata, "brokers unreachable");
    let rig = app.rig();
    rig.poll(&rig.topology()).await;

    let failed = refusal(&call(&app, "klens_clusters", json!({ "cluster": "local" })).await);
    let listed = structured(&call(&app, "klens_clusters", json!({})).await);
    assert_eq!(failed["code"], "NOT_READY");
    let error = failed["error"].as_str().expect("error");
    assert!(
        error.starts_with(
            "klens has not read the topology of cluster 'local' yet; its last attempt failed: "
        ) && error.contains("brokers unreachable"),
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
        { "name": "klens_brokers_list", "available": true },
        { "name": "klens_clusters", "available": true },
        { "name": "klens_group_describe", "available": true },
        { "name": "klens_groups_list", "available": true },
        { "name": "klens_schemas_list", "available": true },
        { "name": "klens_search", "available": true },
        { "name": "klens_topic_describe", "available": true },
        { "name": "klens_topics_list", "available": true },
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
fn a_list_returns_25_rows_unless_asked_and_between_1_and_100() {
    assert_eq!(limit(None), 25);
    assert_eq!(limit(Some(0)), 1);
    assert_eq!(limit(Some(3)), 3);
    assert_eq!(limit(Some(100)), 100);
    assert_eq!(limit(Some(101)), 100);
}

#[test]
fn a_list_says_whether_its_limit_or_the_result_size_cut_it() {
    let showing = |rows: usize, asked: usize, narrow: Option<&str>| {
        let rows = vec!["x".repeat(1000); rows];
        let result = listed(
            rows,
            Some(asked),
            narrow,
            |rows, showing| json!({ "rows": rows, "showing": showing }),
        );
        let result = serde_json::to_value(result).expect("json");
        result["structuredContent"]["showing"]
            .as_str()
            .expect("showing")
            .to_owned()
    };
    let narrow = Some("pass `nameContains`");

    assert_eq!(showing(5, 25, narrow), "5 of 5");
    assert_eq!(
        showing(5, 3, narrow),
        "3 of 5; pass `nameContains`, or raise `limit`, to see others"
    );
    assert_eq!(showing(5, 3, None), "3 of 5; raise `limit` to see others");
    for (narrow, rest) in [
        (
            narrow,
            "50, as no more fit the result; pass `nameContains` to see others",
        ),
        (None, "50, as no more fit the result"),
    ] {
        let cut = showing(50, 100, narrow);
        let (shown, after) = cut.split_once(" of ").expect("a count");
        assert!(
            shown.parse::<usize>().is_ok_and(|shown| shown < 50),
            "{cut}"
        );
        assert_eq!(after, rest);
    }
}

#[tokio::test]
async fn a_cluster_tool_needs_no_cluster_when_the_caller_sees_only_one() {
    let app = TestApp::of([FakeCluster::local(), FakeCluster::named("prod")])
        .ingested()
        .await;
    let local_only = app.with_access(access([viewer().on(&["local"])]));
    let blind = app.with_access(access(Vec::new()));

    let served = structured(&call(&local_only, "klens_brokers_list", json!({})).await);
    let ambiguous = refusal(&call(&app, "klens_brokers_list", json!({})).await);
    let nothing = refusal(&call(&blind, "klens_brokers_list", json!({})).await);

    assert_eq!(served["brokers"][0]["id"], 1);
    assert_eq!(ambiguous["code"], "INVALID_REQUEST");
    assert_eq!(ambiguous["error"], "pass `cluster` as one of local, prod");
    assert_eq!(nothing["code"], "INVALID_REQUEST");
    assert_eq!(nothing["error"], "you can see no cluster");
}

#[tokio::test]
async fn brokers_list_reads_hosts_and_log_dirs_from_the_snapshot() {
    let app = TestApp::local().await;

    let listed = structured(&call(&app, "klens_brokers_list", json!({})).await);

    assert_eq!(
        listed,
        json!({
            "brokers": [{
                "id": 1,
                "host": "localhost",
                "port": 9092,
                "rack": null,
                "controller": null,
                "partitionCount": 2,
                "leaderCount": 2,
                "sizeBytes": 6144,
                "logDirs": [{
                    "path": "/var/lib/kafka/data",
                    "error": null,
                    "totalBytes": 1_000_000,
                    "usableBytes": 750_000,
                    "cordoned": false,
                    "sizeBytes": 6144,
                    "replicaCount": 2,
                }],
            }],
            "showing": "1 of 1",
        })
    );
    assert_eq!(app.cluster().calls(Api::LogDirs), 0);
}

#[tokio::test]
async fn brokers_list_leaves_unread_log_dirs_null_and_stops_at_the_limit() {
    let cluster = FakeCluster::local();
    cluster.add_broker(2);
    cluster.add_broker(3);
    let app = TestApp::of([cluster]).build();
    let rig = app.rig();
    rig.poll(&rig.topology()).await;

    let listed = structured(&call(&app, "klens_brokers_list", json!({ "limit": 2 })).await);

    let brokers = listed["brokers"].as_array().expect("brokers");
    assert_eq!(brokers.len(), 2);
    assert_eq!(brokers[0]["sizeBytes"], Value::Null);
    assert_eq!(brokers[0]["logDirs"], Value::Null);
    assert_eq!(listed["showing"], "2 of 3; raise `limit` to see others");
}

#[tokio::test]
async fn schemas_list_gives_versions_and_ids_only_in_detail() {
    let app = TestApp::local().await;

    let concise = structured(&call(&app, "klens_schemas_list", json!({})).await);
    let detailed = structured(
        &call(
            &app,
            "klens_schemas_list",
            json!({ "responseFormat": "DETAILED" }),
        )
        .await,
    );

    let row = json!({
        "subject": "orders.created-value",
        "latestVersion": 2,
        "type": "AVRO",
        "compatibility": "BACKWARD",
    });
    assert_eq!(
        concise,
        json!({ "subjects": [row], "showing": "1 of 1", "notice": CLIENT_VALUES_NOTICE })
    );
    let mut full = row;
    full["latestSchemaId"] = json!(1);
    full["versions"] = json!([{ "version": 1, "id": null }, { "version": 2, "id": 1 }]);
    assert_eq!(detailed["subjects"], json!([full]));
}

#[tokio::test]
async fn schemas_list_filters_names_in_any_case() {
    let cluster = FakeCluster::local();
    cluster.set_subjects(vec![
        subject("orders.created-value", 1, 1),
        subject("payments-key", 2, 1),
        subject("Payments-value", 3, 1),
    ]);
    let app = TestApp::over(cluster).await;

    let payments = structured(
        &call(
            &app,
            "klens_schemas_list",
            json!({ "nameContains": "PAY", "limit": 1 }),
        )
        .await,
    );

    assert_eq!(payments["subjects"][0]["subject"], "Payments-value");
    assert_eq!(payments["subjects"].as_array().map(Vec::len), Some(1));
    assert_eq!(
        payments["showing"],
        "1 of 2; pass `nameContains`, or raise `limit`, to see others"
    );
}

#[tokio::test]
async fn schemas_list_says_when_there_is_no_registry_to_read() {
    let without = TestApp::over(FakeCluster::local().without_schema_registry()).await;
    let cluster = FakeCluster::local();
    let unread = TestApp::of([cluster.clone()]).build();
    let rig = unread.rig();
    rig.poll(&rig.topology()).await;

    let none = refusal(&call(&without, "klens_schemas_list", json!({})).await);
    let pending = refusal(&call(&unread, "klens_schemas_list", json!({})).await);
    cluster.fail(Api::SchemaSubjects, "registry down");
    rig.poll(&rig.subjects()).await;
    let failed = refusal(&call(&unread, "klens_schemas_list", json!({})).await);

    assert_eq!(none["code"], "NO_SCHEMA_REGISTRY");
    assert_eq!(
        none["hint"],
        "klens reads no schema registry for this cluster, so it knows no subjects or schemas there."
    );
    assert_eq!(pending["code"], "NOT_READY");
    assert_eq!(
        pending["error"],
        "klens has not read the subjects of cluster 'local' yet"
    );
    assert!(
        failed["error"]
            .as_str()
            .is_some_and(|error| error.starts_with(
                "klens has not read the subjects of cluster 'local' yet; its last attempt failed: "
            ) && error.contains("registry down")),
        "{failed}"
    );
}

#[tokio::test]
async fn search_marks_group_and_subject_names_as_client_data() {
    let app = TestApp::local().await;

    let orders = structured(&call(&app, "klens_search", json!({ "query": "order" })).await);
    let brokers = structured(&call(&app, "klens_search", json!({ "query": "localhost" })).await);

    let kinds: Vec<&Value> = orders["hits"]
        .as_array()
        .expect("hits")
        .iter()
        .map(|hit| &hit["kind"])
        .collect();
    assert!(kinds.contains(&&json!("GROUP")), "{orders}");
    assert_eq!(orders["notice"], CLIENT_VALUES_NOTICE);
    assert!(
        brokers["hits"]
            .as_array()
            .expect("hits")
            .iter()
            .all(|hit| hit["kind"] == "NODE"),
        "{brokers}"
    );
    assert!(brokers.get("notice").is_none(), "{brokers}");
}

fn catalog() -> FakeCluster {
    let cluster = FakeCluster::local()
        .with_topic("payments.settled", 3, 0)
        .with_groups([
            group("audit", "audit.log", vec![0]),
            group("ledger", "audit.log", vec![0]),
        ]);
    cluster.put_topic(topic("audit.log", vec![partition(0, vec![1, 2], vec![1])]));
    cluster.set_watermarks("audit.log", 0, Watermarks { low: 0, high: 5 });
    cluster.put_topic(topic(
        "__consumer_offsets",
        vec![partition(0, vec![1], vec![1])],
    ));
    cluster.set_watermarks("__consumer_offsets", 0, Watermarks { low: 0, high: 3 });
    cluster
}

#[tokio::test]
async fn topics_list_reads_each_topic_from_the_snapshot() {
    let app = TestApp::local().await;

    let listed = structured(&call(&app, "klens_topics_list", json!({})).await);

    assert_eq!(
        listed,
        json!({
            "topics": [{
                "name": "orders.created",
                "partitionCount": 2,
                "retainedMessages": 16,
                "sizeBytes": 6144,
                "rate": 0.0,
                "groupCount": 1,
                "underReplicated": false,
            }],
            "showing": "1 of 1",
        })
    );
    assert_eq!(app.cluster().calls(Api::Metadata), 0);
}

#[tokio::test]
async fn detailed_topic_rows_match_the_http_rows_once_measured() {
    let app = TestApp::over(catalog()).await;
    app.store().rates.set(&"orders.created".into(), 2.5);

    let listed = structured(
        &call(
            &app,
            "klens_topics_list",
            json!({ "responseFormat": "DETAILED", "includeInternal": true }),
        )
        .await,
    );
    let http = app.get("/clusters/local/topics").await.ok();

    assert_eq!(listed["topics"], http);
}

#[tokio::test]
async fn topic_counts_klens_has_not_read_are_null_not_zero() {
    let app = TestApp::of([FakeCluster::local()]).build();
    let rig = app.rig();
    rig.poll(&rig.topology()).await;

    let listed = structured(
        &call(
            &app,
            "klens_topics_list",
            json!({ "responseFormat": "DETAILED" }),
        )
        .await,
    );
    let described = structured(
        &call(
            &app,
            "klens_topic_describe",
            json!({ "topic": "orders.created" }),
        )
        .await,
    );

    let row = &listed["topics"][0];
    for key in ["retainedMessages", "producedTotal", "sizeBytes", "rate"] {
        assert_eq!(row[key], Value::Null, "{key}");
        assert_eq!(described[key], Value::Null, "{key}");
    }
    for key in [
        "lowWatermark",
        "highWatermark",
        "retainedMessages",
        "sizeBytes",
    ] {
        assert_eq!(described["partitions"][0][key], Value::Null, "{key}");
    }
    assert_eq!(described["subjects"], Value::Null);
    assert_eq!(
        app.get("/clusters/local/topics").await.ok()[0]["retainedMessages"],
        0,
        "the UI row reads an unread count as 0"
    );
}

#[tokio::test]
async fn a_topic_missing_one_partitions_watermarks_has_no_record_count() {
    let cluster = FakeCluster::local();
    let app = TestApp::over(cluster.clone()).await;
    cluster.put_topic(topic(
        "orders.created",
        vec![
            partition(0, vec![1], vec![1]),
            partition(1, vec![1], vec![1]),
            partition(2, vec![1], vec![1]),
        ],
    ));
    let rig = app.rig();
    rig.poll(&rig.topology()).await;

    let listed = structured(&call(&app, "klens_topics_list", json!({})).await);
    let described = structured(
        &call(
            &app,
            "klens_topic_describe",
            json!({ "topic": "orders.created" }),
        )
        .await,
    );

    assert_eq!(listed["topics"][0]["retainedMessages"], Value::Null);
    assert_eq!(described["retainedMessages"], Value::Null);
    assert_eq!(described["partitions"][0]["retainedMessages"], 8);
    assert_eq!(described["partitions"][2]["retainedMessages"], Value::Null);
}

#[tokio::test]
async fn topics_list_filters_by_name_health_emptiness_and_internal() {
    let app = TestApp::over(catalog()).await;
    let topics = async |arguments: Value| {
        let listed = structured(&call(&app, "klens_topics_list", arguments).await);
        names(&listed, "topics", "name")
    };

    assert_eq!(
        topics(json!({})).await,
        ["audit.log", "orders.created", "payments.settled"]
    );
    assert_eq!(
        topics(json!({ "includeInternal": true })).await,
        [
            "__consumer_offsets",
            "audit.log",
            "orders.created",
            "payments.settled"
        ]
    );
    assert_eq!(
        topics(json!({ "nameContains": "PAY" })).await,
        ["payments.settled"]
    );
    assert_eq!(
        topics(json!({ "underReplicated": true })).await,
        ["audit.log"]
    );
    assert_eq!(
        topics(json!({ "underReplicated": false })).await,
        ["orders.created", "payments.settled"]
    );
    assert_eq!(topics(json!({ "empty": true })).await, ["payments.settled"]);
    assert_eq!(
        topics(json!({ "empty": false })).await,
        ["audit.log", "orders.created"]
    );
}

#[tokio::test]
async fn the_empty_filter_counts_the_topics_it_cannot_judge() {
    let cluster = catalog();
    let app = TestApp::over(cluster.clone()).await;
    cluster.add_topic("fresh", 1, 0);
    let rig = app.rig();
    rig.poll(&rig.topology()).await;
    let list =
        async |arguments: Value| structured(&call(&app, "klens_topics_list", arguments).await);

    let unfiltered = list(json!({})).await;
    let full = list(json!({ "empty": false })).await;
    let bare = list(json!({ "empty": true })).await;

    assert_eq!(unfiltered["topics"][1]["name"], "fresh");
    assert_eq!(unfiltered["topics"][1]["retainedMessages"], Value::Null);
    assert!(unfiltered.get("unmeasured").is_none(), "{unfiltered}");
    assert_eq!(
        names(&full, "topics", "name"),
        ["audit.log", "orders.created"]
    );
    assert_eq!(full["unmeasured"], 1);
    assert_eq!(names(&bare, "topics", "name"), ["payments.settled"]);
    assert_eq!(bare["unmeasured"], 1);
}

#[tokio::test]
async fn topics_list_sorts_largest_first_with_unmeasured_values_last() {
    let app = TestApp::over(catalog()).await;
    app.store().rates.set(&"orders.created".into(), 2.0);
    app.store().rates.set(&"payments.settled".into(), 5.0);

    let mut sorted = Vec::new();
    for sort in ["NAME", "SIZE", "RATE", "RECORDS", "PARTITIONS", "GROUPS"] {
        let listed = structured(&call(&app, "klens_topics_list", json!({ "sort": sort })).await);
        sorted.push((sort, names(&listed, "topics", "name")));
    }

    assert_eq!(
        sorted,
        [
            (
                "NAME",
                vec!["audit.log", "orders.created", "payments.settled"]
            ),
            (
                "SIZE",
                vec!["orders.created", "audit.log", "payments.settled"]
            ),
            (
                "RATE",
                vec!["payments.settled", "orders.created", "audit.log"]
            ),
            (
                "RECORDS",
                vec!["orders.created", "audit.log", "payments.settled"]
            ),
            (
                "PARTITIONS",
                vec!["payments.settled", "orders.created", "audit.log"]
            ),
            (
                "GROUPS",
                vec!["audit.log", "orders.created", "payments.settled"]
            ),
        ]
        .map(|(sort, names)| (sort, names.into_iter().map(str::to_owned).collect()))
    );
}

#[tokio::test]
async fn topics_list_stops_at_the_limit_and_then_at_the_budget() {
    let cluster = FakeCluster::local();
    for id in 0..150 {
        cluster.add_topic(&format!("events.{id:03}"), 1, 1);
    }
    let app = TestApp::over(cluster).await;

    let first = structured(&call(&app, "klens_topics_list", json!({})).await);
    let result = call(&app, "klens_topics_list", json!({ "limit": 500 })).await;

    assert_eq!(first["topics"].as_array().map(Vec::len), Some(25));
    assert_eq!(
        first["showing"],
        "25 of 151; pass `nameContains` or a filter, or raise `limit`, to see others"
    );
    let most = structured(&result);
    let shown = most["topics"].as_array().expect("topics").len();
    assert!(25 < shown && shown < 100, "{shown}");
    assert_eq!(
        most["showing"],
        format!(
            "{shown} of 151, as no more fit the result; pass `nameContains` or a filter to see \
             others"
        )
    );
    assert!(serde_json::to_vec(&result).expect("json").len() <= RESULT_BYTES);
}

#[tokio::test]
async fn topic_describe_joins_partitions_groups_and_subjects() {
    let app = TestApp::local().await;

    let described = structured(
        &call(
            &app,
            "klens_topic_describe",
            json!({ "topic": "orders.created" }),
        )
        .await,
    );

    let partition = |id: i32, size: i64| {
        json!({
            "partition": id,
            "leader": 1,
            "replicas": [1],
            "isr": [1],
            "underReplicated": false,
            "offline": false,
            "lowWatermark": 0,
            "highWatermark": 8,
            "retainedMessages": 8,
            "sizeBytes": size,
        })
    };
    assert_eq!(
        described,
        json!({
            "name": "orders.created",
            "internal": false,
            "partitionCount": 2,
            "replicationFactor": 1,
            "retainedMessages": 16,
            "producedTotal": 16,
            "sizeBytes": 6144,
            "diskBytes": 6144,
            "rate": 0.0,
            "retentionMs": 604_800_000,
            "cleanupPolicy": "DELETE",
            "underReplicatedPartitions": 0,
            "offlinePartitions": 0,
            "groups": [{
                "id": "order-processor",
                "state": "STABLE",
                "memberCount": 1,
                "lagOnTopic": 5,
            }],
            "subjects": [{
                "subject": "orders.created-value",
                "latestVersion": 2,
                "type": "AVRO",
                "compatibility": "BACKWARD",
            }],
            "partitions": [partition(0, 4096), partition(1, 2048)],
            "notice": CLIENT_VALUES_NOTICE,
        })
    );
}

#[tokio::test]
async fn topic_describe_shows_unhealthy_partitions_and_no_subjects_without_a_registry() {
    let cluster = FakeCluster::local().without_schema_registry();
    cluster.put_topic(topic(
        "payments",
        vec![
            partition(0, vec![1, 2], vec![1]),
            offline_partition(1, vec![1, 2]),
        ],
    ));
    let app = TestApp::over(cluster).await;
    app.store().rates.set(&"payments".into(), 0.5);

    let described =
        structured(&call(&app, "klens_topic_describe", json!({ "topic": "payments" })).await);

    assert_eq!(described["underReplicatedPartitions"], 2);
    assert_eq!(described["offlinePartitions"], 1);
    assert_eq!(described["rate"], 0.5);
    assert_eq!(described["partitions"][0]["leader"], 1);
    assert_eq!(described["partitions"][1]["leader"], Value::Null);
    assert_eq!(described["partitions"][1]["offline"], true);
    assert_eq!(described["groups"], json!([]));
    assert_eq!(described["subjects"], Value::Null);
}

#[tokio::test]
async fn topic_describe_finds_a_key_subject_and_names_an_unknown_topic() {
    let cluster = FakeCluster::local();
    cluster.set_subjects(vec![
        subject("orders.created-key", 4, 1),
        subject("orders.created-value", 5, 3),
        subject("orders.created-other", 6, 1),
    ]);
    let app = TestApp::over(cluster).await;

    let described = structured(
        &call(
            &app,
            "klens_topic_describe",
            json!({ "topic": "orders.created" }),
        )
        .await,
    );
    let unknown = refusal(&call(&app, "klens_topic_describe", json!({ "topic": "orders" })).await);

    assert_eq!(
        names(&described, "subjects", "subject"),
        ["orders.created-key", "orders.created-value"]
    );
    assert_eq!(unknown["code"], "UNKNOWN_TOPIC");
    assert_eq!(
        unknown["error"],
        "unknown topic 'orders' in cluster 'local'"
    );
    assert_eq!(unknown["hint"], "Call klens_search to find the exact name.");
}

#[tokio::test]
async fn topic_describe_keeps_the_partitions_that_fit() {
    let cluster = FakeCluster::local();
    cluster.add_topic("wide", 2000, 10);
    let app = TestApp::over(cluster).await;

    let result = call(&app, "klens_topic_describe", json!({ "topic": "wide" })).await;

    let described = structured(&result);
    let shown = described["partitions"]
        .as_array()
        .expect("partitions")
        .len();
    assert!(0 < shown && shown < 2000, "{shown}");
    assert_eq!(described["partitionCount"], 2000);
    assert_eq!(described["retainedMessages"], 20_000);
    assert_eq!(
        described["truncated"],
        format!(
            "{} of 2000 partitions left out to fit the result; the counts above cover every \
             partition",
            2000 - shown
        )
    );
    assert!(serde_json::to_vec(&result).expect("json").len() <= RESULT_BYTES);
}

#[tokio::test]
async fn topic_describe_keeps_the_most_lagging_groups_that_fit() {
    let mut groups: Vec<_> = (0..400)
        .map(|id| group(&format!("consumer-{id:03}"), "orders.created", vec![0]))
        .collect();
    groups.push(group("zz-behind", "orders.created", vec![0, 1]));
    let app = TestApp::over(FakeCluster::local().with_groups(groups)).await;

    let result = call(
        &app,
        "klens_topic_describe",
        json!({ "topic": "orders.created" }),
    )
    .await;

    let described = structured(&result);
    let ids = names(&described, "groups", "id");
    assert_eq!(ids[..2], ["zz-behind", "consumer-000"]);
    assert_eq!(described["groups"][0]["lagOnTopic"], 16);
    assert_eq!(described["partitions"].as_array().map(Vec::len), Some(2));
    assert_eq!(
        described["truncated"],
        format!(
            "{} of 402 groups left out to fit the result; the counts above cover every partition",
            402 - ids.len()
        )
    );
    assert!(serde_json::to_vec(&result).expect("json").len() <= RESULT_BYTES);
}

fn consumers() -> FakeCluster {
    FakeCluster::local()
        .with_topic("payments", 1, 4)
        .with_groups([
            group("billing", "orders.created", vec![0, 1])
                .with_committed(&[("orders.created", 0, 0), ("orders.created", 1, 0)]),
            group("ledger", "payments", vec![0]).with_committed(&[("payments", 0, 4)]),
            group("archive", "payments", vec![0])
                .with_committed(&[("payments", 0, 1)])
                .stopped(),
        ])
}

fn pair(id: &str, topic: &str) -> GroupSnapshot {
    let mut group = group(id, topic, vec![0]);
    let mut second = group.members[0].clone();
    second.id = format!("{id}-m2");
    second.client_id = "c2".into();
    second.host = "10.0.0.2".into();
    second.assignments[0].partitions = vec![1];
    group.members.push(second);
    group
}

#[tokio::test]
async fn groups_list_reads_each_group_from_the_snapshot() {
    let app = TestApp::local().await;

    let listed = structured(&call(&app, "klens_groups_list", json!({})).await);

    assert_eq!(
        listed,
        json!({
            "groups": [{
                "id": "order-processor",
                "state": "STABLE",
                "memberCount": 1,
                "totalLag": 5,
                "lagComplete": true,
            }],
            "showing": "1 of 1",
            "notice": CLIENT_VALUES_NOTICE,
        })
    );
}

#[tokio::test]
async fn detailed_group_rows_match_the_http_rows() {
    let app = TestApp::over(consumers()).await;

    let listed = structured(
        &call(
            &app,
            "klens_groups_list",
            json!({ "responseFormat": "DETAILED" }),
        )
        .await,
    );
    let mut http = app.get("/clusters/local/groups").await.ok();
    http.as_array_mut()
        .expect("rows")
        .sort_by_key(|row| -row["totalLag"].as_i64().expect("a lag"));

    assert_eq!(listed["groups"], http);
}

#[tokio::test]
async fn groups_list_filters_by_name_state_lag_and_topic() {
    let app = TestApp::over(consumers()).await;
    let groups = async |arguments: Value| {
        names(
            &structured(&call(&app, "klens_groups_list", arguments).await),
            "groups",
            "id",
        )
    };

    assert_eq!(
        [
            groups(json!({})).await,
            groups(json!({ "nameContains": "LED" })).await,
            groups(json!({ "state": "EMPTY" })).await,
            groups(json!({ "minLag": 5 })).await,
            groups(json!({ "topic": "payments" })).await,
            groups(json!({ "topic": "payment" })).await,
        ],
        [
            vec!["billing", "order-processor", "archive", "ledger"],
            vec!["ledger"],
            vec!["archive"],
            vec!["billing", "order-processor"],
            vec!["archive", "ledger"],
            vec![],
        ]
        .map(|names| names.into_iter().map(str::to_owned).collect::<Vec<_>>())
    );
}

#[tokio::test]
async fn groups_list_puts_lag_klens_has_not_read_last() {
    let cluster = consumers();
    let app = TestApp::over(cluster.clone()).await;
    cluster.put_group(group("fresh", "orders.created", vec![0]));
    let rig = app.rig();
    rig.poll(&rig.topology()).await;

    let listed = structured(&call(&app, "klens_groups_list", json!({ "minLag": 0 })).await);
    let all = structured(&call(&app, "klens_groups_list", json!({ "limit": 5 })).await);

    assert!(!names(&listed, "groups", "id").contains(&"fresh".to_owned()));
    assert_eq!(all["groups"][4]["id"], "fresh");
    assert_eq!(all["groups"][4]["totalLag"], Value::Null);
    assert_eq!(all["groups"][3]["totalLag"], 0);
}

#[tokio::test]
async fn group_describe_joins_members_offsets_and_findings() {
    let app = TestApp::local().await;

    let described = structured(
        &call(
            &app,
            "klens_group_describe",
            json!({ "group": "order-processor" }),
        )
        .await,
    );

    let offset = |partition: i32, committed: i64| {
        json!({
            "topic": "orders.created",
            "partition": partition,
            "committedOffset": committed,
            "endOffset": 8,
            "lag": 8 - committed,
        })
    };
    assert_eq!(
        described,
        json!({
            "group": "order-processor",
            "state": "STABLE",
            "protocol": "range",
            "totalLag": 5,
            "lagComplete": true,
            "findings": [],
            "members": [{
                "memberId": "member-1",
                "clientId": "orders",
                "host": "127.0.0.1",
                "assignments": [{ "topic": "orders.created", "partitions": [0, 1] }],
                "lag": 5,
            }],
            "partitions": [offset(1, 5), offset(0, 6)],
            "notice": CLIENT_VALUES_NOTICE,
        })
    );
    assert!(app.store().interest.is_hot("order-processor"));
}

#[tokio::test]
async fn group_describe_names_the_member_that_holds_the_lag() {
    let cluster = FakeCluster::local()
        .with_topic("payments", 3, 5000)
        .with_groups([pair("billing", "payments")
            .with_committed(&[("payments", 0, 5000), ("payments", 1, 0)])]);
    let app = TestApp::over(cluster).await;

    let described =
        structured(&call(&app, "klens_group_describe", json!({ "group": "billing" })).await);

    assert_eq!(
        described["findings"],
        json!([
            { "kind": "UNASSIGNED_PARTITIONS", "topic": "payments", "partitions": [2] },
            {
                "kind": "LAG_ON_ONE_MEMBER",
                "memberId": "billing-m2",
                "clientId": "c2",
                "host": "10.0.0.2",
                "lag": 5000,
                "totalLag": 5000,
            },
        ])
    );
    assert_eq!(
        names(&described, "members", "memberId"),
        ["billing-m2", "billing-m1"]
    );
    assert_eq!(described["members"][0]["lag"], 5000);
    assert_eq!(described["members"][1]["lag"], 0);
}

#[tokio::test]
async fn group_describe_leaves_lag_klens_has_not_read_null() {
    let cluster = FakeCluster::local();
    let app = TestApp::over(cluster.clone()).await;
    cluster.put_group(pair("fresh", "orders.created").stopped());
    let rig = app.rig();
    rig.poll(&rig.topology()).await;

    let described =
        structured(&call(&app, "klens_group_describe", json!({ "group": "fresh" })).await);

    assert_eq!(described["state"], "EMPTY");
    assert_eq!(described["totalLag"], Value::Null);
    assert_eq!(described["findings"], json!([{ "kind": "NO_MEMBERS" }]));
    assert_eq!(described["members"], json!([]));
}

#[tokio::test]
async fn a_member_lag_is_null_until_klens_reads_the_offsets() {
    let cluster = FakeCluster::local();
    let app = TestApp::over(cluster.clone()).await;
    cluster.put_group(pair("fresh", "orders.created"));
    let rig = app.rig();
    rig.poll(&rig.topology()).await;

    let described =
        structured(&call(&app, "klens_group_describe", json!({ "group": "fresh" })).await);

    assert_eq!(described["members"][0]["lag"], Value::Null);
    assert_eq!(described["partitions"][0]["committedOffset"], Value::Null);
    assert_eq!(described["partitions"][0]["lag"], Value::Null);
}

#[tokio::test]
async fn a_member_lag_is_null_while_one_of_its_partitions_has_no_end_offset() {
    let cluster = FakeCluster::local();
    let app = TestApp::over(cluster.clone()).await;
    let mut billing = pair("billing", "orders.created")
        .with_committed(&[("orders.created", 0, 6), ("payments", 0, 1)]);
    billing.members[1].assignments[0].topic = "payments".into();
    billing.members[1].assignments[0].partitions = vec![0];
    cluster.add_topic("payments", 1, 4);
    cluster.put_group(billing);
    let rig = app.rig();
    rig.poll(&rig.topology()).await;
    rig.sweep(&rig.offsets()).await;

    let described =
        structured(&call(&app, "klens_group_describe", json!({ "group": "billing" })).await);

    assert_eq!(described["totalLag"], 2);
    assert_eq!(described["lagComplete"], false);
    assert_eq!(
        described["members"],
        json!([
            {
                "memberId": "billing-m1",
                "clientId": "c1",
                "host": "127.0.0.1",
                "assignments": [{ "topic": "orders.created", "partitions": [0] }],
                "lag": 2,
            },
            {
                "memberId": "billing-m2",
                "clientId": "c2",
                "host": "10.0.0.2",
                "assignments": [{ "topic": "payments", "partitions": [0] }],
                "lag": null,
            },
        ])
    );
}

#[tokio::test]
async fn group_describe_keeps_the_most_lagging_members_that_fit() {
    let mut billing = group("billing", "orders.created", vec![0]);
    let template = billing.members[0].clone();
    billing.members = (0..400)
        .map(|id| {
            let mut member = template.clone();
            member.id = format!("billing-consumer-{id:03}-5f0c1d2e-8a4b-4c3d-9e2f-0a1b2c3d4e5f");
            match id {
                0 | 1 => member.assignments[0].partitions = vec![id],
                _ => member.assignments.clear(),
            }
            member
        })
        .collect();
    let app = TestApp::over(FakeCluster::local().with_groups([billing])).await;

    let result = call(&app, "klens_group_describe", json!({ "group": "billing" })).await;

    let described = structured(&result);
    let shown = described["members"].as_array().expect("members").len();
    assert!(2 < shown && shown < 400, "{shown}");
    assert_eq!(described["members"][1]["lag"], 8);
    assert_eq!(described["members"][2]["lag"], 0);
    assert_eq!(described["partitions"].as_array().map(Vec::len), Some(2));
    assert_eq!(
        described["findings"],
        json!([{ "kind": "MORE_MEMBERS_THAN_PARTITIONS", "members": 400, "partitions": 2 }])
    );
    assert_eq!(
        described["truncated"],
        format!(
            "{} of 400 members left out to fit the result; the lag totals and findings above \
             cover every member and partition",
            400 - shown
        )
    );
    assert!(serde_json::to_vec(&result).expect("json").len() <= RESULT_BYTES);
}

#[tokio::test]
async fn group_describe_names_an_unknown_group() {
    let app = TestApp::local().await;

    let refused = refusal(&call(&app, "klens_group_describe", json!({ "group": "ghost" })).await);

    assert_eq!(refused["code"], "UNKNOWN_GROUP");
    assert_eq!(refused["hint"], "Call klens_search to find the exact name.");
}

#[tokio::test]
async fn live_tools_draw_on_a_budget_that_snapshot_tools_leave_alone() {
    let app = TestApp::of([FakeCluster::local()])
        .limits(Limits {
            mcp_live_calls_per_minute: NonZeroU32::MIN,
            ..Limits::new(&Tuning::default())
        })
        .ingested()
        .await;
    let describe = || {
        call(
            &app,
            "klens_group_describe",
            json!({ "group": "order-processor" }),
        )
    };

    let listed = call(&app, "klens_groups_list", json!({})).await;
    let served = describe().await;
    let refused = refusal(&describe().await);
    let still = call(&app, "klens_groups_list", json!({})).await;

    structured(&listed);
    structured(&served);
    structured(&still);
    assert_eq!(
        refused,
        json!({
            "error": "too many calls this minute to tools that read more from Kafka",
            "code": "RATE_LIMITED",
            "hint": "Wait a minute before calling this tool again. Tools that read klens' \
                     snapshot, such as klens_groups_list, still answer meanwhile.",
        })
    );
}

#[test]
fn mcp_reaches_clusters_only_through_the_session() {
    let app = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/app");
    let sources = WalkDir::new(app.join("mcp"))
        .into_iter()
        .map(|entry| entry.expect("a source entry").into_path())
        .chain([app.join("mcp.rs")])
        .filter(|path| path.extension().is_some_and(|extension| extension == "rs"));

    for path in sources {
        let source = std::fs::read_to_string(&path).expect("a source file");
        for (unchecked, skips) in [
            (concat!("state", ".clusters"), "access checks"),
            (concat!("Cluster", "Session"), "obfuscation"),
        ] {
            assert!(
                !source.contains(unchecked),
                "{} names {unchecked}, which skips {skips}",
                path.display()
            );
        }
    }
}
