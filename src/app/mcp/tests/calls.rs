use super::*;

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
        .reply_to_mcp(request_2026("tools/call", Some("klens_search"), params))
        .await
        .ok();
    refusal(&reply["result"]);

    logs.assert_contains(r#"mcp.tool{tool="klens_search" client="unverified:probe"}"#);
    logs.assert_contains(r#"refused a tool call code="UNKNOWN_CLUSTER""#);
    logs.assert_lacks("4111");
}

async fn post_through_serve(app: &TestApp, mcp: &Mcp, body: &Value) -> String {
    let body = body.to_string();
    let response = exchange(
        mcp_router(app.state().clone(), mcp),
        &format!(
            "POST /mcp HTTP/1.1\r\nHost: klens.example.com\r\n\
             Authorization: Bearer {GATEWAY_SECRET}\r\n\
             Accept: application/json, text/event-stream\r\nContent-Type: application/json\r\n\
             Mcp-Protocol-Version: 2025-06-18\r\nContent-Length: {}\r\nConnection: close\r\n\r\n\
             {body}",
            body.len()
        ),
    )
    .await;
    String::from_utf8(response).expect("utf-8 response")
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

#[tokio::test]
async fn with_auth_on_a_write_from_the_gateway_logs_its_audit_line_and_never_the_secret() {
    let mcp = behind_gateway(every_tool());
    let app = TestApp::of([FakeCluster::local(), FakeCluster::named("prod")])
        .auth(auth_granting_nothing())
        .writable(&["local"])
        .ingested()
        .await;
    let _lanes = lanes_writes_wait_on(&app);
    let tools = KlensMcp::tools().list_all();
    let audits = [
        ("klens_topic_create", "created topic"),
        ("klens_record_produce", "produced record"),
        ("klens_schema_register", "registered schema"),
    ];
    let logs = LogCapture::at(Level::INFO);

    for (name, _) in audits {
        let tool = tools.iter().find(|tool| tool.name == name).expect("a tool");
        for cluster in ["local", "prod"] {
            post_through_serve(&app, &mcp, &call_body(name, naming(tool, cluster))).await;
        }
    }

    let text = logs.text();
    for (name, audit) in audits {
        let span = format!(r#"mcp.tool{{tool="{name}"}}"#);
        for message in [audit, r#"refused a tool call code="READ_ONLY_CLUSTER""#] {
            assert!(
                text.lines()
                    .any(|line| line.contains(message) && line.contains(&span)),
                "no `{message}` from {name} in the logs:\n{text}"
            );
        }
    }
    logs.assert_lacks(GATEWAY_SECRET);
}

#[tokio::test]
async fn a_read_stops_and_frees_its_call_when_the_client_leaves() {
    let app = TestApp::of([schemas().with_delay(Api::SubjectSchema, Duration::from_secs(60))])
        .limits(Limits {
            mcp_calls: 1,
            ..Limits::new(&Tuning::default())
        })
        .ingested()
        .await
        .serving_mcp(every_tool());
    let body = call_body(
        "klens_schema_get",
        json!({ "subject": "orders.created-value", "version": 2 }),
    );
    let answer = app.send_to_mcp(mcp_request(&body));

    tokio::select! {
        _ = answer => panic!("the read answered before the client left"),
        () = eventually("a read in flight", || app.cluster().calls(Api::SubjectSchema) == 1) => {}
    }

    eventually("a free call", || app.state().mcp_permit().is_some()).await;
}

#[tokio::test]
async fn a_write_still_logs_its_change_when_the_client_leaves() {
    let cluster = FakeCluster::local().with_delay(Api::CreateTopic, Duration::from_millis(100));
    let app = TestApp::of([cluster])
        .writable(&["local"])
        .ingested()
        .await
        .serving_mcp(every_tool());
    let _lanes = lanes_writes_wait_on(&app);
    let logs = LogCapture::at(Level::INFO);
    let body = call_body("klens_topic_create", json!({ "topic": "invoices" }));
    let answer = app.send_to_mcp(mcp_request(&body));

    tokio::select! {
        _ = answer => panic!("the create answered before the client left"),
        () = eventually("a create in flight", || app.cluster().calls(Api::CreateTopic) == 1) => {}
    }

    eventually("the created topic line", || {
        logs.text().contains("created topic")
    })
    .await;
    let text = logs.text();
    assert!(
        text.lines().any(|line| line.contains("created topic")
            && line.contains(r#"mcp.tool{tool="klens_topic_create"}"#)),
        "{text}"
    );
}
