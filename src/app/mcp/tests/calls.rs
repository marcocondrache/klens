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
        .reply_through_router(request_2026("tools/call", Some("klens_search"), params))
        .await
        .ok();
    refusal(&reply["result"]);

    logs.assert_contains(r#"mcp.tool{tool="klens_search" client="unverified:probe"}"#);
    logs.assert_contains(r#"refused a tool call code="UNKNOWN_CLUSTER""#);
    logs.assert_lacks("4111");
}

#[tokio::test]
async fn a_refused_call_logs_the_user_and_client_of_its_token() {
    let a = Signer::a();
    let idp = Idp::start(&[&a], &[&a]).await;
    let mcp = for_resource("");
    let app = TestApp::of([FakeCluster::local()])
        .auth(idp.auth(&mcp).await)
        .build();
    let token = a.sign(&idp.claims());
    let body = call_body(
        "klens_search",
        json!({ "query": "orders", "cluster": "nope" }),
    );
    let logs = LogCapture::at(Level::INFO);

    let response = post_through_serve(&app, &mcp, &token, &body).await;

    assert!(response.contains("UNKNOWN_CLUSTER"), "{response}");
    let text = logs.text();
    let refused = text
        .lines()
        .find(|line| line.contains(r#"refused a tool call code="UNKNOWN_CLUSTER""#))
        .unwrap_or_else(|| panic!("no refusal in the logs:\n{text}"));
    assert!(
        refused.contains(r#"user="user-1" client="claude-code"}"#),
        "{refused}"
    );
    assert!(
        refused.contains(r#"mcp.tool{tool="klens_search" client="claude-code"}"#),
        "{refused}"
    );
    logs.assert_lacks(&token);
}

async fn post_through_serve(app: &TestApp, mcp: &Mcp, token: &str, body: &Value) -> String {
    let body = body.to_string();
    let response = exchange(
        router(
            app.state().clone(),
            &Config::default().allowed_hosts,
            Some(mcp),
        ),
        &format!(
            "POST /mcp HTTP/1.1\r\nHost: {RESOURCE_HOST}\r\nAuthorization: Bearer {token}\r\n\
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
async fn each_user_of_an_access_token_has_a_live_budget_of_their_own() {
    let app = TestApp::of([FakeCluster::local()])
        .limits(Limits {
            mcp_live_calls_per_minute: NonZeroU32::MIN,
            ..Limits::new(&Tuning::default())
        })
        .ingested()
        .await;
    let exp = Timestamp::now().as_second() + 300;
    let alice = app.with_guard(SessionGuard::token("alice", exp));
    let bob = app.with_guard(SessionGuard::token("bob", exp));
    let describe = async |app: &TestApp| {
        call(
            app,
            "klens_group_describe",
            json!({ "group": "order-processor" }),
        )
        .await
    };

    structured(&describe(&alice).await);
    let spent = refusal(&describe(&alice).await);
    structured(&describe(&bob).await);
    structured(&describe(&app).await);
    let shared = refusal(&describe(&app).await);

    assert_eq!(spent["code"], "RATE_LIMITED");
    assert_eq!(
        shared["code"], "RATE_LIMITED",
        "callers without a token share one"
    );
}

#[tokio::test]
async fn a_write_and_a_refused_write_log_the_user_and_client_of_the_token() {
    let a = Signer::a();
    let idp = Idp::start(&[&a], &[&a]).await;
    let mcp = Mcp {
        privileges: every_tool().privileges,
        ..for_resource("token: {clients: [claude-code]}")
    };
    let app = TestApp::of([FakeCluster::local(), FakeCluster::named("prod")])
        .auth(idp.auth(&mcp).await)
        .writable(&["local"])
        .ingested()
        .await;
    let _lanes = lanes_writes_wait_on(&app);
    let mut claims = idp.claims();
    claims["groups"] = json!(["ops", "writers"]);
    let token = a.sign(&claims);
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
            let body = call_body(name, naming(tool, cluster));
            post_through_serve(&app, &mcp, &token, &body).await;
        }
    }

    let text = logs.text();
    for (name, audit) in audits {
        let span = format!(r#"mcp.tool{{tool="{name}" client="claude-code"}}"#);
        for message in [audit, r#"refused a tool call code="READ_ONLY_CLUSTER""#] {
            assert!(
                text.lines().any(|line| line.contains(message)
                    && line.contains(&span)
                    && line.contains(r#"user="user-1" client="claude-code"}"#)),
                "no `{message}` from {name} with its user and client in the logs:\n{text}"
            );
        }
    }
    logs.assert_lacks(&token);
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
    let answer = app.send_through_router(mcp_request(&body));

    tokio::select! {
        _ = answer => panic!("the read answered before the client left"),
        () = eventually("a read in flight", || app.cluster().calls(Api::SubjectSchema) == 1) => {}
    }

    eventually("a free call", || app.state().mcp_permit().is_some()).await;
}

#[tokio::test]
async fn a_write_still_logs_its_change_when_the_client_leaves_and_shutdown_waits_for_it() {
    let cluster = FakeCluster::local().with_delay(Api::CreateTopic, Duration::from_millis(100));
    let app = TestApp::of([cluster])
        .writable(&["local"])
        .ingested()
        .await
        .serving_mcp(every_tool());
    let _lanes = lanes_writes_wait_on(&app);
    let logs = LogCapture::at(Level::INFO);
    let body = call_body("klens_topic_create", json!({ "topic": "invoices" }));
    let answer = app.send_through_router(mcp_request(&body));

    tokio::select! {
        _ = answer => panic!("the create answered before the client left"),
        () = eventually("a create in flight", || app.cluster().calls(Api::CreateTopic) == 1) => {}
    }

    let writes = app.state().writes();
    writes.close();
    writes.wait().await;

    let text = logs.text();
    assert!(
        text.lines().any(|line| line.contains("created topic")
            && line.contains(r#"mcp.tool{tool="klens_topic_create"}"#)),
        "{text}"
    );
}
