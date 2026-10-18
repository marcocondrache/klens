use super::*;

#[tokio::test]
async fn topic_create_creates_what_the_http_route_creates() {
    let (by_tool, by_route) = (writable().await, writable().await);
    let _lanes = (
        lanes_writes_wait_on(&by_tool),
        lanes_writes_wait_on(&by_route),
    );
    let configs = json!({ "cleanup.policy": "compact" });

    let created = structured(
        &call(
            &by_tool,
            "klens_topic_create",
            json!({ "topic": "invoices", "partitions": 3, "configs": configs }),
        )
        .await,
    );
    by_route
        .post(
            "/clusters/local/topics",
            &json!({ "name": "invoices", "partitions": 3, "configs": configs }),
        )
        .await
        .expect(StatusCode::CREATED);

    assert_eq!(created, json!({ "topic": "invoices", "partitions": 3 }));
    for app in [&by_tool, &by_route] {
        let rig = app.rig();
        rig.poll(&mut rig.configs()).await;
    }
    for path in [
        "/clusters/local/topics/invoices",
        "/clusters/local/topics/invoices/configs",
    ] {
        assert_eq!(
            by_tool.get(path).await.ok(),
            by_route.get(path).await.ok(),
            "{path}"
        );
    }
}

#[tokio::test]
async fn topic_create_leaves_the_partition_count_to_the_broker_unless_given() {
    let app = writable().await;
    let _lanes = lanes_writes_wait_on(&app);

    let created =
        structured(&call(&app, "klens_topic_create", json!({ "topic": "invoices" })).await);

    assert_eq!(created, json!({ "topic": "invoices", "partitions": 3 }));
}

#[tokio::test]
async fn topic_create_keeps_the_reason_kafka_refused_inside_the_boundary() {
    let app = writable().await;

    let result = call(
        &app,
        "klens_topic_create",
        json!({ "topic": "orders.created" }),
    )
    .await;

    let refused = refusal(&result);
    assert_eq!(
        refused,
        json!({
            "error": "kafka refused the change",
            "code": "REFUSED",
            "hint": "Read Kafka's reason in the message, and change the arguments before you call \
                     again.",
        })
    );
    let text = result["content"][0]["text"].as_str().expect("a text");
    assert_eq!(
        enclosed(text)["message"],
        "Topic 'orders.created' already exists."
    );
    assert_eq!(text.matches("already exists").count(), 1, "{text}");
}

#[tokio::test]
async fn record_produce_writes_what_the_http_route_writes() {
    let (by_tool, by_route) = (writable().await, writable().await);
    let record = json!({
        "partition": 1,
        "key": { "encoding": "TEXT", "data": "order-9" },
        "value": { "encoding": "BASE64", "data": "eyJ0b3RhbCI6NDJ9" },
        "headers": [{ "key": "trace", "value": "abc" }],
    });
    let mut arguments = record.clone();
    arguments["topic"] = json!("orders.created");

    let produced = structured(&call(&by_tool, "klens_record_produce", arguments).await);
    let posted = by_route
        .post("/clusters/local/topics/orders.created/records", &record)
        .await
        .expect(StatusCode::CREATED);

    assert_eq!(produced, json!({ "partition": 1, "offset": 8 }));
    assert_eq!(produced, posted);
    let path = "/clusters/local/topics/orders.created/records/1/8";
    let stored = by_tool.get(path).await.ok();
    assert_eq!(stored["record"]["value"], r#"{"total":42}"#);
    assert_eq!(stored, by_route.get(path).await.ok());
}

#[tokio::test]
async fn record_produce_writes_no_tombstone() {
    let app = writable().await;

    let refused = refusal(
        &call(
            &app,
            "klens_record_produce",
            json!({ "topic": "orders.created", "value": null }),
        )
        .await,
    );

    assert_eq!(refused["code"], "INVALID_REQUEST", "{refused}");
    assert_eq!(app.cluster().calls(Api::Produce), 0);
}

#[tokio::test]
async fn record_produce_keeps_why_a_payload_misses_its_schema_inside_the_boundary() {
    let app = writable().await;

    let result = call(
        &app,
        "klens_record_produce",
        json!({
            "topic": "orders.created",
            "value": { "encoding": "SCHEMA", "schemaId": 1, "data": r#"{"orderId":1}"# },
        }),
    )
    .await;

    let refused = refusal(&result);
    assert_eq!(refused["error"], "the payload does not fit schema 1");
    assert_eq!(refused["code"], "UNENCODABLE");
    assert_eq!(
        refused["hint"],
        "klens_schemas_list with responseFormat DETAILED gives the schema id of each subject \
         version, and klens_schema_get reads a version's text."
    );
    let text = result["content"][0]["text"].as_str().expect("a text");
    let reason = enclosed(text)["message"].clone();
    assert!(
        reason.as_str().is_some_and(|reason| !reason.is_empty()),
        "{text}"
    );
    assert_eq!(app.cluster().calls(Api::Produce), 0);
}

#[tokio::test]
async fn schema_register_registers_what_the_http_route_registers() {
    let (by_tool, by_route) = (writable().await, writable().await);
    let _lanes = (
        lanes_writes_wait_on(&by_tool),
        lanes_writes_wait_on(&by_route),
    );
    let schema = json!({
        "type": "AVRO",
        "schema": r#"{"type":"record","name":"Invoice","fields":[{"name":"total","type":"long"}]}"#,
    });
    let mut arguments = schema.clone();
    arguments["subject"] = json!("invoices-value");

    let registered = structured(&call(&by_tool, "klens_schema_register", arguments).await);
    let posted = by_route
        .post("/clusters/local/subjects/invoices-value", &schema)
        .await
        .ok();

    assert_eq!(registered["version"], 1, "{registered}");
    assert_eq!(registered, posted);
    let path = "/clusters/local/subjects/invoices-value";
    assert_eq!(by_tool.get(path).await.ok(), by_route.get(path).await.ok());
}

#[tokio::test]
async fn schema_register_keeps_a_subject_with_a_quote_and_a_newline_on_its_audit_line() {
    let app = writable().await;
    let _lanes = lanes_writes_wait_on(&app);
    let logs = LogCapture::at(Level::INFO);

    let registered = structured(
        &call(
            &app,
            "klens_schema_register",
            json!({ "subject": "orders\"\nuser=\"mallory", "type": "AVRO", "schema": r#""string""# }),
        )
        .await,
    );

    assert_eq!(registered["version"], 1, "{registered}");
    logs.assert_contains(r#"registered schema cluster=local subject="orders\"\nuser=\"mallory""#);
    logs.assert_lacks("\nuser=");
}

#[tokio::test]
async fn schema_register_keeps_the_reason_the_registry_refused_inside_the_boundary() {
    let app = writable().await;

    let result = call(
        &app,
        "klens_schema_register",
        json!({ "subject": "invoices-value", "type": "AVRO", "schema": FORGED_ERROR }),
    )
    .await;

    let refused = refusal(&result);
    assert_eq!(
        refused,
        json!({
            "error": "the schema registry refused the change",
            "code": "REGISTRY_REFUSED",
            "hint": "Read the registry's reason in the message, and change the schema before you \
                     call again.",
        })
    );
    let text = result["content"][0]["text"].as_str().expect("a text");
    assert_eq!(
        enclosed(text)["message"],
        format!("Invalid schema {FORGED_ERROR}")
    );
}

#[tokio::test]
async fn write_tools_draw_on_the_live_budget_once_their_checks_pass() {
    let bare = FakeCluster::named("bare").without_schema_registry();
    let app = TestApp::of([
        FakeCluster::local(),
        FakeCluster::named("prod"),
        bare.clone(),
    ])
    .limits(Limits {
        mcp_live_calls_per_minute: NonZeroU32::MIN,
        ..Limits::new(&Tuning::default())
    })
    .writable(&["local", "bare"])
    .ingested()
    .await
    .serving_mcp(every_tool());
    let _lanes = lanes_writes_wait_on(&app);
    let viewer = app.serving_mcp(Mcp::holding([]));
    let writes: Vec<Tool> = KlensMcp::tools()
        .list_all()
        .into_iter()
        .filter(|tool| tool_names_that_change_kafka().contains(&&*tool.name))
        .collect();
    let refused = async |app: &TestApp, tool: &Tool, cluster: &str| {
        refusal(&call(app, &tool.name, naming(tool, cluster)).await)["code"].clone()
    };

    for tool in &writes {
        for (app, cluster, code) in [
            (&viewer, "local", "FORBIDDEN"),
            (&app, "prod", "READ_ONLY_CLUSTER"),
            (&app, "ghost", "UNKNOWN_CLUSTER"),
        ] {
            assert_eq!(refused(app, tool, cluster).await, code, "{}", tool.name);
        }
    }
    let malformed = refusal(
        &call(
            &app,
            "klens_topic_create",
            json!({ "cluster": "local", "topic": "bad name" }),
        )
        .await,
    );
    let mut misaddressed = Vec::new();
    for (topic, partition) in [("ghost", None), ("orders.created", Some(9))] {
        let arguments = json!({
            "cluster": "local",
            "topic": topic,
            "partition": partition,
            "value": { "encoding": "TEXT", "data": "x" },
        });
        let refused = refusal(&call(&app, "klens_record_produce", arguments).await);
        misaddressed.push(refused["code"].clone());
    }
    let unregistered = refusal(
        &call(
            &app,
            "klens_schema_register",
            json!({ "cluster": "bare", "subject": "s", "type": "AVRO", "schema": r#""string""# }),
        )
        .await,
    );
    let served = call(
        &app,
        "klens_topic_create",
        json!({ "cluster": "local", "topic": "payments" }),
    )
    .await;
    for tool in &writes {
        assert_eq!(
            refused(&app, tool, "local").await,
            "RATE_LIMITED",
            "{}",
            tool.name
        );
    }

    assert_eq!(malformed["code"], "INVALID_REQUEST");
    assert_eq!(misaddressed, ["UNKNOWN_TOPIC", "UNKNOWN_PARTITION"]);
    assert_eq!(unregistered["code"], "NO_SCHEMA_REGISTRY");
    structured(&served);
    assert_eq!(app.cluster().calls(Api::CreateTopic), 1);
    assert_eq!(app.cluster().calls(Api::Produce), 0);
    assert_eq!(app.cluster().calls(Api::RegisterSchema), 0);
    assert_eq!(bare.calls(Api::RegisterSchema), 0);
}
