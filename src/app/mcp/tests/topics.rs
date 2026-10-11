use super::*;

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
    assert_eq!(described["configs"], Value::Null);
    assert_eq!(
        described["omitted"],
        json!({ "section": "configs", "notRead": { "lastError": null } })
    );
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
            "configs": [],
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
async fn topic_describe_shows_the_overrides_among_the_http_configs() {
    let cluster = FakeCluster::local();
    cluster.set_topic_configs(
        "orders.created",
        vec![
            config("cleanup.policy", "delete", ConfigSource::Default),
            config("retention.ms", "86400000", ConfigSource::DynamicTopic),
            config("segment.bytes", "1048576", ConfigSource::StaticBroker),
        ],
    );
    let app = TestApp::over(cluster).await;

    let described = structured(
        &call(
            &app,
            "klens_topic_describe",
            json!({ "topic": "orders.created" }),
        )
        .await,
    );
    let http = app
        .get("/clusters/local/topics/orders.created/configs")
        .await
        .ok();

    let overrides: Vec<&Value> = http
        .as_array()
        .expect("configs")
        .iter()
        .filter(|entry| entry["source"] != "DEFAULT_CONFIG")
        .collect();
    assert_eq!(described["configs"], json!(overrides));
    assert_eq!(
        names(&described, "configs", "name"),
        ["retention.ms", "segment.bytes"]
    );
}

#[tokio::test]
async fn topic_describe_cuts_a_long_override_and_keeps_the_partitions() {
    let cluster = FakeCluster::local();
    let throttled: Vec<String> = (0..3000)
        .map(|partition| format!("{partition}:1"))
        .collect();
    cluster.set_topic_configs(
        "orders.created",
        vec![config(
            "leader.replication.throttled.replicas",
            &throttled.join(","),
            ConfigSource::DynamicTopic,
        )],
    );
    let app = TestApp::over(cluster).await;

    let result = call(
        &app,
        "klens_topic_describe",
        json!({ "topic": "orders.created" }),
    )
    .await;

    let described = structured(&result);
    let shown = &described["configs"][0];
    let value = shown["value"].as_str().expect("a value");
    assert_eq!(value.chars().count(), 500);
    assert!(throttled.join(",").starts_with(value));
    assert_eq!(shown["cut"], true);
    assert_eq!(described["partitions"].as_array().map(Vec::len), Some(2));
    assert!(described.get("truncated").is_none(), "{described}");
    assert!(serde_json::to_vec(&result).expect("json").len() <= RESULT_BYTES);
}

#[tokio::test]
async fn topic_describe_says_when_klens_has_not_read_the_configs() {
    let cluster = FakeCluster::local();
    let app = TestApp::over(cluster.clone()).await;
    cluster.add_topic("invoices", 1, 0);
    cluster.fail(Api::TopicConfigs, "describe configs denied");
    let rig = app.rig();
    rig.poll(&rig.topology()).await;
    rig.poll(&rig.configs()).await;

    let described =
        structured(&call(&app, "klens_topic_describe", json!({ "topic": "invoices" })).await);

    assert_eq!(described["configs"], Value::Null);
    assert_eq!(described["omitted"]["section"], "configs");
    let error = lane_error_in(&described, &described["omitted"]["notRead"]["lastError"]);
    assert!(error.contains("describe configs denied"), "{error}");
    let notice = described["notice"].as_str().expect("a notice");
    assert!(notice.starts_with(CLIENT_VALUES_NOTICE), "{notice}");
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
