use super::*;

const PAN: &str = "4111111111111111";

fn orders(records: Vec<FixtureRecord>) -> FakeCluster {
    FakeCluster::local().with_records(records)
}

fn cards() -> FakeCluster {
    orders((0..3).map(|offset| card_record(offset, PAN)).collect()).with_obfuscation(
        "
        secret: {value: 0123456789abcdef0123456789abcdef}
        rules:
          - topics: ['orders.*']
            headers: ['x-user-id']
            fields:
              - path: card.number
                strategy: hash
        ",
    )
}

fn at(partition: i32, offset: i64) -> Value {
    json!({ "topic": "orders.created", "partition": partition, "offset": offset })
}

#[tokio::test]
async fn record_get_shows_what_the_http_route_returns() {
    let app = TestApp::over(orders(vec![
        FixtureRecord::order(0, 0)
            .key("ord_0")
            .value("paid")
            .header("x-trace", "t-1"),
        FixtureRecord::order(0, 1).value(framed(7, r#"{"total":7}"#)),
        FixtureRecord::order(0, 2).key("ord_2"),
    ]))
    .await;

    for offset in 0..3 {
        let result = call(&app, "klens_record_get", at(0, offset)).await;
        let mut http = app
            .get(&format!(
                "/clusters/local/topics/orders.created/records/0/{offset}"
            ))
            .await
            .ok();

        let mut expected = http["record"].take();
        expected.as_object_mut().expect("a record").remove("topic");
        expected["cut"] = json!(false);
        expected["headersLeftOut"] = json!(0);
        assert_eq!(records_in(text(&result)), [expected]);
    }
}

#[tokio::test]
async fn record_get_names_a_record_it_cannot_find() {
    let app = TestApp::local().await;

    let gone = refusal(&call(&app, "klens_record_get", at(0, 2)).await);
    let nowhere = refusal(&call(&app, "klens_record_get", at(7, 1)).await);

    assert_eq!(gone["code"], "UNKNOWN_OFFSET");
    assert_eq!(
        gone["hint"],
        "Retention or compaction may have removed the record, or the offset may be past the end \
         of the partition. Call klens_topic_describe for each partition's watermarks."
    );
    assert_eq!(nowhere["code"], "UNKNOWN_PARTITION");
    assert_eq!(
        nowhere["hint"],
        "Call klens_topic_describe for the topic's partitions."
    );
}

#[tokio::test]
async fn record_get_cuts_only_what_one_record_cannot_fit() {
    let long = "é".repeat(30_000);
    let app = TestApp::over(orders(vec![
        FixtureRecord::order(0, 0).key("ord_0").value(long.clone()),
    ]))
    .await;

    let result = call(&app, "klens_record_get", at(0, 0)).await;

    let text = text(&result);
    let record = &records_in(text)[0];
    let value = record["value"].as_str().expect("a value");
    assert_eq!(record["cut"], true);
    assert_eq!(record["key"], "ord_0");
    assert!(value.chars().count() > 4_000, "{}", value.len());
    assert!(long.starts_with(value));
    assert!(
        text.contains("the records it touched. The klens UI shows the whole record.\n"),
        "{text}"
    );
    assert!(serde_json::to_vec(&result).expect("json").len() <= RESULT_BYTES);
}

#[tokio::test]
async fn the_record_tools_need_the_records_privilege() {
    let app = TestApp::local().await.serving_mcp(Mcp::holding([]));

    for (tool, arguments) in [
        ("klens_record_get", at(0, 1)),
        ("klens_records_read", json!({ "topic": "orders.created" })),
    ] {
        let refused = refusal(&call(&app, tool, arguments).await);

        assert_eq!(refused["code"], "FORBIDDEN", "{tool}");
        assert_eq!(
            refused["hint"],
            "Call klens_access_explain to see what you may do on each cluster."
        );
    }
    assert_eq!(app.cluster().calls(Api::OpenScan), 0);
}

#[tokio::test]
async fn an_obfuscated_field_stays_obfuscated_for_the_agent() {
    let app = TestApp::over(cards()).await;

    let page = call(
        &app,
        "klens_records_read",
        json!({ "topic": "orders.created" }),
    )
    .await;
    let opened = call(&app, "klens_record_get", at(0, 1)).await;
    let searched = call(
        &app,
        "klens_records_read",
        json!({ "topic": "orders.created", "contains": PAN }),
    )
    .await;

    for text in [text(&page), text(&opened)] {
        assert!(text.contains(OBFUSCATED_NOTICE), "{text}");
        assert!(!text.contains(PAN), "{text}");
        for record in records_in(text) {
            let value = record["value"].as_str().expect("a value");
            assert!(value.contains("\"kx:"), "{value}");
            assert_eq!(
                record["headers"],
                json!([{ "key": "x-user-id", "value": "***" }])
            );
            assert_eq!(record["verbatim"], false);
        }
    }
    assert_eq!(records_in(text(&page)).len(), 3);
    assert!(text(&searched).starts_with("0 records, newest first.\n"));
}

#[tokio::test]
async fn record_tools_draw_on_the_live_budget_once_their_checks_pass() {
    let app = TestApp::of([FakeCluster::local()])
        .limits(Limits {
            mcp_live_calls_per_minute: NonZeroU32::MIN,
            ..Limits::new(&Tuning::default())
        })
        .ingested()
        .await;
    let viewer = app.serving_mcp(Mcp::holding([]));

    let forbidden = refusal(&call(&viewer, "klens_record_get", at(0, 1)).await);
    let hidden = refusal(
        &call(
            &app,
            "klens_record_get",
            json!({ "cluster": "prod", "topic": "orders.created", "partition": 0, "offset": 1 }),
        )
        .await,
    );
    let malformed = refusal(
        &call(
            &app,
            "klens_records_read",
            json!({ "topic": "orders.created", "startOffset": 3 }),
        )
        .await,
    );
    for address in [at(-1, 1), at(0, -1)] {
        let negative = refusal(&call(&app, "klens_record_get", address).await);
        assert_eq!(
            negative["error"], "`partition` and `offset` must be zero or more",
            "{negative}"
        );
    }
    let served = call(&app, "klens_record_get", at(0, 1)).await;
    let spent = refusal(
        &call(
            &app,
            "klens_records_read",
            json!({ "topic": "orders.created" }),
        )
        .await,
    );

    assert_eq!(forbidden["code"], "FORBIDDEN");
    assert_eq!(hidden["code"], "UNKNOWN_CLUSTER");
    assert_eq!(malformed["code"], "INVALID_REQUEST");
    assert_eq!(records_in(text(&served))[0]["key"], "ord_1");
    assert_eq!(spent["code"], "RATE_LIMITED");
    assert_eq!(app.cluster().calls(Api::OpenScan), 1);
}

fn offsets(text: &str) -> Vec<i64> {
    records_in(text)
        .iter()
        .map(|record| record["offset"].as_i64().expect("an offset"))
        .collect()
}

fn cursor(text: &str) -> &str {
    text.split("`cursor` set to `")
        .nth(1)
        .and_then(|rest| rest.split('`').next())
        .unwrap_or_else(|| panic!("no cursor in {text}"))
}

#[tokio::test]
async fn records_read_starts_at_an_offset_or_a_time_in_either_order() {
    let app = TestApp::local().await;
    let read = async |arguments: Value| {
        let mut arguments = arguments;
        arguments["topic"] = json!("orders.created");
        offsets(text(&call(&app, "klens_records_read", arguments).await))
    };

    assert_eq!(read(json!({})).await, [7, 6, 5, 4, 3, 2, 1, 0]);
    assert_eq!(read(json!({ "partitions": [1], "limit": 2 })).await, [6, 4]);
    assert_eq!(
        read(json!({ "partitions": [0], "startOffset": 3, "order": "OLDEST" })).await,
        [3, 5, 7]
    );
    assert_eq!(
        read(json!({ "partitions": [0], "startOffset": 5 })).await,
        [5, 3, 1]
    );
    assert_eq!(
        read(json!({ "order": "OLDEST", "from": "2023-11-14T22:13:24Z", "limit": 2 })).await,
        [4, 5]
    );
    assert_eq!(read(json!({ "to": "2023-11-14T22:13:21Z" })).await, [1, 0]);
    assert_eq!(read(json!({ "contains": "ORD_6" })).await, [6]);
}

#[tokio::test]
async fn records_read_pages_with_its_cursor_and_caps_the_page() {
    let app = TestApp::over(orders(
        (0..60)
            .map(|offset| FixtureRecord::order(0, offset).at(1_700_000_000_000 + offset))
            .collect(),
    ))
    .await;
    let read = async |arguments: Value| {
        let mut arguments = arguments;
        arguments["topic"] = json!("orders.created");
        arguments["order"] = json!("OLDEST");
        call(&app, "klens_records_read", arguments).await
    };

    let first = read(json!({})).await;
    let first = text(&first);
    let second = read(json!({ "cursor": cursor(first) })).await;
    let most = read(json!({ "limit": 500 })).await;
    let last = read(json!({ "partitions": [0], "startOffset": 55 })).await;

    assert!(first.starts_with("10 records, oldest first.\n"), "{first}");
    assert_eq!(offsets(first), (0..10).collect::<Vec<_>>());
    assert_eq!(offsets(text(&second)), (10..20).collect::<Vec<_>>());
    assert_eq!(offsets(text(&most)).len(), 50);
    assert_eq!(offsets(text(&last)), (55..60).collect::<Vec<_>>());
    assert!(
        text(&last).starts_with("5 records, oldest first.\nNo more records match.\n"),
        "{}",
        text(&last)
    );
}

#[tokio::test]
async fn a_page_past_the_budget_cuts_text_but_keeps_every_record_and_its_cursor() {
    let value = "v".repeat(5_000);
    let app = TestApp::over(orders(
        (0..60)
            .map(|offset| {
                FixtureRecord::order(0, offset)
                    .at(1_700_000_000_000 + offset)
                    .key(format!("ord_{offset}"))
                    .value(value.clone())
            })
            .collect(),
    ))
    .await;
    let read = async |cursor: Option<&str>| {
        let mut arguments = json!({ "topic": "orders.created", "order": "OLDEST", "limit": 20 });
        if let Some(cursor) = cursor {
            arguments["cursor"] = json!(cursor);
        }
        call(&app, "klens_records_read", arguments).await
    };

    let first = read(None).await;
    let next = read(Some(cursor(text(&first)))).await;

    for (result, shown) in [(&first, 0..20), (&next, 20..40)] {
        let text = text(result);
        let records = records_in(text);
        assert_eq!(offsets(text), shown.collect::<Vec<_>>());
        assert!(records.iter().all(|record| record["cut"] == true), "{text}");
        assert!(
            records.iter().all(|record| record["key"]
                .as_str()
                .is_some_and(|key| key.starts_with("ord_"))),
            "{text}"
        );
        assert!(
            text.contains(
                "the records it touched. klens_record_get reads one of them with the whole \
                 result to itself.\n"
            ),
            "{text}"
        );
        assert!(serde_json::to_vec(result).expect("json").len() <= RESULT_BYTES);
    }
}

#[tokio::test]
async fn records_read_refuses_a_page_whose_cursor_leaves_its_records_no_room() {
    let app = TestApp::over(orders(
        (0..2_000)
            .map(|partition| FixtureRecord::order(partition, 1_000_000_000))
            .collect(),
    ))
    .await;
    let read = async |arguments: Value| {
        let mut arguments = arguments;
        arguments["topic"] = json!("orders.created");
        arguments["limit"] = json!(1);
        call(&app, "klens_records_read", arguments).await
    };

    let every = refusal(&read(json!({})).await);
    let two = read(json!({ "partitions": [0, 1] })).await;

    assert_eq!(every["code"], "INVALID_REQUEST");
    assert_eq!(
        every["error"],
        "the page does not fit the result even with its text cut, because its cursor names many \
         partitions; pass a smaller `limit` or fewer `partitions`"
    );
    assert_eq!(offsets(text(&two)), [1_000_000_000]);
}

#[tokio::test]
async fn records_read_refuses_arguments_it_cannot_read() {
    let app = TestApp::local().await;
    let read = async |arguments: Value| {
        let mut arguments = arguments;
        arguments["topic"] = json!("orders.created");
        refusal(&call(&app, "klens_records_read", arguments).await)
    };

    let unplaced = read(json!({ "startOffset": 3, "partitions": [0, 1] })).await;
    let negative = read(json!({ "startOffset": -1, "partitions": [0] })).await;
    let forged = read(json!({ "cursor": "v2:o:f:0:3" })).await;
    let inverted = read(json!({
        "from": "2023-11-14T22:13:25Z",
        "to": "2023-11-14T22:13:23Z",
    }))
    .await;

    for refused in [&unplaced, &negative] {
        assert_eq!(refused["code"], "INVALID_REQUEST", "{refused}");
        assert_eq!(
            refused["error"],
            "`startOffset` needs an offset of zero or more and exactly one partition in \
             `partitions`"
        );
    }
    assert_eq!(forged["code"], "INVALID_CURSOR");
    assert_eq!(
        forged["hint"],
        "Pass `cursor` exactly as the last page gave it, with the other arguments that page used."
    );
    assert_eq!(inverted["code"], "INVERTED_TIMESTAMP_RANGE");
    assert_eq!(
        inverted["error"],
        "invalid record query: `from` must not be after `to`"
    );
    assert_eq!(
        inverted["hint"],
        "Pass `from` at or before `to`, then call again."
    );
    assert_eq!(app.cluster().calls(Api::OpenScan), 0);
}
