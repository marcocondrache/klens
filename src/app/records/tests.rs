use std::num::NonZeroUsize;

use axum::http::StatusCode;
use bytes::Bytes;
use serde_json::{Value, json};
use tracing::Level;

use crate::app::Limits;
use crate::app::auth::SessionGuard;
use crate::config::Tuning;
use crate::kafka::model as domain;
use crate::testing::{
    Api, FakeCluster, FixtureRecord, LogCapture, TestApp, access, card_record, framed, quiesce,
    viewer,
};

use super::types::Record;

const RECORDS: &str = "/clusters/local/topics/orders.created/records";
const TAIL: &str = "/clusters/local/topics/orders.created/records/tail";
const EXPORT: &str = "/clusters/local/topics/orders.created/records/export";
const PAN: &str = "4111111111111111";

fn records(page: &Value) -> &[Value] {
    page["records"].as_array().expect("records")
}

fn offsets(records: &[Value]) -> Vec<i64> {
    records
        .iter()
        .map(|record| record["offset"].as_i64().expect("offset"))
        .collect()
}

fn produced(partition: i32, offset: i64, key: impl Into<Bytes>) -> FixtureRecord {
    FixtureRecord::order(partition, offset)
        .at(1_700_000_100_000 + offset)
        .key(key)
}

fn cards(rules: &str) -> FakeCluster {
    FakeCluster::local()
        .with_records((0..3).map(|offset| card_record(offset, PAN)).collect())
        .with_obfuscation(rules)
}

fn paging_by(max_limit: usize) -> Limits {
    let mut tuning = Tuning::default();
    tuning.records.max_limit = NonZeroUsize::new(max_limit).expect("non-zero");
    Limits::new(&tuning)
}

#[test]
fn a_record_keeps_its_wire_schema_id() {
    let record = domain::Record {
        topic: "orders".into(),
        partition: 0,
        offset: 1,
        timestamp: 0,
        key: Some("k".into()),
        value: Some("{}".into()),
        schema_id: Some(12),
        headers: Vec::new(),
        size_bytes: 2,
        verbatim: true,
    };

    assert_eq!(Record::from(record).schema_id, Some(12));
}

#[tokio::test]
async fn records_are_read_live_through_the_scan_path() {
    let app = TestApp::local().await;
    let page = app.get(&format!("{RECORDS}?limit=3")).await.ok();
    let records = records(&page);

    assert_eq!(records.len(), 3);
    assert_eq!(records[0]["topic"], "orders.created");
    assert_eq!(records[0]["sizeBytes"], 24);
    records[0]["timestamp"]
        .as_str()
        .expect("timestamp")
        .parse::<jiff::Timestamp>()
        .expect("RFC 3339");
    assert_eq!(app.cluster().calls(Api::OpenScan), 1);
}

#[tokio::test]
async fn records_accept_rfc3339_timestamp_bounds() {
    let page = TestApp::local()
        .await
        .get(&format!(
            "{RECORDS}?order=OLDEST&from=2023-11-14T22:13:23Z&to=2023-11-14T22:13:25Z"
        ))
        .await
        .ok();
    let records = records(&page);

    assert_eq!(offsets(records), [3, 4, 5]);
    assert_eq!(records[0]["timestamp"], "2023-11-14T22:13:23Z");
    assert_eq!(records[2]["timestamp"], "2023-11-14T22:13:25Z");
}

#[tokio::test]
async fn an_inverted_record_range_is_rejected() {
    TestApp::local()
        .await
        .get(&format!(
            "{RECORDS}?from=2023-11-14T22:13:25Z&to=2023-11-14T22:13:23Z"
        ))
        .await
        .assert_error(StatusCode::BAD_REQUEST, "INVERTED_TIMESTAMP_RANGE");
}

#[tokio::test]
async fn a_cursor_only_pages_the_order_that_minted_it() {
    let app = TestApp::local().await;
    let first = app
        .get(&format!("{RECORDS}?order=OLDEST&limit=2"))
        .await
        .ok();
    let cursor = first["nextCursor"].as_str().expect("next cursor");

    app.get(&format!("{RECORDS}?order=OLDEST&limit=2&cursor={cursor}"))
        .await
        .ok();
    app.get(&format!("{RECORDS}?order=NEWEST&limit=2&cursor={cursor}"))
        .await
        .assert_error(StatusCode::BAD_REQUEST, "INVALID_CURSOR");
}

#[tokio::test]
async fn records_can_be_read_from_a_set_of_partitions() {
    let app = TestApp::local().await;
    let partitions = |page: Value| {
        let mut ids: Vec<_> = records(&page)
            .iter()
            .map(|record| record["partition"].as_i64().expect("partition"))
            .collect();
        ids.sort_unstable();
        ids.dedup();
        ids
    };

    let one = app.get(&format!("{RECORDS}?partition=1")).await.ok();
    let both = app
        .get(&format!("{RECORDS}?partition=1&partition=0"))
        .await
        .ok();

    assert_eq!(partitions(one), [1]);
    assert_eq!(partitions(both), [0, 1]);
}

#[tokio::test]
async fn a_malformed_partition_is_rejected() {
    let app = TestApp::local().await;
    for path in [
        format!("{RECORDS}?partition=one"),
        format!("{RECORDS}?partition=0,1"),
        format!("{TAIL}?partition=one"),
    ] {
        app.get(&path)
            .await
            .assert_error(StatusCode::BAD_REQUEST, "INVALID_REQUEST");
    }
}

#[tokio::test]
async fn an_obfuscated_topic_serves_tokens_instead_of_payloads() {
    let app = TestApp::over(cards(
        "
        secret: {value: 0123456789abcdef0123456789abcdef}
        rules:
          - topics: ['orders.*']
            headers: ['x-user-id']
            fields:
              - path: card.number
                strategy: hash
              - path: card.cvv
                strategy: drop
        ",
    ))
    .await;
    let page = app.get(&format!("{RECORDS}?limit=3")).await.ok();
    let records = records(&page);

    assert_eq!(records.len(), 3);
    for record in records {
        let value = record["value"].as_str().expect("value");
        assert!(value.contains("\"kx:"), "{value}");
        assert!(!value.contains(PAN), "{value}");
        assert!(!value.contains("cvv"), "dropped fields vanish: {value}");
        assert!(
            record["key"].as_str().expect("key").starts_with("ord_"),
            "no rule names the key: {record}"
        );
        assert_eq!(record["headers"][0]["value"], "***");
    }
}

#[tokio::test]
async fn a_page_says_whether_a_rule_covers_its_topic() {
    let path = format!("{RECORDS}?limit=2");
    let plain = TestApp::over(
        FakeCluster::local().with_records((0..2).map(|offset| card_record(offset, PAN)).collect()),
    )
    .await;
    let protected = TestApp::over(cards(
        "
        secret: {value: 0123456789abcdef0123456789abcdef}
        rules:
          - topics: ['orders.*']
            fields:
              - path: card.number
                strategy: mask
        ",
    ))
    .await;

    assert_eq!(plain.get(&path).await.ok()["obfuscated"], json!(false));
    assert_eq!(protected.get(&path).await.ok()["obfuscated"], json!(true));
}

#[tokio::test]
async fn a_pattern_rule_tokens_a_topic_no_registry_ever_decodes() {
    let charges = (0..3)
        .map(|offset| card_record(offset, PAN).value(format!("charged {PAN} for ada@example.com")))
        .collect();
    let app = TestApp::over(FakeCluster::local().with_records(charges).with_obfuscation(
        r"
        secret: {value: 0123456789abcdef0123456789abcdef}
        rules:
          - topics: ['orders.*']
            patterns:
              - regex: '\d{13,19}'
                strategy: hash
              - regex: '[\w.+-]+@[\w-]+\.[\w.]+'
                strategy: mask
        ",
    ))
    .await;
    let page = app.get(&format!("{RECORDS}?limit=3")).await.ok();

    assert_eq!(page["obfuscated"], json!(true));
    let records = records(&page);
    assert_eq!(records.len(), 3);
    for record in records {
        let value = record["value"].as_str().expect("value");
        assert!(value.starts_with("charged kx:"), "{value}");
        assert!(!value.contains(PAN), "{value}");
        assert!(value.ends_with("for ***"), "{value}");
    }
}

#[tokio::test]
async fn an_obfuscated_topic_cannot_be_filtered_on_the_cleartext_it_hides() {
    let app = TestApp::over(cards(
        "
        secret: {value: 0123456789abcdef0123456789abcdef}
        rules:
          - topics: ['orders.*']
            fields:
              - path: card.number
                strategy: hash
        ",
    ))
    .await;
    let hidden = app
        .get(&format!("{RECORDS}?limit=3&contains=4111"))
        .await
        .ok();
    let visible = app
        .get(&format!("{RECORDS}?limit=3&contains=ord_1"))
        .await
        .ok();

    assert!(
        records(&hidden).is_empty(),
        "a filter must not answer questions about an obfuscated field"
    );
    assert_eq!(records(&visible).len(), 1);
}

#[tokio::test]
async fn a_record_opens_by_partition_and_offset() {
    let opened = TestApp::local()
        .await
        .get(&format!("{RECORDS}/0/3"))
        .await
        .ok();

    assert_eq!(opened["record"]["partition"], 0);
    assert_eq!(opened["record"]["offset"], 3);
    assert_eq!(opened["record"]["key"], "ord_3");
    assert_eq!(opened["obfuscated"], json!(false));
}

#[tokio::test]
async fn a_record_is_verbatim_while_its_text_is_its_bytes() {
    let app = TestApp::over(FakeCluster::local().with_records(vec![
        FixtureRecord::order(0, 0).key("ord_0").value(r#"{"total":42}"#),
        card_record(1, PAN),
        FixtureRecord::order(0, 2).key(framed(7, r#"{"id":"ord_2"}"#)),
        FixtureRecord::order(0, 3).key("ord_3").value(vec![0xff, 0x01]),
    ]))
    .await;
    let page = app
        .get(&format!("{RECORDS}?limit=4&order=OLDEST"))
        .await
        .ok();
    let mut verbatim: Vec<_> = records(&page)
        .iter()
        .map(|record| (record["offset"].as_i64(), record["verbatim"].as_bool()))
        .collect();
    verbatim.sort_unstable();

    assert_eq!(
        verbatim,
        [
            (Some(0), Some(true)),
            (Some(1), Some(false)),
            (Some(2), Some(false)),
            (Some(3), Some(false)),
        ]
    );
}

#[tokio::test]
async fn an_obfuscated_topic_is_never_verbatim() {
    let app = TestApp::over(
        FakeCluster::local()
            .with_records(vec![
                FixtureRecord::order(0, 0)
                    .key("ord_0")
                    .value("paid")
                    .header("x-user-id", "ada"),
            ])
            .with_obfuscation(
                "
                secret: {value: 0123456789abcdef0123456789abcdef}
                rules:
                  - topics: ['orders.*']
                    headers: ['x-user-id']
                ",
            ),
    )
    .await;
    let opened = app.get(&format!("{RECORDS}/0/0")).await.ok();

    assert_eq!(opened["record"]["value"], "paid");
    assert_eq!(opened["record"]["headers"][0]["value"], "***");
    assert_eq!(opened["record"]["verbatim"], json!(false));
}

#[tokio::test]
async fn an_offset_with_no_record_is_not_found() {
    let app = TestApp::local().await;
    for offset in ["2", "8", "-1"] {
        app.get(&format!("{RECORDS}/0/{offset}"))
            .await
            .assert_error(StatusCode::NOT_FOUND, "UNKNOWN_OFFSET");
    }
}

#[tokio::test]
async fn a_record_in_an_unknown_partition_is_not_found() {
    TestApp::local()
        .await
        .get(&format!("{RECORDS}/7/1"))
        .await
        .assert_error(StatusCode::NOT_FOUND, "UNKNOWN_PARTITION");
}

#[tokio::test]
async fn an_opened_record_keeps_the_obfuscation_view() {
    let app = TestApp::over(cards(
        "
        secret: {value: 0123456789abcdef0123456789abcdef}
        rules:
          - topics: ['orders.*']
            fields:
              - path: card.number
                strategy: hash
        ",
    ))
    .await;
    let opened = app.get(&format!("{RECORDS}/0/1")).await.ok();
    let value = opened["record"]["value"].as_str().expect("value");

    assert_eq!(opened["obfuscated"], json!(true));
    assert!(value.contains("\"kx:"), "{value}");
    assert!(!value.contains(PAN), "{value}");
}

#[tokio::test]
async fn a_tail_announces_where_it_starts_then_streams_what_arrives() {
    let app = TestApp::local().await;
    let mut tail = app.open(TAIL).await;
    app.cluster().produce(produced(0, 8, "new"));

    let [ready, batch] = <[_; 2]>::try_from(tail.take(2).await).expect("two events");

    assert_eq!(ready.name, "ready");
    assert_eq!(
        ready.data,
        json!({
            "type": "ready",
            "start": [
                { "partition": 0, "offset": 8 },
                { "partition": 1, "offset": 8 },
            ],
            "obfuscated": false,
        })
    );
    assert_eq!(batch.name, "records");
    assert_eq!(batch.data["type"], "records");
    assert_eq!(batch.data["skipped"], 0);
    let records = records(&batch.data);
    assert_eq!(records.len(), 1);
    assert_eq!(records[0]["offset"], 8);
    assert_eq!(records[0]["key"], "new");
}

#[tokio::test]
async fn a_tail_narrows_to_its_partition_and_filter() {
    let app = TestApp::local().await;
    let mut tail = app.open(&format!("{TAIL}?partition=1&contains=hit")).await;
    app.cluster().produce(produced(0, 8, "hit elsewhere"));
    app.cluster().produce(produced(1, 8, "miss"));
    app.cluster().produce(produced(1, 9, "hit"));

    let ready = tail.next().await;
    let batch = tail.next().await;

    assert_eq!(
        ready.data["start"],
        json!([{ "partition": 1, "offset": 8 }])
    );
    let records = records(&batch.data);
    assert_eq!(records.len(), 1);
    assert_eq!(records[0]["partition"], 1);
    assert_eq!(records[0]["key"], "hit");
}

#[tokio::test]
async fn a_tail_follows_a_set_of_partitions() {
    let app = TestApp::local().await;
    let mut tail = app.open(&format!("{TAIL}?partition=1&partition=0")).await;

    let ready = tail.next().await;

    assert_eq!(
        ready.data["start"],
        json!([
            { "partition": 0, "offset": 8 },
            { "partition": 1, "offset": 8 },
        ])
    );
}

#[tokio::test]
async fn an_expired_session_ends_the_tail_with_an_error_frame() {
    let app = TestApp::local().await;
    let mut tail = app.with_guard(SessionGuard::expired()).open(TAIL).await;
    app.cluster().produce(produced(0, 8, "unseen"));

    let ending = tail.take(2).await.pop().expect("two events");

    assert_eq!(ending.name, "error");
    assert_eq!(ending.data["code"], "SESSION_EXPIRED");
}

#[tokio::test]
async fn tails_past_capacity_are_turned_away_until_one_closes() {
    let app = TestApp::of([FakeCluster::local()])
        .limits(Limits {
            live_tails: 1,
            ..Limits::new(&Tuning::default())
        })
        .ingested()
        .await;

    app.get("/clusters/local/topics/ghost/records/tail")
        .await
        .assert_error(StatusCode::NOT_FOUND, "UNKNOWN_TOPIC");
    let open = app.open(TAIL).await;
    app.get(TAIL)
        .await
        .assert_error(StatusCode::SERVICE_UNAVAILABLE, "TOO_MANY_TAILS");

    drop(open);
    app.open(TAIL).await;
}

#[tokio::test]
async fn an_export_pages_through_every_record_as_ndjson() {
    let app = TestApp::of([FakeCluster::local()])
        .limits(paging_by(3))
        .ingested()
        .await;
    let export = app.open(&format!("{EXPORT}?order=OLDEST")).await;

    assert_eq!(export.headers["content-type"], "application/x-ndjson");
    assert_eq!(
        export.headers["content-disposition"],
        "attachment; filename=\"orders.created.ndjson\""
    );
    let records = export.ndjson().await.expect("the export completes");
    assert_eq!(offsets(&records), [0, 1, 2, 3, 4, 5, 6, 7]);
    assert_eq!(records[0]["key"], "ord_0");
    assert_eq!(records[0]["headers"][0]["key"], "source");
    assert_eq!(records[0]["timestamp"], "2023-11-14T22:13:20Z");
    assert_eq!(app.cluster().calls(Api::LowWatermarks), 1);
}

#[tokio::test]
async fn an_export_applies_the_records_view_filter() {
    let app = TestApp::local().await;
    let export = async |query: &str| {
        let export = app.open(&format!("{EXPORT}?{query}")).await;
        offsets(&export.ndjson().await.expect("the export completes"))
    };

    assert_eq!(export("").await, [7, 6, 5, 4, 3, 2, 1, 0]);
    assert_eq!(export("contains=ord_3").await, [3]);
    assert_eq!(export("partition=1&order=OLDEST").await, [0, 2, 4, 6]);
    assert_eq!(
        export("order=OLDEST&from=2023-11-14T22:13:23Z&to=2023-11-14T22:13:25Z").await,
        [3, 4, 5]
    );
}

#[tokio::test]
async fn an_export_leaves_out_records_produced_after_it_opened() {
    let app = TestApp::local().await;
    let export = app.open(&format!("{EXPORT}?order=OLDEST")).await;
    app.cluster().produce(produced(0, 8, "late"));

    let records = export.ndjson().await.expect("the export completes");

    assert_eq!(offsets(&records), [0, 1, 2, 3, 4, 5, 6, 7]);
}

#[tokio::test]
async fn an_expired_session_breaks_off_the_export() {
    let app = TestApp::local().await;
    let export = app.with_guard(SessionGuard::expired()).open(EXPORT).await;

    assert!(export.ndjson().await.is_err());
}

#[tokio::test]
async fn an_export_is_refused_before_it_streams() {
    let app = TestApp::local().await;

    app.with_access(access([viewer()]))
        .get(EXPORT)
        .await
        .assert_error(StatusCode::FORBIDDEN, "FORBIDDEN");
    app.get("/clusters/local/topics/ghost/records/export")
        .await
        .assert_error(StatusCode::NOT_FOUND, "UNKNOWN_TOPIC");
}

async fn writable() -> TestApp {
    TestApp::of([FakeCluster::local()])
        .writable(&["local"])
        .ingested()
        .await
}

#[tokio::test]
async fn a_produced_record_reads_back_at_the_offset_the_produce_names() {
    let app = writable().await;
    let logs = LogCapture::at(Level::INFO);

    let produced = app
        .post(
            RECORDS,
            &json!({
                "partition": 1,
                "key": { "encoding": "TEXT", "data": "order-9" },
                "value": { "encoding": "BASE64", "data": "eyJ0b3RhbCI6NDJ9" },
                "headers": [{ "key": "trace", "value": "abc" }]
            }),
        )
        .await
        .expect(StatusCode::CREATED);

    assert_eq!(produced, json!({ "partition": 1, "offset": 8 }));
    let record = app.get(&format!("{RECORDS}/1/8")).await.ok();
    assert_eq!(record["record"]["key"], "order-9");
    assert_eq!(record["record"]["value"], r#"{"total":42}"#);
    assert_eq!(
        record["record"]["headers"],
        json!([{ "key": "trace", "value": "abc" }])
    );
    assert_eq!(app.cluster().calls(Api::Produce), 1);
    logs.assert_contains("produced record");
}

#[tokio::test]
async fn a_keyless_tombstone_keeps_both_nulls() {
    let app = writable().await;

    app.post(RECORDS, &json!({ "key": null, "value": null }))
        .await
        .expect(StatusCode::CREATED);

    let record = app.get(&format!("{RECORDS}/0/8")).await.ok();
    assert_eq!(record["record"]["key"], Value::Null);
    assert_eq!(record["record"]["value"], Value::Null);
}

#[tokio::test]
async fn a_produce_kafka_would_reject_never_reaches_the_broker() {
    let app = writable().await;

    for (body, status, code) in [
        (
            json!({ "key": { "encoding": "BASE64", "data": "!" }, "value": null }),
            StatusCode::UNPROCESSABLE_ENTITY,
            "INVALID_REQUEST",
        ),
        (
            json!({ "key": null, "value": { "encoding": "HEX", "data": "00" } }),
            StatusCode::UNPROCESSABLE_ENTITY,
            "INVALID_REQUEST",
        ),
        (
            json!({ "partition": 2, "key": null, "value": null }),
            StatusCode::NOT_FOUND,
            "UNKNOWN_PARTITION",
        ),
    ] {
        app.post(RECORDS, &body).await.assert_error(status, code);
    }
    app.post(
        "/clusters/local/topics/ghost/records",
        &json!({ "key": null, "value": null }),
    )
    .await
    .assert_error(StatusCode::NOT_FOUND, "UNKNOWN_TOPIC");

    assert_eq!(app.cluster().calls(Api::Produce), 0);
}

#[tokio::test]
async fn a_payload_that_is_not_base64_names_its_part() {
    let app = writable().await;

    let reply = app
        .post(
            RECORDS,
            &json!({ "key": null, "value": { "encoding": "BASE64", "data": "%%" } }),
        )
        .await;

    assert_eq!(reply.body["error"], "the value is not valid base64");
}

#[tokio::test]
async fn wrapped_base64_decodes_as_one_payload() {
    let app = writable().await;

    app.post(
        RECORDS,
        &json!({ "key": null, "value": { "encoding": "BASE64", "data": "eyJ0b3Rh\r\nbCI6NDJ9\n" } }),
    )
    .await
    .expect(StatusCode::CREATED);

    let record = app.get(&format!("{RECORDS}/0/8")).await.ok();
    assert_eq!(record["record"]["value"], r#"{"total":42}"#);
}

#[tokio::test]
async fn an_internal_topic_takes_no_records() {
    let app = TestApp::of([FakeCluster::local().with_topic("__consumer_offsets", 1, 0)])
        .writable(&["local"])
        .ingested()
        .await;

    app.post(
        "/clusters/local/topics/__consumer_offsets/records",
        &json!({ "key": null, "value": null }),
    )
    .await
    .assert_error(StatusCode::UNPROCESSABLE_ENTITY, "INTERNAL_TOPIC");

    assert_eq!(app.cluster().calls(Api::Produce), 0);
}

#[tokio::test(start_paused = true)]
async fn deleted_records_show_before_the_delete_answers() {
    let app = writable().await;
    let mut rig = app.rig();
    let lane = rig.watermarks();
    rig.spawn(lane);
    quiesce().await;
    let logs = LogCapture::at(Level::INFO);

    app.delete(&format!("{RECORDS}?partition=1&before=5"))
        .await
        .expect(StatusCode::NO_CONTENT);

    let topic = app.get("/clusters/local/topics/orders.created").await.ok();
    let lows: Vec<_> = topic["partitions"]
        .as_array()
        .expect("partitions")
        .iter()
        .map(|partition| partition["lowWatermark"].clone())
        .collect();
    assert_eq!(lows, [json!(0), json!(5)]);
    app.get(&format!("{RECORDS}/1/4"))
        .await
        .assert_error(StatusCode::NOT_FOUND, "UNKNOWN_OFFSET");
    assert_eq!(app.cluster().calls(Api::DeleteRecords), 1);
    logs.assert_contains(
        r#"deleted records cluster=local topic="orders.created" partitions=[1] before=5"#,
    );
}

#[tokio::test(start_paused = true)]
async fn emptying_a_topic_deletes_every_partition_up_to_its_end() {
    let app = writable().await;
    let mut rig = app.rig();
    let lane = rig.watermarks();
    rig.spawn(lane);
    quiesce().await;

    app.delete(RECORDS).await.expect(StatusCode::NO_CONTENT);

    let topic = app.get("/clusters/local/topics/orders.created").await.ok();
    assert_eq!(topic["retainedMessages"], 0);
    let page = app.get(RECORDS).await.ok();
    assert!(records(&page).is_empty(), "{page}");
}

#[tokio::test(start_paused = true)]
async fn a_deletion_kafka_would_reject_never_reaches_the_broker() {
    let app = writable().await;

    for (path, status, code) in [
        (
            format!("{RECORDS}?before=-1"),
            StatusCode::UNPROCESSABLE_ENTITY,
            "INVALID_REQUEST",
        ),
        (
            format!("{RECORDS}?partition=0&partition=2"),
            StatusCode::NOT_FOUND,
            "UNKNOWN_PARTITION",
        ),
        (
            "/clusters/local/topics/ghost/records".to_owned(),
            StatusCode::NOT_FOUND,
            "UNKNOWN_TOPIC",
        ),
    ] {
        app.delete(&path).await.assert_error(status, code);
    }

    assert_eq!(app.cluster().calls(Api::DeleteRecords), 0);
}

#[tokio::test(start_paused = true)]
async fn deleting_past_the_end_carries_the_broker_refusal() {
    let app = writable().await;

    app.delete(&format!("{RECORDS}?partition=0&before=0"))
        .await
        .expect(StatusCode::NO_CONTENT);
    app.delete(&format!("{RECORDS}?partition=0&before=9"))
        .await
        .assert_error(StatusCode::UNPROCESSABLE_ENTITY, "REFUSED");
}
