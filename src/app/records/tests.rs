use axum::http::StatusCode;
use serde_json::json;

use crate::app::auth::SessionGuard;
use crate::app::auth::access::EffectiveAccess;
use crate::kafka::FakeCluster;
use crate::kafka::card_record;
use crate::kafka::model as domain;

use super::super::harness::{
    failure, ok, open_stream, read_frames, seeded, seeded_with, viewer_everywhere,
};
use super::types::Record;

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
        compression: domain::Compression::None,
    };

    assert_eq!(Record::from(record).schema_id, Some(12));
}

#[tokio::test]
async fn records_are_read_live_through_the_scan_path() {
    let state = seeded();
    let data = ok(
        &state,
        "/clusters/local/topics/orders.created/records?limit=3",
    )
    .await;
    let records = data["records"].as_array().expect("records");

    assert_eq!(records.len(), 3);
    assert_eq!(records[0]["topic"], "orders.created");
    assert_eq!(records[0]["sizeBytes"], "24");
    assert_eq!(records[0]["compression"], "NONE");
    records[0]["timestamp"]
        .as_str()
        .expect("timestamp")
        .parse::<jiff::Timestamp>()
        .expect("RFC 3339");
}

#[tokio::test]
async fn records_accept_rfc3339_timestamp_bounds() {
    let state = seeded();
    let data = ok(
        &state,
        "/clusters/local/topics/orders.created/records?order=OLDEST&from=2023-11-14T22:13:23Z&to=2023-11-14T22:13:25Z",
    )
    .await;
    let records = data["records"].as_array().expect("records");
    let offsets: Vec<_> = records
        .iter()
        .map(|record| record["offset"].as_str().expect("offset"))
        .collect();

    assert_eq!(offsets, ["3", "4", "5"]);
    assert_eq!(records[0]["timestamp"], "2023-11-14T22:13:23Z");
    assert_eq!(records[2]["timestamp"], "2023-11-14T22:13:25Z");
}

#[tokio::test]
async fn an_inverted_record_range_is_rejected() {
    let (status, code) = failure(
        &seeded(),
        "/clusters/local/topics/orders.created/records?from=2023-11-14T22:13:25Z&to=2023-11-14T22:13:23Z",
        EffectiveAccess::Unrestricted,
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(code, "INVERTED_TIMESTAMP_RANGE");
}

#[tokio::test]
async fn an_obfuscated_topic_serves_tokens_instead_of_payloads() {
    let pan = "4111111111111111";
    let records = (0..3).map(|offset| card_record(offset, pan)).collect();
    let state = seeded_with(
        FakeCluster::local()
            .with_orders_records(records)
            .with_obfuscation(
                "
                secret: 0123456789abcdef0123456789abcdef
                rules:
                  - topics: ['orders.*']
                    headers: ['x-user-id']
                    fields:
                      - path: card.number
                        strategy: hash
                      - path: card.cvv
                        strategy: drop
                ",
            ),
    )
    .0;
    let data = ok(
        &state,
        "/clusters/local/topics/orders.created/records?limit=3",
    )
    .await;
    let records = data["records"].as_array().expect("records");

    assert_eq!(records.len(), 3);
    for record in records {
        let value = record["value"].as_str().expect("value");
        assert!(value.contains("\"kx:"), "{value}");
        assert!(!value.contains(pan), "{value}");
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
    let pan = "4111111111111111";
    let records: Vec<_> = (0..2).map(|offset| card_record(offset, pan)).collect();
    let rules = "
        rules:
          - topics: ['orders.*']
            fields:
              - path: card.number
                strategy: mask
        ";
    let plain = seeded_with(FakeCluster::local().with_orders_records(records.clone())).0;
    let protected = seeded_with(
        FakeCluster::local()
            .with_orders_records(records)
            .with_obfuscation(rules),
    )
    .0;
    let path = "/clusters/local/topics/orders.created/records?limit=2";

    assert_eq!(
        ok(&plain, path).await["obfuscated"],
        serde_json::json!(false)
    );
    assert_eq!(
        ok(&protected, path).await["obfuscated"],
        serde_json::json!(true)
    );
}

#[tokio::test]
async fn a_pattern_rule_tokens_a_topic_no_registry_ever_decodes() {
    let pan = "4111111111111111";
    let records = (0..3)
        .map(|offset| {
            let mut record = card_record(offset, pan);
            record.value = Some(format!("charged {pan} for ada@example.com"));
            record
        })
        .collect();
    let state = seeded_with(
        FakeCluster::local()
            .with_orders_records(records)
            .with_obfuscation(
                r"
                secret: 0123456789abcdef0123456789abcdef
                rules:
                  - topics: ['orders.*']
                    patterns:
                      - regex: '\d{13,19}'
                        strategy: hash
                      - regex: '[\w.+-]+@[\w-]+\.[\w.]+'
                        strategy: mask
                ",
            ),
    )
    .0;
    let data = ok(
        &state,
        "/clusters/local/topics/orders.created/records?limit=3",
    )
    .await;

    assert_eq!(data["obfuscated"], serde_json::json!(true));
    let records = data["records"].as_array().expect("records");
    assert_eq!(records.len(), 3);
    for record in records {
        let value = record["value"].as_str().expect("value");
        assert!(value.starts_with("charged kx:"), "{value}");
        assert!(!value.contains(pan), "{value}");
        assert!(value.ends_with("for ***"), "{value}");
    }
}

#[tokio::test]
async fn an_obfuscated_topic_cannot_be_filtered_on_the_cleartext_it_hides() {
    let pan = "4111111111111111";
    let records = (0..3).map(|offset| card_record(offset, pan)).collect();
    let state = seeded_with(
        FakeCluster::local()
            .with_orders_records(records)
            .with_obfuscation(
                "
                secret: 0123456789abcdef0123456789abcdef
                rules:
                  - topics: ['orders.*']
                    fields:
                      - path: card.number
                        strategy: hash
                ",
            ),
    )
    .0;
    let hidden = ok(
        &state,
        "/clusters/local/topics/orders.created/records?limit=3&contains=4111",
    )
    .await;
    let visible = ok(
        &state,
        "/clusters/local/topics/orders.created/records?limit=3&contains=ord_1",
    )
    .await;

    assert!(
        hidden["records"].as_array().expect("records").is_empty(),
        "a filter must not answer questions about an obfuscated field"
    );
    assert_eq!(visible["records"].as_array().expect("records").len(), 1);
}

#[tokio::test]
async fn records_are_forbidden_without_the_records_privilege() {
    let (status, code) = failure(
        &seeded(),
        "/clusters/local/topics/orders.created/records",
        viewer_everywhere(),
    )
    .await;

    assert_eq!(
        (status, code.as_str()),
        (StatusCode::FORBIDDEN, "FORBIDDEN")
    );
}

const TAIL: &str = "/clusters/local/topics/orders.created/records/tail";

fn produced(partition: i32, offset: i64, key: &str) -> domain::Record {
    domain::Record {
        topic: "orders.created".into(),
        partition,
        offset,
        timestamp: 1_700_000_100_000 + offset,
        key: Some(key.into()),
        value: None,
        schema_id: None,
        headers: Vec::new(),
        size_bytes: key.len() as u64,
        compression: domain::Compression::None,
    }
}

#[tokio::test]
async fn a_tail_announces_where_it_starts_then_streams_what_arrives() {
    let (state, session) = seeded_with(FakeCluster::local());
    let response = open_stream(
        &state,
        TAIL,
        EffectiveAccess::Unrestricted,
        SessionGuard::open(),
    )
    .await;
    session.produce(produced(0, 8, "new"));

    let frames = read_frames(response, 2).await;

    assert_eq!(frames[0].0, "ready");
    assert_eq!(
        frames[0].1,
        json!({
            "type": "ready",
            "start": [
                { "partition": 0, "offset": "8" },
                { "partition": 1, "offset": "8" },
            ],
            "obfuscated": false,
        })
    );
    assert_eq!(frames[1].0, "records");
    assert_eq!(frames[1].1["type"], "records");
    assert_eq!(frames[1].1["skipped"], "0");
    let records = frames[1].1["records"].as_array().expect("records");
    assert_eq!(records.len(), 1);
    assert_eq!(records[0]["offset"], "8");
    assert_eq!(records[0]["key"], "new");
}

#[tokio::test]
async fn a_tail_narrows_to_its_partition_and_filter() {
    let (state, session) = seeded_with(FakeCluster::local());
    let response = open_stream(
        &state,
        &format!("{TAIL}?partition=1&contains=hit"),
        EffectiveAccess::Unrestricted,
        SessionGuard::open(),
    )
    .await;
    session.produce(produced(0, 8, "hit elsewhere"));
    session.produce(produced(1, 8, "miss"));
    session.produce(produced(1, 9, "hit"));

    let frames = read_frames(response, 2).await;

    assert_eq!(
        frames[0].1["start"],
        json!([{ "partition": 1, "offset": "8" }])
    );
    let records = frames[1].1["records"].as_array().expect("records");
    assert_eq!(records.len(), 1);
    assert_eq!(records[0]["partition"], 1);
    assert_eq!(records[0]["key"], "hit");
}

#[tokio::test]
async fn an_expired_session_ends_the_tail_with_an_error_frame() {
    let (state, session) = seeded_with(FakeCluster::local());
    let response = open_stream(
        &state,
        TAIL,
        EffectiveAccess::Unrestricted,
        SessionGuard::expired(),
    )
    .await;
    session.produce(produced(0, 8, "unseen"));

    let frames = read_frames(response, 2).await;

    assert_eq!(frames[1].0, "error");
    assert_eq!(frames[1].1["code"], "SESSION_EXPIRED");
}

#[tokio::test]
async fn a_tail_is_forbidden_without_the_records_privilege() {
    let (status, code) = failure(&seeded(), TAIL, viewer_everywhere()).await;

    assert_eq!(
        (status, code.as_str()),
        (StatusCode::FORBIDDEN, "FORBIDDEN")
    );
}

#[tokio::test]
async fn tails_past_capacity_are_turned_away_until_one_closes() {
    let state = seeded().with_tail_capacity(1);
    let (status, code) = failure(
        &state,
        "/clusters/local/topics/ghost/records/tail",
        EffectiveAccess::Unrestricted,
    )
    .await;
    assert_eq!(
        (status, code.as_str()),
        (StatusCode::NOT_FOUND, "UNKNOWN_TOPIC"),
        "a tail that never opened gives its seat back"
    );

    let open = open_stream(
        &state,
        TAIL,
        EffectiveAccess::Unrestricted,
        SessionGuard::open(),
    )
    .await;
    assert_eq!(open.status(), StatusCode::OK);

    let (status, code) = failure(&state, TAIL, EffectiveAccess::Unrestricted).await;
    assert_eq!(
        (status, code.as_str()),
        (StatusCode::SERVICE_UNAVAILABLE, "TOO_MANY_TAILS")
    );

    drop(open);
    let reopened = open_stream(
        &state,
        TAIL,
        EffectiveAccess::Unrestricted,
        SessionGuard::open(),
    )
    .await;
    assert_eq!(reopened.status(), StatusCode::OK);
}
