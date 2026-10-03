use e2e::{Kafka, Klens};
use krafka::producer::ProducerRecord;
use serde_json::{Value, json};

use crate::row;

const RECORDS: &str = "/api/clusters/local/topics/orders/records";

async fn orders(count: usize) -> (Kafka, Klens, Vec<String>) {
    let kafka = Kafka::start().await;
    kafka.topic("orders", 1).await;
    let keys: Vec<String> = (0..count).map(|n| format!("order-{n}")).collect();
    kafka.fill("orders", 0, &keys).await;
    let klens = Klens::over(&kafka).await;
    klens
        .eventually("/api/clusters/local/topics", |rows| {
            row(rows, "name", "orders")["retainedMessages"] == count
        })
        .await;
    (kafka, klens, keys)
}

fn keys(records: &[Value]) -> Vec<String> {
    records
        .iter()
        .map(|record| record["key"].as_str().expect("key").to_owned())
        .collect()
}

#[tokio::test]
async fn records_page_back_in_the_order_they_were_produced() {
    let (_kafka, klens, produced) = orders(25).await;

    let mut read = Vec::new();
    let mut page = klens.get(&format!("{RECORDS}?order=OLDEST&limit=10")).await;
    loop {
        read.extend(keys(page["records"].as_array().expect("records")));
        let Some(cursor) = page["nextCursor"].as_str() else {
            break;
        };
        page = klens
            .get(&format!("{RECORDS}?order=OLDEST&limit=10&cursor={cursor}"))
            .await;
    }

    assert_eq!(read, produced);
    let newest = klens.get(&format!("{RECORDS}?limit=1")).await;
    assert_eq!(newest["records"][0]["key"], "order-24");
}

#[tokio::test]
async fn a_record_reads_back_with_its_key_value_and_headers() {
    let (kafka, klens, _) = orders(3).await;
    let offset = kafka
        .send(
            ProducerRecord::new("orders", r#"{"total":42}"#)
                .with_key("order-3")
                .with_partition(0)
                .with_header("source", "checkout"),
        )
        .await;

    let found = klens.get(&format!("{RECORDS}/0/{offset}")).await;
    let record = &found["record"];

    assert_eq!(record["offset"], 3);
    assert_eq!(record["key"], "order-3");
    assert_eq!(record["value"], r#"{"total":42}"#);
    assert_eq!(
        record["headers"],
        json!([{ "key": "source", "value": "checkout" }])
    );
}

#[tokio::test]
async fn a_tail_hears_a_record_produced_after_it_opened() {
    let (kafka, klens, _) = orders(3).await;
    let mut tail = klens.open(&format!("{RECORDS}/tail")).await;

    let ready = tail.next().await;
    kafka.fill("orders", 0, &["late"]).await;
    let batch = tail.find(|event| event.name == "records").await;

    assert_eq!(ready.name, "ready");
    assert_eq!(
        ready.data["start"],
        json!([{ "partition": 0, "offset": 3 }])
    );
    assert_eq!(batch.data["records"][0]["key"], "late");
    assert_eq!(batch.data["records"][0]["offset"], 3);
}

#[tokio::test]
async fn an_export_downloads_every_record_as_ndjson() {
    let (_kafka, klens, produced) = orders(5).await;

    let export = klens.fetch(&format!("{RECORDS}/export?order=OLDEST")).await;

    assert_eq!(export.headers()["content-type"], "application/x-ndjson");
    let lines: Vec<Value> = export
        .text()
        .await
        .expect("the export body")
        .lines()
        .map(|line| serde_json::from_str(line).expect("one json value per line"))
        .collect();
    assert_eq!(keys(&lines), produced);
}
