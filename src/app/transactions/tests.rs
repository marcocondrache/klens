use std::sync::Arc;
use std::time::Duration;

use jiff::Timestamp;
use serde_json::{Value, json};

use crate::kafka::model::TransactionState;
use crate::kafka::store::fixtures::{open_partition, transaction};
use crate::kafka::store::{ClusterStore, OpenPartition, TransactionTable};

use super::super::harness::{ok, ok_as, seeded, state, store_of, viewer_everywhere};

fn now_ms() -> i64 {
    Timestamp::now().as_millisecond()
}

/// Producer 9 left a transaction open on `orders.created` 1 that its
/// coordinator already aborted.
fn stuck() -> TransactionTable {
    let mut aborted = transaction("stale", 9, 1_000, &[("orders.created", 1)]);
    aborted.state = TransactionState::CompleteAbort;
    TransactionTable {
        transactions: vec![aborted],
        open_partitions: vec![OpenPartition {
            producer_epoch: 2,
            last_timestamp_ms: Some(1_700_000_000_000),
            ..open_partition("orders.created", 1, 9, 55)
        }],
        partition_count: 3,
        ..TransactionTable::default()
    }
}

fn committed(store: &ClusterStore, table: TransactionTable) {
    store.transactions.commit(Arc::new(table));
}

fn names(values: &Value) -> Vec<&str> {
    values
        .as_array()
        .expect("an array")
        .iter()
        .map(|value| value.as_str().expect("a string"))
        .collect()
}

#[tokio::test]
async fn a_viewer_sees_the_open_transactions_and_the_hanging_partitions() {
    let state = seeded();
    let started = now_ms() - 1_000;
    let mut table = stuck();
    table.transactions.splice(
        0..0,
        [
            transaction("payments-0", 6, 1_000, &[("payments.settled", 0)]),
            transaction("payments-1", 7, started, &[("orders.created", 0)]),
        ],
    );
    committed(store_of(&state, "local"), table);

    let data = ok_as(&state, "/clusters/local/transactions", viewer_everywhere()).await;

    assert_eq!(
        data["open"][1],
        json!({
            "transactionalId": "payments-1",
            "producerId": 7,
            "producerEpoch": 0,
            "state": "ONGOING",
            "startedAt": Timestamp::from_millisecond(started).unwrap(),
            "timeoutMs": 60_000,
            "pastTimeout": false,
            "partitions": [{ "topic": "orders.created", "partition": 0 }]
        })
    );
    assert_eq!(data["open"].as_array().map(Vec::len), Some(2));
    assert_eq!(data["open"][0]["transactionalId"], "payments-0");
    assert_eq!(data["open"][0]["pastTimeout"], true);
    assert_eq!(
        data["hanging"],
        json!([{
            "topic": "orders.created",
            "partition": 1,
            "producerId": 9,
            "producerEpoch": 2,
            "offset": 55,
            "openSince": Timestamp::from_millisecond(1_700_000_000_000).unwrap(),
            "transactionalId": "stale",
            "reason": "PAST_TIMEOUT",
            "groups": ["order-processor"]
        }])
    );
    assert_eq!(
        data["coverage"],
        json!({
            "partitionCount": 3,
            "openPartitionCount": 1,
            "deniedTopics": [],
            "uncheckedPartitions": 0,
            "unlistedProducers": 0
        })
    );
}

#[tokio::test]
async fn nothing_is_listed_before_the_lanes_commit() {
    let data = ok(&state(), "/clusters/local/transactions").await;

    assert_eq!(data, json!({ "open": [], "hanging": [], "coverage": null }));
}

#[tokio::test]
async fn a_hanging_partition_on_a_topic_no_group_reads_names_no_group() {
    let state = state();
    let mut table = stuck();
    table.transactions[0].partitions = vec![("audit.log".into(), 1)];
    table.open_partitions[0].topic = Arc::from("audit.log");
    committed(store_of(&state, "local"), table);

    let data = ok(&state, "/clusters/local/transactions").await;

    assert_eq!(data["hanging"][0]["groups"], json!([]));
}

#[tokio::test]
async fn a_topic_klens_may_not_read_is_named_instead_of_passing_as_clean() {
    let state = seeded();
    committed(
        store_of(&state, "local"),
        TransactionTable {
            open_partitions: vec![
                open_partition("orders.created", 0, 7, 40),
                open_partition("orders.created", 1, 8, 12),
            ],
            denied_topics: vec![Arc::from("payments.settled")],
            unchecked_partitions: 1,
            ..TransactionTable::default()
        },
    );

    let data = ok(&state, "/clusters/local/transactions").await;

    assert_eq!(
        names(&data["coverage"]["deniedTopics"]),
        vec!["payments.settled"]
    );
    assert_eq!(data["coverage"]["uncheckedPartitions"], 1);
    assert_eq!(data["coverage"]["unlistedProducers"], 2);
    assert_eq!(data["hanging"], json!([]));
}

#[tokio::test]
async fn cluster_health_counts_the_hanging_partitions_once_the_lane_commits() {
    let state = seeded();
    let store = store_of(&state, "local");

    let before = ok(&state, "/clusters/local").await;
    committed(store, stuck());
    store
        .transactions
        .record_poll(Duration::from_millis(5), Some("broker down".into()));
    let after = ok(&state, "/clusters/local").await;

    assert_eq!(before["hangingPartitions"], Value::Null);
    assert_eq!(
        after["hangingPartitions"], 1,
        "a failed look keeps the last count"
    );
    assert_eq!(after["transactions"]["lastError"], "broker down");
}

#[tokio::test]
async fn a_group_names_the_partitions_a_hanging_transaction_holds_back() {
    let state = seeded();

    let before = ok(&state, "/clusters/local/groups/order-processor").await;
    committed(store_of(&state, "local"), stuck());
    let after = ok(&state, "/clusters/local/groups/order-processor").await;

    assert_eq!(before["blockedPartitions"], json!([]));
    assert_eq!(
        after["blockedPartitions"],
        json!([{ "topic": "orders.created", "partition": 1 }])
    );
}
