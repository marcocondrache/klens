use crate::kafka::store::fixtures::{open_partition, transaction};
use crate::kafka::transaction::TransactionState;

use super::*;

const NOW_MS: i64 = 1_800_000_000_000;

fn written(open: OpenPartition, last_timestamp_ms: i64) -> OpenPartition {
    OpenPartition {
        last_timestamp_ms: Some(last_timestamp_ms),
        ..open
    }
}

fn hanging(topic: &str, partition: i32, producer_id: i64, open_offset: i64) -> HangingPartition {
    HangingPartition {
        topic: Arc::from(topic),
        partition,
        producer_id,
        producer_epoch: 0,
        open_offset,
        open_since_ms: None,
        transactional_id: None,
        reason: HangingReason::PastTimeout,
    }
}

#[test]
fn a_transaction_its_coordinator_holds_open_on_the_partition_is_not_hanging() {
    let open = written(open_partition("orders", 0, 7, 40), NOW_MS - 1_000);
    let coordinator = transaction("payments-1", 7, NOW_MS - 1_000, &[("orders", 0)]);

    assert_eq!(
        classify(&open, Some(&coordinator), NOW_MS),
        Verdict::Healthy
    );
}

#[test]
fn a_transaction_open_past_the_max_timeout_is_hanging() {
    let open = OpenPartition {
        max_timeout_ms: 60_000,
        ..written(open_partition("orders", 0, 7, 40), NOW_MS)
    };
    let coordinator = transaction("payments-1", 7, NOW_MS - 60_000, &[("orders", 0)]);

    assert_eq!(
        classify(&open, Some(&coordinator), NOW_MS),
        Verdict::Healthy,
        "a transaction exactly as old as the limit still has time"
    );
    assert_eq!(
        classify(&open, Some(&coordinator), NOW_MS + 1),
        Verdict::Hanging(HangingPartition {
            open_since_ms: Some(NOW_MS - 60_000),
            transactional_id: Some("payments-1".into()),
            ..hanging("orders", 0, 7, 40)
        }),
        "the coordinator's start time wins over the producer's last write"
    );
}

#[test]
fn a_transaction_its_coordinator_does_not_hold_open_is_hanging() {
    let open = written(open_partition("orders", 1, 7, 40), NOW_MS - 1_000);
    let mut finished = transaction("payments-1", 7, NOW_MS - 1_000, &[("orders", 1)]);
    finished.state = TransactionState::CompleteCommit;
    let elsewhere = transaction("payments-1", 7, NOW_MS - 1_000, &[("orders", 0)]);

    for coordinator in [finished, elsewhere] {
        assert_eq!(
            classify(&open, Some(&coordinator), NOW_MS),
            Verdict::Hanging(HangingPartition {
                open_since_ms: Some(NOW_MS - 1_000),
                transactional_id: Some("payments-1".into()),
                reason: HangingReason::UnknownToCoordinator,
                ..hanging("orders", 1, 7, 40)
            }),
        );
    }
}

#[test]
fn a_producer_no_coordinator_lists_hangs_only_past_the_max_timeout() {
    let limited = |last_timestamp_ms| OpenPartition {
        max_timeout_ms: 60_000,
        last_timestamp_ms,
        ..open_partition("orders", 0, 7, 40)
    };

    assert_eq!(
        classify(&limited(Some(NOW_MS - 1_000)), None, NOW_MS),
        Verdict::Unlisted
    );
    assert_eq!(classify(&limited(None), None, NOW_MS), Verdict::Unlisted);
    assert_eq!(
        classify(&limited(Some(NOW_MS - 60_001)), None, NOW_MS),
        Verdict::Hanging(HangingPartition {
            open_since_ms: Some(NOW_MS - 60_001),
            ..hanging("orders", 0, 7, 40)
        }),
    );
}

#[test]
fn a_transaction_seen_once_is_not_hanging_yet() {
    let open = OpenPartition {
        seen_before: false,
        ..written(open_partition("orders", 1, 7, 40), NOW_MS - 1_000)
    };
    let mut finished = transaction("payments-1", 7, NOW_MS - 1_000, &[("orders", 1)]);
    finished.state = TransactionState::CompleteCommit;

    assert_eq!(
        classify(&open, Some(&finished), NOW_MS),
        Verdict::Healthy,
        "it may have finished between the two answers"
    );
}

#[test]
fn hanging_lists_the_partitions_that_will_never_finish_and_counts_unlisted_producers() {
    let mut finished = transaction("stale", 9, NOW_MS - 1_000, &[("orders", 1)]);
    finished.state = TransactionState::CompleteAbort;
    let table = TransactionTable {
        transactions: vec![
            transaction("payments-1", 7, NOW_MS - 1_000, &[("orders", 0)]),
            finished,
        ],
        open_partitions: vec![
            written(open_partition("orders", 0, 7, 40), NOW_MS - 1_000),
            written(open_partition("orders", 1, 9, 12), NOW_MS - 1_000),
            written(open_partition("orders", 2, 11, 3), NOW_MS - 1_000),
            written(open_partition("orders", 3, 11, 8), NOW_MS - 1_000),
        ],
        ..TransactionTable::default()
    };

    assert_eq!(
        Hanging::of(&table, NOW_MS),
        Hanging {
            partitions: vec![HangingPartition {
                open_since_ms: Some(NOW_MS - 1_000),
                transactional_id: Some("stale".into()),
                reason: HangingReason::UnknownToCoordinator,
                ..hanging("orders", 1, 9, 12)
            }],
            unlisted_producers: 1,
        }
    );
}

#[test]
fn hanging_counts_each_partition_once() {
    let found = Hanging {
        partitions: vec![
            hanging("orders", 0, 7, 40),
            hanging("orders", 0, 9, 41),
            hanging("orders", 1, 7, 12),
        ],
        ..Hanging::default()
    };

    assert_eq!(found.partition_count(), 2);
    assert!(found.on("orders", 0));
    assert!(found.on("orders", 1));
    assert!(!found.on("orders", 2));
    assert!(!found.on("payments", 0));
}
