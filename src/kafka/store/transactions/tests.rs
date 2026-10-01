use foldhash::HashMapExt;

use crate::kafka::store::fixtures::{
    open_partition, open_producer, partition, producers, topic, topology, transaction,
};
use crate::kafka::transaction::{ActiveProducer, TransactionState};

use super::*;

fn orders() -> Topology {
    topology(
        vec![
            topic(
                "orders",
                vec![
                    partition(0, vec![1], vec![1]),
                    partition(1, vec![2], vec![2]),
                ],
            ),
            topic("payments", vec![partition(0, vec![1], vec![1])]),
            topic("audit", vec![partition(0, vec![1], vec![1])]),
        ],
        Vec::new(),
    )
}

fn scanned(open: Vec<OpenPartition>) -> LeaderScan {
    LeaderScan {
        open,
        ..LeaderScan::default()
    }
}

fn unseen(open: OpenPartition) -> OpenPartition {
    OpenPartition {
        seen_before: false,
        ..open
    }
}

#[test]
fn a_scan_sorts_partitions_into_open_denied_and_unchecked() {
    let idle = ActiveProducer {
        open_offset: None,
        ..open_producer(8, 1_000, 0)
    };
    let described = vec![
        producers(
            "orders",
            1,
            vec![open_producer(9, 2_000, 12), open_producer(7, 1_000, 40)],
        ),
        producers("orders", 0, vec![idle]),
        PartitionProducers {
            error: Some("NotLeaderOrFollower".into()),
            ..producers("payments", 0, Vec::new())
        },
        PartitionProducers {
            error: Some("Topic authorization failed.".into()),
            ..producers("audit", 0, Vec::new())
        },
    ];

    let scan = LeaderScan::of(&orders(), &described);

    assert_eq!(scan.partitions, 4);
    assert_eq!(
        scan.open,
        vec![
            OpenPartition {
                leader: 2,
                last_timestamp_ms: Some(1_000),
                ..unseen(open_partition("orders", 1, 7, 40))
            },
            OpenPartition {
                leader: 2,
                last_timestamp_ms: Some(2_000),
                ..unseen(open_partition("orders", 1, 9, 12))
            },
        ]
    );
    assert_eq!(
        scan.denied.into_iter().collect::<Vec<_>>(),
        vec![Arc::<str>::from("audit")]
    );
    assert_eq!(scan.unchecked, 1, "the partition that failed");
}

#[test]
fn a_partition_the_leaders_leave_out_is_unchecked() {
    let scan = LeaderScan::of(&orders(), &[producers("orders", 0, Vec::new())]);

    assert_eq!(scan.unchecked, 3);
}

#[test]
fn an_open_partition_topology_does_not_know_has_no_leader() {
    let scan = LeaderScan::of(
        &orders(),
        &[producers("orders", 7, vec![open_producer(7, 1_000, 40)])],
    );

    assert_eq!(scan.open[0].leader, -1);
}

#[test]
fn a_scan_names_each_producer_and_leader_once() {
    let scan = scanned(vec![
        open_partition("orders", 0, 7, 40),
        OpenPartition {
            leader: 2,
            ..open_partition("orders", 1, 7, 12)
        },
        open_partition("payments", 0, 9, 5),
    ]);

    assert_eq!(
        scan.producer_ids().into_iter().collect::<Vec<_>>(),
        vec![7, 9]
    );
    assert_eq!(scan.leaders().into_iter().collect::<Vec<_>>(), vec![1, 2]);
}

#[test]
fn the_table_keeps_every_described_transaction_once_in_id_order() {
    let mut finished = transaction("payments-0", 6, 1_000, &[("orders", 0)]);
    finished.state = TransactionState::CompleteAbort;
    let mut failed = transaction("payments-9", 9, 1_000, &[("orders", 0)]);
    failed.error = Some("CoordinatorLoadInProgress".into());
    let mut preparing = transaction("payments-1", 7, 1_000, &[("orders", 1)]);
    preparing.state = TransactionState::PrepareAbort;
    let ongoing = transaction("payments-2", 8, 1_000, &[("orders", 0)]);

    let table = TransactionTable::assemble(
        vec![
            ongoing.clone(),
            finished.clone(),
            failed,
            preparing.clone(),
            ongoing.clone(),
        ],
        LeaderScan::default(),
        &HashMap::new(),
        None,
    );

    assert_eq!(
        table.transactions,
        vec![finished.clone(), preparing.clone(), ongoing.clone()]
    );
    assert_eq!(table.open().collect::<Vec<_>>(), vec![&preparing, &ongoing]);
    assert_eq!(table.coordinator(6), Some(&finished));
    assert_eq!(
        table.coordinator(9),
        None,
        "a failed description is dropped"
    );
}

#[test]
fn each_open_partition_takes_its_leaders_max_timeout() {
    let mut max_timeouts = HashMap::new();
    max_timeouts.insert(2, 60_000);

    let table = TransactionTable::assemble(
        Vec::new(),
        scanned(vec![
            open_partition("orders", 0, 7, 40),
            OpenPartition {
                leader: 2,
                ..open_partition("orders", 1, 7, 12)
            },
        ]),
        &max_timeouts,
        None,
    );

    assert_eq!(
        table.open_partitions[0].max_timeout_ms,
        DEFAULT_MAX_TIMEOUT_MS
    );
    assert_eq!(table.open_partitions[1].max_timeout_ms, 60_000);
}

#[test]
fn an_open_partition_is_seen_before_only_from_the_same_offset() {
    let previous = TransactionTable::assemble(
        Vec::new(),
        scanned(vec![
            open_partition("orders", 0, 7, 40),
            open_partition("orders", 1, 7, 12),
        ]),
        &HashMap::new(),
        None,
    );
    let scan = || {
        scanned(vec![
            unseen(open_partition("orders", 0, 7, 40)),
            unseen(open_partition("orders", 1, 7, 13)),
            unseen(open_partition("orders", 1, 8, 12)),
            unseen(open_partition("payments", 0, 7, 40)),
        ])
    };

    let first = TransactionTable::assemble(Vec::new(), scan(), &HashMap::new(), None);
    let next = TransactionTable::assemble(Vec::new(), scan(), &HashMap::new(), Some(&previous));

    assert!(first.open_partitions.iter().all(|open| !open.seen_before));
    assert_eq!(
        next.open_partitions
            .iter()
            .map(|open| open.seen_before)
            .collect::<Vec<_>>(),
        vec![true, false, false, false]
    );
}

#[test]
fn the_table_carries_what_the_scan_could_not_see() {
    let scan = LeaderScan {
        partitions: 4,
        denied: [Arc::<str>::from("payments")].into_iter().collect(),
        unchecked: 1,
        ..LeaderScan::default()
    };

    let table = TransactionTable::assemble(Vec::new(), scan, &HashMap::new(), None);

    assert_eq!(table.partition_count, 4);
    assert_eq!(table.denied_topics, vec![Arc::<str>::from("payments")]);
    assert_eq!(table.unchecked_partitions, 1);
}

#[test]
fn open_partitions_count_each_partition_once() {
    let table = TransactionTable {
        open_partitions: vec![
            open_partition("orders", 0, 7, 40),
            open_partition("orders", 0, 8, 41),
            open_partition("orders", 1, 7, 12),
        ],
        ..TransactionTable::default()
    };

    assert_eq!(table.open_partition_count(), 2);
}

#[test]
fn only_an_open_transaction_on_the_partition_can_hold_it_past_its_timeout() {
    let mut finished = transaction("payments-3", 9, 1_000, &[("orders", 3)]);
    finished.state = TransactionState::CompleteCommit;
    let table = TransactionTable {
        transactions: vec![
            transaction("payments-1", 7, 1_000, &[("orders", 0)]),
            transaction("payments-2", 8, 50_000, &[("orders", 1)]),
            finished,
        ],
        ..TransactionTable::default()
    };

    assert!(!table.past_timeout_on("orders", 0, 61_000));
    assert!(table.past_timeout_on("orders", 0, 61_001));
    assert!(!table.past_timeout_on("orders", 1, 61_001));
    assert!(!table.past_timeout_on("orders", 2, i64::MAX));
    assert!(!table.past_timeout_on("orders", 3, i64::MAX));
}
