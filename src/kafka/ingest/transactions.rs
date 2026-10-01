use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use foldhash::{HashMap, HashSet};
use futures::future::join_all;

use crate::kafka::error::KafkaError;
use crate::kafka::session::ClusterSession;
use crate::kafka::store::{
    Change, ClusterStore, DEFAULT_MAX_TIMEOUT_MS, Lane, LeaderScan, Topology, TransactionTable,
};
use crate::kafka::topic_config::ConfigEntry;
use crate::kafka::transaction::{TransactionDescription, TransactionState};

use super::runner::LaneSource;

const MAX_TIMEOUT_CONFIG: &str = "transaction.max.timeout.ms";

pub struct TransactionLane {
    session: Arc<dyn ClusterSession>,
    interval: Duration,
}

impl TransactionLane {
    pub fn with_interval(session: Arc<dyn ClusterSession>, interval: Duration) -> Self {
        Self { session, interval }
    }

    /// Every open transaction, and the coordinator's view of each producer
    /// the leaders hold one open for. Listing by producer id finds a
    /// transactional id whatever state it is in.
    async fn coordinators(
        &self,
        scan: &LeaderScan,
    ) -> Result<Vec<TransactionDescription>, KafkaError> {
        let mut listed = self
            .session
            .list_transactions(&TransactionState::OPEN, &[])
            .await?;
        let known: HashSet<i64> = listed
            .iter()
            .map(|transaction| transaction.producer_id)
            .collect();
        let unknown: Vec<i64> = scan
            .producer_ids()
            .into_iter()
            .filter(|id| !known.contains(id))
            .collect();
        if !unknown.is_empty() {
            listed.extend(self.session.list_transactions(&[], &unknown).await?);
        }
        let ids: BTreeSet<&str> = listed
            .iter()
            .map(|transaction| transaction.transactional_id.as_str())
            .collect();
        self.session
            .describe_transactions(&ids.into_iter().collect::<Vec<_>>())
            .await
    }

    /// Each leader's `transaction.max.timeout.ms`. A broker whose configs
    /// klens cannot read keeps Kafka's default.
    async fn max_timeouts(&self, scan: &LeaderScan) -> HashMap<i32, i64> {
        let leaders = scan.leaders();
        let configs = join_all(leaders.iter().map(|id| self.session.broker_configs(*id))).await;
        leaders
            .into_iter()
            .zip(configs)
            .map(|(id, configs)| (id, max_timeout_ms(configs.ok().as_deref())))
            .collect()
    }
}

fn max_timeout_ms(configs: Option<&[ConfigEntry]>) -> i64 {
    configs
        .and_then(|configs| ConfigEntry::lookup(configs, MAX_TIMEOUT_CONFIG))
        .and_then(|value| value.parse().ok())
        .unwrap_or(DEFAULT_MAX_TIMEOUT_MS)
}

#[async_trait]
impl LaneSource for TransactionLane {
    // Listing asks every broker, and the admin client learns them from the
    // metadata the topology lane refreshes.
    type Upstream = Topology;
    type Table = TransactionTable;
    type Delta = ();

    fn name(&self) -> &'static str {
        "transactions"
    }

    fn interval(&self) -> Duration {
        self.interval
    }

    fn lane<'a>(&self, store: &'a ClusterStore) -> &'a Lane<TransactionTable> {
        &store.transactions
    }

    // The leaders answer before the coordinators, so a transaction that
    // starts in between is missed rather than read as unknown to its
    // coordinator.
    async fn fetch(
        &self,
        _store: &ClusterStore,
        topology: &Topology,
        previous: Option<&Arc<TransactionTable>>,
    ) -> Result<TransactionTable, KafkaError> {
        let every_partition: HashMap<String, Vec<i32>> = topology
            .topics
            .iter()
            .map(|(name, topic)| (name.to_string(), topic.partition_ids()))
            .collect();
        let scan = LeaderScan::of(
            topology,
            &self.session.describe_producers(&every_partition).await?,
        );
        let described = self.coordinators(&scan).await?;
        let max_timeouts = self.max_timeouts(&scan).await;
        Ok(TransactionTable::assemble(
            described,
            scan,
            &max_timeouts,
            previous.map(Arc::as_ref),
        ))
    }

    fn diff(&self, previous: Option<&TransactionTable>, next: &TransactionTable) -> Option<()> {
        (previous != Some(next)).then_some(())
    }

    fn publish(
        &self,
        store: &ClusterStore,
        _previous: Option<&Arc<TransactionTable>>,
        _next: &Arc<TransactionTable>,
        (): (),
    ) {
        store.bus.publish(Change::Transactions);
    }
}

#[cfg(test)]
mod tests {
    use crate::kafka::store::OpenPartition;
    use crate::kafka::store::fixtures::{
        config, open_partition, open_producer, partition, producers, topic, topology, transaction,
    };
    use crate::kafka::testing::FakeCluster;

    use super::super::runner::run;
    use super::*;

    const IDLE: Duration = Duration::from_secs(600);

    fn lane(session: &FakeCluster) -> TransactionLane {
        TransactionLane::with_interval(Arc::new(session.clone()), IDLE)
    }

    fn store(session: &FakeCluster) -> Arc<ClusterStore> {
        Arc::new(ClusterStore::new(session.identity().clone(), IDLE))
    }

    fn orders() -> Topology {
        topology(
            vec![topic(
                "orders",
                vec![
                    partition(0, vec![1], vec![1]),
                    partition(1, vec![2], vec![2]),
                ],
            )],
            Vec::new(),
        )
    }

    async fn look(
        session: &FakeCluster,
        previous: Option<&Arc<TransactionTable>>,
    ) -> TransactionTable {
        lane(session)
            .fetch(&store(session), &orders(), previous)
            .await
            .expect("a table")
    }

    fn ids(table: &TransactionTable) -> Vec<&str> {
        table
            .transactions
            .iter()
            .map(|transaction| transaction.transactional_id.as_str())
            .collect()
    }

    #[tokio::test(start_paused = true)]
    async fn the_lane_commits_the_transactions_once_topology_commits_and_announces_them() {
        let session = FakeCluster::local();
        let mut preparing = transaction("payments-1", 8, 1_000, &[("orders", 1)]);
        preparing.state = TransactionState::PrepareCommit;
        let mut finished = transaction("payments-3", 9, 1_000, &[("orders", 0)]);
        finished.state = TransactionState::CompleteCommit;
        session.set_transactions(Ok(vec![
            transaction("payments-2", 7, 1_000, &[("orders", 0)]),
            preparing,
            finished,
        ]));
        let store = store(&session);
        let mut events = store.bus.subscribe();
        let task = tokio::spawn(run(Arc::clone(&store), lane(&session)));
        tokio::task::yield_now().await;
        assert_eq!(
            session.calls().describe_producers(),
            0,
            "the lane waits for topology"
        );

        store.topology.commit(Arc::new(orders()));
        let change = tokio::time::timeout(IDLE, events.recv())
            .await
            .expect("the first table must be announced")
            .expect("a change");
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }

        assert!(matches!(change, Change::Transactions));
        let table = store.transactions.load().expect("a committed table");
        assert_eq!(ids(&table), vec!["payments-1", "payments-2"]);
        assert_eq!(table.partition_count, 2);
        assert_eq!(
            session.calls().describe_producers(),
            1,
            "the next look waits for the interval"
        );
        task.abort();
    }

    #[tokio::test]
    async fn a_producer_the_open_listing_misses_is_looked_up_by_its_id() {
        let session = FakeCluster::local();
        let mut aborted = transaction("stale", 9, 1_000, &[("orders", 1)]);
        aborted.state = TransactionState::CompleteAbort;
        session.set_transactions(Ok(vec![
            transaction("payments-1", 7, 1_000, &[("orders", 0)]),
            aborted.clone(),
        ]));
        session.set_producers(vec![vec![
            producers("orders", 0, vec![open_producer(7, 1_000, 40)]),
            producers("orders", 1, vec![open_producer(9, 1_000, 12)]),
        ]]);

        let table = look(&session, None).await;

        assert_eq!(ids(&table), vec!["payments-1", "stale"]);
        assert_eq!(table.coordinator(9), Some(&aborted));
        assert_eq!(session.calls().list_transactions(), 2);
    }

    #[tokio::test]
    async fn producers_the_open_listing_names_need_no_lookup() {
        let session = FakeCluster::local();
        session.set_transactions(Ok(vec![transaction(
            "payments-1",
            7,
            1_000,
            &[("orders", 0)],
        )]));
        session.set_producers(vec![vec![producers(
            "orders",
            0,
            vec![open_producer(7, 1_000, 40)],
        )]]);

        look(&session, None).await;

        assert_eq!(session.calls().list_transactions(), 1);
    }

    #[tokio::test]
    async fn each_open_partition_takes_its_leaders_max_timeout() {
        let session = FakeCluster::local();
        session.set_broker_configs(2, vec![config("transaction.max.timeout.ms", "60000")]);
        session.set_producers(vec![vec![
            producers("orders", 0, vec![open_producer(7, 1_000, 40)]),
            producers("orders", 1, vec![open_producer(8, 1_000, 12)]),
        ]]);

        let table = look(&session, None).await;

        assert_eq!(
            table
                .open_partitions
                .iter()
                .map(|open| (open.leader, open.max_timeout_ms))
                .collect::<Vec<_>>(),
            vec![(1, DEFAULT_MAX_TIMEOUT_MS), (2, 60_000)]
        );
    }

    #[test]
    fn the_max_timeout_falls_back_to_the_kafka_default() {
        assert_eq!(max_timeout_ms(None), 900_000);
        assert_eq!(max_timeout_ms(Some(&[])), 900_000);
        assert_eq!(
            max_timeout_ms(Some(&[config("transaction.max.timeout.ms", "soon")])),
            900_000
        );
        assert_eq!(
            max_timeout_ms(Some(&[config("transaction.max.timeout.ms", "60000")])),
            60_000
        );
    }

    #[tokio::test]
    async fn a_partition_still_open_from_the_same_offset_was_seen_before() {
        let session = FakeCluster::local();
        session.set_producers(vec![
            vec![producers("orders", 0, vec![open_producer(7, 1_000, 40)])],
            vec![producers("orders", 0, vec![open_producer(7, 1_000, 40)])],
        ]);

        let first = Arc::new(look(&session, None).await);
        let next = look(&session, Some(&first)).await;

        assert_eq!(
            first.open_partitions,
            vec![OpenPartition {
                last_timestamp_ms: Some(1_000),
                seen_before: false,
                ..open_partition("orders", 0, 7, 40)
            }]
        );
        assert!(next.open_partitions[0].seen_before);
    }

    #[tokio::test]
    async fn a_coordinator_that_cannot_list_fails_the_poll() {
        let session = FakeCluster::local();
        session.set_transactions(Err("UnsupportedVersion"));

        let fetched = lane(&session)
            .fetch(&store(&session), &orders(), None)
            .await;

        assert!(
            matches!(fetched, Err(KafkaError::Admin(message)) if message == "UnsupportedVersion")
        );
    }

    #[test]
    fn only_a_different_table_is_a_change() {
        let lane = lane(&FakeCluster::local());
        assert_eq!(lane.name(), "transactions");
        let open = TransactionTable {
            transactions: vec![transaction("payments-1", 7, 1_000, &[("orders", 0)])],
            ..TransactionTable::default()
        };

        assert_eq!(lane.diff(None, &open), Some(()));
        assert_eq!(lane.diff(Some(&open), &open), None);
        assert_eq!(
            lane.diff(Some(&TransactionTable::default()), &open),
            Some(())
        );
    }
}
