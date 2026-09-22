use foldhash::HashMap;

use crate::kafka::error::KafkaError;
use crate::kafka::limits::RecordLimits;
use crate::kafka::metadata::PartitionMetadata;
use crate::kafka::session::ClusterSession;
use crate::kafka::store::ClusterStore;
use crate::kafka::watermarks::Watermarks;

use super::RecordPage;
use super::plan::apply_timestamp_bounds;
use super::query::RecordQuery;
use super::session::fetch_page;

pub async fn read_page<S: ClusterSession + ?Sized>(
    session: &S,
    store: &ClusterStore,
    query: RecordQuery,
    limits: RecordLimits,
) -> Result<RecordPage, KafkaError> {
    let limit = limits.clamp_limit(query.limit)?;
    query.timestamps.validate()?;

    let partitions = resolve_partitions(session, store, &query).await?;
    let watermarks = window_watermarks(session, &query, &partitions).await?;

    fetch_page(session, &query, &partitions, &watermarks, limit, limits).await
}

async fn resolve_partitions<S: ClusterSession + ?Sized>(
    session: &S,
    store: &ClusterStore,
    query: &RecordQuery,
) -> Result<Vec<i32>, KafkaError> {
    if let Some(topology) = store.topology.load()
        && let Some(topic) = topology.topics.get(query.topic.as_str())
    {
        return select_partitions(store.name(), query, &topic.partitions);
    }

    store.topology.kick();
    let topic = session.topic_metadata(&query.topic).await?;

    select_partitions(store.name(), query, &topic.partitions)
}

fn select_partitions(
    cluster: &str,
    query: &RecordQuery,
    partitions: &[PartitionMetadata],
) -> Result<Vec<i32>, KafkaError> {
    match query.partition {
        None => Ok(partitions.iter().map(|partition| partition.id).collect()),
        Some(id) if partitions.iter().any(|partition| partition.id == id) => Ok(vec![id]),
        Some(id) => Err(KafkaError::UnknownPartition {
            cluster: cluster.to_owned(),
            topic: query.topic.clone(),
            partition: id,
        }),
    }
}

async fn window_watermarks<S: ClusterSession + ?Sized>(
    session: &S,
    query: &RecordQuery,
    partitions: &[i32],
) -> Result<HashMap<i32, Watermarks>, KafkaError> {
    let wanted = HashMap::from_iter([(query.topic.clone(), partitions.to_vec())]);
    let start = query.timestamps.start_seek();
    let end = query.timestamps.end_seek();

    let seek = |timestamp: Option<i64>| async move {
        match timestamp {
            Some(timestamp) => session
                .offsets_for_times(&query.topic, partitions, timestamp)
                .await
                .map(Some),
            None => Ok(None),
        }
    };
    let (mut by_topic, from_offsets, to_offsets) =
        tokio::try_join!(session.watermarks(&wanted), seek(start), seek(end))?;
    let mut watermarks = by_topic.remove(&query.topic).unwrap_or_default();
    if start.is_none() && end.is_none() {
        return Ok(watermarks);
    }

    apply_timestamp_bounds(&mut watermarks, from_offsets.as_ref(), to_offsets.as_ref());
    Ok(watermarks)
}

#[cfg(test)]
mod tests {
    use std::ops::Bound;
    use std::sync::Arc;

    use super::*;
    use crate::kafka::RecordCursor;
    use crate::kafka::model::{Record, RecordOrder, TimestampRange};
    use crate::kafka::scan::Compression;
    use crate::kafka::store::fixtures::{identity, partition, topic, topology};
    use crate::kafka::testing::FakeCluster;
    use jiff::Timestamp;

    fn unix_datetime(ms: i64) -> Timestamp {
        Timestamp::from_millisecond(ms).unwrap_or(Timestamp::UNIX_EPOCH)
    }

    fn store() -> ClusterStore {
        ClusterStore::new(identity("local"))
    }

    fn ingested_store() -> ClusterStore {
        let store = store();
        store.topology.commit(Arc::new(topology(
            vec![topic(
                "orders.created",
                vec![
                    partition(0, vec![1], vec![1]),
                    partition(1, vec![1], vec![1]),
                ],
            )],
            Vec::new(),
        )));
        store
    }

    fn browse_query() -> RecordQuery {
        RecordQuery {
            topic: "orders.created".into(),
            partition: None,
            filter: None,
            timestamps: TimestampRange::default(),
            limit: 50,
            order: RecordOrder::Oldest,
            cursor: None,
            schema_id: None,
        }
    }

    fn browse_record(partition: i32, offset: i64, timestamp: i64) -> Record {
        Record {
            topic: "orders.created".into(),
            partition,
            offset,
            timestamp,
            key: Some(format!("p{partition}-{offset}")),
            value: None,
            schema_id: None,
            headers: Vec::new(),
            size_bytes: 0,
            compression: Compression::None,
        }
    }

    async fn page(
        session: &FakeCluster,
        store: &ClusterStore,
        query: RecordQuery,
    ) -> Result<RecordPage, KafkaError> {
        read_page(session, store, query, RecordLimits::from_env()).await
    }

    #[tokio::test]
    async fn partitions_come_from_the_topology_lane() {
        let session = FakeCluster::local();
        let store = ingested_store();

        let page = page(&session, &store, browse_query()).await.unwrap();

        assert!(!page.records.is_empty());
        assert_eq!(
            session.calls().metadata() + session.calls().topic_metadata(),
            0,
            "a topic the lane already committed must not cost a metadata call"
        );
    }

    #[tokio::test]
    async fn a_topic_the_lane_has_not_seen_costs_one_topic_scoped_metadata_call() {
        let session = FakeCluster::local();
        let store = store();

        let page = page(&session, &store, browse_query()).await.unwrap();

        assert!(!page.records.is_empty());
        assert_eq!(session.calls().topic_metadata(), 1);
        assert_eq!(
            session.calls().metadata(),
            0,
            "one topic must not cost a full cluster fetch"
        );
    }

    #[tokio::test]
    async fn unknown_topics_and_partitions_are_rejected() {
        let session = FakeCluster::local();
        let store = ingested_store();

        let mut missing = browse_query();
        missing.topic = "ghost".into();
        let error = page(&session, &store, missing).await.unwrap_err();
        assert_eq!(error.code(), "UNKNOWN_TOPIC");

        let mut partition = browse_query();
        partition.partition = Some(7);
        let error = page(&session, &store, partition).await.unwrap_err();
        assert_eq!(error.code(), "UNKNOWN_PARTITION");
    }

    #[tokio::test]
    async fn invalid_queries_are_rejected_before_any_broker_call() {
        let session = FakeCluster::local();
        let store = store();

        for limit in [0, -1] {
            let mut query = browse_query();
            query.limit = limit;

            let error = page(&session, &store, query).await.unwrap_err();

            assert_eq!(error.code(), "LIMIT_TOO_SMALL");
            assert_eq!(session.calls().metadata(), 0);
            assert_eq!(session.calls().watermarks(), 0);
        }

        let mut query = browse_query();
        query.timestamps = TimestampRange::from_bounds((
            Bound::Included(unix_datetime(2)),
            Bound::Included(unix_datetime(1)),
        ));

        let error = page(&session, &store, query).await.unwrap_err();

        assert_eq!(error.code(), "INVERTED_TIMESTAMP_RANGE");
        assert_eq!(session.calls().metadata(), 0);
        assert_eq!(session.calls().watermarks(), 0);
    }

    #[tokio::test]
    async fn records_filter_by_timestamp_range() {
        let session = FakeCluster::local();
        let mut query = browse_query();
        query.timestamps = TimestampRange::from_bounds(
            unix_datetime(1_700_000_000_000 + 3_000)..=unix_datetime(1_700_000_000_000 + 5_000),
        );

        let page = page(&session, &ingested_store(), query).await.unwrap();
        let keys: Vec<_> = page
            .records
            .iter()
            .map(|record| record.key.as_deref())
            .collect();

        assert_eq!(keys, vec![Some("ord_3"), Some("ord_4"), Some("ord_5")]);
        assert!(!page.has_more());
    }

    #[tokio::test]
    async fn a_timestamp_window_after_the_log_is_empty() {
        let session = FakeCluster::local();
        let mut query = browse_query();
        query.timestamps = TimestampRange::from_bounds(unix_datetime(1_800_000_000_000)..);

        let page = page(&session, &ingested_store(), query).await.unwrap();

        assert!(page.records.is_empty());
        assert!(!page.has_more());
    }

    #[tokio::test]
    async fn all_partitions_newest_stays_timestamp_ordered_across_cursors() {
        let evening = 1_700_080_000_000;
        let morning = 1_700_037_000_000;
        let mut records = Vec::new();
        for offset in 0..10 {
            records.push(browse_record(0, offset, evening + offset * 1_000));
            records.push(browse_record(1, offset, morning + offset * 1_000));
        }

        let session = FakeCluster::local().with_orders_records(records);
        let store = ingested_store();
        let mut query = browse_query();
        query.limit = 5;
        query.order = RecordOrder::Newest;

        let mut seen = Vec::new();
        for _ in 0..8 {
            let page = page(&session, &store, query.clone()).await.unwrap();
            assert!(!page.records.is_empty());
            seen.extend(
                page.records
                    .iter()
                    .map(|record| (record.partition, record.timestamp)),
            );
            if !page.has_more() {
                break;
            }
            query.cursor = Some(RecordCursor::parse(page.next_cursor.as_deref().unwrap()).unwrap());
        }

        let timestamps: Vec<_> = seen.iter().map(|(_, timestamp)| *timestamp).collect();
        let mut newest_first = timestamps.clone();
        newest_first.sort_by(|left, right| right.cmp(left));
        assert_eq!(timestamps, newest_first);

        let first_morning = seen.iter().position(|(partition, _)| *partition == 1);
        let last_evening = seen.iter().rposition(|(partition, _)| *partition == 0);
        assert!(first_morning.is_some() && last_evening.is_some());
        assert!(
            first_morning.unwrap() > last_evening.unwrap(),
            "partition 1 morning records must not appear before remaining partition 0 evening records"
        );
    }

    #[tokio::test]
    async fn filtered_records_fill_the_requested_limit() {
        let records = (0..500)
            .map(|offset| Record {
                topic: "orders.created".into(),
                partition: 0,
                offset,
                timestamp: offset,
                key: Some(if offset % 40 == 0 {
                    format!("hit-{offset}")
                } else {
                    format!("miss-{offset}")
                }),
                value: None,
                schema_id: None,
                headers: Vec::new(),
                size_bytes: 0,
                compression: Compression::None,
            })
            .collect();

        let session = FakeCluster::local().with_orders_records(records);
        let store = ingested_store();
        let mut query = browse_query();
        query.limit = 10;
        query.order = RecordOrder::Newest;
        query.filter = crate::kafka::compile_contains_filter("hit-");

        let first = page(&session, &store, query.clone()).await.unwrap();
        let keys: Vec<_> = first
            .records
            .iter()
            .map(|record| record.key.as_deref())
            .collect();
        assert_eq!(
            keys,
            vec![
                Some("hit-480"),
                Some("hit-440"),
                Some("hit-400"),
                Some("hit-360"),
                Some("hit-320"),
                Some("hit-280"),
                Some("hit-240"),
                Some("hit-200"),
                Some("hit-160"),
                Some("hit-120"),
            ],
            "a filter must keep scanning until the page limit is filled"
        );
        assert!(first.has_more());

        query.cursor = Some(RecordCursor::parse(first.next_cursor.as_deref().unwrap()).unwrap());
        let second = page(&session, &store, query).await.unwrap();

        assert_eq!(
            second
                .records
                .iter()
                .map(|record| record.key.as_deref())
                .collect::<Vec<_>>(),
            vec![Some("hit-80"), Some("hit-40"), Some("hit-0")]
        );
        assert!(!second.has_more());
    }
}
