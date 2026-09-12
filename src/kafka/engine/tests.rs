use super::*;
use std::ops::Bound;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;

use crate::config::{ClusterConfig, Config};
use crate::kafka::error::KafkaError;
use crate::kafka::group::GroupSnapshot;
use crate::kafka::model::{
    ClusterIdentity, CommittedOffset, ConfigEntry, FetchPlan, MetadataSnapshot, Record,
    RecordOrder, RecordQuery, TimestampRange, Watermarks, unix_datetime,
};
use crate::kafka::session::ClusterSession;
use crate::kafka::testing::{CountingSession, FakeCluster};

fn cluster_config(name: &str) -> ClusterConfig {
    ClusterConfig {
        name: name.to_owned(),
        bootstrap_servers: vec!["localhost:9092".to_owned()],
        security: None,
        schema_registry: None,
        properties: HashMap::new(),
    }
}

#[test]
fn from_config_keeps_cluster_order() {
    let engine = QueryEngine::from_config(&Config {
        bind: "127.0.0.1:8080".parse().unwrap(),
        log_level: "info".into(),
        clusters: vec![cluster_config("b"), cluster_config("a")],
        auth: None,
    })
    .unwrap();

    assert_eq!(engine.names(), vec!["b", "a"]);
}

#[test]
fn identities_keep_config_order() {
    let engine = QueryEngine::from_sessions(vec![
        FakeCluster::named("prod"),
        FakeCluster::named("staging"),
    ]);

    let names: Vec<_> = engine
        .identities()
        .into_iter()
        .map(|identity| identity.name)
        .collect();

    assert_eq!(names, vec!["prod", "staging"]);
}

#[tokio::test]
async fn catalog_includes_partition_watermarks() {
    let engine = QueryEngine::from_sessions(vec![FakeCluster::local()]);
    let topics = engine.catalog("local").await.unwrap().topics;
    assert_eq!(topics.len(), 1);
    assert_eq!(topics[0].name, "orders.created");
    assert_eq!(topics[0].message_count, 16);
    assert_eq!(topics[0].partitions[0].low_watermark, 0);
    assert_eq!(topics[0].partitions[0].high_watermark, 8);
}

#[tokio::test]
async fn catalog_message_counts_sum_high_watermarks() {
    let engine = QueryEngine::from_sessions(vec![FakeCluster::local()]);
    let counts = engine.catalog("local").await.unwrap().message_counts();
    assert_eq!(counts.get("orders.created"), Some(&16));
}

#[tokio::test]
async fn consumer_group_hydrates_only_the_requested_group() {
    let payments = GroupSnapshot {
        id: "payments-processor".into(),
        state: crate::kafka::group::GroupState::Stable,
        protocol: "range".into(),
        coordinator: 1,
        members: vec![crate::kafka::group::GroupMember {
            id: "m-pay".into(),
            client_id: "payments".into(),
            host: "127.0.0.1".into(),
            assignments: vec![crate::kafka::group::MemberAssignment {
                topic: "payments.captured".into(),
                partitions: vec![0],
            }],
        }],
        committed: Vec::new(),
    };
    let cluster = FakeCluster::local()
        .extra_topic("payments.captured", 1, 4)
        .extra_group(payments);
    let session = CountingSession::new(cluster);
    let engine = QueryEngine::from_sessions(vec![session.clone()]);

    let group = engine
        .consumer_group("local", "order-processor")
        .await
        .unwrap();
    assert_eq!(group.id, "order-processor");
    assert_eq!(session.calls.committed_offsets(), 1);
}

#[tokio::test(start_paused = true)]
async fn catalog_fetches_watermarks_in_parallel() {
    let cluster = FakeCluster::local()
        .extra_topic("payments.captured", 1, 4)
        .with_watermark_delay(Duration::from_secs(1));
    let engine = QueryEngine::from_sessions(vec![cluster]);

    let started = tokio::time::Instant::now();
    let mut topics = engine.catalog("local").await.unwrap().topics;
    topics.sort_by(|left, right| left.name.cmp(&right.name));

    assert_eq!(started.elapsed(), Duration::from_secs(1));
    assert_eq!(
        topics
            .iter()
            .map(|topic| (topic.name.as_str(), topic.message_count))
            .collect::<Vec<_>>(),
        vec![("orders.created", 16), ("payments.captured", 4)]
    );
}

#[tokio::test]
async fn schema_subjects_come_from_the_cluster_session() {
    let engine = QueryEngine::from_sessions(vec![FakeCluster::local()]);
    let subjects = engine.schema_subjects("local").await.unwrap();

    assert_eq!(subjects.len(), 1);
    assert_eq!(subjects[0].subject, "orders.created-value");
}

#[tokio::test]
async fn schema_subjects_default_to_empty_when_session_does_not_override() {
    let session = CountingSession::new(FakeCluster::local());
    let engine = QueryEngine::from_sessions(vec![session]);

    assert!(engine.schema_subjects("local").await.unwrap().is_empty());
}

#[tokio::test]
async fn catalog_search_includes_schema_subjects() {
    let engine = QueryEngine::from_sessions(vec![FakeCluster::local()]);
    let snapshot = engine.catalog("local").await.unwrap();
    let subjects = engine.schema_subjects("local").await.unwrap();
    let hits = snapshot.search("order", &subjects);

    assert!(
        hits.iter()
            .any(|hit| hit.kind == crate::kafka::model::SearchKind::Subject
                && hit.id == "orders.created-value")
    );
}

#[tokio::test]
async fn catalog_calls_watermarks_many_once() {
    let session = CountingSession::new(FakeCluster::local());
    let engine = QueryEngine::from_sessions(vec![session.clone()]);

    let snapshot = engine.catalog("local").await.unwrap();
    assert_eq!(session.calls.watermarks_many(), 1);
    assert_eq!(snapshot.topics[0].message_count, 16);
    assert_eq!(snapshot.message_counts().get("orders.created"), Some(&16));
}

#[tokio::test]
async fn catalog_assembles_topics_groups_brokers_and_overview() {
    let engine = QueryEngine::from_sessions(vec![FakeCluster::local()]);
    let snapshot = engine.catalog("local").await.unwrap();

    assert_eq!(snapshot.topics[0].name, "orders.created");
    assert_eq!(snapshot.topics[0].message_count, 16);
    assert_eq!(snapshot.groups[0].id, "order-processor");
    assert_eq!(snapshot.brokers[0].id, 1);
    assert_eq!(snapshot.overview.identity.name, "local");
    assert_eq!(snapshot.overview.topic_count, 1);
    assert_eq!(snapshot.overview.consumer_group_count, 1);
}

#[tokio::test]
async fn catalog_fetches_shared_inputs_once() {
    let session = CountingSession::new(FakeCluster::local());
    let engine = QueryEngine::from_sessions(vec![session.clone()]);
    let snapshot = engine.catalog("local").await.unwrap();

    assert_eq!(snapshot.topics[0].name, "orders.created");
    assert_eq!(snapshot.topics[0].message_count, 16);
    assert_eq!(snapshot.groups[0].id, "order-processor");
    assert_eq!(snapshot.groups[0].lag, 5);
    assert_eq!(session.calls.metadata(), 1);
    assert_eq!(session.calls.consumer_groups(), 1);
    assert_eq!(session.calls.watermarks_many(), 1);
    assert_eq!(session.calls.topics_configs(), 1);
    assert_eq!(session.calls.committed_offsets(), 1);
}

#[tokio::test]
async fn assemble_catalog_skips_config_fetch_when_disabled() {
    let session = CountingSession::new(FakeCluster::local());
    let engine = QueryEngine::from_sessions(vec![session.clone()]);
    let first = engine.assemble_catalog("local", None, true).await.unwrap();
    assert!(first.fetched_configs);
    assert!(!first.reused_topology);
    assert_eq!(session.calls.topics_configs(), 1);

    let reuse = CatalogReuse {
        metadata_hash: first.metadata_hash,
        configs: first.configs.clone(),
        snapshot: Some(Arc::new(first.snapshot.clone())),
    };
    let second = engine
        .assemble_catalog("local", Some(&reuse), false)
        .await
        .unwrap();
    assert!(!second.fetched_configs);
    assert!(second.reused_topology);
    assert_eq!(session.calls.topics_configs(), 1);
    assert_eq!(session.calls.watermarks_many(), 2);
    assert!(second.snapshot.body_eq(&first.snapshot));
}

#[tokio::test]
async fn assemble_catalog_keeps_reused_configs_when_fetch_fails() {
    let session = CountingSession::new(FakeCluster::local().with_configs_error("no configs"));
    let engine = QueryEngine::from_sessions(vec![session.clone()]);
    let good = QueryEngine::from_sessions(vec![FakeCluster::local()])
        .assemble_catalog("local", None, true)
        .await
        .unwrap();
    let reuse = CatalogReuse {
        metadata_hash: 0,
        configs: good.configs.clone(),
        snapshot: None,
    };
    let assembled = engine
        .assemble_catalog("local", Some(&reuse), true)
        .await
        .unwrap();
    assert!(!assembled.fetched_configs);
    assert_eq!(assembled.configs, good.configs);
    assert_eq!(session.calls.topics_configs(), 1);
}

#[tokio::test]
async fn assemble_catalog_reuses_topology_only_when_hash_matches() {
    let engine = QueryEngine::from_sessions(vec![FakeCluster::local()]);
    let first = engine.assemble_catalog("local", None, true).await.unwrap();
    let reuse = CatalogReuse {
        metadata_hash: first.metadata_hash,
        configs: first.configs.clone(),
        snapshot: Some(Arc::new(first.snapshot.clone())),
    };
    let second = engine
        .assemble_catalog("local", Some(&reuse), false)
        .await
        .unwrap();
    assert!(second.reused_topology);
    assert_eq!(second.snapshot.brokers, first.snapshot.brokers);

    let miss = CatalogReuse {
        metadata_hash: first.metadata_hash.wrapping_add(1),
        configs: first.configs.clone(),
        snapshot: Some(Arc::new(first.snapshot.clone())),
    };
    let third = engine
        .assemble_catalog("local", Some(&miss), false)
        .await
        .unwrap();
    assert!(!third.reused_topology);
}

#[tokio::test]
async fn assemble_catalog_applies_new_configs_when_topology_is_reused() {
    let first = QueryEngine::from_sessions(vec![FakeCluster::local()])
        .assemble_catalog("local", None, true)
        .await
        .unwrap();
    assert_eq!(first.snapshot.topics[0].retention_ms, 604_800_000);

    let reuse = CatalogReuse {
        metadata_hash: first.metadata_hash,
        configs: first.configs.clone(),
        snapshot: Some(Arc::new(first.snapshot.clone())),
    };
    let second = QueryEngine::from_sessions(vec![FakeCluster::local().with_topic_configs(
        "orders.created",
        vec![
            ConfigEntry {
                name: "cleanup.policy".into(),
                value: Some("compact".into()),
                source: crate::kafka::model::ConfigSource::Default,
                read_only: false,
                sensitive: false,
            },
            ConfigEntry {
                name: "retention.ms".into(),
                value: Some("1000".into()),
                source: crate::kafka::model::ConfigSource::Default,
                read_only: false,
                sensitive: false,
            },
        ],
    )])
    .assemble_catalog("local", Some(&reuse), true)
    .await
    .unwrap();

    assert!(second.reused_topology);
    assert!(second.fetched_configs);
    assert_eq!(
        second.snapshot.topics[0].cleanup_policy,
        crate::kafka::model::CleanupPolicy::Compact
    );
    assert_eq!(second.snapshot.topics[0].retention_ms, 1000);
    assert_eq!(second.snapshot.brokers, first.snapshot.brokers);
}

#[tokio::test]
async fn catalog_unknown_cluster_is_an_error() {
    let engine = QueryEngine::from_sessions(vec![FakeCluster::local()]);
    let error = engine.catalog("missing").await.unwrap_err();
    assert!(matches!(error, KafkaError::UnknownCluster(name) if name == "missing"));
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
        compression: crate::kafka::record::Compression::None,
    }
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

    let engine =
        QueryEngine::from_sessions(vec![FakeCluster::local().with_orders_records(records)]);
    let mut query = browse_query();
    query.limit = 5;
    query.order = RecordOrder::Newest;

    let mut seen = Vec::new();
    for _ in 0..8 {
        let page = engine.records("local", query.clone()).await.unwrap();
        assert!(!page.records.is_empty());
        seen.extend(
            page.records
                .iter()
                .map(|record| (record.partition, record.timestamp)),
        );
        if !page.has_more {
            break;
        }
        query.cursor =
            Some(crate::kafka::RecordCursor::parse(page.next_cursor.as_deref().unwrap()).unwrap());
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
async fn records_filter_by_timestamp_range() {
    let engine = QueryEngine::from_sessions(vec![FakeCluster::local()]);
    let mut query = browse_query();
    query.timestamps = TimestampRange::from_bounds(
        unix_datetime(1_700_000_000_000 + 3_000)..=unix_datetime(1_700_000_000_000 + 5_000),
    );

    let page = engine.records("local", query).await.unwrap();
    let keys: Vec<_> = page
        .records
        .iter()
        .map(|record| record.key.as_deref())
        .collect();

    assert_eq!(keys, vec![Some("ord_3"), Some("ord_4"), Some("ord_5")]);
    assert!(!page.has_more);
}

#[tokio::test]
async fn filtered_records_fill_the_requested_limit() {
    let mut records = Vec::new();
    for offset in 0..500 {
        let key = if offset % 40 == 0 {
            format!("hit-{offset}")
        } else {
            format!("miss-{offset}")
        };
        records.push(Record {
            topic: "orders.created".into(),
            partition: 0,
            offset,
            timestamp: offset,
            key: Some(key),
            value: None,
            schema_id: None,
            headers: Vec::new(),
            size_bytes: 0,
            compression: crate::kafka::record::Compression::None,
        });
    }

    let engine =
        QueryEngine::from_sessions(vec![FakeCluster::local().with_orders_records(records)]);
    let mut query = browse_query();
    query.limit = 10;
    query.order = RecordOrder::Newest;
    query.filter =
        crate::kafka::compile_record_filter(r#"keyText.lowerAscii().contains("hit-")"#).unwrap();

    let page = engine.records("local", query.clone()).await.unwrap();
    assert_eq!(
        page.records.len(),
        10,
        "filter must keep scanning until the page limit is filled; got {:?}",
        page.records
            .iter()
            .map(|record| record.key.as_deref())
            .collect::<Vec<_>>()
    );
    assert!(page.has_more);
    let keys: Vec<_> = page
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
        ]
    );

    query.cursor =
        Some(crate::kafka::RecordCursor::parse(page.next_cursor.as_deref().unwrap()).unwrap());
    let page_two = engine.records("local", query).await.unwrap();
    let page_two_keys: Vec<_> = page_two
        .records
        .iter()
        .map(|record| record.key.as_deref())
        .collect();
    assert_eq!(
        page_two_keys,
        vec![Some("hit-80"), Some("hit-40"), Some("hit-0")]
    );
    assert!(!page_two.has_more);
}

#[tokio::test]
async fn records_timestamp_from_after_the_log_is_empty() {
    let engine = QueryEngine::from_sessions(vec![FakeCluster::local()]);
    let mut query = browse_query();
    query.timestamps = TimestampRange::from_bounds(unix_datetime(1_800_000_000_000)..);

    let page = engine.records("local", query).await.unwrap();
    assert!(page.records.is_empty());
    assert!(!page.has_more);
}

#[tokio::test]
async fn records_reject_timestamp_from_after_to() {
    let engine = QueryEngine::from_sessions(vec![FakeCluster::local()]);
    let mut query = browse_query();
    query.timestamps = TimestampRange::from_bounds((
        Bound::Included(unix_datetime(2)),
        Bound::Included(unix_datetime(1)),
    ));

    let error = engine.records("local", query).await.unwrap_err();
    assert!(error.to_string().contains("timestampFrom"));
}

#[derive(Clone)]
struct SharedFake {
    identity: ClusterIdentity,
    inner: Arc<Mutex<FakeCluster>>,
}

impl SharedFake {
    fn new(inner: FakeCluster) -> Self {
        Self {
            identity: inner.identity().clone(),
            inner: Arc::new(Mutex::new(inner)),
        }
    }

    fn snapshot(&self) -> FakeCluster {
        self.inner.lock().expect("fake cluster").clone()
    }

    fn add_partition(&self, topic: &str, id: i32, watermarks: Watermarks) {
        self.inner
            .lock()
            .expect("fake cluster")
            .add_partition(topic, id, watermarks);
    }

    fn drop_partition(&self, topic: &str, id: i32) {
        self.inner
            .lock()
            .expect("fake cluster")
            .drop_partition(topic, id);
    }
}

#[async_trait]
impl ClusterSession for SharedFake {
    fn identity(&self) -> &ClusterIdentity {
        &self.identity
    }

    async fn metadata(&self) -> Result<MetadataSnapshot, KafkaError> {
        self.snapshot().metadata().await
    }

    async fn watermarks(&self, topic: &str) -> Result<HashMap<i32, Watermarks>, KafkaError> {
        self.snapshot().watermarks(topic).await
    }

    async fn offsets_for_times(
        &self,
        topic: &str,
        partitions: &[i32],
        timestamp: i64,
    ) -> Result<HashMap<i32, Option<i64>>, KafkaError> {
        self.snapshot()
            .offsets_for_times(topic, partitions, timestamp)
            .await
    }

    async fn topics_configs(
        &self,
        topics: &[&str],
    ) -> Result<HashMap<String, Vec<ConfigEntry>>, KafkaError> {
        self.snapshot().topics_configs(topics).await
    }

    async fn broker_configs(&self, broker_id: i32) -> Result<Vec<ConfigEntry>, KafkaError> {
        self.snapshot().broker_configs(broker_id).await
    }

    async fn consumer_groups(&self) -> Result<Vec<GroupSnapshot>, KafkaError> {
        self.snapshot().consumer_groups().await
    }

    async fn committed_offsets(
        &self,
        group_id: &str,
        partitions: &[(String, i32)],
    ) -> Result<Vec<CommittedOffset>, KafkaError> {
        self.snapshot()
            .committed_offsets(group_id, partitions)
            .await
    }

    async fn records(&self, plan: &FetchPlan) -> Result<Vec<Record>, KafkaError> {
        self.snapshot().records(plan).await
    }
}

#[tokio::test]
async fn watermarks_many_sees_topology_change_without_a_ttl() {
    let mut cluster = FakeCluster::local();
    let first_meta = cluster.metadata().await.unwrap();
    let first = cluster
        .watermarks_many(&first_meta.topic_partition_pairs(&["orders.created"]))
        .await;
    assert_eq!(
        first.get("orders.created"),
        Some(&HashMap::from([
            (0, Watermarks { low: 0, high: 8 }),
            (1, Watermarks { low: 0, high: 8 }),
        ]))
    );

    cluster.add_partition("orders.created", 2, Watermarks { low: 1, high: 4 });
    cluster.drop_partition("orders.created", 1);

    let second_meta = cluster.metadata().await.unwrap();
    let second = cluster
        .watermarks_many(&second_meta.topic_partition_pairs(&["orders.created"]))
        .await;
    assert_eq!(
        second.get("orders.created"),
        Some(&HashMap::from([
            (0, Watermarks { low: 0, high: 8 }),
            (2, Watermarks { low: 1, high: 4 }),
        ]))
    );
}

#[tokio::test]
async fn assemble_catalog_picks_up_partition_churn() {
    let cluster = SharedFake::new(FakeCluster::local());
    let engine = QueryEngine::from_sessions(vec![cluster.clone()]);

    let first = engine.catalog("local").await.unwrap();
    assert_eq!(
        first.topics[0]
            .partitions
            .iter()
            .map(|partition| (
                partition.id,
                partition.low_watermark,
                partition.high_watermark
            ))
            .collect::<Vec<_>>(),
        vec![(0, 0, 8), (1, 0, 8)]
    );

    cluster.add_partition("orders.created", 2, Watermarks { low: 1, high: 4 });
    cluster.drop_partition("orders.created", 1);

    let second = engine.catalog("local").await.unwrap();
    assert_eq!(
        second.topics[0]
            .partitions
            .iter()
            .map(|partition| (
                partition.id,
                partition.low_watermark,
                partition.high_watermark
            ))
            .collect::<Vec<_>>(),
        vec![(0, 0, 8), (2, 1, 4)]
    );
}

#[tokio::test]
async fn watermarks_many_does_not_fetch_metadata() {
    let session = CountingSession::new(FakeCluster::local());
    let meta = session.metadata().await.unwrap();
    assert_eq!(session.calls.metadata(), 1);

    let marks = session
        .watermarks_many(&meta.topic_partition_pairs(&["orders.created"]))
        .await;

    assert_eq!(session.calls.metadata(), 1);
    assert_eq!(
        marks.get("orders.created"),
        Some(&HashMap::from([
            (0, Watermarks { low: 0, high: 8 }),
            (1, Watermarks { low: 0, high: 8 }),
        ]))
    );
}
