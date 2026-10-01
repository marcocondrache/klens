use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
use std::time::Duration;

use foldhash::{HashMap, HashMapExt, HashSet, HashSetExt};

use async_trait::async_trait;
use bytes::Bytes;
use krafka::testing::FakeBroker;
use tokio::sync::OnceCell;

use crate::config::{KafkaTuning, ScanTuning};
use crate::kafka::acl::{
    Acl, AclListing, AclOperation, AclPatternType, AclPermission, AclResourceType,
};
use crate::kafka::cluster::ClusterIdentity;
use crate::kafka::error::KafkaError;
use crate::kafka::group::{
    CommittedOffset, GroupMember, GroupSnapshot, GroupState, MemberAssignment,
};
use crate::kafka::metadata::{
    BrokerMetadata, MetadataSnapshot, PartitionMetadata, TopicMetadata, Watermarks,
};
use crate::kafka::model::{PartitionWindow, RawRecord, ScanConsumer, TailConsumer, TailPosition};
use crate::kafka::quota::{ClientQuota, QuotaEntity, QuotaEntityType, QuotaListing, QuotaValues};
use crate::kafka::registry::{RegisteredSchema, SchemaCompatibility, SchemaSubject, SchemaType};
use crate::kafka::scan::RecordHeader;
use crate::kafka::scan::obfuscate::ObfuscationPolicy;
use crate::kafka::scan::payload::{DecodedPayload, PayloadCodec, PayloadSlot};
use crate::kafka::session::ClusterSession;
use crate::kafka::storage::{LogDir, ReplicaLog};
use crate::kafka::topic_config::{ConfigEntry, ConfigSource};

const SUBJECT_SCHEMA: &str =
    r#"{"type":"record","name":"Order","fields":[{"name":"orderId","type":"string"}]}"#;

#[derive(Debug, Clone)]
pub struct FixtureRecord {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub timestamp: i64,
    pub key: Option<Bytes>,
    pub value: Option<Bytes>,
    pub headers: Vec<RecordHeader>,
    pub size_bytes: u64,
}

#[derive(Clone)]
pub struct FakeCluster {
    identity: ClusterIdentity,
    inner: Arc<Inner>,
}

struct Inner {
    broker: OnceCell<FakeBroker>,
    metadata: Mutex<MetadataSnapshot>,
    watermarks: Mutex<HashMap<String, HashMap<i32, Watermarks>>>,
    topic_configs: Mutex<HashMap<String, Vec<ConfigEntry>>>,
    broker_configs: Mutex<HashMap<i32, Vec<ConfigEntry>>>,
    log_dirs: Mutex<Result<Vec<LogDir>, String>>,
    groups: Mutex<Vec<GroupSnapshot>>,
    records: Mutex<Vec<FixtureRecord>>,
    subjects: Mutex<Vec<SchemaSubject>>,
    acls: Mutex<Result<AclListing, String>>,
    quotas: Mutex<Result<QuotaListing, String>>,
    subjects_error: Mutex<Option<String>>,
    offsets_error: Mutex<Option<String>>,
    records_delay: Mutex<Duration>,
    offsets_delay: Mutex<Duration>,
    consume_timeout: Mutex<Option<Duration>>,
    watermark_growth: Mutex<Option<Arc<WatermarkGrowth>>>,
    assignments: Mutex<Vec<Vec<(i32, i64, i64)>>>,
    consumers: AtomicUsize,
    tail_seeks: Mutex<Vec<Vec<(i32, i64)>>>,
    tail_polls: AtomicUsize,
    tail_lag_reads: AtomicUsize,
    codec: Arc<CountingCodec>,
    obfuscation: Mutex<Option<Arc<ObfuscationPolicy>>>,
    calls: SessionCalls,
}

#[derive(Debug)]
struct WatermarkGrowth {
    step: i64,
    grown: AtomicI64,
}

impl FakeCluster {
    pub fn local() -> Self {
        let identity = ClusterIdentity::new("local");

        let metadata = MetadataSnapshot {
            cluster_id: Some("test-cluster".into()),
            brokers: vec![BrokerMetadata {
                id: 1,
                host: "localhost".into(),
                port: 9092,
            }],
            topics: vec![TopicMetadata {
                name: "orders.created".into(),
                internal: false,
                partitions: vec![
                    PartitionMetadata {
                        id: 0,
                        leader: 1,
                        replicas: vec![1],
                        isr: vec![1],
                    },
                    PartitionMetadata {
                        id: 1,
                        leader: 1,
                        replicas: vec![1],
                        isr: vec![1],
                    },
                ],
            }],
        };

        let watermarks = HashMap::from_iter([(
            "orders.created".into(),
            HashMap::from_iter([
                (0, Watermarks { low: 0, high: 8 }),
                (1, Watermarks { low: 0, high: 8 }),
            ]),
        )]);

        let topic_configs = HashMap::from_iter([(
            "orders.created".into(),
            vec![
                ConfigEntry {
                    name: "cleanup.policy".into(),
                    value: Some("delete".into()),
                    source: ConfigSource::Default,
                    read_only: false,
                    sensitive: false,
                },
                ConfigEntry {
                    name: "retention.ms".into(),
                    value: Some("604800000".into()),
                    source: ConfigSource::Default,
                    read_only: false,
                    sensitive: false,
                },
            ],
        )]);

        let broker_configs = HashMap::from_iter([(
            1,
            vec![ConfigEntry {
                name: "log.retention.hours".into(),
                value: Some("168".into()),
                source: ConfigSource::Default,
                read_only: false,
                sensitive: false,
            }],
        )]);

        let log_dirs = vec![LogDir {
            broker: 1,
            path: "/var/lib/kafka/data".into(),
            error: None,
            total_bytes: Some(1_000_000),
            usable_bytes: Some(750_000),
            cordoned: false,
            replicas: vec![
                ReplicaLog {
                    topic: "orders.created".into(),
                    partition: 0,
                    size_bytes: 4_096,
                    future: false,
                },
                ReplicaLog {
                    topic: "orders.created".into(),
                    partition: 1,
                    size_bytes: 2_048,
                    future: false,
                },
            ],
        }];

        let groups = vec![GroupSnapshot {
            id: "order-processor".into(),
            state: GroupState::Stable,
            protocol: "range".into(),
            members: vec![GroupMember {
                id: "member-1".into(),
                client_id: "orders".into(),
                host: "127.0.0.1".into(),
                assignments: vec![MemberAssignment {
                    topic: "orders.created".into(),
                    partitions: vec![0, 1],
                }],
            }],
            committed: vec![
                CommittedOffset {
                    topic: "orders.created".into(),
                    partition: 0,
                    offset: 6,
                },
                CommittedOffset {
                    topic: "orders.created".into(),
                    partition: 1,
                    offset: 5,
                },
            ],
        }];

        let records = (0..8)
            .map(|offset| FixtureRecord {
                topic: "orders.created".into(),
                partition: i32::from(offset % 2 == 0),
                offset: i64::from(offset),
                timestamp: 1_700_000_000_000 + i64::from(offset) * 1_000,
                key: Some(format!("ord_{offset}").into()),
                value: Some(format!(r#"{{"orderId":"ord_{offset}"}}"#).into()),
                headers: vec![RecordHeader {
                    key: "source".into(),
                    value: "checkout".into(),
                }],
                size_bytes: 24,
            })
            .collect();

        let subjects = vec![SchemaSubject {
            subject: "orders.created-value".into(),
            id: 1,
            schema_type: SchemaType::Avro,
            latest_version: 2,
            versions: vec![1, 2],
            compatibility: SchemaCompatibility::Backward,
        }];

        Self {
            identity,
            inner: Arc::new(Inner {
                broker: OnceCell::new(),
                metadata: Mutex::new(metadata),
                watermarks: Mutex::new(watermarks),
                topic_configs: Mutex::new(topic_configs),
                broker_configs: Mutex::new(broker_configs),
                log_dirs: Mutex::new(Ok(log_dirs)),
                groups: Mutex::new(groups),
                records: Mutex::new(records),
                subjects: Mutex::new(subjects),
                acls: Mutex::new(Ok(AclListing::Enabled(local_acls()))),
                quotas: Mutex::new(Ok(QuotaListing::Described(local_quotas()))),
                subjects_error: Mutex::new(None),
                offsets_error: Mutex::new(None),
                records_delay: Mutex::new(Duration::ZERO),
                offsets_delay: Mutex::new(Duration::ZERO),
                consume_timeout: Mutex::new(None),
                watermark_growth: Mutex::new(None),
                assignments: Mutex::new(Vec::new()),
                consumers: AtomicUsize::new(0),
                tail_seeks: Mutex::new(Vec::new()),
                tail_polls: AtomicUsize::new(0),
                tail_lag_reads: AtomicUsize::new(0),
                codec: Arc::new(CountingCodec::default()),
                obfuscation: Mutex::new(None),
                calls: SessionCalls::default(),
            }),
        }
    }

    pub fn named(name: &str) -> Self {
        let mut cluster = Self::local();
        cluster.identity.name = name.to_owned();
        cluster
    }

    pub fn with_records_delay(self, delay: Duration) -> Self {
        *self.inner.records_delay.lock().expect("records delay") = delay;
        self
    }

    pub fn with_offsets_delay(self, delay: Duration) -> Self {
        *self.inner.offsets_delay.lock().expect("offsets delay") = delay;
        self
    }

    pub fn with_obfuscation(self, yaml: &str) -> Self {
        let config = crate::config::parse(yaml).expect("obfuscation config parses");

        *self.inner.obfuscation.lock().expect("obfuscation") =
            Some(Arc::new(ObfuscationPolicy::compile(&config)));
        self
    }

    pub fn with_consume_timeout(self, timeout: Duration) -> Self {
        *self.inner.consume_timeout.lock().expect("consume timeout") = Some(timeout);
        self
    }

    pub fn with_growing_watermarks(self, step: i64) -> Self {
        *self
            .inner
            .watermark_growth
            .lock()
            .expect("watermark growth") = Some(Arc::new(WatermarkGrowth {
            step,
            grown: AtomicI64::new(0),
        }));
        self
    }

    pub fn with_subjects_error(self, message: impl Into<String>) -> Self {
        *self.inner.subjects_error.lock().expect("subjects error") = Some(message.into());
        self
    }

    pub fn extra_topic(self, name: &str, partitions: i32, high: i64) -> Self {
        {
            let mut metadata = self.inner.metadata.lock().expect("metadata");
            metadata.topics.push(TopicMetadata {
                name: name.to_owned(),
                internal: false,
                partitions: (0..partitions)
                    .map(|id| PartitionMetadata {
                        id,
                        leader: 1,
                        replicas: vec![1],
                        isr: vec![1],
                    })
                    .collect(),
            });
        }
        let marks: HashMap<i32, Watermarks> = (0..partitions)
            .map(|id| (id, Watermarks { low: 0, high }))
            .collect();
        self.inner
            .watermarks
            .lock()
            .expect("watermarks")
            .insert(name.to_owned(), marks.clone());
        if let Some(broker) = self.inner.broker.get() {
            seed_topic(broker, name, &marks);
        }
        self
    }

    pub fn extra_group(self, group: GroupSnapshot) -> Self {
        self.inner.groups.lock().expect("groups").push(group);
        self
    }

    pub fn put_group(&self, group: GroupSnapshot) {
        let mut groups = self.inner.groups.lock().expect("groups");
        match groups.iter_mut().find(|existing| existing.id == group.id) {
            Some(existing) => *existing = group,
            None => groups.push(group),
        }
    }

    pub fn remove_group(&self, id: &str) {
        self.inner
            .groups
            .lock()
            .expect("groups")
            .retain(|group| group.id != id);
    }

    pub fn commit_offsets(&self, id: &str, committed: Vec<CommittedOffset>) {
        if let Some(group) = self
            .inner
            .groups
            .lock()
            .expect("groups")
            .iter_mut()
            .find(|group| group.id == id)
        {
            group.committed = committed;
        }
    }

    pub fn set_topic_configs(&self, topic: &str, configs: Vec<ConfigEntry>) {
        self.inner
            .topic_configs
            .lock()
            .expect("topic configs")
            .insert(topic.to_owned(), configs);
    }

    pub fn set_log_dirs(&self, log_dirs: Result<Vec<LogDir>, &str>) {
        *self.inner.log_dirs.lock().expect("log dirs") = log_dirs.map_err(str::to_owned);
    }

    pub fn set_acls(&self, acls: Result<AclListing, &str>) {
        *self.inner.acls.lock().expect("acls") = acls.map_err(str::to_owned);
    }

    pub fn set_subjects(&self, subjects: Vec<SchemaSubject>) {
        *self.inner.subjects.lock().expect("subjects") = subjects;
    }

    pub fn set_quotas(&self, quotas: Result<QuotaListing, &str>) {
        *self.inner.quotas.lock().expect("quotas") = quotas.map_err(str::to_owned);
    }

    pub fn set_offsets_error(&self, error: Option<&str>) {
        *self.inner.offsets_error.lock().expect("offsets error") = error.map(str::to_owned);
    }

    pub fn with_orders_records(self, records: Vec<FixtureRecord>) -> Self {
        let mut highs = HashMap::<i32, i64>::new();
        for record in &records {
            let high = highs.entry(record.partition).or_insert(0);
            *high = (*high).max(record.offset + 1);
        }

        let mut ids: Vec<i32> = highs.keys().copied().collect();
        ids.sort_unstable();

        {
            let mut metadata = self.inner.metadata.lock().expect("metadata");
            if let Some(topic) = metadata
                .topics
                .iter_mut()
                .find(|topic| topic.name == "orders.created")
            {
                topic.partitions = ids
                    .iter()
                    .map(|id| PartitionMetadata {
                        id: *id,
                        leader: 1,
                        replicas: vec![1],
                        isr: vec![1],
                    })
                    .collect();
            }
        }

        let marks: HashMap<i32, Watermarks> = ids
            .into_iter()
            .map(|id| {
                (
                    id,
                    Watermarks {
                        low: 0,
                        high: highs[&id],
                    },
                )
            })
            .collect();
        self.inner
            .watermarks
            .lock()
            .expect("watermarks")
            .insert("orders.created".into(), marks.clone());
        *self.inner.records.lock().expect("records") = records;
        if let Some(broker) = self.inner.broker.get() {
            seed_topic(broker, "orders.created", &marks);
        }
        self
    }

    pub fn produce(&self, record: FixtureRecord) {
        let marks = {
            let mut watermarks = self.inner.watermarks.lock().expect("watermarks");
            let marks = watermarks
                .entry(record.topic.clone())
                .or_default()
                .entry(record.partition)
                .or_insert(Watermarks { low: 0, high: 0 });
            marks.high = marks.high.max(record.offset + 1);
            *marks
        };
        if let Some(broker) = self.inner.broker.get() {
            apply_watermark(broker, &record.topic, record.partition, marks);
        }
        self.inner.records.lock().expect("records").push(record);
    }

    pub fn tail_seeks(&self) -> Vec<Vec<(i32, i64)>> {
        self.inner.tail_seeks.lock().expect("tail seeks").clone()
    }

    pub fn tail_polls(&self) -> usize {
        self.inner.tail_polls.load(Ordering::SeqCst)
    }

    pub fn tail_lag_reads(&self) -> usize {
        self.inner.tail_lag_reads.load(Ordering::SeqCst)
    }

    pub fn add_partition(&self, topic: &str, id: i32, watermarks: Watermarks) {
        {
            let mut metadata = self.inner.metadata.lock().expect("metadata");
            if let Some(meta) = metadata
                .topics
                .iter_mut()
                .find(|topic_meta| topic_meta.name == topic)
                && !meta.partitions.iter().any(|partition| partition.id == id)
            {
                meta.partitions.push(PartitionMetadata {
                    id,
                    leader: 1,
                    replicas: vec![1],
                    isr: vec![1],
                });
            }
        }
        self.inner
            .watermarks
            .lock()
            .expect("watermarks")
            .entry(topic.to_owned())
            .or_default()
            .insert(id, watermarks);
        if let Some(broker) = self.inner.broker.get() {
            ensure_partition(broker, topic, id);
            apply_watermark(broker, topic, id, watermarks);
        }
    }

    pub fn add_broker(&self, id: i32) {
        self.inner
            .metadata
            .lock()
            .expect("metadata")
            .brokers
            .push(BrokerMetadata {
                id,
                host: "localhost".into(),
                port: 9092 + id,
            });
    }

    pub fn remove_topic(&self, name: &str) {
        self.inner
            .metadata
            .lock()
            .expect("metadata")
            .topics
            .retain(|topic| topic.name != name);
        self.inner
            .watermarks
            .lock()
            .expect("watermarks")
            .remove(name);
    }

    pub fn calls(&self) -> &SessionCalls {
        &self.inner.calls
    }

    pub fn assigned_windows(&self) -> Vec<Vec<(i32, i64, i64)>> {
        self.inner.assignments.lock().expect("assignments").clone()
    }

    pub fn consumers_opened(&self) -> usize {
        self.inner.consumers.load(Ordering::SeqCst)
    }

    pub fn decoded_payloads(&self) -> usize {
        self.inner.codec.decoded.load(Ordering::SeqCst)
    }

    async fn current_ends(
        &self,
        topics: &HashMap<String, Vec<i32>>,
        end: fn(Watermarks) -> i64,
    ) -> HashMap<String, HashMap<i32, i64>> {
        let broker = self.broker().await;
        let stored = self.inner.watermarks.lock().expect("watermarks").clone();

        topics
            .iter()
            .map(|(name, partitions)| {
                let wanted: HashMap<i32, i64> = partitions
                    .iter()
                    .filter_map(|partition| {
                        broker_watermarks(broker, name, *partition)
                            .or_else(|| {
                                stored
                                    .get(name)
                                    .and_then(|marks| marks.get(partition).copied())
                            })
                            .map(|marks| (*partition, end(marks)))
                    })
                    .collect();
                (name.to_owned(), wanted)
            })
            .collect()
    }

    async fn broker(&self) -> &FakeBroker {
        self.inner
            .broker
            .get_or_init(|| async {
                let broker = FakeBroker::start().await.expect("fake broker");
                let watermarks = self.inner.watermarks.lock().expect("watermarks").clone();
                for (topic, marks) in watermarks {
                    seed_topic(&broker, &topic, &marks);
                }
                broker
            })
            .await
    }
}

fn seed_topic(broker: &FakeBroker, topic: &str, marks: &HashMap<i32, Watermarks>) {
    let Some(max_id) = marks.keys().copied().max() else {
        return;
    };
    if !broker.create_topic(topic, max_id + 1) {
        broker.add_partitions(topic, max_id + 1);
    }
    for (&id, &marks) in marks {
        apply_watermark(broker, topic, id, marks);
    }
}

fn ensure_partition(broker: &FakeBroker, topic: &str, partition: i32) {
    if broker.with_state(|state| state.partition(topic, partition).is_some()) {
        return;
    }
    if broker.with_state(|state| state.topics.contains_key(topic)) {
        broker.add_partitions(topic, partition + 1);
    } else {
        broker.create_topic(topic, partition + 1);
    }
}

fn apply_watermark(broker: &FakeBroker, topic: &str, partition: i32, marks: Watermarks) {
    broker.with_state(|state| {
        if let Some(partition) = state.partition_mut(topic, partition) {
            partition.log_start_offset = marks.low;
            partition.next_offset = marks.high;
        }
    });
}

fn broker_watermarks(broker: &FakeBroker, topic: &str, partition: i32) -> Option<Watermarks> {
    broker.with_state(|state| {
        state
            .partition(topic, partition)
            .map(|partition| Watermarks {
                low: partition.log_start_offset,
                high: partition.next_offset,
            })
    })
}

#[async_trait]
impl ClusterSession for FakeCluster {
    fn identity(&self) -> &ClusterIdentity {
        &self.identity
    }

    fn consume_timeout(&self) -> Duration {
        self.inner
            .consume_timeout
            .lock()
            .expect("consume timeout")
            .unwrap_or(KafkaTuning::default().consume_timeout)
    }

    fn scan_poll_wait(&self) -> Duration {
        ScanTuning::default().poll_wait
    }

    async fn metadata(&self) -> Result<MetadataSnapshot, KafkaError> {
        self.inner.calls.metadata.fetch_add(1, Ordering::SeqCst);
        Ok(self.inner.metadata.lock().expect("metadata").clone())
    }

    async fn topic_metadata(&self, topic: &str) -> Result<TopicMetadata, KafkaError> {
        self.inner
            .calls
            .topic_metadata
            .fetch_add(1, Ordering::SeqCst);
        self.inner
            .metadata
            .lock()
            .expect("metadata")
            .topic(topic)
            .cloned()
            .ok_or_else(|| KafkaError::UnknownTopic {
                cluster: self.identity.name.clone(),
                topic: topic.to_owned(),
            })
    }

    async fn low_watermarks(
        &self,
        topics: &HashMap<String, Vec<i32>>,
    ) -> Result<HashMap<String, HashMap<i32, i64>>, KafkaError> {
        self.inner
            .calls
            .low_watermarks
            .fetch_add(1, Ordering::SeqCst);
        Ok(self.current_ends(topics, |marks| marks.low).await)
    }

    async fn high_watermarks(
        &self,
        topics: &HashMap<String, Vec<i32>>,
    ) -> Result<HashMap<String, HashMap<i32, i64>>, KafkaError> {
        self.inner
            .calls
            .high_watermarks
            .fetch_add(1, Ordering::SeqCst);
        let mut highs = self.current_ends(topics, |marks| marks.high).await;
        let growth = self
            .inner
            .watermark_growth
            .lock()
            .expect("watermark growth")
            .clone();
        if let Some(growth) = growth {
            for partitions in highs.values_mut() {
                if let Some(high) = partitions.get_mut(&0) {
                    *high += growth.grown.fetch_add(growth.step, Ordering::SeqCst);
                }
            }
        }
        Ok(highs)
    }

    async fn offsets_for_times(
        &self,
        topic: &str,
        partitions: &[i32],
        timestamp: i64,
    ) -> Result<HashMap<i32, Option<i64>>, KafkaError> {
        let records = self.inner.records.lock().expect("records").clone();
        Ok(partitions
            .iter()
            .map(|partition| {
                let offset = records
                    .iter()
                    .filter(|record| {
                        record.topic == topic
                            && record.partition == *partition
                            && record.timestamp >= timestamp
                    })
                    .map(|record| record.offset)
                    .min();
                (*partition, offset)
            })
            .collect())
    }

    async fn topic_configs(
        &self,
        topics: &[&str],
    ) -> Result<HashMap<String, Vec<ConfigEntry>>, KafkaError> {
        self.inner
            .calls
            .topic_configs
            .fetch_add(1, Ordering::SeqCst);
        let configs = self.inner.topic_configs.lock().expect("topic configs");
        Ok(topics
            .iter()
            .filter_map(|topic| {
                configs
                    .get(*topic)
                    .cloned()
                    .map(|entries| ((*topic).to_owned(), entries))
            })
            .collect())
    }

    async fn broker_configs(&self, broker_id: i32) -> Result<Vec<ConfigEntry>, KafkaError> {
        Ok(self
            .inner
            .broker_configs
            .lock()
            .expect("broker configs")
            .get(&broker_id)
            .cloned()
            .unwrap_or_default())
    }

    async fn log_dirs(&self) -> Result<Vec<LogDir>, KafkaError> {
        self.inner.calls.log_dirs.fetch_add(1, Ordering::SeqCst);
        self.inner
            .log_dirs
            .lock()
            .expect("log dirs")
            .clone()
            .map_err(KafkaError::Admin)
    }

    async fn groups(&self) -> Result<Vec<GroupSnapshot>, KafkaError> {
        Ok(self.inner.groups.lock().expect("groups").clone())
    }

    async fn committed_offsets(
        &self,
        group_id: &str,
        partitions: Option<&[(String, i32)]>,
    ) -> Result<Vec<CommittedOffset>, KafkaError> {
        self.inner
            .calls
            .committed_offsets
            .fetch_add(1, Ordering::SeqCst);
        let in_flight = self
            .inner
            .calls
            .offsets_in_flight
            .fetch_add(1, Ordering::SeqCst)
            + 1;
        self.inner
            .calls
            .offsets_peak
            .fetch_max(in_flight, Ordering::SeqCst);

        let delay = *self.inner.offsets_delay.lock().expect("offsets delay");
        if !delay.is_zero() {
            tokio::time::sleep(delay).await;
        }
        self.inner
            .calls
            .offsets_in_flight
            .fetch_sub(1, Ordering::SeqCst);

        if let Some(message) = &*self.inner.offsets_error.lock().expect("offsets error") {
            return Err(KafkaError::Admin(message.clone()));
        }
        Ok(self
            .inner
            .groups
            .lock()
            .expect("groups")
            .iter()
            .find(|group| group.id == group_id)
            .map(|group| {
                group
                    .committed
                    .iter()
                    .filter(|offset| {
                        partitions.is_none_or(|partitions| {
                            partitions.iter().any(|(topic, partition)| {
                                *topic == offset.topic && *partition == offset.partition
                            })
                        })
                    })
                    .cloned()
                    .collect()
            })
            .unwrap_or_default())
    }

    async fn open_scan(
        &self,
        topic: &str,
        windows: &[PartitionWindow],
    ) -> Result<Box<dyn ScanConsumer>, KafkaError> {
        self.inner.consumers.fetch_add(1, Ordering::SeqCst);
        let scan = FakeScan {
            cluster: self.inner.clone(),
            topic: topic.to_owned(),
            pending: Mutex::new(Vec::new()),
            windows: Mutex::new(HashMap::new()),
            paused: Mutex::new(HashSet::new()),
            owed: Mutex::new(Duration::ZERO),
        };
        scan.reassign(windows).await?;
        Ok(Box::new(scan))
    }

    async fn open_tail(
        &self,
        topic: &str,
        start: &[TailPosition],
    ) -> Result<Box<dyn TailConsumer>, KafkaError> {
        self.inner.consumers.fetch_add(1, Ordering::SeqCst);
        Ok(Box::new(FakeTail {
            cluster: self.inner.clone(),
            topic: topic.to_owned(),
            positions: Mutex::new(
                start
                    .iter()
                    .map(|position| (position.partition, position.offset))
                    .collect(),
            ),
        }))
    }

    fn payload_codec(&self) -> Option<Arc<dyn PayloadCodec>> {
        Some(self.inner.codec.clone())
    }

    fn obfuscation(&self) -> Option<Arc<ObfuscationPolicy>> {
        self.inner.obfuscation.lock().expect("obfuscation").clone()
    }

    async fn schema_subjects(&self) -> Result<Vec<SchemaSubject>, KafkaError> {
        if let Some(message) = &*self.inner.subjects_error.lock().expect("subjects error") {
            return Err(KafkaError::SchemaRegistry {
                cluster: self.identity.name.clone(),
                message: message.clone(),
            });
        }
        Ok(self.inner.subjects.lock().expect("subjects").clone())
    }

    async fn subject_schema(
        &self,
        subject: &str,
        version: i32,
    ) -> Result<RegisteredSchema, KafkaError> {
        self.inner
            .subjects
            .lock()
            .expect("subjects")
            .iter()
            .find(|registered| {
                registered.subject == subject
                    && (version == 0 || registered.versions.contains(&version))
            })
            .map(|registered| RegisteredSchema {
                id: registered.id,
                schema_type: registered.schema_type,
                schema: SUBJECT_SCHEMA.to_owned(),
                references: Vec::new(),
            })
            .ok_or_else(|| KafkaError::UnknownSubject {
                cluster: self.identity.name.clone(),
                subject: subject.to_owned(),
                version,
            })
    }

    async fn acls(&self) -> Result<AclListing, KafkaError> {
        self.inner.calls.acls.fetch_add(1, Ordering::SeqCst);
        self.inner
            .acls
            .lock()
            .expect("acls")
            .clone()
            .map_err(KafkaError::Admin)
    }

    async fn client_quotas(&self) -> Result<QuotaListing, KafkaError> {
        self.inner.calls.quotas.fetch_add(1, Ordering::SeqCst);
        self.inner
            .quotas
            .lock()
            .expect("quotas")
            .clone()
            .map_err(KafkaError::Admin)
    }
}

struct FakeScan {
    cluster: Arc<Inner>,
    topic: String,
    pending: Mutex<Vec<RawRecord>>,
    windows: Mutex<HashMap<i32, (i64, i64)>>,
    paused: Mutex<HashSet<i32>>,
    owed: Mutex<Duration>,
}

impl FakeScan {
    fn position_of(&self, partition: i32) -> Option<i64> {
        let windows = self.windows.lock().expect("windows");
        let (_, end) = *windows.get(&partition)?;
        let next = self
            .pending
            .lock()
            .expect("pending")
            .iter()
            .filter(|record| record.partition == partition)
            .map(|record| record.offset)
            .min();
        Some(next.unwrap_or(end))
    }
}

#[async_trait]
impl ScanConsumer for FakeScan {
    async fn reassign(&self, windows: &[PartitionWindow]) -> Result<(), KafkaError> {
        self.cluster.assignments.lock().expect("assignments").push(
            windows
                .iter()
                .map(|window| (window.partition, window.start, window.end))
                .collect(),
        );
        self.paused.lock().expect("paused").clear();
        *self.owed.lock().expect("owed") =
            *self.cluster.records_delay.lock().expect("records delay");

        let mut assigned = HashMap::new();
        for window in windows {
            assigned.insert(window.partition, (window.start, window.end));
        }

        let mut pending: Vec<RawRecord> = self
            .cluster
            .records
            .lock()
            .expect("records")
            .iter()
            .filter(|record| record.topic == self.topic)
            .filter(|record| {
                assigned
                    .get(&record.partition)
                    .is_some_and(|(start, end)| record.offset >= *start && record.offset < *end)
            })
            .map(raw_record)
            .collect();
        pending.sort_by_key(|record| (record.partition, record.offset));

        *self.pending.lock().expect("pending") = pending;
        *self.windows.lock().expect("windows") = assigned;
        Ok(())
    }

    async fn poll(&self, max_wait: Duration) -> Result<Vec<RawRecord>, KafkaError> {
        let owed = *self.owed.lock().expect("owed");
        if !owed.is_zero() {
            let slice = owed.min(max_wait);
            tokio::time::sleep(slice).await;
            *self.owed.lock().expect("owed") = owed - slice;
            if slice < owed {
                return Ok(Vec::new());
            }
        }

        let paused = self.paused.lock().expect("paused").clone();
        let mut pending = self.pending.lock().expect("pending");
        let (ready, held) = pending
            .drain(..)
            .partition(|record| !paused.contains(&record.partition));
        *pending = held;
        Ok(ready)
    }

    async fn pause(&self, partitions: &[i32]) {
        let mut paused = self.paused.lock().expect("paused");
        paused.extend(partitions.iter().copied());
    }

    async fn seek_to_end(&self, _windows: &[PartitionWindow]) {}

    async fn position(&self, partition: i32) -> Option<i64> {
        self.position_of(partition)
    }

    async fn lag(&self, partition: i32) -> Option<u64> {
        let position = self.position_of(partition)?;
        let high = self
            .cluster
            .watermarks
            .lock()
            .expect("watermarks")
            .get(&self.topic)
            .and_then(|partitions| partitions.get(&partition))
            .map(|marks| marks.high)?;
        Some(high.saturating_sub(position).max(0) as u64)
    }

    async fn close(&self) {}
}

pub const FAKE_TAIL_POLL_RECORDS: usize = 4;

struct FakeTail {
    cluster: Arc<Inner>,
    topic: String,
    positions: Mutex<HashMap<i32, i64>>,
}

impl FakeTail {
    fn take(&self) -> Vec<RawRecord> {
        let mut positions = self.positions.lock().expect("positions");
        let mut ready: Vec<RawRecord> = self
            .cluster
            .records
            .lock()
            .expect("records")
            .iter()
            .filter(|record| record.topic == self.topic)
            .filter(|record| {
                positions
                    .get(&record.partition)
                    .is_some_and(|position| record.offset >= *position)
            })
            .map(raw_record)
            .collect();
        ready.sort_by_key(|record| (record.partition, record.offset));
        ready.truncate(FAKE_TAIL_POLL_RECORDS);

        for record in &ready {
            positions.insert(record.partition, record.offset + 1);
        }
        ready
    }
}

#[async_trait]
impl TailConsumer for FakeTail {
    async fn poll(&self, max_wait: Duration) -> Result<Vec<RawRecord>, KafkaError> {
        self.cluster.tail_polls.fetch_add(1, Ordering::SeqCst);
        let ready = self.take();
        if !ready.is_empty() || self.positions.lock().expect("positions").is_empty() {
            return Ok(ready);
        }
        tokio::time::sleep(max_wait).await;
        Ok(self.take())
    }

    async fn position(&self, partition: i32) -> Option<i64> {
        self.positions
            .lock()
            .expect("positions")
            .get(&partition)
            .copied()
    }

    async fn lags(&self) -> HashMap<i32, u64> {
        self.cluster.tail_lag_reads.fetch_add(1, Ordering::SeqCst);
        let watermarks = self.cluster.watermarks.lock().expect("watermarks");
        let Some(highs) = watermarks.get(&self.topic) else {
            return HashMap::new();
        };
        self.positions
            .lock()
            .expect("positions")
            .iter()
            .filter_map(|(&partition, &position)| {
                let high = highs.get(&partition)?.high;
                Some((partition, high.saturating_sub(position).max(0) as u64))
            })
            .collect()
    }

    async fn seek(&self, positions: &[TailPosition]) -> Result<(), KafkaError> {
        self.cluster.tail_seeks.lock().expect("tail seeks").push(
            positions
                .iter()
                .map(|position| (position.partition, position.offset))
                .collect(),
        );
        let mut current = self.positions.lock().expect("positions");
        for position in positions {
            current.insert(position.partition, position.offset);
        }
        Ok(())
    }
}

fn raw_record(record: &FixtureRecord) -> RawRecord {
    RawRecord {
        partition: record.partition,
        offset: record.offset,
        timestamp: record.timestamp,
        key: record.key.clone(),
        value: record.value.clone(),
        headers: record
            .headers
            .iter()
            .map(|header| {
                (
                    Bytes::from(header.key.clone()),
                    Some(Bytes::from(header.value.clone())),
                )
            })
            .collect(),
    }
}

pub fn framed(schema_id: u32, body: &str) -> Bytes {
    schemreg::encode_wire_format(schema_id, body.as_bytes())
}

pub fn card_record(offset: i64, pan: &str) -> FixtureRecord {
    FixtureRecord {
        topic: "orders.created".into(),
        partition: 0,
        offset,
        timestamp: 1_700_000_000_000 + offset,
        key: Some(format!("ord_{offset}").into()),
        value: Some(framed(
            7,
            &format!(r#"{{"orderId":"ord_{offset}","card":{{"number":"{pan}","cvv":"123"}}}}"#),
        )),
        headers: vec![RecordHeader {
            key: "x-user-id".into(),
            value: "ada".into(),
        }],
        size_bytes: 0,
    }
}

#[derive(Default)]
struct CountingCodec {
    decoded: AtomicUsize,
}

#[async_trait]
impl PayloadCodec for CountingCodec {
    async fn decode_batch(&self, slots: &mut [PayloadSlot]) {
        self.decoded.fetch_add(slots.len(), Ordering::SeqCst);

        for slot in slots {
            let Ok((_, body)) = schemreg::decode_wire_prefix(&slot.raw) else {
                continue;
            };
            let Ok(json) = serde_json::from_slice(&slot.raw[body..]) else {
                continue;
            };

            slot.decoded = Some(DecodedPayload::decoded(slot.raw.clone(), json));
        }
    }
}

#[derive(Debug, Default)]
pub struct SessionCalls {
    metadata: AtomicUsize,
    topic_metadata: AtomicUsize,
    low_watermarks: AtomicUsize,
    high_watermarks: AtomicUsize,
    topic_configs: AtomicUsize,
    log_dirs: AtomicUsize,
    committed_offsets: AtomicUsize,
    offsets_in_flight: AtomicUsize,
    offsets_peak: AtomicUsize,
    acls: AtomicUsize,
    quotas: AtomicUsize,
}

impl SessionCalls {
    pub fn metadata(&self) -> usize {
        self.metadata.load(Ordering::SeqCst)
    }

    pub fn topic_metadata(&self) -> usize {
        self.topic_metadata.load(Ordering::SeqCst)
    }

    pub fn low_watermarks(&self) -> usize {
        self.low_watermarks.load(Ordering::SeqCst)
    }

    pub fn high_watermarks(&self) -> usize {
        self.high_watermarks.load(Ordering::SeqCst)
    }

    pub fn topic_configs(&self) -> usize {
        self.topic_configs.load(Ordering::SeqCst)
    }

    pub fn log_dirs(&self) -> usize {
        self.log_dirs.load(Ordering::SeqCst)
    }

    pub fn committed_offsets(&self) -> usize {
        self.committed_offsets.load(Ordering::SeqCst)
    }

    pub fn committed_offsets_peak(&self) -> usize {
        self.offsets_peak.load(Ordering::SeqCst)
    }

    pub fn acls(&self) -> usize {
        self.acls.load(Ordering::SeqCst)
    }

    pub fn quotas(&self) -> usize {
        self.quotas.load(Ordering::SeqCst)
    }
}

pub fn local_acls() -> Vec<Acl> {
    vec![
        Acl {
            resource_type: AclResourceType::Topic,
            resource_name: "orders.created".into(),
            pattern_type: AclPatternType::Literal,
            principal: "User:alice".into(),
            host: "*".into(),
            operation: AclOperation::Read,
            permission: AclPermission::Allow,
        },
        Acl {
            resource_type: AclResourceType::Topic,
            resource_name: "orders.".into(),
            pattern_type: AclPatternType::Prefixed,
            principal: "User:eve".into(),
            host: "10.0.0.1".into(),
            operation: AclOperation::Write,
            permission: AclPermission::Deny,
        },
        Acl {
            resource_type: AclResourceType::Group,
            resource_name: "order-processor".into(),
            pattern_type: AclPatternType::Literal,
            principal: "User:order-processor".into(),
            host: "*".into(),
            operation: AclOperation::Read,
            permission: AclPermission::Allow,
        },
    ]
}

fn quota_entity(parts: &[(QuotaEntityType, Option<&str>)]) -> Vec<QuotaEntity> {
    parts
        .iter()
        .map(|(entity_type, name)| QuotaEntity {
            entity_type: *entity_type,
            name: name.map(str::to_owned),
        })
        .collect()
}

fn local_quotas() -> Vec<ClientQuota> {
    vec![
        ClientQuota {
            entity: quota_entity(&[(QuotaEntityType::User, Some("alice"))]),
            values: QuotaValues {
                producer_byte_rate: Some(1_048_576.0),
                consumer_byte_rate: Some(2_097_152.0),
                ..QuotaValues::default()
            },
        },
        ClientQuota {
            entity: quota_entity(&[
                (QuotaEntityType::User, Some("alice")),
                (QuotaEntityType::ClientId, Some("checkout")),
            ]),
            values: QuotaValues {
                producer_byte_rate: Some(524_288.0),
                ..QuotaValues::default()
            },
        },
        ClientQuota {
            entity: quota_entity(&[(QuotaEntityType::User, None)]),
            values: QuotaValues {
                request_percentage: Some(50.0),
                controller_mutation_rate: Some(10.0),
                ..QuotaValues::default()
            },
        },
        ClientQuota {
            entity: quota_entity(&[(QuotaEntityType::ClientId, None)]),
            values: QuotaValues {
                consumer_byte_rate: Some(1_048_576.0),
                ..QuotaValues::default()
            },
        },
        ClientQuota {
            entity: quota_entity(&[(QuotaEntityType::Ip, Some("10.0.0.7"))]),
            values: QuotaValues {
                connection_creation_rate: Some(20.0),
                ..QuotaValues::default()
            },
        },
    ]
}
