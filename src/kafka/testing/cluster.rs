use std::collections::BTreeMap;
use std::num::NonZeroU16;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use async_trait::async_trait;
use foldhash::{HashMap, HashMapExt};

use super::consumers::{CountingCodec, FakeScan, FakeTail};
use super::fixtures::{config_entry, offsets, partition, topic};
use super::records::FixtureRecord;
use super::world;
use crate::config::{KafkaTuning, ScanTuning};
use crate::kafka::acl::{Acl, AclListing};
use crate::kafka::cluster::ClusterIdentity;
use crate::kafka::error::KafkaError;
use crate::kafka::group::{CommittedOffset, GroupSnapshot, GroupState};
use crate::kafka::metadata::{
    BrokerMetadata, MetadataSnapshot, NewTopic, TopicMetadata, Watermarks,
};
use crate::kafka::model::{
    NewRecord, PartitionWindow, ProducedRecord, RecordDeletion, ScanConsumer, TailConsumer,
    TailPosition,
};
use crate::kafka::quota::{ClientQuota, QuotaListing, QuotaValues};
use crate::kafka::registry::{
    NewSchema, RegisteredSchema, RegisteredVersion, SchemaCompatibility, SchemaDeletion,
    SchemaSubject, SchemaType,
};
use crate::kafka::scan::obfuscate::ObfuscationPolicy;
use crate::kafka::scan::payload::PayloadCodec;
use crate::kafka::session::ClusterSession;
use crate::kafka::storage::LogDir;
use crate::kafka::topic_config::{ConfigEdit, ConfigEntry};
use crate::testing::yaml;

const SUBJECT_SCHEMA: &str =
    r#"{"type":"record","name":"Order","fields":[{"name":"orderId","type":"string"}]}"#;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Api {
    Metadata,
    TopicMetadata,
    LowWatermarks,
    HighWatermarks,
    OffsetsForTimes,
    TopicConfigs,
    BrokerConfigs,
    LogDirs,
    Groups,
    CommittedOffsets,
    OpenScan,
    OpenTail,
    SchemaSubjects,
    SubjectSchema,
    Acls,
    ClientQuotas,
    CreateTopic,
    DeleteTopic,
    AlterTopicConfigs,
    AddPartitions,
    DeleteRecords,
    Produce,
    AlterGroupOffsets,
    DeleteGroup,
    DeleteGroupOffsets,
    RegisterSchema,
    DeleteSchema,
    SetCompatibility,
    CreateAcls,
    DeleteAcl,
    AlterClientQuota,
}

#[derive(Clone)]
pub struct FakeCluster {
    identity: ClusterIdentity,
    world: Arc<Mutex<World>>,
    codec: Arc<CountingCodec>,
}

pub(super) struct World {
    metadata: MetadataSnapshot,
    pub(super) watermarks: HashMap<String, HashMap<i32, Watermarks>>,
    topic_configs: HashMap<String, Vec<ConfigEntry>>,
    broker_configs: HashMap<i32, Vec<ConfigEntry>>,
    log_dirs: Vec<LogDir>,
    groups: Vec<GroupSnapshot>,
    pub(super) records: Vec<FixtureRecord>,
    schema_registry: bool,
    subjects: Vec<SchemaSubject>,
    schemas: HashMap<(String, i32), RegisteredSchema>,
    acls: AclListing,
    quotas: QuotaListing,
    faults: HashMap<Api, String>,
    delays: HashMap<Api, Duration>,
    traffic: HashMap<Api, Traffic>,
    growth: Option<Growth>,
    consume_timeout: Option<Duration>,
    obfuscation: Option<Arc<ObfuscationPolicy>>,
    pub(super) records_delay: Duration,
    pub(super) assignments: Vec<Vec<(i32, i64, i64)>>,
    pub(super) tail_seeks: Vec<Vec<(i32, i64)>>,
    pub(super) tail_polls: usize,
    pub(super) tail_lag_reads: usize,
}

#[derive(Default)]
struct Traffic {
    calls: usize,
    in_flight: usize,
    peak: usize,
}

struct Growth {
    step: i64,
    grown: i64,
}

pub(super) fn lock(world: &Mutex<World>) -> MutexGuard<'_, World> {
    world.lock().expect("fake cluster world")
}

impl FakeCluster {
    pub fn local() -> Self {
        let local = world::local();
        Self {
            identity: ClusterIdentity::new("local"),
            world: Arc::new(Mutex::new(World {
                metadata: local.metadata,
                watermarks: local.watermarks,
                topic_configs: local.topic_configs,
                broker_configs: local.broker_configs,
                log_dirs: local.log_dirs,
                groups: local.groups,
                records: local.records,
                schema_registry: true,
                subjects: local.subjects,
                schemas: HashMap::new(),
                acls: local.acls,
                quotas: local.quotas,
                faults: HashMap::new(),
                delays: HashMap::new(),
                traffic: HashMap::new(),
                growth: None,
                consume_timeout: None,
                obfuscation: None,
                records_delay: Duration::ZERO,
                assignments: Vec::new(),
                tail_seeks: Vec::new(),
                tail_polls: 0,
                tail_lag_reads: 0,
            })),
            codec: Arc::new(CountingCodec::default()),
        }
    }

    pub fn named(name: &str) -> Self {
        let mut cluster = Self::local();
        cluster.identity.name = name.to_owned();
        cluster
    }

    pub fn with_topic(self, name: &str, partitions: i32, high: i64) -> Self {
        self.add_topic(name, partitions, high);
        self
    }

    pub fn with_groups(self, groups: impl IntoIterator<Item = GroupSnapshot>) -> Self {
        for group in groups {
            self.put_group(group);
        }
        self
    }

    pub fn with_records(self, records: Vec<FixtureRecord>) -> Self {
        {
            let mut world = self.world();
            let mut highs: BTreeMap<String, BTreeMap<i32, i64>> = BTreeMap::new();
            for record in &records {
                let high = highs
                    .entry(record.topic().to_owned())
                    .or_default()
                    .entry(record.partition())
                    .or_default();
                *high = (*high).max(record.offset() + 1);
            }
            for (name, partitions) in highs {
                world.put_topic(replicated(&name, partitions.keys().copied()));
                world.watermarks.insert(
                    name,
                    partitions
                        .into_iter()
                        .map(|(id, high)| (id, Watermarks { low: 0, high }))
                        .collect(),
                );
            }
            world.records = records;
        }
        self
    }

    pub fn with_obfuscation(self, source: &str) -> Self {
        self.world().obfuscation = Some(Arc::new(ObfuscationPolicy::compile(&yaml(source))));
        self
    }

    pub fn without_schema_registry(self) -> Self {
        {
            let mut world = self.world();
            world.schema_registry = false;
            world.subjects.clear();
        }
        self
    }

    pub fn with_consume_timeout(self, timeout: Duration) -> Self {
        self.world().consume_timeout = Some(timeout);
        self
    }

    pub fn with_records_delay(self, delay: Duration) -> Self {
        self.world().records_delay = delay;
        self
    }

    pub fn with_delay(self, api: Api, delay: Duration) -> Self {
        self.world().delays.insert(api, delay);
        self
    }

    pub fn with_growing_watermarks(self, step: i64) -> Self {
        self.world().growth = Some(Growth { step, grown: 0 });
        self
    }

    pub fn fail(&self, api: Api, message: &str) {
        self.world().faults.insert(api, message.to_owned());
    }

    pub fn add_topic(&self, name: &str, partitions: i32, high: i64) {
        let mut world = self.world();
        world.put_topic(replicated(name, 0..partitions));
        world.watermarks.insert(
            name.to_owned(),
            (0..partitions)
                .map(|id| (id, Watermarks { low: 0, high }))
                .collect(),
        );
    }

    pub fn add_partition(&self, topic: &str, id: i32) {
        let mut world = self.world();
        if let Some(meta) = world
            .metadata
            .topics
            .iter_mut()
            .find(|meta| meta.name == topic)
            && !meta.partitions.iter().any(|partition| partition.id == id)
        {
            meta.partitions.push(partition(id, vec![1], vec![1]));
        }
        world
            .watermarks
            .entry(topic.to_owned())
            .or_default()
            .entry(id)
            .or_insert(Watermarks { low: 0, high: 0 });
    }

    pub fn set_watermarks(&self, topic: &str, partition: i32, watermarks: Watermarks) {
        self.world()
            .watermarks
            .entry(topic.to_owned())
            .or_default()
            .insert(partition, watermarks);
    }

    pub fn put_topic(&self, topic: TopicMetadata) {
        self.world().put_topic(topic);
    }

    pub fn remove_topic(&self, name: &str) {
        let mut world = self.world();
        world.metadata.topics.retain(|topic| topic.name != name);
        world.watermarks.remove(name);
    }

    pub fn add_broker(&self, id: i32) {
        self.world().metadata.brokers.push(BrokerMetadata {
            id,
            host: "localhost".into(),
            port: 9092 + id,
        });
    }

    pub fn put_group(&self, group: GroupSnapshot) {
        let mut world = self.world();
        match world
            .groups
            .iter_mut()
            .find(|existing| existing.id == group.id)
        {
            Some(existing) => *existing = group,
            None => world.groups.push(group),
        }
    }

    pub fn remove_group(&self, id: &str) {
        self.world().groups.retain(|group| group.id != id);
    }

    pub fn commit_offsets(&self, id: &str, committed: &[(&str, i32, i64)]) {
        if let Some(group) = self.world().groups.iter_mut().find(|group| group.id == id) {
            group.committed = offsets(committed).committed;
        }
    }

    pub fn set_topic_configs(&self, topic: &str, configs: Vec<ConfigEntry>) {
        self.world().topic_configs.insert(topic.to_owned(), configs);
    }

    pub fn set_subjects(&self, subjects: Vec<SchemaSubject>) {
        self.world().subjects = subjects;
    }

    pub fn set_acls(&self, acls: AclListing) {
        self.world().acls = acls;
    }

    pub fn set_quotas(&self, quotas: QuotaListing) {
        self.world().quotas = quotas;
    }

    pub fn produce(&self, record: FixtureRecord) {
        let mut world = self.world();
        let marks = world
            .watermarks
            .entry(record.topic().to_owned())
            .or_default()
            .entry(record.partition())
            .or_insert(Watermarks { low: 0, high: 0 });
        marks.high = marks.high.max(record.offset() + 1);
        world.records.push(record);
    }

    pub fn calls(&self, api: Api) -> usize {
        self.world()
            .traffic
            .get(&api)
            .map_or(0, |traffic| traffic.calls)
    }

    pub fn peak(&self, api: Api) -> usize {
        self.world()
            .traffic
            .get(&api)
            .map_or(0, |traffic| traffic.peak)
    }

    pub fn reset_calls(&self) {
        self.world().traffic.clear();
    }

    pub fn assigned_windows(&self) -> Vec<Vec<(i32, i64, i64)>> {
        self.world().assignments.clone()
    }

    pub fn tail_seeks(&self) -> Vec<Vec<(i32, i64)>> {
        self.world().tail_seeks.clone()
    }

    pub fn tail_polls(&self) -> usize {
        self.world().tail_polls
    }

    pub fn tail_lag_reads(&self) -> usize {
        self.world().tail_lag_reads
    }

    pub fn decoded_payloads(&self) -> usize {
        self.codec.decoded()
    }

    fn world(&self) -> MutexGuard<'_, World> {
        lock(&self.world)
    }

    async fn answer(&self, api: Api) -> Result<(), KafkaError> {
        let delay = {
            let mut world = self.world();
            let traffic = world.traffic.entry(api).or_default();
            traffic.calls += 1;
            traffic.in_flight += 1;
            traffic.peak = traffic.peak.max(traffic.in_flight);
            world.delays.get(&api).copied()
        };
        let in_flight = InFlight {
            world: &self.world,
            api,
        };
        if let Some(delay) = delay {
            tokio::time::sleep(delay).await;
        }
        drop(in_flight);

        match self.world().faults.get(&api) {
            None => Ok(()),
            Some(message) => Err(match api {
                Api::SchemaSubjects
                | Api::SubjectSchema
                | Api::RegisterSchema
                | Api::DeleteSchema
                | Api::SetCompatibility => KafkaError::SchemaRegistry {
                    cluster: self.identity.name.clone(),
                    message: message.clone(),
                },
                _ => KafkaError::Admin(message.clone()),
            }),
        }
    }

    fn ends(
        &self,
        topics: &HashMap<String, Vec<i32>>,
        end: fn(Watermarks) -> i64,
    ) -> HashMap<String, HashMap<i32, i64>> {
        let world = self.world();
        topics
            .iter()
            .map(|(name, partitions)| {
                let marks = world.watermarks.get(name);
                let ends = partitions
                    .iter()
                    .filter_map(|partition| {
                        let marks = marks?.get(partition)?;
                        Some((*partition, end(*marks)))
                    })
                    .collect();
                (name.to_owned(), ends)
            })
            .collect()
    }
}

impl World {
    fn authorized_acls(&mut self) -> Result<&mut Vec<Acl>, KafkaError> {
        match &mut self.acls {
            AclListing::Enabled(rows) => Ok(rows),
            AclListing::Disabled => Err(KafkaError::Refused(
                "No Authorizer is configured.".to_owned(),
            )),
            AclListing::Denied => Err(KafkaError::Refused("ClusterAuthorizationFailed".to_owned())),
        }
    }

    fn put_topic(&mut self, topic: TopicMetadata) {
        match self
            .metadata
            .topics
            .iter_mut()
            .find(|existing| existing.name == topic.name)
        {
            Some(existing) => *existing = topic,
            None => self.metadata.topics.push(topic),
        }
    }
}

fn replicated(name: &str, partitions: impl IntoIterator<Item = i32>) -> TopicMetadata {
    topic(
        name,
        partitions
            .into_iter()
            .map(|id| partition(id, vec![1], vec![1]))
            .collect(),
    )
}

struct InFlight<'a> {
    world: &'a Mutex<World>,
    api: Api,
}

impl Drop for InFlight<'_> {
    fn drop(&mut self) {
        if let Some(traffic) = lock(self.world).traffic.get_mut(&self.api) {
            traffic.in_flight -= 1;
        }
    }
}

#[async_trait]
impl ClusterSession for FakeCluster {
    fn identity(&self) -> &ClusterIdentity {
        &self.identity
    }

    fn consume_timeout(&self) -> Duration {
        self.world()
            .consume_timeout
            .unwrap_or(KafkaTuning::default().consume_timeout)
    }

    fn scan_poll_wait(&self) -> Duration {
        ScanTuning::default().poll_wait
    }

    async fn metadata(&self) -> Result<MetadataSnapshot, KafkaError> {
        self.answer(Api::Metadata).await?;
        Ok(self.world().metadata.clone())
    }

    async fn topic_metadata(&self, topic: &str) -> Result<TopicMetadata, KafkaError> {
        self.answer(Api::TopicMetadata).await?;
        self.world()
            .metadata
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
        self.answer(Api::LowWatermarks).await?;
        Ok(self.ends(topics, |marks| marks.low))
    }

    async fn high_watermarks(
        &self,
        topics: &HashMap<String, Vec<i32>>,
    ) -> Result<HashMap<String, HashMap<i32, i64>>, KafkaError> {
        self.answer(Api::HighWatermarks).await?;
        let mut highs = self.ends(topics, |marks| marks.high);
        if let Some(growth) = &mut self.world().growth {
            for partitions in highs.values_mut() {
                if let Some(high) = partitions.get_mut(&0) {
                    *high += growth.grown;
                    growth.grown += growth.step;
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
        self.answer(Api::OffsetsForTimes).await?;
        let world = self.world();
        Ok(partitions
            .iter()
            .map(|partition| {
                let offset = world
                    .records
                    .iter()
                    .filter(|record| {
                        record.topic() == topic
                            && record.partition() == *partition
                            && record.timestamp() >= timestamp
                    })
                    .map(FixtureRecord::offset)
                    .min();
                (*partition, offset)
            })
            .collect())
    }

    async fn topic_configs(
        &self,
        topics: &[&str],
    ) -> Result<HashMap<String, Vec<ConfigEntry>>, KafkaError> {
        self.answer(Api::TopicConfigs).await?;
        let world = self.world();
        Ok(topics
            .iter()
            .filter_map(|topic| {
                let entries = world.topic_configs.get(*topic)?.clone();
                Some(((*topic).to_owned(), entries))
            })
            .collect())
    }

    async fn broker_configs(&self, broker_id: i32) -> Result<Vec<ConfigEntry>, KafkaError> {
        self.answer(Api::BrokerConfigs).await?;
        Ok(self
            .world()
            .broker_configs
            .get(&broker_id)
            .cloned()
            .unwrap_or_default())
    }

    async fn log_dirs(&self) -> Result<Vec<LogDir>, KafkaError> {
        self.answer(Api::LogDirs).await?;
        Ok(self.world().log_dirs.clone())
    }

    async fn groups(&self) -> Result<Vec<GroupSnapshot>, KafkaError> {
        self.answer(Api::Groups).await?;
        Ok(self.world().groups.clone())
    }

    async fn committed_offsets(
        &self,
        group_id: &str,
        partitions: Option<&[(String, i32)]>,
    ) -> Result<Vec<CommittedOffset>, KafkaError> {
        self.answer(Api::CommittedOffsets).await?;
        let wanted = |offset: &&CommittedOffset| {
            partitions.is_none_or(|partitions| {
                partitions.iter().any(|(topic, partition)| {
                    *topic == offset.topic && *partition == offset.partition
                })
            })
        };
        Ok(self
            .world()
            .groups
            .iter()
            .find(|group| group.id == group_id)
            .map(|group| group.committed.iter().filter(wanted).cloned().collect())
            .unwrap_or_default())
    }

    async fn open_scan(
        &self,
        topic: &str,
        windows: &[PartitionWindow],
    ) -> Result<Box<dyn ScanConsumer>, KafkaError> {
        self.answer(Api::OpenScan).await?;
        let scan = FakeScan::new(Arc::clone(&self.world), topic);
        scan.reassign(windows).await?;
        Ok(Box::new(scan))
    }

    async fn open_tail(
        &self,
        topic: &str,
        start: &[TailPosition],
    ) -> Result<Box<dyn TailConsumer>, KafkaError> {
        self.answer(Api::OpenTail).await?;
        Ok(Box::new(FakeTail::new(
            Arc::clone(&self.world),
            topic,
            start,
        )))
    }

    fn payload_codec(&self) -> Option<Arc<dyn PayloadCodec>> {
        Some(self.codec.clone())
    }

    fn obfuscation(&self) -> Option<Arc<ObfuscationPolicy>> {
        self.world().obfuscation.clone()
    }

    fn has_schema_registry(&self) -> bool {
        self.world().schema_registry
    }

    async fn schema_subjects(&self) -> Result<Vec<SchemaSubject>, KafkaError> {
        self.answer(Api::SchemaSubjects).await?;
        Ok(self.world().subjects.clone())
    }

    async fn subject_schema(
        &self,
        subject: &str,
        version: i32,
    ) -> Result<RegisteredSchema, KafkaError> {
        self.answer(Api::SubjectSchema).await?;
        let world = self.world();
        world
            .subjects
            .iter()
            .find(|registered| {
                registered.subject == subject
                    && (version == 0 || registered.versions.contains(&version))
            })
            .map(|registered| {
                let version = if version == 0 {
                    registered.latest_version
                } else {
                    version
                };
                world
                    .schemas
                    .get(&(subject.to_owned(), version))
                    .cloned()
                    .unwrap_or_else(|| RegisteredSchema {
                        id: registered.id,
                        schema_type: registered.schema_type,
                        schema: SUBJECT_SCHEMA.to_owned(),
                        references: Vec::new(),
                    })
            })
            .ok_or_else(|| KafkaError::UnknownSubject {
                cluster: self.identity.name.clone(),
                subject: subject.to_owned(),
                version,
            })
    }

    async fn acls(&self) -> Result<AclListing, KafkaError> {
        self.answer(Api::Acls).await?;
        Ok(self.world().acls.clone())
    }

    async fn client_quotas(&self) -> Result<QuotaListing, KafkaError> {
        self.answer(Api::ClientQuotas).await?;
        Ok(self.world().quotas.clone())
    }

    async fn create_topic(&self, topic: &NewTopic) -> Result<(), KafkaError> {
        self.answer(Api::CreateTopic).await?;
        if self.world().metadata.topic(&topic.name).is_some() {
            return Err(KafkaError::Refused(format!(
                "Topic '{}' already exists.",
                topic.name
            )));
        }
        let partitions = topic.partitions.map_or(1, |count| count.get().into());
        self.add_topic(&topic.name, partitions, 0);
        self.set_topic_configs(
            &topic.name,
            topic
                .configs
                .iter()
                .map(|(name, value)| config_entry(name, value))
                .collect(),
        );
        Ok(())
    }

    async fn delete_topic(&self, topic: &str) -> Result<(), KafkaError> {
        self.answer(Api::DeleteTopic).await?;
        if self.world().metadata.topic(topic).is_none() {
            return Err(KafkaError::Refused(
                "This server does not host this topic-partition.".to_owned(),
            ));
        }
        self.remove_topic(topic);
        Ok(())
    }

    async fn alter_topic_configs(&self, topic: &str, edit: &ConfigEdit) -> Result<(), KafkaError> {
        self.answer(Api::AlterTopicConfigs).await?;
        let mut world = self.world();
        let entries = world.topic_configs.entry(topic.to_owned()).or_default();
        entries.retain(|entry| {
            !edit.reset.contains(&entry.name) && !edit.set.contains_key(&entry.name)
        });
        entries.extend(
            edit.set
                .iter()
                .map(|(name, value)| config_entry(name, value)),
        );
        Ok(())
    }

    async fn add_partitions(&self, topic: &str, total: NonZeroU16) -> Result<(), KafkaError> {
        self.answer(Api::AddPartitions).await?;
        let current = self
            .world()
            .metadata
            .topic(topic)
            .map_or(0, |meta| meta.partitions.len());
        let total = usize::from(total.get());
        if total <= current {
            return Err(KafkaError::Refused(format!(
                "Topic currently has {current} partitions, which is higher than the requested {total}."
            )));
        }
        for id in current..total {
            self.add_partition(topic, i32::try_from(id).expect("a partition id"));
        }
        Ok(())
    }

    async fn delete_records(
        &self,
        deletion: &RecordDeletion,
    ) -> Result<BTreeMap<i32, i64>, KafkaError> {
        self.answer(Api::DeleteRecords).await?;
        let mut world = self.world();
        let mut lows = BTreeMap::new();
        for (&partition, &before) in &deletion.before {
            let marks = world
                .watermarks
                .get_mut(&deletion.topic)
                .and_then(|partitions| partitions.get_mut(&partition))
                .ok_or_else(|| KafkaError::Refused("UnknownTopicOrPartition".to_owned()))?;
            let low = before.unwrap_or(marks.high);
            if low > marks.high {
                return Err(KafkaError::Refused("OffsetOutOfRange".to_owned()));
            }
            marks.low = marks.low.max(low);
            lows.insert(partition, marks.low);
        }
        world.records.retain(|record| {
            record.topic() != deletion.topic
                || lows
                    .get(&record.partition())
                    .is_none_or(|&low| record.offset() >= low)
        });
        Ok(lows)
    }

    async fn produce(&self, record: &NewRecord) -> Result<ProducedRecord, KafkaError> {
        self.answer(Api::Produce).await?;
        let partition = record.partition.unwrap_or(0);
        let offset = self
            .world()
            .watermarks
            .get(&record.topic)
            .and_then(|partitions| partitions.get(&partition))
            .map_or(0, |marks| marks.high);
        let mut stored = FixtureRecord::new(&record.topic, partition, offset);
        if let Some(key) = &record.key {
            stored = stored.key(key.clone());
        }
        if let Some(value) = &record.value {
            stored = stored.value(value.clone());
        }
        for header in &record.headers {
            stored = stored.header(&header.key, &header.value);
        }
        FakeCluster::produce(self, stored);
        Ok(ProducedRecord { partition, offset })
    }

    async fn alter_group_offsets(
        &self,
        group: &str,
        offsets: &[CommittedOffset],
    ) -> Result<(), KafkaError> {
        self.answer(Api::AlterGroupOffsets).await?;
        let mut world = self.world();
        let index = match world
            .groups
            .iter()
            .position(|snapshot| snapshot.id == group)
        {
            Some(index) => index,
            None => {
                world.groups.push(GroupSnapshot {
                    id: group.to_owned(),
                    state: GroupState::Empty,
                    protocol: String::new(),
                    members: Vec::new(),
                    committed: Vec::new(),
                });
                world.groups.len() - 1
            }
        };
        let snapshot = &mut world.groups[index];
        if !snapshot.members.is_empty() {
            return Err(KafkaError::Refused("UnknownMemberId".to_owned()));
        }
        for offset in offsets {
            snapshot.committed.retain(|committed| {
                committed.topic != offset.topic || committed.partition != offset.partition
            });
            snapshot.committed.push(offset.clone());
        }
        Ok(())
    }

    async fn delete_group(&self, group: &str) -> Result<(), KafkaError> {
        self.answer(Api::DeleteGroup).await?;
        let mut world = self.world();
        let Some(index) = world
            .groups
            .iter()
            .position(|snapshot| snapshot.id == group)
        else {
            return Err(KafkaError::Refused("GroupIdNotFound".to_owned()));
        };
        if !world.groups[index].members.is_empty() {
            return Err(KafkaError::Refused("NonEmptyGroup".to_owned()));
        }
        world.groups.remove(index);
        Ok(())
    }

    async fn delete_group_offsets(
        &self,
        group: &str,
        topic: &str,
        partitions: &[i32],
    ) -> Result<(), KafkaError> {
        self.answer(Api::DeleteGroupOffsets).await?;
        let mut world = self.world();
        let Some(snapshot) = world
            .groups
            .iter_mut()
            .find(|snapshot| snapshot.id == group)
        else {
            return Err(KafkaError::Refused("GroupIdNotFound".to_owned()));
        };
        let subscribed = snapshot.members.iter().any(|member| {
            member
                .assignments
                .iter()
                .any(|assignment| assignment.topic == topic)
        });
        if subscribed {
            return Err(KafkaError::Refused("GroupSubscribedToTopic".to_owned()));
        }
        snapshot
            .committed
            .retain(|offset| offset.topic != topic || !partitions.contains(&offset.partition));
        Ok(())
    }

    async fn register_schema(&self, schema: &NewSchema) -> Result<RegisteredVersion, KafkaError> {
        self.answer(Api::RegisterSchema).await?;
        let mut world = self.world();
        if !world.schema_registry {
            return Err(KafkaError::NoSchemaRegistry(self.identity.name.clone()));
        }
        if schema.schema_type != SchemaType::Protobuf
            && serde_json::from_str::<serde_json::Value>(&schema.schema).is_err()
        {
            return Err(KafkaError::RegistryRefused(format!(
                "Invalid schema {}",
                schema.schema
            )));
        }
        if let Some(((_, version), held)) = world.schemas.iter().find(|((subject, _), held)| {
            *subject == schema.subject
                && held.schema_type == schema.schema_type
                && held.schema == schema.schema
                && held.references == schema.references
        }) {
            return Ok(RegisteredVersion {
                id: held.id,
                version: *version,
            });
        }
        let id = 1 + world
            .subjects
            .iter()
            .map(|subject| subject.id)
            .chain(world.schemas.values().map(|held| held.id))
            .max()
            .unwrap_or(0);
        let version = match world
            .subjects
            .iter_mut()
            .find(|subject| subject.subject == schema.subject)
        {
            Some(subject) => {
                subject.latest_version += 1;
                subject.versions.push(subject.latest_version);
                subject.id = id;
                subject.schema_type = schema.schema_type;
                subject.latest_version
            }
            None => {
                world.subjects.push(SchemaSubject {
                    subject: schema.subject.clone(),
                    id,
                    schema_type: schema.schema_type,
                    latest_version: 1,
                    versions: vec![1],
                    compatibility: SchemaCompatibility::Backward,
                });
                1
            }
        };
        world.schemas.insert(
            (schema.subject.clone(), version),
            RegisteredSchema {
                id,
                schema_type: schema.schema_type,
                schema: schema.schema.clone(),
                references: schema.references.clone(),
            },
        );
        Ok(RegisteredVersion { id, version })
    }

    async fn delete_schema(&self, deletion: &SchemaDeletion) -> Result<(), KafkaError> {
        self.answer(Api::DeleteSchema).await?;
        let mut world = self.world();
        if !world.schema_registry {
            return Err(KafkaError::NoSchemaRegistry(self.identity.name.clone()));
        }
        let unknown = || KafkaError::UnknownSubject {
            cluster: self.identity.name.clone(),
            subject: deletion.subject.clone(),
            version: deletion.version.unwrap_or(0),
        };
        let index = world
            .subjects
            .iter()
            .position(|subject| subject.subject == deletion.subject)
            .ok_or_else(unknown)?;
        let subject = &mut world.subjects[index];
        match deletion.version {
            Some(version) if subject.versions.contains(&version) => {
                subject.versions.retain(|&kept| kept != version);
            }
            Some(_) => return Err(unknown()),
            None => subject.versions.clear(),
        }
        match subject.versions.last() {
            Some(&latest) => subject.latest_version = latest,
            None => {
                world.subjects.remove(index);
            }
        }
        world.schemas.retain(|(subject, version), _| {
            *subject != deletion.subject || deletion.version.is_some_and(|gone| gone != *version)
        });
        Ok(())
    }

    async fn set_compatibility(
        &self,
        subject: &str,
        level: SchemaCompatibility,
    ) -> Result<(), KafkaError> {
        self.answer(Api::SetCompatibility).await?;
        let mut world = self.world();
        if !world.schema_registry {
            return Err(KafkaError::NoSchemaRegistry(self.identity.name.clone()));
        }
        let known = world
            .subjects
            .iter_mut()
            .find(|known| known.subject == subject)
            .ok_or_else(|| KafkaError::UnknownSubject {
                cluster: self.identity.name.clone(),
                subject: subject.to_owned(),
                version: 0,
            })?;
        known.compatibility = level;
        Ok(())
    }

    async fn create_acls(&self, acls: &[Acl]) -> Result<(), KafkaError> {
        self.answer(Api::CreateAcls).await?;
        let mut world = self.world();
        let rows = world.authorized_acls()?;
        for acl in acls {
            if !rows.contains(acl) {
                rows.push(acl.clone());
            }
        }
        Ok(())
    }

    async fn delete_acl(&self, acl: &Acl) -> Result<(), KafkaError> {
        self.answer(Api::DeleteAcl).await?;
        self.world().authorized_acls()?.retain(|row| row != acl);
        Ok(())
    }

    async fn alter_client_quota(&self, quota: &ClientQuota) -> Result<(), KafkaError> {
        self.answer(Api::AlterClientQuota).await?;
        let mut world = self.world();
        let QuotaListing::Described(quotas) = &mut world.quotas else {
            return Err(KafkaError::Refused("ClusterAuthorizationFailed".to_owned()));
        };
        quotas.retain(|known| known.entity != quota.entity);
        if quota.values != QuotaValues::default() {
            quotas.push(quota.clone());
        }
        Ok(())
    }
}
