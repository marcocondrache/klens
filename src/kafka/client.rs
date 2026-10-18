mod convert;
mod groups;
mod offsets;
mod pool;
mod scan;
mod tail;
#[cfg(test)]
pub(crate) mod testing;
mod transport;

use std::collections::BTreeMap;
use std::num::NonZeroU16;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use foldhash::{HashMap, HashMapExt};
use krafka::admin::{
    AclFilter, AlterClientQuotasOptions, AlterConsumerGroupOffsetsOptions,
    AlterUserScramCredentialsOptions, ClientQuotaAlteration, ClientQuotaFilter, ConfigResource,
    ConsumerGroupListing, CreateAclsOptions, CreatePartitionsOptions, CreateTopicsOptions,
    DeleteAclsOptions, DeleteConsumerGroupOffsetsOptions, DeleteConsumerGroupsOptions,
    DeleteRecordsOptions, DeleteTopicsOptions, DescribeAclsOptions, DescribeClientQuotasOptions,
    DescribeConfigsOptions, DescribeConsumerGroupsOptions, DescribeLogDirsOptions,
    DescribeUserScramCredentialsOptions, IncrementalAlterConfigsOptions,
    ListConsumerGroupOffsetsOptions, ListConsumerGroupsOptions, ListOffsetsOptions,
    ListTopicsOptions, ListedOffset, OffsetSpec, ScramCredentialDeletion, TopicPartition,
};
use krafka::error::KrafkaError;
use krafka::producer::Producer;
use tokio::sync::OnceCell;

use crate::config::{self, ScanTuning, Tuning};
use crate::kafka::acl::{Acl, AclListing};
use crate::kafka::cluster::ClusterIdentity;
use crate::kafka::error::KafkaError;
use crate::kafka::group::{CommittedOffset, GroupSnapshot};
use crate::kafka::metadata::{MetadataSnapshot, NewTopic, TopicMetadata, validate_topic_name};
use crate::kafka::model::{
    NewRecord, PartitionWindow, ProducedRecord, RecordDeletion, ScanConsumer, TailConsumer,
    TailPosition,
};
use crate::kafka::quota::{ClientQuota, DescribedQuota, QuotaListing};
use crate::kafka::registry::client::SchemaRegistryClient;
use crate::kafka::registry::decode::PayloadDecoder;
use crate::kafka::registry::{
    KnownSchemas, NewSchema, RegisteredSchema, RegisteredVersion, SchemaCompatibility,
    SchemaDeletion, SchemaSubject,
};
use crate::kafka::scan::obfuscate::ObfuscationPolicy;
use crate::kafka::scan::payload::PayloadCodec;
use crate::kafka::scram::{DescribedUser, NewScramCredential, ScramListing, ScramMechanism};
use crate::kafka::session::{ClusterSession, OffsetLookup};
use crate::kafka::storage::LogDir;
use crate::kafka::topic_config::{BrokerScope, ConfigEdit, ConfigEntry};

use convert::{committed_from_krafka, produce_refusal, refused, unless_denied};
use groups::{
    ACTIVE_GROUP_STATES, LISTED_GROUP_TYPES, snapshots_from_descriptions, split_empty_groups,
};
use offsets::{log_offsets, time_offsets};
use pool::ScanPool;
use scan::ReaderConfig;
use tail::TailLease;
use transport::{Connector, Transport};

pub struct KafkaClient {
    identity: ClusterIdentity,
    consume_timeout: Duration,
    scan_poll_wait: Duration,
    scan_tuning: ScanTuning,
    scan_reader: ReaderConfig,
    tail_reader: ReaderConfig,
    connector: Connector,
    link: OnceCell<Link>,
    producer: OnceCell<Producer>,
    schema_registry: Option<Arc<PayloadDecoder>>,
    obfuscation: Option<Arc<ObfuscationPolicy>>,
}

/// The clients klens holds once a bootstrap server has answered.
struct Link {
    transport: Transport,
    scans: Arc<ScanPool>,
}

impl Link {
    async fn open(
        connector: &Connector,
        scan_tuning: &ScanTuning,
        scan_reader: ReaderConfig,
    ) -> Result<Self, KafkaError> {
        let transport = Transport::connect(connector).await?;
        let scan_client = connector.connect().await?;

        Ok(Self {
            transport,
            scans: ScanPool::spawn(scan_client, scan_tuning, scan_reader),
        })
    }
}

impl std::fmt::Debug for KafkaClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaClient")
            .field("identity", &self.identity)
            .finish_non_exhaustive()
    }
}

impl KafkaClient {
    /// Reaches no broker. The client connects on first use.
    pub fn new(name: &str, cluster: &config::Cluster, tuning: &Tuning) -> Result<Self, KafkaError> {
        let identity = ClusterIdentity::new(name);
        let schema_registry = cluster
            .schema_registry
            .as_ref()
            .map(|registry| {
                SchemaRegistryClient::new(identity.name.clone(), registry, &tuning.schema_registry)
                    .map(|client| {
                        Arc::new(PayloadDecoder::new(
                            client,
                            tuning.schema_registry.missing_schema_ttl,
                        ))
                    })
            })
            .transpose()?;

        let obfuscation = cluster
            .obfuscation
            .as_ref()
            .map(|rules| Arc::new(ObfuscationPolicy::compile(rules)));

        Ok(Self {
            identity,
            consume_timeout: tuning.kafka.consume_timeout,
            scan_poll_wait: tuning.scan.poll_wait,
            scan_tuning: tuning.scan,
            scan_reader: ReaderConfig::new(tuning, tuning.scan.poll_wait),
            tail_reader: ReaderConfig::new(tuning, tuning.tail.poll_wait),
            connector: Connector::new(name, cluster, &tuning.kafka),
            link: OnceCell::new(),
            producer: OnceCell::new(),
            schema_registry,
            obfuscation,
        })
    }

    /// A failed attempt leaves the cell empty, so the next call tries again
    /// and a cluster that was down when klens started is read once it answers.
    async fn link(&self) -> Result<&Link, KafkaError> {
        self.link
            .get_or_try_init(|| Link::open(&self.connector, &self.scan_tuning, self.scan_reader))
            .await
    }

    async fn transport(&self) -> Result<&Transport, KafkaError> {
        Ok(&self.link().await?.transport)
    }

    async fn list_offsets(
        &self,
        topics: &HashMap<String, Vec<i32>>,
        spec: OffsetSpec,
    ) -> Result<OffsetLookup, KafkaError> {
        let specs = offset_specs(topics, spec);
        if specs.is_empty() {
            return Ok(OffsetLookup::default());
        }

        let listed = self
            .transport()
            .await?
            .admin
            .list_offsets(specs, ListOffsetsOptions::default())
            .await?;
        Ok(log_offsets(listed.into_iter().map(listed_offset_parts)))
    }
}

#[async_trait]
impl ClusterSession for KafkaClient {
    fn identity(&self) -> &ClusterIdentity {
        &self.identity
    }

    fn consume_timeout(&self) -> Duration {
        self.consume_timeout
    }

    fn scan_poll_wait(&self) -> Duration {
        self.scan_poll_wait
    }

    async fn metadata(&self) -> Result<MetadataSnapshot, KafkaError> {
        let transport = self.transport().await?;

        // Listing the topics refreshes the metadata cache the clients share.
        transport
            .admin
            .list_topics(ListTopicsOptions::default().include_internal(true))
            .await?;
        let cached = transport.metadata.fetch_metadata(None).await?;
        Ok(MetadataSnapshot::from_krafka(cached))
    }

    async fn topic_metadata(&self, topic: &str) -> Result<TopicMetadata, KafkaError> {
        let unknown = || KafkaError::UnknownTopic {
            cluster: self.identity.name.clone(),
            topic: topic.to_owned(),
        };
        if validate_topic_name(topic).is_err() {
            return Err(unknown());
        }

        self.transport()
            .await?
            .metadata
            .fetch_metadata(Some(topic))
            .await?
            .topics
            .into_iter()
            .next()
            .map(TopicMetadata::from_krafka)
            .ok_or_else(unknown)
    }

    async fn groups(&self) -> Result<Vec<GroupSnapshot>, KafkaError> {
        let admin = &self.transport().await?.admin;
        let listing = ListConsumerGroupsOptions::default().types(owned(&LISTED_GROUP_TYPES));
        let active_listing = listing.clone().states(owned(&ACTIVE_GROUP_STATES));
        let (listed, active) = tokio::try_join!(
            admin.list_consumer_groups(listing),
            admin.list_consumer_groups(active_listing),
        )?;
        let (active, mut groups) = split_empty_groups(group_ids(listed)?, group_ids(active)?);
        groups.extend(snapshots_from_descriptions(
            admin
                .describe_consumer_groups(active, DescribeConsumerGroupsOptions::default())
                .await?,
        ));
        Ok(groups)
    }

    async fn committed_offsets(
        &self,
        group_id: &str,
        partitions: Option<&[(&str, i32)]>,
    ) -> Result<Vec<CommittedOffset>, KafkaError> {
        let mut options = ListConsumerGroupOffsetsOptions::default();
        if let Some(partitions) = partitions {
            let partitions: Vec<TopicPartition> = partitions
                .iter()
                .filter(|(topic, _)| validate_topic_name(topic).is_ok())
                .map(|&(topic, partition)| TopicPartition::new(topic, partition))
                .collect();
            if partitions.is_empty() {
                return Ok(Vec::new());
            }
            options = options.partitions(partitions);
        }

        let listed = self
            .transport()
            .await?
            .admin
            .list_consumer_group_offsets(group_id, options)
            .await?;
        Ok(committed_from_krafka(group_id, listed))
    }

    async fn low_watermarks(
        &self,
        topics: &HashMap<String, Vec<i32>>,
    ) -> Result<OffsetLookup, KafkaError> {
        self.list_offsets(topics, OffsetSpec::Earliest).await
    }

    async fn high_watermarks(
        &self,
        topics: &HashMap<String, Vec<i32>>,
    ) -> Result<OffsetLookup, KafkaError> {
        self.list_offsets(topics, OffsetSpec::Latest).await
    }

    async fn offsets_for_times(
        &self,
        topic: &str,
        partitions: &[i32],
        timestamp: i64,
    ) -> Result<HashMap<i32, Option<i64>>, KafkaError> {
        if partitions.is_empty() || validate_topic_name(topic).is_err() {
            return Ok(HashMap::new());
        }

        let specs = partitions.iter().map(|&partition| {
            (
                TopicPartition::new(topic, partition),
                OffsetSpec::Timestamp(timestamp),
            )
        });
        let listed = self
            .transport()
            .await?
            .admin
            .list_offsets(specs, ListOffsetsOptions::default())
            .await?;
        time_offsets(listed.into_iter().map(listed_offset_parts))
    }

    async fn topic_configs(
        &self,
        topics: &[&str],
    ) -> Result<HashMap<String, Vec<ConfigEntry>>, KafkaError> {
        if topics.is_empty() {
            return Ok(HashMap::new());
        }

        let described = self
            .transport()
            .await?
            .admin
            .describe_configs(
                topics.iter().map(|topic| ConfigResource::topic(*topic)),
                DescribeConfigsOptions::default(),
            )
            .await?;

        described
            .into_iter()
            .filter_map(|(resource, configs)| match configs {
                Ok(configs) => Some(Ok((
                    resource.name,
                    configs.into_iter().map(ConfigEntry::from).collect(),
                ))),
                // A broker that refuses one topic, such as for a missing
                // DescribeConfigs ACL, leaves that topic unknown rather than
                // failing every other topic's configs.
                Err(KrafkaError::Broker { .. }) => None,
                Err(error) => Some(Err(error.into())),
            })
            .collect()
    }

    async fn broker_configs(&self, broker_id: i32) -> Result<Vec<ConfigEntry>, KafkaError> {
        let described = self
            .transport()
            .await?
            .admin
            .describe_configs(
                [ConfigResource::broker(broker_id)],
                DescribeConfigsOptions::default(),
            )
            .await?;

        match described.into_values().next() {
            Some(Ok(configs)) => Ok(configs.into_iter().map(ConfigEntry::from).collect()),
            Some(Err(KrafkaError::Broker { message, .. })) => Err(KafkaError::BrokerConfigs {
                id: broker_id,
                message,
            }),
            Some(Err(error)) => Err(error.into()),
            None => Ok(Vec::new()),
        }
    }

    async fn log_dirs(&self) -> Result<Vec<LogDir>, KafkaError> {
        let transport = self.transport().await?;
        let cached = transport.metadata.fetch_metadata(None).await?;
        let brokers = cached.brokers.iter().map(|broker| broker.id());
        let described = transport
            .admin
            .describe_log_dirs(brokers, DescribeLogDirsOptions::default())
            .await?;

        let mut dirs = Vec::new();
        for (broker, broker_dirs) in described {
            dirs.extend(
                broker_dirs?
                    .into_iter()
                    .map(|(path, dir)| LogDir::from_krafka(broker, path, dir)),
            );
        }
        Ok(dirs)
    }

    async fn open_scan(
        &self,
        topic: &str,
        windows: &[PartitionWindow],
    ) -> Result<Box<dyn ScanConsumer>, KafkaError> {
        Ok(Box::new(
            self.link().await?.scans.acquire(topic, windows).await?,
        ))
    }

    async fn open_tail(
        &self,
        topic: &str,
        start: &[TailPosition],
    ) -> Result<Box<dyn TailConsumer>, KafkaError> {
        Ok(Box::new(
            TailLease::open(&self.connector, topic, start, self.tail_reader).await?,
        ))
    }

    fn payload_codec(&self) -> Option<Arc<dyn PayloadCodec>> {
        self.schema_registry
            .clone()
            .map(|decoder| decoder as Arc<dyn PayloadCodec>)
    }

    fn obfuscation(&self) -> Option<Arc<ObfuscationPolicy>> {
        self.obfuscation.clone()
    }

    fn has_schema_registry(&self) -> bool {
        self.schema_registry.is_some()
    }

    async fn schema_subjects(
        &self,
        known: &dyn KnownSchemas,
    ) -> Result<Vec<SchemaSubject>, KafkaError> {
        let Some(decoder) = &self.schema_registry else {
            return Ok(Vec::new());
        };
        decoder.client().subjects(known).await
    }

    async fn subject_schema(
        &self,
        subject: &str,
        version: i32,
    ) -> Result<RegisteredSchema, KafkaError> {
        let Some(decoder) = &self.schema_registry else {
            return Err(KafkaError::UnknownSubject {
                cluster: self.identity.name.clone(),
                subject: subject.to_owned(),
                version,
            });
        };
        decoder
            .client()
            .schema_by_subject_version(subject, version)
            .await
    }

    async fn schema_version_ids(
        &self,
        versions: &[(Arc<str>, i32)],
    ) -> Result<Vec<(Arc<str>, RegisteredVersion)>, KafkaError> {
        let Some(decoder) = &self.schema_registry else {
            return Ok(Vec::new());
        };
        Ok(decoder.client().version_ids(versions).await)
    }

    async fn acls(&self) -> Result<AclListing, KafkaError> {
        AclListing::from_admin_result(
            &self.identity.name,
            self.transport()
                .await?
                .admin
                .describe_acls(AclFilter::all(), DescribeAclsOptions::default())
                .await,
        )
    }

    async fn client_quotas(&self) -> Result<QuotaListing, KafkaError> {
        let described = self
            .transport()
            .await?
            .admin
            .describe_client_quotas(
                ClientQuotaFilter::all(),
                DescribeClientQuotasOptions::default(),
            )
            .await;
        let Some(described) = unless_denied(described)? else {
            return Ok(QuotaListing::Denied);
        };

        Ok(QuotaListing::from_describe(
            &self.identity.name,
            described
                .into_iter()
                .map(|(entity, values)| DescribedQuota {
                    entity: entity.0.into_iter().collect(),
                    values: values.into_iter().collect(),
                }),
        ))
    }

    async fn scram_users(&self) -> Result<ScramListing, KafkaError> {
        let described = self
            .transport()
            .await?
            .admin
            .describe_user_scram_credentials(None, DescribeUserScramCredentialsOptions::default())
            .await;
        let Some(described) = unless_denied(described)? else {
            return Ok(ScramListing::Denied);
        };

        let mut users = Vec::with_capacity(described.len());
        for (name, infos) in described {
            users.push(DescribedUser {
                name,
                credentials: infos?
                    .into_iter()
                    .map(|info| (info.mechanism, info.iterations))
                    .collect(),
            });
        }
        Ok(ScramListing::from_describe(users))
    }

    async fn create_topic(&self, topic: &NewTopic) -> Result<(), KafkaError> {
        self.transport()
            .await?
            .admin
            .create_topics([topic.to_krafka()?], CreateTopicsOptions::default())
            .await?
            .into_values()
            .try_for_each(refused)
    }

    async fn delete_topic(&self, topic: &str) -> Result<(), KafkaError> {
        self.transport()
            .await?
            .admin
            .delete_topics([topic], DeleteTopicsOptions::default())
            .await?
            .into_values()
            .try_for_each(refused)
    }

    async fn alter_topic_configs(&self, topic: &str, edit: &ConfigEdit) -> Result<(), KafkaError> {
        self.alter_configs(ConfigResource::topic(topic), edit).await
    }

    async fn alter_broker_configs(
        &self,
        scope: BrokerScope,
        edit: &ConfigEdit,
    ) -> Result<(), KafkaError> {
        self.alter_configs(scope.to_krafka(), edit).await
    }

    async fn add_partitions(&self, topic: &str, total: NonZeroU16) -> Result<(), KafkaError> {
        self.transport()
            .await?
            .admin
            .create_partitions(
                [(topic, i32::from(total.get()))],
                CreatePartitionsOptions::default(),
            )
            .await?
            .into_values()
            .try_for_each(refused)
    }

    async fn delete_records(
        &self,
        deletion: &RecordDeletion,
    ) -> Result<BTreeMap<i32, i64>, KafkaError> {
        self.transport()
            .await?
            .admin
            .delete_records(deletion.to_krafka(), DeleteRecordsOptions::default())
            .await?
            .into_iter()
            .map(|(partition, low_watermark)| Ok((partition.partition, refused(low_watermark)?)))
            .collect()
    }

    async fn encode_payload(&self, schema_id: i32, json: &str) -> Result<Bytes, KafkaError> {
        self.registry()?.encode(schema_id, json).await
    }

    async fn produce(&self, record: &NewRecord) -> Result<ProducedRecord, KafkaError> {
        let transport = self.transport().await?;
        let producer = self
            .producer
            .get_or_try_init(|| transport.producer())
            .await?;
        let sent = producer
            .send(record.to_krafka())
            .await
            .map_err(produce_refusal)?;
        Ok(ProducedRecord {
            partition: sent.partition,
            offset: sent.offset,
        })
    }

    async fn alter_group_offsets(
        &self,
        group: &str,
        offsets: &[CommittedOffset],
    ) -> Result<(), KafkaError> {
        let offsets = offsets.iter().map(|offset| {
            (
                TopicPartition::new(offset.topic.as_str(), offset.partition),
                offset.offset,
            )
        });
        self.transport()
            .await?
            .admin
            .alter_consumer_group_offsets(
                group,
                offsets,
                AlterConsumerGroupOffsetsOptions::default(),
            )
            .await?
            .into_values()
            .try_for_each(refused)
    }

    async fn delete_group(&self, group: &str) -> Result<(), KafkaError> {
        self.transport()
            .await?
            .admin
            .delete_consumer_groups([group], DeleteConsumerGroupsOptions::default())
            .await?
            .into_values()
            .try_for_each(refused)
    }

    async fn delete_group_offsets(
        &self,
        group: &str,
        topic: &str,
        partitions: &[i32],
    ) -> Result<(), KafkaError> {
        let partitions = partitions
            .iter()
            .map(|&partition| TopicPartition::new(topic, partition));
        self.transport()
            .await?
            .admin
            .delete_consumer_group_offsets(
                group,
                partitions,
                DeleteConsumerGroupOffsetsOptions::default(),
            )
            .await?
            .into_values()
            .try_for_each(refused)
    }

    async fn register_schema(&self, schema: &NewSchema) -> Result<RegisteredVersion, KafkaError> {
        self.registry()?.register(schema).await
    }

    async fn delete_schema(&self, deletion: &SchemaDeletion) -> Result<(), KafkaError> {
        self.registry()?.delete(deletion).await
    }

    async fn set_compatibility(
        &self,
        subject: &str,
        level: SchemaCompatibility,
    ) -> Result<(), KafkaError> {
        self.registry()?.set_compatibility(subject, level).await
    }

    async fn create_acls(&self, acls: &[Acl]) -> Result<(), KafkaError> {
        let bindings = acls
            .iter()
            .map(Acl::to_krafka)
            .collect::<Result<Vec<_>, _>>()?;
        self.transport()
            .await?
            .admin
            .create_acls(bindings, CreateAclsOptions::default())
            .await?
            .into_iter()
            .try_for_each(|(_, created)| refused(created))
    }

    async fn delete_acl(&self, acl: &Acl) -> Result<(), KafkaError> {
        let deleted = self
            .transport()
            .await?
            .admin
            .delete_acls(vec![acl.to_krafka_filter()?], DeleteAclsOptions::default())
            .await?;
        for (_, deleted) in deleted {
            if let Some((_, error)) = refused(deleted)?.failed.into_iter().next() {
                return refused(Err(error));
            }
        }
        Ok(())
    }

    async fn alter_client_quota(&self, quota: &ClientQuota) -> Result<(), KafkaError> {
        let alteration = ClientQuotaAlteration::new(
            quota
                .entity
                .iter()
                .map(|part| (part.entity_type.wire(), part.name.as_deref()))
                .collect(),
            quota
                .values
                .entries()
                .into_iter()
                .map(|(key, value)| (key.to_owned(), value))
                .collect(),
        );
        self.transport()
            .await?
            .admin
            .alter_client_quotas(vec![alteration], AlterClientQuotasOptions::default())
            .await?
            .into_values()
            .try_for_each(refused)
    }

    async fn set_scram_credential(
        &self,
        credential: &NewScramCredential,
    ) -> Result<(), KafkaError> {
        let credential = credential.clone();
        let upsertion = tokio::task::spawn_blocking(move || credential.upsertion())
            .await
            .map_err(|error| KafkaError::Admin(error.to_string()))?;
        self.transport()
            .await?
            .admin
            .alter_user_scram_credentials(
                Vec::new(),
                vec![upsertion],
                AlterUserScramCredentialsOptions::default(),
            )
            .await?
            .into_values()
            .try_for_each(refused)
    }

    async fn delete_scram_credential(
        &self,
        user: &str,
        mechanism: ScramMechanism,
    ) -> Result<(), KafkaError> {
        let deletion = ScramCredentialDeletion {
            name: user.to_owned(),
            mechanism: mechanism.wire(),
        };
        self.transport()
            .await?
            .admin
            .alter_user_scram_credentials(
                vec![deletion],
                Vec::new(),
                AlterUserScramCredentialsOptions::default(),
            )
            .await?
            .into_values()
            .try_for_each(refused)
    }
}

impl KafkaClient {
    fn registry(&self) -> Result<&SchemaRegistryClient, KafkaError> {
        self.schema_registry
            .as_ref()
            .map(|decoder| decoder.client())
            .ok_or_else(|| KafkaError::NoSchemaRegistry(self.identity.name.clone()))
    }

    async fn alter_configs(
        &self,
        resource: ConfigResource,
        edit: &ConfigEdit,
    ) -> Result<(), KafkaError> {
        self.transport()
            .await?
            .admin
            .incremental_alter_configs(
                [(resource, edit.to_krafka())],
                IncrementalAlterConfigsOptions::default(),
            )
            .await?
            .into_values()
            .try_for_each(refused)
    }
}

fn owned(values: &[&str]) -> Vec<String> {
    values.iter().map(|&value| value.to_owned()).collect()
}

fn group_ids(
    listed: std::collections::HashMap<i32, Result<Vec<ConsumerGroupListing>, KrafkaError>>,
) -> Result<Vec<String>, KafkaError> {
    let mut ids = Vec::new();
    for groups in listed.into_values() {
        ids.extend(groups?.into_iter().map(|group| group.group_id));
    }
    Ok(ids)
}

fn offset_specs<Topic: AsRef<str>>(
    topics: &HashMap<Topic, Vec<i32>>,
    spec: OffsetSpec,
) -> Vec<(TopicPartition, OffsetSpec)> {
    topics
        .iter()
        .filter(|(topic, _)| validate_topic_name(topic.as_ref()).is_ok())
        .flat_map(|(topic, partitions)| {
            partitions
                .iter()
                .map(move |&partition| (TopicPartition::new(topic.as_ref(), partition), spec))
        })
        .collect()
}

fn listed_offset_parts(
    (partition, listed): (TopicPartition, Result<ListedOffset, KrafkaError>),
) -> (String, i32, Result<i64, KrafkaError>) {
    let offset = listed.map(|listed| listed.offset);
    (partition.topic, partition.partition, offset)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::num::NonZeroU16;

    use krafka::error::ErrorCode;
    use krafka::testing::ApiKey;
    use krafka::testing::Control;

    use super::testing::Broker;
    use super::*;
    use crate::kafka::error::Refusal;
    use crate::kafka::group::MemberAssignment;
    use crate::kafka::metadata::Watermarks;
    use crate::kafka::model::{
        PartitionWindow, Record, RecordOrder, RecordQuery, SchemaType, TimestampRange,
    };
    use crate::kafka::registry::testing::FakeRegistry;
    use crate::kafka::scan::plan::LogSpan;
    use crate::kafka::scan::session::{PageRequest, scan_once};
    use crate::kafka::session::watermarks;
    use crate::kafka::store::SubjectTable;
    use crate::testing::LogCapture;

    fn window(partition: i32, start: i64, end: i64) -> PartitionWindow {
        PartitionWindow {
            partition,
            start,
            end,
        }
    }

    fn wanted(topic: &str, partitions: &[i32]) -> HashMap<String, Vec<i32>> {
        HashMap::from_iter([(topic.to_owned(), partitions.to_vec())])
    }

    fn offsets(records: &[Record]) -> Vec<i64> {
        records.iter().map(|record| record.offset).collect()
    }

    fn committed(topic: &str, partition: i32, offset: i64) -> CommittedOffset {
        CommittedOffset {
            topic: topic.to_owned(),
            partition,
            offset,
        }
    }

    async fn commit(client: &KafkaClient, group: &str, topic: &str, offset: i64) {
        client
            .alter_group_offsets(group, &[committed(topic, 0, offset)])
            .await
            .expect("commit");
    }

    async fn close(client: KafkaClient) {
        let transport = client.transport().await.expect("kafka link");
        transport.admin.close().await.expect("close admin");
    }

    #[tokio::test]
    async fn a_cluster_that_was_down_is_read_once_it_answers() {
        let broker = Broker::start().await;
        broker.crash(0);
        let client = KafkaClient::new("test", &broker.config(), &Tuning::default()).unwrap();
        client.metadata().await.expect_err("no broker answers");

        broker.restart(0);

        client.metadata().await.expect("a later call connects");
    }

    #[tokio::test]
    async fn connecting_does_not_warn_about_the_connection_memory_ceiling() {
        let logs = LogCapture::at(tracing::Level::WARN);

        let broker = Broker::start().await;
        broker.client().await.metadata().await.expect("metadata");

        logs.assert_lacks("memory ceiling");
    }

    #[tokio::test]
    async fn admin_calls_do_not_warn_about_missing_close() {
        let logs = LogCapture::at(tracing::Level::WARN);

        let broker = Broker::orders(1).await;
        let client = broker.client().await;
        watermarks(&client, &wanted("orders", &[0]))
            .await
            .expect("watermarks");
        watermarks(&client, &wanted("orders", &[0]))
            .await
            .expect("second watermarks");

        logs.assert_lacks("AdminClient dropped without close");
    }

    #[tokio::test]
    async fn empty_partitions_skip_kafka() {
        let broker = Broker::start().await;
        let client = broker.client().await;
        broker.clear_requests();

        let offsets = client.committed_offsets("unused", Some(&[])).await.unwrap();
        assert!(offsets.is_empty());
        assert!(
            client
                .low_watermarks(&HashMap::new())
                .await
                .unwrap()
                .found()
                .is_empty()
        );
        assert!(
            client
                .high_watermarks(&HashMap::new())
                .await
                .unwrap()
                .found()
                .is_empty()
        );
        assert!(
            client
                .offsets_for_times("orders", &[], 0)
                .await
                .unwrap()
                .is_empty()
        );
        assert!(broker.requests().is_empty());
    }

    #[tokio::test]
    async fn broker_io_is_bounded_by_the_configured_request_timeout() {
        let broker = Broker::start().await;
        let mut tuning = Tuning::default();
        // Above krafka's 100 ms backoff between metadata fetches, so the
        // fetch after the one at connect is sent before the deadline.
        tuning.kafka.request_timeout = Duration::from_millis(500);
        tuning.kafka.connect_timeout = Duration::from_millis(500);
        let client = broker.client_with(&tuning).await;
        broker.clear_requests();
        broker.on(ApiKey::Metadata, |_| Control::Silence);

        let error = tokio::time::timeout(Duration::from_secs(2), client.metadata())
            .await
            .expect("krafka must bound the request without an adapter timeout")
            .unwrap_err();

        assert!(matches!(error, KafkaError::Krafka(_)), "{error:?}");
        assert!(broker.request_count(ApiKey::Metadata) > 0, "{error:?}");
        close(client).await;
    }

    #[tokio::test]
    async fn a_broker_that_does_not_serve_an_admin_api_fails_that_call() {
        let broker = Broker::start().await;
        let client = broker.client().await;
        client.metadata().await.expect("metadata");

        for (api, error) in [
            ("ListGroups", client.groups().await.err()),
            (
                "DescribeConfigs",
                client.topic_configs(&["orders"]).await.err(),
            ),
            ("DescribeLogDirs", client.log_dirs().await.err()),
            ("DescribeClientQuotas", client.client_quotas().await.err()),
            (
                "DescribeUserScramCredentials",
                client.scram_users().await.err(),
            ),
        ] {
            assert!(
                matches!(error, Some(KafkaError::Krafka(_))),
                "{api}: {error:?}"
            );
        }
    }

    #[tokio::test]
    async fn metadata_reads_topics_and_partitions_from_broker() {
        let broker = Broker::start().await;
        broker.topic("orders", 2);

        let meta = broker.client().await.metadata().await.expect("metadata");

        let topic = meta.topic("orders").expect("orders topic");
        assert_eq!(
            topic
                .partitions
                .iter()
                .map(|partition| partition.id)
                .collect::<Vec<_>>(),
            vec![0, 1]
        );
        assert!(!topic.internal);
    }

    #[tokio::test]
    async fn metadata_reads_the_rack_each_broker_advertises() {
        let broker = Broker::start().await;
        broker.set_broker_rack(0, Some("eu-west-1a"));

        let meta = broker.client().await.metadata().await.expect("metadata");

        let racks: Vec<_> = meta
            .brokers
            .iter()
            .map(|meta| (meta.id, meta.rack.as_deref()))
            .collect();
        assert_eq!(racks, [(0, Some("eu-west-1a"))]);
    }

    #[tokio::test]
    async fn metadata_costs_kafka_a_single_metadata_request() {
        let broker = Broker::start().await;
        // Unadvertised, krafka would refuse DescribeCluster before sending it,
        // and the request log could not show a poll that asked for it.
        broker.set_api_versions(ApiKey::DescribeCluster, 0, 1);
        let client = broker.client().await;
        broker.clear_requests();

        client.metadata().await.expect("metadata");

        let apis: Vec<ApiKey> = broker
            .requests()
            .iter()
            .map(|request| request.api_key)
            .collect();
        assert_eq!(apis, [ApiKey::Metadata]);
    }

    #[tokio::test]
    async fn a_topic_name_kafka_would_reject_has_no_time_offsets() {
        let broker = Broker::start().await;
        let client = broker.client().await;

        let offsets = client
            .offsets_for_times("orders/eu", &[0], 0)
            .await
            .expect("time offsets");

        assert!(offsets.is_empty());
    }

    #[tokio::test]
    async fn client_reads_watermarks_and_time_offsets() {
        let broker = Broker::orders(1).await;
        let client = broker.client().await;

        let marks = watermarks(&client, &wanted("orders", &[0]))
            .await
            .expect("watermarks");
        assert_eq!(marks["orders"][&0], Watermarks { low: 0, high: 1 });

        let offsets = client
            .offsets_for_times("orders", &[0], 0)
            .await
            .expect("time offsets");
        assert_eq!(offsets.get(&0), Some(&Some(0)));
    }

    #[tokio::test]
    async fn a_time_past_the_last_record_has_no_offset() {
        let broker = Broker::orders(1).await;
        let mut tuning = Tuning::default();
        // krafka retries its own "no offset" answer until the deadline.
        tuning.kafka.request_timeout = Duration::from_millis(500);
        tuning.kafka.connect_timeout = Duration::from_millis(500);
        let client = broker.client_with(&tuning).await;

        let offsets = client
            .offsets_for_times("orders", &[0], i64::MAX / 2)
            .await
            .expect("time offsets");

        assert_eq!(offsets, HashMap::from_iter([(0, None)]));
    }

    #[tokio::test]
    async fn low_and_high_watermarks_list_one_end_each() {
        let broker = Broker::start().await;
        broker.topic("orders", 2);
        broker.with_state(|state| {
            for (id, low, high) in [(0, 1, 5), (1, 0, 2)] {
                let partition = state.partition_mut("orders", id).expect("partition");
                partition.log_start_offset = low;
                partition.next_offset = high;
            }
        });
        let client = broker.client().await;
        client.metadata().await.expect("metadata");
        let both = wanted("orders", &[0, 1]);

        broker.clear_requests();
        let lows = client
            .low_watermarks(&both)
            .await
            .expect("low watermarks")
            .found();
        assert_eq!(broker.request_count(ApiKey::ListOffsets), 1);

        broker.clear_requests();
        let highs = client
            .high_watermarks(&both)
            .await
            .expect("high watermarks")
            .found();
        assert_eq!(broker.request_count(ApiKey::ListOffsets), 1);
        assert_eq!(lows["orders"], HashMap::from_iter([(0, 1), (1, 0)]));
        assert_eq!(highs["orders"], HashMap::from_iter([(0, 5), (1, 2)]));
    }

    #[tokio::test]
    async fn an_illegal_topic_name_does_not_fail_the_other_watermarks() {
        let broker = Broker::orders(1).await;
        let client = broker.client().await;
        let mut topics = wanted("orders", &[0]);
        topics.insert(String::new(), vec![0]);

        let marks = watermarks(&client, &topics).await.expect("watermarks");
        assert_eq!(marks["orders"][&0], Watermarks { low: 0, high: 1 });
        assert!(!marks.contains_key(""));

        broker.clear_requests();
        assert!(
            client
                .low_watermarks(&wanted("", &[0]))
                .await
                .unwrap()
                .found()
                .is_empty()
        );
        assert!(
            client
                .high_watermarks(&wanted("", &[0]))
                .await
                .unwrap()
                .found()
                .is_empty()
        );
        assert!(
            client
                .committed_offsets("unused", Some(&[("", 0)]))
                .await
                .unwrap()
                .is_empty()
        );
        assert!(broker.requests().is_empty());
    }

    #[tokio::test]
    async fn client_reads_records() {
        let broker = Broker::orders(1).await;
        let client = broker.client().await;

        let records = scan_once(
            &client,
            "orders",
            &[window(0, 0, 1)],
            10,
            RecordOrder::Oldest,
        )
        .await
        .expect("records");
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].offset, 0);
        assert_eq!(records[0].value.as_deref(), Some("hello"));
        assert_eq!(records[0].key.as_deref(), Some("k"));

        let past_high = scan_once(
            &client,
            "orders",
            &[window(0, 5, 10)],
            10,
            RecordOrder::Oldest,
        )
        .await
        .expect("empty past high watermark");
        assert!(past_high.is_empty());
    }

    #[tokio::test]
    async fn consecutive_scans_reuse_a_consumer_without_looking_offsets_up() {
        let broker = Broker::orders(4).await;
        let client = broker.client().await;
        let pass = async |start: i64, end: i64| {
            scan_once(
                &client,
                "orders",
                &[window(0, start, end)],
                10,
                RecordOrder::Oldest,
            )
            .await
        };

        let first = pass(2, 4).await.expect("first pass");
        let second = pass(0, 2).await.expect("second pass");

        assert_eq!(offsets(&first), [2, 3]);
        assert_eq!(offsets(&second), [0, 1]);
        assert_eq!(
            broker.request_count(ApiKey::ListOffsets),
            0,
            "a window start is known before the consumer exists"
        );
        assert_eq!(broker.request_count(ApiKey::JoinGroup), 0);

        let (first, second) =
            tokio::try_join!(pass(0, 2), pass(2, 4)).expect("independent concurrent scans");
        assert_eq!(offsets(&first), [0, 1]);
        assert_eq!(offsets(&second), [2, 3]);
    }

    #[tokio::test]
    async fn scan_drains_prefetched_records_before_completing() {
        let broker = Broker::orders(550).await;
        let client = broker.client().await;

        let records = scan_once(
            &client,
            "orders",
            &[window(0, 0, 550)],
            550,
            RecordOrder::Oldest,
        )
        .await
        .unwrap();

        assert_eq!(offsets(&records), (0..550).collect::<Vec<_>>());
    }

    #[tokio::test]
    async fn a_round_trip_slower_than_the_poll_max_wait_still_delivers() {
        let broker = Broker::orders(2).await;
        let client = broker.client().await;
        let slow = client.scan_poll_wait * 3;
        broker.on(ApiKey::Fetch, move |_| Control::Delay(slow));

        let records = scan_once(
            &client,
            "orders",
            &[window(0, 0, 2)],
            2,
            RecordOrder::Oldest,
        )
        .await
        .expect("a slow round trip is not a timeout");

        assert_eq!(offsets(&records), [0, 1]);
    }

    #[tokio::test]
    async fn record_deadline_bounds_broker_io() {
        let broker = Broker::orders(1).await;
        let mut client = broker.client().await;
        broker.on(ApiKey::Fetch, |_| Control::Silence);
        client.consume_timeout = Duration::from_millis(50);

        let error = tokio::time::timeout(
            Duration::from_secs(2),
            scan_once(
                &client,
                "orders",
                &[window(0, 0, 1)],
                1,
                RecordOrder::Oldest,
            ),
        )
        .await
        .expect("broker I/O must honor the scan deadline")
        .unwrap_err();

        assert!(matches!(error, KafkaError::Timeout));
    }

    #[tokio::test]
    async fn altered_group_offsets_read_back_per_topic() {
        let broker = Broker::start().await;
        broker.topic("orders", 2);
        broker.topic("payments", 1);
        let client = broker.client().await;
        let offsets = [
            committed("orders", 0, 4),
            committed("orders", 1, 7),
            committed("payments", 0, 2),
        ];

        client
            .alter_group_offsets("billing", &offsets)
            .await
            .expect("offset commit");

        let mut read = client
            .committed_offsets("billing", None)
            .await
            .expect("offset fetch");
        read.sort_by(|left, right| {
            (&left.topic, left.partition).cmp(&(&right.topic, right.partition))
        });
        assert_eq!(read, offsets);
    }

    #[tokio::test]
    async fn a_refused_offset_commit_carries_the_broker_error() {
        let broker = Broker::orders(1).await;
        let client = broker.client().await;
        broker.on(ApiKey::OffsetCommit, |_| {
            Control::Error(ErrorCode::UnknownMemberId)
        });

        let error = client
            .alter_group_offsets("billing", &[committed("orders", 0, 1)])
            .await
            .unwrap_err();

        assert!(
            matches!(
                &error,
                KafkaError::Refused(Refusal {
                    code: ErrorCode::UnknownMemberId,
                    ..
                })
            ),
            "{error}"
        );
    }

    #[tokio::test]
    async fn committed_offsets_come_back_from_a_broker_that_offers_offset_fetch_v10() {
        let broker = Broker::orders(1).await;
        // Kafka 4 offers v10, which names topics by id instead of by name.
        broker.set_api_versions(ApiKey::OffsetFetch, 5, 10);
        broker.set_api_versions(ApiKey::OffsetCommit, 7, 10);
        let client = broker.client().await;
        commit(&client, "orders-group", "orders", 1).await;

        for partitions in [None, Some(&[("orders", 0)][..])] {
            let fetched = client
                .committed_offsets("orders-group", partitions)
                .await
                .expect("offset fetch");

            assert_eq!(fetched, vec![committed("orders", 0, 1)], "{partitions:?}");
        }
    }

    #[tokio::test]
    async fn lists_committed_offsets_with_and_without_a_partition_filter() {
        let broker = Broker::orders(1).await;
        let client = broker.client().await;
        commit(&client, "orders-group", "orders", 1).await;
        let committed = vec![committed("orders", 0, 1)];

        let filtered = client
            .committed_offsets("orders-group", Some(&[("orders", 0)]))
            .await
            .expect("offset fetch");
        let every = client
            .committed_offsets("orders-group", None)
            .await
            .expect("offset fetch");

        assert_eq!(filtered, committed);
        assert_eq!(every, committed);
    }

    #[tokio::test]
    async fn describes_classic_group_member_assignments() {
        let broker = Broker::start().await;
        broker.topic("orders", 2);
        let consumer = broker
            .kafka()
            .request_timeout(Duration::from_secs(2))
            .connect_timeout(Duration::from_secs(2))
            .connect()
            .await
            .expect("kafka")
            .consumer("orders-group")
            .build()
            .await
            .expect("consumer");
        consumer.subscribe(["orders"]).await.expect("subscribe");
        let consumer = Arc::new(consumer);
        let polling = tokio::spawn({
            let consumer = Arc::clone(&consumer);
            async move {
                loop {
                    let _ = consumer.poll(Duration::from_millis(100)).await;
                }
            }
        });
        assert!(
            broker
                .wait_for_requests(ApiKey::SyncGroup, 1, Duration::from_secs(15))
                .await,
            "the consumer must finish join and sync before describe"
        );

        let described = broker
            .client()
            .await
            .transport()
            .await
            .expect("kafka link")
            .admin
            .describe_consumer_groups(["orders-group"], DescribeConsumerGroupsOptions::default())
            .await
            .expect("describe group");
        let group = snapshots_from_descriptions(described)
            .into_iter()
            .find(|group| group.id == "orders-group")
            .expect("orders-group");

        assert_eq!(
            group.members[0].assignments,
            vec![MemberAssignment {
                topic: "orders".into(),
                partitions: vec![0, 1],
            }]
        );
        polling.abort();
        consumer.close().await.expect("close consumer");
    }

    #[tokio::test]
    async fn eight_groups_share_one_client() {
        let broker = Broker::orders(1).await;
        let client = Arc::new(broker.client().await);
        for index in 0..8 {
            commit(&client, &format!("g{index}"), "orders", 1).await;
        }

        let fetches = (0..8).map(|index| {
            let client = Arc::clone(&client);
            async move {
                client
                    .committed_offsets(&format!("g{index}"), Some(&[("orders", 0)]))
                    .await
            }
        });
        let results = futures::future::join_all(fetches).await;

        for result in results {
            assert_eq!(result.expect("offset fetch")[0].offset, 1);
        }
    }

    #[tokio::test]
    async fn create_topic_creates_it_once() {
        let broker = Broker::start().await;
        let client = broker.client().await;
        let topic = NewTopic {
            name: "orders".into(),
            partitions: NonZeroU16::new(3),
            replication_factor: None,
            configs: BTreeMap::new(),
        };

        client.create_topic(&topic).await.expect("created");
        let created = client.topic_metadata("orders").await.unwrap();
        assert_eq!(created.partitions.len(), 3);

        let error = client.create_topic(&topic).await.unwrap_err();
        assert!(
            matches!(
                &error,
                KafkaError::Refused(Refusal {
                    code: ErrorCode::TopicAlreadyExists,
                    ..
                })
            ),
            "{error}"
        );
    }

    #[tokio::test]
    async fn delete_topic_deletes_it_once() {
        let broker = Broker::start().await;
        broker.topic("orders", 2);
        let client = broker.client().await;

        client.delete_topic("orders").await.expect("deleted");
        assert!(!broker.with_state(|state| state.topics.contains_key("orders")));

        let error = client.delete_topic("orders").await.unwrap_err();
        assert!(matches!(error, KafkaError::Refused(_)), "{error}");
    }

    #[tokio::test]
    async fn produce_reports_where_kafka_stored_each_record() {
        let broker = Broker::start().await;
        broker.topic("orders", 2);
        let client = broker.client().await;
        let record = NewRecord {
            topic: "orders".to_owned(),
            partition: Some(1),
            key: None,
            value: Some(bytes::Bytes::from_static(b"hello")),
            headers: Vec::new(),
        };

        let first = client.produce(&record).await.expect("first");
        let second = client.produce(&record).await.expect("second");

        assert_eq!(
            first,
            ProducedRecord {
                partition: 1,
                offset: 0
            }
        );
        assert_eq!(
            second,
            ProducedRecord {
                partition: 1,
                offset: 1
            }
        );
    }

    #[tokio::test]
    async fn a_produce_retried_after_its_append_is_stored_once() {
        let broker = Broker::start().await;
        broker.topic("orders", 1);
        let client = broker.client().await;
        let logs = LogCapture::at(tracing::Level::WARN);
        broker.on_once(ApiKey::Produce, |_| {
            Control::ApplyThen(Box::new(Control::Error(
                ErrorCode::NotEnoughReplicasAfterAppend,
            )))
        });
        let record = NewRecord {
            topic: "orders".to_owned(),
            partition: Some(0),
            key: None,
            value: None,
            headers: Vec::new(),
        };

        client.produce(&record).await.expect("retried produce");

        assert_eq!(broker.request_count(ApiKey::Produce), 2);
        assert_eq!(broker.all_records("orders").expect("records").len(), 1);
        logs.assert_lacks("delivery_timeout is shorter");
    }

    #[tokio::test]
    async fn a_finished_partition_read_ahead_does_not_starve_the_others() {
        let broker = Broker::start().await;
        broker.topic("orders", 2);
        broker.produce_to_each("orders", 2, 2_000).await;
        let client = broker.client().await;
        let watermarks: HashMap<i32, Watermarks> = (0..2)
            .map(|partition| {
                (
                    partition,
                    Watermarks {
                        low: 0,
                        high: 2_000,
                    },
                )
            })
            .collect();
        let query = RecordQuery {
            topic: "orders".into(),
            partitions: vec![0, 1],
            filter: None,
            timestamps: TimestampRange::UNBOUNDED,
            limit: 50,
            order: RecordOrder::Oldest,
            cursor: None,
            schema_id: None,
        };

        let request = PageRequest::new(query, Tuning::default().records).expect("request");
        let span = LogSpan::new(vec![0, 1], watermarks);

        let started = std::time::Instant::now();
        let page = request.fetch(&client, &span).await.expect("page");

        assert!(
            page.complete,
            "a full buffer of a paused partition must not stop the others being fetched"
        );
        assert!(started.elapsed() < client.consume_timeout / 2);
        assert_eq!(page.records.len(), 50);
    }

    fn order_schema() -> NewSchema {
        NewSchema {
            subject: "orders-value".to_owned(),
            schema_type: SchemaType::Avro,
            schema: r#""string""#.to_owned(),
            references: Vec::new(),
        }
    }

    #[tokio::test]
    async fn a_schema_registers_through_the_clusters_registry() {
        let broker = Broker::start().await;
        let registry = FakeRegistry::start().await;
        let client = broker.client_with_registry(&registry).await;

        assert!(client.has_schema_registry());
        let registered = client.register_schema(&order_schema()).await.unwrap();

        assert_eq!(registered.version, 1);
        let subjects = client
            .schema_subjects(&SubjectTable::default())
            .await
            .unwrap();
        assert_eq!(subjects[0].subject, "orders-value");
    }

    #[tokio::test]
    async fn a_schema_deletes_through_the_clusters_registry() {
        let broker = Broker::start().await;
        let registry = FakeRegistry::start().await;
        let client = broker.client_with_registry(&registry).await;
        client.register_schema(&order_schema()).await.unwrap();

        client
            .delete_schema(&SchemaDeletion {
                subject: "orders-value".to_owned(),
                version: None,
                permanent: false,
            })
            .await
            .unwrap();

        assert!(
            client
                .schema_subjects(&SubjectTable::default())
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn a_compatibility_level_sets_through_the_clusters_registry() {
        let broker = Broker::start().await;
        let registry = FakeRegistry::start().await;
        let client = broker.client_with_registry(&registry).await;
        client.register_schema(&order_schema()).await.unwrap();

        client
            .set_compatibility("orders-value", SchemaCompatibility::FullTransitive)
            .await
            .unwrap();

        let subjects = client
            .schema_subjects(&SubjectTable::default())
            .await
            .unwrap();
        assert_eq!(
            subjects[0].compatibility,
            SchemaCompatibility::FullTransitive
        );
    }

    #[tokio::test]
    async fn a_payload_encodes_with_the_clusters_registry() {
        let broker = Broker::start().await;
        let registry = FakeRegistry::start().await;
        let client = broker.client_with_registry(&registry).await;
        let registered = client.register_schema(&order_schema()).await.unwrap();

        let encoded = client
            .encode_payload(registered.id, r#""abc""#)
            .await
            .unwrap();

        let id = u8::try_from(registered.id).unwrap();
        assert_eq!(encoded, [0, 0, 0, 0, id, 6, b'a', b'b', b'c'].as_slice());
    }

    #[tokio::test]
    async fn version_ids_read_through_the_clusters_registry() {
        let broker = Broker::start().await;
        let registry = FakeRegistry::start().await;
        let client = broker.client_with_registry(&registry).await;
        let registered = client.register_schema(&order_schema()).await.unwrap();

        let ids = client
            .schema_version_ids(&[(Arc::from("orders-value"), 1)])
            .await
            .unwrap();

        assert_eq!(ids.len(), 1);
        assert_eq!(&*ids[0].0, "orders-value");
        assert_eq!(ids[0].1.id, registered.id);
    }

    #[tokio::test]
    async fn a_cluster_without_a_registry_knows_no_version_ids() {
        let client = Broker::start().await.client().await;

        let ids = client
            .schema_version_ids(&[(Arc::from("orders-value"), 1)])
            .await
            .unwrap();

        assert!(ids.is_empty());
    }

    #[tokio::test]
    async fn a_cluster_without_a_registry_changes_no_schema() {
        let client = Broker::start().await.client().await;
        let deletion = SchemaDeletion {
            subject: "orders-value".to_owned(),
            version: None,
            permanent: false,
        };

        assert!(!client.has_schema_registry());
        let errors = [
            client.register_schema(&order_schema()).await.unwrap_err(),
            client.delete_schema(&deletion).await.unwrap_err(),
            client
                .set_compatibility("orders-value", SchemaCompatibility::Full)
                .await
                .unwrap_err(),
            client.encode_payload(1, r#""abc""#).await.unwrap_err(),
        ];

        for error in errors {
            assert!(
                matches!(&error, KafkaError::NoSchemaRegistry(cluster) if cluster == "test"),
                "{error:?}"
            );
        }
    }
}
