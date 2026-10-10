use std::num::NonZeroU16;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::FromRequestParts;
use axum::http::Extensions;
use axum::http::request::Parts;
use bytes::Bytes;
use futures::{FutureExt as _, future};

use crate::AppState;
use crate::app::auth::SessionGuard;
use crate::app::auth::access::{
    AccessError, AddPartitionsCap, AlterBrokerConfigsCap, AlterQuotasCap, AlterTopicConfigsCap,
    BrokerConfigsCap, ClusterAccess, CreateAclsCap, CreateTopicsCap, DeleteAclsCap,
    DeleteGroupsCap, DeleteOffsetsCap, DeleteRecordsCap, DeleteSchemasCap,
    DeleteScramCredentialsCap, DeleteTopicsCap, EffectiveAccess, ProduceCap, RecordsCap,
    RegisterSchemasCap, ResetOffsetsCap, SchemaTextCap, SetCompatibilityCap,
    SetScramCredentialsCap,
};
use crate::kafka::model::{
    Acl, BrokerScope, ClientQuota, CommittedOffset, ConfigEdit, ConfigSource, FoundRecord,
    NewRecord, NewSchema, NewScramCredential, NewTopic, OffsetMove, OffsetReset, ProducedRecord,
    QuotaValues, RecordAt, RecordDeletion, RegisteredSchema, RegisteredVersion,
    SchemaCompatibility, SchemaDeletion, ScramMechanism,
};
use crate::kafka::store::{ClusterStore, GroupInfo, Lane, TopicInfo, reread_until};
use crate::kafka::{
    Cluster, ConfigEntry, Export, KafkaError, RecordPage, RecordQuery, Tail, TailLimits, TailQuery,
};

use super::error::ApiError;

const SETTLE: Duration = Duration::from_secs(10);

pub(crate) struct Session {
    pub state: AppState,
    pub access: EffectiveAccess,
    pub guard: SessionGuard,
}

pub(crate) struct ClusterHandle<'a> {
    pub access: ClusterAccess<'a>,
    pub store: &'a Arc<ClusterStore>,
    cluster: &'a Cluster,
    limits: TailLimits,
}

impl<'a> ClusterHandle<'a> {
    pub(crate) fn name(&self) -> &str {
        self.access.cluster()
    }

    pub(crate) fn has_schema_registry(&self) -> bool {
        self.cluster.session.has_schema_registry()
    }

    pub(crate) fn is_writable(&self) -> bool {
        self.cluster.writable
    }

    pub(crate) fn records(&self) -> Result<Granted<'a, RecordsCap>, AccessError> {
        self.access.records().map(|cap| self.grant(cap))
    }

    pub(crate) fn broker_configs(&self) -> Result<Granted<'a, BrokerConfigsCap>, AccessError> {
        self.access.broker_configs().map(|cap| self.grant(cap))
    }

    pub(crate) fn schema_text(&self) -> Result<Granted<'a, SchemaTextCap>, AccessError> {
        self.access.schema_text().map(|cap| self.grant(cap))
    }

    fn writable(&self) -> Result<(), AccessError> {
        if self.cluster.writable {
            Ok(())
        } else {
            Err(AccessError::ReadOnlyCluster(self.name().to_owned()))
        }
    }

    fn grant<Cap>(&self, cap: Cap) -> Granted<'a, Cap> {
        Granted {
            _cap: cap,
            cluster: self.cluster,
            limits: self.limits,
        }
    }
}

macro_rules! writes {
    ($($method:ident => $token:ident),* $(,)?) => {
        impl<'a> ClusterHandle<'a> {
            $(
                pub(crate) fn $method(&self) -> Result<Granted<'a, $token>, AccessError> {
                    self.writable()?;
                    self.access.$method().map(|cap| self.grant(cap))
                }
            )*
        }
    };
}

writes! {
    create_topics => CreateTopicsCap,
    delete_topics => DeleteTopicsCap,
    alter_topic_configs => AlterTopicConfigsCap,
    add_partitions => AddPartitionsCap,
    delete_records => DeleteRecordsCap,
    produce => ProduceCap,
    reset_offsets => ResetOffsetsCap,
    delete_offsets => DeleteOffsetsCap,
    delete_groups => DeleteGroupsCap,
    register_schemas => RegisterSchemasCap,
    set_compatibility => SetCompatibilityCap,
    delete_schemas => DeleteSchemasCap,
    create_acls => CreateAclsCap,
    delete_acls => DeleteAclsCap,
    alter_quotas => AlterQuotasCap,
    set_scram_credentials => SetScramCredentialsCap,
    delete_scram_credentials => DeleteScramCredentialsCap,
    alter_broker_configs => AlterBrokerConfigsCap,
}

/// A privilege checked on one cluster. The live calls it guards hang off
/// it, so none of them can run without the check.
pub(crate) struct Granted<'a, Cap> {
    _cap: Cap,
    cluster: &'a Cluster,
    limits: TailLimits,
}

impl Granted<'_, RecordsCap> {
    pub(crate) async fn read(&self, query: RecordQuery) -> Result<RecordPage, KafkaError> {
        self.cluster.records(query, self.limits.records).await
    }

    pub(crate) async fn record(&self, at: RecordAt) -> Result<FoundRecord, KafkaError> {
        self.cluster.record(at, self.limits.records).await
    }

    pub(crate) async fn export(&self, query: RecordQuery) -> Result<Export, KafkaError> {
        self.cluster.export(query, self.limits.records).await
    }

    pub(crate) async fn tail(&self, query: TailQuery) -> Result<Tail, KafkaError> {
        self.cluster.tail(query, self.limits).await
    }
}

impl Granted<'_, BrokerConfigsCap> {
    pub(crate) async fn broker_configs(&self, id: i32) -> Result<Vec<ConfigEntry>, KafkaError> {
        self.cluster.broker_configs(id).await
    }
}

impl Granted<'_, SchemaTextCap> {
    pub(crate) async fn subject_schema(
        &self,
        subject: &str,
        version: i32,
    ) -> Result<RegisteredSchema, KafkaError> {
        self.cluster.session.subject_schema(subject, version).await
    }
}

impl Granted<'_, CreateTopicsCap> {
    pub(crate) async fn create_topic(&self, topic: &NewTopic) -> Result<(), KafkaError> {
        self.cluster.session.create_topic(topic).await?;
        tracing::info!(cluster = %self.cluster.store.name(), topic = %topic.name, "created topic");
        self.settle(&self.cluster.store.topology, |topology| {
            topology.topics.contains_key(topic.name.as_str())
        })
        .await;
        Ok(())
    }
}

impl Granted<'_, AlterTopicConfigsCap> {
    pub(crate) async fn alter_topic_configs(
        &self,
        topic: &str,
        edit: &ConfigEdit,
    ) -> Result<(), KafkaError> {
        self.writable_topic(topic, |_| ())?;
        self.cluster
            .session
            .alter_topic_configs(topic, edit)
            .await?;
        tracing::info!(
            cluster = %self.cluster.store.name(),
            topic,
            set = ?edit.set.keys().collect::<Vec<_>>(),
            reset = ?edit.reset,
            "altered topic configs"
        );
        self.settle(&self.cluster.store.configs, |table| {
            table
                .get(topic)
                .is_some_and(|entries| edit.shows_in(entries, ConfigSource::DynamicTopic))
        })
        .await;
        Ok(())
    }
}

impl Granted<'_, AddPartitionsCap> {
    pub(crate) async fn add_partitions(
        &self,
        topic: &str,
        count: NonZeroU16,
    ) -> Result<(), KafkaError> {
        self.writable_topic(topic, |_| ())?;
        self.cluster.session.add_partitions(topic, count).await?;
        tracing::info!(cluster = %self.cluster.store.name(), topic, count, "added partitions");
        let count = usize::from(count.get());
        self.settle(&self.cluster.store.topology, |known| {
            known
                .topics
                .get(topic)
                .is_some_and(|info| info.partitions.len() >= count)
        })
        .await;
        Ok(())
    }
}

impl Granted<'_, DeleteRecordsCap> {
    /// Without `partitions` the deletion covers every partition, and without
    /// `before` it deletes every record.
    pub(crate) async fn delete_records(
        &self,
        topic: &str,
        partitions: &[i32],
        before: Option<i64>,
    ) -> Result<(), KafkaError> {
        let known = self.writable_topic(topic, TopicInfo::partition_ids)?;
        if let Some(&partition) = partitions
            .iter()
            .find(|partition| !known.contains(partition))
        {
            return Err(self.unknown_partition(topic, partition));
        }
        let deletion = RecordDeletion {
            topic: topic.to_owned(),
            before: if partitions.is_empty() {
                &known
            } else {
                partitions
            }
            .iter()
            .map(|&partition| (partition, before))
            .collect(),
        };
        let lows = self.cluster.session.delete_records(&deletion).await?;
        tracing::info!(
            cluster = %self.cluster.store.name(),
            topic,
            partitions = ?deletion.before.keys().collect::<Vec<_>>(),
            before,
            "deleted records"
        );
        self.settle(&self.cluster.store.watermarks, |marks| {
            lows.iter().all(|(&partition, &low)| {
                marks
                    .get(topic, partition)
                    .is_some_and(|mark| mark.low >= low)
            })
        })
        .await;
        Ok(())
    }
}

impl Granted<'_, DeleteTopicsCap> {
    pub(crate) async fn delete_topic(&self, topic: &str) -> Result<(), KafkaError> {
        self.writable_topic(topic, |_| ())?;
        self.cluster.session.delete_topic(topic).await?;
        tracing::info!(cluster = %self.cluster.store.name(), topic, "deleted topic");
        self.settle(&self.cluster.store.topology, |known| {
            !known.topics.contains_key(topic)
        })
        .await;
        Ok(())
    }
}

impl Granted<'_, ProduceCap> {
    pub(crate) async fn encode(&self, schema_id: i32, json: &str) -> Result<Bytes, KafkaError> {
        self.cluster.session.encode_payload(schema_id, json).await
    }

    pub(crate) async fn produce(&self, record: &NewRecord) -> Result<ProducedRecord, KafkaError> {
        let topic = record.topic.as_str();
        let partitions = self.writable_topic(topic, TopicInfo::partition_ids)?;
        if let Some(partition) = record.partition
            && !partitions.contains(&partition)
        {
            return Err(self.unknown_partition(topic, partition));
        }
        let produced = self.cluster.session.produce(record).await?;
        tracing::info!(
            cluster = %self.cluster.store.name(),
            topic,
            partition = produced.partition,
            offset = produced.offset,
            "produced record"
        );
        Ok(produced)
    }
}

impl Granted<'_, ResetOffsetsCap> {
    pub(crate) async fn plan_reset(
        &self,
        reset: &OffsetReset,
    ) -> Result<Vec<OffsetMove>, KafkaError> {
        self.known_group(&reset.group, |_| ())?;
        self.plan(reset).await
    }

    pub(crate) async fn reset_offsets(
        &self,
        reset: &OffsetReset,
    ) -> Result<Vec<OffsetMove>, KafkaError> {
        self.stopped_group(&reset.group)?;
        let moves = self.plan(reset).await?;
        if moves.is_empty() {
            return Ok(moves);
        }
        let committed: Vec<CommittedOffset> = moves.iter().map(OffsetMove::committed).collect();
        self.cluster
            .session
            .alter_group_offsets(&reset.group, &committed)
            .await?;
        tracing::info!(
            cluster = %self.cluster.store.name(),
            group = reset.group.as_str(),
            topic = ?reset.topic,
            partitions = committed.len(),
            to = ?reset.to,
            "reset group offsets"
        );
        let _watching = self.cluster.store.interest.lease_group(&reset.group);
        self.settle(&self.cluster.store.offsets, |table| {
            table
                .get(&reset.group)
                .is_some_and(|offsets| offsets.shows(&committed))
        })
        .await;
        Ok(moves)
    }

    async fn plan(&self, reset: &OffsetReset) -> Result<Vec<OffsetMove>, KafkaError> {
        let Some(topic) = &reset.topic else {
            return self.cluster.plan_reset(&reset.group, None, reset.to).await;
        };
        let known = self.writable_topic(topic, TopicInfo::partition_ids)?;
        if let Some(&partition) = reset
            .partitions
            .iter()
            .find(|partition| !known.contains(partition))
        {
            return Err(self.unknown_partition(topic, partition));
        }
        let partitions = if reset.partitions.is_empty() {
            &known
        } else {
            &reset.partitions
        };
        self.cluster
            .plan_reset(&reset.group, Some((topic, partitions)), reset.to)
            .await
    }
}

impl Granted<'_, DeleteGroupsCap> {
    pub(crate) async fn delete_group(&self, group: &str) -> Result<(), KafkaError> {
        self.stopped_group(group)?;
        self.cluster.session.delete_group(group).await?;
        tracing::info!(cluster = %self.cluster.store.name(), group, "deleted group");
        self.settle(&self.cluster.store.topology, |known| {
            !known.groups.contains_key(group)
        })
        .await;
        Ok(())
    }
}

impl Granted<'_, DeleteOffsetsCap> {
    pub(crate) async fn delete_offsets(&self, group: &str, topic: &str) -> Result<(), KafkaError> {
        if self.known_group(group, |info| info.consumes(topic))? {
            return Err(KafkaError::ConsumedTopic {
                group: group.to_owned(),
                topic: topic.to_owned(),
            });
        }
        let partitions: Vec<i32> = self
            .cluster
            .session
            .committed_offsets(group, None)
            .await?
            .into_iter()
            .filter(|offset| offset.topic == topic)
            .map(|offset| offset.partition)
            .collect();
        if partitions.is_empty() {
            return Ok(());
        }
        self.cluster
            .session
            .delete_group_offsets(group, topic, &partitions)
            .await?;
        tracing::info!(
            cluster = %self.cluster.store.name(),
            group,
            topic,
            partitions = partitions.len(),
            "deleted group offsets"
        );
        let _watching = self.cluster.store.interest.lease_group(group);
        self.settle(&self.cluster.store.offsets, |table| {
            table.get(group).is_none_or(|offsets| {
                offsets
                    .partitions()
                    .all(|(committed, _)| committed != topic)
            })
        })
        .await;
        Ok(())
    }
}

impl Granted<'_, RegisterSchemasCap> {
    pub(crate) async fn register_schema(
        &self,
        schema: &NewSchema,
    ) -> Result<RegisteredVersion, KafkaError> {
        let registered = self.cluster.session.register_schema(schema).await?;
        tracing::info!(
            cluster = %self.cluster.store.name(),
            subject = schema.subject.as_str(),
            id = registered.id,
            version = registered.version,
            "registered schema"
        );
        self.settle(&self.cluster.store.subjects, |table| {
            table
                .get(&schema.subject)
                .is_some_and(|info| info.versions.contains(&registered.version))
        })
        .await;
        Ok(registered)
    }
}

impl Granted<'_, DeleteSchemasCap> {
    pub(crate) async fn delete_schema(&self, deletion: &SchemaDeletion) -> Result<(), KafkaError> {
        let subject = deletion.subject.as_str();
        self.known_subject(subject, deletion.version)?;
        self.cluster.session.delete_schema(deletion).await?;
        tracing::info!(
            cluster = %self.cluster.store.name(),
            subject,
            version = deletion.version,
            permanent = deletion.permanent,
            "deleted schema"
        );
        self.settle(&self.cluster.store.subjects, |table| {
            table.get(subject).is_none_or(|info| {
                deletion
                    .version
                    .is_some_and(|version| !info.versions.contains(&version))
            })
        })
        .await;
        Ok(())
    }
}

impl Granted<'_, SetCompatibilityCap> {
    pub(crate) async fn set_compatibility(
        &self,
        subject: &str,
        level: SchemaCompatibility,
    ) -> Result<(), KafkaError> {
        self.known_subject(subject, None)?;
        self.cluster
            .session
            .set_compatibility(subject, level)
            .await?;
        tracing::info!(
            cluster = %self.cluster.store.name(),
            subject,
            compatibility = ?level,
            "set schema compatibility"
        );
        self.settle(&self.cluster.store.subjects, |table| {
            table
                .get(subject)
                .is_some_and(|info| info.compatibility == level)
        })
        .await;
        Ok(())
    }
}

impl Granted<'_, CreateAclsCap> {
    pub(crate) async fn create_acls(&self, acls: &[Acl]) -> Result<(), KafkaError> {
        self.cluster.session.create_acls(acls).await?;
        for acl in acls {
            tracing::info!(
                cluster = %self.cluster.store.name(),
                acl = acl.to_string().as_str(),
                "created acl"
            );
        }
        self.settle(&self.cluster.store.acls, |listing| {
            acls.iter().all(|acl| listing.contains(acl))
        })
        .await;
        Ok(())
    }
}

impl Granted<'_, DeleteAclsCap> {
    pub(crate) async fn delete_acl(&self, acl: &Acl) -> Result<(), KafkaError> {
        self.cluster.session.delete_acl(acl).await?;
        tracing::info!(
            cluster = %self.cluster.store.name(),
            acl = acl.to_string().as_str(),
            "deleted acl"
        );
        self.settle(&self.cluster.store.acls, |listing| !listing.contains(acl))
            .await;
        Ok(())
    }
}

impl Granted<'_, AlterQuotasCap> {
    pub(crate) async fn set_client_quota(&self, quota: &ClientQuota) -> Result<(), KafkaError> {
        self.cluster.session.alter_client_quota(quota).await?;
        tracing::info!(
            cluster = %self.cluster.store.name(),
            quota = quota.to_string().as_str(),
            "set client quota"
        );
        self.settle(&self.cluster.store.quotas, |listing| {
            listing
                .values(&quota.entity)
                .map_or(quota.values == QuotaValues::default(), |values| {
                    *values == quota.values
                })
        })
        .await;
        Ok(())
    }
}

impl Granted<'_, SetScramCredentialsCap> {
    pub(crate) async fn set_scram_credential(
        &self,
        credential: &NewScramCredential,
    ) -> Result<(), KafkaError> {
        self.cluster
            .session
            .set_scram_credential(credential)
            .await?;
        tracing::info!(
            cluster = %self.cluster.store.name(),
            scram_user = credential.user.as_str(),
            mechanism = %credential.mechanism,
            iterations = credential.iterations,
            "set scram credential"
        );
        self.settle(&self.cluster.store.scram_users, |listing| {
            listing.iterations(&credential.user, credential.mechanism)
                == Some(credential.iterations)
        })
        .await;
        Ok(())
    }
}

impl Granted<'_, DeleteScramCredentialsCap> {
    pub(crate) async fn delete_scram_credential(
        &self,
        user: &str,
        mechanism: ScramMechanism,
    ) -> Result<(), KafkaError> {
        self.cluster
            .session
            .delete_scram_credential(user, mechanism)
            .await?;
        tracing::info!(
            cluster = %self.cluster.store.name(),
            scram_user = user,
            %mechanism,
            "deleted scram credential"
        );
        self.settle(&self.cluster.store.scram_users, |listing| {
            listing.iterations(user, mechanism).is_none()
        })
        .await;
        Ok(())
    }
}

impl Granted<'_, AlterBrokerConfigsCap> {
    pub(crate) async fn alter_broker_configs(
        &self,
        scope: BrokerScope,
        edit: &ConfigEdit,
    ) -> Result<(), KafkaError> {
        if let BrokerScope::Broker(id) = scope {
            self.cluster.known_broker(id)?;
        }
        self.cluster
            .session
            .alter_broker_configs(scope, edit)
            .await?;
        tracing::info!(
            cluster = %self.cluster.store.name(),
            %scope,
            set = ?edit.set.keys().collect::<Vec<_>>(),
            reset = ?edit.reset,
            "altered broker configs"
        );
        let brokers = match scope {
            BrokerScope::Broker(id) => vec![id],
            BrokerScope::Cluster => self
                .cluster
                .store
                .topology
                .load()
                .map(|topology| topology.brokers.keys().copied().collect())
                .unwrap_or_default(),
        };
        let source = scope.source();
        let shown = brokers.into_iter().map(|id| {
            reread_until(
                move || self.cluster.session.broker_configs(id),
                |entries| edit.shows_in(entries, source),
            )
        });
        self.settle_on(future::join_all(shown).map(drop)).await;
        Ok(())
    }
}

impl<Cap> Granted<'_, Cap> {
    fn stopped_group(&self, group: &str) -> Result<(), KafkaError> {
        if self.known_group(group, |info| info.state.has_members())? {
            return Err(KafkaError::ActiveGroup {
                group: group.to_owned(),
            });
        }
        Ok(())
    }

    fn known_group<T>(
        &self,
        group: &str,
        read: impl FnOnce(&GroupInfo) -> T,
    ) -> Result<T, KafkaError> {
        self.cluster
            .store
            .topology
            .load()
            .as_ref()
            .and_then(|known| known.group(group))
            .map(read)
            .ok_or_else(|| KafkaError::UnknownGroup {
                cluster: self.cluster.store.name().to_owned(),
                group: group.to_owned(),
            })
    }

    fn known_subject(&self, subject: &str, version: Option<i32>) -> Result<(), KafkaError> {
        let known = self.cluster.store.subjects.load().is_some_and(|table| {
            table
                .get(subject)
                .is_some_and(|info| version.is_none_or(|version| info.versions.contains(&version)))
        });
        if known {
            Ok(())
        } else {
            Err(KafkaError::UnknownSubject {
                cluster: self.cluster.store.name().to_owned(),
                subject: subject.to_owned(),
                version: version.unwrap_or(0),
            })
        }
    }
    fn unknown_partition(&self, topic: &str, partition: i32) -> KafkaError {
        KafkaError::UnknownPartition {
            cluster: self.cluster.store.name().to_owned(),
            topic: topic.to_owned(),
            partition,
        }
    }

    fn writable_topic<T>(
        &self,
        topic: &str,
        read: impl FnOnce(&TopicInfo) -> T,
    ) -> Result<T, KafkaError> {
        let topology = self.cluster.store.topology.load();
        match topology.as_ref().and_then(|known| known.topics.get(topic)) {
            None => Err(KafkaError::UnknownTopic {
                cluster: self.cluster.store.name().to_owned(),
                topic: topic.to_owned(),
            }),
            Some(info) if info.internal => Err(KafkaError::InternalTopic(topic.to_owned())),
            Some(info) => Ok(read(info)),
        }
    }

    async fn settle<T>(&self, lane: &Lane<T>, done: impl Fn(&T) -> bool) {
        self.settle_on(lane.refresh_until(done)).await;
    }

    async fn settle_on(&self, shown: impl Future<Output = ()>) {
        if tokio::time::timeout(SETTLE, shown).await.is_err() {
            tracing::debug!(
                cluster = %self.cluster.store.name(),
                "the cluster did not show a change in time"
            );
        }
    }
}

impl Session {
    pub(crate) fn cluster<'a>(&'a self, name: &'a str) -> Result<ClusterHandle<'a>, ApiError> {
        let access = self.access.cluster(name)?;
        let cluster = self.state.clusters.get(access.cluster())?;
        Ok(ClusterHandle {
            access,
            store: &cluster.store,
            cluster,
            limits: self.state.limits,
        })
    }

    pub(crate) fn clusters(&self) -> impl Iterator<Item = ClusterHandle<'_>> {
        self.state
            .clusters
            .names()
            .filter_map(|name| self.cluster(name).ok())
    }

    /// `A` is the access type the admission layer inserts, so an MCP tool
    /// builds its session only from `Narrowed` access.
    pub(crate) fn take<A>(extensions: &mut Extensions, state: &AppState) -> Option<Self>
    where
        A: Into<EffectiveAccess> + Send + Sync + 'static,
    {
        let access = extensions.remove::<A>()?.into();
        let guard = extensions.remove::<SessionGuard>()?;
        Some(Self {
            state: state.clone(),
            access,
            guard,
        })
    }
}

impl FromRequestParts<AppState> for Session {
    type Rejection = ApiError;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        Self::take::<EffectiveAccess>(&mut parts.extensions, state).ok_or(ApiError::Unauthorized)
    }
}
