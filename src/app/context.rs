use std::sync::Arc;
use std::time::Duration;

use axum::extract::FromRequestParts;
use axum::http::Extensions;
use axum::http::request::Parts;

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
use crate::kafka::store::{ClusterStore, GroupInfo, Lane, TopicInfo};
use crate::kafka::{Cluster, KafkaError, TailLimits};

use super::error::ApiError;

mod acls;
mod brokers;
mod groups;
mod quotas;
mod records;
mod schemas;
mod scram_users;
mod topics;

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

    pub(crate) fn latest_version(&self, subject: &str) -> Result<i32, ApiError> {
        self.store
            .subjects
            .load()
            .and_then(|subjects| subjects.get(subject).map(|info| info.latest_version))
            .ok_or_else(|| {
                KafkaError::UnknownSubject {
                    cluster: self.name().to_owned(),
                    subject: subject.to_owned(),
                    version: 0,
                }
                .into()
            })
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
