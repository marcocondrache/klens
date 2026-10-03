use std::sync::Arc;
use std::time::Duration;

use axum::extract::FromRequestParts;
use axum::http::request::Parts;

use crate::AppState;
use crate::app::auth::SessionGuard;
use crate::app::auth::access::{
    AccessError, ClusterAccess, ConfigsCap, EffectiveAccess, ManageTopicsCap, RecordsCap,
    SchemaTextCap,
};
use crate::kafka::model::{FoundRecord, NewTopic, RecordAt, RegisteredSchema};
use crate::kafka::store::{ClusterStore, Lane};
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

    pub(crate) fn records(&self) -> Result<Granted<'a, RecordsCap>, AccessError> {
        self.access.records().map(|cap| self.grant(cap))
    }

    pub(crate) fn configs(&self) -> Result<Granted<'a, ConfigsCap>, AccessError> {
        self.access.configs().map(|cap| self.grant(cap))
    }

    pub(crate) fn schema_text(&self) -> Result<Granted<'a, SchemaTextCap>, AccessError> {
        self.access.schema_text().map(|cap| self.grant(cap))
    }

    pub(crate) fn manage_topics(&self) -> Result<Granted<'a, ManageTopicsCap>, AccessError> {
        self.writable()?;
        self.access.manage_topics().map(|cap| self.grant(cap))
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

impl Granted<'_, ConfigsCap> {
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

impl Granted<'_, ManageTopicsCap> {
    pub(crate) async fn create_topic(&self, topic: &NewTopic) -> Result<(), KafkaError> {
        self.cluster.session.create_topic(topic).await?;
        tracing::info!(cluster = %self.cluster.store.name(), topic = %topic.name, "created topic");
        self.settle(&self.cluster.store.topology, |topology| {
            topology.topics.contains_key(topic.name.as_str())
        })
        .await;
        Ok(())
    }

    pub(crate) async fn delete_topic(&self, topic: &str) -> Result<(), KafkaError> {
        let topology = &self.cluster.store.topology;
        let internal = topology
            .load()
            .and_then(|known| known.topics.get(topic).map(|entry| entry.internal));
        match internal {
            None => {
                return Err(KafkaError::UnknownTopic {
                    cluster: self.cluster.store.name().to_owned(),
                    topic: topic.to_owned(),
                });
            }
            Some(true) => return Err(KafkaError::InternalTopic(topic.to_owned())),
            Some(false) => {}
        }
        self.cluster.session.delete_topic(topic).await?;
        tracing::info!(cluster = %self.cluster.store.name(), topic, "deleted topic");
        self.settle(topology, |known| !known.topics.contains_key(topic))
            .await;
        Ok(())
    }
}

impl<Cap> Granted<'_, Cap> {
    async fn settle<T>(&self, lane: &Lane<T>, done: impl Fn(&T) -> bool) {
        if tokio::time::timeout(SETTLE, lane.refresh_until(done))
            .await
            .is_err()
        {
            tracing::debug!(
                cluster = %self.cluster.store.name(),
                "the store did not show a change in time"
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
}

impl FromRequestParts<AppState> for Session {
    type Rejection = ApiError;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        let Some(access) = parts.extensions.remove::<EffectiveAccess>() else {
            return Err(ApiError::Unauthorized);
        };
        let Some(guard) = parts.extensions.remove::<SessionGuard>() else {
            return Err(ApiError::Unauthorized);
        };
        Ok(Self {
            state: state.clone(),
            access,
            guard,
        })
    }
}
