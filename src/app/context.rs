use std::sync::Arc;

use axum::extract::FromRequestParts;
use axum::http::request::Parts;

use crate::AppState;
use crate::app::auth::SessionGuard;
use crate::app::auth::access::{
    AccessError, AclsCap, ClusterAccess, ConfigsCap, DeleteGroupOffsetsCap, EffectiveAccess,
    RecordsCap, ResetOffsetsCap, SchemaTextCap,
};
use crate::kafka::model::{AclListing, CommittedOffset, RegisteredSchema};
use crate::kafka::store::ClusterStore;
use crate::kafka::{
    Cluster, ConfigEntry, KafkaError, OffsetMove, RecordPage, RecordQuery, ResetScope, ResetTarget,
    Tail, TailLimits, TailQuery, plan_reset,
};

use super::error::ApiError;

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

    pub(crate) fn acls(&self) -> Result<Granted<'a, AclsCap>, AccessError> {
        self.access.acls().map(|cap| self.grant(cap))
    }

    pub(crate) fn reset_offsets(&self) -> Result<Granted<'a, ResetOffsetsCap>, AccessError> {
        self.access.reset_offsets().map(|cap| self.grant(cap))
    }

    pub(crate) fn delete_group_offsets(
        &self,
    ) -> Result<Granted<'a, DeleteGroupOffsetsCap>, AccessError> {
        self.access
            .delete_group_offsets()
            .map(|cap| self.grant(cap))
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

impl Granted<'_, AclsCap> {
    pub(crate) async fn list(&self) -> Result<AclListing, KafkaError> {
        self.cluster.session.acls().await
    }
}

impl Granted<'_, ResetOffsetsCap> {
    pub(crate) async fn plan(
        &self,
        group: &str,
        scope: &ResetScope,
        target: ResetTarget,
    ) -> Result<Vec<OffsetMove>, KafkaError> {
        plan_reset(self.cluster.session.as_ref(), group, scope, target).await
    }

    pub(crate) async fn commit(
        &self,
        group: &str,
        offsets: &[CommittedOffset],
    ) -> Result<(), KafkaError> {
        self.cluster
            .session
            .commit_group_offsets(group, offsets)
            .await
    }
}

impl Granted<'_, DeleteGroupOffsetsCap> {
    pub(crate) async fn committed_partitions(
        &self,
        group: &str,
        topic: &str,
    ) -> Result<Vec<i32>, KafkaError> {
        Ok(self
            .cluster
            .session
            .committed_offsets(group, None)
            .await?
            .into_iter()
            .filter(|offset| offset.topic == topic)
            .map(|offset| offset.partition)
            .collect())
    }

    pub(crate) async fn delete(
        &self,
        group: &str,
        partitions: &[(String, i32)],
    ) -> Result<(), KafkaError> {
        self.cluster
            .session
            .delete_group_offsets(group, partitions)
            .await
    }
}

impl Session {
    pub(crate) fn cluster<'a>(&'a self, name: &'a str) -> Result<ClusterHandle<'a>, ApiError> {
        let access = self.state.cluster_access(&self.access, name)?;
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
