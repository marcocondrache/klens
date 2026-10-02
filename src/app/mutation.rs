//! The one path by which klens changes a cluster.
//!
//! ```ignore
//! session
//!     .cluster(&name)?
//!     .write(
//!         Mutation::new(Privilege::Configs, "topic.configs.alter", &topic)
//!             .kicks(&[LaneId::Configs]),
//!     )?
//!     .apply(|kafka| async move { kafka.alter_topic_configs(&topic, &changes).await })
//!     .await?;
//! ```
#![cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "the first route that changes a cluster calls this"
    )
)]

use std::sync::Arc;

use tokio::time::Instant;

use crate::app::auth::access::{AccessError, Privilege};
use crate::kafka::store::LaneId;
use crate::kafka::{Cluster, ClusterSession, KafkaError};
use crate::telemetry::AUDIT;

use super::context::ClusterHandle;
use super::error::ApiError;

#[cfg(test)]
mod tests;

pub(crate) struct Mutation {
    privilege: Privilege,
    action: &'static str,
    resource: String,
    kick: &'static [LaneId],
}

impl Mutation {
    /// `action` names the change in the audit log, `noun.verb` style such as
    /// `topic.create`. `resource` names what it changes.
    pub(crate) fn new(
        privilege: Privilege,
        action: &'static str,
        resource: impl Into<String>,
    ) -> Self {
        Self {
            privilege,
            action,
            resource: resource.into(),
            kick: &[],
        }
    }

    pub(crate) fn kicks(mut self, lanes: &'static [LaneId]) -> Self {
        self.kick = lanes;
        self
    }
}

/// A change cleared to run on one cluster. Only [`ClusterHandle::write`]
/// makes one, and [`Write::apply`] spends it.
pub(crate) struct Write<'a> {
    mutation: Mutation,
    cluster: &'a Cluster,
    actor: Option<&'a str>,
}

impl<'a> ClusterHandle<'a> {
    /// A refusal is audited too.
    pub(crate) fn write(&self, mutation: Mutation) -> Result<Write<'a>, ApiError> {
        let cleared = if !self.cluster.writable {
            Err(AccessError::ReadOnly(self.name().to_owned()))
        } else {
            self.access.check(mutation.privilege)
        };
        if let Err(error) = cleared {
            tracing::info!(
                target: AUDIT,
                actor = actor(self.actor),
                cluster = self.name(),
                action = mutation.action,
                resource = mutation.resource.as_str(),
                code = error.code(),
                "change refused"
            );
            return Err(error.into());
        }
        Ok(Write {
            mutation,
            cluster: self.cluster,
            actor: self.actor,
        })
    }
}

impl Write<'_> {
    pub(crate) async fn apply<T, Change>(
        self,
        change: impl FnOnce(Arc<dyn ClusterSession>) -> Change,
    ) -> Result<T, KafkaError>
    where
        Change: Future<Output = Result<T, KafkaError>> + Send,
    {
        let Self {
            mutation,
            cluster,
            actor: subject,
        } = self;
        let started = Instant::now();
        let outcome = change(Arc::clone(&cluster.session)).await;
        let elapsed_ms = started.elapsed().as_millis() as u64;

        match &outcome {
            Ok(_) => tracing::info!(
                target: AUDIT,
                actor = actor(subject),
                cluster = cluster.name(),
                action = mutation.action,
                resource = mutation.resource.as_str(),
                elapsed_ms,
                "change applied"
            ),
            Err(error) => tracing::info!(
                target: AUDIT,
                actor = actor(subject),
                cluster = cluster.name(),
                action = mutation.action,
                resource = mutation.resource.as_str(),
                elapsed_ms,
                code = error.code(),
                %error,
                "change failed"
            ),
        }

        // A failed change may still have applied in part.
        cluster.store.kick(mutation.kick);
        outcome
    }
}

fn actor(subject: Option<&str>) -> &str {
    subject.unwrap_or("anonymous")
}
