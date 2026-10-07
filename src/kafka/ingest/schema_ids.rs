use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;

use crate::kafka::error::KafkaError;
use crate::kafka::session::ClusterSession;
use crate::kafka::store::{Change, ClusterStore, Lane, SchemaIdTable, SubjectTable, SubjectsDelta};

use super::runner::LaneSource;

pub struct SchemaIdLane {
    session: Arc<dyn ClusterSession>,
    interval: Duration,
}

impl SchemaIdLane {
    pub fn with_interval(session: Arc<dyn ClusterSession>, interval: Duration) -> Self {
        Self { session, interval }
    }
}

#[async_trait]
impl LaneSource for SchemaIdLane {
    type Upstream = SubjectTable;
    type Table = SchemaIdTable;
    type Delta = SubjectsDelta;

    fn name(&self) -> &'static str {
        "schema_ids"
    }

    fn interval(&self) -> Duration {
        self.interval
    }

    fn lane<'a>(&self, store: &'a ClusterStore) -> &'a Lane<SchemaIdTable> {
        &store.schema_ids
    }

    async fn fetch(
        &self,
        store: &ClusterStore,
        subjects: &SubjectTable,
        previous: Option<&Arc<SchemaIdTable>>,
    ) -> Result<SchemaIdTable, KafkaError> {
        let mut table = SchemaIdTable::default();
        let mut wanted = Vec::new();

        for (subject, info) in &subjects.subjects {
            // A registry that reuses the latest version number for another
            // schema was reset or had the subject deleted and registered again.
            let held = previous
                .and_then(|previous| previous.subjects.get(subject))
                .filter(|held| {
                    held.get(&info.latest_version)
                        .is_none_or(|&id| id == info.id)
                });
            let ids = table.subjects.entry(Arc::clone(subject)).or_default();
            for &version in &info.versions {
                let known = if version == info.latest_version {
                    Some(info.id)
                } else {
                    held.and_then(|held| held.get(&version).copied())
                };
                match known {
                    Some(id) => {
                        ids.insert(version, id);
                    }
                    None => wanted.push((Arc::clone(subject), version)),
                }
            }
        }

        if !wanted.is_empty() {
            let found = self.session.schema_version_ids(&wanted).await?;
            if found.len() < wanted.len() {
                tracing::warn!(
                    cluster = %store.name(),
                    missing = wanted.len() - found.len(),
                    "schema versions left without an id"
                );
            }
            for (subject, registered) in found {
                table
                    .subjects
                    .entry(subject)
                    .or_default()
                    .insert(registered.version, registered.id);
            }
        }

        Ok(table)
    }

    fn stale(&self, _fetched: &SubjectTable, _latest: &SubjectTable) -> bool {
        true
    }

    fn diff(
        &self,
        previous: Option<&SchemaIdTable>,
        next: &SchemaIdTable,
    ) -> Option<SubjectsDelta> {
        SubjectsDelta::of_ids(previous, next)
    }

    fn publish(
        &self,
        store: &ClusterStore,
        _previous: Option<&Arc<SchemaIdTable>>,
        _next: &Arc<SchemaIdTable>,
        delta: SubjectsDelta,
    ) {
        store.bus.publish(Change::Subjects(Arc::new(delta)));
    }
}

#[cfg(test)]
mod tests;
