use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use foldhash::HashSet;

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
        let held_for = |subject: &str| previous.and_then(|previous| previous.subjects.get(subject));

        // A registry that reuses a version number for another schema was reset
        // or had the subject deleted and registered again. A subject past the
        // versions held has its newest held id read again to catch that.
        let checks: Vec<_> = subjects
            .subjects
            .iter()
            .filter_map(|(subject, info)| {
                let held = held_for(subject)?;
                if held.contains_key(&info.latest_version) {
                    return None;
                }
                let (&version, _) = held.last_key_value()?;
                Some((Arc::clone(subject), version))
            })
            .collect();
        let confirmed: HashSet<Arc<str>> = if checks.is_empty() {
            HashSet::default()
        } else {
            self.session
                .schema_version_ids(&checks)
                .await?
                .into_iter()
                .filter(|(subject, found)| {
                    held_for(subject).and_then(|held| held.get(&found.version)) == Some(&found.id)
                })
                .map(|(subject, _)| subject)
                .collect()
        };

        let mut table = SchemaIdTable::default();
        let mut wanted = Vec::new();

        for (subject, info) in &subjects.subjects {
            let held = held_for(subject).filter(|held| match held.get(&info.latest_version) {
                Some(&id) => id == info.id,
                None => confirmed.contains(subject),
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
