use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;

use crate::environment::SUBJECT_LANE_INTERVAL;
use crate::kafka::error::KafkaError;
use crate::kafka::session::ClusterSession;
use crate::kafka::store::{Change, ClusterStore, Interner, Lane, SubjectTable, SubjectsDelta};

use super::runner::{LaneSource, floor};

/// The Schema Registry list projection: subject, id, type, versions,
/// compatibility.
///
/// Schema bodies are never stored here. Shipping every body to render a list
/// of names is what made the old sweep expensive; the body is an on-demand
/// per-subject fetch.
pub struct SubjectLane {
    session: Arc<dyn ClusterSession>,
    interval: Duration,
}

impl SubjectLane {
    pub fn new(session: Arc<dyn ClusterSession>) -> Self {
        Self::with_interval(session, *SUBJECT_LANE_INTERVAL)
    }

    pub fn with_interval(session: Arc<dyn ClusterSession>, interval: Duration) -> Self {
        Self {
            session,
            interval: floor(interval),
        }
    }
}

#[async_trait]
impl LaneSource for SubjectLane {
    type Table = SubjectTable;
    type Delta = SubjectsDelta;

    fn name(&self) -> &'static str {
        "subjects"
    }

    fn interval(&self) -> Duration {
        self.interval
    }

    fn lane<'a>(&self, store: &'a ClusterStore) -> &'a Lane<SubjectTable> {
        &store.subjects
    }

    async fn fetch(
        &self,
        _store: &ClusterStore,
        previous: Option<&Arc<SubjectTable>>,
    ) -> Result<Option<SubjectTable>, KafkaError> {
        let subjects = self.session.schema_subjects().await?;

        let mut interner = match previous {
            Some(previous) => Interner::seeded(previous.subjects.keys()),
            None => Interner::default(),
        };
        Ok(Some(SubjectTable::assemble(&subjects, &mut interner)))
    }

    fn diff(&self, previous: Option<&SubjectTable>, next: &SubjectTable) -> Option<SubjectsDelta> {
        SubjectsDelta::between(previous, next)
    }

    fn publish(
        &self,
        store: &ClusterStore,
        version: u64,
        _previous: Option<&Arc<SubjectTable>>,
        _next: &Arc<SubjectTable>,
        mut delta: SubjectsDelta,
    ) {
        delta.version = version;
        store.rebuild_search();
        store.bus.publish(Change::Subjects(Arc::new(delta)));
    }
}
