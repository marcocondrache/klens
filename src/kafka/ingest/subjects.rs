use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;

use crate::kafka::error::KafkaError;
use crate::kafka::session::ClusterSession;
use crate::kafka::store::{Change, ClusterStore, Interner, Lane, SubjectTable, SubjectsDelta};

use super::runner::LaneSource;

pub struct SubjectLane {
    session: Arc<dyn ClusterSession>,
    interval: Duration,
}

impl SubjectLane {
    pub fn with_interval(session: Arc<dyn ClusterSession>, interval: Duration) -> Self {
        Self { session, interval }
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
        store.bus.publish(Change::Subjects(Arc::new(delta)));
    }
}
