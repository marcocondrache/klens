use std::sync::Arc;
use std::time::Duration;

use crate::environment::SUBJECT_LANE_INTERVAL;
use crate::kafka::error::KafkaError;
use crate::kafka::session::ClusterSession;
use crate::kafka::store::{ClusterStore, SubjectInfo, SubjectTable, SubjectsChanged};

use super::runner::LaneSource;

pub struct SubjectSource {
    session: Arc<dyn ClusterSession>,
    store: Arc<ClusterStore>,
}

impl SubjectSource {
    pub fn new(session: Arc<dyn ClusterSession>, store: Arc<ClusterStore>) -> Self {
        Self { session, store }
    }
}

impl LaneSource for SubjectSource {
    type Table = SubjectTable;
    type Delta = SubjectsChanged;

    async fn fetch(&self, _prev: Option<&Arc<SubjectTable>>) -> Result<SubjectTable, KafkaError> {
        let subjects = self.session.schema_subjects().await?;
        let mut table = SubjectTable::default();
        for subject in subjects {
            table.subjects.insert(
                Arc::from(subject.subject.as_str()),
                SubjectInfo {
                    id: subject.id,
                    schema_type: subject.schema_type,
                    latest_version: subject.latest_version,
                    versions: subject.versions,
                    compatibility: subject.compatibility,
                    degraded: false,
                    error: None,
                },
            );
        }
        Ok(table)
    }

    fn diff(&self, prev: Option<&SubjectTable>, next: &SubjectTable) -> Option<SubjectsChanged> {
        if prev.is_some_and(|prev| prev == next) {
            None
        } else {
            Some(SubjectsChanged {
                version: self.store.subjects.version() + 1,
            })
        }
    }

    fn interval(&self) -> Duration {
        *SUBJECT_LANE_INTERVAL
    }

    fn after_commit(&self, _table: &Arc<SubjectTable>, _version: u64, _delta: &SubjectsChanged) {
        self.store.rebuild_search();
    }
}
