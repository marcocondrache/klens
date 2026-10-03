use std::sync::Arc;

use tokio::sync::broadcast::Receiver;
use tokio::sync::broadcast::error::TryRecvError;

use crate::config::IngestTuning;
use crate::testing::{identity, settle};

use super::bus::ChangeBus;
use super::{
    Change, ClusterStore, ConfigsDelta, GroupOffsetsWave, Lane, LogDirsDelta, SubjectsDelta,
    TopologyDelta, WatermarksTick,
};

impl ClusterStore {
    pub fn named(name: &str) -> Self {
        Self::new(identity(name), IngestTuning::default().interest_ttl)
    }
}

impl<T> Lane<T> {
    pub async fn committed(&self) -> Arc<T> {
        self.reaches(1).await
    }

    pub async fn reaches(&self, version: u64) -> Arc<T> {
        settle(&format!("lane version {version}"), || {
            (self.version() >= version).then(|| self.load()).flatten()
        })
        .await
    }
}

impl ChangeBus {
    pub fn probe(&self) -> BusProbe {
        BusProbe(self.subscribe())
    }
}

pub struct BusProbe(Receiver<Change>);

impl BusProbe {
    #[track_caller]
    pub fn next<T>(&mut self, pick: impl FnOnce(Change) -> Option<T>) -> T {
        let Some(change) = self.recv() else {
            panic!("the bus carried no change");
        };
        let Some(picked) = pick(change.clone()) else {
            panic!("the bus carried {change:?} instead");
        };
        picked
    }

    pub fn drain(&mut self) -> Vec<Change> {
        std::iter::from_fn(|| self.recv()).collect()
    }

    #[track_caller]
    pub fn assert_quiet(&mut self) {
        if let Some(change) = self.recv() {
            panic!("the bus carried {change:?}");
        }
    }

    #[track_caller]
    fn recv(&mut self) -> Option<Change> {
        match self.0.try_recv() {
            Ok(change) => Some(change),
            Err(TryRecvError::Empty) => None,
            Err(error) => panic!("the bus failed: {error}"),
        }
    }
}

impl Change {
    pub fn topology(self) -> Option<Arc<TopologyDelta>> {
        match self {
            Self::Topology(delta) => Some(delta),
            _ => None,
        }
    }

    pub fn watermarks(self) -> Option<Arc<WatermarksTick>> {
        match self {
            Self::Watermarks(tick) => Some(tick),
            _ => None,
        }
    }

    pub fn group_offsets(self) -> Option<Arc<GroupOffsetsWave>> {
        match self {
            Self::GroupOffsets(wave) => Some(wave),
            _ => None,
        }
    }

    pub fn configs(self) -> Option<Arc<ConfigsDelta>> {
        match self {
            Self::Configs(delta) => Some(delta),
            _ => None,
        }
    }

    pub fn subjects(self) -> Option<Arc<SubjectsDelta>> {
        match self {
            Self::Subjects(delta) => Some(delta),
            _ => None,
        }
    }

    pub fn log_dirs(self) -> Option<Arc<LogDirsDelta>> {
        match self {
            Self::LogDirs(delta) => Some(delta),
            _ => None,
        }
    }

    pub fn acls(self) -> Option<()> {
        matches!(self, Self::Acls).then_some(())
    }

    pub fn quotas(self) -> Option<()> {
        matches!(self, Self::Quotas).then_some(())
    }
}
