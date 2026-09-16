use std::collections::HashMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use tokio::sync::broadcast;

const CAPACITY: usize = 256;

#[derive(Clone)]
pub struct ChangeBus {
    sender: broadcast::Sender<Change>,
}

impl Default for ChangeBus {
    fn default() -> Self {
        Self::new()
    }
}

impl ChangeBus {
    pub fn new() -> Self {
        let (sender, _) = broadcast::channel(CAPACITY);
        Self { sender }
    }

    pub fn publish(&self, change: Change) {
        let _ = self.sender.send(change);
    }

    pub fn subscribe(&self) -> broadcast::Receiver<Change> {
        self.sender.subscribe()
    }
}

#[derive(Clone, Debug)]
pub enum Change {
    Topology(Arc<TopologyDelta>),
    Watermarks(Arc<WatermarksTick>),
    GroupOffsets(Arc<GroupOffsetsWave>),
    Configs(Arc<ConfigsDelta>),
    Subjects { version: u64 },
}

impl From<TopologyDelta> for Option<Change> {
    fn from(delta: TopologyDelta) -> Self {
        Some(Change::Topology(Arc::new(delta)))
    }
}

impl From<WatermarksTick> for Option<Change> {
    fn from(tick: WatermarksTick) -> Self {
        Some(Change::Watermarks(Arc::new(tick)))
    }
}

impl From<GroupOffsetsWave> for Option<Change> {
    fn from(wave: GroupOffsetsWave) -> Self {
        Some(Change::GroupOffsets(Arc::new(wave)))
    }
}

impl From<ConfigsDelta> for Option<Change> {
    fn from(delta: ConfigsDelta) -> Self {
        Some(Change::Configs(Arc::new(delta)))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TopologyDelta {
    pub version: u64,
    pub added_topics: Vec<Arc<str>>,
    pub removed_topics: Vec<Arc<str>>,
    pub changed_topics: Vec<Arc<str>>,
    pub added_groups: Vec<Arc<str>>,
    pub removed_groups: Vec<Arc<str>>,
    pub changed_groups: Vec<Arc<str>>,
    pub brokers_changed: bool,
}

impl TopologyDelta {
    pub fn is_empty(&self) -> bool {
        self.added_topics.is_empty()
            && self.removed_topics.is_empty()
            && self.changed_topics.is_empty()
            && self.added_groups.is_empty()
            && self.removed_groups.is_empty()
            && self.changed_groups.is_empty()
            && !self.brokers_changed
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct WatermarksTick {
    pub at: DateTime<Utc>,
    pub rates: HashMap<Arc<str>, f64>,
    pub cluster_rate: f64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupOffsetView {
    pub topic: Arc<str>,
    pub partition: i32,
    pub committed: i64,
    pub end: i64,
    pub lag: i64,
    pub member_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupLagUpdate {
    pub group: Arc<str>,
    pub total_lag: i64,
    pub lag_complete: bool,
    pub offsets: Vec<GroupOffsetView>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupOffsetsWave {
    pub at: DateTime<Utc>,
    pub groups: Vec<GroupLagUpdate>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ConfigsDelta {
    pub topics: Vec<Arc<str>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SubjectsChanged {
    pub version: u64,
}

impl From<SubjectsChanged> for Option<Change> {
    fn from(changed: SubjectsChanged) -> Self {
        Some(Change::Subjects {
            version: changed.version,
        })
    }
}
