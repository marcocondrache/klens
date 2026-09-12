use std::collections::{BTreeSet, HashMap};
use std::time::{SystemTime, UNIX_EPOCH};

use chrono::{DateTime, Utc};

use crate::config::SecurityProtocol;
use crate::kafka::broker::Broker;
use crate::kafka::cluster::{ClusterIdentity, ClusterOverview};
use crate::kafka::group::ConsumerGroup;
use crate::kafka::registry::SchemaSubject;
use crate::kafka::search::{SearchHit, search_snapshot};
use crate::kafka::topic::Topic;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterSnapshot {
    pub updated_at: DateTime<Utc>,
    pub topics: Vec<Topic>,
    pub groups: Vec<ConsumerGroup>,
    pub brokers: Vec<Broker>,
    pub overview: ClusterOverview,
}

impl ClusterSnapshot {
    pub fn from_topics(topics: Vec<Topic>) -> Self {
        Self::from_catalog(topics, Vec::new())
    }

    pub fn from_groups(groups: Vec<ConsumerGroup>) -> Self {
        Self::from_catalog(Vec::new(), groups)
    }

    pub fn from_catalog(topics: Vec<Topic>, groups: Vec<ConsumerGroup>) -> Self {
        Self::assemble(
            topics,
            groups,
            Vec::new(),
            ClusterOverview::offline(empty_identity()),
        )
    }

    pub fn assemble(
        topics: Vec<Topic>,
        groups: Vec<ConsumerGroup>,
        brokers: Vec<Broker>,
        overview: ClusterOverview,
    ) -> Self {
        Self {
            updated_at: wall_clock(),
            topics,
            groups,
            brokers,
            overview,
        }
    }

    pub fn topic(&self, name: &str) -> Option<&Topic> {
        self.topics.iter().find(|topic| topic.name == name)
    }

    pub fn group(&self, id: &str) -> Option<&ConsumerGroup> {
        self.groups.iter().find(|group| group.id == id)
    }

    pub fn broker(&self, id: i32) -> Option<&Broker> {
        self.brokers.iter().find(|broker| broker.id == id)
    }

    pub fn message_counts(&self) -> HashMap<String, u64> {
        self.topics
            .iter()
            .map(|topic| (topic.name.clone(), topic.message_count))
            .collect()
    }

    pub fn search(&self, term: &str, subjects: &[SchemaSubject]) -> Vec<SearchHit> {
        search_snapshot(term, &self.topics, &self.brokers, &self.groups, subjects)
    }

    pub fn body_eq(&self, other: &Self) -> bool {
        self.topics == other.topics
            && self.groups == other.groups
            && self.brokers == other.brokers
            && self.overview == other.overview
    }

    pub fn roster_eq(&self, other: &Self) -> bool {
        self.topic_names() == other.topic_names()
            && self.group_ids() == other.group_ids()
            && self.broker_ids() == other.broker_ids()
    }

    fn topic_names(&self) -> BTreeSet<&str> {
        self.topics
            .iter()
            .map(|topic| topic.name.as_str())
            .collect()
    }

    fn group_ids(&self) -> BTreeSet<&str> {
        self.groups.iter().map(|group| group.id.as_str()).collect()
    }

    fn broker_ids(&self) -> BTreeSet<i32> {
        self.brokers.iter().map(|broker| broker.id).collect()
    }

    pub fn groups_for_topic(&self, topic: Option<&str>) -> Vec<ConsumerGroup> {
        match topic {
            Some(topic) => self
                .groups
                .iter()
                .filter(|group| group.topics.iter().any(|name| name == topic))
                .cloned()
                .collect(),
            None => self.groups.clone(),
        }
    }
}

pub(crate) fn empty_identity() -> ClusterIdentity {
    ClusterIdentity {
        name: String::new(),
        bootstrap_servers: Vec::new(),
        security_protocol: SecurityProtocol::Plaintext,
    }
}

pub(crate) fn wall_clock() -> DateTime<Utc> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    DateTime::<Utc>::from_timestamp(now.as_secs() as i64, now.subsec_nanos())
        .unwrap_or(DateTime::<Utc>::UNIX_EPOCH)
}
