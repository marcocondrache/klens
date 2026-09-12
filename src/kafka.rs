mod adapter;
mod broker;
mod catalog;
mod cluster;
mod engine;
mod error;
mod group;
mod limits;
mod metadata;
mod record;
mod registry;
mod search;
mod session;
mod topic;
mod topic_config;
mod watermarks;

mod lag;
pub(crate) mod model;
mod rates;
mod series;

#[cfg(test)]
mod testing;

pub use adapter::KafkaClusterConfig;
pub use catalog::{
    CatalogAssemble, CatalogCache, CatalogHealth, CatalogPoller, CatalogReuse, ClusterSnapshot,
    PollLane, SubjectCache,
};
pub use engine::QueryEngine;
pub use error::{KafkaError, QueryError};
pub use lag::LagStore;
pub use limits::RecordLimits;
pub use model::{
    Broker, ClusterHealth, ClusterIdentity, ClusterOverview, ConfigEntry, ConsumerGroup, Record,
    RecordPage, RecordQuery, SearchHit, TimestampRange, Topic,
};
pub use rates::{RateStore, TopicRate};
pub use record::cursor::RecordCursor;
pub use record::filter::{RecordFilter, compile as compile_record_filter};
pub use series::ThroughputPoint;
pub use session::ClusterSession;

#[cfg(test)]
pub use testing::FakeCluster;
