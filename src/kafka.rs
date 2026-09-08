mod assignment;
mod browse;
mod catalog;
mod config;
mod engine;
mod error;
mod factory;
mod handle;
pub(crate) mod model;
mod rates;
mod schema;
mod schema_registry;
mod session;

#[cfg(test)]
mod testing;

pub use config::KafkaClusterConfig;
pub use engine::QueryEngine;
pub use error::KafkaError;
pub use model::{
    Broker, ClusterHealth, ClusterIdentity, ClusterOverview, ConfigEntry, ConsumerGroup, Record,
    RecordPage, RecordQuery, SearchHit, Topic,
};
pub use rates::{RateStore, ThroughputPoint, TopicRate};
pub use session::ClusterSession;

#[cfg(test)]
pub use testing::FakeCluster;
