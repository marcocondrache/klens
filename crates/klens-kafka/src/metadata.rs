use std::time::Duration;

use async_trait::async_trait;
use serde::Serialize;

use crate::error::KafkaError;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct BrokerInfo {
    pub id: i32,
    pub host: String,
    pub port: i32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ClusterInfo {
    pub brokers: Vec<BrokerInfo>,
}

#[async_trait]
pub trait MetadataApi: Send + Sync {
    async fn describe_cluster(&self, timeout: Duration) -> Result<ClusterInfo, KafkaError>;
}
