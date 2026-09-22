//! Per-cluster Kafka I/O port.
//!
//! Everything that talks to a broker goes through [`ClusterSession`], and
//! [`SessionSet`] holds one per configured cluster. Production is
//! [`super::client::KafkaClient`]. Tests use an in-memory fake cluster.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use foldhash::HashMap;
use futures::future::try_join_all;
use indexmap::IndexMap;

use crate::config::Config;
use crate::kafka::client::KafkaClient;
use crate::kafka::error::KafkaError;
use crate::kafka::model::{
    AclListing, ClusterIdentity, CommittedOffset, ConfigEntry, GroupSnapshot, MetadataSnapshot,
    PartitionWindow, RegisteredSchema, ScanConsumer, SchemaSubject, TopicMetadata, Watermarks,
};
use crate::kafka::scan::obfuscate::ObfuscationPolicy;
use crate::kafka::scan::payload::PayloadCodec;

/// Per-cluster Kafka I/O. Matches [`super::client::KafkaClient`].
#[async_trait]
pub trait ClusterSession: Send + Sync + 'static {
    fn identity(&self) -> &ClusterIdentity;

    /// Every topic and broker in the cluster.
    async fn metadata(&self) -> Result<MetadataSnapshot, KafkaError>;

    /// One topic's partitions, served from the client's cache when it is
    /// fresh.
    async fn topic_metadata(&self, topic: &str) -> Result<TopicMetadata, KafkaError>;

    /// Low and high watermarks for the given partitions, grouped by topic.
    ///
    /// The caller supplies partitions from a metadata snapshot it already
    /// has. This method does not refetch cluster metadata. Implementations
    /// shard the request by cached leader.
    async fn watermarks(
        &self,
        topics: &HashMap<String, Vec<i32>>,
    ) -> Result<HashMap<String, HashMap<i32, Watermarks>>, KafkaError>;

    /// Earliest offset at or after `timestamp` (unix ms) for each answered
    /// partition.
    ///
    /// `None` is Kafka's invalid offset (nothing at or after that time). A
    /// missing key means the broker omitted that partition.
    async fn offsets_for_times(
        &self,
        topic: &str,
        partitions: &[i32],
        timestamp: i64,
    ) -> Result<HashMap<i32, Option<i64>>, KafkaError>;

    async fn topic_configs(
        &self,
        topics: &[&str],
    ) -> Result<HashMap<String, Vec<ConfigEntry>>, KafkaError>;

    async fn broker_configs(&self, broker_id: i32) -> Result<Vec<ConfigEntry>, KafkaError>;

    async fn groups(&self) -> Result<Vec<GroupSnapshot>, KafkaError>;

    /// `None` asks for every partition the group has committed, for a group
    /// with no assignment to narrow the fetch to.
    async fn committed_offsets(
        &self,
        group_id: &str,
        partitions: Option<&[(String, i32)]>,
    ) -> Result<Vec<CommittedOffset>, KafkaError>;

    /// Open a consumer for one page request, already assigned to `windows`.
    async fn open_scan(
        &self,
        topic: &str,
        windows: &[PartitionWindow],
    ) -> Result<Box<dyn ScanConsumer>, KafkaError>;

    /// Registry-aware payload decoding, when the cluster has a registry.
    ///
    /// `None` means payloads are returned as-is.
    fn payload_codec(&self) -> Option<Arc<dyn PayloadCodec>> {
        None
    }

    /// Obfuscation rules for this cluster, compiled at startup.
    ///
    /// `None` means records leave the process exactly as they came off the
    /// wire.
    fn obfuscation(&self) -> Option<Arc<ObfuscationPolicy>> {
        None
    }

    async fn schema_subjects(&self) -> Result<Vec<SchemaSubject>, KafkaError> {
        Ok(Vec::new())
    }

    async fn subject_schema(
        &self,
        subject: &str,
        version: i32,
    ) -> Result<RegisteredSchema, KafkaError> {
        Err(KafkaError::UnknownSubject {
            cluster: self.identity().name.clone(),
            subject: subject.to_owned(),
            version,
        })
    }

    /// All ACL bindings the broker will describe, or authorizer-off.
    ///
    /// Default is an enabled empty list (session has no ACL source).
    /// Production always uses `AclFilter::all()`; this method takes no filter.
    async fn acls(&self) -> Result<AclListing, KafkaError> {
        Ok(AclListing::Enabled(Vec::new()))
    }

    fn consume_timeout(&self) -> Duration {
        *crate::environment::CONSUME_TIMEOUT
    }
}

/// Every configured cluster's I/O port, in config order.
pub struct SessionSet {
    sessions: IndexMap<String, Arc<dyn ClusterSession>>,
}

impl SessionSet {
    /// Connects one client per configured cluster, in parallel.
    pub async fn from_config(config: &Config) -> Result<Self, KafkaError> {
        Ok(Self::from_sessions(
            try_join_all(config.clusters.iter().map(KafkaClient::new)).await?,
        ))
    }

    pub fn from_sessions(sessions: Vec<impl ClusterSession>) -> Self {
        Self {
            sessions: sessions
                .into_iter()
                .map(|session| {
                    (
                        session.identity().name.clone(),
                        Arc::new(session) as Arc<dyn ClusterSession>,
                    )
                })
                .collect(),
        }
    }

    pub fn names(&self) -> Vec<&str> {
        self.sessions.keys().map(String::as_str).collect()
    }

    pub fn identities(&self) -> Vec<ClusterIdentity> {
        self.sessions
            .values()
            .map(|session| session.identity().clone())
            .collect()
    }

    pub fn sessions(&self) -> Vec<Arc<dyn ClusterSession>> {
        self.sessions.values().map(Arc::clone).collect()
    }

    pub fn session(&self, name: &str) -> Result<&dyn ClusterSession, KafkaError> {
        self.sessions
            .get(name)
            .map(Arc::as_ref)
            .ok_or_else(|| KafkaError::UnknownCluster(name.to_owned()))
    }
}

impl std::fmt::Debug for SessionSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SessionSet")
            .field("clusters", &self.names())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ClusterConfig;
    use crate::kafka::testing::FakeCluster;

    fn cluster_config(name: &str) -> ClusterConfig {
        ClusterConfig {
            name: name.to_owned(),
            bootstrap_servers: vec!["localhost:9092".to_owned()],
            security: None,
            schema_registry: None,
            obfuscation: None,
            properties: Default::default(),
            ingest: Default::default(),
        }
    }

    fn config(clusters: Vec<ClusterConfig>) -> Config {
        Config {
            bind: "127.0.0.1:8080".parse().unwrap(),
            log_level: "info".into(),
            clusters,
            auth: None,
        }
    }

    #[tokio::test]
    async fn from_config_connects_all_clusters_and_keeps_config_order() {
        use krafka::protocol::ApiKey;
        use krafka::testing::FakeBroker;

        let first = FakeBroker::start().await.unwrap();
        let second = FakeBroker::start().await.unwrap();
        let mut b = cluster_config("b");
        b.bootstrap_servers = vec![first.bootstrap_servers()];
        let mut a = cluster_config("a");
        a.bootstrap_servers = vec![second.bootstrap_servers()];

        let sessions = SessionSet::from_config(&config(vec![b, a])).await.unwrap();

        assert_eq!(sessions.names(), vec!["b", "a"]);
        assert!(first.request_count(ApiKey::Metadata) > 0);
        assert!(second.request_count(ApiKey::Metadata) > 0);
    }

    #[tokio::test]
    async fn from_config_returns_connection_errors() {
        let mut cluster = cluster_config("invalid");
        cluster.bootstrap_servers.clear();

        let result = SessionSet::from_config(&config(vec![cluster])).await;

        assert!(matches!(result, Err(KafkaError::Krafka(_))));
    }

    #[test]
    fn identities_keep_config_order() {
        let sessions = SessionSet::from_sessions(vec![
            FakeCluster::named("prod"),
            FakeCluster::named("staging"),
        ]);

        let names: Vec<String> = sessions
            .identities()
            .into_iter()
            .map(|identity| identity.name)
            .collect();

        assert_eq!(names, vec!["prod", "staging"]);
    }

    #[test]
    fn an_unknown_cluster_is_an_error() {
        let sessions = SessionSet::from_sessions(vec![FakeCluster::local()]);

        assert!(sessions.session("local").is_ok());
        assert!(matches!(
            sessions.session("missing"),
            Err(KafkaError::UnknownCluster(name)) if name == "missing"
        ));
    }
}
