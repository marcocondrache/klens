use std::sync::Arc;
use std::time::Duration;

use futures::future::try_join_all;
use indexmap::IndexMap;

use crate::config::{
    ClusterConfig, ClusterIngestConfig, ClusterName, Config, IngestTuning, SecurityProtocol,
};
use crate::kafka::client::KafkaClient;
use crate::kafka::error::KafkaError;
use crate::kafka::limits::{RecordLimits, TailLimits};
use crate::kafka::model::{ConfigEntry, RecordPage, RecordQuery};
use crate::kafka::scan::read::read_page;
use crate::kafka::scan::tail::{Tail, TailQuery};
use crate::kafka::session::ClusterSession;
use crate::kafka::store::ClusterStore;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterIdentity {
    pub name: String,
    pub bootstrap_servers: Vec<String>,
    pub security_protocol: SecurityProtocol,
}

impl ClusterIdentity {
    pub fn new(name: &ClusterName, config: &ClusterConfig) -> Self {
        Self {
            name: name.to_string(),
            bootstrap_servers: config.bootstrap_servers.clone(),
            security_protocol: config.security.protocol(),
        }
    }
}

/// One configured cluster: the connection that reads it live, the store its
/// ingest lanes fill, and how often those lanes poll.
pub struct Cluster {
    pub session: Arc<dyn ClusterSession>,
    pub store: Arc<ClusterStore>,
    pub ingest: ClusterIngestConfig,
}

impl Cluster {
    fn new(
        session: Arc<dyn ClusterSession>,
        ingest: ClusterIngestConfig,
        interest_ttl: Duration,
    ) -> Self {
        Self {
            store: Arc::new(ClusterStore::new(session.identity().clone(), interest_ttl)),
            session,
            ingest,
        }
    }

    pub fn name(&self) -> &str {
        self.store.name()
    }

    pub async fn records(
        &self,
        query: RecordQuery,
        limits: RecordLimits,
    ) -> Result<RecordPage, KafkaError> {
        read_page(self.session.as_ref(), &self.store, query, limits).await
    }

    pub async fn tail(&self, query: TailQuery, limits: TailLimits) -> Result<Tail, KafkaError> {
        Tail::open(self.session.as_ref(), &self.store, query, limits).await
    }

    pub async fn broker_configs(&self, id: i32) -> Result<Vec<ConfigEntry>, KafkaError> {
        if let Some(topology) = self.store.topology.load()
            && !topology.brokers.contains_key(&id)
        {
            return Err(KafkaError::UnknownBroker {
                cluster: self.name().to_owned(),
                id,
            });
        }

        self.session.broker_configs(id).await
    }
}

/// Every configured cluster, by name, in config order.
pub struct Clusters {
    clusters: IndexMap<String, Cluster>,
}

impl Clusters {
    pub async fn connect(config: &Config) -> Result<Self, KafkaError> {
        let tuning = &config.tuning;
        let sessions = try_join_all(
            config
                .clusters
                .iter()
                .map(|(name, cluster)| KafkaClient::new(name, cluster, tuning)),
        )
        .await?;
        Ok(Self::from_clusters(
            sessions
                .into_iter()
                .zip(config.clusters.iter())
                .map(|(session, (_, cluster))| {
                    Cluster::new(
                        Arc::new(session),
                        cluster.ingest,
                        tuning.ingest.interest_ttl,
                    )
                }),
        ))
    }

    pub fn from_sessions(sessions: Vec<impl ClusterSession>) -> Self {
        Self::from_clusters(sessions.into_iter().map(|session| {
            Cluster::new(
                Arc::new(session),
                ClusterIngestConfig::default(),
                IngestTuning::default().interest_ttl,
            )
        }))
    }

    fn from_clusters(clusters: impl IntoIterator<Item = Cluster>) -> Self {
        Self {
            clusters: clusters
                .into_iter()
                .map(|cluster| (cluster.name().to_owned(), cluster))
                .collect(),
        }
    }

    pub fn get(&self, name: &str) -> Result<&Cluster, KafkaError> {
        self.clusters
            .get(name)
            .ok_or_else(|| KafkaError::UnknownCluster(name.to_owned()))
    }

    pub fn names(&self) -> impl Iterator<Item = &str> {
        self.clusters.keys().map(String::as_str)
    }

    pub fn iter(&self) -> impl Iterator<Item = &Cluster> {
        self.clusters.values()
    }

    pub fn ready(&self) -> bool {
        self.clusters.values().all(|cluster| cluster.store.ready())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kafka::store::Topology;
    use crate::kafka::testing::FakeCluster;

    fn cluster_config() -> ClusterConfig {
        ClusterConfig {
            bootstrap_servers: vec!["localhost:9092".to_owned()],
            security: Default::default(),
            schema_registry: None,
            obfuscation: None,
            properties: Default::default(),
            ingest: Default::default(),
            writes: Vec::new(),
        }
    }

    fn config(clusters: Vec<(&str, ClusterConfig)>) -> Config {
        Config {
            bind: "127.0.0.1:8080".parse().unwrap(),
            log_level: "info".into(),
            clusters: clusters
                .into_iter()
                .map(|(name, cluster)| (name.parse().unwrap(), cluster))
                .collect(),
            auth: None,
            tuning: Default::default(),
        }
    }

    #[test]
    fn identity_uses_the_configured_name_and_defaults_the_protocol() {
        let identity = ClusterIdentity::new(&"local".parse().unwrap(), &cluster_config());

        assert_eq!(identity.name, "local");
        assert_eq!(identity.security_protocol, SecurityProtocol::Plaintext);
    }

    #[tokio::test]
    async fn connect_reaches_every_cluster_and_keeps_config_order() {
        use krafka::protocol::ApiKey;
        use krafka::testing::FakeBroker;

        let first = FakeBroker::start().await.unwrap();
        let second = FakeBroker::start().await.unwrap();
        let mut b = cluster_config();
        b.bootstrap_servers = vec![first.bootstrap_servers()];
        let mut a = cluster_config();
        a.bootstrap_servers = vec![second.bootstrap_servers()];

        let clusters = Clusters::connect(&config(vec![("b", b), ("a", a)]))
            .await
            .unwrap();

        assert_eq!(clusters.names().collect::<Vec<_>>(), vec!["b", "a"]);
        assert!(first.request_count(ApiKey::Metadata) > 0);
        assert!(second.request_count(ApiKey::Metadata) > 0);
    }

    #[tokio::test]
    async fn connect_gives_each_cluster_its_own_ingest_cadence() {
        use krafka::testing::FakeBroker;

        let first = FakeBroker::start().await.unwrap();
        let second = FakeBroker::start().await.unwrap();
        let mut fast = cluster_config();
        fast.bootstrap_servers = vec![first.bootstrap_servers()];
        fast.ingest.topology = Duration::from_secs(1);
        let mut slow = cluster_config();
        slow.bootstrap_servers = vec![second.bootstrap_servers()];
        slow.ingest.topology = Duration::from_secs(600);

        let clusters = Clusters::connect(&config(vec![("fast", fast), ("slow", slow)]))
            .await
            .unwrap();

        assert_eq!(
            clusters.get("fast").unwrap().ingest.topology,
            Duration::from_secs(1)
        );
        assert_eq!(
            clusters.get("slow").unwrap().ingest.topology,
            Duration::from_secs(600)
        );
    }

    #[tokio::test]
    async fn connect_returns_connection_errors() {
        let mut cluster = cluster_config();
        cluster.bootstrap_servers.clear();

        let result = Clusters::connect(&config(vec![("invalid", cluster)])).await;

        assert!(matches!(result, Err(KafkaError::Krafka(_))));
    }

    #[test]
    fn an_unknown_cluster_is_an_error() {
        let clusters = Clusters::from_sessions(vec![FakeCluster::local()]);

        assert_eq!(clusters.get("local").unwrap().name(), "local");
        assert!(matches!(
            clusters.get("missing"),
            Err(KafkaError::UnknownCluster(name)) if name == "missing"
        ));
    }

    #[test]
    fn clusters_keep_config_order_and_gate_readiness() {
        let clusters = Clusters::from_sessions(vec![
            FakeCluster::named("prod"),
            FakeCluster::named("staging"),
        ]);

        assert_eq!(
            clusters.names().collect::<Vec<_>>(),
            vec!["prod", "staging"]
        );
        assert!(!clusters.ready());

        let commit = |name| {
            clusters
                .get(name)
                .unwrap()
                .store
                .topology
                .commit(Arc::new(Topology::default()))
        };
        commit("prod");
        assert!(!clusters.ready(), "one cluster is not the whole set");

        commit("staging");
        assert!(clusters.ready());
    }
}
