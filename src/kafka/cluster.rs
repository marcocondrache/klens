use std::sync::Arc;
use std::time::Duration;

use futures::future::try_join_all;
use indexmap::IndexMap;

use crate::config::{self, IngestTuning, Tuning};
use crate::kafka::client::KafkaClient;
use crate::kafka::error::KafkaError;
use crate::kafka::limits::{RecordLimits, TailLimits};
use crate::kafka::model::{
    ConfigEntry, FoundRecord, OffsetMove, RecordAt, RecordPage, RecordQuery, ResetTarget,
};
use crate::kafka::reset::plan_reset;
use crate::kafka::scan::export::Export;
use crate::kafka::scan::read::{read_page, read_record};
use crate::kafka::scan::tail::{Tail, TailQuery};
use crate::kafka::session::ClusterSession;
use crate::kafka::store::ClusterStore;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterIdentity {
    pub name: String,
}

impl ClusterIdentity {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_owned(),
        }
    }
}

pub struct Cluster {
    pub session: Arc<dyn ClusterSession>,
    pub store: Arc<ClusterStore>,
    pub writable: bool,
}

impl Cluster {
    fn new(session: Arc<dyn ClusterSession>, interest_ttl: Duration, writable: bool) -> Self {
        Self {
            store: Arc::new(ClusterStore::new(session.identity().clone(), interest_ttl)),
            session,
            writable,
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

    pub async fn record(
        &self,
        at: RecordAt,
        limits: RecordLimits,
    ) -> Result<FoundRecord, KafkaError> {
        read_record(self.session.as_ref(), &self.store, at, limits).await
    }

    pub async fn export(
        &self,
        query: RecordQuery,
        limits: RecordLimits,
    ) -> Result<Export, KafkaError> {
        Export::open(Arc::clone(&self.session), &self.store, query, limits).await
    }

    pub async fn tail(&self, query: TailQuery, limits: TailLimits) -> Result<Tail, KafkaError> {
        Tail::open(self.session.as_ref(), &self.store, query, limits).await
    }

    pub async fn broker_configs(&self, id: i32) -> Result<Vec<ConfigEntry>, KafkaError> {
        self.known_broker(id)?;
        self.session.broker_configs(id).await
    }

    /// Refuses a broker the topology does not list, once it lists any.
    pub fn known_broker(&self, id: i32) -> Result<(), KafkaError> {
        match self.store.topology.load() {
            Some(topology) if !topology.brokers.contains_key(&id) => {
                Err(KafkaError::UnknownBroker {
                    cluster: self.name().to_owned(),
                    id,
                })
            }
            _ => Ok(()),
        }
    }

    pub async fn plan_reset(
        &self,
        group: &str,
        partitions: Option<(&str, &[i32])>,
        to: ResetTarget,
    ) -> Result<Vec<OffsetMove>, KafkaError> {
        plan_reset(self.session.as_ref(), group, partitions, to).await
    }
}

/// Every configured cluster, by name, in config order.
pub struct Clusters {
    clusters: IndexMap<String, Cluster>,
}

impl Clusters {
    pub async fn connect(
        clusters: &IndexMap<String, config::Cluster>,
        tuning: &Tuning,
    ) -> Result<Self, KafkaError> {
        let sessions = try_join_all(
            clusters
                .iter()
                .map(|(name, cluster)| KafkaClient::new(name, cluster, tuning)),
        )
        .await?;
        Ok(Self::from_clusters(
            sessions
                .into_iter()
                .zip(clusters.values())
                .map(|(session, cluster)| {
                    Cluster::new(
                        Arc::new(session),
                        tuning.ingest.interest_ttl,
                        cluster.writable,
                    )
                }),
        ))
    }

    /// Every cluster is read-only, as when the config omits `writable`.
    pub fn from_sessions(sessions: Vec<impl ClusterSession>) -> Self {
        Self::from_clusters(sessions.into_iter().map(|session| {
            Cluster::new(
                Arc::new(session),
                IngestTuning::default().interest_ttl,
                false,
            )
        }))
    }

    #[cfg(test)]
    pub(crate) fn writable(mut self, names: &[&str]) -> Self {
        for name in names {
            self.clusters
                .get_mut(*name)
                .expect("configured cluster")
                .writable = true;
        }
        self
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
    use krafka::protocol::ApiKey;

    use super::*;
    use crate::kafka::client::testing::Broker;
    use crate::kafka::store::Topology;
    use crate::testing::{FakeCluster, yaml};

    async fn connect(clusters: Vec<(&str, config::Cluster)>) -> Result<Clusters, KafkaError> {
        let clusters = clusters
            .into_iter()
            .map(|(name, cluster)| (name.to_owned(), cluster))
            .collect();
        Clusters::connect(&clusters, &Tuning::default()).await
    }

    #[tokio::test]
    async fn connect_reaches_every_cluster_and_keeps_config_order() {
        let first = Broker::start().await;
        let second = Broker::start().await;

        let mut open = second.config();
        open.writable = true;
        let clusters = connect(vec![("b", first.config()), ("a", open)])
            .await
            .unwrap();

        assert_eq!(clusters.names().collect::<Vec<_>>(), vec!["b", "a"]);
        assert!(!clusters.get("b").unwrap().writable);
        assert!(
            clusters.get("a").unwrap().writable,
            "each cluster keeps its own switch"
        );
        assert!(first.request_count(ApiKey::Metadata) > 0);
        assert!(second.request_count(ApiKey::Metadata) > 0);
    }

    #[tokio::test]
    async fn connect_returns_connection_errors() {
        let result = connect(vec![("invalid", yaml("bootstrap_servers: []"))]).await;

        assert!(matches!(result, Err(KafkaError::Krafka(_))));
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
