use std::collections::HashMap;
use std::sync::Arc;

use tokio::task::JoinSet;
use tokio::time::timeout;

use crate::config::Config;
use crate::environment::{OFFSET_FETCH_BATCH, OVERVIEW_BUDGET};
use crate::kafka::catalog::{
    assemble_brokers, assemble_group, assemble_overview, assemble_topic, clamp_record_limit,
    clamp_record_page, groups_for_topic, plan_records, search_catalog,
    should_fetch_list_watermarks,
};
use crate::kafka::error::KafkaError;
use crate::kafka::model::{
    Broker, ClusterIdentity, ClusterOverview, ConfigEntry, ConsumerGroup, GroupSnapshot,
    RecordPage, RecordQuery, SearchHit, Topic, Watermarks,
};
use crate::kafka::registry::ClusterRegistry;
use crate::kafka::session::ClusterSession;

/// Answers GraphQL catalog and browse queries from [`ClusterSession`]s.
pub struct QueryEngine {
    clusters: HashMap<String, Arc<dyn ClusterSession>>,
    order: Vec<String>,
}

impl std::fmt::Debug for QueryEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueryEngine")
            .field("clusters", &self.order)
            .finish()
    }
}

impl QueryEngine {
    pub fn from_config(config: &Config) -> Result<Self, KafkaError> {
        Ok(Self::from_registry(ClusterRegistry::from_config(config)?))
    }

    pub fn from_registry(registry: ClusterRegistry) -> Self {
        let order = registry.names();
        let clusters = registry
            .into_sessions()
            .into_iter()
            .map(|(name, handle)| {
                let session: Arc<dyn ClusterSession> = handle;
                (name, session)
            })
            .collect();

        Self { clusters, order }
    }

    pub fn from_sessions(sessions: Vec<Arc<dyn ClusterSession>>) -> Self {
        let mut clusters = HashMap::with_capacity(sessions.len());
        let mut order = Vec::with_capacity(sessions.len());

        for session in sessions {
            let name = session.identity().name.clone();
            order.push(name.clone());
            clusters.insert(name, session);
        }

        Self { clusters, order }
    }

    pub fn names(&self) -> Vec<String> {
        self.order.clone()
    }

    fn session(&self, name: &str) -> Result<&Arc<dyn ClusterSession>, KafkaError> {
        self.clusters
            .get(name)
            .ok_or_else(|| KafkaError::UnknownCluster(name.to_owned()))
    }

    pub async fn clusters(&self) -> Vec<ClusterOverview> {
        let mut join = JoinSet::new();
        for (index, name) in self.order.iter().enumerate() {
            let Some(session) = self.clusters.get(name) else {
                continue;
            };
            let session = Arc::clone(session);
            join.spawn(async move { (index, Self::overview_of(session).await) });
        }

        let mut slots: Vec<Option<ClusterOverview>> = vec![None; self.order.len()];
        while let Some(result) = join.join_next().await {
            match result {
                Ok((index, overview)) => slots[index] = Some(overview),
                Err(error) => tracing::warn!(%error, "cluster overview task failed"),
            }
        }

        slots.into_iter().flatten().collect()
    }

    pub async fn cluster(&self, name: &str) -> Option<ClusterOverview> {
        let session = self.clusters.get(name)?;
        Some(Self::overview_of(Arc::clone(session)).await)
    }

    async fn overview_of(session: Arc<dyn ClusterSession>) -> ClusterOverview {
        let identity = session.identity().clone();
        match timeout(
            *OVERVIEW_BUDGET,
            Self::load_overview(session, identity.clone()),
        )
        .await
        {
            Ok(overview) => overview,
            Err(_) => {
                tracing::warn!(cluster = %identity.name, "cluster overview timed out");
                ClusterOverview::offline(identity)
            }
        }
    }

    async fn load_overview(
        session: Arc<dyn ClusterSession>,
        identity: ClusterIdentity,
    ) -> ClusterOverview {
        let metadata = session.metadata();
        let groups = session.consumer_groups();
        match tokio::join!(metadata, groups) {
            (Ok(meta), groups) => {
                let group_count = groups.map(|groups| groups.len() as i32).unwrap_or(0);
                assemble_overview(identity, &meta, group_count)
            }
            (Err(error), _) => {
                tracing::warn!(cluster = %identity.name, %error, "cluster metadata failed");
                ClusterOverview::offline(identity)
            }
        }
    }

    pub async fn brokers(&self, cluster: &str) -> Result<Vec<Broker>, KafkaError> {
        let session = self.session(cluster)?;
        Ok(assemble_brokers(&session.metadata().await?))
    }

    pub async fn broker(&self, cluster: &str, id: i32) -> Result<Broker, KafkaError> {
        self.brokers(cluster)
            .await?
            .into_iter()
            .find(|broker| broker.id == id)
            .ok_or_else(|| KafkaError::UnknownBroker {
                cluster: cluster.to_owned(),
                id,
            })
    }

    pub async fn broker_configs(
        &self,
        cluster: &str,
        id: i32,
    ) -> Result<Vec<ConfigEntry>, KafkaError> {
        let session = self.session(cluster)?;
        if session.metadata().await?.broker(id).is_none() {
            return Err(KafkaError::UnknownBroker {
                cluster: cluster.to_owned(),
                id,
            });
        }
        session.broker_configs(id).await
    }

    pub async fn topics(&self, cluster: &str) -> Result<Vec<Topic>, KafkaError> {
        let session = self.session(cluster)?;
        let meta = session.metadata().await?;
        let configs = session
            .topic_configs(&meta.topic_names())
            .await
            .unwrap_or_default();
        let groups = session.consumer_groups().await.unwrap_or_default();
        let fetch_watermarks = should_fetch_list_watermarks(&meta);

        let mut topics = Vec::with_capacity(meta.topics.len());
        for topic in &meta.topics {
            let partitions: Vec<i32> = topic
                .partitions
                .iter()
                .map(|partition| partition.id)
                .collect();
            let watermarks = if fetch_watermarks {
                session
                    .watermarks(&topic.name, &partitions)
                    .await
                    .unwrap_or_default()
            } else {
                HashMap::new()
            };
            topics.push(assemble_topic(
                topic,
                &watermarks,
                configs.get(&topic.name).map(Vec::as_slice),
                groups_for_topic(&topic.name, &groups),
            ));
        }

        Ok(topics)
    }

    pub async fn topic(&self, cluster: &str, name: &str) -> Result<Topic, KafkaError> {
        let session = self.session(cluster)?;
        let meta = session.metadata().await?;
        let topic = meta.topic(name).ok_or_else(|| KafkaError::UnknownTopic {
            cluster: cluster.to_owned(),
            topic: name.to_owned(),
        })?;
        let partitions: Vec<i32> = topic
            .partitions
            .iter()
            .map(|partition| partition.id)
            .collect();
        let watermarks = session
            .watermarks(name, &partitions)
            .await
            .unwrap_or_default();
        let configs = session
            .topic_configs(&[name.to_owned()])
            .await
            .unwrap_or_default();
        let groups = session.consumer_groups().await.unwrap_or_default();

        Ok(assemble_topic(
            topic,
            &watermarks,
            configs.get(name).map(Vec::as_slice),
            groups_for_topic(name, &groups),
        ))
    }

    pub async fn topic_configs(
        &self,
        cluster: &str,
        name: &str,
    ) -> Result<Vec<ConfigEntry>, KafkaError> {
        let session = self.session(cluster)?;
        if session.metadata().await?.topic(name).is_none() {
            return Err(KafkaError::UnknownTopic {
                cluster: cluster.to_owned(),
                topic: name.to_owned(),
            });
        }

        session
            .topic_configs(&[name.to_owned()])
            .await
            .map(|mut configs| configs.remove(name).unwrap_or_default())
    }

    pub async fn consumer_groups(&self, cluster: &str) -> Result<Vec<ConsumerGroup>, KafkaError> {
        let session = self.session(cluster)?;
        let mut snapshots = session.consumer_groups().await?;
        Self::hydrate_committed_offsets(session, &mut snapshots).await;
        let ends = self.end_offsets(session.as_ref(), &snapshots).await;
        Ok(snapshots
            .iter()
            .map(|group| assemble_group(group, &ends))
            .collect())
    }

    async fn hydrate_committed_offsets(
        session: &Arc<dyn ClusterSession>,
        groups: &mut [GroupSnapshot],
    ) {
        for chunk in groups.chunks_mut(*OFFSET_FETCH_BATCH) {
            let mut join = JoinSet::new();
            for (offset, group) in chunk.iter().enumerate() {
                let partitions = group.assigned_partitions();
                if partitions.is_empty() {
                    continue;
                }

                let session = Arc::clone(session);
                let group_id = group.id.clone();
                join.spawn(async move {
                    let committed = session
                        .committed_offsets(&group_id, &partitions)
                        .await
                        .unwrap_or_default();
                    (offset, committed)
                });
            }

            while let Some(result) = join.join_next().await {
                if let Ok((offset, committed)) = result {
                    chunk[offset].committed = committed;
                }
            }
        }
    }

    pub async fn consumer_group(
        &self,
        cluster: &str,
        id: &str,
    ) -> Result<ConsumerGroup, KafkaError> {
        self.consumer_groups(cluster)
            .await?
            .into_iter()
            .find(|group| group.id == id)
            .ok_or_else(|| KafkaError::UnknownGroup {
                cluster: cluster.to_owned(),
                id: id.to_owned(),
            })
    }

    async fn end_offsets(
        &self,
        session: &dyn ClusterSession,
        groups: &[GroupSnapshot],
    ) -> HashMap<(String, i32), i64> {
        let mut by_topic: HashMap<String, Vec<i32>> = HashMap::new();
        for group in groups {
            for (topic, partition) in group.assigned_partitions() {
                by_topic.entry(topic).or_default().push(partition);
            }
            for committed in &group.committed {
                by_topic
                    .entry(committed.topic.clone())
                    .or_default()
                    .push(committed.partition);
            }
        }

        let mut ends = HashMap::new();
        for (topic, mut partitions) in by_topic {
            partitions.sort();
            partitions.dedup();
            if let Ok(watermarks) = session.watermarks(&topic, &partitions).await {
                for (partition, Watermarks { high, .. }) in watermarks {
                    ends.insert((topic.clone(), partition), high);
                }
            }
        }
        ends
    }

    pub async fn records(&self, query: RecordQuery) -> Result<RecordPage, KafkaError> {
        let session = self.session(&query.cluster)?;
        let meta = session.metadata().await?;
        let topic = meta
            .topic(&query.topic)
            .ok_or_else(|| KafkaError::UnknownTopic {
                cluster: query.cluster.clone(),
                topic: query.topic.clone(),
            })?;

        if let Some(partition) = query.partition
            && topic.partition(partition).is_none()
        {
            return Err(KafkaError::UnknownPartition {
                cluster: query.cluster.clone(),
                topic: query.topic.clone(),
                partition,
            });
        }

        let limit = clamp_record_limit(query.limit).map_err(KafkaError::InvalidQuery)?;
        let page = clamp_record_page(query.page).map_err(KafkaError::InvalidQuery)?;
        let partitions: Vec<i32> = match query.partition {
            Some(id) => vec![id],
            None => topic
                .partitions
                .iter()
                .map(|partition| partition.id)
                .collect(),
        };
        let watermarks = session.watermarks(&query.topic, &partitions).await?;
        let plan = plan_records(&query, topic, &watermarks, limit, page);
        let records = session.records(&plan).await?;
        Ok(RecordPage {
            records,
            has_more: plan.has_more,
        })
    }

    pub async fn search(&self, cluster: &str, term: &str) -> Result<Vec<SearchHit>, KafkaError> {
        let session = self.session(cluster)?;
        let meta = session.metadata().await?;
        let groups = session.consumer_groups().await.unwrap_or_default();
        Ok(search_catalog(term, &meta.topics, &meta.brokers, &groups))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use async_trait::async_trait;

    use crate::kafka::error::KafkaError;
    use crate::kafka::model::{
        ClusterHealth, CommittedOffset, ConfigEntry, FetchPlan, MetadataSnapshot, Record,
        Watermarks,
    };
    use crate::kafka::testing::FakeCluster;

    struct Probe {
        inner: Arc<FakeCluster>,
        delay: Duration,
        group_lists: AtomicUsize,
        committed: AtomicUsize,
    }

    impl Probe {
        fn new(inner: Arc<FakeCluster>, delay: Duration) -> Arc<Self> {
            Arc::new(Self {
                inner,
                delay,
                group_lists: AtomicUsize::new(0),
                committed: AtomicUsize::new(0),
            })
        }
    }

    #[async_trait]
    impl ClusterSession for Probe {
        fn identity(&self) -> &ClusterIdentity {
            self.inner.identity()
        }

        async fn metadata(&self) -> Result<MetadataSnapshot, KafkaError> {
            if !self.delay.is_zero() {
                tokio::time::sleep(self.delay).await;
            }
            self.inner.metadata().await
        }

        async fn watermarks(
            &self,
            topic: &str,
            partitions: &[i32],
        ) -> Result<HashMap<i32, Watermarks>, KafkaError> {
            self.inner.watermarks(topic, partitions).await
        }

        async fn topic_configs(
            &self,
            topics: &[String],
        ) -> Result<HashMap<String, Vec<ConfigEntry>>, KafkaError> {
            self.inner.topic_configs(topics).await
        }

        async fn broker_configs(&self, broker_id: i32) -> Result<Vec<ConfigEntry>, KafkaError> {
            self.inner.broker_configs(broker_id).await
        }

        async fn consumer_groups(&self) -> Result<Vec<GroupSnapshot>, KafkaError> {
            self.group_lists.fetch_add(1, Ordering::SeqCst);
            if !self.delay.is_zero() {
                tokio::time::sleep(self.delay).await;
            }
            self.inner.consumer_groups().await
        }

        async fn committed_offsets(
            &self,
            group_id: &str,
            partitions: &[(String, i32)],
        ) -> Result<Vec<CommittedOffset>, KafkaError> {
            self.committed.fetch_add(1, Ordering::SeqCst);
            self.inner.committed_offsets(group_id, partitions).await
        }

        async fn records(&self, plan: &FetchPlan) -> Result<Vec<Record>, KafkaError> {
            self.inner.records(plan).await
        }
    }

    #[tokio::test]
    async fn cluster_list_keeps_config_order() {
        let engine = QueryEngine::from_sessions(vec![
            FakeCluster::named("prod"),
            FakeCluster::named("staging"),
        ]);

        let names: Vec<_> = engine
            .clusters()
            .await
            .into_iter()
            .map(|cluster| cluster.identity.name)
            .collect();

        assert_eq!(names, vec!["prod", "staging"]);
    }

    #[tokio::test]
    async fn cluster_list_does_not_fetch_committed_offsets() {
        let probe = Probe::new(FakeCluster::local(), Duration::ZERO);
        let engine =
            QueryEngine::from_sessions(vec![Arc::clone(&probe) as Arc<dyn ClusterSession>]);

        let overviews = engine.clusters().await;
        assert_eq!(overviews[0].consumer_group_count, 1);
        assert_eq!(probe.group_lists.load(Ordering::SeqCst), 1);
        assert_eq!(probe.committed.load(Ordering::SeqCst), 0);

        engine.consumer_groups("local").await.unwrap();
        assert_eq!(probe.committed.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn cluster_list_marks_slow_cluster_offline_without_blocking_others() {
        let slow = Probe::new(FakeCluster::named("slow"), Duration::from_secs(60));
        let fast = FakeCluster::named("fast");
        let engine =
            QueryEngine::from_sessions(vec![Arc::clone(&slow) as Arc<dyn ClusterSession>, fast]);

        let overviews = engine.clusters().await;
        assert_eq!(overviews[0].identity.name, "slow");
        assert_eq!(overviews[0].health, ClusterHealth::Offline);
        assert_eq!(overviews[1].identity.name, "fast");
        assert_eq!(overviews[1].health, ClusterHealth::Healthy);
    }
}
