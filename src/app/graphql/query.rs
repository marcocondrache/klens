use std::sync::Arc;

use juniper::graphql_object;

use super::types::{
    Broker, CatalogHealth, Cluster, ClusterCatalog, ConfigEntry, ConsumerGroup, RecordPage,
    RecordQuery, SchemaSubject, SearchResult, SearchResults, ThroughputPoint, Topic,
};
use crate::AppState;
use crate::kafka::KafkaError;

pub struct Query;

#[graphql_object(context = AppState)]
impl Query {
    async fn clusters(context: &AppState) -> Vec<Cluster> {
        context
            .cluster_overviews()
            .await
            .into_iter()
            .map(Cluster::from)
            .collect()
    }

    async fn cluster(context: &AppState, name: String) -> Option<Cluster> {
        context.cluster_overview(&name).await.map(Cluster::from)
    }

    async fn brokers(context: &AppState, cluster: String) -> Result<Vec<Broker>, KafkaError> {
        let snapshot = context.catalog_snapshot(&cluster).await?;
        Ok(snapshot.brokers.iter().cloned().map(Broker::from).collect())
    }

    async fn broker(
        context: &AppState,
        cluster: String,
        id: i32,
    ) -> Result<Option<Broker>, KafkaError> {
        let snapshot = context.catalog_snapshot(&cluster).await?;
        Ok(snapshot.broker(id).cloned().map(Broker::from))
    }

    async fn broker_configs(
        context: &AppState,
        cluster: String,
        id: i32,
    ) -> Result<Vec<ConfigEntry>, KafkaError> {
        Ok(context
            .live_broker_configs(&cluster, id)
            .await?
            .into_iter()
            .map(ConfigEntry::from)
            .collect())
    }

    async fn cluster_catalog(
        context: &AppState,
        cluster: String,
    ) -> Result<ClusterCatalog, KafkaError> {
        let snapshot = context.catalog_snapshot(&cluster).await?;
        Ok(ClusterCatalog { snapshot, cluster })
    }

    async fn catalog_health(
        context: &AppState,
        cluster: String,
    ) -> Result<CatalogHealth, KafkaError> {
        context.require_cluster(&cluster)?;
        Ok(CatalogHealth::from(context.catalog_health(&cluster)))
    }

    async fn topic(
        context: &AppState,
        cluster: String,
        name: String,
    ) -> Result<Option<Topic>, KafkaError> {
        let snapshot = context.catalog_snapshot(&cluster).await?;
        Ok(snapshot
            .topics
            .iter()
            .position(|topic| topic.name == name)
            .map(|index| {
                let rate = context.series_topic_rate(&cluster, &snapshot.topics[index].name);
                Topic::from_snapshot(snapshot, index, rate)
            }))
    }

    async fn topic_configs(
        context: &AppState,
        cluster: String,
        name: String,
    ) -> Result<Vec<ConfigEntry>, KafkaError> {
        Ok(context
            .live_topic_configs(&cluster, &name)
            .await?
            .into_iter()
            .map(ConfigEntry::from)
            .collect())
    }

    async fn consumer_groups(
        context: &AppState,
        cluster: String,
        topic: Option<String>,
    ) -> Result<Vec<ConsumerGroup>, KafkaError> {
        let snapshot = context.catalog_snapshot(&cluster).await?;
        Ok(snapshot
            .groups
            .iter()
            .enumerate()
            .filter(|(_, group)| {
                topic
                    .as_deref()
                    .is_none_or(|name| group.topics.iter().any(|topic| topic == name))
            })
            .map(|(index, _)| ConsumerGroup::from_snapshot(Arc::clone(&snapshot), index))
            .collect())
    }

    async fn consumer_group(
        context: &AppState,
        cluster: String,
        id: String,
    ) -> Result<Option<ConsumerGroup>, KafkaError> {
        let snapshot = context.catalog_snapshot(&cluster).await?;
        Ok(snapshot
            .groups
            .iter()
            .position(|group| group.id == id)
            .map(|index| ConsumerGroup::from_snapshot(snapshot, index)))
    }

    async fn topic_throughput(
        context: &AppState,
        cluster: String,
        topic: String,
    ) -> Vec<ThroughputPoint> {
        context
            .series_topic_history(&cluster, &topic)
            .into_iter()
            .map(ThroughputPoint::from)
            .collect()
    }

    async fn group_lag_history(
        context: &AppState,
        cluster: String,
        id: String,
    ) -> Vec<ThroughputPoint> {
        context
            .series_group_lag_history(&cluster, &id)
            .into_iter()
            .map(ThroughputPoint::from)
            .collect()
    }

    async fn schema_subjects(
        context: &AppState,
        cluster: String,
    ) -> Result<Vec<SchemaSubject>, KafkaError> {
        Ok(context
            .subject_snapshot(&cluster)
            .await?
            .iter()
            .cloned()
            .map(SchemaSubject::from)
            .collect())
    }

    async fn records(context: &AppState, query: RecordQuery) -> Result<RecordPage, KafkaError> {
        let cluster = query.cluster.clone();
        let query = query.try_into()?;
        Ok(RecordPage::from(
            context.live_records(&cluster, query).await?,
        ))
    }

    async fn search(
        context: &AppState,
        cluster: String,
        term: String,
    ) -> Result<SearchResults, KafkaError> {
        let search = context.catalog_search(&cluster, &term).await?;
        Ok(SearchResults {
            hits: search.hits.into_iter().map(SearchResult::from).collect(),
            schema_registry_error: search.schema_registry_error,
        })
    }
}
