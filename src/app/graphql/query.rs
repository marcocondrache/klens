use juniper::{FieldResult, graphql_object};

use super::types::{
    Broker, CatalogHealth, Cluster, ClusterCatalog, ConfigEntry, ConsumerGroup, RecordPage,
    RecordQuery, SchemaSubject, SearchResult, ThroughputPoint, Topic,
};
use crate::AppState;

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

    async fn brokers(context: &AppState, cluster: String) -> FieldResult<Vec<Broker>> {
        let snapshot = context.catalog_snapshot(&cluster).await?;
        Ok(snapshot.brokers.iter().cloned().map(Broker::from).collect())
    }

    async fn broker(context: &AppState, cluster: String, id: i32) -> FieldResult<Option<Broker>> {
        let snapshot = context.catalog_snapshot(&cluster).await?;
        Ok(snapshot.broker(id).cloned().map(Broker::from))
    }

    async fn broker_configs(
        context: &AppState,
        cluster: String,
        id: i32,
    ) -> FieldResult<Vec<ConfigEntry>> {
        Ok(context
            .query
            .broker_configs(&cluster, id)
            .await?
            .into_iter()
            .map(ConfigEntry::from)
            .collect())
    }

    async fn topics(context: &AppState, cluster: String) -> FieldResult<Vec<Topic>> {
        let snapshot = context.catalog_snapshot(&cluster).await?;
        Ok(map_topics(context, &cluster, &snapshot.topics))
    }

    async fn cluster_catalog(context: &AppState, cluster: String) -> FieldResult<ClusterCatalog> {
        let snapshot = context.catalog_snapshot(&cluster).await?;
        Ok(ClusterCatalog {
            updated_at: snapshot.updated_at,
            topics: map_topics(context, &cluster, &snapshot.topics),
            consumer_groups: map_groups(&snapshot.groups),
        })
    }

    async fn catalog_health(context: &AppState, cluster: String) -> FieldResult<CatalogHealth> {
        let _ = context.query.session(&cluster)?;
        Ok(CatalogHealth::from(context.catalog_health(&cluster)))
    }

    async fn topic(
        context: &AppState,
        cluster: String,
        name: String,
    ) -> FieldResult<Option<Topic>> {
        Ok(context
            .catalog_snapshot(&cluster)
            .await?
            .topic(&name)
            .cloned()
            .map(|topic| map_topic(context, &cluster, topic)))
    }

    async fn topic_configs(
        context: &AppState,
        cluster: String,
        name: String,
    ) -> FieldResult<Vec<ConfigEntry>> {
        Ok(context
            .query
            .topic_configs(&cluster, &name)
            .await?
            .into_iter()
            .map(ConfigEntry::from)
            .collect())
    }

    async fn consumer_groups(
        context: &AppState,
        cluster: String,
        topic: Option<String>,
    ) -> FieldResult<Vec<ConsumerGroup>> {
        let snapshot = context.catalog_snapshot(&cluster).await?;
        Ok(map_groups(&snapshot.groups_for_topic(topic.as_deref())))
    }

    async fn consumer_group(
        context: &AppState,
        cluster: String,
        id: String,
    ) -> FieldResult<Option<ConsumerGroup>> {
        Ok(context
            .catalog_snapshot(&cluster)
            .await?
            .group(&id)
            .cloned()
            .map(ConsumerGroup::from))
    }

    async fn cluster_throughput(context: &AppState, cluster: String) -> Vec<ThroughputPoint> {
        context
            .rates
            .cluster_history(&cluster)
            .into_iter()
            .map(ThroughputPoint::from)
            .collect()
    }

    async fn topic_throughput(
        context: &AppState,
        cluster: String,
        topic: String,
    ) -> Vec<ThroughputPoint> {
        context
            .rates
            .topic_history(&cluster, &topic)
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
            .lags
            .history(&cluster, &id)
            .into_iter()
            .map(ThroughputPoint::from)
            .collect()
    }

    async fn schema_subjects(
        context: &AppState,
        cluster: String,
    ) -> FieldResult<Vec<SchemaSubject>> {
        Ok(context
            .subject_snapshot(&cluster)
            .await?
            .iter()
            .cloned()
            .map(SchemaSubject::from)
            .collect())
    }

    async fn records(context: &AppState, query: RecordQuery) -> FieldResult<RecordPage> {
        let cluster = query.cluster.clone();
        let query = query
            .try_into()
            .map_err(crate::kafka::KafkaError::InvalidQuery)?;
        Ok(RecordPage::from(
            context.query.records(&cluster, query).await?,
        ))
    }

    async fn search(
        context: &AppState,
        cluster: String,
        term: String,
    ) -> FieldResult<Vec<SearchResult>> {
        let snapshot = context.catalog_snapshot(&cluster).await?;
        let subjects = context.subject_snapshot(&cluster).await.unwrap_or_default();
        Ok(snapshot
            .search(&term, subjects.as_ref())
            .into_iter()
            .map(SearchResult::from)
            .collect())
    }
}

fn map_topics(context: &AppState, cluster: &str, topics: &[crate::kafka::Topic]) -> Vec<Topic> {
    topics
        .iter()
        .cloned()
        .map(|topic| map_topic(context, cluster, topic))
        .collect()
}

fn map_topic(context: &AppState, cluster: &str, topic: crate::kafka::Topic) -> Topic {
    let rate = context.rates.topic_rate(cluster, &topic.name);
    Topic::from_domain(topic, rate.as_ref())
}

fn map_groups(groups: &[crate::kafka::ConsumerGroup]) -> Vec<ConsumerGroup> {
    groups.iter().cloned().map(ConsumerGroup::from).collect()
}
