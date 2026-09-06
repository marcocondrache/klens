use juniper::{FieldResult, graphql_object};

use super::types::{
    Acl, Broker, Cluster, ConfigEntry, ConsumerGroup, RecordQuery, SchemaSubject, SearchResult,
    ThroughputPoint, Topic, TopicRecord,
};
use crate::AppState;

pub(super) struct Query;

#[graphql_object(context = AppState)]
impl Query {
    async fn clusters(context: &AppState) -> Vec<Cluster> {
        context
            .query
            .clusters()
            .await
            .into_iter()
            .map(Cluster::from)
            .collect()
    }

    async fn cluster(context: &AppState, name: String) -> Option<Cluster> {
        context.query.cluster(&name).await.map(Cluster::from)
    }

    async fn brokers(context: &AppState, cluster: String) -> FieldResult<Vec<Broker>> {
        Ok(context
            .query
            .brokers(&cluster)
            .await?
            .into_iter()
            .map(Broker::from)
            .collect())
    }

    async fn broker(context: &AppState, cluster: String, id: i32) -> FieldResult<Option<Broker>> {
        match context.query.broker(&cluster, id).await {
            Ok(broker) => Ok(Some(Broker::from(broker))),
            Err(crate::kafka::KafkaError::UnknownBroker { .. }) => Ok(None),
            Err(error) => Err(error.into()),
        }
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
        Ok(context
            .query
            .topics(&cluster)
            .await?
            .into_iter()
            .map(Topic::from)
            .collect())
    }

    async fn topic(
        context: &AppState,
        cluster: String,
        name: String,
    ) -> FieldResult<Option<Topic>> {
        match context.query.topic(&cluster, &name).await {
            Ok(topic) => Ok(Some(Topic::from(topic))),
            Err(crate::kafka::KafkaError::UnknownTopic { .. }) => Ok(None),
            Err(error) => Err(error.into()),
        }
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
    ) -> FieldResult<Vec<ConsumerGroup>> {
        Ok(context
            .query
            .consumer_groups(&cluster)
            .await?
            .into_iter()
            .map(ConsumerGroup::from)
            .collect())
    }

    async fn consumer_group(
        context: &AppState,
        cluster: String,
        id: String,
    ) -> FieldResult<Option<ConsumerGroup>> {
        match context.query.consumer_group(&cluster, &id).await {
            Ok(group) => Ok(Some(ConsumerGroup::from(group))),
            Err(crate::kafka::KafkaError::UnknownGroup { .. }) => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    async fn cluster_throughput(cluster: String) -> Vec<ThroughputPoint> {
        let _ = cluster;
        Vec::new()
    }

    async fn topic_throughput(cluster: String, topic: String) -> Vec<ThroughputPoint> {
        let _ = (cluster, topic);
        Vec::new()
    }

    async fn schema_subjects(cluster: String) -> Vec<SchemaSubject> {
        let _ = cluster;
        Vec::new()
    }

    async fn acls(cluster: String) -> Vec<Acl> {
        let _ = cluster;
        Vec::new()
    }

    async fn records(context: &AppState, query: RecordQuery) -> FieldResult<Vec<TopicRecord>> {
        Ok(context
            .query
            .records(query.into())
            .await?
            .into_iter()
            .map(TopicRecord::from)
            .collect())
    }

    async fn search(
        context: &AppState,
        cluster: String,
        term: String,
    ) -> FieldResult<Vec<SearchResult>> {
        Ok(context
            .query
            .search(&cluster, &term)
            .await?
            .into_iter()
            .map(SearchResult::from)
            .collect())
    }
}
