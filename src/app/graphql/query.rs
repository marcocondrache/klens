use juniper::{FieldResult, graphql_object};

use super::types::{
    Acl, Broker, Cluster, ConfigEntry, ConsumerGroup, RecordPage, RecordQuery, SchemaSubject,
    SearchResult, ThroughputPoint, Topic,
};
use crate::AppState;

pub struct Query;

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
        let topics = context.query.topics(&cluster).await?;
        Ok(topics
            .into_iter()
            .map(|topic| {
                let rate = context.rates.topic_rate(&cluster, &topic.name);
                Topic::from_domain(topic, rate.as_ref())
            })
            .collect())
    }

    async fn topic(
        context: &AppState,
        cluster: String,
        name: String,
    ) -> FieldResult<Option<Topic>> {
        match context.query.topic(&cluster, &name).await {
            Ok(topic) => {
                let rate = context.rates.topic_rate(&cluster, &topic.name);
                Ok(Some(Topic::from_domain(topic, rate.as_ref())))
            }
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

    async fn schema_subjects(
        context: &AppState,
        cluster: String,
    ) -> FieldResult<Vec<SchemaSubject>> {
        Ok(context
            .query
            .schema_subjects(&cluster)
            .await?
            .into_iter()
            .map(SchemaSubject::from)
            .collect())
    }

    async fn acls(cluster: String) -> Vec<Acl> {
        let _ = cluster;
        Vec::new()
    }

    async fn records(context: &AppState, query: RecordQuery) -> FieldResult<RecordPage> {
        Ok(RecordPage::from(context.query.records(query.into()).await?))
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
