use juniper::graphql_object;

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
            .clusters
            .list()
            .into_iter()
            .map(|client| Cluster::from_client(&client))
            .collect()
    }

    async fn cluster(context: &AppState, name: String) -> Option<Cluster> {
        context
            .clusters
            .get(&name)
            .map(|client| Cluster::from_client(&client))
    }

    async fn brokers(cluster: String) -> Vec<Broker> {
        let _ = cluster;
        Vec::new()
    }

    async fn broker(cluster: String, id: i32) -> Option<Broker> {
        let _ = (cluster, id);
        None
    }

    async fn broker_configs(cluster: String, id: i32) -> Vec<ConfigEntry> {
        let _ = (cluster, id);
        Vec::new()
    }

    async fn topics(cluster: String) -> Vec<Topic> {
        let _ = cluster;
        Vec::new()
    }

    async fn topic(cluster: String, name: String) -> Option<Topic> {
        let _ = (cluster, name);
        None
    }

    async fn topic_configs(cluster: String, name: String) -> Vec<ConfigEntry> {
        let _ = (cluster, name);
        Vec::new()
    }

    async fn consumer_groups(cluster: String) -> Vec<ConsumerGroup> {
        let _ = cluster;
        Vec::new()
    }

    async fn consumer_group(cluster: String, id: String) -> Option<ConsumerGroup> {
        let _ = (cluster, id);
        None
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

    async fn records(query: RecordQuery) -> Vec<TopicRecord> {
        let _ = query;
        Vec::new()
    }

    async fn search(cluster: String, term: String) -> Vec<SearchResult> {
        let _ = (cluster, term);
        Vec::new()
    }
}
