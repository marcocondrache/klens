use juniper::graphql_object;

use super::context::GraphQlContext;
use super::error::GqlError;
use super::types::{
    AclListing, Broker, CatalogHealth, Cluster, ClusterCatalog, ConfigEntry, ConsumerGroup,
    RecordPage, RecordQuery, SchemaSubject, SearchResult, SearchResults, ThroughputPoint, Topic,
};
use crate::app::auth::access::Privilege;
use crate::kafka::KafkaError;

pub struct Query;

#[graphql_object(context = GraphQlContext)]
impl Query {
    async fn clusters(context: &GraphQlContext) -> Vec<Cluster> {
        context
            .cluster_overviews()
            .await
            .into_iter()
            .filter(|overview| context.access.can_see_cluster(&overview.identity.name))
            .map(Cluster::from)
            .collect()
    }

    async fn cluster(context: &GraphQlContext, name: String) -> Option<Cluster> {
        if !context.access.can_see_cluster(&name) {
            return None;
        }
        context.cluster_overview(&name).await.map(Cluster::from)
    }

    async fn brokers(context: &GraphQlContext, cluster: String) -> Result<Vec<Broker>, KafkaError> {
        context.allow_cluster(&cluster)?;
        let snapshot = context.catalog_snapshot(&cluster).await?;
        Ok(snapshot.brokers.iter().cloned().map(Broker::from).collect())
    }

    async fn broker(
        context: &GraphQlContext,
        cluster: String,
        id: i32,
    ) -> Result<Option<Broker>, KafkaError> {
        context.allow_cluster(&cluster)?;
        let snapshot = context.catalog_snapshot(&cluster).await?;
        Ok(snapshot.broker(id).cloned().map(Broker::from))
    }

    async fn broker_configs(
        context: &GraphQlContext,
        cluster: String,
        id: i32,
    ) -> Result<Vec<ConfigEntry>, GqlError> {
        context.allow_privilege(Privilege::LiveConfig, &cluster)?;
        Ok(context
            .live_broker_configs(&cluster, id)
            .await?
            .into_iter()
            .map(ConfigEntry::from)
            .collect())
    }

    async fn cluster_catalog(
        context: &GraphQlContext,
        cluster: String,
    ) -> Result<ClusterCatalog, KafkaError> {
        context.allow_cluster(&cluster)?;
        let snapshot = context.catalog_snapshot(&cluster).await?;
        Ok(ClusterCatalog {
            updated_at: snapshot.updated_at,
            topics: map_topics(context, &cluster, &snapshot.topics),
            consumer_groups: map_groups(&snapshot.groups),
        })
    }

    async fn catalog_health(
        context: &GraphQlContext,
        cluster: String,
    ) -> Result<CatalogHealth, KafkaError> {
        context.allow_cluster(&cluster)?;
        context.require_cluster(&cluster)?;
        Ok(CatalogHealth::from(context.catalog_health(&cluster)))
    }

    async fn topic(
        context: &GraphQlContext,
        cluster: String,
        name: String,
    ) -> Result<Option<Topic>, KafkaError> {
        context.allow_cluster(&cluster)?;
        Ok(context
            .catalog_snapshot(&cluster)
            .await?
            .topic(&name)
            .cloned()
            .map(|topic| map_topic(context, &cluster, topic)))
    }

    async fn topic_configs(
        context: &GraphQlContext,
        cluster: String,
        name: String,
    ) -> Result<Vec<ConfigEntry>, GqlError> {
        context.allow_privilege(Privilege::LiveConfig, &cluster)?;
        Ok(context
            .live_topic_configs(&cluster, &name)
            .await?
            .into_iter()
            .map(ConfigEntry::from)
            .collect())
    }

    async fn consumer_groups(
        context: &GraphQlContext,
        cluster: String,
        topic: Option<String>,
    ) -> Result<Vec<ConsumerGroup>, KafkaError> {
        context.allow_cluster(&cluster)?;
        let snapshot = context.catalog_snapshot(&cluster).await?;
        Ok(map_groups(&snapshot.groups_for_topic(topic.as_deref())))
    }

    async fn consumer_group(
        context: &GraphQlContext,
        cluster: String,
        id: String,
    ) -> Result<Option<ConsumerGroup>, KafkaError> {
        context.allow_cluster(&cluster)?;
        Ok(context
            .catalog_snapshot(&cluster)
            .await?
            .group(&id)
            .cloned()
            .map(ConsumerGroup::from))
    }

    async fn topic_throughput(
        context: &GraphQlContext,
        cluster: String,
        topic: String,
    ) -> Vec<ThroughputPoint> {
        if context.allow_cluster(&cluster).is_err() {
            return Vec::new();
        }
        context
            .series_topic_history(&cluster, &topic)
            .into_iter()
            .map(ThroughputPoint::from)
            .collect()
    }

    async fn group_lag_history(
        context: &GraphQlContext,
        cluster: String,
        id: String,
    ) -> Vec<ThroughputPoint> {
        if context.allow_cluster(&cluster).is_err() {
            return Vec::new();
        }
        context
            .series_group_lag_history(&cluster, &id)
            .into_iter()
            .map(ThroughputPoint::from)
            .collect()
    }

    async fn schema_subjects(
        context: &GraphQlContext,
        cluster: String,
    ) -> Result<Vec<SchemaSubject>, KafkaError> {
        context.allow_cluster(&cluster)?;
        let redact = !context.access.allows(Privilege::SchemaText, &cluster);
        Ok(context
            .subject_snapshot(&cluster)
            .await?
            .iter()
            .cloned()
            .map(|subject| {
                let mut mapped = SchemaSubject::from(subject);
                if redact {
                    mapped.schema.clear();
                }
                mapped
            })
            .collect())
    }

    async fn acls(context: &GraphQlContext, cluster: String) -> Result<AclListing, GqlError> {
        context.allow_privilege(Privilege::Acls, &cluster)?;
        Ok(AclListing::from(context.live_acls(&cluster).await?))
    }

    async fn records(context: &GraphQlContext, query: RecordQuery) -> Result<RecordPage, GqlError> {
        let cluster = query.cluster.clone();
        context.allow_privilege(Privilege::Records, &cluster)?;
        let query = query.try_into()?;
        Ok(RecordPage::from(
            context.live_records(&cluster, query).await?,
        ))
    }

    async fn search(
        context: &GraphQlContext,
        cluster: String,
        term: String,
    ) -> Result<SearchResults, KafkaError> {
        context.allow_cluster(&cluster)?;
        let search = context.catalog_search(&cluster, &term).await?;
        Ok(SearchResults {
            hits: search.hits.into_iter().map(SearchResult::from).collect(),
            schema_registry_error: search.schema_registry_error,
        })
    }
}

fn map_topics(
    context: &GraphQlContext,
    cluster: &str,
    topics: &[crate::kafka::Topic],
) -> Vec<Topic> {
    topics
        .iter()
        .cloned()
        .map(|topic| map_topic(context, cluster, topic))
        .collect()
}

fn map_topic(context: &GraphQlContext, cluster: &str, topic: crate::kafka::Topic) -> Topic {
    let rate = context.series_topic_rate(cluster, &topic.name);
    Topic::from_domain(topic, rate.as_ref())
}

fn map_groups(groups: &[crate::kafka::ConsumerGroup]) -> Vec<ConsumerGroup> {
    groups.iter().cloned().map(ConsumerGroup::from).collect()
}
