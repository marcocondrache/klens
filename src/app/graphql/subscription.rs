use futures::StreamExt;
use futures::stream::{self, BoxStream};
use juniper::{FieldError, FieldResult, graphql_subscription};

use super::types::{CatalogUpdated, ConsumerGroup, TopicRate};
use crate::AppState;
use crate::app::sampler::SamplerMap;

pub struct Subscription;

type TopicRateStream = BoxStream<'static, FieldResult<Vec<TopicRate>>>;
type ConsumerGroupStream = BoxStream<'static, FieldResult<ConsumerGroup>>;
type CatalogUpdatedStream = BoxStream<'static, FieldResult<CatalogUpdated>>;

/// Shared samples must be [`Clone`], which [`FieldError`] is not, so failures
/// travel as a message and are rebuilt per subscriber.
type Sample<T> = Result<T, String>;

#[derive(Default)]
pub(crate) struct Samplers {
    topic_rates: SamplerMap<String, Sample<Vec<TopicRate>>>,
    group_lag: SamplerMap<(String, String), Sample<ConsumerGroup>>,
}

#[graphql_subscription(context = AppState)]
impl Subscription {
    async fn topic_rates(context: &AppState, cluster: String) -> TopicRateStream {
        let sampler = context.samplers.topic_rates.attach(cluster.clone(), {
            let state = context.clone();
            move || {
                let state = state.clone();
                let cluster = cluster.clone();
                async move { sample_topic_rates(&state, &cluster).await }
            }
        });

        Box::pin(sampler.stream().map(reported))
    }

    async fn consumer_group_lag(
        context: &AppState,
        cluster: String,
        id: String,
    ) -> ConsumerGroupStream {
        let key = (cluster.clone(), id.clone());
        let sampler = context.samplers.group_lag.attach(key, {
            let state = context.clone();
            move || {
                let state = state.clone();
                let cluster = cluster.clone();
                let id = id.clone();
                async move { sample_consumer_group_lag(&state, &cluster, &id).await }
            }
        });

        Box::pin(sampler.stream().map(reported))
    }

    async fn catalog_updated(context: &AppState, cluster: String) -> CatalogUpdatedStream {
        let mut updates = context.catalog.subscribe_updates(&cluster);
        let _ = updates.borrow_and_update();

        Box::pin(stream::unfold(
            (updates, cluster),
            |(mut updates, cluster)| async move {
                loop {
                    updates.changed().await.ok()?;
                    let Some(revision) = updates.borrow_and_update().clone() else {
                        continue;
                    };
                    if revision.cluster == cluster {
                        return Some((Ok(CatalogUpdated::from(revision)), (updates, cluster)));
                    }
                }
            },
        ))
    }
}

fn reported<T>(sample: Sample<T>) -> FieldResult<T> {
    sample.map_err(FieldError::from)
}

async fn sample_topic_rates(state: &AppState, cluster: &str) -> Sample<Vec<TopicRate>> {
    Ok(state
        .rates
        .topic_rates(cluster)
        .into_iter()
        .map(TopicRate::from)
        .collect())
}

async fn sample_consumer_group_lag(
    state: &AppState,
    cluster: &str,
    id: &str,
) -> Sample<ConsumerGroup> {
    let group = state
        .query
        .consumer_group(cluster, id)
        .await
        .map_err(|error| error.to_string())?;
    state.lags.observe(cluster, id, group.lag);
    Ok(ConsumerGroup::from(group))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicI64, Ordering};
    use std::time::Duration;

    use async_trait::async_trait;
    use futures::StreamExt;
    use juniper::{EmptyMutation, RootNode, SubscriptionCoordinator, http::GraphQLRequest};
    use juniper_subscriptions::Coordinator;

    use super::*;
    use crate::app::graphql::query::Query;
    use crate::environment::SAMPLE_INTERVAL;
    use crate::kafka::model::{
        ClusterIdentity, CommittedOffset, ConfigEntry, FetchPlan, GroupSnapshot, MetadataSnapshot,
        Record, Watermarks,
    };
    use crate::kafka::{ClusterSession, FakeCluster, KafkaError, QueryEngine};

    struct GrowingCluster {
        inner: FakeCluster,
        extra: AtomicI64,
    }

    impl GrowingCluster {
        fn new() -> Self {
            Self {
                inner: FakeCluster::local(),
                extra: AtomicI64::new(0),
            }
        }
    }

    #[async_trait]
    impl ClusterSession for GrowingCluster {
        fn identity(&self) -> &ClusterIdentity {
            self.inner.identity()
        }

        async fn metadata(&self) -> Result<MetadataSnapshot, KafkaError> {
            self.inner.metadata().await
        }

        async fn watermarks(&self, topic: &str) -> Result<HashMap<i32, Watermarks>, KafkaError> {
            let extra = self.extra.fetch_add(10, Ordering::SeqCst);
            let mut marks = self.inner.watermarks(topic).await?;
            if let Some(partition) = marks.get_mut(&0) {
                partition.high += extra;
            }
            Ok(marks)
        }

        async fn offsets_for_times(
            &self,
            topic: &str,
            partitions: &[i32],
            timestamp: i64,
        ) -> Result<HashMap<i32, Option<i64>>, KafkaError> {
            self.inner
                .offsets_for_times(topic, partitions, timestamp)
                .await
        }

        async fn topics_configs(
            &self,
            topics: &[&str],
        ) -> Result<HashMap<String, Vec<ConfigEntry>>, KafkaError> {
            self.inner.topics_configs(topics).await
        }

        async fn broker_configs(&self, broker_id: i32) -> Result<Vec<ConfigEntry>, KafkaError> {
            self.inner.broker_configs(broker_id).await
        }

        async fn consumer_groups(&self) -> Result<Vec<GroupSnapshot>, KafkaError> {
            self.inner.consumer_groups().await
        }

        async fn committed_offsets(
            &self,
            group_id: &str,
            partitions: &[(String, i32)],
        ) -> Result<Vec<CommittedOffset>, KafkaError> {
            self.inner.committed_offsets(group_id, partitions).await
        }

        async fn records(&self, plan: &FetchPlan) -> Result<Vec<Record>, KafkaError> {
            self.inner.records(plan).await
        }
    }

    fn schema() -> RootNode<Query, EmptyMutation<AppState>, Subscription> {
        RootNode::new(Query, EmptyMutation::<AppState>::new(), Subscription)
    }

    async fn wait_until(mut predicate: impl FnMut() -> bool) {
        for _ in 0..200 {
            if predicate() {
                return;
            }
            tokio::task::yield_now().await;
        }
        panic!("condition not met");
    }

    fn catalog_updated_request() -> GraphQLRequest {
        serde_json::from_str(
            r#"{ "query": "subscription { catalogUpdated(cluster: \"local\") { cluster generation } }" }"#,
        )
        .unwrap()
    }

    fn topic_rates_request() -> GraphQLRequest {
        serde_json::from_str(
            r#"{ "query": "subscription { topicRates(cluster: \"local\") { name messagesPerSec } }" }"#,
        )
        .unwrap()
    }

    #[tokio::test]
    async fn catalog_updated_subscription_skips_watermark_only_stores() {
        let state = AppState::new(Arc::new(QueryEngine::from_sessions(vec![
            FakeCluster::local(),
        ])));
        let coordinator = Coordinator::new(schema());
        let request = catalog_updated_request();
        let mut stream = coordinator.subscribe(&request, &state).await.unwrap();

        let first = state.catalog_snapshot("local").await.unwrap();
        tokio::task::yield_now().await;
        let first_event = tokio::time::timeout(Duration::from_millis(50), stream.next())
            .await
            .expect("first roster store notifies")
            .unwrap();
        let first_event = serde_json::to_value(first_event).unwrap();
        assert_eq!(first_event["data"]["catalogUpdated"]["cluster"], "local");
        assert_eq!(first_event["data"]["catalogUpdated"]["generation"], 1);

        let mut louder = (*first).clone();
        louder.topics[0].message_count += 10;
        louder.updated_at = first.updated_at;
        state.catalog.store("local", louder);
        assert!(
            tokio::time::timeout(Duration::from_millis(50), stream.next())
                .await
                .is_err()
        );

        let mut roster = (*first).clone();
        roster.topics.push(crate::kafka::Topic {
            name: "payments.settled".into(),
            internal: false,
            partitions: Vec::new(),
            replication_factor: 1,
            message_count: 0,
            cleanup_policy: crate::kafka::model::CleanupPolicy::Delete,
            retention_ms: 0,
            consumer_groups: Vec::new(),
            under_replicated: false,
        });
        state.catalog.store("local", roster);
        let second = tokio::time::timeout(Duration::from_millis(50), stream.next())
            .await
            .expect("roster change notifies")
            .unwrap();
        let second = serde_json::to_value(second).unwrap();
        assert_eq!(second["data"]["catalogUpdated"]["generation"], 2);
    }

    #[tokio::test(start_paused = true)]
    async fn topic_rates_subscription_emits_watermark_delta() {
        let state = AppState::new(Arc::new(QueryEngine::from_sessions(vec![
            GrowingCluster::new(),
        ])))
        .with_catalog_poller(Duration::from_secs(1));
        wait_until(|| state.catalog.snapshot("local").is_some()).await;
        wait_until(|| !state.rates.topic_rates("local").is_empty()).await;

        let coordinator = Coordinator::new(schema());
        let request = topic_rates_request();
        let mut stream = coordinator.subscribe(&request, &state).await.unwrap();

        let first = stream.next().await.unwrap();
        let first = serde_json::to_value(first).unwrap();
        assert_eq!(first["data"]["topicRates"][0]["name"], "orders.created");
        assert_eq!(first["data"]["topicRates"][0]["messagesPerSec"], 0.0);

        tokio::time::advance(Duration::from_secs(1) + Duration::from_millis(1)).await;
        wait_until(|| state.rates.cluster_history("local").len() >= 2).await;
        assert!(
            state
                .rates
                .topic_rate("local", "orders.created")
                .unwrap()
                .messages_per_sec
                > 0.0
        );

        tokio::time::advance(*SAMPLE_INTERVAL - Duration::from_secs(1) + Duration::from_millis(1))
            .await;

        let second = stream.next().await.unwrap();
        let second = serde_json::to_value(second).unwrap();
        assert_eq!(second["data"]["topicRates"][0]["name"], "orders.created");
        assert!(
            second["data"]["topicRates"][0]["messagesPerSec"]
                .as_f64()
                .unwrap()
                > 0.0
        );
    }

    #[tokio::test(start_paused = true)]
    async fn concurrent_subscribers_do_not_observe_rates() {
        let state = AppState::new(Arc::new(QueryEngine::from_sessions(vec![
            GrowingCluster::new(),
        ])))
        .with_catalog_poller(Duration::from_secs(1));
        wait_until(|| !state.rates.topic_rates("local").is_empty()).await;

        let coordinator = Coordinator::new(schema());
        let request = topic_rates_request();
        let mut first = coordinator.subscribe(&request, &state).await.unwrap();
        let mut second = coordinator.subscribe(&request, &state).await.unwrap();

        first.next().await.unwrap();
        second.next().await.unwrap();
        let after_first = state.rates.cluster_history("local").len();

        tokio::time::advance(Duration::from_secs(1) + Duration::from_millis(1)).await;
        wait_until(|| state.rates.cluster_history("local").len() > after_first).await;
        tokio::time::advance(*SAMPLE_INTERVAL - Duration::from_secs(1) + Duration::from_millis(1))
            .await;

        first.next().await.unwrap();
        second.next().await.unwrap();

        assert!(
            state.rates.cluster_history("local").len() < 4,
            "two subscribers must share poller samples, not observe on each WS tick"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn topic_rates_subscription_does_not_list_offsets() {
        let state = AppState::new(Arc::new(QueryEngine::from_sessions(vec![
            GrowingCluster::new(),
        ])));
        let coordinator = Coordinator::new(schema());
        let request = topic_rates_request();
        let mut stream = coordinator.subscribe(&request, &state).await.unwrap();

        let first = stream.next().await.unwrap();
        let first = serde_json::to_value(first).unwrap();
        assert_eq!(first["data"]["topicRates"], serde_json::json!([]));

        tokio::time::advance(*SAMPLE_INTERVAL + Duration::from_millis(1)).await;
        let second = stream.next().await.unwrap();
        let second = serde_json::to_value(second).unwrap();
        assert_eq!(second["data"]["topicRates"], serde_json::json!([]));
        assert!(state.rates.cluster_history("local").is_empty());
        assert!(state.catalog.snapshot("local").is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn consumer_group_lag_subscription_emits_updated_lag() {
        let state = AppState::new(Arc::new(QueryEngine::from_sessions(vec![
            GrowingCluster::new(),
        ])));
        let coordinator = Coordinator::new(schema());
        let request: GraphQLRequest = serde_json::from_str(
            r#"{ "query": "subscription { consumerGroupLag(cluster: \"local\", id: \"order-processor\") { id lag offsets { partition lag endOffset } } }" }"#,
        )
        .unwrap();

        let mut stream = coordinator.subscribe(&request, &state).await.unwrap();

        let first = stream.next().await.unwrap();
        let first = serde_json::to_value(first).unwrap();
        assert_eq!(first["data"]["consumerGroupLag"]["id"], "order-processor");
        let first_lag = first["data"]["consumerGroupLag"]["lag"].as_f64().unwrap();
        assert!(first_lag >= 0.0);

        tokio::time::advance(*SAMPLE_INTERVAL + Duration::from_millis(1)).await;

        let second = stream.next().await.unwrap();
        let second = serde_json::to_value(second).unwrap();
        assert_eq!(second["data"]["consumerGroupLag"]["id"], "order-processor");
        let second_lag = second["data"]["consumerGroupLag"]["lag"].as_f64().unwrap();
        assert!(
            second_lag > first_lag,
            "expected lag to grow after high watermarks advance (first={first_lag}, second={second_lag})"
        );
    }
}
