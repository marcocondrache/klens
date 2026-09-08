use futures::stream::{self, BoxStream};
use juniper::{FieldResult, graphql_subscription};

use super::types::TopicRate;
use crate::AppState;
use crate::environment::SAMPLE_INTERVAL;

pub struct Subscription;

type TopicRateStream = BoxStream<'static, FieldResult<Vec<TopicRate>>>;

#[graphql_subscription(context = AppState)]
impl Subscription {
    async fn topic_rates(context: &AppState, cluster: String) -> TopicRateStream {
        let state = context.clone();
        Box::pin(stream::unfold(
            (state, cluster, true),
            |(state, cluster, first)| async move {
                if !first {
                    tokio::time::sleep(*SAMPLE_INTERVAL).await;
                }
                let item = sample_topic_rates(&state, &cluster).await;
                Some((item, (state, cluster, false)))
            },
        ))
    }
}

async fn sample_topic_rates(state: &AppState, cluster: &str) -> FieldResult<Vec<TopicRate>> {
    let counts = state.query.topic_message_counts(cluster).await?;
    state.rates.observe(cluster, counts);
    Ok(state
        .rates
        .topic_rates(cluster)
        .into_iter()
        .map(TopicRate::from)
        .collect())
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

        async fn watermarks(
            &self,
            topic: &str,
            partitions: &[i32],
        ) -> Result<HashMap<i32, Watermarks>, KafkaError> {
            let extra = self.extra.fetch_add(10, Ordering::SeqCst);
            let mut marks = self.inner.watermarks(topic, partitions).await?;
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

        async fn topic_configs(
            &self,
            topics: &[&str],
        ) -> Result<HashMap<String, Vec<ConfigEntry>>, KafkaError> {
            self.inner.topic_configs(topics).await
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

    #[tokio::test(start_paused = true)]
    async fn topic_rates_subscription_emits_watermark_delta() {
        let state = AppState::new(Arc::new(QueryEngine::from_sessions(vec![
            GrowingCluster::new(),
        ])));
        let coordinator = Coordinator::new(schema());
        let request: GraphQLRequest = serde_json::from_str(
            r#"{ "query": "subscription { topicRates(cluster: \"local\") { name messagesPerSec } }" }"#,
        )
        .unwrap();

        let mut stream = coordinator.subscribe(&request, &state).await.unwrap();

        let first = stream.next().await.unwrap();
        let first = serde_json::to_value(first).unwrap();
        assert_eq!(first["data"]["topicRates"][0]["name"], "orders.created");
        assert_eq!(first["data"]["topicRates"][0]["messagesPerSec"], 0.0);

        tokio::time::advance(*SAMPLE_INTERVAL + Duration::from_millis(1)).await;

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
}
