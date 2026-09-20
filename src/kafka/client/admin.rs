use std::collections::HashMap;

use futures::future::try_join_all;
use krafka::admin::{
    AdminClient as KrafkaAdmin, ConsumerGroupDescription, ListOffsetResult, OffsetSpec,
};
use tokio::sync::Semaphore;

use crate::environment::{ADMIN_FAN_CONCURRENCY, GROUP_DESCRIBE_CHUNK};
use crate::kafka::error::KafkaError;

pub(super) struct AdminFan {
    admin: KrafkaAdmin,
    permits: Semaphore,
}

impl AdminFan {
    pub(super) fn new(admin: KrafkaAdmin) -> Self {
        Self {
            admin,
            permits: Semaphore::new((*ADMIN_FAN_CONCURRENCY).max(1)),
        }
    }

    pub(super) fn admin(&self) -> &KrafkaAdmin {
        &self.admin
    }

    pub(super) async fn list_offsets(
        &self,
        topics: &HashMap<String, Vec<i32>>,
        spec: OffsetSpec,
    ) -> Result<Vec<ListOffsetResult>, KafkaError> {
        let shards = topics.iter().map(|(topic, partitions)| async move {
            let _permit = self.permit().await;
            self.admin
                .list_offsets(&[(topic.as_str(), partitions.as_slice())], spec)
                .await
        });

        Ok(try_join_all(shards).await?.into_iter().flatten().collect())
    }

    /// Coordinator lookups stay serial *within* a chunk, so the chunk size is
    /// what bounds the longest serial run.
    pub(super) async fn describe_groups(
        &self,
        ids: &[String],
    ) -> Result<Vec<ConsumerGroupDescription>, KafkaError> {
        let chunks = ids
            .chunks((*GROUP_DESCRIBE_CHUNK).max(1))
            .map(|chunk| async {
                let _permit = self.permit().await;
                self.admin.describe_consumer_groups(chunk.to_vec()).await
            });

        Ok(try_join_all(chunks).await?.into_iter().flatten().collect())
    }

    async fn permit(&self) -> tokio::sync::SemaphorePermit<'_> {
        self.permits.acquire().await.expect("admin fan permits")
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use krafka::protocol::ApiKey;
    use krafka::testing::{Control, FakeBroker};

    use super::*;

    async fn fan(broker: &FakeBroker) -> AdminFan {
        AdminFan::new(
            KrafkaAdmin::builder()
                .bootstrap_servers(broker.bootstrap_servers())
                .request_timeout(Duration::from_secs(10))
                .connect_timeout(Duration::from_secs(5))
                .build()
                .await
                .expect("admin client"),
        )
    }

    #[tokio::test]
    async fn list_offsets_shards_by_topic_and_overlaps_the_leaders() {
        let cluster = FakeBroker::start_cluster(3).await.expect("fake cluster");
        let topics = ["orders", "payments", "shipments"];
        for (leader, topic) in topics.iter().enumerate() {
            assert!(cluster.create_topic(topic, 1));
            assert!(cluster.set_leader(topic, 0, leader as i32));
        }

        let fan = fan(&cluster).await;
        cluster.clear_requests();
        cluster.on(ApiKey::ListOffsets, |_| {
            Control::Delay(Duration::from_millis(200))
        });

        let wanted: HashMap<String, Vec<i32>> = topics
            .iter()
            .map(|topic| ((*topic).to_owned(), vec![0]))
            .collect();
        let started = std::time::Instant::now();
        let listed = fan
            .list_offsets(&wanted, OffsetSpec::Latest)
            .await
            .expect("list offsets");

        assert_eq!(listed.len(), 3);
        assert_eq!(cluster.request_count(ApiKey::ListOffsets), 3);
        assert!(
            started.elapsed() < Duration::from_millis(500),
            "three 200 ms leaders must answer in one wave: {:?}",
            started.elapsed()
        );
        fan.admin().close().await;
    }

    #[tokio::test]
    async fn an_empty_request_costs_no_round_trip() {
        let broker = FakeBroker::start().await.expect("fake broker");
        let fan = fan(&broker).await;
        broker.clear_requests();

        assert!(
            fan.list_offsets(&HashMap::new(), OffsetSpec::Latest)
                .await
                .expect("list offsets")
                .is_empty()
        );
        assert!(fan.describe_groups(&[]).await.expect("describe").is_empty());
        assert!(broker.requests().is_empty());

        fan.admin().close().await;
    }
}
