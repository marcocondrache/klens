use std::collections::HashMap;
use std::sync::Arc;

use futures::future::try_join_all;
use krafka::admin::{
    AclFilter, AdminClient as KrafkaAdmin, ConsumerGroupDescription, ConsumerGroupListing,
    DescribeAclsResult, DescribeConfigsRequest, DescribeConfigsResourceResult, GroupListing,
    GroupOffsetEntry, ListOffsetResult, OffsetSpec, OffsetVisibility,
};
use krafka::metadata::ClusterMetadata;
use tokio::sync::Semaphore;

use crate::environment::{ADMIN_FAN_CONCURRENCY, GROUP_DESCRIBE_CHUNK};
use crate::kafka::error::KafkaError;

pub(super) struct AdminFan {
    admin: KrafkaAdmin,
    metadata: Arc<ClusterMetadata>,
    permits: Semaphore,
}

impl AdminFan {
    pub(super) fn new(admin: KrafkaAdmin, metadata: Arc<ClusterMetadata>) -> Self {
        Self {
            admin,
            metadata,
            permits: Semaphore::new((*ADMIN_FAN_CONCURRENCY).max(1)),
        }
    }

    pub(super) async fn list_offsets(
        &self,
        topics: &HashMap<String, Vec<i32>>,
        spec: OffsetSpec,
    ) -> Result<Vec<ListOffsetResult>, KafkaError> {
        let shards = self.shard_by_leader(topics);
        let calls = shards.into_iter().map(|topics| async move {
            let _permit = self.permit().await;
            let query: Vec<(&str, &[i32])> = topics
                .iter()
                .map(|(topic, partitions)| (topic.as_str(), partitions.as_slice()))
                .collect();
            self.admin.list_offsets(&query, spec).await
        });

        Ok(try_join_all(calls).await?.into_iter().flatten().collect())
    }

    fn shard_by_leader(&self, wanted: &HashMap<String, Vec<i32>>) -> Vec<Vec<(String, Vec<i32>)>> {
        let mut by_leader: HashMap<Option<i32>, HashMap<String, Vec<i32>>> = HashMap::new();
        for (topic, partitions) in wanted {
            for &partition in partitions {
                let leader = self.metadata.leader(topic, partition).filter(|&id| id >= 0);
                by_leader
                    .entry(leader)
                    .or_default()
                    .entry(topic.clone())
                    .or_default()
                    .push(partition);
            }
        }
        by_leader
            .into_values()
            .map(|topics| topics.into_iter().collect())
            .collect()
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

    pub(super) async fn list_consumer_groups(
        &self,
        listing: &GroupListing,
    ) -> Result<Vec<ConsumerGroupListing>, KafkaError> {
        Ok(self.admin.list_consumer_groups(listing).await?)
    }

    pub(super) async fn describe_consumer_group_offsets(
        &self,
        group_id: &str,
        query: Option<&[(&str, &[i32])]>,
        visibility: OffsetVisibility,
    ) -> Result<Vec<GroupOffsetEntry>, KafkaError> {
        Ok(self
            .admin
            .describe_consumer_group_offsets(group_id, query, visibility)
            .await?)
    }

    pub(super) async fn describe_configs(
        &self,
        request: DescribeConfigsRequest,
    ) -> Result<Vec<DescribeConfigsResourceResult>, KafkaError> {
        Ok(self.admin.describe_configs_per_resource(request).await?)
    }

    pub(super) async fn describe_acls(
        &self,
        filter: AclFilter,
    ) -> Result<DescribeAclsResult, krafka::error::KrafkaError> {
        self.admin.describe_acls(filter).await
    }

    #[cfg(test)]
    pub(super) async fn close(&self) {
        self.admin.close().await;
    }

    #[cfg(test)]
    pub(super) fn request_timeout(&self) -> std::time::Duration {
        self.admin.request_timeout()
    }

    #[cfg(test)]
    pub(super) async fn alter_consumer_group_offsets(
        &self,
        group_id: &str,
        topic_offsets: &[(&str, &[(i32, i64)])],
    ) -> Result<Vec<krafka::admin::AlterGroupOffsetResult>, KafkaError> {
        Ok(self
            .admin
            .alter_consumer_group_offsets(group_id, topic_offsets)
            .await?)
    }

    async fn permit(&self) -> tokio::sync::SemaphorePermit<'_> {
        self.permits.acquire().await.expect("admin fan permits")
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use krafka::client::KrafkaClient as KrafkaSharedClient;
    use krafka::protocol::ApiKey;
    use krafka::testing::{Control, FakeBroker};

    use super::*;

    async fn fan(broker: &FakeBroker) -> AdminFan {
        let client = KrafkaSharedClient::builder(broker.bootstrap_servers())
            .request_timeout(Duration::from_secs(10))
            .connect_timeout(Duration::from_secs(5))
            .build()
            .await
            .expect("shared client");
        client.metadata().refresh().await.expect("metadata");
        let admin = KrafkaAdmin::builder()
            .with_client(&client)
            .request_timeout(Duration::from_secs(10))
            .connect_timeout(Duration::from_secs(5))
            .build()
            .await
            .expect("admin client");
        AdminFan::new(admin, Arc::clone(client.metadata()))
    }

    #[tokio::test]
    async fn list_offsets_shards_by_leader_and_overlaps_the_leaders() {
        let cluster = FakeBroker::start_cluster(3).await.expect("fake cluster");
        assert!(cluster.create_topic("orders", 3));
        for leader in 0..3 {
            assert!(cluster.set_leader("orders", leader, leader));
        }

        let fan = fan(&cluster).await;
        cluster.clear_requests();
        cluster.on(ApiKey::ListOffsets, |_| {
            Control::Delay(Duration::from_millis(200))
        });

        let wanted = HashMap::from([("orders".to_owned(), vec![0, 1, 2])]);
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
        fan.close().await;
    }

    #[tokio::test]
    async fn list_offsets_batches_topics_that_share_a_leader() {
        let cluster = FakeBroker::start_cluster(3).await.expect("fake cluster");
        let topics = ["orders", "payments", "shipments"];
        for topic in topics {
            assert!(cluster.create_topic(topic, 1));
            assert!(cluster.set_leader(topic, 0, 0));
        }

        let fan = fan(&cluster).await;
        cluster.clear_requests();

        let wanted: HashMap<String, Vec<i32>> = topics
            .iter()
            .map(|topic| ((*topic).to_owned(), vec![0]))
            .collect();
        let listed = fan
            .list_offsets(&wanted, OffsetSpec::Latest)
            .await
            .expect("list offsets");

        assert_eq!(listed.len(), 3);
        assert_eq!(cluster.request_count(ApiKey::ListOffsets), 1);
        fan.close().await;
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

        fan.close().await;
    }
}
