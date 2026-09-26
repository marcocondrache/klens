use async_trait::async_trait;
use foldhash::HashMap;

use crate::kafka::error::KafkaError;
use crate::kafka::group::CommittedOffset;
use crate::kafka::writes::ClusterWrites;

use super::{KafkaClient, partitions_by_topic};

#[async_trait]
impl ClusterWrites for KafkaClient {
    async fn commit_group_offsets(
        &self,
        group: &str,
        offsets: &[CommittedOffset],
    ) -> Result<(), KafkaError> {
        if offsets.is_empty() {
            return Ok(());
        }

        let mut by_topic: HashMap<&str, Vec<(i32, i64)>> = HashMap::default();
        for offset in offsets {
            by_topic
                .entry(offset.topic.as_str())
                .or_default()
                .push((offset.partition, offset.offset));
        }
        let request: Vec<(&str, &[(i32, i64)])> = by_topic
            .iter()
            .map(|(topic, partitions)| (*topic, partitions.as_slice()))
            .collect();

        let results = self
            .transport
            .admin
            .alter_consumer_group_offsets(group, &request)
            .await?;
        match results.into_iter().find_map(|result| result.error) {
            Some(error) => Err(rejection(&self.identity.name, group, &error)),
            None => Ok(()),
        }
    }

    async fn delete_group_offsets(
        &self,
        group: &str,
        partitions: &[(String, i32)],
    ) -> Result<(), KafkaError> {
        if partitions.is_empty() {
            return Ok(());
        }

        let by_topic = partitions_by_topic(partitions);
        let request: Vec<(&str, &[i32])> = by_topic
            .iter()
            .map(|(topic, partitions)| (topic.as_str(), partitions.as_slice()))
            .collect();

        let result = self
            .transport
            .admin
            .delete_consumer_group_offsets(group, &request)
            .await?;
        let partition_error = result
            .topics
            .into_iter()
            .flat_map(|topic| topic.partitions)
            .find_map(|partition| partition.error);
        match result.error.or(partition_error) {
            Some(error) => Err(rejection(&self.identity.name, group, &error)),
            None => Ok(()),
        }
    }
}

fn rejection(cluster: &str, group: &str, code: &str) -> KafkaError {
    match code {
        "NonEmptyGroup"
        | "UnknownMemberId"
        | "IllegalGeneration"
        | "RebalanceInProgress"
        | "GroupSubscribedToTopic" => KafkaError::GroupNotEmpty {
            cluster: cluster.to_owned(),
            group: group.to_owned(),
        },
        "GroupIdNotFound" => KafkaError::UnknownGroup {
            cluster: cluster.to_owned(),
            group: group.to_owned(),
        },
        denied if denied.ends_with("AuthorizationFailed") => KafkaError::Denied(denied.to_owned()),
        other => KafkaError::Admin(format!("group '{group}': {other}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kafka::client::tests::{kafka_client, produce_krafka};
    use crate::kafka::session::ClusterSession;

    #[test]
    fn a_live_group_is_reported_as_not_empty() {
        for code in [
            "NonEmptyGroup",
            "UnknownMemberId",
            "IllegalGeneration",
            "RebalanceInProgress",
            "GroupSubscribedToTopic",
        ] {
            assert_eq!(
                rejection("prod", "billing", code).code(),
                "GROUP_NOT_EMPTY",
                "{code}"
            );
        }
    }

    #[test]
    fn broker_authorization_failures_are_denials() {
        let group = rejection("prod", "billing", "GroupAuthorizationFailed");
        let topic = rejection("prod", "billing", "TopicAuthorizationFailed");

        assert_eq!(group.code(), "KAFKA_DENIED");
        assert_eq!(topic.code(), "KAFKA_DENIED");
        assert_eq!(
            group.to_string(),
            "kafka denied the request: GroupAuthorizationFailed"
        );
    }

    #[test]
    fn other_rejections_keep_the_broker_code() {
        assert_eq!(
            rejection("prod", "billing", "GroupIdNotFound").code(),
            "UNKNOWN_GROUP"
        );
        let other = rejection("prod", "billing", "CoordinatorNotAvailable");
        assert_eq!(other.code(), "ADMIN");
        assert_eq!(
            other.to_string(),
            "kafka admin request failed: group 'billing': CoordinatorNotAvailable"
        );
    }

    #[tokio::test]
    async fn commits_offsets_for_an_empty_group() {
        let broker = krafka::testing::FakeBroker::start()
            .await
            .expect("fake broker");
        assert!(broker.create_topic("orders", 2));
        produce_krafka(&broker.bootstrap_servers(), "orders", 4).await;

        let client = kafka_client(&broker.bootstrap_servers()).await;
        client
            .commit_group_offsets(
                "orders-group",
                &[
                    CommittedOffset {
                        topic: "orders".into(),
                        partition: 0,
                        offset: 1,
                    },
                    CommittedOffset {
                        topic: "orders".into(),
                        partition: 1,
                        offset: 0,
                    },
                ],
            )
            .await
            .expect("commit");

        let mut committed = client
            .committed_offsets("orders-group", None)
            .await
            .expect("offset fetch");
        committed.sort_by_key(|offset| offset.partition);
        assert_eq!(
            committed
                .iter()
                .map(|offset| (offset.partition, offset.offset))
                .collect::<Vec<_>>(),
            vec![(0, 1), (1, 0)]
        );
    }

    #[tokio::test]
    async fn a_failed_offset_delete_is_reported() {
        let broker = krafka::testing::FakeBroker::start()
            .await
            .expect("fake broker");
        assert!(broker.create_topic("orders", 1));
        let client = kafka_client(&broker.bootstrap_servers()).await;

        let result = client
            .delete_group_offsets("orders-group", &[("orders".into(), 0)])
            .await;

        assert!(result.is_err(), "{result:?}");
    }

    #[tokio::test]
    async fn nothing_to_write_skips_kafka() {
        let broker = krafka::testing::FakeBroker::start()
            .await
            .expect("fake broker");
        let client = kafka_client(&broker.bootstrap_servers()).await;
        broker.clear_requests();

        client.commit_group_offsets("g", &[]).await.expect("commit");
        client.delete_group_offsets("g", &[]).await.expect("delete");

        assert!(broker.requests().is_empty());
    }
}
