use std::sync::{Arc, Mutex};

use rdkafka::admin::AdminClient;
use rdkafka::client::DefaultClientContext;

use super::deadline::Deadline;
use super::group_offsets::{self, NativeQueue};
use crate::kafka::error::KafkaError;
use crate::kafka::group::CommittedOffset;

/// Process-lifetime admin handle: metadata, groups, configs, and OffsetFetch
/// for groups we are not a member of.
///
/// OffsetFetch shares one result queue for the client lifetime. Destroying a
/// per-call queue while librdkafka still holds coordinator callbacks aborts
/// in `rd_kafka_enq_once`. Calls are serialized; catalog hydrate of eight
/// groups waits in line instead of overlapping RPCs.
pub(super) struct AdminPlane {
    client: Arc<AdminClient<DefaultClientContext>>,
    offsets: Arc<Mutex<NativeQueue>>,
}

impl AdminPlane {
    pub(super) fn new(client: AdminClient<DefaultClientContext>) -> Result<Self, KafkaError> {
        let client = Arc::new(client);
        let queue = NativeQueue::new(client.inner().native_ptr())?;
        Ok(Self {
            client,
            offsets: Arc::new(Mutex::new(queue)),
        })
    }

    pub(super) fn client(&self) -> Arc<AdminClient<DefaultClientContext>> {
        Arc::clone(&self.client)
    }

    pub(super) async fn group_offsets(
        &self,
        group_id: &str,
        partitions: &[(String, i32)],
        deadline: Deadline,
    ) -> Result<Vec<CommittedOffset>, KafkaError> {
        group_offsets::list(&self.client, &self.offsets, group_id, partitions, deadline).await
    }
}
