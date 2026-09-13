//! OffsetFetch for an arbitrary group on the process-lifetime admin client.
//!
//! rdkafka 0.39 has no Rust `ListConsumerGroupOffsets`. librdkafka 2.12 does.
//! This file is the only new unsafe island: one group per C call, one private
//! result queue, RAII cleanup on success, error, timeout, and panic.

use std::ffi::{CStr, CString, c_char};
use std::slice;
use std::sync::{Arc, Mutex};

use rdkafka::Offset;
use rdkafka::admin::AdminClient;
use rdkafka::bindings;
use rdkafka::client::DefaultClientContext;
use rdkafka::error::{IsError, KafkaError as RdKafkaError, RDKafkaErrorCode};
use rdkafka::topic_partition_list::TopicPartitionList;

use super::blocking::run_blocking;
use super::deadline::Deadline;
use crate::kafka::error::KafkaError;
use crate::kafka::group::CommittedOffset;

pub(super) async fn list(
    admin: &Arc<AdminClient<DefaultClientContext>>,
    queue: &Arc<Mutex<NativeQueue>>,
    group_id: &str,
    partitions: &[(String, i32)],
    deadline: Deadline,
) -> Result<Vec<CommittedOffset>, KafkaError> {
    if partitions.is_empty() {
        return Ok(Vec::new());
    }

    let remaining = deadline.remaining()?;
    let admin = Arc::clone(admin);
    let queue = Arc::clone(queue);
    let group_id = group_id.to_owned();
    let partitions = partitions.to_vec();
    run_blocking(remaining, move || {
        let queue = queue
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        fetch(&admin, &queue, &group_id, &partitions, deadline)
    })
    .await
}

fn fetch(
    admin: &AdminClient<DefaultClientContext>,
    queue: &NativeQueue,
    group_id: &str,
    partitions: &[(String, i32)],
    deadline: Deadline,
) -> Result<Vec<CommittedOffset>, KafkaError> {
    let timeout_ms = i32::try_from(deadline.remaining()?.as_millis())
        .map_err(|_| KafkaError::Admin("timeout too large".into()))?;
    let group =
        CString::new(group_id).map_err(|_| KafkaError::Admin("group id contains NUL".into()))?;

    let mut tpl = TopicPartitionList::new();
    for (topic, partition) in partitions {
        tpl.add_partition(topic, *partition);
    }

    let rk = admin.inner().native_ptr();
    let options = NativeAdminOptions::new(rk, timeout_ms)?;
    let mut request = NativeListOffsets::new(group.as_ptr(), tpl.ptr())?;

    // SAFETY: `rk` is valid for the `AdminClient` we hold. `request`,
    // `options`, and `queue` are owned and not shared. librdkafka copies the
    // single list pointer; count is 1. The call does not take ownership.
    unsafe {
        bindings::rd_kafka_ListConsumerGroupOffsets(
            rk,
            request.as_mut_ptr(),
            1,
            options.as_ptr(),
            queue.as_ptr(),
        );
    }
    drop(request);
    drop(options);

    NativeEvent::poll(queue).and_then(parse_event)
}

fn parse_event(event: NativeEvent) -> Result<Vec<CommittedOffset>, KafkaError> {
    // SAFETY: `event` owns a non-null result event until Drop.
    unsafe {
        let err = bindings::rd_kafka_event_error(event.as_ptr());
        if err.is_error() {
            return Err(map_rdkafka(err.into()));
        }

        let result = bindings::rd_kafka_event_ListConsumerGroupOffsets_result(event.as_ptr());
        if result.is_null() {
            return Err(KafkaError::Admin(
                "missing ListConsumerGroupOffsets result".into(),
            ));
        }

        let mut n_groups = 0usize;
        let groups =
            bindings::rd_kafka_ListConsumerGroupOffsets_result_groups(result, &mut n_groups);
        if groups.is_null() {
            return Ok(Vec::new());
        }

        let mut offsets = Vec::new();
        for group in slice::from_raw_parts(groups, n_groups) {
            let error = bindings::rd_kafka_group_result_error(*group);
            if !error.is_null() {
                let code = bindings::rd_kafka_error_code(error);
                if code.is_error() {
                    return Err(map_rdkafka(code.into()));
                }
            }
            offsets.extend(committed_from_native(
                bindings::rd_kafka_group_result_partitions(*group),
            ));
        }
        Ok(offsets)
    }
}

/// Copies topic/partition/offset out of a list owned by the result event.
///
/// SAFETY: `list` is null or a valid list that outlives this call. The
/// returned values own their strings; the native list is not destroyed here.
unsafe fn committed_from_native(
    list: *const bindings::rd_kafka_topic_partition_list_t,
) -> Vec<CommittedOffset> {
    if list.is_null() {
        return Vec::new();
    }

    let list = unsafe { &*list };
    if list.elems.is_null() || list.cnt <= 0 {
        return Vec::new();
    }

    let elems = unsafe { slice::from_raw_parts(list.elems, list.cnt as usize) };
    committed_from_raw(elems.iter().filter_map(|elem| {
        if elem.topic.is_null() {
            return None;
        }
        let topic = unsafe { CStr::from_ptr(elem.topic) }
            .to_string_lossy()
            .into_owned();
        Some((topic, elem.partition, elem.offset))
    }))
}

fn committed_from_raw(
    entries: impl IntoIterator<Item = (String, i32, i64)>,
) -> Vec<CommittedOffset> {
    entries
        .into_iter()
        .filter_map(|(topic, partition, raw)| match Offset::from_raw(raw) {
            Offset::Offset(offset) => Some(CommittedOffset {
                topic,
                partition,
                offset,
            }),
            _ => None,
        })
        .collect()
}

fn map_rdkafka(code: RDKafkaErrorCode) -> KafkaError {
    match code {
        RDKafkaErrorCode::OperationTimedOut
        | RDKafkaErrorCode::TimedOutQueue
        | RDKafkaErrorCode::RequestTimedOut
        | RDKafkaErrorCode::MessageTimedOut => KafkaError::Timeout,
        _ => KafkaError::Client(RdKafkaError::OffsetFetch(code)),
    }
}

pub(super) struct NativeQueue(*mut bindings::rd_kafka_queue_t);

// SAFETY: `KafkaClient` only lends the queue to one `spawn_blocking` at a
// time through a mutex. librdkafka documents `rd_kafka_queue_t` as usable
// from the thread that polls it.
unsafe impl Send for NativeQueue {}

impl NativeQueue {
    pub(super) fn new(rk: *mut bindings::rd_kafka_t) -> Result<Self, KafkaError> {
        // SAFETY: `rk` is a live `rd_kafka_t` from `AdminClient`.
        let ptr = unsafe { bindings::rd_kafka_queue_new(rk) };
        if ptr.is_null() {
            return Err(KafkaError::Admin("failed to create admin queue".into()));
        }
        Ok(Self(ptr))
    }

    fn as_ptr(&self) -> *mut bindings::rd_kafka_queue_t {
        self.0
    }
}

impl Drop for NativeQueue {
    fn drop(&mut self) {
        if !self.0.is_null() {
            // SAFETY: we uniquely own this queue; librdkafka allows destroy
            // once no poll is in flight (callers only drop after poll).
            unsafe { bindings::rd_kafka_queue_destroy(self.0) };
        }
    }
}

struct NativeAdminOptions(*mut bindings::rd_kafka_AdminOptions_t);

impl NativeAdminOptions {
    fn new(rk: *mut bindings::rd_kafka_t, timeout_ms: i32) -> Result<Self, KafkaError> {
        // SAFETY: `rk` is a live `rd_kafka_t`.
        let ptr = unsafe {
            bindings::rd_kafka_AdminOptions_new(
                rk,
                bindings::rd_kafka_admin_op_t::RD_KAFKA_ADMIN_OP_LISTCONSUMERGROUPOFFSETS,
            )
        };
        if ptr.is_null() {
            return Err(KafkaError::Admin("failed to create admin options".into()));
        }

        let mut errstr = vec![0 as c_char; 512];
        // SAFETY: `ptr` is a new options object; `errstr` is a writable buffer.
        let rc = unsafe {
            bindings::rd_kafka_AdminOptions_set_request_timeout(
                ptr,
                timeout_ms,
                errstr.as_mut_ptr(),
                errstr.len(),
            )
        };
        if rc.is_error() {
            // SAFETY: we still uniquely own `ptr`.
            unsafe { bindings::rd_kafka_AdminOptions_destroy(ptr) };
            return Err(KafkaError::Admin("failed to set request timeout".into()));
        }
        Ok(Self(ptr))
    }

    fn as_ptr(&self) -> *const bindings::rd_kafka_AdminOptions_t {
        self.0
    }
}

impl Drop for NativeAdminOptions {
    fn drop(&mut self) {
        if !self.0.is_null() {
            // SAFETY: unique owner; librdkafka copies what it needs at submit.
            unsafe { bindings::rd_kafka_AdminOptions_destroy(self.0) };
        }
    }
}

struct NativeListOffsets(*mut bindings::rd_kafka_ListConsumerGroupOffsets_t);

impl NativeListOffsets {
    fn new(
        group_id: *const c_char,
        partitions: *const bindings::rd_kafka_topic_partition_list_t,
    ) -> Result<Self, KafkaError> {
        // SAFETY: `group_id` is a live CString; `partitions` is a live TPL.
        // librdkafka copies both.
        let ptr = unsafe { bindings::rd_kafka_ListConsumerGroupOffsets_new(group_id, partitions) };
        if ptr.is_null() {
            return Err(KafkaError::Admin(
                "failed to create ListConsumerGroupOffsets".into(),
            ));
        }
        Ok(Self(ptr))
    }

    fn as_mut_ptr(&mut self) -> *mut *mut bindings::rd_kafka_ListConsumerGroupOffsets_t {
        &mut self.0
    }
}

impl Drop for NativeListOffsets {
    fn drop(&mut self) {
        if !self.0.is_null() {
            // SAFETY: unique owner; destroy after submit as the C example does.
            unsafe { bindings::rd_kafka_ListConsumerGroupOffsets_destroy(self.0) };
        }
    }
}

struct NativeEvent(*mut bindings::rd_kafka_event_t);

impl NativeEvent {
    fn poll(queue: &NativeQueue) -> Result<Self, KafkaError> {
        // SAFETY: `queue` is a live private result queue.
        let ptr = unsafe { bindings::rd_kafka_queue_poll(queue.as_ptr(), -1) };
        if ptr.is_null() {
            return Err(KafkaError::Timeout);
        }
        Ok(Self(ptr))
    }

    fn as_ptr(&self) -> *mut bindings::rd_kafka_event_t {
        self.0
    }
}

impl Drop for NativeEvent {
    fn drop(&mut self) {
        // SAFETY: `rd_kafka_event_destroy` accepts NULL.
        unsafe { bindings::rd_kafka_event_destroy(self.0) };
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rdkafka::admin::AdminClient;
    use rdkafka::client::DefaultClientContext;
    use rdkafka::config::ClientConfig;
    use rdkafka::consumer::{BaseConsumer, CommitMode, Consumer};
    use rdkafka::mocking::MockCluster;
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use rdkafka::producer::{FutureProducer, FutureRecord};

    use crate::config::ClusterConfig;
    use crate::kafka::client::KafkaClient;

    #[test]
    fn committed_from_raw_keeps_only_concrete_offsets() {
        let offsets = committed_from_raw([
            ("orders".into(), 0, 12),
            ("orders".into(), 1, bindings::RD_KAFKA_OFFSET_INVALID as i64),
            (
                "orders".into(),
                2,
                bindings::RD_KAFKA_OFFSET_BEGINNING as i64,
            ),
            ("orders".into(), 3, bindings::RD_KAFKA_OFFSET_END as i64),
            ("orders".into(), 4, -3),
            ("payments".into(), 0, 0),
        ]);

        assert_eq!(
            offsets,
            vec![
                CommittedOffset {
                    topic: "orders".into(),
                    partition: 0,
                    offset: 12,
                },
                CommittedOffset {
                    topic: "orders".into(),
                    partition: 4,
                    offset: -3,
                },
                CommittedOffset {
                    topic: "payments".into(),
                    partition: 0,
                    offset: 0,
                },
            ]
        );
    }

    #[test]
    fn map_rdkafka_times_out_on_librdkafka_timeouts() {
        assert!(matches!(
            map_rdkafka(RDKafkaErrorCode::OperationTimedOut),
            KafkaError::Timeout
        ));
        assert!(matches!(
            map_rdkafka(RDKafkaErrorCode::UnknownTopicOrPartition),
            KafkaError::Client(_)
        ));
    }

    #[tokio::test]
    async fn empty_partitions_skip_kafka() {
        let mock = MockCluster::new(1).expect("mock cluster");
        let client = kafka_client(&mock.bootstrap_servers());
        let offsets = client.committed_offsets("unused", &[]).await.unwrap();
        assert!(offsets.is_empty());
    }

    #[tokio::test]
    async fn expired_deadline_is_timeout() {
        let mock = MockCluster::new(1).expect("mock cluster");
        let admin: Arc<AdminClient<DefaultClientContext>> = Arc::new(
            ClientConfig::new()
                .set("bootstrap.servers", mock.bootstrap_servers())
                .create()
                .expect("admin"),
        );
        let queue = Arc::new(Mutex::new(
            NativeQueue::new(admin.inner().native_ptr()).expect("queue"),
        ));
        let deadline = Deadline::from(Duration::from_millis(1));
        tokio::time::sleep(Duration::from_millis(2)).await;
        let error = list(&admin, &queue, "unused", &[("orders".into(), 0)], deadline)
            .await
            .unwrap_err();
        assert!(matches!(error, KafkaError::Timeout));
    }

    #[tokio::test]
    async fn lists_committed_offsets_on_a_mock_cluster() {
        let mock = MockCluster::new(1).expect("mock cluster");
        mock.create_topic("orders", 1, 1).expect("topic");

        let bootstrap = mock.bootstrap_servers();
        produce(&bootstrap, "orders").await;
        commit(&bootstrap, "orders-group", "orders", 1);

        let client = kafka_client(&bootstrap);
        let offsets = client
            .committed_offsets("orders-group", &[("orders".into(), 0)])
            .await
            .expect("offset fetch");

        assert_eq!(
            offsets,
            vec![CommittedOffset {
                topic: "orders".into(),
                partition: 0,
                offset: 1,
            }]
        );
    }

    #[tokio::test]
    async fn eight_groups_share_one_client() {
        let mock = MockCluster::new(1).expect("mock cluster");
        mock.create_topic("orders", 1, 1).expect("topic");
        let bootstrap = mock.bootstrap_servers();
        produce(&bootstrap, "orders").await;

        for index in 0..8 {
            commit(&bootstrap, &format!("g{index}"), "orders", 1);
        }

        let client = Arc::new(kafka_client(&bootstrap));
        let fetches = (0..8).map(|index| {
            let client = Arc::clone(&client);
            async move {
                client
                    .committed_offsets(&format!("g{index}"), &[("orders".into(), 0)])
                    .await
            }
        });
        let results = futures::future::join_all(fetches).await;
        assert_eq!(results.len(), 8);
        for result in results {
            assert_eq!(result.expect("offset fetch")[0].offset, 1);
        }
    }

    fn kafka_client(bootstrap: &str) -> KafkaClient {
        KafkaClient::connect(&ClusterConfig {
            name: "test".into(),
            bootstrap_servers: vec![bootstrap.to_owned()],
            security: None,
            schema_registry: None,
            properties: HashMap::new(),
        })
        .expect("kafka client")
    }

    async fn produce(bootstrap: &str, topic: &str) {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", bootstrap)
            .create()
            .expect("producer");
        producer
            .send(
                FutureRecord::to(topic).payload("hello").key("k"),
                Duration::from_secs(5),
            )
            .await
            .expect("produce");
    }

    fn commit(bootstrap: &str, group: &str, topic: &str, offset: i64) {
        let consumer: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", bootstrap)
            .set("group.id", group)
            .set("enable.auto.commit", "false")
            .create()
            .expect("consumer");
        let mut tpl = TopicPartitionList::new();
        tpl.add_partition_offset(topic, 0, Offset::Offset(offset))
            .expect("offset");
        consumer.assign(&tpl).expect("assign");
        consumer.commit(&tpl, CommitMode::Sync).expect("commit");
    }
}
