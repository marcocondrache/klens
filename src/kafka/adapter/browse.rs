use std::collections::HashMap;
use std::time::Instant;

use rdkafka::Message;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::error::KafkaError as RdKafkaError;
use rdkafka::message::{Headers, Timestamp};
use rdkafka::topic_partition_list::Offset;
use rdkafka::topic_partition_list::TopicPartitionList;
use tokio::time::timeout;

use super::factory::ClientFactory;
use crate::kafka::error::KafkaError;
use crate::kafka::model::{Compression, FetchPlan, Record, RecordHeader, decode_bytes};
use crate::kafka::registry::decode::{PayloadDecoder, decode_field};

pub async fn consume(
    factory: &ClientFactory,
    plan: &FetchPlan,
    budget: std::time::Duration,
    decoder: Option<&PayloadDecoder>,
) -> Result<Vec<Record>, KafkaError> {
    if plan.windows.is_empty() || plan.limit == 0 {
        return Ok(Vec::new());
    }

    let consumer: StreamConsumer = factory.browser()?;
    let mut tpl = TopicPartitionList::new();

    for window in &plan.windows {
        if window.is_empty() {
            continue;
        }
        tpl.add_partition_offset(&plan.topic, window.partition, Offset::Offset(window.start))?;
    }

    if tpl.count() == 0 {
        return Ok(Vec::new());
    }

    consumer.assign(&tpl)?;

    let deadline = Instant::now() + budget;
    let mut remaining: HashMap<i32, i64> = plan
        .windows
        .iter()
        .filter(|window| !window.is_empty())
        .map(|window| (window.partition, window.end))
        .collect();
    let mut records = Vec::new();

    while !remaining.is_empty() {
        let leftover = deadline.saturating_duration_since(Instant::now());
        if leftover.is_zero() {
            break;
        }

        match timeout(leftover, consumer.recv()).await {
            Err(_) => break,
            Ok(Err(RdKafkaError::PartitionEOF(partition))) => {
                remaining.remove(&partition);
            }
            Ok(Err(error)) => return Err(error.into()),
            Ok(Ok(message)) => {
                let partition = message.partition();
                let offset = message.offset();
                if remaining
                    .get(&partition)
                    .is_some_and(|end| offset + 1 >= *end)
                {
                    remaining.remove(&partition);
                }

                let record = record_from_message(&message, decoder).await;
                if record.matches(&plan.search) {
                    records.push(record);
                }
            }
        }
    }

    records.sort_by(|left, right| left.cmp_for_order(right, plan.order));
    records.truncate(plan.limit);
    Ok(records)
}

async fn record_from_message(
    message: &rdkafka::message::BorrowedMessage<'_>,
    decoder: Option<&PayloadDecoder>,
) -> Record {
    let headers = message
        .headers()
        .map(|headers| {
            (0..headers.count())
                .filter_map(|index| {
                    let header = headers.try_get(index)?;
                    Some(RecordHeader {
                        key: header.key.to_owned(),
                        value: header.value.map(decode_bytes).unwrap_or_default(),
                    })
                })
                .collect()
        })
        .unwrap_or_default();

    let timestamp = match message.timestamp() {
        Timestamp::NotAvailable => 0,
        Timestamp::CreateTime(ms) | Timestamp::LogAppendTime(ms) => ms,
    };

    let size_bytes = message.key().map(|key| key.len()).unwrap_or(0)
        + message.payload().map(|payload| payload.len()).unwrap_or(0);

    Record {
        topic: message.topic().to_owned(),
        partition: message.partition(),
        offset: message.offset(),
        timestamp,
        key: decode_field(decoder, message.key()).await,
        value: decode_field(decoder, message.payload()).await,
        headers,
        size_bytes: size_bytes as u64,
        compression: Compression::None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kafka::registry::decode::decode_field;

    fn record(value: Option<String>) -> Record {
        Record {
            topic: "orders".into(),
            partition: 0,
            offset: 0,
            timestamp: 0,
            key: None,
            value,
            headers: Vec::new(),
            size_bytes: 0,
            compression: Compression::None,
        }
    }

    #[tokio::test]
    async fn search_matches_decoded_json_fields() {
        let value = decode_field(None, Some(br#"{"orderId":"abc"}"#))
            .await
            .unwrap();
        assert!(record(Some(value)).matches("orderid"));
    }

    #[tokio::test]
    async fn search_does_not_match_unrelated_payloads() {
        let value = decode_field(None, Some(b"binary-looking")).await.unwrap();
        assert!(!record(Some(value)).matches("orderid"));
    }
}
