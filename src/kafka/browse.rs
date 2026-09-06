use std::collections::HashMap;
use std::time::Instant;

use rdkafka::Message;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::error::KafkaError as RdKafkaError;
use rdkafka::message::{Headers, Timestamp};
use rdkafka::topic_partition_list::Offset;
use rdkafka::topic_partition_list::TopicPartitionList;
use tokio::time::timeout;

use crate::kafka::error::KafkaError;
use crate::kafka::factory::ClientFactory;
use crate::kafka::model::{
    Compression, FetchPlan, Record, RecordHeader, RecordOrder, decode_bytes,
};

pub async fn consume(
    factory: &ClientFactory,
    plan: &FetchPlan,
    budget: std::time::Duration,
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

    while !remaining.is_empty() && records.len() < plan.limit {
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

                let record = record_from_message(&message);
                if record.matches(&plan.search) {
                    records.push(record);
                }
            }
        }
    }

    records.sort_by(|left, right| match plan.order {
        RecordOrder::Newest => left
            .timestamp
            .cmp(&right.timestamp)
            .reverse()
            .then(left.offset.cmp(&right.offset).reverse()),
        RecordOrder::Oldest => left
            .timestamp
            .cmp(&right.timestamp)
            .then(left.offset.cmp(&right.offset)),
    });
    records.truncate(plan.limit);
    Ok(records)
}

fn record_from_message(message: &rdkafka::message::BorrowedMessage<'_>) -> Record {
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
        key: message.key().map(decode_bytes),
        value: message.payload().map(decode_bytes),
        headers,
        size_bytes: size_bytes as u64,
        compression: Compression::None,
    }
}
