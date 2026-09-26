use std::sync::Arc;

use bytes::Bytes;

use super::filter::{CompiledFilter, RawField, Verdict};
use super::obfuscate::{Field, TopicObfuscator};
use super::payload::{DecodedPayload, PayloadCodec, PayloadSlot, needs_decode};
use super::session::RawRecord;
use super::{Record, RecordHeader};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Screen {
    Deferred,
    NeedsPayload,
}

pub struct Kept {
    kind: Kind,
}

enum Kind {
    Pending(RawRecord),
    Decoded(DecodedRecord),
}

impl Kept {
    pub fn pending(raw: RawRecord) -> Self {
        Self {
            kind: Kind::Pending(raw),
        }
    }

    fn decoded(record: DecodedRecord) -> Self {
        Self {
            kind: Kind::Decoded(record),
        }
    }

    pub fn raw(&self) -> &RawRecord {
        match &self.kind {
            Kind::Pending(raw) => raw,
            Kind::Decoded(record) => record.raw(),
        }
    }
}

pub struct DecodedRecord {
    raw: RawRecord,
    key: Option<DecodedPayload>,
    value: Option<DecodedPayload>,
}

impl DecodedRecord {
    pub fn raw(&self) -> &RawRecord {
        &self.raw
    }

    pub fn into_record(self, topic: &str) -> Record {
        let schema_id = self.value.as_ref().and_then(DecodedPayload::wire_schema_id);
        let headers = self
            .raw
            .headers
            .iter()
            .map(|(key, value)| RecordHeader {
                key: String::from_utf8_lossy(key).into_owned(),
                value: value
                    .as_deref()
                    .map(|value| String::from_utf8_lossy(value).into_owned())
                    .unwrap_or_default(),
            })
            .collect();

        Record {
            topic: topic.to_owned(),
            partition: self.raw.partition,
            offset: self.raw.offset,
            timestamp: self.raw.timestamp.max(0),
            size_bytes: self.raw.size_bytes(),
            compression: self.raw.compression,
            key: self.key.map(DecodedPayload::into_text),
            value: self.value.map(DecodedPayload::into_text),
            schema_id,
            headers,
        }
    }
}

enum PayloadView<'a> {
    Raw {
        key: Option<RawField<'a>>,
        value: Option<RawField<'a>>,
    },
    Obfuscated,
}

impl<'a> PayloadView<'a> {
    fn verdict(self, filter: &CompiledFilter) -> Option<Screen> {
        match self {
            PayloadView::Obfuscated => Some(Screen::NeedsPayload),
            PayloadView::Raw { key, value } => match filter.on_raw(key, value) {
                Verdict::Fail => None,
                Verdict::Pass => Some(Screen::Deferred),
                Verdict::NeedsPayload => Some(Screen::NeedsPayload),
            },
        }
    }
}

pub struct RecordPipeline {
    codec: Option<Arc<dyn PayloadCodec>>,
    filter: Option<CompiledFilter>,
    obfuscator: Option<Arc<TopicObfuscator>>,
    fallback_schema_id: Option<i32>,
}

impl RecordPipeline {
    pub fn new(
        codec: Option<Arc<dyn PayloadCodec>>,
        filter: Option<CompiledFilter>,
        obfuscator: Option<Arc<TopicObfuscator>>,
        fallback_schema_id: Option<i32>,
    ) -> Self {
        Self {
            codec,
            filter,
            obfuscator,
            fallback_schema_id,
        }
    }

    pub fn obfuscated(&self) -> bool {
        self.obfuscator.is_some()
    }

    pub fn screen(&self, raw: &mut RawRecord) -> Option<Screen> {
        if let Some(obfuscator) = &self.obfuscator {
            obfuscator.mask_headers(&mut raw.headers);
        }

        let Some(filter) = &self.filter else {
            return Some(Screen::Deferred);
        };

        self.payload_view(raw).verdict(filter)
    }

    pub async fn decode_and_filter(&self, records: Vec<RawRecord>) -> Vec<Kept> {
        if records.is_empty() {
            return Vec::new();
        }

        let mut slots = Vec::with_capacity(records.len() * 2);
        let mut candidates = Vec::with_capacity(records.len());
        for raw in records {
            let key = push_slot(&mut slots, raw.key.clone(), None);
            let value = push_slot(&mut slots, raw.value.clone(), self.fallback_schema_id);
            candidates.push(Candidate { raw, key, value });
        }

        let mut decoded = self.decode(slots).await;
        let mut kept = Vec::with_capacity(candidates.len());
        for candidate in candidates {
            let mut key = candidate.key.and_then(|index| decoded[index].take());
            let mut value = candidate.value.and_then(|index| decoded[index].take());
            self.obfuscate(&mut key, &mut value);

            if let Some(filter) = &self.filter
                && !filter.on_payload(key.as_ref(), value.as_ref())
            {
                continue;
            }
            key.iter_mut()
                .chain(value.iter_mut())
                .for_each(DecodedPayload::drop_tree_if_rendered);

            kept.push(Kept::decoded(DecodedRecord {
                raw: candidate.raw,
                key,
                value,
            }));
        }
        kept
    }

    pub async fn decode_deferred(&self, page: Vec<Kept>) -> Vec<DecodedRecord> {
        let mut slots = Vec::with_capacity(page.len() * 2);
        let staged: Vec<Stage> = page
            .into_iter()
            .map(|kept| match kept.kind {
                Kind::Decoded(record) => Stage::Decoded(record),
                Kind::Pending(raw) => {
                    let key = push_slot(&mut slots, raw.key.clone(), None);
                    let value = push_slot(&mut slots, raw.value.clone(), self.fallback_schema_id);
                    Stage::Pending { raw, key, value }
                }
            })
            .collect();

        let mut decoded = self.decode(slots).await;
        staged
            .into_iter()
            .map(|stage| match stage {
                Stage::Decoded(record) => record,
                Stage::Pending { raw, key, value } => {
                    let mut key = key.and_then(|index| decoded[index].take());
                    let mut value = value.and_then(|index| decoded[index].take());
                    self.obfuscate(&mut key, &mut value);
                    DecodedRecord { raw, key, value }
                }
            })
            .collect()
    }

    fn payload_view<'a>(&'a self, raw: &'a RawRecord) -> PayloadView<'a> {
        if self
            .obfuscator
            .as_ref()
            .is_some_and(|obfuscator| obfuscator.hides_payload())
        {
            PayloadView::Obfuscated
        } else {
            PayloadView::Raw {
                key: self.field(raw.key.as_deref(), None),
                value: self.field(raw.value.as_deref(), self.fallback_schema_id),
            }
        }
    }

    fn obfuscate(&self, key: &mut Option<DecodedPayload>, value: &mut Option<DecodedPayload>) {
        let Some(obfuscator) = &self.obfuscator else {
            return;
        };

        obfuscator.apply(Field::Key, key);
        obfuscator.apply(Field::Value, value);
    }

    async fn decode(&self, mut slots: Vec<PayloadSlot>) -> Vec<Option<DecodedPayload>> {
        if let Some(codec) = &self.codec
            && !slots.is_empty()
        {
            codec.decode_batch(&mut slots).await;
        }
        slots.into_iter().map(|slot| Some(slot.take())).collect()
    }

    fn field<'a>(
        &self,
        bytes: Option<&'a [u8]>,
        fallback_schema_id: Option<i32>,
    ) -> Option<RawField<'a>> {
        bytes.map(|bytes| RawField {
            bytes,
            framed: self.codec.is_some() && needs_decode(bytes, fallback_schema_id),
        })
    }
}

enum Stage {
    Decoded(DecodedRecord),
    Pending {
        raw: RawRecord,
        key: Option<usize>,
        value: Option<usize>,
    },
}

fn push_slot(
    slots: &mut Vec<PayloadSlot>,
    bytes: Option<Bytes>,
    fallback_schema_id: Option<i32>,
) -> Option<usize> {
    bytes.map(|bytes| {
        slots.push(PayloadSlot::new(bytes, fallback_schema_id));
        slots.len() - 1
    })
}

struct Candidate {
    raw: RawRecord,
    key: Option<usize>,
    value: Option<usize>,
}
