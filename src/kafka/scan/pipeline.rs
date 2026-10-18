use std::sync::Arc;

use bytes::Bytes;

use crate::kafka::blocking::off_runtime;

use super::batch::{RecordBatch, SortKey};
use super::filter::{CompiledFilter, RawField, Verdict};
use super::obfuscate::{Field, TopicObfuscator};
use super::payload::{DecodedPayload, PayloadCodec, PayloadSlot, ResolvedCodec, needs_decode};
use super::session::RawRecord;
use super::{Record, RecordHeader};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Screen {
    Deferred,
    NeedsPayload,
}

pub enum Kept {
    Pending(RawRecord),
    Decoded(DecodedRecord),
}

impl Kept {
    pub fn raw(&self) -> &RawRecord {
        match self {
            Self::Pending(raw) => raw,
            Self::Decoded(record) => record.raw(),
        }
    }
}

pub struct DecodedRecord {
    raw: RawRecord,
    key: Option<DecodedPayload>,
    value: Option<DecodedPayload>,
    masked: bool,
}

impl DecodedRecord {
    fn raw(&self) -> &RawRecord {
        &self.raw
    }

    fn into_record(self, topic: &str) -> Record {
        let schema_id = self.value.as_ref().and_then(DecodedPayload::wire_schema_id);
        let size_bytes = self.raw.size_bytes();

        let mut headers_verbatim = true;
        let headers = self
            .raw
            .headers
            .into_iter()
            .map(|(key, value)| {
                let (value, verbatim) = header_text(value.as_deref());
                headers_verbatim &= verbatim;
                RecordHeader { key, value }
            })
            .collect();

        let verbatim = !self.masked
            && headers_verbatim
            && [&self.key, &self.value]
                .into_iter()
                .flatten()
                .all(DecodedPayload::is_verbatim);

        Record {
            topic: topic.to_owned(),
            partition: self.raw.partition,
            offset: self.raw.offset,
            timestamp: self.raw.timestamp.max(0),
            size_bytes,
            key: self.key.map(DecodedPayload::into_text),
            value: self.value.map(DecodedPayload::into_text),
            schema_id,
            headers,
            verbatim,
        }
    }
}

/// A missing or non-UTF-8 header value cannot round-trip through the text form.
fn header_text(value: Option<&[u8]>) -> (String, bool) {
    let Some(bytes) = value else {
        return (String::new(), false);
    };
    match str::from_utf8(bytes) {
        Ok(text) => (text.to_owned(), true),
        Err(_) => (String::from_utf8_lossy(bytes).into_owned(), false),
    }
}

pub struct RecordPipeline {
    codec: Option<Arc<dyn PayloadCodec>>,
    stages: Arc<Stages>,
}

impl RecordPipeline {
    pub fn new(
        codec: Option<Arc<dyn PayloadCodec>>,
        filter: Option<CompiledFilter>,
        obfuscator: Option<Arc<TopicObfuscator>>,
        fallback_schema_id: Option<i32>,
    ) -> Self {
        let stages = Stages {
            decodes: codec.is_some(),
            filter,
            obfuscator,
            fallback_schema_id,
        };

        Self {
            codec,
            stages: Arc::new(stages),
        }
    }

    pub fn obfuscated(&self) -> bool {
        self.stages.obfuscated()
    }

    /// Screens one poll into `batch` and returns how many records the batch
    /// turned away.
    pub async fn admit(
        &self,
        batch: &mut RecordBatch<Kept>,
        records: Vec<(SortKey, RawRecord)>,
    ) -> u64 {
        if records.is_empty() {
            return 0;
        }

        let stages = Arc::clone(&self.stages);
        let mut held = batch.take();
        let (held, candidates, rejected) = off_runtime(move || {
            let (candidates, rejected) = stages.screen(&mut held, records);
            (held, candidates, rejected)
        })
        .await;
        *batch = held;

        for (sort, kept) in self.decode_and_filter(candidates).await {
            batch.push(sort, kept);
        }
        rejected
    }

    pub async fn render(&self, page: Vec<Kept>, topic: &str) -> Vec<Record> {
        if page.is_empty() {
            return Vec::new();
        }

        let mut slots = Vec::with_capacity(page.len() * 2);
        let staged: Vec<Stage> = page
            .into_iter()
            .map(|kept| match kept {
                Kept::Decoded(record) => Stage::Decoded(record),
                Kept::Pending(raw) => {
                    let key = push_slot(&mut slots, raw.key.clone(), None);
                    let value = push_slot(
                        &mut slots,
                        raw.value.clone(),
                        self.stages.fallback_schema_id,
                    );
                    Stage::Pending { raw, key, value }
                }
            })
            .collect();

        let resolved = self.resolve(&slots).await;
        let stages = Arc::clone(&self.stages);
        let topic = topic.to_owned();
        off_runtime(move || {
            let mut decoded = decode(resolved, slots);
            staged
                .into_iter()
                .map(|stage| match stage {
                    Stage::Decoded(record) => record,
                    Stage::Pending { raw, key, value } => {
                        let mut key = key.and_then(|index| decoded[index].take());
                        let mut value = value.and_then(|index| decoded[index].take());
                        stages.obfuscate(&mut key, &mut value);
                        stages.decoded(raw, key, value)
                    }
                })
                .map(|record| record.into_record(&topic))
                .collect()
        })
        .await
    }

    async fn decode_and_filter(&self, records: Vec<(SortKey, RawRecord)>) -> Vec<(SortKey, Kept)> {
        if records.is_empty() {
            return Vec::new();
        }

        let mut slots = Vec::with_capacity(records.len() * 2);
        let mut candidates = Vec::with_capacity(records.len());
        for (sort, raw) in records {
            let key = push_slot(&mut slots, raw.key.clone(), None);
            let value = push_slot(
                &mut slots,
                raw.value.clone(),
                self.stages.fallback_schema_id,
            );
            candidates.push(Candidate {
                sort,
                raw,
                key,
                value,
            });
        }

        let resolved = self.resolve(&slots).await;
        let stages = Arc::clone(&self.stages);
        off_runtime(move || stages.filter_decoded(candidates, decode(resolved, slots))).await
    }

    async fn resolve(&self, slots: &[PayloadSlot]) -> Option<Box<dyn ResolvedCodec>> {
        match &self.codec {
            Some(codec) => Some(codec.resolve(slots).await),
            None => None,
        }
    }
}

/// The steps whose cost grows with the bytes a poll carries. They run on the
/// blocking pool, where a large poll cannot stall the runtime's workers.
struct Stages {
    decodes: bool,
    filter: Option<CompiledFilter>,
    obfuscator: Option<Arc<TopicObfuscator>>,
    fallback_schema_id: Option<i32>,
}

impl Stages {
    fn obfuscated(&self) -> bool {
        self.obfuscator.is_some()
    }

    fn screen(
        &self,
        batch: &mut RecordBatch<Kept>,
        records: Vec<(SortKey, RawRecord)>,
    ) -> (Vec<(SortKey, RawRecord)>, u64) {
        let mut candidates = Vec::new();
        let mut rejected = 0;

        for (sort, mut raw) in records {
            if !batch.admits(&sort) {
                rejected += 1;
                continue;
            }

            match self.screen_record(&mut raw) {
                None => continue,
                Some(Screen::Deferred) => batch.push(sort, Kept::Pending(raw)),
                Some(Screen::NeedsPayload) => candidates.push((sort, raw)),
            }
        }

        (candidates, rejected)
    }

    fn screen_record(&self, raw: &mut RawRecord) -> Option<Screen> {
        if let Some(obfuscator) = &self.obfuscator {
            obfuscator.obfuscate_headers(&mut raw.headers);
        }

        let Some(filter) = &self.filter else {
            return Some(Screen::Deferred);
        };

        if self
            .obfuscator
            .as_ref()
            .is_some_and(|obfuscator| obfuscator.hides_payload())
        {
            return Some(Screen::NeedsPayload);
        }

        let key = self.field(raw.key.as_deref(), None);
        let value = self.field(raw.value.as_deref(), self.fallback_schema_id);
        match filter.on_raw(key, value) {
            Verdict::Fail => None,
            Verdict::Pass => Some(Screen::Deferred),
            Verdict::NeedsPayload => Some(Screen::NeedsPayload),
        }
    }

    fn filter_decoded(
        &self,
        candidates: Vec<Candidate>,
        mut decoded: Vec<Option<DecodedPayload>>,
    ) -> Vec<(SortKey, Kept)> {
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

            kept.push((
                candidate.sort,
                Kept::Decoded(self.decoded(candidate.raw, key, value)),
            ));
        }
        kept
    }

    fn obfuscate(&self, key: &mut Option<DecodedPayload>, value: &mut Option<DecodedPayload>) {
        let Some(obfuscator) = &self.obfuscator else {
            return;
        };

        obfuscator.apply(Field::Key, key);
        obfuscator.apply(Field::Value, value);
    }

    fn decoded(
        &self,
        raw: RawRecord,
        key: Option<DecodedPayload>,
        value: Option<DecodedPayload>,
    ) -> DecodedRecord {
        DecodedRecord {
            raw,
            key,
            value,
            masked: self.obfuscated(),
        }
    }

    fn field<'a>(
        &self,
        bytes: Option<&'a [u8]>,
        fallback_schema_id: Option<i32>,
    ) -> Option<RawField<'a>> {
        bytes.map(|bytes| RawField {
            bytes,
            framed: self.decodes && needs_decode(bytes, fallback_schema_id),
        })
    }
}

fn decode(
    resolved: Option<Box<dyn ResolvedCodec>>,
    mut slots: Vec<PayloadSlot>,
) -> Vec<Option<DecodedPayload>> {
    if let Some(resolved) = resolved {
        resolved.decode(&mut slots);
    }
    slots.into_iter().map(|slot| Some(slot.take())).collect()
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
    sort: SortKey,
    raw: RawRecord,
    key: Option<usize>,
    value: Option<usize>,
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use std::thread::{self, ThreadId};

    use async_trait::async_trait;
    use tracing::{Instrument, Level};

    use super::*;
    use crate::kafka::scan::cursor::CursorDirection;
    use crate::kafka::scan::query::RecordOrder;
    use crate::testing::{LogCapture, framed};

    /// Records the thread each decode runs on and logs from it.
    #[derive(Clone, Default)]
    struct Probe(Arc<Mutex<Vec<ThreadId>>>);

    #[async_trait]
    impl PayloadCodec for Probe {
        async fn resolve(&self, _: &[PayloadSlot]) -> Box<dyn ResolvedCodec> {
            Box::new(self.clone())
        }
    }

    impl ResolvedCodec for Probe {
        fn decode(&self, _: &mut [PayloadSlot]) {
            self.0.lock().unwrap().push(thread::current().id());
            tracing::warn!("probed");
        }
    }

    #[tokio::test]
    async fn payloads_decode_off_the_runtime_thread_under_the_callers_span() {
        let logs = LogCapture::at(Level::INFO);

        for filter in [CompiledFilter::contains("ord").unwrap(), None] {
            let probe = Probe::default();
            let pipeline = RecordPipeline::new(Some(Arc::new(probe.clone())), filter, None, None);
            let raw = RawRecord {
                partition: 0,
                offset: 0,
                timestamp: 0,
                key: None,
                value: Some(framed(7, "{}")),
                headers: Vec::new(),
            };
            let mut batch = RecordBatch::new(1, RecordOrder::Newest, CursorDirection::Forward);

            async {
                pipeline
                    .admit(&mut batch, vec![(raw.sort_key(), raw)])
                    .await;
                pipeline.render(batch.into_sorted(), "orders").await;
            }
            .instrument(tracing::info_span!("http.request", path = "/records"))
            .await;

            let threads = probe.0.lock().unwrap();
            assert_eq!(
                threads.len(),
                1,
                "a filter decodes on admit, a page on render"
            );
            assert_ne!(threads[0], thread::current().id());
        }

        assert_eq!(logs.count(r#"http.request{path="/records"}: "#), 2);
    }

    fn verbatim(value: Option<&'static [u8]>) -> bool {
        DecodedRecord {
            raw: RawRecord {
                partition: 0,
                offset: 0,
                timestamp: 0,
                key: None,
                value: None,
                headers: vec![("trace".to_owned(), value.map(Bytes::from_static))],
            },
            key: None,
            value: None,
            masked: false,
        }
        .into_record("orders")
        .verbatim
    }

    #[test]
    fn a_record_is_verbatim_only_while_every_header_is_text() {
        assert!(verbatim(Some(b"abc")));
        assert!(!verbatim(None), "a null value would come back empty");
        assert!(!verbatim(Some(&[0xff])));
    }
}
