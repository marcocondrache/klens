use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use foldhash::{HashMap, HashSet};

use super::cluster::{World, lock};
use crate::kafka::error::KafkaError;
use crate::kafka::model::{PartitionWindow, RawRecord, ScanConsumer, TailConsumer, TailPosition};
use crate::kafka::scan::payload::{DecodedPayload, PayloadCodec, PayloadSlot};

pub const FAKE_TAIL_POLL_RECORDS: usize = 4;

pub(super) struct FakeScan {
    world: Arc<Mutex<World>>,
    topic: String,
    state: Mutex<ScanState>,
}

#[derive(Default)]
struct ScanState {
    pending: Vec<RawRecord>,
    windows: HashMap<i32, (i64, i64)>,
    paused: HashSet<i32>,
    owed: Duration,
}

impl FakeScan {
    pub(super) fn new(world: Arc<Mutex<World>>, topic: &str) -> Self {
        Self {
            world,
            topic: topic.to_owned(),
            state: Mutex::new(ScanState::default()),
        }
    }

    fn state(&self) -> std::sync::MutexGuard<'_, ScanState> {
        self.state.lock().expect("fake scan")
    }

    fn position_of(&self, partition: i32) -> Option<i64> {
        let state = self.state();
        let (_, end) = *state.windows.get(&partition)?;
        let next = state
            .pending
            .iter()
            .filter(|record| record.partition == partition)
            .map(|record| record.offset)
            .min();
        Some(next.unwrap_or(end))
    }
}

#[async_trait]
impl ScanConsumer for FakeScan {
    async fn reassign(&self, windows: &[PartitionWindow]) -> Result<(), KafkaError> {
        let assigned: HashMap<i32, (i64, i64)> = windows
            .iter()
            .map(|window| (window.partition, (window.start, window.end)))
            .collect();
        let (mut pending, owed) = {
            let mut world = lock(&self.world);
            world.assignments.push(
                windows
                    .iter()
                    .map(|window| (window.partition, window.start, window.end))
                    .collect(),
            );
            let pending: Vec<RawRecord> = world
                .records
                .iter()
                .filter(|record| record.topic() == self.topic)
                .filter(|record| {
                    assigned
                        .get(&record.partition())
                        .is_some_and(|(start, end)| (*start..*end).contains(&record.offset()))
                })
                .map(|record| record.raw())
                .collect();
            (pending, world.records_delay)
        };
        pending.sort_by_key(|record| (record.partition, record.offset));

        let mut state = self.state();
        state.pending = pending;
        state.windows = assigned;
        state.paused.clear();
        state.owed = owed;
        Ok(())
    }

    async fn poll(&self, max_wait: Duration) -> Result<Vec<RawRecord>, KafkaError> {
        let owed = self.state().owed;
        if !owed.is_zero() {
            let slice = owed.min(max_wait);
            tokio::time::sleep(slice).await;
            self.state().owed = owed - slice;
            if slice < owed {
                return Ok(Vec::new());
            }
        }

        let mut state = self.state();
        let (ready, held) = std::mem::take(&mut state.pending)
            .into_iter()
            .partition(|record| !state.paused.contains(&record.partition));
        state.pending = held;
        Ok(ready)
    }

    async fn pause(&self, partitions: &[i32]) {
        self.state().paused.extend(partitions.iter().copied());
    }

    async fn seek_to_end(&self, _windows: &[PartitionWindow]) {}

    async fn position(&self, partition: i32) -> Option<i64> {
        self.position_of(partition)
    }

    async fn lag(&self, partition: i32) -> Option<u64> {
        let position = self.position_of(partition)?;
        let high = lock(&self.world)
            .watermarks
            .get(&self.topic)?
            .get(&partition)?
            .high;
        Some(high.saturating_sub(position).max(0) as u64)
    }

    async fn close(&self) {}
}

pub(super) struct FakeTail {
    world: Arc<Mutex<World>>,
    topic: String,
    positions: Mutex<HashMap<i32, i64>>,
}

impl FakeTail {
    pub(super) fn new(world: Arc<Mutex<World>>, topic: &str, start: &[TailPosition]) -> Self {
        Self {
            world,
            topic: topic.to_owned(),
            positions: Mutex::new(
                start
                    .iter()
                    .map(|position| (position.partition, position.offset))
                    .collect(),
            ),
        }
    }

    fn positions(&self) -> std::sync::MutexGuard<'_, HashMap<i32, i64>> {
        self.positions.lock().expect("fake tail")
    }

    fn take(&self) -> Vec<RawRecord> {
        let mut positions = self.positions();
        let mut ready: Vec<RawRecord> = lock(&self.world)
            .records
            .iter()
            .filter(|record| record.topic() == self.topic)
            .filter(|record| {
                positions
                    .get(&record.partition())
                    .is_some_and(|position| record.offset() >= *position)
            })
            .map(|record| record.raw())
            .collect();
        ready.sort_by_key(|record| (record.partition, record.offset));
        ready.truncate(FAKE_TAIL_POLL_RECORDS);

        for record in &ready {
            positions.insert(record.partition, record.offset + 1);
        }
        ready
    }
}

#[async_trait]
impl TailConsumer for FakeTail {
    async fn poll(&self, max_wait: Duration) -> Result<Vec<RawRecord>, KafkaError> {
        lock(&self.world).tail_polls += 1;
        let ready = self.take();
        if !ready.is_empty() || self.positions().is_empty() {
            return Ok(ready);
        }
        tokio::time::sleep(max_wait).await;
        Ok(self.take())
    }

    async fn position(&self, partition: i32) -> Option<i64> {
        self.positions().get(&partition).copied()
    }

    async fn lags(&self) -> HashMap<i32, u64> {
        let positions = self.positions().clone();
        let mut world = lock(&self.world);
        world.tail_lag_reads += 1;
        let Some(highs) = world.watermarks.get(&self.topic) else {
            return HashMap::default();
        };
        positions
            .iter()
            .filter_map(|(&partition, &position)| {
                let high = highs.get(&partition)?.high;
                Some((partition, high.saturating_sub(position).max(0) as u64))
            })
            .collect()
    }

    async fn seek(&self, positions: &[TailPosition]) -> Result<(), KafkaError> {
        lock(&self.world).tail_seeks.push(
            positions
                .iter()
                .map(|position| (position.partition, position.offset))
                .collect(),
        );
        let mut current = self.positions();
        for position in positions {
            current.insert(position.partition, position.offset);
        }
        Ok(())
    }
}

#[derive(Default)]
pub(super) struct CountingCodec {
    decoded: AtomicUsize,
}

impl CountingCodec {
    pub(super) fn decoded(&self) -> usize {
        self.decoded.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl PayloadCodec for CountingCodec {
    async fn decode_batch(&self, slots: &mut [PayloadSlot]) {
        self.decoded.fetch_add(slots.len(), Ordering::SeqCst);

        for slot in slots {
            let Ok((_, body)) = schemreg::decode_wire_prefix(&slot.raw) else {
                continue;
            };
            let Ok(json) = serde_json::from_slice(&slot.raw[body..]) else {
                continue;
            };

            slot.decoded = Some(DecodedPayload::decoded(slot.raw.clone(), json));
        }
    }
}
