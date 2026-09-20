use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};

use crate::kafka::store::{Lane, WatermarkTable};
use crate::kafka::watermarks::Watermarks;

/// The watermark table plus when those marks were last known good.
///
/// `verified_at` is the later of the last commit (`sampled_at`) and the last
/// successful lane check (`checked_at`). It is not GraphQL lane health
/// and not a reason to recommit a quiet cluster.
pub struct VerifiedWatermarks {
    table: Arc<WatermarkTable>,
    verified_at: DateTime<Utc>,
}

impl VerifiedWatermarks {
    pub fn observe(lane: &Lane<WatermarkTable>) -> Option<Self> {
        let table = lane.load()?;
        let verified_at = lane
            .health()
            .checked_at
            .map_or(table.sampled_at, |checked| checked.max(table.sampled_at));
        Some(Self { table, verified_at })
    }

    pub fn plan(
        &self,
        topic: &str,
        partitions: &[i32],
        now: DateTime<Utc>,
        max_age: Duration,
    ) -> Option<HashMap<i32, Watermarks>> {
        if now
            .signed_duration_since(self.verified_at)
            .num_milliseconds()
            > max_age.as_millis() as i64
        {
            return None;
        }
        let marks = self.table.topic(topic)?;
        partitions
            .iter()
            .map(|partition| marks.get(partition).map(|marks| (*partition, *marks)))
            .collect()
    }
}
