use std::sync::Arc;

use foldhash::HashMap;

use crate::kafka::error::KafkaError;
use crate::kafka::limits::RecordLimits;
use crate::kafka::metadata::Watermarks;
use crate::kafka::session::ClusterSession;
use crate::kafka::store::ClusterStore;

use super::Record;
use super::cursor::RecordCursor;
use super::query::RecordQuery;
use super::read::{resolve_partitions, window_watermarks};
use super::session::fetch_page;

/// Watermarks are read once at open, so records produced during an export
/// do not keep it running.
pub struct Export {
    session: Arc<dyn ClusterSession>,
    query: RecordQuery,
    partitions: Vec<i32>,
    watermarks: HashMap<i32, Watermarks>,
    page: usize,
    limits: RecordLimits,
    done: bool,
}

impl Export {
    pub async fn open(
        session: Arc<dyn ClusterSession>,
        store: &ClusterStore,
        query: RecordQuery,
        limits: RecordLimits,
    ) -> Result<Self, KafkaError> {
        let page = limits.clamp_limit(query.limit)?;
        let partitions =
            resolve_partitions(session.as_ref(), store, &query.topic, &query.partitions).await?;
        let watermarks = window_watermarks(session.as_ref(), &query, &partitions).await?;

        Ok(Self {
            session,
            query,
            partitions,
            watermarks,
            page,
            limits,
            done: false,
        })
    }

    pub async fn next(&mut self) -> Result<Option<Vec<Record>>, KafkaError> {
        if self.done {
            return Ok(None);
        }

        let page = fetch_page(
            self.session.as_ref(),
            &self.query,
            &self.partitions,
            &self.watermarks,
            self.page,
            self.limits,
        )
        .await?;
        let next = page
            .next_cursor
            .as_deref()
            .map(RecordCursor::parse)
            .transpose()?;
        if next.is_some() && next == self.query.cursor {
            return Err(KafkaError::Timeout);
        }

        self.done = next.is_none();
        self.query.cursor = next;
        Ok(Some(page.records))
    }
}
