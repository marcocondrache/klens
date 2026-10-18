use std::sync::Arc;

use crate::kafka::error::KafkaError;
use crate::kafka::limits::RecordLimits;
use crate::kafka::session::ClusterSession;
use crate::kafka::store::ClusterStore;

use super::Record;
use super::cursor::RecordCursor;
use super::plan::LogSpan;
use super::query::RecordQuery;
use super::read::resolve_span;
use super::session::PageRequest;

/// Watermarks are read once at open, so records produced during an export
/// do not keep it running.
pub struct Export {
    session: Arc<dyn ClusterSession>,
    request: PageRequest,
    span: LogSpan,
    done: bool,
}

impl Export {
    pub async fn open(
        session: Arc<dyn ClusterSession>,
        store: &ClusterStore,
        query: RecordQuery,
        limits: RecordLimits,
    ) -> Result<Self, KafkaError> {
        let request = PageRequest::new(query, limits)?;
        let span = resolve_span(session.as_ref(), store, request.query()).await?;

        Ok(Self {
            session,
            request,
            span,
            done: false,
        })
    }

    pub async fn next(&mut self) -> Result<Option<Vec<Record>>, KafkaError> {
        if self.done {
            return Ok(None);
        }

        let page = self
            .request
            .fetch(self.session.as_ref(), &self.span)
            .await?;
        let next = page
            .next_cursor
            .as_deref()
            .map(RecordCursor::parse)
            .transpose()?;
        if next.is_some() && next == self.request.query().cursor {
            return Err(KafkaError::Timeout);
        }

        self.done = next.is_none();
        self.request.set_cursor(next);
        Ok(Some(page.records))
    }
}
