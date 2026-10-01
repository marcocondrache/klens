use axum::body::{Body, Bytes};
use axum::http::header::{CONTENT_DISPOSITION, CONTENT_TYPE};
use axum::response::{IntoResponse, Response};
use futures::stream::{self, Stream};

use crate::app::auth::SessionGuard;
use crate::kafka::Export;
use crate::kafka::model as domain;

use super::super::context::Session;
use super::super::error::ApiError;
use super::super::extract::{Path, Query};
use super::denied;
use super::types::{ExportParams, Record, export_query};

pub(super) async fn export(
    session: Session,
    Path((name, topic)): Path<(String, String)>,
    Query(params): Query<ExportParams>,
) -> Result<Response, ApiError> {
    let records = session.cluster(&name)?.records()?;
    let disposition = format!("attachment; filename=\"{topic}.ndjson\"");
    let export = records.export(export_query(topic, params)?).await?;
    let lines = Lines {
        export,
        guard: session.guard,
        cluster: name,
    };

    Ok((
        [
            (CONTENT_TYPE, "application/x-ndjson".to_owned()),
            (CONTENT_DISPOSITION, disposition),
        ],
        Body::from_stream(lines.into_stream()),
    )
        .into_response())
}

struct Lines {
    export: Export,
    guard: SessionGuard,
    cluster: String,
}

impl Lines {
    fn into_stream(self) -> impl Stream<Item = Result<Bytes, ApiError>> {
        stream::try_unfold(self, |mut state| async move {
            if let Some(error) = denied(&state.guard, &state.cluster) {
                return Err(error);
            }
            let Some(records) = state.export.next().await? else {
                return Ok(None);
            };
            Ok(Some((ndjson(records), state)))
        })
    }
}

fn ndjson(records: Vec<domain::Record>) -> Bytes {
    let mut lines = Vec::new();
    for record in records {
        serde_json::to_writer(&mut lines, &Record::from(record)).expect("record is serializable");
        lines.push(b'\n');
    }
    Bytes::from(lines)
}
