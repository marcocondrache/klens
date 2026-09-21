use axum::Json;
use axum::Router;
use axum::extract::{Path, Query};
use axum::routing::get;

use crate::AppState;

use super::context::Session;
use super::error::ApiError;

mod types;

#[cfg(test)]
mod tests;

pub(crate) use types::{Compression, Record, RecordHeader, RecordOrder, RecordPage};
use types::{RecordParams, record_query};

pub(crate) fn router() -> Router<AppState> {
    Router::new().route("/", get(records))
}

async fn records(
    session: Session,
    Path((name, topic)): Path<(String, String)>,
    Query(params): Query<RecordParams>,
) -> Result<Json<RecordPage>, ApiError> {
    let capability = session.cluster(&name)?.access.records()?;
    Ok(Json(RecordPage::from(
        session
            .state
            .live_records(capability.cluster(), record_query(topic, params)?)
            .await?,
    )))
}
