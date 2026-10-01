use axum::Json;
use axum::Router;
use axum::routing::get;

use crate::AppState;
use crate::app::auth::SessionGuard;

use super::context::Session;
use super::error::ApiError;
use super::extract::{Path, Query};

mod export;
mod tail;
pub mod types;

#[cfg(test)]
mod tests;

pub(crate) use types::RecordPage;
use types::{RecordParams, record_query};

pub(crate) fn router() -> Router<AppState> {
    Router::new()
        .route("/", get(records))
        .route("/export", get(export::export))
        .route("/tail", get(tail::tail))
}

async fn records(
    session: Session,
    Path((name, topic)): Path<(String, String)>,
    Query(params): Query<RecordParams>,
) -> Result<Json<RecordPage>, ApiError> {
    let records = session.cluster(&name)?.records()?;
    Ok(Json(RecordPage::from(
        records.read(record_query(topic, params)?).await?,
    )))
}

fn denied(guard: &SessionGuard, cluster: &str) -> Option<ApiError> {
    let Some(access) = guard.revalidate() else {
        return Some(ApiError::SessionExpired);
    };
    access
        .cluster(cluster)
        .and_then(|cluster| cluster.records().map(drop))
        .err()
        .map(ApiError::from)
}
