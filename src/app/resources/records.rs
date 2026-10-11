use axum::Json;
use axum::Router;
use axum::http::StatusCode;
use axum::routing::get;

use crate::AppState;
use crate::app::auth::SessionGuard;

use crate::app::context::Session;
use crate::app::error::ApiError;
use crate::app::extract::{self, Path, Query};

mod export;
mod tail;
pub mod types;

#[cfg(test)]
mod tests;

pub(crate) use types::RecordPage;
use types::{
    DeleteParams, LookupParams, ProduceRecord, ProducedRecord, RecordLookup, RecordParams,
};

pub(crate) fn router() -> Router<AppState> {
    Router::new()
        .route("/", get(records).post(produce).delete(delete_records))
        .route("/export", get(export::export))
        .route("/tail", get(tail::tail))
        .route("/{partition}/{offset}", get(record))
}

async fn records(
    session: Session,
    Path((name, topic)): Path<(String, String)>,
    Query(params): Query<RecordParams>,
) -> Result<Json<RecordPage>, ApiError> {
    let records = session.cluster(&name)?.records()?;
    Ok(Json(RecordPage::from(
        records.read(params.into_query(topic)?).await?,
    )))
}

async fn produce(
    session: Session,
    Path((name, topic)): Path<(String, String)>,
    extract::Json(request): extract::Json<ProduceRecord>,
) -> Result<(StatusCode, Json<ProducedRecord>), ApiError> {
    let cluster = session.cluster(&name)?;
    let producer = cluster.produce()?;
    let record = request.into_record(topic, &producer).await?;
    let produced = producer.produce(&record).await?;
    Ok((StatusCode::CREATED, Json(produced.into())))
}

async fn delete_records(
    session: Session,
    Path((name, topic)): Path<(String, String)>,
    Query(params): Query<DeleteParams>,
) -> Result<StatusCode, ApiError> {
    let cluster = session.cluster(&name)?;
    let topics = cluster.delete_records()?;
    let before = params.before()?;
    topics
        .delete_records(&topic, &params.partition, before)
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn record(
    session: Session,
    Path((name, topic, partition, offset)): Path<(String, String, i32, i64)>,
    Query(params): Query<LookupParams>,
) -> Result<Json<RecordLookup>, ApiError> {
    let records = session.cluster(&name)?.records()?;
    Ok(Json(RecordLookup::from(
        records
            .record(params.into_record_at(topic, partition, offset))
            .await?,
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
