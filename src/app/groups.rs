use axum::Json;
use axum::Router;
use axum::http::StatusCode;
use axum::routing::{get, patch};

use crate::AppState;
use crate::kafka::KafkaError;

use super::context::Session;
use super::error::ApiError;
use super::extract::{self, Path, Query};

pub mod types;

#[cfg(test)]
mod tests;

use types::DeleteOffsets;
pub(crate) use types::{GroupDetail, GroupOffset, GroupRow, GroupState, OffsetMove, ResetOffsets};

pub(crate) fn router() -> Router<AppState> {
    Router::new()
        .route("/", get(groups))
        // Group ids may contain `/`.
        .route("/{*group}", get(group).delete(delete_group))
}

/// A group's committed offsets, under their own prefix because a group id
/// may contain `/` and so must end the path.
pub(crate) fn offsets_router() -> Router<AppState> {
    Router::new().route("/{*group}", patch(reset_offsets).delete(delete_offsets))
}

async fn groups(
    session: Session,
    Path(name): Path<String>,
) -> Result<Json<Vec<GroupRow>>, ApiError> {
    let cluster = session.cluster(&name)?;
    Ok(Json(
        cluster
            .store
            .group_rows()
            .into_iter()
            .map(GroupRow::from)
            .collect(),
    ))
}

async fn group(
    session: Session,
    Path((name, id)): Path<(String, String)>,
) -> Result<Json<GroupDetail>, ApiError> {
    let cluster = session.cluster(&name)?;
    cluster
        .store
        .group_detail(&id)
        .map(GroupDetail::from)
        .map(Json)
        .ok_or_else(|| {
            KafkaError::UnknownGroup {
                cluster: cluster.name().to_owned(),
                group: id,
            }
            .into()
        })
}

async fn delete_group(
    session: Session,
    Path((name, group)): Path<(String, String)>,
) -> Result<StatusCode, ApiError> {
    let cluster = session.cluster(&name)?;
    cluster.manage_groups()?.delete_group(&group).await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn reset_offsets(
    session: Session,
    Path((name, group)): Path<(String, String)>,
    extract::Json(request): extract::Json<ResetOffsets>,
) -> Result<Json<Vec<OffsetMove>>, ApiError> {
    let cluster = session.cluster(&name)?;
    let groups = cluster.manage_groups()?;
    let dry_run = request.dry_run;
    let reset = request.into_reset(group)?;
    let moves = if dry_run {
        groups.plan_reset(&reset).await?
    } else {
        groups.reset_offsets(&reset).await?
    };
    Ok(Json(moves.into_iter().map(OffsetMove::from).collect()))
}

async fn delete_offsets(
    session: Session,
    Path((name, group)): Path<(String, String)>,
    Query(params): Query<DeleteOffsets>,
) -> Result<StatusCode, ApiError> {
    let cluster = session.cluster(&name)?;
    cluster
        .manage_groups()?
        .delete_offsets(&group, &params.topic)
        .await?;
    Ok(StatusCode::NO_CONTENT)
}
