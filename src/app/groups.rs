use axum::Json;
use axum::Router;
use axum::routing::get;

use crate::AppState;
use crate::kafka::KafkaError;

use super::context::Session;
use super::error::ApiError;
use super::extract::Path;

pub mod types;

#[cfg(test)]
mod tests;

pub(crate) use types::{GroupDetail, GroupOffset, GroupRow, GroupState};

pub(crate) fn router() -> Router<AppState> {
    Router::new()
        .route("/", get(groups))
        // Group ids may contain `/`.
        .route("/{*group}", get(group))
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
