use axum::Json;
use axum::Router;
use axum::routing::get;
use serde::Deserialize;

use crate::AppState;
use crate::kafka::KafkaError;

use super::context::Session;
use super::error::ApiError;
use super::extract::{Path, Query};
use super::paging::{name_matches, page};

pub mod types;

#[cfg(test)]
mod tests;

pub(crate) use types::{GroupDetail, GroupOffset, GroupRow, GroupRowPage, GroupState};

pub(crate) fn router() -> Router<AppState> {
    Router::new()
        .route("/", get(groups))
        // Group ids may contain `/`.
        .route("/{*group}", get(group))
}

#[derive(Debug, Default, Deserialize)]
struct PageQuery {
    contains: Option<String>,
    after: Option<String>,
    limit: Option<i32>,
}

async fn groups(
    session: Session,
    Path(name): Path<String>,
    Query(query): Query<PageQuery>,
) -> Result<Json<GroupRowPage>, ApiError> {
    let cluster = session.cluster(&name)?;
    let rows: Vec<_> = cluster
        .store
        .group_rows()
        .into_iter()
        .filter(|row| name_matches(query.contains.as_deref(), &row.id))
        .collect();

    let total = rows.len() as i32;
    let (rows, next_cursor) = page(rows, query.after.as_deref(), query.limit, |row| {
        row.id.to_string()
    });

    Ok(Json(GroupRowPage {
        rows: rows.into_iter().map(GroupRow::from).collect(),
        total,
        next_cursor,
    }))
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
