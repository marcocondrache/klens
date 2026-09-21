use axum::Json;
use axum::Router;
use axum::extract::{Path, Query};
use axum::routing::get;
use serde::Deserialize;

use crate::AppState;

use super::context::Session;
use super::error::ApiError;

mod types;

#[cfg(test)]
mod tests;

pub(crate) use types::{SearchHit, SearchKind};

pub(crate) fn router() -> Router<AppState> {
    Router::new().route("/", get(search))
}

#[derive(Debug, Default, Deserialize)]
struct SearchQuery {
    #[serde(default)]
    q: String,
}

async fn search(
    session: Session,
    Path(name): Path<String>,
    Query(query): Query<SearchQuery>,
) -> Result<Json<Vec<SearchHit>>, ApiError> {
    let cluster = session.cluster(&name)?;
    Ok(Json(
        cluster
            .store
            .search(&query.q)
            .into_iter()
            .map(SearchHit::from)
            .collect(),
    ))
}
