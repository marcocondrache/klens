use axum::Json;
use axum::Router;
use axum::routing::get;
use serde::Deserialize;

use crate::AppState;

use crate::app::context::Session;
use crate::app::error::ApiError;
use crate::app::extract::{Path, Query};

pub mod types;

#[cfg(test)]
mod tests;

pub(crate) use types::SearchHit;

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
            .await
            .into_iter()
            .map(SearchHit::from)
            .collect(),
    ))
}
