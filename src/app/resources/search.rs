use axum::Json;
use axum::Router;
use axum::routing::get;
use serde::Deserialize;

use crate::AppState;
use crate::kafka::store::search::MAX_QUERY_CHARS;

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

/// The search endpoint and `klens_search` hold a query to the same length.
pub(crate) fn refuse_long_query(query: &str) -> Result<(), ApiError> {
    if query.chars().count() > MAX_QUERY_CHARS {
        return Err(ApiError::unprocessable(format!(
            "a query holds at most {MAX_QUERY_CHARS} characters"
        )));
    }
    Ok(())
}

async fn search(
    session: Session,
    Path(name): Path<String>,
    Query(query): Query<SearchQuery>,
) -> Result<Json<Vec<SearchHit>>, ApiError> {
    refuse_long_query(&query.q)?;
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
