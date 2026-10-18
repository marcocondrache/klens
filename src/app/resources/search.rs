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

/// The fuzzy matcher's memory grows with the query, and a Kafka name is at
/// most 249 characters.
pub(crate) const MAX_QUERY_CHARS: usize = 256;

pub(crate) fn check_query_length(query: &str) -> Result<(), ApiError> {
    if query.chars().count() > MAX_QUERY_CHARS {
        return Err(ApiError::unprocessable(format!(
            "a query holds at most {MAX_QUERY_CHARS} characters"
        )));
    }
    Ok(())
}

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
    check_query_length(&query.q)?;
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
