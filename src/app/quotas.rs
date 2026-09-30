use axum::Json;
use axum::Router;
use axum::routing::get;

use crate::AppState;

use super::context::Session;
use super::error::ApiError;
use super::extract::Path;

pub mod types;

#[cfg(test)]
mod tests;

pub(crate) use types::QuotaListing;

pub(crate) fn router() -> Router<AppState> {
    Router::new().route("/", get(quotas))
}

async fn quotas(
    session: Session,
    Path(name): Path<String>,
) -> Result<Json<QuotaListing>, ApiError> {
    let configs = session.cluster(&name)?.configs()?;
    Ok(Json(QuotaListing::from(
        configs.client_quotas().await?.as_ref(),
    )))
}
