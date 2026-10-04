use axum::Json;
use axum::Router;
use axum::http::StatusCode;
use axum::routing::get;

use crate::AppState;

use super::context::Session;
use super::error::ApiError;
use super::extract::{self, Path};

pub mod types;

#[cfg(test)]
mod tests;

pub(crate) use types::{ClientQuota, QuotaListing};

pub(crate) fn router() -> Router<AppState> {
    Router::new().route("/", get(quotas).put(set_client_quota))
}

async fn quotas(
    session: Session,
    Path(name): Path<String>,
) -> Result<Json<QuotaListing>, ApiError> {
    let cluster = session.cluster(&name)?;
    cluster.access.configs()?;
    Ok(Json(QuotaListing::new(
        cluster.store.quotas.load().as_deref(),
        cluster.store.quotas.health().into(),
    )))
}

async fn set_client_quota(
    session: Session,
    Path(name): Path<String>,
    extract::Json(request): extract::Json<ClientQuota>,
) -> Result<StatusCode, ApiError> {
    let cluster = session.cluster(&name)?;
    let quotas = cluster.manage_acls()?;
    quotas.set_client_quota(&request.into_quota()?).await?;
    Ok(StatusCode::NO_CONTENT)
}
