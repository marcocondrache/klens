use axum::Json;
use axum::Router;
use axum::routing::get;

use crate::AppState;

use super::configs::ConfigEntry;
use super::context::Session;
use super::error::ApiError;
use super::extract::Path;

pub mod types;

#[cfg(test)]
mod tests;

pub(crate) use types::BrokerRow;

pub(crate) fn router() -> Router<AppState> {
    Router::new()
        .route("/", get(brokers))
        .route("/{id}/configs", get(broker_configs))
}

async fn brokers(
    session: Session,
    Path(name): Path<String>,
) -> Result<Json<Vec<BrokerRow>>, ApiError> {
    let cluster = session.cluster(&name)?;
    Ok(Json(
        cluster
            .store
            .broker_rows()
            .into_iter()
            .map(BrokerRow::from)
            .collect(),
    ))
}

async fn broker_configs(
    session: Session,
    Path((name, id)): Path<(String, i32)>,
) -> Result<Json<Vec<ConfigEntry>>, ApiError> {
    let configs = session.cluster(&name)?.configs()?;
    Ok(Json(
        configs
            .broker_configs(id)
            .await?
            .into_iter()
            .map(ConfigEntry::from)
            .collect(),
    ))
}
