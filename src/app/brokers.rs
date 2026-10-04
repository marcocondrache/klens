use axum::Json;
use axum::Router;
use axum::http::StatusCode;
use axum::routing::{get, patch};

use crate::AppState;
use crate::kafka::model::BrokerScope;

use super::configs::{ConfigEntry, EditConfigs};
use super::context::Session;
use super::error::ApiError;
use super::extract::{self, Path};

pub mod types;

#[cfg(test)]
mod tests;

pub(crate) use types::BrokerRow;

pub(crate) fn router() -> Router<AppState> {
    Router::new()
        .route("/", get(brokers))
        .route("/configs", patch(alter_broker_defaults))
        .route(
            "/{id}/configs",
            get(broker_configs).patch(alter_broker_configs),
        )
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

async fn alter_broker_configs(
    session: Session,
    Path((name, id)): Path<(String, i32)>,
    extract::Json(request): extract::Json<EditConfigs>,
) -> Result<StatusCode, ApiError> {
    alter(&session, &name, BrokerScope::Broker(id), request).await
}

async fn alter_broker_defaults(
    session: Session,
    Path(name): Path<String>,
    extract::Json(request): extract::Json<EditConfigs>,
) -> Result<StatusCode, ApiError> {
    alter(&session, &name, BrokerScope::Cluster, request).await
}

async fn alter(
    session: &Session,
    name: &str,
    scope: BrokerScope,
    request: EditConfigs,
) -> Result<StatusCode, ApiError> {
    let cluster = session.cluster(name)?;
    let brokers = cluster.manage_brokers()?;
    brokers
        .alter_broker_configs(scope, &request.into_edit()?)
        .await?;
    Ok(StatusCode::NO_CONTENT)
}
