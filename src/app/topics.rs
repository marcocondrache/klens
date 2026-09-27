use axum::Json;
use axum::Router;
use axum::routing::get;

use crate::AppState;
use crate::kafka::KafkaError;

use super::configs::ConfigEntry;
use super::context::Session;
use super::error::ApiError;
use super::extract::Path;

pub mod types;

#[cfg(test)]
mod tests;

pub(crate) use types::{TopicDetail, TopicGroupRow, TopicRow};

pub(crate) fn router() -> Router<AppState> {
    Router::new()
        .route("/", get(topics))
        .nest("/{topic}", topic_routes())
}

fn topic_routes() -> Router<AppState> {
    Router::new()
        .route("/", get(topic))
        .route("/groups", get(topic_groups))
        .route("/configs", get(topic_configs))
        .nest("/records", super::records::router())
}

async fn topics(
    session: Session,
    Path(name): Path<String>,
) -> Result<Json<Vec<TopicRow>>, ApiError> {
    let cluster = session.cluster(&name)?;
    Ok(Json(
        cluster
            .store
            .topic_rows()
            .into_iter()
            .map(TopicRow::from)
            .collect(),
    ))
}

async fn topic(
    session: Session,
    Path((name, topic)): Path<(String, String)>,
) -> Result<Json<TopicDetail>, ApiError> {
    let cluster = session.cluster(&name)?;
    cluster
        .store
        .topic_detail(&topic)
        .map(TopicDetail::from)
        .map(Json)
        .ok_or_else(|| unknown_topic(cluster.name(), &topic))
}

async fn topic_groups(
    session: Session,
    Path((name, topic)): Path<(String, String)>,
) -> Result<Json<Vec<TopicGroupRow>>, ApiError> {
    let cluster = session.cluster(&name)?;
    Ok(Json(
        cluster
            .store
            .topic_groups(&topic)
            .into_iter()
            .map(TopicGroupRow::from)
            .collect(),
    ))
}

async fn topic_configs(
    session: Session,
    Path((name, topic)): Path<(String, String)>,
) -> Result<Json<Vec<ConfigEntry>>, ApiError> {
    let cluster = session.cluster(&name)?;
    cluster.access.configs()?;
    cluster
        .store
        .topic_configs(&topic)
        .map(|entries| Json(entries.into_iter().map(ConfigEntry::from).collect()))
        .ok_or_else(|| unknown_topic(cluster.name(), &topic))
}

fn unknown_topic(cluster: &str, topic: &str) -> ApiError {
    KafkaError::UnknownTopic {
        cluster: cluster.to_owned(),
        topic: topic.to_owned(),
    }
    .into()
}
