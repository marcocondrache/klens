use axum::Json;
use axum::Router;
use axum::http::StatusCode;
use axum::routing::get;
use serde::Deserialize;

use crate::AppState;
use crate::kafka::KafkaError;
use crate::kafka::model::NewTopic;

use super::configs::ConfigEntry;
use super::context::Session;
use super::error::ApiError;
use super::extract::{self, Path, Query};
use super::paging::{name_filter, page};

pub mod types;

#[cfg(test)]
mod tests;

pub(crate) use types::{
    CreateTopic, TopicDetail, TopicGroupRow, TopicRow, TopicRowPage, TopicSortField,
};

pub(crate) fn router() -> Router<AppState> {
    Router::new()
        .route("/", get(topics).post(create_topic))
        .nest("/{topic}", topic_routes())
}

fn topic_routes() -> Router<AppState> {
    Router::new()
        .route("/", get(topic))
        .route("/groups", get(topic_groups))
        .route("/configs", get(topic_configs))
        .nest("/records", super::records::router())
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TopicQuery {
    contains: Option<String>,
    sort: Option<TopicSortField>,
    #[serde(default)]
    desc: bool,
    after: Option<String>,
    limit: Option<i32>,
}

async fn topics(
    session: Session,
    Path(name): Path<String>,
    Query(query): Query<TopicQuery>,
) -> Result<Json<TopicRowPage>, ApiError> {
    let cluster = session.cluster(&name)?;
    let matches = name_filter(query.contains.as_deref());
    let mut rows: Vec<_> = cluster
        .store
        .topic_rows()
        .into_iter()
        .filter(|row| matches(&row.name))
        .collect();
    sort_topics(
        &mut rows,
        query.sort.unwrap_or(TopicSortField::Name),
        query.desc,
    );

    let total = rows.len() as i32;
    let (rows, next_cursor) = page(rows, query.after.as_deref(), query.limit, |row| &row.name);

    Ok(Json(TopicRowPage {
        rows: rows.into_iter().map(TopicRow::from).collect(),
        total,
        next_cursor,
    }))
}

async fn create_topic(
    session: Session,
    Path(name): Path<String>,
    extract::Json(request): extract::Json<CreateTopic>,
) -> Result<StatusCode, ApiError> {
    let cluster = session.cluster(&name)?;
    let capability = cluster.access.manage_topics()?;
    let topic = NewTopic::try_from(request)?;
    let outcome = session.state.create_topic(capability, &topic).await;
    session.audit(capability.cluster(), "topic.create", topic.name(), &outcome);
    outcome?;
    Ok(StatusCode::CREATED)
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
    let capability = cluster.access.configs()?;
    cluster
        .store
        .topic_configs(&topic)
        .map(|entries| Json(entries.into_iter().map(ConfigEntry::from).collect()))
        .ok_or_else(|| unknown_topic(capability.cluster(), &topic))
}

fn unknown_topic(cluster: &str, topic: &str) -> ApiError {
    KafkaError::UnknownTopic {
        cluster: cluster.to_owned(),
        topic: topic.to_owned(),
    }
    .into()
}

fn sort_topics(rows: &mut [crate::kafka::store::TopicRow], field: TopicSortField, desc: bool) {
    match field {
        TopicSortField::Name => rows.sort_by(|left, right| left.name.cmp(&right.name)),
        TopicSortField::Rate => {
            rows.sort_by(|left, right| {
                left.rate
                    .total_cmp(&right.rate)
                    .then(left.name.cmp(&right.name))
            });
        }
        TopicSortField::RetainedMessages => rows.sort_by(|left, right| {
            left.retained_messages
                .cmp(&right.retained_messages)
                .then(left.name.cmp(&right.name))
        }),
        TopicSortField::Partitions => rows.sort_by(|left, right| {
            left.partition_count
                .cmp(&right.partition_count)
                .then(left.name.cmp(&right.name))
        }),
        TopicSortField::Groups => rows.sort_by(|left, right| {
            left.group_count
                .cmp(&right.group_count)
                .then(left.name.cmp(&right.name))
        }),
    }
    if desc {
        rows.reverse();
    }
}
