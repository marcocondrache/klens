use axum::Json;
use axum::Router;
use axum::extract::{Path, Query};
use axum::routing::get;
use serde::Deserialize;

use crate::AppState;
use crate::kafka::KafkaError;

use super::context::Session;
use super::error::ApiError;
use super::types::{
    AclListing, BrokerRow, ClusterGrant, ClusterHealth, ConfigEntry, GroupDetail, GroupRow,
    GroupRowPage, Identity, RecordPage, RecordParams, SearchHit, SubjectDetail, SubjectRow,
    SubjectRowsResult, TopicDetail, TopicGroupRow, TopicRow, TopicRowPage, TopicSortField,
    record_query,
};
use super::updates;

pub(crate) fn router() -> Router<AppState> {
    Router::new()
        .route("/api/whoami", get(whoami))
        .route("/api/clusters", get(clusters))
        .route("/api/clusters/{cluster}", get(cluster))
        .route("/api/clusters/{cluster}/topics", get(topics))
        .route("/api/clusters/{cluster}/topics/{topic}", get(topic))
        .route(
            "/api/clusters/{cluster}/topics/{topic}/groups",
            get(topic_groups),
        )
        .route(
            "/api/clusters/{cluster}/topics/{topic}/configs",
            get(topic_configs),
        )
        .route(
            "/api/clusters/{cluster}/topics/{topic}/records",
            get(records),
        )
        .route("/api/clusters/{cluster}/groups", get(groups))
        // Group ids and subject names may contain `/`.
        .route("/api/clusters/{cluster}/groups/{*group}", get(group))
        .route("/api/clusters/{cluster}/brokers", get(brokers))
        .route(
            "/api/clusters/{cluster}/brokers/{id}/configs",
            get(broker_configs),
        )
        .route("/api/clusters/{cluster}/subjects", get(subjects))
        .route("/api/clusters/{cluster}/subjects/{*subject}", get(subject))
        .route("/api/clusters/{cluster}/acls", get(acls))
        .route("/api/clusters/{cluster}/search", get(search))
        .route("/api/clusters/{cluster}/updates", get(updates::updates))
}

async fn whoami(session: Session) -> Json<Identity> {
    let clusters = session
        .access
        .visible_clusters(session.state.stores.names())
        .into_iter()
        .filter_map(|name| {
            let access = session.access.cluster(name).ok()?;
            Some(ClusterGrant {
                cluster: name.to_owned(),
                roles: access
                    .role_names()
                    .into_iter()
                    .map(ToOwned::to_owned)
                    .collect(),
                privileges: access.privileges().into_iter().map(Into::into).collect(),
            })
        })
        .collect();

    Json(Identity {
        subject: session.guard.subject().map(ToOwned::to_owned),
        clusters,
    })
}

async fn clusters(session: Session) -> Json<Vec<ClusterHealth>> {
    Json(
        session
            .state
            .stores
            .iter()
            .filter(|store| session.access.can_see_cluster(store.name()))
            .map(|store| ClusterHealth::from(store.health()))
            .collect(),
    )
}

async fn cluster(
    session: Session,
    Path(name): Path<String>,
) -> Result<Json<ClusterHealth>, ApiError> {
    let cluster = session.cluster(&name)?;
    Ok(Json(ClusterHealth::from(cluster.store.health())))
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
    let mut rows: Vec<_> = cluster
        .store
        .topic_rows()
        .into_iter()
        .filter(|row| name_matches(query.contains.as_deref(), &row.name))
        .collect();
    sort_topics(
        &mut rows,
        query.sort.unwrap_or(TopicSortField::Name),
        query.desc,
    );

    let total = rows.len() as i32;
    let (rows, next_cursor) = page(rows, query.after.as_deref(), query.limit, |row| {
        row.name.to_string()
    });

    Ok(Json(TopicRowPage {
        rows: rows.into_iter().map(TopicRow::from).collect(),
        total,
        next_cursor,
    }))
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

#[derive(Debug, Default, Deserialize)]
struct PageQuery {
    contains: Option<String>,
    after: Option<String>,
    limit: Option<i32>,
}

async fn groups(
    session: Session,
    Path(name): Path<String>,
    Query(query): Query<PageQuery>,
) -> Result<Json<GroupRowPage>, ApiError> {
    let cluster = session.cluster(&name)?;
    let rows: Vec<_> = cluster
        .store
        .group_rows()
        .into_iter()
        .filter(|row| name_matches(query.contains.as_deref(), &row.id))
        .collect();

    let total = rows.len() as i32;
    let (rows, next_cursor) = page(rows, query.after.as_deref(), query.limit, |row| {
        row.id.to_string()
    });

    Ok(Json(GroupRowPage {
        rows: rows.into_iter().map(GroupRow::from).collect(),
        total,
        next_cursor,
    }))
}

async fn group(
    session: Session,
    Path((name, id)): Path<(String, String)>,
) -> Result<Json<GroupDetail>, ApiError> {
    let cluster = session.cluster(&name)?;
    cluster
        .store
        .group_detail(&id)
        .map(GroupDetail::from)
        .map(Json)
        .ok_or_else(|| {
            KafkaError::UnknownGroup {
                cluster: cluster.name().to_owned(),
                group: id,
            }
            .into()
        })
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
    let capability = session.cluster(&name)?.access.configs()?;
    Ok(Json(
        session
            .state
            .live_broker_configs(capability.cluster(), id)
            .await?
            .into_iter()
            .map(ConfigEntry::from)
            .collect(),
    ))
}

async fn subjects(
    session: Session,
    Path(name): Path<String>,
) -> Result<Json<SubjectRowsResult>, ApiError> {
    let cluster = session.cluster(&name)?;
    Ok(Json(SubjectRowsResult {
        rows: cluster
            .store
            .subject_rows()
            .into_iter()
            .map(SubjectRow::from)
            .collect(),
        source_health: cluster.store.subjects.health().into(),
    }))
}

#[derive(Debug, Default, Deserialize)]
struct VersionQuery {
    version: Option<i32>,
}

async fn subject(
    session: Session,
    Path((name, subject)): Path<(String, String)>,
    Query(query): Query<VersionQuery>,
) -> Result<Json<SubjectDetail>, ApiError> {
    let cluster = session.cluster(&name)?;
    let capability = cluster.access.schema_text()?;
    let version = match query.version {
        Some(version) => version,
        None => latest_version(&cluster, &subject)?,
    };

    Ok(Json(SubjectDetail::new(
        subject.clone(),
        version,
        session
            .state
            .live_subject_schema(capability.cluster(), &subject, version)
            .await?,
    )))
}

async fn acls(session: Session, Path(name): Path<String>) -> Result<Json<AclListing>, ApiError> {
    let capability = session.cluster(&name)?.access.acls()?;
    Ok(Json(AclListing::from(
        session.state.live_acls(capability.cluster()).await?,
    )))
}

async fn records(
    session: Session,
    Path((name, topic)): Path<(String, String)>,
    Query(params): Query<RecordParams>,
) -> Result<Json<RecordPage>, ApiError> {
    let capability = session.cluster(&name)?.access.records()?;
    Ok(Json(RecordPage::from(
        session
            .state
            .live_records(capability.cluster(), record_query(topic, params)?)
            .await?,
    )))
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

fn unknown_topic(cluster: &str, topic: &str) -> ApiError {
    KafkaError::UnknownTopic {
        cluster: cluster.to_owned(),
        topic: topic.to_owned(),
    }
    .into()
}

fn latest_version(
    cluster: &super::context::ClusterHandle<'_>,
    subject: &str,
) -> Result<i32, ApiError> {
    cluster
        .store
        .subject_rows()
        .into_iter()
        .find(|row| row.subject.as_ref() == subject)
        .map(|row| row.info.latest_version)
        .ok_or_else(|| {
            KafkaError::UnknownSubject {
                cluster: cluster.name().to_owned(),
                subject: subject.to_owned(),
                version: 0,
            }
            .into()
        })
}

fn name_matches(contains: Option<&str>, name: &str) -> bool {
    match contains.map(str::trim).filter(|value| !value.is_empty()) {
        None => true,
        Some(needle) => name.to_lowercase().contains(&needle.to_lowercase()),
    }
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

fn page<T>(
    rows: Vec<T>,
    after: Option<&str>,
    limit: Option<i32>,
    key: impl Fn(&T) -> String,
) -> (Vec<T>, Option<String>) {
    let start = match after.map(str::trim).filter(|after| !after.is_empty()) {
        None => 0,
        Some(after) => rows
            .iter()
            .position(|row| key(row) == after)
            .map_or(0, |index| index + 1),
    };

    let limit = limit
        .filter(|limit| *limit > 0)
        .map_or(rows.len(), |limit| limit as usize);

    let mut rows: Vec<T> = rows.into_iter().skip(start).collect();
    let exhausted = rows.len() <= limit;
    rows.truncate(limit);

    let next_cursor = (!exhausted).then(|| rows.last().map(&key)).flatten();
    (rows, next_cursor)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn keys(rows: &[&str]) -> Vec<String> {
        rows.iter().map(|row| (*row).to_owned()).collect()
    }

    #[test]
    fn an_absent_limit_returns_every_row() {
        let (rows, cursor) = page(keys(&["a", "b", "c"]), None, None, Clone::clone);

        assert_eq!(rows, keys(&["a", "b", "c"]));
        assert_eq!(cursor, None);
    }

    #[test]
    fn paging_resumes_after_the_cursor_row() {
        let rows = keys(&["a", "b", "c", "d"]);

        let (first, cursor) = page(rows.clone(), None, Some(2), Clone::clone);
        assert_eq!(first, keys(&["a", "b"]));
        assert_eq!(cursor.as_deref(), Some("b"));

        let (second, cursor) = page(rows, cursor.as_deref(), Some(2), Clone::clone);
        assert_eq!(second, keys(&["c", "d"]));
        assert_eq!(cursor, None, "the last page has no cursor");
    }

    #[test]
    fn a_vanished_cursor_row_restarts_rather_than_failing() {
        let (rows, _) = page(keys(&["a", "b"]), Some("gone"), Some(1), Clone::clone);

        assert_eq!(rows, keys(&["a"]));
    }

    #[test]
    fn name_filters_are_case_insensitive_substrings() {
        assert!(name_matches(Some("ORDERS"), "orders.created"));
        assert!(!name_matches(Some("pay"), "orders.created"));
        assert!(name_matches(Some("   "), "anything"));
        assert!(name_matches(None, "anything"));
    }
}
