use juniper::graphql_object;

use super::context::{Cluster, GraphQlContext};
use super::error::GqlError;
use super::types::{
    AclListing, BrokerRow, ClusterGrant, ClusterHealth, ConfigEntry, GroupDetail, GroupRow,
    GroupRowPage, Identity, RecordPage, RecordQueryInput, RowFilter, SearchHit, SubjectDetail,
    SubjectRow, SubjectRowsResult, TopicDetail, TopicGroupRow, TopicRow, TopicRowPage, TopicSort,
    TopicSortField,
};

pub struct Query;

#[graphql_object(context = GraphQlContext)]
impl Query {
    /// Who the session is and, per cluster, exactly what it may do.
    fn whoami(context: &GraphQlContext) -> Identity {
        let clusters = context
            .access
            .visible_clusters(context.state.stores.names())
            .into_iter()
            .filter_map(|name| {
                let access = context.access.cluster(name).ok()?;
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

        Identity {
            subject: context.guard.subject().map(ToOwned::to_owned),
            clusters,
        }
    }

    /// Per-lane freshness and counts for every visible cluster. Replaces
    /// polling a catalog just to discover how stale it is.
    fn clusters(context: &GraphQlContext) -> Vec<ClusterHealth> {
        context
            .state
            .stores
            .iter()
            .filter(|store| context.access.can_see_cluster(store.name()))
            .map(|store| ClusterHealth::from(store.health()))
            .collect()
    }

    fn topic_rows(
        context: &GraphQlContext,
        cluster: String,
        filter: Option<RowFilter>,
        sort: Option<TopicSort>,
        after: Option<String>,
        limit: Option<i32>,
    ) -> Result<TopicRowPage, GqlError> {
        let cluster = context.cluster(&cluster)?;
        let filter = filter.unwrap_or_default();
        let sort = sort.unwrap_or_default();

        let mut rows: Vec<_> = cluster
            .store
            .topic_rows()
            .into_iter()
            .filter(|row| filter.matches(&row.name))
            .collect();
        sort_topics(&mut rows, &sort);

        let total = rows.len() as i32;
        let (rows, next_cursor) = page(rows, after.as_deref(), limit, |row| row.name.to_string());

        Ok(TopicRowPage {
            rows: rows.into_iter().map(TopicRow::from).collect(),
            total,
            next_cursor,
        })
    }

    fn topic(
        context: &GraphQlContext,
        cluster: String,
        name: String,
    ) -> Result<Option<TopicDetail>, GqlError> {
        let cluster = context.cluster(&cluster)?;
        Ok(cluster.store.topic_detail(&name).map(TopicDetail::from))
    }

    fn topic_groups(
        context: &GraphQlContext,
        cluster: String,
        topic: String,
    ) -> Result<Vec<TopicGroupRow>, GqlError> {
        let cluster = context.cluster(&cluster)?;
        Ok(cluster
            .store
            .topic_groups(&topic)
            .into_iter()
            .map(TopicGroupRow::from)
            .collect())
    }

    /// Served from the config lane, not the broker: a config sweep is slow
    /// and its result is the same for everyone.
    fn topic_configs(
        context: &GraphQlContext,
        cluster: String,
        name: String,
    ) -> Result<Vec<ConfigEntry>, GqlError> {
        let cluster = context.cluster(&cluster)?;
        let capability = cluster.access.configs()?;
        cluster
            .store
            .topic_configs(&name)
            .map(|entries| entries.into_iter().map(ConfigEntry::from).collect())
            .ok_or_else(|| unknown_topic(capability.cluster(), &name))
    }

    fn group_rows(
        context: &GraphQlContext,
        cluster: String,
        filter: Option<RowFilter>,
        after: Option<String>,
        limit: Option<i32>,
    ) -> Result<GroupRowPage, GqlError> {
        let cluster = context.cluster(&cluster)?;
        let filter = filter.unwrap_or_default();

        let rows: Vec<_> = cluster
            .store
            .group_rows()
            .into_iter()
            .filter(|row| filter.matches(&row.id))
            .collect();

        let total = rows.len() as i32;
        let (rows, next_cursor) = page(rows, after.as_deref(), limit, |row| row.id.to_string());

        Ok(GroupRowPage {
            rows: rows.into_iter().map(GroupRow::from).collect(),
            total,
            next_cursor,
        })
    }

    /// Registers interest, which promotes the group to the offsets lane's
    /// fast tier while someone is looking at it.
    fn group(
        context: &GraphQlContext,
        cluster: String,
        id: String,
    ) -> Result<Option<GroupDetail>, GqlError> {
        let cluster = context.cluster(&cluster)?;
        Ok(cluster.store.group_detail(&id).map(GroupDetail::from))
    }

    fn broker_rows(context: &GraphQlContext, cluster: String) -> Result<Vec<BrokerRow>, GqlError> {
        let cluster = context.cluster(&cluster)?;
        Ok(cluster
            .store
            .broker_rows()
            .into_iter()
            .map(BrokerRow::from)
            .collect())
    }

    /// No lane sweeps broker configs, so this one stays live.
    async fn broker_configs(
        context: &GraphQlContext,
        cluster: String,
        id: i32,
    ) -> Result<Vec<ConfigEntry>, GqlError> {
        let capability = context.cluster(&cluster)?.access.configs()?;
        Ok(context
            .state
            .live_broker_configs(capability.cluster(), id)
            .await?
            .into_iter()
            .map(ConfigEntry::from)
            .collect())
    }

    fn subject_rows(
        context: &GraphQlContext,
        cluster: String,
    ) -> Result<SubjectRowsResult, GqlError> {
        let cluster = context.cluster(&cluster)?;
        Ok(SubjectRowsResult {
            rows: cluster
                .store
                .subject_rows()
                .into_iter()
                .map(SubjectRow::from)
                .collect(),
            source_health: cluster.store.subjects.health().into(),
        })
    }

    /// Schema bodies are large, rarely read, and privileged, so the subjects
    /// lane keeps only the listing and the body is fetched on demand.
    async fn subject(
        context: &GraphQlContext,
        cluster: String,
        name: String,
        version: Option<i32>,
    ) -> Result<SubjectDetail, GqlError> {
        let cluster = context.cluster(&cluster)?;
        let capability = cluster.access.schema_text()?;
        let version = match version {
            Some(version) => version,
            None => latest_version(&cluster, &name)?,
        };

        Ok(SubjectDetail::new(
            name.clone(),
            version,
            context
                .state
                .live_subject_schema(capability.cluster(), &name, version)
                .await?,
        ))
    }

    async fn acls(context: &GraphQlContext, cluster: String) -> Result<AclListing, GqlError> {
        let capability = context.cluster(&cluster)?.access.acls()?;
        Ok(AclListing::from(
            context.state.live_acls(capability.cluster()).await?,
        ))
    }

    async fn records(
        context: &GraphQlContext,
        cluster: String,
        query: RecordQueryInput,
    ) -> Result<RecordPage, GqlError> {
        let capability = context.cluster(&cluster)?.access.records()?;
        Ok(RecordPage::from(
            context
                .state
                .live_records(capability.cluster(), query.try_into()?)
                .await?,
        ))
    }

    /// Answered from the prebuilt index, so a per-keystroke search never
    /// walks the catalog.
    fn search(
        context: &GraphQlContext,
        cluster: String,
        term: String,
    ) -> Result<Vec<SearchHit>, GqlError> {
        let cluster = context.cluster(&cluster)?;
        Ok(cluster
            .store
            .search(&term)
            .into_iter()
            .map(SearchHit::from)
            .collect())
    }
}

fn unknown_topic(cluster: &str, topic: &str) -> GqlError {
    GqlError::from(crate::kafka::KafkaError::UnknownTopic {
        cluster: cluster.to_owned(),
        topic: topic.to_owned(),
    })
}

fn latest_version(cluster: &Cluster<'_>, subject: &str) -> Result<i32, GqlError> {
    cluster
        .store
        .subject_rows()
        .into_iter()
        .find(|row| row.subject.as_ref() == subject)
        .map(|row| row.info.latest_version)
        .ok_or_else(|| {
            GqlError::from(crate::kafka::KafkaError::UnknownSubject {
                cluster: cluster.name().to_owned(),
                subject: subject.to_owned(),
                version: 0,
            })
        })
}

fn sort_topics(rows: &mut [crate::kafka::store::TopicRow], sort: &TopicSort) {
    match sort.field.unwrap_or(TopicSortField::Name) {
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
    if sort.desc {
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
}
