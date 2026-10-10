use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::CallToolResult;
use rmcp::{tool, tool_router};
use schemars::JsonSchema;
use serde::Deserialize;
use serde_json::json;

use crate::app::context::Session;
use crate::app::error::ApiError;
use crate::app::search::types::SearchKind;

use super::super::types::{
    AccessList, ClusterDetail, ClusterList, ClusterRights, ClusterRow, SearchResult,
};
use super::super::untrusted::Boundary;
use super::super::{CLIENT_VALUES_NOTICE, MAX_QUERY_CHARS};
use crate::app::mcp::fit::fitted;
use crate::app::mcp::gate::rights;
use crate::app::mcp::lanes::{lane_error_notice, topology, unread};
use crate::app::mcp::server::KlensMcp;
use crate::app::mcp::view::{hits, unhealthy_partitions};
#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ClusterFilter {
    /// One cluster's name. Omit it for every cluster you can see.
    cluster: Option<String>,
}

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct SearchQuery {
    /// Words to match against names.
    #[schemars(length(max = MAX_QUERY_CHARS))]
    query: String,
    /// One cluster's name. Omit it to search every cluster you can see.
    cluster: Option<String>,
}

#[tool_router(router = cluster_tools, vis = "pub(super)")]
impl KlensMcp {
    /// Lists the Kafka clusters you can see with their health: broker, topic, partition, group and subject counts, under-replicated and offline partition counts, and each background read (lane) that failed or has not run yet.
    /// Pass `cluster` to also list that cluster's under-replicated and offline partitions, offline first, with their leaders and replicas.
    /// Call this first to learn the names other tools take as `cluster`.
    #[tool(
        title = "List clusters and their health",
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn klens_clusters(
        &self,
        session: Session,
        Parameters(filter): Parameters<ClusterFilter>,
    ) -> Result<CallToolResult, ApiError> {
        let boundary = Boundary::new();
        let Some(name) = filter.cluster else {
            let rows: Vec<ClusterRow> = session
                .clusters()
                .map(|cluster| ClusterRow::new(cluster.store.health(), &boundary))
                .collect();
            let notice = rows
                .iter()
                .any(ClusterRow::has_lane_error)
                .then(|| lane_error_notice(&boundary));
            return Ok(fitted(
                &rows,
                "pass `cluster` to read one cluster",
                |clusters, truncated| {
                    json!(ClusterList {
                        clusters,
                        notice: notice.as_deref(),
                        truncated
                    })
                },
            ));
        };
        let cluster = session.cluster(&name)?;
        let partitions = unhealthy_partitions(&*topology(&cluster)?);
        let row = ClusterRow::new(cluster.store.health(), &boundary);
        let notice = row.has_lane_error().then(|| lane_error_notice(&boundary));
        Ok(fitted(
            &partitions,
            "the counts above cover every partition",
            |unhealthy_partitions, truncated| {
                json!(ClusterDetail {
                    row: &row,
                    unhealthy_partitions,
                    notice: notice.as_deref(),
                    truncated,
                })
            },
        ))
    }

    /// Explains what you may do on each cluster you can see: your privileges under the ceiling the klens operator set for MCP, whether the cluster accepts changes, and each tool or section with the privilege it needs when it is not available.
    /// Everyone sees the catalog of clusters, topics, groups, brokers and subjects. Privileges cover record payloads, configs, schema text, ACLs and changes. A tool that changes Kafka is available only on a cluster that accepts changes.
    /// Call it after a FORBIDDEN or READ_ONLY_CLUSTER error, or before work that needs a privilege.
    #[tool(
        title = "Explain what you may do",
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn klens_access_explain(
        &self,
        session: Session,
        Parameters(filter): Parameters<ClusterFilter>,
    ) -> Result<CallToolResult, ApiError> {
        let clusters: Vec<ClusterRights> = match &filter.cluster {
            Some(name) => vec![rights(&session.cluster(name)?)],
            None => session.clusters().map(|cluster| rights(&cluster)).collect(),
        };
        Ok(fitted(
            &clusters,
            "pass `cluster` to explain one cluster",
            |clusters, truncated| {
                json!(AccessList {
                    clusters,
                    truncated
                })
            },
        ))
    }

    /// Finds topics, consumer groups, brokers and schema subjects by name, on one cluster or on every cluster you can see. Use it to turn a vague name into the exact one.
    /// Matching is fuzzy and ignores case: `ord cre` finds `orders.created`, `!test` leaves out names that match `test`, and `^prod` keeps names that start with `prod`.
    /// Each cluster gives up to 20 matches, best first, each with its kind (TOPIC, GROUP, NODE for a broker, or SUBJECT) and exact id.
    /// `notReady` names each cluster klens has not read yet, where no match does not mean the name is absent.
    #[tool(
        title = "Search names",
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn klens_search(
        &self,
        session: Session,
        Parameters(search): Parameters<SearchQuery>,
    ) -> Result<CallToolResult, ApiError> {
        if search.query.chars().count() > MAX_QUERY_CHARS {
            return Err(ApiError::unprocessable(format!(
                "a query holds at most {MAX_QUERY_CHARS} characters"
            )));
        }
        let clusters = match &search.cluster {
            Some(name) => {
                let cluster = session.cluster(name)?;
                topology(&cluster)?;
                vec![cluster]
            }
            None => session.clusters().collect(),
        };
        let boundary = Boundary::new();
        let mut found = Vec::new();
        let mut not_ready = Vec::new();
        for cluster in &clusters {
            if let Some(lane) = unread(cluster, "topology", &cluster.store.topology, &boundary) {
                not_ready.push(lane);
                continue;
            }
            not_ready.extend(unread(
                cluster,
                "subjects",
                &cluster.store.subjects,
                &boundary,
            ));
            found.extend(hits(cluster, &search.query));
        }
        let notices: Vec<String> = [
            found
                .iter()
                .any(|found| matches!(found.hit.kind, SearchKind::Group | SearchKind::Subject))
                .then(|| CLIENT_VALUES_NOTICE.to_owned()),
            not_ready
                .iter()
                .any(|lane| lane.last_error.is_some())
                .then(|| lane_error_notice(&boundary)),
        ]
        .into_iter()
        .flatten()
        .collect();
        let notice = (!notices.is_empty()).then(|| notices.join(" "));
        Ok(fitted(
            &found,
            "pass `cluster` or a longer query",
            |hits, truncated| {
                json!(SearchResult {
                    hits,
                    not_ready: &not_ready,
                    notice: notice.as_deref(),
                    truncated,
                })
            },
        ))
    }
}
