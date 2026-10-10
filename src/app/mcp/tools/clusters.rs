use jiff::Timestamp;
use rmcp::handler::server::wrapper::Parameters;
use rmcp::{tool, tool_router};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::app::context::{ClusterHandle, Session};
use crate::app::error::ApiError;
use crate::app::mcp::ext::ClusterExt as _;
use crate::app::mcp::gate::{ToolGate, ToolRights};
use crate::app::mcp::reply::{Rows, fit, reply};
use crate::app::mcp::server::{KlensMcp, ToolResult};
use crate::app::mcp::tools::gates;
use crate::app::mcp::untrusted::Boundary;
use crate::app::mcp::{CLIENT_VALUES_NOTICE, MAX_QUERY_CHARS};
use crate::app::search::SearchHit;
use crate::app::search::types::SearchKind;
use crate::app::whoami::types::PrivilegeName;
use crate::kafka::store::projections::ClusterHealthView;
use crate::kafka::store::{Lane, LaneHealth};

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

pub(super) const GATES: &[ToolGate] = &[
    ToolGate::open("klens_clusters"),
    ToolGate::open("klens_access_explain"),
    ToolGate::open("klens_search"),
];

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ClusterRow {
    cluster: String,
    ready: bool,
    broker_count: Option<i32>,
    topic_count: Option<i32>,
    partition_count: Option<i32>,
    group_count: Option<i32>,
    subject_count: Option<i32>,
    under_replicated_partitions: Option<i32>,
    offline_partitions: Option<i32>,
    unhealthy_lanes: Vec<UnhealthyLane>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct UnhealthyLane {
    lane: &'static str,
    last_error: Option<String>,
    updated_at: Option<Timestamp>,
}

impl ClusterRow {
    /// Counts stay null until the lane that holds them has been read.
    fn new(health: ClusterHealthView, boundary: &Boundary) -> Self {
        let ready = health.topology.updated_at.is_some();
        let measured = |count| ready.then_some(count);
        let subjects_read = health.subjects.updated_at.is_some();
        let lanes = [
            ("topology", health.topology),
            ("watermarks", health.watermarks),
            ("offsets", health.offsets),
            ("configs", health.configs),
            ("subjects", health.subjects),
            ("log_dirs", health.log_dirs),
            ("acls", health.acls),
            ("quotas", health.quotas),
            ("scram_users", health.scram_users),
        ];
        Self {
            ready,
            cluster: health.cluster,
            broker_count: measured(health.broker_count),
            topic_count: measured(health.topic_count),
            partition_count: measured(health.partition_count),
            group_count: measured(health.group_count),
            subject_count: subjects_read.then_some(health.subject_count),
            under_replicated_partitions: measured(health.under_replicated_partitions),
            offline_partitions: measured(health.offline_partitions),
            unhealthy_lanes: lanes
                .into_iter()
                .filter(|(_, lane)| !lane.healthy())
                .map(|(lane, health)| UnhealthyLane::new(lane, health, boundary))
                .collect(),
        }
    }

    fn has_lane_error(&self) -> bool {
        self.unhealthy_lanes
            .iter()
            .any(|lane| lane.last_error.is_some())
    }
}

impl UnhealthyLane {
    fn new(lane: &'static str, health: LaneHealth, boundary: &Boundary) -> Self {
        Self {
            lane,
            last_error: boundary.lane_error(health.last_error),
            updated_at: health.updated_at,
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct UnhealthyPartition {
    topic: String,
    partition: i32,
    leader: Option<i32>,
    replicas: Vec<i32>,
    isr: Vec<i32>,
    offline: bool,
}

impl UnhealthyPartition {
    /// Every partition of the cluster that is under-replicated or offline, offline first.
    fn all(cluster: &ClusterHandle<'_>) -> Result<Vec<Self>, ApiError> {
        let topology = cluster.topology()?;
        let mut partitions: Vec<Self> = topology
            .topics
            .iter()
            .flat_map(|(topic, info)| {
                info.partitions
                    .iter()
                    .filter(|partition| partition.under_replicated() || partition.offline())
                    .map(|partition| Self {
                        topic: topic.to_string(),
                        partition: partition.id,
                        leader: (!partition.offline()).then_some(partition.leader),
                        replicas: partition.replicas.clone(),
                        isr: partition.isr.clone(),
                        offline: partition.offline(),
                    })
            })
            .collect();
        partitions.sort_by_key(|partition| !partition.offline);
        Ok(partitions)
    }
}

#[derive(Serialize)]
struct ClusterList {
    clusters: Rows<ClusterRow>,
    #[serde(skip_serializing_if = "Option::is_none")]
    notice: Option<String>,
}

reply!(ClusterList: clusters; "pass `cluster` to read one cluster");

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ClusterDetail {
    #[serde(flatten)]
    row: ClusterRow,
    unhealthy_partitions: Rows<UnhealthyPartition>,
    #[serde(skip_serializing_if = "Option::is_none")]
    notice: Option<String>,
}

reply!(ClusterDetail: unhealthy_partitions; "the counts above cover every partition");

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ClusterRights {
    cluster: String,
    writable: bool,
    privileges: Vec<PrivilegeName>,
    tools: Vec<ToolRights>,
}

impl ClusterRights {
    fn of(cluster: &ClusterHandle<'_>) -> Self {
        Self {
            cluster: cluster.name().to_owned(),
            writable: cluster.is_writable(),
            privileges: cluster
                .access
                .privileges()
                .into_iter()
                .map(Into::into)
                .collect(),
            tools: gates().flat_map(|gate| gate.rights(cluster)).collect(),
        }
    }
}

#[derive(Serialize)]
struct AccessList {
    clusters: Rows<ClusterRights>,
}

reply!(AccessList: clusters; "pass `cluster` to explain one cluster");

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ClusterHit {
    cluster: String,
    #[serde(flatten)]
    hit: SearchHit,
}

/// A cluster lane klens has not read, where no match does not mean no name.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct UnreadLane {
    cluster: String,
    lane: &'static str,
    last_error: Option<String>,
}

impl UnreadLane {
    fn of<T>(
        cluster: &ClusterHandle<'_>,
        name: &'static str,
        lane: &Lane<T>,
        boundary: &Boundary,
    ) -> Option<Self> {
        (!lane.ready()).then(|| Self {
            cluster: cluster.name().to_owned(),
            lane: name,
            last_error: boundary.lane_error(lane.health().last_error),
        })
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct SearchResult {
    hits: Rows<ClusterHit>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    not_ready: Vec<UnreadLane>,
    #[serde(skip_serializing_if = "Option::is_none")]
    notice: Option<String>,
}

impl SearchResult {
    /// The matches in each cluster, and the clusters whose names klens has not read.
    fn find(clusters: &[ClusterHandle<'_>], query: &str) -> Self {
        let boundary = Boundary::new();
        let mut hits = Vec::new();
        let mut not_ready = Vec::new();
        for cluster in clusters {
            let topology = &cluster.store.topology;
            if let Some(lane) = UnreadLane::of(cluster, "topology", topology, &boundary) {
                not_ready.push(lane);
                continue;
            }
            let subjects = &cluster.store.subjects;
            not_ready.extend(UnreadLane::of(cluster, "subjects", subjects, &boundary));
            hits.extend(
                cluster
                    .store
                    .search(query)
                    .into_iter()
                    .map(|hit| ClusterHit {
                        cluster: cluster.name().to_owned(),
                        hit: SearchHit::from(hit),
                    }),
            );
        }
        let names_client_values = hits
            .iter()
            .any(|found| matches!(found.hit.kind, SearchKind::Group | SearchKind::Subject));
        let carries_lane_error = not_ready.iter().any(|lane| lane.last_error.is_some());
        let notices: Vec<String> = [
            names_client_values.then(|| CLIENT_VALUES_NOTICE.to_owned()),
            carries_lane_error.then(|| boundary.lane_error_notice()),
        ]
        .into_iter()
        .flatten()
        .collect();
        Self {
            hits: Rows::new("hits", hits),
            not_ready,
            notice: (!notices.is_empty()).then(|| notices.join(" ")),
        }
    }
}

reply!(SearchResult: hits; "pass `cluster` or a longer query");

#[tool_router(router = cluster_tools, vis = "pub(super)")]
impl KlensMcp {
    /// Lists the Kafka clusters you can see with their health: broker, topic, partition, group and subject counts, under-replicated and offline partition counts, and each background read (lane) that failed or has not run yet.
    /// Pass `cluster` to also list that cluster's under-replicated and offline partitions, offline first, with their leaders and replicas.
    /// Call this first to learn the names other tools take as `cluster`.
    #[tool(title = "List clusters and their health")]
    async fn klens_clusters(
        &self,
        session: Session,
        Parameters(filter): Parameters<ClusterFilter>,
    ) -> ToolResult {
        let boundary = Boundary::new();
        let Some(name) = filter.cluster else {
            let rows: Vec<ClusterRow> = session
                .clusters()
                .map(|cluster| ClusterRow::new(cluster.store.health(), &boundary))
                .collect();
            let carries_lane_error = rows.iter().any(ClusterRow::has_lane_error);
            return Ok(fit(ClusterList {
                clusters: Rows::new("clusters", rows),
                notice: carries_lane_error.then(|| boundary.lane_error_notice()),
            }));
        };
        let cluster = session.cluster(&name)?;
        let partitions = UnhealthyPartition::all(&cluster)?;
        let row = ClusterRow::new(cluster.store.health(), &boundary);
        Ok(fit(ClusterDetail {
            notice: row.has_lane_error().then(|| boundary.lane_error_notice()),
            row,
            unhealthy_partitions: Rows::new("unhealthyPartitions", partitions),
        }))
    }

    /// Explains what you may do on each cluster you can see: your privileges under the ceiling the klens operator set for MCP, whether the cluster accepts changes, and each tool or section with the privilege it needs when it is not available.
    /// Everyone sees the catalog of clusters, topics, groups, brokers and subjects. Privileges cover record payloads, configs, schema text, ACLs and changes. A tool that changes Kafka is available only on a cluster that accepts changes.
    /// Call it after a FORBIDDEN or READ_ONLY_CLUSTER error, or before work that needs a privilege.
    #[tool(title = "Explain what you may do")]
    async fn klens_access_explain(
        &self,
        session: Session,
        Parameters(filter): Parameters<ClusterFilter>,
    ) -> ToolResult {
        let clusters: Vec<ClusterRights> = match &filter.cluster {
            Some(name) => vec![ClusterRights::of(&session.cluster(name)?)],
            None => session.clusters().map(|c| ClusterRights::of(&c)).collect(),
        };
        Ok(fit(AccessList {
            clusters: Rows::new("clusters", clusters),
        }))
    }

    /// Finds topics, consumer groups, brokers and schema subjects by name, on one cluster or on every cluster you can see. Use it to turn a vague name into the exact one.
    /// Matching is fuzzy and ignores case: `ord cre` finds `orders.created`, `!test` leaves out names that match `test`, and `^prod` keeps names that start with `prod`.
    /// Each cluster gives up to 20 matches, best first, each with its kind (TOPIC, GROUP, NODE for a broker, or SUBJECT) and exact id.
    /// `notReady` names each cluster klens has not read yet, where no match does not mean the name is absent.
    #[tool(title = "Search names")]
    async fn klens_search(
        &self,
        session: Session,
        Parameters(search): Parameters<SearchQuery>,
    ) -> ToolResult {
        if search.query.chars().count() > MAX_QUERY_CHARS {
            return Err(ApiError::unprocessable(format!(
                "a query holds at most {MAX_QUERY_CHARS} characters"
            )));
        }
        let clusters = match &search.cluster {
            Some(name) => {
                let cluster = session.cluster(name)?;
                cluster.topology()?;
                vec![cluster]
            }
            None => session.clusters().collect(),
        };
        Ok(fit(SearchResult::find(&clusters, &search.query)))
    }
}
