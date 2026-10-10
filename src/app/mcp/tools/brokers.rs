use rmcp::handler::server::wrapper::Parameters;
use rmcp::{tool, tool_router};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::app::auth::access::Privilege;
use crate::app::brokers::types::LogDir;
use crate::app::context::{ClusterHandle, Session};
use crate::app::error::ApiError;
use crate::app::mcp::MAX_ROWS;
use crate::app::mcp::configs::{ConfigSection, Section};
use crate::app::mcp::ext::{ClusterExt as _, SessionExt as _};
use crate::app::mcp::gate::ToolGate;
use crate::app::mcp::reply::{Page, fit, reply};
use crate::app::mcp::server::{KlensMcp, ToolResult};
use crate::kafka::KafkaError;
use crate::kafka::store::projections;

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct BrokersQuery {
    /// A cluster name from klens_clusters. Optional when you see only one.
    cluster: Option<String>,
    /// Lists only brokers with a higher id, such as the last one shown.
    after: Option<i32>,
    /// Describes this broker alone, with its config overrides.
    broker: Option<i32>,
    /// How many brokers to return: 25 unless given, at most 100.
    #[schemars(range(min = 1, max = MAX_ROWS))]
    limit: Option<usize>,
}

pub(super) const GATES: &[ToolGate] = &[ToolGate::open("klens_brokers_list")
    .with_sections(&[(Section::Configs, Privilege::BrokerConfigs)])];

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct BrokerRow {
    id: i32,
    host: String,
    port: i32,
    rack: Option<String>,
    controller: Option<bool>,
    partition_count: i32,
    leader_count: i32,
    size_bytes: Option<i64>,
    log_dirs: Option<Vec<LogDir>>,
}

impl BrokerRow {
    /// Every broker of the cluster by id. The controller and the log dirs stay
    /// null until klens has read them.
    fn read(cluster: &ClusterHandle<'_>) -> Result<Vec<Self>, ApiError> {
        let topology = cluster.topology()?;
        let log_dirs = cluster.store.log_dirs.load();
        let rows = cluster.store.broker_rows().into_iter().map(|row| {
            let log_dirs_read = log_dirs
                .as_deref()
                .is_some_and(|table| table.broker(row.id).is_some());
            Self::new(row, topology.controller.is_some(), log_dirs_read)
        });
        Ok(rows.collect())
    }

    fn new(row: projections::BrokerRow, controller_known: bool, log_dirs_read: bool) -> Self {
        Self {
            id: row.id,
            host: row.host,
            port: row.port,
            rack: row.rack,
            controller: controller_known.then_some(row.controller),
            partition_count: row.partition_count,
            leader_count: row.leader_count,
            size_bytes: row.size_bytes,
            log_dirs: log_dirs_read.then(|| row.log_dirs.into_iter().map(LogDir::from).collect()),
        }
    }
}

#[derive(Serialize)]
struct BrokerList {
    #[serde(flatten)]
    page: Page<BrokerRow>,
}

reply!(BrokerList: page);

#[derive(Serialize)]
struct BrokerDetail {
    #[serde(flatten)]
    broker: BrokerRow,
    #[serde(flatten)]
    configs: ConfigSection,
}

reply!(BrokerDetail: configs; "the klens UI shows every config");

#[tool_router(router = broker_tools, vis = "pub(super)")]
impl KlensMcp {
    /// Lists a cluster's brokers by id with their address, rack, controller flag, partition counts, size and log dirs.
    /// Size and log dirs stay null until klens can describe log dirs, which needs the Describe operation on the Cluster resource.
    /// With `broker`, it returns that broker alone with `configs`, each config whose value is not Kafka's default. It reads them live from Kafka, so these calls are limited per minute. Without the BROKER_CONFIGS privilege `configs` is null and `omitted` names that privilege.
    #[tool(title = "List brokers")]
    async fn klens_brokers_list(
        &self,
        session: Session,
        Parameters(query): Parameters<BrokersQuery>,
    ) -> ToolResult {
        let cluster = session.cluster_or_only(query.cluster.as_deref())?;
        let mut brokers = BrokerRow::read(&cluster)?;
        let Some(id) = query.broker else {
            brokers.retain(|broker| query.after.is_none_or(|after| broker.id > after));
            return Ok(fit(BrokerList {
                page: Page::new(
                    "brokers",
                    brokers,
                    query.limit,
                    Some("pass the last id shown as `after`"),
                ),
            }));
        };
        if query.after.is_some() || query.limit.is_some() {
            return Err(ApiError::unprocessable(
                "pass `broker` without `after` or `limit`",
            ));
        }
        let Some(broker) = brokers.into_iter().find(|broker| broker.id == id) else {
            return Err(KafkaError::UnknownBroker {
                cluster: cluster.name().to_owned(),
                id,
            }
            .into());
        };
        let configs = match cluster.broker_configs() {
            Ok(granted) => {
                self.live_call(&session)?;
                ConfigSection::overrides(granted.broker_configs(id).await?)
            }
            Err(error) => ConfigSection::withheld(error)?,
        };
        Ok(fit(BrokerDetail { broker, configs }))
    }
}
