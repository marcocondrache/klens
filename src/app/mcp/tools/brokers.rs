use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::CallToolResult;
use rmcp::{tool, tool_router};
use schemars::JsonSchema;
use serde::Deserialize;
use serde_json::json;

use crate::kafka::KafkaError;

use crate::app::context::Session;
use crate::app::error::ApiError;

use super::super::MAX_ROWS;
use super::super::gate::ToolGate;
use super::super::types::{BrokerDetail, BrokerList, BrokerRow, Section};
use crate::app::auth::access::Privilege;
use crate::app::mcp::fit::{fitted, listed, one_cluster};
use crate::app::mcp::lanes::topology;
use crate::app::mcp::server::KlensMcp;
use crate::app::mcp::view::{omitted, overrides};
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
    ) -> Result<CallToolResult, ApiError> {
        let cluster = one_cluster(&session, query.cluster.as_deref())?;
        let topology = topology(&cluster)?;
        let log_dirs = cluster.store.log_dirs.load();
        let mut rows = cluster.store.broker_rows().into_iter().map(|row| {
            let read = log_dirs
                .as_deref()
                .is_some_and(|table| table.broker(row.id).is_some());
            BrokerRow::new(row, topology.controller.is_some(), read)
        });
        if let Some(id) = query.broker {
            if query.after.is_some() || query.limit.is_some() {
                return Err(ApiError::unprocessable(
                    "pass `broker` without `after` or `limit`",
                ));
            }
            let Some(broker) = rows.find(|row| row.id == id) else {
                return Err(KafkaError::UnknownBroker {
                    cluster: cluster.name().to_owned(),
                    id,
                }
                .into());
            };
            let (configs, omitted) = match cluster.broker_configs() {
                Ok(granted) => {
                    if !self.state.mcp_live_call(session.guard.subject()) {
                        return Err(ApiError::TooManyLiveCalls);
                    }
                    (Some(overrides(granted.broker_configs(id).await?)), None)
                }
                Err(error) => (None, Some(omitted(Section::Configs, error)?)),
            };
            return Ok(fitted(
                configs.as_deref().unwrap_or_default(),
                "the klens UI shows every config",
                |shown, truncated| {
                    json!(BrokerDetail {
                        broker: &broker,
                        configs: configs.is_some().then_some(shown),
                        omitted: omitted.as_ref(),
                        truncated,
                    })
                },
            ));
        }
        let brokers: Vec<BrokerRow> = rows
            .filter(|row| query.after.is_none_or(|after| row.id > after))
            .collect();
        Ok(listed(
            brokers,
            query.limit,
            Some("pass the last id shown as `after`"),
            |brokers, showing| json!(BrokerList { brokers, showing }),
        ))
    }
}
