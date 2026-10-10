use rmcp::handler::server::router::tool::ToolRouter;

use schemars::JsonSchema;
use serde::Deserialize;

use super::server::KlensMcp;

mod acls;
mod brokers;
mod clusters;
mod groups;
mod records;
mod schemas;
mod topics;

#[derive(Clone, Copy, Default, PartialEq, Eq, Deserialize, JsonSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
#[schemars(inline)]
pub(super) enum ResponseFormat {
    #[default]
    Concise,
    Detailed,
}

impl KlensMcp {
    pub(super) fn tools() -> ToolRouter<Self> {
        Self::cluster_tools()
            + Self::topic_tools()
            + Self::group_tools()
            + Self::record_tools()
            + Self::broker_tools()
            + Self::schema_tools()
            + Self::acl_tools()
    }
}
