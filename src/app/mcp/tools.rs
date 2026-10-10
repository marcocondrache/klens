use std::sync::LazyLock;

use rmcp::handler::server::router::tool::ToolRouter;

use schemars::JsonSchema;
use serde::Deserialize;

use super::gate::ToolGate;
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
        let mut router = Self::cluster_tools()
            + Self::topic_tools()
            + Self::group_tools()
            + Self::record_tools()
            + Self::broker_tools()
            + Self::schema_tools()
            + Self::acl_tools();
        for gate in gates() {
            let route = router
                .map
                .get_mut(gate.name)
                .expect("a gate names a registered tool");
            route.attr.annotations = Some(gate.annotations());
        }
        router
    }
}

/// Every tool's gate, by name, the order a client reads in the access list.
pub(super) fn gates() -> impl Iterator<Item = &'static ToolGate> {
    static SORTED: LazyLock<Vec<&'static ToolGate>> = LazyLock::new(|| {
        let mut gates: Vec<_> = [
            acls::GATES,
            brokers::GATES,
            clusters::GATES,
            groups::GATES,
            records::GATES,
            schemas::GATES,
            topics::GATES,
        ]
        .into_iter()
        .flatten()
        .collect();
        gates.sort_by_key(|gate| gate.name);
        gates
    });
    SORTED.iter().copied()
}
