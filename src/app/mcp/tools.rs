use std::sync::LazyLock;

use rmcp::handler::server::router::tool::ToolRouter;

use super::gate::ToolGate;
use super::server::KlensMcp;

/// Lists the tool modules, each with the router its `#[tool_router]` makes.
/// A module defines `GATES` beside its tools.
macro_rules! tools {
    ($($module:ident => $router:ident),+ $(,)?) => {
        $(mod $module;)+

        impl KlensMcp {
            pub(super) fn tools() -> ToolRouter<Self> {
                let mut router = ToolRouter::new() $(+ Self::$router())+;
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
                let mut gates: Vec<_> = [$($module::GATES),+].into_iter().flatten().collect();
                gates.sort_by_key(|gate| gate.name);
                gates
            });
            SORTED.iter().copied()
        }
    };
}

tools! {
    acls => acl_tools,
    brokers => broker_tools,
    clusters => cluster_tools,
    groups => group_tools,
    records => record_tools,
    schemas => schema_tools,
    topics => topic_tools,
}
