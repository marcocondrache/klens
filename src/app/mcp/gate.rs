use crate::config::Mcp;

use crate::app::auth::access::{ClusterAccess, Privilege};
use crate::app::context::{ClusterHandle, Session};

use super::types::{ClusterRights, Section, ToolRights};
pub(super) struct ToolGate {
    pub(super) name: &'static str,
    pub(super) needs: Option<Privilege>,
    pub(super) sections: &'static [(Section, Privilege)],
}

impl ToolGate {
    pub(super) fn changes(&self) -> bool {
        self.needs
            .is_some_and(|privilege| Mcp::WRITES.contains(&privilege))
    }
}

pub(super) const TOOLS: &[ToolGate] = &[
    ToolGate {
        name: "klens_access_explain",
        needs: None,
        sections: &[],
    },
    ToolGate {
        name: "klens_acls_list",
        needs: Some(Privilege::Acls),
        sections: &[],
    },
    ToolGate {
        name: "klens_brokers_list",
        needs: None,
        sections: &[(Section::Configs, Privilege::BrokerConfigs)],
    },
    ToolGate {
        name: "klens_clusters",
        needs: None,
        sections: &[],
    },
    ToolGate {
        name: "klens_group_describe",
        needs: None,
        sections: &[],
    },
    ToolGate {
        name: "klens_groups_list",
        needs: None,
        sections: &[],
    },
    ToolGate {
        name: "klens_record_get",
        needs: Some(Privilege::Records),
        sections: &[],
    },
    ToolGate {
        name: "klens_record_produce",
        needs: Some(Privilege::Produce),
        sections: &[],
    },
    ToolGate {
        name: "klens_records_read",
        needs: Some(Privilege::Records),
        sections: &[],
    },
    ToolGate {
        name: "klens_schema_get",
        needs: Some(Privilege::SchemaText),
        sections: &[],
    },
    ToolGate {
        name: "klens_schema_register",
        needs: Some(Privilege::RegisterSchemas),
        sections: &[],
    },
    ToolGate {
        name: "klens_schemas_list",
        needs: None,
        sections: &[],
    },
    ToolGate {
        name: "klens_search",
        needs: None,
        sections: &[],
    },
    ToolGate {
        name: "klens_topic_create",
        needs: Some(Privilege::CreateTopics),
        sections: &[],
    },
    ToolGate {
        name: "klens_topic_describe",
        needs: None,
        sections: &[(Section::Configs, Privilege::TopicConfigs)],
    },
    ToolGate {
        name: "klens_topics_list",
        needs: None,
        sections: &[],
    },
];

pub(super) fn offered(session: &Session, tool: &str) -> bool {
    TOOLS.iter().any(|gate| {
        gate.name == tool
            && (gate.needs.is_none() || session.clusters().any(|cluster| usable(&cluster, gate)))
    })
}

pub(super) fn usable(cluster: &ClusterHandle<'_>, gate: &ToolGate) -> bool {
    gate.needs
        .is_none_or(|privilege| cluster.access.allows(privilege))
        && (!gate.changes() || cluster.is_writable())
}

pub(super) fn rights(cluster: &ClusterHandle<'_>) -> ClusterRights {
    ClusterRights {
        cluster: cluster.name().to_owned(),
        writable: cluster.is_writable(),
        privileges: cluster
            .access
            .privileges()
            .into_iter()
            .map(Into::into)
            .collect(),
        tools: TOOLS
            .iter()
            .flat_map(|gate| {
                let sections = gate.sections.iter().map(|&(section, needs)| {
                    tool_rights(&cluster.access, gate.name, Some(section), Some(needs))
                });
                let tool = ToolRights {
                    available: usable(cluster, gate),
                    ..tool_rights(&cluster.access, gate.name, None, gate.needs)
                };
                std::iter::once(tool).chain(sections)
            })
            .collect(),
    }
}

pub(super) fn tool_rights(
    access: &ClusterAccess<'_>,
    name: &'static str,
    section: Option<Section>,
    needs: Option<Privilege>,
) -> ToolRights {
    let missing = needs.filter(|privilege| !access.allows(*privilege));
    ToolRights {
        name,
        section,
        available: missing.is_none(),
        needs: missing.map(Into::into),
    }
}
