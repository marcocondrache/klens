//! Which privileges let a caller use a tool.

use rmcp::model::ToolAnnotations;
use serde::Serialize;

use crate::app::auth::access::{ClusterAccess, Privilege};
use crate::app::context::{ClusterHandle, Session};
use crate::app::whoami::types::PrivilegeName;
use crate::config::Mcp;

use super::configs::Section;
use super::tools::gates;

/// The privilege a tool needs, and the privilege each section of its result needs.
pub(super) struct ToolGate {
    pub(super) name: &'static str,
    pub(super) needs: Option<Privilege>,
    pub(super) sections: &'static [(Section, Privilege)],
    idempotent: bool,
}

impl ToolGate {
    pub(super) const fn open(name: &'static str) -> Self {
        Self {
            name,
            needs: None,
            sections: &[],
            idempotent: true,
        }
    }

    pub(super) const fn needing(name: &'static str, privilege: Privilege) -> Self {
        Self {
            needs: Some(privilege),
            ..Self::open(name)
        }
    }

    pub(super) const fn with_sections(self, sections: &'static [(Section, Privilege)]) -> Self {
        Self { sections, ..self }
    }

    /// A repeated call of this tool changes Kafka again.
    pub(super) const fn not_idempotent(self) -> Self {
        Self {
            idempotent: false,
            ..self
        }
    }

    pub(super) fn named(name: &str) -> Option<&'static Self> {
        gates().find(|gate| gate.name == name)
    }

    pub(super) fn changes(&self) -> bool {
        self.needs
            .is_some_and(|privilege| Mcp::WRITES.contains(&privilege))
    }

    pub(super) fn annotations(&self) -> ToolAnnotations {
        ToolAnnotations::new()
            .read_only(!self.changes())
            .destructive(false)
            .idempotent(self.idempotent)
            .open_world(false)
    }

    /// Whether the caller may use the tool on some cluster they see.
    pub(super) fn offered_to(&self, session: &Session) -> bool {
        self.needs.is_none() || session.clusters().any(|cluster| self.usable_on(&cluster))
    }

    /// A tool that changes Kafka also needs a cluster that accepts changes.
    pub(super) fn usable_on(&self, cluster: &ClusterHandle<'_>) -> bool {
        self.needs
            .is_none_or(|privilege| cluster.access.allows(privilege))
            && (!self.changes() || cluster.is_writable())
    }

    /// The tool, then each of its sections, with what each lacks on the cluster.
    pub(super) fn rights(&self, cluster: &ClusterHandle<'_>) -> Vec<ToolRights> {
        let tool = ToolRights {
            available: self.usable_on(cluster),
            ..ToolRights::check(&cluster.access, self.name, None, self.needs)
        };
        let sections = self.sections.iter().map(|&(section, needs)| {
            ToolRights::check(&cluster.access, self.name, Some(section), Some(needs))
        });
        std::iter::once(tool).chain(sections).collect()
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct ToolRights {
    name: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    section: Option<Section>,
    available: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    needs: Option<PrivilegeName>,
}

impl ToolRights {
    pub(super) fn check(
        access: &ClusterAccess<'_>,
        name: &'static str,
        section: Option<Section>,
        needs: Option<Privilege>,
    ) -> Self {
        let missing = needs.filter(|privilege| !access.allows(*privilege));
        Self {
            name,
            section,
            available: missing.is_none(),
            needs: missing.map(Into::into),
        }
    }
}
