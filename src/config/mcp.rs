#[cfg(test)]
use std::net::Ipv4Addr;
use std::net::SocketAddr;

use serde::de::Error as _;
use serde::{Deserialize, Deserializer};

use super::{KeyMaterial, Privilege};

const READS: [Privilege; 5] = [
    Privilege::Records,
    Privilege::TopicConfigs,
    Privilege::BrokerConfigs,
    Privilege::SchemaText,
    Privilege::Acls,
];

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Mcp {
    /// What every MCP call may read or change beyond the catalog, whoever
    /// makes it. Omitted, it holds the five reads, so MCP changes nothing
    /// until it names `create_topics`, `produce` or `register_schemas`.
    #[serde(default = "reads", deserialize_with = "tool_privileges")]
    pub privileges: Vec<Privilege>,
    /// Omitted, MCP reaches every cluster. A list, even an empty one, limits
    /// it to those clusters.
    pub clusters: Option<Vec<String>>,
    /// A secret of at least 32 bytes that a gateway sends with every call as
    /// `Authorization: Bearer`, such as agentgateway's `backendAuth.key`. Set,
    /// `/mcp` answers only calls that carry it, from any browser origin, since
    /// the gateway forwards the client's.
    pub secret: Option<KeyMaterial>,
    /// `/mcp` answers here and never on the UI's `bind`, so a gateway can
    /// reach the tools without reaching the UI.
    pub bind: SocketAddr,
}

impl Mcp {
    pub const WRITES: &[Privilege] = &[
        Privilege::CreateTopics,
        Privilege::Produce,
        Privilege::RegisterSchemas,
    ];

    pub fn writes(&self) -> bool {
        self.privileges
            .iter()
            .any(|privilege| Self::WRITES.contains(privilege))
    }

    pub fn reads_private_text(&self) -> bool {
        self.privileges
            .iter()
            .any(|privilege| matches!(privilege, Privilege::Records | Privilege::SchemaText))
    }
}

#[cfg(test)]
impl Default for Mcp {
    fn default() -> Self {
        Self {
            privileges: reads(),
            clusters: None,
            secret: None,
            bind: SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
        }
    }
}

#[cfg(test)]
impl Mcp {
    pub fn holding(privileges: impl IntoIterator<Item = Privilege>) -> Self {
        Self {
            privileges: privileges.into_iter().collect(),
            ..Self::default()
        }
    }
}

fn reads() -> Vec<Privilege> {
    READS.to_vec()
}

fn tool_privileges<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Vec<Privilege>, D::Error> {
    let privileges = Vec::<Privilege>::deserialize(deserializer)?;
    if privileges
        .iter()
        .any(|privilege| !READS.contains(privilege) && !Mcp::WRITES.contains(privilege))
    {
        return Err(D::Error::custom(
            "MCP tools use only records, topic_configs, broker_configs, schema_text, acls, \
             create_topics, produce, and register_schemas, so privileges may name no other",
        ));
    }
    Ok(privileges)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::{yaml, yaml_err};

    #[test]
    fn a_block_without_bind_stops_the_load() {
        assert_eq!(
            yaml_err::<Mcp>("{}"),
            r#"missing configuration field "value.bind""#
        );
    }

    #[test]
    fn a_block_with_only_bind_serves_every_read_on_every_cluster() {
        let mcp: Mcp = yaml("bind: 0.0.0.0:8090");

        assert_eq!(mcp.privileges, READS);
        assert_eq!(mcp.clusters, None);
        assert!(!mcp.writes());
    }

    #[test]
    fn reads_privileges_and_clusters() {
        let some: Mcp =
            yaml("{bind: 0.0.0.0:8090, privileges: [records], clusters: [dev, staging]}");
        let none: Mcp = yaml("{bind: 0.0.0.0:8090, privileges: [], clusters: []}");

        assert_eq!(some.privileges, [Privilege::Records]);
        assert_eq!(
            some.clusters.as_deref(),
            Some(&["dev".into(), "staging".into()][..])
        );
        assert!(none.privileges.is_empty());
        assert_eq!(none.clusters.as_deref(), Some(&[][..]));
    }

    #[test]
    fn reads_the_three_write_privileges_beside_the_reads() {
        let mcp: Mcp = yaml(
            "{bind: 0.0.0.0:8090, privileges: [topic_configs, create_topics, produce, register_schemas]}",
        );

        assert_eq!(
            mcp.privileges,
            [
                Privilege::TopicConfigs,
                Privilege::CreateTopics,
                Privilege::Produce,
                Privilege::RegisterSchemas
            ]
        );
        assert!(mcp.writes());
        assert!(!mcp.reads_private_text());
    }

    #[test]
    fn any_other_write_privilege_stops_the_load() {
        for privilege in [
            "delete_topics",
            "set_scram_credentials",
            "set_compatibility",
        ] {
            let error = yaml_err::<Mcp>(&format!(
                "{{bind: 0.0.0.0:8090, privileges: [produce, {privilege}]}}"
            ));

            assert_eq!(
                error,
                "MCP tools use only records, topic_configs, broker_configs, schema_text, acls, \
                 create_topics, produce, and register_schemas, so privileges may name no other for key \
                 `privileges`",
                "{privilege}"
            );
        }
    }

    #[test]
    fn records_and_schema_text_are_the_private_text() {
        for (privileges, private) in [
            ("[records]", true),
            ("[schema_text]", true),
            ("[topic_configs, broker_configs, acls, produce]", false),
        ] {
            let mcp: Mcp = yaml(&format!("{{bind: 0.0.0.0:8090, privileges: {privileges}}}"));

            assert_eq!(mcp.reads_private_text(), private, "{privileges}");
        }
    }
}
