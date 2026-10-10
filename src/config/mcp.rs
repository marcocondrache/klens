use serde::de::Error as _;
use serde::{Deserialize, Deserializer};

use super::Privilege;

const READS: [Privilege; 5] = [
    Privilege::Records,
    Privilege::TopicConfigs,
    Privilege::BrokerConfigs,
    Privilege::SchemaText,
    Privilege::Acls,
];

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Mcp {
    /// The most an MCP client may read beyond the catalog, whatever its roles
    /// grant. MCP serves no writes yet.
    #[serde(deserialize_with = "reads")]
    pub privileges: Vec<Privilege>,
    /// Omitted, MCP reaches every cluster. A list, even an empty one, limits
    /// it to those clusters.
    pub clusters: Option<Vec<String>>,
}

impl Default for Mcp {
    fn default() -> Self {
        Self {
            privileges: READS.to_vec(),
            clusters: None,
        }
    }
}

fn reads<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Vec<Privilege>, D::Error> {
    let privileges = Vec::<Privilege>::deserialize(deserializer)?;
    if privileges
        .iter()
        .any(|privilege| !READS.contains(privilege))
    {
        return Err(D::Error::custom(
            "MCP serves reads only, so privileges may name only records, topic_configs, \
             broker_configs, schema_text, and acls",
        ));
    }
    Ok(privileges)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::{yaml, yaml_err};

    #[test]
    fn an_empty_block_serves_every_read_on_every_cluster() {
        let mcp: Mcp = yaml("{}");

        assert_eq!(mcp.privileges, READS);
        assert_eq!(mcp.clusters, None);
        assert_eq!(mcp, Mcp::default());
    }

    #[test]
    fn reads_privileges_and_clusters() {
        let some: Mcp = yaml("{privileges: [records], clusters: [dev, staging]}");
        let none: Mcp = yaml("{privileges: [], clusters: []}");

        assert_eq!(some.privileges, [Privilege::Records]);
        assert_eq!(
            some.clusters.as_deref(),
            Some(&["dev".into(), "staging".into()][..])
        );
        assert!(none.privileges.is_empty());
        assert_eq!(none.clusters.as_deref(), Some(&[][..]));
    }

    #[test]
    fn a_write_privilege_stops_the_load() {
        let error = yaml_err::<Mcp>("privileges: [records, produce]");

        assert_eq!(
            error,
            "MCP serves reads only, so privileges may name only records, topic_configs, \
             broker_configs, schema_text, and acls at line 1, column 13"
        );
    }
}
