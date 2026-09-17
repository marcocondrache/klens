use crate::config::{ClusterConfig, SecurityProtocol};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterIdentity {
    pub name: String,
    pub bootstrap_servers: Vec<String>,
    pub security_protocol: SecurityProtocol,
}

impl From<&ClusterConfig> for ClusterIdentity {
    fn from(config: &ClusterConfig) -> Self {
        Self {
            name: config.name.trim().to_owned(),
            bootstrap_servers: config.bootstrap_servers.clone(),
            security_protocol: config
                .security
                .as_ref()
                .map(|security| security.protocol)
                .unwrap_or(SecurityProtocol::Plaintext),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identity_trims_the_configured_name_and_defaults_the_protocol() {
        let identity = ClusterIdentity::from(&ClusterConfig {
            name: "  local  ".to_owned(),
            bootstrap_servers: vec!["localhost:9092".to_owned()],
            security: None,
            schema_registry: None,
            properties: Default::default(),
        });

        assert_eq!(identity.name, "local");
        assert_eq!(identity.security_protocol, SecurityProtocol::Plaintext);
    }
}
