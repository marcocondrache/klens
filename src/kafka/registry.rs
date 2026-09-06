use std::collections::HashMap;
use std::sync::Arc;

use crate::config::{ClusterConfig, Config, ConfigError};
use crate::kafka::client::ClusterClient;
use crate::kafka::error::KafkaError;

#[derive(Debug, Clone, Default)]
pub struct ClusterRegistry {
    clusters: Arc<HashMap<String, Arc<ClusterClient>>>,
    order: Arc<[String]>,
}

impl ClusterRegistry {
    pub fn from_config(config: &Config) -> Result<Self, KafkaError> {
        config.validate()?;
        Self::build(config.clusters.clone())
    }

    pub fn build(configs: Vec<ClusterConfig>) -> Result<Self, KafkaError> {
        let mut clusters = HashMap::with_capacity(configs.len());
        let mut order = Vec::with_capacity(configs.len());

        for config in configs {
            let name = config.name.trim().to_owned();

            if name.is_empty() {
                return Err(ConfigError::invalid_cluster(
                    config.name.clone(),
                    "name must not be empty",
                )
                .into());
            }

            if clusters.contains_key(&name) {
                return Err(ConfigError::invalid_cluster(name, "duplicate cluster name").into());
            }

            let client = Arc::new(ClusterClient::from_config(config)?);
            clusters.insert(name.clone(), client);
            order.push(name);
        }

        Ok(Self {
            clusters: Arc::new(clusters),
            order: order.into(),
        })
    }

    pub fn get(&self, name: &str) -> Option<Arc<ClusterClient>> {
        self.clusters.get(name).cloned()
    }

    pub fn list(&self) -> Vec<Arc<ClusterClient>> {
        self.order
            .iter()
            .filter_map(|name| self.clusters.get(name).cloned())
            .collect()
    }

    pub fn names(&self) -> Vec<String> {
        self.order.to_vec()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cluster(name: &str) -> ClusterConfig {
        ClusterConfig {
            name: name.to_owned(),
            bootstrap_servers: vec!["localhost:9092".to_owned()],
            security: None,
            properties: HashMap::new(),
        }
    }

    #[test]
    fn preserves_configuration_order() {
        let registry = ClusterRegistry::build(vec![cluster("b"), cluster("a")]).unwrap();

        assert_eq!(registry.names(), vec!["b", "a"]);
        assert_eq!(
            registry
                .list()
                .iter()
                .map(|c| c.name().to_owned())
                .collect::<Vec<_>>(),
            vec!["b", "a"]
        );
    }

    #[test]
    fn rejects_duplicate_names() {
        let error = ClusterRegistry::build(vec![cluster("a"), cluster("a")]).unwrap_err();

        assert!(error.to_string().contains("duplicate cluster name"));
    }

    #[test]
    fn rejects_empty_names() {
        let error = ClusterRegistry::build(vec![cluster("  ")]).unwrap_err();

        assert!(error.to_string().contains("name must not be empty"));
    }

    #[test]
    fn returns_none_for_unknown_cluster() {
        let registry = ClusterRegistry::build(vec![cluster("a")]).unwrap();

        assert!(registry.get("missing").is_none());
        assert!(registry.get("a").is_some());
    }

    #[test]
    fn builds_from_root_config() {
        let config = Config {
            clusters: vec![cluster("b"), cluster("a")],
            ..Config::default()
        };

        let registry = ClusterRegistry::from_config(&config).unwrap();

        assert_eq!(registry.names(), vec!["b", "a"]);
    }
}
