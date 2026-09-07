use std::collections::HashMap;
use std::sync::Arc;

use crate::config::{ClusterConfig, Config, ConfigError};
use crate::kafka::error::KafkaError;
use crate::kafka::handle::ClusterHandle;

#[derive(Debug, Default)]
pub struct ClusterRegistry {
    clusters: HashMap<String, Arc<ClusterHandle>>,
    order: Vec<String>,
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

            let handle = Arc::new(ClusterHandle::from_config(config)?);
            clusters.insert(name.clone(), handle);
            order.push(name);
        }

        Ok(Self { clusters, order })
    }

    pub fn get(&self, name: &str) -> Option<Arc<ClusterHandle>> {
        self.clusters.get(name).cloned()
    }

    pub fn list(&self) -> Vec<Arc<ClusterHandle>> {
        self.order
            .iter()
            .filter_map(|name| self.clusters.get(name).cloned())
            .collect()
    }

    pub fn names(&self) -> Vec<String> {
        self.order.clone()
    }

    pub(crate) fn into_sessions(self) -> HashMap<String, Arc<ClusterHandle>> {
        self.clusters
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kafka::session::ClusterSession;

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
                .map(|cluster| cluster.identity().name.clone())
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
            bind: "127.0.0.1:8080".parse().unwrap(),
            log_level: "info".into(),
            clusters: vec![cluster("b"), cluster("a")],
            auth: None,
        };

        let registry = ClusterRegistry::from_config(&config).unwrap();

        assert_eq!(registry.names(), vec!["b", "a"]);
    }
}
