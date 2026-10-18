use std::sync::Arc;

use crate::app::context::{ClusterHandle, Session};
use crate::app::error::ApiError;
use crate::kafka::KafkaError;
use crate::kafka::store::{Lane, Topology};

pub(super) trait SessionExt {
    fn cluster_or_only<'a>(&'a self, name: Option<&'a str>) -> Result<ClusterHandle<'a>, ApiError>;
}

impl SessionExt for Session {
    fn cluster_or_only<'a>(&'a self, name: Option<&'a str>) -> Result<ClusterHandle<'a>, ApiError> {
        if let Some(name) = name {
            return self.cluster(name);
        }
        let mut visible: Vec<ClusterHandle<'_>> = self.clusters().collect();
        if visible.len() == 1 {
            return Ok(visible.remove(0));
        }
        let names: Vec<&str> = visible.iter().map(ClusterHandle::name).collect();
        Err(ApiError::unprocessable(if names.is_empty() {
            "you can see no cluster".to_owned()
        } else {
            format!("pass `cluster` as one of {}", names.join(", "))
        }))
    }
}

pub(super) trait ClusterExt {
    fn topology(&self) -> Result<Arc<Topology>, ApiError>;

    fn require_schema_registry(&self) -> Result<(), ApiError>;

    fn snapshot<T>(&self, lane_name: &'static str, lane: &Lane<T>) -> Result<Arc<T>, ApiError>;
}

impl ClusterExt for ClusterHandle<'_> {
    fn topology(&self) -> Result<Arc<Topology>, ApiError> {
        self.snapshot("topology", &self.store.topology)
    }

    fn require_schema_registry(&self) -> Result<(), ApiError> {
        if self.has_schema_registry() {
            Ok(())
        } else {
            Err(KafkaError::NoSchemaRegistry(self.name().to_owned()).into())
        }
    }

    fn snapshot<T>(&self, lane_name: &'static str, lane: &Lane<T>) -> Result<Arc<T>, ApiError> {
        lane.load().ok_or_else(|| ApiError::NotReady {
            cluster: self.name().to_owned(),
            lane: lane_name,
            last_error: lane.health().last_error,
        })
    }
}
