//! Reads every tool makes of a session and the cluster it names.

use std::sync::Arc;

use crate::app::context::{ClusterHandle, Session};
use crate::app::error::ApiError;
use crate::kafka::KafkaError;
use crate::kafka::store::{Lane, Topology};

pub(super) trait SessionExt {
    /// The cluster named, or else the only one the caller sees.
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

    /// Fails unless klens reads a schema registry for the cluster.
    fn require_schema_registry(&self) -> Result<(), ApiError>;

    /// What a lane last read. A lane before its first read holds nothing, which
    /// must not read as an empty cluster.
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
