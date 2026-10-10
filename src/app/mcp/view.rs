use std::borrow::Cow;

use crate::kafka::model as domain;
use crate::kafka::store::Topology;

use crate::app::auth::access::AccessError;
use crate::app::context::ClusterHandle;
use crate::app::error::ApiError;
use crate::app::search::types::SearchHit;

use super::MAX_CLIENT_VALUE_CHARS;
use super::types::{ClusterHit, ConfigRow, Omitted, Reason, Section, UnhealthyPartition};
use super::untrusted::clip;
pub(super) fn unhealthy_partitions(topology: &Topology) -> Vec<UnhealthyPartition> {
    let mut partitions: Vec<UnhealthyPartition> = topology
        .topics
        .iter()
        .flat_map(|(topic, info)| {
            info.partitions
                .iter()
                .filter(|partition| partition.under_replicated() || partition.offline())
                .map(|partition| UnhealthyPartition {
                    topic: topic.to_string(),
                    partition: partition.id,
                    leader: (!partition.offline()).then_some(partition.leader),
                    replicas: partition.replicas.clone(),
                    isr: partition.isr.clone(),
                    offline: partition.offline(),
                })
        })
        .collect();
    partitions.sort_by_key(|partition| !partition.offline);
    partitions
}

pub(super) fn omitted(section: Section, error: AccessError) -> Result<Omitted, ApiError> {
    match error {
        AccessError::Forbidden { privilege, .. } => Ok(Omitted {
            section,
            reason: Reason::Needs(privilege.into()),
        }),
        error => Err(error.into()),
    }
}

pub(super) fn overrides(entries: Vec<domain::ConfigEntry>) -> Vec<ConfigRow> {
    entries
        .into_iter()
        .filter(|entry| entry.source != domain::ConfigSource::Default)
        .map(ConfigRow::new)
        .collect()
}

pub(super) fn shortened(text: &str) -> Cow<'_, str> {
    match clip(text, MAX_CLIENT_VALUE_CHARS) {
        (kept, true) => Cow::Owned(format!("{kept}…")),
        (kept, false) => Cow::Borrowed(kept),
    }
}

pub(super) fn hits(cluster: &ClusterHandle<'_>, query: &str) -> impl Iterator<Item = ClusterHit> {
    let name = cluster.name().to_owned();
    cluster
        .store
        .search(query)
        .into_iter()
        .map(move |hit| ClusterHit {
            cluster: name.clone(),
            hit: SearchHit::from(hit),
        })
}
