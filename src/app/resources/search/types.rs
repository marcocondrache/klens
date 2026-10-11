use serde::{Deserialize, Serialize};
use ts_rs::TS;

use crate::kafka::model as domain;
use crate::r#macro::from_same_variants;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SearchKind {
    Topic,
    Group,
    Node,
    Subject,
}

from_same_variants!(domain::SearchKind => SearchKind { Topic, Group, Node, Subject });

#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct SearchHit {
    pub kind: SearchKind,
    pub id: String,
    pub label: String,
    pub detail: String,
}

impl From<domain::SearchHit> for SearchHit {
    fn from(hit: domain::SearchHit) -> Self {
        Self {
            kind: hit.kind.into(),
            id: hit.id,
            label: hit.label,
            detail: hit.detail,
        }
    }
}
