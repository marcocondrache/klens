use serde::{Deserialize, Serialize};
use ts_rs::TS;

use crate::app::auth::access::Privilege;
use crate::r#macro::from_same_variants;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum PrivilegeName {
    Records,
    Configs,
    SchemaText,
    Acls,
}

from_same_variants!(Privilege => PrivilegeName { Records, Configs, SchemaText, Acls });

/// What the session may do on one cluster. Pairwise: a wider grant elsewhere
/// does not raise this one.
#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ClusterGrant {
    pub cluster: String,
    /// Names of the roles that granted this access, for tracing a privilege
    /// back to an IdP group mapping. Empty when no role table applies.
    pub roles: Vec<String>,
    pub privileges: Vec<PrivilegeName>,
}

#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Identity {
    /// `null` when authentication is disabled.
    pub subject: Option<String>,
    pub clusters: Vec<ClusterGrant>,
}
