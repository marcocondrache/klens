use serde::Serialize;
use ts_rs::TS;

use crate::app::auth::access::Privilege;

/// What the session may do on one cluster. Pairwise: a wider grant elsewhere
/// does not raise this one.
#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct ClusterGrant {
    pub cluster: String,
    /// Names of the roles that granted this access, for tracing a privilege
    /// back to an IdP group mapping. Empty when no role table applies.
    pub roles: Vec<String>,
    pub privileges: Vec<Privilege>,
    /// False when the cluster refuses every change, whatever the privileges.
    pub writable: bool,
}

#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct Identity {
    /// `null` when authentication is disabled.
    pub subject: Option<String>,
    pub clusters: Vec<ClusterGrant>,
}
