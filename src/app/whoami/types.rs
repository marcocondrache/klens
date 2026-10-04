use serde::{Deserialize, Serialize};
use ts_rs::TS;

use crate::app::auth::access::Privilege;
use crate::r#macro::from_same_variants;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum PrivilegeName {
    Records,
    TopicConfigs,
    BrokerConfigs,
    SchemaText,
    Acls,
    CreateTopics,
    DeleteTopics,
    AlterTopicConfigs,
    AddPartitions,
    DeleteRecords,
    Produce,
    ResetOffsets,
    DeleteOffsets,
    DeleteGroups,
    RegisterSchemas,
    SetCompatibility,
    DeleteSchemas,
    CreateAcls,
    DeleteAcls,
    AlterQuotas,
    SetScramCredentials,
    DeleteScramCredentials,
    AlterBrokerConfigs,
}

from_same_variants!(Privilege => PrivilegeName {
    Records,
    TopicConfigs,
    BrokerConfigs,
    SchemaText,
    Acls,
    CreateTopics,
    DeleteTopics,
    AlterTopicConfigs,
    AddPartitions,
    DeleteRecords,
    Produce,
    ResetOffsets,
    DeleteOffsets,
    DeleteGroups,
    RegisterSchemas,
    SetCompatibility,
    DeleteSchemas,
    CreateAcls,
    DeleteAcls,
    AlterQuotas,
    SetScramCredentials,
    DeleteScramCredentials,
    AlterBrokerConfigs,
});

/// What the session may do on one cluster. Pairwise: a wider grant elsewhere
/// does not raise this one.
#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct ClusterGrant {
    pub cluster: String,
    /// Names of the roles that granted this access, for tracing a privilege
    /// back to an IdP group mapping. Empty when no role table applies.
    pub roles: Vec<String>,
    pub privileges: Vec<PrivilegeName>,
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
