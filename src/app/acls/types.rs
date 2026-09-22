use serde::{Deserialize, Serialize};
use ts_rs::TS;

use crate::kafka::model as domain;
use crate::r#macro::from_same_variants;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum AclAuthorizer {
    Enabled,
    Disabled,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum AclResourceType {
    Topic,
    Group,
    Cluster,
    TransactionalId,
    DelegationToken,
}

from_same_variants!(domain::AclResourceType => AclResourceType {
    Topic,
    Group,
    Cluster,
    TransactionalId,
    DelegationToken,
});

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum AclPatternType {
    Literal,
    Prefixed,
}

from_same_variants!(domain::AclPatternType => AclPatternType { Literal, Prefixed });

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum AclOperation {
    All,
    Read,
    Write,
    Create,
    Delete,
    Alter,
    Describe,
    ClusterAction,
    DescribeConfigs,
    AlterConfigs,
    IdempotentWrite,
}

from_same_variants!(domain::AclOperation => AclOperation {
    All,
    Read,
    Write,
    Create,
    Delete,
    Alter,
    Describe,
    ClusterAction,
    DescribeConfigs,
    AlterConfigs,
    IdempotentWrite,
});

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum AclPermission {
    Allow,
    Deny,
}

from_same_variants!(domain::AclPermission => AclPermission { Allow, Deny });

#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Acl {
    pub resource_type: AclResourceType,
    pub resource_name: String,
    pub pattern_type: AclPatternType,
    pub principal: String,
    pub host: String,
    pub operation: AclOperation,
    pub permission: AclPermission,
}

impl From<domain::Acl> for Acl {
    fn from(acl: domain::Acl) -> Self {
        Self {
            resource_type: acl.resource_type.into(),
            resource_name: acl.resource_name,
            pattern_type: acl.pattern_type.into(),
            principal: acl.principal,
            host: acl.host,
            operation: acl.operation.into(),
            permission: acl.permission.into(),
        }
    }
}

#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub(crate) struct AclListing {
    pub authorizer: AclAuthorizer,
    pub bindings: Vec<Acl>,
}

impl From<domain::AclListing> for AclListing {
    fn from(listing: domain::AclListing) -> Self {
        match listing {
            domain::AclListing::Enabled(rows) => Self {
                authorizer: AclAuthorizer::Enabled,
                bindings: rows.into_iter().map(Acl::from).collect(),
            },
            domain::AclListing::Disabled => Self {
                authorizer: AclAuthorizer::Disabled,
                bindings: Vec::new(),
            },
        }
    }
}
