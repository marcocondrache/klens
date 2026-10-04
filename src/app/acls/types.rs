use serde::{Deserialize, Serialize};
use ts_rs::TS;

use crate::kafka::model as domain;
use crate::r#macro::from_same_variants;

use super::super::clusters::LaneHealth;
use super::super::error::ApiError;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum AclStatus {
    /// The lane has not described the ACLs yet.
    Pending,
    Enabled,
    Disabled,
    Denied,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum AclResourceType {
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
from_same_variants!(AclResourceType => domain::AclResourceType {
    Topic,
    Group,
    Cluster,
    TransactionalId,
    DelegationToken,
});

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum AclPatternType {
    Literal,
    Prefixed,
}

from_same_variants!(domain::AclPatternType => AclPatternType { Literal, Prefixed });
from_same_variants!(AclPatternType => domain::AclPatternType { Literal, Prefixed });

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum AclOperation {
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
from_same_variants!(AclOperation => domain::AclOperation {
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
pub enum AclPermission {
    Allow,
    Deny,
}

from_same_variants!(domain::AclPermission => AclPermission { Allow, Deny });
from_same_variants!(AclPermission => domain::AclPermission { Allow, Deny });

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Acl {
    pub resource_type: AclResourceType,
    pub resource_name: String,
    pub pattern_type: AclPatternType,
    pub principal: String,
    pub host: String,
    pub operation: AclOperation,
    pub permission: AclPermission,
}

impl From<&domain::Acl> for Acl {
    fn from(acl: &domain::Acl) -> Self {
        Self {
            resource_type: acl.resource_type.into(),
            resource_name: acl.resource_name.clone(),
            pattern_type: acl.pattern_type.into(),
            principal: acl.principal.clone(),
            host: acl.host.clone(),
            operation: acl.operation.into(),
            permission: acl.permission.into(),
        }
    }
}

impl From<Acl> for domain::Acl {
    fn from(acl: Acl) -> Self {
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

#[derive(Debug, Clone, Deserialize, TS)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CreateAcls {
    pub bindings: Vec<Acl>,
}

impl CreateAcls {
    pub(crate) fn into_acls(self) -> Result<Vec<domain::Acl>, ApiError> {
        if self.bindings.is_empty() {
            return Err(ApiError::unprocessable("name at least one binding"));
        }
        Ok(self.bindings.into_iter().map(domain::Acl::from).collect())
    }
}

#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct AclListing {
    pub status: AclStatus,
    pub bindings: Vec<Acl>,
    pub source_health: LaneHealth,
}

impl AclListing {
    pub(crate) fn new(listing: Option<&domain::AclListing>, health: LaneHealth) -> Self {
        let (status, bindings) = match listing {
            None => (AclStatus::Pending, Vec::new()),
            Some(domain::AclListing::Enabled(rows)) => {
                (AclStatus::Enabled, rows.iter().map(Acl::from).collect())
            }
            Some(domain::AclListing::Disabled) => (AclStatus::Disabled, Vec::new()),
            Some(domain::AclListing::Denied) => (AclStatus::Denied, Vec::new()),
        };
        Self {
            status,
            bindings,
            source_health: health,
        }
    }
}
