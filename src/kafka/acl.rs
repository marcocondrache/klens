use krafka::admin::DescribeAclsResult;
use krafka::error::{ErrorCode, KrafkaError};
use krafka::protocol::{
    AclBinding, AclOperation as WireOperation, AclPatternType as WirePattern,
    AclPermissionType as WirePermission, AclResourceType as WireResource,
};
use tracing::warn;

use crate::kafka::error::KafkaError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Acl {
    pub resource_type: AclResourceType,
    pub resource_name: String,
    pub pattern_type: AclPatternType,
    pub principal: String,
    pub host: String,
    pub operation: AclOperation,
    pub permission: AclPermission,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AclResourceType {
    Topic,
    Group,
    Cluster,
    TransactionalId,
    DelegationToken,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AclPatternType {
    Literal,
    Prefixed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AclPermission {
    Allow,
    Deny,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AclListing {
    Enabled(Vec<Acl>),
    Disabled,
}

impl AclListing {
    pub fn from_admin_result(
        cluster: &str,
        result: Result<DescribeAclsResult, KrafkaError>,
    ) -> Result<Self, KafkaError> {
        match result {
            Err(error) => Self::from_krafka_error(error),
            Ok(described) => {
                Self::from_describe(cluster, described.error.as_deref(), described.bindings)
            }
        }
    }

    fn from_krafka_error(error: KrafkaError) -> Result<Self, KafkaError> {
        match error {
            KrafkaError::Broker {
                code: ErrorCode::SecurityDisabled,
                ..
            } => Ok(Self::Disabled),
            other => Err(KafkaError::from(other)),
        }
    }

    fn from_describe(
        cluster: &str,
        error: Option<&str>,
        bindings: Vec<AclBinding>,
    ) -> Result<Self, KafkaError> {
        if let Some(message) = error {
            if is_security_disabled_text(message) {
                return Ok(Self::Disabled);
            }
            return Err(KafkaError::Admin(message.to_owned()));
        }

        let rows = bindings
            .into_iter()
            .filter_map(|binding| match Acl::try_from(binding) {
                Ok(acl) => Some(acl),
                Err(sentinel) => {
                    warn!(cluster, field = sentinel.field, "dropping sentinel ACL row");
                    None
                }
            })
            .collect();
        Ok(Self::Enabled(rows))
    }

    pub fn bindings(&self) -> &[Acl] {
        match self {
            Self::Enabled(rows) => rows,
            Self::Disabled => &[],
        }
    }
}

impl TryFrom<AclBinding> for Acl {
    type Error = SentinelAcl;

    fn try_from(binding: AclBinding) -> Result<Self, SentinelAcl> {
        Ok(Self {
            resource_type: AclResourceType::try_from(binding.resource_type)?,
            resource_name: binding.resource_name,
            pattern_type: AclPatternType::try_from(binding.pattern_type)?,
            principal: binding.principal,
            host: binding.host,
            operation: AclOperation::try_from(binding.operation)?,
            permission: AclPermission::try_from(binding.permission_type)?,
        })
    }
}

impl TryFrom<WireResource> for AclResourceType {
    type Error = SentinelAcl;

    fn try_from(value: WireResource) -> Result<Self, Self::Error> {
        match value {
            WireResource::Topic => Ok(Self::Topic),
            WireResource::Group => Ok(Self::Group),
            WireResource::Cluster => Ok(Self::Cluster),
            WireResource::TransactionalId => Ok(Self::TransactionalId),
            WireResource::DelegationToken => Ok(Self::DelegationToken),
            _ => Err(SentinelAcl {
                field: "resource_type",
            }),
        }
    }
}

impl TryFrom<WirePattern> for AclPatternType {
    type Error = SentinelAcl;

    fn try_from(value: WirePattern) -> Result<Self, Self::Error> {
        match value {
            WirePattern::Literal => Ok(Self::Literal),
            WirePattern::Prefixed => Ok(Self::Prefixed),
            _ => Err(SentinelAcl {
                field: "pattern_type",
            }),
        }
    }
}

impl TryFrom<WireOperation> for AclOperation {
    type Error = SentinelAcl;

    fn try_from(value: WireOperation) -> Result<Self, Self::Error> {
        match value {
            WireOperation::All => Ok(Self::All),
            WireOperation::Read => Ok(Self::Read),
            WireOperation::Write => Ok(Self::Write),
            WireOperation::Create => Ok(Self::Create),
            WireOperation::Delete => Ok(Self::Delete),
            WireOperation::Alter => Ok(Self::Alter),
            WireOperation::Describe => Ok(Self::Describe),
            WireOperation::ClusterAction => Ok(Self::ClusterAction),
            WireOperation::DescribeConfigs => Ok(Self::DescribeConfigs),
            WireOperation::AlterConfigs => Ok(Self::AlterConfigs),
            WireOperation::IdempotentWrite => Ok(Self::IdempotentWrite),
            _ => Err(SentinelAcl { field: "operation" }),
        }
    }
}

impl TryFrom<WirePermission> for AclPermission {
    type Error = SentinelAcl;

    fn try_from(value: WirePermission) -> Result<Self, Self::Error> {
        match value {
            WirePermission::Allow => Ok(Self::Allow),
            WirePermission::Deny => Ok(Self::Deny),
            _ => Err(SentinelAcl {
                field: "permission",
            }),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SentinelAcl {
    pub field: &'static str,
}

/// krafka's `DescribeAclsResult` carries the broker error only as text: the
/// error-code name or the Kafka protocol default message.
fn is_security_disabled_text(message: &str) -> bool {
    let lower = message.to_ascii_lowercase();
    lower.contains("securitydisabled")
        || lower.contains("security_disabled")
        || lower.contains("security features are disabled")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stored_binding() -> AclBinding {
        AclBinding {
            resource_type: WireResource::Topic,
            resource_name: "orders.created".into(),
            pattern_type: WirePattern::Literal,
            principal: "User:alice".into(),
            host: "*".into(),
            operation: WireOperation::Read,
            permission_type: WirePermission::Allow,
        }
    }

    fn stored_acl() -> Acl {
        Acl {
            resource_type: AclResourceType::Topic,
            resource_name: "orders.created".into(),
            pattern_type: AclPatternType::Literal,
            principal: "User:alice".into(),
            host: "*".into(),
            operation: AclOperation::Read,
            permission: AclPermission::Allow,
        }
    }

    #[test]
    fn try_from_maps_a_concrete_binding() {
        assert_eq!(Acl::try_from(stored_binding()).unwrap(), stored_acl());
    }

    #[test]
    fn try_from_maps_every_stored_resource_type() {
        let cases = [
            (WireResource::Topic, AclResourceType::Topic),
            (WireResource::Group, AclResourceType::Group),
            (WireResource::Cluster, AclResourceType::Cluster),
            (
                WireResource::TransactionalId,
                AclResourceType::TransactionalId,
            ),
            (
                WireResource::DelegationToken,
                AclResourceType::DelegationToken,
            ),
        ];
        for (wire, expected) in cases {
            let mut binding = stored_binding();
            binding.resource_type = wire;
            assert_eq!(Acl::try_from(binding).unwrap().resource_type, expected);
        }
    }

    #[test]
    fn try_from_maps_every_stored_pattern_operation_and_permission() {
        for (wire, expected) in [
            (WirePattern::Literal, AclPatternType::Literal),
            (WirePattern::Prefixed, AclPatternType::Prefixed),
        ] {
            let mut binding = stored_binding();
            binding.pattern_type = wire;
            assert_eq!(Acl::try_from(binding).unwrap().pattern_type, expected);
        }

        let operations = [
            (WireOperation::All, AclOperation::All),
            (WireOperation::Read, AclOperation::Read),
            (WireOperation::Write, AclOperation::Write),
            (WireOperation::Create, AclOperation::Create),
            (WireOperation::Delete, AclOperation::Delete),
            (WireOperation::Alter, AclOperation::Alter),
            (WireOperation::Describe, AclOperation::Describe),
            (WireOperation::ClusterAction, AclOperation::ClusterAction),
            (
                WireOperation::DescribeConfigs,
                AclOperation::DescribeConfigs,
            ),
            (WireOperation::AlterConfigs, AclOperation::AlterConfigs),
            (
                WireOperation::IdempotentWrite,
                AclOperation::IdempotentWrite,
            ),
        ];
        for (wire, expected) in operations {
            let mut binding = stored_binding();
            binding.operation = wire;
            assert_eq!(Acl::try_from(binding).unwrap().operation, expected);
        }

        for (wire, expected) in [
            (WirePermission::Allow, AclPermission::Allow),
            (WirePermission::Deny, AclPermission::Deny),
        ] {
            let mut binding = stored_binding();
            binding.permission_type = wire;
            assert_eq!(Acl::try_from(binding).unwrap().permission, expected);
        }
    }

    #[test]
    fn try_from_rejects_sentinels_in_every_enum_position() {
        let cases: [(fn(&mut AclBinding), &str); 8] = [
            (
                |binding| binding.resource_type = WireResource::Any,
                "resource_type",
            ),
            (
                |binding| binding.resource_type = WireResource::Unknown,
                "resource_type",
            ),
            (
                |binding| binding.pattern_type = WirePattern::Any,
                "pattern_type",
            ),
            (
                |binding| binding.pattern_type = WirePattern::Unknown,
                "pattern_type",
            ),
            (
                |binding| binding.operation = WireOperation::Any,
                "operation",
            ),
            (
                |binding| binding.operation = WireOperation::Unknown,
                "operation",
            ),
            (
                |binding| binding.permission_type = WirePermission::Any,
                "permission",
            ),
            (
                |binding| binding.permission_type = WirePermission::Unknown,
                "permission",
            ),
        ];
        for (mutate, field) in cases {
            let mut binding = stored_binding();
            mutate(&mut binding);
            assert_eq!(Acl::try_from(binding).unwrap_err().field, field);
        }
    }

    #[test]
    fn broker_security_disabled_is_a_disabled_listing() {
        let listing = AclListing::from_admin_result(
            "local",
            Err(KrafkaError::Broker {
                code: ErrorCode::SecurityDisabled,
                message: "Security features are disabled.".into(),
            }),
        )
        .unwrap();
        assert_eq!(listing, AclListing::Disabled);
        assert!(listing.bindings().is_empty());
    }

    #[test]
    fn other_broker_errors_stay_client_errors() {
        let error = AclListing::from_admin_result(
            "local",
            Err(KrafkaError::Broker {
                code: ErrorCode::TopicAuthorizationFailed,
                message: "not authorized".into(),
            }),
        )
        .unwrap_err();
        assert!(matches!(error, KafkaError::Krafka(_)));
        assert_eq!(error.code(), "CLIENT");
    }

    #[test]
    fn describe_error_text_maps_security_disabled_names() {
        for message in [
            "SecurityDisabled",
            "SECURITY_DISABLED",
            "Broker: SECURITY_DISABLED",
            "Security features are disabled.",
        ] {
            let listing =
                AclListing::from_describe("local", Some(message), vec![stored_binding()]).unwrap();
            assert_eq!(listing, AclListing::Disabled, "{message}");
        }
    }

    #[test]
    fn describe_error_text_does_not_match_authorizer_prose() {
        let error = AclListing::from_describe(
            "local",
            Some("authorizer is not configured"),
            vec![stored_binding()],
        )
        .unwrap_err();
        assert!(
            matches!(error, KafkaError::Admin(message) if message == "authorizer is not configured")
        );
    }

    #[test]
    fn other_describe_errors_fail_the_call() {
        let error =
            AclListing::from_describe("local", Some("TopicAuthorizationFailed"), Vec::new())
                .unwrap_err();
        assert!(
            matches!(error, KafkaError::Admin(message) if message == "TopicAuthorizationFailed")
        );
    }

    #[test]
    fn good_bindings_become_an_enabled_listing() {
        let listing = AclListing::from_describe("local", None, vec![stored_binding()]).unwrap();
        assert_eq!(listing, AclListing::Enabled(vec![stored_acl()]));
    }

    #[test]
    fn sentinel_rows_are_dropped_from_an_otherwise_good_list() {
        let mut unknown = stored_binding();
        unknown.operation = WireOperation::Unknown;
        let listing =
            AclListing::from_describe("local", None, vec![stored_binding(), unknown]).unwrap();
        assert_eq!(listing, AclListing::Enabled(vec![stored_acl()]));
    }
}
