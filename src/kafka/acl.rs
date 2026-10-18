use std::fmt::{self, Display, Formatter};

use krafka::admin::{
    AclBinding, AclFilter, AclOperation as WireOperation, AclPatternType as WirePattern,
    AclPermissionType as WirePermission, AclResourceType as WireResource,
};
use krafka::error::{ErrorCode, KrafkaError};
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

impl Display for Acl {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{:?} {} from {} to {:?} {:?} {:?} {}",
            self.permission,
            self.principal,
            self.host,
            self.operation,
            self.pattern_type,
            self.resource_type,
            self.resource_name,
        )
    }
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
    Denied,
}

impl AclListing {
    pub fn contains(&self, acl: &Acl) -> bool {
        matches!(self, Self::Enabled(rows) if rows.contains(acl))
    }

    pub fn from_admin_result(
        cluster: &str,
        result: Result<Vec<AclBinding>, KrafkaError>,
    ) -> Result<Self, KafkaError> {
        match result {
            Err(error) => Self::from_krafka_error(error),
            Ok(bindings) => Ok(Self::from_describe(cluster, bindings)),
        }
    }

    fn from_krafka_error(error: KrafkaError) -> Result<Self, KafkaError> {
        match error {
            KrafkaError::Broker {
                code: ErrorCode::SecurityDisabled,
                ..
            } => Ok(Self::Disabled),
            KrafkaError::Broker {
                code: ErrorCode::ClusterAuthorizationFailed,
                ..
            } => Ok(Self::Denied),
            other => Err(KafkaError::from(other)),
        }
    }

    fn from_describe(cluster: &str, bindings: Vec<AclBinding>) -> Self {
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
        Self::Enabled(rows)
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

// krafka numbers pattern types one below Kafka, whose LITERAL is 3 and
// PREFIXED is 4, so a stored literal decodes as `Prefixed` and a stored prefix
// as `Unknown`.
impl TryFrom<WirePattern> for AclPatternType {
    type Error = SentinelAcl;

    fn try_from(value: WirePattern) -> Result<Self, Self::Error> {
        match value {
            WirePattern::Prefixed => Ok(Self::Literal),
            WirePattern::Unknown => Ok(Self::Prefixed),
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

impl From<AclResourceType> for WireResource {
    fn from(value: AclResourceType) -> Self {
        match value {
            AclResourceType::Topic => Self::Topic,
            AclResourceType::Group => Self::Group,
            AclResourceType::Cluster => Self::Cluster,
            AclResourceType::TransactionalId => Self::TransactionalId,
            AclResourceType::DelegationToken => Self::DelegationToken,
        }
    }
}

impl AclPatternType {
    /// The krafka pattern type Kafka reads as this one. krafka has none that
    /// Kafka reads as PREFIXED.
    fn to_krafka(self) -> Result<WirePattern, KafkaError> {
        match self {
            Self::Literal => Ok(WirePattern::Prefixed),
            Self::Prefixed => Err(KafkaError::PrefixedAcl),
        }
    }
}

impl From<AclOperation> for WireOperation {
    fn from(value: AclOperation) -> Self {
        match value {
            AclOperation::All => Self::All,
            AclOperation::Read => Self::Read,
            AclOperation::Write => Self::Write,
            AclOperation::Create => Self::Create,
            AclOperation::Delete => Self::Delete,
            AclOperation::Alter => Self::Alter,
            AclOperation::Describe => Self::Describe,
            AclOperation::ClusterAction => Self::ClusterAction,
            AclOperation::DescribeConfigs => Self::DescribeConfigs,
            AclOperation::AlterConfigs => Self::AlterConfigs,
            AclOperation::IdempotentWrite => Self::IdempotentWrite,
        }
    }
}

impl From<AclPermission> for WirePermission {
    fn from(value: AclPermission) -> Self {
        match value {
            AclPermission::Allow => Self::Allow,
            AclPermission::Deny => Self::Deny,
        }
    }
}

impl Acl {
    pub(crate) fn to_krafka(&self) -> Result<AclBinding, KafkaError> {
        Ok(AclBinding::new(
            self.resource_type.into(),
            self.resource_name.as_str(),
            self.principal.as_str(),
            self.host.as_str(),
            self.operation.into(),
            self.permission.into(),
        )
        .with_pattern_type(self.pattern_type.to_krafka()?))
    }

    pub(crate) fn to_krafka_filter(&self) -> Result<AclFilter, KafkaError> {
        Ok(
            AclFilter::for_resource(self.resource_type.into(), self.resource_name.as_str())
                .pattern_type(self.pattern_type.to_krafka()?)
                .principal(self.principal.as_str())
                .host(self.host.as_str())
                .operation(self.operation.into())
                .permission_type(self.permission.into()),
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SentinelAcl {
    pub field: &'static str,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stored_binding() -> AclBinding {
        AclBinding {
            resource_type: WireResource::Topic,
            resource_name: "orders.created".into(),
            pattern_type: WirePattern::Prefixed,
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
            (WirePattern::Prefixed, AclPatternType::Literal),
            (WirePattern::Unknown, AclPatternType::Prefixed),
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
        type Mutate = fn(&mut AclBinding);
        let cases: [(Mutate, &str); 8] = [
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
                |binding| binding.pattern_type = WirePattern::Literal,
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
    fn a_denied_describe_is_a_denied_listing() {
        let listing = AclListing::from_admin_result(
            "local",
            Err(KrafkaError::Broker {
                code: ErrorCode::ClusterAuthorizationFailed,
                message: "Cluster authorization failed.".into(),
            }),
        )
        .unwrap();
        assert_eq!(listing, AclListing::Denied);
    }

    #[test]
    fn good_bindings_become_an_enabled_listing() {
        let listing = AclListing::from_describe("local", vec![stored_binding()]);
        assert_eq!(listing, AclListing::Enabled(vec![stored_acl()]));
    }

    #[test]
    fn sentinel_rows_are_dropped_from_an_otherwise_good_list() {
        let mut unknown = stored_binding();
        unknown.operation = WireOperation::Unknown;
        let listing = AclListing::from_describe("local", vec![stored_binding(), unknown]);
        assert_eq!(listing, AclListing::Enabled(vec![stored_acl()]));
    }

    fn group_acl() -> Acl {
        Acl {
            resource_type: AclResourceType::Group,
            resource_name: "order-processor".into(),
            principal: "User:bob".into(),
            host: "10.0.0.1".into(),
            operation: AclOperation::Describe,
            permission: AclPermission::Deny,
            ..stored_acl()
        }
    }

    #[test]
    fn a_literal_acl_reaches_kafka_as_literal_and_matches_only_itself() {
        let binding = stored_acl().to_krafka().unwrap();
        assert_eq!(binding.pattern_type.to_i8(), 3, "Kafka's LITERAL");
        assert_eq!(Acl::try_from(binding), Ok(stored_acl()));

        let filter = group_acl().to_krafka_filter().unwrap();
        assert_eq!(filter.pattern_type.to_i8(), 3, "Kafka's LITERAL");
        assert_eq!(filter.resource_type, WireResource::Group);
        assert_eq!(filter.resource_name.as_deref(), Some("order-processor"));
        assert_eq!(filter.principal.as_deref(), Some("User:bob"));
        assert_eq!(filter.host.as_deref(), Some("10.0.0.1"));
        assert_eq!(filter.operation, WireOperation::Describe);
        assert_eq!(filter.permission_type, WirePermission::Deny);
    }

    #[test]
    fn a_prefixed_acl_is_refused_before_it_reaches_kafka() {
        let prefixed = Acl {
            pattern_type: AclPatternType::Prefixed,
            ..stored_acl()
        };

        assert!(matches!(prefixed.to_krafka(), Err(KafkaError::PrefixedAcl)));
        assert!(matches!(
            prefixed.to_krafka_filter(),
            Err(KafkaError::PrefixedAcl)
        ));
    }

    #[test]
    fn every_binding_field_survives_the_trip_to_the_wire() {
        let resources = [
            AclResourceType::Topic,
            AclResourceType::Group,
            AclResourceType::Cluster,
            AclResourceType::TransactionalId,
            AclResourceType::DelegationToken,
        ];
        for resource in resources {
            assert_eq!(
                AclResourceType::try_from(WireResource::from(resource)),
                Ok(resource)
            );
        }
        let operations = [
            AclOperation::All,
            AclOperation::Read,
            AclOperation::Write,
            AclOperation::Create,
            AclOperation::Delete,
            AclOperation::Alter,
            AclOperation::Describe,
            AclOperation::ClusterAction,
            AclOperation::DescribeConfigs,
            AclOperation::AlterConfigs,
            AclOperation::IdempotentWrite,
        ];
        for operation in operations {
            assert_eq!(
                AclOperation::try_from(WireOperation::from(operation)),
                Ok(operation)
            );
        }
        for permission in [AclPermission::Allow, AclPermission::Deny] {
            assert_eq!(
                AclPermission::try_from(WirePermission::from(permission)),
                Ok(permission)
            );
        }
    }

    #[test]
    fn a_binding_reads_as_who_may_do_what_to_which_resource() {
        assert_eq!(
            group_acl().to_string(),
            "Deny User:bob from 10.0.0.1 to Describe Literal Group order-processor"
        );
    }

    #[test]
    fn only_an_enabled_listing_contains_a_binding() {
        let listing = AclListing::Enabled(vec![stored_acl()]);

        assert!(listing.contains(&stored_acl()));
        assert!(!listing.contains(&group_acl()));
        assert!(!AclListing::Disabled.contains(&stored_acl()));
    }
}
