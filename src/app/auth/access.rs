//! Pairwise grants and capability handles.
//!
//! A grant is a `(role, scope)` pair and stays that way: grants are never
//! merged. Effective access is evaluated **per cluster** as the highest role
//! among the grants whose scope covers that cluster, so Admin on `prod` plus
//! Viewer on `payments` leaves `payments` a viewer.
//!
//! Privileged work takes a capability token ([`RecordsCap`], [`ConfigsCap`],
//! [`SchemaTextCap`], [`AclsCap`]). The tokens carry a cluster name and
//! cannot be constructed outside this module, so the only way to call a
//! privileged operation is to have passed its check.

use std::collections::BTreeSet;
use std::fmt::{Display, Formatter};

use crate::config::{RoleBinding, RoleName, RolesConfig, default_groups_claim};

const MAX_GROUPS: usize = 64;

/// Admin outranks Viewer, which is what "highest role among covering grants"
/// means.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Role {
    Viewer,
    Admin,
}

impl Role {
    fn allows(self, privilege: Privilege) -> bool {
        match self {
            Self::Admin => true,
            Self::Viewer => match privilege {
                Privilege::Records
                | Privilege::Configs
                | Privilege::SchemaText
                | Privilege::Acls => false,
            },
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Self::Admin => "admin",
            Self::Viewer => "viewer",
        }
    }
}

impl From<RoleName> for Role {
    fn from(name: RoleName) -> Self {
        match name {
            RoleName::Admin => Self::Admin,
            RoleName::Viewer => Self::Viewer,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Privilege {
    Records,
    Configs,
    SchemaText,
    Acls,
}

impl Privilege {
    pub const ALL: [Self; 4] = [Self::Records, Self::Configs, Self::SchemaText, Self::Acls];

    pub fn name(self) -> &'static str {
        match self {
            Self::Records => "records",
            Self::Configs => "configs",
            Self::SchemaText => "schemaText",
            Self::Acls => "acls",
        }
    }
}

impl Display for Privilege {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.name())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ClusterScope {
    All,
    Only(BTreeSet<String>),
}

impl ClusterScope {
    pub fn contains(&self, cluster: &str) -> bool {
        match self {
            Self::All => true,
            Self::Only(names) => names.contains(cluster),
        }
    }

    fn from_list(clusters: Option<&[String]>) -> Self {
        match clusters {
            None => Self::All,
            Some(names) => Self::Only(names.iter().cloned().collect()),
        }
    }
}

/// One configured binding, exactly as configured. Grants are never combined:
/// combining them is what let a wide role escalate onto a narrow scope.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Grant {
    pub role: Role,
    pub scope: ClusterScope,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EffectiveAccess {
    /// Auth is off, or configured without role bindings.
    Unrestricted,
    Granted(Vec<Grant>),
}

impl EffectiveAccess {
    /// Highest role among the grants covering `cluster`. `None` means the
    /// cluster is invisible to this session.
    pub fn role_for(&self, cluster: &str) -> Option<Role> {
        match self {
            Self::Unrestricted => Some(Role::Admin),
            Self::Granted(grants) => grants
                .iter()
                .filter(|grant| grant.scope.contains(cluster))
                .map(|grant| grant.role)
                .max(),
        }
    }

    /// The one fallible call every resolver makes. Holding a
    /// [`ClusterAccess`] is proof the cluster is visible.
    pub fn cluster<'a>(&self, name: &'a str) -> Result<ClusterAccess<'a>, AccessError> {
        match self.role_for(name) {
            Some(role) => Ok(ClusterAccess {
                cluster: name,
                role,
            }),
            None => Err(AccessError::UnknownCluster(name.to_owned())),
        }
    }

    pub fn can_see_cluster(&self, cluster: &str) -> bool {
        self.role_for(cluster).is_some()
    }

    pub fn visible_clusters<'a>(&self, all: impl Iterator<Item = &'a str>) -> Vec<&'a str> {
        all.filter(|name| self.can_see_cluster(name)).collect()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AccessError {
    /// An invisible cluster reads as missing rather than forbidden, so a
    /// session cannot probe for clusters it is not allowed to know about.
    UnknownCluster(String),
    Forbidden {
        cluster: String,
        privilege: Privilege,
    },
}

impl AccessError {
    pub fn code(&self) -> &'static str {
        match self {
            Self::UnknownCluster(_) => "UNKNOWN_CLUSTER",
            Self::Forbidden { .. } => "FORBIDDEN",
        }
    }
}

impl Display for AccessError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnknownCluster(cluster) => write!(formatter, "unknown cluster '{cluster}'"),
            Self::Forbidden { cluster, privilege } => write!(
                formatter,
                "'{privilege}' is not permitted on cluster '{cluster}'"
            ),
        }
    }
}

impl std::error::Error for AccessError {}

/// Proof that a session may see a cluster, and the gate to everything it may
/// do there.
#[derive(Clone, Copy, Debug)]
pub struct ClusterAccess<'a> {
    cluster: &'a str,
    role: Role,
}

impl<'a> ClusterAccess<'a> {
    pub fn cluster(&self) -> &'a str {
        self.cluster
    }

    pub fn role(&self) -> Role {
        self.role
    }

    pub fn allows(&self, privilege: Privilege) -> bool {
        self.role.allows(privilege)
    }

    pub fn privileges(&self) -> Vec<Privilege> {
        Privilege::ALL
            .into_iter()
            .filter(|privilege| self.allows(*privilege))
            .collect()
    }

    fn capability(&self, privilege: Privilege) -> Result<Capability<'a>, AccessError> {
        if self.allows(privilege) {
            Ok(Capability {
                cluster: self.cluster,
            })
        } else {
            Err(AccessError::Forbidden {
                cluster: self.cluster.to_owned(),
                privilege,
            })
        }
    }
}

/// The only way to build a capability token, and private to this module.
#[derive(Clone, Copy, Debug)]
struct Capability<'a> {
    cluster: &'a str,
}

/// Declares a capability token: a carrier of the cluster name that nothing
/// outside this module can construct.
macro_rules! capability {
    ($(#[$meta:meta])* $token:ident, $method:ident, $privilege:expr) => {
        $(#[$meta])*
        #[derive(Clone, Copy, Debug)]
        pub struct $token<'a>(Capability<'a>);

        impl<'a> $token<'a> {
            pub fn cluster(&self) -> &'a str {
                self.0.cluster
            }
        }

        impl<'a> ClusterAccess<'a> {
            pub fn $method(&self) -> Result<$token<'a>, AccessError> {
                self.capability($privilege).map($token)
            }
        }
    };
}

capability!(
    /// Required to browse records.
    RecordsCap,
    records,
    Privilege::Records
);
capability!(
    /// Required to read live topic and broker configs.
    ConfigsCap,
    configs,
    Privilege::Configs
);
capability!(
    /// Required to read a schema body.
    SchemaTextCap,
    schema_text,
    Privilege::SchemaText
);
capability!(
    /// Required to list ACL bindings.
    AclsCap,
    acls,
    Privilege::Acls
);

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Identity<'a> {
    pub groups: &'a [String],
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RoleTable {
    pub groups_claim: String,
    bindings: Vec<CompiledBinding>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct CompiledBinding {
    groups: BTreeSet<String>,
    role: Role,
    scope: ClusterScope,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AccessPolicy {
    Disabled,
    Open { groups_claim: String },
    Bound(RoleTable),
}

impl AccessPolicy {
    pub fn disabled() -> Self {
        Self::Disabled
    }

    pub fn open() -> Self {
        Self::Open {
            groups_claim: default_groups_claim(),
        }
    }

    pub fn from_roles(roles: Option<&RolesConfig>) -> Self {
        match roles {
            None => Self::open(),
            Some(config) => Self::Bound(RoleTable::compile(config)),
        }
    }

    pub fn groups_claim(&self) -> &str {
        match self {
            Self::Disabled => "groups",
            Self::Open { groups_claim } => groups_claim,
            Self::Bound(table) => &table.groups_claim,
        }
    }

    pub fn admit(&self, identity: &Identity<'_>) -> Option<EffectiveAccess> {
        match self {
            Self::Disabled | Self::Open { .. } => Some(EffectiveAccess::Unrestricted),
            Self::Bound(table) => {
                if identity.groups.len() > MAX_GROUPS {
                    return None;
                }
                table.resolve(identity.groups).map(EffectiveAccess::Granted)
            }
        }
    }
}

impl RoleTable {
    fn compile(config: &RolesConfig) -> Self {
        Self {
            groups_claim: config.claim.clone(),
            bindings: config
                .bindings
                .iter()
                .map(CompiledBinding::from_config)
                .collect(),
        }
    }

    /// Every matching binding becomes its own grant. `None` means no binding
    /// matched, which is a refusal to admit the session at all.
    fn resolve(&self, groups: &[String]) -> Option<Vec<Grant>> {
        let present: BTreeSet<&str> = groups.iter().map(String::as_str).collect();
        let grants: Vec<Grant> = self
            .bindings
            .iter()
            .filter(|binding| {
                binding
                    .groups
                    .iter()
                    .any(|group| present.contains(group.as_str()))
            })
            .map(|binding| Grant {
                role: binding.role,
                scope: binding.scope.clone(),
            })
            .collect();

        (!grants.is_empty()).then_some(grants)
    }
}

impl CompiledBinding {
    fn from_config(binding: &RoleBinding) -> Self {
        Self {
            groups: binding.groups.iter().cloned().collect(),
            role: Role::from(binding.role),
            scope: ClusterScope::from_list(binding.clusters.as_deref()),
        }
    }
}

pub fn groups_from_json(value: &serde_json::Value, claim: &str) -> Vec<String> {
    match value.get(claim) {
        Some(serde_json::Value::String(name)) if !name.is_empty() => vec![name.clone()],
        Some(serde_json::Value::Array(items)) => items
            .iter()
            .filter_map(|item| item.as_str())
            .filter(|name| !name.is_empty())
            .map(ToOwned::to_owned)
            .collect(),
        _ => Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{RoleBinding, RoleName, RolesConfig};

    fn table(bindings: Vec<RoleBinding>) -> AccessPolicy {
        AccessPolicy::from_roles(Some(&RolesConfig {
            claim: "groups".into(),
            bindings,
        }))
    }

    fn binding(groups: &[&str], role: RoleName, clusters: Option<&[&str]>) -> RoleBinding {
        RoleBinding {
            groups: groups.iter().map(|group| (*group).to_owned()).collect(),
            role,
            clusters: clusters.map(|names| names.iter().map(|name| (*name).to_owned()).collect()),
        }
    }

    fn admit(policy: &AccessPolicy, groups: &[&str]) -> Option<EffectiveAccess> {
        let owned: Vec<String> = groups.iter().map(|group| (*group).to_owned()).collect();
        policy.admit(&Identity { groups: &owned })
    }

    #[test]
    fn omitted_roles_are_unrestricted() {
        let policy = AccessPolicy::from_roles(None);
        let access = admit(&policy, &[]).unwrap();

        assert_eq!(access, EffectiveAccess::Unrestricted);
        assert!(access.cluster("prod").unwrap().records().is_ok());
    }

    #[test]
    fn unmatched_groups_are_refused() {
        let policy = table(vec![binding(&["klens-admins"], RoleName::Admin, None)]);
        assert_eq!(admit(&policy, &["other"]), None);
        assert_eq!(admit(&policy, &[]), None);
    }

    #[test]
    fn admin_holds_every_capability_on_every_cluster() {
        let policy = table(vec![binding(&["klens-admins"], RoleName::Admin, None)]);
        let access = admit(&policy, &["klens-admins"]).unwrap();

        let prod = access.cluster("prod").unwrap();
        assert_eq!(prod.role(), Role::Admin);
        assert_eq!(prod.records().unwrap().cluster(), "prod");
        assert!(prod.configs().is_ok());
        assert!(prod.schema_text().is_ok());
        assert!(prod.acls().is_ok());
        assert_eq!(prod.privileges(), Privilege::ALL.to_vec());
        assert!(access.cluster("staging").unwrap().schema_text().is_ok());
    }

    #[test]
    fn viewer_sees_the_catalog_and_nothing_privileged() {
        let policy = table(vec![binding(&["klens-viewers"], RoleName::Viewer, None)]);
        let access = admit(&policy, &["klens-viewers"]).unwrap();
        let prod = access.cluster("prod").expect("the cluster is visible");

        assert!(prod.privileges().is_empty());
        assert_eq!(
            prod.records().unwrap_err(),
            AccessError::Forbidden {
                cluster: "prod".into(),
                privilege: Privilege::Records,
            }
        );
        assert!(prod.configs().is_err());
        assert!(prod.schema_text().is_err());
        assert!(prod.acls().is_err());
    }

    #[test]
    fn a_cluster_outside_every_scope_reads_as_unknown() {
        let policy = table(vec![binding(
            &["payments-viewers"],
            RoleName::Viewer,
            Some(&["payments"]),
        )]);
        let access = admit(&policy, &["payments-viewers"]).unwrap();

        assert!(access.cluster("payments").is_ok());
        assert_eq!(
            access.cluster("prod").unwrap_err(),
            AccessError::UnknownCluster("prod".into())
        );
        assert_eq!(
            access.visible_clusters(["prod", "payments"].into_iter()),
            vec!["payments"]
        );
    }

    #[test]
    fn a_wider_admin_grant_does_not_escalate_a_narrow_viewer_grant() {
        let policy = table(vec![
            binding(&["payments-viewers"], RoleName::Viewer, Some(&["payments"])),
            binding(&["klens-admins"], RoleName::Admin, Some(&["prod"])),
        ]);
        let access = admit(&policy, &["payments-viewers", "klens-admins"]).unwrap();

        assert_eq!(access.role_for("prod"), Some(Role::Admin));
        assert_eq!(
            access.role_for("payments"),
            Some(Role::Viewer),
            "the admin grant covers prod only"
        );
        assert!(access.cluster("prod").unwrap().records().is_ok());
        assert!(
            access.cluster("payments").unwrap().records().is_err(),
            "scopes must not union under a globally-maxed role"
        );
    }

    #[test]
    fn overlapping_grants_take_the_highest_covering_role() {
        let policy = table(vec![
            binding(&["everyone"], RoleName::Viewer, None),
            binding(&["ops"], RoleName::Admin, Some(&["prod"])),
        ]);
        let access = admit(&policy, &["everyone", "ops"]).unwrap();

        assert_eq!(access.role_for("prod"), Some(Role::Admin));
        assert_eq!(access.role_for("staging"), Some(Role::Viewer));
        assert!(access.cluster("staging").unwrap().configs().is_err());
    }

    #[test]
    fn too_many_groups_are_refused() {
        let policy = table(vec![binding(&["klens-admins"], RoleName::Admin, None)]);
        let groups: Vec<String> = (0..MAX_GROUPS + 1).map(|i| format!("g{i}")).collect();
        assert_eq!(policy.admit(&Identity { groups: &groups }), None);
    }

    #[test]
    fn groups_claim_reads_string_or_array() {
        let object = serde_json::json!({ "groups": ["a", "b"] });
        assert_eq!(groups_from_json(&object, "groups"), ["a", "b"]);
        let single = serde_json::json!({ "groups": "ops" });
        assert_eq!(groups_from_json(&single, "groups"), ["ops"]);
        let missing = serde_json::json!({ "roles": ["x"] });
        assert!(groups_from_json(&missing, "groups").is_empty());
    }
}
