use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{Display, Formatter};
use std::sync::Arc;

use crate::config::{PrivilegeName, RolesConfig, default_groups_claim};
use crate::r#macro::from_same_variants;

const MAX_GROUPS: usize = 64;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Privilege {
    Records,
    Configs,
    SchemaText,
    Acls,
    ResetOffsets,
    DeleteGroupOffsets,
}

impl Privilege {
    pub const ALL: [Self; 6] = [
        Self::Records,
        Self::Configs,
        Self::SchemaText,
        Self::Acls,
        Self::ResetOffsets,
        Self::DeleteGroupOffsets,
    ];

    pub fn name(self) -> &'static str {
        match self {
            Self::Records => "records",
            Self::Configs => "configs",
            Self::SchemaText => "schemaText",
            Self::Acls => "acls",
            Self::ResetOffsets => "resetOffsets",
            Self::DeleteGroupOffsets => "deleteGroupOffsets",
        }
    }

    const fn bit(self) -> u32 {
        match self {
            Self::Records => 1 << 0,
            Self::Configs => 1 << 1,
            Self::SchemaText => 1 << 2,
            Self::Acls => 1 << 3,
            Self::ResetOffsets => 1 << 4,
            Self::DeleteGroupOffsets => 1 << 5,
        }
    }
}

impl Display for Privilege {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.name())
    }
}

from_same_variants!(PrivilegeName => Privilege {
    Records,
    Configs,
    SchemaText,
    Acls,
    ResetOffsets,
    DeleteGroupOffsets,
});

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PrivilegeSet(u32);

impl PrivilegeSet {
    pub const NONE: Self = Self(0);
    pub const ALL: Self = Self((1 << Privilege::ALL.len()) - 1);
    pub const READS: Self = Self(
        Privilege::Records.bit()
            | Privilege::Configs.bit()
            | Privilege::SchemaText.bit()
            | Privilege::Acls.bit(),
    );
    pub const WRITES: Self = Self(Self::ALL.0 & !Self::READS.0);

    pub fn from_privileges(privileges: impl IntoIterator<Item = Privilege>) -> Self {
        privileges
            .into_iter()
            .fold(Self::NONE, |set, privilege| Self(set.0 | privilege.bit()))
    }

    pub fn contains(self, privilege: Privilege) -> bool {
        self.0 & privilege.bit() != 0
    }

    pub const fn union(self, other: Self) -> Self {
        Self(self.0 | other.0)
    }

    pub const fn intersection(self, other: Self) -> Self {
        Self(self.0 & other.0)
    }

    pub fn iter(self) -> impl Iterator<Item = Privilege> {
        Privilege::ALL
            .into_iter()
            .filter(move |privilege| self.contains(*privilege))
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ClusterScope {
    All,
    Only(Arc<BTreeSet<String>>),
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
            Some(names) => Self::Only(Arc::new(names.iter().cloned().collect())),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Grant {
    pub role_name: Arc<str>,
    pub privileges: PrivilegeSet,
    pub scope: ClusterScope,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EffectiveAccess {
    Unrestricted,
    Granted(Vec<Grant>),
}

impl EffectiveAccess {
    pub fn privileges_for(&self, cluster: &str) -> Option<PrivilegeSet> {
        match self {
            Self::Unrestricted => Some(PrivilegeSet::ALL),
            Self::Granted(grants) => grants
                .iter()
                .filter(|grant| grant.scope.contains(cluster))
                .map(|grant| grant.privileges)
                .reduce(PrivilegeSet::union),
        }
    }

    pub fn cluster<'a>(&'a self, name: &'a str) -> Result<ClusterAccess<'a>, AccessError> {
        match self.privileges_for(name) {
            Some(privileges) => Ok(ClusterAccess {
                cluster: name,
                privileges,
                grants: match self {
                    Self::Unrestricted => &[],
                    Self::Granted(grants) => grants,
                },
            }),
            None => Err(AccessError::UnknownCluster(name.to_owned())),
        }
    }

    pub fn can_see_cluster(&self, cluster: &str) -> bool {
        self.privileges_for(cluster).is_some()
    }

    pub fn visible_clusters<'a>(&self, all: impl Iterator<Item = &'a str>) -> Vec<&'a str> {
        all.filter(|name| self.can_see_cluster(name)).collect()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AccessError {
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

#[derive(Clone, Copy, Debug)]
pub struct ClusterAccess<'a> {
    cluster: &'a str,
    privileges: PrivilegeSet,
    grants: &'a [Grant],
}

impl<'a> ClusterAccess<'a> {
    pub fn cluster(&self) -> &'a str {
        self.cluster
    }

    pub fn role_names(&self) -> Vec<&'a str> {
        let mut names: Vec<&'a str> = self
            .grants
            .iter()
            .filter(|grant| grant.scope.contains(self.cluster))
            .map(|grant| grant.role_name.as_ref())
            .collect();
        names.sort_unstable();
        names.dedup();
        names
    }

    pub fn allows(&self, privilege: Privilege) -> bool {
        self.privileges.contains(privilege)
    }

    pub fn privileges(&self) -> Vec<Privilege> {
        self.privileges.iter().collect()
    }

    pub fn capped(self, ceiling: PrivilegeSet) -> Self {
        Self {
            privileges: self.privileges.intersection(ceiling),
            ..self
        }
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

#[derive(Clone, Copy, Debug)]
struct Capability<'a> {
    cluster: &'a str,
}

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

capability!(RecordsCap, records, Privilege::Records);
capability!(ConfigsCap, configs, Privilege::Configs);
capability!(SchemaTextCap, schema_text, Privilege::SchemaText);
capability!(AclsCap, acls, Privilege::Acls);
capability!(ResetOffsetsCap, reset_offsets, Privilege::ResetOffsets);
capability!(
    DeleteGroupOffsetsCap,
    delete_group_offsets,
    Privilege::DeleteGroupOffsets
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
    role_name: Arc<str>,
    privileges: PrivilegeSet,
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
        let definitions: BTreeMap<&str, PrivilegeSet> = config
            .definitions
            .iter()
            .map(|(role, privileges)| {
                (
                    role.as_str(),
                    PrivilegeSet::from_privileges(privileges.iter().copied().map(Privilege::from)),
                )
            })
            .collect();

        Self {
            groups_claim: config.claim.clone(),
            bindings: config
                .bindings
                .iter()
                .filter_map(|binding| {
                    let privileges = *definitions.get(binding.role.as_str())?;
                    Some(CompiledBinding {
                        groups: binding.groups.iter().cloned().collect(),
                        role_name: Arc::from(binding.role.as_str()),
                        privileges,
                        scope: ClusterScope::from_list(binding.clusters.as_deref()),
                    })
                })
                .collect(),
        }
    }

    fn resolve(&self, groups: &[String]) -> Option<Vec<Grant>> {
        let grants: Vec<Grant> = self
            .bindings
            .iter()
            .filter(|binding| {
                groups
                    .iter()
                    .any(|group| binding.groups.contains(group.as_str()))
            })
            .map(|binding| Grant {
                role_name: Arc::clone(&binding.role_name),
                privileges: binding.privileges,
                scope: binding.scope.clone(),
            })
            .collect();

        (!grants.is_empty()).then_some(grants)
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
    use crate::config::{PrivilegeName, RoleBinding, RolesConfig};

    const EVERYTHING: &[PrivilegeName] = &[
        PrivilegeName::Records,
        PrivilegeName::Configs,
        PrivilegeName::SchemaText,
        PrivilegeName::Acls,
        PrivilegeName::ResetOffsets,
        PrivilegeName::DeleteGroupOffsets,
    ];

    fn table(definitions: &[(&str, &[PrivilegeName])], bindings: Vec<RoleBinding>) -> AccessPolicy {
        AccessPolicy::from_roles(Some(&RolesConfig {
            claim: "groups".into(),
            definitions: definitions
                .iter()
                .map(|(role, privileges)| ((*role).to_owned(), privileges.to_vec()))
                .collect(),
            bindings,
        }))
    }

    fn binding(groups: &[&str], role: &str, clusters: Option<&[&str]>) -> RoleBinding {
        RoleBinding {
            groups: groups.iter().map(|group| (*group).to_owned()).collect(),
            role: role.to_owned(),
            clusters: clusters.map(|names| names.iter().map(|name| (*name).to_owned()).collect()),
        }
    }

    fn admit(policy: &AccessPolicy, groups: &[&str]) -> Option<EffectiveAccess> {
        let owned: Vec<String> = groups.iter().map(|group| (*group).to_owned()).collect();
        policy.admit(&Identity { groups: &owned })
    }

    fn set(privileges: &[Privilege]) -> PrivilegeSet {
        PrivilegeSet::from_privileges(privileges.iter().copied())
    }

    #[test]
    fn omitted_roles_are_unrestricted() {
        let policy = AccessPolicy::from_roles(None);
        let access = admit(&policy, &[]).unwrap();

        assert_eq!(access, EffectiveAccess::Unrestricted);
        assert!(access.cluster("prod").unwrap().records().is_ok());
        assert_eq!(access.privileges_for("prod"), Some(PrivilegeSet::ALL));
        assert!(
            access.cluster("prod").unwrap().role_names().is_empty(),
            "no role table decided anything"
        );
    }

    #[test]
    fn unmatched_groups_are_refused() {
        let policy = table(
            &[("admin", EVERYTHING)],
            vec![binding(&["klens-admins"], "admin", None)],
        );
        assert_eq!(admit(&policy, &["other"]), None);
        assert_eq!(admit(&policy, &[]), None);
    }

    #[test]
    fn a_binding_naming_an_undefined_role_grants_nothing() {
        let policy = table(
            &[("admin", EVERYTHING)],
            vec![binding(&["klens-admins"], "unknown-role", None)],
        );
        assert_eq!(
            admit(&policy, &["klens-admins"]),
            None,
            "validation rejects this config; compiling it must still fail closed"
        );
    }

    #[test]
    fn a_role_holding_every_privilege_holds_every_capability() {
        let policy = table(
            &[("admin", EVERYTHING)],
            vec![binding(&["klens-admins"], "admin", None)],
        );
        let access = admit(&policy, &["klens-admins"]).unwrap();

        let prod = access.cluster("prod").unwrap();
        assert_eq!(prod.role_names(), vec!["admin"]);
        assert_eq!(prod.records().unwrap().cluster(), "prod");
        assert!(prod.configs().is_ok());
        assert!(prod.schema_text().is_ok());
        assert!(prod.acls().is_ok());
        assert_eq!(prod.privileges(), Privilege::ALL.to_vec());
        assert_eq!(access.privileges_for("prod"), Some(PrivilegeSet::ALL));
        assert!(access.cluster("staging").unwrap().schema_text().is_ok());
    }

    #[test]
    fn a_role_grants_exactly_what_it_declares() {
        let policy = table(
            &[(
                "operator",
                &[PrivilegeName::Records, PrivilegeName::Configs],
            )],
            vec![binding(&["kafka-operators"], "operator", None)],
        );
        let access = admit(&policy, &["kafka-operators"]).unwrap();
        let prod = access.cluster("prod").unwrap();

        assert_eq!(
            access.privileges_for("prod"),
            Some(set(&[Privilege::Records, Privilege::Configs]))
        );
        assert_eq!(
            prod.privileges(),
            vec![Privilege::Records, Privilege::Configs]
        );
        assert!(prod.records().is_ok());
        assert!(prod.configs().is_ok());
        assert_eq!(
            prod.schema_text().unwrap_err(),
            AccessError::Forbidden {
                cluster: "prod".into(),
                privilege: Privilege::SchemaText,
            }
        );
        assert!(prod.acls().is_err());
    }

    #[test]
    fn a_role_without_privileges_sees_the_catalog_and_nothing_else() {
        let policy = table(
            &[("viewer", &[])],
            vec![binding(&["klens-viewers"], "viewer", None)],
        );
        let access = admit(&policy, &["klens-viewers"]).unwrap();
        let prod = access.cluster("prod").expect("the cluster is visible");

        assert_eq!(
            access.privileges_for("prod"),
            Some(PrivilegeSet::NONE),
            "visible with nothing on it, not invisible"
        );
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
        let policy = table(
            &[("viewer", &[])],
            vec![binding(
                &["payments-viewers"],
                "viewer",
                Some(&["payments"]),
            )],
        );
        let access = admit(&policy, &["payments-viewers"]).unwrap();

        assert!(access.cluster("payments").is_ok());
        assert_eq!(access.privileges_for("prod"), None);
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
        let policy = table(
            &[("admin", EVERYTHING), ("viewer", &[])],
            vec![
                binding(&["payments-viewers"], "viewer", Some(&["payments"])),
                binding(&["klens-admins"], "admin", Some(&["prod"])),
            ],
        );
        let access = admit(&policy, &["payments-viewers", "klens-admins"]).unwrap();

        assert_eq!(access.privileges_for("prod"), Some(PrivilegeSet::ALL));
        assert_eq!(
            access.privileges_for("payments"),
            Some(PrivilegeSet::NONE),
            "the admin grant covers prod only"
        );
        assert!(access.cluster("prod").unwrap().records().is_ok());
        assert!(
            access.cluster("payments").unwrap().records().is_err(),
            "scopes must not union under a grant held elsewhere"
        );
    }

    #[test]
    fn overlapping_grants_take_the_union_of_covering_roles() {
        let policy = table(
            &[("admin", EVERYTHING), ("viewer", &[])],
            vec![
                binding(&["everyone"], "viewer", None),
                binding(&["ops"], "admin", Some(&["prod"])),
            ],
        );
        let access = admit(&policy, &["everyone", "ops"]).unwrap();

        assert_eq!(access.privileges_for("prod"), Some(PrivilegeSet::ALL));
        assert_eq!(access.privileges_for("staging"), Some(PrivilegeSet::NONE));
        assert!(access.cluster("staging").unwrap().configs().is_err());
    }

    #[test]
    fn incomparable_roles_union_only_where_both_grants_reach() {
        let policy = table(
            &[
                (
                    "operator",
                    &[PrivilegeName::Records, PrivilegeName::Configs],
                ),
                ("auditor", &[PrivilegeName::Acls, PrivilegeName::SchemaText]),
            ],
            vec![
                binding(&["kafka-operators"], "operator", None),
                binding(&["security-team"], "auditor", Some(&["prod"])),
            ],
        );
        let access = admit(&policy, &["kafka-operators", "security-team"]).unwrap();

        assert_eq!(
            access.privileges_for("prod"),
            Some(PrivilegeSet::READS),
            "neither role contains the other; both apply"
        );
        assert_eq!(
            access.privileges_for("staging"),
            Some(set(&[Privilege::Records, Privilege::Configs])),
            "the auditor grant is scoped to prod"
        );

        let staging = access.cluster("staging").unwrap();
        assert!(staging.records().is_ok());
        assert!(staging.acls().is_err());
        assert!(staging.schema_text().is_err());
    }

    #[test]
    fn role_names_report_every_covering_grant_deduplicated() {
        let policy = table(
            &[
                (
                    "operator",
                    &[PrivilegeName::Records, PrivilegeName::Configs],
                ),
                ("auditor", &[PrivilegeName::Acls, PrivilegeName::SchemaText]),
            ],
            vec![
                binding(&["kafka-operators"], "operator", None),
                binding(&["oncall"], "operator", Some(&["prod"])),
                binding(&["security-team"], "auditor", Some(&["prod"])),
            ],
        );
        let access = admit(&policy, &["kafka-operators", "oncall", "security-team"]).unwrap();

        assert_eq!(
            access.cluster("prod").unwrap().role_names(),
            vec!["auditor", "operator"],
            "two bindings name 'operator' on prod; the name is reported once"
        );
        assert_eq!(
            access.cluster("staging").unwrap().role_names(),
            vec!["operator"]
        );
    }

    #[test]
    fn too_many_groups_are_refused() {
        let policy = table(
            &[("admin", EVERYTHING)],
            vec![binding(&["klens-admins"], "admin", None)],
        );
        let groups: Vec<String> = (0..MAX_GROUPS + 1).map(|i| format!("g{i}")).collect();
        assert_eq!(policy.admit(&Identity { groups: &groups }), None);
    }

    #[test]
    fn privilege_sets_union_and_test_by_bit() {
        let records = set(&[Privilege::Records]);
        let acls = set(&[Privilege::Acls]);

        assert!(records.contains(Privilege::Records));
        assert!(!records.contains(Privilege::Acls));
        assert_eq!(
            records.union(acls).iter().collect::<Vec<_>>(),
            vec![Privilege::Records, Privilege::Acls]
        );
        assert_eq!(PrivilegeSet::default(), PrivilegeSet::NONE);
        assert_eq!(
            PrivilegeSet::from_privileges(Privilege::ALL),
            PrivilegeSet::ALL
        );
        assert!(PrivilegeSet::NONE.iter().next().is_none());
    }

    from_same_variants!(Privilege => PrivilegeName {
        Records,
        Configs,
        SchemaText,
        Acls,
        ResetOffsets,
        DeleteGroupOffsets,
    });

    #[test]
    fn every_privilege_either_reads_or_writes() {
        assert_eq!(
            PrivilegeSet::READS.intersection(PrivilegeSet::WRITES),
            PrivilegeSet::NONE
        );
        assert_eq!(
            PrivilegeSet::READS.union(PrivilegeSet::WRITES),
            PrivilegeSet::ALL
        );
        for privilege in Privilege::ALL {
            assert_eq!(
                PrivilegeSet::WRITES.contains(privilege),
                PrivilegeName::from(privilege).is_write(),
                "{privilege} is classified differently by the config"
            );
        }
    }

    #[test]
    fn a_cluster_ceiling_narrows_even_unrestricted_access() {
        let access = EffectiveAccess::Unrestricted;
        let ceiling = PrivilegeSet::READS.union(set(&[Privilege::ResetOffsets]));
        let staging = access.cluster("staging").unwrap().capped(ceiling);

        assert!(staging.records().is_ok());
        assert_eq!(staging.reset_offsets().unwrap().cluster(), "staging");
        assert_eq!(
            staging.delete_group_offsets().unwrap_err(),
            AccessError::Forbidden {
                cluster: "staging".into(),
                privilege: Privilege::DeleteGroupOffsets,
            }
        );
    }

    #[test]
    fn a_ceiling_never_adds_what_no_role_granted() {
        let policy = table(
            &[("viewer", &[PrivilegeName::Records])],
            vec![binding(&["everyone"], "viewer", None)],
        );
        let access = admit(&policy, &["everyone"]).unwrap();
        let prod = access.cluster("prod").unwrap().capped(PrivilegeSet::ALL);

        assert!(prod.records().is_ok());
        assert!(prod.reset_offsets().is_err());
        assert_eq!(prod.privileges(), vec![Privilege::Records]);
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
