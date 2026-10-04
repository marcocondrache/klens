use std::collections::BTreeSet;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

use indexmap::IndexMap;

pub use crate::config::Privilege;
use crate::config::Role;

const MAX_GROUPS: usize = 64;

impl Privilege {
    pub const ALL: [Self; 10] = [
        Self::Records,
        Self::Configs,
        Self::SchemaText,
        Self::Acls,
        Self::ManageTopics,
        Self::Produce,
        Self::ManageGroups,
        Self::ManageSchemas,
        Self::ManageAcls,
        Self::ManageBrokers,
    ];

    pub fn name(self) -> &'static str {
        match self {
            Self::Records => "records",
            Self::Configs => "configs",
            Self::SchemaText => "schemaText",
            Self::Acls => "acls",
            Self::ManageTopics => "manageTopics",
            Self::Produce => "produce",
            Self::ManageGroups => "manageGroups",
            Self::ManageSchemas => "manageSchemas",
            Self::ManageAcls => "manageAcls",
            Self::ManageBrokers => "manageBrokers",
        }
    }

    const fn bit(self) -> u16 {
        1 << self as u16
    }
}

impl Display for Privilege {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.name())
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PrivilegeSet(u16);

impl PrivilegeSet {
    pub const NONE: Self = Self(0);
    pub const ALL: Self = Self((1 << Privilege::ALL.len()) - 1);

    pub fn from_privileges(privileges: impl IntoIterator<Item = Privilege>) -> Self {
        privileges
            .into_iter()
            .fold(Self::NONE, |set, privilege| Self(set.0 | privilege.bit()))
    }

    pub fn contains(self, privilege: Privilege) -> bool {
        self.0 & privilege.bit() != 0
    }

    pub fn union(self, other: Self) -> Self {
        Self(self.0 | other.0)
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
    ReadOnlyCluster(String),
}

impl AccessError {
    pub fn code(&self) -> &'static str {
        match self {
            Self::UnknownCluster(_) => "UNKNOWN_CLUSTER",
            Self::Forbidden { .. } => "FORBIDDEN",
            Self::ReadOnlyCluster(_) => "READ_ONLY_CLUSTER",
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
            Self::ReadOnlyCluster(cluster) => write!(formatter, "cluster '{cluster}' is read-only"),
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

    fn check(&self, privilege: Privilege) -> Result<(), AccessError> {
        if self.allows(privilege) {
            Ok(())
        } else {
            Err(AccessError::Forbidden {
                cluster: self.cluster.to_owned(),
                privilege,
            })
        }
    }
}

/// Each token proves one privilege was checked; only [`ClusterAccess`] can
/// make one.
macro_rules! capability {
    ($(#[$meta:meta])* $token:ident, $method:ident, $privilege:expr) => {
        $(#[$meta])*
        #[derive(Clone, Copy, Debug)]
        pub struct $token(());

        impl ClusterAccess<'_> {
            pub fn $method(&self) -> Result<$token, AccessError> {
                self.check($privilege).map(|()| $token(()))
            }
        }
    };
}

capability!(RecordsCap, records, Privilege::Records);
capability!(ConfigsCap, configs, Privilege::Configs);
capability!(SchemaTextCap, schema_text, Privilege::SchemaText);
capability!(AclsCap, acls, Privilege::Acls);
capability!(ManageTopicsCap, manage_topics, Privilege::ManageTopics);
capability!(ProduceCap, produce, Privilege::Produce);
capability!(ManageGroupsCap, manage_groups, Privilege::ManageGroups);
capability!(ManageSchemasCap, manage_schemas, Privilege::ManageSchemas);
capability!(ManageAclsCap, manage_acls, Privilege::ManageAcls);
capability!(ManageBrokersCap, manage_brokers, Privilege::ManageBrokers);

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Identity<'a> {
    pub groups: &'a [String],
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RoleTable {
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
    Open,
    Bound(RoleTable),
}

impl AccessPolicy {
    pub fn disabled() -> Self {
        Self::Disabled
    }

    pub fn from_roles(roles: Option<&IndexMap<String, Role>>) -> Self {
        match roles {
            None => Self::Open,
            Some(roles) => Self::Bound(RoleTable::compile(roles)),
        }
    }

    pub fn admit(&self, identity: &Identity<'_>) -> Option<EffectiveAccess> {
        match self {
            Self::Disabled | Self::Open => Some(EffectiveAccess::Unrestricted),
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
    fn compile(roles: &IndexMap<String, Role>) -> Self {
        Self {
            bindings: roles
                .iter()
                .flat_map(|(name, role)| {
                    let role_name: Arc<str> = Arc::from(name.as_str());
                    let privileges = PrivilegeSet::from_privileges(role.privileges.iter().copied());
                    role.bindings.iter().map(move |binding| CompiledBinding {
                        groups: binding.groups.iter().cloned().collect(),
                        role_name: Arc::clone(&role_name),
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
    use crate::config::Binding;

    const EVERYTHING: &[Privilege] = &Privilege::ALL;

    struct Bound<'a> {
        groups: &'a [&'a str],
        role: &'a str,
        clusters: Option<&'a [&'a str]>,
    }

    fn table(definitions: &[(&str, &[Privilege])], bindings: Vec<Bound<'_>>) -> AccessPolicy {
        let roles = definitions
            .iter()
            .map(|(name, privileges)| {
                let role = Role {
                    privileges: privileges.to_vec(),
                    bindings: bindings
                        .iter()
                        .filter(|bound| bound.role == *name)
                        .map(|bound| Binding {
                            groups: bound
                                .groups
                                .iter()
                                .map(|group| (*group).to_owned())
                                .collect(),
                            clusters: bound
                                .clusters
                                .map(|names| names.iter().map(|name| (*name).to_owned()).collect()),
                        })
                        .collect(),
                };
                ((*name).to_owned(), role)
            })
            .collect();
        AccessPolicy::from_roles(Some(&roles))
    }

    fn binding<'a>(
        groups: &'a [&'a str],
        role: &'a str,
        clusters: Option<&'a [&'a str]>,
    ) -> Bound<'a> {
        Bound {
            groups,
            role,
            clusters,
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
    fn a_role_holding_every_privilege_holds_every_capability() {
        let policy = table(
            &[("admin", EVERYTHING)],
            vec![binding(&["klens-admins"], "admin", None)],
        );
        let access = admit(&policy, &["klens-admins"]).unwrap();

        let prod = access.cluster("prod").unwrap();
        assert_eq!(prod.role_names(), vec!["admin"]);
        assert!(prod.records().is_ok());
        assert!(prod.configs().is_ok());
        assert!(prod.schema_text().is_ok());
        assert!(prod.acls().is_ok());
        assert!(prod.manage_topics().is_ok());
        assert!(prod.produce().is_ok());
        assert!(prod.manage_groups().is_ok());
        assert!(prod.manage_schemas().is_ok());
        assert!(prod.manage_acls().is_ok());
        assert!(prod.manage_brokers().is_ok());
        assert_eq!(prod.privileges(), Privilege::ALL.to_vec());
        assert_eq!(access.privileges_for("prod"), Some(PrivilegeSet::ALL));
        assert!(access.cluster("staging").unwrap().schema_text().is_ok());
    }

    #[test]
    fn a_role_grants_exactly_what_it_declares() {
        let policy = table(
            &[("operator", &[Privilege::Records, Privilege::Configs])],
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
                ("operator", &[Privilege::Records, Privilege::Configs]),
                ("auditor", &[Privilege::Acls, Privilege::SchemaText]),
            ],
            vec![
                binding(&["kafka-operators"], "operator", None),
                binding(&["security-team"], "auditor", Some(&["prod"])),
            ],
        );
        let access = admit(&policy, &["kafka-operators", "security-team"]).unwrap();

        assert_eq!(
            access.privileges_for("prod"),
            Some(set(&[
                Privilege::Records,
                Privilege::Configs,
                Privilege::SchemaText,
                Privilege::Acls,
            ])),
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
                ("operator", &[Privilege::Records, Privilege::Configs]),
                ("auditor", &[Privilege::Acls, Privilege::SchemaText]),
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
