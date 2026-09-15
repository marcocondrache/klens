use std::collections::BTreeSet;

use crate::config::{RoleBinding, RoleName, RolesConfig, default_groups_claim};

const MAX_GROUPS: usize = 64;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    Admin,
    Viewer,
}

impl From<RoleName> for Role {
    fn from(name: RoleName) -> Self {
        match name {
            RoleName::Admin => Self::Admin,
            RoleName::Viewer => Self::Viewer,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Privilege {
    Records,
    LiveConfig,
    SchemaText,
    Acls,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ClusterScope {
    All,
    Only(BTreeSet<String>),
}

impl ClusterScope {
    fn contains(&self, cluster: &str) -> bool {
        match self {
            Self::All => true,
            Self::Only(names) => names.contains(cluster),
        }
    }

    fn union(self, other: Self) -> Self {
        match (self, other) {
            (Self::All, _) | (_, Self::All) => Self::All,
            (Self::Only(mut left), Self::Only(right)) => {
                left.extend(right);
                Self::Only(left)
            }
        }
    }

    fn from_list(clusters: Option<&[String]>) -> Self {
        match clusters {
            None => Self::All,
            Some(names) => Self::Only(names.iter().cloned().collect()),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Grant {
    pub role: Role,
    pub clusters: ClusterScope,
}

impl Grant {
    fn merge(self, other: Self) -> Self {
        let role = match (self.role, other.role) {
            (Role::Admin, _) | (_, Role::Admin) => Role::Admin,
            (Role::Viewer, Role::Viewer) => Role::Viewer,
        };
        Self {
            role,
            clusters: self.clusters.union(other.clusters),
        }
    }

    fn allows(&self, privilege: Privilege, cluster: &str) -> bool {
        if !self.clusters.contains(cluster) {
            return false;
        }
        match self.role {
            Role::Admin => true,
            Role::Viewer => match privilege {
                Privilege::Records
                | Privilege::LiveConfig
                | Privilege::SchemaText
                | Privilege::Acls => false,
            },
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EffectiveAccess {
    Unrestricted,
    Restricted(Grant),
}

impl EffectiveAccess {
    pub fn can_see_cluster(&self, cluster: &str) -> bool {
        match self {
            Self::Unrestricted => true,
            Self::Restricted(grant) => grant.clusters.contains(cluster),
        }
    }

    pub fn allows(&self, privilege: Privilege, cluster: &str) -> bool {
        match self {
            Self::Unrestricted => true,
            Self::Restricted(grant) => grant.allows(privilege, cluster),
        }
    }

    pub fn role(&self) -> Option<Role> {
        match self {
            Self::Unrestricted => None,
            Self::Restricted(grant) => Some(grant.role),
        }
    }

    pub fn clusters(&self) -> Option<&ClusterScope> {
        match self {
            Self::Unrestricted => None,
            Self::Restricted(grant) => Some(&grant.clusters),
        }
    }
}

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
    clusters: ClusterScope,
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
                table
                    .resolve(identity.groups)
                    .map(EffectiveAccess::Restricted)
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

    fn resolve(&self, groups: &[String]) -> Option<Grant> {
        let present: BTreeSet<&str> = groups.iter().map(String::as_str).collect();
        let mut grant: Option<Grant> = None;
        for binding in &self.bindings {
            if binding
                .groups
                .iter()
                .any(|group| present.contains(group.as_str()))
            {
                let next = Grant {
                    role: binding.role,
                    clusters: binding.clusters.clone(),
                };
                grant = Some(match grant {
                    Some(current) => current.merge(next),
                    None => next,
                });
            }
        }
        grant
    }
}

impl CompiledBinding {
    fn from_config(binding: &RoleBinding) -> Self {
        Self {
            groups: binding.groups.iter().cloned().collect(),
            role: Role::from(binding.role),
            clusters: ClusterScope::from_list(binding.clusters.as_deref()),
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
        assert!(access.allows(Privilege::Records, "prod"));
    }

    #[test]
    fn unmatched_groups_are_refused() {
        let policy = table(vec![binding(&["klens-admins"], RoleName::Admin, None)]);
        assert_eq!(admit(&policy, &["other"]), None);
        assert_eq!(admit(&policy, &[]), None);
    }

    #[test]
    fn admin_sees_records_on_every_cluster() {
        let policy = table(vec![binding(&["klens-admins"], RoleName::Admin, None)]);
        let access = admit(&policy, &["klens-admins"]).unwrap();
        assert!(access.can_see_cluster("prod"));
        assert!(access.allows(Privilege::Records, "prod"));
        assert!(access.allows(Privilege::SchemaText, "staging"));
    }

    #[test]
    fn viewer_sees_catalog_only() {
        let policy = table(vec![binding(&["klens-viewers"], RoleName::Viewer, None)]);
        let access = admit(&policy, &["klens-viewers"]).unwrap();
        assert!(access.can_see_cluster("prod"));
        assert!(!access.allows(Privilege::Records, "prod"));
        assert!(!access.allows(Privilege::LiveConfig, "prod"));
        assert!(!access.allows(Privilege::SchemaText, "prod"));
        assert!(!access.allows(Privilege::Acls, "prod"));
    }

    #[test]
    fn viewer_cluster_list_hides_other_clusters() {
        let policy = table(vec![binding(
            &["payments-viewers"],
            RoleName::Viewer,
            Some(&["payments"]),
        )]);
        let access = admit(&policy, &["payments-viewers"]).unwrap();
        assert!(access.can_see_cluster("payments"));
        assert!(!access.can_see_cluster("prod"));
        assert!(!access.allows(Privilege::Records, "payments"));
    }

    #[test]
    fn admin_wins_and_cluster_sets_union() {
        let policy = table(vec![
            binding(&["payments-viewers"], RoleName::Viewer, Some(&["payments"])),
            binding(&["klens-admins"], RoleName::Admin, Some(&["prod"])),
        ]);
        let access = admit(&policy, &["payments-viewers", "klens-admins"]).unwrap();
        match access {
            EffectiveAccess::Restricted(ref grant) => {
                assert_eq!(grant.role, Role::Admin);
                assert!(grant.clusters.contains("payments"));
                assert!(grant.clusters.contains("prod"));
            }
            EffectiveAccess::Unrestricted => panic!("expected a grant"),
        }
        assert!(access.allows(Privilege::Records, "payments"));
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
