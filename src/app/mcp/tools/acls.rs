use rmcp::handler::server::wrapper::Parameters;
use rmcp::{tool, tool_router};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::app::acls::Acl;
use crate::app::acls::types::{AclOperation, AclPermission, AclResourceType, AclStatus};
use crate::app::auth::access::Privilege;
use crate::app::context::Session;
use crate::app::mcp::args::NameFilter;
use crate::app::mcp::ext::{ClusterExt as _, SessionExt as _};
use crate::app::mcp::gate::ToolGate;
use crate::app::mcp::reply::{Cut, Page, Reply, fit};
use crate::app::mcp::server::{KlensMcp, ToolResult};
use crate::app::mcp::{CLIENT_VALUES_NOTICE, MAX_ROWS};
use crate::kafka::model::AclListing;

#[derive(Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct AclsQuery {
    /// A cluster name from klens_clusters. Optional when you see only one.
    cluster: Option<String>,
    /// Keeps bindings whose principal, resource name or host holds this text, in any case.
    contains: Option<String>,
    /// Keeps bindings on this type of resource.
    resource_type: Option<AclResourceType>,
    /// Keeps bindings for this exact operation.
    operation: Option<AclOperation>,
    /// Keeps bindings with this permission.
    permission: Option<AclPermission>,
    /// How many bindings to return: 25 unless given, at most 100.
    #[schemars(range(min = 1, max = MAX_ROWS))]
    limit: Option<usize>,
}

pub(super) const GATES: &[ToolGate] = &[ToolGate::needing("klens_acls_list", Privilege::Acls)];

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct AclList {
    status: AclStatus,
    #[serde(flatten)]
    page: Page<Acl>,
    notice: &'static str,
}

impl Reply for AclList {
    fn lists(&mut self) -> Vec<&mut dyn Cut> {
        vec![self.page.rows()]
    }

    fn kept_whole(&self) -> Option<&str> {
        None
    }
}

impl AclsQuery {
    fn bindings(&self, listing: &AclListing) -> (AclStatus, Vec<Acl>) {
        let rows = match listing {
            AclListing::Enabled(rows) => rows.as_slice(),
            AclListing::Disabled | AclListing::Denied => &[],
        };
        let status = match listing {
            AclListing::Enabled(_) => AclStatus::Enabled,
            AclListing::Disabled => AclStatus::Disabled,
            AclListing::Denied => AclStatus::Denied,
        };
        let names = NameFilter::new(self.contains.as_deref());
        let bindings = rows
            .iter()
            .map(Acl::from)
            .filter(|acl| {
                names.matches(&acl.principal)
                    || names.matches(&acl.resource_name)
                    || names.matches(&acl.host)
            })
            .filter(|acl| {
                self.resource_type
                    .is_none_or(|kind| acl.resource_type == kind)
            })
            .filter(|acl| {
                self.operation
                    .is_none_or(|operation| acl.operation == operation)
            })
            .filter(|acl| {
                self.permission
                    .is_none_or(|permission| acl.permission == permission)
            })
            .collect();
        (status, bindings)
    }
}

#[tool_router(router = acl_tools, vis = "pub(super)")]
impl KlensMcp {
    /// Lists a cluster's ACL bindings.
    /// A PREFIXED binding covers every name that starts with its resource name, the resource name * covers every resource of its type, and the operation ALL covers every operation.
    /// `status` DISABLED means the cluster runs no authorizer, and DENIED means klens' own Kafka user may not describe ACLs.
    #[tool(title = "List ACLs")]
    async fn klens_acls_list(
        &self,
        session: Session,
        Parameters(query): Parameters<AclsQuery>,
    ) -> ToolResult {
        let cluster = session.cluster_or_only(query.cluster.as_deref())?;
        cluster.access.acls()?;
        let listing = cluster.snapshot("acls", &cluster.store.acls)?;
        let (status, bindings) = query.bindings(&listing);
        Ok(fit(AclList {
            status,
            page: Page::new(
                "bindings",
                bindings,
                query.limit,
                Some("pass `contains` or another filter"),
            ),
            notice: CLIENT_VALUES_NOTICE,
        }))
    }
}
