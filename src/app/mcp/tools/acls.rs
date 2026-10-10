use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::CallToolResult;
use rmcp::{tool, tool_router};
use schemars::JsonSchema;
use serde::Deserialize;
use serde_json::json;

use crate::kafka::model as domain;

use crate::app::acls::Acl;
use crate::app::acls::types::{AclOperation, AclPermission, AclResourceType, AclStatus};
use crate::app::context::Session;
use crate::app::error::ApiError;

use super::super::gate::ToolGate;
use super::super::types::AclList;
use super::super::{CLIENT_VALUES_NOTICE, MAX_ROWS};
use crate::app::auth::access::Privilege;
use crate::app::mcp::fit::{listed, name_filter, one_cluster};
use crate::app::mcp::lanes::snapshot;
use crate::app::mcp::server::KlensMcp;
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
    ) -> Result<CallToolResult, ApiError> {
        let cluster = one_cluster(&session, query.cluster.as_deref())?;
        cluster.access.acls()?;
        let listing = snapshot(&cluster, "acls", &cluster.store.acls)?;
        let (status, rows) = match &*listing {
            domain::AclListing::Enabled(rows) => (AclStatus::Enabled, rows.as_slice()),
            domain::AclListing::Disabled => (AclStatus::Disabled, [].as_slice()),
            domain::AclListing::Denied => (AclStatus::Denied, [].as_slice()),
        };
        let named = name_filter(query.contains.as_deref());
        let bindings: Vec<Acl> = rows
            .iter()
            .map(Acl::from)
            .filter(|acl| named(&acl.principal) || named(&acl.resource_name) || named(&acl.host))
            .filter(|acl| {
                query
                    .resource_type
                    .is_none_or(|kind| acl.resource_type == kind)
            })
            .filter(|acl| {
                query
                    .operation
                    .is_none_or(|operation| acl.operation == operation)
            })
            .filter(|acl| {
                query
                    .permission
                    .is_none_or(|permission| acl.permission == permission)
            })
            .collect();
        Ok(listed(
            bindings,
            query.limit,
            Some("pass `contains` or another filter"),
            |bindings, showing| {
                json!(AclList {
                    status,
                    bindings,
                    showing,
                    notice: CLIENT_VALUES_NOTICE,
                })
            },
        ))
    }
}
