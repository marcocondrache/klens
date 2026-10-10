use std::panic::AssertUnwindSafe;
use std::sync::Arc;

use axum::http::request::Parts;
use futures::FutureExt as _;
use rmcp::handler::server::common::FromContextPart;
use rmcp::handler::server::router::tool::ToolRouter;
use rmcp::handler::server::tool::{IntoCallToolResult, ToolCallContext};
use rmcp::model::{
    CacheScope, CallToolRequestParams, CallToolResponse, ErrorCode, ListToolsResult,
    PaginatedRequestParams, ProtocolVersion,
};
use rmcp::service::RequestContext;
use rmcp::{ErrorData, RoleServer, ServerHandler, tool_handler};
use tracing::Instrument as _;
use tracing::field::Empty;

use crate::AppState;

use crate::app::auth::SessionGuard;
use crate::app::auth::access::Narrowed;
use crate::app::context::Session;
use crate::app::error::ApiError;

use super::MAX_CLIENT_CHARS;
use crate::app::mcp::gate::{TOOLS, offered};
pub(crate) struct KlensMcp {
    pub(super) state: AppState,
    pub(super) tools: Arc<ToolRouter<Self>>,
}

impl KlensMcp {
    fn client(&self, context: &RequestContext<RoleServer>) -> Option<String> {
        if self.state.auth.is_enabled() {
            let parts = context.extensions.get::<Parts>()?;
            let guard = parts.extensions.get::<SessionGuard>()?;
            return guard.client().map(ToOwned::to_owned);
        }
        let name: String = context
            .meta
            .client_info()?
            .name
            .chars()
            .take(MAX_CLIENT_CHARS)
            .collect();
        Some(format!("unverified:{name}"))
    }
}

impl FromContextPart<ToolCallContext<'_, KlensMcp>> for Session {
    fn from_context_part(context: &mut ToolCallContext<'_, KlensMcp>) -> Result<Self, ErrorData> {
        caller(&context.service.state, &mut context.request_context)
    }
}

pub(super) fn caller(
    state: &AppState,
    context: &mut RequestContext<RoleServer>,
) -> Result<Session, ErrorData> {
    context
        .extensions
        .get_mut::<Parts>()
        .and_then(|parts| Session::take::<Narrowed>(&mut parts.extensions, state))
        .ok_or_else(|| ErrorData::internal_error("the request carries no MCP session", None))
}

#[tool_handler(
    router = self.tools,
    name = "klens",
    instructions = "klens shows Kafka clusters as its background reads last saw them. Start with \
                    klens_clusters for cluster names and health. Use klens_search to find the \
                    exact name of a topic, group, broker or schema subject, and \
                    klens_access_explain when a call is refused. A tool reads klens' snapshot \
                    and costs Kafka nothing unless it says it reads live or changes Kafka. A \
                    value klens has not measured yet is null, and a tool fails with NOT_READY \
                    until klens has read what it needs. A list returns 25 rows unless `limit` \
                    asks for up to 100, and `showing` says how many matched."
)]
impl ServerHandler for KlensMcp {
    async fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
        mut context: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, ErrorData> {
        let session = caller(&self.state, &mut context)?;
        let tools = self
            .tools
            .list_all()
            .into_iter()
            .filter(|tool| offered(&session, &tool.name))
            .collect();
        let mut list = ListToolsResult::with_all_items(tools);
        if context
            .protocol_version()
            .is_some_and(|version| version >= ProtocolVersion::V_2026_07_28)
        {
            list = list.with_cache_scope(CacheScope::Private);
        }
        Ok(list)
    }

    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResponse, ErrorData> {
        let Some(route) = self.tools.map.get(&*request.name) else {
            return Err(ErrorData::invalid_params("tool not found", None));
        };
        let span = tracing::info_span!("mcp.tool", tool = &*route.attr.name, client = Empty);
        if let Some(client) = self.client(&context) {
            span.record("client", client.as_str());
        }
        let cancelled = context.ct.clone();
        // A write that reached Kafka must still log its audit line after the
        // client goes away, so only a read stops when it does.
        let changes = TOOLS
            .iter()
            .any(|gate| gate.name == route.attr.name && gate.changes());
        async move {
            let Some(_permit) = self.state.mcp_permit() else {
                return ApiError::RateLimited.into_call_tool_result();
            };
            let call = AssertUnwindSafe((route.call)(ToolCallContext::new(self, request, context)));
            tokio::select! {
                response = call.catch_unwind() => {
                    match response {
                        // rmcp answers arguments that miss the input schema
                        // with serde's message alone, so they get a code and a
                        // hint like every other refusal.
                        Ok(Err(error)) if error.code == ErrorCode::INVALID_PARAMS => {
                            ApiError::unprocessable(error.message).into_call_tool_result()
                        }
                        Ok(response) => response,
                        // rmcp runs the call in a task of its own, out of reach
                        // of the server's CatchPanicLayer, and a panic there
                        // leaves the request without an answer.
                        Err(_) => Err(ErrorData::internal_error("the tool failed", None)),
                    }
                }
                () = cancelled.cancelled(), if !changes => {
                    Err(ErrorData::internal_error("the client cancelled the call", None))
                }
            }
        }
        .instrument(span)
        .await
    }
}
