//! MCP gateway HTTP routes.

use crate::error::ApiError;
use crate::extract::{AuthenticatedIdentity, OrgIdHeader};
use crate::server::AppState;
use axum::routing::{get, post};
use axum::{Extension, Json};
use horizons_core::core_agents::mcp::McpClient;
use horizons_core::core_agents::mcp::{McpToolCall, McpToolResult};
use horizons_core::core_agents::mcp_gateway::McpServerConfig;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug, Deserialize)]
pub struct McpConfigRequest {
    pub servers: Vec<McpServerConfig>,
}

#[derive(Debug, Serialize)]
pub struct McpConfigResponse {
    pub ok: bool,
    pub server_count: usize,
}

#[derive(Debug, Serialize)]
pub struct McpToolsResponse {
    pub tools: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct McpCallRequest {
    pub tool_name: String,
    #[serde(default)]
    pub arguments: serde_json::Value,
    #[serde(default)]
    pub request_id: Option<String>,
    /// Optional override of when this request was created.
    #[serde(default)]
    pub requested_at: Option<chrono::DateTime<chrono::Utc>>,
    /// Optional metadata passthrough.
    #[serde(default)]
    pub metadata: HashMap<String, String>,
}

#[tracing::instrument(level = "debug", skip_all)]
pub fn router() -> axum::Router {
    axum::Router::new()
        .route("/mcp/config", post(post_config))
        .route("/mcp/tools", get(get_tools))
        .route("/mcp/call", post(post_call))
}

/// POST /api/v1/mcp/config
#[tracing::instrument(level = "info", skip_all)]
async fn post_config(
    OrgIdHeader(_org_id): OrgIdHeader,
    Extension(state): Extension<Arc<AppState>>,
    Json(req): Json<McpConfigRequest>,
) -> Result<Json<McpConfigResponse>, ApiError> {
    let gateway = state.mcp_gateway.as_ref().ok_or_else(|| {
        ApiError::InvalidInput("mcp gateway not configured (set HORIZONS_MCP_CONFIG)".to_string())
    })?;
    let server_count = req.servers.len();
    gateway.reconfigure(req.servers).await?;
    Ok(Json(McpConfigResponse {
        ok: true,
        server_count,
    }))
}

/// GET /api/v1/mcp/tools
#[tracing::instrument(level = "info", skip_all)]
async fn get_tools(
    OrgIdHeader(_org_id): OrgIdHeader,
    Extension(state): Extension<Arc<AppState>>,
) -> Result<Json<McpToolsResponse>, ApiError> {
    let gateway = state.mcp_gateway.as_ref().ok_or_else(|| {
        ApiError::InvalidInput("mcp gateway not configured (set HORIZONS_MCP_CONFIG)".to_string())
    })?;
    let tools = gateway.list_tools().await?;
    Ok(Json(McpToolsResponse { tools }))
}

/// POST /api/v1/mcp/call
#[tracing::instrument(level = "info", skip_all)]
async fn post_call(
    AuthenticatedIdentity {
        org_id: _org_id,
        identity,
    }: AuthenticatedIdentity,
    Extension(state): Extension<Arc<AppState>>,
    Json(req): Json<McpCallRequest>,
) -> Result<Json<McpToolResult>, ApiError> {
    let gateway = state.mcp_gateway.as_ref().ok_or_else(|| {
        ApiError::InvalidInput("mcp gateway not configured (set HORIZONS_MCP_CONFIG)".to_string())
    })?;

    let request_id = req
        .request_id
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

    // Scope enforcement: the caller does not get to pick scopes; scopes are policy/config-driven.
    let requested_scopes: Vec<String> = std::env::var("HORIZONS_MCP_CALL_SCOPES")
        .ok()
        .unwrap_or_default()
        .split(',')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect();

    let call = McpToolCall::new(
        req.tool_name,
        req.arguments,
        requested_scopes,
        identity,
        request_id,
        req.requested_at,
    )?;

    let result = gateway.call_tool(call).await?;
    Ok(Json(result))
}
