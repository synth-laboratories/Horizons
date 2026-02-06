use crate::models::AgentIdentity;
use crate::{Error, Result};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// An MCP tool call request. The transport (JSON-RPC, HTTP, etc.) is implementation-defined.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct McpToolCall {
    pub tool_name: String,
    pub arguments: serde_json::Value,
    pub requested_scopes: Vec<String>,
    pub request_id: String,
    pub requested_at: DateTime<Utc>,
}

impl McpToolCall {
    #[tracing::instrument(level = "debug", skip(arguments))]
    pub fn new(
        tool_name: impl Into<String> + std::fmt::Debug,
        arguments: serde_json::Value,
        requested_scopes: Vec<String>,
        request_id: impl Into<String> + std::fmt::Debug,
        requested_at: Option<DateTime<Utc>>,
    ) -> Result<Self> {
        let tool_name = tool_name.into();
        if tool_name.trim().is_empty() {
            return Err(Error::InvalidInput("tool_name is empty".to_string()));
        }
        if requested_scopes.iter().any(|s| s.trim().is_empty()) {
            return Err(Error::InvalidInput(
                "requested_scopes contains empty scope".to_string(),
            ));
        }
        let request_id = request_id.into();
        if request_id.trim().is_empty() {
            return Err(Error::InvalidInput("request_id is empty".to_string()));
        }
        Ok(Self {
            tool_name,
            arguments,
            requested_scopes,
            request_id,
            requested_at: requested_at.unwrap_or_else(Utc::now),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct McpToolResult {
    pub request_id: String,
    pub ok: bool,
    pub output: serde_json::Value,
    pub finished_at: DateTime<Utc>,
}

#[async_trait]
pub trait McpScopeProvider: Send + Sync {
    async fn scopes_for(&self, identity: &AgentIdentity) -> Result<Vec<String>>;
}

#[async_trait]
pub trait McpClient: Send + Sync {
    async fn call_tool(&self, call: McpToolCall) -> Result<McpToolResult>;
    fn name(&self) -> &'static str;
}

#[tracing::instrument(level = "debug", skip(available_scopes))]
pub fn scopes_satisfy(required: &[String], available_scopes: &[String]) -> bool {
    // Exact-string scope match for v0.0.x. (No wildcards, no hierarchical inference.)
    required
        .iter()
        .all(|r| available_scopes.iter().any(|a| a == r))
}

#[tracing::instrument(level = "debug", skip(provider, required_scopes))]
pub async fn ensure_scopes(
    provider: &dyn McpScopeProvider,
    identity: &AgentIdentity,
    required_scopes: &[String],
) -> Result<()> {
    if required_scopes.is_empty() {
        return Ok(());
    }
    let available = provider.scopes_for(identity).await?;
    if !scopes_satisfy(required_scopes, &available) {
        return Err(Error::Unauthorized("mcp scope check failed".to_string()));
    }
    Ok(())
}
