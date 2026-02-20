//! Reusable MCP JSON-RPC 2.0 tool server.
//!
//! Provides a framework-agnostic tool registry and JSON-RPC handler that
//! implements the subset of the MCP protocol used by Codex:
//!   - `initialize`
//!   - `tools/list`
//!   - `tools/call`
//!
//! The consumer (e.g. sb-hzn-app) mounts this behind an HTTP route and
//! passes request bodies through `handle_request()`.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::RwLock;

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// Definition of an MCP tool (schema sent to the agent).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolDef {
    pub name: String,
    pub description: String,
    /// JSON Schema object describing the tool's input parameters.
    #[serde(rename = "inputSchema")]
    pub input_schema: serde_json::Value,
}

/// Type-erased async tool handler.
///
/// Receives the `arguments` object from the `tools/call` request and returns
/// either a JSON value (success) or a string error message.
pub type ToolHandler = Arc<
    dyn Fn(
            serde_json::Value,
        ) -> Pin<Box<dyn Future<Output = Result<serde_json::Value, String>> + Send>>
        + Send
        + Sync,
>;

/// A reusable MCP tool server that can be mounted on any HTTP framework.
///
/// Thread-safe and cheaply cloneable (all state is behind `Arc`).
#[derive(Clone)]
pub struct McpToolServer {
    tools: Arc<RwLock<Vec<(ToolDef, ToolHandler)>>>,
    /// Maps bearer token → session_id for auth.
    sessions: Arc<RwLock<HashMap<String, String>>>,
    server_name: String,
}

/// HTTP-level response from the MCP handler.
pub struct McpResponse {
    pub status: u16,
    pub body: serde_json::Value,
    /// Extra headers to include in the HTTP response.
    pub headers: Vec<(String, String)>,
}

// ---------------------------------------------------------------------------
// JSON-RPC 2.0 types (internal)
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct JsonRpcRequest {
    #[allow(dead_code)]
    jsonrpc: Option<String>,
    #[serde(default)]
    id: serde_json::Value,
    method: String,
    #[serde(default)]
    params: serde_json::Value,
}

#[derive(Serialize)]
struct JsonRpcResponse {
    jsonrpc: &'static str,
    id: serde_json::Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<JsonRpcError>,
}

#[derive(Serialize)]
struct JsonRpcError {
    code: i64,
    message: String,
}

// ---------------------------------------------------------------------------
// Implementation
// ---------------------------------------------------------------------------

impl McpToolServer {
    pub fn new() -> Self {
        Self::with_name("horizons-mcp-tool-server")
    }

    pub fn with_name(name: &str) -> Self {
        Self {
            tools: Arc::new(RwLock::new(Vec::new())),
            sessions: Arc::new(RwLock::new(HashMap::new())),
            server_name: name.to_string(),
        }
    }

    /// Remove all registered tools. Call before re-registering for a new run
    /// to avoid stale handlers from previous runs being matched first.
    pub async fn clear_tools(&self) {
        self.tools.write().await.clear();
    }

    /// Register a tool with a typed async handler.
    ///
    /// The handler closure receives the `arguments` JSON object from the
    /// `tools/call` request and should return `Ok(value)` or `Err(message)`.
    pub async fn register_tool<F, Fut>(
        &self,
        name: impl Into<String>,
        description: impl Into<String>,
        input_schema: serde_json::Value,
        handler: F,
    ) where
        F: Fn(serde_json::Value) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<serde_json::Value, String>> + Send + 'static,
    {
        let def = ToolDef {
            name: name.into(),
            description: description.into(),
            input_schema,
        };
        let handler: ToolHandler = Arc::new(move |args| Box::pin(handler(args)));
        self.tools.write().await.push((def, handler));
    }

    /// Create a session and return its bearer token.
    pub async fn create_session(&self, session_id: &str) -> String {
        let token = format!("mcp-{}", uuid::Uuid::new_v4());
        self.sessions
            .write()
            .await
            .insert(token.clone(), session_id.to_string());
        token
    }

    /// Revoke a session token.
    pub async fn revoke_session(&self, token: &str) {
        self.sessions.write().await.remove(token);
    }

    /// Check whether a bearer token is currently registered for this server.
    pub async fn has_session_token(&self, token: &str) -> bool {
        self.sessions.read().await.contains_key(token)
    }

    /// Handle an incoming HTTP request (framework-agnostic).
    ///
    /// `auth_header` should be the value of the `Authorization` header (e.g.
    /// `"Bearer mcp-xxxxx"`). Returns `McpResponse` with status code, body, and headers.
    ///
    /// Implements the MCP Streamable HTTP transport protocol:
    /// - `initialize` → returns capabilities + protocolVersion, sets `Mcp-Session-Id`
    /// - `notifications/initialized` → accepted as no-op notification (returns 204)
    /// - `tools/list` → returns registered tools
    /// - `tools/call` → calls tool handler and returns result
    pub async fn handle_request(&self, auth_header: Option<&str>, body: &[u8]) -> McpResponse {
        // 1. Authenticate.
        if let Err(resp) = self.check_auth(auth_header).await {
            tracing::warn!(
                server = %self.server_name,
                status = resp.status,
                "MCP auth failed"
            );
            return resp;
        }

        // 2. Parse JSON-RPC request.
        let req: JsonRpcRequest = match serde_json::from_slice(body) {
            Ok(r) => r,
            Err(e) => {
                tracing::error!(
                    server = %self.server_name,
                    err = %e,
                    body_len = body.len(),
                    "MCP JSON-RPC parse error"
                );
                return McpResponse {
                    status: 400,
                    headers: vec![],
                    body: serde_json::to_value(JsonRpcResponse {
                        jsonrpc: "2.0",
                        id: serde_json::Value::Null,
                        result: None,
                        error: Some(JsonRpcError {
                            code: -32700,
                            message: format!("parse error: {e}"),
                        }),
                    })
                    .unwrap(),
                };
            }
        };

        tracing::info!(
            server = %self.server_name,
            method = %req.method,
            id = %req.id,
            "MCP JSON-RPC request"
        );

        // 3. Handle notifications (no id, no response expected).
        //    MCP clients send `notifications/initialized` after the initialize handshake.
        //    Per the MCP Streamable HTTP transport spec, respond with 202 Accepted.
        if req.method.starts_with("notifications/") {
            tracing::info!(
                server = %self.server_name,
                method = %req.method,
                "MCP notification accepted"
            );
            return McpResponse {
                status: 202,
                headers: vec![],
                body: serde_json::Value::Null,
            };
        }

        // 4. Dispatch by method.
        let is_initialize = req.method == "initialize";
        let result = match req.method.as_str() {
            "initialize" => self.handle_initialize().await,
            "tools/list" => self.handle_tools_list().await,
            "tools/call" => {
                let tool_name = req
                    .params
                    .get("name")
                    .and_then(|n| n.as_str())
                    .unwrap_or("?");
                tracing::info!(
                    server = %self.server_name,
                    tool = %tool_name,
                    "MCP tools/call dispatching"
                );
                self.handle_tools_call(req.params).await
            }
            other => Err(JsonRpcError {
                code: -32601,
                message: format!("method not found: {other}"),
            }),
        };

        match &result {
            Ok(_) => tracing::info!(
                server = %self.server_name,
                method = %req.method,
                "MCP request succeeded"
            ),
            Err(e) => tracing::error!(
                server = %self.server_name,
                method = %req.method,
                code = e.code,
                err = %e.message,
                "MCP request failed"
            ),
        }

        let resp = match result {
            Ok(value) => JsonRpcResponse {
                jsonrpc: "2.0",
                id: req.id,
                result: Some(value),
                error: None,
            },
            Err(error) => JsonRpcResponse {
                jsonrpc: "2.0",
                id: req.id,
                result: None,
                error: Some(error),
            },
        };

        // For the initialize response, include the Mcp-Session-Id header
        // as required by the MCP Streamable HTTP transport.
        let headers = if is_initialize {
            vec![(
                "Mcp-Session-Id".to_string(),
                uuid::Uuid::new_v4().to_string(),
            )]
        } else {
            vec![]
        };

        McpResponse {
            status: 200,
            headers,
            body: serde_json::to_value(resp).unwrap(),
        }
    }

    /// Return the number of registered tools.
    pub async fn tool_count(&self) -> usize {
        self.tools.read().await.len()
    }

    // -----------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------

    async fn check_auth(&self, auth_header: Option<&str>) -> Result<(), McpResponse> {
        let token = auth_header
            .and_then(|h| h.strip_prefix("Bearer "))
            .map(|t| t.trim());

        let Some(token) = token else {
            return Err(McpResponse {
                status: 401,
                headers: vec![],
                body: serde_json::json!({"error": "missing or invalid Authorization header"}),
            });
        };

        let sessions = self.sessions.read().await;
        if !sessions.contains_key(token) {
            return Err(McpResponse {
                status: 403,
                headers: vec![],
                body: serde_json::json!({"error": "invalid or revoked session token"}),
            });
        }

        Ok(())
    }

    async fn handle_initialize(&self) -> Result<serde_json::Value, JsonRpcError> {
        Ok(serde_json::json!({
            "protocolVersion": "2025-06-18",
            "capabilities": {
                "tools": {}
            },
            "serverInfo": {
                "name": self.server_name,
                "version": env!("CARGO_PKG_VERSION"),
            }
        }))
    }

    async fn handle_tools_list(&self) -> Result<serde_json::Value, JsonRpcError> {
        let tools = self.tools.read().await;
        let tool_defs: Vec<&ToolDef> = tools.iter().map(|(def, _)| def).collect();
        Ok(serde_json::json!({ "tools": tool_defs }))
    }

    async fn handle_tools_call(
        &self,
        params: serde_json::Value,
    ) -> Result<serde_json::Value, JsonRpcError> {
        let name = params
            .get("name")
            .and_then(|n| n.as_str())
            .unwrap_or("")
            .to_string();
        let arguments = params
            .get("arguments")
            .cloned()
            .unwrap_or(serde_json::json!({}));

        // Find the tool handler.
        let tools = self.tools.read().await;
        let handler = tools.iter().find(|(def, _)| def.name == name);

        let Some((_, handler)) = handler else {
            return Err(JsonRpcError {
                code: -32602,
                message: format!("unknown tool: {name}"),
            });
        };

        // Clone the handler to release the read lock before calling.
        let handler = handler.clone();
        drop(tools);

        match handler(arguments).await {
            Ok(value) => {
                let text = if value.is_string() {
                    value.as_str().unwrap().to_string()
                } else {
                    serde_json::to_string_pretty(&value).unwrap_or_else(|_| value.to_string())
                };
                Ok(serde_json::json!({
                    "content": [{
                        "type": "text",
                        "text": text,
                    }]
                }))
            }
            Err(e) => Ok(serde_json::json!({
                "content": [{
                    "type": "text",
                    "text": format!("ERROR: {e}"),
                }],
                "isError": true,
            })),
        }
    }
}

impl Default for McpToolServer {
    fn default() -> Self {
        Self::new()
    }
}
