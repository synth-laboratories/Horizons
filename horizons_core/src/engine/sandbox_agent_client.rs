//! HTTP client for the sandbox-agent REST API.
//!
//! sandbox-agent (v0.2.0+) runs inside provisioned containers and exposes a
//! REST API on port 2468 for managing coding agent sessions
//! (Codex, Claude Code, OpenCode, Amp).
//!
//! Protocol flow:
//!   POST /v1/sessions/{session_id}                            → create session
//!   POST /v1/sessions/{session_id}/messages                   → send message (fire-and-forget)
//!   GET  /v1/sessions/{session_id}/events/sse                 → universal event stream
//!   POST /v1/sessions/{session_id}/permissions/{id}/reply     → approve/deny permission
//!   POST /v1/sessions/{session_id}/terminate                  → terminate session
//!
//! Event types are re-exported from the `sandbox_agent_universal_agent_schema`
//! crate for type-safe deserialization.

use crate::{Error, Result};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::time::Duration;

// Re-export universal event types from the sandbox-agent crate so downstream
// code (sandbox_runtime, sandboxed_agent) can use them without a direct dep.
pub use sandbox_agent_universal_agent_schema::{
    EventSource, PermissionEventData, PermissionStatus, SessionEndReason, SessionEndedData,
    UniversalEvent, UniversalEventData, UniversalEventType,
};

// ---------------------------------------------------------------------------
// ACP JSON-RPC types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize)]
pub struct JsonRpcRequest {
    pub jsonrpc: &'static str,
    pub method: String,
    pub id: serde_json::Value,
    pub params: serde_json::Value,
}

impl JsonRpcRequest {
    pub fn new(method: &str, id: impl Into<serde_json::Value>, params: serde_json::Value) -> Self {
        Self {
            jsonrpc: "2.0",
            method: method.to_string(),
            id: id.into(),
            params,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct JsonRpcResponse {
    pub jsonrpc: String,
    pub id: Option<serde_json::Value>,
    pub result: Option<serde_json::Value>,
    pub error: Option<JsonRpcError>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonRpcError {
    pub code: i64,
    pub message: String,
    pub data: Option<serde_json::Value>,
}

/// A JSON-RPC message received on the SSE stream.
/// Can be a notification (no id), a server request (has id+method), or a response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonRpcMessage {
    pub jsonrpc: Option<String>,
    pub id: Option<serde_json::Value>,
    pub method: Option<String>,
    pub params: Option<serde_json::Value>,
    pub result: Option<serde_json::Value>,
    pub error: Option<JsonRpcError>,
}

// ---------------------------------------------------------------------------
// ACP response types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AcpInitResult {
    pub protocol_version: serde_json::Value,
    pub agent_info: Option<serde_json::Value>,
    pub agent_capabilities: Option<serde_json::Value>,
    pub auth_methods: Option<Vec<AcpAuthMethod>>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AcpAuthMethod {
    pub id: String,
    pub name: String,
    pub description: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AcpSessionNewResult {
    pub session_id: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AcpPromptResult {
    #[serde(default)]
    pub stop_reason: Option<String>,
    #[serde(flatten)]
    pub extra: serde_json::Value,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AcpServerInfo {
    pub server_id: String,
    pub agent: String,
    pub created_at_ms: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AcpServerListResponse {
    pub servers: Vec<AcpServerInfo>,
}

// ---------------------------------------------------------------------------
// REST API response types
// ---------------------------------------------------------------------------

/// Response from `POST /v1/sessions/{session_id}`.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateSessionResponse {
    pub healthy: bool,
    #[serde(default)]
    pub native_session_id: Option<String>,
    #[serde(default)]
    pub error: Option<serde_json::Value>,
}

// ---------------------------------------------------------------------------
// Session list types (for REST fallback health checks)
// ---------------------------------------------------------------------------

/// A single session entry from `GET /v1/sessions`.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionListEntry {
    pub session_id: String,
    #[serde(default)]
    pub ended: bool,
}

/// Response from `GET /v1/sessions`.
#[derive(Debug, Clone, Deserialize)]
pub struct SessionListResponse {
    pub sessions: Vec<SessionListEntry>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionLiveness {
    Active,
    Ended,
    Missing,
}

// ---------------------------------------------------------------------------
// Legacy types (kept for backward compat with existing code)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Deserialize)]
pub struct HealthResponse {
    pub status: String,
}

// ---------------------------------------------------------------------------
// Client
// ---------------------------------------------------------------------------

/// HTTP client for a single sandbox-agent instance.
///
/// Each provisioned sandbox gets its own `SandboxAgentClient` pointed at the
/// sandbox-agent's base URL. Uses ACP (Agent Client Protocol) JSON-RPC for
/// session management.
#[derive(Debug, Clone)]
pub struct SandboxAgentClient {
    base_url: String,
    http: Client,
    /// Long-timeout client for prompt requests that may block for minutes.
    http_long: Client,
    auth_token: Option<String>,
}

impl SandboxAgentClient {
    /// Create a new client for the sandbox-agent at `base_url` (e.g. `http://localhost:2468`).
    pub fn new(base_url: impl Into<String>, auth_token: Option<String>) -> Self {
        let http = Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .expect("failed to build reqwest client");
        let http_long = Client::builder()
            .timeout(Duration::from_secs(600))
            .build()
            .expect("failed to build long-timeout reqwest client");
        Self {
            base_url: base_url.into().trim_end_matches('/').to_string(),
            http,
            http_long,
            auth_token,
        }
    }

    fn url(&self, path: &str) -> String {
        format!("{}{}", self.base_url, path)
    }

    fn apply_auth(&self, req: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        if let Some(token) = &self.auth_token {
            req.bearer_auth(token)
        } else {
            req
        }
    }

    // -----------------------------------------------------------------------
    // Health (unchanged — REST endpoint still works)
    // -----------------------------------------------------------------------

    /// Check if the sandbox-agent server is healthy.
    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn health(&self) -> Result<bool> {
        let req = self.apply_auth(self.http.get(self.url("/v1/health")));
        let resp = req.send().await.map_err(Error::backend_reqwest)?;
        Ok(resp.status().is_success())
    }

    // -----------------------------------------------------------------------
    // Agent management (unchanged — REST endpoint still works)
    // -----------------------------------------------------------------------

    /// Install a coding agent inside the sandbox.
    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn install_agent(&self, agent: &str) -> Result<()> {
        let req = self.apply_auth(
            self.http
                .post(self.url(&format!("/v1/agents/{agent}/install")))
                .json(&serde_json::json!({ "reinstall": false })),
        );
        let resp = req.send().await.map_err(Error::backend_reqwest)?;
        if !resp.status().is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(Error::BackendMessage(format!(
                "sandbox-agent install_agent({agent}) failed: {body}"
            )));
        }
        Ok(())
    }

    // -----------------------------------------------------------------------
    // ACP: JSON-RPC helpers
    // -----------------------------------------------------------------------

    /// Send a JSON-RPC request to the ACP endpoint and parse the response.
    async fn acp_rpc(
        &self,
        server_id: &str,
        query: Option<&str>,
        method: &str,
        id: u64,
        params: serde_json::Value,
        long_timeout: bool,
    ) -> Result<serde_json::Value> {
        let path = match query {
            Some(q) => format!("/v1/acp/{server_id}?{q}"),
            None => format!("/v1/acp/{server_id}"),
        };
        let body = JsonRpcRequest::new(method, serde_json::Value::from(id), params);
        let client = if long_timeout {
            &self.http_long
        } else {
            &self.http
        };
        let req = self.apply_auth(client.post(self.url(&path)).json(&body));
        let resp = req.send().await.map_err(Error::backend_reqwest)?;

        if !resp.status().is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(Error::BackendMessage(format!(
                "ACP {method} failed (HTTP): {body}"
            )));
        }

        let rpc_resp: JsonRpcResponse = resp.json().await.map_err(Error::backend_reqwest)?;
        if let Some(err) = rpc_resp.error {
            return Err(Error::BackendMessage(format!(
                "ACP {method} failed: {} (code {})",
                err.message, err.code
            )));
        }
        Ok(rpc_resp.result.unwrap_or(serde_json::Value::Null))
    }

    // -----------------------------------------------------------------------
    // ACP: Session lifecycle
    // -----------------------------------------------------------------------

    /// Initialize an ACP server for the given agent.
    ///
    /// This must be the first call for a `server_id`. The `agent` query param
    /// tells sandbox-agent which coding agent to use (codex, claude, opencode).
    #[tracing::instrument(level = "info", skip(self))]
    pub async fn acp_initialize(&self, server_id: &str, agent: &str) -> Result<AcpInitResult> {
        let params = serde_json::json!({
            "protocolVersion": "1",
            "clientInfo": {
                "name": "horizons",
                "version": env!("CARGO_PKG_VERSION"),
            }
        });
        let query = format!("agent={agent}");
        let result = self
            .acp_rpc(server_id, Some(&query), "initialize", 1, params, false)
            .await?;
        serde_json::from_value(result.clone()).map_err(|e| {
            Error::BackendMessage(format!("ACP initialize: bad response: {e}: {result}"))
        })
    }

    /// Create a new session inside an ACP server.
    ///
    /// Accepts optional config overrides (e.g. `[{"id":"mode","value":"full-access"}]`)
    /// that are forwarded to the agent so it starts in the desired permission/model mode.
    #[tracing::instrument(level = "info", skip(self, config_overrides))]
    pub async fn acp_session_new(
        &self,
        server_id: &str,
        cwd: &str,
        config_overrides: Option<Vec<serde_json::Value>>,
    ) -> Result<AcpSessionNewResult> {
        let mut params = serde_json::json!({
            "cwd": cwd,
            "mcpServers": [],
        });
        if let Some(overrides) = config_overrides {
            params["configOverrides"] = serde_json::Value::Array(overrides);
        }
        let result = self
            .acp_rpc(server_id, None, "session/new", 2, params, false)
            .await?;
        serde_json::from_value(result.clone()).map_err(|e| {
            Error::BackendMessage(format!("ACP session/new: bad response: {e}: {result}"))
        })
    }

    /// Send a prompt to an ACP session.
    ///
    /// **Blocks** until the agent finishes its turn. For long-running prompts,
    /// use a background task for SSE event reading (permissions, progress).
    #[tracing::instrument(level = "info", skip(self, prompt))]
    pub async fn acp_session_prompt(
        &self,
        server_id: &str,
        session_id: &str,
        prompt: &str,
    ) -> Result<AcpPromptResult> {
        let params = serde_json::json!({
            "sessionId": session_id,
            "prompt": [
                { "type": "text", "text": prompt }
            ],
        });
        let result = self
            .acp_rpc(server_id, None, "session/prompt", 3, params, true)
            .await?;
        serde_json::from_value(result.clone()).map_err(|e| {
            Error::BackendMessage(format!("ACP session/prompt: bad response: {e}: {result}"))
        })
    }

    /// Respond to an ACP server-initiated request (e.g. permission approval).
    ///
    /// Sends a JSON-RPC response with the given `id` and `result`.
    #[tracing::instrument(level = "debug", skip(self, result))]
    pub async fn acp_respond(
        &self,
        server_id: &str,
        id: serde_json::Value,
        result: serde_json::Value,
    ) -> Result<()> {
        let path = format!("/v1/acp/{server_id}");
        let body = serde_json::json!({
            "jsonrpc": "2.0",
            "id": id,
            "result": result,
        });
        let req = self.apply_auth(self.http.post(self.url(&path)).json(&body));
        let resp = req.send().await.map_err(Error::backend_reqwest)?;
        if !resp.status().is_success() {
            let text = resp.text().await.unwrap_or_default();
            tracing::warn!(%text, "ACP respond returned non-success");
        }
        Ok(())
    }

    /// Connect to the ACP SSE event stream.
    ///
    /// Returns the raw reqwest Response configured for streaming. Caller is
    /// responsible for reading and parsing SSE frames.
    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn acp_sse_connect(&self, server_id: &str) -> Result<reqwest::Response> {
        let path = format!("/v1/acp/{server_id}");
        let req = self.apply_auth(
            self.http_long
                .get(self.url(&path))
                .header("Accept", "text/event-stream"),
        );
        let resp = req.send().await.map_err(Error::backend_reqwest)?;
        if !resp.status().is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(Error::BackendMessage(format!(
                "ACP SSE connect failed: {body}"
            )));
        }
        Ok(resp)
    }

    /// Close an ACP server, terminating all sessions.
    #[tracing::instrument(level = "info", skip(self))]
    pub async fn acp_close(&self, server_id: &str) -> Result<()> {
        let path = format!("/v1/acp/{server_id}");
        let req = self.apply_auth(self.http.delete(self.url(&path)));
        let resp = req.send().await.map_err(Error::backend_reqwest)?;
        if !resp.status().is_success() {
            let body = resp.text().await.unwrap_or_default();
            tracing::warn!(%body, "ACP close returned non-success (server may already be gone)");
        }
        Ok(())
    }

    /// List active ACP servers.
    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn acp_list_servers(&self) -> Result<Vec<AcpServerInfo>> {
        let req = self.apply_auth(self.http.get(self.url("/v1/acp")));
        let resp = req.send().await.map_err(Error::backend_reqwest)?;
        if !resp.status().is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(Error::BackendMessage(format!(
                "ACP list servers failed: {body}"
            )));
        }
        let list: AcpServerListResponse = resp.json().await.map_err(Error::backend_reqwest)?;
        Ok(list.servers)
    }

    // -----------------------------------------------------------------------
    // REST API: Session lifecycle
    // -----------------------------------------------------------------------

    /// Create a new session via the REST API.
    ///
    /// `POST /v1/sessions/{session_id}` with the agent type, optional model,
    /// and optional permission mode.
    #[tracing::instrument(level = "info", skip(self))]
    pub async fn create_session(
        &self,
        session_id: &str,
        agent: &str,
        model: Option<&str>,
        permission_mode: Option<&str>,
    ) -> Result<CreateSessionResponse> {
        let mut body = serde_json::json!({
            "agent": agent,
        });
        if let Some(m) = model {
            body["model"] = serde_json::Value::String(m.to_string());
        }
        if let Some(pm) = permission_mode {
            body["permissionMode"] = serde_json::Value::String(pm.to_string());
        }
        let path = format!("/v1/sessions/{session_id}");
        let req = self.apply_auth(self.http.post(self.url(&path)).json(&body));
        let resp = req.send().await.map_err(Error::backend_reqwest)?;
        if !resp.status().is_success() {
            let text = resp.text().await.unwrap_or_default();
            return Err(Error::BackendMessage(format!(
                "create_session failed: {text}"
            )));
        }
        resp.json()
            .await
            .map_err(|e| Error::BackendMessage(format!("create_session: bad response: {e}")))
    }

    /// Send a message to a session (fire-and-forget).
    ///
    /// `POST /v1/sessions/{session_id}/messages` — returns 204 immediately.
    /// The agent processes the message asynchronously; events arrive on the SSE stream.
    #[tracing::instrument(level = "info", skip(self, message))]
    pub async fn post_message(&self, session_id: &str, message: &str) -> Result<()> {
        let path = format!("/v1/sessions/{session_id}/messages");
        let body = serde_json::json!({ "message": message });
        let req = self.apply_auth(self.http_long.post(self.url(&path)).json(&body));
        let resp = req.send().await.map_err(Error::backend_reqwest)?;
        if !resp.status().is_success() {
            let text = resp.text().await.unwrap_or_default();
            return Err(Error::BackendMessage(format!(
                "post_message failed: {text}"
            )));
        }
        Ok(())
    }

    /// Connect to the universal events SSE stream.
    ///
    /// `GET /v1/sessions/{session_id}/events/sse` — returns a continuous SSE stream
    /// of `UniversalEvent` objects (not JSON-RPC). The stream stays open until the
    /// session ends or the connection drops.
    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn events_sse_connect(&self, session_id: &str) -> Result<reqwest::Response> {
        let path = format!("/v1/sessions/{session_id}/events/sse");
        let req = self.apply_auth(
            self.http_long
                .get(self.url(&path))
                .header("Accept", "text/event-stream"),
        );
        let resp = req.send().await.map_err(Error::backend_reqwest)?;
        if !resp.status().is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(Error::BackendMessage(format!(
                "events SSE connect failed: {body}"
            )));
        }
        Ok(resp)
    }

    /// Reply to a permission request via the REST API.
    ///
    /// `POST /v1/sessions/{session_id}/permissions/{permission_id}/reply`
    /// with `{"reply": "once"|"always"|"reject"}`.
    #[tracing::instrument(level = "info", skip(self))]
    pub async fn permission_reply(
        &self,
        session_id: &str,
        permission_id: &str,
        reply: &str,
    ) -> Result<()> {
        let path = format!("/v1/sessions/{session_id}/permissions/{permission_id}/reply");
        let body = serde_json::json!({ "reply": reply });
        let req = self.apply_auth(self.http.post(self.url(&path)).json(&body));
        let resp = req.send().await.map_err(Error::backend_reqwest)?;
        if !resp.status().is_success() {
            let text = resp.text().await.unwrap_or_default();
            tracing::warn!(%text, %permission_id, "permission_reply returned non-success");
            return Err(Error::BackendMessage(format!(
                "permission_reply failed: {text}"
            )));
        }
        Ok(())
    }

    /// Terminate a session.
    ///
    /// `POST /v1/sessions/{session_id}/terminate`
    #[tracing::instrument(level = "info", skip(self))]
    pub async fn terminate_session(&self, session_id: &str) -> Result<()> {
        let path = format!("/v1/sessions/{session_id}/terminate");
        let req = self.apply_auth(self.http.post(self.url(&path)));
        let resp = req.send().await.map_err(Error::backend_reqwest)?;
        if !resp.status().is_success() {
            let text = resp.text().await.unwrap_or_default();
            tracing::warn!(%text, "terminate_session returned non-success");
        }
        Ok(())
    }

    /// Check whether a session has ended via the REST API.
    ///
    /// `GET /v1/sessions` → find session by ID.
    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn session_liveness(&self, session_id: &str) -> Result<SessionLiveness> {
        let req = self.apply_auth(self.http.get(self.url("/v1/sessions")));
        let resp = req.send().await.map_err(Error::backend_reqwest)?;
        if !resp.status().is_success() {
            let text = resp.text().await.unwrap_or_default();
            return Err(Error::BackendMessage(format!(
                "GET /v1/sessions failed: {text}"
            )));
        }
        let list: SessionListResponse = resp
            .json()
            .await
            .map_err(|e| Error::BackendMessage(format!("GET /v1/sessions: bad response: {e}")))?;
        Ok(match list
            .sessions
            .iter()
            .find(|s| s.session_id == session_id)
            .map(|s| s.ended)
        {
            Some(true) => SessionLiveness::Ended,
            Some(false) => SessionLiveness::Active,
            None => SessionLiveness::Missing,
        })
    }

    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn is_session_ended(&self, session_id: &str) -> Result<bool> {
        Ok(matches!(
            self.session_liveness(session_id).await?,
            SessionLiveness::Ended
        ))
    }
}

// ---------------------------------------------------------------------------
// SSE frame parser
// ---------------------------------------------------------------------------

/// Parse a single SSE `data:` payload into a JSON-RPC message.
pub fn parse_sse_json_rpc(data: &str) -> Option<JsonRpcMessage> {
    serde_json::from_str(data).ok()
}
