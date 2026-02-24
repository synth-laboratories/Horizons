//! HTTP client for the sandbox-agent API (https://github.com/rivet-dev/sandbox-agent).
//!
//! sandbox-agent runs inside provisioned containers and exposes an HTTP API on
//! port 2468 for managing coding agent sessions (Codex, Claude Code, OpenCode).
//! This module provides a typed Rust client that drives the session lifecycle:
//! create → send message → poll events → handle permissions → terminate.
//!
//! Event types are re-exported from the `sandbox_agent_universal_agent_schema`
//! crate for type-safe deserialization.

use crate::{Error, Result};
use futures_util::StreamExt;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::OnceLock;
use std::time::Duration;
use tokio::sync::Mutex;

// Re-export universal event types from the sandbox-agent crate so downstream
// code (sandbox_runtime, sandboxed_agent) can use them without a direct dep.
pub use sandbox_agent_universal_agent_schema::{
    EventSource, PermissionEventData, PermissionStatus, SessionEndReason, SessionEndedData,
    UniversalEvent, UniversalEventData, UniversalEventType,
};

// ---------------------------------------------------------------------------
// Request / response types for the sandbox-agent HTTP API
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateSessionRequest {
    pub agent: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub agent_mode: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub permission_mode: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateSessionResponse {
    pub healthy: bool,
    pub error: Option<serde_json::Value>,
    pub native_session_id: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct MessageRequest {
    pub message: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EventsResponse {
    pub events: Vec<serde_json::Value>,
    pub has_more: bool,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionInfo {
    pub session_id: String,
    pub agent: String,
    pub ended: bool,
    pub event_count: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SessionListResponse {
    pub sessions: Vec<SessionInfo>,
}

#[derive(Debug, Clone, Serialize)]
pub struct PermissionReplyRequest {
    pub reply: String,
}

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
/// sandbox-agent's base URL.
#[derive(Debug, Clone)]
pub struct SandboxAgentClient {
    base_url: String,
    http: Client,
    auth_token: Option<String>,
}

#[derive(Debug, Clone, Default)]
struct CompatSessionState {
    acp_session_id: Option<String>,
    next_seq: u64,
    queued_events: Vec<serde_json::Value>,
}

fn compat_state() -> &'static Mutex<HashMap<String, CompatSessionState>> {
    static STATE: OnceLock<Mutex<HashMap<String, CompatSessionState>>> = OnceLock::new();
    STATE.get_or_init(|| Mutex::new(HashMap::new()))
}

fn compat_key(base_url: &str, session_id: &str) -> String {
    format!("{base_url}::{session_id}")
}

impl SandboxAgentClient {
    /// Create a new client for the sandbox-agent at `base_url` (e.g. `http://localhost:2468`).
    pub fn new(base_url: impl Into<String>, auth_token: Option<String>) -> Self {
        let http = Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .expect("failed to build reqwest client");
        Self {
            base_url: base_url.into().trim_end_matches('/').to_string(),
            http,
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

    async fn acp_post(
        &self,
        server_id: &str,
        agent: Option<&str>,
        payload: &serde_json::Value,
    ) -> Result<serde_json::Value> {
        let mut url = self.url(&format!("/v1/acp/{server_id}"));
        if let Some(agent) = agent {
            let sep = if url.contains('?') { "&" } else { "?" };
            url.push_str(sep);
            url.push_str("agent=");
            url.push_str(agent);
        }
        let req = self.apply_auth(
            self.http
                .post(url)
                .header("content-type", "application/json")
                .json(payload),
        );
        let resp = req.send().await.map_err(Error::backend_reqwest)?;
        if !resp.status().is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(Error::BackendMessage(format!(
                "sandbox-agent acp post failed: {body}"
            )));
        }
        resp.json::<serde_json::Value>()
            .await
            .map_err(Error::backend_reqwest)
    }

    async fn acp_collect_until_prompt_done(
        &self,
        server_id: &str,
        prompt_id: u64,
    ) -> Result<Vec<serde_json::Value>> {
        let req = self.apply_auth(
            self.http
                .get(self.url(&format!("/v1/acp/{server_id}")))
                .header("accept", "text/event-stream"),
        );
        let resp = req.send().await.map_err(Error::backend_reqwest)?;
        if !resp.status().is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(Error::BackendMessage(format!(
                "sandbox-agent acp stream failed: {body}"
            )));
        }

        let mut stream = resp.bytes_stream();
        let mut buf = String::new();
        let mut out = Vec::new();

        let collect = async {
            while let Some(chunk_res) = stream.next().await {
                let chunk = chunk_res.map_err(Error::backend_reqwest)?;
                buf.push_str(&String::from_utf8_lossy(&chunk));

                while let Some(split) = buf.find("\n\n") {
                    let block = buf[..split].to_string();
                    buf = buf[split + 2..].to_string();

                    for line in block.lines() {
                        let Some(data) = line.strip_prefix("data: ") else {
                            continue;
                        };
                        let envelope: serde_json::Value = match serde_json::from_str(data) {
                            Ok(v) => v,
                            Err(_) => continue,
                        };
                        let done = envelope.get("id").and_then(|v| v.as_u64()) == Some(prompt_id)
                            && (envelope.get("result").is_some()
                                || envelope.get("error").is_some());
                        out.push(envelope);
                        if done {
                            return Ok::<(), Error>(());
                        }
                    }
                }
            }
            Ok::<(), Error>(())
        };

        tokio::time::timeout(Duration::from_secs(60), collect)
            .await
            .map_err(|_| {
                Error::BackendMessage("timeout waiting for ACP prompt result".to_string())
            })??;

        Ok(out)
    }

    // -----------------------------------------------------------------------
    // Health
    // -----------------------------------------------------------------------

    /// Check if the sandbox-agent server is healthy.
    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn health(&self) -> Result<bool> {
        let req = self.apply_auth(self.http.get(self.url("/v1/health")));
        let resp = req.send().await.map_err(Error::backend_reqwest)?;
        Ok(resp.status().is_success())
    }

    // -----------------------------------------------------------------------
    // Agent management
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
    // Session lifecycle
    // -----------------------------------------------------------------------

    /// Create a new agent session.
    #[tracing::instrument(level = "info", skip(self))]
    pub async fn create_session(
        &self,
        session_id: &str,
        request: &CreateSessionRequest,
    ) -> Result<CreateSessionResponse> {
        let init_payload = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2025-06-18",
                "clientCapabilities": {},
                "clientInfo": { "name": "horizons", "version": "0.1" }
            }
        });
        let init_resp = self
            .acp_post(session_id, Some(&request.agent), &init_payload)
            .await?;
        if init_resp.get("error").is_some() {
            return Err(Error::BackendMessage(format!(
                "sandbox-agent initialize failed: {init_resp}"
            )));
        }

        let new_payload = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 2,
            "method": "session/new",
            "params": {
                "cwd": "/",
                "mcpServers": []
            }
        });
        let new_resp = self.acp_post(session_id, None, &new_payload).await?;
        if let Some(err) = new_resp.get("error") {
            return Err(Error::BackendMessage(format!(
                "sandbox-agent session/new failed: {err}"
            )));
        }
        let acp_session_id = new_resp
            .get("result")
            .and_then(|r| r.get("sessionId"))
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .ok_or_else(|| {
                Error::BackendMessage("sandbox-agent session/new missing sessionId".to_string())
            })?;

        if let Some(mode) = request.permission_mode.as_deref() {
            let mode_id = if mode == "bypass" {
                "full-access"
            } else {
                "read-only"
            };
            let set_mode = serde_json::json!({
                "jsonrpc": "2.0",
                "id": 20,
                "method": "session/set_mode",
                "params": { "sessionId": acp_session_id, "modeId": mode_id }
            });
            let _ = self.acp_post(session_id, None, &set_mode).await;
        }
        if let Some(model) = request.model.as_deref() {
            let set_model = serde_json::json!({
                "jsonrpc": "2.0",
                "id": 21,
                "method": "session/set_model",
                "params": { "sessionId": acp_session_id, "modelId": model }
            });
            let _ = self.acp_post(session_id, None, &set_model).await;
        }

        let mut state = compat_state().lock().await;
        state.insert(
            compat_key(&self.base_url, session_id),
            CompatSessionState {
                acp_session_id: Some(acp_session_id.clone()),
                next_seq: 1,
                queued_events: Vec::new(),
            },
        );

        Ok(CreateSessionResponse {
            healthy: true,
            error: None,
            native_session_id: Some(acp_session_id),
        })
    }

    /// Send a message to an existing session.
    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn send_message(&self, session_id: &str, message: &str) -> Result<()> {
        let acp_session_id = {
            let state = compat_state().lock().await;
            state
                .get(&compat_key(&self.base_url, session_id))
                .and_then(|s| s.acp_session_id.clone())
                .ok_or_else(|| {
                    Error::BackendMessage(format!("unknown ACP session: {session_id}"))
                })?
        };

        let prompt_id = 3_u64;
        let prompt_payload = serde_json::json!({
            "jsonrpc": "2.0",
            "id": prompt_id,
            "method": "session/prompt",
            "params": {
                "sessionId": acp_session_id,
                "prompt": [{ "type": "text", "text": message }]
            }
        });

        let stream_task = self.acp_collect_until_prompt_done(session_id, prompt_id);
        let post_task = self.acp_post(session_id, None, &prompt_payload);
        let (envelopes, post_resp) = tokio::join!(stream_task, post_task);
        post_resp?;
        let envelopes = envelopes?;

        let mut assistant_text = String::new();
        let mut failed_error: Option<String> = None;
        for env in envelopes {
            if let Some(update) = env
                .get("params")
                .and_then(|v| v.get("update"))
                .and_then(|v| v.as_object())
            {
                if update.get("sessionUpdate").and_then(|v| v.as_str())
                    == Some("agent_message_chunk")
                {
                    if let Some(text) = update
                        .get("content")
                        .and_then(|c| c.get("text"))
                        .and_then(|v| v.as_str())
                    {
                        assistant_text.push_str(text);
                    }
                }
            }
            if env.get("id").and_then(|v| v.as_u64()) == Some(prompt_id) {
                if let Some(err) = env.get("error") {
                    failed_error = Some(err.to_string());
                }
            }
        }

        let mut state = compat_state().lock().await;
        if let Some(s) = state.get_mut(&compat_key(&self.base_url, session_id)) {
            let mut next_seq = s.next_seq.max(1);
            if !assistant_text.is_empty() {
                s.queued_events.push(serde_json::json!({
                    "sequence": next_seq,
                    "type": "item.completed",
                    "data": {
                        "item": {
                            "role": "assistant",
                            "content": [{ "type": "text", "text": assistant_text }]
                        }
                    }
                }));
                next_seq += 1;
            }

            if let Some(err) = failed_error {
                s.queued_events.push(serde_json::json!({
                    "sequence": next_seq,
                    "type": "turn.ended",
                    "data": { "metadata": { "status": "failed", "error": { "message": err } } }
                }));
                next_seq += 1;
            } else {
                s.queued_events.push(serde_json::json!({
                    "sequence": next_seq,
                    "type": "turn.ended",
                    "data": { "metadata": { "status": "completed" } }
                }));
                next_seq += 1;
            }
            s.next_seq = next_seq;
        }

        Ok(())
    }

    /// Poll for events from a session.
    ///
    /// `offset` is the last seen event sequence (exclusive); pass 0 to get all events.
    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn fetch_events(&self, session_id: &str, offset: u64) -> Result<EventsResponse> {
        let state = compat_state().lock().await;
        let events = state
            .get(&compat_key(&self.base_url, session_id))
            .map(|s| {
                s.queued_events
                    .iter()
                    .filter(|e| e.get("sequence").and_then(|v| v.as_u64()).unwrap_or(0) > offset)
                    .cloned()
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        Ok(EventsResponse {
            events,
            has_more: false,
        })
    }

    /// Reply to a permission request.
    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn reply_permission(
        &self,
        _session_id: &str,
        _permission_id: &str,
        _reply: &str,
    ) -> Result<()> {
        // ACP compatibility path currently uses full-access mode for bypass;
        // explicit permission reply is not needed for the internal app flow.
        Ok(())
    }

    /// Terminate a session.
    #[tracing::instrument(level = "info", skip(self))]
    pub async fn terminate_session(&self, session_id: &str) -> Result<()> {
        let _ = self
            .apply_auth(self.http.delete(self.url(&format!("/v1/acp/{session_id}"))))
            .send()
            .await;
        let mut state = compat_state().lock().await;
        state.remove(&compat_key(&self.base_url, session_id));
        Ok(())
    }

    /// List active sessions.
    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn list_sessions(&self) -> Result<Vec<SessionInfo>> {
        let state = compat_state().lock().await;
        let sessions = state
            .iter()
            .filter(|(k, _)| k.starts_with(&self.base_url))
            .map(|(k, sess)| SessionInfo {
                session_id: k
                    .split_once("::")
                    .map(|(_, sid)| sid.to_string())
                    .unwrap_or_else(|| k.clone()),
                agent: "codex".to_string(),
                ended: false,
                event_count: sess.queued_events.len() as u64,
            })
            .collect::<Vec<_>>();
        Ok(sessions)
    }
}
