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
        let req = self.apply_auth(
            self.http
                .post(self.url(&format!("/v1/sessions/{session_id}")))
                .json(request),
        );
        let resp = req.send().await.map_err(Error::backend_reqwest)?;
        if !resp.status().is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(Error::BackendMessage(format!(
                "sandbox-agent create_session failed: {body}"
            )));
        }
        resp.json::<CreateSessionResponse>()
            .await
            .map_err(Error::backend_reqwest)
    }

    /// Send a message to an existing session.
    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn send_message(&self, session_id: &str, message: &str) -> Result<()> {
        let req = self.apply_auth(
            self.http
                .post(self.url(&format!("/v1/sessions/{session_id}/messages")))
                .json(&MessageRequest {
                    message: message.to_string(),
                }),
        );
        let resp = req.send().await.map_err(Error::backend_reqwest)?;
        if !resp.status().is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(Error::BackendMessage(format!(
                "sandbox-agent send_message failed: {body}"
            )));
        }
        Ok(())
    }

    /// Poll for events from a session.
    ///
    /// `offset` is the last seen event sequence (exclusive); pass 0 to get all events.
    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn fetch_events(&self, session_id: &str, offset: u64) -> Result<EventsResponse> {
        let url = format!("/v1/sessions/{session_id}/events?offset={offset}&include_raw=false");
        let req = self.apply_auth(self.http.get(self.url(&url)));
        let resp = req.send().await.map_err(Error::backend_reqwest)?;
        if !resp.status().is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(Error::BackendMessage(format!(
                "sandbox-agent fetch_events failed: {body}"
            )));
        }
        resp.json::<EventsResponse>()
            .await
            .map_err(Error::backend_reqwest)
    }

    /// Reply to a permission request.
    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn reply_permission(
        &self,
        session_id: &str,
        permission_id: &str,
        reply: &str,
    ) -> Result<()> {
        let req = self.apply_auth(
            self.http
                .post(self.url(&format!(
                    "/v1/sessions/{session_id}/permissions/{permission_id}/reply"
                )))
                .json(&PermissionReplyRequest {
                    reply: reply.to_string(),
                }),
        );
        let resp = req.send().await.map_err(Error::backend_reqwest)?;
        if !resp.status().is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(Error::BackendMessage(format!(
                "sandbox-agent reply_permission failed: {body}"
            )));
        }
        Ok(())
    }

    /// Terminate a session.
    #[tracing::instrument(level = "info", skip(self))]
    pub async fn terminate_session(&self, session_id: &str) -> Result<()> {
        let req = self.apply_auth(
            self.http
                .post(self.url(&format!("/v1/sessions/{session_id}/terminate"))),
        );
        let resp = req.send().await.map_err(Error::backend_reqwest)?;
        if !resp.status().is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(Error::BackendMessage(format!(
                "sandbox-agent terminate_session failed: {body}"
            )));
        }
        Ok(())
    }

    /// List active sessions.
    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn list_sessions(&self) -> Result<Vec<SessionInfo>> {
        let req = self.apply_auth(self.http.get(self.url("/v1/sessions")));
        let resp = req.send().await.map_err(Error::backend_reqwest)?;
        if !resp.status().is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(Error::BackendMessage(format!(
                "sandbox-agent list_sessions failed: {body}"
            )));
        }
        let list: SessionListResponse = resp.json().await.map_err(Error::backend_reqwest)?;
        Ok(list.sessions)
    }
}
