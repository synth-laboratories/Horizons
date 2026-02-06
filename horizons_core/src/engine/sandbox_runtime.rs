//! Sandbox runtime orchestrator.
//!
//! Coordinates the full lifecycle of running a coding agent in a sandbox:
//! provision container → create session → send message → poll events →
//! handle permissions → detect completion → collect results → release.

use crate::engine::models::{PermissionMode, SandboxConfig, SandboxHandle, SandboxResult};
use crate::engine::sandbox_agent_client::{CreateSessionRequest, SandboxAgentClient};
use crate::engine::traits::SandboxBackend;
use crate::{Error, Result};
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use std::sync::Arc;
use std::time::Duration;

/// Interval between event polling iterations.
const EVENT_POLL_INTERVAL: Duration = Duration::from_millis(500);

/// Snapshot of a long-running sandbox agent run started via `start_agent_tracked`.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ActiveSandboxRun {
    /// Stable run identifier (not the container/sandbox ID).
    pub run_id: String,
    pub org_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<String>,
    pub sandbox: SandboxHandle,
    pub session_id: String,
    pub restart_count: u32,
    pub started_at: DateTime<Utc>,
    pub config: SandboxConfig,
    pub instruction: String,
}

/// The sandbox runtime orchestrates provisioning a container, driving a coding
/// agent session via sandbox-agent, and collecting the results.
///
/// It holds a reference to the sandbox backend (Docker or Daytona) and provides
/// the `run_agent()` method which encapsulates the entire execution flow.
pub struct SandboxRuntime {
    backend: Arc<dyn SandboxBackend>,
    active: DashMap<String, ActiveSandboxRun>,
}

impl SandboxRuntime {
    pub fn new(backend: Arc<dyn SandboxBackend>) -> Self {
        Self {
            backend,
            active: DashMap::new(),
        }
    }

    /// Run a coding agent session in a new sandbox, returning structured results.
    ///
    /// This is the primary entry point for executing agents. It handles:
    /// 1. Provisioning the container via the configured backend
    /// 2. Waiting for sandbox-agent to become healthy
    /// 3. Creating a session for the requested agent
    /// 4. Sending the instruction message
    /// 5. Polling for events and auto-approving permissions (in bypass mode)
    /// 6. Detecting session completion
    /// 7. Collecting results and releasing the container
    #[tracing::instrument(level = "info", skip(self, config, instruction), fields(agent = %config.agent))]
    pub async fn run_agent(
        &self,
        config: &SandboxConfig,
        instruction: &str,
    ) -> Result<(SandboxResult, SandboxHandle)> {
        let start = std::time::Instant::now();

        // 1. Provision the container.
        let handle = self.backend.provision(config).await?;
        tracing::info!(handle_id = %handle.id, "sandbox provisioned");

        // Run the session, ensuring we release on both success and failure.
        let result = self
            .run_session_in_sandbox(&handle, config, instruction)
            .await;

        match result {
            Ok(mut sandbox_result) => {
                sandbox_result.duration_seconds = start.elapsed().as_secs_f64();
                // Release container after successful completion.
                if let Err(e) = self.backend.release(&handle).await {
                    tracing::warn!(%e, "failed to release sandbox after success");
                }
                Ok((sandbox_result, handle))
            }
            Err(e) => {
                // Best-effort cleanup on failure.
                if let Err(release_err) = self.backend.release(&handle).await {
                    tracing::warn!(%release_err, "failed to release sandbox after error");
                }
                Err(e)
            }
        }
    }

    /// Internal: run the agent session inside an already-provisioned sandbox.
    async fn run_session_in_sandbox(
        &self,
        handle: &SandboxHandle,
        config: &SandboxConfig,
        instruction: &str,
    ) -> Result<SandboxResult> {
        // 2. Wait for sandbox-agent to be healthy.
        self.backend.wait_ready(handle).await?;
        tracing::info!("sandbox-agent is ready");

        // 3. Create the sandbox-agent client and session.
        let client = SandboxAgentClient::new(&handle.sandbox_agent_url, None);
        let session_id = uuid::Uuid::new_v4().to_string();

        let create_req = CreateSessionRequest {
            agent: config.agent.as_sandbox_agent_str().to_string(),
            agent_mode: Some("build".to_string()),
            permission_mode: Some(config.permission_mode.as_str().to_string()),
            model: config.model.clone(),
        };

        let create_resp = client.create_session(&session_id, &create_req).await?;
        if !create_resp.healthy {
            return Err(Error::BackendMessage(format!(
                "sandbox-agent session not healthy: {:?}",
                create_resp.error
            )));
        }
        tracing::info!(%session_id, "agent session created");

        // 4. Send the instruction message.
        client.send_message(&session_id, instruction).await?;
        tracing::info!("instruction sent");

        // 5. Poll for events until the session ends or timeout.
        let mut all_events: Vec<serde_json::Value> = Vec::new();
        let mut offset: u64 = 0;
        let mut session_ended = false;
        let mut final_output: Option<String> = None;
        let mut error_msg: Option<String> = None;

        let timeout_deadline =
            tokio::time::Instant::now() + Duration::from_secs(config.timeout_seconds);

        loop {
            if tokio::time::Instant::now() > timeout_deadline {
                tracing::warn!("session timed out, terminating");
                let _ = client.terminate_session(&session_id).await;
                error_msg = Some("session timed out".to_string());
                break;
            }

            let events_resp = client.fetch_events(&session_id, offset).await?;

            for event_json in &events_resp.events {
                // Track the maximum sequence number for offset.
                if let Some(seq) = event_json.get("sequence").and_then(|v| v.as_u64()) {
                    if seq > offset {
                        offset = seq;
                    }
                }

                // Parse the event type to detect session end and permission requests.
                let event_type = event_json
                    .get("type")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");

                match event_type {
                    "session.ended" => {
                        session_ended = true;

                        // Extract end reason.
                        if let Some(data) = event_json.get("data") {
                            if let Some(reason) = data.get("reason").and_then(|r| r.as_str()) {
                                if reason == "error" {
                                    error_msg = data
                                        .get("message")
                                        .and_then(|m| m.as_str())
                                        .map(|s| s.to_string());
                                }
                            }
                        }
                    }
                    "permission.requested" => {
                        // Auto-approve permissions in bypass mode.
                        if config.permission_mode == PermissionMode::Bypass {
                            if let Some(data) = event_json.get("data") {
                                if let Some(pid) =
                                    data.get("permission_id").and_then(|v| v.as_str())
                                {
                                    tracing::debug!(%pid, "auto-approving permission");
                                    let _ =
                                        client.reply_permission(&session_id, pid, "always").await;
                                }
                            }
                        }
                    }
                    "item.completed" => {
                        // Try to extract final text output from the last completed item.
                        if let Some(data) = event_json.get("data") {
                            if let Some(item) = data.get("item") {
                                if let Some(content) =
                                    item.get("content").and_then(|c| c.as_array())
                                {
                                    for part in content {
                                        if part.get("type").and_then(|t| t.as_str()) == Some("text")
                                        {
                                            if let Some(text) =
                                                part.get("text").and_then(|t| t.as_str())
                                            {
                                                final_output = Some(text.to_string());
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                    _ => {}
                }

                all_events.push(event_json.clone());
            }

            if session_ended {
                tracing::info!("session ended, collected {} events", all_events.len());
                break;
            }

            tokio::time::sleep(EVENT_POLL_INTERVAL).await;
        }

        Ok(SandboxResult {
            events: all_events,
            completed: session_ended && error_msg.is_none(),
            duration_seconds: 0.0, // Caller fills this in.
            final_output,
            error: error_msg,
        })
    }

    /// Run an agent without waiting for completion — returns the handle and client
    /// for external event polling. Useful for SSE streaming.
    #[tracing::instrument(level = "info", skip(self, config, instruction), fields(agent = %config.agent))]
    pub async fn start_agent(
        &self,
        config: &SandboxConfig,
        instruction: &str,
    ) -> Result<(SandboxHandle, String)> {
        let handle = self.backend.provision(config).await?;
        self.backend.wait_ready(&handle).await?;

        let client = SandboxAgentClient::new(&handle.sandbox_agent_url, None);
        let session_id = uuid::Uuid::new_v4().to_string();

        let create_req = CreateSessionRequest {
            agent: config.agent.as_sandbox_agent_str().to_string(),
            agent_mode: Some("build".to_string()),
            permission_mode: Some(config.permission_mode.as_str().to_string()),
            model: config.model.clone(),
        };

        let create_resp = client.create_session(&session_id, &create_req).await?;
        if !create_resp.healthy {
            let _ = self.backend.release(&handle).await;
            return Err(Error::BackendMessage(format!(
                "sandbox-agent session not healthy: {:?}",
                create_resp.error
            )));
        }

        client.send_message(&session_id, instruction).await?;

        Ok((handle, session_id))
    }

    /// Start an agent session and track it in-memory for monitoring/restart.
    ///
    /// Returns a stable `run_id` which can be used for subsequent health checks,
    /// SSE polling, release, and crash recovery.
    #[tracing::instrument(level = "info", skip(self, config, instruction), fields(agent = %config.agent))]
    pub async fn start_agent_tracked(
        &self,
        org_id: String,
        agent_id: Option<String>,
        config: &SandboxConfig,
        instruction: &str,
    ) -> Result<ActiveSandboxRun> {
        let (handle, session_id) = self.start_agent(config, instruction).await?;
        let run_id = ulid::Ulid::new().to_string();
        let run = ActiveSandboxRun {
            run_id: run_id.clone(),
            org_id,
            agent_id,
            sandbox: handle,
            session_id,
            restart_count: 0,
            started_at: Utc::now(),
            config: config.clone(),
            instruction: instruction.to_string(),
        };
        self.active.insert(run_id, run.clone());
        Ok(run)
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub fn list_active(&self) -> Vec<ActiveSandboxRun> {
        self.active.iter().map(|e| e.value().clone()).collect()
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub fn get_active(&self, run_id: &str) -> Option<ActiveSandboxRun> {
        self.active.get(run_id).map(|e| e.value().clone())
    }

    /// Release a tracked run (best-effort) and remove it from the active set.
    #[tracing::instrument(level = "info", skip_all)]
    pub async fn release_run(&self, run_id: &str) -> Result<()> {
        let run = self
            .active
            .remove(run_id)
            .map(|(_, v)| v)
            .ok_or_else(|| Error::NotFound(format!("sandbox run not found: {run_id}")))?;
        self.backend.release(&run.sandbox).await
    }

    /// Restart a tracked run by provisioning a new sandbox and replaying the original instruction.
    ///
    /// This does not attempt checkpoint/resume; it is a best-effort "restart from scratch" policy
    /// intended for dev ergonomics and crash cleanup.
    #[tracing::instrument(level = "info", skip_all)]
    pub async fn restart_run(&self, run_id: &str) -> Result<ActiveSandboxRun> {
        let Some(mut run) = self.get_active(run_id) else {
            return Err(Error::NotFound(format!("sandbox run not found: {run_id}")));
        };

        let (new_handle, new_session_id) = self.start_agent(&run.config, &run.instruction).await?;

        // Release the old sandbox best-effort.
        let _ = self.backend.release(&run.sandbox).await;

        run.sandbox = new_handle;
        run.session_id = new_session_id;
        run.restart_count = run.restart_count.saturating_add(1);
        run.started_at = Utc::now();
        self.active.insert(run_id.to_string(), run.clone());
        Ok(run)
    }

    /// Release a sandbox by its handle.
    #[tracing::instrument(level = "info", skip_all)]
    pub async fn release(&self, handle: &SandboxHandle) -> Result<()> {
        self.backend.release(handle).await
    }

    /// Get a client for an existing sandbox handle.
    pub fn client_for(&self, handle: &SandboxHandle) -> SandboxAgentClient {
        SandboxAgentClient::new(&handle.sandbox_agent_url, None)
    }
}
