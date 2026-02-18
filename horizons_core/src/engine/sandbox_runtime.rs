//! Sandbox runtime orchestrator.
//!
//! Coordinates the full lifecycle of running a coding agent in a sandbox:
//! provision container → create session → SSE event stream →
//! post message → collect results → release.

use crate::engine::models::{PermissionMode, SandboxConfig, SandboxHandle, SandboxResult};
use crate::engine::sandbox_agent_client::SandboxAgentClient;
use crate::engine::traits::SandboxBackend;
use crate::engine::victoria_logs::{VictoriaLogsEmitter, VictoriaSessionCtx};
use crate::{Error, Result};
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use futures_util::StreamExt;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

/// Channels returned by `start_agent()` for monitoring a fire-and-forget session.
///
/// The caller receives the SSE event stream and a one-shot completion signal.
pub struct StartAgentChannels {
    pub events_rx: tokio::sync::mpsc::Receiver<serde_json::Value>,
    pub completion_rx: tokio::sync::oneshot::Receiver<bool>,
    pub sse_task: tokio::task::JoinHandle<()>,
}

/// Snapshot of a long-running sandbox agent run started via `start_agent_tracked`.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ActiveSandboxRun {
    /// Stable run identifier (not the container/sandbox ID).
    pub run_id: String,
    pub org_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<String>,
    pub sandbox: SandboxHandle,
    /// The session ID used for REST API calls.
    pub session_id: String,
    pub restart_count: u32,
    pub started_at: DateTime<Utc>,
    pub config: SandboxConfig,
    pub instruction: String,
    /// Epoch millis of the last SSE event received (0 = no events yet).
    #[serde(skip)]
    pub last_sse_epoch_ms: Arc<AtomicI64>,
}

/// The sandbox runtime orchestrates provisioning a container, driving a coding
/// agent session via sandbox-agent's REST API, and collecting the results.
///
/// It holds a reference to the sandbox backend (Docker or Daytona) and provides
/// the `run_agent()` method which encapsulates the entire execution flow.
pub struct SandboxRuntime {
    backend: Arc<dyn SandboxBackend>,
    active: DashMap<String, ActiveSandboxRun>,
    victoria: Option<VictoriaLogsEmitter>,
}

impl SandboxRuntime {
    pub fn new(backend: Arc<dyn SandboxBackend>) -> Self {
        Self {
            backend,
            active: DashMap::new(),
            victoria: VictoriaLogsEmitter::from_env(),
        }
    }

    /// Access the underlying sandbox backend (for manual orchestration).
    pub fn backend(&self) -> &Arc<dyn SandboxBackend> {
        &self.backend
    }

    /// Run a coding agent session in a new sandbox, returning structured results.
    ///
    /// This is the primary entry point for executing agents. It handles:
    /// 1. Provisioning the container via the configured backend
    /// 2. Waiting for sandbox-agent to become healthy
    /// 3. Creating a session via the REST API
    /// 4. Starting SSE event reader for permission handling
    /// 5. Sending the prompt message
    /// 6. Waiting for completion via SSE events
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

    /// Run the agent session inside an already-provisioned sandbox
    /// using the sandbox-agent REST API.
    ///
    /// Public so that callers (e.g. `OrchestratorAgentRuntime`) can split
    /// provision → run → release when they need to observe the handle mid-run.
    pub async fn run_session_in_sandbox(
        &self,
        handle: &SandboxHandle,
        config: &SandboxConfig,
        instruction: &str,
    ) -> Result<SandboxResult> {
        let start = std::time::Instant::now();

        // 2. Wait for sandbox-agent to be healthy.
        self.backend.wait_ready(handle).await?;
        tracing::info!("sandbox-agent is ready");

        // 3. Create the sandbox-agent client.
        let client = SandboxAgentClient::new(&handle.sandbox_agent_url, None);
        let session_id = uuid::Uuid::new_v4().to_string();
        let agent_str = config.agent.as_sandbox_agent_str();

        // 4. Create REST session.
        let model_ref = config.model.as_deref();
        let perm_mode = match config.permission_mode {
            PermissionMode::Bypass => Some("bypass"),
            _ => None,
        };
        let create_resp = client
            .create_session(&session_id, agent_str, model_ref, perm_mode)
            .await?;
        if !create_resp.healthy {
            let err_detail = create_resp
                .error
                .map(|e| e.to_string())
                .unwrap_or_else(|| "unknown".to_string());
            return Err(Error::BackendMessage(format!(
                "create_session returned unhealthy: {err_detail}"
            )));
        }
        tracing::info!(%session_id, "REST session created");

        // 5. Start SSE event reader with completion channel.
        let bypass_perms = config.permission_mode == PermissionMode::Bypass;
        let (events_tx, mut events_rx) = tokio::sync::mpsc::channel::<serde_json::Value>(256);
        let (completion_tx, completion_rx) = tokio::sync::oneshot::channel::<bool>();
        let client_sse = client.clone();
        let sid_sse = session_id.clone();
        let victoria = self.victoria.clone();
        let victoria_ctx = if victoria.is_some() {
            Some(VictoriaSessionCtx {
                sandbox_id: handle.id.clone(),
                sandbox_backend: handle.backend,
                sandbox_agent_url: handle.sandbox_agent_url.clone(),
                host_port: handle.host_port,
                session_id: session_id.clone(),
                agent: config.agent,
                tags: Arc::new(config.log_tags.clone()),
            })
        } else {
            None
        };
        let victoria_ctx_sse = victoria_ctx.clone();
        let sse_task = tokio::spawn(async move {
            Self::sse_reader(
                client_sse,
                &sid_sse,
                bypass_perms,
                events_tx,
                Some(completion_tx),
                true,
                None,
                victoria,
                victoria_ctx_sse,
            )
            .await;
        });

        // 6. Send message (fire-and-forget).
        tracing::info!("sending message via REST, waiting for completion");
        client.post_message(&session_id, instruction).await?;

        // 7. Wait for completion via SSE events or timeout.
        //
        // IMPORTANT: We must drain `events_rx` concurrently while waiting for
        // completion. The SSE reader sends events through this channel AND
        // signals completion via `completion_tx`. If the channel fills up
        // (capacity=256) before the reader reaches `turn.completed`, the reader
        // blocks on `events_tx.send().await` and never signals completion —
        // causing a deadlock. Draining here prevents that.
        let timeout = Duration::from_secs(config.timeout_seconds);
        let mut output_buf: Option<String> = None;
        let mut all_events = Vec::new();
        let sleep = tokio::time::sleep(timeout);
        tokio::pin!(sleep);
        tokio::pin!(completion_rx);

        let mut completed = false;
        let mut error_msg: Option<String> = None;

        loop {
            tokio::select! {
                result = &mut completion_rx => {
                    match result {
                        Ok(true) => { completed = true; }
                        Ok(false) => { error_msg = Some("session ended with error".to_string()); }
                        Err(_) => { error_msg = Some("SSE reader dropped completion channel".to_string()); }
                    }
                    break;
                }
                Some(event) = events_rx.recv() => {
                    Self::extract_output_from_universal(&event, &mut output_buf);
                    all_events.push(event);
                }
                _ = &mut sleep => {
                    tracing::warn!("session timed out");
                    error_msg = Some("session timed out".to_string());
                    break;
                }
            }
        }

        // Drain any remaining events from the channel.
        while let Ok(event) = events_rx.try_recv() {
            Self::extract_output_from_universal(&event, &mut output_buf);
            all_events.push(event);
        }

        // 8. Abort SSE reader and terminate session.
        sse_task.abort();
        let _ = client.terminate_session(&session_id).await;

        // Emit a synthetic completion record to VictoriaLogs (best-effort).
        if let (Some(v), Some(ctx)) = (self.victoria.as_ref(), victoria_ctx.as_ref()) {
            v.enqueue_session_completed(
                ctx,
                completed,
                start.elapsed().as_secs_f64(),
                error_msg.as_deref(),
            );
        }

        Ok(SandboxResult {
            events: all_events,
            completed,
            duration_seconds: 0.0, // Caller fills this in.
            final_output: output_buf,
            error: error_msg,
        })
    }

    /// Background task: reads SSE events from the universal events stream.
    ///
    /// Handles permission auto-approval in bypass mode and signals completion
    /// when `session.ended` is observed or (if `complete_on_turn` is true)
    /// when `turn.completed` is observed.
    ///
    /// For blocking sessions, set `complete_on_turn = true` (completes after first turn).
    /// For fire-and-forget workers, set `complete_on_turn = false` (only completes on
    /// `session.ended` or SSE stream close).
    pub async fn sse_reader(
        client: SandboxAgentClient,
        session_id: &str,
        bypass_permissions: bool,
        events_tx: tokio::sync::mpsc::Sender<serde_json::Value>,
        completion_tx: Option<tokio::sync::oneshot::Sender<bool>>,
        complete_on_turn: bool,
        last_sse_epoch_ms: Option<Arc<AtomicI64>>,
        victoria: Option<VictoriaLogsEmitter>,
        victoria_ctx: Option<VictoriaSessionCtx>,
    ) {
        let resp = match client.events_sse_connect(session_id).await {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!(%e, "SSE connect failed, permissions may not be handled");
                if let Some(tx) = completion_tx {
                    let _ = tx.send(false);
                }
                return;
            }
        };

        let mut stream = resp.bytes_stream();
        let mut buf = String::new();
        let mut completion_tx = completion_tx;
        let sse_idle_timeout = Duration::from_secs(120);
        let mut seq: u64 = 0;

        loop {
            let chunk = match tokio::time::timeout(sse_idle_timeout, stream.next()).await {
                Ok(Some(chunk)) => chunk,
                Ok(None) => break, // stream ended cleanly
                Err(_timeout) => {
                    // SSE idle timeout — check session status via REST fallback.
                    tracing::info!("SSE idle timeout after 120s, checking session status via REST");
                    match client.is_session_ended(session_id).await {
                        Ok(true) => {
                            tracing::info!("SSE idle timeout: session ended (confirmed via REST)");
                            if let Some(tx) = completion_tx.take() {
                                let _ = tx.send(true);
                            }
                            return;
                        }
                        Ok(false) => {
                            tracing::debug!("SSE idle timeout: session still active per REST, continuing");
                            continue;
                        }
                        Err(e) => {
                            tracing::warn!(%e, "SSE idle timeout: REST session check failed, treating as disconnected");
                            break;
                        }
                    }
                }
            };

            let bytes = match chunk {
                Ok(b) => b,
                Err(e) => {
                    tracing::debug!(%e, "SSE stream error");
                    break;
                }
            };

            buf.push_str(&String::from_utf8_lossy(&bytes));

            // Process complete SSE frames (delimited by blank lines).
            while let Some(pos) = buf.find("\n\n") {
                let frame = buf[..pos].to_string();
                buf = buf[pos + 2..].to_string();

                for line in frame.lines() {
                    let data = match line
                        .strip_prefix("data: ")
                        .or_else(|| line.strip_prefix("data:"))
                    {
                        Some(d) => d.trim(),
                        None => continue,
                    };

                    let event: serde_json::Value = match serde_json::from_str(data) {
                        Ok(v) => v,
                        Err(e) => {
                            tracing::debug!(%e, sse_data = %data, "SSE data not parseable as JSON");
                            continue;
                        }
                    };

                    let event_type = event.get("type").and_then(|t| t.as_str()).unwrap_or("");
                    tracing::debug!(%event_type, "SSE event received");
                    seq = seq.saturating_add(1);

                    // Tee to VictoriaLogs best-effort. Never block the SSE reader.
                    if let (Some(v), Some(ctx)) = (victoria.as_ref(), victoria_ctx.as_ref()) {
                        v.enqueue_event(ctx, seq, &event);
                    }

                    // Update last-active timestamp.
                    if let Some(ref ts) = last_sse_epoch_ms {
                        ts.store(Utc::now().timestamp_millis(), Ordering::Relaxed);
                    }

                    // Auto-approve permission requests in bypass mode.
                    if bypass_permissions && event_type == "permission.requested" {
                        if let Some(perm_id) = event
                            .get("data")
                            .and_then(|d| d.get("permission_id"))
                            .and_then(|p| p.as_str())
                        {
                            tracing::info!(%perm_id, "auto-approving permission (bypass)");
                            if let Err(e) =
                                client.permission_reply(session_id, perm_id, "always").await
                            {
                                tracing::warn!(%e, %perm_id, "permission_reply failed");
                            }
                        }
                    }

                    // Detect session completion via top-level event type.
                    if event_type == "session.ended" {
                        tracing::info!(%event_type, "session.ended event received");
                        if let Some(tx) = completion_tx.take() {
                            let _ = tx.send(true);
                        }
                    }

                    // Detect turn.completed — only signal completion if complete_on_turn is set.
                    if event_type == "turn.completed" {
                        tracing::info!(%event_type, %complete_on_turn, "turn.completed event received");
                        if complete_on_turn {
                            if let Some(tx) = completion_tx.take() {
                                let _ = tx.send(true);
                            }
                        }
                    }

                    // Detect turn.completed embedded as status label in item.completed.
                    if event_type == "item.completed" {
                        if let Some(item) = event.get("data").and_then(|d| d.get("item")) {
                            let kind = item.get("kind").and_then(|k| k.as_str()).unwrap_or("");
                            if kind == "status" {
                                if let Some(content) =
                                    item.get("content").and_then(|c| c.as_array())
                                {
                                    for c in content {
                                        let label =
                                            c.get("label").and_then(|l| l.as_str()).unwrap_or("");
                                        if label == "turn.completed" {
                                            let detail_str = c
                                                .get("detail")
                                                .and_then(|d| d.as_str())
                                                .unwrap_or("");
                                            let failed =
                                                detail_str.contains("\"status\":\"failed\"");
                                            if failed {
                                                tracing::warn!(detail = %detail_str, "turn.completed with failure");
                                            } else {
                                                tracing::info!(%complete_on_turn, "turn.completed status detected");
                                            }
                                            if complete_on_turn {
                                                if let Some(tx) = completion_tx.take() {
                                                    let _ = tx.send(!failed);
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }

                    // Detect errors.
                    if event_type == "error" {
                        let msg = event
                            .get("data")
                            .and_then(|d| d.get("message"))
                            .and_then(|m| m.as_str())
                            .unwrap_or("unknown error");
                        tracing::warn!(%msg, "error event received");
                        if let Some(tx) = completion_tx.take() {
                            let _ = tx.send(false);
                        }
                    }

                    // Forward to collector (ignore send errors — receiver may be
                    // dropped in fire-and-forget mode, but we must keep running
                    // for permission auto-approval).
                    let _ = events_tx.send(event).await;
                }
            }
        }

        // Stream ended without explicit session.ended.
        if let Some(tx) = completion_tx {
            let _ = tx.send(false);
        }
    }

    /// Try to extract text output from universal events.
    ///
    /// Looks for assistant message items with text content.
    fn extract_output_from_universal(event: &serde_json::Value, output_buf: &mut Option<String>) {
        let event_type = event.get("type").and_then(|t| t.as_str()).unwrap_or("");

        if event_type == "item.delta" {
            // Item deltas contain streaming text chunks.
            if let Some(delta) = event
                .get("data")
                .and_then(|d| d.get("delta"))
                .and_then(|t| t.as_str())
            {
                output_buf.get_or_insert_with(String::new).push_str(delta);
            }
        } else if event_type == "item.completed" {
            // Completed items may have final text content.
            if let Some(item) = event.get("data").and_then(|d| d.get("item")) {
                let role = item.get("role").and_then(|r| r.as_str()).unwrap_or("");
                let kind = item.get("kind").and_then(|k| k.as_str()).unwrap_or("");
                if role == "assistant" && kind == "message" {
                    if let Some(content) = item.get("content").and_then(|c| c.as_str()) {
                        *output_buf = Some(content.to_string());
                    }
                }
            }
        }
    }

    /// Run an agent without waiting for completion — returns the handle,
    /// session ID, last-active timestamp, and monitoring channels.
    ///
    /// Returns `(handle, session_id, last_sse_epoch_ms, channels)`.
    #[tracing::instrument(level = "info", skip(self, config, instruction), fields(agent = %config.agent))]
    pub async fn start_agent(
        &self,
        config: &SandboxConfig,
        instruction: &str,
    ) -> Result<(SandboxHandle, String, Arc<AtomicI64>, StartAgentChannels)> {
        let handle = self.backend.provision(config).await?;
        self.backend.wait_ready(&handle).await?;

        let client = SandboxAgentClient::new(&handle.sandbox_agent_url, None);
        let session_id = uuid::Uuid::new_v4().to_string();
        let agent_str = config.agent.as_sandbox_agent_str();

        // Create REST session.
        let model_ref = config.model.as_deref();
        let perm_mode = match config.permission_mode {
            PermissionMode::Bypass => Some("bypass"),
            _ => None,
        };
        let create_resp = client
            .create_session(&session_id, agent_str, model_ref, perm_mode)
            .await?;
        if !create_resp.healthy {
            let err_detail = create_resp
                .error
                .map(|e| e.to_string())
                .unwrap_or_else(|| "unknown".to_string());
            return Err(Error::BackendMessage(format!(
                "create_session returned unhealthy: {err_detail}"
            )));
        }
        tracing::info!(%session_id, "REST session created (fire-and-forget mode)");

        // Start SSE reader with completion channel.
        let bypass_perms = config.permission_mode == PermissionMode::Bypass;
        let (events_tx, events_rx) = tokio::sync::mpsc::channel::<serde_json::Value>(256);
        let (completion_tx, completion_rx) = tokio::sync::oneshot::channel::<bool>();
        let last_sse_epoch_ms = Arc::new(AtomicI64::new(0));
        let last_sse_clone = last_sse_epoch_ms.clone();
        let client_sse = client.clone();
        let sid_sse = session_id.clone();
        let victoria = self.victoria.clone();
        let victoria_ctx = if victoria.is_some() {
            Some(VictoriaSessionCtx {
                sandbox_id: handle.id.clone(),
                sandbox_backend: handle.backend,
                sandbox_agent_url: handle.sandbox_agent_url.clone(),
                host_port: handle.host_port,
                session_id: session_id.clone(),
                agent: config.agent,
                tags: Arc::new(config.log_tags.clone()),
            })
        } else {
            None
        };
        let sse_task = tokio::spawn(async move {
            Self::sse_reader(
                client_sse,
                &sid_sse,
                bypass_perms,
                events_tx,
                Some(completion_tx),
                true,
                Some(last_sse_clone),
                victoria,
                victoria_ctx,
            )
            .await;
        });

        // Send message in background (fire-and-forget).
        let client_msg = client.clone();
        let sid_msg = session_id.clone();
        let instr = instruction.to_string();
        tokio::spawn(async move {
            if let Err(e) = client_msg.post_message(&sid_msg, &instr).await {
                tracing::warn!(%e, "background post_message failed");
            }
        });

        let channels = StartAgentChannels {
            events_rx,
            completion_rx,
            sse_task,
        };

        Ok((handle, session_id, last_sse_epoch_ms, channels))
    }

    /// Start an agent session and track it in-memory for monitoring/restart.
    ///
    /// Returns a stable `run_id` which can be used for subsequent health checks,
    /// SSE polling, release, and crash recovery, plus channels for monitoring.
    #[tracing::instrument(level = "info", skip(self, config, instruction), fields(agent = %config.agent))]
    pub async fn start_agent_tracked(
        &self,
        org_id: String,
        agent_id: Option<String>,
        config: &SandboxConfig,
        instruction: &str,
    ) -> Result<(ActiveSandboxRun, StartAgentChannels)> {
        let (handle, session_id, last_sse_epoch_ms, channels) =
            self.start_agent(config, instruction).await?;
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
            last_sse_epoch_ms,
        };
        self.active.insert(run_id, run.clone());
        Ok((run, channels))
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub fn list_active(&self) -> Vec<ActiveSandboxRun> {
        self.active.iter().map(|e| e.value().clone()).collect()
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub fn get_active(&self, run_id: &str) -> Option<ActiveSandboxRun> {
        self.active.get(run_id).map(|e| e.value().clone())
    }

    /// Return the last SSE activity time for a tracked run, if any event has been received.
    pub fn get_sse_activity(&self, run_id: &str) -> Option<DateTime<Utc>> {
        self.active.get(run_id).and_then(|r| {
            let ms = r.last_sse_epoch_ms.load(Ordering::Relaxed);
            if ms > 0 {
                DateTime::from_timestamp_millis(ms)
            } else {
                None
            }
        })
    }

    /// Release a tracked run (best-effort) and remove it from the active set.
    #[tracing::instrument(level = "info", skip_all)]
    pub async fn release_run(&self, run_id: &str) -> Result<()> {
        let run = self
            .active
            .remove(run_id)
            .map(|(_, v)| v)
            .ok_or_else(|| Error::NotFound(format!("sandbox run not found: {run_id}")))?;
        // Terminate session before releasing the container.
        let client = SandboxAgentClient::new(&run.sandbox.sandbox_agent_url, None);
        let _ = client.terminate_session(&run.session_id).await;
        self.backend.release(&run.sandbox).await
    }

    /// Restart a tracked run by provisioning a new sandbox and replaying the original instruction.
    #[tracing::instrument(level = "info", skip_all)]
    pub async fn restart_run(
        &self,
        run_id: &str,
    ) -> Result<(ActiveSandboxRun, StartAgentChannels)> {
        let Some(mut run) = self.get_active(run_id) else {
            return Err(Error::NotFound(format!("sandbox run not found: {run_id}")));
        };

        let (new_handle, new_session_id, new_last_sse, channels) =
            self.start_agent(&run.config, &run.instruction).await?;

        // Release the old sandbox best-effort.
        let old_client = SandboxAgentClient::new(&run.sandbox.sandbox_agent_url, None);
        let _ = old_client.terminate_session(&run.session_id).await;
        let _ = self.backend.release(&run.sandbox).await;

        run.sandbox = new_handle;
        run.session_id = new_session_id;
        run.restart_count = run.restart_count.saturating_add(1);
        run.started_at = Utc::now();
        run.last_sse_epoch_ms = new_last_sse;
        self.active.insert(run_id.to_string(), run.clone());
        Ok((run, channels))
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
