//! Sandbox runtime orchestrator.
//!
//! Coordinates the full lifecycle of running a coding agent in a sandbox:
//! provision container → create session → SSE event stream →
//! post message → collect results → release.

use crate::engine::models::{
    AgentKind, PermissionMode, SandboxConfig, SandboxHandle, SandboxResult,
};
use crate::engine::sandbox_agent_client::{SandboxAgentClient, SessionLiveness};
use crate::engine::traits::SandboxBackend;
use crate::engine::victoria_logs::{VictoriaLogsEmitter, VictoriaSessionCtx};
use crate::{Error, Result};
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use futures_util::StreamExt;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;
use tokio::sync::mpsc::error::TrySendError;

fn scripted_agent_mode_enabled() -> bool {
    std::env::var("SMR_SCRIPTED_AGENT_MODE")
        .ok()
        .map(|v| {
            let n = v.trim().to_ascii_lowercase();
            matches!(n.as_str(), "1" | "true" | "yes" | "on")
        })
        .unwrap_or(false)
}

/// Channels returned by `start_agent()` for monitoring a fire-and-forget session.
///
/// The caller receives the SSE event stream and a one-shot completion signal.
pub struct StartAgentChannels {
    pub events_rx: tokio::sync::mpsc::Receiver<serde_json::Value>,
    pub completion_rx: tokio::sync::oneshot::Receiver<SessionTerminalSignal>,
    pub sse_task: tokio::task::JoinHandle<()>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SessionTerminalSignal {
    Completed,
    Failed { reason: String },
    Aborted { reason: String },
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
    fn sse_reconnect_max_attempts() -> u32 {
        std::env::var("HORIZONS_SSE_RECONNECT_MAX_ATTEMPTS")
            .ok()
            .and_then(|v| v.parse::<u32>().ok())
            .unwrap_or(4)
            .max(1)
    }

    fn sse_reconnect_base_backoff_ms() -> u64 {
        std::env::var("HORIZONS_SSE_RECONNECT_BASE_BACKOFF_MS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(500)
            .max(100)
    }

    fn sse_dependency_reconnect_max_attempts() -> u32 {
        std::env::var("HORIZONS_SSE_DEPENDENCY_RECONNECT_MAX_ATTEMPTS")
            .ok()
            .and_then(|v| v.parse::<u32>().ok())
            .unwrap_or(24)
            .max(1)
    }

    fn is_argo_origin_unregistered_error(err_text: &str) -> bool {
        let lower = err_text.to_ascii_lowercase();
        lower.contains("origin has been unregistered from argo tunnel")
            || (lower.contains("events sse connect failed: 530")
                && lower.contains("argo tunnel"))
            || (lower.contains("get /v1/sessions failed: 530") && lower.contains("argo tunnel"))
    }

    fn sse_reconnect_backoff(attempt: u32) -> Duration {
        let base = Self::sse_reconnect_base_backoff_ms();
        // 500ms, 1s, 2s, 4s... capped to keep retries responsive.
        let factor = 1u64.checked_shl(attempt.min(8)).unwrap_or(u64::MAX);
        Duration::from_millis(base.saturating_mul(factor).min(5_000))
    }

    async fn maybe_reconnect_or_signal(
        client: &SandboxAgentClient,
        session_id: &str,
        completion_tx: &mut Option<tokio::sync::oneshot::Sender<SessionTerminalSignal>>,
        consecutive_disconnects: &mut u32,
        terminal_reason_if_exhausted: &'static str,
    ) -> bool {
        if completion_tx.is_none() {
            return false;
        }

        let max_attempts = Self::sse_reconnect_max_attempts();
        match client.session_liveness(session_id).await {
            Ok(SessionLiveness::Ended) => {
                tracing::info!("SSE disconnect: session ended (confirmed via REST)");
                if let Some(tx) = completion_tx.take() {
                    let _ = tx.send(SessionTerminalSignal::Completed);
                }
                false
            }
            Ok(SessionLiveness::Missing) => {
                tracing::warn!("SSE disconnect: session missing via REST, treating as aborted");
                if let Some(tx) = completion_tx.take() {
                    let _ = tx.send(SessionTerminalSignal::Aborted {
                        reason: "session_missing".to_string(),
                    });
                }
                false
            }
            Ok(SessionLiveness::Active) => {
                *consecutive_disconnects = consecutive_disconnects.saturating_add(1);
                if *consecutive_disconnects > max_attempts {
                    tracing::warn!(
                        attempts = *consecutive_disconnects,
                        max_attempts,
                        "SSE reconnect attempts exhausted while session still active; aborting"
                    );
                    if let Some(tx) = completion_tx.take() {
                        let _ = tx.send(SessionTerminalSignal::Aborted {
                            reason: terminal_reason_if_exhausted.to_string(),
                        });
                    }
                    return false;
                }
                let backoff = Self::sse_reconnect_backoff(*consecutive_disconnects - 1);
                tracing::warn!(
                    attempts = *consecutive_disconnects,
                    max_attempts,
                    backoff_ms = backoff.as_millis(),
                    "SSE disconnected while session active; retrying stream connection"
                );
                tokio::time::sleep(backoff).await;
                true
            }
            Err(e) => {
                let err_text = format!("{e:#}");
                let is_dependency_unreachable =
                    Self::is_argo_origin_unregistered_error(&err_text);
                let allowed_attempts = if is_dependency_unreachable {
                    Self::sse_dependency_reconnect_max_attempts()
                } else {
                    max_attempts
                };
                *consecutive_disconnects = consecutive_disconnects.saturating_add(1);
                if *consecutive_disconnects > allowed_attempts {
                    if is_dependency_unreachable {
                        tracing::error!(
                            %e,
                            attempts = *consecutive_disconnects,
                            max_attempts = allowed_attempts,
                            reason_code = "dependency_unreachable_argo_origin_unregistered",
                            "SSE reconnect exhausted while upstream tunnel/origin is unreachable; aborting loudly"
                        );
                    } else {
                        tracing::warn!(
                            %e,
                            attempts = *consecutive_disconnects,
                            max_attempts = allowed_attempts,
                            "REST liveness checks failed while reconnecting SSE; aborting"
                        );
                    }
                    if let Some(tx) = completion_tx.take() {
                        let _ = tx.send(SessionTerminalSignal::Aborted {
                            reason: if is_dependency_unreachable {
                                "dependency_unreachable_argo_origin_unregistered".to_string()
                            } else {
                                "rest_session_check_failed".to_string()
                            },
                        });
                    }
                    return false;
                }
                let backoff = Self::sse_reconnect_backoff(*consecutive_disconnects - 1);
                if is_dependency_unreachable {
                    tracing::error!(
                        %e,
                        attempts = *consecutive_disconnects,
                        max_attempts = allowed_attempts,
                        backoff_ms = backoff.as_millis(),
                        reason_code = "dependency_unreachable_argo_origin_unregistered",
                        "runtime.dependency.unreachable: tunnel/origin unreachable during SSE reconnect; retrying"
                    );
                } else {
                    tracing::warn!(
                        %e,
                        attempts = *consecutive_disconnects,
                        max_attempts = allowed_attempts,
                        backoff_ms = backoff.as_millis(),
                        "REST liveness check failed while reconnecting SSE; retrying"
                    );
                }
                tokio::time::sleep(backoff).await;
                true
            }
        }
    }

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

        // Scripted deterministic mode:
        // keep real sandbox provisioning/health/release, but skip external agent/LLM calls.
        if scripted_agent_mode_enabled() {
            self.backend.wait_ready(&handle).await?;
            let synthetic = SandboxResult {
                events: vec![
                    serde_json::json!({
                        "type": "session.created",
                        "data": { "mode": "scripted_agent" },
                    }),
                    serde_json::json!({
                        "type": "turn.completed",
                        "data": { "mode": "scripted_agent", "completed": true },
                    }),
                ],
                completed: true,
                duration_seconds: start.elapsed().as_secs_f64(),
                final_output: Some("scripted_agent_mode".to_string()),
                error: None,
            };
            if let Err(e) = self.backend.release(&handle).await {
                tracing::warn!(%e, "failed to release sandbox in scripted mode");
            }
            return Ok((synthetic, handle));
        }

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
        // Claude rejects "bypass" permission mode when running as root (common in
        // containers). Send default mode; SSE reader still auto-approves via bypass_perms flag.
        let perm_mode = match (config.permission_mode, config.agent) {
            (PermissionMode::Bypass, AgentKind::Claude) => None,
            (PermissionMode::Bypass, _) => Some("bypass"),
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
        let (completion_tx, completion_rx) =
            tokio::sync::oneshot::channel::<SessionTerminalSignal>();
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
        let no_progress_timeout = Duration::from_secs(config.no_progress_timeout_seconds.max(1));
        let mut output_buf: Option<String> = None;
        let mut all_events = Vec::new();
        let sleep = tokio::time::sleep(timeout);
        let no_progress_sleep = tokio::time::sleep(no_progress_timeout);
        tokio::pin!(sleep);
        tokio::pin!(no_progress_sleep);
        tokio::pin!(completion_rx);

        let mut completed = false;
        let mut error_msg: Option<String> = None;

        tracing::info!(
            session_timeout_seconds = config.timeout_seconds,
            no_progress_timeout_seconds = config.no_progress_timeout_seconds.max(1),
            "runtime.phase.entered: awaiting_terminal_signal"
        );
        Self::emit_runtime_status_event(
            self.victoria.as_ref(),
            victoria_ctx.as_ref(),
            "runtime.phase.entered",
            serde_json::json!({
                "phase": "awaiting_terminal_signal",
                "session_timeout_seconds": config.timeout_seconds,
                "no_progress_timeout_seconds": config.no_progress_timeout_seconds.max(1),
            }),
        );

        loop {
            tokio::select! {
                result = &mut completion_rx => {
                    match result {
                        Ok(SessionTerminalSignal::Completed) => {
                            completed = true;
                            tracing::info!("runtime.terminal: completed");
                            Self::emit_runtime_status_event(
                                self.victoria.as_ref(),
                                victoria_ctx.as_ref(),
                                "runtime.terminal",
                                serde_json::json!({
                                    "status": "completed",
                                    "reason_code": "session_completed",
                                }),
                            );
                        }
                        Ok(SessionTerminalSignal::Failed { reason }) => {
                            error_msg = Some(format!("session failed: {reason}"));
                            tracing::warn!(reason = %reason, "runtime.terminal: failed");
                            Self::emit_runtime_status_event(
                                self.victoria.as_ref(),
                                victoria_ctx.as_ref(),
                                "runtime.terminal",
                                serde_json::json!({
                                    "status": "failed",
                                    "reason_code": "session_failed",
                                    "reason": reason,
                                }),
                            );
                        }
                        Ok(SessionTerminalSignal::Aborted { reason }) => {
                            error_msg = Some(format!("session aborted: {reason}"));
                            tracing::warn!(reason = %reason, "runtime.terminal: aborted");
                            Self::emit_runtime_status_event(
                                self.victoria.as_ref(),
                                victoria_ctx.as_ref(),
                                "runtime.terminal",
                                serde_json::json!({
                                    "status": "aborted",
                                    "reason_code": "session_aborted",
                                    "reason": reason,
                                }),
                            );
                        }
                        Err(_) => {
                            error_msg = Some("session terminal channel dropped".to_string());
                            tracing::warn!("runtime.terminal: channel_dropped");
                            Self::emit_runtime_status_event(
                                self.victoria.as_ref(),
                                victoria_ctx.as_ref(),
                                "runtime.terminal",
                                serde_json::json!({
                                    "status": "aborted",
                                    "reason_code": "terminal_channel_dropped",
                                }),
                            );
                        }
                    }
                    break;
                }
                Some(event) = events_rx.recv() => {
                    Self::extract_output_from_universal(&event, &mut output_buf);
                    all_events.push(event);
                    no_progress_sleep
                        .as_mut()
                        .reset(tokio::time::Instant::now() + no_progress_timeout);
                }
                _ = &mut no_progress_sleep => {
                    tracing::error!(
                        no_progress_timeout_seconds = config.no_progress_timeout_seconds.max(1),
                        "runtime.phase.stalled: no progress while awaiting terminal signal"
                    );
                    error_msg = Some(format!(
                        "no_progress_timeout after {}s",
                        config.no_progress_timeout_seconds.max(1)
                    ));
                    Self::emit_runtime_status_event(
                        self.victoria.as_ref(),
                        victoria_ctx.as_ref(),
                        "runtime.phase.stalled",
                        serde_json::json!({
                            "phase": "awaiting_terminal_signal",
                            "reason_code": "no_progress_timeout",
                            "no_progress_timeout_seconds": config.no_progress_timeout_seconds.max(1),
                        }),
                    );
                    break;
                }
                _ = &mut sleep => {
                    tracing::warn!("session timed out");
                    error_msg = Some("session timed out".to_string());
                    Self::emit_runtime_status_event(
                        self.victoria.as_ref(),
                        victoria_ctx.as_ref(),
                        "runtime.terminal",
                        serde_json::json!({
                            "status": "failed",
                            "reason_code": "session_timeout",
                            "session_timeout_seconds": config.timeout_seconds,
                        }),
                    );
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
        completion_tx: Option<tokio::sync::oneshot::Sender<SessionTerminalSignal>>,
        complete_on_turn: bool,
        last_sse_epoch_ms: Option<Arc<AtomicI64>>,
        victoria: Option<VictoriaLogsEmitter>,
        victoria_ctx: Option<VictoriaSessionCtx>,
    ) {
        let mut completion_tx = completion_tx;
        let sse_idle_timeout = Duration::from_secs(120);
        let mut seq: u64 = 0;
        let mut consecutive_disconnects: u32 = 0;

        'reconnect: loop {
            let resp = match client.events_sse_connect(session_id).await {
                Ok(r) => r,
                Err(e) => {
                    tracing::warn!(%e, "SSE connect failed");
                    let retry = Self::maybe_reconnect_or_signal(
                        &client,
                        session_id,
                        &mut completion_tx,
                        &mut consecutive_disconnects,
                        "sse_connect_failed",
                    )
                    .await;
                    if retry {
                        continue 'reconnect;
                    }
                    return;
                }
            };

            let mut stream = resp.bytes_stream();
            let mut buf = String::new();

            loop {
                let chunk = match tokio::time::timeout(sse_idle_timeout, stream.next()).await {
                    Ok(Some(chunk)) => chunk,
                    Ok(None) => {
                        tracing::warn!(
                            "SSE stream closed before terminal event, checking liveness/reconnect"
                        );
                        let retry = Self::maybe_reconnect_or_signal(
                            &client,
                            session_id,
                            &mut completion_tx,
                            &mut consecutive_disconnects,
                            "sse_stream_closed_without_terminal_event",
                        )
                        .await;
                        if retry {
                            continue 'reconnect;
                        }
                        return;
                    }
                    Err(_timeout) => {
                        // SSE idle timeout — check session status via REST strict.
                        tracing::info!(
                            "SSE idle timeout after 120s, checking session status via REST"
                        );
                        match client.session_liveness(session_id).await {
                            Ok(SessionLiveness::Ended) => {
                                tracing::info!(
                                    "SSE idle timeout: session ended (confirmed via REST)"
                                );
                                if let Some(tx) = completion_tx.take() {
                                    let _ = tx.send(SessionTerminalSignal::Completed);
                                }
                                return;
                            }
                            Ok(SessionLiveness::Active) => {
                                tracing::debug!(
                                    "SSE idle timeout: session still active per REST, continuing"
                                );
                                continue;
                            }
                            Ok(SessionLiveness::Missing) => {
                                tracing::warn!(
                                    "SSE idle timeout: session missing via REST, treating as aborted"
                                );
                                if let Some(tx) = completion_tx.take() {
                                    let _ = tx.send(SessionTerminalSignal::Aborted {
                                        reason: "session_missing".to_string(),
                                    });
                                }
                                return;
                            }
                            Err(e) => {
                                tracing::warn!(
                                    %e,
                                    "runtime.dependency.unreachable: REST session check failed during SSE idle timeout"
                                );
                                if let Some(tx) = completion_tx.take() {
                                    let _ = tx.send(SessionTerminalSignal::Aborted {
                                        reason: "rest_session_check_failed".to_string(),
                                    });
                                }
                                return;
                            }
                        }
                    }
                };

                let bytes = match chunk {
                    Ok(b) => {
                        consecutive_disconnects = 0;
                        b
                    }
                    Err(e) => {
                        tracing::warn!(%e, "SSE stream error; checking liveness/reconnect");
                        let retry = Self::maybe_reconnect_or_signal(
                            &client,
                            session_id,
                            &mut completion_tx,
                            &mut consecutive_disconnects,
                            "sse_stream_closed_without_terminal_event",
                        )
                        .await;
                        if retry {
                            continue 'reconnect;
                        }
                        return;
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
                                tracing::debug!(
                                    %e,
                                    sse_data = %data,
                                    "SSE data not parseable as JSON"
                                );
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
                                let _ = tx.send(SessionTerminalSignal::Completed);
                            }
                        }

                        // Detect turn.completed — only signal completion if complete_on_turn is set.
                        if event_type == "turn.completed" {
                            tracing::info!(
                                %event_type,
                                %complete_on_turn,
                                "turn.completed event received"
                            );
                            if complete_on_turn {
                                if let Some(tx) = completion_tx.take() {
                                    let _ = tx.send(SessionTerminalSignal::Completed);
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
                                            let label = c
                                                .get("label")
                                                .and_then(|l| l.as_str())
                                                .unwrap_or("");
                                            if label == "turn.completed" {
                                                let detail_str = c
                                                    .get("detail")
                                                    .and_then(|d| d.as_str())
                                                    .unwrap_or("");
                                                let failed =
                                                    detail_str.contains("\"status\":\"failed\"");
                                                if failed {
                                                    tracing::warn!(
                                                        detail = %detail_str,
                                                        "turn.completed with failure"
                                                    );
                                                } else {
                                                    tracing::info!(
                                                        %complete_on_turn,
                                                        "turn.completed status detected"
                                                    );
                                                }
                                                if complete_on_turn {
                                                    if let Some(tx) = completion_tx.take() {
                                                        if failed {
                                                            let _ = tx.send(
                                                                SessionTerminalSignal::Failed {
                                                                    reason: "turn_completed_failed"
                                                                        .to_string(),
                                                                },
                                                            );
                                                        } else {
                                                            let _ = tx.send(
                                                                SessionTerminalSignal::Completed,
                                                            );
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                                if kind == "tool_result" {
                                    if let Some(content) =
                                        item.get("content").and_then(|c| c.as_array())
                                    {
                                        for c in content {
                                            let call_id = c
                                                .get("call_id")
                                                .and_then(|id| id.as_str())
                                                .unwrap_or("unknown");
                                            let output_val = c.get("output");
                                            let output_str =
                                                output_val.and_then(|v| v.as_str()).map(str::trim);
                                            let null_tool_output =
                                                matches!(output_val, Some(serde_json::Value::Null))
                                                    || matches!(output_str, Some("null"));

                                            if null_tool_output {
                                                let output_preview = output_str
                                                    .map(|s| {
                                                        if s.len() <= 160 {
                                                            s.to_string()
                                                        } else {
                                                            format!("{}...", &s[..160])
                                                        }
                                                    })
                                                    .unwrap_or_else(|| "null".to_string());
                                                tracing::error!(
                                                    tool_call_id = %call_id,
                                                    output_preview = %output_preview,
                                                    "MCP tool returned null output; failing loudly to avoid silent first-turn hangs"
                                                );
                                                Self::emit_runtime_status_event(
                                                    victoria.as_ref(),
                                                    victoria_ctx.as_ref(),
                                                    "runtime.tool_result.null_output",
                                                    serde_json::json!({
                                                        "tool_call_id": call_id,
                                                        "output_preview": output_preview,
                                                        "reason_code": "mcp_tool_returned_null_result",
                                                    }),
                                                );
                                                if let Some(tx) = completion_tx.take() {
                                                    let _ = tx.send(SessionTerminalSignal::Failed {
                                                        reason: format!(
                                                            "mcp_tool_returned_null_result:{call_id}"
                                                        ),
                                                    });
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
                                let _ = tx.send(SessionTerminalSignal::Failed {
                                    reason: msg.to_string(),
                                });
                            }
                        }

                        // Forward to collector. Avoid blocking the SSE reader on
                        // high-volume delta streams, otherwise completion signals can
                        // be delayed indefinitely under backpressure.
                        let is_delta = event_type == "item.delta";
                        match events_tx.try_send(event) {
                            Ok(()) => {}
                            Err(TrySendError::Full(ev)) => {
                                if !is_delta {
                                    let _ = events_tx.send(ev).await;
                                }
                            }
                            Err(TrySendError::Closed(_)) => {}
                        }
                    }
                }
            }
        }
    }

    fn emit_runtime_status_event(
        victoria: Option<&VictoriaLogsEmitter>,
        ctx: Option<&VictoriaSessionCtx>,
        event_type: &str,
        data: serde_json::Value,
    ) {
        if let (Some(v), Some(c)) = (victoria, ctx) {
            let event = serde_json::json!({
                "type": event_type,
                "data": data,
                "time": chrono::Utc::now().to_rfc3339(),
            });
            v.enqueue_event(c, u64::MAX - 1, &event);
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
            if let Some(item) = event.get("data").and_then(|d| d.get("item")) {
                let role = item.get("role").and_then(|r| r.as_str()).unwrap_or("");
                let kind = item.get("kind").and_then(|k| k.as_str()).unwrap_or("");

                // Detect MCP tool errors early — these indicate the MCP server
                // failed to connect and the agent cannot use remote tools.
                if kind == "tool_result" {
                    if let Some(content) = item.get("content").and_then(|c| c.as_array()) {
                        for entry in content {
                            if let Some(output) = entry.get("output").and_then(|o| o.as_str()) {
                                if output.contains("tool_use_error")
                                    && output.contains("No such tool")
                                {
                                    let tool_name = entry
                                        .get("call_id")
                                        .and_then(|c| c.as_str())
                                        .unwrap_or("unknown");
                                    tracing::error!(
                                        tool_call_id = tool_name,
                                        "MCP TOOL NOT AVAILABLE: agent tried to call a tool that is not registered. \
                                         This usually means the MCP server in .mcp.json failed to connect. \
                                         Check: (1) .mcp.json type is 'http' not 'url', \
                                         (2) MCP server URL is reachable from the sandbox, \
                                         (3) auth token is valid. Output: {}",
                                        &output[..output.len().min(300)]
                                    );
                                }
                            }
                        }
                    }
                }

                // Completed items may have final text content.
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
        // Claude rejects "bypass" permission mode when running as root (common in
        // containers). Send default mode; SSE reader still auto-approves via bypass_perms flag.
        let model_ref = config.model.as_deref();
        let perm_mode = match (config.permission_mode, config.agent) {
            (PermissionMode::Bypass, AgentKind::Claude) => None,
            (PermissionMode::Bypass, _) => Some("bypass"),
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
        let (completion_tx, completion_rx) =
            tokio::sync::oneshot::channel::<SessionTerminalSignal>();
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

#[cfg(test)]
mod dependency_tests {
    use super::SandboxRuntime;

    #[test]
    fn argo_origin_unregistered_error_is_detected() {
        let msg = "backend error: GET /v1/sessions failed: 530 The origin has been unregistered from Argo Tunnel";
        assert!(SandboxRuntime::is_argo_origin_unregistered_error(msg));
    }

    #[test]
    fn unrelated_error_is_not_misclassified() {
        let msg = "backend error: GET /v1/sessions failed: 500 internal";
        assert!(!SandboxRuntime::is_argo_origin_unregistered_error(msg));
    }
}

#[cfg(test)]
mod tests {
    use super::SandboxRuntime;
    use std::time::Duration;

    #[test]
    fn sse_reconnect_backoff_grows_and_caps() {
        // Base is >=100ms. Backoff should increase with attempt index and cap at 5s.
        let d0 = SandboxRuntime::sse_reconnect_backoff(0);
        let d1 = SandboxRuntime::sse_reconnect_backoff(1);
        let d2 = SandboxRuntime::sse_reconnect_backoff(2);
        assert!(d1 > d0);
        assert!(d2 > d1);
        assert!(SandboxRuntime::sse_reconnect_backoff(32) <= Duration::from_secs(5));
    }
}
