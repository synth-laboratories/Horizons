//! Orchestrator agent runtime — singleton wrapper around SandboxRuntime + McpToolServer.
//!
//! Provisions a coding agent container with MCP tool access and blocks until
//! the agent completes. Enforces singleton semantics: if a run is already
//! in progress, `run()` returns `None` immediately.

use crate::engine::docker_backend::capture_container_logs;
use crate::engine::mcp_tool_server::McpToolServer;
use crate::engine::models::{
    AgentKind, PermissionMode, SandboxBackendKind, SandboxConfig, SandboxResult,
};
use crate::engine::sandbox_runtime::SandboxRuntime;
use crate::Result;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for the orchestrator agent container.
#[derive(Debug, Clone)]
pub struct OrchestratorAgentConfig {
    pub agent: AgentKind,
    pub model: Option<String>,
    pub image: Option<String>,
    pub env_vars: HashMap<String, String>,
    /// Maximum seconds the orchestrator agent may run (default: 600).
    pub timeout_seconds: u64,
    /// Host URL reachable from inside the Docker container, e.g.
    /// `"host.docker.internal:8787"`.
    pub mcp_host_url: String,
    /// MCP namespace — the path segment after `/mcp/`, e.g. `"orchestrator"`.
    pub mcp_namespace: String,
    /// Extra setup script lines (repo cloning, etc.) appended after MCP config.
    pub extra_setup_lines: Vec<String>,
}

// ---------------------------------------------------------------------------
// Status
// ---------------------------------------------------------------------------

/// Current status of the orchestrator runtime.
#[derive(Debug, Clone, serde::Serialize)]
#[serde(tag = "status")]
pub enum OrchestratorStatus {
    Idle,
    Running {
        started_at: String,
        trigger: serde_json::Value,
        run_id: String,
        /// Docker container ID (set after provisioning completes).
        #[serde(skip_serializing_if = "Option::is_none")]
        container_id: Option<String>,
        /// Host port mapped to sandbox-agent inside the container.
        #[serde(skip_serializing_if = "Option::is_none")]
        host_port: Option<u16>,
    },
}

#[derive(Debug)]
struct RunningState {
    started_at: chrono::DateTime<chrono::Utc>,
    trigger: serde_json::Value,
    run_id: String,
    container_id: Option<String>,
    host_port: Option<u16>,
}

// ---------------------------------------------------------------------------
// Runtime
// ---------------------------------------------------------------------------

/// Singleton orchestrator runtime.
///
/// Wraps `SandboxRuntime` + `McpToolServer` and enforces that at most one
/// orchestrator agent run is active at a time.
pub struct OrchestratorAgentRuntime {
    sandbox: Arc<SandboxRuntime>,
    mcp_server: McpToolServer,
    config: OrchestratorAgentConfig,
    running: Arc<Mutex<Option<RunningState>>>,
}

impl OrchestratorAgentRuntime {
    pub fn new(
        sandbox: Arc<SandboxRuntime>,
        mcp_server: McpToolServer,
        config: OrchestratorAgentConfig,
    ) -> Self {
        Self {
            sandbox,
            mcp_server,
            config,
            running: Arc::new(Mutex::new(None)),
        }
    }

    /// Insert or overwrite a single env var that will be injected into sandbox containers.
    ///
    /// Use this to inject per-run secrets (e.g. per-org `SYNTH_API_KEY` retrieved from the
    /// control plane) after the runtime has been constructed but before calling `run()`.
    pub fn set_env_var(&mut self, key: impl Into<String>, value: impl Into<String>) {
        self.config.env_vars.insert(key.into(), value.into());
    }

    /// Returns true if the given env var key is already present in the sandbox env map.
    pub fn has_env_var(&self, key: &str) -> bool {
        self.config.env_vars.contains_key(key)
    }

    /// Returns the current status (idle or running with metadata).
    pub async fn status(&self) -> OrchestratorStatus {
        let guard = self.running.lock().await;
        match guard.as_ref() {
            None => OrchestratorStatus::Idle,
            Some(state) => OrchestratorStatus::Running {
                started_at: state.started_at.to_rfc3339(),
                trigger: state.trigger.clone(),
                run_id: state.run_id.clone(),
                container_id: state.container_id.clone(),
                host_port: state.host_port,
            },
        }
    }

    /// Run the orchestrator agent. Returns `None` if already running.
    ///
    /// Blocks until the agent session completes (or times out), then cleans up
    /// the MCP session and returns the sandbox result.
    pub async fn run(
        &self,
        instruction: &str,
        trigger: serde_json::Value,
    ) -> Option<Result<(SandboxResult, crate::engine::models::SandboxHandle)>> {
        self.run_with_tags(instruction, trigger, HashMap::new()).await
    }

    /// Run the orchestrator agent with structured log tags that will be attached
    /// to sandbox universal events when teeing to sinks (e.g. VictoriaLogs).
    ///
    /// This is the preferred entry point for embedding runtimes (SMR, SB, etc.)
    /// that need correlation fields like `run_id`, `project_id`, etc.
    pub async fn run_with_tags(
        &self,
        instruction: &str,
        trigger: serde_json::Value,
        log_tags: HashMap<String, String>,
    ) -> Option<Result<(SandboxResult, crate::engine::models::SandboxHandle)>> {
        let run_id = ulid::Ulid::new().to_string();

        // ── Singleton check ──────────────────────────────────────────────
        {
            let mut guard = self.running.lock().await;
            if guard.is_some() {
                tracing::info!("orchestrator already running, skipping");
                return None;
            }
            *guard = Some(RunningState {
                started_at: chrono::Utc::now(),
                trigger: trigger.clone(),
                run_id: run_id.clone(),
                container_id: None,
                host_port: None,
            });
        }

        // ── Generate MCP session token ───────────────────────────────────
        let mcp_token = self.mcp_server.create_session(&run_id).await;

        // ── Build SANDBOX_SETUP_SCRIPT ───────────────────────────────────
        let setup_script = self.build_setup_script(&mcp_token);

        // ── Build SandboxConfig ──────────────────────────────────────────
        let mut env_vars = self.config.env_vars.clone();
        env_vars.insert("ORCHESTRATOR_MCP_TOKEN".to_string(), mcp_token.clone());
        env_vars.insert("SANDBOX_SETUP_SCRIPT".to_string(), setup_script);

        let sandbox_config = SandboxConfig {
            agent: self.config.agent,
            model: self.config.model.clone(),
            permission_mode: PermissionMode::Bypass,
            image: self.config.image.clone(),
            env_vars,
            timeout_seconds: self.config.timeout_seconds,
            workdir: Some("/workspace".to_string()),
            docker_socket: false,
            restart_policy: None,
            log_tags,
        };

        tracing::info!(
            %run_id,
            agent = %self.config.agent,
            model = ?self.config.model,
            "starting orchestrator agent run"
        );

        // ── Provision container ──────────────────────────────────────────
        let start = std::time::Instant::now();
        let handle = match self.sandbox.backend().provision(&sandbox_config).await {
            Ok(h) => h,
            Err(e) => {
                tracing::error!(%run_id, err = ?e, "failed to provision orchestrator container");
                self.mcp_server.revoke_session(&mcp_token).await;
                *self.running.lock().await = None;
                return Some(Err(e));
            }
        };

        // Store container info so status() can report it.
        {
            let mut guard = self.running.lock().await;
            if let Some(state) = guard.as_mut() {
                state.container_id = Some(handle.id.clone());
                state.host_port = Some(handle.host_port);
            }
        }

        tracing::info!(
            %run_id,
            container_id = %handle.id,
            host_port = handle.host_port,
            "orchestrator container provisioned"
        );

        // ── Run the agent session (blocking) ─────────────────────────────
        let result = self
            .sandbox
            .run_session_in_sandbox(&handle, &sandbox_config, instruction)
            .await;

        // ── Release container ────────────────────────────────────────────
        if let Err(e) = self.sandbox.backend().release(&handle).await {
            tracing::warn!(%run_id, %e, "failed to release orchestrator container");
        }

        // ── Cleanup ──────────────────────────────────────────────────────
        self.mcp_server.revoke_session(&mcp_token).await;
        *self.running.lock().await = None;

        match result {
            Ok(mut sandbox_result) => {
                sandbox_result.duration_seconds = start.elapsed().as_secs_f64();
                tracing::info!(
                    %run_id,
                    completed = sandbox_result.completed,
                    duration = sandbox_result.duration_seconds,
                    "orchestrator agent run finished"
                );
                Some(Ok((sandbox_result, handle)))
            }
            Err(e) => {
                // Capture container logs for Docker backends before returning.
                if handle.backend == SandboxBackendKind::Docker {
                    let logs = capture_container_logs(&handle.id, 100).await;
                    tracing::error!(
                        %run_id,
                        container_id = %handle.id,
                        host_port = handle.host_port,
                        err = ?e,
                        container_logs = %logs,
                        "orchestrator agent run failed"
                    );
                } else {
                    tracing::error!(%run_id, err = ?e, "orchestrator agent run failed");
                }
                Some(Err(e))
            }
        }
    }

    /// Access the MCP server (for registering tools or creating the HTTP route).
    pub fn mcp_server(&self) -> &McpToolServer {
        &self.mcp_server
    }

    // -----------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------

    fn build_setup_script(&self, _mcp_token: &str) -> String {
        let mcp_host_url = &self.config.mcp_host_url;
        let mcp_namespace = &self.config.mcp_namespace;
        // If the URL already has a scheme (e.g. https://…trycloudflare.com), use as-is;
        // otherwise prepend http:// (legacy host:port format like host.docker.internal:8081).
        let mcp_base = if mcp_host_url.starts_with("http://") || mcp_host_url.starts_with("https://") {
            mcp_host_url.trim_end_matches('/').to_string()
        } else {
            format!("http://{mcp_host_url}")
        };

        let mut lines = Vec::new();

        // Write Codex MCP config so the agent can reach our tool server.
        lines.push("mkdir -p /root/.codex".to_string());
        lines.push(format!(
            r#"cat > /root/.codex/config.toml << 'TOML'
[mcp_servers.{mcp_namespace}]
enabled = true
url = "{mcp_base}/mcp/{mcp_namespace}"
bearer_token_env_var = "ORCHESTRATOR_MCP_TOKEN"
TOML"#,
        ));

        // Append extra setup lines (repo cloning, etc.)
        for line in &self.config.extra_setup_lines {
            lines.push(line.clone());
        }

        lines.join("\n")
    }
}
