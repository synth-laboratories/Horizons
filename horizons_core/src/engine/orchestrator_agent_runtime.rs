//! Orchestrator agent runtime — singleton wrapper around SandboxRuntime + McpToolServer.
//!
//! Provisions a coding agent container with MCP tool access and blocks until
//! the agent completes. Enforces singleton semantics: if a run is already
//! in progress, `run()` returns `None` immediately.

use crate::Result;
use crate::engine::docker_backend::capture_container_logs;
use crate::engine::mcp_tool_server::McpToolServer;
use crate::engine::models::{
    AgentKind, PermissionMode, SandboxBackendKind, SandboxConfig, SandboxResult,
};
use crate::engine::sandbox_runtime::SandboxRuntime;
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

/// Cleanup guard that makes orchestrator runs cancellation-safe.
///
/// If the run future is dropped (for example by an external timeout), `Drop`
/// schedules best-effort async cleanup to:
/// 1. release the sandbox handle (if provisioned),
/// 2. revoke the MCP session token,
/// 3. clear the singleton `running` flag.
struct RunCleanupGuard {
    run_id: String,
    running: Arc<Mutex<Option<RunningState>>>,
    sandbox: Option<Arc<SandboxRuntime>>,
    mcp_server: Option<McpToolServer>,
    mcp_token: Option<String>,
    handle: Option<crate::engine::models::SandboxHandle>,
    disarmed: bool,
}

impl RunCleanupGuard {
    fn new(
        run_id: String,
        running: Arc<Mutex<Option<RunningState>>>,
        sandbox: Arc<SandboxRuntime>,
        mcp_server: McpToolServer,
        mcp_token: String,
    ) -> Self {
        Self {
            run_id,
            running,
            sandbox: Some(sandbox),
            mcp_server: Some(mcp_server),
            mcp_token: Some(mcp_token),
            handle: None,
            disarmed: false,
        }
    }

    #[cfg(test)]
    fn new_for_test(run_id: String, running: Arc<Mutex<Option<RunningState>>>) -> Self {
        Self {
            run_id,
            running,
            sandbox: None,
            mcp_server: None,
            mcp_token: None,
            handle: None,
            disarmed: false,
        }
    }

    fn set_handle(&mut self, handle: crate::engine::models::SandboxHandle) {
        self.handle = Some(handle);
    }

    async fn cleanup_now(&mut self) {
        if self.disarmed {
            return;
        }

        if let (Some(sandbox), Some(handle)) = (self.sandbox.as_ref(), self.handle.take()) {
            if let Err(e) = sandbox.backend().release(&handle).await {
                tracing::warn!(run_id = %self.run_id, %e, "failed to release orchestrator container");
            }
        }
        if let (Some(mcp_server), Some(token)) = (self.mcp_server.as_ref(), self.mcp_token.take()) {
            mcp_server.revoke_session(&token).await;
        }
        *self.running.lock().await = None;
        self.disarmed = true;
    }
}

impl Drop for RunCleanupGuard {
    fn drop(&mut self) {
        if self.disarmed {
            return;
        }

        let run_id = self.run_id.clone();
        let running = self.running.clone();
        let sandbox = self.sandbox.take();
        let mcp_server = self.mcp_server.take();
        let mcp_token = self.mcp_token.take();
        let handle = self.handle.take();
        self.disarmed = true;

        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                if let (Some(sandbox), Some(handle)) = (sandbox.as_ref(), handle) {
                    if let Err(e) = sandbox.backend().release(&handle).await {
                        tracing::warn!(run_id = %run_id, %e, "failed to release orchestrator container during drop cleanup");
                    }
                }
                if let (Some(mcp_server), Some(token)) = (mcp_server.as_ref(), mcp_token) {
                    mcp_server.revoke_session(&token).await;
                }
                *running.lock().await = None;
                tracing::warn!(run_id = %run_id, "orchestrator run cleanup executed from drop");
            });
        } else {
            tracing::warn!(run_id = %run_id, "tokio runtime unavailable; orchestrator drop cleanup could not be scheduled");
        }
    }
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

    /// Append an extra setup script line that runs inside the sandbox before the agent starts.
    pub fn add_setup_line(&mut self, line: impl Into<String>) {
        self.config.extra_setup_lines.push(line.into());
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
        self.run_with_tags(instruction, trigger, HashMap::new())
            .await
    }

    /// Run the orchestrator agent with structured log tags attached to sandbox events.
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
        let mut cleanup_guard = RunCleanupGuard::new(
            run_id.clone(),
            self.running.clone(),
            self.sandbox.clone(),
            self.mcp_server.clone(),
            mcp_token.clone(),
        );

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
            no_progress_timeout_seconds: 180,
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
                cleanup_guard.cleanup_now().await;
                return Some(Err(e));
            }
        };
        cleanup_guard.set_handle(handle.clone());

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

        // ── Cleanup ──────────────────────────────────────────────────────
        cleanup_guard.cleanup_now().await;

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

    fn build_setup_script(&self, mcp_token: &str) -> String {
        let mcp_host_url = &self.config.mcp_host_url;
        let mcp_namespace = &self.config.mcp_namespace;
        let mcp_base =
            if mcp_host_url.starts_with("http://") || mcp_host_url.starts_with("https://") {
                mcp_host_url.clone()
            } else {
                format!("http://{mcp_host_url}")
            };

        let mut lines = Vec::new();

        // Write agent-specific MCP config so the sandboxed agent can reach our tool server.
        match self.config.agent {
            AgentKind::Claude => {
                // Claude Code reads permissions from ~/.claude/settings.json and MCP
                // server definitions from .mcp.json in the working directory.
                lines.push("mkdir -p /root/.claude".to_string());
                lines.push(format!(
                    r#"cat > /root/.claude/settings.json << 'SETTINGS'
{{
  "permissions": {{
    "allow": ["Bash(*)", "Read(*)", "Write(*)", "Edit(*)", "Glob(*)", "Grep(*)", "WebFetch(*)", "WebSearch(*)", "mcp__{}"],
    "deny": []
  }}
}}
SETTINGS"#,
                    mcp_namespace,
                ));
                lines.push("mkdir -p /workspace".to_string());
                lines.push(format!(
                    r#"cat > /workspace/.mcp.json << 'MCPJSON'
{{
  "mcpServers": {{
    "{mcp_namespace}": {{
      "type": "http",
      "url": "{mcp_base}/mcp/{mcp_namespace}",
      "headers": {{
        "Authorization": "Bearer {mcp_token}"
      }}
    }}
  }}
}}
MCPJSON"#,
                ));
            }
            _ => {
                // Codex reads MCP config from ~/.codex/config.toml.
                lines.push("mkdir -p /root/.codex".to_string());
                // Set chatgpt_base_url in config.toml so LLM calls route through
                // the cache proxy even if OPENAI_BASE_URL env var is ignored.
                let chatgpt_base_url_line = self
                    .config
                    .env_vars
                    .get("OPENAI_BASE_URL")
                    .filter(|v| !v.is_empty())
                    .map(|v| format!("chatgpt_base_url = \"{v}\"\n"))
                    .unwrap_or_default();
                lines.push(format!(
                    r#"cat > /root/.codex/config.toml << 'TOML'
{chatgpt_base_url_line}[mcp_servers.{mcp_namespace}]
enabled = true
url = "{mcp_base}/mcp/{mcp_namespace}"
bearer_token_env_var = "ORCHESTRATOR_MCP_TOKEN"
tool_timeout_sec = 300
TOML"#,
                ));
            }
        }

        // Append extra setup lines (repo cloning, etc.)
        for line in &self.config.extra_setup_lines {
            lines.push(line.clone());
        }

        // Persist critical env vars into /tmp/smr_runtime_env.sh so the
        // sandbox-agent process (started in a separate Daytona exec call)
        // inherits them. The Daytona `env` field at sandbox creation may not
        // propagate to toolbox `process/execute` commands, so we need this
        // explicit persistence — mirroring what agent_context.rs does for
        // worker sandboxes.
        let persist_keys = ["OPENAI_BASE_URL", "ANTHROPIC_BASE_URL", "ANTHROPIC_API_KEY"];
        let mut env_exports = Vec::new();
        // ORCHESTRATOR_MCP_TOKEN is not yet in self.config.env_vars (added
        // in run_with_tags after this method returns), but it's available as
        // the mcp_token parameter.
        env_exports.push(format!("export ORCHESTRATOR_MCP_TOKEN=\"{mcp_token}\""));
        for key in &persist_keys {
            if let Some(val) = self.config.env_vars.get(*key) {
                if !val.is_empty() {
                    env_exports.push(format!("export {key}=\"{val}\""));
                }
            }
        }
        let env_block = env_exports.join("\n");
        lines.push(format!(
            "cat > /tmp/smr_runtime_env.sh << 'SMR_RUNTIME_ENV_EOF'\n\
             {env_block}\n\
             SMR_RUNTIME_ENV_EOF\n\
             chmod 0644 /tmp/smr_runtime_env.sh"
        ));

        lines.join("\n")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::{Duration, sleep};

    #[tokio::test]
    async fn run_cleanup_guard_drop_clears_running_state() {
        let running = Arc::new(Mutex::new(Some(RunningState {
            started_at: chrono::Utc::now(),
            trigger: serde_json::json!({"test": true}),
            run_id: "run_test".to_string(),
            container_id: None,
            host_port: None,
        })));

        {
            let _guard = RunCleanupGuard::new_for_test("run_test".to_string(), running.clone());
        }

        sleep(Duration::from_millis(25)).await;
        assert!(running.lock().await.is_none());
    }

    #[tokio::test]
    async fn run_cleanup_guard_cleanup_now_clears_running_state() {
        let running = Arc::new(Mutex::new(Some(RunningState {
            started_at: chrono::Utc::now(),
            trigger: serde_json::json!({"test": true}),
            run_id: "run_test".to_string(),
            container_id: None,
            host_port: None,
        })));

        let mut guard = RunCleanupGuard::new_for_test("run_test".to_string(), running.clone());
        guard.cleanup_now().await;
        assert!(running.lock().await.is_none());
    }
}
