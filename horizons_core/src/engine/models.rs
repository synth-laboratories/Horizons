use crate::{Error, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EnvTemplate {
    HarborCoding,
    HarborBrowser,
    Archipelago,
    OpenEnv,
}

// ---------------------------------------------------------------------------
// Sandbox runtime models
// ---------------------------------------------------------------------------

/// Which coding agent to run inside the sandbox.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentKind {
    Codex,
    Claude,
    OpenCode,
}

impl AgentKind {
    /// The identifier expected by the sandbox-agent HTTP API.
    pub fn as_sandbox_agent_str(&self) -> &'static str {
        match self {
            Self::Codex => "codex",
            Self::Claude => "claude",
            Self::OpenCode => "opencode",
        }
    }
}

impl std::fmt::Display for AgentKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_sandbox_agent_str())
    }
}

/// How the sandbox-agent should handle permission requests from the coding agent.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PermissionMode {
    /// Auto-approve everything (no human in the loop).
    Bypass,
    /// Present permissions for approval via the Horizons action pipeline.
    Plan,
}

impl PermissionMode {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Bypass => "bypass",
            Self::Plan => "plan",
        }
    }
}

/// Which sandbox backend to use for provisioning.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SandboxBackendKind {
    Docker,
    Daytona,
}

/// Configuration for provisioning a single sandbox.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SandboxConfig {
    /// Which agent to run.
    pub agent: AgentKind,
    /// LLM model override (e.g. `"gpt-5-nano"`, `"claude-sonnet-4-20250514"`).
    pub model: Option<String>,
    /// Permission handling mode.
    pub permission_mode: PermissionMode,
    /// Docker image to use (default: `ubuntu:24.04`).
    pub image: Option<String>,
    /// Environment variables to inject into the container (typically API keys).
    pub env_vars: HashMap<String, String>,
    /// Maximum seconds the agent session may run before timeout.
    pub timeout_seconds: u64,
    /// Working directory inside the container where task files reside.
    pub workdir: Option<String>,

    /// Mount the host Docker socket into the container so the agent can run
    /// Docker commands (build, compose, etc.) via the host daemon.
    #[serde(default)]
    pub docker_socket: bool,

    /// Optional restart policy for long-running sandboxes started via `start_agent_tracked`.
    /// One-shot runs (`run_agent`) do not use this.
    #[serde(default)]
    pub restart_policy: Option<RestartPolicy>,

    /// Optional structured tags for correlating sandbox session events in external sinks
    /// (e.g. `run_id`, `project_id`, `task_key`, etc).
    ///
    /// These are NOT injected into the container environment; they are only used by the
    /// host-side runtime when teeing universal events to sinks like VictoriaLogs.
    #[serde(default)]
    pub log_tags: HashMap<String, String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RestartPolicy {
    /// Maximum number of restarts for a run. `0` disables restarts.
    pub max_restarts: u32,
    /// Base backoff in milliseconds (exponential).
    pub backoff_ms: u64,
}

impl Default for SandboxConfig {
    fn default() -> Self {
        Self {
            agent: AgentKind::Codex,
            model: None,
            permission_mode: PermissionMode::Bypass,
            image: None,
            env_vars: HashMap::new(),
            timeout_seconds: 1800,
            workdir: None,
            docker_socket: false,
            restart_policy: None,
            log_tags: HashMap::new(),
        }
    }
}

/// Opaque handle to a provisioned sandbox.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SandboxHandle {
    /// Unique identifier (container ID for Docker, sandbox ID for Daytona).
    pub id: String,
    /// The base URL of the sandbox-agent server inside the container.
    pub sandbox_agent_url: String,
    /// Which backend provisioned this handle.
    pub backend: SandboxBackendKind,
    /// Port on the host that maps to sandbox-agent inside the container.
    pub host_port: u16,
    /// Additional backend-specific metadata.
    pub metadata: serde_json::Value,
}

/// Collected results from a completed sandbox agent session.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SandboxResult {
    /// All events emitted during the session (universal schema JSON).
    pub events: Vec<serde_json::Value>,
    /// Whether the session ended normally (vs timeout/error).
    pub completed: bool,
    /// Total duration in seconds.
    pub duration_seconds: f64,
    /// Final text output from the agent, if extractable.
    pub final_output: Option<String>,
    /// Error message if the session failed.
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EnvSpec {
    pub template: EnvTemplate,
    pub config: serde_json::Value,
    pub pool_id: Option<String>,
}

impl EnvSpec {
    #[tracing::instrument(level = "debug", skip(config))]
    pub fn new(
        template: EnvTemplate,
        config: serde_json::Value,
        pool_id: Option<String>,
    ) -> Result<Self> {
        if let Some(p) = &pool_id {
            if p.trim().is_empty() {
                return Err(Error::InvalidInput("pool_id is empty".to_string()));
            }
        }
        Ok(Self {
            template,
            config,
            pool_id,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EnvHandle {
    pub id: String,
    pub template: EnvTemplate,
    pub pool_id: Option<String>,
    pub lease_expires_at: Option<DateTime<Utc>>,
    pub metadata: serde_json::Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Observation {
    pub output: serde_json::Value,
    pub reward: Option<f64>,
    pub done: bool,
    pub artifacts: Vec<serde_json::Value>,
}
