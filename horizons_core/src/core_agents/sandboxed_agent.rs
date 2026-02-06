//! SandboxedAgent: bridges the CoreAgentsExecutor to the SandboxRuntime.
//!
//! Implements `AgentSpec` (the trait) so it can be registered with the executor
//! and run like any other agent. When executed, it provisions a sandbox,
//! runs the coding agent inside, and converts the output into `ActionProposal`s.

use crate::Result;
use crate::core_agents::models::{ActionProposal, AgentContext, RiskLevel};
use crate::core_agents::traits::AgentSpec;
use crate::engine::models::{AgentKind, PermissionMode, SandboxConfig};
use crate::engine::sandbox_runtime::SandboxRuntime;
use async_trait::async_trait;
use chrono::Utc;
use std::collections::HashMap;
use std::sync::Arc;

/// A sandboxed coding agent that runs in a Docker/Daytona container via sandbox-agent.
///
/// This struct wraps a `SandboxRuntime` and the configuration for a specific
/// agent (which coding agent, model, permissions, etc). When `run()` is called,
/// it provisions a sandbox, executes the instruction, collects results, and
/// returns them as `ActionProposal`s that go through the approval pipeline.
pub struct SandboxedAgent {
    /// Unique identifier for this agent instance.
    agent_id: String,
    /// Human-readable name.
    agent_name: String,
    /// Which coding agent to run (Codex, Claude, OpenCode).
    agent_kind: AgentKind,
    /// LLM model override.
    model: Option<String>,
    /// Permission handling mode.
    permission_mode: PermissionMode,
    /// Docker image override.
    image: Option<String>,
    /// Environment variables to inject (API keys, etc).
    env_vars: HashMap<String, String>,
    /// Timeout in seconds.
    timeout_seconds: u64,
    /// Working directory inside the container.
    workdir: Option<String>,
    /// The sandbox runtime that handles container orchestration.
    runtime: Arc<SandboxRuntime>,
}

impl SandboxedAgent {
    pub fn new(
        agent_id: impl Into<String>,
        agent_name: impl Into<String>,
        agent_kind: AgentKind,
        runtime: Arc<SandboxRuntime>,
    ) -> Self {
        Self {
            agent_id: agent_id.into(),
            agent_name: agent_name.into(),
            agent_kind,
            model: None,
            permission_mode: PermissionMode::Bypass,
            image: None,
            env_vars: HashMap::new(),
            timeout_seconds: 1800,
            workdir: None,
            runtime,
        }
    }

    /// Set the LLM model override.
    pub fn with_model(mut self, model: impl Into<String>) -> Self {
        self.model = Some(model.into());
        self
    }

    /// Set the permission mode.
    pub fn with_permission_mode(mut self, mode: PermissionMode) -> Self {
        self.permission_mode = mode;
        self
    }

    /// Set the Docker image.
    pub fn with_image(mut self, image: impl Into<String>) -> Self {
        self.image = Some(image.into());
        self
    }

    /// Add environment variables (e.g. API keys).
    pub fn with_env_vars(mut self, vars: HashMap<String, String>) -> Self {
        self.env_vars = vars;
        self
    }

    /// Set the timeout in seconds.
    pub fn with_timeout(mut self, seconds: u64) -> Self {
        self.timeout_seconds = seconds;
        self
    }

    /// Set the working directory inside the container.
    pub fn with_workdir(mut self, workdir: impl Into<String>) -> Self {
        self.workdir = Some(workdir.into());
        self
    }

    /// Build the `SandboxConfig` from this agent's settings.
    fn build_config(&self) -> SandboxConfig {
        let mut env_vars = self.env_vars.clone();
        // If an MCP gateway is configured in the parent environment, pass it through
        // to the sandbox so in-sandbox tools can proxy MCP calls.
        if !env_vars.contains_key("MCP_GATEWAY_URL") {
            let url = std::env::var("MCP_GATEWAY_URL")
                .or_else(|_| std::env::var("HORIZONS_MCP_GATEWAY_URL"))
                .ok();
            if let Some(url) = url {
                if !url.trim().is_empty() {
                    env_vars.insert("MCP_GATEWAY_URL".to_string(), url);
                }
            }
        }

        SandboxConfig {
            agent: self.agent_kind,
            model: self.model.clone(),
            permission_mode: self.permission_mode,
            image: self.image.clone(),
            env_vars,
            timeout_seconds: self.timeout_seconds,
            workdir: self.workdir.clone(),
            restart_policy: None,
        }
    }
}

#[async_trait]
impl AgentSpec for SandboxedAgent {
    async fn id(&self) -> String {
        self.agent_id.clone()
    }

    async fn name(&self) -> String {
        self.agent_name.clone()
    }

    /// Run the sandboxed coding agent and return action proposals.
    ///
    /// The `inputs` JSON may contain an `"instruction"` field which overrides
    /// the default instruction. If not provided, a generic instruction is used.
    #[tracing::instrument(level = "info", skip(self, ctx, inputs), fields(agent_id = %self.agent_id, agent_kind = %self.agent_kind))]
    async fn run(
        &self,
        ctx: AgentContext,
        inputs: Option<serde_json::Value>,
    ) -> Result<Vec<ActionProposal>> {
        // Extract instruction from inputs.
        let instruction = inputs
            .as_ref()
            .and_then(|v| v.get("instruction"))
            .and_then(|v| v.as_str())
            .unwrap_or("Complete the assigned task.")
            .to_string();

        let config = self.build_config();

        // Run the agent in the sandbox.
        let (result, handle) = self.runtime.run_agent(&config, &instruction).await?;

        // Convert the sandbox result into an ActionProposal.
        let now = Utc::now();
        let payload = serde_json::json!({
            "sandbox_handle_id": handle.id,
            "sandbox_backend": handle.backend,
            "agent_kind": self.agent_kind,
            "completed": result.completed,
            "duration_seconds": result.duration_seconds,
            "event_count": result.events.len(),
            "final_output": result.final_output,
            "error": result.error,
        });

        let dedupe_key = format!(
            "sandbox:{}:{}:{}",
            self.agent_id,
            handle.id,
            now.timestamp_millis()
        );

        // Build the context with the project DB handle for the executor.
        let context = serde_json::json!({
            "_project_db_handle": ctx.project_db,
            "sandbox_events": result.events,
        });

        let risk = if result.completed {
            RiskLevel::Low
        } else {
            RiskLevel::Medium
        };

        let proposal = ActionProposal::new(
            ctx.org_id,
            ctx.project_id,
            &self.agent_id,
            "agent.sandbox.result",
            payload,
            risk,
            Some(dedupe_key),
            context,
            now,
            3600, // 1 hour TTL
        )?;

        Ok(vec![proposal])
    }
}
