use crate::Result;
use crate::engine::models::{EnvHandle, EnvSpec, Observation, SandboxConfig, SandboxHandle};
use crate::models::AgentIdentity;
use async_trait::async_trait;

#[async_trait]
pub trait RhodesAdapter: Send + Sync {
    async fn provision(&self, env_spec: EnvSpec, identity: &AgentIdentity) -> Result<EnvHandle>;

    async fn run_step(
        &self,
        env_handle: &EnvHandle,
        action: serde_json::Value,
    ) -> Result<Observation>;

    async fn release(&self, env_handle: &EnvHandle) -> Result<()>;

    async fn health(&self, env_handle: &EnvHandle) -> Result<bool>;
}

// ---------------------------------------------------------------------------
// Sandbox backend — container lifecycle for sandbox-agent
// ---------------------------------------------------------------------------

/// Provisions and manages container environments for sandbox-agent.
///
/// Implementations handle the full lifecycle: create a container, install
/// sandbox-agent inside it, wait until the HTTP server is healthy, and tear
/// the container down when done.
#[async_trait]
pub trait SandboxBackend: Send + Sync {
    /// Spin up a new container and install sandbox-agent.  Returns a handle
    /// that can be used for subsequent operations.
    async fn provision(&self, config: &SandboxConfig) -> Result<SandboxHandle>;

    /// Block until the sandbox-agent HTTP server inside the container responds
    /// to health checks.
    async fn wait_ready(&self, handle: &SandboxHandle) -> Result<()>;

    /// Destroy the container and clean up resources.
    async fn release(&self, handle: &SandboxHandle) -> Result<()>;
}
