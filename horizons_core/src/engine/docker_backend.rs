//! Docker sandbox backend — provisions local Docker containers with sandbox-agent.
//!
//! This backend shells out to the `docker` CLI to manage container lifecycle.
//! It creates a container from the configured image, installs sandbox-agent
//! inside it, and exposes the sandbox-agent HTTP server on a random host port.

use crate::engine::models::{SandboxBackendKind, SandboxConfig, SandboxHandle};
use crate::engine::traits::SandboxBackend;
use crate::{Error, Result};
use async_trait::async_trait;
use std::time::Duration;
use tokio::process::Command;

/// Default Docker image used when the config doesn't specify one.
const DEFAULT_IMAGE: &str = "ubuntu:24.04";

/// Port that sandbox-agent listens on inside the container.
const SANDBOX_AGENT_PORT: u16 = 2468;

/// Maximum time to wait for sandbox-agent to become healthy after provisioning.
const HEALTH_TIMEOUT: Duration = Duration::from_secs(120);

/// Interval between health check polls.
const HEALTH_POLL_INTERVAL: Duration = Duration::from_secs(2);

/// Sandbox-agent install script run inside the container.
/// Downloads the latest release binary and makes it executable.
const INSTALL_SCRIPT: &str = r#"
set -e
apt-get update -qq && apt-get install -y -qq curl > /dev/null 2>&1
curl -fsSL https://github.com/rivet-dev/sandbox-agent/releases/latest/download/sandbox-agent-$(uname -m)-unknown-linux-gnu -o /usr/local/bin/sandbox-agent
chmod +x /usr/local/bin/sandbox-agent
"#;

/// Docker-based sandbox backend.
///
/// Provisions containers using `docker run`, installs sandbox-agent via curl,
/// and starts the sandbox-agent server. Cleanup happens via `docker rm -f`.
#[derive(Debug, Clone)]
pub struct DockerBackend {
    /// Optional Docker network to attach containers to.
    pub network: Option<String>,
}

impl DockerBackend {
    pub fn new(network: Option<String>) -> Self {
        Self { network }
    }

    /// Find a random free port on the host for port mapping.
    fn random_host_port() -> u16 {
        // Use a high-range ephemeral port. In production this would use a
        // proper port allocator; for dev/local use, random in 30000-60000 suffices.
        use rand::Rng;
        let mut rng = rand::thread_rng();
        rng.gen_range(30000..60000)
    }

    /// Build the environment variable flags for `docker run`.
    fn env_flags(config: &SandboxConfig) -> Vec<String> {
        let mut flags = Vec::new();
        for (key, value) in &config.env_vars {
            flags.push("-e".to_string());
            flags.push(format!("{key}={value}"));
        }
        flags
    }

    /// Build the setup script that installs sandbox-agent and starts it.
    fn setup_script(config: &SandboxConfig) -> String {
        let agent = config.agent.as_sandbox_agent_str();
        let permission_mode = config.permission_mode.as_str();
        let no_token_flag = "--no-token";

        // Install sandbox-agent, install the agent, then start the server.
        format!(
            r#"
{INSTALL_SCRIPT}
sandbox-agent install {agent} 2>&1 || true
sandbox-agent start {no_token_flag} --permission-mode {permission_mode} &
# Wait for the server to be listening
for i in $(seq 1 30); do
    if curl -sf http://127.0.0.1:{SANDBOX_AGENT_PORT}/v1/health > /dev/null 2>&1; then
        break
    fi
    sleep 1
done
# Keep container alive
tail -f /dev/null
"#,
        )
    }
}

#[async_trait]
impl SandboxBackend for DockerBackend {
    /// Provision a new Docker container with sandbox-agent installed and running.
    #[tracing::instrument(level = "info", skip(self, config), fields(agent = %config.agent))]
    async fn provision(&self, config: &SandboxConfig) -> Result<SandboxHandle> {
        let image = config.image.as_deref().unwrap_or(DEFAULT_IMAGE);
        let host_port = Self::random_host_port();
        let setup = Self::setup_script(config);

        let mut args = vec![
            "run".to_string(),
            "-d".to_string(),
            "--rm".to_string(),
            "-p".to_string(),
            format!("{host_port}:{SANDBOX_AGENT_PORT}"),
        ];

        // Attach to Docker network if configured.
        if let Some(net) = &self.network {
            args.push("--network".to_string());
            args.push(net.clone());
        }

        // Set working directory if specified.
        if let Some(workdir) = &config.workdir {
            args.push("-w".to_string());
            args.push(workdir.clone());
        }

        // Add environment variables.
        args.extend(Self::env_flags(config));

        // Image and entrypoint.
        args.push(image.to_string());
        args.push("/bin/bash".to_string());
        args.push("-c".to_string());
        args.push(setup);

        tracing::info!(?host_port, ?image, "provisioning docker container");

        let output = Command::new("docker")
            .args(&args)
            .output()
            .await
            .map_err(|e| Error::backend("docker run", e))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(Error::BackendMessage(format!(
                "docker run failed: {stderr}"
            )));
        }

        let container_id = String::from_utf8_lossy(&output.stdout).trim().to_string();
        if container_id.is_empty() {
            return Err(Error::BackendMessage(
                "docker run returned empty container ID".to_string(),
            ));
        }

        tracing::info!(%container_id, %host_port, "container provisioned");

        Ok(SandboxHandle {
            id: container_id,
            sandbox_agent_url: format!("http://127.0.0.1:{host_port}"),
            backend: SandboxBackendKind::Docker,
            host_port,
            metadata: serde_json::json!({
                "image": image,
                "agent": config.agent.as_sandbox_agent_str(),
            }),
        })
    }

    /// Poll the sandbox-agent health endpoint until it responds.
    #[tracing::instrument(level = "debug", skip(self, handle), fields(container = %handle.id))]
    async fn wait_ready(&self, handle: &SandboxHandle) -> Result<()> {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
            .map_err(|e| Error::backend("build health client", e))?;

        let health_url = format!("{}/v1/health", handle.sandbox_agent_url);
        let deadline = tokio::time::Instant::now() + HEALTH_TIMEOUT;

        loop {
            if tokio::time::Instant::now() > deadline {
                return Err(Error::BackendMessage(format!(
                    "sandbox-agent health check timed out for container {}",
                    handle.id,
                )));
            }

            match client.get(&health_url).send().await {
                Ok(resp) if resp.status().is_success() => {
                    tracing::info!("sandbox-agent is healthy");
                    return Ok(());
                }
                Ok(resp) => {
                    tracing::debug!(status = %resp.status(), "health check not ready yet");
                }
                Err(e) => {
                    tracing::debug!(%e, "health check connection error, retrying");
                }
            }

            tokio::time::sleep(HEALTH_POLL_INTERVAL).await;
        }
    }

    /// Force-remove the Docker container.
    #[tracing::instrument(level = "info", skip(self, handle), fields(container = %handle.id))]
    async fn release(&self, handle: &SandboxHandle) -> Result<()> {
        let output = Command::new("docker")
            .args(["rm", "-f", &handle.id])
            .output()
            .await
            .map_err(|e| Error::backend("docker rm", e))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            tracing::warn!(%stderr, "docker rm -f returned non-zero (container may already be gone)");
        }

        tracing::info!("container released");
        Ok(())
    }
}
