//! Docker sandbox backend — provisions local Docker containers with sandbox-agent.
//!
//! This backend shells out to the `docker` CLI to manage container lifecycle.
//! It creates a container from the configured image, volume-mounts the custom
//! sandbox-agent binary, installs the coding agent, and exposes the sandbox-agent
//! HTTP server on a Docker-auto-assigned host port.

use crate::engine::models::{SandboxBackendKind, SandboxConfig, SandboxHandle};
use crate::engine::traits::SandboxBackend;
use crate::{Error, Result};
use async_trait::async_trait;
use std::path::PathBuf;
use std::time::Duration;
use tokio::process::Command;

/// Default Docker image used when the config doesn't specify one.
const DEFAULT_IMAGE: &str = "ubuntu:24.04";

/// Port that sandbox-agent listens on inside the container.
const SANDBOX_AGENT_PORT: u16 = 2468;

/// Maximum time to wait for sandbox-agent to become healthy after provisioning.
/// 600s to accommodate QEMU emulation + repo cloning + Docker CLI install.
const HEALTH_TIMEOUT: Duration = Duration::from_secs(600);

/// Interval between health check polls.
const HEALTH_POLL_INTERVAL: Duration = Duration::from_secs(2);

/// Docker-based sandbox backend.
///
/// Provisions containers using `docker run`, volume-mounts a custom-built
/// sandbox-agent binary with REST API support, and starts the server.
/// Cleanup happens via `docker rm -f`.
#[derive(Debug, Clone)]
pub struct DockerBackend {
    /// Optional Docker network to attach containers to.
    pub network: Option<String>,
    /// Optional path to the sandbox-agent binary on the host.
    /// If provided, it is volume-mounted into the container; otherwise
    /// sandbox-agent is downloaded inside the container via the setup script.
    pub sandbox_agent_bin: Option<String>,
}

impl DockerBackend {
    pub fn new(network: Option<String>) -> Self {
        let bin = std::env::var("SANDBOX_AGENT_BIN")
            .ok()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty());
        Self {
            network,
            sandbox_agent_bin: bin,
        }
    }

    /// Ensure a sandbox-agent binary is available for volume-mounting into containers.
    ///
    /// Resolution order:
    /// 1. `$HOME/.cache/horizons/sandbox-agent-x86_64-unknown-linux-gnu` (already cached)
    /// 2. Local cargo build: `$HOME/.cargo/git/checkouts/sandbox-agent-*/*/target/x86_64-unknown-linux-musl/release/sandbox-agent`
    /// 3. GitHub release download (strict)
    ///
    /// Must be called before wrapping `self` in `Arc`.
    pub async fn ensure_cached_binary(&mut self) -> Result<()> {
        // If already explicitly set via SANDBOX_AGENT_BIN, skip everything.
        if self.sandbox_agent_bin.is_some() {
            return Ok(());
        }

        let home = std::env::var("HOME").unwrap_or_else(|_| "/root".to_string());
        let cache_dir = PathBuf::from(&home).join(".cache/horizons");
        // Match the container architecture we run on local dev machines.
        //
        // - Apple Silicon + Docker Desktop typically runs linux/arm64 containers by default.
        // - Intel hosts run linux/amd64 containers by default.
        //
        // We allow overriding the platform at runtime (see provision()), but for local
        // runs the simplest path is "native containers + native sandbox-agent binary".
        let bin_name = if std::env::consts::ARCH == "aarch64" {
            "sandbox-agent-aarch64-unknown-linux-gnu"
        } else {
            "sandbox-agent-x86_64-unknown-linux-gnu"
        };
        let bin_path = cache_dir.join(bin_name);

        // 1. Already cached?
        if bin_path.exists() {
            tracing::info!(path = %bin_path.display(), "using cached sandbox-agent binary");
            self.sandbox_agent_bin = Some(bin_path.to_string_lossy().into_owned());
            return Ok(());
        }

        std::fs::create_dir_all(&cache_dir).map_err(|e| {
            Error::BackendMessage(format!(
                "failed to create cache dir {}: {e}",
                cache_dir.display()
            ))
        })?;

        // 2. Try to find a local cargo build (cross-compiled for linux-musl).
        let cargo_checkouts = PathBuf::from(&home).join(".cargo/git/checkouts");
        if cargo_checkouts.is_dir() {
            if let Some(local_bin) = Self::find_local_sandbox_agent(&cargo_checkouts) {
                tracing::info!(
                    source = %local_bin.display(),
                    dest = %bin_path.display(),
                    "copying locally-built sandbox-agent to cache"
                );
                if let Err(e) = std::fs::copy(&local_bin, &bin_path) {
                    tracing::warn!(%e, "failed to copy local sandbox-agent, will try download");
                } else {
                    #[cfg(unix)]
                    {
                        use std::os::unix::fs::PermissionsExt;
                        let _ = std::fs::set_permissions(
                            &bin_path,
                            std::fs::Permissions::from_mode(0o755),
                        );
                    }
                    self.sandbox_agent_bin = Some(bin_path.to_string_lossy().into_owned());
                    return Ok(());
                }
            }
        }

        // 3. Download from GitHub releases.
        tracing::info!(path = %bin_path.display(), "downloading sandbox-agent binary to host cache");
        let url = format!(
            "https://github.com/rivet-dev/sandbox-agent/releases/latest/download/{bin_name}"
        );
        let tmp_path = cache_dir.join(format!("{bin_name}.tmp"));
        let output = Command::new("curl")
            .args(["-fsSL", &url, "-o", &tmp_path.to_string_lossy()])
            .output()
            .await
            .map_err(|e| Error::backend("curl sandbox-agent", e))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(Error::BackendMessage(format!(
                "failed to download sandbox-agent: {stderr}"
            )));
        }

        // Make executable and atomically move into place.
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&tmp_path, std::fs::Permissions::from_mode(0o755))
                .map_err(|e| Error::BackendMessage(format!("chmod sandbox-agent: {e}")))?;
        }
        std::fs::rename(&tmp_path, &bin_path)
            .map_err(|e| Error::BackendMessage(format!("rename sandbox-agent into cache: {e}")))?;

        tracing::info!(path = %bin_path.display(), "sandbox-agent binary cached");
        self.sandbox_agent_bin = Some(bin_path.to_string_lossy().into_owned());
        Ok(())
    }

    /// Search cargo git checkouts for a locally cross-compiled sandbox-agent binary.
    fn find_local_sandbox_agent(cargo_checkouts: &std::path::Path) -> Option<PathBuf> {
        let entries = std::fs::read_dir(cargo_checkouts).ok()?;
        for entry in entries.flatten() {
            let name = entry.file_name();
            if !name.to_string_lossy().starts_with("sandbox-agent-") {
                continue;
            }
            // Walk into each checkout revision directory.
            if let Ok(revisions) = std::fs::read_dir(entry.path()) {
                for rev in revisions.flatten() {
                    let candidate = rev
                        .path()
                        .join("target/x86_64-unknown-linux-musl/release/sandbox-agent");
                    if candidate.is_file() {
                        return Some(candidate);
                    }
                }
            }
        }
        None
    }

    /// Query Docker for the host port auto-assigned to `SANDBOX_AGENT_PORT`.
    async fn query_assigned_port(container_id: &str) -> Result<u16> {
        let output = Command::new("docker")
            .args(["port", container_id, &SANDBOX_AGENT_PORT.to_string()])
            .output()
            .await
            .map_err(|e| Error::backend("docker port", e))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(Error::BackendMessage(format!(
                "docker port failed for {container_id}: {stderr}"
            )));
        }

        // Output looks like "0.0.0.0:32768" or ":::32768" — grab the last `:PORT`.
        let stdout = String::from_utf8_lossy(&output.stdout);
        let port_str = stdout
            .trim()
            .lines()
            .next()
            .and_then(|line| line.rsplit(':').next())
            .ok_or_else(|| {
                Error::BackendMessage(format!("unexpected docker port output: {stdout}"))
            })?;

        port_str
            .parse::<u16>()
            .map_err(|e| Error::BackendMessage(format!("invalid port number '{port_str}': {e}")))
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

    /// Build the setup script that installs the agent and starts sandbox-agent.
    fn setup_script(config: &SandboxConfig) -> String {
        let agent = config.agent.as_sandbox_agent_str();
        let no_token_flag = "--no-token";

        let docker_cli_install = if config.docker_socket {
            r#"
# Install Docker CLI (socket is mounted from host).
curl -fsSL https://download.docker.com/linux/static/stable/x86_64/docker-27.5.1.tgz \
  | tar xz --strip-components=1 -C /usr/local/bin docker/docker 2>/dev/null || true
# Install Docker Compose plugin.
mkdir -p /usr/local/lib/docker/cli-plugins
curl -fsSL "https://github.com/docker/compose/releases/latest/download/docker-compose-linux-x86_64" \
  -o /usr/local/lib/docker/cli-plugins/docker-compose 2>/dev/null && \
  chmod +x /usr/local/lib/docker/cli-plugins/docker-compose || true
"#
        } else {
            ""
        };

        format!(
            r#"
set -e
apt-get update -qq && apt-get install -y -qq curl ca-certificates git > /dev/null 2>&1
{docker_cli_install}
# Install sandbox-agent if it's not already present.
# Note: we force `--platform linux/amd64` on Apple Silicon, so `uname -m` inside
# the container should match the published binary arch (typically x86_64).
if ! command -v sandbox-agent >/dev/null 2>&1; then
  curl -fsSL "https://github.com/rivet-dev/sandbox-agent/releases/latest/download/sandbox-agent-$(uname -m)-unknown-linux-gnu" \
    -o /usr/local/bin/sandbox-agent
  chmod +x /usr/local/bin/sandbox-agent
fi

sandbox-agent install-agent {agent} 2>&1 || true
if [ "{agent}" = "codex" ] && [ -n "${{OPENAI_API_KEY:-}}" ]; then
  if [ -x "/root/.local/share/sandbox-agent/bin/codex" ]; then
    printf '%s' "$OPENAI_API_KEY" | /root/.local/share/sandbox-agent/bin/codex login --with-api-key > /dev/null 2>&1 || true
  fi
  # Strict: write auth.json directly in case codex login failed silently (e.g. under QEMU)
  mkdir -p /root/.codex
  printf '{{"OPENAI_API_KEY": "%s"}}' "$OPENAI_API_KEY" > /root/.codex/auth.json
fi

# Configure git identity for commits.
git config --global user.email "horizons-bot@synth.dev"
git config --global user.name "Horizons Bot"

# Set up GitHub CLI auth if GITHUB_TOKEN is available.
if [ -n "${{GITHUB_TOKEN:-}}" ]; then
  (curl -fsSL https://cli.github.com/packages/githubcli-archive-keyring.gpg \
    | dd of=/usr/share/keyrings/githubcli-archive-keyring.gpg 2>/dev/null) || true
  echo "deb [arch=amd64 signed-by=/usr/share/keyrings/githubcli-archive-keyring.gpg] https://cli.github.com/packages stable main" \
    > /etc/apt/sources.list.d/github-cli.list 2>/dev/null || true
  apt-get update -qq && apt-get install -y -qq gh > /dev/null 2>&1 || true
  echo "$GITHUB_TOKEN" | gh auth login --with-token 2>/dev/null || true
fi

# Run custom setup script if provided (clone repos, install deps, etc.)
if [ -n "${{SANDBOX_SETUP_SCRIPT:-}}" ]; then
  echo ">>> Running custom setup script..."
  eval "$SANDBOX_SETUP_SCRIPT" 2>&1 || echo ">>> WARNING: setup script failed (continuing anyway)"
  echo ">>> Custom setup complete."
fi

sandbox-agent server {no_token_flag} --host 0.0.0.0 --port {SANDBOX_AGENT_PORT} &
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

/// Capture the last `tail` lines of logs from a Docker container.
///
/// Best-effort: returns an empty string on any failure. Public so callers
/// (e.g. `orchestrator_agent_runtime`) can grab logs before cleanup.
pub async fn capture_container_logs(container_id: &str, tail: usize) -> String {
    let result = Command::new("docker")
        .args(["logs", "--tail", &tail.to_string(), container_id])
        .output()
        .await;

    match result {
        Ok(output) => {
            let stdout = String::from_utf8_lossy(&output.stdout);
            let stderr = String::from_utf8_lossy(&output.stderr);
            format!("{stdout}{stderr}")
        }
        Err(_) => String::new(),
    }
}

#[async_trait]
impl SandboxBackend for DockerBackend {
    /// Provision a new Docker container with sandbox-agent installed and running.
    #[tracing::instrument(level = "info", skip(self, config), fields(agent = %config.agent))]
    async fn provision(&self, config: &SandboxConfig) -> Result<SandboxHandle> {
        let image = config.image.as_deref().unwrap_or(DEFAULT_IMAGE);
        let setup = Self::setup_script(config);

        let mut args = vec![
            "run".to_string(),
            "-d".to_string(),
            "--rm".to_string(),
            // Let Docker auto-assign a free host port (avoids collisions).
            "-p".to_string(),
            format!("0:{SANDBOX_AGENT_PORT}"),
        ];

        // Optionally volume-mount a custom sandbox-agent binary.
        if let Some(ref bin_path) = self.sandbox_agent_bin {
            args.push("-v".to_string());
            args.push(format!("{bin_path}:/usr/local/bin/sandbox-agent:ro"));
        }

        // Mount host Docker socket so the agent can run docker/compose commands.
        if config.docker_socket {
            args.push("-v".to_string());
            args.push("/var/run/docker.sock:/var/run/docker.sock".to_string());
        }

        // Optional override (useful for debugging or running mixed-arch images).
        // Default: run the host's native container architecture.
        let platform = std::env::var("HORIZONS_DOCKER_PLATFORM")
            .ok()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty());
        if let Some(p) = platform {
            args.push("--platform".to_string());
            args.push(p);
        }

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

        tracing::info!(?image, "provisioning docker container");

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

        // Query the auto-assigned host port.
        let host_port = Self::query_assigned_port(&container_id).await?;

        tracing::info!(%container_id, %host_port, "container provisioned (port auto-assigned)");

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
    #[tracing::instrument(level = "info", skip(self, handle), fields(container = %handle.id, host_port = handle.host_port))]
    async fn wait_ready(&self, handle: &SandboxHandle) -> Result<()> {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
            .map_err(|e| Error::backend("build health client", e))?;

        let health_url = format!("{}/v1/health", handle.sandbox_agent_url);
        let start = std::time::Instant::now();
        let deadline = tokio::time::Instant::now() + HEALTH_TIMEOUT;
        let mut last_progress_log = std::time::Instant::now();
        let progress_interval = Duration::from_secs(30);
        let mut retry_count: u64 = 0;

        loop {
            if tokio::time::Instant::now() > deadline {
                let elapsed = start.elapsed().as_secs();
                let logs = capture_container_logs(&handle.id, 50).await;
                return Err(Error::BackendMessage(format!(
                    "sandbox-agent health check timed out after {elapsed}s for container {} \
                     (host_port={})\n--- container logs (last 50 lines) ---\n{logs}",
                    handle.id, handle.host_port,
                )));
            }

            match client.get(&health_url).send().await {
                Ok(resp) if resp.status().is_success() => {
                    let elapsed = start.elapsed().as_secs();
                    tracing::info!(
                        elapsed_secs = elapsed,
                        retries = retry_count,
                        "sandbox-agent is healthy"
                    );
                    return Ok(());
                }
                Ok(resp) => {
                    retry_count += 1;
                    if retry_count == 1 || retry_count % 10 == 0 {
                        tracing::debug!(
                            retries = retry_count,
                            status = %resp.status(),
                            "health check not ready yet"
                        );
                    }
                }
                Err(e) => {
                    retry_count += 1;
                    if retry_count == 1 || retry_count % 10 == 0 {
                        tracing::debug!(
                            retries = retry_count,
                            %e,
                            "health check connection error, retrying"
                        );
                    }
                }
            }

            // Log progress every 30s so operators can see the wait is active.
            if last_progress_log.elapsed() >= progress_interval {
                let elapsed = start.elapsed().as_secs();
                let timeout = HEALTH_TIMEOUT.as_secs();
                tracing::info!(
                    retries = retry_count,
                    "still waiting for sandbox-agent health check ({elapsed}s / {timeout}s)"
                );
                last_progress_log = std::time::Instant::now();
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
