//! Daytona sandbox backend — provisions sandboxes via the Daytona REST API.
//!
//! Daytona provides remote development environments with a REST API for
//! creating, managing, and destroying sandboxes. This backend uses that API
//! to provision containers, install sandbox-agent, and expose the agent server.

use crate::engine::models::{SandboxBackendKind, SandboxConfig, SandboxHandle};
use crate::engine::traits::SandboxBackend;
use crate::{Error, Result};
use async_trait::async_trait;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Default Daytona API base URL.
const DEFAULT_DAYTONA_API_URL: &str = "http://localhost:3986";

/// Port sandbox-agent listens on inside the Daytona sandbox.
const SANDBOX_AGENT_PORT: u16 = 2468;

/// Maximum time to wait for sandbox-agent health check.
const HEALTH_TIMEOUT: Duration = Duration::from_secs(180);

/// Interval between health check polls.
const HEALTH_POLL_INTERVAL: Duration = Duration::from_secs(3);

// ---------------------------------------------------------------------------
// Daytona API types (subset needed for sandbox provisioning)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct CreateSandboxRequest {
    image: String,
    env_vars: std::collections::HashMap<String, String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    labels: Option<std::collections::HashMap<String, String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    public: Option<bool>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SandboxResponse {
    id: String,
    #[serde(default)]
    #[allow(dead_code)]
    state: String,
}

/// Daytona-based sandbox backend.
///
/// Communicates with the Daytona REST API to provision sandbox environments,
/// install sandbox-agent inside them, and manage their lifecycle.
#[derive(Debug, Clone)]
pub struct DaytonaBackend {
    /// Daytona API base URL.
    api_url: String,
    /// Daytona API key for authentication.
    api_key: String,
    /// HTTP client.
    http: Client,
}

impl DaytonaBackend {
    pub fn new(api_url: Option<String>, api_key: String) -> Self {
        let http = Client::builder()
            .timeout(Duration::from_secs(60))
            .build()
            .expect("failed to build reqwest client");
        Self {
            api_url: api_url.unwrap_or_else(|| DEFAULT_DAYTONA_API_URL.to_string()),
            api_key,
            http,
        }
    }

    /// Execute a command inside a Daytona sandbox.
    async fn exec_in_sandbox(&self, sandbox_id: &str, command: &str) -> Result<String> {
        let url = format!("{}/api/sandboxes/{sandbox_id}/exec", self.api_url);
        let body = serde_json::json!({
            "command": command,
        });
        let resp = self
            .http
            .post(&url)
            .bearer_auth(&self.api_key)
            .json(&body)
            .send()
            .await
            .map_err(Error::backend_reqwest)?;

        if !resp.status().is_success() {
            let text = resp.text().await.unwrap_or_default();
            return Err(Error::BackendMessage(format!(
                "daytona exec failed: {text}"
            )));
        }
        resp.text().await.map_err(Error::backend_reqwest)
    }

    /// Get the preview URL for a port on a Daytona sandbox.
    async fn get_preview_url(&self, sandbox_id: &str, port: u16) -> Result<String> {
        let url = format!("{}/api/sandboxes/{sandbox_id}/preview/{port}", self.api_url);
        let resp = self
            .http
            .get(&url)
            .bearer_auth(&self.api_key)
            .send()
            .await
            .map_err(Error::backend_reqwest)?;

        if !resp.status().is_success() {
            // Fall back to constructing a URL from the sandbox ID.
            return Ok(format!("https://{port}-{sandbox_id}.preview.daytona.app"));
        }

        #[derive(Deserialize)]
        struct Preview {
            url: String,
        }
        let preview: Preview = resp.json().await.map_err(Error::backend_reqwest)?;
        Ok(preview.url)
    }
}

#[async_trait]
impl SandboxBackend for DaytonaBackend {
    /// Provision a sandbox via the Daytona API and install sandbox-agent.
    #[tracing::instrument(level = "info", skip(self, config), fields(agent = %config.agent))]
    async fn provision(&self, config: &SandboxConfig) -> Result<SandboxHandle> {
        let image = config
            .image
            .clone()
            .unwrap_or_else(|| "ubuntu:24.04".to_string());

        let mut env_vars = config.env_vars.clone();
        // Ensure the sandbox knows which port to expose.
        env_vars.insert(
            "SANDBOX_AGENT_PORT".to_string(),
            SANDBOX_AGENT_PORT.to_string(),
        );

        let mut labels = std::collections::HashMap::new();
        labels.insert(
            "horizons.agent".to_string(),
            config.agent.as_sandbox_agent_str().to_string(),
        );

        let body = CreateSandboxRequest {
            image,
            env_vars,
            labels: Some(labels),
            public: Some(true),
        };

        let url = format!("{}/api/sandboxes", self.api_url);
        let resp = self
            .http
            .post(&url)
            .bearer_auth(&self.api_key)
            .json(&body)
            .send()
            .await
            .map_err(Error::backend_reqwest)?;

        if !resp.status().is_success() {
            let text = resp.text().await.unwrap_or_default();
            return Err(Error::BackendMessage(format!(
                "daytona create sandbox failed: {text}"
            )));
        }

        let sandbox: SandboxResponse = resp.json().await.map_err(Error::backend_reqwest)?;
        tracing::info!(sandbox_id = %sandbox.id, "daytona sandbox created");

        // Pre-start setup: write auth.json and execute SANDBOX_SETUP_SCRIPT.
        // Codex CLI `app-server` reads auth from ~/.codex/auth.json, NOT from
        // the OPENAI_API_KEY env var. Write the file so Codex can authenticate.
        self.exec_in_sandbox(&sandbox.id, "mkdir -p /root/.codex")
            .await?;

        if let Some(api_key) = config.env_vars.get("OPENAI_API_KEY") {
            // API keys are alphanumeric + hyphens/underscores, safe in double-quoted shell.
            // Use sh -c "..." (double quotes) to avoid the '\'' escaping that breaks
            // through the Daytona exec API's JSON command field.
            let auth_cmd = format!(
                r#"sh -c "printf '%s' '{{\"OPENAI_API_KEY\":\"{}\"}}' > /root/.codex/auth.json""#,
                api_key
            );
            self.exec_in_sandbox(&sandbox.id, &auth_cmd).await?;
            tracing::info!(sandbox_id = %sandbox.id, "wrote auth.json");
        }

        // Execute SANDBOX_SETUP_SCRIPT (writes MCP config) — the Daytona
        // backend has no entrypoint that evals this automatically.
        if config.env_vars.contains_key("SANDBOX_SETUP_SCRIPT") {
            let script_cmd = "sh -c 'if [ -n \"$SANDBOX_SETUP_SCRIPT\" ]; then eval \"$SANDBOX_SETUP_SCRIPT\" 2>&1 || echo \"WARNING: SANDBOX_SETUP_SCRIPT failed\"; fi'";
            self.exec_in_sandbox(&sandbox.id, script_cmd).await?;
            tracing::info!(sandbox_id = %sandbox.id, "setup script executed");
        }

        // Install sandbox-agent inside the sandbox.
        let agent = config.agent.as_sandbox_agent_str();
        let permission_mode = config.permission_mode.as_str();
        let install_cmd = format!(
            "curl -fsSL https://github.com/rivet-dev/sandbox-agent/releases/latest/download/sandbox-agent-$(uname -m)-unknown-linux-gnu -o /usr/local/bin/sandbox-agent && \
             chmod +x /usr/local/bin/sandbox-agent && \
             sandbox-agent install {agent} 2>&1 || true && \
             nohup sandbox-agent start --no-token --permission-mode {permission_mode} > /tmp/sandbox-agent.log 2>&1 &"
        );

        self.exec_in_sandbox(&sandbox.id, &install_cmd).await?;
        tracing::info!(sandbox_id = %sandbox.id, "sandbox-agent installed");

        // Get the preview URL for the sandbox-agent port.
        let sandbox_agent_url = self
            .get_preview_url(&sandbox.id, SANDBOX_AGENT_PORT)
            .await?;

        Ok(SandboxHandle {
            id: sandbox.id,
            sandbox_agent_url,
            backend: SandboxBackendKind::Daytona,
            host_port: SANDBOX_AGENT_PORT,
            metadata: serde_json::json!({
                "provider": "daytona",
                "agent": agent,
            }),
        })
    }

    /// Poll the sandbox-agent health endpoint until it responds.
    #[tracing::instrument(level = "debug", skip(self, handle), fields(sandbox = %handle.id))]
    async fn wait_ready(&self, handle: &SandboxHandle) -> Result<()> {
        let client = Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .map_err(|e| Error::backend("build health client", e))?;

        let health_url = format!("{}/v1/health", handle.sandbox_agent_url);
        let deadline = tokio::time::Instant::now() + HEALTH_TIMEOUT;

        loop {
            if tokio::time::Instant::now() > deadline {
                return Err(Error::BackendMessage(format!(
                    "sandbox-agent health check timed out for sandbox {}",
                    handle.id,
                )));
            }

            match client.get(&health_url).send().await {
                Ok(resp) if resp.status().is_success() => {
                    tracing::info!("sandbox-agent is healthy");
                    return Ok(());
                }
                Ok(resp) => {
                    tracing::debug!(status = %resp.status(), "health check not ready");
                }
                Err(e) => {
                    tracing::debug!(%e, "health check connection error, retrying");
                }
            }

            tokio::time::sleep(HEALTH_POLL_INTERVAL).await;
        }
    }

    /// Delete the Daytona sandbox.
    #[tracing::instrument(level = "info", skip(self, handle), fields(sandbox = %handle.id))]
    async fn release(&self, handle: &SandboxHandle) -> Result<()> {
        let url = format!("{}/api/sandboxes/{}", self.api_url, handle.id);
        let resp = self
            .http
            .delete(&url)
            .bearer_auth(&self.api_key)
            .send()
            .await
            .map_err(Error::backend_reqwest)?;

        if !resp.status().is_success() {
            let text = resp.text().await.unwrap_or_default();
            tracing::warn!(%text, "daytona delete returned non-success (sandbox may already be gone)");
        }

        tracing::info!("sandbox released");
        Ok(())
    }
}
