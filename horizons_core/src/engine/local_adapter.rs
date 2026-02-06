use crate::engine::models::{EnvHandle, EnvSpec, EnvTemplate, Observation};
use crate::engine::traits::RhodesAdapter;
use crate::models::AgentIdentity;
use crate::{Error, Result};
use async_trait::async_trait;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Action understood by the local adapter.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum LocalAction {
    /// Execute a command inside the provisioned container.
    Exec {
        cmd: Vec<String>,
        timeout_ms: Option<u64>,
        env: Option<HashMap<String, String>>,
        workdir: Option<String>,
    },
}

#[derive(Debug, Clone)]
pub struct DockerLocalAdapterConfig {
    pub docker_bin: String,
    pub default_image: String,
}

impl Default for DockerLocalAdapterConfig {
    fn default() -> Self {
        Self {
            docker_bin: "docker".to_string(),
            default_image: "ubuntu:24.04".to_string(),
        }
    }
}

#[async_trait]
pub trait CommandRunner: Send + Sync {
    async fn run(&self, program: &str, args: &[String]) -> Result<CmdOutput>;
}

#[derive(Debug, Clone, PartialEq)]
pub struct CmdOutput {
    pub status: i32,
    pub stdout: String,
    pub stderr: String,
}

pub struct TokioCommandRunner;

#[async_trait]
impl CommandRunner for TokioCommandRunner {
    #[tracing::instrument(level = "debug", skip_all)]
    async fn run(&self, program: &str, args: &[String]) -> Result<CmdOutput> {
        let mut cmd = tokio::process::Command::new(program);
        cmd.args(args);
        let out = cmd
            .output()
            .await
            .map_err(|e| Error::backend("spawn command", e))?;
        Ok(CmdOutput {
            status: out.status.code().unwrap_or(-1),
            stdout: String::from_utf8_lossy(&out.stdout).to_string(),
            stderr: String::from_utf8_lossy(&out.stderr).to_string(),
        })
    }
}

/// A RhodesAdapter implementation that uses the Docker CLI locally.
///
/// This is intended for dev/test mode and has no cloud dependencies.
pub struct DockerLocalAdapter {
    cfg: DockerLocalAdapterConfig,
    runner: Arc<dyn CommandRunner>,
    // Container id -> template
    active: RwLock<HashMap<String, EnvTemplate>>,
}

impl DockerLocalAdapter {
    #[tracing::instrument(level = "debug", skip(runner))]
    pub fn new(cfg: DockerLocalAdapterConfig, runner: Arc<dyn CommandRunner>) -> Result<Self> {
        if cfg.docker_bin.trim().is_empty() {
            return Err(Error::InvalidInput("docker_bin is empty".to_string()));
        }
        if cfg.default_image.trim().is_empty() {
            return Err(Error::InvalidInput("default_image is empty".to_string()));
        }
        Ok(Self {
            cfg,
            runner,
            active: RwLock::new(HashMap::new()),
        })
    }

    #[tracing::instrument(level = "debug", skip_all)]
    fn image_for(&self, spec: &EnvSpec) -> Result<String> {
        if let Some(img) = spec
            .config
            .get("image")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
        {
            if img.trim().is_empty() {
                return Err(Error::InvalidInput(
                    "env_spec.config.image is empty".to_string(),
                ));
            }
            return Ok(img);
        }
        Ok(self.cfg.default_image.clone())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn docker(&self, args: Vec<String>) -> Result<CmdOutput> {
        self.runner.run(&self.cfg.docker_bin, &args).await
    }
}

#[async_trait]
impl RhodesAdapter for DockerLocalAdapter {
    #[tracing::instrument(level = "info", skip_all)]
    async fn provision(&self, env_spec: EnvSpec, _identity: &AgentIdentity) -> Result<EnvHandle> {
        let image = self.image_for(&env_spec)?;

        // `docker run -d --rm <image> sleep infinity`
        let args = vec![
            "run".to_string(),
            "-d".to_string(),
            "--rm".to_string(),
            image,
            "sleep".to_string(),
            "infinity".to_string(),
        ];
        let out = self.docker(args).await?;
        if out.status != 0 {
            return Err(Error::BackendMessage(format!(
                "docker run failed: {}",
                out.stderr.trim()
            )));
        }
        let id = out.stdout.trim().to_string();
        if id.is_empty() {
            return Err(Error::BackendMessage(
                "docker did not return container id".to_string(),
            ));
        }

        self.active
            .write()
            .await
            .insert(id.clone(), env_spec.template);
        Ok(EnvHandle {
            id,
            template: env_spec.template,
            pool_id: env_spec.pool_id.clone(),
            lease_expires_at: None,
            metadata: serde_json::json!({"image": env_spec.config.get("image")}),
        })
    }

    #[tracing::instrument(level = "info", skip_all)]
    async fn run_step(
        &self,
        env_handle: &EnvHandle,
        action: serde_json::Value,
    ) -> Result<Observation> {
        let a: LocalAction = serde_json::from_value(action)
            .map_err(|e| Error::InvalidInput(format!("invalid local action: {e}")))?;

        // Lease expiry check.
        if let Some(exp) = env_handle.lease_expires_at {
            if Utc::now() >= exp {
                return Err(Error::Conflict("env lease expired".to_string()));
            }
        }

        match a {
            LocalAction::Exec { cmd, .. } => {
                if cmd.is_empty() || cmd.iter().any(|s| s.trim().is_empty()) {
                    return Err(Error::InvalidInput("exec cmd is empty".to_string()));
                }
                // `docker exec <id> <cmd...>`
                let mut args = vec!["exec".to_string(), env_handle.id.clone()];
                args.extend(cmd);
                let out = self.docker(args).await?;
                Ok(Observation {
                    output: serde_json::json!({
                        "status": out.status,
                        "stdout": out.stdout,
                        "stderr": out.stderr,
                    }),
                    reward: None,
                    done: false,
                    artifacts: vec![],
                })
            }
        }
    }

    #[tracing::instrument(level = "info", skip_all)]
    async fn release(&self, env_handle: &EnvHandle) -> Result<()> {
        // Best-effort cleanup. Container uses `--rm` so stop is enough.
        let args = vec!["stop".to_string(), env_handle.id.clone()];
        let _ = self.docker(args).await;
        self.active.write().await.remove(&env_handle.id);
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn health(&self, env_handle: &EnvHandle) -> Result<bool> {
        // `docker inspect -f {{.State.Running}} <id>`
        let args = vec![
            "inspect".to_string(),
            "-f".to_string(),
            "{{.State.Running}}".to_string(),
            env_handle.id.clone(),
        ];
        let out = self.docker(args).await?;
        Ok(out.status == 0 && out.stdout.trim() == "true")
    }
}
