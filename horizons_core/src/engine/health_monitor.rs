//! Sandbox crash detection + restart for long-running `start_agent_tracked` runs.

use crate::engine::sandbox_agent_client::SandboxAgentClient;
use crate::engine::sandbox_runtime::SandboxRuntime;
use crate::events::models::{Event, EventDirection};
use crate::events::traits::EventBus;
use crate::Result;
use dashmap::DashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

/// Monitors active sandbox runs and emits events when health checks fail.
pub struct SandboxHealthMonitor {
    runtime: Arc<SandboxRuntime>,
    event_bus: Arc<dyn EventBus>,
    pub check_interval: Duration,
    pub max_missed_heartbeats: u32,
    missed: DashMap<String, u32>, // run_id -> consecutive failures
}

impl SandboxHealthMonitor {
    #[tracing::instrument(level = "debug", skip_all)]
    pub fn new(runtime: Arc<SandboxRuntime>, event_bus: Arc<dyn EventBus>) -> Self {
        Self {
            runtime,
            event_bus,
            check_interval: Duration::from_secs(30),
            max_missed_heartbeats: 3,
            missed: DashMap::new(),
        }
    }

    /// Start the monitoring loop.
    ///
    /// Cancellation is cooperative; pass a token and call `cancel()` to stop.
    #[tracing::instrument(level = "info", skip_all)]
    pub async fn run(&self, cancel: CancellationToken) {
        let mut interval = tokio::time::interval(self.check_interval);
        loop {
            tokio::select! {
                _ = cancel.cancelled() => {
                    tracing::info!("sandbox health monitor cancelled");
                    return;
                }
                _ = interval.tick() => {
                    if let Err(e) = self.tick().await {
                        tracing::warn!(%e, "sandbox health monitor tick failed");
                    }
                }
            }
        }
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn tick(&self) -> Result<()> {
        let runs = self.runtime.list_active();

        for run in runs {
            let client = SandboxAgentClient::new(&run.sandbox.sandbox_agent_url, None);
            let healthy = client.health().await.unwrap_or(false);

            if healthy {
                self.missed.remove(&run.run_id);
                continue;
            }

            let failures = self
                .missed
                .entry(run.run_id.clone())
                .and_modify(|v| *v = v.saturating_add(1))
                .or_insert(1);

            let payload = serde_json::json!({
                "run_id": run.run_id,
                "sandbox_id": run.sandbox.id,
                "agent_id": run.agent_id,
                "consecutive_failures": *failures,
            });

            // Emit per-check failure events.
            let ev = Event::new(
                run.org_id.clone(),
                None,
                EventDirection::Outbound,
                "engine.sandbox.health_check_failed",
                "engine:sandbox_health_monitor",
                payload.clone(),
                format!("sandbox_health:{}:{}", run.run_id, *failures),
                serde_json::json!({}),
                None,
            )
            .map_err(|e| crate::Error::backend("build health_check_failed event", e))?;
            let _ = self
                .event_bus
                .publish(ev)
                .await;

            if *failures < self.max_missed_heartbeats {
                continue;
            }

            // Crash confirmed: emit crash event, then restart (if configured) or release.
            let ev = Event::new(
                run.org_id.clone(),
                None,
                EventDirection::Outbound,
                "engine.sandbox.crashed",
                "engine:sandbox_health_monitor",
                serde_json::json!({
                    "run_id": run.run_id,
                    "sandbox_id": run.sandbox.id,
                    "agent_id": run.agent_id,
                    "restart_count": run.restart_count,
                }),
                format!("sandbox_crash:{}:{}", run.run_id, run.restart_count),
                serde_json::json!({}),
                None,
            )
            .map_err(|e| crate::Error::backend("build sandbox.crashed event", e))?;
            let _ = self
                .event_bus
                .publish(ev)
                .await;

            let mut restarted = false;
            if let Some(policy) = run.config.restart_policy {
                if policy.max_restarts > 0 && run.restart_count < policy.max_restarts {
                    // Exponential backoff based on the current restart_count.
                    let pow: u32 = run.restart_count.min(16) as u32;
                    let factor = 1u64.checked_shl(pow).unwrap_or(u64::MAX);
                    let backoff = policy
                        .backoff_ms
                        .saturating_mul(factor);
                    if backoff > 0 {
                        tokio::time::sleep(Duration::from_millis(backoff)).await;
                    }

                    match self.runtime.restart_run(&run.run_id).await {
                        Ok(updated) => {
                            restarted = true;
                            let ev = Event::new(
                                updated.org_id.clone(),
                                None,
                                EventDirection::Outbound,
                                "engine.sandbox.restarted",
                                "engine:sandbox_health_monitor",
                                serde_json::json!({
                                    "run_id": updated.run_id,
                                    "sandbox_id": updated.sandbox.id,
                                    "agent_id": updated.agent_id,
                                    "restart_count": updated.restart_count,
                                }),
                                format!(
                                    "sandbox_restart:{}:{}",
                                    updated.run_id, updated.restart_count
                                ),
                                serde_json::json!({}),
                                None,
                            )
                            .map_err(|e| {
                                crate::Error::backend("build sandbox.restarted event", e)
                            })?;
                            let _ = self
                                .event_bus
                                .publish(ev)
                                .await;
                        }
                        Err(e) => {
                            tracing::warn!(run_id = %run.run_id, %e, "restart failed");
                        }
                    }
                }
            }

            if !restarted {
                // No restart policy (or exhausted): best-effort release + stop tracking.
                let _ = self.runtime.release_run(&run.run_id).await;
                self.missed.remove(&run.run_id);
            } else {
                // Restart succeeded: reset missed counter.
                self.missed.remove(&run.run_id);
            }
        }

        Ok(())
    }
}
