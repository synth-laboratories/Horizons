//! Cron-based agent scheduler.
//!
//! Evaluates cron expressions for registered agents and triggers `executor.run()`
//! when agents are due. Runs as a background tokio task.

use crate::core_agents::executor::CoreAgentsExecutor;
use crate::core_agents::traits::CoreAgents;
use crate::models::{AgentIdentity, OrgId, ProjectDbHandle, ProjectId};
use chrono::Utc;
use cron::Schedule;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Tick interval for the scheduler loop.
const TICK_INTERVAL: std::time::Duration = std::time::Duration::from_secs(30);
/// If a run has been marked in-flight longer than this, treat it as stale.
const STALE_IN_FLIGHT: std::time::Duration = std::time::Duration::from_secs(60 * 60);

/// Configuration for a cron-scheduled agent.
#[derive(Debug, Clone)]
pub struct CronAgent {
    pub agent_id: String,
    pub org_id: OrgId,
    pub project_id: ProjectId,
    pub cron_expression: String,
    pub project_db_handle: ProjectDbHandle,
}

/// Background agent scheduler that evaluates cron expressions and triggers
/// agent runs on schedule.
pub struct AgentScheduler {
    /// The executor to call when agents are due.
    executor: Arc<CoreAgentsExecutor>,
    /// Registered cron-scheduled agents.
    cron_agents: Arc<RwLock<Vec<CronAgent>>>,
    /// Tracks the last time each agent was triggered (to avoid double-fires).
    last_triggered: Arc<RwLock<HashMap<String, chrono::DateTime<Utc>>>>,
    /// Tracks agents currently running (best-effort, in-memory only).
    in_flight: Arc<RwLock<HashMap<String, std::time::Instant>>>,
}

impl AgentScheduler {
    pub fn new(executor: Arc<CoreAgentsExecutor>) -> Self {
        Self {
            executor,
            cron_agents: Arc::new(RwLock::new(Vec::new())),
            last_triggered: Arc::new(RwLock::new(HashMap::new())),
            in_flight: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Register a cron-scheduled agent.
    pub async fn register_cron_agent(&self, agent: CronAgent) -> crate::Result<()> {
        // Validate the cron expression.
        Schedule::from_str(&agent.cron_expression).map_err(|e| {
            crate::Error::InvalidInput(format!(
                "invalid cron expression '{}': {e}",
                agent.cron_expression
            ))
        })?;

        let mut agents = self.cron_agents.write().await;
        agents.push(agent);
        Ok(())
    }

    /// Start the scheduler loop. This runs forever (or until the task is dropped).
    ///
    /// Spawns a tokio task that ticks every 30 seconds, checks which agents
    /// are due based on their cron expressions, and fires `executor.run()`.
    pub fn start(self: Arc<Self>) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            tracing::info!("agent scheduler started");
            let mut interval = tokio::time::interval(TICK_INTERVAL);

            loop {
                interval.tick().await;

                if let Err(e) = self.tick().await {
                    tracing::error!(%e, "scheduler tick failed");
                }
            }
        })
    }

    /// Perform a single scheduler tick: evaluate cron expressions, fire due agents.
    #[tracing::instrument(level = "debug", skip(self))]
    async fn tick(&self) -> crate::Result<()> {
        let now = Utc::now();
        let agents = self.cron_agents.read().await;

        for agent in agents.iter() {
            if self
                .is_due(&agent.agent_id, &agent.cron_expression, now)
                .await
            {
                // Prevent duplicate runs if a previous run is still in progress.
                {
                    let mut in_flight = self.in_flight.write().await;
                    if let Some(started) = in_flight.get(&agent.agent_id).copied() {
                        let elapsed = started.elapsed();
                        if elapsed < STALE_IN_FLIGHT {
                            tracing::warn!(
                                agent_id = %agent.agent_id,
                                elapsed_s = elapsed.as_secs(),
                                "agent still in-flight; skipping scheduled run"
                            );
                            continue;
                        }

                        // Stale: clear and allow a new run. Cleanup of any external resources
                        // is best-effort and handled by per-system runtimes.
                        tracing::warn!(
                            agent_id = %agent.agent_id,
                            elapsed_s = elapsed.as_secs(),
                            "stale in-flight run detected; allowing new run"
                        );
                        in_flight.remove(&agent.agent_id);
                    }
                    in_flight.insert(agent.agent_id.clone(), std::time::Instant::now());
                }

                tracing::info!(agent_id = %agent.agent_id, "cron agent is due, triggering run");

                let identity = AgentIdentity::System {
                    name: format!("scheduler:{}", agent.agent_id),
                };

                match self
                    .executor
                    .run(
                        agent.org_id,
                        agent.project_id,
                        agent.project_db_handle.clone(),
                        &agent.agent_id,
                        &identity,
                        None,
                    )
                    .await
                {
                    Ok(result) => {
                        tracing::info!(
                            agent_id = %agent.agent_id,
                            run_id = %result.run_id,
                            proposals = result.proposed_action_ids.len(),
                            "scheduled agent run completed"
                        );
                    }
                    Err(e) => {
                        tracing::error!(
                            agent_id = %agent.agent_id,
                            %e,
                            "scheduled agent run failed"
                        );
                    }
                }

                // Clear in-flight marker.
                {
                    let mut in_flight = self.in_flight.write().await;
                    in_flight.remove(&agent.agent_id);
                }

                // Record trigger time to avoid double-fire within the same tick window.
                let mut triggered = self.last_triggered.write().await;
                triggered.insert(agent.agent_id.clone(), now);
            }
        }

        Ok(())
    }

    /// Check if a cron agent is due to run based on its expression and last trigger time.
    async fn is_due(&self, agent_id: &str, cron_expr: &str, now: chrono::DateTime<Utc>) -> bool {
        let schedule = match Schedule::from_str(cron_expr) {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!(%agent_id, %e, "invalid cron expression, skipping");
                return false;
            }
        };

        let last_triggered = self.last_triggered.read().await;
        let since = last_triggered
            .get(agent_id)
            .copied()
            .unwrap_or_else(|| now - chrono::Duration::hours(24));

        // Check if there's a scheduled time between `since` and `now`.
        schedule.after(&since).take(1).any(|next| next <= now)
    }
}
