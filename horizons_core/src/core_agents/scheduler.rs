//! System 2 self-driving scheduler.
//!
//! Horizons supports three orthogonal scheduling modes for core agents:
//! - Reactive: run agents when matching events arrive (`AgentSchedule::OnEvent`)
//! - Time-driven: cron/interval (`AgentSchedule::Cron`, `AgentSchedule::Interval`)
//! - Logical stepping: explicit `tick()` runs up to N queued runs (step/drain)
//!
//! This module provides an explicit scheduler object that:
//! - polls the EventBus for `on_event` schedules
//! - evaluates cron/interval schedules using `core_agents.next_run_at`
//! - maintains an in-memory run queue so callers can "step" execution
//! - emits scheduler + agent-run lifecycle events to the EventBus
//!
//! Note: `on_event` is implemented via polling (`EventBus::query`). This keeps
//! behavior predictable across deployments and enables deterministic stepping.

use crate::core_agents::models::AgentSchedule;
use crate::core_agents::traits::CoreAgents;
use crate::events::models::{Event, EventDirection, EventQuery};
use crate::events::traits::EventBus;
use crate::models::{AgentIdentity, OrgId, ProjectId};
use crate::onboard::models::{CoreAgentEventCursor, CoreAgentRecord, ListQuery};
use crate::onboard::traits::{CentralDb, ProjectDb};
use crate::{Error, Result};
use chrono::{DateTime, Duration, Utc};
use cron::Schedule;
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::VecDeque;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Debug, Clone)]
pub struct CoreSchedulerConfig {
    /// Max number of events to fetch per `on_event` agent per tick.
    pub on_event_batch_limit: usize,
    /// If no durable cursor exists yet, look back this long for events.
    pub on_event_initial_lookback_seconds: i64,
}

impl Default for CoreSchedulerConfig {
    fn default() -> Self {
        Self {
            on_event_batch_limit: 25,
            on_event_initial_lookback_seconds: 60 * 60, // 1h
        }
    }
}

#[derive(Debug, Clone)]
pub struct ScheduledRun {
    pub org_id: OrgId,
    pub project_id: ProjectId,
    pub agent_id: String,
    pub reason: String,
    pub inputs: Option<Value>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TickResult {
    pub enqueued: usize,
    pub ran: usize,
    pub run_errors: usize,
    pub queue_depth: usize,
}

/// Explicit scheduler object used by Horizons server to run self-driving apps.
#[derive(Clone)]
pub struct CoreScheduler {
    cfg: CoreSchedulerConfig,
    central_db: Arc<dyn CentralDb>,
    project_db: Arc<dyn ProjectDb>,
    event_bus: Arc<dyn EventBus>,
    core_agents: Arc<dyn CoreAgents>,

    queues: Arc<DashMap<(OrgId, ProjectId), Arc<Mutex<VecDeque<ScheduledRun>>>>>,
    // Hot cache of durable `on_event` cursors.
    // Key: (org_id, project_id, agent_id, topic) -> (last_seen_received_at, last_seen_event_id)
    on_event_cursors: Arc<DashMap<(OrgId, ProjectId, String, String), (DateTime<Utc>, String)>>,
}

impl CoreScheduler {
    #[tracing::instrument(level = "debug", skip_all)]
    pub fn new(
        central_db: Arc<dyn CentralDb>,
        project_db: Arc<dyn ProjectDb>,
        event_bus: Arc<dyn EventBus>,
        core_agents: Arc<dyn CoreAgents>,
        cfg: CoreSchedulerConfig,
    ) -> Self {
        Self {
            cfg,
            central_db,
            project_db,
            event_bus,
            core_agents,
            queues: Arc::new(DashMap::new()),
            on_event_cursors: Arc::new(DashMap::new()),
        }
    }

    fn queue_for(
        &self,
        org_id: OrgId,
        project_id: ProjectId,
    ) -> Arc<Mutex<VecDeque<ScheduledRun>>> {
        self.queues
            .entry((org_id, project_id))
            .or_insert_with(|| Arc::new(Mutex::new(VecDeque::new())))
            .clone()
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn tick_project(
        &self,
        org_id: OrgId,
        project_id: ProjectId,
        now: DateTime<Utc>,
        max_runs_per_tick: usize,
    ) -> Result<TickResult> {
        if max_runs_per_tick == 0 {
            return Err(Error::InvalidInput(
                "max_runs_per_tick must be > 0".to_string(),
            ));
        }

        // Ensure project is provisioned before attempting any agent runs.
        let handle = self
            .project_db
            .get_handle(org_id, project_id)
            .await?
            .ok_or_else(|| Error::NotFound("project not provisioned".to_string()))?;

        let mut result = TickResult::default();

        // 1) Enqueue any due runs (time-driven + on_event).
        result.enqueued += self.enqueue_due(org_id, project_id, now).await?;

        // 2) Drain up to N runs.
        let q = self.queue_for(org_id, project_id);
        for _ in 0..max_runs_per_tick {
            let next = {
                let mut guard = q.lock().await;
                guard.pop_front()
            };
            let Some(run) = next else {
                break;
            };

            self.emit_scheduler_event(
                org_id,
                project_id,
                "core.scheduler.agent_run.started",
                serde_json::json!({
                    "agent_id": run.agent_id,
                    "reason": run.reason,
                    "at": now.to_rfc3339(),
                }),
            )
            .await;

            let identity = AgentIdentity::System {
                name: format!("core_scheduler:{}", project_id),
            };

            match self
                .core_agents
                .run(
                    org_id,
                    project_id,
                    handle.clone(),
                    &run.agent_id,
                    &identity,
                    run.inputs.clone(),
                )
                .await
            {
                Ok(agent_res) => {
                    result.ran += 1;
                    self.emit_scheduler_event(
                        org_id,
                        project_id,
                        "core.scheduler.agent_run.completed",
                        serde_json::json!({
                            "agent_id": run.agent_id,
                            "reason": run.reason,
                            "run_id": agent_res.run_id,
                            "proposals": agent_res.proposed_action_ids.len(),
                            "at": Utc::now().to_rfc3339(),
                        }),
                    )
                    .await;
                }
                Err(e) => {
                    result.run_errors += 1;
                    self.emit_scheduler_event(
                        org_id,
                        project_id,
                        "core.scheduler.agent_run.failed",
                        serde_json::json!({
                            "agent_id": run.agent_id,
                            "reason": run.reason,
                            "error": e.to_string(),
                            "at": Utc::now().to_rfc3339(),
                        }),
                    )
                    .await;
                }
            }
        }

        result.queue_depth = q.lock().await.len();

        self.emit_scheduler_event(
            org_id,
            project_id,
            "core.scheduler.tick",
            serde_json::json!({
                "enqueued": result.enqueued,
                "ran": result.ran,
                "run_errors": result.run_errors,
                "queue_depth": result.queue_depth,
                "at": now.to_rfc3339(),
            }),
        )
        .await;

        Ok(result)
    }

    async fn enqueue_due(
        &self,
        org_id: OrgId,
        project_id: ProjectId,
        now: DateTime<Utc>,
    ) -> Result<usize> {
        let mut enqueued = 0usize;

        let agents = self
            .central_db
            .list_core_agents(
                org_id,
                project_id,
                ListQuery {
                    limit: 1000,
                    offset: 0,
                },
            )
            .await?;

        // Time-driven: schedule when due, and persist the next_run_at immediately to prevent re-enqueue.
        for mut a in agents.iter().cloned() {
            let Some(schedule) = a.schedule.clone() else {
                continue;
            };

            match schedule {
                AgentSchedule::Cron(expr) => {
                    if !a.enabled {
                        continue;
                    }
                    if !is_due(a.next_run_at, now) {
                        continue;
                    }
                    let next_run_at = match next_cron_after(&expr, now) {
                        Ok(v) => v,
                        Err(e) => {
                            tracing::warn!(
                                org_id = %org_id,
                                project_id = %project_id,
                                agent_id = %a.agent_id,
                                error = %e,
                                "invalid cron schedule; skipping agent"
                            );
                            continue;
                        }
                    };
                    a.next_run_at = next_run_at;
                    a.updated_at = Utc::now();
                    self.central_db.upsert_core_agent(&a).await?;

                    self.enqueue_run(ScheduledRun {
                        org_id,
                        project_id,
                        agent_id: a.agent_id.clone(),
                        reason: format!("cron:{expr}"),
                        inputs: None,
                    })
                    .await;
                    enqueued += 1;
                }
                AgentSchedule::Interval { seconds } => {
                    if !a.enabled {
                        continue;
                    }
                    if seconds == 0 {
                        continue;
                    }
                    if !is_due(a.next_run_at, now) {
                        continue;
                    }
                    a.next_run_at = Some(now + Duration::seconds(seconds as i64));
                    a.updated_at = Utc::now();
                    self.central_db.upsert_core_agent(&a).await?;

                    self.enqueue_run(ScheduledRun {
                        org_id,
                        project_id,
                        agent_id: a.agent_id.clone(),
                        reason: format!("interval:{seconds}s"),
                        inputs: None,
                    })
                    .await;
                    enqueued += 1;
                }
                AgentSchedule::OnEvent { topic } => {
                    let n = self.enqueue_on_event(&a, &topic, now).await?;
                    enqueued += n;
                }
                AgentSchedule::OnDemand => {}
            }
        }

        Ok(enqueued)
    }

    async fn enqueue_on_event(
        &self,
        agent: &CoreAgentRecord,
        topic: &str,
        now: DateTime<Utc>,
    ) -> Result<usize> {
        let key = (
            agent.org_id,
            agent.project_id,
            agent.agent_id.clone(),
            topic.to_string(),
        );

        // EventBus queries are filtered and ordered by `received_at`, so the cursor must use it too.
        let (last_seen_received_at, last_seen_event_id) = if let Some(v) =
            self.on_event_cursors.get(&key)
        {
            v.value().clone()
        } else {
            let persisted = self
                .central_db
                .get_core_agent_event_cursor(agent.org_id, agent.project_id, &agent.agent_id, topic)
                .await?;
            if let Some(c) = persisted {
                let v = (c.last_seen_received_at, c.last_seen_event_id);
                self.on_event_cursors.insert(key.clone(), v.clone());
                v
            } else {
                let since = now - Duration::seconds(self.cfg.on_event_initial_lookback_seconds);
                let v = (since, String::new());
                self.on_event_cursors.insert(key.clone(), v.clone());
                v
            }
        };

        let query = EventQuery {
            org_id: agent.org_id.to_string(),
            project_id: Some(agent.project_id.to_string()),
            topic: Some(topic.to_string()),
            direction: None,
            since: Some(last_seen_received_at),
            until: None,
            status: None,
            limit: self.cfg.on_event_batch_limit.max(1),
        };

        let events = self
            .event_bus
            .query(query)
            .await
            .map_err(|e| Error::BackendMessage(format!("event_bus.query: {e}")))?;
        if events.is_empty() {
            return Ok(0);
        }

        let mut max_seen = (last_seen_received_at, last_seen_event_id.clone());
        let mut enqueued = 0usize;
        for ev in events {
            let ev_key = (ev.received_at, ev.id.clone());
            if ev_key <= (last_seen_received_at, last_seen_event_id.clone()) {
                continue;
            }
            if ev_key > max_seen {
                max_seen = ev_key.clone();
            }
            self.enqueue_run(ScheduledRun {
                org_id: agent.org_id,
                project_id: agent.project_id,
                agent_id: agent.agent_id.clone(),
                reason: format!("on_event:{}:{}", topic, ev.id),
                inputs: Some(ev.payload),
            })
            .await;
            enqueued += 1;
        }

        if enqueued > 0 {
            let cursor = CoreAgentEventCursor {
                org_id: agent.org_id,
                project_id: agent.project_id,
                agent_id: agent.agent_id.clone(),
                topic: topic.to_string(),
                last_seen_received_at: max_seen.0,
                last_seen_event_id: max_seen.1.clone(),
                updated_at: Utc::now(),
            };
            self.central_db
                .upsert_core_agent_event_cursor(&cursor)
                .await?;
            self.on_event_cursors.insert(key, max_seen);
        }
        Ok(enqueued)
    }

    async fn enqueue_run(&self, run: ScheduledRun) {
        let q = self.queue_for(run.org_id, run.project_id);
        let mut guard = q.lock().await;
        guard.push_back(run);
    }

    async fn emit_scheduler_event(
        &self,
        org_id: OrgId,
        project_id: ProjectId,
        topic: &str,
        payload: Value,
    ) {
        let dedupe_key = format!(
            "{}:{}:{}:{}",
            topic,
            org_id,
            project_id,
            ulid::Ulid::new().to_string()
        );

        let ev = match Event::new(
            org_id.to_string(),
            Some(project_id.to_string()),
            EventDirection::Outbound,
            topic.to_string(),
            "core_scheduler".to_string(),
            payload,
            dedupe_key,
            serde_json::json!({}),
            None,
        ) {
            Ok(e) => e,
            Err(err) => {
                tracing::warn!(error = %err, "failed building scheduler event");
                return;
            }
        };

        if let Err(err) = self.event_bus.publish(ev).await {
            tracing::warn!(error = %err, "failed publishing scheduler event");
        }
    }
}

fn is_due(next_run_at: Option<DateTime<Utc>>, now: DateTime<Utc>) -> bool {
    next_run_at.map(|t| t <= now).unwrap_or(true)
}

fn next_cron_after(expr: &str, now: DateTime<Utc>) -> Result<Option<DateTime<Utc>>> {
    let schedule = Schedule::from_str(expr)
        .map_err(|e| Error::InvalidInput(format!("invalid cron expression '{expr}': {e}")))?;
    Ok(schedule.after(&now).take(1).next())
}
