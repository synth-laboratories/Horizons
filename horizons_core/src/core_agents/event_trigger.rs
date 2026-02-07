//! Event-triggered agent execution.
//!
//! Subscribes to EventBus topics and dispatches agent runs when matching
//! events arrive. This is the glue between Context Refresh ("data arrived")
//! and Core Agents ("react to that data").
//!
//! Usage:
//! ```ignore
//! let trigger = EventTriggeredAgentRunner::new(executor.clone(), event_bus.clone());
//! trigger.register(EventAgentBinding {
//!     topic_prefix: "context.refreshed".to_string(),
//!     agent_id: "scoring_agent".to_string(),
//!     org_id,
//!     project_id,
//!     project_db_handle: handle,
//! });
//! trigger.start(); // spawns background task
//! ```

use crate::core_agents::executor::CoreAgentsExecutor;
use crate::core_agents::traits::CoreAgents;
use crate::events::models::EventQuery;
use crate::events::traits::EventBus;
use crate::models::{AgentIdentity, OrgId, ProjectDbHandle, ProjectId};
use std::sync::Arc;
use tokio::sync::RwLock;

/// How often the trigger loop polls for new events.
const POLL_INTERVAL: std::time::Duration = std::time::Duration::from_secs(5);

/// Binding between an event topic pattern and an agent to trigger.
#[derive(Debug, Clone)]
pub struct EventAgentBinding {
    /// Topic prefix to match events against (e.g. "context.refreshed").
    pub topic_prefix: String,
    /// The agent to run when a matching event arrives.
    pub agent_id: String,
    /// Org scope.
    pub org_id: OrgId,
    /// Project scope.
    pub project_id: ProjectId,
    /// Project DB handle to pass to the agent.
    pub project_db_handle: ProjectDbHandle,
    /// Optional: pass the event payload as the agent's `inputs`.
    pub forward_payload: bool,
}

/// Background runner that polls the EventBus and triggers agent runs
/// when matching events arrive.
pub struct EventTriggeredAgentRunner {
    executor: Arc<CoreAgentsExecutor>,
    event_bus: Arc<dyn EventBus>,
    bindings: Arc<RwLock<Vec<EventAgentBinding>>>,
}

impl EventTriggeredAgentRunner {
    pub fn new(executor: Arc<CoreAgentsExecutor>, event_bus: Arc<dyn EventBus>) -> Self {
        Self {
            executor,
            event_bus,
            bindings: Arc::new(RwLock::new(Vec::new())),
        }
    }

    /// Register a binding: when events matching `topic_prefix` arrive, run `agent_id`.
    pub async fn register(&self, binding: EventAgentBinding) {
        self.bindings.write().await.push(binding);
    }

    /// Start the polling loop. Returns a JoinHandle that runs forever.
    pub fn start(self: Arc<Self>) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            tracing::info!("event-triggered agent runner started");
            let mut interval = tokio::time::interval(POLL_INTERVAL);
            // Track last-seen event timestamps per binding to avoid re-processing.
            let mut last_seen: std::collections::HashMap<String, chrono::DateTime<chrono::Utc>> =
                std::collections::HashMap::new();

            loop {
                interval.tick().await;
                if let Err(e) = self.poll(&mut last_seen).await {
                    tracing::error!(%e, "event trigger poll failed");
                }
            }
        })
    }

    async fn poll(
        &self,
        last_seen: &mut std::collections::HashMap<String, chrono::DateTime<chrono::Utc>>,
    ) -> crate::Result<()> {
        let bindings = self.bindings.read().await.clone();

        for binding in &bindings {
            let binding_key = format!("{}:{}", binding.org_id, binding.topic_prefix);
            let since = last_seen.get(&binding_key).copied();

            let query = EventQuery {
                org_id: binding.org_id.to_string(),
                project_id: Some(binding.project_id.to_string()),
                topic: Some(binding.topic_prefix.clone()),
                since,
                limit: 10,
                ..Default::default()
            };

            let events = match self.event_bus.query(query).await {
                Ok(events) => events,
                Err(e) => {
                    tracing::warn!(
                        topic = %binding.topic_prefix,
                        %e,
                        "failed to query events for trigger"
                    );
                    continue;
                }
            };

            for event in &events {
                // Update high-water mark.
                let ts = event.timestamp;
                let entry = last_seen
                    .entry(binding_key.clone())
                    .or_insert(chrono::DateTime::UNIX_EPOCH.into());
                if ts > *entry {
                    *entry = ts;
                }

                tracing::info!(
                    agent_id = %binding.agent_id,
                    event_id = %event.id,
                    topic = %event.topic,
                    "event trigger: dispatching agent"
                );

                let identity = AgentIdentity::System {
                    name: format!("event_trigger:{}", binding.topic_prefix),
                };

                let inputs = if binding.forward_payload {
                    Some(event.payload.clone())
                } else {
                    None
                };

                match self
                    .executor
                    .run(
                        binding.org_id,
                        binding.project_id,
                        binding.project_db_handle.clone(),
                        &binding.agent_id,
                        &identity,
                        inputs,
                    )
                    .await
                {
                    Ok(result) => {
                        tracing::info!(
                            agent_id = %binding.agent_id,
                            run_id = %result.run_id,
                            proposals = result.proposed_action_ids.len(),
                            "event-triggered agent run completed"
                        );
                        // Ack the event.
                        let _ = self.event_bus.ack(&event.org_id, &event.id).await;
                    }
                    Err(e) => {
                        tracing::error!(
                            agent_id = %binding.agent_id,
                            event_id = %event.id,
                            %e,
                            "event-triggered agent run failed"
                        );
                    }
                }
            }
        }
        Ok(())
    }
}
