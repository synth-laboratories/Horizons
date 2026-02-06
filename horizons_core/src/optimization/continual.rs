//! Continual learning wiring: Voyager memory -> MIPRO -> RLM evaluation.
//!
//! This is intentionally a thin "bridge" layer so `mipro_v2`, `voyager`, and `rlm`
//! can evolve independently.

use crate::events::models::{Event, EventDirection};
use crate::events::traits::EventBus;
use crate::memory::traits::{HorizonsMemory, MemoryType, RetrievalQuery};
use crate::models::OrgId;
use crate::optimization::wiring::MiproContinualLearning;
use crate::{Error, Result};
use chrono::Utc;
use std::sync::Arc;
use std::time::Duration;

use super::traits::{Dataset, Example, MiproConfig, Policy};

#[cfg(feature = "evaluation")]
use crate::evaluation::traits::Evaluator;
#[cfg(feature = "evaluation")]
use crate::evaluation::traits::VerificationCase;

/// Bridges Voyager memory -> MIPRO optimization context.
pub struct ContinualLearningEngine {
    memory: Arc<dyn HorizonsMemory>,
    optimizer: Arc<MiproContinualLearning>,
    evaluator: Arc<dyn Evaluator>,
    event_bus: Arc<dyn EventBus>,
}

impl ContinualLearningEngine {
    #[tracing::instrument(level = "debug", skip_all)]
    pub fn new(
        memory: Arc<dyn HorizonsMemory>,
        optimizer: Arc<MiproContinualLearning>,
        evaluator: Arc<dyn Evaluator>,
        event_bus: Arc<dyn EventBus>,
    ) -> Self {
        Self {
            memory,
            optimizer,
            evaluator,
            event_bus,
        }
    }

    /// Run one optimization cycle.
    #[tracing::instrument(level = "info", skip_all, fields(agent_id = %agent_id))]
    pub async fn run_cycle(
        &self,
        org_id: OrgId,
        agent_id: &str,
        config: &CycleConfig,
    ) -> Result<OptimizationCycleResult> {
        let cycle_id = ulid::Ulid::new().to_string();
        let now = Utc::now();
        let horizon = chrono::Duration::from_std(config.memory_horizon)
            .map_err(|e| Error::InvalidInput(format!("invalid memory_horizon: {e}")))?;

        // 1) Retrieve recent memory items.
        let mut q = RetrievalQuery::new("continual learning context", config.memory_limit);
        q.created_after = Some(now - horizon);
        q.include_episode_summaries = true;
        let items = self.memory.retrieve(org_id, agent_id, q).await?;

        // 2) Convert memory items -> MIPRO dataset examples.
        // ExactMatchMetric expects string expected; keep the dev pipeline simple.
        let mut examples = Vec::new();
        for it in &items {
            examples.push(Example {
                input: it.content.clone(),
                expected: serde_json::Value::String("no".to_string()),
                metadata: serde_json::json!({
                    "memory_item_id": it.id.to_string(),
                    "item_type": it.item_type.0,
                    "created_at": it.created_at.to_rfc3339(),
                }),
            });
        }

        // If memory is empty, still produce a deterministic minimal dataset.
        if examples.is_empty() {
            examples.push(Example {
                input: serde_json::Value::String("noop".to_string()),
                expected: serde_json::Value::String("no".to_string()),
                metadata: serde_json::json!({"synthetic": true}),
            });
        }

        let dataset = Dataset {
            id: format!("cycle:{cycle_id}"),
            examples,
        };

        // 3) Run MIPRO optimization using memory-derived dataset.
        let mipro_cfg = MiproConfig {
            min_improvement: config.min_improvement as f32,
            ..MiproConfig::default()
        };
        let initial_policy = Policy {
            template: "Answer yes or no.".to_string(),
            metadata: serde_json::json!({}),
        };

        let opt = self
            .optimizer
            .inner()
            .run_batch(mipro_cfg, initial_policy.clone(), dataset.clone())
            .await
            .map_err(|e| Error::BackendMessage(format!("mipro_v2 optimization failed: {e}")))?;

        // 4) Evaluate candidate policies via RLM (sampled).
        let baseline_score = self
            .eval_policy_with_rlm(&initial_policy, &dataset, config.eval_samples)
            .await?;
        let optimized_score = self
            .eval_policy_with_rlm(&opt.best_policy, &dataset, config.eval_samples)
            .await?;

        let improved = optimized_score > baseline_score + config.min_improvement;
        let policy = improved.then(|| opt.best_policy.clone());

        // 5) Emit events.
        let completed_payload = serde_json::json!({
            "cycle_id": cycle_id,
            "agent_id": agent_id,
            "improved": improved,
            "baseline_score": baseline_score,
            "optimized_score": optimized_score,
            "memory_items": items.len(),
        });
        if let Ok(ev) = Event::new(
            org_id.to_string(),
            None,
            EventDirection::Outbound,
            "optimization.cycle.completed",
            "optimization:continual_learning_engine",
            completed_payload,
            format!("optimization_cycle_completed:{cycle_id}"),
            serde_json::json!({}),
            None,
        ) {
            let _ = self.event_bus.publish(ev).await;
        }

        if let Some(p) = &policy {
            let payload = serde_json::to_value(p)
                .map_err(|e| Error::backend("serialize policy.updated payload", e))?;
            if let Ok(ev) = Event::new(
                org_id.to_string(),
                None,
                EventDirection::Outbound,
                "policy.updated",
                "optimization:continual_learning_engine",
                payload,
                format!("policy_updated:{cycle_id}"),
                serde_json::json!({}),
                None,
            ) {
                let _ = self.event_bus.publish(ev).await;
            }
        }

        // 6) Store cycle metadata to memory.
        let _ = self
            .memory
            .append(
                org_id,
                agent_id,
                MemoryType::reflection(),
                serde_json::json!({
                    "cycle_id": cycle_id,
                    "improved": improved,
                    "baseline_score": baseline_score,
                    "optimized_score": optimized_score,
                }),
                Utc::now(),
            )
            .await;

        Ok(OptimizationCycleResult {
            cycle_id,
            improved,
            baseline_score,
            optimized_score,
            policy,
        })
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn eval_policy_with_rlm(
        &self,
        policy: &Policy,
        dataset: &Dataset,
        eval_samples: usize,
    ) -> Result<f64> {
        let n = dataset.examples.len().min(eval_samples.max(1));
        let llm = self.optimizer.inner().evaluator().llm().clone();

        let mut total = 0.0f64;
        for ex in dataset.examples.iter().take(n) {
            let prompt = policy
                .render(&ex.input)
                .map_err(|e| Error::BackendMessage(format!("policy render failed: {e}")))?;
            let output = llm
                .complete(&prompt)
                .await
                .map_err(|e| Error::BackendMessage(format!("llm complete failed: {e}")))?;

            let case = VerificationCase::new(prompt, output, None);
            let outcome = self.evaluator.verify(case).await?;
            total += outcome.total_score_0_to_1 as f64;
        }

        Ok(total / (n as f64))
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CycleConfig {
    pub memory_horizon: Duration,
    pub memory_limit: usize,
    pub min_improvement: f64,
    pub eval_samples: usize,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct OptimizationCycleResult {
    pub improved: bool,
    pub baseline_score: f64,
    pub optimized_score: f64,
    pub policy: Option<Policy>,
    pub cycle_id: String,
}
