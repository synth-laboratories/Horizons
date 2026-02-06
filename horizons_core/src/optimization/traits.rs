use crate::Result;
use async_trait::async_trait;

pub use mipro_v2::{
    Dataset, EvalSummary, ExactMatchMetric, Example, LlmClient, MiproConfig, OptimizationResult,
    Optimizer, Policy, VariantSampler,
};

/// Horizons wrapper trait for prompt/policy optimization.
#[async_trait]
pub trait ContinualLearning: Send + Sync {
    async fn run_batch(
        &self,
        cfg: MiproConfig,
        initial_policy: Policy,
        dataset: Dataset,
    ) -> Result<OptimizationResult>;

    async fn evaluate(&self, policy: &Policy, dataset: &Dataset) -> Result<EvalSummary>;
}
