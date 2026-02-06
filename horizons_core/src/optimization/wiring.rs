use crate::{Error, Result};
use async_trait::async_trait;
use std::sync::Arc;

use super::traits::{
    ContinualLearning, Dataset, EvalSummary, LlmClient, MiproConfig, OptimizationResult, Policy,
    VariantSampler,
};

pub struct MiproContinualLearning {
    inner: mipro_v2::Optimizer,
}

impl MiproContinualLearning {
    #[tracing::instrument(skip_all)]
    pub fn new(inner: mipro_v2::Optimizer) -> Self {
        Self { inner }
    }

    #[tracing::instrument(skip_all)]
    pub fn inner(&self) -> &mipro_v2::Optimizer {
        &self.inner
    }
}

#[async_trait]
impl ContinualLearning for MiproContinualLearning {
    #[tracing::instrument(skip_all)]
    async fn run_batch(
        &self,
        cfg: MiproConfig,
        initial_policy: Policy,
        dataset: Dataset,
    ) -> Result<OptimizationResult> {
        self.inner
            .run_batch(cfg, initial_policy, dataset)
            .await
            .map_err(|e| Error::BackendMessage(format!("mipro_v2 run_batch failed: {e}")))
    }

    #[tracing::instrument(skip_all)]
    async fn evaluate(&self, policy: &Policy, dataset: &Dataset) -> Result<EvalSummary> {
        self.inner
            .evaluate(policy, dataset)
            .await
            .map_err(|e| Error::BackendMessage(format!("mipro_v2 evaluate failed: {e}")))
    }
}

/// Build a `ContinualLearning` implementation backed by `mipro_v2`.
#[tracing::instrument(skip_all)]
pub fn build_mipro_continual_learning(
    llm: Arc<dyn LlmClient>,
    sampler: Arc<dyn VariantSampler>,
    metric: Arc<dyn mipro_v2::EvalMetric>,
) -> MiproContinualLearning {
    let evaluator = mipro_v2::Evaluator::new(llm, metric);
    let optimizer = mipro_v2::Optimizer::new(sampler, evaluator);
    MiproContinualLearning::new(optimizer)
}
