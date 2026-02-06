use crate::{Error, Result};
use async_trait::async_trait;
use std::sync::Arc;

use super::traits::{
    EvalReport, Evaluator, RewardOutcome, RewardSignal, VerificationCase, VerifierConfig,
};

pub struct RlmEvaluator {
    inner: rlm::RlmVerifier,
}

impl RlmEvaluator {
    #[tracing::instrument(skip_all)]
    pub fn new(inner: rlm::RlmVerifier) -> Self {
        Self { inner }
    }
}

#[async_trait]
impl Evaluator for RlmEvaluator {
    #[tracing::instrument(skip_all)]
    async fn verify(&self, case: VerificationCase) -> Result<RewardOutcome> {
        self.inner
            .verify(case)
            .await
            .map_err(|e| Error::BackendMessage(format!("rlm verify failed: {e}")))
    }

    #[tracing::instrument(skip_all)]
    async fn verify_report(&self, case: VerificationCase) -> Result<EvalReport> {
        self.inner
            .verify_report(case)
            .await
            .map_err(|e| Error::BackendMessage(format!("rlm verify_report failed: {e}")))
    }
}

#[tracing::instrument(skip_all)]
pub fn build_rlm_evaluator(
    cfg: VerifierConfig,
    signals: Vec<RewardSignal>,
    llm: Option<Arc<dyn rlm::LlmClient>>,
) -> Result<RlmEvaluator> {
    let verifier = rlm::RlmVerifier::new(cfg, signals, llm)
        .map_err(|e| Error::InvalidInput(format!("invalid rlm config: {e}")))?;
    Ok(RlmEvaluator::new(verifier))
}
