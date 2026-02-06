use crate::Result;
use async_trait::async_trait;

pub use rlm::{
    EvalReport, LlmClient, LlmGrade, RewardOutcome, RewardSignal, SignalKind, SignalWeight,
    VerificationCase, VerifierConfig,
};

/// Horizons wrapper trait for output verification / reward scoring.
#[async_trait]
pub trait Evaluator: Send + Sync {
    async fn verify(&self, case: VerificationCase) -> Result<RewardOutcome>;
    async fn verify_report(&self, case: VerificationCase) -> Result<EvalReport>;
}
