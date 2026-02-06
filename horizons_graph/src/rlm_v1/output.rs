use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::errors::RlmErrorInfo;
use super::trace::RlmExecutionTrace;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EvidenceItem {
    pub description: String,
    pub evidence_snippet: String,
    pub iteration: Option<i64>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct RlmStats {
    pub root_calls: i64,
    pub subcalls: i64,
    pub elapsed_ms: i64,
    pub cost_usd: f64,
    pub cached_tokens: i64,
    pub cache_savings_usd: f64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RlmRunResult {
    pub answer: Option<Value>,
    pub confidence: Option<String>,
    pub reasoning_summary: Option<String>,
    pub evidence_list: Vec<EvidenceItem>,
    pub rlm_stats: RlmStats,
    pub execution_trace: RlmExecutionTrace,
    pub error: Option<RlmErrorInfo>,
    pub partial_answer: Option<Value>,
    pub iterations: i64,
    pub rlm_impl: Option<String>,
}
