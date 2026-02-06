pub mod errors;
pub mod limits;
pub mod messages;
pub mod output;
pub mod runner;
pub mod schema;
pub mod tools;
pub mod trace;

pub use errors::{LmError, RlmErrorInfo, RlmErrorKind, RlmRunError, ToolError};
pub use limits::RlmLimits;
pub use output::{EvidenceItem, RlmRunResult, RlmStats};
pub use runner::{RlmEventSink, RlmRunContext, run_rlm};
pub use schema::ResolvedAnswerSchema;
pub use tools::{ToolCall, ToolContext, ToolExecutor, ToolResult, ToolSchema};
pub use trace::{LmCallTrace, RlmExecutionTrace, ToolCallTrace, ToolResultTrace};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RlmRunRequest {
    pub messages_seed: Vec<Value>,
    pub inputs: Value,
    pub query: Option<String>,
    pub is_verifier: bool,
    pub answer_schema: Option<Value>,
    pub limits: RlmLimits,
    pub tool_filter: Option<Vec<String>>,
    pub allowed_tool_capabilities: Option<Vec<String>>,
    pub model: String,
    pub summarizer_model: Option<String>,
    pub supports_parallel_tool_calls: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct UsageMetrics {
    pub prompt_tokens: i64,
    pub completion_tokens: i64,
    pub total_tokens: i64,
    pub cached_tokens: i64,
    pub cost_usd: f64,
    pub cache_savings_usd: f64,
}

#[derive(Clone, Debug)]
pub struct LmRequest {
    pub model: String,
    pub messages: Vec<Value>,
    pub temperature: f64,
    pub max_tokens: Option<i64>,
    pub response_format: Option<Value>,
    pub tools: Option<Vec<Value>>,
    pub tool_choice: Option<Value>,
}

#[derive(Clone, Debug)]
pub struct LmResponse {
    pub text: String,
    pub tool_calls: Vec<ToolCall>,
    pub usage: UsageMetrics,
    pub elapsed_ms: i64,
}

#[async_trait]
pub trait LmClient: Send + Sync {
    async fn call(&self, req: LmRequest) -> Result<LmResponse, LmError>;
}
