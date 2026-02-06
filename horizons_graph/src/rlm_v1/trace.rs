use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct RlmExecutionTrace {
    pub lm_calls: Vec<LmCallTrace>,
    pub tool_calls: Vec<ToolCallTrace>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LmCallTrace {
    pub model: String,
    pub iteration: i64,
    pub started_at_ms: i64,
    pub elapsed_ms: i64,
    pub prompt_tokens: i64,
    pub completion_tokens: i64,
    pub tool_calls_count: i64,
    pub prompt_messages: Vec<Value>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ToolCallTrace {
    pub tool: String,
    pub args: Value,
    pub result: Value,
    pub iteration: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ToolResultTrace {
    pub tool: String,
    pub success: bool,
    pub execution_mode: Option<String>,
    pub elapsed_ms: i64,
}
