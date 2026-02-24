use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct RunConfig {
    #[serde(default)]
    pub budgets: Option<BudgetConfig>,
    #[serde(default)]
    pub concurrency: Option<ConcurrencyConfig>,
    #[serde(default)]
    pub output_config: Option<OutputConfig>,
    #[serde(default)]
    pub stream_completions: Option<bool>,
    #[serde(default)]
    pub rlm_event_level: Option<String>,
    #[serde(default)]
    pub tool_executor: Option<ToolExecutorConfig>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct ToolExecutorConfig {
    #[serde(default)]
    pub endpoint: Option<String>,
    #[serde(default)]
    pub api_key: Option<String>,
    #[serde(default)]
    pub headers: Option<HashMap<String, String>>,
    #[serde(default)]
    pub transport: Option<ToolExecutorTransport>,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolExecutorTransport {
    Execute,
    Mcp,
}

impl Default for ToolExecutorTransport {
    fn default() -> Self {
        Self::Execute
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct BudgetConfig {
    pub max_cost_usd: Option<f64>,
    pub max_tokens: Option<i64>,
    pub max_time_ms: Option<i64>,
    pub max_llm_calls: Option<i64>,
    pub max_supersteps: Option<i64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct ConcurrencyConfig {
    pub max_concurrent_nodes: Option<i64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct OutputConfig {
    pub schema: Option<Value>,
    pub format: Option<String>,
    pub strict: Option<bool>,
    pub extract_from: Option<Vec<String>>,
}
