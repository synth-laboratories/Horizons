use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineSpec {
    pub id: String,
    pub org_id: String,
    pub steps: Vec<PipelineStep>,
    pub on_failure: FailurePolicy,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineStep {
    pub id: String,
    pub kind: StepKind,
    /// Static or templated JSON from prior outputs.
    #[serde(default)]
    pub inputs: Value,
    #[serde(default)]
    pub depends_on: Vec<String>,
    #[serde(default)]
    pub approval_required: bool,
    #[serde(default)]
    pub timeout_ms: Option<u64>,
    #[serde(default)]
    pub retry_policy: Option<RetryPolicy>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum StepKind {
    Agent { spec: AgentSpec },
    GraphRun { graph_id: String },
    ToolCall { tool: String, args: Value },
    LlmCall { model: String, messages: Vec<Value> },
    Custom { handler: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentSpec {
    /// Identifier understood by the configured `Subagent` implementation (often a core agent id).
    pub role: String,
    #[serde(default)]
    pub tools: Vec<String>,
    #[serde(default)]
    pub context: Value,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct RetryPolicy {
    pub max_attempts: u32,
    pub backoff_ms: u64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FailurePolicy {
    Halt,
    SkipAndContinue,
    Retry(u32),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineRun {
    pub id: String,
    pub pipeline_id: String,
    pub status: PipelineStatus,
    pub step_results: BTreeMap<String, StepResult>,
    pub started_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PipelineStatus {
    Queued,
    Running,
    WaitingApproval(String),
    Succeeded,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StepResult {
    pub step_id: String,
    pub status: StepStatus,
    pub output: Option<Value>,
    pub error: Option<String>,
    pub duration_ms: u64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StepStatus {
    Queued,
    Running,
    Succeeded,
    Failed,
    Skipped,
}

