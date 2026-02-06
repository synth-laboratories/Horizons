use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

pub type UUID = Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProjectDbHandle {
    pub org_id: Uuid,
    pub project_id: Uuid,
    pub connection_url: String,
    #[serde(default)]
    pub auth_token: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventDirection {
    Inbound,
    Outbound,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventStatus {
    Pending,
    Processing,
    Succeeded,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    pub event_id: String,
    pub org_id: String,
    #[serde(default)]
    pub project_id: Option<String>,
    pub topic: String,
    pub source: String,
    pub direction: EventDirection,
    pub payload: Value,
    pub dedupe_key: String,
    #[serde(default)]
    pub metadata: std::collections::HashMap<String, Value>,
    pub status: EventStatus,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SubscriptionHandlerType {
    Webhook,
    InternalQueue,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubscriptionHandler {
    #[serde(rename = "type")]
    pub handler_type: SubscriptionHandlerType,
    #[serde(default)]
    pub url: Option<String>,
    #[serde(default)]
    pub queue_name: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubscriptionConfig {
    #[serde(default = "default_max_attempts")]
    pub max_attempts: i64,
    #[serde(default = "default_backoff_ms")]
    pub backoff_ms: i64,
}

fn default_max_attempts() -> i64 {
    3
}
fn default_backoff_ms() -> i64 {
    1000
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Subscription {
    pub subscription_id: String,
    pub org_id: String,
    pub topic_pattern: String,
    pub direction: EventDirection,
    pub handler: SubscriptionHandler,
    pub config: SubscriptionConfig,
    #[serde(default)]
    pub filter: Option<std::collections::HashMap<String, Value>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentRunResult {
    pub run_id: Uuid,
    pub org_id: Uuid,
    pub project_id: Uuid,
    pub agent_id: String,
    pub started_at: DateTime<Utc>,
    pub finished_at: DateTime<Utc>,
    #[serde(default)]
    pub proposed_action_ids: Vec<Uuid>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RiskLevel {
    Low,
    Medium,
    High,
    Critical,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActionStatus {
    Proposed,
    Approved,
    Denied,
    Executed,
    Expired,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActionProposal {
    pub id: Uuid,
    pub org_id: Uuid,
    pub project_id: Uuid,
    pub agent_id: String,
    pub action_type: String,
    pub payload: Value,
    pub risk_level: RiskLevel,
    #[serde(default)]
    pub dedupe_key: Option<String>,
    pub context: Value,
    pub status: ActionStatus,
    pub created_at: DateTime<Utc>,
    #[serde(default)]
    pub decided_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub decided_by: Option<String>,
    #[serde(default)]
    pub decision_reason: Option<String>,
    pub expires_at: DateTime<Utc>,
    #[serde(default)]
    pub execution_result: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StepStatus {
    Queued,
    Running,
    Succeeded,
    Failed,
    Skipped,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StepResult {
    pub step_id: String,
    pub status: StepStatus,
    #[serde(default)]
    pub output: Option<Value>,
    #[serde(default)]
    pub error: Option<String>,
    pub duration_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineRun {
    pub id: String,
    pub pipeline_id: String,
    pub status: Value,
    pub step_results: std::collections::HashMap<String, StepResult>,
    pub started_at: DateTime<Utc>,
    #[serde(default)]
    pub completed_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MemoryType {
    Observation,
    Summary,
    Action,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryItem {
    #[serde(default)]
    pub id: Option<String>,
    pub scope: std::collections::HashMap<String, String>,
    pub item_type: MemoryType,
    pub content: Value,
    #[serde(default)]
    pub index_text: Option<String>,
    #[serde(default)]
    pub importance_0_to_1: Option<f64>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Summary {
    pub agent_id: String,
    pub horizon: String,
    pub content: Value,
    pub at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OptimizationRunRow {
    pub run_id: Uuid,
    pub org_id: Uuid,
    pub project_id: Uuid,
    pub status: String,
    pub started_at: DateTime<Utc>,
    #[serde(default)]
    pub finished_at: Option<DateTime<Utc>>,
    pub cfg: std::collections::HashMap<String, Value>,
    pub initial_policy: std::collections::HashMap<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvalReportRow {
    pub report_id: Uuid,
    pub org_id: Uuid,
    pub project_id: Uuid,
    pub status: String,
    pub created_at: DateTime<Utc>,
    #[serde(default)]
    pub completed_at: Option<DateTime<Utc>>,
    pub case: std::collections::HashMap<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Health {
    pub status: String,
    pub uptime_ms: i64,
}
