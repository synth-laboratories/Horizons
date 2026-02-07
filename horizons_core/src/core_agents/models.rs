use crate::models::{AgentIdentity, OrgId, ProjectDbHandle, ProjectId};
use crate::onboard::traits::ProjectDb;
use crate::{Error, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentSpec {
    pub id: String,
    pub org_id: OrgId,
    pub project_id: ProjectId,
    pub name: String,
    pub sandbox_image: Option<String>,
    pub tools: Vec<String>,
    pub schedule: Option<AgentSchedule>,
    pub config: serde_json::Value,
}

impl AgentSpec {
    #[tracing::instrument(level = "debug", skip(config))]
    pub fn new(
        org_id: OrgId,
        project_id: ProjectId,
        id: impl Into<String> + std::fmt::Debug,
        name: impl Into<String> + std::fmt::Debug,
        sandbox_image: Option<String>,
        tools: Vec<String>,
        schedule: Option<AgentSchedule>,
        config: serde_json::Value,
    ) -> Result<Self> {
        let id = id.into();
        if id.trim().is_empty() {
            return Err(Error::InvalidInput("agent spec id is empty".to_string()));
        }
        let name = name.into();
        if name.trim().is_empty() {
            return Err(Error::InvalidInput("agent spec name is empty".to_string()));
        }
        Ok(Self {
            id,
            org_id,
            project_id,
            name,
            sandbox_image,
            tools,
            schedule,
            config,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentSchedule {
    Cron(String),
    OnDemand,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RiskLevel {
    Low,
    Medium,
    High,
    Critical,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReviewMode {
    Auto,
    Ai,
    Human,
    McpAuth,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActionStatus {
    Proposed,
    Approved,
    Denied,
    /// The action has been dispatched (e.g. an outbound event was published).
    ///
    /// Legacy stored values may use `"executed"`; treat them as dispatched.
    #[serde(alias = "executed")]
    Dispatched,
    Expired,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReviewPolicy {
    pub action_type: String,
    pub risk_level: RiskLevel,
    pub review_mode: ReviewMode,
    pub mcp_scopes: Option<Vec<String>>,
    pub ttl_seconds: u64,
}

impl ReviewPolicy {
    #[tracing::instrument(level = "debug")]
    pub fn validate(&self) -> Result<()> {
        if self.action_type.trim().is_empty() {
            return Err(Error::InvalidInput(
                "review policy action_type is empty".to_string(),
            ));
        }
        if self.ttl_seconds == 0 {
            return Err(Error::InvalidInput(
                "review policy ttl_seconds must be > 0".to_string(),
            ));
        }
        if let Some(scopes) = &self.mcp_scopes {
            if scopes.iter().any(|s| s.trim().is_empty()) {
                return Err(Error::InvalidInput(
                    "review policy contains empty mcp scope".to_string(),
                ));
            }
        }
        Ok(())
    }
}

impl Default for ReviewPolicy {
    fn default() -> Self {
        Self {
            action_type: "*".to_string(),
            risk_level: RiskLevel::Medium,
            review_mode: ReviewMode::Human,
            mcp_scopes: None,
            ttl_seconds: 60 * 60,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ActionProposal {
    pub id: Uuid,
    pub org_id: OrgId,
    pub project_id: ProjectId,
    pub agent_id: String,
    pub action_type: String,
    pub payload: serde_json::Value,
    pub risk_level: RiskLevel,
    /// Optional idempotency key. If `None`, the proposal is treated as non-idempotent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dedupe_key: Option<String>,
    pub context: serde_json::Value,

    pub status: ActionStatus,
    pub created_at: DateTime<Utc>,
    pub decided_at: Option<DateTime<Utc>>,
    pub decided_by: Option<String>,
    pub decision_reason: Option<String>,
    pub expires_at: DateTime<Utc>,
    pub execution_result: Option<serde_json::Value>,
}

impl ActionProposal {
    #[tracing::instrument(level = "debug", skip(payload, context))]
    pub fn new(
        org_id: OrgId,
        project_id: ProjectId,
        agent_id: impl Into<String> + std::fmt::Debug,
        action_type: impl Into<String> + std::fmt::Debug,
        payload: serde_json::Value,
        risk_level: RiskLevel,
        dedupe_key: Option<String>,
        context: serde_json::Value,
        created_at: DateTime<Utc>,
        ttl_seconds: u64,
    ) -> Result<Self> {
        let agent_id = agent_id.into();
        if agent_id.trim().is_empty() {
            return Err(Error::InvalidInput(
                "action proposal agent_id is empty".to_string(),
            ));
        }
        let action_type = action_type.into();
        if action_type.trim().is_empty() {
            return Err(Error::InvalidInput(
                "action proposal action_type is empty".to_string(),
            ));
        }
        if let Some(dk) = &dedupe_key {
            if dk.trim().is_empty() {
                return Err(Error::InvalidInput(
                    "action proposal dedupe_key is empty".to_string(),
                ));
            }
        }
        if ttl_seconds == 0 {
            return Err(Error::InvalidInput(
                "action proposal ttl_seconds must be > 0".to_string(),
            ));
        }

        let expires_at = created_at + chrono::Duration::seconds(ttl_seconds as i64);
        Ok(Self {
            id: Uuid::new_v4(),
            org_id,
            project_id,
            agent_id,
            action_type,
            payload,
            risk_level,
            dedupe_key,
            context,
            status: ActionStatus::Proposed,
            created_at,
            decided_at: None,
            decided_by: None,
            decision_reason: None,
            expires_at,
            execution_result: None,
        })
    }

    #[tracing::instrument(level = "debug")]
    pub fn is_expired(&self, now: DateTime<Utc>) -> bool {
        now >= self.expires_at
    }
}

#[derive(Clone)]
pub struct AgentContext {
    pub org_id: OrgId,
    pub project_id: ProjectId,
    pub identity: AgentIdentity,
    pub project_db: ProjectDbHandle,
    /// A live handle to the project database so agents can execute SQL directly
    /// without needing to construct or hold their own `Arc<dyn ProjectDb>`.
    pub db: Arc<dyn ProjectDb>,
    /// Decrypted credentials keyed by connector_id. Populated by the executor
    /// when a `CredentialManager` is configured.
    pub credentials: Option<serde_json::Value>,
}

impl std::fmt::Debug for AgentContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AgentContext")
            .field("org_id", &self.org_id)
            .field("project_id", &self.project_id)
            .field("identity", &self.identity)
            .field("project_db", &self.project_db)
            .field("credentials", &self.credentials)
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentRunResult {
    pub run_id: Uuid,
    pub org_id: OrgId,
    pub project_id: ProjectId,
    pub agent_id: String,
    pub started_at: DateTime<Utc>,
    pub finished_at: DateTime<Utc>,
    pub proposed_action_ids: Vec<Uuid>,
}

/// Return type for agents that may perform side effects directly (e.g. DB writes,
/// external API calls) instead of / in addition to proposing actions through the
/// approval pipeline.
///
/// `SideEffectOnly` allows agents to report success without returning empty
/// `Vec<ActionProposal>` which was previously the only option.
#[derive(Debug, Clone)]
pub enum AgentOutcome {
    /// Agent produced action proposals for the approval pipeline.
    Proposals(Vec<ActionProposal>),
    /// Agent performed its own side effects and has nothing to propose.
    /// The `result` value is stored in the audit log for observability.
    SideEffectOnly { result: serde_json::Value },
    /// Combination: agent performed side effects AND produced proposals.
    Mixed {
        result: serde_json::Value,
        proposals: Vec<ActionProposal>,
    },
}

impl AgentOutcome {
    /// Extract all proposals (empty vec for SideEffectOnly).
    pub fn proposals(&self) -> Vec<ActionProposal> {
        match self {
            AgentOutcome::Proposals(p) => p.clone(),
            AgentOutcome::SideEffectOnly { .. } => Vec::new(),
            AgentOutcome::Mixed { proposals, .. } => proposals.clone(),
        }
    }

    /// Extract the side-effect result, if any.
    pub fn result(&self) -> Option<&serde_json::Value> {
        match self {
            AgentOutcome::Proposals(_) => None,
            AgentOutcome::SideEffectOnly { result } => Some(result),
            AgentOutcome::Mixed { result, .. } => Some(result),
        }
    }
}
