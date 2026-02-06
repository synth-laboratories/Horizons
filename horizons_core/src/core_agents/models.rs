use crate::models::{AgentIdentity, OrgId, ProjectDbHandle, ProjectId};
use crate::{Error, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
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
    Executed,
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

#[derive(Debug, Clone)]
pub struct AgentContext {
    pub org_id: OrgId,
    pub project_id: ProjectId,
    pub identity: AgentIdentity,
    pub project_db: ProjectDbHandle,
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
