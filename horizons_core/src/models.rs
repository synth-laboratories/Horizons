use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;
use uuid::Uuid;

#[derive(thiserror::Error, Debug, Clone, PartialEq, Eq)]
pub enum IdParseError {
    #[error("invalid uuid: {0}")]
    InvalidUuid(String),
}

/// Tenant identifier.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, sqlx::Type)]
#[serde(transparent)]
#[sqlx(transparent)]
pub struct OrgId(pub Uuid);

impl fmt::Display for OrgId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl From<Uuid> for OrgId {
    fn from(value: Uuid) -> Self {
        Self(value)
    }
}

impl FromStr for OrgId {
    type Err = IdParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let id = Uuid::parse_str(s).map_err(|_| IdParseError::InvalidUuid(s.to_string()))?;
        Ok(Self(id))
    }
}

/// Project identifier.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, sqlx::Type)]
#[serde(transparent)]
#[sqlx(transparent)]
pub struct ProjectId(pub Uuid);

impl fmt::Display for ProjectId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl From<Uuid> for ProjectId {
    fn from(value: Uuid) -> Self {
        Self(value)
    }
}

impl FromStr for ProjectId {
    type Err = IdParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let id = Uuid::parse_str(s).map_err(|_| IdParseError::InvalidUuid(s.to_string()))?;
        Ok(Self(id))
    }
}

/// Actor identity for audit logging and authorization decisions.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AgentIdentity {
    /// A human user of the platform.
    User {
        user_id: Uuid,
        email: Option<String>,
    },
    /// An agent definition / runtime instance.
    Agent { agent_id: String },
    /// A platform system component (scheduler, router, etc.).
    System { name: String },
}

/// Append-only audit entry.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AuditEntry {
    pub id: Uuid,
    pub org_id: OrgId,
    pub project_id: Option<ProjectId>,
    pub actor: AgentIdentity,
    pub action: String,
    pub payload: serde_json::Value,
    pub outcome: String,
    pub created_at: DateTime<Utc>,
}

/// Tenant-scoped platform configuration.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PlatformConfig {
    pub org_id: OrgId,
    pub data: serde_json::Value,
    pub updated_at: DateTime<Utc>,
}

/// Connection handle for a provisioned project database.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectDbHandle {
    pub org_id: OrgId,
    pub project_id: ProjectId,
    pub connection_url: String,
    pub auth_token: Option<String>,
}
