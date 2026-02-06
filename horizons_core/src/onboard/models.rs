use crate::models::{OrgId, ProjectId};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::time::Duration;
use uuid::Uuid;

/// Central DB org record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrgRecord {
    pub org_id: OrgId,
    pub name: String,
    pub created_at: DateTime<Utc>,
}

/// Central DB user role.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UserRole {
    Admin,
    Member,
    ReadOnly,
}

impl UserRole {
    pub fn as_str(&self) -> &'static str {
        match self {
            UserRole::Admin => "admin",
            UserRole::Member => "member",
            UserRole::ReadOnly => "read_only",
        }
    }

    pub fn parse_str(s: &str) -> Option<Self> {
        match s {
            "admin" => Some(UserRole::Admin),
            "member" => Some(UserRole::Member),
            "read_only" => Some(UserRole::ReadOnly),
            _ => None,
        }
    }
}

/// Central DB user record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UserRecord {
    pub user_id: Uuid,
    pub org_id: OrgId,
    pub email: String,
    pub display_name: Option<String>,
    pub role: UserRole,
    pub created_at: DateTime<Utc>,
}

/// Generic list query for tenant-scoped lists.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListQuery {
    pub limit: usize,
    pub offset: usize,
}

/// Audit log query.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AuditQuery {
    pub project_id: Option<ProjectId>,
    pub actor_contains: Option<String>,
    pub action_prefix: Option<String>,
    pub since: Option<DateTime<Utc>>,
    pub until: Option<DateTime<Utc>>,
    pub limit: usize,
}

impl Default for AuditQuery {
    fn default() -> Self {
        Self {
            project_id: None,
            actor_contains: None,
            action_prefix: None,
            since: None,
            until: None,
            limit: 100,
        }
    }
}

/// Encrypted connector credential blob stored in the central DB.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConnectorCredential {
    pub org_id: OrgId,
    pub connector_id: String,
    /// Opaque encrypted bytes (ciphertext). Encryption is handled outside the DB.
    pub ciphertext: Vec<u8>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Identifies a sync cursor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SyncStateKey {
    pub org_id: OrgId,
    pub project_id: Option<ProjectId>,
    pub connector_id: String,
    /// Connector-defined key for partitioned sync state (e.g. inbox id).
    pub scope: String,
}

/// Durable connector sync cursor/state, stored in the central DB.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SyncState {
    pub key: SyncStateKey,
    pub cursor: serde_json::Value,
    pub updated_at: DateTime<Utc>,
}

/// Project DB query parameter (positional).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum ProjectDbParam {
    Null,
    Bool(bool),
    I64(i64),
    F64(f64),
    String(String),
    Bytes(Vec<u8>),
    Json(serde_json::Value),
}

/// Project DB query value (returned from a row).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum ProjectDbValue {
    Null,
    Bool(bool),
    I64(i64),
    F64(f64),
    String(String),
    Bytes(Vec<u8>),
    Json(serde_json::Value),
}

pub type ProjectDbRow = BTreeMap<String, ProjectDbValue>;

/// Vector match result for a similarity search.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct VectorMatch {
    pub id: String,
    pub score: f32,
    pub metadata: serde_json::Value,
}

/// Cache value used in some higher-level call sites.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CacheEntry {
    #[serde(with = "serde_bytes")]
    pub value: Vec<u8>,
    pub ttl: Option<Duration>,
}
