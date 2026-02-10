use crate::core_agents::models::AgentSchedule;
use crate::models::AgentIdentity;
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

/// Central DB project record (human-friendly slug + metadata).
///
/// This is separate from the ProjectDb handle storage; it exists to support
/// consumer-facing APIs that use a stable slug identifier (e.g. Vistas).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectRecord {
    pub org_id: OrgId,
    pub project_id: ProjectId,
    pub slug: String,
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

/// API key record for authentication (bearer token).
///
/// `secret_hash` is stored but not intended to be exposed via APIs.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ApiKeyRecord {
    pub key_id: Uuid,
    pub org_id: OrgId,
    pub name: String,
    pub actor: AgentIdentity,
    pub scopes: Vec<String>,
    #[serde(skip_serializing, default)]
    pub secret_hash: String,
    pub created_at: DateTime<Utc>,
    pub expires_at: Option<DateTime<Utc>>,
    pub last_used_at: Option<DateTime<Utc>>,
}

/// Managed integration resource (Retool-like "Resource").
///
/// `config` is resource-type-specific JSON (e.g. HTTP base URL + env variants).
/// Secrets should be referenced via `$cred:connector_id.key` tokens and resolved
/// at runtime (not stored inline as plaintext).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ResourceRecord {
    pub org_id: OrgId,
    pub resource_id: String,
    pub resource_type: String,
    pub config: serde_json::Value,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Managed operation/query (Retool-like "Query").
///
/// `config` is operation-type-specific JSON (e.g. HTTP path, headers, timeout).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OperationRecord {
    pub org_id: OrgId,
    pub operation_id: String,
    pub resource_id: String,
    pub operation_type: String,
    pub config: serde_json::Value,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Execution record for an operation, used for auditability/debugging.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OperationRunRecord {
    pub id: Uuid,
    pub org_id: OrgId,
    pub operation_id: String,
    pub source_event_id: Option<String>,
    pub status: String,
    pub error: Option<String>,
    pub output: serde_json::Value,
    pub created_at: DateTime<Utc>,
}

/// Persisted core-agent configuration for a single project.
///
/// This is the durable "agent spec" that Horizons uses to:
/// - (re)register runnable agents on startup
/// - drive self-scheduling (cron/interval) and event triggers
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CoreAgentRecord {
    pub org_id: OrgId,
    pub project_id: ProjectId,
    pub agent_id: String,
    pub name: String,
    pub sandbox_image: Option<String>,
    #[serde(default)]
    pub tools: Vec<String>,
    #[serde(default)]
    pub schedule: Option<AgentSchedule>,
    /// Whether time-driven scheduling is enabled for this agent.
    ///
    /// `on_event` agents are always eligible when matching events arrive.
    pub enabled: bool,
    /// Optional scheduler state: the next time a cron/interval agent is due.
    pub next_run_at: Option<DateTime<Utc>>,
    /// Free-form config blob for agent-specific settings.
    #[serde(default)]
    pub config: serde_json::Value,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Per-agent cursor used for `AgentSchedule::OnEvent` scheduling.
///
/// Stores a high-water mark so restarts don't re-enqueue already-seen events.
/// Uses `Event.received_at` (not domain `timestamp`) because EventBus queries
/// are filtered and ordered by `received_at`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CoreAgentEventCursor {
    pub org_id: OrgId,
    pub project_id: ProjectId,
    pub agent_id: String,
    pub topic: String,
    pub last_seen_received_at: DateTime<Utc>,
    pub last_seen_event_id: String,
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

// ── ProjectDb row extraction helpers ────────────────────────────
//
// These reduce the boilerplate of extracting typed values from a
// `ProjectDbRow` (BTreeMap<String, ProjectDbValue>). Every accessor
// returns `Result` so callers can propagate cleanly.

impl ProjectDbValue {
    /// Extract a `String`, returning an error if the value is a different type.
    pub fn as_string(&self) -> crate::Result<&str> {
        match self {
            ProjectDbValue::String(s) => Ok(s),
            other => Err(crate::Error::BackendMessage(format!(
                "expected String, got {other:?}"
            ))),
        }
    }

    /// Extract an `i64`, returning an error if the value is a different type.
    pub fn as_i64(&self) -> crate::Result<i64> {
        match self {
            ProjectDbValue::I64(v) => Ok(*v),
            other => Err(crate::Error::BackendMessage(format!(
                "expected I64, got {other:?}"
            ))),
        }
    }

    /// Extract an `f64`, returning an error if the value is a different type.
    pub fn as_f64(&self) -> crate::Result<f64> {
        match self {
            ProjectDbValue::F64(v) => Ok(*v),
            other => Err(crate::Error::BackendMessage(format!(
                "expected F64, got {other:?}"
            ))),
        }
    }

    /// Extract a `bool`, returning an error if the value is a different type.
    pub fn as_bool(&self) -> crate::Result<bool> {
        match self {
            ProjectDbValue::Bool(v) => Ok(*v),
            other => Err(crate::Error::BackendMessage(format!(
                "expected Bool, got {other:?}"
            ))),
        }
    }

    /// Returns true if the value is `Null`.
    pub fn is_null(&self) -> bool {
        matches!(self, ProjectDbValue::Null)
    }
}

/// Extension helpers for `ProjectDbRow`.
pub trait ProjectDbRowExt {
    /// Get a required String column.
    fn get_string(&self, col: &str) -> crate::Result<String>;

    /// Get an optional String column (returns None for NULL or missing).
    fn get_opt_string(&self, col: &str) -> crate::Result<Option<String>>;

    /// Get a required i64 column.
    fn get_i64(&self, col: &str) -> crate::Result<i64>;

    /// Get an optional i64 column.
    fn get_opt_i64(&self, col: &str) -> crate::Result<Option<i64>>;

    /// Get a required f64 column.
    fn get_f64(&self, col: &str) -> crate::Result<f64>;

    /// Parse a required column as JSON into T.
    fn get_json<T: serde::de::DeserializeOwned>(&self, col: &str) -> crate::Result<T>;

    /// Parse an optional column as JSON into T.
    fn get_opt_json<T: serde::de::DeserializeOwned>(&self, col: &str) -> crate::Result<Option<T>>;
}

impl ProjectDbRowExt for ProjectDbRow {
    fn get_string(&self, col: &str) -> crate::Result<String> {
        match self.get(col) {
            Some(ProjectDbValue::String(s)) => Ok(s.clone()),
            Some(v) => Err(crate::Error::BackendMessage(format!(
                "column '{col}': expected String, got {v:?}"
            ))),
            None => Err(crate::Error::BackendMessage(format!(
                "missing column '{col}'"
            ))),
        }
    }

    fn get_opt_string(&self, col: &str) -> crate::Result<Option<String>> {
        match self.get(col) {
            Some(ProjectDbValue::Null) | None => Ok(None),
            Some(ProjectDbValue::String(s)) if s.trim().is_empty() => Ok(None),
            Some(ProjectDbValue::String(s)) => Ok(Some(s.clone())),
            Some(v) => Err(crate::Error::BackendMessage(format!(
                "column '{col}': expected String|Null, got {v:?}"
            ))),
        }
    }

    fn get_i64(&self, col: &str) -> crate::Result<i64> {
        match self.get(col) {
            Some(ProjectDbValue::I64(v)) => Ok(*v),
            Some(v) => Err(crate::Error::BackendMessage(format!(
                "column '{col}': expected I64, got {v:?}"
            ))),
            None => Err(crate::Error::BackendMessage(format!(
                "missing column '{col}'"
            ))),
        }
    }

    fn get_opt_i64(&self, col: &str) -> crate::Result<Option<i64>> {
        match self.get(col) {
            Some(ProjectDbValue::Null) | None => Ok(None),
            Some(ProjectDbValue::I64(v)) => Ok(Some(*v)),
            Some(v) => Err(crate::Error::BackendMessage(format!(
                "column '{col}': expected I64|Null, got {v:?}"
            ))),
        }
    }

    fn get_f64(&self, col: &str) -> crate::Result<f64> {
        match self.get(col) {
            Some(ProjectDbValue::F64(v)) => Ok(*v),
            Some(v) => Err(crate::Error::BackendMessage(format!(
                "column '{col}': expected F64, got {v:?}"
            ))),
            None => Err(crate::Error::BackendMessage(format!(
                "missing column '{col}'"
            ))),
        }
    }

    fn get_json<T: serde::de::DeserializeOwned>(&self, col: &str) -> crate::Result<T> {
        let s = self.get_string(col)?;
        serde_json::from_str(&s)
            .map_err(|e| crate::Error::BackendMessage(format!("column '{col}': invalid JSON: {e}")))
    }

    fn get_opt_json<T: serde::de::DeserializeOwned>(&self, col: &str) -> crate::Result<Option<T>> {
        match self.get_opt_string(col)? {
            None => Ok(None),
            Some(s) => {
                let v: T = serde_json::from_str(&s).map_err(|e| {
                    crate::Error::BackendMessage(format!("column '{col}': invalid JSON: {e}"))
                })?;
                Ok(Some(v))
            }
        }
    }
}

/// Run a list of DDL migration statements against a ProjectDb.
///
/// Each statement is executed in order. Statements like `CREATE TABLE IF NOT EXISTS`
/// are idempotent by design. The DDL string is split on `;` and empty fragments are
/// skipped.
///
/// Usage:
/// ```ignore
/// const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS foo (id TEXT PRIMARY KEY);";
/// project_db_migrate(&*project_db, org_id, &handle, SCHEMA).await?;
/// ```
pub async fn project_db_migrate(
    db: &dyn crate::onboard::traits::ProjectDb,
    org_id: OrgId,
    handle: &crate::models::ProjectDbHandle,
    ddl: &str,
) -> crate::Result<()> {
    for stmt in ddl.split(';').map(str::trim).filter(|s| !s.is_empty()) {
        db.execute(org_id, handle, stmt, &[]).await?;
    }
    Ok(())
}
