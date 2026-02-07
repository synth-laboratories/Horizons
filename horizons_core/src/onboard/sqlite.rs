//! SQLite-backed CentralDb implementation.
//!
//! Persists sources, orgs, users, sync cursors, credentials, audit log, and
//! refresh runs across restarts. Single WAL-mode SQLite file.
//!
//! Usage:
//! ```ignore
//! let db = SqliteCentralDb::new("/path/to/central.db").await?;
//! ```

use async_trait::async_trait;
use chrono::{DateTime, Utc};

use crate::context_refresh::models::{
    CronSchedule, EventTriggerConfig, RefreshRun, RefreshRunQuery, RefreshRunStatus,
    RefreshTrigger, SourceConfig,
};
use crate::error::{Error as CoreError, Result as CoreResult};
use crate::models::{AgentIdentity, AuditEntry, OrgId, PlatformConfig, ProjectDbHandle, ProjectId};
use crate::onboard::models::{
    ApiKeyRecord, AuditQuery, ConnectorCredential, ListQuery, OperationRecord, OperationRunRecord,
    ResourceRecord, SyncState, SyncStateKey,
};
use crate::onboard::traits::{CentralDb, OrgRecord, UserRecord, UserRole};

use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::{Row, SqlitePool};
use std::path::Path;
use std::str::FromStr;
use uuid::Uuid;

/// A durable, file-backed CentralDb backed by a single SQLite file (WAL mode).
///
/// Suitable for single-node / self-hosted deployments and local development.
/// For multi-node production, use the Postgres-backed `CentralDb` impl.
#[derive(Clone)]
pub struct SqliteCentralDb {
    pool: SqlitePool,
}

impl SqliteCentralDb {
    /// Create (or open) a SQLite CentralDb at the given file path.
    ///
    /// Creates the file and parent directories if they don't exist.
    /// Runs the internal schema migration on startup.
    pub async fn new(path: impl AsRef<Path>) -> CoreResult<Self> {
        let path = path.as_ref();

        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|e| CoreError::backend("sqlite_central_db", e))?;
        }

        let opts = SqliteConnectOptions::from_str(&format!("sqlite://{}?mode=rwc", path.display()))
            .map_err(|e| CoreError::backend("sqlite_central_db", e))?
            .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal)
            .create_if_missing(true);

        let pool = SqlitePoolOptions::new()
            .max_connections(5)
            .connect_with(opts)
            .await
            .map_err(|e| CoreError::backend("sqlite_central_db", e))?;

        sqlx::query(SCHEMA)
            .execute(&pool)
            .await
            .map_err(|e| CoreError::backend("sqlite_central_db_migration", e))?;

        Ok(Self { pool })
    }
}

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS orgs (
    org_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS users (
    org_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    email TEXT NOT NULL,
    display_name TEXT,
    role TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (org_id, user_id)
);

CREATE TABLE IF NOT EXISTS api_keys (
    key_id TEXT PRIMARY KEY,
    org_id TEXT NOT NULL,
    name TEXT NOT NULL,
    actor TEXT NOT NULL,
    scopes TEXT NOT NULL,
    secret_hash TEXT NOT NULL,
    created_at TEXT NOT NULL,
    expires_at TEXT,
    last_used_at TEXT,
    UNIQUE (org_id, name)
);

CREATE INDEX IF NOT EXISTS api_keys_org_idx ON api_keys(org_id);

CREATE TABLE IF NOT EXISTS platform_config (
    org_id TEXT PRIMARY KEY,
    data TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS audit_log (
    id TEXT PRIMARY KEY,
    org_id TEXT NOT NULL,
    project_id TEXT,
    actor TEXT NOT NULL,
    action TEXT NOT NULL,
    payload TEXT NOT NULL,
    outcome TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS connector_credentials (
    org_id TEXT NOT NULL,
    connector_id TEXT NOT NULL,
    ciphertext BLOB NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (org_id, connector_id)
);

CREATE TABLE IF NOT EXISTS sync_state (
    key_str TEXT PRIMARY KEY,
    org_id TEXT NOT NULL,
    project_id TEXT,
    connector_id TEXT NOT NULL,
    scope TEXT NOT NULL,
    cursor TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

	CREATE TABLE IF NOT EXISTS source_configs (
	    org_id TEXT NOT NULL,
	    source_id TEXT NOT NULL,
	    project_id TEXT NOT NULL,
	    connector_id TEXT NOT NULL,
	    scope TEXT NOT NULL,
	    enabled INTEGER NOT NULL,
	    project_db_json TEXT NOT NULL,
	    schedule_json TEXT,
	    event_triggers_json TEXT NOT NULL,
	    processor_json TEXT NOT NULL,
	    settings TEXT NOT NULL,
	    created_at TEXT NOT NULL,
	    updated_at TEXT NOT NULL,
	    PRIMARY KEY (org_id, source_id)
	);

	CREATE TABLE IF NOT EXISTS refresh_runs (
	    run_id TEXT PRIMARY KEY,
	    org_id TEXT NOT NULL,
	    project_id TEXT NOT NULL,
	    source_id TEXT NOT NULL,
	    connector_id TEXT NOT NULL,
	    trigger_json TEXT NOT NULL,
	    status TEXT NOT NULL,
	    started_at TEXT NOT NULL,
	    finished_at TEXT,
	    records_pulled INTEGER NOT NULL,
	    entities_stored INTEGER NOT NULL,
	    error_message TEXT,
	    cursor_json TEXT
	);

	CREATE TABLE IF NOT EXISTS resources (
	    org_id TEXT NOT NULL,
	    resource_id TEXT NOT NULL,
	    resource_type TEXT NOT NULL,
	    config_json TEXT NOT NULL,
	    created_at TEXT NOT NULL,
	    updated_at TEXT NOT NULL,
	    PRIMARY KEY (org_id, resource_id)
	);

	CREATE TABLE IF NOT EXISTS operations (
	    org_id TEXT NOT NULL,
	    operation_id TEXT NOT NULL,
	    resource_id TEXT NOT NULL,
	    operation_type TEXT NOT NULL,
	    config_json TEXT NOT NULL,
	    created_at TEXT NOT NULL,
	    updated_at TEXT NOT NULL,
	    PRIMARY KEY (org_id, operation_id)
	);

	CREATE TABLE IF NOT EXISTS operation_runs (
	    id TEXT PRIMARY KEY,
	    org_id TEXT NOT NULL,
	    operation_id TEXT NOT NULL,
	    source_event_id TEXT,
	    status TEXT NOT NULL,
	    error TEXT,
	    output_json TEXT NOT NULL,
	    created_at TEXT NOT NULL
	);
"#;

// ── Helpers ─────────────────────────────────────────────────────

fn sync_state_key_string(key: &SyncStateKey) -> String {
    let project = key
        .project_id
        .map(|p| p.to_string())
        .unwrap_or_else(|| "_central".to_string());
    format!(
        "{}/{}/{}/{}",
        key.org_id, project, key.connector_id, key.scope
    )
}

fn db_err(e: sqlx::Error) -> CoreError {
    CoreError::backend("sqlite_central_db", e)
}

fn parse_org_id(s: &str) -> OrgId {
    OrgId(Uuid::parse_str(s).unwrap_or(Uuid::nil()))
}

fn parse_project_id(s: &str) -> ProjectId {
    ProjectId(Uuid::parse_str(s).unwrap_or(Uuid::nil()))
}

fn parse_dt(s: &str) -> DateTime<Utc> {
    s.parse::<DateTime<Utc>>().unwrap_or_else(|_| Utc::now())
}

// ── CentralDb impl ─────────────────────────────────────────────

#[async_trait]
impl CentralDb for SqliteCentralDb {
    async fn upsert_org(&self, org: &OrgRecord) -> CoreResult<()> {
        sqlx::query(
            "INSERT INTO orgs (org_id, name, created_at) VALUES (?1, ?2, ?3)
             ON CONFLICT(org_id) DO UPDATE SET name = excluded.name",
        )
        .bind(org.org_id.to_string())
        .bind(&org.name)
        .bind(org.created_at.to_rfc3339())
        .execute(&self.pool)
        .await
        .map_err(db_err)?;
        Ok(())
    }

    async fn get_org(&self, org_id: OrgId) -> CoreResult<Option<OrgRecord>> {
        let row = sqlx::query("SELECT org_id, name, created_at FROM orgs WHERE org_id = ?1")
            .bind(org_id.to_string())
            .fetch_optional(&self.pool)
            .await
            .map_err(db_err)?;

        Ok(row.map(|r| {
            let org_id_str: String = r.get("org_id");
            let name: String = r.get("name");
            let created_at_str: String = r.get("created_at");
            OrgRecord {
                org_id: parse_org_id(&org_id_str),
                name,
                created_at: parse_dt(&created_at_str),
            }
        }))
    }

    async fn upsert_user(&self, user: &UserRecord) -> CoreResult<()> {
        sqlx::query(
            "INSERT INTO users (org_id, user_id, email, display_name, role, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)
             ON CONFLICT(org_id, user_id) DO UPDATE SET
               email = excluded.email,
               display_name = excluded.display_name,
               role = excluded.role",
        )
        .bind(user.org_id.to_string())
        .bind(user.user_id.to_string())
        .bind(&user.email)
        .bind(&user.display_name)
        .bind(user.role.as_str())
        .bind(user.created_at.to_rfc3339())
        .execute(&self.pool)
        .await
        .map_err(db_err)?;
        Ok(())
    }

    async fn get_user(&self, org_id: OrgId, user_id: Uuid) -> CoreResult<Option<UserRecord>> {
        let row = sqlx::query(
            "SELECT org_id, user_id, email, display_name, role, created_at
             FROM users WHERE org_id = ?1 AND user_id = ?2",
        )
        .bind(org_id.to_string())
        .bind(user_id.to_string())
        .fetch_optional(&self.pool)
        .await
        .map_err(db_err)?;

        Ok(row.map(|r| row_to_user(&r)))
    }

    async fn list_users(&self, org_id: OrgId, query: ListQuery) -> CoreResult<Vec<UserRecord>> {
        let rows = sqlx::query(
            "SELECT org_id, user_id, email, display_name, role, created_at
             FROM users WHERE org_id = ?1
             ORDER BY created_at, user_id
             LIMIT ?2 OFFSET ?3",
        )
        .bind(org_id.to_string())
        .bind(query.limit as i64)
        .bind(query.offset as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(db_err)?;

        Ok(rows.iter().map(row_to_user).collect())
    }

    async fn upsert_api_key(&self, key: &ApiKeyRecord) -> CoreResult<()> {
        sqlx::query(
            "INSERT INTO api_keys (key_id, org_id, name, actor, scopes, secret_hash, created_at, expires_at, last_used_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
             ON CONFLICT(key_id) DO UPDATE SET
               org_id = excluded.org_id,
               name = excluded.name,
               actor = excluded.actor,
               scopes = excluded.scopes,
               secret_hash = excluded.secret_hash,
               expires_at = excluded.expires_at,
               last_used_at = excluded.last_used_at",
        )
        .bind(key.key_id.to_string())
        .bind(key.org_id.to_string())
        .bind(&key.name)
        .bind(serde_json::to_string(&key.actor).unwrap_or_default())
        .bind(serde_json::to_string(&key.scopes).unwrap_or("[]".to_string()))
        .bind(&key.secret_hash)
        .bind(key.created_at.to_rfc3339())
        .bind(key.expires_at.map(|dt| dt.to_rfc3339()))
        .bind(key.last_used_at.map(|dt| dt.to_rfc3339()))
        .execute(&self.pool)
        .await
        .map_err(db_err)?;
        Ok(())
    }

    async fn get_api_key_by_id(&self, key_id: Uuid) -> CoreResult<Option<ApiKeyRecord>> {
        let row = sqlx::query(
            "SELECT key_id, org_id, name, actor, scopes, secret_hash, created_at, expires_at, last_used_at
             FROM api_keys WHERE key_id = ?1",
        )
        .bind(key_id.to_string())
        .fetch_optional(&self.pool)
        .await
        .map_err(db_err)?;

        Ok(row.map(|r| {
            let key_id_str: String = r.get("key_id");
            let org_id_str: String = r.get("org_id");
            let name: String = r.get("name");
            let actor_str: String = r.get("actor");
            let scopes_str: String = r.get("scopes");
            let secret_hash: String = r.get("secret_hash");
            let created_at_str: String = r.get("created_at");
            let expires_at_str: Option<String> = r.get("expires_at");
            let last_used_at_str: Option<String> = r.get("last_used_at");

            let key_id = Uuid::parse_str(&key_id_str).unwrap_or(Uuid::nil());
            let actor: AgentIdentity =
                serde_json::from_str(&actor_str).unwrap_or(AgentIdentity::System {
                    name: "unknown".to_string(),
                });
            let scopes: Vec<String> = serde_json::from_str(&scopes_str).unwrap_or_default();

            ApiKeyRecord {
                key_id,
                org_id: parse_org_id(&org_id_str),
                name,
                actor,
                scopes,
                secret_hash,
                created_at: parse_dt(&created_at_str),
                expires_at: expires_at_str.as_deref().map(parse_dt),
                last_used_at: last_used_at_str.as_deref().map(parse_dt),
            }
        }))
    }

    async fn list_api_keys(
        &self,
        org_id: OrgId,
        query: ListQuery,
    ) -> CoreResult<Vec<ApiKeyRecord>> {
        let rows = sqlx::query(
            "SELECT key_id, org_id, name, actor, scopes, secret_hash, created_at, expires_at, last_used_at
             FROM api_keys WHERE org_id = ?1
             ORDER BY created_at DESC, key_id DESC
             LIMIT ?2 OFFSET ?3",
        )
        .bind(org_id.to_string())
        .bind(query.limit as i64)
        .bind(query.offset as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(db_err)?;

        let mut out = Vec::with_capacity(rows.len());
        for r in rows {
            let key_id_str: String = r.get("key_id");
            let org_id_str: String = r.get("org_id");
            let name: String = r.get("name");
            let actor_str: String = r.get("actor");
            let scopes_str: String = r.get("scopes");
            let secret_hash: String = r.get("secret_hash");
            let created_at_str: String = r.get("created_at");
            let expires_at_str: Option<String> = r.get("expires_at");
            let last_used_at_str: Option<String> = r.get("last_used_at");

            let key_id = Uuid::parse_str(&key_id_str).unwrap_or(Uuid::nil());
            let actor: AgentIdentity =
                serde_json::from_str(&actor_str).unwrap_or(AgentIdentity::System {
                    name: "unknown".to_string(),
                });
            let scopes: Vec<String> = serde_json::from_str(&scopes_str).unwrap_or_default();

            out.push(ApiKeyRecord {
                key_id,
                org_id: parse_org_id(&org_id_str),
                name,
                actor,
                scopes,
                secret_hash,
                created_at: parse_dt(&created_at_str),
                expires_at: expires_at_str.as_deref().map(parse_dt),
                last_used_at: last_used_at_str.as_deref().map(parse_dt),
            });
        }
        Ok(out)
    }

    async fn delete_api_key(&self, org_id: OrgId, key_id: Uuid) -> CoreResult<()> {
        sqlx::query("DELETE FROM api_keys WHERE org_id = ?1 AND key_id = ?2")
            .bind(org_id.to_string())
            .bind(key_id.to_string())
            .execute(&self.pool)
            .await
            .map_err(db_err)?;
        Ok(())
    }

    async fn touch_api_key_last_used(&self, key_id: Uuid, at: DateTime<Utc>) -> CoreResult<()> {
        sqlx::query("UPDATE api_keys SET last_used_at = ?2 WHERE key_id = ?1")
            .bind(key_id.to_string())
            .bind(at.to_rfc3339())
            .execute(&self.pool)
            .await
            .map_err(db_err)?;
        Ok(())
    }

    async fn get_platform_config(&self, org_id: OrgId) -> CoreResult<Option<PlatformConfig>> {
        let row =
            sqlx::query("SELECT org_id, data, updated_at FROM platform_config WHERE org_id = ?1")
                .bind(org_id.to_string())
                .fetch_optional(&self.pool)
                .await
                .map_err(db_err)?;

        Ok(row.map(|r| {
            let org_id_str: String = r.get("org_id");
            let data_str: String = r.get("data");
            let updated_at_str: String = r.get("updated_at");
            PlatformConfig {
                org_id: parse_org_id(&org_id_str),
                data: serde_json::from_str(&data_str).unwrap_or_default(),
                updated_at: parse_dt(&updated_at_str),
            }
        }))
    }

    async fn set_platform_config(&self, config: &PlatformConfig) -> CoreResult<()> {
        sqlx::query(
            "INSERT INTO platform_config (org_id, data, updated_at) VALUES (?1, ?2, ?3)
             ON CONFLICT(org_id) DO UPDATE SET data = excluded.data, updated_at = excluded.updated_at",
        )
        .bind(config.org_id.to_string())
        .bind(serde_json::to_string(&config.data).unwrap_or_default())
        .bind(config.updated_at.to_rfc3339())
        .execute(&self.pool)
        .await
        .map_err(db_err)?;
        Ok(())
    }

    async fn append_audit_entry(&self, entry: &AuditEntry) -> CoreResult<()> {
        sqlx::query(
            "INSERT INTO audit_log (id, org_id, project_id, actor, action, payload, outcome, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        )
        .bind(entry.id.to_string())
        .bind(entry.org_id.to_string())
        .bind(entry.project_id.map(|p| p.to_string()))
        .bind(serde_json::to_string(&entry.actor).unwrap_or_default())
        .bind(&entry.action)
        .bind(serde_json::to_string(&entry.payload).unwrap_or_default())
        .bind(&entry.outcome)
        .bind(entry.created_at.to_rfc3339())
        .execute(&self.pool)
        .await
        .map_err(db_err)?;
        Ok(())
    }

    async fn list_audit_entries(
        &self,
        org_id: OrgId,
        query: AuditQuery,
    ) -> CoreResult<Vec<AuditEntry>> {
        let mut conditions = vec!["org_id = ?1".to_string()];
        let mut next_param = 2;

        if query.project_id.is_some() {
            conditions.push(format!("project_id = ?{next_param}"));
            next_param += 1;
        }
        if query.action_prefix.is_some() {
            conditions.push(format!("action LIKE ?{next_param}"));
            next_param += 1;
        }
        if query.actor_contains.is_some() {
            conditions.push(format!("actor LIKE ?{next_param}"));
            next_param += 1;
        }
        if query.since.is_some() {
            conditions.push(format!("created_at >= ?{next_param}"));
            next_param += 1;
        }
        if query.until.is_some() {
            conditions.push(format!("created_at <= ?{next_param}"));
            next_param += 1;
        }

        let sql = format!(
            "SELECT id, org_id, project_id, actor, action, payload, outcome, created_at
             FROM audit_log WHERE {}
             ORDER BY created_at DESC, id DESC
             LIMIT ?{next_param}",
            conditions.join(" AND "),
        );

        let mut q = sqlx::query(&sql).bind(org_id.to_string());

        if let Some(pid) = &query.project_id {
            q = q.bind(pid.to_string());
        }
        if let Some(prefix) = &query.action_prefix {
            q = q.bind(format!("{prefix}%"));
        }
        if let Some(needle) = &query.actor_contains {
            q = q.bind(format!("%{needle}%"));
        }
        if let Some(since) = &query.since {
            q = q.bind(since.to_rfc3339());
        }
        if let Some(until) = &query.until {
            q = q.bind(until.to_rfc3339());
        }
        q = q.bind(query.limit as i64);

        let rows = q.fetch_all(&self.pool).await.map_err(db_err)?;

        Ok(rows
            .iter()
            .map(|r| {
                let id_str: String = r.get("id");
                let org_id_str: String = r.get("org_id");
                let project_id_str: Option<String> = r.get("project_id");
                let actor_str: String = r.get("actor");
                let action: String = r.get("action");
                let payload_str: String = r.get("payload");
                let outcome: String = r.get("outcome");
                let created_at_str: String = r.get("created_at");

                AuditEntry {
                    id: Uuid::parse_str(&id_str).unwrap_or(Uuid::nil()),
                    org_id: parse_org_id(&org_id_str),
                    project_id: project_id_str.map(|s| parse_project_id(&s)),
                    actor: serde_json::from_str(&actor_str).unwrap_or(AgentIdentity::System {
                        name: "unknown".to_string(),
                    }),
                    action,
                    payload: serde_json::from_str(&payload_str).unwrap_or_default(),
                    outcome,
                    created_at: parse_dt(&created_at_str),
                }
            })
            .collect())
    }

    async fn upsert_connector_credential(
        &self,
        credential: &ConnectorCredential,
    ) -> CoreResult<()> {
        sqlx::query(
            "INSERT INTO connector_credentials (org_id, connector_id, ciphertext, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT(org_id, connector_id) DO UPDATE SET
               ciphertext = excluded.ciphertext,
               updated_at = excluded.updated_at",
        )
        .bind(credential.org_id.to_string())
        .bind(&credential.connector_id)
        .bind(&credential.ciphertext)
        .bind(credential.created_at.to_rfc3339())
        .bind(credential.updated_at.to_rfc3339())
        .execute(&self.pool)
        .await
        .map_err(db_err)?;
        Ok(())
    }

    async fn get_connector_credential(
        &self,
        org_id: OrgId,
        connector_id: &str,
    ) -> CoreResult<Option<ConnectorCredential>> {
        let row = sqlx::query(
            "SELECT org_id, connector_id, ciphertext, created_at, updated_at
             FROM connector_credentials WHERE org_id = ?1 AND connector_id = ?2",
        )
        .bind(org_id.to_string())
        .bind(connector_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(db_err)?;

        Ok(row.map(|r| {
            let org_id_str: String = r.get("org_id");
            let connector_id: String = r.get("connector_id");
            let ciphertext: Vec<u8> = r.get("ciphertext");
            let created_at_str: String = r.get("created_at");
            let updated_at_str: String = r.get("updated_at");
            ConnectorCredential {
                org_id: parse_org_id(&org_id_str),
                connector_id,
                ciphertext,
                created_at: parse_dt(&created_at_str),
                updated_at: parse_dt(&updated_at_str),
            }
        }))
    }

    async fn delete_connector_credential(
        &self,
        org_id: OrgId,
        connector_id: &str,
    ) -> CoreResult<()> {
        sqlx::query("DELETE FROM connector_credentials WHERE org_id = ?1 AND connector_id = ?2")
            .bind(org_id.to_string())
            .bind(connector_id)
            .execute(&self.pool)
            .await
            .map_err(db_err)?;
        Ok(())
    }

    async fn list_connector_credentials(
        &self,
        org_id: OrgId,
    ) -> CoreResult<Vec<ConnectorCredential>> {
        let rows = sqlx::query(
            "SELECT org_id, connector_id, ciphertext, created_at, updated_at
             FROM connector_credentials WHERE org_id = ?1 ORDER BY connector_id",
        )
        .bind(org_id.to_string())
        .fetch_all(&self.pool)
        .await
        .map_err(db_err)?;

        Ok(rows
            .iter()
            .map(|r| {
                let org_id_str: String = r.get("org_id");
                let connector_id: String = r.get("connector_id");
                let ciphertext: Vec<u8> = r.get("ciphertext");
                let created_at_str: String = r.get("created_at");
                let updated_at_str: String = r.get("updated_at");
                ConnectorCredential {
                    org_id: parse_org_id(&org_id_str),
                    connector_id,
                    ciphertext,
                    created_at: parse_dt(&created_at_str),
                    updated_at: parse_dt(&updated_at_str),
                }
            })
            .collect())
    }

    async fn upsert_resource(&self, resource: &ResourceRecord) -> CoreResult<()> {
        sqlx::query(
            "INSERT INTO resources (org_id, resource_id, resource_type, config_json, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)
             ON CONFLICT(org_id, resource_id) DO UPDATE SET
               resource_type = excluded.resource_type,
               config_json = excluded.config_json,
               updated_at = excluded.updated_at",
        )
        .bind(resource.org_id.to_string())
        .bind(&resource.resource_id)
        .bind(&resource.resource_type)
        .bind(serde_json::to_string(&resource.config).unwrap_or_else(|_| "{}".to_string()))
        .bind(resource.created_at.to_rfc3339())
        .bind(resource.updated_at.to_rfc3339())
        .execute(&self.pool)
        .await
        .map_err(db_err)?;
        Ok(())
    }

    async fn get_resource(
        &self,
        org_id: OrgId,
        resource_id: &str,
    ) -> CoreResult<Option<ResourceRecord>> {
        let row = sqlx::query(
            "SELECT org_id, resource_id, resource_type, config_json, created_at, updated_at
             FROM resources WHERE org_id = ?1 AND resource_id = ?2",
        )
        .bind(org_id.to_string())
        .bind(resource_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(db_err)?;

        Ok(row.map(|r| {
            let org_id_str: String = r.get("org_id");
            let resource_id: String = r.get("resource_id");
            let resource_type: String = r.get("resource_type");
            let config_str: String = r.get("config_json");
            let created_at_str: String = r.get("created_at");
            let updated_at_str: String = r.get("updated_at");
            ResourceRecord {
                org_id: parse_org_id(&org_id_str),
                resource_id,
                resource_type,
                config: serde_json::from_str(&config_str).unwrap_or_default(),
                created_at: parse_dt(&created_at_str),
                updated_at: parse_dt(&updated_at_str),
            }
        }))
    }

    async fn list_resources(
        &self,
        org_id: OrgId,
        query: ListQuery,
    ) -> CoreResult<Vec<ResourceRecord>> {
        let rows = sqlx::query(
            "SELECT org_id, resource_id, resource_type, config_json, created_at, updated_at
             FROM resources WHERE org_id = ?1
             ORDER BY updated_at DESC
             LIMIT ?2 OFFSET ?3",
        )
        .bind(org_id.to_string())
        .bind(query.limit as i64)
        .bind(query.offset as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(db_err)?;

        Ok(rows
            .iter()
            .map(|r| {
                let org_id_str: String = r.get("org_id");
                let resource_id: String = r.get("resource_id");
                let resource_type: String = r.get("resource_type");
                let config_str: String = r.get("config_json");
                let created_at_str: String = r.get("created_at");
                let updated_at_str: String = r.get("updated_at");
                ResourceRecord {
                    org_id: parse_org_id(&org_id_str),
                    resource_id,
                    resource_type,
                    config: serde_json::from_str(&config_str).unwrap_or_default(),
                    created_at: parse_dt(&created_at_str),
                    updated_at: parse_dt(&updated_at_str),
                }
            })
            .collect())
    }

    async fn upsert_operation(&self, operation: &OperationRecord) -> CoreResult<()> {
        sqlx::query(
            "INSERT INTO operations (org_id, operation_id, resource_id, operation_type, config_json, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
             ON CONFLICT(org_id, operation_id) DO UPDATE SET
               resource_id = excluded.resource_id,
               operation_type = excluded.operation_type,
               config_json = excluded.config_json,
               updated_at = excluded.updated_at",
        )
        .bind(operation.org_id.to_string())
        .bind(&operation.operation_id)
        .bind(&operation.resource_id)
        .bind(&operation.operation_type)
        .bind(serde_json::to_string(&operation.config).unwrap_or_else(|_| "{}".to_string()))
        .bind(operation.created_at.to_rfc3339())
        .bind(operation.updated_at.to_rfc3339())
        .execute(&self.pool)
        .await
        .map_err(db_err)?;
        Ok(())
    }

    async fn get_operation(
        &self,
        org_id: OrgId,
        operation_id: &str,
    ) -> CoreResult<Option<OperationRecord>> {
        let row = sqlx::query(
            "SELECT org_id, operation_id, resource_id, operation_type, config_json, created_at, updated_at
             FROM operations WHERE org_id = ?1 AND operation_id = ?2",
        )
        .bind(org_id.to_string())
        .bind(operation_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(db_err)?;

        Ok(row.map(|r| {
            let org_id_str: String = r.get("org_id");
            let operation_id: String = r.get("operation_id");
            let resource_id: String = r.get("resource_id");
            let operation_type: String = r.get("operation_type");
            let config_str: String = r.get("config_json");
            let created_at_str: String = r.get("created_at");
            let updated_at_str: String = r.get("updated_at");
            OperationRecord {
                org_id: parse_org_id(&org_id_str),
                operation_id,
                resource_id,
                operation_type,
                config: serde_json::from_str(&config_str).unwrap_or_default(),
                created_at: parse_dt(&created_at_str),
                updated_at: parse_dt(&updated_at_str),
            }
        }))
    }

    async fn list_operations(
        &self,
        org_id: OrgId,
        query: ListQuery,
    ) -> CoreResult<Vec<OperationRecord>> {
        let rows = sqlx::query(
            "SELECT org_id, operation_id, resource_id, operation_type, config_json, created_at, updated_at
             FROM operations WHERE org_id = ?1
             ORDER BY updated_at DESC
             LIMIT ?2 OFFSET ?3",
        )
        .bind(org_id.to_string())
        .bind(query.limit as i64)
        .bind(query.offset as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(db_err)?;

        Ok(rows
            .iter()
            .map(|r| {
                let org_id_str: String = r.get("org_id");
                let operation_id: String = r.get("operation_id");
                let resource_id: String = r.get("resource_id");
                let operation_type: String = r.get("operation_type");
                let config_str: String = r.get("config_json");
                let created_at_str: String = r.get("created_at");
                let updated_at_str: String = r.get("updated_at");
                OperationRecord {
                    org_id: parse_org_id(&org_id_str),
                    operation_id,
                    resource_id,
                    operation_type,
                    config: serde_json::from_str(&config_str).unwrap_or_default(),
                    created_at: parse_dt(&created_at_str),
                    updated_at: parse_dt(&updated_at_str),
                }
            })
            .collect())
    }

    async fn append_operation_run(&self, run: &OperationRunRecord) -> CoreResult<()> {
        sqlx::query(
            "INSERT INTO operation_runs (id, org_id, operation_id, source_event_id, status, error, output_json, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        )
        .bind(run.id.to_string())
        .bind(run.org_id.to_string())
        .bind(&run.operation_id)
        .bind(&run.source_event_id)
        .bind(&run.status)
        .bind(&run.error)
        .bind(serde_json::to_string(&run.output).unwrap_or_else(|_| "{}".to_string()))
        .bind(run.created_at.to_rfc3339())
        .execute(&self.pool)
        .await
        .map_err(db_err)?;
        Ok(())
    }

    async fn list_operation_runs(
        &self,
        org_id: OrgId,
        operation_id: Option<&str>,
        query: ListQuery,
    ) -> CoreResult<Vec<OperationRunRecord>> {
        let (sql, binds): (String, Vec<String>) = match operation_id {
            None => (
                "SELECT id, org_id, operation_id, source_event_id, status, error, output_json, created_at
                 FROM operation_runs WHERE org_id = ?1
                 ORDER BY created_at DESC
                 LIMIT ?2 OFFSET ?3"
                    .to_string(),
                vec![],
            ),
            Some(op) => (
                "SELECT id, org_id, operation_id, source_event_id, status, error, output_json, created_at
                 FROM operation_runs WHERE org_id = ?1 AND operation_id = ?2
                 ORDER BY created_at DESC
                 LIMIT ?3 OFFSET ?4"
                    .to_string(),
                vec![op.to_string()],
            ),
        };

        let mut q = sqlx::query(&sql);
        q = q.bind(org_id.to_string());
        if let Some(op) = binds.get(0) {
            q = q.bind(op);
            q = q.bind(query.limit as i64);
            q = q.bind(query.offset as i64);
        } else {
            q = q.bind(query.limit as i64);
            q = q.bind(query.offset as i64);
        }

        let rows = q.fetch_all(&self.pool).await.map_err(db_err)?;
        Ok(rows
            .iter()
            .map(|r| {
                let id_str: String = r.get("id");
                let org_id_str: String = r.get("org_id");
                let operation_id: String = r.get("operation_id");
                let source_event_id: Option<String> = r.get("source_event_id");
                let status: String = r.get("status");
                let error: Option<String> = r.get("error");
                let output_str: String = r.get("output_json");
                let created_at_str: String = r.get("created_at");
                OperationRunRecord {
                    id: Uuid::parse_str(&id_str).unwrap_or(Uuid::nil()),
                    org_id: parse_org_id(&org_id_str),
                    operation_id,
                    source_event_id,
                    status,
                    error,
                    output: serde_json::from_str(&output_str).unwrap_or_default(),
                    created_at: parse_dt(&created_at_str),
                }
            })
            .collect())
    }

    async fn upsert_sync_state(&self, state: &SyncState) -> CoreResult<()> {
        let k = sync_state_key_string(&state.key);
        sqlx::query(
            "INSERT INTO sync_state (key_str, org_id, project_id, connector_id, scope, cursor, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
             ON CONFLICT(key_str) DO UPDATE SET cursor = excluded.cursor, updated_at = excluded.updated_at",
        )
        .bind(&k)
        .bind(state.key.org_id.to_string())
        .bind(state.key.project_id.map(|p| p.to_string()))
        .bind(&state.key.connector_id)
        .bind(&state.key.scope)
        .bind(serde_json::to_string(&state.cursor).unwrap_or_default())
        .bind(state.updated_at.to_rfc3339())
        .execute(&self.pool)
        .await
        .map_err(db_err)?;
        Ok(())
    }

    async fn get_sync_state(&self, key: &SyncStateKey) -> CoreResult<Option<SyncState>> {
        let k = sync_state_key_string(key);
        let row = sqlx::query(
            "SELECT key_str, org_id, project_id, connector_id, scope, cursor, updated_at
             FROM sync_state WHERE key_str = ?1",
        )
        .bind(&k)
        .fetch_optional(&self.pool)
        .await
        .map_err(db_err)?;

        Ok(row.map(|r| row_to_sync_state(&r)))
    }

    async fn delete_sync_state(&self, key: &SyncStateKey) -> CoreResult<()> {
        let k = sync_state_key_string(key);
        sqlx::query("DELETE FROM sync_state WHERE key_str = ?1")
            .bind(&k)
            .execute(&self.pool)
            .await
            .map_err(db_err)?;
        Ok(())
    }

    async fn upsert_source_config(&self, source: &SourceConfig) -> CoreResult<()> {
        let schedule_json = source
            .schedule
            .as_ref()
            .map(|s| serde_json::to_string(s).unwrap_or_default());
        let event_triggers_json = serde_json::to_string(&source.event_triggers).unwrap_or_default();
        let project_db_json = serde_json::to_string(&source.project_db).unwrap_or_default();
        let processor_json = serde_json::to_string(&source.processor).unwrap_or_else(|_| "{\"type\":\"connector\"}".to_string());
        let settings_json = serde_json::to_string(&source.settings).unwrap_or_default();

        sqlx::query(
            "INSERT INTO source_configs (org_id, source_id, project_id, connector_id, scope,
             enabled, project_db_json, schedule_json, event_triggers_json, processor_json, settings,
             created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)
             ON CONFLICT(org_id, source_id) DO UPDATE SET
               project_id = excluded.project_id,
               connector_id = excluded.connector_id,
               scope = excluded.scope,
               enabled = excluded.enabled,
               project_db_json = excluded.project_db_json,
               schedule_json = excluded.schedule_json,
               event_triggers_json = excluded.event_triggers_json,
               processor_json = excluded.processor_json,
               settings = excluded.settings,
               updated_at = excluded.updated_at",
        )
        .bind(source.org_id.to_string())
        .bind(&source.source_id)
        .bind(source.project_id.to_string())
        .bind(&source.connector_id)
        .bind(&source.scope)
        .bind(source.enabled as i32)
        .bind(&project_db_json)
        .bind(&schedule_json)
        .bind(&event_triggers_json)
        .bind(&processor_json)
        .bind(&settings_json)
        .bind(source.created_at.to_rfc3339())
        .bind(source.updated_at.to_rfc3339())
        .execute(&self.pool)
        .await
        .map_err(db_err)?;
        Ok(())
    }

    async fn get_source_config(
        &self,
        org_id: OrgId,
        source_id: &str,
    ) -> CoreResult<Option<SourceConfig>> {
        let row = sqlx::query(
            "SELECT org_id, source_id, project_id, connector_id, scope, enabled,
                    project_db_json, schedule_json, event_triggers_json, processor_json, settings,
                    created_at, updated_at
             FROM source_configs WHERE org_id = ?1 AND source_id = ?2",
        )
        .bind(org_id.to_string())
        .bind(source_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(db_err)?;

        Ok(row.map(|r| row_to_source_config(&r)))
    }

    async fn list_source_configs(
        &self,
        org_id: OrgId,
        query: ListQuery,
    ) -> CoreResult<Vec<SourceConfig>> {
        let rows = sqlx::query(
            "SELECT org_id, source_id, project_id, connector_id, scope, enabled,
                    project_db_json, schedule_json, event_triggers_json, processor_json, settings,
                    created_at, updated_at
             FROM source_configs WHERE org_id = ?1
             ORDER BY source_id
             LIMIT ?2 OFFSET ?3",
        )
        .bind(org_id.to_string())
        .bind(query.limit as i64)
        .bind(query.offset as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(db_err)?;

        Ok(rows.iter().map(row_to_source_config).collect())
    }

    async fn delete_source_config(&self, org_id: OrgId, source_id: &str) -> CoreResult<()> {
        sqlx::query("DELETE FROM source_configs WHERE org_id = ?1 AND source_id = ?2")
            .bind(org_id.to_string())
            .bind(source_id)
            .execute(&self.pool)
            .await
            .map_err(db_err)?;
        Ok(())
    }

    async fn upsert_refresh_run(&self, run: &RefreshRun) -> CoreResult<()> {
        let trigger_json = serde_json::to_string(&run.trigger).unwrap_or_default();
        let status_str = serde_json::to_string(&run.status)
            .unwrap_or_default()
            .trim_matches('"')
            .to_string();
        let cursor_json = run
            .cursor
            .as_ref()
            .map(|c| serde_json::to_string(c).unwrap_or_default());

        sqlx::query(
            "INSERT INTO refresh_runs (run_id, org_id, project_id, source_id, connector_id,
             trigger_json, status, started_at, finished_at, records_pulled, entities_stored,
             error_message, cursor_json)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)
             ON CONFLICT(run_id) DO UPDATE SET
               status = excluded.status,
               finished_at = excluded.finished_at,
               records_pulled = excluded.records_pulled,
               entities_stored = excluded.entities_stored,
               error_message = excluded.error_message,
               cursor_json = excluded.cursor_json",
        )
        .bind(run.run_id.to_string())
        .bind(run.org_id.to_string())
        .bind(run.project_id.to_string())
        .bind(&run.source_id)
        .bind(&run.connector_id)
        .bind(&trigger_json)
        .bind(&status_str)
        .bind(run.started_at.to_rfc3339())
        .bind(run.finished_at.map(|t| t.to_rfc3339()))
        .bind(run.records_pulled as i64)
        .bind(run.entities_stored as i64)
        .bind(&run.error_message)
        .bind(&cursor_json)
        .execute(&self.pool)
        .await
        .map_err(db_err)?;
        Ok(())
    }

    async fn list_refresh_runs(
        &self,
        org_id: OrgId,
        query: RefreshRunQuery,
    ) -> CoreResult<Vec<RefreshRun>> {
        let mut conditions = vec!["org_id = ?1".to_string()];
        let mut next_param = 2;

        if query.source_id.is_some() {
            conditions.push(format!("source_id = ?{next_param}"));
            next_param += 1;
        }
        if query.status.is_some() {
            conditions.push(format!("status = ?{next_param}"));
            next_param += 1;
        }
        if query.since.is_some() {
            conditions.push(format!("started_at >= ?{next_param}"));
            next_param += 1;
        }
        if query.until.is_some() {
            conditions.push(format!("started_at <= ?{next_param}"));
            next_param += 1;
        }

        let sql = format!(
            "SELECT run_id, org_id, project_id, source_id, connector_id, trigger_json,
                    status, started_at, finished_at, records_pulled, entities_stored,
                    error_message, cursor_json
             FROM refresh_runs WHERE {}
             ORDER BY started_at DESC
             LIMIT ?{next_param} OFFSET ?{}",
            conditions.join(" AND "),
            next_param + 1,
        );

        let mut q = sqlx::query(&sql).bind(org_id.to_string());

        if let Some(sid) = &query.source_id {
            q = q.bind(sid.clone());
        }
        if let Some(status) = &query.status {
            let s = serde_json::to_string(status)
                .unwrap_or_default()
                .trim_matches('"')
                .to_string();
            q = q.bind(s);
        }
        if let Some(since) = &query.since {
            q = q.bind(since.to_rfc3339());
        }
        if let Some(until) = &query.until {
            q = q.bind(until.to_rfc3339());
        }
        q = q.bind(query.limit as i64);
        q = q.bind(query.offset as i64);

        let rows = q.fetch_all(&self.pool).await.map_err(db_err)?;

        Ok(rows.iter().map(row_to_refresh_run).collect())
    }
}

// ── Row mapping helpers ─────────────────────────────────────────

fn row_to_user(r: &sqlx::sqlite::SqliteRow) -> UserRecord {
    let org_id_str: String = r.get("org_id");
    let user_id_str: String = r.get("user_id");
    let email: String = r.get("email");
    let display_name: Option<String> = r.get("display_name");
    let role_str: String = r.get("role");
    let created_at_str: String = r.get("created_at");

    UserRecord {
        user_id: Uuid::parse_str(&user_id_str).unwrap_or(Uuid::nil()),
        org_id: parse_org_id(&org_id_str),
        email,
        display_name,
        role: UserRole::parse_str(&role_str).unwrap_or(UserRole::Member),
        created_at: parse_dt(&created_at_str),
    }
}

fn row_to_sync_state(r: &sqlx::sqlite::SqliteRow) -> SyncState {
    let org_id_str: String = r.get("org_id");
    let project_id_str: Option<String> = r.get("project_id");
    let connector_id: String = r.get("connector_id");
    let scope: String = r.get("scope");
    let cursor_str: String = r.get("cursor");
    let updated_at_str: String = r.get("updated_at");

    SyncState {
        key: SyncStateKey {
            org_id: parse_org_id(&org_id_str),
            project_id: project_id_str.map(|s| parse_project_id(&s)),
            connector_id,
            scope,
        },
        cursor: serde_json::from_str(&cursor_str).unwrap_or_default(),
        updated_at: parse_dt(&updated_at_str),
    }
}

fn row_to_source_config(r: &sqlx::sqlite::SqliteRow) -> SourceConfig {
    let org_id_str: String = r.get("org_id");
    let source_id: String = r.get("source_id");
    let project_id_str: String = r.get("project_id");
    let connector_id: String = r.get("connector_id");
    let scope: String = r.get("scope");
    let enabled: i32 = r.get("enabled");
    let project_db_json: String = r.get("project_db_json");
    let schedule_json: Option<String> = r.get("schedule_json");
    let event_triggers_json: String = r.get("event_triggers_json");
    let processor_json: String = r.get("processor_json");
    let settings_str: String = r.get("settings");
    let created_at_str: String = r.get("created_at");
    let updated_at_str: String = r.get("updated_at");

    SourceConfig {
        org_id: parse_org_id(&org_id_str),
        project_id: parse_project_id(&project_id_str),
        project_db: serde_json::from_str(&project_db_json).unwrap_or_else(|_| ProjectDbHandle {
            org_id: parse_org_id(&org_id_str),
            project_id: parse_project_id(&project_id_str),
            connection_url: String::new(),
            auth_token: None,
        }),
        source_id,
        connector_id,
        scope,
        enabled: enabled != 0,
        schedule: schedule_json.and_then(|s| serde_json::from_str::<CronSchedule>(&s).ok()),
        event_triggers: serde_json::from_str::<Vec<EventTriggerConfig>>(&event_triggers_json)
            .unwrap_or_default(),
        processor: serde_json::from_str(&processor_json)
            .unwrap_or_else(|_| crate::context_refresh::models::SourceProcessorSpec::Connector),
        settings: serde_json::from_str(&settings_str).unwrap_or_default(),
        created_at: parse_dt(&created_at_str),
        updated_at: parse_dt(&updated_at_str),
    }
}

fn row_to_refresh_run(r: &sqlx::sqlite::SqliteRow) -> RefreshRun {
    let run_id_str: String = r.get("run_id");
    let org_id_str: String = r.get("org_id");
    let project_id_str: String = r.get("project_id");
    let source_id: String = r.get("source_id");
    let connector_id: String = r.get("connector_id");
    let trigger_json: String = r.get("trigger_json");
    let status_str: String = r.get("status");
    let started_at_str: String = r.get("started_at");
    let finished_at_str: Option<String> = r.get("finished_at");
    let records_pulled: i64 = r.get("records_pulled");
    let entities_stored: i64 = r.get("entities_stored");
    let error_message: Option<String> = r.get("error_message");
    let cursor_json: Option<String> = r.get("cursor_json");

    let status = match status_str.as_str() {
        "running" => RefreshRunStatus::Running,
        "succeeded" => RefreshRunStatus::Succeeded,
        "failed" => RefreshRunStatus::Failed,
        _ => RefreshRunStatus::Running,
    };

    RefreshRun {
        run_id: Uuid::parse_str(&run_id_str).unwrap_or(Uuid::nil()),
        org_id: parse_org_id(&org_id_str),
        project_id: parse_project_id(&project_id_str),
        source_id,
        connector_id,
        trigger: serde_json::from_str(&trigger_json).unwrap_or(RefreshTrigger::OnDemand),
        status,
        started_at: parse_dt(&started_at_str),
        finished_at: finished_at_str.map(|s| parse_dt(&s)),
        records_pulled: records_pulled as u64,
        entities_stored: entities_stored as u64,
        error_message,
        cursor: cursor_json.and_then(|s| serde_json::from_str(&s).ok()),
    }
}
