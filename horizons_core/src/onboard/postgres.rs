use crate::context_refresh::models::{
    CronSchedule, RefreshRun, RefreshRunQuery, RefreshRunStatus, SourceConfig,
};
use crate::models::{AgentIdentity, AuditEntry, OrgId, PlatformConfig, ProjectDbHandle, ProjectId};
use crate::onboard::config::PostgresConfig;
use crate::onboard::models::{
    ApiKeyRecord, AuditQuery, ConnectorCredential, CoreAgentEventCursor, CoreAgentRecord,
    ListQuery, OperationRecord, OperationRunRecord, OrgRecord, ProjectRecord, ResourceRecord,
    SyncState, SyncStateKey, UserRecord, UserRole,
};
use crate::onboard::traits::CentralDb;
use crate::{Error, Result};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::postgres::{PgPoolOptions, PgRow};
use sqlx::{PgPool, Row};
use uuid::Uuid;

const MIGRATION_0001: &str = include_str!("../../migrations/0001_init.sql");

#[derive(Clone)]
pub struct PostgresCentralDb {
    pool: PgPool,
}

impl PostgresCentralDb {
    #[tracing::instrument(level = "debug", skip(cfg))]
    pub async fn connect(cfg: &PostgresConfig) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(cfg.max_connections)
            .acquire_timeout(cfg.acquire_timeout)
            .connect(&cfg.url)
            .await
            .map_err(|e| Error::backend("connect postgres", e))?;
        Ok(Self { pool })
    }

    #[tracing::instrument(level = "debug", skip(pool))]
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    #[tracing::instrument(level = "debug", skip(self))]
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Apply core schema migrations.
    #[tracing::instrument(level = "info", skip(self))]
    pub async fn migrate(&self) -> Result<()> {
        // Single-file migration to keep v0.0.0 self-hosted setup simple.
        //
        // Postgres does not allow multiple SQL statements in a prepared statement, and
        // `sqlx::query(...)` prepares the query. Split on ';' like `project_db_migrate`.
        for stmt in MIGRATION_0001
            .split(';')
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            sqlx::query(stmt)
                .execute(&self.pool)
                .await
                .map_err(|e| Error::backend("apply migrations", e))?;
        }
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(row))]
    fn user_from_row(row: &PgRow) -> Result<UserRecord> {
        let role_str: String = row
            .try_get("role")
            .map_err(|e| Error::backend("user role", e))?;
        let role = UserRole::parse_str(&role_str)
            .ok_or_else(|| Error::BackendMessage(format!("invalid user role in db: {role_str}")))?;

        Ok(UserRecord {
            user_id: row
                .try_get("user_id")
                .map_err(|e| Error::backend("user_id", e))?,
            org_id: OrgId(
                row.try_get::<Uuid, _>("org_id")
                    .map_err(|e| Error::backend("org_id", e))?,
            ),
            email: row
                .try_get("email")
                .map_err(|e| Error::backend("email", e))?,
            display_name: row
                .try_get("display_name")
                .map_err(|e| Error::backend("display_name", e))?,
            role,
            created_at: row
                .try_get("created_at")
                .map_err(|e| Error::backend("created_at", e))?,
        })
    }

    #[tracing::instrument(level = "debug", skip(row))]
    fn org_from_row(row: &PgRow) -> Result<OrgRecord> {
        Ok(OrgRecord {
            org_id: OrgId(
                row.try_get::<Uuid, _>("org_id")
                    .map_err(|e| Error::backend("org_id", e))?,
            ),
            name: row.try_get("name").map_err(|e| Error::backend("name", e))?,
            created_at: row
                .try_get("created_at")
                .map_err(|e| Error::backend("created_at", e))?,
        })
    }

    #[tracing::instrument(level = "debug", skip(row))]
    fn project_from_row(row: &PgRow) -> Result<ProjectRecord> {
        Ok(ProjectRecord {
            org_id: OrgId(
                row.try_get::<Uuid, _>("org_id")
                    .map_err(|e| Error::backend("org_id", e))?,
            ),
            project_id: ProjectId(
                row.try_get::<Uuid, _>("project_id")
                    .map_err(|e| Error::backend("project_id", e))?,
            ),
            slug: row.try_get("slug").map_err(|e| Error::backend("slug", e))?,
            created_at: row
                .try_get("created_at")
                .map_err(|e| Error::backend("created_at", e))?,
        })
    }

    #[tracing::instrument(level = "debug", skip(row))]
    fn credential_from_row(row: &PgRow) -> Result<ConnectorCredential> {
        Ok(ConnectorCredential {
            org_id: OrgId(
                row.try_get::<Uuid, _>("org_id")
                    .map_err(|e| Error::backend("org_id", e))?,
            ),
            connector_id: row
                .try_get("connector_id")
                .map_err(|e| Error::backend("connector_id", e))?,
            ciphertext: row
                .try_get("ciphertext")
                .map_err(|e| Error::backend("ciphertext", e))?,
            created_at: row
                .try_get("created_at")
                .map_err(|e| Error::backend("created_at", e))?,
            updated_at: row
                .try_get("updated_at")
                .map_err(|e| Error::backend("updated_at", e))?,
        })
    }

    #[tracing::instrument(level = "debug", skip(row))]
    fn api_key_from_row(row: &PgRow) -> Result<ApiKeyRecord> {
        let actor: sqlx::types::Json<AgentIdentity> = row
            .try_get("actor")
            .map_err(|e| Error::backend("api_key actor", e))?;
        let scopes: sqlx::types::Json<Vec<String>> = row
            .try_get("scopes")
            .map_err(|e| Error::backend("api_key scopes", e))?;

        Ok(ApiKeyRecord {
            key_id: row
                .try_get("key_id")
                .map_err(|e| Error::backend("key_id", e))?,
            org_id: OrgId(
                row.try_get::<Uuid, _>("org_id")
                    .map_err(|e| Error::backend("org_id", e))?,
            ),
            name: row.try_get("name").map_err(|e| Error::backend("name", e))?,
            actor: actor.0,
            scopes: scopes.0,
            secret_hash: row
                .try_get("secret_hash")
                .map_err(|e| Error::backend("secret_hash", e))?,
            created_at: row
                .try_get("created_at")
                .map_err(|e| Error::backend("created_at", e))?,
            expires_at: row
                .try_get("expires_at")
                .map_err(|e| Error::backend("expires_at", e))?,
            last_used_at: row
                .try_get("last_used_at")
                .map_err(|e| Error::backend("last_used_at", e))?,
        })
    }

    #[tracing::instrument(level = "debug", skip(row))]
    fn resource_from_row(row: &PgRow) -> Result<ResourceRecord> {
        Ok(ResourceRecord {
            org_id: OrgId(
                row.try_get::<Uuid, _>("org_id")
                    .map_err(|e| Error::backend("org_id", e))?,
            ),
            resource_id: row
                .try_get("resource_id")
                .map_err(|e| Error::backend("resource_id", e))?,
            resource_type: row
                .try_get("resource_type")
                .map_err(|e| Error::backend("resource_type", e))?,
            config: row
                .try_get("config")
                .map_err(|e| Error::backend("config", e))?,
            created_at: row
                .try_get("created_at")
                .map_err(|e| Error::backend("created_at", e))?,
            updated_at: row
                .try_get("updated_at")
                .map_err(|e| Error::backend("updated_at", e))?,
        })
    }

    #[tracing::instrument(level = "debug", skip(row))]
    fn operation_from_row(row: &PgRow) -> Result<OperationRecord> {
        Ok(OperationRecord {
            org_id: OrgId(
                row.try_get::<Uuid, _>("org_id")
                    .map_err(|e| Error::backend("org_id", e))?,
            ),
            operation_id: row
                .try_get("operation_id")
                .map_err(|e| Error::backend("operation_id", e))?,
            resource_id: row
                .try_get("resource_id")
                .map_err(|e| Error::backend("resource_id", e))?,
            operation_type: row
                .try_get("operation_type")
                .map_err(|e| Error::backend("operation_type", e))?,
            config: row
                .try_get("config")
                .map_err(|e| Error::backend("config", e))?,
            created_at: row
                .try_get("created_at")
                .map_err(|e| Error::backend("created_at", e))?,
            updated_at: row
                .try_get("updated_at")
                .map_err(|e| Error::backend("updated_at", e))?,
        })
    }

    #[tracing::instrument(level = "debug", skip(row))]
    fn operation_run_from_row(row: &PgRow) -> Result<OperationRunRecord> {
        Ok(OperationRunRecord {
            id: row.try_get("id").map_err(|e| Error::backend("id", e))?,
            org_id: OrgId(
                row.try_get::<Uuid, _>("org_id")
                    .map_err(|e| Error::backend("org_id", e))?,
            ),
            operation_id: row
                .try_get("operation_id")
                .map_err(|e| Error::backend("operation_id", e))?,
            source_event_id: row
                .try_get("source_event_id")
                .map_err(|e| Error::backend("source_event_id", e))?,
            status: row
                .try_get("status")
                .map_err(|e| Error::backend("status", e))?,
            error: row
                .try_get("error")
                .map_err(|e| Error::backend("error", e))?,
            output: row
                .try_get("output")
                .map_err(|e| Error::backend("output", e))?,
            created_at: row
                .try_get("created_at")
                .map_err(|e| Error::backend("created_at", e))?,
        })
    }

    #[tracing::instrument(level = "debug", skip(row))]
    fn config_from_row(row: &PgRow) -> Result<PlatformConfig> {
        Ok(PlatformConfig {
            org_id: OrgId(
                row.try_get::<Uuid, _>("org_id")
                    .map_err(|e| Error::backend("org_id", e))?,
            ),
            data: row.try_get("data").map_err(|e| Error::backend("data", e))?,
            updated_at: row
                .try_get("updated_at")
                .map_err(|e| Error::backend("updated_at", e))?,
        })
    }

    #[tracing::instrument(level = "debug", skip(row))]
    fn audit_from_row(row: &PgRow) -> Result<AuditEntry> {
        let actor_json: serde_json::Value = row
            .try_get("actor")
            .map_err(|e| Error::backend("actor", e))?;
        let actor: AgentIdentity = serde_json::from_value(actor_json)
            .map_err(|e| Error::backend("deserialize actor", e))?;

        Ok(AuditEntry {
            id: row.try_get("id").map_err(|e| Error::backend("id", e))?,
            org_id: OrgId(
                row.try_get::<Uuid, _>("org_id")
                    .map_err(|e| Error::backend("org_id", e))?,
            ),
            project_id: row
                .try_get::<Option<Uuid>, _>("project_id")
                .map_err(|e| Error::backend("project_id", e))?
                .map(ProjectId),
            actor,
            action: row
                .try_get("action")
                .map_err(|e| Error::backend("action", e))?,
            payload: row
                .try_get("payload")
                .map_err(|e| Error::backend("payload", e))?,
            outcome: row
                .try_get("outcome")
                .map_err(|e| Error::backend("outcome", e))?,
            created_at: row
                .try_get("created_at")
                .map_err(|e| Error::backend("created_at", e))?,
        })
    }

    #[tracing::instrument(level = "debug", skip(row))]
    fn source_from_row(row: &PgRow) -> Result<SourceConfig> {
        let org_id = OrgId(
            row.try_get::<Uuid, _>("org_id")
                .map_err(|e| Error::backend("org_id", e))?,
        );
        let project_id = ProjectId(
            row.try_get::<Uuid, _>("project_id")
                .map_err(|e| Error::backend("project_id", e))?,
        );
        let project_db = ProjectDbHandle {
            org_id,
            project_id,
            connection_url: row
                .try_get("project_db_url")
                .map_err(|e| Error::backend("project_db_url", e))?,
            auth_token: row
                .try_get("project_db_token")
                .map_err(|e| Error::backend("project_db_token", e))?,
        };

        let schedule_expr: Option<String> = row
            .try_get("schedule_expr")
            .map_err(|e| Error::backend("schedule_expr", e))?;
        let schedule_next_run_at: Option<DateTime<Utc>> = row
            .try_get("schedule_next_run_at")
            .map_err(|e| Error::backend("schedule_next_run_at", e))?;

        let schedule = match (schedule_expr, schedule_next_run_at) {
            (Some(expr), Some(next)) => Some(CronSchedule {
                expr,
                next_run_at: next,
            }),
            (None, None) => None,
            _ => {
                return Err(Error::BackendMessage(
                    "source_configs schedule columns must both be null or both be set".to_string(),
                ));
            }
        };

        let event_triggers: sqlx::types::Json<
            Vec<crate::context_refresh::models::EventTriggerConfig>,
        > = row
            .try_get("event_triggers")
            .map_err(|e| Error::backend("event_triggers", e))?;

        let processor: crate::context_refresh::models::SourceProcessorSpec = row
            .try_get("processor")
            .map_err(|e| Error::backend("processor", e))
            .and_then(|v: serde_json::Value| {
                serde_json::from_value(v).map_err(|e| Error::backend("deserialize processor", e))
            })?;

        Ok(SourceConfig {
            org_id,
            project_id,
            project_db,
            source_id: row
                .try_get("source_id")
                .map_err(|e| Error::backend("source_id", e))?,
            connector_id: row
                .try_get("connector_id")
                .map_err(|e| Error::backend("connector_id", e))?,
            scope: row
                .try_get("scope")
                .map_err(|e| Error::backend("scope", e))?,
            enabled: row
                .try_get("enabled")
                .map_err(|e| Error::backend("enabled", e))?,
            schedule,
            event_triggers: event_triggers.0,
            processor,
            settings: row
                .try_get("settings")
                .map_err(|e| Error::backend("settings", e))?,
            created_at: row
                .try_get("created_at")
                .map_err(|e| Error::backend("created_at", e))?,
            updated_at: row
                .try_get("updated_at")
                .map_err(|e| Error::backend("updated_at", e))?,
        })
    }

    #[tracing::instrument(level = "debug", skip(row))]
    fn refresh_run_from_row(row: &PgRow) -> Result<RefreshRun> {
        let trigger: crate::context_refresh::models::RefreshTrigger = row
            .try_get("trigger")
            .map_err(|e| Error::backend("trigger", e))
            .and_then(|v: serde_json::Value| {
                serde_json::from_value(v).map_err(|e| Error::backend("deserialize trigger", e))
            })?;

        let status_str: String = row
            .try_get("status")
            .map_err(|e| Error::backend("status", e))?;
        let status = match status_str.as_str() {
            "running" => RefreshRunStatus::Running,
            "succeeded" => RefreshRunStatus::Succeeded,
            "failed" => RefreshRunStatus::Failed,
            other => {
                return Err(Error::BackendMessage(format!(
                    "invalid refresh run status in db: {other}"
                )));
            }
        };

        Ok(RefreshRun {
            run_id: row
                .try_get("run_id")
                .map_err(|e| Error::backend("run_id", e))?,
            org_id: OrgId(
                row.try_get::<Uuid, _>("org_id")
                    .map_err(|e| Error::backend("org_id", e))?,
            ),
            project_id: ProjectId(
                row.try_get::<Uuid, _>("project_id")
                    .map_err(|e| Error::backend("project_id", e))?,
            ),
            source_id: row
                .try_get("source_id")
                .map_err(|e| Error::backend("source_id", e))?,
            connector_id: row
                .try_get("connector_id")
                .map_err(|e| Error::backend("connector_id", e))?,
            trigger,
            status,
            started_at: row
                .try_get("started_at")
                .map_err(|e| Error::backend("started_at", e))?,
            finished_at: row
                .try_get("finished_at")
                .map_err(|e| Error::backend("finished_at", e))?,
            records_pulled: row
                .try_get::<i64, _>("records_pulled")
                .map_err(|e| Error::backend("records_pulled", e))?
                .max(0) as u64,
            entities_stored: row
                .try_get::<i64, _>("entities_stored")
                .map_err(|e| Error::backend("entities_stored", e))?
                .max(0) as u64,
            error_message: row
                .try_get("error_message")
                .map_err(|e| Error::backend("error_message", e))?,
            cursor: row
                .try_get("cursor")
                .map_err(|e| Error::backend("cursor", e))?,
        })
    }
}

#[async_trait]
impl CentralDb for PostgresCentralDb {
    #[tracing::instrument(level = "debug", skip(self, org))]
    async fn upsert_org(&self, org: &OrgRecord) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO orgs (org_id, name, created_at)
            VALUES ($1, $2, $3)
            ON CONFLICT (org_id) DO UPDATE SET name = EXCLUDED.name
            "#,
        )
        .bind(org.org_id.0)
        .bind(&org.name)
        .bind(org.created_at)
        .execute(&self.pool)
        .await
        .map_err(|e| Error::backend("upsert org", e))?;
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn get_org(&self, org_id: OrgId) -> Result<Option<OrgRecord>> {
        let row = sqlx::query("SELECT org_id, name, created_at FROM orgs WHERE org_id = $1")
            .bind(org_id.0)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| Error::backend("get org", e))?;
        Ok(row.as_ref().map(Self::org_from_row).transpose()?)
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn list_orgs(&self, query: ListQuery) -> Result<Vec<OrgRecord>> {
        let rows = sqlx::query(
            r#"
            SELECT org_id, name, created_at
            FROM orgs
            ORDER BY created_at DESC
            LIMIT $1 OFFSET $2
            "#,
        )
        .bind(query.limit as i64)
        .bind(query.offset as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| Error::backend("list orgs", e))?;
        rows.iter().map(Self::org_from_row).collect()
    }

    #[tracing::instrument(level = "debug", skip(self, user))]
    async fn upsert_user(&self, user: &UserRecord) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO users (org_id, user_id, email, display_name, role, created_at)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (org_id, user_id) DO UPDATE
              SET email = EXCLUDED.email,
                  display_name = EXCLUDED.display_name,
                  role = EXCLUDED.role
            "#,
        )
        .bind(user.org_id.0)
        .bind(user.user_id)
        .bind(&user.email)
        .bind(&user.display_name)
        .bind(user.role.as_str())
        .bind(user.created_at)
        .execute(&self.pool)
        .await
        .map_err(|e| Error::backend("upsert user", e))?;
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn get_user(&self, org_id: OrgId, user_id: Uuid) -> Result<Option<UserRecord>> {
        let row = sqlx::query(
            "SELECT org_id, user_id, email, display_name, role, created_at FROM users WHERE org_id = $1 AND user_id = $2",
        )
        .bind(org_id.0)
        .bind(user_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| Error::backend("get user", e))?;
        Ok(row.as_ref().map(Self::user_from_row).transpose()?)
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn list_users(&self, org_id: OrgId, query: ListQuery) -> Result<Vec<UserRecord>> {
        let rows = sqlx::query(
            r#"
            SELECT org_id, user_id, email, display_name, role, created_at
            FROM users
            WHERE org_id = $1
            ORDER BY created_at DESC
            LIMIT $2 OFFSET $3
            "#,
        )
        .bind(org_id.0)
        .bind(query.limit as i64)
        .bind(query.offset as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| Error::backend("list users", e))?;

        let mut out = Vec::with_capacity(rows.len());
        for r in rows {
            out.push(Self::user_from_row(&r)?);
        }
        Ok(out)
    }

    async fn upsert_project(&self, project: &ProjectRecord) -> Result<()> {
        if project.slug.trim().is_empty() {
            return Err(Error::InvalidInput("project slug is empty".to_string()));
        }
        sqlx::query(
            r#"
            INSERT INTO projects (org_id, project_id, slug, created_at)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (org_id, project_id) DO UPDATE
              SET slug = EXCLUDED.slug
            "#,
        )
        .bind(project.org_id.0)
        .bind(project.project_id.0)
        .bind(project.slug.trim())
        .bind(project.created_at)
        .execute(&self.pool)
        .await
        .map_err(|e| Error::backend("upsert project", e))?;
        Ok(())
    }

    async fn get_project_by_id(
        &self,
        org_id: OrgId,
        project_id: ProjectId,
    ) -> Result<Option<ProjectRecord>> {
        let row = sqlx::query(
            "SELECT org_id, project_id, slug, created_at FROM projects WHERE org_id = $1 AND project_id = $2",
        )
        .bind(org_id.0)
        .bind(project_id.0)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| Error::backend("get project by id", e))?;
        Ok(row.as_ref().map(Self::project_from_row).transpose()?)
    }

    async fn get_project_by_slug(&self, org_id: OrgId, slug: &str) -> Result<Option<ProjectRecord>> {
        let row = sqlx::query(
            "SELECT org_id, project_id, slug, created_at FROM projects WHERE org_id = $1 AND slug = $2",
        )
        .bind(org_id.0)
        .bind(slug.trim())
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| Error::backend("get project by slug", e))?;
        Ok(row.as_ref().map(Self::project_from_row).transpose()?)
    }

    async fn list_projects(&self, org_id: OrgId, query: ListQuery) -> Result<Vec<ProjectRecord>> {
        let rows = sqlx::query(
            r#"
            SELECT org_id, project_id, slug, created_at
            FROM projects
            WHERE org_id = $1
            ORDER BY created_at DESC
            LIMIT $2 OFFSET $3
            "#,
        )
        .bind(org_id.0)
        .bind(query.limit as i64)
        .bind(query.offset as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| Error::backend("list projects", e))?;

        let mut out = Vec::with_capacity(rows.len());
        for r in rows {
            out.push(Self::project_from_row(&r)?);
        }
        Ok(out)
    }

    #[tracing::instrument(level = "debug", skip(self, key))]
    async fn upsert_api_key(&self, key: &ApiKeyRecord) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO api_keys
              (key_id, org_id, name, actor, scopes, secret_hash, created_at, expires_at, last_used_at)
            VALUES
              ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            ON CONFLICT (key_id) DO UPDATE SET
              org_id = EXCLUDED.org_id,
              name = EXCLUDED.name,
              actor = EXCLUDED.actor,
              scopes = EXCLUDED.scopes,
              secret_hash = EXCLUDED.secret_hash,
              expires_at = EXCLUDED.expires_at,
              last_used_at = EXCLUDED.last_used_at
            "#,
        )
        .bind(key.key_id)
        .bind(key.org_id.0)
        .bind(&key.name)
        .bind(sqlx::types::Json(&key.actor))
        .bind(sqlx::types::Json(&key.scopes))
        .bind(&key.secret_hash)
        .bind(key.created_at)
        .bind(key.expires_at)
        .bind(key.last_used_at)
        .execute(&self.pool)
        .await
        .map_err(|e| Error::backend("upsert api key", e))?;
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn get_api_key_by_id(&self, key_id: Uuid) -> Result<Option<ApiKeyRecord>> {
        let row = sqlx::query(
            r#"
            SELECT key_id, org_id, name, actor, scopes, secret_hash, created_at, expires_at, last_used_at
            FROM api_keys
            WHERE key_id = $1
            "#,
        )
        .bind(key_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| Error::backend("get api key", e))?;

        Ok(row.as_ref().map(Self::api_key_from_row).transpose()?)
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn list_api_keys(&self, org_id: OrgId, query: ListQuery) -> Result<Vec<ApiKeyRecord>> {
        let rows = sqlx::query(
            r#"
            SELECT key_id, org_id, name, actor, scopes, secret_hash, created_at, expires_at, last_used_at
            FROM api_keys
            WHERE org_id = $1
            ORDER BY created_at DESC
            LIMIT $2 OFFSET $3
            "#,
        )
        .bind(org_id.0)
        .bind(query.limit as i64)
        .bind(query.offset as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| Error::backend("list api keys", e))?;

        let mut out = Vec::with_capacity(rows.len());
        for r in rows {
            out.push(Self::api_key_from_row(&r)?);
        }
        Ok(out)
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn delete_api_key(&self, org_id: OrgId, key_id: Uuid) -> Result<()> {
        sqlx::query("DELETE FROM api_keys WHERE org_id = $1 AND key_id = $2")
            .bind(org_id.0)
            .bind(key_id)
            .execute(&self.pool)
            .await
            .map_err(|e| Error::backend("delete api key", e))?;
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn touch_api_key_last_used(
        &self,
        key_id: Uuid,
        at: chrono::DateTime<chrono::Utc>,
    ) -> Result<()> {
        sqlx::query("UPDATE api_keys SET last_used_at = $2 WHERE key_id = $1")
            .bind(key_id)
            .bind(at)
            .execute(&self.pool)
            .await
            .map_err(|e| Error::backend("touch api key last_used_at", e))?;
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn get_platform_config(&self, org_id: OrgId) -> Result<Option<PlatformConfig>> {
        let row =
            sqlx::query("SELECT org_id, data, updated_at FROM platform_config WHERE org_id = $1")
                .bind(org_id.0)
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| Error::backend("get platform config", e))?;
        Ok(row.as_ref().map(Self::config_from_row).transpose()?)
    }

    #[tracing::instrument(level = "debug", skip(self, config))]
    async fn set_platform_config(&self, config: &PlatformConfig) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO platform_config (org_id, data, updated_at)
            VALUES ($1, $2, $3)
            ON CONFLICT (org_id) DO UPDATE
              SET data = EXCLUDED.data,
                  updated_at = EXCLUDED.updated_at
            "#,
        )
        .bind(config.org_id.0)
        .bind(&config.data)
        .bind(config.updated_at)
        .execute(&self.pool)
        .await
        .map_err(|e| Error::backend("set platform config", e))?;
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self, entry))]
    async fn append_audit_entry(&self, entry: &AuditEntry) -> Result<()> {
        let actor =
            serde_json::to_value(&entry.actor).map_err(|e| Error::backend("serialize actor", e))?;
        sqlx::query(
            r#"
            INSERT INTO audit_log (id, org_id, project_id, actor, action, payload, outcome, created_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            "#,
        )
        .bind(entry.id)
        .bind(entry.org_id.0)
        .bind(entry.project_id.map(|p| p.0))
        .bind(actor)
        .bind(&entry.action)
        .bind(&entry.payload)
        .bind(&entry.outcome)
        .bind(entry.created_at)
        .execute(&self.pool)
        .await
        .map_err(|e| Error::backend("append audit", e))?;
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn list_audit_entries(
        &self,
        org_id: OrgId,
        query: AuditQuery,
    ) -> Result<Vec<AuditEntry>> {
        use sqlx::{Postgres, QueryBuilder};

        let mut qb: QueryBuilder<Postgres> = QueryBuilder::new(
            "SELECT id, org_id, project_id, actor, action, payload, outcome, created_at FROM audit_log WHERE org_id = ",
        );
        qb.push_bind(org_id.0);

        if let Some(project_id) = query.project_id {
            qb.push(" AND project_id = ").push_bind(project_id.0);
        }
        if let Some(prefix) = query.action_prefix {
            qb.push(" AND action LIKE ").push_bind(format!("{prefix}%"));
        }
        if let Some(since) = query.since {
            qb.push(" AND created_at >= ").push_bind(since);
        }
        if let Some(until) = query.until {
            qb.push(" AND created_at <= ").push_bind(until);
        }
        if let Some(contains) = query.actor_contains {
            qb.push(" AND actor::text ILIKE ")
                .push_bind(format!("%{contains}%"));
        }

        qb.push(" ORDER BY created_at DESC LIMIT ")
            .push_bind(query.limit as i64);

        let rows = qb
            .build()
            .fetch_all(&self.pool)
            .await
            .map_err(|e| Error::backend("list audit", e))?;
        let mut out = Vec::with_capacity(rows.len());
        for r in rows {
            out.push(Self::audit_from_row(&r)?);
        }
        Ok(out)
    }

    #[tracing::instrument(level = "debug", skip(self, credential))]
    async fn upsert_connector_credential(&self, credential: &ConnectorCredential) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO connector_credentials (org_id, connector_id, ciphertext, created_at, updated_at)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (org_id, connector_id) DO UPDATE
              SET ciphertext = EXCLUDED.ciphertext,
                  updated_at = EXCLUDED.updated_at
            "#,
        )
        .bind(credential.org_id.0)
        .bind(&credential.connector_id)
        .bind(&credential.ciphertext)
        .bind(credential.created_at)
        .bind(credential.updated_at)
        .execute(&self.pool)
        .await
        .map_err(|e| Error::backend("upsert connector credential", e))?;
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn get_connector_credential(
        &self,
        org_id: OrgId,
        connector_id: &str,
    ) -> Result<Option<ConnectorCredential>> {
        let row = sqlx::query(
            r#"
            SELECT org_id, connector_id, ciphertext, created_at, updated_at
            FROM connector_credentials
            WHERE org_id = $1 AND connector_id = $2
            "#,
        )
        .bind(org_id.0)
        .bind(connector_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| Error::backend("get connector credential", e))?;
        Ok(row.as_ref().map(Self::credential_from_row).transpose()?)
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn delete_connector_credential(&self, org_id: OrgId, connector_id: &str) -> Result<()> {
        sqlx::query("DELETE FROM connector_credentials WHERE org_id = $1 AND connector_id = $2")
            .bind(org_id.0)
            .bind(connector_id)
            .execute(&self.pool)
            .await
            .map_err(|e| Error::backend("delete connector credential", e))?;
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn list_connector_credentials(&self, org_id: OrgId) -> Result<Vec<ConnectorCredential>> {
        let rows = sqlx::query(
            "SELECT org_id, connector_id, ciphertext, created_at, updated_at
             FROM connector_credentials WHERE org_id = $1 ORDER BY connector_id",
        )
        .bind(org_id.0)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| Error::backend("list connector credentials", e))?;
        rows.iter().map(Self::credential_from_row).collect()
    }

    #[tracing::instrument(level = "debug", skip(self, resource))]
    async fn upsert_resource(&self, resource: &ResourceRecord) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO resources (org_id, resource_id, resource_type, config, created_at, updated_at)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (org_id, resource_id) DO UPDATE
              SET resource_type = EXCLUDED.resource_type,
                  config = EXCLUDED.config,
                  updated_at = EXCLUDED.updated_at
            "#,
        )
        .bind(resource.org_id.0)
        .bind(&resource.resource_id)
        .bind(&resource.resource_type)
        .bind(&resource.config)
        .bind(resource.created_at)
        .bind(resource.updated_at)
        .execute(&self.pool)
        .await
        .map_err(|e| Error::backend("upsert resource", e))?;
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn get_resource(
        &self,
        org_id: OrgId,
        resource_id: &str,
    ) -> Result<Option<ResourceRecord>> {
        let row = sqlx::query(
            r#"
            SELECT org_id, resource_id, resource_type, config, created_at, updated_at
            FROM resources
            WHERE org_id = $1 AND resource_id = $2
            "#,
        )
        .bind(org_id.0)
        .bind(resource_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| Error::backend("get resource", e))?;
        Ok(row.as_ref().map(Self::resource_from_row).transpose()?)
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn list_resources(&self, org_id: OrgId, query: ListQuery) -> Result<Vec<ResourceRecord>> {
        let rows = sqlx::query(
            r#"
            SELECT org_id, resource_id, resource_type, config, created_at, updated_at
            FROM resources
            WHERE org_id = $1
            ORDER BY updated_at DESC
            LIMIT $2 OFFSET $3
            "#,
        )
        .bind(org_id.0)
        .bind(query.limit as i64)
        .bind(query.offset as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| Error::backend("list resources", e))?;
        let mut out = Vec::with_capacity(rows.len());
        for r in rows {
            out.push(Self::resource_from_row(&r)?);
        }
        Ok(out)
    }

    #[tracing::instrument(level = "debug", skip(self, operation))]
    async fn upsert_operation(&self, operation: &OperationRecord) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO operations (org_id, operation_id, resource_id, operation_type, config, created_at, updated_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT (org_id, operation_id) DO UPDATE
              SET resource_id = EXCLUDED.resource_id,
                  operation_type = EXCLUDED.operation_type,
                  config = EXCLUDED.config,
                  updated_at = EXCLUDED.updated_at
            "#,
        )
        .bind(operation.org_id.0)
        .bind(&operation.operation_id)
        .bind(&operation.resource_id)
        .bind(&operation.operation_type)
        .bind(&operation.config)
        .bind(operation.created_at)
        .bind(operation.updated_at)
        .execute(&self.pool)
        .await
        .map_err(|e| Error::backend("upsert operation", e))?;
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn get_operation(
        &self,
        org_id: OrgId,
        operation_id: &str,
    ) -> Result<Option<OperationRecord>> {
        let row = sqlx::query(
            r#"
            SELECT org_id, operation_id, resource_id, operation_type, config, created_at, updated_at
            FROM operations
            WHERE org_id = $1 AND operation_id = $2
            "#,
        )
        .bind(org_id.0)
        .bind(operation_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| Error::backend("get operation", e))?;
        Ok(row.as_ref().map(Self::operation_from_row).transpose()?)
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn list_operations(
        &self,
        org_id: OrgId,
        query: ListQuery,
    ) -> Result<Vec<OperationRecord>> {
        let rows = sqlx::query(
            r#"
            SELECT org_id, operation_id, resource_id, operation_type, config, created_at, updated_at
            FROM operations
            WHERE org_id = $1
            ORDER BY updated_at DESC
            LIMIT $2 OFFSET $3
            "#,
        )
        .bind(org_id.0)
        .bind(query.limit as i64)
        .bind(query.offset as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| Error::backend("list operations", e))?;
        let mut out = Vec::with_capacity(rows.len());
        for r in rows {
            out.push(Self::operation_from_row(&r)?);
        }
        Ok(out)
    }

    #[tracing::instrument(level = "debug", skip(self, run))]
    async fn append_operation_run(&self, run: &OperationRunRecord) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO operation_runs (id, org_id, operation_id, source_event_id, status, error, output, created_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            "#,
        )
        .bind(run.id)
        .bind(run.org_id.0)
        .bind(&run.operation_id)
        .bind(&run.source_event_id)
        .bind(&run.status)
        .bind(&run.error)
        .bind(&run.output)
        .bind(run.created_at)
        .execute(&self.pool)
        .await
        .map_err(|e| Error::backend("append operation run", e))?;
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn list_operation_runs(
        &self,
        org_id: OrgId,
        operation_id: Option<&str>,
        query: ListQuery,
    ) -> Result<Vec<OperationRunRecord>> {
        let mut qb = sqlx::QueryBuilder::new(
            "SELECT id, org_id, operation_id, source_event_id, status, error, output, created_at \
             FROM operation_runs WHERE org_id = ",
        );
        qb.push_bind(org_id.0);
        if let Some(op) = operation_id {
            qb.push(" AND operation_id = ").push_bind(op);
        }
        qb.push(" ORDER BY created_at DESC LIMIT ")
            .push_bind(query.limit as i64)
            .push(" OFFSET ")
            .push_bind(query.offset as i64);
        let rows = qb
            .build()
            .fetch_all(&self.pool)
            .await
            .map_err(|e| Error::backend("list operation runs", e))?;
        let mut out = Vec::with_capacity(rows.len());
        for r in rows {
            out.push(Self::operation_run_from_row(&r)?);
        }
        Ok(out)
    }

    #[tracing::instrument(level = "debug", skip(self, agent))]
    async fn upsert_core_agent(&self, agent: &CoreAgentRecord) -> Result<()> {
        let schedule_type = agent.schedule.as_ref().map(|s| match s {
            crate::core_agents::models::AgentSchedule::Cron(_) => "cron",
            crate::core_agents::models::AgentSchedule::Interval { .. } => "interval",
            crate::core_agents::models::AgentSchedule::OnEvent { .. } => "on_event",
            crate::core_agents::models::AgentSchedule::OnDemand => "on_demand",
        });

        sqlx::query(
            r#"
            INSERT INTO core_agents
              (org_id, project_id, agent_id, name, sandbox_image, tools, schedule_type, schedule, enabled, next_run_at, config, created_at, updated_at)
            VALUES
              ($1, $2, $3, $4, $5, $6::jsonb, $7, $8::jsonb, $9, $10, $11::jsonb, $12, $13)
            ON CONFLICT (org_id, project_id, agent_id) DO UPDATE
              SET name = EXCLUDED.name,
                  sandbox_image = EXCLUDED.sandbox_image,
                  tools = EXCLUDED.tools,
                  schedule_type = EXCLUDED.schedule_type,
                  schedule = EXCLUDED.schedule,
                  enabled = EXCLUDED.enabled,
                  next_run_at = EXCLUDED.next_run_at,
                  config = EXCLUDED.config,
                  updated_at = EXCLUDED.updated_at
            "#,
        )
        .bind(agent.org_id.0)
        .bind(agent.project_id.0)
        .bind(&agent.agent_id)
        .bind(&agent.name)
        .bind(&agent.sandbox_image)
        .bind(sqlx::types::Json(&agent.tools))
        .bind(schedule_type)
        .bind(sqlx::types::Json(&agent.schedule))
        .bind(agent.enabled)
        .bind(agent.next_run_at)
        .bind(sqlx::types::Json(&agent.config))
        .bind(agent.created_at)
        .bind(agent.updated_at)
        .execute(&self.pool)
        .await
        .map_err(|e| Error::backend("upsert core agent", e))?;
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn get_core_agent(
        &self,
        org_id: OrgId,
        project_id: ProjectId,
        agent_id: &str,
    ) -> Result<Option<CoreAgentRecord>> {
        let row = sqlx::query(
            r#"
            SELECT org_id, project_id, agent_id, name, sandbox_image, tools, schedule, enabled, next_run_at, config, created_at, updated_at
              FROM core_agents
             WHERE org_id = $1 AND project_id = $2 AND agent_id = $3
            "#,
        )
        .bind(org_id.0)
        .bind(project_id.0)
        .bind(agent_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| Error::backend("get core agent", e))?;

        let Some(row) = row else { return Ok(None) };
        let tools: sqlx::types::Json<Vec<String>> = row
            .try_get("tools")
            .map_err(|e| Error::backend("tools", e))?;
        let schedule: sqlx::types::Json<Option<crate::core_agents::models::AgentSchedule>> = row
            .try_get("schedule")
            .map_err(|e| Error::backend("schedule", e))?;
        let config: sqlx::types::Json<serde_json::Value> = row
            .try_get("config")
            .map_err(|e| Error::backend("config", e))?;

        Ok(Some(CoreAgentRecord {
            org_id: OrgId(
                row.try_get::<Uuid, _>("org_id")
                    .map_err(|e| Error::backend("org_id", e))?,
            ),
            project_id: ProjectId(
                row.try_get::<Uuid, _>("project_id")
                    .map_err(|e| Error::backend("project_id", e))?,
            ),
            agent_id: row
                .try_get("agent_id")
                .map_err(|e| Error::backend("agent_id", e))?,
            name: row.try_get("name").map_err(|e| Error::backend("name", e))?,
            sandbox_image: row
                .try_get("sandbox_image")
                .map_err(|e| Error::backend("sandbox_image", e))?,
            tools: tools.0,
            schedule: schedule.0,
            enabled: row
                .try_get("enabled")
                .map_err(|e| Error::backend("enabled", e))?,
            next_run_at: row
                .try_get("next_run_at")
                .map_err(|e| Error::backend("next_run_at", e))?,
            config: config.0,
            created_at: row
                .try_get("created_at")
                .map_err(|e| Error::backend("created_at", e))?,
            updated_at: row
                .try_get("updated_at")
                .map_err(|e| Error::backend("updated_at", e))?,
        }))
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn list_core_agents(
        &self,
        org_id: OrgId,
        project_id: ProjectId,
        query: ListQuery,
    ) -> Result<Vec<CoreAgentRecord>> {
        let rows = sqlx::query(
            r#"
            SELECT org_id, project_id, agent_id, name, sandbox_image, tools, schedule, enabled, next_run_at, config, created_at, updated_at
              FROM core_agents
             WHERE org_id = $1 AND project_id = $2
             ORDER BY updated_at DESC, agent_id ASC
             LIMIT $3 OFFSET $4
            "#,
        )
        .bind(org_id.0)
        .bind(project_id.0)
        .bind(query.limit as i64)
        .bind(query.offset as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| Error::backend("list core agents", e))?;

        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            let tools: sqlx::types::Json<Vec<String>> = row
                .try_get("tools")
                .map_err(|e| Error::backend("tools", e))?;
            let schedule: sqlx::types::Json<Option<crate::core_agents::models::AgentSchedule>> =
                row.try_get("schedule")
                    .map_err(|e| Error::backend("schedule", e))?;
            let config: sqlx::types::Json<serde_json::Value> = row
                .try_get("config")
                .map_err(|e| Error::backend("config", e))?;

            out.push(CoreAgentRecord {
                org_id: OrgId(
                    row.try_get::<Uuid, _>("org_id")
                        .map_err(|e| Error::backend("org_id", e))?,
                ),
                project_id: ProjectId(
                    row.try_get::<Uuid, _>("project_id")
                        .map_err(|e| Error::backend("project_id", e))?,
                ),
                agent_id: row
                    .try_get("agent_id")
                    .map_err(|e| Error::backend("agent_id", e))?,
                name: row.try_get("name").map_err(|e| Error::backend("name", e))?,
                sandbox_image: row
                    .try_get("sandbox_image")
                    .map_err(|e| Error::backend("sandbox_image", e))?,
                tools: tools.0,
                schedule: schedule.0,
                enabled: row
                    .try_get("enabled")
                    .map_err(|e| Error::backend("enabled", e))?,
                next_run_at: row
                    .try_get("next_run_at")
                    .map_err(|e| Error::backend("next_run_at", e))?,
                config: config.0,
                created_at: row
                    .try_get("created_at")
                    .map_err(|e| Error::backend("created_at", e))?,
                updated_at: row
                    .try_get("updated_at")
                    .map_err(|e| Error::backend("updated_at", e))?,
            });
        }
        Ok(out)
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn delete_core_agent(
        &self,
        org_id: OrgId,
        project_id: ProjectId,
        agent_id: &str,
    ) -> Result<()> {
        sqlx::query(
            r#"
            DELETE FROM core_agents
             WHERE org_id = $1 AND project_id = $2 AND agent_id = $3
            "#,
        )
        .bind(org_id.0)
        .bind(project_id.0)
        .bind(agent_id)
        .execute(&self.pool)
        .await
        .map_err(|e| Error::backend("delete core agent", e))?;
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn set_core_agents_time_enabled(
        &self,
        org_id: OrgId,
        project_id: ProjectId,
        enabled: bool,
    ) -> Result<u64> {
        let res = sqlx::query(
            r#"
            UPDATE core_agents
               SET enabled = $3,
                   updated_at = NOW()
             WHERE org_id = $1 AND project_id = $2
               AND schedule_type IN ('cron','interval')
            "#,
        )
        .bind(org_id.0)
        .bind(project_id.0)
        .bind(enabled)
        .execute(&self.pool)
        .await
        .map_err(|e| Error::backend("set core agents time enabled", e))?;
        Ok(res.rows_affected())
    }

    #[tracing::instrument(level = "debug", skip(self, cursor))]
    async fn upsert_core_agent_event_cursor(&self, cursor: &CoreAgentEventCursor) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO core_agent_event_cursors
                (org_id, project_id, agent_id, topic, last_seen_received_at, last_seen_event_id, updated_at)
            VALUES
                ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT (org_id, project_id, agent_id, topic) DO UPDATE SET
                last_seen_received_at = EXCLUDED.last_seen_received_at,
                last_seen_event_id = EXCLUDED.last_seen_event_id,
                updated_at = EXCLUDED.updated_at
            "#,
        )
        .bind(cursor.org_id.0)
        .bind(cursor.project_id.0)
        .bind(&cursor.agent_id)
        .bind(&cursor.topic)
        .bind(cursor.last_seen_received_at)
        .bind(&cursor.last_seen_event_id)
        .bind(cursor.updated_at)
        .execute(&self.pool)
        .await
        .map_err(|e| Error::backend("upsert core agent event cursor", e))?;
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn get_core_agent_event_cursor(
        &self,
        org_id: OrgId,
        project_id: ProjectId,
        agent_id: &str,
        topic: &str,
    ) -> Result<Option<CoreAgentEventCursor>> {
        let row = sqlx::query(
            r#"
            SELECT org_id, project_id, agent_id, topic, last_seen_received_at, last_seen_event_id, updated_at
              FROM core_agent_event_cursors
             WHERE org_id = $1 AND project_id = $2 AND agent_id = $3 AND topic = $4
             LIMIT 1
            "#,
        )
        .bind(org_id.0)
        .bind(project_id.0)
        .bind(agent_id)
        .bind(topic)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| Error::backend("get core agent event cursor", e))?;

        Ok(row.map(|r| CoreAgentEventCursor {
            org_id: OrgId(r.try_get::<Uuid, _>("org_id").unwrap_or(Uuid::nil())),
            project_id: ProjectId(r.try_get::<Uuid, _>("project_id").unwrap_or(Uuid::nil())),
            agent_id: r.try_get::<String, _>("agent_id").unwrap_or_default(),
            topic: r.try_get::<String, _>("topic").unwrap_or_default(),
            last_seen_received_at: r
                .try_get::<DateTime<Utc>, _>("last_seen_received_at")
                .unwrap_or(DateTime::<Utc>::UNIX_EPOCH),
            last_seen_event_id: r
                .try_get::<String, _>("last_seen_event_id")
                .unwrap_or_default(),
            updated_at: r
                .try_get::<DateTime<Utc>, _>("updated_at")
                .unwrap_or(Utc::now()),
        }))
    }

    #[tracing::instrument(level = "debug", skip(self, state))]
    async fn upsert_sync_state(&self, state: &SyncState) -> Result<()> {
        match state.key.project_id {
            None => {
                sqlx::query(
                    r#"
                    INSERT INTO sync_state_org (org_id, connector_id, scope, cursor, updated_at)
                    VALUES ($1, $2, $3, $4, $5)
                    ON CONFLICT (org_id, connector_id, scope) DO UPDATE
                      SET cursor = EXCLUDED.cursor,
                          updated_at = EXCLUDED.updated_at
                    "#,
                )
                .bind(state.key.org_id.0)
                .bind(&state.key.connector_id)
                .bind(&state.key.scope)
                .bind(&state.cursor)
                .bind(state.updated_at)
                .execute(&self.pool)
                .await
                .map_err(|e| Error::backend("upsert sync state (org)", e))?;
            }
            Some(project_id) => {
                sqlx::query(
                    r#"
                    INSERT INTO sync_state_project (org_id, project_id, connector_id, scope, cursor, updated_at)
                    VALUES ($1, $2, $3, $4, $5, $6)
                    ON CONFLICT (org_id, project_id, connector_id, scope) DO UPDATE
                      SET cursor = EXCLUDED.cursor,
                          updated_at = EXCLUDED.updated_at
                    "#,
                )
                .bind(state.key.org_id.0)
                .bind(project_id.0)
                .bind(&state.key.connector_id)
                .bind(&state.key.scope)
                .bind(&state.cursor)
                .bind(state.updated_at)
                .execute(&self.pool)
                .await
                .map_err(|e| Error::backend("upsert sync state (project)", e))?;
            }
        }
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn get_sync_state(&self, key: &SyncStateKey) -> Result<Option<SyncState>> {
        match key.project_id {
            None => {
                let row = sqlx::query(
                    "SELECT cursor, updated_at FROM sync_state_org WHERE org_id = $1 AND connector_id = $2 AND scope = $3",
                )
                .bind(key.org_id.0)
                .bind(&key.connector_id)
                .bind(&key.scope)
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| Error::backend("get sync state (org)", e))?;
                match row {
                    None => Ok(None),
                    Some(r) => Ok(Some(SyncState {
                        key: key.clone(),
                        cursor: r
                            .try_get("cursor")
                            .map_err(|e| Error::backend("cursor", e))?,
                        updated_at: r
                            .try_get("updated_at")
                            .map_err(|e| Error::backend("updated_at", e))?,
                    })),
                }
            }
            Some(project_id) => {
                let row = sqlx::query(
                    "SELECT cursor, updated_at FROM sync_state_project WHERE org_id = $1 AND project_id = $2 AND connector_id = $3 AND scope = $4",
                )
                .bind(key.org_id.0)
                .bind(project_id.0)
                .bind(&key.connector_id)
                .bind(&key.scope)
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| Error::backend("get sync state (project)", e))?;
                match row {
                    None => Ok(None),
                    Some(r) => Ok(Some(SyncState {
                        key: key.clone(),
                        cursor: r
                            .try_get("cursor")
                            .map_err(|e| Error::backend("cursor", e))?,
                        updated_at: r
                            .try_get("updated_at")
                            .map_err(|e| Error::backend("updated_at", e))?,
                    })),
                }
            }
        }
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn delete_sync_state(&self, key: &SyncStateKey) -> Result<()> {
        match key.project_id {
            None => {
                sqlx::query("DELETE FROM sync_state_org WHERE org_id = $1 AND connector_id = $2 AND scope = $3")
                    .bind(key.org_id.0)
                    .bind(&key.connector_id)
                    .bind(&key.scope)
                    .execute(&self.pool)
                    .await
                    .map_err(|e| Error::backend("delete sync state (org)", e))?;
            }
            Some(project_id) => {
                sqlx::query("DELETE FROM sync_state_project WHERE org_id = $1 AND project_id = $2 AND connector_id = $3 AND scope = $4")
                    .bind(key.org_id.0)
                    .bind(project_id.0)
                    .bind(&key.connector_id)
                    .bind(&key.scope)
                    .execute(&self.pool)
                    .await
                    .map_err(|e| Error::backend("delete sync state (project)", e))?;
            }
        }
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self, source))]
    async fn upsert_source_config(&self, source: &SourceConfig) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO source_configs (
              org_id, source_id, project_id, connector_id, scope, enabled,
              project_db_url, project_db_token,
              schedule_expr, schedule_next_run_at,
              event_triggers, processor, settings, created_at, updated_at
            )
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
            ON CONFLICT (org_id, source_id) DO UPDATE
              SET project_id = EXCLUDED.project_id,
                  connector_id = EXCLUDED.connector_id,
                  scope = EXCLUDED.scope,
                  enabled = EXCLUDED.enabled,
                  project_db_url = EXCLUDED.project_db_url,
                  project_db_token = EXCLUDED.project_db_token,
                  schedule_expr = EXCLUDED.schedule_expr,
                  schedule_next_run_at = EXCLUDED.schedule_next_run_at,
                  event_triggers = EXCLUDED.event_triggers,
                  processor = EXCLUDED.processor,
                  settings = EXCLUDED.settings,
                  updated_at = EXCLUDED.updated_at
            "#,
        )
        .bind(source.org_id.0)
        .bind(&source.source_id)
        .bind(source.project_id.0)
        .bind(&source.connector_id)
        .bind(&source.scope)
        .bind(source.enabled)
        .bind(&source.project_db.connection_url)
        .bind(&source.project_db.auth_token)
        .bind(source.schedule.as_ref().map(|s| s.expr.as_str()))
        .bind(source.schedule.as_ref().map(|s| s.next_run_at))
        .bind(sqlx::types::Json(&source.event_triggers))
        .bind(sqlx::types::Json(&source.processor))
        .bind(&source.settings)
        .bind(source.created_at)
        .bind(source.updated_at)
        .execute(&self.pool)
        .await
        .map_err(|e| Error::backend("upsert source config", e))?;
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn get_source_config(
        &self,
        org_id: OrgId,
        source_id: &str,
    ) -> Result<Option<SourceConfig>> {
        let row = sqlx::query(
            r#"
            SELECT org_id, source_id, project_id, connector_id, scope, enabled,
                   project_db_url, project_db_token,
                   schedule_expr, schedule_next_run_at,
                   event_triggers, processor, settings, created_at, updated_at
            FROM source_configs
            WHERE org_id = $1 AND source_id = $2
            "#,
        )
        .bind(org_id.0)
        .bind(source_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| Error::backend("get source config", e))?;
        Ok(row.as_ref().map(Self::source_from_row).transpose()?)
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn list_source_configs(
        &self,
        org_id: OrgId,
        query: ListQuery,
    ) -> Result<Vec<SourceConfig>> {
        let rows = sqlx::query(
            r#"
            SELECT org_id, source_id, project_id, connector_id, scope, enabled,
                   project_db_url, project_db_token,
                   schedule_expr, schedule_next_run_at,
                   event_triggers, processor, settings, created_at, updated_at
            FROM source_configs
            WHERE org_id = $1
            ORDER BY created_at DESC
            LIMIT $2 OFFSET $3
            "#,
        )
        .bind(org_id.0)
        .bind(query.limit as i64)
        .bind(query.offset as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| Error::backend("list source configs", e))?;

        let mut out = Vec::with_capacity(rows.len());
        for r in rows {
            out.push(Self::source_from_row(&r)?);
        }
        Ok(out)
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn delete_source_config(&self, org_id: OrgId, source_id: &str) -> Result<()> {
        sqlx::query("DELETE FROM source_configs WHERE org_id = $1 AND source_id = $2")
            .bind(org_id.0)
            .bind(source_id)
            .execute(&self.pool)
            .await
            .map_err(|e| Error::backend("delete source config", e))?;
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self, run))]
    async fn upsert_refresh_run(&self, run: &RefreshRun) -> Result<()> {
        let trigger = serde_json::to_value(&run.trigger)
            .map_err(|e| Error::backend("serialize trigger", e))?;
        let status = match run.status {
            RefreshRunStatus::Running => "running",
            RefreshRunStatus::Succeeded => "succeeded",
            RefreshRunStatus::Failed => "failed",
        };

        sqlx::query(
            r#"
            INSERT INTO refresh_runs (
              run_id, org_id, project_id, source_id, connector_id,
              trigger, status, started_at, finished_at,
              records_pulled, entities_stored, error_message, cursor
            )
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
            ON CONFLICT (run_id) DO UPDATE
              SET status = EXCLUDED.status,
                  finished_at = EXCLUDED.finished_at,
                  records_pulled = EXCLUDED.records_pulled,
                  entities_stored = EXCLUDED.entities_stored,
                  error_message = EXCLUDED.error_message,
                  cursor = EXCLUDED.cursor
            "#,
        )
        .bind(run.run_id)
        .bind(run.org_id.0)
        .bind(run.project_id.0)
        .bind(&run.source_id)
        .bind(&run.connector_id)
        .bind(trigger)
        .bind(status)
        .bind(run.started_at)
        .bind(run.finished_at)
        .bind(run.records_pulled as i64)
        .bind(run.entities_stored as i64)
        .bind(&run.error_message)
        .bind(&run.cursor)
        .execute(&self.pool)
        .await
        .map_err(|e| Error::backend("upsert refresh run", e))?;
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn list_refresh_runs(
        &self,
        org_id: OrgId,
        query: RefreshRunQuery,
    ) -> Result<Vec<RefreshRun>> {
        use sqlx::{Postgres, QueryBuilder};

        let mut qb: QueryBuilder<Postgres> = QueryBuilder::new(
            "SELECT run_id, org_id, project_id, source_id, connector_id, trigger, status, started_at, finished_at, records_pulled, entities_stored, error_message, cursor FROM refresh_runs WHERE org_id = ",
        );
        qb.push_bind(org_id.0);

        if let Some(project_id) = query.project_id {
            qb.push(" AND project_id = ").push_bind(project_id.0);
        }
        if let Some(source_id) = query.source_id.as_deref() {
            qb.push(" AND source_id = ").push_bind(source_id);
        }
        if let Some(status) = query.status {
            let status_str = match status {
                RefreshRunStatus::Running => "running",
                RefreshRunStatus::Succeeded => "succeeded",
                RefreshRunStatus::Failed => "failed",
            };
            qb.push(" AND status = ").push_bind(status_str);
        }
        if let Some(since) = query.since {
            qb.push(" AND started_at >= ").push_bind(since);
        }
        if let Some(until) = query.until {
            qb.push(" AND started_at <= ").push_bind(until);
        }

        qb.push(" ORDER BY started_at DESC LIMIT ")
            .push_bind(query.limit as i64)
            .push(" OFFSET ")
            .push_bind(query.offset as i64);

        let rows = qb
            .build()
            .fetch_all(&self.pool)
            .await
            .map_err(|e| Error::backend("list refresh runs", e))?;

        let mut out = Vec::with_capacity(rows.len());
        for r in rows {
            out.push(Self::refresh_run_from_row(&r)?);
        }
        Ok(out)
    }
}
