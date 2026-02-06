use crate::context_refresh::models::{
    CronSchedule, RefreshRun, RefreshRunQuery, RefreshRunStatus, SourceConfig,
};
use crate::models::{AgentIdentity, AuditEntry, OrgId, PlatformConfig, ProjectDbHandle, ProjectId};
use crate::onboard::config::PostgresConfig;
use crate::onboard::models::{
    AuditQuery, ConnectorCredential, ListQuery, OrgRecord, SyncState, SyncStateKey, UserRecord,
    UserRole,
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
        sqlx::query(MIGRATION_0001)
            .execute(&self.pool)
            .await
            .map_err(|e| Error::backend("apply migrations", e))?;
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
              event_triggers, settings, created_at, updated_at
            )
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
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
                   event_triggers, settings, created_at, updated_at
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
                   event_triggers, settings, created_at, updated_at
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
