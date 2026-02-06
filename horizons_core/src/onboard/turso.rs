use crate::models::{OrgId, ProjectDbHandle, ProjectId};
use crate::onboard::config::ProjectDbConfig;
use crate::onboard::models::{ListQuery, ProjectDbParam, ProjectDbRow, ProjectDbValue};
use crate::onboard::traits::{ProjectDb, ensure_handle_org};
use crate::{Error, Result};
use async_trait::async_trait;
use chrono::Utc;
use libsql::{Builder, Database, Value};
use sqlx::{PgPool, Row};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Clone)]
pub struct LibsqlProjectDb {
    meta_pool: PgPool,
    root_dir: PathBuf,
    cache: Arc<RwLock<HashMap<String, Arc<Database>>>>,
}

impl LibsqlProjectDb {
    #[tracing::instrument(level = "debug", skip(meta_pool, cfg))]
    pub fn new(meta_pool: PgPool, cfg: &ProjectDbConfig) -> Self {
        Self {
            meta_pool,
            root_dir: cfg.root_dir.clone(),
            cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    #[tracing::instrument(level = "debug")]
    pub fn project_db_path_for(
        root_dir: impl AsRef<Path> + std::fmt::Debug,
        org_id: OrgId,
        project_id: ProjectId,
    ) -> PathBuf {
        root_dir
            .as_ref()
            .join(org_id.to_string())
            .join(format!("{}.db", project_id))
    }

    #[tracing::instrument(level = "debug", skip(self))]
    pub fn project_db_path(&self, org_id: OrgId, project_id: ProjectId) -> PathBuf {
        Self::project_db_path_for(&self.root_dir, org_id, project_id)
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn ensure_root_dirs(&self, org_id: OrgId) -> Result<()> {
        let dir = self.root_dir.join(org_id.to_string());
        std::fs::create_dir_all(&dir)
            .map_err(|e| Error::backend(format!("create project db dir {dir:?}"), e))?;
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self, handle))]
    async fn db_for_handle(&self, handle: &ProjectDbHandle) -> Result<Arc<Database>> {
        let key = format!(
            "{}|{}",
            handle.connection_url,
            handle.auth_token.as_deref().unwrap_or("")
        );

        if let Some(db) = self.cache.read().await.get(&key).cloned() {
            return Ok(db);
        }

        let mut guard = self.cache.write().await;
        if let Some(db) = guard.get(&key).cloned() {
            return Ok(db);
        }

        let db = if handle.connection_url.starts_with("libsql://") {
            let token = handle.auth_token.clone().ok_or_else(|| {
                Error::InvalidInput("project db handle missing auth_token for remote libsql".into())
            })?;
            Builder::new_remote(handle.connection_url.clone(), token)
                .build()
                .await
                .map_err(|e| Error::backend("build remote libsql db", e))?
        } else {
            Builder::new_local(handle.connection_url.clone())
                .build()
                .await
                .map_err(|e| Error::backend("build local libsql db", e))?
        };

        let db = Arc::new(db);
        guard.insert(key, Arc::clone(&db));
        Ok(db)
    }

    #[tracing::instrument(level = "debug", skip(params))]
    fn to_libsql_params(params: &[ProjectDbParam]) -> Result<Vec<Value>> {
        let mut out = Vec::with_capacity(params.len());
        for p in params {
            let v = match p {
                ProjectDbParam::Null => Value::Null,
                ProjectDbParam::Bool(b) => Value::Integer(if *b { 1 } else { 0 }),
                ProjectDbParam::I64(i) => Value::Integer(*i),
                ProjectDbParam::F64(f) => Value::Real(*f),
                ProjectDbParam::String(s) => Value::Text(s.clone()),
                ProjectDbParam::Bytes(b) => Value::Blob(b.clone()),
                ProjectDbParam::Json(v) => Value::Text(
                    serde_json::to_string(v).map_err(|e| Error::backend("json param", e))?,
                ),
            };
            out.push(v);
        }
        Ok(out)
    }

    #[tracing::instrument(level = "debug")]
    fn value_to_project(value: Value) -> ProjectDbValue {
        match value {
            Value::Null => ProjectDbValue::Null,
            Value::Integer(i) => ProjectDbValue::I64(i),
            Value::Real(f) => ProjectDbValue::F64(f),
            Value::Text(s) => ProjectDbValue::String(s),
            Value::Blob(b) => ProjectDbValue::Bytes(b),
        }
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn upsert_handle_record(&self, handle: &ProjectDbHandle) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO project_dbs (org_id, project_id, connection_url, auth_token, created_at)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (org_id, project_id) DO UPDATE
              SET connection_url = EXCLUDED.connection_url,
                  auth_token = EXCLUDED.auth_token
            "#,
        )
        .bind(handle.org_id.0)
        .bind(handle.project_id.0)
        .bind(&handle.connection_url)
        .bind(&handle.auth_token)
        .bind(Utc::now())
        .execute(&self.meta_pool)
        .await
        .map_err(|e| Error::backend("upsert project handle", e))?;
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(row))]
    fn handle_from_row(row: &sqlx::postgres::PgRow) -> Result<ProjectDbHandle> {
        Ok(ProjectDbHandle {
            org_id: OrgId(
                row.try_get::<uuid::Uuid, _>("org_id")
                    .map_err(|e| Error::backend("org_id", e))?,
            ),
            project_id: ProjectId(
                row.try_get::<uuid::Uuid, _>("project_id")
                    .map_err(|e| Error::backend("project_id", e))?,
            ),
            connection_url: row
                .try_get("connection_url")
                .map_err(|e| Error::backend("connection_url", e))?,
            auth_token: row
                .try_get("auth_token")
                .map_err(|e| Error::backend("auth_token", e))?,
        })
    }
}

#[async_trait]
impl ProjectDb for LibsqlProjectDb {
    #[tracing::instrument(level = "info", skip(self))]
    async fn provision(&self, org_id: OrgId, project_id: ProjectId) -> Result<ProjectDbHandle> {
        self.ensure_root_dirs(org_id).await?;
        let path = self.project_db_path(org_id, project_id);

        // Provision by opening/building the database once (creates file if missing).
        Builder::new_local(path.to_string_lossy().to_string())
            .build()
            .await
            .map_err(|e| Error::backend("provision local project db", e))?;

        let handle = ProjectDbHandle {
            org_id,
            project_id,
            connection_url: path.to_string_lossy().to_string(),
            auth_token: None,
        };

        self.upsert_handle_record(&handle).await?;
        Ok(handle)
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn get_handle(
        &self,
        org_id: OrgId,
        project_id: ProjectId,
    ) -> Result<Option<ProjectDbHandle>> {
        let row = sqlx::query(
            "SELECT org_id, project_id, connection_url, auth_token FROM project_dbs WHERE org_id = $1 AND project_id = $2",
        )
        .bind(org_id.0)
        .bind(project_id.0)
        .fetch_optional(&self.meta_pool)
        .await
        .map_err(|e| Error::backend("get project handle", e))?;
        Ok(row.as_ref().map(Self::handle_from_row).transpose()?)
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn list_projects(&self, org_id: OrgId, query: ListQuery) -> Result<Vec<ProjectDbHandle>> {
        let rows = sqlx::query(
            r#"
            SELECT org_id, project_id, connection_url, auth_token
            FROM project_dbs
            WHERE org_id = $1
            ORDER BY created_at DESC
            LIMIT $2 OFFSET $3
            "#,
        )
        .bind(org_id.0)
        .bind(query.limit as i64)
        .bind(query.offset as i64)
        .fetch_all(&self.meta_pool)
        .await
        .map_err(|e| Error::backend("list projects", e))?;

        let mut out = Vec::with_capacity(rows.len());
        for r in rows {
            out.push(Self::handle_from_row(&r)?);
        }
        Ok(out)
    }

    #[tracing::instrument(level = "debug", skip(self, handle, sql, params))]
    async fn query(
        &self,
        org_id: OrgId,
        handle: &ProjectDbHandle,
        sql: &str,
        params: &[ProjectDbParam],
    ) -> Result<Vec<ProjectDbRow>> {
        ensure_handle_org(handle, org_id)?;

        let db = self.db_for_handle(handle).await?;
        let conn = db
            .connect()
            .map_err(|e| Error::backend("connect project db", e))?;
        let values = Self::to_libsql_params(params)?;
        let mut rows = conn
            .query(sql, libsql::params_from_iter(values))
            .await
            .map_err(|e| Error::backend("project db query", e))?;

        let mut out = Vec::new();
        while let Some(row) = rows
            .next()
            .await
            .map_err(|e| Error::backend("project db next row", e))?
        {
            let mut map = ProjectDbRow::new();
            let col_count = row.column_count();
            for i in 0..(col_count as i32) {
                let name = row
                    .column_name(i)
                    .ok_or_else(|| Error::BackendMessage("missing column name".to_string()))?;
                let val = row
                    .get_value(i)
                    .map_err(|e| Error::backend("project db get_value", e))?;
                map.insert(name.to_string(), Self::value_to_project(val));
            }
            out.push(map);
        }
        Ok(out)
    }

    #[tracing::instrument(level = "debug", skip(self, handle, sql, params))]
    async fn execute(
        &self,
        org_id: OrgId,
        handle: &ProjectDbHandle,
        sql: &str,
        params: &[ProjectDbParam],
    ) -> Result<u64> {
        ensure_handle_org(handle, org_id)?;

        let db = self.db_for_handle(handle).await?;
        let conn = db
            .connect()
            .map_err(|e| Error::backend("connect project db", e))?;
        let values = Self::to_libsql_params(params)?;
        let n = conn
            .execute(sql, libsql::params_from_iter(values))
            .await
            .map_err(|e| Error::backend("project db execute", e))?;
        Ok(n)
    }
}
