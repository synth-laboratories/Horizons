use async_trait::async_trait;
use chrono::Utc;
use sqlx::postgres::{PgArguments, PgPool, PgPoolOptions};
use sqlx::{Arguments, Column, Row};

use crate::models::{OrgId, ProjectDbHandle, ProjectId};
use crate::onboard::config::PostgresConfig;
use crate::onboard::models::{ListQuery, ProjectDbParam, ProjectDbRow, ProjectDbValue};
use crate::onboard::traits::{ProjectDb, ensure_handle_org};
use crate::{Error, Result};

/// Postgres-backed ProjectDb implementation.
///
/// Each project is isolated in a dedicated schema and queried via
/// `SET LOCAL search_path` per request.
#[derive(Clone)]
pub struct PostgresProjectDb {
    pool: PgPool,
}

impl PostgresProjectDb {
    pub async fn connect(cfg: &PostgresConfig) -> Result<Self> {
        let pg_url = cfg.sqlx_url();
        let pool = PgPoolOptions::new()
            .max_connections(cfg.max_connections)
            .acquire_timeout(cfg.acquire_timeout)
            .connect(&pg_url)
            .await
            .map_err(|e| Error::backend("connect postgres_project_db", e))?;
        Ok(Self { pool })
    }

    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    fn schema_name(org_id: OrgId, project_id: ProjectId) -> String {
        let org = org_id.to_string().replace('-', "");
        let proj = project_id.to_string().replace('-', "");
        format!("p_{}_{}", &org[..16], &proj[..16])
    }

    fn handle_schema(handle: &ProjectDbHandle) -> String {
        handle
            .auth_token
            .clone()
            .unwrap_or_else(|| Self::schema_name(handle.org_id, handle.project_id))
    }

    fn rewrite_sql_placeholders(sql: &str) -> String {
        let chars: Vec<char> = sql.chars().collect();
        let mut out = String::with_capacity(sql.len());
        let mut i = 0usize;
        while i < chars.len() {
            if chars[i] == '?' {
                let mut j = i + 1;
                while j < chars.len() && chars[j].is_ascii_digit() {
                    j += 1;
                }
                if j > i + 1 {
                    out.push('$');
                    for ch in &chars[(i + 1)..j] {
                        out.push(*ch);
                    }
                    i = j;
                    continue;
                }
            }
            out.push(chars[i]);
            i += 1;
        }
        out
    }

    fn add_params(args: &mut PgArguments, params: &[ProjectDbParam]) -> Result<()> {
        for p in params {
            match p {
                ProjectDbParam::Null => {
                    args.add(Option::<String>::None)
                        .map_err(|e| Error::BackendMessage(format!("bind null: {e}")))?;
                }
                ProjectDbParam::Bool(v) => args
                    .add(*v)
                    .map_err(|e| Error::BackendMessage(format!("bind bool: {e}")))?,
                ProjectDbParam::I64(v) => args
                    .add(*v)
                    .map_err(|e| Error::BackendMessage(format!("bind i64: {e}")))?,
                ProjectDbParam::F64(v) => args
                    .add(*v)
                    .map_err(|e| Error::BackendMessage(format!("bind f64: {e}")))?,
                ProjectDbParam::String(v) => args
                    .add(v.clone())
                    .map_err(|e| Error::BackendMessage(format!("bind string: {e}")))?,
                ProjectDbParam::Bytes(v) => args
                    .add(v.clone())
                    .map_err(|e| Error::BackendMessage(format!("bind bytes: {e}")))?,
                ProjectDbParam::Json(v) => args
                    .add(sqlx::types::Json(v.clone()))
                    .map_err(|e| Error::BackendMessage(format!("bind json: {e}")))?,
            }
        }
        Ok(())
    }

    fn pg_row_to_project(row: &sqlx::postgres::PgRow) -> Result<ProjectDbRow> {
        let mut out = ProjectDbRow::new();
        for col in row.columns() {
            let name = col.name();

            if let Ok(v) = row.try_get::<Option<String>, _>(name) {
                if let Some(v) = v {
                    out.insert(name.to_string(), ProjectDbValue::String(v));
                    continue;
                }
            }
            if let Ok(v) = row.try_get::<Option<i64>, _>(name) {
                if let Some(v) = v {
                    out.insert(name.to_string(), ProjectDbValue::I64(v));
                    continue;
                }
            }
            if let Ok(v) = row.try_get::<Option<i32>, _>(name) {
                if let Some(v) = v {
                    out.insert(name.to_string(), ProjectDbValue::I64(v as i64));
                    continue;
                }
            }
            if let Ok(v) = row.try_get::<Option<f64>, _>(name) {
                if let Some(v) = v {
                    out.insert(name.to_string(), ProjectDbValue::F64(v));
                    continue;
                }
            }
            if let Ok(v) = row.try_get::<Option<f32>, _>(name) {
                if let Some(v) = v {
                    out.insert(name.to_string(), ProjectDbValue::F64(v as f64));
                    continue;
                }
            }
            if let Ok(v) = row.try_get::<Option<bool>, _>(name) {
                if let Some(v) = v {
                    out.insert(name.to_string(), ProjectDbValue::Bool(v));
                    continue;
                }
            }
            if let Ok(v) = row.try_get::<Option<Vec<u8>>, _>(name) {
                if let Some(v) = v {
                    out.insert(name.to_string(), ProjectDbValue::Bytes(v));
                    continue;
                }
            }
            if let Ok(v) = row.try_get::<Option<serde_json::Value>, _>(name) {
                if let Some(v) = v {
                    out.insert(name.to_string(), ProjectDbValue::Json(v));
                    continue;
                }
            }
            if let Ok(v) = row.try_get::<Option<chrono::DateTime<Utc>>, _>(name) {
                if let Some(v) = v {
                    out.insert(name.to_string(), ProjectDbValue::String(v.to_rfc3339()));
                    continue;
                }
            }

            out.insert(name.to_string(), ProjectDbValue::Null);
        }
        Ok(out)
    }
}

#[async_trait]
impl ProjectDb for PostgresProjectDb {
    async fn provision(&self, org_id: OrgId, project_id: ProjectId) -> Result<ProjectDbHandle> {
        let schema = Self::schema_name(org_id, project_id);
        let schema_sql = format!("CREATE SCHEMA IF NOT EXISTS \"{schema}\"");
        sqlx::query(&schema_sql)
            .execute(&self.pool)
            .await
            .map_err(|e| Error::backend("create project schema", e))?;

        sqlx::query(
            r#"
            INSERT INTO project_dbs (org_id, project_id, connection_url, auth_token, created_at)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (org_id, project_id) DO UPDATE
              SET connection_url = EXCLUDED.connection_url,
                  auth_token = EXCLUDED.auth_token
            "#,
        )
        .bind(org_id.0)
        .bind(project_id.0)
        .bind("postgres://central")
        .bind(&schema)
        .bind(Utc::now())
        .execute(&self.pool)
        .await
        .map_err(|e| Error::backend("upsert project_dbs", e))?;

        Ok(ProjectDbHandle {
            org_id,
            project_id,
            connection_url: "postgres://central".to_string(),
            auth_token: Some(schema),
        })
    }

    async fn get_handle(
        &self,
        org_id: OrgId,
        project_id: ProjectId,
    ) -> Result<Option<ProjectDbHandle>> {
        let row = sqlx::query(
            r#"
            SELECT connection_url, auth_token
            FROM project_dbs
            WHERE org_id = $1 AND project_id = $2
            LIMIT 1
            "#,
        )
        .bind(org_id.0)
        .bind(project_id.0)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| Error::backend("get project handle", e))?;

        Ok(row.map(|r| ProjectDbHandle {
            org_id,
            project_id,
            connection_url: r
                .try_get("connection_url")
                .unwrap_or_else(|_| "postgres://central".to_string()),
            auth_token: r.try_get("auth_token").ok().flatten(),
        }))
    }

    async fn list_projects(&self, org_id: OrgId, query: ListQuery) -> Result<Vec<ProjectDbHandle>> {
        let rows = sqlx::query(
            r#"
            SELECT project_id, connection_url, auth_token
            FROM project_dbs
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
            out.push(ProjectDbHandle {
                org_id,
                project_id: ProjectId(
                    r.try_get::<uuid::Uuid, _>("project_id")
                        .map_err(|e| Error::backend("project_id", e))?,
                ),
                connection_url: r
                    .try_get("connection_url")
                    .unwrap_or_else(|_| "postgres://central".to_string()),
                auth_token: r.try_get("auth_token").ok().flatten(),
            });
        }
        Ok(out)
    }

    async fn query(
        &self,
        org_id: OrgId,
        handle: &ProjectDbHandle,
        sql: &str,
        params: &[ProjectDbParam],
    ) -> Result<Vec<ProjectDbRow>> {
        ensure_handle_org(handle, org_id)?;
        let schema = Self::handle_schema(handle);
        let sql = Self::rewrite_sql_placeholders(sql);

        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| Error::backend("begin project query tx", e))?;
        let set_search_path = format!("SET LOCAL search_path TO \"{schema}\", public");
        sqlx::query(&set_search_path)
            .execute(&mut *tx)
            .await
            .map_err(|e| Error::backend("set project search_path", e))?;

        let mut args = PgArguments::default();
        Self::add_params(&mut args, params)?;
        let rows = sqlx::query_with(&sql, args)
            .fetch_all(&mut *tx)
            .await
            .map_err(|e| Error::backend("project db query", e))?;
        tx.commit()
            .await
            .map_err(|e| Error::backend("commit project query tx", e))?;

        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            out.push(Self::pg_row_to_project(&row)?);
        }
        Ok(out)
    }

    async fn execute(
        &self,
        org_id: OrgId,
        handle: &ProjectDbHandle,
        sql: &str,
        params: &[ProjectDbParam],
    ) -> Result<u64> {
        ensure_handle_org(handle, org_id)?;
        let schema = Self::handle_schema(handle);
        let sql = Self::rewrite_sql_placeholders(sql);

        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| Error::backend("begin project execute tx", e))?;
        let set_search_path = format!("SET LOCAL search_path TO \"{schema}\", public");
        sqlx::query(&set_search_path)
            .execute(&mut *tx)
            .await
            .map_err(|e| Error::backend("set project search_path", e))?;

        let mut args = PgArguments::default();
        Self::add_params(&mut args, params)?;
        let res = sqlx::query_with(&sql, args)
            .execute(&mut *tx)
            .await
            .map_err(|e| Error::backend("project db execute", e))?;
        tx.commit()
            .await
            .map_err(|e| Error::backend("commit project execute tx", e))?;
        Ok(res.rows_affected())
    }
}
