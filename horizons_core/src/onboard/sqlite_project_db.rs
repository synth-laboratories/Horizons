//! SQLite-backed ProjectDb implementation.
//!
//! This is intended for local development and tests where Turso/libSQL +
//! Postgres metadata isn't available. It provisions one SQLite file per project
//! under a root directory.

use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    str::FromStr,
    sync::Arc,
};

use async_trait::async_trait;
use dashmap::DashMap;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
use sqlx::{Arguments, Column, Row, SqlitePool};
use tokio::sync::RwLock;

use crate::models::{OrgId, ProjectDbHandle, ProjectId};
use crate::onboard::models::{ListQuery, ProjectDbParam, ProjectDbRow, ProjectDbValue};
use crate::onboard::traits::{ProjectDb, ensure_handle_org};
use crate::{Error, Result};

#[derive(Clone)]
pub struct SqliteProjectDb {
    root_dir: PathBuf,
    handles: Arc<RwLock<HashMap<(OrgId, ProjectId), ProjectDbHandle>>>,
    pools: Arc<DashMap<String, SqlitePool>>,
}

impl SqliteProjectDb {
    pub fn new(root_dir: impl AsRef<Path>) -> Self {
        Self {
            root_dir: root_dir.as_ref().to_path_buf(),
            handles: Arc::new(RwLock::new(HashMap::new())),
            pools: Arc::new(DashMap::new()),
        }
    }

    pub fn project_db_path(&self, org_id: OrgId, project_id: ProjectId) -> PathBuf {
        self.root_dir
            .join(org_id.to_string())
            .join(format!("{project_id}.db"))
    }

    async fn pool_for_handle(&self, handle: &ProjectDbHandle) -> Result<SqlitePool> {
        let url = handle.connection_url.clone();
        if let Some(p) = self.pools.get(&url) {
            return Ok(p.value().clone());
        }

        let guard = self.handles.write().await;
        // Double-check after acquiring a write guard (avoid thundering herd).
        if let Some(p) = self.pools.get(&url) {
            return Ok(p.value().clone());
        }

        let opts = SqliteConnectOptions::from_str(&url)
            .map_err(|e| Error::backend("sqlite_project_db", e))?
            .journal_mode(SqliteJournalMode::Wal)
            .create_if_missing(true);

        let pool = SqlitePoolOptions::new()
            .max_connections(5)
            .connect_with(opts)
            .await
            .map_err(|e| Error::backend("sqlite_project_db", e))?;

        drop(guard);
        self.pools.insert(url.clone(), pool.clone());
        Ok(pool)
    }

    fn add_params(
        args: &mut sqlx::sqlite::SqliteArguments<'_>,
        params: &[ProjectDbParam],
    ) -> Result<()> {
        for p in params {
            match p {
                ProjectDbParam::Null => {
                    args.add(Option::<String>::None).map_err(|e| {
                        Error::BackendMessage(format!("sqlite_project_db bind: {e}"))
                    })?;
                }
                ProjectDbParam::Bool(b) => args
                    .add(*b)
                    .map_err(|e| Error::BackendMessage(format!("sqlite_project_db bind: {e}")))?,
                ProjectDbParam::I64(i) => args
                    .add(*i)
                    .map_err(|e| Error::BackendMessage(format!("sqlite_project_db bind: {e}")))?,
                ProjectDbParam::F64(f) => args
                    .add(*f)
                    .map_err(|e| Error::BackendMessage(format!("sqlite_project_db bind: {e}")))?,
                ProjectDbParam::String(s) => args
                    .add(s.clone())
                    .map_err(|e| Error::BackendMessage(format!("sqlite_project_db bind: {e}")))?,
                ProjectDbParam::Bytes(b) => args
                    .add(b.clone())
                    .map_err(|e| Error::BackendMessage(format!("sqlite_project_db bind: {e}")))?,
                ProjectDbParam::Json(v) => {
                    let s =
                        serde_json::to_string(v).map_err(|e| Error::backend("json param", e))?;
                    args.add(s).map_err(|e| {
                        Error::BackendMessage(format!("sqlite_project_db bind: {e}"))
                    })?;
                }
            }
        }
        Ok(())
    }

    fn sqlite_row_to_project(row: &sqlx::sqlite::SqliteRow) -> Result<ProjectDbRow> {
        let mut out = ProjectDbRow::new();
        for col in row.columns() {
            let name = col.name();

            // Best-effort decoding. Many call sites store JSON as TEXT and decode themselves.
            if let Ok(v) = row.try_get::<Option<i64>, _>(name) {
                if let Some(v) = v {
                    out.insert(name.to_string(), ProjectDbValue::I64(v));
                    continue;
                }
            }
            if let Ok(v) = row.try_get::<Option<f64>, _>(name) {
                if let Some(v) = v {
                    out.insert(name.to_string(), ProjectDbValue::F64(v));
                    continue;
                }
            }
            if let Ok(v) = row.try_get::<Option<String>, _>(name) {
                if let Some(v) = v {
                    out.insert(name.to_string(), ProjectDbValue::String(v));
                    continue;
                }
            }
            if let Ok(v) = row.try_get::<Option<Vec<u8>>, _>(name) {
                if let Some(v) = v {
                    out.insert(name.to_string(), ProjectDbValue::Bytes(v));
                    continue;
                }
            }

            out.insert(name.to_string(), ProjectDbValue::Null);
        }
        Ok(out)
    }
}

#[async_trait]
impl ProjectDb for SqliteProjectDb {
    async fn provision(&self, org_id: OrgId, project_id: ProjectId) -> Result<ProjectDbHandle> {
        let dir = self.root_dir.join(org_id.to_string());
        tokio::fs::create_dir_all(&dir)
            .await
            .map_err(|e| Error::backend("sqlite_project_db mkdir", e))?;

        let path = self.project_db_path(org_id, project_id);
        let url = format!("sqlite://{}?mode=rwc", path.display());

        // Ensure file exists and is openable.
        let opts = SqliteConnectOptions::from_str(&url)
            .map_err(|e| Error::backend("sqlite_project_db", e))?
            .journal_mode(SqliteJournalMode::Wal)
            .create_if_missing(true);
        let _ = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(opts)
            .await
            .map_err(|e| Error::backend("sqlite_project_db", e))?;

        let handle = ProjectDbHandle {
            org_id,
            project_id,
            connection_url: url,
            auth_token: None,
        };

        self.handles
            .write()
            .await
            .insert((org_id, project_id), handle.clone());
        Ok(handle)
    }

    async fn get_handle(
        &self,
        org_id: OrgId,
        project_id: ProjectId,
    ) -> Result<Option<ProjectDbHandle>> {
        Ok(self
            .handles
            .read()
            .await
            .get(&(org_id, project_id))
            .cloned())
    }

    async fn list_projects(&self, org_id: OrgId, query: ListQuery) -> Result<Vec<ProjectDbHandle>> {
        let handles = self.handles.read().await;
        let mut out: Vec<ProjectDbHandle> = handles
            .iter()
            .filter(|((oid, _), _)| *oid == org_id)
            .map(|(_, h)| h.clone())
            .collect();
        out.sort_by(|a, b| b.project_id.to_string().cmp(&a.project_id.to_string()));
        let start = query.offset.min(out.len());
        let end = (start + query.limit).min(out.len());
        Ok(out[start..end].to_vec())
    }

    async fn query(
        &self,
        org_id: OrgId,
        handle: &ProjectDbHandle,
        sql: &str,
        params: &[ProjectDbParam],
    ) -> Result<Vec<ProjectDbRow>> {
        ensure_handle_org(handle, org_id)?;
        let pool = self.pool_for_handle(handle).await?;
        let mut args = sqlx::sqlite::SqliteArguments::default();
        Self::add_params(&mut args, params)?;
        let rows = sqlx::query_with(sql, args)
            .fetch_all(&pool)
            .await
            .map_err(|e| Error::backend("sqlite_project_db query", e))?;

        let mut out = Vec::with_capacity(rows.len());
        for r in rows {
            out.push(Self::sqlite_row_to_project(&r)?);
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
        let pool = self.pool_for_handle(handle).await?;
        let mut args = sqlx::sqlite::SqliteArguments::default();
        Self::add_params(&mut args, params)?;
        let res = sqlx::query_with(sql, args)
            .execute(&pool)
            .await
            .map_err(|e| Error::backend("sqlite_project_db execute", e))?;
        Ok(res.rows_affected())
    }
}
