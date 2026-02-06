use super::traits::{ContinualLearning, Dataset, MiproConfig, OptimizationResult, Policy};
use crate::models::{AgentIdentity, AuditEntry, OrgId, ProjectDbHandle, ProjectId};
use crate::onboard::traits::{
    CentralDb, Filestore, ProjectDb, ProjectDbParam, ProjectDbRow, ProjectDbValue,
    ensure_handle_org,
};
use crate::{Error, Result};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use uuid::Uuid;

const RUNS_TABLE_DDL: &str = r#"
CREATE TABLE IF NOT EXISTS horizons_optimization_runs (
  run_id TEXT PRIMARY KEY,
  org_id TEXT NOT NULL,
  project_id TEXT NOT NULL,
  status TEXT NOT NULL,
  created_at TEXT NOT NULL,
  finished_at TEXT NULL,
  artifact_key TEXT NULL,
  error_message TEXT NULL
);
CREATE INDEX IF NOT EXISTS hor_opt_runs_org_proj_created
  ON horizons_optimization_runs(org_id, project_id, created_at);
"#;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OptimizationRunStatus {
    Running,
    Succeeded,
    Failed,
}

impl OptimizationRunStatus {
    fn as_str(self) -> &'static str {
        match self {
            OptimizationRunStatus::Running => "running",
            OptimizationRunStatus::Succeeded => "succeeded",
            OptimizationRunStatus::Failed => "failed",
        }
    }

    fn from_str(s: &str) -> Result<Self> {
        match s {
            "running" => Ok(Self::Running),
            "succeeded" => Ok(Self::Succeeded),
            "failed" => Ok(Self::Failed),
            other => Err(Error::InvalidInput(format!(
                "unknown optimization run status: {other}"
            ))),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OptimizationRunRow {
    pub run_id: Uuid,
    pub org_id: OrgId,
    pub project_id: ProjectId,
    pub status: OptimizationRunStatus,
    pub created_at: DateTime<Utc>,
    pub finished_at: Option<DateTime<Utc>>,
    pub artifact_key: Option<String>,
    pub error_message: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OptimizationArtifact {
    pub run_id: Uuid,
    pub org_id: OrgId,
    pub project_id: ProjectId,
    pub created_at: DateTime<Utc>,
    pub identity: AgentIdentity,
    pub cfg: MiproConfig,
    pub initial_policy: Policy,
    pub dataset: Dataset,
    pub result: OptimizationResult,
}

#[derive(Clone)]
pub struct OptimizationEngine {
    central_db: Arc<dyn CentralDb>,
    project_db: Arc<dyn ProjectDb>,
    filestore: Arc<dyn Filestore>,
    cl: Arc<dyn ContinualLearning>,
}

impl OptimizationEngine {
    #[tracing::instrument(level = "debug", skip(central_db, project_db, filestore, cl))]
    pub fn new(
        central_db: Arc<dyn CentralDb>,
        project_db: Arc<dyn ProjectDb>,
        filestore: Arc<dyn Filestore>,
        cl: Arc<dyn ContinualLearning>,
    ) -> Self {
        Self {
            central_db,
            project_db,
            filestore,
            cl,
        }
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn ensure_schema(&self, org_id: OrgId, handle: &ProjectDbHandle) -> Result<()> {
        ensure_handle_org(handle, org_id)?;
        for stmt in RUNS_TABLE_DDL
            .split(';')
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            self.project_db.execute(org_id, handle, stmt, &[]).await?;
        }
        Ok(())
    }

    #[tracing::instrument(level = "info", skip_all)]
    pub async fn run(
        &self,
        org_id: OrgId,
        project_id: ProjectId,
        project_db: &ProjectDbHandle,
        identity: &AgentIdentity,
        cfg: MiproConfig,
        initial_policy: Policy,
        dataset: Dataset,
    ) -> Result<OptimizationRunRow> {
        ensure_handle_org(project_db, org_id)?;
        if project_db.project_id != project_id {
            return Err(Error::InvalidInput("project_id mismatch".to_string()));
        }
        self.ensure_schema(org_id, project_db).await?;

        let run_id = Uuid::new_v4();
        let created_at = Utc::now();

        self.insert_row(
            org_id,
            project_id,
            project_db,
            run_id,
            OptimizationRunStatus::Running,
            created_at,
            None,
            None,
            None,
        )
        .await?;

        self.central_db
            .append_audit_entry(&AuditEntry {
                id: Uuid::new_v4(),
                org_id,
                project_id: Some(project_id),
                actor: identity.clone(),
                action: "optimization.run.started".to_string(),
                payload: serde_json::json!({
                    "run_id": run_id.to_string(),
                    "status": "running",
                }),
                outcome: "ok".to_string(),
                created_at,
            })
            .await?;

        let outcome = self
            .cl
            .run_batch(cfg.clone(), initial_policy.clone(), dataset.clone())
            .await;

        match outcome {
            Ok(result) => {
                let artifact_key = optimization_artifact_key(run_id);
                let artifact = OptimizationArtifact {
                    run_id,
                    org_id,
                    project_id,
                    created_at,
                    identity: identity.clone(),
                    cfg,
                    initial_policy,
                    dataset,
                    result,
                };
                let bytes = serde_json::to_vec(&artifact)
                    .map_err(|e| Error::backend("serialize optimization artifact", e))?;
                self.filestore
                    .put(org_id, Some(project_id), &artifact_key, Bytes::from(bytes))
                    .await?;

                let finished_at = Utc::now();
                self.update_row(
                    org_id,
                    project_id,
                    project_db,
                    run_id,
                    OptimizationRunStatus::Succeeded,
                    Some(finished_at),
                    Some(&artifact_key),
                    None,
                )
                .await?;

                self.central_db
                    .append_audit_entry(&AuditEntry {
                        id: Uuid::new_v4(),
                        org_id,
                        project_id: Some(project_id),
                        actor: identity.clone(),
                        action: "optimization.run.finished".to_string(),
                        payload: serde_json::json!({
                            "run_id": run_id.to_string(),
                            "status": "succeeded",
                            "artifact_key": artifact_key,
                        }),
                        outcome: "ok".to_string(),
                        created_at: finished_at,
                    })
                    .await?;

                Ok(OptimizationRunRow {
                    run_id,
                    org_id,
                    project_id,
                    status: OptimizationRunStatus::Succeeded,
                    created_at,
                    finished_at: Some(finished_at),
                    artifact_key: Some(artifact_key),
                    error_message: None,
                })
            }
            Err(e) => {
                let finished_at = Utc::now();
                let msg = e.to_string();
                self.update_row(
                    org_id,
                    project_id,
                    project_db,
                    run_id,
                    OptimizationRunStatus::Failed,
                    Some(finished_at),
                    None,
                    Some(&msg),
                )
                .await?;

                self.central_db
                    .append_audit_entry(&AuditEntry {
                        id: Uuid::new_v4(),
                        org_id,
                        project_id: Some(project_id),
                        actor: identity.clone(),
                        action: "optimization.run.finished".to_string(),
                        payload: serde_json::json!({
                            "run_id": run_id.to_string(),
                            "status": "failed",
                            "error": msg,
                        }),
                        outcome: "failed".to_string(),
                        created_at: finished_at,
                    })
                    .await?;

                Ok(OptimizationRunRow {
                    run_id,
                    org_id,
                    project_id,
                    status: OptimizationRunStatus::Failed,
                    created_at,
                    finished_at: Some(finished_at),
                    artifact_key: None,
                    error_message: Some(msg),
                })
            }
        }
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn list_status(
        &self,
        org_id: OrgId,
        project_id: ProjectId,
        project_db: &ProjectDbHandle,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<OptimizationRunRow>> {
        if limit == 0 {
            return Err(Error::InvalidInput("limit must be > 0".to_string()));
        }
        ensure_handle_org(project_db, org_id)?;
        if project_db.project_id != project_id {
            return Err(Error::InvalidInput("project_id mismatch".to_string()));
        }
        self.ensure_schema(org_id, project_db).await?;

        let sql = r#"
SELECT run_id, org_id, project_id, status, created_at, finished_at, artifact_key, error_message
  FROM horizons_optimization_runs
 WHERE org_id = ?1 AND project_id = ?2
 ORDER BY created_at DESC
 LIMIT ?3 OFFSET ?4
"#;
        let params = vec![
            ProjectDbParam::String(org_id.to_string()),
            ProjectDbParam::String(project_id.to_string()),
            ProjectDbParam::I64(limit as i64),
            ProjectDbParam::I64(offset as i64),
        ];
        let rows = self
            .project_db
            .query(org_id, project_db, sql, &params)
            .await?;
        rows.into_iter().map(row_to_run).collect()
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn get_report(
        &self,
        org_id: OrgId,
        project_id: ProjectId,
        project_db: &ProjectDbHandle,
        run_id: Uuid,
    ) -> Result<OptimizationArtifact> {
        ensure_handle_org(project_db, org_id)?;
        if project_db.project_id != project_id {
            return Err(Error::InvalidInput("project_id mismatch".to_string()));
        }
        self.ensure_schema(org_id, project_db).await?;

        let sql = r#"
SELECT artifact_key
  FROM horizons_optimization_runs
 WHERE org_id = ?1 AND project_id = ?2 AND run_id = ?3
 LIMIT 1
"#;
        let params = vec![
            ProjectDbParam::String(org_id.to_string()),
            ProjectDbParam::String(project_id.to_string()),
            ProjectDbParam::String(run_id.to_string()),
        ];
        let rows = self
            .project_db
            .query(org_id, project_db, sql, &params)
            .await?;
        if rows.is_empty() {
            return Err(Error::NotFound(format!(
                "optimization run not found: {run_id}"
            )));
        }
        let key = get_opt_string(&rows[0], "artifact_key")?
            .ok_or_else(|| Error::NotFound("optimization artifact not available".to_string()))?;

        let Some(bytes) = self.filestore.get(org_id, Some(project_id), &key).await? else {
            return Err(Error::NotFound("optimization artifact missing".to_string()));
        };
        let artifact: OptimizationArtifact = serde_json::from_slice(&bytes)
            .map_err(|e| Error::backend("deserialize optimization artifact", e))?;
        Ok(artifact)
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn insert_row(
        &self,
        org_id: OrgId,
        project_id: ProjectId,
        project_db: &ProjectDbHandle,
        run_id: Uuid,
        status: OptimizationRunStatus,
        created_at: DateTime<Utc>,
        finished_at: Option<DateTime<Utc>>,
        artifact_key: Option<&str>,
        error_message: Option<&str>,
    ) -> Result<()> {
        let sql = r#"
INSERT INTO horizons_optimization_runs
  (run_id, org_id, project_id, status, created_at, finished_at, artifact_key, error_message)
VALUES
  (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
"#;
        let params = vec![
            ProjectDbParam::String(run_id.to_string()),
            ProjectDbParam::String(org_id.to_string()),
            ProjectDbParam::String(project_id.to_string()),
            ProjectDbParam::String(status.as_str().to_string()),
            ProjectDbParam::String(created_at.to_rfc3339()),
            ProjectDbParam::String(finished_at.map(|d| d.to_rfc3339()).unwrap_or_default()),
            ProjectDbParam::String(artifact_key.unwrap_or_default().to_string()),
            ProjectDbParam::String(error_message.unwrap_or_default().to_string()),
        ];
        self.project_db
            .execute(org_id, project_db, sql, &params)
            .await?;
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn update_row(
        &self,
        org_id: OrgId,
        project_id: ProjectId,
        project_db: &ProjectDbHandle,
        run_id: Uuid,
        status: OptimizationRunStatus,
        finished_at: Option<DateTime<Utc>>,
        artifact_key: Option<&str>,
        error_message: Option<&str>,
    ) -> Result<()> {
        let sql = r#"
UPDATE horizons_optimization_runs
   SET status = ?4,
       finished_at = ?5,
       artifact_key = ?6,
       error_message = ?7
 WHERE org_id = ?1 AND project_id = ?2 AND run_id = ?3
"#;
        let params = vec![
            ProjectDbParam::String(org_id.to_string()),
            ProjectDbParam::String(project_id.to_string()),
            ProjectDbParam::String(run_id.to_string()),
            ProjectDbParam::String(status.as_str().to_string()),
            ProjectDbParam::String(finished_at.map(|d| d.to_rfc3339()).unwrap_or_default()),
            ProjectDbParam::String(artifact_key.unwrap_or_default().to_string()),
            ProjectDbParam::String(error_message.unwrap_or_default().to_string()),
        ];
        self.project_db
            .execute(org_id, project_db, sql, &params)
            .await?;
        Ok(())
    }
}

#[tracing::instrument(level = "debug")]
fn optimization_artifact_key(run_id: Uuid) -> String {
    format!("optimization/runs/{run_id}.json")
}

fn get_string(row: &ProjectDbRow, col: &str) -> Result<String> {
    match row.get(col) {
        Some(ProjectDbValue::String(s)) => Ok(s.clone()),
        Some(v) => Err(Error::BackendMessage(format!(
            "unexpected column type for {col}: {v:?}"
        ))),
        None => Err(Error::BackendMessage(format!("missing column {col}"))),
    }
}

fn get_opt_string(row: &ProjectDbRow, col: &str) -> Result<Option<String>> {
    let s = get_string(row, col)?;
    if s.trim().is_empty() {
        Ok(None)
    } else {
        Ok(Some(s))
    }
}

fn parse_dt_opt(row: &ProjectDbRow, col: &str) -> Result<Option<DateTime<Utc>>> {
    let Some(s) = get_opt_string(row, col)? else {
        return Ok(None);
    };
    let dt = DateTime::parse_from_rfc3339(&s)
        .map(|dt| dt.with_timezone(&Utc))
        .map_err(|e| Error::BackendMessage(format!("invalid datetime in {col}: {e}")))?;
    Ok(Some(dt))
}

fn row_to_run(row: ProjectDbRow) -> Result<OptimizationRunRow> {
    let run_id: Uuid = get_string(&row, "run_id")?
        .parse()
        .map_err(|_| Error::BackendMessage("invalid uuid run_id".to_string()))?;
    let org_id: OrgId = get_string(&row, "org_id")?
        .parse()
        .map_err(|e| Error::InvalidInput(format!("invalid org_id: {e:?}")))?;
    let project_id: ProjectId = get_string(&row, "project_id")?
        .parse()
        .map_err(|e| Error::InvalidInput(format!("invalid project_id: {e:?}")))?;
    let status = OptimizationRunStatus::from_str(&get_string(&row, "status")?)?;
    let created_at_s = get_string(&row, "created_at")?;
    let created_at = DateTime::parse_from_rfc3339(&created_at_s)
        .map(|dt| dt.with_timezone(&Utc))
        .map_err(|e| Error::BackendMessage(format!("invalid datetime in created_at: {e}")))?;

    Ok(OptimizationRunRow {
        run_id,
        org_id,
        project_id,
        status,
        created_at,
        finished_at: parse_dt_opt(&row, "finished_at")?,
        artifact_key: get_opt_string(&row, "artifact_key")?,
        error_message: get_opt_string(&row, "error_message")?,
    })
}
