use super::traits::{EvalReport, Evaluator, VerificationCase};
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

const REPORTS_TABLE_DDL: &str = r#"
CREATE TABLE IF NOT EXISTS horizons_eval_reports (
  report_id TEXT PRIMARY KEY,
  org_id TEXT NOT NULL,
  project_id TEXT NOT NULL,
  created_at TEXT NOT NULL,
  artifact_key TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS her_org_proj_created
  ON horizons_eval_reports(org_id, project_id, created_at);
"#;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvalReportRow {
    pub report_id: Uuid,
    pub org_id: OrgId,
    pub project_id: ProjectId,
    pub created_at: DateTime<Utc>,
    pub artifact_key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvalArtifact {
    pub report_id: Uuid,
    pub org_id: OrgId,
    pub project_id: ProjectId,
    pub created_at: DateTime<Utc>,
    pub identity: AgentIdentity,
    pub case: VerificationCase,
    pub report: EvalReport,
}

#[derive(Clone)]
pub struct EvaluationEngine {
    central_db: Arc<dyn CentralDb>,
    project_db: Arc<dyn ProjectDb>,
    filestore: Arc<dyn Filestore>,
    evaluator: Arc<dyn Evaluator>,
}

impl EvaluationEngine {
    #[tracing::instrument(level = "debug", skip(central_db, project_db, filestore, evaluator))]
    pub fn new(
        central_db: Arc<dyn CentralDb>,
        project_db: Arc<dyn ProjectDb>,
        filestore: Arc<dyn Filestore>,
        evaluator: Arc<dyn Evaluator>,
    ) -> Self {
        Self {
            central_db,
            project_db,
            filestore,
            evaluator,
        }
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn ensure_schema(&self, org_id: OrgId, handle: &ProjectDbHandle) -> Result<()> {
        ensure_handle_org(handle, org_id)?;
        for stmt in REPORTS_TABLE_DDL
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
        case: VerificationCase,
    ) -> Result<EvalReportRow> {
        ensure_handle_org(project_db, org_id)?;
        if project_db.project_id != project_id {
            return Err(Error::InvalidInput("project_id mismatch".to_string()));
        }
        self.ensure_schema(org_id, project_db).await?;

        let report_id = Uuid::new_v4();
        let created_at = Utc::now();

        let report = self.evaluator.verify_report(case.clone()).await?;
        let artifact = EvalArtifact {
            report_id,
            org_id,
            project_id,
            created_at,
            identity: identity.clone(),
            case: case.clone(),
            report,
        };

        let artifact_key = eval_artifact_key(report_id);
        let bytes = serde_json::to_vec(&artifact)
            .map_err(|e| Error::backend("serialize eval artifact", e))?;
        self.filestore
            .put(org_id, Some(project_id), &artifact_key, Bytes::from(bytes))
            .await?;

        self.insert_row(
            org_id,
            project_id,
            project_db,
            report_id,
            created_at,
            &artifact_key,
        )
        .await?;

        self.central_db
            .append_audit_entry(&AuditEntry {
                id: Uuid::new_v4(),
                org_id,
                project_id: Some(project_id),
                actor: identity.clone(),
                action: "evaluation.run".to_string(),
                payload: serde_json::json!({
                    "report_id": report_id.to_string(),
                    "artifact_key": artifact_key,
                }),
                outcome: "ok".to_string(),
                created_at,
            })
            .await?;

        Ok(EvalReportRow {
            report_id,
            org_id,
            project_id,
            created_at,
            artifact_key,
        })
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn list_reports(
        &self,
        org_id: OrgId,
        project_id: ProjectId,
        project_db: &ProjectDbHandle,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<EvalReportRow>> {
        if limit == 0 {
            return Err(Error::InvalidInput("limit must be > 0".to_string()));
        }
        ensure_handle_org(project_db, org_id)?;
        if project_db.project_id != project_id {
            return Err(Error::InvalidInput("project_id mismatch".to_string()));
        }
        self.ensure_schema(org_id, project_db).await?;

        let sql = r#"
SELECT report_id, org_id, project_id, created_at, artifact_key
  FROM horizons_eval_reports
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
        rows.into_iter().map(row_to_report_row).collect()
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn get_report(
        &self,
        org_id: OrgId,
        project_id: ProjectId,
        _project_db: &ProjectDbHandle,
        report_id: Uuid,
    ) -> Result<EvalArtifact> {
        let key = eval_artifact_key(report_id);
        let Some(bytes) = self.filestore.get(org_id, Some(project_id), &key).await? else {
            return Err(Error::NotFound("eval artifact missing".to_string()));
        };
        let artifact: EvalArtifact = serde_json::from_slice(&bytes)
            .map_err(|e| Error::backend("deserialize eval artifact", e))?;
        if artifact.org_id != org_id || artifact.project_id != project_id {
            return Err(Error::Unauthorized(
                "eval artifact org/project mismatch".to_string(),
            ));
        }
        Ok(artifact)
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn insert_row(
        &self,
        org_id: OrgId,
        project_id: ProjectId,
        project_db: &ProjectDbHandle,
        report_id: Uuid,
        created_at: DateTime<Utc>,
        artifact_key: &str,
    ) -> Result<()> {
        let sql = r#"
INSERT INTO horizons_eval_reports
  (report_id, org_id, project_id, created_at, artifact_key)
VALUES
  (?1, ?2, ?3, ?4, ?5)
"#;
        let params = vec![
            ProjectDbParam::String(report_id.to_string()),
            ProjectDbParam::String(org_id.to_string()),
            ProjectDbParam::String(project_id.to_string()),
            ProjectDbParam::String(created_at.to_rfc3339()),
            ProjectDbParam::String(artifact_key.to_string()),
        ];
        self.project_db
            .execute(org_id, project_db, sql, &params)
            .await?;
        Ok(())
    }
}

#[tracing::instrument(level = "debug")]
fn eval_artifact_key(report_id: Uuid) -> String {
    format!("eval/reports/{report_id}.json")
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

fn row_to_report_row(row: ProjectDbRow) -> Result<EvalReportRow> {
    let report_id: Uuid = get_string(&row, "report_id")?
        .parse()
        .map_err(|_| Error::BackendMessage("invalid uuid report_id".to_string()))?;
    let org_id: OrgId = get_string(&row, "org_id")?
        .parse()
        .map_err(|e| Error::InvalidInput(format!("invalid org_id: {e:?}")))?;
    let project_id: ProjectId = get_string(&row, "project_id")?
        .parse()
        .map_err(|e| Error::InvalidInput(format!("invalid project_id: {e:?}")))?;
    let created_at_s = get_string(&row, "created_at")?;
    let created_at = DateTime::parse_from_rfc3339(&created_at_s)
        .map(|dt| dt.with_timezone(&Utc))
        .map_err(|e| Error::BackendMessage(format!("invalid datetime in created_at: {e}")))?;
    let artifact_key = get_string(&row, "artifact_key")?;
    Ok(EvalReportRow {
        report_id,
        org_id,
        project_id,
        created_at,
        artifact_key,
    })
}
