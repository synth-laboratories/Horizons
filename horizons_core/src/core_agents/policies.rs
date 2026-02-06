use crate::core_agents::models::{ReviewMode, ReviewPolicy, RiskLevel};
use crate::models::ProjectDbHandle;
use crate::onboard::traits::{ProjectDb, ProjectDbParam, ProjectDbRow, ensure_handle_org};
use crate::{Error, Result};
use chrono::{DateTime, Utc};
use std::sync::Arc;

const POLICIES_TABLE_DDL: &str = r#"
CREATE TABLE IF NOT EXISTS horizons_review_policies (
  org_id TEXT NOT NULL,
  action_type TEXT NOT NULL,
  risk_level TEXT NOT NULL,
  review_mode TEXT NOT NULL,
  mcp_scopes_json TEXT NOT NULL,
  ttl_seconds BIGINT NOT NULL,
  updated_at TEXT NOT NULL,
  PRIMARY KEY (org_id, action_type, risk_level)
);
CREATE INDEX IF NOT EXISTS hrp_org_action_idx ON horizons_review_policies (org_id, action_type);
"#;

#[derive(Clone)]
pub struct PolicyStore {
    project_db: Arc<dyn ProjectDb>,
}

impl PolicyStore {
    #[tracing::instrument(level = "debug", skip(project_db))]
    pub fn new(project_db: Arc<dyn ProjectDb>) -> Self {
        Self { project_db }
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn ensure_schema(
        &self,
        org_id: crate::OrgId,
        handle: &ProjectDbHandle,
    ) -> Result<()> {
        ensure_handle_org(handle, org_id)?;
        for stmt in POLICIES_TABLE_DDL
            .split(';')
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            self.project_db.execute(org_id, handle, stmt, &[]).await?;
        }
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn upsert(
        &self,
        org_id: crate::OrgId,
        handle: &ProjectDbHandle,
        policy: &ReviewPolicy,
        now: DateTime<Utc>,
    ) -> Result<()> {
        policy.validate()?;
        self.ensure_schema(org_id, handle).await?;

        let scopes_json = serde_json::to_string(&policy.mcp_scopes)
            .map_err(|e| Error::backend("serialize mcp_scopes", e))?;
        let sql = r#"
INSERT INTO horizons_review_policies
  (org_id, action_type, risk_level, review_mode, mcp_scopes_json, ttl_seconds, updated_at)
VALUES
  (?1, ?2, ?3, ?4, ?5, ?6, ?7)
ON CONFLICT(org_id, action_type, risk_level) DO UPDATE SET
  review_mode = excluded.review_mode,
  mcp_scopes_json = excluded.mcp_scopes_json,
  ttl_seconds = excluded.ttl_seconds,
  updated_at = excluded.updated_at
"#;
        let params = vec![
            ProjectDbParam::String(org_id.to_string()),
            ProjectDbParam::String(policy.action_type.clone()),
            ProjectDbParam::String(risk_to_str(policy.risk_level).to_string()),
            ProjectDbParam::String(mode_to_str(policy.review_mode).to_string()),
            ProjectDbParam::String(scopes_json),
            ProjectDbParam::I64(policy.ttl_seconds as i64),
            ProjectDbParam::String(now.to_rfc3339()),
        ];
        self.project_db
            .execute(org_id, handle, sql, &params)
            .await?;
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn get_policy(
        &self,
        org_id: crate::OrgId,
        handle: &ProjectDbHandle,
        action_type: &str,
        risk: RiskLevel,
    ) -> Result<(ReviewMode, ReviewPolicy)> {
        if action_type.trim().is_empty() {
            return Err(Error::InvalidInput("action_type is empty".to_string()));
        }
        self.ensure_schema(org_id, handle).await?;

        // Most specific: exact action_type, then wildcard "*".
        for at in [action_type, "*"] {
            if let Some(p) = self.get_policy_exact(org_id, handle, at, risk).await? {
                return Ok((p.review_mode, p));
            }
        }

        // Default mapping if nothing configured.
        Ok((
            default_mode_for_risk(risk),
            default_policy_for(action_type, risk),
        ))
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn get_policy_exact(
        &self,
        org_id: crate::OrgId,
        handle: &ProjectDbHandle,
        action_type: &str,
        risk: RiskLevel,
    ) -> Result<Option<ReviewPolicy>> {
        let sql = r#"
SELECT action_type, risk_level, review_mode, mcp_scopes_json, ttl_seconds
  FROM horizons_review_policies
 WHERE org_id = ?1 AND action_type = ?2 AND risk_level = ?3
 LIMIT 1
"#;
        let params = vec![
            ProjectDbParam::String(org_id.to_string()),
            ProjectDbParam::String(action_type.to_string()),
            ProjectDbParam::String(risk_to_str(risk).to_string()),
        ];

        let rows: Vec<ProjectDbRow> = self.project_db.query(org_id, handle, sql, &params).await?;
        if rows.is_empty() {
            return Ok(None);
        }
        let row = &rows[0];

        let action_type = get_string(row, "action_type")?;
        let risk_level_s = get_string(row, "risk_level")?;
        let review_mode_s = get_string(row, "review_mode")?;
        let scopes_json = get_string(row, "mcp_scopes_json")?;
        let ttl = get_i64(row, "ttl_seconds")?;

        let mcp_scopes: Option<Vec<String>> = serde_json::from_str(&scopes_json)
            .map_err(|e| Error::backend("deserialize mcp_scopes", e))?;
        let policy = ReviewPolicy {
            action_type,
            risk_level: risk_from_str(&risk_level_s)?,
            review_mode: mode_from_str(&review_mode_s)?,
            mcp_scopes,
            ttl_seconds: ttl.max(0) as u64,
        };
        policy.validate()?;
        Ok(Some(policy))
    }
}

#[tracing::instrument(level = "debug")]
pub fn default_mode_for_risk(risk: RiskLevel) -> ReviewMode {
    match risk {
        RiskLevel::Low => ReviewMode::Auto,
        RiskLevel::Medium => ReviewMode::Ai,
        RiskLevel::High | RiskLevel::Critical => ReviewMode::Human,
    }
}

#[tracing::instrument(level = "debug")]
pub fn default_policy_for(action_type: &str, risk: RiskLevel) -> ReviewPolicy {
    ReviewPolicy {
        action_type: action_type.to_string(),
        risk_level: risk,
        review_mode: default_mode_for_risk(risk),
        mcp_scopes: None,
        ttl_seconds: 60 * 60,
    }
}

#[tracing::instrument(level = "debug")]
pub fn risk_to_str(r: RiskLevel) -> &'static str {
    match r {
        RiskLevel::Low => "low",
        RiskLevel::Medium => "medium",
        RiskLevel::High => "high",
        RiskLevel::Critical => "critical",
    }
}

#[tracing::instrument(level = "debug")]
pub fn risk_from_str(s: &str) -> Result<RiskLevel> {
    match s {
        "low" => Ok(RiskLevel::Low),
        "medium" => Ok(RiskLevel::Medium),
        "high" => Ok(RiskLevel::High),
        "critical" => Ok(RiskLevel::Critical),
        other => Err(Error::InvalidInput(format!("unknown risk level: {other}"))),
    }
}

#[tracing::instrument(level = "debug")]
pub fn mode_to_str(m: ReviewMode) -> &'static str {
    match m {
        ReviewMode::Auto => "auto",
        ReviewMode::Ai => "ai",
        ReviewMode::Human => "human",
        ReviewMode::McpAuth => "mcp_auth",
    }
}

#[tracing::instrument(level = "debug")]
pub fn mode_from_str(s: &str) -> Result<ReviewMode> {
    match s {
        "auto" => Ok(ReviewMode::Auto),
        "ai" => Ok(ReviewMode::Ai),
        "human" => Ok(ReviewMode::Human),
        "mcp_auth" => Ok(ReviewMode::McpAuth),
        other => Err(Error::InvalidInput(format!("unknown review mode: {other}"))),
    }
}

fn get_string(row: &ProjectDbRow, col: &str) -> Result<String> {
    match row.get(col) {
        Some(crate::onboard::traits::ProjectDbValue::String(s)) => Ok(s.clone()),
        Some(v) => Err(Error::BackendMessage(format!(
            "unexpected column type for {col}: {v:?}"
        ))),
        None => Err(Error::BackendMessage(format!("missing column {col}"))),
    }
}

fn get_i64(row: &ProjectDbRow, col: &str) -> Result<i64> {
    match row.get(col) {
        Some(crate::onboard::traits::ProjectDbValue::I64(v)) => Ok(*v),
        Some(v) => Err(Error::BackendMessage(format!(
            "unexpected column type for {col}: {v:?}"
        ))),
        None => Err(Error::BackendMessage(format!("missing column {col}"))),
    }
}
