use crate::core_agents::approvals as approval_sm;
use crate::core_agents::models::{
    ActionProposal, ActionStatus, AgentContext, AgentRunResult, ReviewMode, ReviewPolicy, RiskLevel,
};
use crate::core_agents::policies::PolicyStore;
use crate::core_agents::traits::{ActionApprover, AgentSpec, CoreAgents, ReviewDecision};
use crate::models::{AgentIdentity, AuditEntry, OrgId, ProjectDbHandle, ProjectId};
use crate::onboard::traits::{
    CentralDb, ProjectDb, ProjectDbParam, ProjectDbRow, ProjectDbValue, ensure_handle_org,
};
use crate::{Error, Result};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use crate::events::models::{Event, EventDirection};
use crate::events::traits::EventBus;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use uuid::Uuid;

const ACTIONS_TABLE_DDL: &str = r#"
CREATE TABLE IF NOT EXISTS horizons_action_proposals (
  id TEXT PRIMARY KEY,
  org_id TEXT NOT NULL,
  project_id TEXT NOT NULL,
  agent_id TEXT NOT NULL,
  action_type TEXT NOT NULL,
  risk_level TEXT NOT NULL,
  review_mode TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  context_json TEXT NOT NULL,
  dedupe_key TEXT NOT NULL,
  status TEXT NOT NULL,
  created_at TEXT NOT NULL,
  expires_at TEXT NOT NULL,
  decided_at TEXT NULL,
  decided_by TEXT NULL,
  decision_reason TEXT NULL,
  execution_result_json TEXT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS hap_org_dedupe_key ON horizons_action_proposals (org_id, dedupe_key);
CREATE INDEX IF NOT EXISTS hap_org_status_created_at ON horizons_action_proposals (org_id, status, created_at);
"#;

#[derive(Clone)]
pub struct CoreAgentsExecutor {
    central_db: Arc<dyn CentralDb>,
    project_db: Arc<dyn ProjectDb>,
    event_bus: Arc<dyn EventBus>,
    policy_store: PolicyStore,
    ai_approver: Option<Arc<dyn ActionApprover>>,
    agents: Arc<RwLock<HashMap<String, Arc<dyn AgentSpec>>>>,
}

impl CoreAgentsExecutor {
    #[tracing::instrument(level = "debug", skip(central_db, project_db, event_bus, ai_approver))]
    pub fn new(
        central_db: Arc<dyn CentralDb>,
        project_db: Arc<dyn ProjectDb>,
        event_bus: Arc<dyn EventBus>,
        ai_approver: Option<Arc<dyn ActionApprover>>,
    ) -> Self {
        let policy_store = PolicyStore::new(project_db.clone());
        Self {
            central_db,
            project_db,
            event_bus,
            policy_store,
            ai_approver,
            agents: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn list_registered_agent_ids(&self) -> Vec<String> {
        let agents = self.agents.read().await;
        let mut ids: Vec<String> = agents.keys().cloned().collect();
        ids.sort();
        ids
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn ensure_schema(&self, org_id: OrgId, handle: &ProjectDbHandle) -> Result<()> {
        ensure_handle_org(handle, org_id)?;
        for stmt in ACTIONS_TABLE_DDL
            .split(';')
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            self.project_db.execute(org_id, handle, stmt, &[]).await?;
        }
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn insert_action(
        &self,
        org_id: OrgId,
        handle: &ProjectDbHandle,
        proposal: &ActionProposal,
        review_mode: ReviewMode,
    ) -> Result<()> {
        self.ensure_schema(org_id, handle).await?;

        let sql = r#"
INSERT INTO horizons_action_proposals
  (id, org_id, project_id, agent_id, action_type, risk_level, review_mode,
   payload_json, context_json, dedupe_key, status, created_at, expires_at,
   decided_at, decided_by, decision_reason, execution_result_json)
VALUES
  (?1, ?2, ?3, ?4, ?5, ?6, ?7,
   ?8, ?9, ?10, ?11, ?12, ?13,
   ?14, ?15, ?16, ?17)
ON CONFLICT(org_id, dedupe_key) DO NOTHING
"#;

        let payload_json = serde_json::to_string(&proposal.payload)
            .map_err(|e| Error::backend("serialize action payload", e))?;
        let context_json = serde_json::to_string(&proposal.context)
            .map_err(|e| Error::backend("serialize action context", e))?;
        let execution_json = proposal
            .execution_result
            .as_ref()
            .map(|v| serde_json::to_string(v))
            .transpose()
            .map_err(|e| Error::backend("serialize execution result", e))?;

        let params = vec![
            ProjectDbParam::String(proposal.id.to_string()),
            ProjectDbParam::String(proposal.org_id.to_string()),
            ProjectDbParam::String(proposal.project_id.to_string()),
            ProjectDbParam::String(proposal.agent_id.clone()),
            ProjectDbParam::String(proposal.action_type.clone()),
            ProjectDbParam::String(risk_to_str(proposal.risk_level).to_string()),
            ProjectDbParam::String(mode_to_str(review_mode).to_string()),
            ProjectDbParam::String(payload_json),
            ProjectDbParam::String(context_json),
            ProjectDbParam::String(proposal.dedupe_key.clone()),
            ProjectDbParam::String(status_to_str(proposal.status).to_string()),
            ProjectDbParam::String(proposal.created_at.to_rfc3339()),
            ProjectDbParam::String(proposal.expires_at.to_rfc3339()),
            ProjectDbParam::String(
                proposal
                    .decided_at
                    .map(|d| d.to_rfc3339())
                    .unwrap_or_default(),
            ),
            ProjectDbParam::String(proposal.decided_by.clone().unwrap_or_default()),
            ProjectDbParam::String(proposal.decision_reason.clone().unwrap_or_default()),
            ProjectDbParam::String(execution_json.unwrap_or_default()),
        ];

        self.project_db
            .execute(org_id, handle, sql, &params)
            .await?;
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn update_action(
        &self,
        org_id: OrgId,
        handle: &ProjectDbHandle,
        p: &ActionProposal,
    ) -> Result<()> {
        self.ensure_schema(org_id, handle).await?;
        let sql = r#"
UPDATE horizons_action_proposals
   SET status = ?3,
       decided_at = ?4,
       decided_by = ?5,
       decision_reason = ?6,
       execution_result_json = ?7
 WHERE org_id = ?1 AND id = ?2
"#;

        let decided_at = p.decided_at.map(|d| d.to_rfc3339()).unwrap_or_default();
        let decided_by = p.decided_by.clone().unwrap_or_default();
        let decision_reason = p.decision_reason.clone().unwrap_or_default();
        let execution_json = p
            .execution_result
            .as_ref()
            .map(|v| serde_json::to_string(v))
            .transpose()
            .map_err(|e| Error::backend("serialize execution result", e))?
            .unwrap_or_default();

        let params = vec![
            ProjectDbParam::String(org_id.to_string()),
            ProjectDbParam::String(p.id.to_string()),
            ProjectDbParam::String(status_to_str(p.status).to_string()),
            ProjectDbParam::String(decided_at),
            ProjectDbParam::String(decided_by),
            ProjectDbParam::String(decision_reason),
            ProjectDbParam::String(execution_json),
        ];
        self.project_db
            .execute(org_id, handle, sql, &params)
            .await?;
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn get_action(
        &self,
        org_id: OrgId,
        handle: &ProjectDbHandle,
        action_id: Uuid,
    ) -> Result<Option<(ActionProposal, ReviewMode)>> {
        self.ensure_schema(org_id, handle).await?;
        let sql = r#"
SELECT id, org_id, project_id, agent_id, action_type, risk_level, review_mode,
       payload_json, context_json, dedupe_key, status, created_at, expires_at,
       decided_at, decided_by, decision_reason, execution_result_json
  FROM horizons_action_proposals
 WHERE org_id = ?1 AND id = ?2
 LIMIT 1
"#;
        let params = vec![
            ProjectDbParam::String(org_id.to_string()),
            ProjectDbParam::String(action_id.to_string()),
        ];
        let rows = self.project_db.query(org_id, handle, sql, &params).await?;
        if rows.is_empty() {
            return Ok(None);
        }
        let row = &rows[0];
        let review_mode = mode_from_str(&get_string(row, "review_mode")?)?;
        let p = action_from_row(row)?;
        Ok(Some((p, review_mode)))
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn publish_action_event(
        &self,
        p: &ActionProposal,
        topic: &str,
        status: &str,
        reason: Option<&str>,
    ) -> Result<String> {
        let payload = serde_json::json!({
            "action_id": p.id.to_string(),
            "action_type": p.action_type,
            "agent_id": p.agent_id,
            "org_id": p.org_id.to_string(),
            "project_id": p.project_id.to_string(),
            "status": status,
            "reason": reason,
            "payload": p.payload,
            "context": p.context,
        });

        let ev = Event::new(
            p.org_id.to_string(),
            Some(p.project_id.to_string()),
            EventDirection::Outbound,
            topic,
            format!("agent:{}", p.agent_id),
            payload,
            format!("action:{}:{}", p.action_type, p.dedupe_key),
            serde_json::json!({}),
            None,
        )
        .map_err(|e| Error::InvalidInput(format!("invalid action event: {e}")))?;

        self.event_bus
            .publish(ev)
            .await
            .map_err(|e| Error::backend("publish action event", e))
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn resolve_policy(
        &self,
        org_id: OrgId,
        project_db: &ProjectDbHandle,
        action_type: &str,
        risk: RiskLevel,
    ) -> Result<(ReviewMode, ReviewPolicy)> {
        let (mode, mut p) = self
            .policy_store
            .get_policy(org_id, project_db, action_type, risk)
            .await?;

        // Safety: if AI review isn't configured, treat AI review as Human.
        if mode == ReviewMode::Ai && self.ai_approver.is_none() {
            p.review_mode = ReviewMode::Human;
            return Ok((ReviewMode::Human, p));
        }
        Ok((mode, p))
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn audit(&self, entry: AuditEntry) -> Result<()> {
        self.central_db.append_audit_entry(&entry).await
    }
}

#[async_trait]
impl CoreAgents for CoreAgentsExecutor {
    #[tracing::instrument(level = "debug", skip_all)]
    async fn register_agent(&self, agent: Arc<dyn AgentSpec>) -> Result<()> {
        let id = agent.id().await;
        if id.trim().is_empty() {
            return Err(Error::InvalidInput("agent id is empty".to_string()));
        }
        let mut agents = self.agents.write().await;
        agents.insert(id, agent);
        Ok(())
    }

    #[tracing::instrument(level = "info", skip_all)]
    async fn run(
        &self,
        org_id: OrgId,
        project_id: ProjectId,
        project_db: ProjectDbHandle,
        agent_id: &str,
        identity: &AgentIdentity,
        inputs: Option<serde_json::Value>,
    ) -> Result<AgentRunResult> {
        if agent_id.trim().is_empty() {
            return Err(Error::InvalidInput("agent_id is empty".to_string()));
        }
        ensure_handle_org(&project_db, org_id)?;
        if project_db.project_id != project_id {
            return Err(Error::InvalidInput(
                "project_db handle project_id mismatch".to_string(),
            ));
        }

        let agent = {
            let agents = self.agents.read().await;
            agents
                .get(agent_id)
                .cloned()
                .ok_or_else(|| Error::NotFound(format!("agent not registered: {agent_id}")))?
        };

        let started_at = Utc::now();
        let ctx = AgentContext {
            org_id,
            project_id,
            identity: identity.clone(),
            project_db: project_db.clone(),
        };

        let proposals = agent.run(ctx, inputs).await?;

        let mut proposed_action_ids = Vec::with_capacity(proposals.len());
        for p in proposals {
            let id = self.propose_action(p, identity).await?;
            proposed_action_ids.push(id);
        }

        let finished_at = Utc::now();
        let run = AgentRunResult {
            run_id: Uuid::new_v4(),
            org_id,
            project_id,
            agent_id: agent_id.to_string(),
            started_at,
            finished_at,
            proposed_action_ids,
        };

        self.audit(AuditEntry {
            id: Uuid::new_v4(),
            org_id,
            project_id: Some(project_id),
            actor: identity.clone(),
            action: "core_agents.run".to_string(),
            payload: serde_json::to_value(&run).map_err(|e| Error::backend("serialize run", e))?,
            outcome: "ok".to_string(),
            created_at: finished_at,
        })
        .await?;

        Ok(run)
    }

    #[tracing::instrument(level = "info", skip_all)]
    async fn propose_action(
        &self,
        mut proposal: ActionProposal,
        identity: &AgentIdentity,
    ) -> Result<Uuid> {
        // Determine policy and set TTL accordingly.
        let (mode, policy) = self
            .resolve_policy(
                proposal.org_id,
                &proposal_project_db_handle(&proposal)?,
                &proposal.action_type,
                proposal.risk_level,
            )
            .await?;

        // Update expires_at based on policy TTL (policy is authoritative).
        proposal.expires_at =
            proposal.created_at + chrono::Duration::seconds(policy.ttl_seconds as i64);

        // Persist proposed action.
        let project_db = proposal_project_db_handle(&proposal)?;
        self.insert_action(proposal.org_id, &project_db, &proposal, mode)
            .await?;

        self.audit(AuditEntry {
            id: Uuid::new_v4(),
            org_id: proposal.org_id,
            project_id: Some(proposal.project_id),
            actor: identity.clone(),
            action: "core_agents.action.proposed".to_string(),
            payload: serde_json::to_value(&proposal)
                .map_err(|e| Error::backend("serialize proposal", e))?,
            outcome: "ok".to_string(),
            created_at: Utc::now(),
        })
        .await?;

        match mode {
            ReviewMode::Auto => {
                // Auto-approve and execute immediately.
                self.approve(
                    proposal.org_id,
                    proposal.project_id,
                    &project_db,
                    proposal.id,
                    "auto",
                    "auto-approved",
                )
                .await?;
            }
            ReviewMode::Ai => {
                // AI review if configured, else policy resolution already downgraded to Human.
                let Some(approver) = self.ai_approver.clone() else {
                    return Ok(proposal.id);
                };
                let decision = approver.review(&policy, &proposal, identity).await?;
                match decision {
                    ReviewDecision::Approved { reason } => {
                        self.approve(
                            proposal.org_id,
                            proposal.project_id,
                            &project_db,
                            proposal.id,
                            approver.name(),
                            &reason,
                        )
                        .await?;
                    }
                    ReviewDecision::Denied { reason } => {
                        self.deny(
                            proposal.org_id,
                            proposal.project_id,
                            &project_db,
                            proposal.id,
                            approver.name(),
                            &reason,
                        )
                        .await?;
                    }
                }
            }
            ReviewMode::Human | ReviewMode::McpAuth => {
                // Pending until explicit approval/denial.
            }
        }

        Ok(proposal.id)
    }

    #[tracing::instrument(level = "info", skip_all)]
    async fn approve(
        &self,
        org_id: OrgId,
        project_id: ProjectId,
        project_db: &ProjectDbHandle,
        action_id: Uuid,
        approver_id: &str,
        reason: &str,
    ) -> Result<()> {
        ensure_handle_org(project_db, org_id)?;
        if project_db.project_id != project_id {
            return Err(Error::InvalidInput("project_id mismatch".to_string()));
        }

        let Some((p0, _mode)) = self.get_action(org_id, project_db, action_id).await? else {
            return Err(Error::NotFound(format!("action not found: {action_id}")));
        };
        if p0.project_id != project_id {
            return Err(Error::Unauthorized("action project mismatch".to_string()));
        }

        let now = Utc::now();
        let mut p = approval_sm::approve(p0, approver_id, reason, now)?;
        self.update_action(org_id, project_db, &p).await?;

        // "Execute" means routing outward via Event Sync.
        let topic = format!("action.{}.approved", p.action_type);
        let event_id = self
            .publish_action_event(&p, &topic, "approved", Some(reason))
            .await?;

        p = approval_sm::mark_executed(p, serde_json::json!({ "event_id": event_id }), Utc::now())?;
        self.update_action(org_id, project_db, &p).await?;

        self.audit(AuditEntry {
            id: Uuid::new_v4(),
            org_id,
            project_id: Some(project_id),
            actor: AgentIdentity::System {
                name: "core_agents".to_string(),
            },
            action: "core_agents.action.approved".to_string(),
            payload: serde_json::to_value(&p)
                .map_err(|e| Error::backend("serialize approved action", e))?,
            outcome: "ok".to_string(),
            created_at: Utc::now(),
        })
        .await?;

        Ok(())
    }

    #[tracing::instrument(level = "info", skip_all)]
    async fn deny(
        &self,
        org_id: OrgId,
        project_id: ProjectId,
        project_db: &ProjectDbHandle,
        action_id: Uuid,
        approver_id: &str,
        reason: &str,
    ) -> Result<()> {
        ensure_handle_org(project_db, org_id)?;
        if project_db.project_id != project_id {
            return Err(Error::InvalidInput("project_id mismatch".to_string()));
        }

        let Some((p0, _mode)) = self.get_action(org_id, project_db, action_id).await? else {
            return Err(Error::NotFound(format!("action not found: {action_id}")));
        };
        if p0.project_id != project_id {
            return Err(Error::Unauthorized("action project mismatch".to_string()));
        }

        let now = Utc::now();
        let p = approval_sm::deny(p0, approver_id, reason, now)?;
        self.update_action(org_id, project_db, &p).await?;

        let topic = format!("action.{}.denied", p.action_type);
        let _ = self
            .publish_action_event(&p, &topic, "denied", Some(reason))
            .await?;

        self.audit(AuditEntry {
            id: Uuid::new_v4(),
            org_id,
            project_id: Some(project_id),
            actor: AgentIdentity::System {
                name: "core_agents".to_string(),
            },
            action: "core_agents.action.denied".to_string(),
            payload: serde_json::to_value(&p)
                .map_err(|e| Error::backend("serialize denied action", e))?,
            outcome: "ok".to_string(),
            created_at: Utc::now(),
        })
        .await?;

        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn list_pending(
        &self,
        org_id: OrgId,
        project_id: ProjectId,
        project_db: &ProjectDbHandle,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<ActionProposal>> {
        if limit == 0 {
            return Err(Error::InvalidInput("limit must be > 0".to_string()));
        }
        ensure_handle_org(project_db, org_id)?;
        if project_db.project_id != project_id {
            return Err(Error::InvalidInput("project_id mismatch".to_string()));
        }
        self.ensure_schema(org_id, project_db).await?;

        let sql = r#"
SELECT id, org_id, project_id, agent_id, action_type, risk_level, review_mode,
       payload_json, context_json, dedupe_key, status, created_at, expires_at,
       decided_at, decided_by, decision_reason, execution_result_json
  FROM horizons_action_proposals
 WHERE org_id = ?1 AND project_id = ?2 AND status = 'proposed'
 ORDER BY created_at ASC
 LIMIT ?3 OFFSET ?4
"#;
        let params = vec![
            ProjectDbParam::String(org_id.to_string()),
            ProjectDbParam::String(project_id.to_string()),
            ProjectDbParam::I64(limit as i64),
            ProjectDbParam::I64(offset as i64),
        ];
        let rows: Vec<ProjectDbRow> = self
            .project_db
            .query(org_id, project_db, sql, &params)
            .await?;
        let mut out = Vec::with_capacity(rows.len());
        for r in rows {
            out.push(action_from_row(&r)?);
        }
        Ok(out)
    }

    #[tracing::instrument(level = "info", skip_all)]
    async fn upsert_policy(
        &self,
        org_id: OrgId,
        project_id: ProjectId,
        project_db: &ProjectDbHandle,
        policy: ReviewPolicy,
        identity: &AgentIdentity,
    ) -> Result<()> {
        ensure_handle_org(project_db, org_id)?;
        if project_db.project_id != project_id {
            return Err(Error::InvalidInput("project_id mismatch".to_string()));
        }
        let now = Utc::now();
        self.policy_store
            .upsert(org_id, project_db, &policy, now)
            .await?;

        self.audit(AuditEntry {
            id: Uuid::new_v4(),
            org_id,
            project_id: Some(project_id),
            actor: identity.clone(),
            action: "core_agents.policy.upsert".to_string(),
            payload: serde_json::to_value(&policy)
                .map_err(|e| Error::backend("serialize policy", e))?,
            outcome: "ok".to_string(),
            created_at: now,
        })
        .await?;

        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn get_policy(
        &self,
        org_id: OrgId,
        project_id: ProjectId,
        project_db: &ProjectDbHandle,
        action_type: &str,
        risk: RiskLevel,
    ) -> Result<(ReviewMode, ReviewPolicy)> {
        ensure_handle_org(project_db, org_id)?;
        if project_db.project_id != project_id {
            return Err(Error::InvalidInput("project_id mismatch".to_string()));
        }
        self.policy_store
            .get_policy(org_id, project_db, action_type, risk)
            .await
    }
}

fn proposal_project_db_handle(p: &ActionProposal) -> Result<ProjectDbHandle> {
    // Phase D2 assumes the caller provides a valid handle. The executor uses the handle passed into
    // run()/approve()/deny()/list_pending(). For propose_action() we require the proposal to carry
    // the handle's org/project IDs only, not the connection itself.
    //
    // In practice, propose_action() is called from `run()` where the handle exists; we persist the
    // handle in context and pass it in through the proposal in `context_json` if desired.
    //
    // For v0.0.x we store actions in the *current* project DB handle via the API surface; therefore
    // propose_action() requires the project DB handle to be embedded by the caller using `context`.
    let h = p
        .context
        .get("_project_db_handle")
        .cloned()
        .ok_or_else(|| {
            Error::InvalidInput("proposal.context missing _project_db_handle".to_string())
        })?;
    let handle: ProjectDbHandle = serde_json::from_value(h)
        .map_err(|e| Error::backend("deserialize _project_db_handle", e))?;
    if handle.org_id != p.org_id || handle.project_id != p.project_id {
        return Err(Error::InvalidInput(
            "_project_db_handle org/project mismatch".to_string(),
        ));
    }
    Ok(handle)
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

fn parse_dt(s: &str, col: &str) -> Result<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(s)
        .map(|dt| dt.with_timezone(&Utc))
        .map_err(|e| Error::BackendMessage(format!("invalid datetime in {col}: {e}")))
}

fn action_from_row(row: &ProjectDbRow) -> Result<ActionProposal> {
    let id: Uuid = get_string(row, "id")?
        .parse()
        .map_err(|_| Error::BackendMessage("invalid uuid id".to_string()))?;
    let org_id: OrgId = get_string(row, "org_id")?
        .parse()
        .map_err(|e| Error::InvalidInput(format!("invalid org_id: {e:?}")))?;
    let project_id: ProjectId = get_string(row, "project_id")?
        .parse()
        .map_err(|e| Error::InvalidInput(format!("invalid project_id: {e:?}")))?;
    let agent_id = get_string(row, "agent_id")?;
    let action_type = get_string(row, "action_type")?;
    let risk = risk_from_str(&get_string(row, "risk_level")?)?;
    let dedupe_key = get_string(row, "dedupe_key")?;
    let status = status_from_str(&get_string(row, "status")?)?;

    let payload_json = get_string(row, "payload_json")?;
    let payload: serde_json::Value = serde_json::from_str(&payload_json)
        .map_err(|e| Error::backend("deserialize payload_json", e))?;

    let context_json = get_string(row, "context_json")?;
    let context: serde_json::Value = serde_json::from_str(&context_json)
        .map_err(|e| Error::backend("deserialize context_json", e))?;

    let created_at = parse_dt(&get_string(row, "created_at")?, "created_at")?;
    let expires_at = parse_dt(&get_string(row, "expires_at")?, "expires_at")?;

    let decided_at = match row.get("decided_at") {
        Some(ProjectDbValue::String(s)) if !s.trim().is_empty() => Some(parse_dt(s, "decided_at")?),
        _ => None,
    };
    let decided_by = match row.get("decided_by") {
        Some(ProjectDbValue::String(s)) if !s.trim().is_empty() => Some(s.clone()),
        _ => None,
    };
    let decision_reason = match row.get("decision_reason") {
        Some(ProjectDbValue::String(s)) if !s.trim().is_empty() => Some(s.clone()),
        _ => None,
    };
    let execution_result = match row.get("execution_result_json") {
        Some(ProjectDbValue::String(s)) if !s.trim().is_empty() => {
            let v: serde_json::Value = serde_json::from_str(s)
                .map_err(|e| Error::backend("deserialize execution_result_json", e))?;
            Some(v)
        }
        _ => None,
    };

    Ok(ActionProposal {
        id,
        org_id,
        project_id,
        agent_id,
        action_type,
        payload,
        risk_level: risk,
        dedupe_key,
        context,
        status,
        created_at,
        decided_at,
        decided_by,
        decision_reason,
        expires_at,
        execution_result,
    })
}

fn risk_to_str(r: RiskLevel) -> &'static str {
    crate::core_agents::policies::risk_to_str(r)
}

fn risk_from_str(s: &str) -> Result<RiskLevel> {
    crate::core_agents::policies::risk_from_str(s)
}

fn mode_to_str(m: ReviewMode) -> &'static str {
    crate::core_agents::policies::mode_to_str(m)
}

fn mode_from_str(s: &str) -> Result<ReviewMode> {
    crate::core_agents::policies::mode_from_str(s)
}

fn status_to_str(s: ActionStatus) -> &'static str {
    match s {
        ActionStatus::Proposed => "proposed",
        ActionStatus::Approved => "approved",
        ActionStatus::Denied => "denied",
        ActionStatus::Executed => "executed",
        ActionStatus::Expired => "expired",
    }
}

fn status_from_str(s: &str) -> Result<ActionStatus> {
    match s {
        "proposed" => Ok(ActionStatus::Proposed),
        "approved" => Ok(ActionStatus::Approved),
        "denied" => Ok(ActionStatus::Denied),
        "executed" => Ok(ActionStatus::Executed),
        "expired" => Ok(ActionStatus::Expired),
        other => Err(Error::InvalidInput(format!(
            "unknown action status: {other}"
        ))),
    }
}
