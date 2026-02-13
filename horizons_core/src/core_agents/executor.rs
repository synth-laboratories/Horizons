use crate::core_agents::approvals as approval_sm;
use crate::core_agents::mcp::McpScopeProvider;
use crate::core_agents::models::{
    ActionProposal, ActionStatus, AgentContext, AgentRunResult, ReviewMode, ReviewPolicy, RiskLevel,
};
use crate::core_agents::policies::PolicyStore;
use crate::core_agents::traits::{ActionApprover, AgentSpec, CoreAgents, ReviewDecision};
use crate::events::models::{Event, EventDirection};
use crate::events::traits::EventBus;
use crate::models::{AgentIdentity, AuditEntry, OrgId, ProjectDbHandle, ProjectId};
use crate::onboard::secrets::CredentialManager;
use crate::onboard::traits::{
    CentralDb, ProjectDb, ProjectDbParam, ProjectDbRow, ProjectDbValue, ensure_handle_org,
};
use crate::{Error, Result};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
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
  dedupe_key TEXT NULL,
  status TEXT NOT NULL,
  created_at TEXT NOT NULL,
  expires_at TEXT NOT NULL,
  decided_at TEXT NULL,
  decided_by TEXT NULL,
  decision_reason TEXT NULL,
  execution_result_json TEXT NULL
);
-- NOTE: SQLite's `ON CONFLICT(col, ...)` only matches *non-partial* UNIQUE constraints/indexes.
-- A plain UNIQUE index allows multiple NULLs anyway, so we don't need a partial index here.
DROP INDEX IF EXISTS hap_org_dedupe_key;
CREATE UNIQUE INDEX IF NOT EXISTS hap_org_dedupe_key
  ON horizons_action_proposals (org_id, dedupe_key);
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
    credential_manager: Option<Arc<CredentialManager>>,
    mcp_scope_provider: Option<Arc<dyn McpScopeProvider>>,
    ensured_action_schema: Arc<Mutex<std::collections::HashSet<String>>>,
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
            credential_manager: None,
            mcp_scope_provider: None,
            ensured_action_schema: Arc::new(Mutex::new(std::collections::HashSet::new())),
        }
    }

    /// Set the credential manager for injecting credentials into `AgentContext`.
    pub fn with_credential_manager(mut self, cm: Arc<CredentialManager>) -> Self {
        self.credential_manager = Some(cm);
        self
    }

    /// Set the MCP scope provider used for `ReviewMode::McpAuth` gates.
    pub fn with_mcp_scope_provider(mut self, p: Arc<dyn McpScopeProvider>) -> Self {
        self.mcp_scope_provider = Some(p);
        self
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
        let key = format!("{org_id}:{}", handle.project_id);
        {
            let ensured = self.ensured_action_schema.lock().await;
            if ensured.contains(&key) {
                return Ok(());
            }
        }

        // Serialize DDL (especially DROP INDEX) to avoid `SQLITE_BUSY`/locking failures under
        // concurrent readers/writers.
        let mut ensured = self.ensured_action_schema.lock().await;
        if ensured.contains(&key) {
            return Ok(());
        }
        for stmt in ACTIONS_TABLE_DDL
            .split(';')
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            self.project_db.execute(org_id, handle, stmt, &[]).await?;
        }
        ensured.insert(key);
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn insert_action(
        &self,
        org_id: OrgId,
        handle: &ProjectDbHandle,
        proposal: &ActionProposal,
        review_mode: ReviewMode,
    ) -> Result<(ActionProposal, ReviewMode, bool)> {
        self.ensure_schema(org_id, handle).await?;

        // If the caller didn't provide an idempotency key, generate one so we can still
        // store + query the row consistently. This keeps "no dedupe_key" opt-in semantics:
        // a new generated key means each call inserts a distinct row.
        let dedupe_key = proposal
            .dedupe_key
            .clone()
            .unwrap_or_else(|| format!("auto:{}", Uuid::new_v4()));
        let mut proposal_to_insert = proposal.clone();
        proposal_to_insert.dedupe_key = Some(dedupe_key.clone());

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

        let payload_json = serde_json::to_string(&proposal_to_insert.payload)
            .map_err(|e| Error::backend("serialize action payload", e))?;
        let context_json = serde_json::to_string(&proposal_to_insert.context)
            .map_err(|e| Error::backend("serialize action context", e))?;
        let execution_json = proposal_to_insert
            .execution_result
            .as_ref()
            .map(|v| serde_json::to_string(v))
            .transpose()
            .map_err(|e| Error::backend("serialize execution result", e))?;

        let params = vec![
            ProjectDbParam::String(proposal_to_insert.id.to_string()),
            ProjectDbParam::String(proposal_to_insert.org_id.to_string()),
            ProjectDbParam::String(proposal_to_insert.project_id.to_string()),
            ProjectDbParam::String(proposal_to_insert.agent_id.clone()),
            ProjectDbParam::String(proposal_to_insert.action_type.clone()),
            ProjectDbParam::String(risk_to_str(proposal_to_insert.risk_level).to_string()),
            ProjectDbParam::String(mode_to_str(review_mode).to_string()),
            ProjectDbParam::String(payload_json),
            ProjectDbParam::String(context_json),
            ProjectDbParam::String(dedupe_key.clone()),
            ProjectDbParam::String(status_to_str(proposal_to_insert.status).to_string()),
            ProjectDbParam::String(proposal_to_insert.created_at.to_rfc3339()),
            ProjectDbParam::String(proposal_to_insert.expires_at.to_rfc3339()),
            proposal_to_insert
                .decided_at
                .map(|d| ProjectDbParam::String(d.to_rfc3339()))
                .unwrap_or(ProjectDbParam::Null),
            proposal_to_insert
                .decided_by
                .clone()
                .map(ProjectDbParam::String)
                .unwrap_or(ProjectDbParam::Null),
            proposal_to_insert
                .decision_reason
                .clone()
                .map(ProjectDbParam::String)
                .unwrap_or(ProjectDbParam::Null),
            execution_json
                .map(ProjectDbParam::String)
                .unwrap_or(ProjectDbParam::Null),
        ];

        let rows = self
            .project_db
            .execute(org_id, handle, sql, &params)
            .await?;

        // Idempotency: if we hit the unique constraint, return the existing row and
        // ensure downstream logic doesn't trigger side effects twice.
        if rows == 0 {
            let Some(existing) = self
                .get_action_by_dedupe_key(org_id, handle, &dedupe_key)
                .await?
            else {
                return Err(Error::BackendMessage(
                    "action proposal dedupe_key conflict but existing row not found".to_string(),
                ));
            };
            return Ok((existing.0, existing.1, false));
        }

        Ok((proposal_to_insert, review_mode, true))
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
    async fn get_action_by_dedupe_key(
        &self,
        org_id: OrgId,
        handle: &ProjectDbHandle,
        dedupe_key: &str,
    ) -> Result<Option<(ActionProposal, ReviewMode)>> {
        self.ensure_schema(org_id, handle).await?;
        let sql = r#"
SELECT id, org_id, project_id, agent_id, action_type, risk_level, review_mode,
       payload_json, context_json, dedupe_key, status, created_at, expires_at,
       decided_at, decided_by, decision_reason, execution_result_json
  FROM horizons_action_proposals
 WHERE org_id = ?1 AND dedupe_key = ?2
 LIMIT 1
"#;
        let params = vec![
            ProjectDbParam::String(org_id.to_string()),
            ProjectDbParam::String(dedupe_key.to_string()),
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
            format!(
                "action:{}:{}",
                p.action_type,
                p.dedupe_key.clone().unwrap_or_else(|| p.id.to_string())
            ),
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

    /// Enforce `ReviewMode::McpAuth` as a real gate.
    ///
    /// Fail closed if:
    /// - the policy does not specify required MCP scopes
    /// - no scope provider is configured
    /// - the effective principal lacks required scopes
    ///
    /// Always emits an audit entry describing the check and outcome.
    #[tracing::instrument(level = "info", skip_all)]
    async fn enforce_mcp_auth_gate(
        &self,
        org_id: OrgId,
        project_id: ProjectId,
        project_db: &ProjectDbHandle,
        proposal: &ActionProposal,
        identity: &AgentIdentity,
    ) -> Result<()> {
        let (_mode, policy) = self
            .policy_store
            .get_policy(
                org_id,
                project_db,
                &proposal.action_type,
                proposal.risk_level,
            )
            .await?;

        let required_scopes = policy.mcp_scopes.clone().unwrap_or_default();

        let mut available_scopes: Vec<String> = Vec::new();
        let mut missing_scopes: Vec<String> = Vec::new();

        let failure_reason: Option<String> = if required_scopes.is_empty() {
            Some("mcp_auth policy missing required mcp_scopes".to_string())
        } else {
            match &self.mcp_scope_provider {
                None => {
                    missing_scopes = required_scopes.clone();
                    Some("mcp_auth scope provider not configured".to_string())
                }
                Some(provider) => {
                    available_scopes = provider.scopes_for(identity).await?;
                    missing_scopes = required_scopes
                        .iter()
                        .filter(|r| !available_scopes.iter().any(|a| a == *r))
                        .cloned()
                        .collect();
                    if missing_scopes.is_empty() {
                        None
                    } else {
                        Some(format!(
                            "missing required mcp scopes: {}",
                            missing_scopes.join(", ")
                        ))
                    }
                }
            }
        };

        let now = Utc::now();
        let (outcome, details) = match &failure_reason {
            None => ("ok", None),
            Some(s) => ("denied", Some(s.as_str())),
        };

        self.audit(AuditEntry {
            id: Uuid::new_v4(),
            org_id,
            project_id: Some(project_id),
            actor: identity.clone(),
            action: "core_agents.action.mcp_auth.gate".to_string(),
            payload: serde_json::json!({
                "action_id": proposal.id,
                "action_type": proposal.action_type,
                "risk_level": proposal.risk_level,
                "policy_review_mode": policy.review_mode,
                "required_scopes": required_scopes,
                "available_scopes": available_scopes,
                "missing_scopes": missing_scopes,
                "details": details,
            }),
            outcome: outcome.to_string(),
            created_at: now,
        })
        .await?;

        match failure_reason {
            None => Ok(()),
            Some(s) => Err(Error::Unauthorized(s)),
        }
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

        // Inject decrypted credentials if a CredentialManager is configured.
        let credentials = match &self.credential_manager {
            Some(cm) => {
                let ids = cm.list_connector_ids(org_id).await.unwrap_or_default();
                let mut map = serde_json::Map::new();
                for id in ids {
                    if let Ok(Some(v)) = cm.retrieve(org_id, &id).await {
                        map.insert(id, v);
                    }
                }
                if map.is_empty() {
                    None
                } else {
                    Some(serde_json::Value::Object(map))
                }
            }
            None => None,
        };

        let ctx = AgentContext {
            org_id,
            project_id,
            identity: identity.clone(),
            project_db: project_db.clone(),
            db: self.project_db.clone(),
            credentials,
        };

        let outcome = agent.run(ctx, inputs).await?;
        let outcome_value = outcome.result().cloned();
        let proposals = outcome.proposals();

        let mut proposed_action_ids = Vec::with_capacity(proposals.len());
        for p in proposals {
            let id = self
                .propose_action(org_id, project_id, &project_db, p, identity)
                .await?;
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
            outcome: outcome_value,
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
        org_id: OrgId,
        project_id: ProjectId,
        project_db: &ProjectDbHandle,
        mut proposal: ActionProposal,
        identity: &AgentIdentity,
    ) -> Result<Uuid> {
        ensure_handle_org(project_db, org_id)?;
        if project_db.project_id != project_id {
            return Err(Error::InvalidInput("project_id mismatch".to_string()));
        }
        if proposal.org_id != org_id || proposal.project_id != project_id {
            return Err(Error::InvalidInput(
                "proposal org_id/project_id mismatch".to_string(),
            ));
        }

        // Determine policy and set TTL accordingly.
        let (mode, policy) = self
            .resolve_policy(
                org_id,
                project_db,
                &proposal.action_type,
                proposal.risk_level,
            )
            .await?;

        // Update expires_at based on policy TTL (policy is authoritative).
        proposal.expires_at =
            proposal.created_at + chrono::Duration::seconds(policy.ttl_seconds as i64);

        // Persist proposed action.
        let (stored, stored_mode, inserted) = self
            .insert_action(org_id, project_db, &proposal, mode)
            .await?;

        // If this is an idempotent duplicate, return the existing proposal ID and do not
        // re-trigger audits/approvals/execution side effects.
        if !inserted {
            return Ok(stored.id);
        }

        self.audit(AuditEntry {
            id: Uuid::new_v4(),
            org_id: stored.org_id,
            project_id: Some(stored.project_id),
            actor: identity.clone(),
            action: "core_agents.action.proposed".to_string(),
            payload: serde_json::to_value(&stored)
                .map_err(|e| Error::backend("serialize proposal", e))?,
            outcome: "ok".to_string(),
            created_at: Utc::now(),
        })
        .await?;

        match stored_mode {
            ReviewMode::Auto => {
                // Auto-approve and execute immediately.
                let auto_identity = AgentIdentity::System {
                    name: "core_agents:auto".to_string(),
                };
                self.approve(
                    org_id,
                    project_id,
                    project_db,
                    stored.id,
                    &auto_identity,
                    "auto-approved",
                )
                .await?;
            }
            ReviewMode::Ai => {
                // AI review if configured, else policy resolution already downgraded to Human.
                let Some(approver) = self.ai_approver.clone() else {
                    return Ok(stored.id);
                };
                let decision = approver.review(&policy, &stored, identity).await?;
                let ai_identity = AgentIdentity::System {
                    name: format!("ai_approver:{}", approver.name()),
                };
                match decision {
                    ReviewDecision::Approved { reason } => {
                        self.approve(
                            org_id,
                            project_id,
                            project_db,
                            stored.id,
                            &ai_identity,
                            &reason,
                        )
                        .await?;
                    }
                    ReviewDecision::Denied { reason } => {
                        self.deny(
                            org_id,
                            project_id,
                            project_db,
                            stored.id,
                            &ai_identity,
                            &reason,
                        )
                        .await?;
                    }
                }
            }
            ReviewMode::Human => {
                // Pending until explicit approval/denial.
            }
            ReviewMode::McpAuth => {
                // MCP auth gate: automatically dispatch if the effective principal
                // has the required MCP scopes; otherwise fail closed and deny.
                match self
                    .approve(
                        org_id,
                        project_id,
                        project_db,
                        stored.id,
                        identity,
                        "mcp_auth gate",
                    )
                    .await
                {
                    Ok(()) => {}
                    Err(Error::Unauthorized(msg)) => {
                        let r = format!("mcp_auth denied: {msg}");
                        self.deny(org_id, project_id, project_db, stored.id, identity, &r)
                            .await?;
                    }
                    Err(e) => return Err(e),
                }
            }
        }

        Ok(stored.id)
    }

    #[tracing::instrument(level = "info", skip_all)]
    async fn approve(
        &self,
        org_id: OrgId,
        project_id: ProjectId,
        project_db: &ProjectDbHandle,
        action_id: Uuid,
        identity: &AgentIdentity,
        reason: &str,
    ) -> Result<()> {
        ensure_handle_org(project_db, org_id)?;
        if project_db.project_id != project_id {
            return Err(Error::InvalidInput("project_id mismatch".to_string()));
        }

        let Some((p0, mode)) = self.get_action(org_id, project_db, action_id).await? else {
            return Err(Error::NotFound(format!("action not found: {action_id}")));
        };
        if p0.project_id != project_id {
            return Err(Error::Unauthorized("action project mismatch".to_string()));
        }

        if mode == ReviewMode::McpAuth {
            self.enforce_mcp_auth_gate(org_id, project_id, project_db, &p0, identity)
                .await?;
        }

        let approver_id = identity_to_actor_id(identity);
        let now = Utc::now();
        let mut p = approval_sm::approve(p0, &approver_id, reason, now)?;
        self.update_action(org_id, project_db, &p).await?;

        // Near-term semantics:
        // "Dispatched" means "published an outbound event via the Event Sync layer".
        // Delivery confirmation is not yet tracked end-to-end.
        let topic = format!("action.{}.approved", p.action_type);
        let event_id = self
            .publish_action_event(&p, &topic, "approved", Some(reason))
            .await?;

        p = approval_sm::mark_dispatched(
            p,
            serde_json::json!({
                "dispatch_event_id": event_id,
                "dispatch_kind": "event_sync",
            }),
            Utc::now(),
        )?;
        self.update_action(org_id, project_db, &p).await?;

        self.audit(AuditEntry {
            id: Uuid::new_v4(),
            org_id,
            project_id: Some(project_id),
            actor: identity.clone(),
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
        identity: &AgentIdentity,
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

        let approver_id = identity_to_actor_id(identity);
        let now = Utc::now();
        let p = approval_sm::deny(p0, &approver_id, reason, now)?;
        self.update_action(org_id, project_db, &p).await?;

        let topic = format!("action.{}.denied", p.action_type);
        let _ = self
            .publish_action_event(&p, &topic, "denied", Some(reason))
            .await?;

        self.audit(AuditEntry {
            id: Uuid::new_v4(),
            org_id,
            project_id: Some(project_id),
            actor: identity.clone(),
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
    match row.get(col) {
        Some(ProjectDbValue::Null) => Ok(None),
        Some(ProjectDbValue::String(s)) => {
            if s.trim().is_empty() {
                Ok(None)
            } else {
                Ok(Some(s.clone()))
            }
        }
        Some(v) => Err(Error::BackendMessage(format!(
            "unexpected column type for {col}: {v:?}"
        ))),
        None => Ok(None),
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
    let dedupe_key = get_opt_string(row, "dedupe_key")?;
    let status = status_from_str(&get_string(row, "status")?)?;

    let payload_json = get_string(row, "payload_json")?;
    let payload: serde_json::Value = serde_json::from_str(&payload_json)
        .map_err(|e| Error::backend("deserialize payload_json", e))?;

    let context_json = get_string(row, "context_json")?;
    let context: serde_json::Value = serde_json::from_str(&context_json)
        .map_err(|e| Error::backend("deserialize context_json", e))?;

    let created_at = parse_dt(&get_string(row, "created_at")?, "created_at")?;
    let expires_at = parse_dt(&get_string(row, "expires_at")?, "expires_at")?;

    let decided_at = get_opt_string(row, "decided_at")?
        .map(|s| parse_dt(&s, "decided_at"))
        .transpose()?;
    let decided_by = get_opt_string(row, "decided_by")?;
    let decision_reason = get_opt_string(row, "decision_reason")?;
    let execution_result = match get_opt_string(row, "execution_result_json")? {
        Some(s) => {
            let v: serde_json::Value = serde_json::from_str(&s)
                .map_err(|e| Error::backend("deserialize execution_result_json", e))?;
            Some(v)
        }
        None => None,
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

fn identity_to_actor_id(identity: &AgentIdentity) -> String {
    match identity {
        AgentIdentity::User { user_id, .. } => user_id.to_string(),
        AgentIdentity::Agent { agent_id } => agent_id.clone(),
        AgentIdentity::System { name } => format!("system:{name}"),
    }
}

fn status_to_str(s: ActionStatus) -> &'static str {
    match s {
        ActionStatus::Proposed => "proposed",
        ActionStatus::Approved => "approved",
        ActionStatus::Denied => "denied",
        ActionStatus::Dispatched => "dispatched",
        ActionStatus::Expired => "expired",
    }
}

fn status_from_str(s: &str) -> Result<ActionStatus> {
    match s {
        "proposed" => Ok(ActionStatus::Proposed),
        "approved" => Ok(ActionStatus::Approved),
        "denied" => Ok(ActionStatus::Denied),
        "dispatched" => Ok(ActionStatus::Dispatched),
        // Backwards compat: older rows used "executed" for "dispatched".
        "executed" => Ok(ActionStatus::Dispatched),
        "expired" => Ok(ActionStatus::Expired),
        other => Err(Error::InvalidInput(format!(
            "unknown action status: {other}"
        ))),
    }
}
