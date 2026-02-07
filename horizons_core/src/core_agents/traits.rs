use crate::Result;
use crate::core_agents::models::{
    ActionProposal, AgentContext, AgentOutcome, AgentRunResult, ReviewMode, ReviewPolicy,
};
use crate::models::{AgentIdentity, OrgId, ProjectDbHandle, ProjectId};
use async_trait::async_trait;
use std::sync::Arc;
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReviewDecision {
    Approved { reason: String },
    Denied { reason: String },
}

#[async_trait]
pub trait AgentSpec: Send + Sync {
    async fn id(&self) -> String;
    async fn name(&self) -> String;

    /// Run the agent. Return an `AgentOutcome` describing the results.
    ///
    /// - `AgentOutcome::Proposals(vec)` — classic proposal-only agents.
    /// - `AgentOutcome::SideEffectOnly { result }` — agents that do their own
    ///   DB writes or external calls and don't need the approval pipeline.
    /// - `AgentOutcome::Mixed { result, proposals }` — both.
    ///
    /// For backward compatibility, the default implementation wraps the legacy
    /// `run_legacy` method that returns `Vec<ActionProposal>`.
    async fn run(
        &self,
        ctx: AgentContext,
        inputs: Option<serde_json::Value>,
    ) -> Result<AgentOutcome> {
        let proposals = self.run_legacy(ctx, inputs).await?;
        Ok(AgentOutcome::Proposals(proposals))
    }

    /// Legacy entrypoint for agents that only return proposals.
    /// Override `run` directly for new agents; this exists only for backward compat.
    async fn run_legacy(
        &self,
        _ctx: AgentContext,
        _inputs: Option<serde_json::Value>,
    ) -> Result<Vec<ActionProposal>> {
        Ok(Vec::new())
    }
}

/// Optional AI-review interface for medium-risk actions.
#[async_trait]
pub trait ActionApprover: Send + Sync {
    async fn review(
        &self,
        policy: &ReviewPolicy,
        proposal: &ActionProposal,
        identity: &AgentIdentity,
    ) -> Result<ReviewDecision>;

    fn name(&self) -> &'static str;
}

#[async_trait]
pub trait CoreAgents: Send + Sync {
    async fn register_agent(&self, agent: Arc<dyn AgentSpec>) -> Result<()>;

    async fn run(
        &self,
        org_id: OrgId,
        project_id: ProjectId,
        project_db: ProjectDbHandle,
        agent_id: &str,
        identity: &AgentIdentity,
        inputs: Option<serde_json::Value>,
    ) -> Result<AgentRunResult>;

    async fn propose_action(
        &self,
        org_id: OrgId,
        project_id: ProjectId,
        project_db: &ProjectDbHandle,
        proposal: ActionProposal,
        identity: &AgentIdentity,
    ) -> Result<Uuid>;

    async fn approve(
        &self,
        org_id: OrgId,
        project_id: ProjectId,
        project_db: &ProjectDbHandle,
        action_id: Uuid,
        identity: &AgentIdentity,
        reason: &str,
    ) -> Result<()>;

    async fn deny(
        &self,
        org_id: OrgId,
        project_id: ProjectId,
        project_db: &ProjectDbHandle,
        action_id: Uuid,
        identity: &AgentIdentity,
        reason: &str,
    ) -> Result<()>;

    async fn list_pending(
        &self,
        org_id: OrgId,
        project_id: ProjectId,
        project_db: &ProjectDbHandle,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<ActionProposal>>;

    async fn upsert_policy(
        &self,
        org_id: OrgId,
        project_id: ProjectId,
        project_db: &ProjectDbHandle,
        policy: ReviewPolicy,
        identity: &AgentIdentity,
    ) -> Result<()>;

    async fn get_policy(
        &self,
        org_id: OrgId,
        project_id: ProjectId,
        project_db: &ProjectDbHandle,
        action_type: &str,
        risk: crate::core_agents::models::RiskLevel,
    ) -> Result<(ReviewMode, ReviewPolicy)>;
}
