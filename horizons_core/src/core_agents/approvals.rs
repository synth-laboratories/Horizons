use crate::core_agents::models::{ActionProposal, ActionStatus};
use crate::{Error, Result};
use chrono::{DateTime, Utc};

#[tracing::instrument(level = "debug")]
pub fn ensure_not_expired(proposal: &ActionProposal, now: DateTime<Utc>) -> Result<()> {
    if proposal.is_expired(now) {
        return Err(Error::Conflict("action proposal is expired".to_string()));
    }
    Ok(())
}

#[tracing::instrument(level = "debug")]
pub fn approve(
    mut proposal: ActionProposal,
    approver_id: &str,
    reason: &str,
    now: DateTime<Utc>,
) -> Result<ActionProposal> {
    if approver_id.trim().is_empty() {
        return Err(Error::InvalidInput("approver_id is empty".to_string()));
    }
    if reason.trim().is_empty() {
        return Err(Error::InvalidInput("reason is empty".to_string()));
    }
    if proposal.status != ActionStatus::Proposed {
        return Err(Error::Conflict(format!(
            "cannot approve action in status {:?}",
            proposal.status
        )));
    }
    ensure_not_expired(&proposal, now)?;

    proposal.status = ActionStatus::Approved;
    proposal.decided_at = Some(now);
    proposal.decided_by = Some(approver_id.to_string());
    proposal.decision_reason = Some(reason.to_string());
    Ok(proposal)
}

#[tracing::instrument(level = "debug")]
pub fn deny(
    mut proposal: ActionProposal,
    approver_id: &str,
    reason: &str,
    now: DateTime<Utc>,
) -> Result<ActionProposal> {
    if approver_id.trim().is_empty() {
        return Err(Error::InvalidInput("approver_id is empty".to_string()));
    }
    if reason.trim().is_empty() {
        return Err(Error::InvalidInput("reason is empty".to_string()));
    }
    if proposal.status != ActionStatus::Proposed {
        return Err(Error::Conflict(format!(
            "cannot deny action in status {:?}",
            proposal.status
        )));
    }
    ensure_not_expired(&proposal, now)?;

    proposal.status = ActionStatus::Denied;
    proposal.decided_at = Some(now);
    proposal.decided_by = Some(approver_id.to_string());
    proposal.decision_reason = Some(reason.to_string());
    Ok(proposal)
}

#[tracing::instrument(level = "debug")]
pub fn mark_dispatched(
    mut proposal: ActionProposal,
    execution_result: serde_json::Value,
    now: DateTime<Utc>,
) -> Result<ActionProposal> {
    if proposal.status != ActionStatus::Approved {
        return Err(Error::Conflict(format!(
            "cannot dispatch action in status {:?}",
            proposal.status
        )));
    }
    ensure_not_expired(&proposal, now)?;
    proposal.status = ActionStatus::Dispatched;
    proposal.execution_result = Some(execution_result);
    Ok(proposal)
}

#[deprecated(note = "use mark_dispatched (\"executed\" was renamed to \"dispatched\")")]
#[tracing::instrument(level = "debug")]
pub fn mark_executed(
    proposal: ActionProposal,
    execution_result: serde_json::Value,
    now: DateTime<Utc>,
) -> Result<ActionProposal> {
    mark_dispatched(proposal, execution_result, now)
}

#[tracing::instrument(level = "debug")]
pub fn mark_expired(mut proposal: ActionProposal, now: DateTime<Utc>) -> ActionProposal {
    if proposal.status == ActionStatus::Dispatched
        || proposal.status == ActionStatus::Denied
        || proposal.status == ActionStatus::Expired
    {
        return proposal;
    }
    if proposal.is_expired(now) {
        proposal.status = ActionStatus::Expired;
        proposal.decided_at = proposal.decided_at.or(Some(now));
        proposal.decision_reason = proposal.decision_reason.or(Some("expired".to_string()));
    }
    proposal
}
