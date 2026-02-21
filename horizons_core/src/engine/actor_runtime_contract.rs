//! Shared actor runtime contracts for master/slave orchestration loops.
//!
//! This module is intentionally pure and deterministic so systems can:
//! - run the same tick logic in different hosts/processes,
//! - replay ticks for debugging,
//! - validate transitions before side effects are emitted.

use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

/// Monotonic logical time for a run.
pub type TickId = u64;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkerRole {
    MasterPrimary,
    MasterDebug,
    Slave,
    Observer,
}

impl WorkerRole {
    pub fn is_master(self) -> bool {
        matches!(self, WorkerRole::MasterPrimary | WorkerRole::MasterDebug)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerLease {
    pub worker_id: String,
    pub role: WorkerRole,
    pub healthy: bool,
    pub lease_expires_tick: TickId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MessageMode {
    Queue,
    Steer,
    Interrupt,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryStatus {
    Pending,
    Applied,
    Rejected,
    Ignored,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "action")]
pub enum ControlAction {
    PauseRun,
    ResumeRun,
    PromoteToMasterDebug { worker_id: String },
    DemoteToSlave { worker_id: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ControlMessage {
    pub id: String,
    /// Stable per-run ordering key supplied by caller.
    pub seq: u64,
    pub mode: MessageMode,
    pub from: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub to: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub body: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub action: Option<ControlAction>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RuntimeMessage {
    pub id: String,
    /// Stable per-run ordering key supplied by caller.
    pub seq: u64,
    pub mode: MessageMode,
    pub from: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub to: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub body: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub action: Option<ControlAction>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MessageDeliveryReceipt {
    pub receipt_id: String,
    pub message_id: String,
    pub run_id: String,
    pub tick_id: TickId,
    pub status: DeliveryStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskDependencyPolicy {
    OnSuccessStartDownstream,
    OnFailurePauseDownstream,
    OnFailureCancelDownstream,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IntentKind {
    PlanTasks,
    DispatchTask,
    CreateLinearTicket,
    PauseDependencyBranch,
    MergeOrRequeue,
    RequestHumanInput,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProposedIntent {
    pub idempotency_key: String,
    pub from_worker_id: String,
    pub kind: IntentKind,
    #[serde(default)]
    pub detail: serde_json::Value,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TickState {
    pub run_id: String,
    pub tick_id: TickId,
    pub paused: bool,
    pub workers: Vec<WorkerLease>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TransitionKind {
    Paused,
    PauseNoop,
    Resumed,
    ResumeNoop,
    MessageDelivered,
    PromotedToMasterDebug,
    PromoteNoop,
    DemotedToSlave,
    DemoteNoop,
    ElectedPrimaryMaster,
    DemotedExtraPrimaryMaster,
    DemotedStalePrimaryMaster,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TickTransition {
    pub kind: TransitionKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub worker_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RejectedIntent {
    pub idempotency_key: String,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RejectedControl {
    pub message_id: String,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TickOutcome {
    pub next_tick_id: TickId,
    pub paused: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub primary_master_worker_id: Option<String>,
    pub workers: Vec<WorkerLease>,
    pub transitions: Vec<TickTransition>,
    pub accepted_intents: Vec<ProposedIntent>,
    pub rejected_intents: Vec<RejectedIntent>,
    #[serde(default)]
    pub rejected_controls: Vec<RejectedControl>,
}

impl TickOutcome {
    pub fn control_delivery_status(&self, message_id: &str) -> DeliveryStatus {
        if self
            .rejected_controls
            .iter()
            .any(|r| r.message_id == message_id)
        {
            return DeliveryStatus::Rejected;
        }
        if self
            .transitions
            .iter()
            .any(|t| t.source.as_deref() == Some(message_id))
        {
            return DeliveryStatus::Applied;
        }
        DeliveryStatus::Ignored
    }

    pub fn control_rejection_reason(&self, message_id: &str) -> Option<&str> {
        self.rejected_controls
            .iter()
            .find(|r| r.message_id == message_id)
            .map(|r| r.reason.as_str())
    }
}

impl RuntimeMessage {
    pub fn as_control_message(&self) -> Option<ControlMessage> {
        self.action.as_ref()?;
        Some(ControlMessage {
            id: self.id.clone(),
            seq: self.seq,
            mode: self.mode,
            from: self.from.clone(),
            to: self.to.clone(),
            body: self.body.clone(),
            action: self.action.clone(),
        })
    }
}

pub fn parse_message_mode(value: &str) -> Option<MessageMode> {
    match value.trim().to_ascii_lowercase().as_str() {
        "queue" => Some(MessageMode::Queue),
        "steer" => Some(MessageMode::Steer),
        "interrupt" => Some(MessageMode::Interrupt),
        _ => None,
    }
}

pub fn message_mode_str(mode: MessageMode) -> &'static str {
    match mode {
        MessageMode::Queue => "queue",
        MessageMode::Steer => "steer",
        MessageMode::Interrupt => "interrupt",
    }
}

pub fn parse_control_action(
    action: &str,
    payload: Option<&serde_json::Value>,
) -> Option<ControlAction> {
    match action.trim().to_ascii_lowercase().as_str() {
        "pause_run" => Some(ControlAction::PauseRun),
        "resume_run" => Some(ControlAction::ResumeRun),
        "promote_to_master_debug" => {
            let worker_id = payload
                .and_then(|v| v.get("worker_id"))
                .and_then(|v| v.as_str())
                .map(|s| s.trim())
                .filter(|s| !s.is_empty())?
                .to_string();
            Some(ControlAction::PromoteToMasterDebug { worker_id })
        }
        "demote_to_slave" => {
            let worker_id = payload
                .and_then(|v| v.get("worker_id"))
                .and_then(|v| v.as_str())
                .map(|s| s.trim())
                .filter(|s| !s.is_empty())?
                .to_string();
            Some(ControlAction::DemoteToSlave { worker_id })
        }
        _ => None,
    }
}

pub fn control_action_name(action: &ControlAction) -> &'static str {
    match action {
        ControlAction::PauseRun => "pause_run",
        ControlAction::ResumeRun => "resume_run",
        ControlAction::PromoteToMasterDebug { .. } => "promote_to_master_debug",
        ControlAction::DemoteToSlave { .. } => "demote_to_slave",
    }
}

pub fn control_action_payload(action: &ControlAction) -> Option<serde_json::Value> {
    match action {
        ControlAction::PauseRun | ControlAction::ResumeRun => None,
        ControlAction::PromoteToMasterDebug { worker_id }
        | ControlAction::DemoteToSlave { worker_id } => {
            Some(serde_json::json!({ "worker_id": worker_id }))
        }
    }
}

pub fn delivery_status_str(status: DeliveryStatus) -> &'static str {
    match status {
        DeliveryStatus::Pending => "pending",
        DeliveryStatus::Applied => "applied",
        DeliveryStatus::Rejected => "rejected",
        DeliveryStatus::Ignored => "ignored",
    }
}

/// Advance a run by one logical tick.
///
/// The function is pure and deterministic:
/// - control actions are applied first (ordered by mode priority and sequence),
/// - primary master lease is repaired/elected,
/// - intents are validated and partitioned into accepted/rejected sets.
pub fn advance_tick(
    state: TickState,
    mut control_messages: Vec<ControlMessage>,
    mut proposed_intents: Vec<ProposedIntent>,
) -> TickOutcome {
    let mut paused = state.paused;
    let mut transitions = Vec::new();
    let mut rejected_controls = Vec::new();

    let mut workers: BTreeMap<String, WorkerLease> = state
        .workers
        .into_iter()
        .map(|w| (w.worker_id.clone(), w))
        .collect();

    control_messages.sort_by(|a, b| {
        mode_priority(a.mode)
            .cmp(&mode_priority(b.mode))
            .then(a.seq.cmp(&b.seq))
            .then(a.id.cmp(&b.id))
    });

    for msg in &control_messages {
        let Some(action) = msg.action.as_ref() else {
            match msg.mode {
                MessageMode::Interrupt => {
                    rejected_controls.push(RejectedControl {
                        message_id: msg.id.clone(),
                        reason: "interrupt_requires_action".to_string(),
                    });
                }
                MessageMode::Queue | MessageMode::Steer => {
                    transitions.push(TickTransition {
                        kind: TransitionKind::MessageDelivered,
                        worker_id: None,
                        source: Some(msg.id.clone()),
                    });
                }
            }
            continue;
        };
        match action {
            ControlAction::PauseRun => {
                if !paused {
                    paused = true;
                    transitions.push(TickTransition {
                        kind: TransitionKind::Paused,
                        worker_id: None,
                        source: Some(msg.id.clone()),
                    });
                } else {
                    transitions.push(TickTransition {
                        kind: TransitionKind::PauseNoop,
                        worker_id: None,
                        source: Some(msg.id.clone()),
                    });
                }
            }
            ControlAction::ResumeRun => {
                if paused {
                    paused = false;
                    transitions.push(TickTransition {
                        kind: TransitionKind::Resumed,
                        worker_id: None,
                        source: Some(msg.id.clone()),
                    });
                } else {
                    transitions.push(TickTransition {
                        kind: TransitionKind::ResumeNoop,
                        worker_id: None,
                        source: Some(msg.id.clone()),
                    });
                }
            }
            ControlAction::PromoteToMasterDebug { worker_id } => {
                if msg.to.as_deref().is_some_and(|to| to != worker_id) {
                    rejected_controls.push(RejectedControl {
                        message_id: msg.id.clone(),
                        reason: "target_mismatch".to_string(),
                    });
                    continue;
                }
                if let Some(worker) = workers.get_mut(worker_id) {
                    if worker.role != WorkerRole::MasterDebug {
                        worker.role = WorkerRole::MasterDebug;
                        transitions.push(TickTransition {
                            kind: TransitionKind::PromotedToMasterDebug,
                            worker_id: Some(worker_id.clone()),
                            source: Some(msg.id.clone()),
                        });
                    } else {
                        transitions.push(TickTransition {
                            kind: TransitionKind::PromoteNoop,
                            worker_id: Some(worker_id.clone()),
                            source: Some(msg.id.clone()),
                        });
                    }
                } else {
                    rejected_controls.push(RejectedControl {
                        message_id: msg.id.clone(),
                        reason: "unknown_worker".to_string(),
                    });
                }
            }
            ControlAction::DemoteToSlave { worker_id } => {
                if msg.to.as_deref().is_some_and(|to| to != worker_id) {
                    rejected_controls.push(RejectedControl {
                        message_id: msg.id.clone(),
                        reason: "target_mismatch".to_string(),
                    });
                    continue;
                }
                if let Some(worker) = workers.get_mut(worker_id) {
                    if worker.role != WorkerRole::Slave {
                        worker.role = WorkerRole::Slave;
                        transitions.push(TickTransition {
                            kind: TransitionKind::DemotedToSlave,
                            worker_id: Some(worker_id.clone()),
                            source: Some(msg.id.clone()),
                        });
                    } else {
                        transitions.push(TickTransition {
                            kind: TransitionKind::DemoteNoop,
                            worker_id: Some(worker_id.clone()),
                            source: Some(msg.id.clone()),
                        });
                    }
                } else {
                    rejected_controls.push(RejectedControl {
                        message_id: msg.id.clone(),
                        reason: "unknown_worker".to_string(),
                    });
                }
            }
        }
    }

    // Convert unhealthy/expired primary leases into debug role before election.
    // This prevents stale primary labels from surviving across ticks and keeps
    // "who is currently primary" derivable from role + lease validity.
    for worker in workers.values_mut() {
        if worker.role == WorkerRole::MasterPrimary
            && (!worker.healthy || worker.lease_expires_tick < state.tick_id)
        {
            worker.role = WorkerRole::MasterDebug;
            transitions.push(TickTransition {
                kind: TransitionKind::DemotedStalePrimaryMaster,
                worker_id: Some(worker.worker_id.clone()),
                source: None,
            });
        }
    }

    let active_primary: Vec<String> = workers
        .values()
        .filter(|w| {
            w.role == WorkerRole::MasterPrimary
                && w.healthy
                && w.lease_expires_tick >= state.tick_id
        })
        .map(|w| w.worker_id.clone())
        .collect();

    let primary_master_worker_id = if active_primary.is_empty() {
        let candidate = workers
            .values()
            .filter(|w| {
                (w.role == WorkerRole::MasterPrimary || w.role == WorkerRole::MasterDebug)
                    && w.healthy
                    && w.lease_expires_tick >= state.tick_id
            })
            .map(|w| w.worker_id.clone())
            .min();
        if let Some(worker_id) = candidate.clone()
            && let Some(worker) = workers.get_mut(&worker_id)
            && worker.role != WorkerRole::MasterPrimary
        {
            worker.role = WorkerRole::MasterPrimary;
            transitions.push(TickTransition {
                kind: TransitionKind::ElectedPrimaryMaster,
                worker_id: Some(worker_id.clone()),
                source: None,
            });
        }
        candidate
    } else {
        let elected = active_primary.iter().min().cloned();
        if let Some(primary_id) = elected.clone() {
            for worker_id in active_primary {
                if worker_id == primary_id {
                    continue;
                }
                if let Some(worker) = workers.get_mut(&worker_id) {
                    worker.role = WorkerRole::MasterDebug;
                    transitions.push(TickTransition {
                        kind: TransitionKind::DemotedExtraPrimaryMaster,
                        worker_id: Some(worker_id),
                        source: None,
                    });
                }
            }
        }
        elected
    };

    proposed_intents.sort_by(|a, b| {
        let a_priority = workers
            .get(&a.from_worker_id)
            .map(|w| intent_role_priority(w.role))
            .unwrap_or(u8::MAX);
        let b_priority = workers
            .get(&b.from_worker_id)
            .map(|w| intent_role_priority(w.role))
            .unwrap_or(u8::MAX);
        a_priority
            .cmp(&b_priority)
            .then(a.idempotency_key.cmp(&b.idempotency_key))
            .then(a.from_worker_id.cmp(&b.from_worker_id))
    });

    let mut seen = BTreeSet::new();
    let mut accepted_intents = Vec::new();
    let mut rejected_intents = Vec::new();

    for intent in proposed_intents {
        if intent.idempotency_key.trim().is_empty() {
            rejected_intents.push(RejectedIntent {
                idempotency_key: intent.idempotency_key,
                reason: "empty_idempotency_key".to_string(),
            });
            continue;
        }
        if !seen.insert(intent.idempotency_key.clone()) {
            rejected_intents.push(RejectedIntent {
                idempotency_key: intent.idempotency_key,
                reason: "duplicate_idempotency_key".to_string(),
            });
            continue;
        }
        let Some(worker) = workers.get(&intent.from_worker_id) else {
            rejected_intents.push(RejectedIntent {
                idempotency_key: intent.idempotency_key,
                reason: "unknown_worker".to_string(),
            });
            continue;
        };
        if !worker.role.is_master() {
            rejected_intents.push(RejectedIntent {
                idempotency_key: intent.idempotency_key,
                reason: "intent_requires_master_role".to_string(),
            });
            continue;
        }
        if !worker.healthy {
            rejected_intents.push(RejectedIntent {
                idempotency_key: intent.idempotency_key,
                reason: "intent_worker_unhealthy".to_string(),
            });
            continue;
        }
        if worker.lease_expires_tick < state.tick_id {
            rejected_intents.push(RejectedIntent {
                idempotency_key: intent.idempotency_key,
                reason: "intent_worker_lease_expired".to_string(),
            });
            continue;
        }
        if paused && intent.kind != IntentKind::RequestHumanInput {
            rejected_intents.push(RejectedIntent {
                idempotency_key: intent.idempotency_key,
                reason: "run_paused".to_string(),
            });
            continue;
        }
        accepted_intents.push(intent);
    }

    TickOutcome {
        next_tick_id: state.tick_id.saturating_add(1),
        paused,
        primary_master_worker_id,
        workers: workers.into_values().collect(),
        transitions,
        accepted_intents,
        rejected_intents,
        rejected_controls,
    }
}

fn mode_priority(mode: MessageMode) -> u8 {
    match mode {
        MessageMode::Interrupt => 0,
        MessageMode::Steer => 1,
        MessageMode::Queue => 2,
    }
}

fn intent_role_priority(role: WorkerRole) -> u8 {
    match role {
        WorkerRole::MasterPrimary => 0,
        WorkerRole::MasterDebug => 1,
        WorkerRole::Slave => 2,
        WorkerRole::Observer => 3,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn worker(id: &str, role: WorkerRole) -> WorkerLease {
        WorkerLease {
            worker_id: id.to_string(),
            role,
            healthy: true,
            lease_expires_tick: 100,
        }
    }

    #[test]
    fn elects_primary_master_from_debug_pool() {
        let state = TickState {
            run_id: "r1".to_string(),
            tick_id: 10,
            paused: false,
            workers: vec![
                worker("w2", WorkerRole::MasterDebug),
                worker("w1", WorkerRole::MasterDebug),
                worker("w3", WorkerRole::Slave),
            ],
        };
        let out = advance_tick(state, vec![], vec![]);
        assert_eq!(out.primary_master_worker_id.as_deref(), Some("w1"));
        assert!(
            out.transitions
                .iter()
                .any(|t| t.kind == TransitionKind::ElectedPrimaryMaster
                    && t.worker_id.as_deref() == Some("w1"))
        );
    }

    #[test]
    fn demotes_extra_primary_masters_deterministically() {
        let state = TickState {
            run_id: "r1".to_string(),
            tick_id: 10,
            paused: false,
            workers: vec![
                worker("w2", WorkerRole::MasterPrimary),
                worker("w1", WorkerRole::MasterPrimary),
            ],
        };
        let out = advance_tick(state, vec![], vec![]);
        assert_eq!(out.primary_master_worker_id.as_deref(), Some("w1"));
        let w2 = out
            .workers
            .iter()
            .find(|w| w.worker_id == "w2")
            .expect("w2 should exist");
        assert_eq!(w2.role, WorkerRole::MasterDebug);
    }

    #[test]
    fn demotes_stale_primary_and_re_elects() {
        let mut stale_primary = worker("w2", WorkerRole::MasterPrimary);
        stale_primary.healthy = false;
        let state = TickState {
            run_id: "r1".to_string(),
            tick_id: 10,
            paused: false,
            workers: vec![stale_primary, worker("w1", WorkerRole::MasterDebug)],
        };
        let out = advance_tick(state, vec![], vec![]);
        assert_eq!(out.primary_master_worker_id.as_deref(), Some("w1"));
        assert!(out.transitions.iter().any(|t| {
            t.kind == TransitionKind::DemotedStalePrimaryMaster && t.worker_id.as_deref() == Some("w2")
        }));
    }

    #[test]
    fn rejects_slave_intents_and_paused_intents() {
        let state = TickState {
            run_id: "r1".to_string(),
            tick_id: 10,
            paused: true,
            workers: vec![
                worker("master", WorkerRole::MasterPrimary),
                worker("slave", WorkerRole::Slave),
            ],
        };
        let intents = vec![
            ProposedIntent {
                idempotency_key: "i1".to_string(),
                from_worker_id: "slave".to_string(),
                kind: IntentKind::DispatchTask,
                detail: serde_json::json!({}),
            },
            ProposedIntent {
                idempotency_key: "i2".to_string(),
                from_worker_id: "master".to_string(),
                kind: IntentKind::PlanTasks,
                detail: serde_json::json!({}),
            },
            ProposedIntent {
                idempotency_key: "i3".to_string(),
                from_worker_id: "master".to_string(),
                kind: IntentKind::RequestHumanInput,
                detail: serde_json::json!({}),
            },
        ];

        let out = advance_tick(state, vec![], intents);
        assert_eq!(out.accepted_intents.len(), 1);
        assert_eq!(out.accepted_intents[0].idempotency_key, "i3");
        assert_eq!(out.rejected_intents.len(), 2);
    }

    #[test]
    fn rejects_intents_from_unhealthy_or_expired_master() {
        let mut unhealthy_master = worker("master-unhealthy", WorkerRole::MasterPrimary);
        unhealthy_master.healthy = false;
        let mut expired_master = worker("master-expired", WorkerRole::MasterPrimary);
        expired_master.lease_expires_tick = 1;
        let state = TickState {
            run_id: "r1".to_string(),
            tick_id: 10,
            paused: false,
            workers: vec![unhealthy_master, expired_master],
        };
        let intents = vec![
            ProposedIntent {
                idempotency_key: "i-unhealthy".to_string(),
                from_worker_id: "master-unhealthy".to_string(),
                kind: IntentKind::PlanTasks,
                detail: serde_json::json!({}),
            },
            ProposedIntent {
                idempotency_key: "i-expired".to_string(),
                from_worker_id: "master-expired".to_string(),
                kind: IntentKind::PlanTasks,
                detail: serde_json::json!({}),
            },
        ];

        let out = advance_tick(state, vec![], intents);
        assert!(out.accepted_intents.is_empty());
        assert_eq!(out.rejected_intents.len(), 2);
        assert!(
            out.rejected_intents
                .iter()
                .any(|r| r.reason == "intent_worker_unhealthy")
        );
        assert!(
            out.rejected_intents
                .iter()
                .any(|r| r.reason == "intent_worker_lease_expired")
        );
    }

    #[test]
    fn control_promotion_applies_before_election() {
        let state = TickState {
            run_id: "r1".to_string(),
            tick_id: 10,
            paused: false,
            workers: vec![worker("w9", WorkerRole::Slave)],
        };
        let control = vec![ControlMessage {
            id: "m1".to_string(),
            seq: 1,
            mode: MessageMode::Interrupt,
            from: "human".to_string(),
            to: Some("w9".to_string()),
            body: Some("promote for debugging".to_string()),
            action: Some(ControlAction::PromoteToMasterDebug {
                worker_id: "w9".to_string(),
            }),
        }];
        let out = advance_tick(state, control, vec![]);
        assert_eq!(out.primary_master_worker_id.as_deref(), Some("w9"));
    }

    #[test]
    fn delivery_status_tracks_control_transition_source() {
        let state = TickState {
            run_id: "r1".to_string(),
            tick_id: 1,
            paused: false,
            workers: vec![worker("master", WorkerRole::MasterPrimary)],
        };
        let out = advance_tick(
            state,
            vec![ControlMessage {
                id: "m1".to_string(),
                seq: 1,
                mode: MessageMode::Interrupt,
                from: "dashboard".to_string(),
                to: None,
                body: None,
                action: Some(ControlAction::PauseRun),
            }],
            vec![],
        );
        assert_eq!(out.control_delivery_status("m1"), DeliveryStatus::Applied);
        assert_eq!(
            out.control_delivery_status("missing"),
            DeliveryStatus::Ignored
        );
    }

    #[test]
    fn idempotent_controls_are_recorded_as_applied() {
        let state = TickState {
            run_id: "r1".to_string(),
            tick_id: 5,
            paused: true,
            workers: vec![worker("master", WorkerRole::MasterDebug)],
        };
        let out = advance_tick(
            state,
            vec![
                ControlMessage {
                    id: "pause_noop".to_string(),
                    seq: 1,
                    mode: MessageMode::Interrupt,
                    from: "dashboard".to_string(),
                    to: None,
                    body: None,
                    action: Some(ControlAction::PauseRun),
                },
                ControlMessage {
                    id: "promote_noop".to_string(),
                    seq: 2,
                    mode: MessageMode::Interrupt,
                    from: "dashboard".to_string(),
                    to: Some("master".to_string()),
                    body: None,
                    action: Some(ControlAction::PromoteToMasterDebug {
                        worker_id: "master".to_string(),
                    }),
                },
            ],
            vec![],
        );
        assert_eq!(
            out.control_delivery_status("pause_noop"),
            DeliveryStatus::Applied
        );
        assert_eq!(
            out.control_delivery_status("promote_noop"),
            DeliveryStatus::Applied
        );
    }

    #[test]
    fn control_rejects_unknown_worker_target() {
        let state = TickState {
            run_id: "r1".to_string(),
            tick_id: 1,
            paused: false,
            workers: vec![worker("master", WorkerRole::MasterPrimary)],
        };
        let out = advance_tick(
            state,
            vec![ControlMessage {
                id: "missing-worker".to_string(),
                seq: 1,
                mode: MessageMode::Interrupt,
                from: "dashboard".to_string(),
                to: Some("ghost".to_string()),
                body: None,
                action: Some(ControlAction::PromoteToMasterDebug {
                    worker_id: "ghost".to_string(),
                }),
            }],
            vec![],
        );
        assert_eq!(
            out.control_delivery_status("missing-worker"),
            DeliveryStatus::Rejected
        );
        assert_eq!(
            out.control_rejection_reason("missing-worker"),
            Some("unknown_worker")
        );
    }

    #[test]
    fn control_rejects_target_mismatch() {
        let state = TickState {
            run_id: "r1".to_string(),
            tick_id: 1,
            paused: false,
            workers: vec![worker("w1", WorkerRole::Slave)],
        };
        let out = advance_tick(
            state,
            vec![ControlMessage {
                id: "target-mismatch".to_string(),
                seq: 1,
                mode: MessageMode::Interrupt,
                from: "dashboard".to_string(),
                to: Some("w1".to_string()),
                body: None,
                action: Some(ControlAction::PromoteToMasterDebug {
                    worker_id: "w2".to_string(),
                }),
            }],
            vec![],
        );
        assert_eq!(
            out.control_delivery_status("target-mismatch"),
            DeliveryStatus::Rejected
        );
        assert_eq!(
            out.control_rejection_reason("target-mismatch"),
            Some("target_mismatch")
        );
    }

    #[test]
    fn primary_master_wins_duplicate_intent_idempotency_conflicts() {
        let state = TickState {
            run_id: "r1".to_string(),
            tick_id: 1,
            paused: false,
            workers: vec![
                worker("primary", WorkerRole::MasterPrimary),
                worker("debug", WorkerRole::MasterDebug),
            ],
        };
        let intents = vec![
            ProposedIntent {
                idempotency_key: "dup-key".to_string(),
                from_worker_id: "debug".to_string(),
                kind: IntentKind::PlanTasks,
                detail: serde_json::json!({"source":"debug"}),
            },
            ProposedIntent {
                idempotency_key: "dup-key".to_string(),
                from_worker_id: "primary".to_string(),
                kind: IntentKind::PlanTasks,
                detail: serde_json::json!({"source":"primary"}),
            },
        ];
        let out = advance_tick(state, vec![], intents);
        assert_eq!(out.accepted_intents.len(), 1);
        assert_eq!(out.accepted_intents[0].from_worker_id, "primary");
        assert_eq!(out.rejected_intents.len(), 1);
        assert_eq!(out.rejected_intents[0].reason, "duplicate_idempotency_key");
    }

    #[test]
    fn queue_message_without_action_is_applied() {
        let state = TickState {
            run_id: "r1".to_string(),
            tick_id: 1,
            paused: false,
            workers: vec![worker("master", WorkerRole::MasterPrimary)],
        };
        let out = advance_tick(
            state,
            vec![ControlMessage {
                id: "q1".to_string(),
                seq: 1,
                mode: MessageMode::Queue,
                from: "operator".to_string(),
                to: None,
                body: Some("non-interrupting hint".to_string()),
                action: None,
            }],
            vec![],
        );
        assert_eq!(out.control_delivery_status("q1"), DeliveryStatus::Applied);
        assert_eq!(out.control_rejection_reason("q1"), None);
    }

    #[test]
    fn interrupt_message_without_action_is_rejected() {
        let state = TickState {
            run_id: "r1".to_string(),
            tick_id: 1,
            paused: false,
            workers: vec![worker("master", WorkerRole::MasterPrimary)],
        };
        let out = advance_tick(
            state,
            vec![ControlMessage {
                id: "i1".to_string(),
                seq: 1,
                mode: MessageMode::Interrupt,
                from: "operator".to_string(),
                to: None,
                body: Some("stop now".to_string()),
                action: None,
            }],
            vec![],
        );
        assert_eq!(out.control_delivery_status("i1"), DeliveryStatus::Rejected);
        assert_eq!(
            out.control_rejection_reason("i1"),
            Some("interrupt_requires_action")
        );
    }
}
