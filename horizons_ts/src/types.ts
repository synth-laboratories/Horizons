export type UUID = string;

export interface ProjectDbHandle {
  org_id: UUID;
  project_id: UUID;
  connection_url: string;
  auth_token?: string | null;
}

export type EventDirection = "inbound" | "outbound";
export type EventStatus = "pending" | "processing" | "succeeded" | "failed";

export interface Event {
  event_id: string;
  org_id: string;
  project_id?: string;
  topic: string;
  source: string;
  direction: EventDirection;
  payload: unknown;
  dedupe_key: string;
  metadata?: Record<string, unknown>;
  status: EventStatus;
  created_at: string;
}

export interface AgentRunResult {
  run_id: UUID;
  org_id: UUID;
  project_id: UUID;
  agent_id: string;
  started_at: string;
  finished_at: string;
  proposed_action_ids: UUID[];
}

export type RiskLevel = "low" | "medium" | "high" | "critical";
export type ActionStatus = "proposed" | "approved" | "denied" | "executed" | "expired";

export interface ActionProposal {
  id: UUID;
  org_id: UUID;
  project_id: UUID;
  agent_id: string;
  action_type: string;
  payload: unknown;
  risk_level: RiskLevel;
  dedupe_key?: string | null;
  context: unknown;
  status: ActionStatus;
  created_at: string;
  decided_at?: string | null;
  decided_by?: string | null;
  decision_reason?: string | null;
  expires_at: string;
  execution_result?: unknown;
}

export interface StepResult {
  step_id: string;
  status: string;
  output?: unknown | null;
  error?: string | null;
  duration_ms: number;
}

export interface PipelineRun {
  id: string;
  pipeline_id: string;
  status: unknown;
  step_results: Record<string, StepResult>;
  started_at: string;
  completed_at?: string | null;
}

export interface OptimizationRunRow {
  run_id: UUID;
  org_id: UUID;
  project_id: UUID;
  status: string;
  started_at: string;
  finished_at?: string | null;
  cfg: Record<string, unknown>;
  initial_policy: Record<string, unknown>;
}

export interface EvalReportRow {
  report_id: UUID;
  org_id: UUID;
  project_id: UUID;
  status: string;
  created_at: string;
  completed_at?: string | null;
  case: Record<string, unknown>;
}
