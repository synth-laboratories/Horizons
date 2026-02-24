export type UUID = string;

export interface ProjectDbHandle {
  org_id: UUID;
  project_id: UUID;
  connection_url: string;
  auth_token?: string | null;
}

export type EventDirection = "inbound" | "outbound";
export type EventStatus = "pending" | "processing" | "delivered" | "failed" | "dead_lettered";

export interface Event {
  id: string;
  org_id: string;
  project_id?: string;
  timestamp: string;
  received_at: string;
  topic: string;
  source: string;
  direction: EventDirection;
  payload: unknown;
  dedupe_key: string;
  status: EventStatus;
  retry_count: number;
  metadata: Record<string, unknown>;
  last_attempt_at?: string | null;
}

export interface CircuitBreakerConfig {
  failure_threshold: number;
  open_duration_ms: number;
  success_threshold: number;
}

export interface SubscriptionConfig {
  max_retries: number;
  retry_backoff_base_ms: number;
  retry_backoff_max_ms: number;
  circuit_breaker: CircuitBreakerConfig;
}

export type SubscriptionHandler =
  | { type: "webhook"; url: string; headers: [string, string][]; timeout_ms?: number | null }
  | { type: "operation"; operation_id: string; environment?: string | null }
  | { type: "internal_queue"; queue_name: string }
  | { type: "callback"; handler_id: string };

export interface Subscription {
  id: string;
  org_id: string;
  topic_pattern: string;
  direction: EventDirection;
  handler: SubscriptionHandler;
  config: SubscriptionConfig;
  filter?: Record<string, unknown> | null;
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
export type ActionStatus =
  | "proposed"
  | "approved"
  | "denied"
  | "dispatched"
  // Canonical name (server previously returned "executed" for "dispatched").
  | "executed"
  | "expired";

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
