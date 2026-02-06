from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional
from uuid import UUID

from pydantic import BaseModel, Field


class AgentIdentityUser(BaseModel):
    user_id: UUID
    email: Optional[str] = None


class AgentIdentityAgent(BaseModel):
    agent_id: str


class AgentIdentitySystem(BaseModel):
    name: str


class AgentIdentity(BaseModel):
    type: str
    user_id: Optional[UUID] = None
    email: Optional[str] = None
    agent_id: Optional[str] = None
    name: Optional[str] = None

    @classmethod
    def user(cls, user_id: UUID, email: Optional[str] = None) -> "AgentIdentity":
        return cls(type="user", user_id=user_id, email=email)

    @classmethod
    def agent(cls, agent_id: str) -> "AgentIdentity":
        return cls(type="agent", agent_id=agent_id)

    @classmethod
    def system(cls, name: str = "http") -> "AgentIdentity":
        return cls(type="system", name=name)


class ProjectDbHandle(BaseModel):
    org_id: UUID
    project_id: UUID
    connection_url: str
    auth_token: Optional[str] = None


class Project(BaseModel):
    handle: ProjectDbHandle


class EventDirection(str, Enum):
    inbound = "inbound"
    outbound = "outbound"


class EventStatus(str, Enum):
    pending = "pending"
    processing = "processing"
    succeeded = "succeeded"
    failed = "failed"


class Event(BaseModel):
    event_id: str
    org_id: str
    project_id: Optional[str] = None
    topic: str
    source: str
    direction: EventDirection
    payload: Any
    dedupe_key: str
    metadata: Dict[str, Any] = Field(default_factory=dict)
    status: EventStatus
    created_at: datetime


class SubscriptionHandlerType(str, Enum):
    webhook = "webhook"
    internal_queue = "internal_queue"


class SubscriptionHandler(BaseModel):
    type: SubscriptionHandlerType
    url: Optional[str] = None
    queue_name: Optional[str] = None


class SubscriptionConfig(BaseModel):
    max_attempts: int = 3
    backoff_ms: int = 1000


class Subscription(BaseModel):
    subscription_id: str
    org_id: str
    topic_pattern: str
    direction: EventDirection
    handler: SubscriptionHandler
    config: SubscriptionConfig
    filter: Optional[Dict[str, Any]] = None


class AgentRunResult(BaseModel):
    run_id: UUID
    org_id: UUID
    project_id: UUID
    agent_id: str
    started_at: datetime
    finished_at: datetime
    proposed_action_ids: List[UUID] = Field(default_factory=list)


class RiskLevel(str, Enum):
    low = "low"
    medium = "medium"
    high = "high"
    critical = "critical"


class ReviewMode(str, Enum):
    auto = "auto"
    ai = "ai"
    human = "human"
    mcp_auth = "mcp_auth"


class ActionStatus(str, Enum):
    proposed = "proposed"
    approved = "approved"
    denied = "denied"
    executed = "executed"
    expired = "expired"


class ActionProposal(BaseModel):
    id: UUID
    org_id: UUID
    project_id: UUID
    agent_id: str
    action_type: str
    payload: Any
    risk_level: RiskLevel
    dedupe_key: Optional[str] = None
    context: Any
    status: ActionStatus
    created_at: datetime
    decided_at: Optional[datetime]
    decided_by: Optional[str]
    decision_reason: Optional[str]
    expires_at: datetime
    execution_result: Optional[Any] = None


class StepStatus(str, Enum):
    queued = "queued"
    running = "running"
    succeeded = "succeeded"
    failed = "failed"
    skipped = "skipped"


class PipelineStatus(str, Enum):
    queued = "queued"
    running = "running"
    succeeded = "succeeded"
    failed = "failed"

    # WaitingApproval is represented as {"waiting_approval": "<step_id>"} in some clients;
    # the REST API returns a tagged enum; keep the model permissive by allowing raw strings.


class StepResult(BaseModel):
    step_id: str
    status: StepStatus
    output: Optional[Any] = None
    error: Optional[str] = None
    duration_ms: int


class PipelineRun(BaseModel):
    id: str
    pipeline_id: str
    status: Any
    step_results: Dict[str, StepResult]
    started_at: datetime
    completed_at: Optional[datetime] = None


class MemoryType(str, Enum):
    observation = "observation"
    summary = "summary"
    action = "action"


class MemoryItem(BaseModel):
    id: Optional[str] = None
    scope: Dict[str, str]
    item_type: MemoryType
    content: Any
    index_text: Optional[str] = None
    importance_0_to_1: Optional[float] = None
    created_at: datetime


class Summary(BaseModel):
    agent_id: str
    horizon: str
    content: Any
    at: datetime


class OptimizationRunRow(BaseModel):
    run_id: UUID
    org_id: UUID
    project_id: UUID
    status: str
    started_at: datetime
    finished_at: Optional[datetime]
    cfg: Dict[str, Any]
    initial_policy: Dict[str, Any]


class EvalReportRow(BaseModel):
    report_id: UUID
    org_id: UUID
    project_id: UUID
    status: str
    created_at: datetime
    completed_at: Optional[datetime]
    case: Dict[str, Any]


class Health(BaseModel):
    status: str
    uptime_ms: int
