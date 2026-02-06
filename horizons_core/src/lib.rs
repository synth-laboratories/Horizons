//! Horizons core library: traits and shared models used across all systems.

pub mod context_refresh;
pub mod core_agents;
pub mod engine;
pub mod error;
#[cfg(feature = "evaluation")]
pub mod evaluation;
pub mod events;
#[cfg(feature = "memory")]
pub mod memory;
pub mod models;
pub mod o11y;
pub mod onboard;
#[cfg(feature = "optimization")]
pub mod optimization;
pub mod pipelines;

pub use context_refresh::models::{
    ContextEntity, CronSchedule, EventTriggerConfig, RefreshRun, RefreshRunQuery, RefreshRunStatus,
    RefreshTrigger, SourceConfig, SyncCursor,
};
pub use context_refresh::traits::{Connector, ContextRefresh};
pub use core_agents::models::{
    ActionProposal, ActionStatus, AgentContext, AgentRunResult, AgentSchedule, AgentSpec,
    ReviewMode, ReviewPolicy, RiskLevel,
};
pub use core_agents::traits::{
    ActionApprover, AgentSpec as AgentSpecTrait, CoreAgents, ReviewDecision,
};
pub use error::{Error, Result};
pub use models::{AgentIdentity, AuditEntry, OrgId, PlatformConfig, ProjectDbHandle, ProjectId};
pub use o11y::traits::{
    O11yConfig, O11yExporter, OtelEvent, OtelSpan, OtelSpanStatus, OtelStatusCode, OtlpProtocol,
    RedactionLevel,
};
pub use onboard::traits::{
    AuditQuery, BytesStream, Cache, CentralDb, ConnectorCredential, Filestore, GraphStore,
    ListQuery, OrgRecord, ProjectDb, ProjectDbParam, ProjectDbRow, ProjectDbValue, SyncState,
    SyncStateKey, UserRecord, UserRole, VectorMatch, VectorStore, ensure_handle_org,
};

#[cfg(feature = "evaluation")]
pub use evaluation::traits as evaluation_traits;
#[cfg(feature = "memory")]
pub use memory::traits as memory_traits;
#[cfg(feature = "optimization")]
pub use optimization::traits as optimization_traits;
