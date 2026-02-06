use crate::context_refresh::models::{
    ContextEntity, RefreshRun, RefreshTrigger, SourceConfig, SyncCursor,
};
use crate::models::{AgentIdentity, OrgId};
use crate::onboard::traits::{CentralDb, ProjectDb};
use crate::{Error, Result};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// A raw record pulled from an external source (connector-defined payload).
#[derive(Debug, Clone, PartialEq)]
pub struct RawRecord {
    /// Stable identifier in the external system.
    pub source_id: String,
    /// Connector-provided idempotency key.
    pub dedupe_key: String,
    /// Connector-defined payload.
    pub payload: serde_json::Value,
    /// Access-control list for the record (connector-defined principals).
    pub acl: Vec<String>,
}

impl RawRecord {
    #[tracing::instrument(level = "debug", skip(payload))]
    pub fn new(
        source_id: impl Into<String> + std::fmt::Debug,
        dedupe_key: impl Into<String> + std::fmt::Debug,
        payload: serde_json::Value,
        acl: Vec<String>,
    ) -> Result<Self> {
        let source_id = source_id.into();
        if source_id.trim().is_empty() {
            return Err(Error::InvalidInput(
                "raw record source_id is empty".to_string(),
            ));
        }
        let dedupe_key = dedupe_key.into();
        if dedupe_key.trim().is_empty() {
            return Err(Error::InvalidInput(
                "raw record dedupe_key is empty".to_string(),
            ));
        }
        Ok(Self {
            source_id,
            dedupe_key,
            payload,
            acl,
        })
    }
}

/// Output from a connector pull() call.
#[derive(Debug, Clone, PartialEq)]
pub struct PullResult {
    pub records: Vec<RawRecord>,
    pub next_cursor: Option<SyncCursor>,
}

/// A connector pulls and processes data from an external system.
///
/// Connector implementations live in `horizons_integrations` or customer code.
#[async_trait]
pub trait Connector: Send + Sync {
    /// Connector identifier (stable, used in `SourceConfig.connector_id`).
    async fn id(&self) -> &'static str;

    /// Pull raw records since the last cursor.
    async fn pull(
        &self,
        org_id: OrgId,
        source: &SourceConfig,
        cursor: Option<SyncCursor>,
    ) -> Result<PullResult>;

    /// Transform raw records into normalized entities.
    async fn process(
        &self,
        org_id: OrgId,
        source: &SourceConfig,
        records: Vec<RawRecord>,
    ) -> Result<Vec<ContextEntity>>;
}

/// Result of a refresh execution.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RefreshResult {
    pub run: RefreshRun,
    pub published_event_id: Option<String>,
}

/// High-level status view for a source.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RefreshStatus {
    pub org_id: OrgId,
    pub source_id: String,
    pub last_run: Option<RefreshRun>,
    pub updated_at: DateTime<Utc>,
}

#[async_trait]
pub trait ContextRefresh: Send + Sync {
    /// Register an in-process connector implementation.
    async fn register_connector(&self, connector: Arc<dyn Connector>) -> Result<()>;

    /// Register or update a source configuration (stored durably in CentralDb).
    async fn register_source(&self, identity: &AgentIdentity, source: SourceConfig) -> Result<()>;

    /// Trigger a refresh by source_id.
    async fn trigger_refresh(
        &self,
        identity: &AgentIdentity,
        org_id: OrgId,
        source_id: &str,
        trigger: RefreshTrigger,
    ) -> Result<RefreshResult>;

    /// Get status (latest run) for a source.
    async fn get_status(&self, org_id: OrgId, source_id: &str) -> Result<RefreshStatus>;
}

/// Shared dependencies required to run Context Refresh.
///
/// This is a helper for construction; `ContextRefreshEngine` takes these as trait objects.
#[derive(Clone)]
pub struct ContextRefreshDeps {
    pub central_db: Arc<dyn CentralDb>,
    pub project_db: Arc<dyn ProjectDb>,
    pub event_bus: Arc<dyn crate::events::traits::EventBus>,
}
