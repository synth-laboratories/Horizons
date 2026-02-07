use crate::models::{OrgId, ProjectDbHandle, ProjectId};
use crate::{Error, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// How pulled records are transformed into `ContextEntity` objects.
///
/// Default is `Connector` (connector provides `process()`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SourceProcessorSpec {
    /// Use the registered connector's `process()` implementation.
    Connector,
    /// Run a pipeline after `pull()` to produce normalized entities.
    ///
    /// The pipeline must produce an `entities` array (either as the step output itself,
    /// or as `{ "entities": [...] }`) in the configured `entities_step_id`.
    Pipeline {
        spec: crate::pipelines::models::PipelineSpec,
        entities_step_id: String,
    },
}

impl Default for SourceProcessorSpec {
    fn default() -> Self {
        SourceProcessorSpec::Connector
    }
}

/// A normalized entity produced by Context Refresh and stored for later retrieval by agents.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ContextEntity {
    pub id: Uuid,
    pub org_id: OrgId,
    pub project_id: Option<ProjectId>,
    pub entity_type: String,
    pub content: serde_json::Value,
    pub source: String,
    pub source_id: String,
    pub dedupe_key: String,
    pub acl: Vec<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl ContextEntity {
    #[tracing::instrument(level = "debug", skip(content))]
    pub fn new(
        org_id: OrgId,
        project_id: Option<ProjectId>,
        entity_type: impl Into<String> + std::fmt::Debug,
        content: serde_json::Value,
        source: impl Into<String> + std::fmt::Debug,
        source_id: impl Into<String> + std::fmt::Debug,
        dedupe_key: impl Into<String> + std::fmt::Debug,
        acl: Vec<String>,
        now: Option<DateTime<Utc>>,
    ) -> Result<Self> {
        let entity_type = entity_type.into();
        if entity_type.trim().is_empty() {
            return Err(Error::InvalidInput("entity_type is empty".to_string()));
        }

        let source = source.into();
        if source.trim().is_empty() {
            return Err(Error::InvalidInput("source is empty".to_string()));
        }

        let source_id = source_id.into();
        if source_id.trim().is_empty() {
            return Err(Error::InvalidInput("source_id is empty".to_string()));
        }

        let dedupe_key = dedupe_key.into();
        if dedupe_key.trim().is_empty() {
            return Err(Error::InvalidInput("dedupe_key is empty".to_string()));
        }

        let now = now.unwrap_or_else(Utc::now);
        Ok(Self {
            id: Uuid::new_v4(),
            org_id,
            project_id,
            entity_type,
            content,
            source,
            source_id,
            dedupe_key,
            acl,
            created_at: now,
            updated_at: now,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RefreshTrigger {
    OnDemand,
    Cron { schedule_id: String },
    Event(EventTriggerEvent),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventTriggerEvent {
    /// Identifier for the inbound trigger source (e.g. "crm_update").
    pub source: String,
    /// Dot-delimited event topic that triggered the refresh.
    pub topic: String,
}

/// Durable sync cursor state passed to/from connectors.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SyncCursor {
    pub value: serde_json::Value,
}

impl SyncCursor {
    #[tracing::instrument(level = "debug", skip(value))]
    pub fn new(value: serde_json::Value) -> Self {
        Self { value }
    }
}

/// Per-source cron schedule (UTC).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CronSchedule {
    pub expr: String,
    pub next_run_at: DateTime<Utc>,
}

impl CronSchedule {
    #[tracing::instrument(level = "debug")]
    pub fn new(
        expr: impl Into<String> + std::fmt::Debug,
        next_run_at: DateTime<Utc>,
    ) -> Result<Self> {
        let expr = expr.into();
        if expr.trim().is_empty() {
            return Err(Error::InvalidInput("cron expr is empty".to_string()));
        }
        Ok(Self { expr, next_run_at })
    }
}

/// Event-driven trigger mapping for a source.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EventTriggerConfig {
    /// Glob-like pattern for topics, e.g. "crm.*" or "signal.created".
    pub topic_pattern: String,
    /// Optional filter for event.source.
    pub source: Option<String>,
    /// Optional payload filter (must be a subset of the event payload).
    pub filter: Option<serde_json::Value>,
}

impl EventTriggerConfig {
    #[tracing::instrument(level = "debug", skip(filter))]
    pub fn new(
        topic_pattern: impl Into<String> + std::fmt::Debug,
        source: Option<String>,
        filter: Option<serde_json::Value>,
    ) -> Result<Self> {
        let topic_pattern = topic_pattern.into();
        if topic_pattern.trim().is_empty() {
            return Err(Error::InvalidInput("topic_pattern is empty".to_string()));
        }
        if let Some(src) = &source {
            if src.trim().is_empty() {
                return Err(Error::InvalidInput(
                    "event trigger source is empty".to_string(),
                ));
            }
        }
        Ok(Self {
            topic_pattern,
            source,
            filter,
        })
    }
}

/// Registered ingestion source + scheduling configuration.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SourceConfig {
    pub org_id: OrgId,
    pub project_id: ProjectId,
    pub project_db: ProjectDbHandle,
    /// Unique identifier within an org, e.g. "gmail:inbox:primary".
    pub source_id: String,
    /// Connector implementation identifier, e.g. "gmail" or "hubspot".
    pub connector_id: String,
    /// Sync-state scope (connector-defined partition key).
    pub scope: String,
    pub enabled: bool,
    pub schedule: Option<CronSchedule>,
    pub event_triggers: Vec<EventTriggerConfig>,
    /// Optional processing layer on top of `pull()`.
    ///
    /// If not set in stored configs, defaults to `connector`.
    #[serde(default)]
    pub processor: SourceProcessorSpec,
    /// Connector-specific configuration blob (auth references, filters, etc.).
    pub settings: serde_json::Value,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl SourceConfig {
    #[tracing::instrument(level = "debug", skip(settings, event_triggers))]
    pub fn new(
        org_id: OrgId,
        project_id: ProjectId,
        project_db: ProjectDbHandle,
        source_id: impl Into<String> + std::fmt::Debug,
        connector_id: impl Into<String> + std::fmt::Debug,
        scope: impl Into<String> + std::fmt::Debug,
        enabled: bool,
        schedule: Option<CronSchedule>,
        event_triggers: Vec<EventTriggerConfig>,
        settings: serde_json::Value,
        now: Option<DateTime<Utc>>,
    ) -> Result<Self> {
        let source_id = source_id.into();
        if source_id.trim().is_empty() {
            return Err(Error::InvalidInput("source_id is empty".to_string()));
        }

        let connector_id = connector_id.into();
        if connector_id.trim().is_empty() {
            return Err(Error::InvalidInput("connector_id is empty".to_string()));
        }

        let scope = scope.into();
        if scope.trim().is_empty() {
            return Err(Error::InvalidInput("scope is empty".to_string()));
        }

        if project_db.org_id != org_id || project_db.project_id != project_id {
            return Err(Error::InvalidInput(
                "project_db handle does not match org_id/project_id".to_string(),
            ));
        }

        let now = now.unwrap_or_else(Utc::now);
        Ok(Self {
            org_id,
            project_id,
            project_db,
            source_id,
            connector_id,
            scope,
            enabled,
            schedule,
            event_triggers,
            processor: SourceProcessorSpec::default(),
            settings,
            created_at: now,
            updated_at: now,
        })
    }

    #[tracing::instrument(level = "debug")]
    pub fn touch(&mut self, at: DateTime<Utc>) {
        self.updated_at = at;
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RefreshRunStatus {
    Running,
    Succeeded,
    Failed,
}

/// A single refresh execution record (durable, queryable).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RefreshRun {
    pub run_id: Uuid,
    pub org_id: OrgId,
    pub project_id: ProjectId,
    pub source_id: String,
    pub connector_id: String,
    pub trigger: RefreshTrigger,
    pub status: RefreshRunStatus,
    pub started_at: DateTime<Utc>,
    pub finished_at: Option<DateTime<Utc>>,
    pub records_pulled: u64,
    pub entities_stored: u64,
    pub error_message: Option<String>,
    pub cursor: Option<serde_json::Value>,
}

impl RefreshRun {
    #[tracing::instrument(level = "debug")]
    pub fn new_running(
        org_id: OrgId,
        project_id: ProjectId,
        source_id: impl Into<String> + std::fmt::Debug,
        connector_id: impl Into<String> + std::fmt::Debug,
        trigger: RefreshTrigger,
        started_at: DateTime<Utc>,
    ) -> Result<Self> {
        let source_id = source_id.into();
        if source_id.trim().is_empty() {
            return Err(Error::InvalidInput("source_id is empty".to_string()));
        }
        let connector_id = connector_id.into();
        if connector_id.trim().is_empty() {
            return Err(Error::InvalidInput("connector_id is empty".to_string()));
        }
        Ok(Self {
            run_id: Uuid::new_v4(),
            org_id,
            project_id,
            source_id,
            connector_id,
            trigger,
            status: RefreshRunStatus::Running,
            started_at,
            finished_at: None,
            records_pulled: 0,
            entities_stored: 0,
            error_message: None,
            cursor: None,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RefreshRunQuery {
    pub project_id: Option<ProjectId>,
    pub source_id: Option<String>,
    pub status: Option<RefreshRunStatus>,
    pub since: Option<DateTime<Utc>>,
    pub until: Option<DateTime<Utc>>,
    pub limit: usize,
    pub offset: usize,
}

impl Default for RefreshRunQuery {
    fn default() -> Self {
        Self {
            project_id: None,
            source_id: None,
            status: None,
            since: None,
            until: None,
            limit: 100,
            offset: 0,
        }
    }
}
