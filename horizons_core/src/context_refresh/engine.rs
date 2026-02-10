use crate::context_refresh::models::{
    ContextEntity, RefreshRun, RefreshRunQuery, RefreshRunStatus, RefreshTrigger, SourceConfig,
    SourceProcessorSpec, SyncCursor,
};
use crate::context_refresh::traits::{
    Connector, ContextRefresh, PullResult, RefreshResult, RefreshStatus,
};
use crate::events::models::{Event, EventDirection};
use crate::events::traits::EventBus;
use crate::models::{AgentIdentity, OrgId};
use crate::onboard::secrets::CredentialManager;
use crate::onboard::traits::{
    CentralDb, ListQuery, ProjectDb, ProjectDbParam, SyncState, SyncStateKey, ensure_handle_org,
};
use crate::pipelines::models::{PipelineSpec, StepKind};
use crate::pipelines::traits::PipelineRunner;
use crate::{Error, Result};
use async_trait::async_trait;
use chrono::Utc;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use uuid::Uuid;

/// Default Context Refresh implementation.
pub struct ContextRefreshEngine {
    central_db: Arc<dyn CentralDb>,
    project_db: Arc<dyn ProjectDb>,
    event_bus: Arc<dyn EventBus>,
    connectors: RwLock<HashMap<String, Arc<dyn Connector>>>,
    credential_manager: Option<Arc<CredentialManager>>,
    pipelines: Option<Arc<dyn PipelineRunner>>,
}

impl ContextRefreshEngine {
    #[tracing::instrument(level = "debug", skip(central_db, project_db, event_bus))]
    pub fn new(
        central_db: Arc<dyn CentralDb>,
        project_db: Arc<dyn ProjectDb>,
        event_bus: Arc<dyn EventBus>,
    ) -> Self {
        Self {
            central_db,
            project_db,
            event_bus,
            connectors: RwLock::new(HashMap::new()),
            credential_manager: None,
            pipelines: None,
        }
    }

    /// Set the credential manager for resolving `$cred:` tokens in source settings.
    pub fn with_credential_manager(mut self, cm: Arc<CredentialManager>) -> Self {
        self.credential_manager = Some(cm);
        self
    }

    /// Set a pipeline runner used for non-connector processing (graphs/agents/tools as pipeline steps).
    pub fn with_pipeline_runner(mut self, pipelines: Arc<dyn PipelineRunner>) -> Self {
        self.pipelines = Some(pipelines);
        self
    }

    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn list_sources(&self, org_id: OrgId, query: ListQuery) -> Result<Vec<SourceConfig>> {
        self.central_db.list_source_configs(org_id, query).await
    }

    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn list_refresh_runs(
        &self,
        org_id: OrgId,
        query: RefreshRunQuery,
    ) -> Result<Vec<RefreshRun>> {
        self.central_db.list_refresh_runs(org_id, query).await
    }

    #[tracing::instrument(level = "debug", skip(self, source))]
    async fn validate_source_has_connector(&self, source: &SourceConfig) -> Result<()> {
        let connectors = self.connectors.read().await;
        if !connectors.contains_key(&source.connector_id) {
            return Err(Error::NotFound(format!(
                "connector '{}' not registered",
                source.connector_id
            )));
        }
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn get_connector(&self, connector_id: &str) -> Result<Arc<dyn Connector>> {
        let connectors = self.connectors.read().await;
        connectors
            .get(connector_id)
            .cloned()
            .ok_or_else(|| Error::NotFound(format!("connector '{connector_id}' not registered")))
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn load_cursor(&self, source: &SourceConfig) -> Result<Option<SyncCursor>> {
        let key = SyncStateKey {
            org_id: source.org_id,
            project_id: Some(source.project_id),
            connector_id: source.connector_id.clone(),
            scope: source.scope.clone(),
        };
        Ok(self
            .central_db
            .get_sync_state(&key)
            .await?
            .map(|s| SyncCursor::new(s.cursor)))
    }

    #[tracing::instrument(level = "debug", skip(self, cursor))]
    async fn save_cursor(&self, source: &SourceConfig, cursor: Option<SyncCursor>) -> Result<()> {
        if let Some(cursor) = cursor {
            let state = SyncState {
                key: SyncStateKey {
                    org_id: source.org_id,
                    project_id: Some(source.project_id),
                    connector_id: source.connector_id.clone(),
                    scope: source.scope.clone(),
                },
                cursor: cursor.value,
                updated_at: Utc::now(),
            };
            self.central_db.upsert_sync_state(&state).await?;
        }
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn ensure_entity_table(&self, org_id: OrgId, source: &SourceConfig) -> Result<()> {
        ensure_handle_org(&source.project_db, org_id)?;

        // SQLite/libSQL-compatible DDL. This is Horizons-owned storage inside the project DB.
        // Apps can keep their own tables separate.
        let ddl = r#"
CREATE TABLE IF NOT EXISTS horizons_context_entities (
  id TEXT PRIMARY KEY,
  org_id TEXT NOT NULL,
  entity_type TEXT NOT NULL,
  content TEXT NOT NULL,
  source TEXT NOT NULL,
  source_id TEXT NOT NULL,
  dedupe_key TEXT NOT NULL,
  acl TEXT NOT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS hce_org_dedupe_key
  ON horizons_context_entities(org_id, dedupe_key);
CREATE INDEX IF NOT EXISTS hce_org_updated_at
  ON horizons_context_entities(org_id, updated_at);
-- Query-pattern indexes used by downstream apps (OpenRevenue/Internal/etc).
CREATE INDEX IF NOT EXISTS hce_org_source_updated_at
  ON horizons_context_entities(org_id, source, updated_at);
CREATE INDEX IF NOT EXISTS hce_org_entity_type_updated_at
  ON horizons_context_entities(org_id, entity_type, updated_at);
CREATE INDEX IF NOT EXISTS hce_org_source_source_id
  ON horizons_context_entities(org_id, source, source_id);

-- Stable, versioned read contract for context entities.
--
-- Apps should query this view (not the base table) so we can evolve the
-- underlying storage without forcing downstream SQL changes.
DROP VIEW IF EXISTS horizons_v1_context_entities;
CREATE VIEW horizons_v1_context_entities AS
  SELECT
    id,
    org_id,
    entity_type,
    content,
    source,
    source_id,
    dedupe_key,
    acl,
    created_at,
    updated_at
  FROM horizons_context_entities;
"#;

        // `execute` is per-statement in most backends; split conservatively.
        for stmt in ddl.split(';').map(str::trim).filter(|s| !s.is_empty()) {
            self.project_db
                .execute(org_id, &source.project_db, stmt, &[])
                .await?;
        }
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self, entities))]
    async fn store_entities(
        &self,
        org_id: OrgId,
        source: &SourceConfig,
        entities: &[ContextEntity],
    ) -> Result<u64> {
        ensure_handle_org(&source.project_db, org_id)?;

        // Ensure internal schema exists.
        self.ensure_entity_table(org_id, source).await?;

        let mut stored = 0u64;
        for ent in entities {
            if ent.org_id != org_id {
                return Err(Error::InvalidInput(
                    "entity org_id does not match refresh org_id".to_string(),
                ));
            }

            // Upsert by (org_id, dedupe_key) for idempotency across re-pulls.
            let sql = r#"
INSERT INTO horizons_context_entities
  (id, org_id, entity_type, content, source, source_id, dedupe_key, acl, created_at, updated_at)
VALUES
  (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
ON CONFLICT(org_id, dedupe_key) DO UPDATE SET
  entity_type = excluded.entity_type,
  content = excluded.content,
  source = excluded.source,
  source_id = excluded.source_id,
  acl = excluded.acl,
  updated_at = excluded.updated_at
"#;

            let params = vec![
                ProjectDbParam::String(ent.id.to_string()),
                ProjectDbParam::String(ent.org_id.to_string()),
                ProjectDbParam::String(ent.entity_type.clone()),
                ProjectDbParam::String(
                    serde_json::to_string(&ent.content)
                        .map_err(|e| Error::backend("serialize entity content", e))?,
                ),
                ProjectDbParam::String(ent.source.clone()),
                ProjectDbParam::String(ent.source_id.clone()),
                ProjectDbParam::String(ent.dedupe_key.clone()),
                ProjectDbParam::String(
                    serde_json::to_string(&ent.acl)
                        .map_err(|e| Error::backend("serialize entity acl", e))?,
                ),
                ProjectDbParam::String(ent.created_at.to_rfc3339()),
                ProjectDbParam::String(ent.updated_at.to_rfc3339()),
            ];

            let _ = self
                .project_db
                .execute(org_id, &source.project_db, sql, &params)
                .await?;
            stored += 1;
        }
        Ok(stored)
    }

    #[tracing::instrument(level = "debug", skip(self, trigger))]
    async fn publish_refreshed_event(
        &self,
        source: &SourceConfig,
        run_id: Uuid,
        trigger: &RefreshTrigger,
        records_pulled: u64,
        entities_stored: u64,
    ) -> Result<String> {
        let trigger_json = serde_json::to_value(trigger)
            .map_err(|e| Error::backend("serialize refresh trigger", e))?;
        let payload = serde_json::json!({
            "run_id": run_id.to_string(),
            "source_id": source.source_id.clone(),
            "connector_id": source.connector_id.clone(),
            "scope": source.scope.clone(),
            "records_pulled": records_pulled,
            "entities_stored": entities_stored,
            "trigger": trigger_json,
        });

        let dedupe_key = format!("context_refreshed:{}", run_id);
        let ev = Event::new(
            source.org_id.to_string(),
            Some(source.project_id.to_string()),
            EventDirection::Outbound,
            "context.refreshed",
            "system:context_refresh",
            payload,
            dedupe_key,
            serde_json::json!({}),
            None,
        )
        .map_err(|e| Error::InvalidInput(format!("invalid refreshed event: {e}")))?;

        self.event_bus
            .publish(ev)
            .await
            .map_err(|e| Error::backend("publish context.refreshed event", e))
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn process_records(
        &self,
        identity: &AgentIdentity,
        org_id: OrgId,
        source: &SourceConfig,
        connector: Arc<dyn Connector>,
        records: Vec<crate::context_refresh::traits::RawRecord>,
        resolved_source: &SourceConfig,
    ) -> Result<Vec<ContextEntity>> {
        match &source.processor {
            SourceProcessorSpec::Connector => connector.process(org_id, source, records).await,
            SourceProcessorSpec::Pipeline {
                spec,
                entities_step_id,
            } => {
                let Some(pipelines) = self.pipelines.clone() else {
                    return Err(Error::InvalidInput(
                        "source processor 'pipeline' requires pipelines runner".to_string(),
                    ));
                };

                // Make the stored spec tenant-correct and ensure agent steps can persist outputs.
                let mut spec = spec.clone();
                spec.org_id = source.org_id.to_string();
                inject_project_db_handle_into_pipeline_agents(&mut spec, &source.project_db);

                // Provide a stable contract for ingestion pipelines.
                let inputs = serde_json::json!({
                    "source": resolved_source,
                    "records": records,
                    "settings": resolved_source.settings,
                });

                let run = pipelines.run(&spec, inputs, identity).await?;
                let step = run
                    .step_results
                    .get(entities_step_id)
                    .ok_or_else(|| {
                        Error::InvalidInput(format!(
                            "pipeline did not produce step_results for entities_step_id='{entities_step_id}'"
                        ))
                    })?;

                if step.status != crate::pipelines::models::StepStatus::Succeeded {
                    return Err(Error::BackendMessage(format!(
                        "entities step did not succeed: step_id={entities_step_id} status={:?} error={:?}",
                        step.status, step.error
                    )));
                }

                let output = step.output.clone().unwrap_or(serde_json::Value::Null);
                let entities_val = output.get("entities").cloned().unwrap_or(output.clone());
                let arr = entities_val.as_array().ok_or_else(|| {
                    Error::InvalidInput(
                        "pipeline entities output must be an array or {\"entities\": [...]}"
                            .to_string(),
                    )
                })?;

                let now = Utc::now();
                let mut out: Vec<ContextEntity> = Vec::with_capacity(arr.len());
                for (idx, item) in arr.iter().enumerate() {
                    let obj = item.as_object().ok_or_else(|| {
                        Error::InvalidInput(format!(
                            "pipeline entity at index {idx} must be an object"
                        ))
                    })?;

                    let entity_type =
                        obj.get("entity_type")
                            .and_then(|v| v.as_str())
                            .ok_or_else(|| {
                                Error::InvalidInput(format!(
                                    "pipeline entity at index {idx} missing entity_type"
                                ))
                            })?;
                    let content = obj.get("content").cloned().ok_or_else(|| {
                        Error::InvalidInput(format!(
                            "pipeline entity at index {idx} missing content"
                        ))
                    })?;
                    let dedupe_key =
                        obj.get("dedupe_key")
                            .and_then(|v| v.as_str())
                            .ok_or_else(|| {
                                Error::InvalidInput(format!(
                                    "pipeline entity at index {idx} missing dedupe_key"
                                ))
                            })?;
                    let source_id = obj
                        .get("source_id")
                        .and_then(|v| v.as_str())
                        .unwrap_or(&source.source_id);

                    let acl = obj
                        .get("acl")
                        .and_then(|v| v.as_array())
                        .map(|items| {
                            items
                                .iter()
                                .filter_map(|s| s.as_str().map(|x| x.to_string()))
                                .collect::<Vec<_>>()
                        })
                        .unwrap_or_default();

                    out.push(ContextEntity::new(
                        source.org_id,
                        Some(source.project_id),
                        entity_type,
                        content,
                        source.connector_id.clone(),
                        source_id.to_string(),
                        dedupe_key.to_string(),
                        acl,
                        Some(now),
                    )?);
                }

                Ok(out)
            }
        }
    }
}

#[async_trait]
impl ContextRefresh for ContextRefreshEngine {
    #[tracing::instrument(level = "debug", skip(self, connector))]
    async fn register_connector(&self, connector: Arc<dyn Connector>) -> Result<()> {
        let id = connector.id().await.to_string();
        if id.trim().is_empty() {
            return Err(Error::InvalidInput("connector id is empty".to_string()));
        }
        let mut connectors = self.connectors.write().await;
        connectors.insert(id, connector);
        Ok(())
    }

    #[tracing::instrument(level = "info", skip(self, identity, source))]
    async fn register_source(
        &self,
        identity: &AgentIdentity,
        mut source: SourceConfig,
    ) -> Result<()> {
        // Ensure connector is registered before we persist the source.
        self.validate_source_has_connector(&source).await?;

        // Auto-merge credentials into source settings at registration time.
        // This resolves `$cred:connector_id` tokens in `settings` so the
        // connector can validate its config immediately, and the stored settings
        // are ready for use without manual credential merging by the caller.
        if let Some(cm) = &self.credential_manager {
            source.settings = cm.resolve(source.org_id, &source.settings).await?;
        }

        source.touch(Utc::now());
        self.central_db.upsert_source_config(&source).await?;

        // Audit.
        let entry = crate::models::AuditEntry {
            id: Uuid::new_v4(),
            org_id: source.org_id,
            project_id: Some(source.project_id),
            actor: identity.clone(),
            action: "context_refresh.source.upsert".to_string(),
            payload: serde_json::to_value(&source)
                .map_err(|e| Error::backend("serialize source config", e))?,
            outcome: "ok".to_string(),
            created_at: Utc::now(),
        };
        self.central_db.append_audit_entry(&entry).await?;

        Ok(())
    }

    #[tracing::instrument(level = "info", skip(self, identity))]
    async fn trigger_refresh(
        &self,
        identity: &AgentIdentity,
        org_id: OrgId,
        source_id: &str,
        trigger: RefreshTrigger,
    ) -> Result<RefreshResult> {
        if source_id.trim().is_empty() {
            return Err(Error::InvalidInput("source_id is empty".to_string()));
        }

        let source = self
            .central_db
            .get_source_config(org_id, source_id)
            .await?
            .ok_or_else(|| Error::NotFound(format!("source '{source_id}' not found")))?;

        if source.org_id != org_id {
            return Err(Error::Unauthorized(
                "source org_id does not match requested org_id".to_string(),
            ));
        }

        if !source.enabled {
            return Err(Error::Conflict(format!("source '{source_id}' is disabled")));
        }

        // Fetch connector.
        let connector = self.get_connector(&source.connector_id).await?;

        // Record a durable run entry early (crash recovery).
        let started_at = Utc::now();
        let mut run = RefreshRun::new_running(
            source.org_id,
            source.project_id,
            source.source_id.clone(),
            source.connector_id.clone(),
            trigger.clone(),
            started_at,
        )?;
        self.central_db.upsert_refresh_run(&run).await?;

        // Resolve credential tokens in source settings before pulling.
        let resolved_source = match &self.credential_manager {
            Some(cm) => {
                let resolved_settings = cm.resolve(org_id, &source.settings).await?;
                SourceConfig {
                    settings: resolved_settings,
                    ..source.clone()
                }
            }
            None => source.clone(),
        };

        // Pull and process.
        let cursor = self.load_cursor(&source).await?;
        let PullResult {
            records,
            next_cursor,
        } = connector.pull(org_id, &resolved_source, cursor).await?;
        let records_pulled = records.len() as u64;

        let entities = self
            .process_records(
                identity,
                org_id,
                &source,
                connector.clone(),
                records,
                &resolved_source,
            )
            .await?;

        // Store.
        let entities_stored = self.store_entities(org_id, &source, &entities).await?;

        // Save cursor.
        self.save_cursor(&source, next_cursor.clone()).await?;

        // Publish event.
        let published_event_id = match self
            .publish_refreshed_event(
                &source,
                run.run_id,
                &trigger,
                records_pulled,
                entities_stored,
            )
            .await
        {
            Ok(id) => Some(id),
            Err(e) => {
                // Mark run as failed if we can't publish. Storage succeeded, but downstream wasn't notified.
                run.status = RefreshRunStatus::Failed;
                run.finished_at = Some(Utc::now());
                run.records_pulled = records_pulled;
                run.entities_stored = entities_stored;
                run.error_message = Some(format!("publish failed: {e}"));
                run.cursor = next_cursor.as_ref().map(|c| c.value.clone());
                let _ = self.central_db.upsert_refresh_run(&run).await;

                // Audit.
                let entry = crate::models::AuditEntry {
                    id: Uuid::new_v4(),
                    org_id,
                    project_id: Some(source.project_id),
                    actor: identity.clone(),
                    action: "context_refresh.run".to_string(),
                    payload: serde_json::json!({
                        "run_id": run.run_id.to_string(),
                        "source_id": source.source_id.clone(),
                        "connector_id": source.connector_id.clone(),
                        "trigger": trigger,
                        "records_pulled": records_pulled,
                        "entities_stored": entities_stored,
                        "status": "failed",
                        "error": e.to_string(),
                    }),
                    outcome: "failed".to_string(),
                    created_at: Utc::now(),
                };
                let _ = self.central_db.append_audit_entry(&entry).await;

                return Err(e);
            }
        };

        // Update run record success.
        run.status = RefreshRunStatus::Succeeded;
        run.finished_at = Some(Utc::now());
        run.records_pulled = records_pulled;
        run.entities_stored = entities_stored;
        run.error_message = None;
        run.cursor = next_cursor.as_ref().map(|c| c.value.clone());
        self.central_db.upsert_refresh_run(&run).await?;

        // Audit.
        let entry = crate::models::AuditEntry {
            id: Uuid::new_v4(),
            org_id,
            project_id: Some(source.project_id),
            actor: identity.clone(),
            action: "context_refresh.run".to_string(),
            payload: serde_json::json!({
                "run_id": run.run_id.to_string(),
                "source_id": source.source_id.clone(),
                "connector_id": source.connector_id.clone(),
                "trigger": trigger,
                "records_pulled": records_pulled,
                "entities_stored": entities_stored,
                "status": "succeeded",
                "published_event_id": published_event_id,
            }),
            outcome: "ok".to_string(),
            created_at: Utc::now(),
        };
        self.central_db.append_audit_entry(&entry).await?;

        Ok(RefreshResult {
            run,
            published_event_id,
        })
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn get_status(&self, org_id: OrgId, source_id: &str) -> Result<RefreshStatus> {
        if source_id.trim().is_empty() {
            return Err(Error::InvalidInput("source_id is empty".to_string()));
        }

        let runs = self
            .central_db
            .list_refresh_runs(
                org_id,
                RefreshRunQuery {
                    source_id: Some(source_id.to_string()),
                    limit: 1,
                    ..Default::default()
                },
            )
            .await?;

        Ok(RefreshStatus {
            org_id,
            source_id: source_id.to_string(),
            last_run: runs.into_iter().next(),
            updated_at: Utc::now(),
        })
    }
}

fn inject_project_db_handle_into_pipeline_agents(
    spec: &mut PipelineSpec,
    handle: &crate::models::ProjectDbHandle,
) {
    for step in spec.steps.iter_mut() {
        if let StepKind::Agent { spec: agent_spec } = &mut step.kind {
            // Mirror the convenience behavior in `horizons_server/src/routes/pipelines.rs`.
            if let serde_json::Value::Object(m) = &mut agent_spec.context {
                m.entry("_project_db_handle".to_string())
                    .or_insert_with(|| {
                        serde_json::to_value(handle).unwrap_or(serde_json::Value::Null)
                    });
            } else if agent_spec.context.is_null() {
                agent_spec.context = serde_json::json!({ "_project_db_handle": handle });
            } else {
                agent_spec.context = serde_json::json!({ "_project_db_handle": handle, "context": agent_spec.context });
            }
        }
    }
}
