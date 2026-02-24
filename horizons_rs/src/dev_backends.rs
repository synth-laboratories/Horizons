use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures_util::StreamExt;
use horizons_core::context_refresh::engine::ContextRefreshEngine;
use horizons_core::context_refresh::models::{RefreshRun, RefreshRunQuery, SourceConfig};
use horizons_core::context_refresh::traits::{
    Connector, ContextRefresh as ContextRefreshTrait, PullResult, RawRecord,
};
use horizons_core::core_agents::executor::CoreAgentsExecutor;
use horizons_core::core_agents::mcp::McpScopeProvider;
use horizons_core::core_agents::mcp_gateway::{McpGateway, McpServerConfig, McpTransport};
use horizons_core::core_agents::traits::{
    AgentSpec as CoreAgentSpec, CoreAgents as CoreAgentsTrait,
};
#[cfg(feature = "evaluation")]
use horizons_core::evaluation::engine::EvaluationEngine;
#[cfg(feature = "evaluation")]
use horizons_core::evaluation::traits::{RewardSignal, SignalKind, SignalWeight, VerifierConfig};
#[cfg(feature = "evaluation")]
use horizons_core::evaluation::wiring::build_rlm_evaluator;
use horizons_core::events::bus::RedisEventBus;
use horizons_core::events::config::EventSyncConfig;
use horizons_core::events::models::{Event, EventQuery, EventStatus, Subscription};
use horizons_core::events::operations::CentralDbOperationExecutor;
use horizons_core::events::traits::EventBus;
use horizons_core::events::webhook::WebhookSender;
use horizons_core::events::{Error as EventsError, Result as EventsResult};
#[cfg(feature = "memory")]
use horizons_core::memory::traits::HorizonsMemory;
#[cfg(feature = "memory")]
use horizons_core::memory::wiring::{VoyagerBackedHorizonsMemory, build_voyager_memory};
use horizons_core::models::{
    AgentIdentity, AuditEntry, OrgId, PlatformConfig, ProjectDbHandle, ProjectId,
};
use horizons_core::onboard::config::{PostgresConfig, RedisConfig};
use horizons_core::onboard::postgres::PostgresCentralDb;
use horizons_core::onboard::redis::RedisCache;
use horizons_core::onboard::secrets::CredentialManager;
use horizons_core::onboard::traits::{
    AuditQuery, Cache, CentralDb, ConnectorCredential, Filestore, GraphStore, ListQuery, OrgRecord,
    OperationRecord, OperationRunRecord, ProjectDb, ProjectDbParam, ProjectDbRow, ProjectDbValue,
    ResourceRecord, SyncState, SyncStateKey, UserRecord, UserRole, VectorMatch, VectorStore,
};
#[cfg(feature = "optimization")]
use horizons_core::optimization::engine::OptimizationEngine;
#[cfg(feature = "optimization")]
use horizons_core::optimization::traits::{
    ExactMatchMetric, LlmClient as MiproLlmClient, VariantSampler as MiproVariantSampler,
};
#[cfg(feature = "optimization")]
use horizons_core::optimization::wiring::build_mipro_continual_learning;
use horizons_core::{Error as CoreError, Result as CoreResult};
use sqlx::PgPool;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::{Column, Row, SqlitePool, TypeInfo, ValueRef};
use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{RwLock, broadcast};
use tokio_stream::wrappers::BroadcastStream;
use uuid::Uuid;

use crate::server::AppState;

#[derive(Clone)]
struct StaticMcpScopeProvider {
    scopes: Vec<String>,
}

#[async_trait]
impl McpScopeProvider for StaticMcpScopeProvider {
    async fn scopes_for(&self, _identity: &AgentIdentity) -> CoreResult<Vec<String>> {
        Ok(self.scopes.clone())
    }
}

fn parse_mcp_transport(v: &serde_json::Value) -> anyhow::Result<McpTransport> {
    let obj = v
        .as_object()
        .ok_or_else(|| anyhow::anyhow!("mcp transport must be an object"))?;

    // Accept either:
    // 1) {"type":"stdio", "command":"...", "args":[...], "env":{...}}
    // 2) {"type":"http", "url":"...", "headers":{...}}
    // 3) {"stdio": {...}} or {"http": {...}}
    if let Some(t) = obj.get("type").and_then(|v| v.as_str()) {
        match t {
            "stdio" => {
                let command = obj
                    .get("command")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| anyhow::anyhow!("mcp stdio transport missing command"))?
                    .to_string();
                let args = obj
                    .get("args")
                    .and_then(|v| v.as_array())
                    .map(|a| {
                        a.iter()
                            .filter_map(|x| x.as_str().map(|s| s.to_string()))
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default();
                let env = obj
                    .get("env")
                    .and_then(|v| v.as_object())
                    .map(|m| {
                        m.iter()
                            .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                            .collect::<HashMap<String, String>>()
                    })
                    .unwrap_or_default();
                return Ok(McpTransport::Stdio { command, args, env });
            }
            "http" => {
                let url = obj
                    .get("url")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| anyhow::anyhow!("mcp http transport missing url"))?
                    .to_string();
                let headers = obj
                    .get("headers")
                    .and_then(|v| v.as_object())
                    .map(|m| {
                        m.iter()
                            .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                            .collect::<HashMap<String, String>>()
                    })
                    .unwrap_or_default();
                let call_path = obj
                    .get("call_path")
                    .and_then(|v| v.as_str())
                    .unwrap_or("/call")
                    .to_string();
                return Ok(McpTransport::Http {
                    url,
                    headers,
                    call_path,
                });
            }
            other => return Err(anyhow::anyhow!("unknown mcp transport type: {other}")),
        }
    }

    if let Some(stdio) = obj.get("stdio") {
        let mut inner = stdio.clone();
        if let Some(o) = inner.as_object_mut() {
            o.insert(
                "type".to_string(),
                serde_json::Value::String("stdio".to_string()),
            );
        }
        return parse_mcp_transport(&inner);
    }

    if let Some(http) = obj.get("http") {
        let mut inner = http.clone();
        if let Some(o) = inner.as_object_mut() {
            o.insert(
                "type".to_string(),
                serde_json::Value::String("http".to_string()),
            );
        }
        return parse_mcp_transport(&inner);
    }

    // Fallback: treat the object itself as a transport object with inferred type.
    if obj.contains_key("command") {
        let mut v = v.clone();
        if let Some(o) = v.as_object_mut() {
            o.insert(
                "type".to_string(),
                serde_json::Value::String("stdio".to_string()),
            );
        }
        return parse_mcp_transport(&v);
    }
    if obj.contains_key("url") {
        let mut v = v.clone();
        if let Some(o) = v.as_object_mut() {
            o.insert(
                "type".to_string(),
                serde_json::Value::String("http".to_string()),
            );
        }
        return parse_mcp_transport(&v);
    }

    Err(anyhow::anyhow!(
        "unable to infer mcp transport type (expected stdio/http)"
    ))
}

fn parse_mcp_config(s: &str) -> anyhow::Result<Vec<McpServerConfig>> {
    let v: serde_json::Value = serde_json::from_str(s)?;
    let mut out = Vec::new();

    match v {
        serde_json::Value::Array(arr) => {
            for item in arr {
                let obj = item
                    .as_object()
                    .ok_or_else(|| anyhow::anyhow!("mcp server entry must be an object"))?;
                let name = obj
                    .get("name")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| anyhow::anyhow!("mcp server missing name"))?
                    .to_string();
                let transport_val = obj
                    .get("transport")
                    .ok_or_else(|| anyhow::anyhow!("mcp server missing transport"))?;
                let transport = parse_mcp_transport(transport_val)?;
                out.push(McpServerConfig { name, transport });
            }
        }
        serde_json::Value::Object(map) => {
            for (name, cfg) in map {
                let transport_val = cfg.get("transport").unwrap_or(&cfg);
                let transport = parse_mcp_transport(transport_val)?;
                out.push(McpServerConfig { name, transport });
            }
        }
        _ => {
            return Err(anyhow::anyhow!(
                "HORIZONS_MCP_CONFIG must be array or object"
            ));
        }
    }

    Ok(out)
}

#[cfg(feature = "memory")]
#[derive(Debug)]
struct HashEmbedder {
    dim: usize,
}

#[cfg(feature = "memory")]
impl HashEmbedder {
    fn new(dim: usize) -> Self {
        Self { dim }
    }
}

#[cfg(feature = "memory")]
#[async_trait]
impl voyager::EmbeddingModel for HashEmbedder {
    async fn embed(&self, _scope: &voyager::Scope, text: &str) -> voyager::Result<Vec<f32>> {
        let mut v = vec![0.0f32; self.dim];
        for token in text.split_whitespace() {
            let mut h = 0u64;
            for b in token.as_bytes() {
                h = h.wrapping_mul(131).wrapping_add(*b as u64);
            }
            let idx = (h as usize) % self.dim;
            v[idx] += 1.0;
        }
        Ok(v)
    }

    fn name(&self) -> &'static str {
        "hash"
    }
}

#[cfg(feature = "memory")]
#[derive(Debug)]
struct MockSummarizer;

#[cfg(feature = "memory")]
#[async_trait]
impl voyager::summarizer::SummarizationModel for MockSummarizer {
    async fn summarize(
        &self,
        _scope: &voyager::Scope,
        items: &[voyager::MemoryItem],
    ) -> voyager::Result<String> {
        Ok(format!("summary of {} items", items.len()))
    }

    fn name(&self) -> &'static str {
        "mock"
    }
}

#[cfg(feature = "optimization")]
#[derive(Debug)]
struct DevMiproLlm;

#[cfg(feature = "optimization")]
#[async_trait]
impl MiproLlmClient for DevMiproLlm {
    async fn complete(&self, prompt: &str) -> mipro_v2::Result<String> {
        if prompt.contains("GOOD") {
            Ok("yes".to_string())
        } else {
            Ok("no".to_string())
        }
    }

    fn name(&self) -> &'static str {
        "dev-mock"
    }
}

struct DevNoopConnector;

#[async_trait]
impl Connector for DevNoopConnector {
    async fn id(&self) -> &'static str {
        "noop"
    }

    async fn pull(
        &self,
        _org_id: horizons_core::OrgId,
        _source: &horizons_core::context_refresh::models::SourceConfig,
        _cursor: Option<horizons_core::context_refresh::models::SyncCursor>,
    ) -> CoreResult<PullResult> {
        Ok(PullResult {
            records: Vec::new(),
            next_cursor: None,
        })
    }

    async fn process(
        &self,
        _org_id: horizons_core::OrgId,
        _source: &horizons_core::context_refresh::models::SourceConfig,
        _records: Vec<RawRecord>,
    ) -> CoreResult<Vec<horizons_core::context_refresh::models::ContextEntity>> {
        Ok(Vec::new())
    }
}

struct DevNoopAgent;

#[async_trait]
impl CoreAgentSpec for DevNoopAgent {
    async fn id(&self) -> String {
        "dev.noop".to_string()
    }

    async fn name(&self) -> String {
        "Dev Noop".to_string()
    }

    async fn run(
        &self,
        _ctx: horizons_core::core_agents::models::AgentContext,
        _inputs: Option<serde_json::Value>,
    ) -> CoreResult<horizons_core::core_agents::models::AgentOutcome> {
        Ok(horizons_core::core_agents::models::AgentOutcome::Proposals(Vec::new()))
    }
}

#[derive(Clone)]
pub struct DevCentralDb {
    orgs: Arc<RwLock<HashMap<OrgId, OrgRecord>>>,
    users: Arc<RwLock<HashMap<(OrgId, Uuid), UserRecord>>>,
    platform_config: Arc<RwLock<HashMap<OrgId, PlatformConfig>>>,
    audit: Arc<RwLock<HashMap<OrgId, Vec<AuditEntry>>>>,
    credentials: Arc<RwLock<HashMap<(OrgId, String), ConnectorCredential>>>,
    resources: Arc<RwLock<HashMap<(OrgId, String), ResourceRecord>>>,
    operations: Arc<RwLock<HashMap<(OrgId, String), OperationRecord>>>,
    operation_runs: Arc<RwLock<Vec<OperationRunRecord>>>,
    sync_state: Arc<RwLock<HashMap<String, SyncState>>>,
    sources: Arc<RwLock<HashMap<(OrgId, String), SourceConfig>>>,
    runs: Arc<RwLock<Vec<RefreshRun>>>,
}

impl DevCentralDb {
    #[tracing::instrument(level = "debug")]
    pub fn new() -> Self {
        Self {
            orgs: Arc::new(RwLock::new(HashMap::new())),
            users: Arc::new(RwLock::new(HashMap::new())),
            platform_config: Arc::new(RwLock::new(HashMap::new())),
            audit: Arc::new(RwLock::new(HashMap::new())),
            credentials: Arc::new(RwLock::new(HashMap::new())),
            resources: Arc::new(RwLock::new(HashMap::new())),
            operations: Arc::new(RwLock::new(HashMap::new())),
            operation_runs: Arc::new(RwLock::new(Vec::new())),
            sync_state: Arc::new(RwLock::new(HashMap::new())),
            sources: Arc::new(RwLock::new(HashMap::new())),
            runs: Arc::new(RwLock::new(Vec::new())),
        }
    }
}

fn sync_state_key_string(key: &SyncStateKey) -> String {
    let project = key
        .project_id
        .map(|p| p.to_string())
        .unwrap_or_else(|| "_central".to_string());
    format!(
        "{}/{}/{}/{}",
        key.org_id, project, key.connector_id, key.scope
    )
}

#[async_trait]
impl CentralDb for DevCentralDb {
    #[tracing::instrument(level = "debug", skip_all)]
    async fn upsert_org(&self, org: &OrgRecord) -> CoreResult<()> {
        self.orgs.write().await.insert(org.org_id, org.clone());
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn get_org(&self, org_id: OrgId) -> CoreResult<Option<OrgRecord>> {
        Ok(self.orgs.read().await.get(&org_id).cloned())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn upsert_user(&self, user: &UserRecord) -> CoreResult<()> {
        self.users
            .write()
            .await
            .insert((user.org_id, user.user_id), user.clone());
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn get_user(&self, org_id: OrgId, user_id: Uuid) -> CoreResult<Option<UserRecord>> {
        Ok(self.users.read().await.get(&(org_id, user_id)).cloned())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn list_users(&self, org_id: OrgId, query: ListQuery) -> CoreResult<Vec<UserRecord>> {
        let users = self.users.read().await;
        let mut rows: Vec<UserRecord> = users
            .values()
            .filter(|u| u.org_id == org_id)
            .cloned()
            .collect();
        rows.sort_by_key(|u| (u.created_at, u.user_id));
        Ok(rows
            .into_iter()
            .skip(query.offset)
            .take(query.limit)
            .collect())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn get_platform_config(&self, org_id: OrgId) -> CoreResult<Option<PlatformConfig>> {
        Ok(self.platform_config.read().await.get(&org_id).cloned())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn set_platform_config(&self, config: &PlatformConfig) -> CoreResult<()> {
        self.platform_config
            .write()
            .await
            .insert(config.org_id, config.clone());
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn append_audit_entry(&self, entry: &AuditEntry) -> CoreResult<()> {
        let mut audit = self.audit.write().await;
        audit.entry(entry.org_id).or_default().push(entry.clone());
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn list_audit_entries(
        &self,
        org_id: OrgId,
        query: AuditQuery,
    ) -> CoreResult<Vec<AuditEntry>> {
        let audit = self.audit.read().await;
        let mut entries = audit.get(&org_id).cloned().unwrap_or_default();

        if let Some(project_id) = query.project_id {
            entries.retain(|e| e.project_id == Some(project_id));
        }
        if let Some(prefix) = query.action_prefix.as_deref() {
            entries.retain(|e| e.action.starts_with(prefix));
        }
        if let Some(needle) = query.actor_contains.as_deref() {
            entries.retain(|e| {
                serde_json::to_string(&e.actor)
                    .unwrap_or_default()
                    .contains(needle)
            });
        }
        if let Some(since) = query.since {
            entries.retain(|e| e.created_at >= since);
        }
        if let Some(until) = query.until {
            entries.retain(|e| e.created_at <= until);
        }

        entries.sort_by_key(|e| (e.created_at, e.id));
        entries.reverse();

        Ok(entries.into_iter().take(query.limit).collect())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn upsert_connector_credential(
        &self,
        credential: &ConnectorCredential,
    ) -> CoreResult<()> {
        self.credentials.write().await.insert(
            (credential.org_id, credential.connector_id.clone()),
            credential.clone(),
        );
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn get_connector_credential(
        &self,
        org_id: OrgId,
        connector_id: &str,
    ) -> CoreResult<Option<ConnectorCredential>> {
        Ok(self
            .credentials
            .read()
            .await
            .get(&(org_id, connector_id.to_string()))
            .cloned())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn delete_connector_credential(
        &self,
        org_id: OrgId,
        connector_id: &str,
    ) -> CoreResult<()> {
        self.credentials
            .write()
            .await
            .remove(&(org_id, connector_id.to_string()));
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn list_connector_credentials(
        &self,
        org_id: OrgId,
    ) -> CoreResult<Vec<ConnectorCredential>> {
        let creds = self.credentials.read().await;
        Ok(creds
            .iter()
            .filter(|((oid, _), _)| *oid == org_id)
            .map(|(_, v)| v.clone())
            .collect())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn upsert_resource(&self, resource: &ResourceRecord) -> CoreResult<()> {
        self.resources.write().await.insert(
            (resource.org_id, resource.resource_id.clone()),
            resource.clone(),
        );
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn get_resource(
        &self,
        org_id: OrgId,
        resource_id: &str,
    ) -> CoreResult<Option<ResourceRecord>> {
        Ok(self
            .resources
            .read()
            .await
            .get(&(org_id, resource_id.to_string()))
            .cloned())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn list_resources(&self, org_id: OrgId, query: ListQuery) -> CoreResult<Vec<ResourceRecord>> {
        let resources = self.resources.read().await;
        let mut rows: Vec<ResourceRecord> = resources
            .values()
            .filter(|r| r.org_id == org_id)
            .cloned()
            .collect();
        rows.sort_by_key(|r| (r.updated_at, r.resource_id.clone()));
        rows.reverse();
        Ok(rows
            .into_iter()
            .skip(query.offset)
            .take(query.limit)
            .collect())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn upsert_operation(&self, operation: &OperationRecord) -> CoreResult<()> {
        self.operations.write().await.insert(
            (operation.org_id, operation.operation_id.clone()),
            operation.clone(),
        );
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn get_operation(
        &self,
        org_id: OrgId,
        operation_id: &str,
    ) -> CoreResult<Option<OperationRecord>> {
        Ok(self
            .operations
            .read()
            .await
            .get(&(org_id, operation_id.to_string()))
            .cloned())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn list_operations(&self, org_id: OrgId, query: ListQuery) -> CoreResult<Vec<OperationRecord>> {
        let operations = self.operations.read().await;
        let mut rows: Vec<OperationRecord> = operations
            .values()
            .filter(|o| o.org_id == org_id)
            .cloned()
            .collect();
        rows.sort_by_key(|o| (o.updated_at, o.operation_id.clone()));
        rows.reverse();
        Ok(rows
            .into_iter()
            .skip(query.offset)
            .take(query.limit)
            .collect())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn append_operation_run(&self, run: &OperationRunRecord) -> CoreResult<()> {
        self.operation_runs.write().await.push(run.clone());
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn list_operation_runs(
        &self,
        org_id: OrgId,
        operation_id: Option<&str>,
        query: ListQuery,
    ) -> CoreResult<Vec<OperationRunRecord>> {
        let runs = self.operation_runs.read().await;
        let mut out: Vec<OperationRunRecord> = runs
            .iter()
            .filter(|r| r.org_id == org_id)
            .filter(|r| operation_id.map(|op| r.operation_id == op).unwrap_or(true))
            .cloned()
            .collect();
        out.sort_by_key(|r| (r.created_at, r.id));
        out.reverse();
        Ok(out
            .into_iter()
            .skip(query.offset)
            .take(query.limit)
            .collect())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn upsert_sync_state(&self, state: &SyncState) -> CoreResult<()> {
        let k = sync_state_key_string(&state.key);
        self.sync_state.write().await.insert(k, state.clone());
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn get_sync_state(&self, key: &SyncStateKey) -> CoreResult<Option<SyncState>> {
        let k = sync_state_key_string(key);
        Ok(self.sync_state.read().await.get(&k).cloned())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn delete_sync_state(&self, key: &SyncStateKey) -> CoreResult<()> {
        let k = sync_state_key_string(key);
        self.sync_state.write().await.remove(&k);
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn upsert_source_config(&self, source: &SourceConfig) -> CoreResult<()> {
        self.sources
            .write()
            .await
            .insert((source.org_id, source.source_id.clone()), source.clone());
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn get_source_config(
        &self,
        org_id: OrgId,
        source_id: &str,
    ) -> CoreResult<Option<SourceConfig>> {
        Ok(self
            .sources
            .read()
            .await
            .get(&(org_id, source_id.to_string()))
            .cloned())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn list_source_configs(
        &self,
        org_id: OrgId,
        query: ListQuery,
    ) -> CoreResult<Vec<SourceConfig>> {
        let sources = self.sources.read().await;
        let mut out: Vec<SourceConfig> = sources
            .values()
            .filter(|s| s.org_id == org_id)
            .cloned()
            .collect();
        out.sort_by_key(|s| s.source_id.clone());
        Ok(out
            .into_iter()
            .skip(query.offset)
            .take(query.limit)
            .collect())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn delete_source_config(&self, org_id: OrgId, source_id: &str) -> CoreResult<()> {
        self.sources
            .write()
            .await
            .remove(&(org_id, source_id.to_string()));
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn upsert_refresh_run(&self, run: &RefreshRun) -> CoreResult<()> {
        let mut runs = self.runs.write().await;
        if let Some(existing) = runs.iter_mut().find(|r| r.run_id == run.run_id) {
            *existing = run.clone();
        } else {
            runs.push(run.clone());
        }
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn list_refresh_runs(
        &self,
        org_id: OrgId,
        query: RefreshRunQuery,
    ) -> CoreResult<Vec<RefreshRun>> {
        let runs = self.runs.read().await;
        let mut out: Vec<RefreshRun> = runs
            .iter()
            .filter(|r| r.org_id == org_id)
            .filter(|r| query.source_id.as_ref().map_or(true, |s| &r.source_id == s))
            .cloned()
            .collect();
        out.sort_by(|a, b| b.started_at.cmp(&a.started_at));
        let start = query.offset.min(out.len());
        let end = (start + query.limit).min(out.len());
        Ok(out[start..end].to_vec())
    }
}

#[derive(Clone)]
pub struct DevProjectDb {
    root: PathBuf,
    handles: Arc<RwLock<HashMap<(OrgId, ProjectId), ProjectDbHandle>>>,
    pools: Arc<RwLock<HashMap<(OrgId, ProjectId), SqlitePool>>>,
}

impl DevProjectDb {
    #[tracing::instrument(level = "debug")]
    pub async fn new(root: PathBuf) -> anyhow::Result<Self> {
        tokio::fs::create_dir_all(&root).await?;
        Ok(Self {
            root,
            handles: Arc::new(RwLock::new(HashMap::new())),
            pools: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    fn db_path(&self, org_id: OrgId, project_id: ProjectId) -> PathBuf {
        self.root
            .join(org_id.to_string())
            .join(format!("{}.db", project_id))
    }

    async fn pool_for(&self, org_id: OrgId, project_id: ProjectId) -> CoreResult<SqlitePool> {
        if let Some(pool) = self.pools.read().await.get(&(org_id, project_id)).cloned() {
            return Ok(pool);
        }

        let path = self.db_path(org_id, project_id);
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|e| CoreError::backend("create sqlite dir", e))?;
        }

        let opts = SqliteConnectOptions::new()
            .filename(&path)
            .create_if_missing(true);
        let pool = SqlitePoolOptions::new()
            .max_connections(5)
            .connect_with(opts)
            .await
            .map_err(|e| CoreError::backend("connect sqlite", e))?;

        self.pools
            .write()
            .await
            .insert((org_id, project_id), pool.clone());
        Ok(pool)
    }

    fn decode_row(row: &sqlx::sqlite::SqliteRow) -> ProjectDbRow {
        let mut out = BTreeMap::new();
        for col in row.columns() {
            let name = col.name().to_string();
            let idx = col.ordinal();
            let raw = row.try_get_raw(idx);
            let v = match raw {
                Ok(raw) if raw.is_null() => ProjectDbValue::Null,
                Ok(raw) => {
                    let type_name = raw.type_info().name().to_ascii_uppercase();
                    match type_name.as_str() {
                        "INTEGER" => row
                            .try_get::<i64, _>(idx)
                            .map(ProjectDbValue::I64)
                            .unwrap_or(ProjectDbValue::Null),
                        "REAL" => row
                            .try_get::<f64, _>(idx)
                            .map(ProjectDbValue::F64)
                            .unwrap_or(ProjectDbValue::Null),
                        "TEXT" => row
                            .try_get::<String, _>(idx)
                            .map(ProjectDbValue::String)
                            .unwrap_or(ProjectDbValue::Null),
                        "BLOB" => row
                            .try_get::<Vec<u8>, _>(idx)
                            .map(ProjectDbValue::Bytes)
                            .unwrap_or(ProjectDbValue::Null),
                        _ => row
                            .try_get::<String, _>(idx)
                            .map(ProjectDbValue::String)
                            .unwrap_or(ProjectDbValue::Null),
                    }
                }
                Err(_) => ProjectDbValue::Null,
            };
            out.insert(name, v);
        }
        out
    }
}

#[async_trait]
impl ProjectDb for DevProjectDb {
    #[tracing::instrument(level = "debug", skip_all)]
    async fn provision(&self, org_id: OrgId, project_id: ProjectId) -> CoreResult<ProjectDbHandle> {
        let pool = self.pool_for(org_id, project_id).await?;
        // Ensure DB file exists by running a no-op query.
        sqlx::query("SELECT 1")
            .execute(&pool)
            .await
            .map_err(|e| CoreError::backend("sqlite select 1", e))?;

        let handle = ProjectDbHandle {
            org_id,
            project_id,
            connection_url: format!("sqlite://{}", self.db_path(org_id, project_id).display()),
            auth_token: None,
        };
        self.handles
            .write()
            .await
            .insert((org_id, project_id), handle.clone());
        Ok(handle)
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn get_handle(
        &self,
        org_id: OrgId,
        project_id: ProjectId,
    ) -> CoreResult<Option<ProjectDbHandle>> {
        Ok(self
            .handles
            .read()
            .await
            .get(&(org_id, project_id))
            .cloned())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn list_projects(
        &self,
        org_id: OrgId,
        query: ListQuery,
    ) -> CoreResult<Vec<ProjectDbHandle>> {
        let handles = self.handles.read().await;
        let mut rows: Vec<ProjectDbHandle> = handles
            .values()
            .filter(|h| h.org_id == org_id)
            .cloned()
            .collect();
        rows.sort_by_key(|h| h.project_id.to_string());
        Ok(rows
            .into_iter()
            .skip(query.offset)
            .take(query.limit)
            .collect())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn query(
        &self,
        org_id: OrgId,
        handle: &ProjectDbHandle,
        sql: &str,
        params: &[ProjectDbParam],
    ) -> CoreResult<Vec<ProjectDbRow>> {
        horizons_core::onboard::traits::ensure_handle_org(handle, org_id)?;
        let pool = self.pool_for(org_id, handle.project_id).await?;
        let mut q = sqlx::query(sql);
        for p in params {
            q = match p {
                ProjectDbParam::Null => q.bind(Option::<String>::None),
                ProjectDbParam::Bool(v) => q.bind(*v),
                ProjectDbParam::I64(v) => q.bind(*v),
                ProjectDbParam::F64(v) => q.bind(*v),
                ProjectDbParam::String(v) => q.bind(v),
                ProjectDbParam::Bytes(v) => q.bind(v),
                ProjectDbParam::Json(v) => q.bind(v),
            };
        }
        let rows = q
            .fetch_all(&pool)
            .await
            .map_err(|e| CoreError::backend("sqlite query", e))?;
        Ok(rows.iter().map(Self::decode_row).collect())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn execute(
        &self,
        org_id: OrgId,
        handle: &ProjectDbHandle,
        sql: &str,
        params: &[ProjectDbParam],
    ) -> CoreResult<u64> {
        horizons_core::onboard::traits::ensure_handle_org(handle, org_id)?;
        let pool = self.pool_for(org_id, handle.project_id).await?;
        let mut q = sqlx::query(sql);
        for p in params {
            q = match p {
                ProjectDbParam::Null => q.bind(Option::<String>::None),
                ProjectDbParam::Bool(v) => q.bind(*v),
                ProjectDbParam::I64(v) => q.bind(*v),
                ProjectDbParam::F64(v) => q.bind(*v),
                ProjectDbParam::String(v) => q.bind(v),
                ProjectDbParam::Bytes(v) => q.bind(v),
                ProjectDbParam::Json(v) => q.bind(v),
            };
        }
        let res = q
            .execute(&pool)
            .await
            .map_err(|e| CoreError::backend("sqlite execute", e))?;
        Ok(res.rows_affected())
    }
}

#[derive(Clone)]
pub struct DevFilestore {
    root: PathBuf,
}

impl DevFilestore {
    #[tracing::instrument(level = "debug")]
    pub async fn new(root: PathBuf) -> anyhow::Result<Self> {
        tokio::fs::create_dir_all(&root).await?;
        Ok(Self { root })
    }

    fn validate_key(key: &str) -> CoreResult<()> {
        if key.trim().is_empty() {
            return Err(CoreError::InvalidInput("key is empty".to_string()));
        }
        if key.starts_with('/') || key.contains("..") {
            return Err(CoreError::InvalidInput("invalid key".to_string()));
        }
        Ok(())
    }

    fn path_for(
        &self,
        org_id: OrgId,
        project_id: Option<ProjectId>,
        key: &str,
    ) -> CoreResult<PathBuf> {
        Self::validate_key(key)?;
        let base = match project_id {
            Some(pid) => self.root.join(org_id.to_string()).join(pid.to_string()),
            None => self.root.join(org_id.to_string()).join("_central"),
        };
        Ok(base.join(key))
    }
}

#[async_trait]
impl Filestore for DevFilestore {
    #[tracing::instrument(level = "debug", skip_all)]
    async fn put(
        &self,
        org_id: OrgId,
        project_id: Option<ProjectId>,
        key: &str,
        data: Bytes,
    ) -> CoreResult<()> {
        let path = self.path_for(org_id, project_id, key)?;
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|e| CoreError::backend("create filestore dir", e))?;
        }
        tokio::fs::write(&path, &data)
            .await
            .map_err(|e| CoreError::backend("filestore write", e))?;
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn get(
        &self,
        org_id: OrgId,
        project_id: Option<ProjectId>,
        key: &str,
    ) -> CoreResult<Option<Bytes>> {
        let path = self.path_for(org_id, project_id, key)?;
        match tokio::fs::read(&path).await {
            Ok(bytes) => Ok(Some(Bytes::from(bytes))),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(CoreError::backend("filestore read", e)),
        }
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn list(
        &self,
        org_id: OrgId,
        project_id: Option<ProjectId>,
        prefix: &str,
    ) -> CoreResult<Vec<String>> {
        let base = match project_id {
            Some(pid) => self.root.join(org_id.to_string()).join(pid.to_string()),
            None => self.root.join(org_id.to_string()).join("_central"),
        };
        let base_prefix = if prefix.trim().is_empty() {
            base.clone()
        } else {
            base.join(prefix)
        };
        let mut out = Vec::new();
        if tokio::fs::metadata(&base_prefix).await.is_err() {
            return Ok(out);
        }

        let mut stack = vec![base_prefix];
        while let Some(dir) = stack.pop() {
            let mut rd = tokio::fs::read_dir(&dir)
                .await
                .map_err(|e| CoreError::backend("filestore read_dir", e))?;
            while let Some(ent) = rd
                .next_entry()
                .await
                .map_err(|e| CoreError::backend("filestore next_entry", e))?
            {
                let path = ent.path();
                let meta = ent
                    .metadata()
                    .await
                    .map_err(|e| CoreError::backend("filestore metadata", e))?;
                if meta.is_dir() {
                    stack.push(path);
                } else if meta.is_file() {
                    if let Ok(rel) = path.strip_prefix(&base) {
                        out.push(rel.to_string_lossy().to_string());
                    }
                }
            }
        }
        out.sort();
        Ok(out)
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn delete(
        &self,
        org_id: OrgId,
        project_id: Option<ProjectId>,
        key: &str,
    ) -> CoreResult<()> {
        let path = self.path_for(org_id, project_id, key)?;
        match tokio::fs::remove_file(&path).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(CoreError::backend("filestore delete", e)),
        }
    }
}

#[derive(Clone)]
pub struct DevCache {
    data: Arc<RwLock<HashMap<String, (Bytes, Option<DateTime<Utc>>)>>>,
    channels: Arc<RwLock<HashMap<String, broadcast::Sender<Bytes>>>>,
}

impl DevCache {
    #[tracing::instrument(level = "debug")]
    pub fn new() -> Self {
        Self {
            data: Arc::new(RwLock::new(HashMap::new())),
            channels: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    fn scoped_key(org_id: OrgId, project_id: Option<ProjectId>, key: &str) -> String {
        match project_id {
            Some(pid) => format!("{org_id}/{pid}/{key}"),
            None => format!("{org_id}/_central/{key}"),
        }
    }

    async fn channel(
        &self,
        org_id: OrgId,
        project_id: Option<ProjectId>,
        name: &str,
    ) -> broadcast::Sender<Bytes> {
        let scoped = Self::scoped_key(org_id, project_id, name);
        if let Some(tx) = self.channels.read().await.get(&scoped).cloned() {
            return tx;
        }
        let mut chans = self.channels.write().await;
        chans
            .entry(scoped)
            .or_insert_with(|| broadcast::channel(1024).0)
            .clone()
    }
}

#[async_trait]
impl Cache for DevCache {
    #[tracing::instrument(level = "debug", skip_all)]
    async fn get(
        &self,
        org_id: OrgId,
        project_id: Option<ProjectId>,
        key: &str,
    ) -> CoreResult<Option<Bytes>> {
        let sk = Self::scoped_key(org_id, project_id, key);
        let mut map = self.data.write().await;
        let Some((val, expires)) = map.get(&sk).cloned() else {
            return Ok(None);
        };
        if let Some(expires) = expires {
            if Utc::now() >= expires {
                map.remove(&sk);
                return Ok(None);
            }
        }
        Ok(Some(val))
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn set(
        &self,
        org_id: OrgId,
        project_id: Option<ProjectId>,
        key: &str,
        value: Bytes,
        ttl: Option<Duration>,
    ) -> CoreResult<()> {
        let sk = Self::scoped_key(org_id, project_id, key);
        let expires = ttl.map(|d| Utc::now() + chrono::Duration::from_std(d).unwrap_or_default());
        self.data.write().await.insert(sk, (value, expires));
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn del(
        &self,
        org_id: OrgId,
        project_id: Option<ProjectId>,
        key: &str,
    ) -> CoreResult<u64> {
        let sk = Self::scoped_key(org_id, project_id, key);
        Ok(self.data.write().await.remove(&sk).map(|_| 1).unwrap_or(0))
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn publish(
        &self,
        org_id: OrgId,
        project_id: Option<ProjectId>,
        channel: &str,
        message: Bytes,
    ) -> CoreResult<()> {
        let tx = self.channel(org_id, project_id, channel).await;
        let _ = tx.send(message);
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn subscribe(
        &self,
        org_id: OrgId,
        project_id: Option<ProjectId>,
        channel: &str,
    ) -> CoreResult<horizons_core::onboard::traits::BytesStream> {
        let tx = self.channel(org_id, project_id, channel).await;
        let rx = tx.subscribe();
        let stream = BroadcastStream::new(rx).filter_map(|item| async move {
            match item {
                Ok(bytes) => Some(Ok(bytes)),
                Err(_) => None,
            }
        });
        Ok(Box::pin(stream))
    }
}

#[derive(Clone)]
pub struct DevGraphStore {
    nodes: Arc<RwLock<Vec<serde_json::Value>>>,
    edges: Arc<RwLock<Vec<serde_json::Value>>>,
}

impl DevGraphStore {
    #[tracing::instrument(level = "debug")]
    pub fn new() -> Self {
        Self {
            nodes: Arc::new(RwLock::new(Vec::new())),
            edges: Arc::new(RwLock::new(Vec::new())),
        }
    }
}

#[async_trait]
impl GraphStore for DevGraphStore {
    #[tracing::instrument(level = "debug", skip_all)]
    async fn query(
        &self,
        _org_id: OrgId,
        _project_id: Option<ProjectId>,
        cypher: &str,
        _params: serde_json::Value,
    ) -> CoreResult<Vec<serde_json::Value>> {
        // Dev backend: return nodes by default, edges if the query looks edge-focused.
        let cypher_lc = cypher.to_ascii_lowercase();
        if cypher_lc.contains("edge") || cypher_lc.contains("[r]") || cypher_lc.contains("rel") {
            return Ok(self.edges.read().await.clone());
        }
        Ok(self.nodes.read().await.clone())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn upsert_node(
        &self,
        _org_id: OrgId,
        _project_id: Option<ProjectId>,
        label: &str,
        identity: serde_json::Value,
        props: serde_json::Value,
    ) -> CoreResult<String> {
        let id = ulid::Ulid::new().to_string();
        let node = serde_json::json!({
            "id": id,
            "label": label,
            "identity": identity,
            "props": props,
        });
        self.nodes.write().await.push(node);
        Ok(id)
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn upsert_edge(
        &self,
        _org_id: OrgId,
        _project_id: Option<ProjectId>,
        from: &str,
        to: &str,
        rel: &str,
        props: serde_json::Value,
    ) -> CoreResult<()> {
        let edge = serde_json::json!({
            "from": from,
            "to": to,
            "rel": rel,
            "props": props,
        });
        self.edges.write().await.push(edge);
        Ok(())
    }
}

#[derive(Clone)]
pub struct DevVectorStore {
    // key: org/project/collection/id
    items: Arc<RwLock<HashMap<String, (Vec<f32>, serde_json::Value)>>>,
}

impl DevVectorStore {
    #[tracing::instrument(level = "debug")]
    pub fn new() -> Self {
        Self {
            items: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    fn scoped_key(
        org_id: OrgId,
        project_id: Option<ProjectId>,
        collection: &str,
        id: &str,
    ) -> String {
        match project_id {
            Some(pid) => format!("{org_id}/{pid}/{collection}/{id}"),
            None => format!("{org_id}/_central/{collection}/{id}"),
        }
    }

    fn cosine(a: &[f32], b: &[f32]) -> f32 {
        let mut dot = 0.0;
        let mut na = 0.0;
        let mut nb = 0.0;
        for (x, y) in a.iter().zip(b.iter()) {
            dot += x * y;
            na += x * x;
            nb += y * y;
        }
        if na == 0.0 || nb == 0.0 {
            0.0
        } else {
            dot / (na.sqrt() * nb.sqrt())
        }
    }
}

#[async_trait]
impl VectorStore for DevVectorStore {
    #[tracing::instrument(level = "debug", skip_all)]
    async fn upsert(
        &self,
        org_id: OrgId,
        project_id: Option<ProjectId>,
        collection: &str,
        id: &str,
        embedding: Vec<f32>,
        metadata: serde_json::Value,
    ) -> CoreResult<()> {
        let key = Self::scoped_key(org_id, project_id, collection, id);
        self.items.write().await.insert(key, (embedding, metadata));
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn search(
        &self,
        org_id: OrgId,
        project_id: Option<ProjectId>,
        collection: &str,
        embedding: Vec<f32>,
        limit: usize,
        _filter: Option<serde_json::Value>,
    ) -> CoreResult<Vec<VectorMatch>> {
        let prefix = match project_id {
            Some(pid) => format!("{org_id}/{pid}/{collection}/"),
            None => format!("{org_id}/_central/{collection}/"),
        };
        let items = self.items.read().await;
        let mut matches: Vec<VectorMatch> = items
            .iter()
            .filter(|(k, _)| k.starts_with(&prefix))
            .map(|(k, (vec, meta))| VectorMatch {
                id: k[prefix.len()..].to_string(),
                score: Self::cosine(&embedding, vec),
                metadata: meta.clone(),
            })
            .collect();
        matches.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        matches.truncate(limit);
        Ok(matches)
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn delete(
        &self,
        org_id: OrgId,
        project_id: Option<ProjectId>,
        collection: &str,
        id: &str,
    ) -> CoreResult<()> {
        let key = Self::scoped_key(org_id, project_id, collection, id);
        self.items.write().await.remove(&key);
        Ok(())
    }
}

#[derive(Clone)]
pub struct DevEventBus {
    events: Arc<RwLock<Vec<Event>>>,
    subs: Arc<RwLock<Vec<Subscription>>>,
}

impl DevEventBus {
    #[tracing::instrument(level = "debug")]
    pub fn new() -> Self {
        Self {
            events: Arc::new(RwLock::new(Vec::new())),
            subs: Arc::new(RwLock::new(Vec::new())),
        }
    }
}

#[async_trait]
impl EventBus for DevEventBus {
    #[tracing::instrument(level = "debug", skip_all)]
    async fn publish(&self, event: Event) -> EventsResult<String> {
        let id = event.id.clone();
        self.events.write().await.push(event);
        Ok(id)
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn subscribe(&self, sub: Subscription) -> EventsResult<String> {
        let id = sub.id.clone();
        self.subs.write().await.push(sub);
        Ok(id)
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn unsubscribe(&self, org_id: &str, sub_id: &str) -> EventsResult<()> {
        let mut subs = self.subs.write().await;
        subs.retain(|s| !(s.org_id == org_id && s.id == sub_id));
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn ack(&self, org_id: &str, event_id: &str) -> EventsResult<()> {
        let mut events = self.events.write().await;
        let Some(e) = events
            .iter_mut()
            .find(|e| e.org_id == org_id && e.id == event_id)
        else {
            return Err(EventsError::message("event not found"));
        };
        e.status = EventStatus::Delivered;
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn nack(&self, org_id: &str, event_id: &str, _reason: &str) -> EventsResult<()> {
        let mut events = self.events.write().await;
        let Some(e) = events
            .iter_mut()
            .find(|e| e.org_id == org_id && e.id == event_id)
        else {
            return Err(EventsError::message("event not found"));
        };
        e.retry_count += 1;
        e.status = if e.retry_count > 5 {
            EventStatus::DeadLettered
        } else {
            EventStatus::Failed
        };
        e.last_attempt_at = Some(Utc::now());
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn query(&self, filter: EventQuery) -> EventsResult<Vec<Event>> {
        filter.validate()?;
        let events = self.events.read().await;
        let mut out: Vec<Event> = events
            .iter()
            .filter(|e| e.org_id == filter.org_id)
            .cloned()
            .collect();

        if let Some(pid) = filter.project_id.as_deref() {
            out.retain(|e| e.project_id.as_deref() == Some(pid));
        }
        if let Some(topic) = filter.topic.as_deref() {
            out.retain(|e| e.topic == topic);
        }
        if let Some(dir) = filter.direction.clone() {
            out.retain(|e| e.direction == dir);
        }
        if let Some(status) = filter.status.clone() {
            out.retain(|e| e.status == status);
        }
        if let Some(since) = filter.since {
            out.retain(|e| e.timestamp >= since);
        }
        if let Some(until) = filter.until {
            out.retain(|e| e.timestamp <= until);
        }

        out.sort_by_key(|e| (e.timestamp, e.id.clone()));
        out.reverse();
        out.truncate(filter.limit);
        Ok(out)
    }
}

#[tracing::instrument(level = "debug", skip_all)]
pub async fn build_dev_state(data_dir: impl AsRef<Path>) -> anyhow::Result<AppState> {
    let data_dir = data_dir.as_ref().to_path_buf();
    tokio::fs::create_dir_all(&data_dir).await?;

    let central_db_url = std::env::var("DATABASE_URL")
        .or_else(|_| std::env::var("HORIZONS_CENTRAL_DB_URL"))
        .ok();
    let redis_url = std::env::var("REDIS_URL")
        .or_else(|_| std::env::var("HORIZONS_REDIS_URL"))
        .ok();

    let central_db: Arc<dyn CentralDb> = if let Some(url) = central_db_url.clone() {
        let cfg = PostgresConfig {
            url,
            max_connections: 10,
            acquire_timeout: Duration::from_secs(5),
        };
        let db = PostgresCentralDb::connect(&cfg).await?;
        db.migrate().await?;
        Arc::new(db)
    } else {
        Arc::new(DevCentralDb::new())
    };

    // Secrets: local dev uses a file-backed 32-byte master key under the data dir.
    let master_key_path = data_dir.join("master.key");
    let master_key = CredentialManager::generate_or_load_key(&master_key_path)
        .map_err(|e| anyhow::anyhow!("master key: {e}"))?;
    let credential_manager = Arc::new(CredentialManager::new(central_db.clone(), &master_key));

    let project_db = Arc::new(DevProjectDb::new(data_dir.join("project_dbs")).await?);
    let filestore = Arc::new(DevFilestore::new(data_dir.join("files")).await?);
    let cache: Arc<dyn Cache> = if let Some(url) = redis_url.clone() {
        let cfg = RedisConfig {
            url,
            key_prefix: None,
        };
        Arc::new(RedisCache::new(&cfg).await?)
    } else {
        Arc::new(DevCache::new())
    };
    let graph_store = Arc::new(DevGraphStore::new());
    let vector_store: Arc<dyn VectorStore> = if let Some(url) =
        std::env::var("HORIZONS_VECTOR_DB_URL")
            .ok()
            .or_else(|| central_db_url.clone())
    {
        let dim = std::env::var("HORIZONS_VECTOR_DIM")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(64);
        let pool = PgPool::connect(&url).await?;
        Arc::new(horizons_integrations::vector::pgvector::PgVectorStore::new(
            pool, dim,
        ))
    } else {
        Arc::new(DevVectorStore::new())
    };
    let event_bus: Arc<dyn EventBus> =
        if let (Some(pg), Some(redis)) = (central_db_url.clone(), redis_url.clone()) {
            let mut cfg = EventSyncConfig::default();
            cfg.postgres_url = pg;
            cfg.redis_url = redis;
            let op_exec = Arc::new(CentralDbOperationExecutor::new(
                central_db.clone(),
                Some(credential_manager.clone()),
                WebhookSender::new(
                    std::time::Duration::from_millis(cfg.outbound_webhook_timeout_ms),
                    None,
                ),
            )) as Arc<dyn horizons_core::events::operations::OperationExecutor>;
            Arc::new(RedisEventBus::connect(cfg, Some(op_exec)).await?)
        } else {
            Arc::new(DevEventBus::new())
        };

    // Near-term MCP scope provider is env-driven.
    // Used both by the MCP gateway and `ReviewMode::McpAuth` action gates.
    let mcp_scopes = std::env::var("HORIZONS_MCP_SCOPES")
        .ok()
        .unwrap_or_default()
        .split(',')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect::<Vec<_>>();
    let mcp_scope_provider: Arc<dyn McpScopeProvider> =
        Arc::new(StaticMcpScopeProvider { scopes: mcp_scopes });

    let context_refresh: Arc<dyn ContextRefreshTrait> = Arc::new(ContextRefreshEngine::new(
        central_db.clone(),
        project_db.clone(),
        event_bus.clone(),
    )
    .with_credential_manager(credential_manager.clone()));
    context_refresh
        .register_connector(Arc::new(DevNoopConnector))
        .await?;

    let core_agents = Arc::new(CoreAgentsExecutor::new(
        central_db.clone(),
        project_db.clone(),
        event_bus.clone(),
        None,
    )
    .with_credential_manager(credential_manager.clone())
    .with_mcp_scope_provider(mcp_scope_provider.clone()));
    core_agents.register_agent(Arc::new(DevNoopAgent)).await?;

    let mcp_gateway: Option<Arc<McpGateway>> = if let Ok(cfg) = std::env::var("HORIZONS_MCP_CONFIG")
    {
        let gateway = Arc::new(McpGateway::new(mcp_scope_provider.clone()));
        let servers = parse_mcp_config(&cfg)?;
        gateway.reconfigure(servers).await?;
        Some(gateway)
    } else {
        None
    };

    #[cfg(feature = "memory")]
    let memory: Arc<dyn HorizonsMemory> = {
        let embedder = Arc::new(HashEmbedder::new(64)) as Arc<dyn voyager::EmbeddingModel>;
        let summarizer =
            Arc::new(MockSummarizer) as Arc<dyn voyager::summarizer::SummarizationModel>;
        let mut cfg = voyager::config::VoyagerConfig::default();
        cfg.retrieval.relevance_weight = 1.0;
        cfg.retrieval.recency_weight = 0.0;
        cfg.retrieval.importance_weight = 0.0;
        cfg.summarization.min_items = 2;
        let mem = build_voyager_memory(
            graph_store.clone(),
            vector_store.clone(),
            embedder,
            summarizer,
            cfg,
        );
        Arc::new(VoyagerBackedHorizonsMemory::new(mem)) as Arc<dyn HorizonsMemory>
    };

    #[cfg(feature = "optimization")]
    let optimization: Arc<OptimizationEngine> = {
        let llm = Arc::new(DevMiproLlm) as Arc<dyn MiproLlmClient>;
        let sampler = Arc::new(mipro_v2::BasicSampler::new()) as Arc<dyn MiproVariantSampler>;
        let metric = Arc::new(ExactMatchMetric) as Arc<dyn mipro_v2::EvalMetric>;
        let cl = Arc::new(build_mipro_continual_learning(llm, sampler, metric))
            as Arc<dyn horizons_core::optimization::traits::ContinualLearning>;
        Arc::new(OptimizationEngine::new(
            central_db.clone(),
            project_db.clone(),
            filestore.clone(),
            cl,
        ))
    };

    #[cfg(feature = "evaluation")]
    let evaluation: Arc<EvaluationEngine> = {
        let cfg = VerifierConfig {
            pass_threshold: 0.0,
        };
        let signals = vec![RewardSignal {
            name: "contains_ok".to_string(),
            weight: SignalWeight(1.0),
            kind: SignalKind::Contains {
                needle: "ok".to_string(),
            },
            description: String::new(),
        }];
        let ev = Arc::new(build_rlm_evaluator(cfg, signals, None)?)
            as Arc<dyn horizons_core::evaluation::traits::Evaluator>;
        Arc::new(EvaluationEngine::new(
            central_db.clone(),
            project_db.clone(),
            filestore.clone(),
            ev,
        ))
    };

    // Seed a default org and user so the dev server is usable.
    let org_id = OrgId(Uuid::new_v4());
    central_db
        .upsert_org(&OrgRecord {
            org_id,
            name: "dev".to_string(),
            created_at: Utc::now(),
        })
        .await?;
    central_db
        .upsert_user(&UserRecord {
            user_id: Uuid::new_v4(),
            org_id,
            email: "dev@example.invalid".to_string(),
            display_name: Some("Dev".to_string()),
            role: UserRole::Admin,
            created_at: Utc::now(),
        })
        .await?;

    // Initialize sandbox runtime with Docker backend for dev.
    // The sandbox engine is opt-in: only enabled when HORIZONS_SANDBOX_BACKEND is set.
    let sandbox_runtime = match std::env::var("HORIZONS_SANDBOX_BACKEND").ok().as_deref() {
        Some("docker") => {
            let docker_network = std::env::var("HORIZONS_DOCKER_NETWORK").ok();
            let backend = Arc::new(horizons_core::engine::docker_backend::DockerBackend::new(
                docker_network,
            ));
            Some(Arc::new(
                horizons_core::engine::sandbox_runtime::SandboxRuntime::new(backend),
            ))
        }
        Some("daytona") => {
            let api_url = std::env::var("DAYTONA_API_URL").ok();
            let api_key =
                std::env::var("DAYTONA_API_KEY").unwrap_or_else(|_| "dev-key".to_string());
            let backend = Arc::new(horizons_core::engine::daytona_backend::DaytonaBackend::new(
                api_url, api_key,
            ));
            Some(Arc::new(
                horizons_core::engine::sandbox_runtime::SandboxRuntime::new(backend),
            ))
        }
        _ => {
            tracing::info!(
                "sandbox runtime not configured (set HORIZONS_SANDBOX_BACKEND=docker|daytona to enable)"
            );
            None
        }
    };

    Ok(AppState::new(
        central_db,
        project_db,
        filestore,
        cache,
        graph_store,
        vector_store,
        event_bus,
        context_refresh,
        core_agents,
        mcp_gateway,
        sandbox_runtime,
        #[cfg(feature = "memory")]
        memory,
        #[cfg(feature = "optimization")]
        optimization,
        #[cfg(feature = "evaluation")]
        evaluation,
    ))
}
