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
    ApiKeyRecord, AuditQuery, Cache, CentralDb, ConnectorCredential, CoreAgentEventCursor,
    CoreAgentRecord, Filestore, GraphStore, ListQuery, OperationRecord, OperationRunRecord,
    OrgRecord, ProjectDb, ProjectDbParam, ProjectDbRow, ProjectDbValue, ProjectRecord,
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
use horizons_graph::GraphEngine;
use horizons_graph::llm::LlmClient as GraphLlmClient;
use horizons_graph::llm::{ApiMode, LlmEndpoint, LlmRequest};
use horizons_graph::tools::DefaultToolExecutor as GraphToolExecutor;

struct BuiltinGraphRunner {
    engine: Arc<GraphEngine>,
}

#[async_trait]
impl horizons_core::pipelines::traits::GraphRunner for BuiltinGraphRunner {
    #[tracing::instrument(level = "info", skip_all, fields(graph_id = %graph_id))]
    async fn run_graph(
        &self,
        graph_id: &str,
        mut inputs: serde_json::Value,
        context: serde_json::Value,
        _identity: &AgentIdentity,
    ) -> CoreResult<serde_json::Value> {
        // Inject pipeline context for graph nodes and tool calls.
        match inputs.as_object_mut() {
            Some(m) => {
                m.entry("_pipeline".to_string())
                    .or_insert_with(|| context.clone());
            }
            None => {
                inputs = serde_json::json!({
                    "input": inputs,
                    "_pipeline": context,
                });
            }
        }

        let yaml = horizons_graph::registry::get_builtin_graph_yaml(graph_id).ok_or_else(|| {
            CoreError::NotFound(format!("unknown built-in graph_id '{graph_id}'"))
        })?;
        let graph_value: serde_json::Value = serde_yaml::from_str(yaml).map_err(|e| {
            CoreError::InvalidInput(format!("invalid built-in graph yaml ({graph_id}): {e}"))
        })?;

        let ir = horizons_graph::ir::GraphIr {
            schema_version: horizons_graph::ir::GRAPH_IR_SCHEMA_VERSION.to_string(),
            graph: Some(graph_value),
            metadata: None,
            nodes: Vec::new(),
            edges: Vec::new(),
            entrypoints: Vec::new(),
        };

        let run = self
            .engine
            .execute_graph_ir(&ir, inputs, None)
            .await
            .map_err(|e| CoreError::backend("pipeline graph run failed", e))?;

        Ok(run.output)
    }
}

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
#[derive(Clone)]
struct OpenAiEmbedder {
    client: reqwest::Client,
    api_base: String,
    api_key: String,
    model: String,
}

#[cfg(feature = "memory")]
impl OpenAiEmbedder {
    fn from_env() -> anyhow::Result<Self> {
        let api_base = std::env::var("OPENAI_BASE_URL")
            .unwrap_or_else(|_| "https://api.openai.com/v1".to_string());
        let api_key = std::env::var("OPENAI_API_KEY")
            .map_err(|_| anyhow::anyhow!("OPENAI_API_KEY is required for OpenAiEmbedder"))?;
        let model = std::env::var("HORIZONS_VOYAGER_EMBEDDING_MODEL")
            .unwrap_or_else(|_| "text-embedding-3-small".to_string());
        Ok(Self {
            client: reqwest::Client::new(),
            api_base: api_base.trim_end_matches('/').to_string(),
            api_key,
            model,
        })
    }

    fn embeddings_url(&self) -> String {
        format!("{}/embeddings", self.api_base.trim_end_matches('/'))
    }
}

#[cfg(feature = "memory")]
#[async_trait]
impl voyager::EmbeddingModel for OpenAiEmbedder {
    async fn embed(&self, _scope: &voyager::Scope, text: &str) -> voyager::Result<Vec<f32>> {
        // Minimal OpenAI-compatible embeddings request.
        let resp = self
            .client
            .post(self.embeddings_url())
            .bearer_auth(&self.api_key)
            .json(&serde_json::json!({
                "model": self.model,
                "input": text,
            }))
            .send()
            .await
            .map_err(|e| {
                voyager::VoyagerError::EmbeddingFailed(format!("embeddings request failed: {e}"))
            })?;

        let status = resp.status();
        let value = resp.json::<serde_json::Value>().await.map_err(|e| {
            voyager::VoyagerError::EmbeddingFailed(format!("embeddings response parse failed: {e}"))
        })?;

        if !status.is_success() {
            return Err(voyager::VoyagerError::EmbeddingFailed(format!(
                "embeddings request failed ({status}): {value}"
            )));
        }

        let emb = value
            .get("data")
            .and_then(|v| v.as_array())
            .and_then(|arr| arr.first())
            .and_then(|v| v.get("embedding"))
            .and_then(|v| v.as_array())
            .ok_or_else(|| {
                voyager::VoyagerError::EmbeddingFailed(
                    "missing embeddings data[0].embedding".to_string(),
                )
            })?;

        let mut out = Vec::with_capacity(emb.len());
        for x in emb {
            let f = x.as_f64().ok_or_else(|| {
                voyager::VoyagerError::EmbeddingFailed("non-numeric embedding value".to_string())
            })?;
            out.push(f as f32);
        }
        Ok(out)
    }

    fn name(&self) -> &'static str {
        "openai"
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

#[cfg(feature = "memory")]
#[derive(Clone)]
struct GraphBackedSummarizer {
    client: GraphLlmClient,
    model: String,
    endpoint: LlmEndpoint,
}

#[cfg(feature = "memory")]
impl GraphBackedSummarizer {
    fn from_env() -> anyhow::Result<Self> {
        let model = std::env::var("HORIZONS_VOYAGER_SUMMARY_MODEL")
            .unwrap_or_else(|_| "gpt-4o-mini".to_string());
        // Use chat completions by default; can be overridden via GRAPH_LLM_API_MODE.
        let mut endpoint = horizons_graph::llm::resolve_endpoint(&model, None)
            .map_err(|e| anyhow::anyhow!("resolve llm endpoint: {e}"))?;
        if std::env::var("GRAPH_LLM_API_MODE").is_err() {
            endpoint.mode = ApiMode::ChatCompletions;
        }
        Ok(Self {
            client: GraphLlmClient::new(),
            model,
            endpoint,
        })
    }
}

#[cfg(feature = "memory")]
#[async_trait]
impl voyager::summarizer::SummarizationModel for GraphBackedSummarizer {
    async fn summarize(
        &self,
        _scope: &voyager::Scope,
        items: &[voyager::MemoryItem],
    ) -> voyager::Result<String> {
        // Keep prompts bounded; the goal is to exercise "real" summarization, not dump everything.
        let mut lines = Vec::new();
        for it in items.iter().take(50) {
            let content = match &it.content {
                serde_json::Value::String(s) => s.clone(),
                other => other.to_string(),
            };
            let short = if content.len() > 400 {
                &content[..400]
            } else {
                &content
            };
            lines.push(format!(
                "- [{}] {} {}",
                it.item_type.0,
                it.created_at.to_rfc3339(),
                short.replace('\n', " ")
            ));
        }
        let prompt = format!(
            "Summarize the following agent memory items.\n\
Rules:\n\
- Output a concise summary.\n\
- Include key entities, decisions, and recurring themes.\n\
- If there are action items, list them.\n\n\
ITEMS:\n{}",
            lines.join("\n")
        );

        let req = LlmRequest {
            model: self.model.clone(),
            messages: vec![
                serde_json::json!({"role": "system", "content": "You summarize agent memory."}),
                serde_json::json!({"role": "user", "content": prompt}),
            ],
            temperature: 0.2,
            max_tokens: Some(300),
            response_format: None,
            endpoint: self.endpoint.clone(),
            stream: false,
            tools: None,
            tool_choice: None,
            previous_response_id: None,
        };

        let resp = self.client.send(&req, None).await.map_err(|e| {
            voyager::VoyagerError::SummarizationFailed(format!("llm summarize failed: {e}"))
        })?;
        Ok(resp.text)
    }

    fn name(&self) -> &'static str {
        "graph-backed"
    }
}

#[cfg(feature = "memory")]
fn build_voyager_embedder_from_env() -> Arc<dyn voyager::EmbeddingModel> {
    let mode = std::env::var("HORIZONS_VOYAGER_EMBEDDER")
        .ok()
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();

    if mode == "openai" {
        match OpenAiEmbedder::from_env() {
            Ok(e) => return Arc::new(e) as Arc<dyn voyager::EmbeddingModel>,
            Err(err) => {
                tracing::warn!("failed to init openai embedder; falling back to hash: {err}")
            }
        }
    }

    let dim = std::env::var("HORIZONS_VECTOR_DIM")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(64);
    Arc::new(HashEmbedder::new(dim)) as Arc<dyn voyager::EmbeddingModel>
}

#[cfg(feature = "memory")]
fn build_voyager_summarizer_from_env() -> Arc<dyn voyager::summarizer::SummarizationModel> {
    let mode = std::env::var("HORIZONS_VOYAGER_SUMMARIZER")
        .ok()
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    if mode == "llm" || mode == "graph" || mode == "real" {
        match GraphBackedSummarizer::from_env() {
            Ok(s) => return Arc::new(s) as Arc<dyn voyager::summarizer::SummarizationModel>,
            Err(err) => {
                tracing::warn!("failed to init llm summarizer; falling back to mock: {err}")
            }
        }
    }
    Arc::new(MockSummarizer) as Arc<dyn voyager::summarizer::SummarizationModel>
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

#[cfg(feature = "optimization")]
#[derive(Clone)]
struct GraphBackedMiproLlm {
    client: GraphLlmClient,
    model: String,
    endpoint: horizons_graph::llm::LlmEndpoint,
    temperature: f64,
}

#[cfg(feature = "optimization")]
impl GraphBackedMiproLlm {
    fn from_env() -> mipro_v2::Result<Self> {
        let model =
            std::env::var("HORIZONS_MIPRO_MODEL").unwrap_or_else(|_| "gpt-4o-mini".to_string());
        // Rewrites should explore; keep a default > 0 unless explicitly overridden.
        let temperature = std::env::var("HORIZONS_MIPRO_TEMPERATURE")
            .ok()
            .and_then(|v| v.trim().parse::<f64>().ok())
            .unwrap_or(0.7);
        let endpoint = horizons_graph::llm::resolve_endpoint(&model, None)
            .map_err(|e| mipro_v2::MiproError::Llm(e.to_string()))?;
        Ok(Self {
            client: GraphLlmClient::new(),
            model,
            endpoint,
            temperature,
        })
    }
}

#[cfg(feature = "optimization")]
#[async_trait]
impl MiproLlmClient for GraphBackedMiproLlm {
    async fn complete(&self, prompt: &str) -> mipro_v2::Result<String> {
        let req = horizons_graph::llm::LlmRequest {
            model: self.model.clone(),
            messages: vec![serde_json::json!({"role": "user", "content": prompt})],
            temperature: self.temperature,
            max_tokens: Some(256),
            response_format: None,
            endpoint: self.endpoint.clone(),
            stream: false,
            tools: None,
            tool_choice: None,
            previous_response_id: None,
        };
        let resp = self
            .client
            .send(&req, None)
            .await
            .map_err(|e| mipro_v2::MiproError::Llm(e.to_string()))?;
        Ok(resp.text)
    }

    fn name(&self) -> &'static str {
        "graph-backed"
    }
}

#[cfg(feature = "optimization")]
fn build_mipro_llm_from_env() -> Arc<dyn MiproLlmClient> {
    let mode = std::env::var("HORIZONS_MIPRO_LLM")
        .ok()
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();

    // Default behavior:
    // - If the user explicitly requests mock, use it.
    // - If the user explicitly requests real, use it.
    // - Otherwise, default to the deterministic dev mock (avoids accidental API spend in dev).
    if mode == "mock" || mode == "dev-mock" {
        return Arc::new(DevMiproLlm) as Arc<dyn MiproLlmClient>;
    }

    if mode == "real" || mode == "graph" {
        match GraphBackedMiproLlm::from_env() {
            Ok(llm) => return Arc::new(llm) as Arc<dyn MiproLlmClient>,
            Err(e) => {
                tracing::warn!("failed to init real mipro llm; falling back to dev mock: {e}")
            }
        }
    }

    Arc::new(DevMiproLlm) as Arc<dyn MiproLlmClient>
}

#[cfg(feature = "optimization")]
fn build_mipro_rewrite_llm_from_env() -> Arc<dyn MiproLlmClient> {
    build_mipro_llm_from_env()
}

#[cfg(feature = "optimization")]
#[derive(Debug, serde::Deserialize)]
struct TaskAppConfigMarker {
    task_app_url: String,
    #[serde(default)]
    seed: Option<u64>,
    #[serde(default)]
    env_config: serde_json::Value,
    #[serde(default)]
    policy_config: serde_json::Value,
}

#[cfg(feature = "optimization")]
fn _extract_taskapp_marker(prompt: &str) -> mipro_v2::Result<(String, TaskAppConfigMarker)> {
    const MARKER: &str = "HORIZONS_TASKAPP_CONFIG=";
    let idx = prompt.rfind(MARKER).ok_or_else(|| {
        mipro_v2::MiproError::InvalidArgument(
            "missing task app config marker in prompt (expected HORIZONS_TASKAPP_CONFIG=... at end)"
                .to_string(),
        )
    })?;
    let (before, after) = prompt.split_at(idx);
    let json_str = after.get(MARKER.len()..).unwrap_or_default().trim();

    let cfg: TaskAppConfigMarker = serde_json::from_str(json_str).map_err(|e| {
        mipro_v2::MiproError::InvalidArgument(format!("failed to parse task app config JSON: {e}"))
    })?;
    Ok((before.trim_end().to_string(), cfg))
}

#[cfg(feature = "optimization")]
#[derive(Clone)]
struct TaskAppMiproLlm {
    rewrite_llm: Arc<dyn MiproLlmClient>,
    http: reqwest::Client,
    environment_api_key: Option<String>,
    provider_api_key: Option<String>,
    inference_url: String,
}

#[cfg(feature = "optimization")]
impl TaskAppMiproLlm {
    fn from_env(rewrite_llm: Arc<dyn MiproLlmClient>) -> Self {
        let environment_api_key = std::env::var("ENVIRONMENT_API_KEY").ok();
        let provider_api_key = std::env::var("OPENAI_API_KEY")
            .ok()
            .or_else(|| std::env::var("GROQ_API_KEY").ok());
        let inference_url = std::env::var("HORIZONS_TASKAPP_INFERENCE_URL")
            .ok()
            .unwrap_or_else(|| "https://api.openai.com/v1".to_string());

        Self {
            rewrite_llm,
            http: reqwest::Client::new(),
            environment_api_key,
            provider_api_key,
            inference_url,
        }
    }

    fn _is_rewrite_prompt(prompt: &str) -> bool {
        // Matches mipro_v2::BasicSampler's rewrite prompt.
        prompt
            .trim_start()
            .starts_with("Rewrite this prompt to be clearer")
            || prompt.contains("\n\nPROMPT:\n")
    }
}

#[cfg(feature = "optimization")]
#[async_trait]
impl MiproLlmClient for TaskAppMiproLlm {
    async fn complete(&self, prompt: &str) -> mipro_v2::Result<String> {
        if Self::_is_rewrite_prompt(prompt) {
            return self.rewrite_llm.complete(prompt).await;
        }

        let (system_prompt, cfg) = _extract_taskapp_marker(prompt)?;

        // Merge configs coming from the marker with provider auth from the Horizons process env.
        let mut policy_config = match cfg.policy_config {
            serde_json::Value::Object(map) => serde_json::Value::Object(map),
            serde_json::Value::Null => serde_json::json!({}),
            other => {
                return Err(mipro_v2::MiproError::InvalidArgument(format!(
                    "policy_config must be an object; got: {other}"
                )));
            }
        };
        if let serde_json::Value::Object(map) = &mut policy_config {
            map.insert(
                "system_prompt".to_string(),
                serde_json::Value::String(system_prompt),
            );
            map.entry("inference_url".to_string())
                .or_insert_with(|| serde_json::Value::String(self.inference_url.clone()));
            if let Some(k) = &self.provider_api_key {
                map.entry("api_key".to_string())
                    .or_insert_with(|| serde_json::Value::String(k.clone()));
            }
        }

        let env_config = match cfg.env_config {
            serde_json::Value::Object(map) => serde_json::Value::Object(map),
            serde_json::Value::Null => serde_json::json!({}),
            other => {
                return Err(mipro_v2::MiproError::InvalidArgument(format!(
                    "env_config must be an object; got: {other}"
                )));
            }
        };

        let trace_correlation_id = uuid::Uuid::new_v4().to_string();
        let req_body = serde_json::json!({
            "trace_correlation_id": trace_correlation_id,
            "env": {
                "seed": cfg.seed.unwrap_or(0) as i64,
                "config": env_config,
            },
            "policy": {
                "config": policy_config,
            },
        });

        let url = format!("{}/rollout", cfg.task_app_url.trim_end_matches('/'));
        let mut req = self.http.post(url).json(&req_body);
        if let Some(k) = &self.environment_api_key {
            req = req.header("x-api-key", k);
        }

        let resp = req
            .send()
            .await
            .map_err(|e| mipro_v2::MiproError::Llm(format!("task app request failed: {e}")))?;
        let status = resp.status();
        let text = resp.text().await.map_err(|e| {
            mipro_v2::MiproError::Llm(format!("task app response read failed: {e}"))
        })?;
        let val: serde_json::Value = serde_json::from_str(&text).map_err(|e| {
            mipro_v2::MiproError::Llm(format!(
                "task app response JSON decode failed: {e}; status={status}; body_prefix={}",
                text.chars().take(400).collect::<String>()
            ))
        })?;
        if !status.is_success() {
            return Err(mipro_v2::MiproError::Llm(format!(
                "task app returned non-2xx: {status} body={val}"
            )));
        }

        let reward = val
            .get("reward_info")
            .and_then(|v| v.get("outcome_reward"))
            .and_then(|v| v.as_f64())
            .ok_or_else(|| {
                mipro_v2::MiproError::Llm(format!(
                    "task app response missing reward_info.outcome_reward; body={val}"
                ))
            })?;
        Ok(format!("{reward}"))
    }

    fn name(&self) -> &'static str {
        "taskapp-rollout"
    }
}

#[cfg(feature = "optimization")]
#[derive(Debug, Default)]
struct ParseFloatMetric;

#[cfg(feature = "optimization")]
#[async_trait]
impl mipro_v2::EvalMetric for ParseFloatMetric {
    async fn score(&self, _example: &mipro_v2::Example, output: &str) -> mipro_v2::Result<f32> {
        let s = output.trim().parse::<f32>().map_err(|e| {
            mipro_v2::MiproError::InvalidArgument(format!(
                "expected task app reward output to be a float; got={output:?} err={e}"
            ))
        })?;
        Ok(s)
    }

    fn name(&self) -> &'static str {
        "taskapp_reward"
    }
}

#[cfg(feature = "optimization")]
fn build_mipro_llm_and_metric_from_env() -> (Arc<dyn MiproLlmClient>, Arc<dyn mipro_v2::EvalMetric>)
{
    let eval_mode = std::env::var("HORIZONS_MIPRO_EVAL")
        .ok()
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();

    if eval_mode == "taskapp" || eval_mode == "taskapp_reward" || eval_mode == "reward" {
        let rewrite_llm = build_mipro_rewrite_llm_from_env();
        let llm = Arc::new(TaskAppMiproLlm::from_env(rewrite_llm)) as Arc<dyn MiproLlmClient>;
        let metric = Arc::new(ParseFloatMetric) as Arc<dyn mipro_v2::EvalMetric>;
        return (llm, metric);
    }

    let llm = build_mipro_rewrite_llm_from_env();
    let metric = Arc::new(ExactMatchMetric) as Arc<dyn mipro_v2::EvalMetric>;
    (llm, metric)
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
        Ok(horizons_core::core_agents::models::AgentOutcome::Proposals(
            Vec::new(),
        ))
    }
}

#[derive(Clone)]
pub struct DevCentralDb {
    orgs: Arc<RwLock<HashMap<OrgId, OrgRecord>>>,
    users: Arc<RwLock<HashMap<(OrgId, Uuid), UserRecord>>>,
    projects: Arc<RwLock<HashMap<(OrgId, ProjectId), ProjectRecord>>>,
    api_keys: Arc<RwLock<HashMap<Uuid, ApiKeyRecord>>>,
    platform_config: Arc<RwLock<HashMap<OrgId, PlatformConfig>>>,
    audit: Arc<RwLock<HashMap<OrgId, Vec<AuditEntry>>>>,
    credentials: Arc<RwLock<HashMap<(OrgId, String), ConnectorCredential>>>,
    resources: Arc<RwLock<HashMap<(OrgId, String), ResourceRecord>>>,
    operations: Arc<RwLock<HashMap<(OrgId, String), OperationRecord>>>,
    operation_runs: Arc<RwLock<Vec<OperationRunRecord>>>,
    core_agents: Arc<
        RwLock<
            HashMap<(OrgId, ProjectId, String), horizons_core::onboard::models::CoreAgentRecord>,
        >,
    >,
    core_agent_event_cursors:
        Arc<RwLock<HashMap<(OrgId, ProjectId, String, String), CoreAgentEventCursor>>>,
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
            projects: Arc::new(RwLock::new(HashMap::new())),
            api_keys: Arc::new(RwLock::new(HashMap::new())),
            platform_config: Arc::new(RwLock::new(HashMap::new())),
            audit: Arc::new(RwLock::new(HashMap::new())),
            credentials: Arc::new(RwLock::new(HashMap::new())),
            resources: Arc::new(RwLock::new(HashMap::new())),
            operations: Arc::new(RwLock::new(HashMap::new())),
            operation_runs: Arc::new(RwLock::new(Vec::new())),
            core_agents: Arc::new(RwLock::new(HashMap::new())),
            core_agent_event_cursors: Arc::new(RwLock::new(HashMap::new())),
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

    async fn list_orgs(&self, query: ListQuery) -> CoreResult<Vec<OrgRecord>> {
        let orgs = self.orgs.read().await;
        let mut rows: Vec<OrgRecord> = orgs.values().cloned().collect();
        // uuid::Uuid doesn't implement Ord, so sort deterministically using the string form.
        rows.sort_by(|a, b| {
            (a.created_at, a.org_id.to_string()).cmp(&(b.created_at, b.org_id.to_string()))
        });
        Ok(rows
            .into_iter()
            .skip(query.offset)
            .take(query.limit)
            .collect())
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

    async fn upsert_project(&self, project: &ProjectRecord) -> CoreResult<()> {
        if project.slug.trim().is_empty() {
            return Err(CoreError::InvalidInput("project slug is empty".to_string()));
        }
        self.projects
            .write()
            .await
            .insert((project.org_id, project.project_id), project.clone());
        Ok(())
    }

    async fn get_project_by_id(
        &self,
        org_id: OrgId,
        project_id: ProjectId,
    ) -> CoreResult<Option<ProjectRecord>> {
        Ok(self
            .projects
            .read()
            .await
            .get(&(org_id, project_id))
            .cloned())
    }

    async fn get_project_by_slug(
        &self,
        org_id: OrgId,
        slug: &str,
    ) -> CoreResult<Option<ProjectRecord>> {
        let slug = slug.trim();
        if slug.is_empty() {
            return Ok(None);
        }
        let projects = self.projects.read().await;
        Ok(projects
            .values()
            .find(|p| p.org_id == org_id && p.slug == slug)
            .cloned())
    }

    async fn list_projects(
        &self,
        org_id: OrgId,
        query: ListQuery,
    ) -> CoreResult<Vec<ProjectRecord>> {
        let projects = self.projects.read().await;
        let mut rows: Vec<ProjectRecord> = projects
            .values()
            .filter(|p| p.org_id == org_id)
            .cloned()
            .collect();
        rows.sort_by_key(|p| (p.created_at, p.project_id.to_string()));
        rows.reverse();
        Ok(rows
            .into_iter()
            .skip(query.offset)
            .take(query.limit)
            .collect())
    }

    async fn upsert_api_key(&self, key: &ApiKeyRecord) -> CoreResult<()> {
        self.api_keys.write().await.insert(key.key_id, key.clone());
        Ok(())
    }

    async fn get_api_key_by_id(&self, key_id: Uuid) -> CoreResult<Option<ApiKeyRecord>> {
        Ok(self.api_keys.read().await.get(&key_id).cloned())
    }

    async fn list_api_keys(
        &self,
        org_id: OrgId,
        query: ListQuery,
    ) -> CoreResult<Vec<ApiKeyRecord>> {
        let keys = self.api_keys.read().await;
        let mut rows: Vec<ApiKeyRecord> = keys
            .values()
            .filter(|k| k.org_id == org_id)
            .cloned()
            .collect();
        rows.sort_by_key(|k| (k.created_at, k.key_id));
        rows.reverse();
        Ok(rows
            .into_iter()
            .skip(query.offset)
            .take(query.limit)
            .collect())
    }

    async fn delete_api_key(&self, org_id: OrgId, key_id: Uuid) -> CoreResult<()> {
        let mut keys = self.api_keys.write().await;
        let should_remove = keys.get(&key_id).is_some_and(|k| k.org_id == org_id);
        if should_remove {
            keys.remove(&key_id);
        }
        Ok(())
    }

    async fn touch_api_key_last_used(&self, key_id: Uuid, at: DateTime<Utc>) -> CoreResult<()> {
        let mut keys = self.api_keys.write().await;
        if let Some(k) = keys.get_mut(&key_id) {
            k.last_used_at = Some(at);
        }
        Ok(())
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
    async fn list_resources(
        &self,
        org_id: OrgId,
        query: ListQuery,
    ) -> CoreResult<Vec<ResourceRecord>> {
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
    async fn list_operations(
        &self,
        org_id: OrgId,
        query: ListQuery,
    ) -> CoreResult<Vec<OperationRecord>> {
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
    async fn upsert_core_agent(&self, agent: &CoreAgentRecord) -> CoreResult<()> {
        self.core_agents.write().await.insert(
            (agent.org_id, agent.project_id, agent.agent_id.clone()),
            agent.clone(),
        );
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn get_core_agent(
        &self,
        org_id: OrgId,
        project_id: ProjectId,
        agent_id: &str,
    ) -> CoreResult<Option<CoreAgentRecord>> {
        Ok(self
            .core_agents
            .read()
            .await
            .get(&(org_id, project_id, agent_id.to_string()))
            .cloned())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn list_core_agents(
        &self,
        org_id: OrgId,
        project_id: ProjectId,
        query: ListQuery,
    ) -> CoreResult<Vec<CoreAgentRecord>> {
        let agents = self.core_agents.read().await;
        let mut rows: Vec<CoreAgentRecord> = agents
            .values()
            .filter(|a| a.org_id == org_id && a.project_id == project_id)
            .cloned()
            .collect();
        rows.sort_by_key(|a| (a.updated_at, a.agent_id.clone()));
        rows.reverse();
        Ok(rows
            .into_iter()
            .skip(query.offset)
            .take(query.limit)
            .collect())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn delete_core_agent(
        &self,
        org_id: OrgId,
        project_id: ProjectId,
        agent_id: &str,
    ) -> CoreResult<()> {
        self.core_agents
            .write()
            .await
            .remove(&(org_id, project_id, agent_id.to_string()));
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn set_core_agents_time_enabled(
        &self,
        org_id: OrgId,
        project_id: ProjectId,
        enabled: bool,
    ) -> CoreResult<u64> {
        use horizons_core::core_agents::models::AgentSchedule;

        let mut agents = self.core_agents.write().await;
        let mut updated = 0u64;
        for ((o, p, _), a) in agents.iter_mut() {
            if *o != org_id || *p != project_id {
                continue;
            }
            let is_time = matches!(
                a.schedule,
                Some(AgentSchedule::Cron(_)) | Some(AgentSchedule::Interval { .. })
            );
            if !is_time {
                continue;
            }
            if a.enabled != enabled {
                a.enabled = enabled;
                a.updated_at = Utc::now();
                updated += 1;
            }
        }
        Ok(updated)
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn upsert_core_agent_event_cursor(
        &self,
        cursor: &CoreAgentEventCursor,
    ) -> CoreResult<()> {
        self.core_agent_event_cursors.write().await.insert(
            (
                cursor.org_id,
                cursor.project_id,
                cursor.agent_id.clone(),
                cursor.topic.clone(),
            ),
            cursor.clone(),
        );
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn get_core_agent_event_cursor(
        &self,
        org_id: OrgId,
        project_id: ProjectId,
        agent_id: &str,
        topic: &str,
    ) -> CoreResult<Option<CoreAgentEventCursor>> {
        Ok(self
            .core_agent_event_cursors
            .read()
            .await
            .get(&(org_id, project_id, agent_id.to_string(), topic.to_string()))
            .cloned())
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
    // Voyager dev memory uses the HelixGraphStoreAdapter which issues a small set of
    // cypher queries (CREATE/MATCH MemoryItem). Our dev graph store doesn't implement
    // cypher, so we special-case those queries to make Voyager work in dev/bench.
    memory_items: Arc<RwLock<HashMap<String, String>>>,
}

impl DevGraphStore {
    #[tracing::instrument(level = "debug")]
    pub fn new() -> Self {
        Self {
            nodes: Arc::new(RwLock::new(Vec::new())),
            edges: Arc::new(RwLock::new(Vec::new())),
            memory_items: Arc::new(RwLock::new(HashMap::new())),
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
        params: serde_json::Value,
    ) -> CoreResult<Vec<serde_json::Value>> {
        // Special-case Voyager memory cypher used by HelixGraphStoreAdapter.
        // This keeps dev mode functional without needing a real cypher engine.
        let cypher_lc = cypher.to_ascii_lowercase();
        if cypher_lc.contains("memoryitem") {
            let org_id = params.get("org_id").and_then(|v| v.as_str());
            let agent_id = params.get("agent_id").and_then(|v| v.as_str());
            let id = params.get("id").and_then(|v| v.as_str());

            // Append: CREATE (m:MemoryItem {.., item_json: $item_json}) ...
            if cypher_lc.contains("create (m:memoryitem") {
                if let (Some(org), Some(agent), Some(mid), Some(item_json)) = (
                    org_id,
                    agent_id,
                    id,
                    params.get("item_json").and_then(|v| v.as_str()),
                ) {
                    let key = format!("{org}/{agent}/{mid}");
                    self.memory_items
                        .write()
                        .await
                        .insert(key, item_json.to_string());
                }
                return Ok(Vec::new());
            }

            // Get: MATCH (m:MemoryItem {id: $id, org_id: $org_id, agent_id: $agent_id}) ...
            if cypher_lc.contains("match (m:memoryitem")
                && cypher_lc.contains("id: $id")
                && cypher_lc.contains("limit 1")
            {
                if let (Some(org), Some(agent), Some(mid)) = (org_id, agent_id, id) {
                    let key = format!("{org}/{agent}/{mid}");
                    if let Some(item_json) = self.memory_items.read().await.get(&key) {
                        return Ok(vec![serde_json::json!({ "item_json": item_json })]);
                    }
                    return Ok(Vec::new());
                }
            }

            // List: MATCH (m:MemoryItem {org_id: $org_id, agent_id: $agent_id}) ...
            if cypher_lc.contains("match (m:memoryitem")
                && cypher_lc.contains("org_id: $org_id")
                && cypher_lc.contains("agent_id: $agent_id")
                && !cypher_lc.contains("id: $id")
            {
                if let (Some(org), Some(agent)) = (org_id, agent_id) {
                    let prefix = format!("{org}/{agent}/");
                    let items = self.memory_items.read().await;
                    let mut out = Vec::new();
                    for (k, v) in items.iter() {
                        if k.starts_with(&prefix) {
                            out.push(serde_json::json!({ "item_json": v }));
                        }
                    }
                    return Ok(out);
                }
            }
        }

        // Dev backend: return nodes by default, edges if the query looks edge-focused.
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
        let mut events = self.events.write().await;
        if let Some(existing) = events
            .iter()
            .find(|e| e.org_id == event.org_id && e.dedupe_key == event.dedupe_key)
        {
            return Ok(existing.id.clone());
        }
        let id = event.id.clone();
        events.push(event);
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

    #[tracing::instrument(level = "debug", skip_all)]
    async fn list_subscriptions(&self, org_id: &str) -> EventsResult<Vec<Subscription>> {
        let subs = self.subs.read().await;
        Ok(subs
            .iter()
            .filter(|s| s.org_id == org_id)
            .cloned()
            .collect())
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
        // Use durable SQLite-backed CentralDb instead of in-memory DevCentralDb.
        // Data survives across restarts (stored at {data_dir}/central.db).
        let db = horizons_core::onboard::sqlite::SqliteCentralDb::new(data_dir.join("central.db"))
            .await?;
        Arc::new(db)
    };

    // Secrets: local dev uses a file-backed 32-byte master key under the data dir.
    // This enables `$cred:...` token resolution for connectors, agents, and managed operations.
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
    let event_bus: Arc<dyn EventBus> = if let (Some(pg), Some(redis)) =
        (central_db_url.clone(), redis_url.clone())
    {
        let mut cfg = EventSyncConfig::default();
        cfg.postgres_url = pg;
        cfg.redis_url = redis;
        cfg.webhook_signing_secret = std::env::var("HORIZONS_WEBHOOK_SIGNING_SECRET")
            .ok()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty());
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

    let core_agents = Arc::new(
        CoreAgentsExecutor::new(
            central_db.clone(),
            project_db.clone(),
            event_bus.clone(),
            None,
        )
        .with_credential_manager(credential_manager.clone())
        .with_mcp_scope_provider(mcp_scope_provider.clone()),
    );
    core_agents.register_agent(Arc::new(DevNoopAgent)).await?;

    let core_scheduler = Arc::new(horizons_core::core_agents::scheduler::CoreScheduler::new(
        central_db.clone(),
        project_db.clone(),
        event_bus.clone(),
        core_agents.clone() as Arc<dyn horizons_core::core_agents::traits::CoreAgents>,
        horizons_core::core_agents::scheduler::CoreSchedulerConfig::default(),
    ));

    let mcp_gateway: Option<Arc<McpGateway>> = if let Ok(cfg) = std::env::var("HORIZONS_MCP_CONFIG")
    {
        let gateway = Arc::new(McpGateway::new(mcp_scope_provider.clone()));
        let servers = parse_mcp_config(&cfg)?;
        gateway.reconfigure(servers).await?;
        Some(gateway)
    } else {
        None
    };

    // Graph engine (used by verifier graphs and context ingestion pipelines).
    // Configuration is env-driven (LLM keys, tool executor endpoint, python backend, etc).
    let graph_llm = Arc::new(GraphLlmClient::new()) as Arc<dyn horizons_graph::llm::LlmClientApi>;
    let graph_tools =
        Arc::new(GraphToolExecutor::from_env()) as Arc<dyn horizons_graph::tools::ToolExecutor>;
    let graph_engine = Arc::new(GraphEngine::new(graph_llm, graph_tools));

    let pipelines: Arc<dyn horizons_core::pipelines::traits::PipelineRunner> = {
        let subagent = Arc::new(horizons_core::pipelines::engine::CoreAgentsSubagent::new(
            core_agents.clone(),
        )) as Arc<dyn horizons_core::pipelines::traits::Subagent>;

        let mcp_client = mcp_gateway
            .clone()
            .map(|g| g as Arc<dyn horizons_core::core_agents::mcp::McpClient>);

        let graph_runner = Some(Arc::new(BuiltinGraphRunner {
            engine: graph_engine.clone(),
        })
            as Arc<dyn horizons_core::pipelines::traits::GraphRunner>);

        Arc::new(
            horizons_core::pipelines::engine::DefaultPipelineRunner::new(
                event_bus.clone(),
                subagent,
                mcp_client,
                graph_runner,
            ),
        ) as Arc<dyn horizons_core::pipelines::traits::PipelineRunner>
    };

    let context_refresh: Arc<dyn ContextRefreshTrait> = Arc::new(
        ContextRefreshEngine::new(central_db.clone(), project_db.clone(), event_bus.clone())
            .with_credential_manager(credential_manager.clone())
            .with_pipeline_runner(pipelines.clone()),
    );
    context_refresh
        .register_connector(Arc::new(DevNoopConnector))
        .await?;

    #[cfg(feature = "memory")]
    let memory: Arc<dyn HorizonsMemory> = {
        let embedder = build_voyager_embedder_from_env();
        let summarizer = build_voyager_summarizer_from_env();
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
        let (llm, metric) = build_mipro_llm_and_metric_from_env();
        let sampler = Arc::new(mipro_v2::BasicSampler::new()) as Arc<dyn MiproVariantSampler>;
        let cl_impl = Arc::new(build_mipro_continual_learning(llm, sampler, metric));
        let cl = cl_impl.clone() as Arc<dyn horizons_core::optimization::traits::ContinualLearning>;
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

    #[cfg(all(feature = "memory", feature = "optimization", feature = "evaluation"))]
    let continual_learning: Arc<
        horizons_core::optimization::continual::ContinualLearningEngine,
    > = {
        // Rebuild the same underlying optimizer/evaluator wiring used above so the cycle endpoint
        // can access the concrete MIPRO optimizer (for prompt rendering + LLM access).
        let (llm, metric) = build_mipro_llm_and_metric_from_env();
        let sampler = Arc::new(mipro_v2::BasicSampler::new()) as Arc<dyn MiproVariantSampler>;
        let cl_impl = Arc::new(build_mipro_continual_learning(llm, sampler, metric));

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

        Arc::new(
            horizons_core::optimization::continual::ContinualLearningEngine::new(
                memory.clone(),
                cl_impl,
                ev,
                event_bus.clone(),
            ),
        )
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
        graph_engine,
        event_bus,
        context_refresh,
        core_agents,
        core_scheduler,
        pipelines,
        Some(credential_manager),
        mcp_gateway,
        sandbox_runtime,
        #[cfg(feature = "memory")]
        memory,
        #[cfg(feature = "optimization")]
        optimization,
        #[cfg(feature = "evaluation")]
        evaluation,
        #[cfg(all(feature = "memory", feature = "optimization", feature = "evaluation"))]
        continual_learning,
    ))
}
