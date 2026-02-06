use crate::config::{HelixHttpConfig, VectorHttpConfig};
use crate::models::{MemoryItem, MemoryType, Scope, VectorMatch};
use crate::{Result, VoyagerError};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use ulid::Ulid;

#[derive(Debug, Clone)]
pub struct StoreListFilter {
    pub type_filter: Option<Vec<MemoryType>>,
    pub include_episode_summaries: bool,
    pub created_after: Option<DateTime<Utc>>,
    pub created_before: Option<DateTime<Utc>>,
    pub limit: Option<usize>,
}

impl Default for StoreListFilter {
    fn default() -> Self {
        Self {
            type_filter: None,
            include_episode_summaries: true,
            created_after: None,
            created_before: None,
            limit: None,
        }
    }
}

#[async_trait]
pub trait MemoryStore: Send + Sync {
    async fn append(&self, item: MemoryItem) -> Result<()>;
    async fn get(&self, scope: &Scope, id: Ulid) -> Result<Option<MemoryItem>>;
    async fn list(&self, scope: &Scope, filter: &StoreListFilter) -> Result<Vec<MemoryItem>>;
}

#[derive(Debug, Clone)]
pub struct VectorSearchFilter {
    pub type_filter: Option<Vec<MemoryType>>,
    pub include_episode_summaries: bool,
    pub created_after: Option<DateTime<Utc>>,
    pub created_before: Option<DateTime<Utc>>,
    pub limit: usize,
}

#[async_trait]
pub trait VectorIndex: Send + Sync {
    async fn upsert(&self, scope: &Scope, item: &MemoryItem, embedding: Vec<f32>) -> Result<()>;
    async fn search(
        &self,
        scope: &Scope,
        query_embedding: Vec<f32>,
        filter: &VectorSearchFilter,
    ) -> Result<Vec<VectorMatch>>;
}

#[derive(Debug, Default)]
pub struct InMemoryStore {
    // Append-only log; retrieval scans are fine for unit tests and small deployments.
    items: RwLock<Vec<MemoryItem>>,
}

impl InMemoryStore {
    #[tracing::instrument]
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl MemoryStore for InMemoryStore {
    #[tracing::instrument(skip_all)]
    async fn append(&self, item: MemoryItem) -> Result<()> {
        self.items.write().await.push(item);
        Ok(())
    }

    #[tracing::instrument(skip_all)]
    async fn get(&self, scope: &Scope, id: Ulid) -> Result<Option<MemoryItem>> {
        let items = self.items.read().await;
        Ok(items
            .iter()
            .find(|it| it.id == id && it.org_id == scope.org_id && it.agent_id == scope.agent_id)
            .cloned())
    }

    #[tracing::instrument(skip_all)]
    async fn list(&self, scope: &Scope, filter: &StoreListFilter) -> Result<Vec<MemoryItem>> {
        let items = self.items.read().await;
        let mut out: Vec<MemoryItem> = items
            .iter()
            .filter(|it| it.org_id == scope.org_id && it.agent_id == scope.agent_id)
            .filter(|it| {
                if !filter.include_episode_summaries
                    && it.item_type == MemoryType::episode_summary()
                {
                    return false;
                }
                true
            })
            .filter(|it| {
                if let Some(after) = filter.created_after.as_ref() {
                    if it.created_at < *after {
                        return false;
                    }
                }
                if let Some(before) = filter.created_before.as_ref() {
                    if it.created_at > *before {
                        return false;
                    }
                }
                true
            })
            .filter(|it| {
                if let Some(types) = &filter.type_filter {
                    return types.contains(&it.item_type);
                }
                true
            })
            .cloned()
            .collect();

        // Deterministic order: newest first.
        out.sort_by_key(|it| std::cmp::Reverse(it.created_at));
        if let Some(limit) = filter.limit {
            out.truncate(limit);
        }
        Ok(out)
    }
}

#[derive(Debug, Clone)]
struct IndexedEmbedding {
    embedding: Vec<f32>,
    created_at: DateTime<Utc>,
    item_type: MemoryType,
}

#[derive(Debug, Default)]
pub struct InMemoryVectorIndex {
    // scope_key -> id -> embedding
    inner: RwLock<HashMap<(String, String), HashMap<Ulid, IndexedEmbedding>>>,
}

impl InMemoryVectorIndex {
    #[tracing::instrument]
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl VectorIndex for InMemoryVectorIndex {
    #[tracing::instrument(skip_all)]
    async fn upsert(&self, scope: &Scope, item: &MemoryItem, embedding: Vec<f32>) -> Result<()> {
        if item.org_id != scope.org_id || item.agent_id != scope.agent_id {
            return Err(VoyagerError::InvalidArgument(
                "scope does not match item".to_string(),
            ));
        }

        let mut inner = self.inner.write().await;
        let scope_key = (scope.org_id.clone(), scope.agent_id.clone());
        let by_id = inner.entry(scope_key).or_default();
        by_id.insert(
            item.id,
            IndexedEmbedding {
                embedding,
                created_at: item.created_at,
                item_type: item.item_type.clone(),
            },
        );
        Ok(())
    }

    #[tracing::instrument(skip_all)]
    async fn search(
        &self,
        scope: &Scope,
        query_embedding: Vec<f32>,
        filter: &VectorSearchFilter,
    ) -> Result<Vec<VectorMatch>> {
        let inner = self.inner.read().await;
        let Some(by_id) = inner.get(&(scope.org_id.clone(), scope.agent_id.clone())) else {
            return Ok(vec![]);
        };

        let mut matches: Vec<VectorMatch> = by_id
            .iter()
            .filter(|(_, emb)| {
                if !filter.include_episode_summaries
                    && emb.item_type == MemoryType::episode_summary()
                {
                    return false;
                }
                if let Some(types) = &filter.type_filter {
                    if !types.contains(&emb.item_type) {
                        return false;
                    }
                }
                if let Some(after) = filter.created_after.as_ref() {
                    if emb.created_at < *after {
                        return false;
                    }
                }
                if let Some(before) = filter.created_before.as_ref() {
                    if emb.created_at > *before {
                        return false;
                    }
                }
                true
            })
            .map(|(id, emb)| VectorMatch {
                id: *id,
                similarity: cosine_similarity(&query_embedding, &emb.embedding),
                created_at: emb.created_at,
                item_type: emb.item_type.clone(),
            })
            .collect();

        // Highest similarity first, deterministic tie-break by created_at desc, then ULID.
        matches.sort_by(|a, b| {
            b.similarity
                .partial_cmp(&a.similarity)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| b.created_at.cmp(&a.created_at))
                .then_with(|| b.id.cmp(&a.id))
        });

        matches.truncate(filter.limit);
        Ok(matches)
    }
}

fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
    if a.is_empty() || b.is_empty() || a.len() != b.len() {
        return 0.0;
    }
    let mut dot = 0.0f32;
    let mut na = 0.0f32;
    let mut nb = 0.0f32;
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

// -----------------------------------------------------------------------------
// Best-effort HTTP adapters (optional in practice, but included so `voyager` is usable
// standalone with Helix + a vector service).
// -----------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct HelixCypherStore {
    client: reqwest::Client,
    cfg: HelixHttpConfig,
}

impl HelixCypherStore {
    #[tracing::instrument]
    pub fn new(cfg: HelixHttpConfig) -> Result<Self> {
        if cfg.base_url.trim().is_empty() {
            return Err(VoyagerError::InvalidConfig(
                "helix.base_url is required".to_string(),
            ));
        }
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_millis(cfg.timeout_ms))
            .build()?;
        Ok(Self { client, cfg })
    }

    fn url(&self) -> String {
        format!(
            "{}{}",
            self.cfg.base_url.trim_end_matches('/'),
            if self.cfg.query_path.starts_with('/') {
                self.cfg.query_path.clone()
            } else {
                format!("/{}", self.cfg.query_path)
            }
        )
    }

    async fn cypher(&self, cypher: &str, params: serde_json::Value) -> Result<serde_json::Value> {
        #[derive(Serialize)]
        struct Req<'a> {
            cypher: &'a str,
            params: serde_json::Value,
        }

        let resp = self
            .client
            .post(self.url())
            .json(&Req { cypher, params })
            .send()
            .await?
            .error_for_status()?;
        Ok(resp.json::<serde_json::Value>().await?)
    }

    fn rows(v: serde_json::Value) -> Vec<serde_json::Value> {
        match v {
            serde_json::Value::Array(a) => a,
            serde_json::Value::Object(mut o) => {
                for key in ["rows", "data", "result", "results"] {
                    if let Some(val) = o.remove(key) {
                        return Self::rows(val);
                    }
                }
                vec![serde_json::Value::Object(o)]
            }
            other => vec![other],
        }
    }

    fn decode_item(row: &serde_json::Value) -> Option<MemoryItem> {
        // We try a few common shapes:
        // - {"item": { ...MemoryItem... }}
        // - {"item_json": "{...}"} or {"item_json": {...}}
        // - {...MemoryItem...}
        match row {
            serde_json::Value::Object(map) => {
                if let Some(v) = map.get("item") {
                    return serde_json::from_value(v.clone()).ok();
                }
                if let Some(v) = map.get("item_json") {
                    match v {
                        serde_json::Value::String(s) => return serde_json::from_str(s).ok(),
                        serde_json::Value::Object(_) => {
                            return serde_json::from_value(v.clone()).ok();
                        }
                        _ => {}
                    }
                }
                serde_json::from_value(row.clone()).ok()
            }
            _ => serde_json::from_value(row.clone()).ok(),
        }
    }
}

#[async_trait]
impl MemoryStore for HelixCypherStore {
    #[tracing::instrument(skip_all)]
    async fn append(&self, item: MemoryItem) -> Result<()> {
        let item_json = serde_json::to_string(&item)?;
        let cypher = r#"
MERGE (o:Org {org_id: $org_id})
MERGE (a:Agent {org_id: $org_id, agent_id: $agent_id})
MERGE (a)-[:BELONGS_TO]->(o)
CREATE (m:MemoryItem {id: $id, org_id: $org_id, agent_id: $agent_id, item_type: $item_type, created_at: $created_at, item_json: $item_json})
CREATE (a)-[:HAS_MEMORY]->(m)
RETURN m.id as id
"#;
        let params = serde_json::json!({
            "id": item.id.to_string(),
            "org_id": item.org_id,
            "agent_id": item.agent_id,
            "item_type": item.item_type.0,
            "created_at": item.created_at.to_rfc3339(),
            "item_json": item_json,
        });
        let _ = self.cypher(cypher, params).await?;
        Ok(())
    }

    #[tracing::instrument(skip_all)]
    async fn get(&self, scope: &Scope, id: Ulid) -> Result<Option<MemoryItem>> {
        let cypher = r#"
MATCH (m:MemoryItem {id: $id, org_id: $org_id, agent_id: $agent_id})
RETURN m.item_json as item_json
LIMIT 1
"#;
        let params = serde_json::json!({
            "id": id.to_string(),
            "org_id": scope.org_id,
            "agent_id": scope.agent_id,
        });
        let resp = self.cypher(cypher, params).await?;
        let mut rows = Self::rows(resp);
        if rows.is_empty() {
            return Ok(None);
        }
        let row = rows.remove(0);
        let Some(item) = Self::decode_item(&row) else {
            return Ok(None);
        };
        Ok(Some(item))
    }

    #[tracing::instrument(skip_all)]
    async fn list(&self, scope: &Scope, filter: &StoreListFilter) -> Result<Vec<MemoryItem>> {
        // Note: for Helix we keep the query simple and apply some filters in-memory
        // to avoid needing advanced parameterization support from the backend.
        let cypher = r#"
MATCH (m:MemoryItem {org_id: $org_id, agent_id: $agent_id})
RETURN m.item_json as item_json, m.created_at as created_at
"#;
        let params = serde_json::json!({
            "org_id": scope.org_id,
            "agent_id": scope.agent_id,
        });
        let resp = self.cypher(cypher, params).await?;
        let rows = Self::rows(resp);
        let mut items: Vec<MemoryItem> = rows
            .iter()
            .filter_map(Self::decode_item)
            .filter(|it| {
                if !filter.include_episode_summaries
                    && it.item_type == MemoryType::episode_summary()
                {
                    return false;
                }
                if let Some(types) = &filter.type_filter {
                    if !types.contains(&it.item_type) {
                        return false;
                    }
                }
                if let Some(after) = filter.created_after.as_ref() {
                    if it.created_at < *after {
                        return false;
                    }
                }
                if let Some(before) = filter.created_before.as_ref() {
                    if it.created_at > *before {
                        return false;
                    }
                }
                true
            })
            .collect();

        items.sort_by_key(|it| std::cmp::Reverse(it.created_at));
        if let Some(limit) = filter.limit {
            items.truncate(limit);
        }
        Ok(items)
    }
}

#[derive(Debug, Clone)]
pub struct HttpVectorIndex {
    client: reqwest::Client,
    cfg: VectorHttpConfig,
}

impl HttpVectorIndex {
    #[tracing::instrument]
    pub fn new(cfg: VectorHttpConfig) -> Result<Self> {
        if cfg.base_url.trim().is_empty() {
            return Err(VoyagerError::InvalidConfig(
                "vector.base_url is required".to_string(),
            ));
        }
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_millis(cfg.timeout_ms))
            .build()?;
        Ok(Self { client, cfg })
    }

    fn url(&self, path: &str) -> String {
        format!(
            "{}{}",
            self.cfg.base_url.trim_end_matches('/'),
            if path.starts_with('/') {
                path.to_string()
            } else {
                format!("/{}", path)
            }
        )
    }
}

#[derive(Debug, Serialize)]
struct UpsertReq<'a> {
    org_id: &'a str,
    agent_id: &'a str,
    id: String,
    embedding: Vec<f32>,
    created_at: String,
    item_type: &'a str,
    importance_0_to_1: Option<f32>,
}

#[derive(Debug, Serialize)]
struct SearchReq<'a> {
    org_id: &'a str,
    agent_id: &'a str,
    embedding: Vec<f32>,
    limit: usize,
    type_filter: Option<Vec<String>>,
    include_episode_summaries: bool,
    created_after: Option<String>,
    created_before: Option<String>,
}

#[derive(Debug, Deserialize)]
struct SearchResp {
    matches: Vec<SearchMatch>,
}

#[derive(Debug, Deserialize)]
struct SearchMatch {
    id: String,
    similarity: f32,
    created_at: String,
    item_type: String,
}

#[async_trait]
impl VectorIndex for HttpVectorIndex {
    #[tracing::instrument(skip_all)]
    async fn upsert(&self, scope: &Scope, item: &MemoryItem, embedding: Vec<f32>) -> Result<()> {
        let req = UpsertReq {
            org_id: &scope.org_id,
            agent_id: &scope.agent_id,
            id: item.id.to_string(),
            embedding,
            created_at: item.created_at.to_rfc3339(),
            item_type: &item.item_type.0,
            importance_0_to_1: item.importance_0_to_1,
        };
        self.client
            .post(self.url(&self.cfg.upsert_path))
            .json(&req)
            .send()
            .await?
            .error_for_status()?;
        Ok(())
    }

    #[tracing::instrument(skip_all)]
    async fn search(
        &self,
        scope: &Scope,
        query_embedding: Vec<f32>,
        filter: &VectorSearchFilter,
    ) -> Result<Vec<VectorMatch>> {
        let req = SearchReq {
            org_id: &scope.org_id,
            agent_id: &scope.agent_id,
            embedding: query_embedding,
            limit: filter.limit,
            type_filter: filter
                .type_filter
                .as_ref()
                .map(|v| v.iter().map(|t| t.0.clone()).collect()),
            include_episode_summaries: filter.include_episode_summaries,
            created_after: filter.created_after.map(|d| d.to_rfc3339()),
            created_before: filter.created_before.map(|d| d.to_rfc3339()),
        };
        let resp = self
            .client
            .post(self.url(&self.cfg.search_path))
            .json(&req)
            .send()
            .await?
            .error_for_status()?;
        let body: serde_json::Value = resp.json().await?;
        // Accept either {"matches":[...]} or {"data":{"matches":[...]}} shapes.
        let matches_value = body
            .get("matches")
            .cloned()
            .or_else(|| body.get("data").and_then(|d| d.get("matches")).cloned())
            .unwrap_or_else(|| serde_json::Value::Array(vec![]));
        let parsed: SearchResp =
            serde_json::from_value(serde_json::json!({ "matches": matches_value }))?;
        let mut out = vec![];
        for m in parsed.matches {
            let id = m
                .id
                .parse::<Ulid>()
                .map_err(|e| VoyagerError::VectorIndex(format!("invalid ULID in response: {e}")))?;
            let created_at = DateTime::parse_from_rfc3339(&m.created_at)
                .map_err(|e| {
                    VoyagerError::VectorIndex(format!("invalid created_at in response: {e}"))
                })?
                .with_timezone(&Utc);
            out.push(VectorMatch {
                id,
                similarity: m.similarity,
                created_at,
                item_type: MemoryType(m.item_type),
            });
        }
        Ok(out)
    }
}

pub type DynMemoryStore = Arc<dyn MemoryStore>;
pub type DynVectorIndex = Arc<dyn VectorIndex>;