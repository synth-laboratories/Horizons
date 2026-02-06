# voyager

General-purpose long-term agent memory: append-only storage, semantic retrieval, and optional
batch summarization.

This crate is standalone and has **no dependency on Horizons**.

## Concepts

- **Scope**: `{ org_id, agent_id }` (tenant and agent isolation).
- **MemoryItem**: append-only record with a type and JSON content.
- **index_text**: optional natural-language text used for embeddings (useful for skill/code entries).
- **Retrieval**: vector similarity + a configurable recency bias.
- **Summarization**: compress older items into an `episode_summary` item.

## Minimal Usage (in-memory)

`voyager` is designed to be backend-agnostic. For tests and small use cases you can use the
in-memory implementations.

```rust
use std::sync::Arc;
use voyager::config::VoyagerConfig;
use voyager::memory::{EmbeddingModel, VoyagerMemory};
use voyager::models::{MemoryType, RetrievalQuery, Scope};
use voyager::store::{InMemoryStore, InMemoryVectorIndex};
use voyager::summarizer::SummarizationModel;
use voyager::Result;

#[derive(Debug)]
struct DummyEmbedder;

#[async_trait::async_trait]
impl EmbeddingModel for DummyEmbedder {
    async fn embed(&self, _scope: &Scope, text: &str) -> Result<Vec<f32>> {
        // Replace with a real embedder.
        Ok(vec![text.len() as f32])
    }
    fn name(&self) -> &'static str { "dummy" }
}

#[derive(Debug)]
struct DummySummarizer;

#[async_trait::async_trait]
impl SummarizationModel for DummySummarizer {
    async fn summarize(&self, _scope: &Scope, items: &[voyager::models::MemoryItem]) -> Result<String> {
        Ok(format!("{} items", items.len()))
    }
    fn name(&self) -> &'static str { "dummy" }
}

#[tokio::main]
async fn main() -> Result<()> {
    let scope = Scope::new("org_123", "agent_abc");

    let store = Arc::new(InMemoryStore::new());
    let vectors = Arc::new(InMemoryVectorIndex::new());
    let embedder = Arc::new(DummyEmbedder);
    let summarizer = Arc::new(DummySummarizer);

    let cfg = VoyagerConfig::default();
    let mem = VoyagerMemory::new(store, vectors, embedder, summarizer, cfg);

    mem.append_text(&scope, MemoryType::observation(), "met Alice at the meetup").await?;
    let items = mem.retrieve(&scope, RetrievalQuery::new("Alice", 5)).await?;
    println!("items={}", items.len());
    Ok(())
}
```
