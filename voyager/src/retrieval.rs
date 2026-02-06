use crate::config::RetrievalConfig;
use crate::models::{MemoryItem, VectorMatch};
use chrono::{DateTime, Utc};

#[derive(Debug, Clone)]
pub struct ScoredItem {
    pub item: MemoryItem,
    pub similarity: f32,
    pub recency: f32,
    pub importance: f32,
    pub score: f32,
}

#[tracing::instrument(skip_all)]
pub fn rerank_with_recency(
    items: Vec<(MemoryItem, VectorMatch)>,
    now: DateTime<Utc>,
    cfg: &RetrievalConfig,
) -> Vec<ScoredItem> {
    let w_rel = cfg.relevance_weight.max(0.0);
    let w_rec = cfg.recency_weight.max(0.0);
    let w_imp = cfg.importance_weight.max(0.0);
    let denom = (w_rel + w_rec + w_imp).max(f32::MIN_POSITIVE);
    let w_rel = w_rel / denom;
    let w_rec = w_rec / denom;
    let w_imp = w_imp / denom;
    let half_life_ms = cfg.recency_half_life_ms.max(1) as f64;
    let ln2 = std::f64::consts::LN_2;

    let mut scored: Vec<ScoredItem> = items
        .into_iter()
        .map(|(item, m)| {
            let age_ms = (now - item.created_at).num_milliseconds().max(0) as f64;
            let recency = (-ln2 * age_ms / half_life_ms).exp() as f32;
            let sim = m.similarity.clamp(0.0, 1.0);
            let importance = item.importance_0_to_1.unwrap_or(0.0).clamp(0.0, 1.0);
            // Voyager/Reflexion-style retrieval: combine relevance, recency, and importance.
            let score = w_rel * sim + w_rec * recency + w_imp * importance;
            ScoredItem {
                item,
                similarity: sim,
                recency,
                importance,
                score,
            }
        })
        .collect();

    scored.sort_by(|a, b| {
        b.score
            .partial_cmp(&a.score)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| b.item.created_at.cmp(&a.item.created_at))
            .then_with(|| b.item.id.cmp(&a.item.id))
    });

    scored
}

#[tracing::instrument(skip_all)]
pub fn rerank(
    items: Vec<(MemoryItem, VectorMatch)>,
    now: DateTime<Utc>,
    cfg: &RetrievalConfig,
) -> Vec<ScoredItem> {
    rerank_with_recency(items, now, cfg)
}
