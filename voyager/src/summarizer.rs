use crate::config::SummarizationConfig;
use crate::models::{MemoryItem, MemoryType, Scope, Summary};
use crate::{Result, VoyagerError};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use ulid::Ulid;

#[async_trait]
pub trait SummarizationModel: Send + Sync {
    async fn summarize(&self, scope: &Scope, items: &[MemoryItem]) -> Result<String>;
    fn name(&self) -> &'static str;
}

#[derive(Debug, Clone)]
pub struct SummaryBuild {
    pub summary: Summary,
    pub item: MemoryItem,
}

#[tracing::instrument]
pub fn parse_horizon(horizon: &str) -> Result<std::time::Duration> {
    let s = horizon.trim();
    if s.is_empty() {
        return Err(VoyagerError::InvalidArgument(
            "horizon must be non-empty".to_string(),
        ));
    }

    let (num, suffix) = s
        .chars()
        .position(|c| !c.is_ascii_digit())
        .map(|idx| s.split_at(idx))
        .unwrap_or((s, ""));

    let n: u64 = num
        .parse()
        .map_err(|_| VoyagerError::InvalidArgument(format!("invalid horizon number: {horizon}")))?;

    let dur = match suffix {
        "ms" => std::time::Duration::from_millis(n),
        "s" => std::time::Duration::from_secs(n),
        "m" => std::time::Duration::from_secs(n * 60),
        "h" => std::time::Duration::from_secs(n * 60 * 60),
        "d" => std::time::Duration::from_secs(n * 60 * 60 * 24),
        _ => {
            return Err(VoyagerError::InvalidArgument(format!(
                "invalid horizon suffix (expected ms|s|m|h|d): {horizon}"
            )));
        }
    };
    if dur.is_zero() {
        return Err(VoyagerError::InvalidArgument(
            "horizon must be > 0".to_string(),
        ));
    }
    Ok(dur)
}

#[tracing::instrument(skip_all)]
pub fn build_summary_item(
    scope: &Scope,
    now: DateTime<Utc>,
    horizon_start: DateTime<Utc>,
    horizon_end: DateTime<Utc>,
    item_ids: Vec<Ulid>,
    text: String,
) -> SummaryBuild {
    let id = Ulid::new();
    let summary = Summary {
        id,
        org_id: scope.org_id.clone(),
        agent_id: scope.agent_id.clone(),
        created_at: now,
        horizon_start,
        horizon_end,
        item_ids: item_ids.clone(),
        text: text.clone(),
    };

    let item = MemoryItem {
        id,
        org_id: scope.org_id.clone(),
        agent_id: scope.agent_id.clone(),
        item_type: MemoryType::episode_summary(),
        index_text: None,
        importance_0_to_1: None,
        content: serde_json::json!({
            "text": text,
            "item_ids": item_ids.iter().map(|u| u.to_string()).collect::<Vec<_>>(),
            "horizon_start": horizon_start.to_rfc3339(),
            "horizon_end": horizon_end.to_rfc3339(),
        }),
        created_at: now,
    };

    SummaryBuild { summary, item }
}

#[tracing::instrument(skip_all)]
pub fn select_eligible_items_for_summary(
    all_items: &[MemoryItem],
    now: DateTime<Utc>,
    horizon: std::time::Duration,
    cfg: &SummarizationConfig,
) -> (DateTime<Utc>, DateTime<Utc>, Vec<MemoryItem>) {
    let cutoff =
        now - chrono::Duration::milliseconds(horizon.as_millis().min(i64::MAX as u128) as i64);

    // "Compress old items": summarize items created at or before the cutoff.
    // Summaries are special items and are not summarized again.
    let mut candidates: Vec<MemoryItem> = all_items
        .iter()
        .filter(|it| it.item_type != MemoryType::episode_summary())
        .filter(|it| it.created_at <= cutoff)
        .cloned()
        .collect();

    // Oldest first so the summarizer reads in time order.
    candidates.sort_by_key(|it| it.created_at);

    if candidates.len() > cfg.max_items {
        candidates.truncate(cfg.max_items);
    }

    let horizon_start = candidates.first().map(|i| i.created_at).unwrap_or(cutoff);
    let horizon_end = candidates.last().map(|i| i.created_at).unwrap_or(cutoff);

    (horizon_start, horizon_end, candidates)
}
