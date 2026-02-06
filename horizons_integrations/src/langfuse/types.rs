use chrono::{DateTime, Utc};
use serde::Serialize;
use std::collections::BTreeMap;

#[derive(Debug, Clone, Serialize)]
pub struct IngestionBatch {
    pub batch: Vec<IngestionEvent>,
}

/// Langfuse ingestion events (minimal subset).
///
/// This is intentionally conservative: Langfuse's ingestion API evolves; Horizons maps
/// OTel-like spans into a small set of observation primitives.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum IngestionEvent {
    TraceCreate(TraceCreate),
    SpanCreate(SpanCreate),
    GenerationCreate(GenerationCreate),
}

#[derive(Debug, Clone, Serialize)]
pub struct TraceCreate {
    pub id: String,
    #[serde(rename = "timestamp")]
    pub timestamp: DateTime<Utc>,
    pub name: String,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, serde_json::Value>,
}

#[derive(Debug, Clone, Serialize)]
pub struct SpanCreate {
    pub id: String,
    #[serde(rename = "traceId")]
    pub trace_id: String,
    #[serde(
        rename = "parentObservationId",
        skip_serializing_if = "Option::is_none"
    )]
    pub parent_observation_id: Option<String>,
    pub name: String,
    #[serde(rename = "startTime")]
    pub start_time: DateTime<Utc>,
    #[serde(rename = "endTime")]
    pub end_time: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, serde_json::Value>,
}

#[derive(Debug, Clone, Serialize)]
pub struct GenerationCreate {
    pub id: String,
    #[serde(rename = "traceId")]
    pub trace_id: String,
    #[serde(
        rename = "parentObservationId",
        skip_serializing_if = "Option::is_none"
    )]
    pub parent_observation_id: Option<String>,
    pub name: String,
    #[serde(rename = "startTime")]
    pub start_time: DateTime<Utc>,
    #[serde(rename = "endTime")]
    pub end_time: DateTime<Utc>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub usage: BTreeMap<String, serde_json::Value>,

    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, serde_json::Value>,
}
