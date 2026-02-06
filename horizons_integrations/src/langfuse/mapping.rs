use crate::langfuse::types::{
    GenerationCreate, IngestionBatch, IngestionEvent, SpanCreate, TraceCreate,
};
use horizons_core::Result;
use horizons_core::o11y::traits::OtelSpan;
use std::collections::{BTreeMap, HashMap};

#[tracing::instrument(level = "debug", skip_all)]
pub fn spans_to_langfuse_batch(spans: &[OtelSpan]) -> Result<IngestionBatch> {
    let mut by_trace: HashMap<&str, Vec<&OtelSpan>> = HashMap::new();
    for s in spans {
        by_trace.entry(&s.trace_id).or_default().push(s);
    }

    let mut batch: Vec<IngestionEvent> = Vec::new();
    for (_trace_id, mut ts) in by_trace {
        // Sort by start time; root is the earliest span with no parent.
        ts.sort_by_key(|s| s.start_time);
        let root = ts
            .iter()
            .find(|s| s.parent_span_id.is_none())
            .copied()
            .unwrap_or(ts[0]);

        batch.push(IngestionEvent::TraceCreate(TraceCreate {
            id: root.trace_id.clone(),
            timestamp: root.start_time,
            name: root.name.clone(),
            metadata: root.attributes.clone(),
        }));

        for s in ts {
            if s.span_id == root.span_id {
                continue;
            }
            if is_generation_span(s) {
                batch.push(IngestionEvent::GenerationCreate(map_generation(s)));
            } else {
                batch.push(IngestionEvent::SpanCreate(SpanCreate {
                    id: s.span_id.clone(),
                    trace_id: s.trace_id.clone(),
                    parent_observation_id: s.parent_span_id.clone(),
                    name: s.name.clone(),
                    start_time: s.start_time,
                    end_time: s.end_time,
                    metadata: s.attributes.clone(),
                }));
            }
        }
    }

    Ok(IngestionBatch { batch })
}

fn is_generation_span(s: &OtelSpan) -> bool {
    s.name.starts_with("gen_ai.")
}

fn map_generation(s: &OtelSpan) -> GenerationCreate {
    let model = s
        .attributes
        .get("gen_ai.request.model")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    let mut usage = BTreeMap::new();
    for k in [
        "gen_ai.usage.prompt_tokens",
        "gen_ai.usage.completion_tokens",
        "gen_ai.usage.total_tokens",
        "gen_ai.usage.cost",
    ] {
        if let Some(v) = s.attributes.get(k) {
            usage.insert(k.to_string(), v.clone());
        }
    }

    GenerationCreate {
        id: s.span_id.clone(),
        trace_id: s.trace_id.clone(),
        parent_observation_id: s.parent_span_id.clone(),
        name: s.name.clone(),
        start_time: s.start_time,
        end_time: s.end_time,
        model,
        usage,
        metadata: s.attributes.clone(),
    }
}
