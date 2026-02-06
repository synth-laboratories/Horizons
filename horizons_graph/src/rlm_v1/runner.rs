use std::time::Instant;

use async_trait::async_trait;
use serde_json::{Value, json};

use super::errors::{RlmErrorInfo, RlmErrorKind, RlmRunError};
use super::limits::{LimitStatus, RlmLimits};
use super::messages::{
    build_system_message, estimate_messages_tokens, sanitize_message_sequence, truncate_text,
};
use super::output::{EvidenceItem, RlmRunResult, RlmStats};
use super::schema::{ResolvedAnswerSchema, resolve_answer_schema, validate_answer_simple};
use super::tools::{
    ToolCall, ToolContext, ToolExecutor, ToolRegistry, ToolResult, build_tool_registry,
    tool_denied_result,
};
use super::trace::{LmCallTrace, RlmExecutionTrace, ToolCallTrace};
use super::{LmClient, LmRequest, RlmRunRequest, UsageMetrics};

const HISTORY_FILENAME: &str = "rlm_history.jsonl";
const HISTORY_FIELD_NAME: &str = "_rlm_history";
const MAX_TOOL_OUTPUT_CHARS: usize = 4000;
const MAX_EVENT_PREVIEW_CHARS: usize = 4000;
const TRACE_PROMPT_CHARS: usize = 8000;
const EVIDENCE_HEADER: &str =
    "ACCUMULATED EVIDENCE (recorded via add_evidence tool - DO NOT re-record these):";
const MUST_USE_TOOLS_MESSAGE: &str = "You MUST use tools and then call submit_answer.";

#[async_trait]
pub trait HistoryStore: Send + Sync {
    async fn append(
        &self,
        filename: &str,
        field_name: &str,
        content: &str,
    ) -> Result<(), RlmRunError>;
}

#[async_trait]
pub trait RlmTraceSink: Send + Sync {
    async fn now_ms(&self) -> i64;
    async fn record_prompt_render(&self, _elapsed_ms: i64) {}
    async fn record_message_serialize(&self, _elapsed_ms: i64) {}
    async fn record_response_parse(&self, _elapsed_ms: i64) {}
    async fn record_tool_dispatch(&self, _elapsed_ms: i64) {}
    async fn record_message_update(&self, _elapsed_ms: i64) {}
    async fn record_tool_result_serialize(&self, _elapsed_ms: i64) {}
    async fn record_tool_result_process(&self, _elapsed_ms: i64) {}
    async fn record_summarize_overhead(&self, _elapsed_ms: i64) {}
    async fn record_llm_prompt(
        &self,
        _node: &str,
        _model: &str,
        _iteration: i64,
        _messages: Vec<Value>,
    ) {
    }
    async fn record_llm_call(
        &self,
        _model: &str,
        _node: &str,
        _iteration: i64,
        _started_at_ms: i64,
        _elapsed_ms: i64,
        _prompt_tokens: i64,
        _completion_tokens: i64,
        _tool_enabled: bool,
        _tool_calls_count: i64,
    ) {
    }
    async fn record_tool_call(
        &self,
        _tool: &str,
        _node: &str,
        _iteration: i64,
        _started_at_ms: i64,
        _elapsed_ms: i64,
        _execution_mode: &str,
        _args: Value,
        _success: bool,
        _sandbox_timing: Option<Value>,
    ) {
    }
}

#[async_trait]
pub trait RlmEventSink: Send + Sync {
    async fn emit(&self, event_type: &str, payload: Value);
}

#[derive(Clone)]
pub struct RlmRunContext {
    pub run_id: String,
    pub node_name: String,
    pub rlm_impl: String,
    pub history_store: Option<std::sync::Arc<dyn HistoryStore>>,
    pub trace_sink: Option<std::sync::Arc<dyn RlmTraceSink>>,
    pub event_sink: Option<std::sync::Arc<dyn RlmEventSink>>,
}

impl std::fmt::Debug for RlmRunContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RlmRunContext")
            .field("run_id", &self.run_id)
            .field("node_name", &self.node_name)
            .field("rlm_impl", &self.rlm_impl)
            .field("history_store", &self.history_store.as_ref().map(|_| "..."))
            .field("trace_sink", &self.trace_sink.as_ref().map(|_| "..."))
            .field("event_sink", &self.event_sink.as_ref().map(|_| "..."))
            .finish()
    }
}

pub async fn run_rlm(
    request: RlmRunRequest,
    lm: &dyn LmClient,
    tools: &dyn ToolExecutor,
    ctx: RlmRunContext,
) -> Result<RlmRunResult, RlmRunError> {
    let mut messages = request.messages_seed.clone();
    let resolved_schema = resolve_answer_schema(request.answer_schema.clone(), request.is_verifier);
    let tool_registry = build_tool_registry(
        request.tool_filter.as_deref(),
        request.allowed_tool_capabilities.as_deref(),
        request.is_verifier,
        &resolved_schema,
    );

    let start = Instant::now();
    let mut evidence_list: Vec<EvidenceItem> = Vec::new();
    let mut trace = RlmExecutionTrace::default();
    let mut stats = RlmStats::default();
    let mut iterations_used = 0i64;
    let mut root_calls = 0i64;
    let mut subcalls = 0i64;
    let mut limit_status = LimitStatus::default();

    emit_event(
        &ctx,
        "rlm_started",
        json!({
            "node": ctx.node_name,
            "rlm_impl": ctx.rlm_impl,
            "model": request.model.clone(),
            "max_iterations": request.limits.max_iterations,
            "max_root_calls": request.limits.max_root_calls,
            "max_subcalls": request.limits.max_subcalls,
            "max_time_ms": request.limits.max_time_ms,
            "max_cost_usd": request.limits.max_cost_usd,
        }),
    )
    .await;

    for iteration in 0..request.limits.max_iterations {
        if root_calls >= request.limits.max_root_calls {
            limit_status.hit_root_calls = true;
            break;
        }
        if start.elapsed().as_millis() as i64 >= request.limits.max_time_ms {
            limit_status.hit_time = true;
            break;
        }
        if stats.cost_usd >= request.limits.max_cost_usd {
            limit_status.hit_cost = true;
            break;
        }

        iterations_used = (iteration + 1) as i64;

        emit_event(
            &ctx,
            "rlm_iteration_started",
            json!({
                "node": ctx.node_name,
                "rlm_impl": ctx.rlm_impl,
                "iteration": iterations_used,
                "root_calls": root_calls,
                "subcalls": subcalls,
            }),
        )
        .await;

        if should_compact(&messages, &request.limits) {
            let summarize_overhead_start = Instant::now();
            messages = compact_messages_to_history_file(
                &messages,
                &ctx.history_store,
                request.limits.keep_recent_messages,
            )
            .await;
            if let Some(trace_sink) = ctx.trace_sink.as_ref() {
                let summarize_overhead_ms = summarize_overhead_start.elapsed().as_millis() as i64;
                trace_sink
                    .record_summarize_overhead(summarize_overhead_ms)
                    .await;
            }
        }

        messages = sanitize_message_sequence(&messages);
        let elapsed_ms = start.elapsed().as_millis() as i64;
        let front_matter = build_rlm_front_matter(
            iteration,
            request.limits.max_iterations,
            root_calls,
            request.limits.max_root_calls,
            subcalls,
            request.limits.max_subcalls,
            elapsed_ms,
            request.limits.max_time_ms,
        );

        let message_serialize_start = Instant::now();
        let mut request_messages = Vec::with_capacity(messages.len() + 2);
        request_messages.push(build_system_message(front_matter));
        if !evidence_list.is_empty() {
            request_messages.push(build_system_message(build_evidence_notice(&evidence_list)));
        }
        request_messages.extend(messages.clone());
        if let Some(trace_sink) = ctx.trace_sink.as_ref() {
            let message_serialize_ms = message_serialize_start.elapsed().as_millis() as i64;
            trace_sink
                .record_message_serialize(message_serialize_ms)
                .await;
        }

        let lm_req = LmRequest {
            model: request.model.clone(),
            messages: request_messages.clone(),
            temperature: 0.0,
            max_tokens: None,
            response_format: None,
            tools: Some(tool_registry.schemas.clone()),
            tool_choice: None,
        };

        emit_event(
            &ctx,
            "rlm_llm_request",
            json!({
                "node": ctx.node_name,
                "rlm_impl": ctx.rlm_impl,
                "iteration": iterations_used,
                "model": request.model.clone(),
                "prompt_tokens_est": estimate_messages_tokens(&request_messages),
                "tool_count": tool_registry.schemas.len(),
            }),
        )
        .await;

        if let Some(trace_sink) = ctx.trace_sink.as_ref() {
            trace_sink
                .record_llm_prompt(
                    &ctx.node_name,
                    &request.model,
                    iterations_used,
                    request_messages.clone(),
                )
                .await;
        }

        let llm_started_at_ms = if let Some(trace_sink) = ctx.trace_sink.as_ref() {
            trace_sink.now_ms().await
        } else {
            0
        };

        let lm_resp = lm
            .call(lm_req)
            .await
            .map_err(|err| RlmRunError::Lm(err.message))?;
        root_calls += 1;
        stats = record_usage(stats, &lm_resp.usage, lm_resp.elapsed_ms);
        stats.root_calls = root_calls;
        stats.subcalls = subcalls;
        if stats.cost_usd >= request.limits.max_cost_usd {
            limit_status.hit_cost = true;
            break;
        }

        if let Some(trace_sink) = ctx.trace_sink.as_ref() {
            trace_sink
                .record_llm_call(
                    &request.model,
                    &ctx.node_name,
                    iterations_used,
                    llm_started_at_ms,
                    lm_resp.elapsed_ms,
                    lm_resp.usage.prompt_tokens,
                    lm_resp.usage.completion_tokens,
                    true,
                    lm_resp.tool_calls.len() as i64,
                )
                .await;
        }

        emit_event(
            &ctx,
            "rlm_llm_response",
            json!({
                "node": ctx.node_name,
                "rlm_impl": ctx.rlm_impl,
                "iteration": iterations_used,
                "model": request.model.clone(),
                "prompt_tokens": lm_resp.usage.prompt_tokens,
                "completion_tokens": lm_resp.usage.completion_tokens,
                "elapsed_ms": lm_resp.elapsed_ms,
                "tool_calls": lm_resp.tool_calls.len(),
            }),
        )
        .await;

        trace.lm_calls.push(LmCallTrace {
            model: request.model.clone(),
            iteration: iterations_used,
            started_at_ms: llm_started_at_ms,
            elapsed_ms: lm_resp.elapsed_ms,
            prompt_tokens: lm_resp.usage.prompt_tokens,
            completion_tokens: lm_resp.usage.completion_tokens,
            tool_calls_count: lm_resp.tool_calls.len() as i64,
            prompt_messages: summarize_messages_for_trace(&request_messages),
        });

        if lm_resp.tool_calls.is_empty() {
            messages.push(build_system_message(MUST_USE_TOOLS_MESSAGE.to_string()));
            emit_event(
                &ctx,
                "rlm_iteration_completed",
                json!({
                    "node": ctx.node_name,
                    "rlm_impl": ctx.rlm_impl,
                    "iteration": iterations_used,
                    "root_calls": root_calls,
                    "subcalls": subcalls,
                    "cost_usd": stats.cost_usd,
                }),
            )
            .await;
            continue;
        }

        if let Some(result) = handle_terminal_tools(
            &lm_resp.tool_calls,
            &resolved_schema,
            &mut messages,
            &evidence_list,
            &trace,
            &stats,
            &start,
            iterations_used,
            &ctx,
        )
        .await?
        {
            return Ok(result);
        }

        let message_update_start = Instant::now();
        let tool_call_payloads = lm_resp
            .tool_calls
            .iter()
            .enumerate()
            .map(|(idx, tc)| {
                json!({
                    "id": tc.id.clone().unwrap_or_else(|| format!("call_{idx}")),
                    "type": "function",
                    "function": {
                        "name": tc.name,
                        "arguments": serde_json::to_string(&tc.arguments).unwrap_or_else(|_| "{}".to_string())
                    }
                })
            })
            .collect::<Vec<Value>>();
        messages.push(json!({
            "role": "assistant",
            "content": lm_resp.text,
            "tool_calls": tool_call_payloads,
        }));
        if let Some(trace_sink) = ctx.trace_sink.as_ref() {
            let message_update_ms = message_update_start.elapsed().as_millis() as i64;
            trace_sink.record_message_update(message_update_ms).await;
        }

        let remaining = remaining_subcalls(request.limits.max_subcalls, subcalls);
        if remaining == 0 {
            continue;
        }

        let calls_to_run = lm_resp
            .tool_calls
            .into_iter()
            .take(remaining)
            .collect::<Vec<_>>();

        if should_parallelize(&calls_to_run, &request, &tool_registry) {
            emit_tool_calls_started(&ctx, iterations_used, &calls_to_run).await;
            let ctx_for_tools =
                build_tool_context(&ctx, &request, iterations_used, root_calls, subcalls);
            let results = tools
                .execute_many_concurrently(calls_to_run.clone(), ctx_for_tools)
                .await;
            record_parallel_results(
                calls_to_run,
                results,
                iterations_used,
                &mut messages,
                &mut trace,
                &ctx,
                &tool_registry,
                &mut subcalls,
            )
            .await?;
        } else {
            let mut tool_dispatch_overhead_ms = 0i64;
            for (idx, mut call) in calls_to_run.into_iter().enumerate() {
                if subcalls >= request.limits.max_subcalls {
                    break;
                }
                if call.id.is_none() {
                    call.id = Some(format!("call_{idx}"));
                }
                emit_tool_call_started(&ctx, iterations_used, &call).await;
                let dispatch_start = Instant::now();
                let tool_started_at_ms = if let Some(trace_sink) = ctx.trace_sink.as_ref() {
                    trace_sink.now_ms().await
                } else {
                    0
                };

                let tool_result = execute_tool_call(
                    call.clone(),
                    &request,
                    &tool_registry,
                    &mut evidence_list,
                    iterations_used,
                    build_tool_context(&ctx, &request, iterations_used, root_calls, subcalls),
                    lm,
                    tools,
                    &mut stats,
                )
                .await?;

                let dispatch_elapsed_ms = dispatch_start.elapsed().as_millis() as i64;
                let overhead = (dispatch_elapsed_ms - tool_result.elapsed_ms).max(0);
                tool_dispatch_overhead_ms += overhead;

                subcalls += 1;
                record_tool_result(
                    call,
                    tool_result,
                    iterations_used,
                    tool_started_at_ms,
                    &mut messages,
                    &mut trace,
                    &ctx,
                )
                .await?;
            }
            if tool_dispatch_overhead_ms > 0 {
                if let Some(trace_sink) = ctx.trace_sink.as_ref() {
                    trace_sink
                        .record_tool_dispatch(tool_dispatch_overhead_ms)
                        .await;
                }
            }
        }
        stats.subcalls = subcalls;
        stats.root_calls = root_calls;

        emit_event(
            &ctx,
            "rlm_iteration_completed",
            json!({
                "node": ctx.node_name,
                "rlm_impl": ctx.rlm_impl,
                "iteration": iterations_used,
                "root_calls": root_calls,
                "subcalls": subcalls,
                "cost_usd": stats.cost_usd,
            }),
        )
        .await;
    }

    limit_status.hit_iterations =
        !limit_status.hit_root_calls && !limit_status.hit_time && !limit_status.hit_cost;

    let elapsed_ms = start.elapsed().as_millis() as i64;
    let reasons = limit_status.reasons();
    let reason_text = if reasons.is_empty() {
        "unknown".to_string()
    } else {
        reasons.join(",")
    };
    let limit_message = format!(
        "RLM max iterations or budget reached (reason: {reason_text}; iterations {iterations_used}/{max_iterations}, root_calls {root_calls}/{max_root_calls}, subcalls {subcalls}/{max_subcalls}, elapsed_ms {elapsed_ms}/{max_time_ms}, cost_usd {cost_usd:.6}/{max_cost_usd:.6})",
        reason_text = reason_text,
        iterations_used = iterations_used,
        max_iterations = request.limits.max_iterations,
        root_calls = root_calls,
        max_root_calls = request.limits.max_root_calls,
        subcalls = subcalls,
        max_subcalls = request.limits.max_subcalls,
        elapsed_ms = elapsed_ms,
        max_time_ms = request.limits.max_time_ms,
        cost_usd = stats.cost_usd,
        max_cost_usd = request.limits.max_cost_usd,
    );

    Ok(build_error_result(
        RlmErrorKind::LimitReached,
        limit_message,
        &messages,
        evidence_list,
        &stats,
        &trace,
        iterations_used,
        &start,
        Some(&limit_status),
    ))
}

fn record_usage(mut stats: RlmStats, usage: &UsageMetrics, elapsed_ms: i64) -> RlmStats {
    stats.elapsed_ms += elapsed_ms;
    stats.cost_usd += usage.cost_usd;
    stats.cached_tokens += usage.cached_tokens;
    stats.cache_savings_usd += usage.cache_savings_usd;
    stats
}

async fn emit_event(ctx: &RlmRunContext, event_type: &str, payload: Value) {
    if let Some(sink) = ctx.event_sink.as_ref() {
        sink.emit(event_type, payload).await;
    }
}

async fn emit_tool_call_started(ctx: &RlmRunContext, iteration: i64, call: &ToolCall) {
    emit_event(
        ctx,
        "rlm_tool_call_started",
        json!({
            "node": ctx.node_name,
            "rlm_impl": ctx.rlm_impl,
            "iteration": iteration,
            "tool": call.name,
            "call_id": call.id.clone().unwrap_or_else(|| "call".to_string()),
            "args_preview": preview_json(&call.arguments),
        }),
    )
    .await;
}

async fn emit_tool_calls_started(ctx: &RlmRunContext, iteration: i64, calls: &[ToolCall]) {
    for call in calls {
        emit_tool_call_started(ctx, iteration, call).await;
    }
}

fn preview_json(value: &Value) -> String {
    let redacted = redact_value(value);
    let text = redacted.to_string();
    if text.len() > MAX_EVENT_PREVIEW_CHARS {
        let mut truncated = String::new();
        for (idx, ch) in text.chars().enumerate() {
            if idx >= MAX_EVENT_PREVIEW_CHARS {
                truncated.push_str("...");
                break;
            }
            truncated.push(ch);
        }
        return truncated;
    }
    text
}

fn redact_value(value: &Value) -> Value {
    match value {
        Value::Object(map) => {
            let mut redacted = serde_json::Map::new();
            for (key, val) in map {
                if is_sensitive_key(key) {
                    redacted.insert(key.clone(), Value::String("[redacted]".to_string()));
                } else {
                    redacted.insert(key.clone(), redact_value(val));
                }
            }
            Value::Object(redacted)
        }
        Value::Array(items) => Value::Array(items.iter().map(redact_value).collect::<Vec<_>>()),
        other => other.clone(),
    }
}

fn is_sensitive_key(key: &str) -> bool {
    let normalized = key.to_lowercase();
    normalized.contains("api_key")
        || normalized.contains("apikey")
        || normalized.contains("token")
        || normalized.contains("secret")
        || normalized.contains("password")
        || normalized.contains("authorization")
        || normalized.contains("cookie")
        || normalized.contains("session")
        || normalized.contains("access_key")
}

fn extract_error_message(value: &Value) -> Option<String> {
    value
        .get("error")
        .and_then(|v| v.as_str())
        .map(|v| v.to_string())
}

fn should_compact(messages: &[Value], limits: &RlmLimits) -> bool {
    if let Some(budget) = limits.prompt_token_budget {
        return estimate_messages_tokens(messages) > budget;
    }
    messages.len() > limits.max_messages_before_compact
}

async fn compact_messages_to_history_file(
    messages: &[Value],
    history_store: &Option<std::sync::Arc<dyn HistoryStore>>,
    keep_recent_messages: usize,
) -> Vec<Value> {
    if history_store.is_none() {
        return messages.to_vec();
    }

    let mut system_messages: Vec<Value> = Vec::new();
    for msg in messages {
        if msg.get("role").and_then(|v| v.as_str()) == Some("system") {
            system_messages.push(msg.clone());
        } else {
            break;
        }
    }
    let num_system_msgs = system_messages.len();

    let keep_recent = keep_recent_messages.min(messages.len().saturating_sub(num_system_msgs));
    let compact_end = messages.len().saturating_sub(keep_recent);
    if compact_end <= num_system_msgs {
        return messages.to_vec();
    }
    let to_compact = &messages[num_system_msgs..compact_end];

    let mut lines = String::new();
    for (idx, msg) in to_compact.iter().enumerate() {
        let role = msg
            .get("role")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown");
        let entry = json!({
            "kind": "message",
            "index": num_system_msgs + idx,
            "role": role,
            "message": msg,
        });
        lines.push_str(&serde_json::to_string(&entry).unwrap_or_else(|_| "{}".to_string()));
        lines.push('\n');
    }

    if !lines.is_empty() {
        if let Some(store) = history_store {
            let _ = store
                .append(HISTORY_FILENAME, HISTORY_FIELD_NAME, &lines)
                .await;
        }
    }

    let mut compacted = Vec::new();
    compacted.extend(system_messages);

    let already_has_pointer = compacted.iter().any(|msg| {
        msg.get("role").and_then(|v| v.as_str()) == Some("system")
            && msg
                .get("content")
                .and_then(|v| v.as_str())
                .is_some_and(|s| s.contains(HISTORY_FILENAME))
    });
    if !already_has_pointer {
        compacted.push(build_system_message(format!(
            "Older conversation/tool results were compacted to {HISTORY_FILENAME}. \
Use local_search/local_grep/view_lines on that file to retrieve details. \
Do NOT ask to see the full history inline."
        )));
    }

    compacted.extend(messages[compact_end..].iter().cloned());
    compacted
}

fn build_evidence_notice(evidence_list: &[EvidenceItem]) -> String {
    let mut notice = String::from(EVIDENCE_HEADER);
    notice.push('\n');
    for (idx, ev) in evidence_list.iter().enumerate() {
        let snippet_preview = if ev.evidence_snippet.len() > 200 {
            format!("{}...", &ev.evidence_snippet[..200])
        } else {
            ev.evidence_snippet.clone()
        };
        notice.push_str(&format!(
            "{}. {}: {}\n",
            idx + 1,
            ev.description,
            snippet_preview
        ));
    }
    notice
}

fn build_rlm_front_matter(
    iteration: usize,
    max_iterations: usize,
    root_calls: i64,
    max_root_calls: i64,
    subcalls: i64,
    max_subcalls: i64,
    elapsed_ms: i64,
    max_time_ms: i64,
) -> String {
    let iter_used = iteration + 1;
    let iter_pct = percent_used(iter_used as i64, max_iterations as i64);
    let root_pct = percent_used(root_calls, max_root_calls);
    let sub_pct = percent_used(subcalls, max_subcalls);
    let time_pct = percent_used(elapsed_ms, max_time_ms);
    format!(
        "RLM_LIMITS\n\
iteration: {iter_used}/{max_iterations} ({iter_pct}%)\n\
root_calls: {root_calls}/{max_root_calls} ({root_pct}%)\n\
subcalls: {subcalls}/{max_subcalls} ({sub_pct}%)\n\
elapsed_ms: {elapsed_ms}/{max_time_ms} ({time_pct}%)\n\
remaining_ms: {remaining_ms}\n",
        iter_used = iter_used,
        max_iterations = max_iterations,
        iter_pct = iter_pct,
        root_calls = root_calls,
        max_root_calls = max_root_calls,
        root_pct = root_pct,
        subcalls = subcalls,
        max_subcalls = max_subcalls,
        sub_pct = sub_pct,
        elapsed_ms = elapsed_ms,
        max_time_ms = max_time_ms,
        time_pct = time_pct,
        remaining_ms = (max_time_ms - elapsed_ms).max(0),
    )
}

fn percent_used(used: i64, limit: i64) -> i64 {
    if limit <= 0 {
        return 0;
    }
    let used = used.max(0).min(limit);
    (used * 100) / limit
}

fn latest_message_content(messages: &[Value]) -> Option<Value> {
    messages
        .iter()
        .rev()
        .find_map(|msg| msg.get("content").cloned())
}

fn summarize_messages_for_trace(messages: &[Value]) -> Vec<Value> {
    let mut summarized = Vec::with_capacity(messages.len());
    for msg in messages {
        let Some(obj) = msg.as_object() else {
            let text = truncate_text(&msg.to_string(), TRACE_PROMPT_CHARS);
            summarized.push(json!({"role": "unknown", "content": text}));
            continue;
        };
        let role = obj
            .get("role")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown");
        let content_value = obj.get("content").cloned().unwrap_or(Value::Null);
        let content_text = if let Value::String(text) = content_value {
            text
        } else if content_value.is_null() {
            String::new()
        } else {
            content_value.to_string()
        };
        let truncated = truncate_text(&content_text, TRACE_PROMPT_CHARS);
        summarized.push(json!({
            "role": role,
            "content": truncated,
        }));
    }
    summarized
}

fn should_parallelize(
    tool_calls: &[ToolCall],
    request: &RlmRunRequest,
    registry: &ToolRegistry,
) -> bool {
    if !request.supports_parallel_tool_calls {
        return false;
    }
    if tool_calls.len() <= 1 {
        return false;
    }
    for call in tool_calls {
        if call.name == "submit_answer"
            || call.name == "give_up"
            || call.name == "add_evidence"
            || call.name == "delegate_lm"
        {
            return false;
        }
        if call.name == "materialize_context" || call.name == "codex_exec" {
            return false;
        }
        if !registry.allowed_names.contains(&call.name) {
            return false;
        }
    }
    true
}

fn remaining_subcalls(max_subcalls: i64, subcalls: i64) -> usize {
    if max_subcalls <= 0 {
        return 0;
    }
    (max_subcalls - subcalls).max(0) as usize
}

fn build_tool_context(
    ctx: &RlmRunContext,
    request: &RlmRunRequest,
    iteration: i64,
    root_calls: i64,
    subcalls: i64,
) -> ToolContext {
    ToolContext {
        run_id: ctx.run_id.clone(),
        node_name: ctx.node_name.clone(),
        iteration,
        root_calls,
        subcalls,
        inputs: std::sync::Arc::new(request.inputs.clone()),
    }
}

async fn execute_tool_call(
    call: ToolCall,
    request: &RlmRunRequest,
    registry: &ToolRegistry,
    evidence_list: &mut Vec<EvidenceItem>,
    iteration: i64,
    tool_ctx: ToolContext,
    lm: &dyn LmClient,
    tools: &dyn ToolExecutor,
    stats: &mut RlmStats,
) -> Result<ToolResult, RlmRunError> {
    if !registry.allowed_names.contains(&call.name) {
        let denied = tool_denied_result(&call.name, &registry.allowed_names);
        return Ok(ToolResult {
            tool_name: call.name,
            result: denied.clone(),
            agent_visible_payload: denied,
            elapsed_ms: 0,
            success: false,
            execution_mode: Some("local".to_string()),
            sandbox_timing: None,
        });
    }

    if call.name == "add_evidence" {
        let description = call
            .arguments
            .get("description")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let evidence_snippet = call
            .arguments
            .get("evidence_snippet")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        evidence_list.push(EvidenceItem {
            description,
            evidence_snippet,
            iteration: Some(iteration),
        });
        let result = json!({
            "success": true,
            "message": "Evidence recorded",
            "evidence_count": evidence_list.len(),
            "_local": true,
        });
        return Ok(ToolResult {
            tool_name: call.name,
            result: result.clone(),
            agent_visible_payload: result,
            elapsed_ms: 0,
            success: true,
            execution_mode: Some("local".to_string()),
            sandbox_timing: None,
        });
    }

    if call.name == "delegate_lm" {
        let (result, elapsed_ms) =
            execute_delegate_lm(lm, &call.arguments, &request.model, stats).await?;
        return Ok(ToolResult {
            tool_name: call.name,
            result: result.clone(),
            agent_visible_payload: result,
            elapsed_ms,
            success: true,
            execution_mode: Some("local".to_string()),
            sandbox_timing: None,
        });
    }

    let result = tools
        .execute(call.clone(), tool_ctx)
        .await
        .map_err(|err| RlmRunError::Tool(err.message))?;
    Ok(result)
}

async fn record_tool_result(
    call: ToolCall,
    mut tool_result: ToolResult,
    iteration: i64,
    tool_started_at_ms: i64,
    messages: &mut Vec<Value>,
    trace: &mut RlmExecutionTrace,
    ctx: &RlmRunContext,
) -> Result<(), RlmRunError> {
    let result_process_start = Instant::now();
    tool_result.result = truncate_tool_result(&call.name, tool_result.result);
    tool_result.agent_visible_payload = tool_result.result.clone();
    let result_process_ms = result_process_start.elapsed().as_millis() as i64;
    if let Some(trace_sink) = ctx.trace_sink.as_ref() {
        trace_sink
            .record_tool_result_process(result_process_ms)
            .await;
    }

    if let Some(trace_sink) = ctx.trace_sink.as_ref() {
        trace_sink
            .record_tool_call(
                &call.name,
                &ctx.node_name,
                iteration,
                tool_started_at_ms,
                tool_result.elapsed_ms,
                tool_result
                    .execution_mode
                    .clone()
                    .unwrap_or_else(|| "unknown".to_string())
                    .as_str(),
                call.arguments.clone(),
                tool_result.success,
                tool_result.sandbox_timing.clone(),
            )
            .await;
    }

    let event_type = if tool_result.success {
        "rlm_tool_call_completed"
    } else {
        "rlm_tool_call_failed"
    };
    emit_event(
        ctx,
        event_type,
        json!({
            "node": ctx.node_name,
            "rlm_impl": ctx.rlm_impl,
            "iteration": iteration,
            "tool": call.name,
            "call_id": call.id.clone().unwrap_or_else(|| "call".to_string()),
            "elapsed_ms": tool_result.elapsed_ms,
            "success": tool_result.success,
            "error": extract_error_message(&tool_result.result),
            "result_preview": preview_json(&tool_result.result),
        }),
    )
    .await;

    trace.tool_calls.push(ToolCallTrace {
        tool: call.name.clone(),
        args: call.arguments.clone(),
        result: tool_result.result.clone(),
        iteration,
    });

    let tool_serialize_start = Instant::now();
    let tool_content = serde_json::to_string(&tool_result.agent_visible_payload)
        .unwrap_or_else(|_| "{}".to_string());
    let tool_serialize_ms = tool_serialize_start.elapsed().as_millis() as i64;
    if let Some(trace_sink) = ctx.trace_sink.as_ref() {
        trace_sink
            .record_tool_result_serialize(tool_serialize_ms)
            .await;
    }

    let message_update_start = Instant::now();
    messages.push(json!({
        "role": "tool",
        "tool_call_id": call.id.clone().unwrap_or_else(|| "call".to_string()),
        "content": tool_content,
    }));
    if let Some(trace_sink) = ctx.trace_sink.as_ref() {
        let message_update_ms = message_update_start.elapsed().as_millis() as i64;
        trace_sink.record_message_update(message_update_ms).await;
    }

    Ok(())
}

async fn record_parallel_results(
    calls: Vec<ToolCall>,
    results: Vec<Result<ToolResult, super::ToolError>>,
    iteration: i64,
    messages: &mut Vec<Value>,
    trace: &mut RlmExecutionTrace,
    ctx: &RlmRunContext,
    registry: &ToolRegistry,
    subcalls: &mut i64,
) -> Result<(), RlmRunError> {
    for (idx, (call, result)) in calls.into_iter().zip(results.into_iter()).enumerate() {
        let tool_started_at_ms = if let Some(trace_sink) = ctx.trace_sink.as_ref() {
            trace_sink.now_ms().await
        } else {
            0
        };
        let tool_result = match result {
            Ok(res) => res,
            Err(err) => ToolResult {
                tool_name: call.name.clone(),
                result: json!({"success": false, "error": err.message}),
                agent_visible_payload: json!({"success": false, "error": err.message}),
                elapsed_ms: 0,
                success: false,
                execution_mode: Some("unknown".to_string()),
                sandbox_timing: None,
            },
        };

        *subcalls += 1;
        let mut updated_call = call;
        if updated_call.id.is_none() {
            updated_call.id = Some(format!("call_{idx}"));
        }
        if !registry.allowed_names.contains(&updated_call.name) {
            let denied = tool_denied_result(&updated_call.name, &registry.allowed_names);
            messages.push(json!({
                "role": "tool",
                "tool_call_id": updated_call.id.clone().unwrap_or_else(|| format!("call_{idx}")),
                "content": serde_json::to_string(&denied).unwrap_or_else(|_| "{}".to_string()),
            }));
            emit_event(
                ctx,
                "rlm_tool_call_failed",
                json!({
                    "node": ctx.node_name,
                    "rlm_impl": ctx.rlm_impl,
                    "iteration": iteration,
                    "tool": updated_call.name,
                    "call_id": updated_call.id.clone().unwrap_or_else(|| "call".to_string()),
                    "elapsed_ms": tool_result.elapsed_ms,
                    "success": false,
                    "error": "tool_denied",
                }),
            )
            .await;
            continue;
        }
        record_tool_result(
            updated_call,
            tool_result,
            iteration,
            tool_started_at_ms,
            messages,
            trace,
            ctx,
        )
        .await?;
    }
    Ok(())
}

async fn handle_terminal_tools(
    tool_calls: &[ToolCall],
    resolved_schema: &Option<ResolvedAnswerSchema>,
    messages: &mut Vec<Value>,
    evidence_list: &Vec<EvidenceItem>,
    trace: &RlmExecutionTrace,
    stats: &RlmStats,
    start: &Instant,
    iterations_used: i64,
    ctx: &RlmRunContext,
) -> Result<Option<RlmRunResult>, RlmRunError> {
    for tool_call in tool_calls {
        if tool_call.name == "give_up" {
            let reason = tool_call
                .arguments
                .get("reason")
                .and_then(|v| v.as_str())
                .unwrap_or("RLM gave up without a reason.")
                .to_string();
            emit_event(
                ctx,
                "rlm_give_up",
                json!({
                    "node": ctx.node_name,
                    "rlm_impl": ctx.rlm_impl,
                    "iteration": iterations_used,
                    "reason": reason.clone(),
                }),
            )
            .await;
            return Ok(Some(build_error_result(
                RlmErrorKind::GiveUp,
                reason,
                messages,
                evidence_list.clone(),
                stats,
                trace,
                iterations_used,
                start,
                None,
            )));
        }

        if tool_call.name == "submit_answer" {
            let answer = tool_call
                .arguments
                .get("answer")
                .cloned()
                .unwrap_or(Value::Null);
            if let Some(schema) = resolved_schema.as_ref().map(|s| &s.schema) {
                if let Some(error_msg) = validate_answer_simple(&answer, schema) {
                    let tc_id = tool_call
                        .id
                        .clone()
                        .unwrap_or_else(|| "submit_answer_call".to_string());
                    let required = schema
                        .get("required")
                        .and_then(|v| v.as_array())
                        .map(|arr| arr.iter().filter_map(|v| v.as_str()).collect::<Vec<_>>())
                        .unwrap_or_default();
                    messages.push(json!({
                        "role": "assistant",
                        "content": Value::Null,
                        "tool_calls": [{
                            "id": tc_id.clone(),
                            "type": "function",
                            "function": {
                                "name": "submit_answer",
                                "arguments": serde_json::to_string(&tool_call.arguments).unwrap_or_default()
                            }
                        }],
                    }));
                    messages.push(json!({
                        "role": "tool",
                        "tool_call_id": tc_id,
                        "content": serde_json::to_string(&json!({
                            "error": error_msg,
                            "valid": false,
                            "required_format": format!("Answer MUST contain: {}", required.join(", ")),
                            "hint": format!("Call submit_answer again with the correct format. Your answer must be an object with these required fields: {:?}", required),
                        })).unwrap_or_default(),
                    }));
                    return Ok(None);
                }
            }

            emit_event(
                ctx,
                "rlm_submit_answer",
                json!({
                    "node": ctx.node_name,
                    "rlm_impl": ctx.rlm_impl,
                    "iteration": iterations_used,
                    "answer_preview": preview_json(&answer),
                    "schema_valid": true,
                }),
            )
            .await;

            let mut result_value = if answer.is_object() {
                answer
                    .as_object()
                    .cloned()
                    .map(Value::Object)
                    .unwrap_or(json!({}))
            } else {
                json!({"answer": answer})
            };

            if let Value::Object(ref mut obj) = result_value {
                obj.insert(
                    "_rlm_stats".to_string(),
                    json!({
                        "root_calls": stats.root_calls,
                        "subcalls": stats.subcalls,
                        "elapsed_ms": start.elapsed().as_millis() as i64,
                        "cost_usd": stats.cost_usd,
                        "cached_tokens": stats.cached_tokens,
                        "cache_savings_usd": stats.cache_savings_usd
                    }),
                );
                obj.insert(
                    "_trace".to_string(),
                    json!({"tool_calls": trace.tool_calls}),
                );
                obj.insert("_iterations".to_string(), json!(iterations_used));
                obj.insert("_rlm_impl".to_string(), json!("v1"));
            }

            return Ok(Some(RlmRunResult {
                answer: Some(result_value),
                confidence: tool_call
                    .arguments
                    .get("confidence")
                    .and_then(|v| v.as_str())
                    .map(|v| v.to_string()),
                reasoning_summary: tool_call
                    .arguments
                    .get("reasoning_summary")
                    .and_then(|v| v.as_str())
                    .map(|v| v.to_string()),
                evidence_list: evidence_list.clone(),
                rlm_stats: RlmStats {
                    root_calls: stats.root_calls,
                    subcalls: stats.subcalls,
                    elapsed_ms: start.elapsed().as_millis() as i64,
                    cost_usd: stats.cost_usd,
                    cached_tokens: stats.cached_tokens,
                    cache_savings_usd: stats.cache_savings_usd,
                },
                execution_trace: trace.clone(),
                error: None,
                partial_answer: None,
                iterations: iterations_used,
                rlm_impl: Some("v1".to_string()),
            }));
        }
    }

    Ok(None)
}

fn build_error_result(
    kind: RlmErrorKind,
    message: String,
    messages: &[Value],
    evidence_list: Vec<EvidenceItem>,
    stats: &RlmStats,
    trace: &RlmExecutionTrace,
    iterations_used: i64,
    start: &Instant,
    limit_status: Option<&LimitStatus>,
) -> RlmRunResult {
    let partial_answer = latest_message_content(messages);
    let details = if kind == RlmErrorKind::LimitReached {
        limit_status.map(limit_details_json)
    } else {
        None
    };
    RlmRunResult {
        answer: None,
        confidence: None,
        reasoning_summary: None,
        evidence_list,
        rlm_stats: RlmStats {
            root_calls: stats.root_calls,
            subcalls: stats.subcalls,
            elapsed_ms: start.elapsed().as_millis() as i64,
            cost_usd: stats.cost_usd,
            cached_tokens: stats.cached_tokens,
            cache_savings_usd: stats.cache_savings_usd,
        },
        execution_trace: trace.clone(),
        error: Some(RlmErrorInfo {
            kind,
            message,
            details,
        }),
        partial_answer,
        iterations: iterations_used,
        rlm_impl: Some("v1".to_string()),
    }
}

fn limit_details_json(status: &LimitStatus) -> Value {
    json!({
        "hit_iterations": status.hit_iterations,
        "hit_root_calls": status.hit_root_calls,
        "hit_time": status.hit_time,
        "hit_cost": status.hit_cost,
        "reasons": status.reasons(),
    })
}

fn truncate_tool_result(tool_name: &str, result: Value) -> Value {
    let serialized = result.to_string();
    if serialized.len() <= MAX_TOOL_OUTPUT_CHARS {
        return result;
    }
    if let Value::Object(mut map) = result {
        if let Some(stdout) = map.get_mut("stdout") {
            if let Some(text) = stdout.as_str() {
                *stdout = Value::String(truncate_text(text, MAX_TOOL_OUTPUT_CHARS / 2));
            }
        }
        if let Some(stderr) = map.get_mut("stderr") {
            if let Some(text) = stderr.as_str() {
                *stderr = Value::String(truncate_text(text, MAX_TOOL_OUTPUT_CHARS / 4));
            }
        }
        map.insert("_truncated".to_string(), Value::Bool(true));
        map.insert(
            "_hint".to_string(),
            Value::String(format!(
                "Output truncated for tool {tool_name}; rerun with narrower query."
            )),
        );
        return Value::Object(map);
    }
    json!({
        "_truncated": true,
        "_hint": format!("Output truncated for tool {tool_name}"),
        "summary": truncate_text(&serialized, MAX_TOOL_OUTPUT_CHARS),
    })
}

async fn execute_delegate_lm(
    lm: &dyn LmClient,
    args: &Value,
    default_model: &str,
    stats: &mut RlmStats,
) -> Result<(Value, i64), RlmRunError> {
    let prompt = args
        .get("prompt")
        .and_then(|v| v.as_str())
        .ok_or_else(|| RlmRunError::Execution("delegate_lm missing prompt".to_string()))?;
    // Validate model against allowed list - LLMs sometimes hallucinate invalid model names
    const ALLOWED_MODELS: &[&str] = &["gpt-4.1-nano", "gpt-4.1-mini", "gpt-4.1"];

    let requested_model = args.get("model").and_then(|v| v.as_str());
    let model = if let Some(m) = requested_model {
        if ALLOWED_MODELS.contains(&m) {
            m.to_string()
        } else {
            tracing::warn!(
                "delegate_lm: invalid model '{}' requested, falling back to default '{}'",
                m,
                default_model
            );
            default_model.to_string()
        }
    } else {
        default_model.to_string()
    };
    // The "role" parameter describes the persona, NOT the OpenAI message role.
    // OpenAI only accepts: system, assistant, user, function, tool, developer.
    // We incorporate custom roles (like "reviewer") into the system prompt instead.
    let role = args.get("role").and_then(|v| v.as_str());
    let system_prompt = args.get("system_prompt").and_then(|v| v.as_str());
    // Build response_format: prefer explicit response_format, else construct from json_schema
    let response_format = args.get("response_format").cloned().or_else(|| {
        args.get("json_schema").map(|schema| {
            json!({
                "type": "json_schema",
                "json_schema": {
                    "name": "delegate_output",
                    "schema": schema,
                }
            })
        })
    });

    // Validate and normalize response_format for OpenAI compatibility
    // OpenAI requires: {"type": "json_schema"|"json_object"|"text", ...}
    // For json_schema, the inner schema must have "type": "object" at root
    let response_format = if let Some(ref rf) = response_format {
        if let Some(obj) = rf.as_object() {
            if obj.contains_key("type") {
                // Already has type field at top level - check if it's a valid response_format
                let rf_type = obj.get("type").and_then(|v| v.as_str()).unwrap_or("");
                if rf_type == "json_schema" || rf_type == "json_object" || rf_type == "text" {
                    // Valid OpenAI response_format - use as-is
                    Some(rf.clone())
                } else {
                    // Has "type" but it's "object"/"string"/etc - this is a JSON Schema, not response_format
                    // Wrap it in proper response_format structure
                    tracing::debug!(
                        "delegate_lm: detected raw JSON Schema with type={}, auto-wrapping",
                        rf_type
                    );
                    Some(json!({
                        "type": "json_schema",
                        "json_schema": {
                            "name": "delegate_output",
                            "schema": rf,
                            "strict": true,
                        }
                    }))
                }
            } else {
                // Missing 'type' field entirely - this looks like an example output, not a schema
                // OpenAI structured outputs require a proper JSON Schema with "type": "object"
                // We can't safely convert an example to a schema, so skip response_format
                tracing::warn!(
                    "delegate_lm: response_format has no 'type' field (looks like example output, not JSON Schema). \
                     For structured output, pass a valid JSON Schema with 'type': 'object'. Ignoring response_format."
                );
                None
            }
        } else {
            // response_format is not an object - invalid
            tracing::warn!(
                "delegate_lm: response_format must be an object, ignoring. Got: {:?}",
                rf
            );
            None
        }
    } else {
        None
    };

    let mut messages = Vec::new();
    // Build system message: combine explicit system_prompt with role description
    let effective_system = match (system_prompt, role) {
        (Some(sp), Some(r)) => Some(format!("You are a {}.\n\n{}", r, sp)),
        (Some(sp), None) => Some(sp.to_string()),
        (None, Some(r)) => Some(format!("You are a {}.", r)),
        (None, None) => None,
    };
    if let Some(sys) = effective_system {
        messages.push(json!({
            "role": "system",
            "content": sys,
        }));
    }
    // User message with the actual prompt
    messages.push(json!({
        "role": "user",
        "content": prompt,
    }));

    let lm_req = LmRequest {
        model: model.clone(),
        messages,
        temperature: 0.0,
        max_tokens: None,
        response_format: response_format.clone(),
        tools: None,
        tool_choice: None,
    };
    let response = lm
        .call(lm_req)
        .await
        .map_err(|err| RlmRunError::Lm(err.message))?;
    *stats = record_usage(stats.clone(), &response.usage, response.elapsed_ms);

    if response_format.is_some() {
        let parsed = serde_json::from_str::<Value>(&response.text).map_err(|err| {
            RlmRunError::Execution(format!("delegate_lm invalid json response: {err}"))
        })?;
        return Ok((
            json!({
                "success": true,
                "model": model,
                "output": parsed,
                "_local": true
            }),
            response.elapsed_ms,
        ));
    }

    Ok((
        json!({
            "success": true,
            "model": model,
            "content": response.text,
            "_local": true
        }),
        response.elapsed_ms,
    ))
}
