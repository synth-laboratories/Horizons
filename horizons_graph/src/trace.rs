//! V4 Execution Trace with comprehensive timing information.
//!
//! This module defines a standardized trace format that captures:
//! - Per-node execution timing
//! - Tool call timing
//! - LLM call timing
//! - Overall execution breakdown

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::time::Instant;

pub const TRACE_SCHEMA_VERSION: &str = "v4";

/// Root trace object returned with every graph execution
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ExecutionTrace {
    /// Schema version for backwards compatibility
    pub schema_version: String,
    /// Total execution time in milliseconds
    pub total_elapsed_ms: i64,
    /// Time spent in node execution (sum of all nodes)
    pub node_time_ms: i64,
    /// Overhead time (total - node_time)
    pub overhead_ms: i64,
    /// Breakdown of overhead sources
    pub overhead_breakdown: OverheadBreakdown,
    /// Per-node timing details
    pub nodes: Vec<NodeTrace>,
    /// All tool calls with timing
    pub tool_calls: Vec<ToolCallTrace>,
    /// All LLM calls with timing
    pub llm_calls: Vec<LlmCallTrace>,
    /// Execution timeline (ordered events)
    pub timeline: Vec<TimelineEvent>,
    /// Rendered LLM prompts
    pub llm_prompts: Vec<LlmPromptTrace>,
}

/// Breakdown of where overhead time goes
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct OverheadBreakdown {
    /// Graph parsing/validation time
    pub graph_setup_ms: i64,
    /// HTTP/serialization overhead
    pub http_overhead_ms: i64,
    /// Redis/storage operations
    pub storage_ms: i64,
    /// Input/output mapping evaluation
    pub mapping_ms: i64,
    /// RLM prompt rendering
    pub rlm_prompt_render_ms: i64,
    /// RLM message preparation/serialization
    pub rlm_message_serialize_ms: i64,
    /// RLM response parsing (tool extraction, etc.)
    pub rlm_response_parse_ms: i64,
    /// RLM tool dispatch overhead (excluding tool execution time)
    pub rlm_tool_dispatch_ms: i64,
    /// RLM message updates (appending assistant/tool messages)
    pub rlm_message_update_ms: i64,
    /// RLM tool result serialization to string
    pub rlm_tool_result_serialize_ms: i64,
    /// RLM tool result truncation/processing
    pub rlm_tool_result_process_ms: i64,
    /// RLM message summarization overhead (excluding LLM call)
    pub rlm_summarize_overhead_ms: i64,
    /// Other unaccounted overhead
    pub other_ms: i64,
}

/// Timing for a single node execution
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeTrace {
    /// Node name
    pub name: String,
    /// Node type (llm_call, tool_call, map_node, etc.)
    pub node_type: String,
    /// When this node started (ms from graph start)
    pub started_at_ms: i64,
    /// Total execution time for this node
    pub elapsed_ms: i64,
    /// Number of times this node executed
    pub executions: i64,
    /// Input keys received
    pub input_keys: Vec<String>,
    /// Output keys produced
    pub output_keys: Vec<String>,
    /// Sub-timings within this node
    pub breakdown: Option<NodeBreakdown>,
}

/// Detailed breakdown within a node
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct NodeBreakdown {
    /// Time spent in input mapping
    pub input_mapping_ms: i64,
    /// Time spent in core execution
    pub execution_ms: i64,
    /// Time spent in output mapping
    pub output_mapping_ms: i64,
    /// Time spent in hooks
    pub hooks_ms: i64,
}

/// Timing for a single tool call
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolCallTrace {
    /// Tool name
    pub tool: String,
    /// Which node invoked this tool
    pub node: String,
    /// Iteration within the node (for RLM loops)
    pub iteration: i64,
    /// When this call started (ms from graph start)
    pub started_at_ms: i64,
    /// Execution time
    pub elapsed_ms: i64,
    /// Whether executed locally or in sandbox
    pub execution_mode: String, // "local" or "sandbox"
    /// Tool arguments (truncated if large)
    pub args: Value,
    /// Tool result success
    pub success: bool,
    /// Sandbox timing breakdown (if sandbox execution)
    pub sandbox_timing: Option<SandboxTiming>,
}

/// Timing breakdown for sandbox execution
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SandboxTiming {
    /// Time waiting for sandbox provision
    pub provision_ms: i64,
    /// Time waiting for workspace lock
    pub lock_wait_ms: i64,
    /// Time setting up workspace
    pub workspace_setup_ms: i64,
    /// Time uploading files
    pub file_upload_ms: i64,
    /// Actual command execution time
    pub command_exec_ms: i64,
}

/// Timing for a single LLM call
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LlmCallTrace {
    /// Model name
    pub model: String,
    /// Which node invoked this LLM
    pub node: String,
    /// Iteration within the node
    pub iteration: i64,
    /// When this call started (ms from graph start)
    pub started_at_ms: i64,
    /// Total latency
    pub elapsed_ms: i64,
    /// Time to first token (if streaming)
    pub ttft_ms: Option<i64>,
    /// Prompt tokens
    pub prompt_tokens: i64,
    /// Completion tokens
    pub completion_tokens: i64,
    /// Whether tools were provided
    pub has_tools: bool,
    /// Number of tool calls in response
    pub tool_calls_count: i64,
}

/// Rendered prompt content for an LLM request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LlmPromptTrace {
    /// Timestamp (ms from graph start)
    pub ts_ms: i64,
    /// Node name
    pub node: String,
    /// Model name
    pub model: String,
    /// Iteration within the node (if applicable)
    pub iteration: i64,
    /// Messages (possibly truncated)
    pub messages: Vec<Value>,
}

/// Single event in the execution timeline
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimelineEvent {
    /// Timestamp (ms from graph start)
    pub ts_ms: i64,
    /// Event type
    pub event_type: String,
    /// Event details
    pub details: Value,
}

/// Builder for constructing traces during execution
pub struct TraceBuilder {
    start_time: Instant,
    trace: ExecutionTrace,
}

impl TraceBuilder {
    pub fn new() -> Self {
        TraceBuilder {
            start_time: Instant::now(),
            trace: ExecutionTrace {
                schema_version: TRACE_SCHEMA_VERSION.to_string(),
                ..Default::default()
            },
        }
    }

    /// Get current timestamp in ms from start
    pub fn now_ms(&self) -> i64 {
        self.start_time.elapsed().as_millis() as i64
    }

    /// Record graph setup time
    pub fn record_setup_time(&mut self, elapsed_ms: i64) {
        self.trace.overhead_breakdown.graph_setup_ms = elapsed_ms;
        self.add_timeline_event(
            "graph_setup_complete",
            serde_json::json!({
                "elapsed_ms": elapsed_ms
            }),
        );
    }

    pub fn record_rlm_prompt_render(&mut self, elapsed_ms: i64) {
        self.trace.overhead_breakdown.rlm_prompt_render_ms += elapsed_ms;
        self.add_timeline_event(
            "rlm_prompt_render",
            serde_json::json!({
                "elapsed_ms": elapsed_ms
            }),
        );
    }

    pub fn record_llm_prompt(
        &mut self,
        node: &str,
        model: &str,
        iteration: i64,
        messages: Vec<Value>,
    ) {
        self.trace.llm_prompts.push(LlmPromptTrace {
            ts_ms: self.now_ms(),
            node: node.to_string(),
            model: model.to_string(),
            iteration,
            messages,
        });
        self.add_timeline_event(
            "llm_prompt_content",
            serde_json::json!({
                "node": node,
                "model": model,
                "iteration": iteration,
            }),
        );
    }

    pub fn record_rlm_message_serialize(&mut self, elapsed_ms: i64) {
        self.trace.overhead_breakdown.rlm_message_serialize_ms += elapsed_ms;
        self.add_timeline_event(
            "rlm_message_serialize",
            serde_json::json!({
                "elapsed_ms": elapsed_ms
            }),
        );
    }

    pub fn record_rlm_response_parse(&mut self, elapsed_ms: i64) {
        self.trace.overhead_breakdown.rlm_response_parse_ms += elapsed_ms;
        self.add_timeline_event(
            "rlm_response_parse",
            serde_json::json!({
                "elapsed_ms": elapsed_ms
            }),
        );
    }

    pub fn record_rlm_tool_dispatch(&mut self, elapsed_ms: i64) {
        self.trace.overhead_breakdown.rlm_tool_dispatch_ms += elapsed_ms;
        self.add_timeline_event(
            "rlm_tool_dispatch",
            serde_json::json!({
                "elapsed_ms": elapsed_ms
            }),
        );
    }

    pub fn record_rlm_message_update(&mut self, elapsed_ms: i64) {
        self.trace.overhead_breakdown.rlm_message_update_ms += elapsed_ms;
        self.add_timeline_event(
            "rlm_message_update",
            serde_json::json!({
                "elapsed_ms": elapsed_ms
            }),
        );
    }

    pub fn record_rlm_tool_result_serialize(&mut self, elapsed_ms: i64) {
        self.trace.overhead_breakdown.rlm_tool_result_serialize_ms += elapsed_ms;
        self.add_timeline_event(
            "rlm_tool_result_serialize",
            serde_json::json!({
                "elapsed_ms": elapsed_ms
            }),
        );
    }

    pub fn record_rlm_tool_result_process(&mut self, elapsed_ms: i64) {
        self.trace.overhead_breakdown.rlm_tool_result_process_ms += elapsed_ms;
        self.add_timeline_event(
            "rlm_tool_result_process",
            serde_json::json!({
                "elapsed_ms": elapsed_ms
            }),
        );
    }

    pub fn record_rlm_summarize_overhead(&mut self, elapsed_ms: i64) {
        self.trace.overhead_breakdown.rlm_summarize_overhead_ms += elapsed_ms;
        self.add_timeline_event(
            "rlm_summarize_overhead",
            serde_json::json!({
                "elapsed_ms": elapsed_ms
            }),
        );
    }

    /// Start tracking a node execution
    pub fn start_node(&mut self, name: &str, node_type: &str, _input_keys: Vec<String>) -> i64 {
        let started_at = self.now_ms();
        self.add_timeline_event(
            "node_started",
            serde_json::json!({
                "node": name,
                "node_type": node_type,
            }),
        );
        started_at
    }

    /// Finish tracking a node execution
    pub fn finish_node(
        &mut self,
        name: &str,
        node_type: &str,
        started_at_ms: i64,
        input_keys: Vec<String>,
        output_keys: Vec<String>,
        breakdown: Option<NodeBreakdown>,
    ) {
        let elapsed_ms = self.now_ms() - started_at_ms;

        // Update existing or add new
        if let Some(existing) = self.trace.nodes.iter_mut().find(|n| n.name == name) {
            existing.elapsed_ms += elapsed_ms;
            existing.executions += 1;
        } else {
            self.trace.nodes.push(NodeTrace {
                name: name.to_string(),
                node_type: node_type.to_string(),
                started_at_ms,
                elapsed_ms,
                executions: 1,
                input_keys,
                output_keys,
                breakdown,
            });
        }

        self.trace.node_time_ms += elapsed_ms;
        self.add_timeline_event(
            "node_finished",
            serde_json::json!({
                "node": name,
                "elapsed_ms": elapsed_ms,
            }),
        );
    }

    /// Record a tool call
    pub fn record_tool_call(
        &mut self,
        tool: &str,
        node: &str,
        iteration: i64,
        started_at_ms: i64,
        elapsed_ms: i64,
        execution_mode: &str,
        args: Value,
        success: bool,
        sandbox_timing: Option<SandboxTiming>,
    ) {
        self.trace.tool_calls.push(ToolCallTrace {
            tool: tool.to_string(),
            node: node.to_string(),
            iteration,
            started_at_ms,
            elapsed_ms,
            execution_mode: execution_mode.to_string(),
            args,
            success,
            sandbox_timing,
        });
        self.add_timeline_event(
            "tool_call",
            serde_json::json!({
                "tool": tool,
                "node": node,
                "elapsed_ms": elapsed_ms,
                "mode": execution_mode,
            }),
        );
    }

    /// Record an LLM call
    pub fn record_llm_call(
        &mut self,
        model: &str,
        node: &str,
        iteration: i64,
        started_at_ms: i64,
        elapsed_ms: i64,
        prompt_tokens: i64,
        completion_tokens: i64,
        has_tools: bool,
        tool_calls_count: i64,
    ) {
        self.trace.llm_calls.push(LlmCallTrace {
            model: model.to_string(),
            node: node.to_string(),
            iteration,
            started_at_ms,
            elapsed_ms,
            ttft_ms: None,
            prompt_tokens,
            completion_tokens,
            has_tools,
            tool_calls_count,
        });
        self.add_timeline_event(
            "llm_call",
            serde_json::json!({
                "model": model,
                "node": node,
                "elapsed_ms": elapsed_ms,
                "tokens": prompt_tokens + completion_tokens,
            }),
        );
    }

    /// Add a timeline event
    pub fn add_timeline_event(&mut self, event_type: &str, details: Value) {
        self.trace.timeline.push(TimelineEvent {
            ts_ms: self.now_ms(),
            event_type: event_type.to_string(),
            details,
        });
    }

    /// Finalize and return the trace
    pub fn finalize(mut self) -> ExecutionTrace {
        self.trace.total_elapsed_ms = self.now_ms();
        self.trace.overhead_ms = self.trace.total_elapsed_ms - self.trace.node_time_ms;

        // Calculate other overhead
        let accounted = self.trace.overhead_breakdown.graph_setup_ms
            + self.trace.overhead_breakdown.http_overhead_ms
            + self.trace.overhead_breakdown.storage_ms
            + self.trace.overhead_breakdown.mapping_ms
            + self.trace.overhead_breakdown.rlm_prompt_render_ms
            + self.trace.overhead_breakdown.rlm_message_serialize_ms
            + self.trace.overhead_breakdown.rlm_response_parse_ms
            + self.trace.overhead_breakdown.rlm_tool_dispatch_ms
            + self.trace.overhead_breakdown.rlm_message_update_ms
            + self.trace.overhead_breakdown.rlm_tool_result_serialize_ms
            + self.trace.overhead_breakdown.rlm_tool_result_process_ms
            + self.trace.overhead_breakdown.rlm_summarize_overhead_ms;
        self.trace.overhead_breakdown.other_ms = self.trace.overhead_ms - accounted;

        self.add_timeline_event(
            "graph_complete",
            serde_json::json!({
                "total_ms": self.trace.total_elapsed_ms,
                "node_time_ms": self.trace.node_time_ms,
                "overhead_ms": self.trace.overhead_ms,
            }),
        );

        self.trace
    }

    /// Convert to JSON Value
    pub fn to_value(&self) -> Value {
        serde_json::to_value(&self.trace).unwrap_or(Value::Null)
    }
}

impl Default for TraceBuilder {
    fn default() -> Self {
        Self::new()
    }
}
