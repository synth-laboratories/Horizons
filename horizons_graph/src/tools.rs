use async_trait::async_trait;
use regex::RegexBuilder;
use serde_json::{Value, json};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::error::{GraphError, Result};
use crate::run::{ToolExecutorConfig, ToolExecutorTransport};

#[cfg(feature = "rlm_v1")]
use crate::rlm_v1::tools::python_sandbox::MontyREPL;

/// Materialized file stored in memory for local tool operations
#[derive(Clone, Debug)]
pub struct MaterializedFile {
    pub field_name: String,
    pub filename: String,
    pub content: String,
}

/// Shared state for local tool operations within a run
#[derive(Default)]
pub struct LocalToolState {
    /// Materialized files: filename -> content
    materialized: HashMap<String, MaterializedFile>,

    /// Persistent per-run state for `exec_python` (RLM v1 tool).
    ///
    /// Lazy-initialized on the first call.
    #[cfg(feature = "rlm_v1")]
    python_repl: Option<MontyREPL>,
}

impl LocalToolState {
    pub fn new() -> Self {
        Self {
            materialized: HashMap::new(),
            #[cfg(feature = "rlm_v1")]
            python_repl: None,
        }
    }

    pub fn materialize(&mut self, field_name: &str, filename: &str, content: String) {
        self.materialized.insert(
            filename.to_string(),
            MaterializedFile {
                field_name: field_name.to_string(),
                filename: filename.to_string(),
                content,
            },
        );
    }

    pub fn get_file(&self, filename: &str) -> Option<&MaterializedFile> {
        self.materialized.get(filename)
    }

    pub fn available_files(&self) -> Vec<String> {
        self.materialized.keys().cloned().collect()
    }
}

pub type SharedLocalToolState = Arc<RwLock<LocalToolState>>;

#[async_trait]
pub trait ToolExecutor: Send + Sync {
    async fn execute(
        &self,
        tool_name: &str,
        args: &Value,
        context: &Value,
        local_state: Option<&SharedLocalToolState>,
        graph_inputs: Option<&Value>,
    ) -> Result<Value>;
}

#[derive(Clone)]
pub struct DefaultToolExecutor {
    client: reqwest::Client,
    endpoint: Option<String>,
    api_key: Option<String>,
    headers: HashMap<String, String>,
    transport: ToolExecutorTransport,
}

impl DefaultToolExecutor {
    pub fn from_env() -> Self {
        let endpoint = std::env::var("GRAPH_TOOL_EXECUTOR_URL")
            .or_else(|_| std::env::var("TOOL_EXECUTOR_URL"))
            .ok()
            .filter(|value| !value.trim().is_empty())
            .map(|value| value.trim_end_matches('/').to_string());
        let api_key = std::env::var("GRAPH_TOOL_EXECUTOR_API_KEY")
            .or_else(|_| std::env::var("TOOL_EXECUTOR_API_KEY"))
            .ok();
        DefaultToolExecutor {
            client: reqwest::Client::new(),
            endpoint,
            api_key,
            headers: HashMap::new(),
            transport: ToolExecutorTransport::Execute,
        }
    }

    pub fn from_request_config(cfg: Option<&ToolExecutorConfig>) -> Self {
        let mut executor = Self::from_env();
        let Some(cfg) = cfg else {
            return executor;
        };

        if let Some(endpoint) = cfg.endpoint.as_deref() {
            let endpoint = endpoint.trim();
            if !endpoint.is_empty() {
                executor.endpoint = Some(endpoint.trim_end_matches('/').to_string());
            }
        }
        if let Some(api_key) = cfg.api_key.as_deref() {
            let api_key = api_key.trim();
            if !api_key.is_empty() {
                executor.api_key = Some(api_key.to_string());
            }
        }
        if let Some(headers) = cfg.headers.as_ref() {
            for (key, value) in headers {
                let key = key.trim();
                let value = value.trim();
                if key.is_empty() {
                    continue;
                }
                if !value.is_empty() {
                    executor.headers.insert(key.to_string(), value.to_string());
                }
            }
        }
        if let Some(transport) = cfg.transport {
            executor.transport = transport;
        }
        executor
    }

    /// Execute a tool - routes to Rust implementation for local tools, Python for others.
    ///
    /// # Tool Routing Summary
    ///
    /// ## Pure Rust (this executor, ~0.1ms):
    /// - `local_grep`: Regex search on materialized content
    /// - `local_search`: Substring search on materialized content
    /// - `local_regex`: Regex extraction with capture groups
    /// - `view_lines`: View line window from materialized file
    /// - `materialize_context`: Store input fields for local searching
    ///
    /// ## Handled in api.rs RLM loop (not via this executor):
    /// - `submit_answer`: Submit final answer (terminates RLM)
    /// - `give_up`: Early termination with structured error
    /// - `delegate_lm`: Sub-LLM calls with structured output (RUST-NATIVE)
    ///
    /// ## Python backend (via execute_remote):
    /// - `codex_exec`: Sandbox shell execution (Daytona/Docker)
    /// - Any custom tools defined in graph YAML
    async fn execute_impl(
        &self,
        tool_name: &str,
        args: &Value,
        context: &Value,
        local_state: Option<&SharedLocalToolState>,
        graph_inputs: Option<&Value>,
    ) -> Result<Value> {
        // Route local tools to Rust implementations (fast path, ~0.1ms)
        match tool_name {
            "local_grep" => self.local_grep(args, local_state).await,
            "local_search" => self.local_search(args, local_state).await,
            "local_regex" => self.local_regex(args, local_state).await,
            "view_lines" => self.view_lines(args, local_state, graph_inputs).await,
            "materialize_context" => {
                self.materialize_context(args, local_state, graph_inputs)
                    .await
            }
            #[cfg(feature = "rlm_v1")]
            "exec_python" => self.exec_python(args, local_state, graph_inputs).await,
            // `codex_exec` is meant for shell execution via a remote sandbox, but in practice
            // many agents use it for ad-hoc Python. If the tool executor is not configured,
            // fall back to the in-process Monty sandbox for python-like snippets.
            "codex_exec" => {
                self.codex_exec(args, local_state, graph_inputs, context)
                    .await
            }
            // =====================================================================
            // TOOLS THAT GO TO PYTHON BACKEND (via execute_remote):
            // =====================================================================
            // - codex_exec: Sandbox shell execution (Daytona/Docker)
            // - Any user-defined custom tools in graph YAML
            //
            // NOTE: codex_exec requires Python because it needs external
            // sandbox infrastructure (Daytona/Docker).
            //
            // ALSO NOTE: submit_answer, give_up, delegate_lm are handled
            // directly in api.rs RLM loop, NOT via this executor.
            // =====================================================================
            _ => self.execute_remote(tool_name, args, context).await,
        }
    }

    async fn codex_exec(
        &self,
        args: &Value,
        local_state: Option<&SharedLocalToolState>,
        graph_inputs: Option<&Value>,
        context: &Value,
    ) -> Result<Value> {
        let command = args
            .get("command")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let force_sandbox = args
            .get("force_sandbox")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let timeout_s = args
            .get("timeout_seconds")
            .and_then(|v| v.as_u64())
            .unwrap_or(30);

        // If the caller explicitly requests the remote sandbox, preserve the old behavior.
        if force_sandbox {
            return self.execute_remote("codex_exec", args, context).await;
        }

        // Heuristic: if it looks like a python snippet (not a shell command), run it via Monty.
        #[cfg(feature = "rlm_v1")]
        {
            if looks_like_python_snippet(command) {
                let py_args = json!({
                    "code": command,
                    "timeout_ms": (timeout_s.saturating_mul(1000)).min(300_000),
                });
                return self.exec_python(&py_args, local_state, graph_inputs).await;
            }
        }

        // Otherwise, use the remote tool executor.
        self.execute_remote("codex_exec", args, context).await
    }

    /// Execute tool via Python backend (for complex tools like codex_exec)
    async fn execute_remote(
        &self,
        tool_name: &str,
        args: &Value,
        context: &Value,
    ) -> Result<Value> {
        let endpoint = self
            .endpoint
            .as_ref()
            .ok_or_else(|| GraphError::unavailable("tool executor not configured"))?;
        let mut request = match self.transport {
            ToolExecutorTransport::Execute => {
                let url = format!("{endpoint}/execute");
                self.client.post(url).json(&serde_json::json!({
                    "tool": tool_name,
                    "args": args,
                    "context": context,
                }))
            }
            ToolExecutorTransport::Mcp => {
                let url = if endpoint.ends_with("/call") {
                    endpoint.to_string()
                } else {
                    format!("{endpoint}/call")
                };
                self.client.post(url).json(&serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": "graph-rlm-tool",
                    "method": "tools/call",
                    "params": {
                        "name": tool_name,
                        "arguments": args,
                    },
                }))
            }
        };
        request = self.apply_auth_headers(request);
        let response = request
            .send()
            .await
            .map_err(|err| GraphError::internal(format!("tool executor request failed: {err}")))?;
        if !response.status().is_success() {
            let status = response.status();
            let body = response
                .text()
                .await
                .unwrap_or_else(|_| "unknown".to_string());
            return Err(GraphError::internal(format!(
                "tool executor failed ({status}): {body}"
            )));
        }
        let parsed = response.json::<Value>().await;
        match self.transport {
            ToolExecutorTransport::Execute => match parsed {
                Ok(value) => Ok(value),
                Err(_) => Ok(Value::String(
                    "tool executor returned non-json response".to_string(),
                )),
            },
            ToolExecutorTransport::Mcp => match parsed {
                Ok(value) => {
                    if let Some(error) = value.get("error") {
                        return Err(GraphError::internal(format!(
                            "tool executor returned jsonrpc error: {error}"
                        )));
                    }
                    if let Some(result) = value.get("result") {
                        Ok(result.clone())
                    } else {
                        Err(GraphError::internal(
                            "tool executor did not return jsonrpc result".to_string(),
                        ))
                    }
                }
                Err(_) => Ok(Value::String(
                    "tool executor returned non-json response".to_string(),
                )),
            },
        }
    }

    fn apply_auth_headers(&self, mut request: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        let mut has_authorization = false;
        for key in self.headers.keys() {
            if key.eq_ignore_ascii_case("authorization") {
                has_authorization = true;
                break;
            }
        }

        if let Some(api_key) = &self.api_key {
            if !api_key.trim().is_empty() && !has_authorization {
                request = request.bearer_auth(api_key.trim());
            }
        }

        for (key, value) in &self.headers {
            request = request.header(key.as_str(), value.as_str());
        }

        request
    }

    // =========================================================================
    // LOCAL TOOLS (Pure Rust, ~0.1ms)
    // =========================================================================

    /// Materialize input field to local memory for searching
    async fn materialize_context(
        &self,
        args: &Value,
        local_state: Option<&SharedLocalToolState>,
        graph_inputs: Option<&Value>,
    ) -> Result<Value> {
        let state = local_state.ok_or_else(|| GraphError::internal("local state not available"))?;
        let inputs =
            graph_inputs.ok_or_else(|| GraphError::internal("graph inputs not available"))?;

        let field_name = args
            .get("field_name")
            .and_then(|v| v.as_str())
            .unwrap_or("context");
        let filename = args
            .get("filename")
            .and_then(|v| v.as_str())
            .unwrap_or("context.txt");

        // Get content from graph inputs
        let raw_content = inputs.get(field_name);
        let content = match raw_content {
            Some(Value::String(s)) => s.clone(),
            Some(Value::Object(_) | Value::Array(_)) => {
                serde_json::to_string_pretty(raw_content.unwrap())
                    .unwrap_or_else(|_| "{}".to_string())
            }
            Some(other) => other.to_string(),
            None => {
                return Ok(json!({
                    "success": false,
                    "error": format!("Field '{}' not found in inputs", field_name),
                }));
            }
        };

        let chars = content.len();

        // Store in local state
        {
            let mut state = state.write().await;
            state.materialize(field_name, filename, content);
        }

        Ok(json!({
            "success": true,
            "filename": filename,
            "chars": chars,
            "available_locally": true,
            "_local": true,
            "_rust": true,
        }))
    }

    /// Execute Python code in-process via Monty (RLM v1 tool).
    #[cfg(feature = "rlm_v1")]
    async fn exec_python(
        &self,
        args: &Value,
        local_state: Option<&SharedLocalToolState>,
        graph_inputs: Option<&Value>,
    ) -> Result<Value> {
        let state = local_state.ok_or_else(|| GraphError::internal("local state not available"))?;
        let code = args
            .get("code")
            .and_then(|v| v.as_str())
            .ok_or_else(|| GraphError::bad_request("exec_python: missing 'code' parameter"))?;
        let timeout_ms = args
            .get("timeout_ms")
            .and_then(|v| v.as_u64())
            .unwrap_or(5000);

        let mut state_guard = state.write().await;

        if state_guard.python_repl.is_none() {
            let mut repl = MontyREPL::new();
            inject_contexts_into_repl(&mut repl, &state_guard.materialized, graph_inputs);
            state_guard.python_repl = Some(repl);
        }

        // Temporarily take ownership of the REPL so we can read other state fields
        // (like `materialized`) without borrow conflicts.
        let repl_opt = state_guard.python_repl.take();
        let mut repl = repl_opt.expect("python_repl just initialized");

        // Re-inject contexts each call (materialized files can be added mid-run).
        inject_contexts_into_repl(&mut repl, &state_guard.materialized, graph_inputs);

        let exec_res = repl.execute(code, timeout_ms).await;
        state_guard.python_repl = Some(repl);
        let success = exec_res.error.is_none();

        Ok(json!({
            "success": success,
            "stdout": exec_res.stdout,
            "return_value": exec_res.return_value,
            "error": exec_res.error,
            "execution_time_ms": exec_res.execution_time_ms,
            "variables": exec_res.variables,
            "_local": true,
            "_rust": true,
            "_sandbox": "monty",
        }))
    }

    /// View a window of lines from a materialized file - pure Rust
    async fn view_lines(
        &self,
        args: &Value,
        local_state: Option<&SharedLocalToolState>,
        graph_inputs: Option<&Value>,
    ) -> Result<Value> {
        let state = local_state.ok_or_else(|| GraphError::internal("local state not available"))?;

        let filename = args
            .get("file")
            .or_else(|| args.get("filename"))
            .and_then(|v| v.as_str())
            .unwrap_or("context.txt");
        let start_line = args.get("start_line").and_then(|v| v.as_u64()).unwrap_or(1) as usize;
        let max_lines = args
            .get("max_lines")
            .and_then(|v| v.as_u64())
            .unwrap_or(50)
            .min(100) as usize;

        let start_line = start_line.max(1);

        // Try to get from local state first
        let state_guard = state.read().await;
        let mat_file = state_guard.get_file(filename);

        let content: String;
        let source: &str;

        if let Some(mf) = mat_file {
            content = mf.content.clone();
            source = "materialized";
        } else {
            // Auto-materialize from inputs if file not found
            drop(state_guard); // Release read lock before write

            let inputs = if let Some(i) = graph_inputs {
                i
            } else {
                let state_guard = state.read().await;
                return Ok(json!({
                    "success": false,
                    "error": format!("File '{}' not materialized. Call materialize_context first.", filename),
                    "available_files": state_guard.available_files(),
                    "_rust": true,
                }));
            };

            // Guess field name from filename
            let field_name = match filename {
                "trace.json" => "trace_content",
                "rubric.json" => "rubric_content",
                "context.txt" => "context",
                _ => filename.trim_end_matches(".json").trim_end_matches(".txt"),
            };

            let raw_content = inputs.get(field_name);
            let materialized_content = match raw_content {
                Some(Value::String(s)) => s.clone(),
                Some(Value::Object(_) | Value::Array(_)) => {
                    serde_json::to_string_pretty(raw_content.unwrap())
                        .unwrap_or_else(|_| "{}".to_string())
                }
                Some(other) => other.to_string(),
                None => {
                    let state_guard = state.read().await;
                    return Ok(json!({
                        "success": false,
                        "error": format!("File '{}' not materialized and field '{}' not found in inputs.", filename, field_name),
                        "available_files": state_guard.available_files(),
                        "_rust": true,
                    }));
                }
            };

            // Store it for future use
            {
                let mut state_write = state.write().await;
                state_write.materialize(field_name, filename, materialized_content.clone());
            }

            content = materialized_content;
            source = "auto_materialized";
        }

        let lines: Vec<&str> = content.lines().collect();
        let total_lines = lines.len();
        let start_idx = (start_line - 1).min(total_lines.saturating_sub(1));
        let end_idx = (start_idx + max_lines).min(total_lines);

        let window: Vec<Value> = (start_idx..end_idx)
            .map(|idx| {
                let line = lines[idx];
                let truncated = if line.len() > 500 {
                    // Find a valid UTF-8 char boundary at or before byte 500
                    let mut end_byte = 500;
                    while end_byte > 0 && !line.is_char_boundary(end_byte) {
                        end_byte -= 1;
                    }
                    format!(
                        "{}... [{} more chars]",
                        &line[..end_byte],
                        line.len() - end_byte
                    )
                } else {
                    line.to_string()
                };
                json!({
                    "line_num": idx + 1,
                    "content": truncated,
                })
            })
            .collect();

        Ok(json!({
            "success": true,
            "file": filename,
            "start_line": start_idx + 1,
            "end_line": end_idx,
            "total_lines": total_lines,
            "has_more": end_idx < total_lines,
            "lines": window,
            "source": source,
            "_local": true,
            "_rust": true,
        }))
    }

    /// Fast local grep with regex - pure Rust
    async fn local_grep(
        &self,
        args: &Value,
        local_state: Option<&SharedLocalToolState>,
    ) -> Result<Value> {
        let state = local_state.ok_or_else(|| GraphError::internal("local state not available"))?;

        let pattern = args.get("pattern").and_then(|v| v.as_str()).unwrap_or("");
        let filename = args
            .get("file")
            .and_then(|v| v.as_str())
            .unwrap_or("context.txt");
        let case_insensitive = args
            .get("ignore_case")
            .and_then(|v| v.as_bool())
            .unwrap_or(true);
        let max_matches = args
            .get("max_matches")
            .and_then(|v| v.as_u64())
            .unwrap_or(15) as usize;
        let max_line_chars = args
            .get("max_line_chars")
            .and_then(|v| v.as_u64())
            .unwrap_or(200) as usize;
        let context_lines = args
            .get("context_lines")
            .and_then(|v| v.as_u64())
            .unwrap_or(0) as usize;
        let offset = args.get("offset").and_then(|v| v.as_u64()).unwrap_or(0) as usize;

        let state = state.read().await;
        let mat_file = match state.get_file(filename) {
            Some(f) => f,
            None => {
                return Ok(json!({
                    "success": false,
                    "error": format!("File '{}' not materialized. Call materialize_context first.", filename),
                    "available_files": state.available_files(),
                    "_rust": true,
                }));
            }
        };

        // Compile regex
        let regex = match RegexBuilder::new(pattern)
            .case_insensitive(case_insensitive)
            .build()
        {
            Ok(r) => r,
            Err(e) => {
                return Ok(json!({
                    "success": false,
                    "error": format!("Invalid regex: {}", e),
                    "_rust": true,
                }));
            }
        };

        let lines: Vec<&str> = mat_file.content.lines().collect();
        let mut matches = Vec::new();
        let mut total_output_chars = 0usize;
        let max_total_chars = 4000usize;
        let mut skipped = 0usize;
        let mut total_matches_found = 0usize;

        for (i, line) in lines.iter().enumerate() {
            if regex.is_match(line) {
                total_matches_found += 1;
                // Skip matches until we reach offset
                if skipped < offset {
                    skipped += 1;
                    continue;
                }

                let truncated = truncate_str(line, max_line_chars);
                let mut match_entry = json!({
                    "line_num": i + 1,
                    "content": truncated,
                });

                // Add context lines if requested
                if context_lines > 0 {
                    let start_ctx = i.saturating_sub(context_lines);
                    let end_ctx = (i + context_lines + 1).min(lines.len());
                    let ctx: Vec<String> = lines[start_ctx..end_ctx]
                        .iter()
                        .map(|l| truncate_str(l, max_line_chars))
                        .collect();
                    match_entry["context"] = json!(ctx);
                }

                total_output_chars += truncated.len();
                matches.push(match_entry);

                if matches.len() >= max_matches || total_output_chars >= max_total_chars {
                    break;
                }
            }
        }

        let has_more = total_matches_found > offset + matches.len();
        Ok(json!({
            "success": true,
            "matches": matches,
            "match_count": matches.len(),
            "total_matches": total_matches_found,
            "offset": offset,
            "has_more": has_more,
            "next_offset": if has_more { offset + matches.len() } else { 0 },
            "_local": true,
            "_rust": true,
        }))
    }

    /// Fast local substring search - pure Rust
    async fn local_search(
        &self,
        args: &Value,
        local_state: Option<&SharedLocalToolState>,
    ) -> Result<Value> {
        let state = local_state.ok_or_else(|| GraphError::internal("local state not available"))?;

        let query = args.get("query").and_then(|v| v.as_str()).unwrap_or("");
        let filename = args
            .get("file")
            .and_then(|v| v.as_str())
            .unwrap_or("context.txt");
        let case_insensitive = args
            .get("ignore_case")
            .and_then(|v| v.as_bool())
            .unwrap_or(true);
        let max_matches = args
            .get("max_matches")
            .and_then(|v| v.as_u64())
            .unwrap_or(15) as usize;
        let max_line_chars = args
            .get("max_line_chars")
            .and_then(|v| v.as_u64())
            .unwrap_or(200) as usize;
        let offset = args.get("offset").and_then(|v| v.as_u64()).unwrap_or(0) as usize;

        let state = state.read().await;
        let mat_file = match state.get_file(filename) {
            Some(f) => f,
            None => {
                return Ok(json!({
                    "success": false,
                    "error": format!("File '{}' not materialized.", filename),
                    "available_files": state.available_files(),
                    "_rust": true,
                }));
            }
        };

        let search_query = if case_insensitive {
            query.to_lowercase()
        } else {
            query.to_string()
        };

        let lines: Vec<&str> = mat_file.content.lines().collect();
        let mut matches = Vec::new();
        let mut total_output_chars = 0usize;
        let max_total_chars = 4000usize;
        let mut skipped = 0usize;
        let mut total_matches_found = 0usize;

        for (i, line) in lines.iter().enumerate() {
            let search_line = if case_insensitive {
                line.to_lowercase()
            } else {
                line.to_string()
            };

            if search_line.contains(&search_query) {
                total_matches_found += 1;
                if skipped < offset {
                    skipped += 1;
                    continue;
                }

                let truncated = truncate_str(line, max_line_chars);
                total_output_chars += truncated.len();
                matches.push(json!({
                    "line_num": i + 1,
                    "content": truncated,
                }));

                if matches.len() >= max_matches || total_output_chars >= max_total_chars {
                    break;
                }
            }
        }

        let has_more = total_matches_found > offset + matches.len();
        Ok(json!({
            "success": true,
            "matches": matches,
            "match_count": matches.len(),
            "total_matches": total_matches_found,
            "offset": offset,
            "has_more": has_more,
            "next_offset": if has_more { offset + matches.len() } else { 0 },
            "_local": true,
            "_rust": true,
        }))
    }

    /// Extract regex matches with capture groups - pure Rust
    async fn local_regex(
        &self,
        args: &Value,
        local_state: Option<&SharedLocalToolState>,
    ) -> Result<Value> {
        let state = local_state.ok_or_else(|| GraphError::internal("local state not available"))?;

        let pattern = args.get("pattern").and_then(|v| v.as_str()).unwrap_or("");
        let filename = args
            .get("file")
            .and_then(|v| v.as_str())
            .unwrap_or("context.txt");
        let flags_str = args.get("flags").and_then(|v| v.as_str()).unwrap_or("i");
        let max_match_chars = args
            .get("max_match_chars")
            .and_then(|v| v.as_u64())
            .unwrap_or(500) as usize;

        let state = state.read().await;
        let mat_file = match state.get_file(filename) {
            Some(f) => f,
            None => {
                return Ok(json!({
                    "success": false,
                    "error": format!("File '{}' not materialized.", filename),
                    "_rust": true,
                }));
            }
        };

        // Parse flags
        let case_insensitive = flags_str.contains('i');
        let multi_line = flags_str.contains('m');
        let dot_all = flags_str.contains('s');

        let regex = match RegexBuilder::new(pattern)
            .case_insensitive(case_insensitive)
            .multi_line(multi_line)
            .dot_matches_new_line(dot_all)
            .build()
        {
            Ok(r) => r,
            Err(e) => {
                return Ok(json!({
                    "success": false,
                    "error": format!("Invalid regex: {}", e),
                    "_rust": true,
                }));
            }
        };

        let mut matches = Vec::new();
        let mut total_output_chars = 0usize;
        let max_total_chars = 30000usize;

        for cap in regex.captures_iter(&mat_file.content) {
            let full_match = cap.get(0).map_or("", |m| m.as_str());
            let truncated = truncate_str(full_match, max_match_chars);

            let mut match_info = json!({
                "match": truncated,
                "start": cap.get(0).map_or(0, |m| m.start()),
                "end": cap.get(0).map_or(0, |m| m.end()),
            });

            // Add capture groups if any
            if cap.len() > 1 {
                let groups: Vec<Option<String>> = (1..cap.len())
                    .map(|i| {
                        cap.get(i)
                            .map(|m| truncate_str(m.as_str(), max_match_chars))
                    })
                    .collect();
                match_info["groups"] = json!(groups);
            }

            total_output_chars += truncated.len();
            matches.push(match_info);

            if matches.len() >= 50 || total_output_chars >= max_total_chars {
                break;
            }
        }

        Ok(json!({
            "success": true,
            "matches": matches,
            "total_matches": matches.len(),
            "truncated": matches.len() >= 50 || total_output_chars >= max_total_chars,
            "_local": true,
            "_rust": true,
        }))
    }
}

fn looks_like_python_snippet(command: &str) -> bool {
    let s = command.trim();
    if s.is_empty() {
        return false;
    }
    // If it looks like an explicit shell invocation, assume it's shell.
    let lower = s.to_ascii_lowercase();
    for prefix in [
        "bash ", "sh ", "zsh ", "python ", "python3 ", "rg ", "grep ", "cat ", "head ", "tail ",
        "wc ", "ls ", "find ", "sed ", "awk ",
    ] {
        if lower.starts_with(prefix) {
            return false;
        }
    }
    // Multi-line almost always indicates code.
    if s.contains('\n') {
        return true;
    }
    // Common python statement/function prefixes.
    for kw in [
        "import ", "from ", "def ", "class ", "for ", "while ", "with ", "if ", "print(",
    ] {
        if lower.starts_with(kw) {
            return true;
        }
    }
    // Basic assignment like `x = ...`.
    if s.contains('=')
        && !s.contains("==")
        && !s.contains("!=")
        && !s.contains(">=")
        && !s.contains("<=")
    {
        return true;
    }
    false
}

/// Truncate string with indicator (UTF-8 safe)
fn truncate_str(s: &str, max_len: usize) -> String {
    if s.len() <= max_len {
        s.to_string()
    } else {
        // Find a valid char boundary at or before max_len
        let mut end = max_len.min(s.len());
        while end > 0 && !s.is_char_boundary(end) {
            end -= 1;
        }
        let remaining = s.len() - end;
        format!("{}... [{} more bytes]", &s[..end], remaining)
    }
}

#[async_trait]
impl ToolExecutor for DefaultToolExecutor {
    async fn execute(
        &self,
        tool_name: &str,
        args: &Value,
        context: &Value,
        local_state: Option<&SharedLocalToolState>,
        graph_inputs: Option<&Value>,
    ) -> Result<Value> {
        self.execute_impl(tool_name, args, context, local_state, graph_inputs)
            .await
    }
}

#[cfg(feature = "rlm_v1")]
fn inject_contexts_into_repl(
    repl: &mut MontyREPL,
    materialized: &HashMap<String, MaterializedFile>,
    graph_inputs: Option<&Value>,
) {
    // All materialized files are available via `files` in the sandbox.
    for (filename, mat_file) in materialized {
        repl.inject_context(filename, mat_file.content.clone());
    }

    // Choose default `context` with a stable preference order.
    let mut preferred: Option<String> = None;
    for mf in materialized.values() {
        if mf.field_name == "context" {
            preferred = Some(mf.content.clone());
            break;
        }
    }
    if preferred.is_none() {
        if let Some(mf) = materialized.get("context.txt") {
            preferred = Some(mf.content.clone());
        }
    }
    if preferred.is_none() && !materialized.is_empty() {
        let mut filenames = materialized.keys().cloned().collect::<Vec<_>>();
        filenames.sort();
        if let Some(first) = filenames.first() {
            if let Some(mf) = materialized.get(first) {
                preferred = Some(mf.content.clone());
            }
        }
    }
    if preferred.is_none() {
        if let Some(inputs) = graph_inputs {
            if let Some(v) = inputs.get("context") {
                preferred = Some(match v {
                    Value::String(s) => s.clone(),
                    Value::Object(_) | Value::Array(_) => {
                        serde_json::to_string_pretty(v).unwrap_or_else(|_| v.to_string())
                    }
                    other => other.to_string(),
                });
            }
        }
    }

    if let Some(content) = preferred {
        repl.inject_context("context", content);
    }
}
