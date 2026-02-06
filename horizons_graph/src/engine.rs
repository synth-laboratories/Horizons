use async_recursion::async_recursion;
use async_trait::async_trait;
use futures_util::StreamExt;
use futures_util::future::BoxFuture;
use futures_util::stream::FuturesUnordered;
use serde_json::{Map, Value, json};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{Mutex, Semaphore};

use crate::definition::{GraphDefinition, MapConfig, NodeDefinition, ReduceConfig};
use crate::error::{GraphError, Result};
use crate::ir::{
    GraphIr, Strictness, canonicalize_graph_ir, normalize_graph_ir, validate_graph_ir,
};
use crate::llm::{ApiMode, LlmChunkCallback, LlmClientApi, LlmRequest, UsageSummary};
use crate::mapping::eval_mapping_expression;
use crate::python;
use crate::run::RunConfig;
use crate::template::render_messages;
use crate::tools::{LocalToolState, SharedLocalToolState, ToolExecutor};
use crate::trace::{SandboxTiming, TraceBuilder};

pub struct GraphEngine {
    llm: Arc<dyn LlmClientApi>,
    tools: Arc<dyn ToolExecutor>,
}

impl GraphEngine {
    pub fn new(llm: Arc<dyn LlmClientApi>, tools: Arc<dyn ToolExecutor>) -> Self {
        Self { llm, tools }
    }

    pub async fn execute_graph_ir(
        &self,
        graph_ir: &GraphIr,
        inputs: Value,
        run_config: Option<RunConfig>,
    ) -> Result<GraphRun> {
        let normalized = normalize_graph_ir(graph_ir);
        let validation = validate_graph_ir(&normalized, Strictness::Strict);
        if !validation.ok() {
            return Err(GraphError::invalid_graph(validation.diagnostics));
        }
        let graph_def = GraphDefinition::from_ir(&normalized)?;
        self.execute(&graph_def, inputs, run_config).await
    }

    pub async fn execute(
        &self,
        graph: &GraphDefinition,
        inputs: Value,
        run_config: Option<RunConfig>,
    ) -> Result<GraphRun> {
        let trace_handle = Arc::new(Mutex::new(TraceBuilder::new()));

        let budgets = run_config.as_ref().and_then(|cfg| cfg.budgets.as_ref());
        let max_supersteps = budgets.and_then(|b| b.max_supersteps);
        let max_llm_calls = budgets.and_then(|b| b.max_llm_calls);
        let max_time_ms = budgets.and_then(|b| b.max_time_ms);
        let start_time = Instant::now();

        let mut usage = UsageAccumulator::default();
        let mut exec_state = initial_state(Some(inputs.clone()));

        let total_incoming = build_in_degree(graph);
        let mut incoming_seen: HashMap<String, i64> =
            total_incoming.keys().map(|key| (key.clone(), 0)).collect();
        let mut ready = initial_ready_queue(graph, &total_incoming);
        let mut executed = HashSet::new();
        let mut activated: HashSet<String> = ready.iter().cloned().collect();
        let mut steps = 0i64;

        while let Some(node_id) = ready.pop_front() {
            if executed.contains(&node_id) {
                continue;
            }
            if max_supersteps.is_some_and(|limit| steps >= limit) {
                return Err(GraphError::bad_request("max_supersteps exceeded"));
            }
            if max_time_ms.is_some_and(|limit| start_time.elapsed().as_millis() as i64 >= limit) {
                return Err(GraphError::bad_request("run exceeded max_time_ms"));
            }

            let node = graph.nodes.get(&node_id).ok_or_else(|| {
                GraphError::internal(format!("node '{node_id}' not found in graph definition"))
            })?;

            let node_inputs = resolve_node_inputs(node, &exec_state)?;

            let input_keys = node_inputs
                .as_object()
                .map(|map| map.keys().cloned().collect::<Vec<String>>())
                .unwrap_or_default();
            let trace_node_type = node
                .implementation
                .as_ref()
                .and_then(|v| v.get("type"))
                .and_then(|v| v.as_str())
                .map_or_else(
                    || node.node_type.clone(),
                    |impl_type| format!("{}:{impl_type}", node.node_type),
                );
            let node_started_at_ms = {
                let mut trace = trace_handle.lock().await;
                trace.start_node(&node.name, &trace_node_type, input_keys.clone())
            };

            let node_output = self
                .execute_node(
                    node,
                    &node_id,
                    &node_inputs,
                    &exec_state,
                    run_config.as_ref(),
                    max_llm_calls,
                    &mut usage,
                    trace_handle.clone(),
                    &inputs,
                )
                .await?;

            if let Some(map) = exec_state.as_object_mut() {
                apply_node_output(map, &node.name, node_output.clone());
                apply_output_mapping(
                    self.tools.as_ref(),
                    map,
                    &node_output,
                    node.output_mapping.as_ref(),
                    &node.name,
                    &inputs,
                )
                .await?;
            }

            let output_keys = node_output
                .as_object()
                .map(|map| map.keys().cloned().collect::<Vec<String>>())
                .unwrap_or_default();
            {
                let mut trace = trace_handle.lock().await;
                trace.finish_node(
                    &node.name,
                    &node.node_type,
                    node_started_at_ms,
                    input_keys,
                    output_keys,
                    None,
                );
            }

            executed.insert(node_id.clone());
            steps += 1;

            if let Some(edges) = graph.control_edges.get(&node_id) {
                for edge in edges {
                    let passed =
                        evaluate_edge_condition(edge.condition.as_ref(), &exec_state, &node_output)
                            .await?;
                    if passed {
                        activated.insert(edge.target.clone());
                    }
                    if let Some(count) = incoming_seen.get_mut(&edge.target) {
                        *count += 1;
                        if *count >= *total_incoming.get(&edge.target).unwrap_or(&0)
                            && activated.contains(&edge.target)
                        {
                            ready.push_back(edge.target.clone());
                        }
                    }
                }
            }
        }

        if executed.is_empty() && !graph.nodes.is_empty() {
            return Err(GraphError::bad_request(
                "graph execution did not execute any nodes",
            ));
        }

        let (final_output, validation_result, validation_failed) =
            apply_output_config(&exec_state, run_config.as_ref())?;
        if validation_failed {
            return Err(GraphError::bad_request("output validation failed"));
        }

        let execution_trace = {
            let mut guard = trace_handle.lock().await;
            let builder = std::mem::take(&mut *guard);
            serde_json::to_value(builder.finalize()).unwrap_or(Value::Null)
        };

        Ok(GraphRun {
            output: final_output,
            usage_summary: usage.to_value(),
            validation_result,
            execution_trace,
            graph_hash: hash_graph_ir_safe(graph_ir_fingerprint(graph)),
        })
    }

    #[async_recursion]
    async fn execute_node(
        &self,
        node: &NodeDefinition,
        node_id: &str,
        inputs: &Value,
        full_state: &Value,
        run_config: Option<&RunConfig>,
        max_llm_calls: Option<i64>,
        usage: &mut UsageAccumulator,
        trace_handle: Arc<Mutex<TraceBuilder>>,
        graph_inputs: &Value,
    ) -> Result<Value> {
        let node_kind = node.node_type.to_lowercase().replace('_', "");
        if node_kind == "mapnode" {
            let config = node
                .map_config
                .as_ref()
                .ok_or_else(|| GraphError::bad_request("MapNode missing configuration"))?;
            return self
                .execute_map_node(
                    node,
                    inputs,
                    full_state,
                    config,
                    run_config,
                    max_llm_calls,
                    usage,
                    trace_handle,
                    graph_inputs,
                )
                .await;
        }
        if node_kind == "reducenode" {
            let config = node
                .reduce_config
                .as_ref()
                .ok_or_else(|| GraphError::bad_request("ReduceNode missing configuration"))?;
            return self
                .execute_reduce_node(
                    node,
                    inputs,
                    full_state,
                    config,
                    run_config,
                    max_llm_calls,
                    usage,
                    trace_handle,
                )
                .await;
        }

        let impl_value = node
            .implementation
            .as_ref()
            .ok_or_else(|| GraphError::bad_request("node missing implementation"))?;
        let impl_obj = impl_value
            .as_object()
            .ok_or_else(|| GraphError::bad_request("node implementation must be object"))?;
        let impl_type = impl_obj.get("type").and_then(|v| v.as_str()).unwrap_or("");

        match impl_type {
            "template_transform" => {
                if let Some(limit) = max_llm_calls {
                    if usage.llm_calls >= limit {
                        return Err(GraphError::bad_request("max_llm_calls exceeded"));
                    }
                }
                let model_name = impl_obj
                    .get("model_name")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| {
                        GraphError::bad_request("template_transform missing model_name")
                    })?
                    .to_string();
                let temperature = impl_obj
                    .get("temperature")
                    .and_then(|v| v.as_f64())
                    .unwrap_or(0.0);
                let max_tokens = impl_obj.get("max_tokens").and_then(|v| v.as_i64());
                let sequence = impl_obj.get("sequence").ok_or_else(|| {
                    GraphError::bad_request("template_transform missing sequence")
                })?;
                let messages = render_messages(sequence, inputs)
                    .map_err(|err| GraphError::bad_request(err.0))?;

                let mut endpoint = crate::llm::resolve_endpoint(&model_name, Some(impl_value))?;
                endpoint.mode = ApiMode::ChatCompletions;
                let stream = run_config
                    .and_then(|cfg| cfg.stream_completions)
                    .unwrap_or(false);
                let response_format = impl_obj.get("response_format").cloned();

                let request = LlmRequest {
                    model: model_name.clone(),
                    messages: messages.clone(),
                    temperature,
                    max_tokens,
                    response_format,
                    endpoint,
                    stream,
                    tools: None,
                    tool_choice: None,
                    previous_response_id: None,
                };

                {
                    let mut trace = trace_handle.lock().await;
                    trace.record_llm_prompt(
                        &node.name,
                        &model_name,
                        1,
                        summarize_messages_for_trace(&messages),
                    );
                }

                let on_chunk: Option<LlmChunkCallback> = None;
                let llm_timer = Instant::now();
                let response = self.llm.send(&request, on_chunk).await?;
                let llm_elapsed_ms = llm_timer.elapsed().as_millis() as i64;
                usage.record(&model_name, &response.usage)?;

                {
                    let prompt = response.usage.prompt_tokens.unwrap_or(0);
                    let completion = response.usage.completion_tokens.unwrap_or(0);
                    let has_tools = false;
                    let started_at_ms = {
                        let trace = trace_handle.lock().await;
                        trace.now_ms().saturating_sub(llm_elapsed_ms)
                    };
                    let mut trace = trace_handle.lock().await;
                    trace.record_llm_call(
                        &model_name,
                        &node.name,
                        1,
                        started_at_ms,
                        llm_elapsed_ms,
                        prompt,
                        completion,
                        has_tools,
                        0,
                    );
                }

                Ok(parse_llm_output(
                    response.text,
                    impl_obj.get("response_format"),
                    impl_obj.get("output_schema"),
                ))
            }
            "python_function" => {
                let fn_str = impl_obj
                    .get("fn_str")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| GraphError::bad_request("python_function missing fn_str"))?;
                let args = json!({
                    "mode": "python_function",
                    "fn_str": fn_str,
                    "inputs": inputs,
                    "state": full_state,
                });
                let result = python::run_python(&args).await;
                match result {
                    Ok(value) => Ok(value),
                    Err(err) => Err(err),
                }
            }
            "tool_call" => {
                let tool_name = impl_obj
                    .get("tool")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| GraphError::bad_request("tool_call missing tool"))?;
                let mut args = inputs.as_object().cloned().unwrap_or_default();
                if let Some(params) = impl_obj.get("parameters").and_then(|v| v.as_object()) {
                    for (key, value) in params {
                        args.insert(key.clone(), value.clone());
                    }
                }
                let context = json!({
                    "node_id": node_id,
                    "node": node.name,
                });
                self.tools
                    .execute(
                        tool_name,
                        &Value::Object(args),
                        &context,
                        None,
                        Some(graph_inputs),
                    )
                    .await
            }
            "rlm_compute" => {
                self.execute_rlm_v1(
                    node,
                    inputs,
                    impl_value,
                    impl_obj,
                    usage,
                    trace_handle,
                    graph_inputs,
                )
                .await
            }
            other => Err(GraphError::bad_request(format!(
                "unsupported implementation type '{other}'"
            ))),
        }
    }

    fn execute_node_boxed<'a>(
        &'a self,
        node: &'a NodeDefinition,
        node_id: &'a str,
        inputs: &'a Value,
        full_state: &'a Value,
        run_config: Option<&'a RunConfig>,
        max_llm_calls: Option<i64>,
        usage: &'a mut UsageAccumulator,
        trace_handle: Arc<Mutex<TraceBuilder>>,
        graph_inputs: &'a Value,
    ) -> BoxFuture<'a, Result<Value>> {
        Box::pin(self.execute_node(
            node,
            node_id,
            inputs,
            full_state,
            run_config,
            max_llm_calls,
            usage,
            trace_handle,
            graph_inputs,
        ))
    }

    async fn execute_map_node(
        &self,
        node: &NodeDefinition,
        inputs: &Value,
        full_state: &Value,
        config: &MapConfig,
        run_config: Option<&RunConfig>,
        max_llm_calls: Option<i64>,
        usage: &mut UsageAccumulator,
        trace_handle: Arc<Mutex<TraceBuilder>>,
        graph_inputs: &Value,
    ) -> Result<Value> {
        let inner_node = node
            .inner_node
            .as_ref()
            .ok_or_else(|| GraphError::bad_request("MapNode missing inner_node"))?;
        let inputs_obj = inputs
            .as_object()
            .ok_or_else(|| GraphError::bad_request("MapNode inputs must be an object"))?;
        let items_value = inputs_obj
            .get("items")
            .cloned()
            .unwrap_or_else(|| Value::Array(Vec::new()));
        let items = items_value
            .as_array()
            .ok_or_else(|| GraphError::bad_request("MapNode expected items to be a list"))?;

        let mut mapped_state = Map::new();
        for (key, value) in inputs_obj {
            if key != "items" {
                mapped_state.insert(key.clone(), value.clone());
            }
        }

        if items.is_empty() {
            let mut result = Map::new();
            result.insert(format!("{}_outputs", node.name), Value::Array(Vec::new()));
            result.insert(format!("{}_errors", node.name), Value::Array(Vec::new()));
            result.insert(format!("{}_count", node.name), Value::Number(0.into()));
            result.insert(
                format!("{}_success_count", node.name),
                Value::Number(0.into()),
            );
            return Ok(Value::Object(result));
        }

        let state_obj = full_state.as_object().cloned().unwrap_or_default();
        let global_limit = run_config
            .and_then(|cfg| cfg.concurrency.as_ref())
            .and_then(|cfg| cfg.max_concurrent_nodes)
            .unwrap_or(config.max_concurrent as i64)
            .max(1) as usize;
        let limit = config.max_concurrent.min(global_limit).max(1);
        let semaphore = Arc::new(Semaphore::new(limit));
        let mut tasks = FuturesUnordered::new();

        for (idx, item) in items.iter().cloned().enumerate() {
            let permit = semaphore.clone();
            let inner_node = (**inner_node).clone();
            let item_key = config.item_key.clone();
            let state_keys = config.state_keys.clone();
            let mapped_state = mapped_state.clone();
            let state_obj = state_obj.clone();
            let engine = self.clone_for_task();
            let item_timeout_ms = config.item_timeout_ms;
            let trace_handle = trace_handle.clone();
            let graph_inputs = graph_inputs.clone();
            let run_config_owned = run_config.cloned();

            tasks.push(async move {
                let _permit = permit
                    .acquire()
                    .await
                    .map_err(|_| GraphError::internal("map node semaphore closed"))?;
                let mut item_state = Map::new();
                for (key, value) in mapped_state {
                    item_state.insert(key, value);
                }
                if let Some(keys) = state_keys {
                    for key in keys {
                        if let Some(value) = state_obj.get(&key) {
                            item_state.insert(key, value.clone());
                        }
                    }
                }
                item_state.insert(item_key, item);
                item_state.insert("index".to_string(), Value::Number((idx as i64).into()));
                let item_state_value = Value::Object(item_state);
                let item_inputs = resolve_node_inputs(&inner_node, &item_state_value)?;
                let mut local_usage = UsageAccumulator::default();
                let result = tokio::time::timeout(
                    std::time::Duration::from_millis(item_timeout_ms),
                    engine.execute_node(
                        &inner_node,
                        "<map_item>",
                        &item_inputs,
                        &item_state_value,
                        run_config_owned.as_ref(),
                        max_llm_calls,
                        &mut local_usage,
                        trace_handle.clone(),
                        &graph_inputs,
                    ),
                )
                .await;
                match result {
                    Ok(Ok(output)) => Ok::<
                        (usize, Value, Option<Value>, UsageAccumulator),
                        GraphError,
                    >((idx, output, None, local_usage)),
                    Ok(Err(err)) => Ok((
                        idx,
                        Value::Null,
                        Some(json!({
                            "type": "ExecutionError",
                            "message": err.to_string(),
                            "item_index": idx,
                            "inner_node": inner_node.name,
                        })),
                        local_usage,
                    )),
                    Err(_) => Ok((
                        idx,
                        Value::Null,
                        Some(json!({
                            "type": "TimeoutError",
                            "message": format!("Item {idx} timed out after {item_timeout_ms}ms"),
                            "item_index": idx,
                            "inner_node": inner_node.name,
                        })),
                        local_usage,
                    )),
                }
            });
        }

        let mut outputs = vec![Value::Null; items.len()];
        let mut errors = vec![Value::Null; items.len()];
        let mut success_count = 0usize;

        while let Some(result) = tasks.next().await {
            match result {
                Ok((idx, output, error, local_usage)) => {
                    if error.is_none() {
                        success_count += 1;
                        outputs[idx] = output;
                        errors[idx] = Value::Null;
                    } else if let Some(err_value) = error {
                        outputs[idx] = Value::Null;
                        errors[idx] = err_value;
                    }
                    usage.merge(local_usage);
                }
                Err(err) => {
                    return Err(GraphError::internal(format!("MapNode task failed: {err}")));
                }
            }
        }

        let mut result = Map::new();
        result.insert(format!("{}_outputs", node.name), Value::Array(outputs));
        result.insert(format!("{}_errors", node.name), Value::Array(errors));
        result.insert(
            format!("{}_count", node.name),
            Value::Number((items.len() as i64).into()),
        );
        result.insert(
            format!("{}_success_count", node.name),
            Value::Number((success_count as i64).into()),
        );
        Ok(Value::Object(result))
    }

    async fn execute_reduce_node(
        &self,
        node: &NodeDefinition,
        inputs: &Value,
        full_state: &Value,
        config: &ReduceConfig,
        run_config: Option<&RunConfig>,
        max_llm_calls: Option<i64>,
        usage: &mut UsageAccumulator,
        trace_handle: Arc<Mutex<TraceBuilder>>,
    ) -> Result<Value> {
        let inputs_obj = inputs
            .as_object()
            .ok_or_else(|| GraphError::bad_request("ReduceNode inputs must be an object"))?;
        let results_value = inputs_obj
            .get("results")
            .cloned()
            .unwrap_or_else(|| Value::Array(Vec::new()));
        let results = results_value
            .as_array()
            .ok_or_else(|| GraphError::bad_request("ReduceNode expected results to be a list"))?;

        check_reduce_success_threshold(results, config)?;

        let output = if config.reduce_strategy.as_deref() == Some("llm") {
            let inner_node = node.inner_node.as_ref().ok_or_else(|| {
                GraphError::bad_request("ReduceNode llm strategy requires inner_node")
            })?;
            let mut inner_state = full_state.as_object().cloned().unwrap_or_default();
            let results_key = config
                .results_key
                .clone()
                .unwrap_or_else(|| "event_reviews".to_string());
            inner_state.insert(results_key, Value::Array(results.clone()));
            let inner_state_value = Value::Object(inner_state);
            let inner_inputs = resolve_node_inputs(inner_node, &inner_state_value)?;
            let inner_output = self
                .execute_node_boxed(
                    inner_node,
                    "<reduce_inner>",
                    &inner_inputs,
                    &inner_state_value,
                    run_config,
                    max_llm_calls,
                    usage,
                    trace_handle,
                    &Value::Null,
                )
                .await?;
            if let Value::Object(map) = &inner_output {
                map.get("output")
                    .cloned()
                    .or_else(|| map.get(&format!("{}_output", inner_node.name)).cloned())
                    .unwrap_or_else(|| inner_output.clone())
            } else {
                inner_output
            }
        } else if let Some(strategy) = config.reduce_strategy.as_deref() {
            apply_reduce_strategy(strategy, results, config.value_key.as_deref())?
        } else if let Some(fn_str) = config.reduce_fn.as_deref() {
            let args = json!({
                "mode": "reduce_fn",
                "fn_str": fn_str,
                "results": results,
            });
            python::run_python(&args).await?
        } else {
            return Err(GraphError::bad_request(
                "ReduceNode requires reduce_strategy, reduce_fn, or inner_node",
            ));
        };

        let mut result = Map::new();
        result.insert(format!("{}_output", node.name), output);
        Ok(Value::Object(result))
    }

    async fn execute_rlm_v1(
        &self,
        node: &NodeDefinition,
        inputs: &Value,
        impl_value: &Value,
        impl_obj: &Map<String, Value>,
        usage: &mut UsageAccumulator,
        trace_handle: Arc<Mutex<TraceBuilder>>,
        _graph_inputs: &Value,
    ) -> Result<Value> {
        let model_name = impl_obj
            .get("model_name")
            .and_then(|v| v.as_str())
            .unwrap_or("gpt-4o")
            .to_string();

        let max_iterations = impl_obj
            .get("max_iterations")
            .and_then(|v| v.as_i64())
            .unwrap_or(25) as usize;
        let max_root_calls = impl_obj
            .get("max_root_calls")
            .and_then(|v| v.as_i64())
            .unwrap_or(50);
        let max_subcalls = impl_obj
            .get("max_subcalls")
            .and_then(|v| v.as_i64())
            .unwrap_or(100);
        let max_time_ms = impl_obj
            .get("max_time_ms")
            .and_then(|v| v.as_i64())
            .unwrap_or(120_000);
        let max_cost_usd = impl_obj
            .get("max_cost_usd")
            .and_then(|v| v.as_f64())
            .unwrap_or(f64::INFINITY);

        let max_messages_before_compact = impl_obj
            .get("max_messages_before_summary")
            .and_then(|v| v.as_i64())
            .unwrap_or(15) as usize;
        let keep_recent_messages = impl_obj
            .get("keep_recent_messages")
            .and_then(|v| v.as_i64())
            .unwrap_or(6) as usize;
        let prompt_token_budget = impl_obj
            .get("prompt_token_budget")
            .and_then(|v| v.as_i64())
            .map(|v| v as usize);

        let is_verifier = impl_obj
            .get("is_verifier")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let raw_answer_schema = impl_obj.get("answer_schema").cloned();

        let system_prompt = impl_obj
            .get("system_prompt")
            .and_then(|v| v.as_str())
            .or_else(|| inputs.get("system_prompt").and_then(|v| v.as_str()))
            .unwrap_or("You are an assistant. Use tools and call submit_answer.")
            .to_string();
        let user_prompt = impl_obj
            .get("user_prompt")
            .and_then(|v| v.as_str())
            .or_else(|| inputs.get("user_prompt").and_then(|v| v.as_str()));

        let tools_filter = impl_obj
            .get("tools")
            .and_then(|v| v.as_array())
            .map(|items| {
                items
                    .iter()
                    .filter_map(|item| item.as_str().map(|v| v.to_string()))
                    .collect::<Vec<String>>()
            });

        let limits = crate::rlm_v1::RlmLimits {
            max_iterations,
            max_root_calls,
            max_subcalls,
            max_time_ms,
            max_cost_usd,
            max_messages_before_compact,
            keep_recent_messages,
            prompt_token_budget,
        };

        let local_tool_state: SharedLocalToolState =
            Arc::new(tokio::sync::RwLock::new(LocalToolState::new()));

        let mut materialized_inputs: HashMap<String, String> = HashMap::new();
        if let Some(map) = inputs.as_object() {
            for (field_name, value) in map {
                if value.is_null() {
                    continue;
                }
                let serialized = serialize_input_value(value);
                if serialized.len() <= 2000 {
                    continue;
                }
                let filename = match field_name.as_str() {
                    "trace_content" => "trace.json".to_string(),
                    "rubric_content" => "rubric.json".to_string(),
                    _ => infer_input_filename(field_name, value),
                };
                materialized_inputs.insert(field_name.clone(), filename.clone());
                let args = json!({"field_name": field_name, "filename": filename});
                let ctx = json!({"node": node.name, "purpose": "materialize_context"});
                let _ = self
                    .tools
                    .execute(
                        "materialize_context",
                        &args,
                        &ctx,
                        Some(&local_tool_state),
                        Some(inputs),
                    )
                    .await;
            }
        }

        let rendered_system_prompt =
            render_rlm_prompt(Some(&system_prompt), inputs, &materialized_inputs);
        let rendered_user_prompt = render_rlm_prompt(user_prompt, inputs, &materialized_inputs);

        let mut messages = vec![json!({"role": "system", "content": rendered_system_prompt})];
        if !materialized_inputs.is_empty() {
            let mut notice = String::from(
                "Large inputs have been materialized to files. Use local_grep/local_search/view_lines/exec_python/codex_exec for inspection.\n",
            );
            for (field, filename) in &materialized_inputs {
                notice.push_str(&format!("- {field}: {filename}\n"));
            }
            messages.push(json!({"role": "system", "content": notice}));
        }
        messages.push(json!({"role": "user", "content": rendered_user_prompt}));

        let usage_handle = Arc::new(Mutex::new(UsageAccumulator::default()));

        let lm_client = GraphLmClient {
            llm: self.llm.clone(),
            impl_value: impl_value.clone(),
            usage: usage_handle.clone(),
            trace: trace_handle.clone(),
            node_name: node.name.clone(),
        };

        let tool_executor = GraphToolExecutor {
            tools: self.tools.clone(),
            local_tool_state: local_tool_state.clone(),
        };

        let run_request = crate::rlm_v1::RlmRunRequest {
            messages_seed: messages,
            inputs: inputs.clone(),
            query: inputs
                .get("query")
                .and_then(|v| v.as_str())
                .map(|v| v.to_string()),
            is_verifier,
            answer_schema: raw_answer_schema,
            limits,
            tool_filter: tools_filter,
            allowed_tool_capabilities: None,
            model: model_name.clone(),
            summarizer_model: None,
            supports_parallel_tool_calls: model_name.to_lowercase().contains("gpt-5"),
        };
        let run_ctx = crate::rlm_v1::RlmRunContext {
            run_id: "local".to_string(),
            node_name: node.name.clone(),
            rlm_impl: "v1".to_string(),
            history_store: None,
            trace_sink: None,
            event_sink: None,
        };

        let mut result = crate::rlm_v1::run_rlm(run_request, &lm_client, &tool_executor, run_ctx)
            .await
            .map_err(|err| GraphError::internal(format!("rlm v1 failed: {err}")))?;

        {
            let mut guard = usage_handle.lock().await;
            usage.merge(std::mem::take(&mut *guard));
        }

        if let Some(answer) = result.answer.take() {
            return Ok(answer);
        }

        let error = result.error.unwrap_or(crate::rlm_v1::RlmErrorInfo {
            kind: crate::rlm_v1::RlmErrorKind::ExecutionError,
            message: "RLM failed without a structured error".to_string(),
            details: None,
        });
        Ok(json!({
            "error": {
                "type": "VerifierError",
                "subtype": format!("{:?}", error.kind),
                "message": error.message,
            },
            "partial_answer": result.partial_answer.unwrap_or(Value::Null),
            "_iterations": result.iterations,
        }))
    }

    fn clone_for_task(&self) -> GraphEngineTask {
        GraphEngineTask {
            llm: self.llm.clone(),
            tools: self.tools.clone(),
        }
    }
}

#[derive(Clone)]
struct GraphEngineTask {
    llm: Arc<dyn LlmClientApi>,
    tools: Arc<dyn ToolExecutor>,
}

impl GraphEngineTask {
    async fn execute_node(
        &self,
        node: &NodeDefinition,
        node_id: &str,
        inputs: &Value,
        full_state: &Value,
        run_config: Option<&RunConfig>,
        max_llm_calls: Option<i64>,
        usage: &mut UsageAccumulator,
        trace_handle: Arc<Mutex<TraceBuilder>>,
        graph_inputs: &Value,
    ) -> Result<Value> {
        let engine = GraphEngine::new(self.llm.clone(), self.tools.clone());
        engine
            .execute_node(
                node,
                node_id,
                inputs,
                full_state,
                run_config,
                max_llm_calls,
                usage,
                trace_handle,
                graph_inputs,
            )
            .await
    }
}

#[derive(Debug, Clone)]
pub struct GraphRun {
    pub output: Value,
    pub usage_summary: Value,
    pub validation_result: Option<Value>,
    pub execution_trace: Value,
    pub graph_hash: Option<String>,
}

#[derive(Clone)]
struct GraphLmClient {
    llm: Arc<dyn LlmClientApi>,
    impl_value: Value,
    usage: Arc<Mutex<UsageAccumulator>>,
    trace: Arc<Mutex<TraceBuilder>>,
    node_name: String,
}

#[async_trait]
impl crate::rlm_v1::LmClient for GraphLmClient {
    async fn call(
        &self,
        req: crate::rlm_v1::LmRequest,
    ) -> std::result::Result<crate::rlm_v1::LmResponse, crate::rlm_v1::LmError> {
        let mut endpoint = crate::llm::resolve_endpoint(&req.model, Some(&self.impl_value))
            .map_err(|e| crate::rlm_v1::LmError {
                message: e.to_string(),
            })?;
        endpoint.mode = ApiMode::ChatCompletions;

        let request = LlmRequest {
            model: req.model.clone(),
            messages: req.messages.clone(),
            temperature: req.temperature,
            max_tokens: req.max_tokens,
            response_format: req.response_format.clone(),
            endpoint,
            stream: false,
            tools: req.tools.clone(),
            tool_choice: req.tool_choice.clone(),
            previous_response_id: None,
        };

        {
            let mut trace = self.trace.lock().await;
            trace.record_llm_prompt(
                &self.node_name,
                &req.model,
                1,
                summarize_messages_for_trace(&request.messages),
            );
        }

        let llm_timer = Instant::now();
        let response = self
            .llm
            .send(&request, None)
            .await
            .map_err(|e| crate::rlm_v1::LmError {
                message: e.to_string(),
            })?;
        let llm_elapsed_ms = llm_timer.elapsed().as_millis() as i64;

        {
            let mut usage = self.usage.lock().await;
            usage
                .record(&req.model, &response.usage)
                .map_err(|e| crate::rlm_v1::LmError {
                    message: e.to_string(),
                })?;
        }

        let raw = response.raw.clone().unwrap_or(Value::Null);
        let tool_calls = extract_tool_calls_from_chat(&raw)
            .into_iter()
            .map(|tc| crate::rlm_v1::ToolCall {
                id: tc.id,
                name: tc.name,
                arguments: tc.arguments,
            })
            .collect::<Vec<_>>();

        let prompt_tokens = response.usage.prompt_tokens.unwrap_or(0);
        let completion_tokens = response.usage.completion_tokens.unwrap_or(0);
        let cached_tokens = response.usage.cached_tokens.unwrap_or(0);
        let total_tokens = response
            .usage
            .total_tokens
            .unwrap_or(prompt_tokens + completion_tokens);

        Ok(crate::rlm_v1::LmResponse {
            text: response.text,
            tool_calls,
            usage: crate::rlm_v1::UsageMetrics {
                prompt_tokens,
                completion_tokens,
                total_tokens,
                cached_tokens,
                cost_usd: 0.0,
                cache_savings_usd: 0.0,
            },
            elapsed_ms: llm_elapsed_ms,
        })
    }
}

#[derive(Clone)]
struct GraphToolExecutor {
    tools: Arc<dyn ToolExecutor>,
    local_tool_state: SharedLocalToolState,
}

#[async_trait]
impl crate::rlm_v1::ToolExecutor for GraphToolExecutor {
    async fn execute(
        &self,
        call: crate::rlm_v1::ToolCall,
        ctx: crate::rlm_v1::ToolContext,
    ) -> std::result::Result<crate::rlm_v1::ToolResult, crate::rlm_v1::ToolError> {
        let context = json!({
            "run_id": ctx.run_id,
            "node": ctx.node_name,
            "iteration": ctx.iteration,
        });
        let tool_timer = Instant::now();
        let result = self
            .tools
            .execute(
                &call.name,
                &call.arguments,
                &context,
                Some(&self.local_tool_state),
                Some(ctx.inputs.as_ref()),
            )
            .await
            .unwrap_or_else(|err| json!({"success": false, "error": err.to_string()}));
        let tool_elapsed_ms = tool_timer.elapsed().as_millis() as i64;
        let (execution_mode, success, sandbox_timing) = parse_tool_trace(&result);
        let sandbox_value = sandbox_timing.and_then(|t| serde_json::to_value(t).ok());
        Ok(crate::rlm_v1::ToolResult {
            tool_name: call.name,
            result: result.clone(),
            agent_visible_payload: result,
            elapsed_ms: tool_elapsed_ms,
            success,
            execution_mode: Some(execution_mode),
            sandbox_timing: sandbox_value,
        })
    }

    async fn execute_many_concurrently(
        &self,
        calls: Vec<crate::rlm_v1::ToolCall>,
        ctx: crate::rlm_v1::ToolContext,
    ) -> Vec<std::result::Result<crate::rlm_v1::ToolResult, crate::rlm_v1::ToolError>> {
        if calls.is_empty() {
            return Vec::new();
        }
        let max_parallel = 4usize;
        let semaphore = Arc::new(Semaphore::new(max_parallel));
        let mut futures = FuturesUnordered::new();
        let calls_len = calls.len();
        for (idx, call) in calls.into_iter().enumerate() {
            let semaphore = semaphore.clone();
            let executor = self.clone();
            let ctx = ctx.clone();
            futures.push(async move {
                let permit = semaphore.acquire_owned().await;
                if permit.is_err() {
                    return (
                        idx,
                        Err(crate::rlm_v1::ToolError {
                            message: "tool concurrency semaphore closed".to_string(),
                        }),
                    );
                }
                let _permit = permit.unwrap();
                (idx, executor.execute(call, ctx).await)
            });
        }
        let mut results: Vec<
            Option<std::result::Result<crate::rlm_v1::ToolResult, crate::rlm_v1::ToolError>>,
        > = (0..calls_len).map(|_| None).collect();
        while let Some((idx, res)) = futures.next().await {
            results[idx] = Some(res);
        }
        results
            .into_iter()
            .map(|maybe| {
                maybe.unwrap_or_else(|| {
                    Err(crate::rlm_v1::ToolError {
                        message: "tool execution missing result".to_string(),
                    })
                })
            })
            .collect()
    }
}

fn initial_state(inputs: Option<Value>) -> Value {
    match inputs {
        Some(Value::Object(map)) => Value::Object(map),
        Some(other) => json!({ "input": other }),
        None => Value::Object(Map::new()),
    }
}

fn resolve_node_inputs(node: &NodeDefinition, state: &Value) -> Result<Value> {
    let mapping = node.input_mapping.as_ref();
    let value = match mapping {
        None => state.clone(),
        Some(Value::String(expr)) => {
            let trimmed = expr.trim();
            if trimmed == "state" {
                state.clone()
            } else {
                eval_mapping_expression(trimmed, state, None)
                    .map_err(|err| GraphError::bad_request(err.0))?
            }
        }
        Some(Value::Object(map)) => Value::Object(map.clone()),
        Some(other) => {
            return Err(GraphError::bad_request(format!(
                "input_mapping must be string or object, got {}",
                other
            )));
        }
    };

    if !value.is_object() {
        return Err(GraphError::bad_request(
            "input_mapping did not evaluate to an object",
        ));
    }
    Ok(value)
}

async fn evaluate_edge_condition(
    condition: Option<&Value>,
    exec_state: &Value,
    node_output: &Value,
) -> Result<bool> {
    let Some(condition) = condition else {
        return Ok(true);
    };
    let source = extract_condition_source(condition)?;
    let args = json!({
        "mode": "condition",
        "code": source,
        "state": exec_state,
        "node_output": node_output,
    });
    let result = python::run_python(&args).await?;
    match result {
        Value::Bool(flag) => Ok(flag),
        Value::Number(num) => Ok(num.as_i64().unwrap_or(0) != 0),
        Value::String(text) => Ok(text == "true" || text == "True"),
        other => Err(GraphError::bad_request(format!(
            "edge condition returned non-boolean: {other}"
        ))),
    }
}

fn extract_condition_source(condition: &Value) -> Result<String> {
    match condition {
        Value::String(text) => Ok(text.clone()),
        Value::Object(map) => {
            if let Some(fn_str) = map.get("fn_str").and_then(|v| v.as_str()) {
                return Ok(fn_str.to_string());
            }
            if let Some(text) = map.get("condition").and_then(|v| v.as_str()) {
                return Ok(text.to_string());
            }
            if let Some(text) = map.get("condition_str").and_then(|v| v.as_str()) {
                return Ok(text.to_string());
            }
            Err(GraphError::bad_request(
                "edge condition object missing fn_str",
            ))
        }
        _ => Err(GraphError::bad_request(
            "edge condition must be string or object",
        )),
    }
}

fn apply_node_output(state: &mut Map<String, Value>, node_name: &str, output: Value) {
    let output_key = format!("{node_name}_output");
    let has_explicit_output_key = output
        .as_object()
        .is_some_and(|m| m.contains_key(output_key.as_str()));

    if let Value::Object(map) = &output {
        for (key, value) in map {
            state.insert(key.clone(), value.clone());
        }
    }

    // If the node already returned `{node}_output` explicitly (e.g. ReduceNode),
    // don't overwrite it with the whole output object (which would cause nesting).
    if !has_explicit_output_key {
        state.insert(output_key, output);
    }
}

async fn apply_output_mapping(
    tool_executor: &dyn ToolExecutor,
    state: &mut Map<String, Value>,
    node_output: &Value,
    mapping: Option<&Value>,
    node_name: &str,
    graph_inputs: &Value,
) -> Result<()> {
    let Some(mapping) = mapping else {
        return Ok(());
    };
    match mapping {
        Value::String(expr) => {
            let updates = if let Ok(value) =
                eval_mapping_expression(expr, &Value::Object(state.clone()), Some(node_output))
            {
                value
            } else {
                let args = json!({
                    "mode": "output_mapping",
                    "code": expr,
                    "state": Value::Object(state.clone()),
                    "node_output": node_output,
                });
                python::run_python(&args).await?
            };
            let updates_value = if updates.get("state").is_some() {
                updates.get("state").cloned().unwrap_or(Value::Null)
            } else {
                updates
            };
            let updates = updates_value.as_object().ok_or_else(|| {
                GraphError::bad_request("output_mapping did not evaluate to object")
            })?;
            for (key, value) in updates {
                state.insert(key.clone(), value.clone());
            }
        }
        Value::Object(map) => {
            for (key, value) in map {
                let resolved = if let Some(path) = value.as_str() {
                    resolve_path(state, node_output, path)
                } else {
                    value.clone()
                };
                state.insert(key.clone(), resolved);
            }
        }
        other => {
            return Err(GraphError::bad_request(format!(
                "output_mapping must be string or object, got {}",
                other
            )));
        }
    }

    // Some graphs expect output-mapping hooks to be able to call tools (rare).
    // We keep `tool_executor` in the signature so we can support it later without API churn.
    let _ = (tool_executor, node_name, graph_inputs);
    Ok(())
}

fn resolve_path(state: &Map<String, Value>, node_output: &Value, path: &str) -> Value {
    let mut parts = path.split('.');
    let Some(first) = parts.next() else {
        return Value::Null;
    };
    let mut current = if first == "node_output" {
        node_output.clone()
    } else if first == "state" {
        Value::Object(state.clone())
    } else {
        state.get(first).cloned().unwrap_or(Value::Null)
    };
    for part in parts {
        current = match current {
            Value::Object(map) => map.get(part).cloned().unwrap_or(Value::Null),
            _ => Value::Null,
        };
    }
    current
}

fn build_in_degree(graph: &GraphDefinition) -> HashMap<String, i64> {
    let mut in_degree: HashMap<String, i64> = graph.nodes.keys().map(|k| (k.clone(), 0)).collect();
    for edges in graph.control_edges.values() {
        for edge in edges {
            if let Some(entry) = in_degree.get_mut(&edge.target) {
                *entry += 1;
            }
        }
    }
    in_degree
}

fn initial_ready_queue(
    graph: &GraphDefinition,
    in_degree: &HashMap<String, i64>,
) -> VecDeque<String> {
    if !graph.start_nodes.is_empty() {
        let mut seen = HashSet::new();
        let mut queue = VecDeque::new();
        for node in &graph.start_nodes {
            if seen.insert(node.clone()) {
                queue.push_back(node.clone());
            }
        }
        return queue;
    }
    let mut queue = VecDeque::new();
    for (node, count) in in_degree {
        if *count == 0 {
            queue.push_back(node.clone());
        }
    }
    queue
}

fn parse_llm_output(
    text: String,
    response_format: Option<&Value>,
    output_schema: Option<&Value>,
) -> Value {
    let wants_json = response_format.is_some() || output_schema.is_some();
    let trimmed = text.trim();
    if wants_json || trimmed.starts_with('{') || trimmed.starts_with('[') {
        if let Ok(value) = serde_json::from_str::<Value>(trimmed) {
            return value;
        }
    }
    Value::String(text)
}

fn serialize_input_value(value: &Value) -> String {
    if value.is_object() || value.is_array() {
        serde_json::to_string_pretty(value).unwrap_or_else(|_| value.to_string())
    } else {
        value.as_str().unwrap_or(&value.to_string()).to_string()
    }
}

fn infer_input_filename(field_name: &str, value: &Value) -> String {
    let ext = if value.is_object() || value.is_array() {
        "json"
    } else {
        "txt"
    };
    let sanitized = sanitize_filename(field_name);
    format!("{sanitized}.{ext}")
}

fn sanitize_filename(value: &str) -> String {
    let mut out = String::new();
    for ch in value.chars() {
        if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
            out.push(ch);
        } else {
            out.push('_');
        }
    }
    if out.is_empty() {
        return "input".to_string();
    }
    if out.len() > 64 {
        out.truncate(64);
    }
    out
}

fn render_rlm_prompt(
    template: Option<&str>,
    inputs: &Value,
    materialized_inputs: &HashMap<String, String>,
) -> String {
    let Some(template) = template else {
        return inputs.to_string();
    };
    let mut rendered = template.to_string();
    if let Some(map) = inputs.as_object() {
        for (key, value) in map {
            let placeholder = format!("<input>{key}</input>");
            let replacement = if let Some(filename) = materialized_inputs.get(key) {
                format!("[file: {filename}]")
            } else if value.is_object() || value.is_array() {
                serialize_input_value(value)
            } else {
                value.as_str().unwrap_or(&value.to_string()).to_string()
            };
            rendered = rendered.replace(&placeholder, &replacement);
        }
    }
    rendered
}

fn summarize_messages_for_trace(messages: &[Value]) -> Vec<Value> {
    let mut summarized = Vec::with_capacity(messages.len());
    for msg in messages {
        let Some(obj) = msg.as_object() else {
            summarized.push(json!({"role": "unknown", "content": msg.to_string()}));
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
        let truncated = truncate_text(&content_text, 8000);
        summarized.push(json!({"role": role, "content": truncated}));
    }
    summarized
}

fn truncate_text(text: &str, max_chars: usize) -> String {
    if text.chars().count() <= max_chars {
        return text.to_string();
    }
    let truncate_at = text
        .char_indices()
        .nth(max_chars)
        .map_or(text.len(), |(idx, _)| idx);
    format!("{}... (truncated)", &text[..truncate_at])
}

#[derive(Clone)]
struct RlmToolCall {
    id: Option<String>,
    name: String,
    arguments: Value,
}

fn extract_tool_calls_from_chat(raw: &Value) -> Vec<RlmToolCall> {
    let mut out = Vec::new();
    let Some(choices) = raw.get("choices").and_then(|v| v.as_array()) else {
        return out;
    };
    let Some(first) = choices.first() else {
        return out;
    };
    let Some(message) = first.get("message") else {
        return out;
    };
    let Some(tool_calls) = message.get("tool_calls").and_then(|v| v.as_array()) else {
        return out;
    };
    for item in tool_calls {
        let id = item
            .get("id")
            .and_then(|v| v.as_str())
            .map(|v| v.to_string());
        let function = item.get("function").and_then(|v| v.as_object());
        let name = function
            .and_then(|f| f.get("name"))
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        let args_val = function.and_then(|f| f.get("arguments"));
        let arguments = parse_tool_call_args(args_val);
        if name.trim().is_empty() {
            continue;
        }
        out.push(RlmToolCall {
            id,
            name,
            arguments,
        });
    }
    out
}

fn parse_tool_call_args(value: Option<&Value>) -> Value {
    match value {
        Some(Value::String(text)) => {
            serde_json::from_str::<Value>(text).unwrap_or(Value::String(text.clone()))
        }
        Some(Value::Object(map)) => Value::Object(map.clone()),
        Some(Value::Array(items)) => Value::Array(items.clone()),
        Some(other) => other.clone(),
        None => Value::Object(Map::new()),
    }
}

fn parse_tool_trace(result: &Value) -> (String, bool, Option<SandboxTiming>) {
    let success = result
        .get("success")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let is_local = result.get("_local").and_then(|v| v.as_bool());
    let execution_mode = match is_local {
        Some(true) => "local".to_string(),
        Some(false) => "sandbox".to_string(),
        None => {
            if result.get("_timing_breakdown").is_some() {
                "sandbox".to_string()
            } else {
                "unknown".to_string()
            }
        }
    };
    let sandbox_timing = parse_sandbox_timing(result.get("_timing_breakdown"));
    (execution_mode, success, sandbox_timing)
}

fn parse_sandbox_timing(value: Option<&Value>) -> Option<SandboxTiming> {
    let obj = value?.as_object()?;
    let read_ms = |key: &str| -> i64 {
        obj.get(key)
            .and_then(|v| v.as_f64())
            .map_or(0, |v| v.round() as i64)
    };
    Some(SandboxTiming {
        provision_ms: read_ms("container_provision_ms"),
        lock_wait_ms: read_ms("workspace_lock_wait_ms"),
        workspace_setup_ms: read_ms("workspace_setup_ms"),
        file_upload_ms: read_ms("file_upload_ms"),
        command_exec_ms: read_ms("command_exec_ms"),
    })
}

fn check_reduce_success_threshold(results: &[Value], config: &ReduceConfig) -> Result<()> {
    if results.is_empty() {
        if config.require_all_success || config.min_success > 0.0 {
            return Err(GraphError::bad_request(
                "ReduceNode received empty results but success threshold required",
            ));
        }
        return Ok(());
    }
    let success_count = results.iter().filter(|v| !v.is_null()).count();
    let total = results.len();
    let success_ratio = success_count as f64 / total as f64;
    if config.require_all_success && success_count < total {
        return Err(GraphError::bad_request(format!(
            "ReduceNode requires all items to succeed, but {}/{} failed",
            total - success_count,
            total
        )));
    }
    if success_ratio < config.min_success {
        return Err(GraphError::bad_request(format!(
            "ReduceNode success ratio {:.2}% below min_success {:.2}%",
            success_ratio * 100.0,
            config.min_success * 100.0
        )));
    }
    Ok(())
}

fn apply_reduce_strategy(
    strategy: &str,
    results: &[Value],
    value_key: Option<&str>,
) -> Result<Value> {
    let valid: Vec<&Value> = results.iter().filter(|v| !v.is_null()).collect();
    match strategy {
        "count" => Ok(Value::Number((valid.len() as i64).into())),
        "first" => Ok(valid.first().copied().cloned().unwrap_or(Value::Null)),
        "last" => Ok(valid.last().copied().cloned().unwrap_or(Value::Null)),
        "concat" | "mean" | "sum" | "max" | "min" => {
            let key = value_key.ok_or_else(|| {
                GraphError::bad_request(format!(
                    "ReduceNode strategy '{strategy}' requires value_key"
                ))
            })?;
            let mut values = Vec::new();
            for item in &valid {
                if let Some(obj) = item.as_object() {
                    if let Some(val) = obj.get(key) {
                        values.push(val.clone());
                    }
                }
            }
            if strategy == "concat" {
                return Ok(Value::Array(values));
            }
            let mut numbers = Vec::new();
            for value in values {
                if let Some(num) = value.as_f64() {
                    numbers.push(num);
                }
            }
            match strategy {
                "mean" => {
                    let avg = if numbers.is_empty() {
                        0.0
                    } else {
                        numbers.iter().sum::<f64>() / numbers.len() as f64
                    };
                    Ok(Value::Number(
                        serde_json::Number::from_f64(avg)
                            .unwrap_or_else(|| serde_json::Number::from(0)),
                    ))
                }
                "sum" => {
                    let total = numbers.iter().sum::<f64>();
                    Ok(Value::Number(
                        serde_json::Number::from_f64(total)
                            .unwrap_or_else(|| serde_json::Number::from(0)),
                    ))
                }
                "max" => Ok(numbers
                    .iter()
                    .copied()
                    .fold(None::<f64>, |acc, val| {
                        Some(acc.map_or(val, |cur| cur.max(val)))
                    })
                    .and_then(serde_json::Number::from_f64)
                    .map_or(Value::Null, Value::Number)),
                "min" => Ok(numbers
                    .iter()
                    .copied()
                    .fold(None::<f64>, |acc, val| {
                        Some(acc.map_or(val, |cur| cur.min(val)))
                    })
                    .and_then(serde_json::Number::from_f64)
                    .map_or(Value::Null, Value::Number)),
                _ => Err(GraphError::bad_request(format!(
                    "Unsupported reduce strategy '{strategy}'"
                ))),
            }
        }
        other => Err(GraphError::bad_request(format!(
            "Unsupported reduce strategy '{other}'"
        ))),
    }
}

fn apply_output_config(
    exec_state: &Value,
    run_config: Option<&RunConfig>,
) -> Result<(Value, Option<Value>, bool)> {
    let Some(output_config) = run_config.and_then(|cfg| cfg.output_config.as_ref()) else {
        return Ok((exec_state.clone(), None, false));
    };

    let extract_from = output_config.extract_from.clone().unwrap_or_default();
    let (mut output, extraction_path, mut errors) = extract_output(exec_state, &extract_from);

    if let Some(format) = output_config.format.as_deref() {
        if format.eq_ignore_ascii_case("json") {
            if let Some(text) = output.as_str() {
                if let Ok(parsed) = serde_json::from_str::<Value>(text) {
                    output = parsed;
                }
            }
        }
    }

    if let Some(schema) = &output_config.schema {
        let (schema_valid, schema_errors) = validate_output_schema(schema, &output);
        if !schema_valid {
            errors.extend(schema_errors);
        }
    }

    let strict = output_config.strict.unwrap_or(true);
    let valid = errors.is_empty();
    let validation_failed = strict && !valid;
    let validation_result = json!({
        "valid": valid,
        "errors": errors,
        "warnings": [],
        "extraction_path": extraction_path,
        "format": output_config.format,
    });
    Ok((output, Some(validation_result), validation_failed))
}

fn extract_output(
    exec_state: &Value,
    extract_from: &[String],
) -> (Value, Option<String>, Vec<String>) {
    if extract_from.is_empty() {
        return (exec_state.clone(), Some("(root)".to_string()), Vec::new());
    }

    let Some(map) = exec_state.as_object() else {
        return (
            Value::Null,
            None,
            vec![format!(
                "Cannot extract output: expected object, got {}",
                value_type_name(exec_state)
            )],
        );
    };

    for path in extract_from {
        if path == "(root)" || path == "$" {
            return (exec_state.clone(), Some("(root)".to_string()), Vec::new());
        }

        let mut current = Value::Object(map.clone());
        let mut found = true;
        for part in path.split('.') {
            if let Value::Object(obj) = &current {
                if let Some(next) = obj.get(part) {
                    current = next.clone();
                } else {
                    found = false;
                    break;
                }
            } else {
                found = false;
                break;
            }
        }

        if found {
            return (current, Some(path.clone()), Vec::new());
        }
    }

    (
        Value::Null,
        None,
        vec![format!(
            "Could not extract output. Tried paths: {extract_from:?}"
        )],
    )
}

fn value_type_name(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "bool",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

fn validate_output_schema(schema: &Value, output: &Value) -> (bool, Vec<String>) {
    let compiled = jsonschema::JSONSchema::compile(schema);
    let Ok(compiled) = compiled else {
        return (false, vec!["invalid output schema".to_string()]);
    };
    let result = compiled.validate(output);
    match result {
        Ok(()) => (true, Vec::new()),
        Err(errors) => (
            false,
            errors.map(|err| err.to_string()).collect::<Vec<String>>(),
        ),
    }
}

#[derive(Default)]
struct UsageAccumulator {
    llm_calls: i64,
    prompt_tokens: i64,
    completion_tokens: i64,
    total_tokens: i64,
    cached_tokens: i64,
    by_model: HashMap<String, ModelUsage>,
}

#[derive(Default)]
struct ModelUsage {
    calls: i64,
    prompt_tokens: i64,
    completion_tokens: i64,
    total_tokens: i64,
    cached_tokens: i64,
}

impl UsageAccumulator {
    fn record(&mut self, model: &str, usage: &UsageSummary) -> Result<()> {
        self.llm_calls += 1;
        let prompt = usage.prompt_tokens.unwrap_or(0);
        let completion = usage.completion_tokens.unwrap_or(0);
        let cached = usage.cached_tokens.unwrap_or(0);
        let total = usage.total_tokens.unwrap_or(prompt + completion);
        self.prompt_tokens += prompt;
        self.completion_tokens += completion;
        self.total_tokens += total;
        self.cached_tokens += cached;
        let entry = self.by_model.entry(model.to_string()).or_default();
        entry.calls += 1;
        entry.prompt_tokens += prompt;
        entry.completion_tokens += completion;
        entry.total_tokens += total;
        entry.cached_tokens += cached;
        Ok(())
    }

    fn merge(&mut self, other: UsageAccumulator) {
        self.llm_calls += other.llm_calls;
        self.prompt_tokens += other.prompt_tokens;
        self.completion_tokens += other.completion_tokens;
        self.total_tokens += other.total_tokens;
        self.cached_tokens += other.cached_tokens;
        for (model, usage) in other.by_model {
            let entry = self.by_model.entry(model).or_default();
            entry.calls += usage.calls;
            entry.prompt_tokens += usage.prompt_tokens;
            entry.completion_tokens += usage.completion_tokens;
            entry.total_tokens += usage.total_tokens;
            entry.cached_tokens += usage.cached_tokens;
        }
    }

    fn to_value(&self) -> Value {
        let mut models = Map::new();
        for (model, usage) in &self.by_model {
            models.insert(
                model.clone(),
                json!({
                    "calls": usage.calls,
                    "prompt_tokens": usage.prompt_tokens,
                    "completion_tokens": usage.completion_tokens,
                    "total_tokens": usage.total_tokens,
                    "cached_tokens": usage.cached_tokens,
                }),
            );
        }
        json!({
            "llm_calls": self.llm_calls,
            "prompt_tokens": self.prompt_tokens,
            "completion_tokens": self.completion_tokens,
            "total_tokens": self.total_tokens,
            "cached_tokens": self.cached_tokens,
            "by_model": models,
        })
    }
}

fn graph_ir_fingerprint(graph: &GraphDefinition) -> GraphIr {
    let mut nodes = Vec::new();
    for (id, node) in &graph.nodes {
        nodes.push(crate::ir::GraphNode {
            id: id.clone(),
            node_type: node.node_type.clone(),
            config: node.implementation.clone(),
            inputs: Vec::new(),
            outputs: Vec::new(),
        });
    }
    let ir = GraphIr {
        schema_version: crate::ir::GRAPH_IR_SCHEMA_VERSION.to_string(),
        graph: None,
        metadata: None,
        nodes,
        edges: Vec::new(),
        entrypoints: graph.start_nodes.clone(),
    };
    canonicalize_graph_ir(&ir)
}

fn hash_graph_ir_safe(ir: GraphIr) -> Option<String> {
    crate::ir::hash_graph_ir(&ir).ok()
}
