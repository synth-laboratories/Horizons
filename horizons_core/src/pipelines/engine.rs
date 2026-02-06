use crate::core_agents::executor::CoreAgentsExecutor;
use crate::core_agents::mcp::{McpClient, McpToolCall};
use crate::core_agents::traits::CoreAgents as _;
use crate::events::models::{Event, EventDirection};
use crate::events::traits::EventBus;
use crate::models::{AgentIdentity, ProjectDbHandle};
use crate::pipelines::models::{
    FailurePolicy, PipelineRun, PipelineSpec, PipelineStatus, PipelineStep, StepKind, StepResult,
    StepStatus,
};
use crate::pipelines::traits::{GraphRunner, PipelineRunner, Subagent};
use crate::{Error, Result};
use async_trait::async_trait;
use chrono::Utc;
use dashmap::DashMap;
use serde_json::Value;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::Arc;

struct PipelineRunState {
    spec: PipelineSpec,
    inputs: Value,
    order: Vec<String>,
    approved: HashSet<String>,
    run: PipelineRun,
}

/// Default in-memory pipeline runner.
pub struct DefaultPipelineRunner {
    event_bus: Arc<dyn EventBus>,
    subagent: Arc<dyn Subagent>,
    mcp_client: Option<Arc<dyn McpClient>>,
    graph_runner: Option<Arc<dyn GraphRunner>>,
    runs: DashMap<String, tokio::sync::Mutex<PipelineRunState>>,
}

impl DefaultPipelineRunner {
    #[tracing::instrument(level = "debug", skip_all)]
    pub fn new(
        event_bus: Arc<dyn EventBus>,
        subagent: Arc<dyn Subagent>,
        mcp_client: Option<Arc<dyn McpClient>>,
        graph_runner: Option<Arc<dyn GraphRunner>>,
    ) -> Self {
        Self {
            event_bus,
            subagent,
            mcp_client,
            graph_runner,
            runs: DashMap::new(),
        }
    }

    async fn emit(&self, org_id: &str, topic: &str, payload: Value, dedupe_key: String) {
        let ev = Event::new(
            org_id.to_string(),
            None,
            EventDirection::Outbound,
            topic,
            "pipelines:runner",
            payload,
            dedupe_key,
            serde_json::json!({}),
            None,
        );
        let Ok(ev) = ev else {
            return;
        };
        let _ = self.event_bus.publish(ev).await;
    }

    fn find_step<'a>(spec: &'a PipelineSpec, step_id: &str) -> Option<&'a PipelineStep> {
        spec.steps.iter().find(|s| s.id == step_id)
    }

    async fn execute_until_blocked(
        &self,
        st: &mut PipelineRunState,
        identity: &AgentIdentity,
    ) -> Result<()> {
        st.run.status = PipelineStatus::Running;

        for step_id in st.order.clone() {
            let step = Self::find_step(&st.spec, &step_id).ok_or_else(|| {
                Error::BackendMessage(format!("pipeline step not found: {step_id}"))
            })?;

            // Skip already completed steps (resume path).
            if let Some(r) = st.run.step_results.get(&step_id) {
                if matches!(
                    r.status,
                    StepStatus::Succeeded | StepStatus::Failed | StepStatus::Skipped
                ) {
                    continue;
                }
            }

            if step.approval_required && !st.approved.contains(&step_id) {
                st.run.status = PipelineStatus::WaitingApproval(step_id.clone());
                self.emit(
                    &st.spec.org_id,
                    "pipeline.waiting_approval",
                    serde_json::json!({
                        "run_id": st.run.id,
                        "pipeline_id": st.run.pipeline_id,
                        "step_id": step_id,
                    }),
                    format!("pipeline_waiting_approval:{}:{}", st.run.id, step_id),
                )
                .await;
                return Ok(());
            }

            // Resolve step inputs (templates reference previous outputs).
            let mut ctx = build_template_context(&st.inputs, &st.run.step_results);
            if let Value::Object(m) = &mut ctx {
                m.insert(
                    "run".to_string(),
                    serde_json::json!({
                        "run_id": st.run.id,
                        "pipeline_id": st.run.pipeline_id,
                        "org_id": st.spec.org_id,
                    }),
                );
            }
            let step_inputs = resolve_templates(step.inputs.clone(), &ctx)?;

            st.run.step_results.insert(
                step_id.clone(),
                StepResult {
                    step_id: step_id.clone(),
                    status: StepStatus::Running,
                    output: None,
                    error: None,
                    duration_ms: 0,
                },
            );
            self.emit(
                &st.spec.org_id,
                "pipeline.step.started",
                serde_json::json!({
                    "run_id": st.run.id,
                    "pipeline_id": st.run.pipeline_id,
                    "step_id": step_id,
                    "kind": step.kind,
                }),
                format!("pipeline_step_started:{}:{}", st.run.id, step_id),
            )
            .await;

            let started = std::time::Instant::now();
            let fut = self.execute_step(step, step_inputs, identity, &ctx, st.spec.on_failure);
            let out = if let Some(timeout_ms) = step.timeout_ms {
                match tokio::time::timeout(std::time::Duration::from_millis(timeout_ms), fut).await
                {
                    Ok(r) => r,
                    Err(_) => Err(Error::BackendMessage(format!(
                        "step timed out after {timeout_ms}ms"
                    ))),
                }
            } else {
                fut.await
            };
            let duration_ms = started.elapsed().as_millis() as u64;

            match out {
                Ok(v) => {
                    st.run.step_results.insert(
                        step_id.clone(),
                        StepResult {
                            step_id: step_id.clone(),
                            status: StepStatus::Succeeded,
                            output: Some(v.clone()),
                            error: None,
                            duration_ms,
                        },
                    );
                    self.emit(
                        &st.spec.org_id,
                        "pipeline.step.completed",
                        serde_json::json!({
                            "run_id": st.run.id,
                            "pipeline_id": st.run.pipeline_id,
                            "step_id": step_id,
                            "duration_ms": duration_ms,
                        }),
                        format!("pipeline_step_completed:{}:{}", st.run.id, step_id),
                    )
                    .await;
                }
                Err(e) => {
                    let msg = e.to_string();
                    st.run.step_results.insert(
                        step_id.clone(),
                        StepResult {
                            step_id: step_id.clone(),
                            status: StepStatus::Failed,
                            output: None,
                            error: Some(msg.clone()),
                            duration_ms,
                        },
                    );
                    self.emit(
                        &st.spec.org_id,
                        "pipeline.step.failed",
                        serde_json::json!({
                            "run_id": st.run.id,
                            "pipeline_id": st.run.pipeline_id,
                            "step_id": step_id,
                            "duration_ms": duration_ms,
                            "error": msg,
                        }),
                        format!("pipeline_step_failed:{}:{}", st.run.id, step_id),
                    )
                    .await;

                    match st.spec.on_failure {
                        FailurePolicy::SkipAndContinue => {
                            continue;
                        }
                        FailurePolicy::Retry(n) => {
                            // Retry is handled within execute_step when configured; treat this as halt
                            // once attempts are exhausted.
                            let _ = n;
                            st.run.status = PipelineStatus::Failed;
                            st.run.completed_at = Some(Utc::now());
                            return Ok(());
                        }
                        FailurePolicy::Halt => {
                            st.run.status = PipelineStatus::Failed;
                            st.run.completed_at = Some(Utc::now());
                            return Ok(());
                        }
                    }
                }
            }
        }

        st.run.status = PipelineStatus::Succeeded;
        st.run.completed_at = Some(Utc::now());
        self.emit(
            &st.spec.org_id,
            "pipeline.run.completed",
            serde_json::json!({
                "run_id": st.run.id,
                "pipeline_id": st.run.pipeline_id,
                "status": "succeeded",
            }),
            format!("pipeline_run_completed:{}", st.run.id),
        )
        .await;

        Ok(())
    }

    async fn execute_step(
        &self,
        step: &PipelineStep,
        step_inputs: Value,
        identity: &AgentIdentity,
        ctx: &Value,
        on_failure: FailurePolicy,
    ) -> Result<Value> {
        let mut attempts = 1u32;
        let mut backoff_ms = 0u64;

        if let Some(rp) = step.retry_policy {
            attempts = rp.max_attempts.max(1);
            backoff_ms = rp.backoff_ms;
        } else if let FailurePolicy::Retry(n) = on_failure {
            attempts = (n + 1).max(1);
        }

        for attempt in 1..=attempts {
            let res = self.execute_step_once(step, step_inputs.clone(), identity, ctx).await;
            match res {
                Ok(v) => return Ok(v),
                Err(e) => {
                    if attempt >= attempts {
                        return Err(e);
                    }
                    if backoff_ms > 0 {
                        tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)).await;
                    }
                }
            }
        }

        Err(Error::BackendMessage("unreachable retry loop".to_string()))
    }

    async fn execute_step_once(
        &self,
        step: &PipelineStep,
        step_inputs: Value,
        identity: &AgentIdentity,
        ctx: &Value,
    ) -> Result<Value> {
        match &step.kind {
            StepKind::Agent { spec } => {
                let mut c = spec.context.clone();
                // Provide a stable place for agent implementations to find pipeline-level context.
                if let Value::Object(m) = &mut c {
                    m.insert(
                        "pipeline_inputs".to_string(),
                        ctx.get("inputs").cloned().unwrap_or(Value::Null),
                    );
                }
                self.subagent
                    .run(&spec.role, step_inputs, spec.tools.clone(), c)
                    .await
            }
            StepKind::ToolCall { tool, args } => {
                let Some(mcp) = self.mcp_client.clone() else {
                    return Err(Error::InvalidInput(
                        "pipeline tool step requires MCP gateway".to_string(),
                    ));
                };
                let args = resolve_templates(args.clone(), ctx)?;
                let call = McpToolCall::new(
                    tool.clone(),
                    args,
                    vec![],
                    identity.clone(),
                    ulid::Ulid::new().to_string(),
                    None,
                )?;
                let res = mcp.call_tool(call).await?;
                Ok(res.output)
            }
            StepKind::GraphRun { graph_id } => {
                let Some(gr) = self.graph_runner.clone() else {
                    return Err(Error::InvalidInput(
                        "pipeline graph step requires graph runner".to_string(),
                    ));
                };
                gr.run_graph(graph_id, step_inputs, ctx.clone(), identity).await
            }
            StepKind::LlmCall { .. } => Err(Error::InvalidInput(
                "LlmCall step not implemented".to_string(),
            )),
            StepKind::Custom { handler } => Err(Error::InvalidInput(format!(
                "Custom step handler not registered: {handler}"
            ))),
        }
    }
}

/// Subagent implementation that runs registered core agents via `CoreAgentsExecutor`.
pub struct CoreAgentsSubagent {
    core_agents: Arc<CoreAgentsExecutor>,
}

impl CoreAgentsSubagent {
    #[tracing::instrument(level = "debug", skip_all)]
    pub fn new(core_agents: Arc<CoreAgentsExecutor>) -> Self {
        Self { core_agents }
    }
}

#[async_trait]
impl Subagent for CoreAgentsSubagent {
    #[tracing::instrument(level = "info", skip_all, fields(role = %role))]
    async fn run(
        &self,
        role: &str,
        input: Value,
        _tools: Vec<String>,
        context: Value,
    ) -> Result<Value> {
        let h = context
            .get("_project_db_handle")
            .cloned()
            .ok_or_else(|| Error::InvalidInput("subagent context missing _project_db_handle".to_string()))?;
        let handle: ProjectDbHandle =
            serde_json::from_value(h).map_err(|e| Error::backend("deserialize _project_db_handle", e))?;

        let identity = AgentIdentity::System {
            name: "pipeline".to_string(),
        };
        let result = self
            .core_agents
            .run(
                handle.org_id,
                handle.project_id,
                handle.clone(),
                role,
                &identity,
                Some(input),
            )
            .await?;

        serde_json::to_value(result).map_err(|e| Error::backend("serialize subagent output", e))
    }
}

#[async_trait]
impl PipelineRunner for DefaultPipelineRunner {
    #[tracing::instrument(level = "info", skip_all)]
    async fn run(
        &self,
        spec: &PipelineSpec,
        inputs: Value,
        identity: &AgentIdentity,
    ) -> Result<PipelineRun> {
        if spec.id.trim().is_empty() {
            return Err(Error::InvalidInput("pipeline id is empty".to_string()));
        }
        if spec.org_id.trim().is_empty() {
            return Err(Error::InvalidInput("pipeline org_id is empty".to_string()));
        }
        let order = topo_sort(&spec.steps)?;

        let run_id = ulid::Ulid::new().to_string();
        let started_at = Utc::now();
        let run = PipelineRun {
            id: run_id.clone(),
            pipeline_id: spec.id.clone(),
            status: PipelineStatus::Queued,
            step_results: BTreeMap::new(),
            started_at,
            completed_at: None,
        };

        let state = PipelineRunState {
            spec: spec.clone(),
            inputs,
            order,
            approved: HashSet::new(),
            run,
        };

        self.runs
            .insert(run_id.clone(), tokio::sync::Mutex::new(state));

        // Execute synchronously until completion or approval gate.
        let lock = self
            .runs
            .get(&run_id)
            .ok_or_else(|| Error::BackendMessage("pipeline run missing".to_string()))?;
        let mut st = lock.value().lock().await;
        self.execute_until_blocked(&mut st, identity).await?;
        Ok(st.run.clone())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn get_run(&self, run_id: &str) -> Result<Option<PipelineRun>> {
        let Some(lock) = self.runs.get(run_id) else {
            return Ok(None);
        };
        let st = lock.value().lock().await;
        Ok(Some(st.run.clone()))
    }

    #[tracing::instrument(level = "info", skip_all)]
    async fn approve_step(
        &self,
        run_id: &str,
        step_id: &str,
        identity: &AgentIdentity,
    ) -> Result<()> {
        let lock = self
            .runs
            .get(run_id)
            .ok_or_else(|| Error::NotFound(format!("pipeline run not found: {run_id}")))?;
        let mut st = lock.value().lock().await;

        match &st.run.status {
            PipelineStatus::WaitingApproval(waiting) if waiting == step_id => {}
            PipelineStatus::WaitingApproval(waiting) => {
                return Err(Error::InvalidInput(format!(
                    "pipeline run waiting for approval of {waiting}, not {step_id}"
                )));
            }
            other => {
                return Err(Error::InvalidInput(format!(
                    "pipeline run not waiting approval (status={other:?})"
                )));
            }
        }

        st.approved.insert(step_id.to_string());
        self.emit(
            &st.spec.org_id,
            "pipeline.step.approved",
            serde_json::json!({
                "run_id": st.run.id,
                "pipeline_id": st.run.pipeline_id,
                "step_id": step_id,
            }),
            format!("pipeline_step_approved:{}:{}", st.run.id, step_id),
        )
        .await;

        self.execute_until_blocked(&mut st, identity).await?;
        Ok(())
    }

    #[tracing::instrument(level = "info", skip_all)]
    async fn cancel(&self, run_id: &str) -> Result<()> {
        let lock = self
            .runs
            .get(run_id)
            .ok_or_else(|| Error::NotFound(format!("pipeline run not found: {run_id}")))?;
        let mut st = lock.value().lock().await;
        st.run.status = PipelineStatus::Failed;
        st.run.completed_at = Some(Utc::now());
        self.emit(
            &st.spec.org_id,
            "pipeline.run.cancelled",
            serde_json::json!({
                "run_id": st.run.id,
                "pipeline_id": st.run.pipeline_id,
            }),
            format!("pipeline_run_cancelled:{}", st.run.id),
        )
        .await;
        Ok(())
    }
}

fn topo_sort(steps: &[PipelineStep]) -> Result<Vec<String>> {
    let mut indegree: HashMap<String, usize> = HashMap::new();
    let mut edges: HashMap<String, Vec<String>> = HashMap::new();
    let mut ids = HashSet::new();

    for s in steps {
        if s.id.trim().is_empty() {
            return Err(Error::InvalidInput("pipeline step id is empty".to_string()));
        }
        if !ids.insert(s.id.clone()) {
            return Err(Error::InvalidInput(format!(
                "duplicate pipeline step id: {}",
                s.id
            )));
        }
        indegree.insert(s.id.clone(), 0);
        edges.entry(s.id.clone()).or_default();
    }

    for s in steps {
        for dep in &s.depends_on {
            if !ids.contains(dep) {
                return Err(Error::InvalidInput(format!(
                    "step {} depends on unknown step {}",
                    s.id, dep
                )));
            }
            *indegree.get_mut(&s.id).unwrap() += 1;
            edges.entry(dep.clone()).or_default().push(s.id.clone());
        }
    }

    let mut q: VecDeque<String> = indegree
        .iter()
        .filter_map(|(k, &v)| if v == 0 { Some(k.clone()) } else { None })
        .collect();
    let mut out = Vec::with_capacity(steps.len());
    while let Some(n) = q.pop_front() {
        out.push(n.clone());
        if let Some(children) = edges.get(&n) {
            for c in children {
                let d = indegree.get_mut(c).unwrap();
                *d -= 1;
                if *d == 0 {
                    q.push_back(c.clone());
                }
            }
        }
    }

    if out.len() != steps.len() {
        return Err(Error::InvalidInput(
            "pipeline steps contain a cycle".to_string(),
        ));
    }

    Ok(out)
}

fn build_template_context(inputs: &Value, step_results: &BTreeMap<String, StepResult>) -> Value {
    let mut steps_obj = serde_json::Map::new();
    for (id, r) in step_results {
        steps_obj.insert(
            id.clone(),
            serde_json::json!({
                "output": r.output,
                "status": r.status,
            }),
        );
    }
    serde_json::json!({
        "inputs": inputs,
        "steps": Value::Object(steps_obj),
    })
}

fn resolve_templates(v: Value, ctx: &Value) -> Result<Value> {
    match v {
        Value::String(s) => resolve_string_template(&s, ctx),
        Value::Array(arr) => {
            let mut out = Vec::with_capacity(arr.len());
            for item in arr {
                out.push(resolve_templates(item, ctx)?);
            }
            Ok(Value::Array(out))
        }
        Value::Object(map) => {
            let mut out = serde_json::Map::new();
            for (k, val) in map {
                out.insert(k, resolve_templates(val, ctx)?);
            }
            Ok(Value::Object(out))
        }
        other => Ok(other),
    }
}

fn resolve_string_template(s: &str, ctx: &Value) -> Result<Value> {
    // Fast path: exact token replacement.
    if let Some(inner) = s.strip_prefix("{{").and_then(|x| x.strip_suffix("}}")) {
        let path = inner.trim();
        if let Some(v) = lookup_path(ctx, path) {
            return Ok(v.clone());
        }
    }

    // Slow path: interpolate multiple tokens into a string.
    let mut out = s.to_string();
    let re = regex::Regex::new(r"\{\{\s*([^\}]+?)\s*\}\}")
        .map_err(|e| Error::BackendMessage(format!("template regex error: {e}")))?;
    for cap in re.captures_iter(s) {
        let whole = cap.get(0).unwrap().as_str();
        let path = cap.get(1).unwrap().as_str().trim();
        if let Some(v) = lookup_path(ctx, path) {
            let rep = match v {
                Value::String(x) => x.clone(),
                other => other.to_string(),
            };
            out = out.replace(whole, &rep);
        }
    }
    Ok(Value::String(out))
}

fn lookup_path<'a>(root: &'a Value, path: &str) -> Option<&'a Value> {
    let mut cur = root;
    for part in path.split('.') {
        if part.is_empty() {
            continue;
        }
        match cur {
            Value::Object(m) => {
                cur = m.get(part)?;
            }
            _ => return None,
        }
    }
    Some(cur)
}
