use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::HashSet;
use std::sync::Arc;

use super::schema::{ResolvedAnswerSchema, submit_answer_schema};

pub mod builtin;
pub mod python_sandbox;

pub type ToolSchema = Value;

#[derive(Clone, Debug)]
pub struct ToolRegistry {
    pub schemas: Vec<ToolSchema>,
    pub allowed_names: HashSet<String>,
}

pub fn build_tool_registry(
    filter: Option<&[String]>,
    allowed_capabilities: Option<&[String]>,
    is_verifier: bool,
    answer_schema: &Option<ResolvedAnswerSchema>,
) -> ToolRegistry {
    let mut tools = Vec::new();
    let mut allowed_names = HashSet::new();
    let filter_set: Option<HashSet<String>> = filter.map(|items| items.iter().cloned().collect());
    let allowed_caps: HashSet<String> = allowed_capabilities
        .unwrap_or(&[])
        .iter()
        .cloned()
        .collect();

    for tool in builtin::builtin_tools() {
        let name = tool
            .get("function")
            .and_then(|v| v.get("name"))
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        if let Some(filter_set) = &filter_set {
            if !filter_set.contains(&name) {
                continue;
            }
        }
        let is_control_tool = name == "give_up";
        if !allowed_caps.is_empty()
            && !is_control_tool
            && !tool_capabilities_for(&name)
                .iter()
                .all(|cap| allowed_caps.contains(*cap))
        {
            continue;
        }
        allowed_names.insert(name);
        tools.push(tool);
    }

    tools.push(submit_answer_schema(is_verifier, answer_schema));
    allowed_names.insert("submit_answer".to_string());

    ToolRegistry {
        schemas: tools,
        allowed_names,
    }
}

pub fn tool_denied_result(tool_name: &str, allowed_tool_names: &HashSet<String>) -> Value {
    let mut allowed = allowed_tool_names.iter().cloned().collect::<Vec<String>>();
    allowed.sort();
    json!({
        "success": false,
        "error": format!("Tool '{}' is not allowed for this run.", tool_name),
        "allowed_tools": allowed,
        "_local": true,
    })
}

fn tool_capabilities_for(tool_name: &str) -> &'static [&'static str] {
    match tool_name {
        "materialize_context" | "local_grep" | "local_search" | "view_lines" => &["filesystem"],
        "exec_python" => &["compute"],
        "codex_exec" => &["shell"],
        "query_lm" | "delegate_lm" => &["delegate"],
        _ => &[],
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ToolCall {
    pub id: Option<String>,
    pub name: String,
    pub arguments: Value,
}

#[derive(Clone, Debug)]
pub struct ToolContext {
    pub run_id: String,
    pub node_name: String,
    pub iteration: i64,
    pub root_calls: i64,
    pub subcalls: i64,
    pub inputs: Arc<Value>,
}

#[derive(Clone, Debug)]
pub struct ToolResult {
    pub tool_name: String,
    pub result: Value,
    pub agent_visible_payload: Value,
    pub elapsed_ms: i64,
    pub success: bool,
    pub execution_mode: Option<String>,
    pub sandbox_timing: Option<Value>,
}

#[async_trait::async_trait]
pub trait ToolExecutor: Send + Sync {
    async fn execute(
        &self,
        call: ToolCall,
        ctx: ToolContext,
    ) -> Result<ToolResult, super::ToolError>;

    async fn execute_many_concurrently(
        &self,
        calls: Vec<ToolCall>,
        ctx: ToolContext,
    ) -> Vec<Result<ToolResult, super::ToolError>>;
}
