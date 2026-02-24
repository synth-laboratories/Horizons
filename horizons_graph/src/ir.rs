use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};

pub const GRAPH_IR_SCHEMA_VERSION: &str = "v1";

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct GraphIr {
    pub schema_version: String,
    #[serde(default)]
    pub graph: Option<Value>,
    #[serde(default)]
    pub metadata: Option<GraphMetadata>,
    #[serde(default)]
    pub nodes: Vec<GraphNode>,
    #[serde(default)]
    pub edges: Vec<GraphEdge>,
    #[serde(default)]
    pub entrypoints: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct GraphMetadata {
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub graph_type: Option<String>,
    #[serde(default)]
    pub tags: Vec<String>,
    #[serde(default)]
    pub capabilities: Vec<String>,
    #[serde(default)]
    pub input_schema: Option<Value>,
    #[serde(default)]
    pub output_schema: Option<Value>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct GraphNode {
    pub id: String,
    #[serde(rename = "type")]
    pub node_type: String,
    #[serde(default)]
    pub config: Option<Value>,
    #[serde(default)]
    pub inputs: Vec<String>,
    #[serde(default)]
    pub outputs: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct GraphEdge {
    pub source: String,
    pub target: String,
    #[serde(default)]
    pub output: Option<String>,
    #[serde(default)]
    pub input: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct Diagnostic {
    pub severity: DiagnosticSeverity,
    pub message: String,
    #[serde(default)]
    pub path: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DiagnosticSeverity {
    Error,
    Warning,
    Info,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
#[derive(Default)]
pub enum Strictness {
    #[default]
    Strict,
    Lenient,
}

#[derive(Clone, Debug)]
pub struct ValidationResult {
    pub diagnostics: Vec<Diagnostic>,
}

impl ValidationResult {
    pub fn ok(&self) -> bool {
        !self
            .diagnostics
            .iter()
            .any(|diag| diag.severity == DiagnosticSeverity::Error)
    }
}

pub fn validate_graph_ir(graph: &GraphIr, strictness: Strictness) -> ValidationResult {
    let mut diagnostics = Vec::new();

    if graph.schema_version.trim().is_empty() {
        push_error(
            &mut diagnostics,
            "schema_version is required",
            Some("schema_version".to_string()),
        );
    } else if graph.schema_version != GRAPH_IR_SCHEMA_VERSION {
        diagnostics.push(Diagnostic {
            severity: if matches!(strictness, Strictness::Strict) {
                DiagnosticSeverity::Error
            } else {
                DiagnosticSeverity::Warning
            },
            message: format!(
                "unexpected schema_version '{}', expected '{}'",
                graph.schema_version, GRAPH_IR_SCHEMA_VERSION
            ),
            path: Some("schema_version".to_string()),
        });
    }

    if let Some(graph_value) = &graph.graph {
        if !graph.nodes.is_empty()
            || !graph.edges.is_empty()
            || !graph.entrypoints.is_empty()
            || graph.metadata.is_some()
        {
            push_error(
                &mut diagnostics,
                "graph field is mutually exclusive with nodes/edges/entrypoints/metadata",
                Some("graph".to_string()),
            );
        }
        diagnostics.extend(validate_graph_definition_value(graph_value, strictness));
    } else {
        diagnostics.extend(validate_canonical_graph_ir(graph, strictness));
    }

    ValidationResult { diagnostics }
}

pub fn normalize_graph_ir(graph: &GraphIr) -> GraphIr {
    let mut normalized = graph.clone();
    if let Some(graph_value) = &graph.graph {
        normalized.graph = Some(normalize_graph_definition_value(graph_value));
    }
    normalized
}

pub fn canonicalize_graph_ir(graph: &GraphIr) -> GraphIr {
    let mut canonical = graph.clone();
    if let Some(graph_value) = &graph.graph {
        canonical.graph = Some(canonicalize_graph_definition_value(graph_value));
    } else {
        canonical.nodes.sort_by(|a, b| a.id.cmp(&b.id));
        canonical.edges.sort_by(|a, b| {
            a.source
                .cmp(&b.source)
                .then_with(|| a.target.cmp(&b.target))
                .then_with(|| a.output.cmp(&b.output))
                .then_with(|| a.input.cmp(&b.input))
        });
        canonical.entrypoints.sort();
    }
    canonical
}

pub fn hash_graph_ir(graph: &GraphIr) -> Result<String, serde_json::Error> {
    let payload = serde_json::to_vec(graph)?;
    let digest = Sha256::digest(&payload);
    Ok(hex::encode(digest))
}

fn validate_canonical_graph_ir(graph: &GraphIr, strictness: Strictness) -> Vec<Diagnostic> {
    let mut diagnostics = Vec::new();

    if graph.nodes.is_empty() {
        push_error(
            &mut diagnostics,
            "graph must define at least one node",
            Some("nodes".to_string()),
        );
    }

    let mut seen = HashSet::new();
    for (idx, node) in graph.nodes.iter().enumerate() {
        let path = format!("nodes[{idx}].id");
        if node.id.trim().is_empty() {
            push_error(&mut diagnostics, "node id is required", Some(path));
            continue;
        }
        if !seen.insert(node.id.as_str()) {
            push_error(
                &mut diagnostics,
                format!("duplicate node id '{}'", node.id),
                Some(path),
            );
        }
        if node.node_type.trim().is_empty() {
            push_error(
                &mut diagnostics,
                format!("node '{}' is missing type", node.id),
                Some(format!("nodes[{idx}].type")),
            );
        }
    }

    for (idx, edge) in graph.edges.iter().enumerate() {
        if !seen.contains(edge.source.as_str()) {
            push_error(
                &mut diagnostics,
                format!("edge source '{}' not found", edge.source),
                Some(format!("edges[{idx}].source")),
            );
        }
        if !seen.contains(edge.target.as_str()) {
            push_error(
                &mut diagnostics,
                format!("edge target '{}' not found", edge.target),
                Some(format!("edges[{idx}].target")),
            );
        }
    }

    if !graph.entrypoints.is_empty() {
        for (idx, entry) in graph.entrypoints.iter().enumerate() {
            if !seen.contains(entry.as_str()) {
                push_error(
                    &mut diagnostics,
                    format!("entrypoint '{}' not found", entry),
                    Some(format!("entrypoints[{idx}]")),
                );
            }
        }
    }

    if strictness == Strictness::Lenient {
        diagnostics.retain(|diag| diag.severity != DiagnosticSeverity::Info);
    }

    diagnostics
}

fn validate_graph_definition_value(value: &Value, strictness: Strictness) -> Vec<Diagnostic> {
    let mut diagnostics = Vec::new();
    let Some(obj) = value.as_object() else {
        push_error(
            &mut diagnostics,
            "graph must be an object",
            Some("graph".to_string()),
        );
        return diagnostics;
    };

    let graph_type = graph_type_from_value(obj);
    let is_verifier = graph_type.as_deref().is_some_and(is_verifier_graph);
    let is_dag = graph_type.as_deref().is_some_and(is_dag_graph);

    let mut allowed_keys = HashSet::from([
        "nodes",
        "control_edges",
        "start_nodes",
        "end_nodes",
        "metadata",
        "state_update_hooks",
        "input_mapping",
        "tools",
        "graph_system_id",
        "termination_policies",
        "name",
        "type",
        "streaming_config",
        "system_name",
    ]);
    if is_verifier {
        allowed_keys.insert("aggregation_policy");
        allowed_keys.insert("verdict_weights");
    }

    if !obj.contains_key("nodes") {
        push_error(
            &mut diagnostics,
            "Missing required top-level keys: ['nodes']",
            Some("nodes".to_string()),
        );
    }

    let unknown_keys: Vec<String> = obj
        .keys()
        .filter(|key| !allowed_keys.contains(key.as_str()))
        .cloned()
        .collect();
    if !unknown_keys.is_empty() {
        push_error(
            &mut diagnostics,
            format!("Unknown top-level keys found: {unknown_keys:?}"),
            None,
        );
    }

    if let Some(nodes_value) = obj.get("nodes") {
        if !nodes_value.is_object() && !nodes_value.is_array() {
            push_error(
                &mut diagnostics,
                "'nodes' key must be a dictionary or a list.",
                Some("nodes".to_string()),
            );
        }
    }
    if let Some(edges_value) = obj.get("control_edges") {
        if !edges_value.is_object() {
            push_error(
                &mut diagnostics,
                "Optional 'control_edges' key must be a dictionary if present.",
                Some("control_edges".to_string()),
            );
        }
    }
    if let Some(start_nodes) = obj.get("start_nodes") {
        if !start_nodes.is_array() {
            push_error(
                &mut diagnostics,
                "Optional 'start_nodes' key must be a list if present.",
                Some("start_nodes".to_string()),
            );
        }
    }
    if let Some(end_nodes) = obj.get("end_nodes") {
        if !end_nodes.is_null() && !end_nodes.is_array() {
            push_error(
                &mut diagnostics,
                "Optional 'end_nodes' key must be a list if present.",
                Some("end_nodes".to_string()),
            );
        }
    }
    if let Some(metadata) = obj.get("metadata") {
        if !metadata.is_null() && !metadata.is_object() {
            push_error(
                &mut diagnostics,
                "Optional 'metadata' key must be a dictionary if present.",
                Some("metadata".to_string()),
            );
        }
    }
    if let Some(state_update_hooks) = obj.get("state_update_hooks") {
        if !state_update_hooks.is_null() && !state_update_hooks.is_object() {
            push_error(
                &mut diagnostics,
                "Optional 'state_update_hooks' key must be a dictionary if present.",
                Some("state_update_hooks".to_string()),
            );
        }
    }

    let nodes_map = obj.get("nodes").and_then(|nodes| nodes.as_object());
    let control_edges = obj.get("control_edges").and_then(|edges| edges.as_object());

    if let Some(nodes) = nodes_map {
        diagnostics.extend(validate_node_definitions(nodes));
        diagnostics.extend(validate_edge_definitions(nodes, control_edges));
        diagnostics.extend(validate_start_end_nodes(nodes, obj));
        diagnostics.extend(validate_termination_policies(
            obj.get("termination_policies"),
        ));

        if is_dag {
            if let Some(control_edges) = control_edges {
                if let Some(cycle) = detect_cycle(nodes, control_edges) {
                    let message = format!("cycle detected: {cycle:?}");
                    diagnostics.push(Diagnostic {
                        severity: if matches!(strictness, Strictness::Strict) {
                            DiagnosticSeverity::Error
                        } else {
                            DiagnosticSeverity::Warning
                        },
                        message,
                        path: Some("control_edges".to_string()),
                    });
                }
            }
        }
    }

    diagnostics
}

fn validate_node_definitions(nodes: &Map<String, Value>) -> Vec<Diagnostic> {
    let mut diagnostics = Vec::new();
    let special_node_types = ["map_node", "reduce_node", "MapNode", "ReduceNode", "tool"];

    for (node_name, node_def) in nodes {
        let Some(node_obj) = node_def.as_object() else {
            push_error(
                &mut diagnostics,
                format!("Definition for node '{node_name}' must be a dictionary."),
                Some(format!("nodes.{node_name}")),
            );
            continue;
        };

        let node_type = node_obj.get("type").and_then(|value| value.as_str());
        let node_type_is_special =
            node_type.is_some_and(|value| special_node_types.contains(&value));

        if node_type_is_special {
            if matches!(node_type, Some("map_node" | "MapNode")) {
                if !node_obj.contains_key("inner_node") {
                    push_error(
                        &mut diagnostics,
                        format!("MapNode '{node_name}' is missing required 'inner_node' key."),
                        Some(format!("nodes.{node_name}.inner_node")),
                    );
                }
            } else if matches!(node_type, Some("reduce_node" | "ReduceNode")) {
                let has_strategy = node_obj.contains_key("reduce_strategy");
                let has_fn = node_obj.contains_key("reduce_fn");
                let has_inner = node_obj.contains_key("inner_node");
                if !has_strategy && !has_fn && !has_inner {
                    push_error(
                        &mut diagnostics,
                        format!(
                            "ReduceNode '{node_name}' must have 'reduce_strategy', 'reduce_fn', or 'inner_node'."
                        ),
                        Some(format!("nodes.{node_name}")),
                    );
                }
            }
        } else if !node_obj.contains_key("implementation") {
            push_error(
                &mut diagnostics,
                format!("Node '{node_name}' is missing the 'implementation' key."),
                Some(format!("nodes.{node_name}.implementation")),
            );
        } else if !node_obj
            .get("implementation")
            .is_some_and(|value| value.is_object())
        {
            push_error(
                &mut diagnostics,
                format!("'implementation' for node '{node_name}' must be a dictionary."),
                Some(format!("nodes.{node_name}.implementation")),
            );
        }
    }

    diagnostics
}

fn validate_edge_definitions(
    nodes: &Map<String, Value>,
    control_edges: Option<&Map<String, Value>>,
) -> Vec<Diagnostic> {
    let mut diagnostics = Vec::new();
    let Some(control_edges) = control_edges else {
        return diagnostics;
    };

    for (source_node, edge_list) in control_edges {
        if !nodes.contains_key(source_node) {
            push_error(
                &mut diagnostics,
                format!("Edge source '{source_node}' is not a defined node."),
                Some(format!("control_edges.{source_node}")),
            );
        }

        let Some(edges) = edge_list.as_array() else {
            push_error(
                &mut diagnostics,
                format!("Edges for source '{source_node}' must be defined as a list."),
                Some(format!("control_edges.{source_node}")),
            );
            continue;
        };

        for (idx, edge_def) in edges.iter().enumerate() {
            let Some(edge_obj) = edge_def.as_object() else {
                push_error(
                    &mut diagnostics,
                    format!(
                        "Edge definition at index {idx} for source '{source_node}' must be a dictionary."
                    ),
                    Some(format!("control_edges.{source_node}[{idx}]")),
                );
                continue;
            };

            let target_node = edge_obj.get("target").and_then(|value| value.as_str());
            if target_node.is_none() {
                push_error(
                    &mut diagnostics,
                    format!(
                        "Edge definition at index {idx} for source '{source_node}' is missing a 'target' string."
                    ),
                    Some(format!("control_edges.{source_node}[{idx}].target")),
                );
            } else if let Some(target) = target_node {
                if !nodes.contains_key(target) {
                    push_error(
                        &mut diagnostics,
                        format!(
                            "Edge target '{target}' (from source '{source_node}', index {idx}) is not a defined node."
                        ),
                        Some(format!("control_edges.{source_node}[{idx}].target")),
                    );
                }
            }

            if let Some(condition) = edge_obj.get("condition") {
                if !condition.is_string() && !condition.is_object() {
                    push_error(
                        &mut diagnostics,
                        format!(
                            "Optional 'condition' for edge at index {idx} (source '{source_node}') must be a string or dict if provided (got {}). Otherwise omit or set to None.",
                            value_type_name(condition),
                        ),
                        Some(format!("control_edges.{source_node}[{idx}].condition")),
                    );
                }
            }
        }
    }

    diagnostics
}

fn validate_start_end_nodes(
    nodes: &Map<String, Value>,
    obj: &Map<String, Value>,
) -> Vec<Diagnostic> {
    let mut diagnostics = Vec::new();

    if let Some(start_nodes) = obj.get("start_nodes").and_then(|value| value.as_array()) {
        for (idx, node_name) in start_nodes.iter().enumerate() {
            let Some(node_name_str) = node_name.as_str() else {
                push_error(
                    &mut diagnostics,
                    format!(
                        "Start node entry must be a string, got {}.",
                        value_type_name(node_name)
                    ),
                    Some(format!("start_nodes[{idx}]")),
                );
                continue;
            };
            if !nodes.contains_key(node_name_str) {
                push_error(
                    &mut diagnostics,
                    format!("Start node '{node_name_str}' is not defined in the 'nodes' section."),
                    Some(format!("start_nodes[{idx}]")),
                );
            }
        }
    }

    if let Some(end_nodes) = obj.get("end_nodes").and_then(|value| value.as_array()) {
        for (idx, node_name) in end_nodes.iter().enumerate() {
            let Some(node_name_str) = node_name.as_str() else {
                push_error(
                    &mut diagnostics,
                    format!(
                        "End node entry must be a string, got {}.",
                        value_type_name(node_name)
                    ),
                    Some(format!("end_nodes[{idx}]")),
                );
                continue;
            };
            if !nodes.contains_key(node_name_str) {
                push_error(
                    &mut diagnostics,
                    format!("End node '{node_name_str}' is not defined in the 'nodes' section."),
                    Some(format!("end_nodes[{idx}]")),
                );
            }
        }
    }

    diagnostics
}

fn validate_termination_policies(value: Option<&Value>) -> Vec<Diagnostic> {
    let mut diagnostics = Vec::new();
    let Some(value) = value else {
        return diagnostics;
    };
    let Some(policies) = value.as_array() else {
        push_error(
            &mut diagnostics,
            "Optional 'termination_policies' key must be a list.",
            Some("termination_policies".to_string()),
        );
        return diagnostics;
    };

    for (idx, policy_conf) in policies.iter().enumerate() {
        let Some(policy_obj) = policy_conf.as_object() else {
            push_error(
                &mut diagnostics,
                format!(
                    "Item {idx} in termination_policies must be a dictionary, got {}.",
                    value_type_name(policy_conf),
                ),
                Some(format!("termination_policies[{idx}]")),
            );
            continue;
        };

        let policy_type = policy_obj.get("type").and_then(|value| value.as_str());
        if policy_type.is_none() {
            push_error(
                &mut diagnostics,
                format!("Policy config at index {idx} is missing a 'type' string."),
                Some(format!("termination_policies[{idx}].type")),
            );
        }

        if let Some(priority) = policy_obj.get("priority") {
            if !priority.is_i64() {
                push_error(
                    &mut diagnostics,
                    format!(
                        "Optional 'priority' in policy config at index {idx} must be an integer."
                    ),
                    Some(format!("termination_policies[{idx}].priority")),
                );
            }
        }

        if let Some(policy_type) = policy_type {
            if policy_type.eq_ignore_ascii_case("custom") {
                let fn_str = policy_obj.get("fn_str").and_then(|value| value.as_str());
                if fn_str.is_none() {
                    push_error(
                        &mut diagnostics,
                        format!("Custom policy config at index {idx} is missing 'fn_str' string."),
                        Some(format!("termination_policies[{idx}].fn_str")),
                    );
                }
            }
        }
    }

    diagnostics
}

fn normalize_graph_definition_value(value: &Value) -> Value {
    let Some(obj) = value.as_object() else {
        return value.clone();
    };
    let mut normalized = obj.clone();

    let metadata_value = normalized
        .get("metadata")
        .and_then(|value| value.as_object().cloned())
        .unwrap_or_default();
    let mut metadata = metadata_value;

    for key in [
        "name",
        "type",
        "streaming_config",
        "system_name",
        "graph_system_id",
    ] {
        if let Some(value) = normalized.remove(key) {
            metadata.entry(key.to_string()).or_insert(value);
        }
    }

    if !metadata.contains_key("type") {
        metadata.insert(
            "type".to_string(),
            Value::String("ConditionalGraph".to_string()),
        );
    }

    normalized.insert("metadata".to_string(), Value::Object(metadata));
    Value::Object(normalized)
}

fn canonicalize_graph_definition_value(value: &Value) -> Value {
    let Some(obj) = value.as_object() else {
        return value.clone();
    };
    let mut entries: Vec<(String, Value)> = obj
        .iter()
        .map(|(key, val)| (key.clone(), canonicalize_graph_entry(key, val)))
        .collect();
    entries.sort_by(|a, b| a.0.cmp(&b.0));
    let mut map = Map::new();
    for (key, val) in entries {
        map.insert(key, val);
    }
    Value::Object(map)
}

fn canonicalize_graph_entry(key: &str, value: &Value) -> Value {
    match key {
        "control_edges" => canonicalize_control_edges(value),
        "start_nodes" | "end_nodes" => canonicalize_string_array(value),
        _ => canonicalize_value(value),
    }
}

fn canonicalize_control_edges(value: &Value) -> Value {
    let Some(obj) = value.as_object() else {
        return canonicalize_value(value);
    };
    let mut entries: Vec<(String, Value)> = Vec::new();
    for (source, edges_value) in obj {
        let canonical_edges = if let Some(edges) = edges_value.as_array() {
            let mut canonical_list: Vec<Value> = edges.iter().map(canonicalize_value).collect();
            canonical_list.sort_by_key(edge_sort_key);
            Value::Array(canonical_list)
        } else {
            canonicalize_value(edges_value)
        };
        entries.push((source.clone(), canonical_edges));
    }
    entries.sort_by(|a, b| a.0.cmp(&b.0));
    let mut map = Map::new();
    for (key, val) in entries {
        map.insert(key, val);
    }
    Value::Object(map)
}

fn canonicalize_string_array(value: &Value) -> Value {
    let Some(array) = value.as_array() else {
        return canonicalize_value(value);
    };
    if array.iter().all(|item| item.is_string()) {
        let mut items: Vec<String> = array
            .iter()
            .filter_map(|item| item.as_str().map(|s| s.to_string()))
            .collect();
        items.sort();
        Value::Array(items.into_iter().map(Value::String).collect())
    } else {
        canonicalize_value(value)
    }
}

fn canonicalize_value(value: &Value) -> Value {
    match value {
        Value::Object(obj) => {
            let mut entries: Vec<(String, Value)> = obj
                .iter()
                .map(|(key, val)| (key.clone(), canonicalize_value(val)))
                .collect();
            entries.sort_by(|a, b| a.0.cmp(&b.0));
            let mut map = Map::new();
            for (key, val) in entries {
                map.insert(key, val);
            }
            Value::Object(map)
        }
        Value::Array(arr) => Value::Array(arr.iter().map(canonicalize_value).collect()),
        _ => value.clone(),
    }
}

fn edge_sort_key(value: &Value) -> String {
    let canonical = canonicalize_value(value);
    serde_json::to_string(&canonical).unwrap_or_default()
}

fn graph_type_from_value(obj: &Map<String, Value>) -> Option<String> {
    if let Some(metadata) = obj.get("metadata").and_then(|value| value.as_object()) {
        if let Some(graph_type) = metadata.get("type").and_then(|value| value.as_str()) {
            return Some(graph_type.to_string());
        }
    }
    obj.get("type")
        .and_then(|value| value.as_str())
        .map(|value| value.to_string())
}

fn is_verifier_graph(graph_type: &str) -> bool {
    graph_type.eq_ignore_ascii_case("VerifierGraph") || graph_type.eq_ignore_ascii_case("VERIFIER")
}

fn is_dag_graph(graph_type: &str) -> bool {
    graph_type.eq_ignore_ascii_case("DAG")
}

fn detect_cycle(
    nodes: &Map<String, Value>,
    control_edges: &Map<String, Value>,
) -> Option<Vec<String>> {
    let node_keys: HashSet<String> = nodes.keys().cloned().collect();
    let mut adjacency: HashMap<String, Vec<String>> = HashMap::new();
    for (source, edges_value) in control_edges {
        let Some(edges) = edges_value.as_array() else {
            continue;
        };
        if !node_keys.contains(source) {
            continue;
        }
        for edge in edges {
            if let Some(target) = edge
                .as_object()
                .and_then(|obj| obj.get("target"))
                .and_then(|value| value.as_str())
            {
                if node_keys.contains(target) {
                    adjacency
                        .entry(source.clone())
                        .or_default()
                        .push(target.to_string());
                }
            }
        }
    }

    let mut state: HashMap<String, u8> = HashMap::new();
    let mut stack: Vec<String> = Vec::new();

    for node in &node_keys {
        if matches!(state.get(node), Some(2)) {
            continue;
        }
        if let Some(cycle) = dfs_cycle(node, &adjacency, &mut state, &mut stack) {
            return Some(cycle);
        }
    }

    None
}

fn dfs_cycle(
    node: &str,
    adjacency: &HashMap<String, Vec<String>>,
    state: &mut HashMap<String, u8>,
    stack: &mut Vec<String>,
) -> Option<Vec<String>> {
    state.insert(node.to_string(), 1);
    stack.push(node.to_string());

    if let Some(neighbors) = adjacency.get(node) {
        for neighbor in neighbors {
            match state.get(neighbor).copied() {
                Some(1) => {
                    if let Some(pos) = stack.iter().position(|item| item == neighbor) {
                        let mut cycle = stack[pos..].to_vec();
                        cycle.push(neighbor.clone());
                        return Some(cycle);
                    }
                }
                Some(2) => continue,
                _ => {
                    if let Some(cycle) = dfs_cycle(neighbor, adjacency, state, stack) {
                        return Some(cycle);
                    }
                }
            }
        }
    }

    stack.pop();
    state.insert(node.to_string(), 2);
    None
}

fn push_error(diagnostics: &mut Vec<Diagnostic>, message: impl Into<String>, path: Option<String>) {
    diagnostics.push(Diagnostic {
        severity: DiagnosticSeverity::Error,
        message: message.into(),
        path,
    });
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
