use serde_json::Value;
use std::collections::HashMap;

use crate::error::GraphError;
use crate::ir::GraphIr;

#[derive(Clone, Debug)]
pub struct GraphDefinition {
    pub name: Option<String>,
    pub metadata: Option<Value>,
    pub nodes: HashMap<String, NodeDefinition>,
    pub control_edges: HashMap<String, Vec<ControlEdge>>,
    pub start_nodes: Vec<String>,
    pub end_nodes: Vec<String>,
}

#[derive(Clone, Debug)]
pub struct NodeDefinition {
    pub name: String,
    pub node_type: String,
    pub input_mapping: Option<Value>,
    pub output_mapping: Option<Value>,
    pub implementation: Option<Value>,
    pub inner_node: Option<Box<NodeDefinition>>,
    pub map_config: Option<MapConfig>,
    pub reduce_config: Option<ReduceConfig>,
}

#[derive(Clone, Debug)]
pub struct ControlEdge {
    pub target: String,
    pub condition: Option<Value>,
}

#[derive(Clone, Debug)]
pub struct MapConfig {
    pub item_key: String,
    pub state_keys: Option<Vec<String>>,
    pub max_concurrent: usize,
    pub item_timeout_ms: u64,
}

#[derive(Clone, Debug)]
pub struct ReduceConfig {
    pub reduce_strategy: Option<String>,
    pub reduce_fn: Option<String>,
    pub value_key: Option<String>,
    pub results_key: Option<String>,
    pub min_success: f64,
    pub require_all_success: bool,
}

impl GraphDefinition {
    pub fn from_ir(graph_ir: &GraphIr) -> Result<Self, GraphError> {
        if let Some(value) = &graph_ir.graph {
            return GraphDefinition::from_value(value);
        }
        Err(GraphError::bad_request(
            "graph execution requires graph definition in graph field",
        ))
    }

    pub fn from_value(value: &Value) -> Result<Self, GraphError> {
        let obj = value
            .as_object()
            .ok_or_else(|| GraphError::bad_request("graph definition must be an object"))?;
        let name = obj.get("name").and_then(|v| v.as_str()).map(|v| v.to_string());
        let metadata = obj.get("metadata").cloned();
        let nodes_value = obj
            .get("nodes")
            .and_then(|v| v.as_object())
            .ok_or_else(|| GraphError::bad_request("graph definition missing nodes object"))?;
        let mut nodes = HashMap::new();
        for (key, node_value) in nodes_value {
            let definition = parse_node_definition(key, node_value)?;
            nodes.insert(key.clone(), definition);
        }
        let control_edges = parse_control_edges(obj.get("control_edges"))?;
        let start_nodes = parse_string_list(obj.get("start_nodes"));
        let end_nodes = parse_string_list(obj.get("end_nodes"));

        Ok(GraphDefinition {
            name,
            metadata,
            nodes,
            control_edges,
            start_nodes,
            end_nodes,
        })
    }
}

fn parse_node_definition(
    node_key: &str,
    node_value: &Value,
) -> Result<NodeDefinition, GraphError> {
    let node_obj = node_value
        .as_object()
        .ok_or_else(|| GraphError::bad_request(format!("node '{node_key}' must be an object")))?;
    let name = node_obj.get("name").and_then(|v| v.as_str()).unwrap_or(node_key).to_string();
    let node_type = node_obj.get("type").and_then(|v| v.as_str()).unwrap_or("DagNode").to_string();
    let input_mapping = node_obj.get("input_mapping").cloned();
    let output_mapping = node_obj.get("output_mapping").cloned();
    let implementation = node_obj.get("implementation").cloned();

    let normalized_type = node_type.to_lowercase();
    let mut inner_node = None;
    let mut map_config = None;
    let mut reduce_config = None;

    if normalized_type == "mapnode" || normalized_type == "map_node" {
        let inner_value = node_obj.get("inner_node").ok_or_else(|| {
            GraphError::bad_request(format!("MapNode '{name}' requires inner_node"))
        })?;
        let default_inner_name = format!("{name}_inner");
        let inner_name =
            inner_value.get("name").and_then(|v| v.as_str()).unwrap_or(&default_inner_name);
        let mut inner_def = parse_node_definition(inner_name, inner_value)?;
        inner_def.name = inner_name.to_string();
        inner_node = Some(Box::new(inner_def));

        let state_keys = node_obj.get("state_keys").and_then(|v| v.as_array()).map(|items| {
            items
                .iter()
                .filter_map(|item| item.as_str().map(|v| v.to_string()))
                .collect::<Vec<String>>()
        });
        let max_concurrent =
            node_obj.get("max_concurrent").and_then(|v| v.as_u64()).unwrap_or(10) as usize;
        let item_timeout_ms =
            node_obj.get("item_timeout_ms").and_then(|v| v.as_u64()).unwrap_or(60000);
        let item_key =
            node_obj.get("item_key").and_then(|v| v.as_str()).unwrap_or("item").to_string();
        map_config = Some(MapConfig {
            item_key,
            state_keys,
            max_concurrent,
            item_timeout_ms,
        });
    } else if normalized_type == "reducenode" || normalized_type == "reduce_node" {
        if let Some(inner_value) = node_obj.get("inner_node") {
            let default_inner_name = format!("{name}_inner");
            let inner_name =
                inner_value.get("name").and_then(|v| v.as_str()).unwrap_or(&default_inner_name);
            let mut inner_def = parse_node_definition(inner_name, inner_value)?;
            inner_def.name = inner_name.to_string();
            inner_node = Some(Box::new(inner_def));
        }
        let reduce_strategy =
            node_obj.get("reduce_strategy").and_then(|v| v.as_str()).map(|v| v.to_string());
        let reduce_fn = node_obj.get("reduce_fn").and_then(|v| v.as_str()).map(|v| v.to_string());
        let value_key = node_obj.get("value_key").and_then(|v| v.as_str()).map(|v| v.to_string());
        let results_key =
            node_obj.get("results_key").and_then(|v| v.as_str()).map(|v| v.to_string());
        let min_success = node_obj.get("min_success").and_then(|v| v.as_f64()).unwrap_or(0.0);
        let require_all_success =
            node_obj.get("require_all_success").and_then(|v| v.as_bool()).unwrap_or(false);
        reduce_config = Some(ReduceConfig {
            reduce_strategy,
            reduce_fn,
            value_key,
            results_key,
            min_success,
            require_all_success,
        });
    }

    Ok(NodeDefinition {
        name,
        node_type,
        input_mapping,
        output_mapping,
        implementation,
        inner_node,
        map_config,
        reduce_config,
    })
}

fn parse_control_edges(
    value: Option<&Value>,
) -> Result<HashMap<String, Vec<ControlEdge>>, GraphError> {
    let mut result = HashMap::new();
    let Some(value) = value else {
        return Ok(result);
    };
    let map = value
        .as_object()
        .ok_or_else(|| GraphError::bad_request("control_edges must be an object if provided"))?;
    for (source, edges_value) in map {
        let edges = edges_value.as_array().ok_or_else(|| {
            GraphError::bad_request(format!("control_edges.{source} must be a list"))
        })?;
        let mut parsed_edges = Vec::new();
        for edge_value in edges {
            let edge_obj = edge_value.as_object().ok_or_else(|| {
                GraphError::bad_request(format!("control_edges.{source} entries must be objects"))
            })?;
            let target = edge_obj.get("target").and_then(|v| v.as_str()).ok_or_else(|| {
                GraphError::bad_request(format!("control_edges.{source} entries require target"))
            })?;
            let condition = edge_obj.get("condition").cloned();
            parsed_edges.push(ControlEdge {
                target: target.to_string(),
                condition,
            });
        }
        result.insert(source.clone(), parsed_edges);
    }
    Ok(result)
}

fn parse_string_list(value: Option<&Value>) -> Vec<String> {
    value
        .and_then(|v| v.as_array())
        .map(|items| {
            items
                .iter()
                .filter_map(|item| item.as_str().map(|v| v.to_string()))
                .collect::<Vec<String>>()
        })
        .unwrap_or_default()
}
