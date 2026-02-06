use crate::error::ApiError;
use crate::extract::{OrgIdHeader, ProjectIdHeader};
use crate::server::AppState;
use axum::Extension;
use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use horizons_graph::ir::{
    canonicalize_graph_ir, normalize_graph_ir, validate_graph_ir, GraphIr, Strictness,
    GRAPH_IR_SCHEMA_VERSION,
};
use horizons_graph::registry as graph_registry;

#[derive(Debug, Deserialize)]
pub struct GraphQueryRequest {
    pub cypher: String,
    #[serde(default)]
    pub params: serde_json::Value,
}

#[derive(Debug, Serialize)]
pub struct GraphQueryResponse {
    pub results: Vec<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
pub struct UpsertEdge {
    pub from: String,
    pub to: String,
    pub rel: String,
    #[serde(default)]
    pub props: serde_json::Value,
}

#[derive(Debug, Deserialize)]
pub struct GraphUpsertRequest {
    pub label: String,
    /// Stable identity for idempotent upsert (e.g. {"external_id": "..."}).
    #[serde(default)]
    pub identity: serde_json::Value,
    #[serde(default)]
    pub props: serde_json::Value,
    #[serde(default)]
    pub edges: Vec<UpsertEdge>,
}

#[derive(Debug, Serialize)]
pub struct GraphUpsertResponse {
    pub node_id: String,
}

#[tracing::instrument(level = "debug", skip_all)]
pub fn router() -> axum::Router {
    axum::Router::new()
        .route("/graph/query", post(query))
        .route("/graph/upsert", post(upsert))
        .route("/graph/validate", post(validate))
        .route("/graph/normalize", post(normalize))
        .route("/graph/execute", post(execute))
        .route("/graph/registry", axum::routing::get(list_registry))
        .route("/graph/registry/{graph_id}", axum::routing::get(get_registry_yaml))
}

#[tracing::instrument(level = "debug", skip_all)]
pub async fn query(
    OrgIdHeader(org_id): OrgIdHeader,
    ProjectIdHeader(project_id): ProjectIdHeader,
    Extension(state): Extension<Arc<AppState>>,
    Json(req): Json<GraphQueryRequest>,
) -> Result<Json<GraphQueryResponse>, ApiError> {
    let results = state
        .graph_store
        .query(org_id, project_id, &req.cypher, req.params)
        .await?;
    Ok(Json(GraphQueryResponse { results }))
}

#[tracing::instrument(level = "debug", skip_all)]
pub async fn upsert(
    OrgIdHeader(org_id): OrgIdHeader,
    ProjectIdHeader(project_id): ProjectIdHeader,
    Extension(state): Extension<Arc<AppState>>,
    Json(req): Json<GraphUpsertRequest>,
) -> Result<Json<GraphUpsertResponse>, ApiError> {
    let node_id = state
        .graph_store
        .upsert_node(org_id, project_id, &req.label, req.identity, req.props)
        .await?;

    for e in req.edges {
        state
            .graph_store
            .upsert_edge(org_id, project_id, &e.from, &e.to, &e.rel, e.props)
            .await?;
    }

    Ok(Json(GraphUpsertResponse { node_id }))
}

// ---------------------------------------------------------------------------
// Graph Engine Routes (horizons_graph)
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct GraphValidateRequest {
    pub graph_id: Option<String>,
    pub graph_ir: Option<GraphIr>,
    pub graph_yaml: Option<String>,
    #[serde(default)]
    pub strictness: Option<Strictness>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct GraphValidateResponse {
    pub ok: bool,
    pub diagnostics: Vec<horizons_graph::ir::Diagnostic>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct GraphNormalizeRequest {
    pub graph_id: Option<String>,
    pub graph_ir: Option<GraphIr>,
    pub graph_yaml: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct GraphNormalizeResponse {
    pub graph_ir_canonical: GraphIr,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct GraphExecuteRequest {
    pub graph_id: Option<String>,
    pub graph_ir: Option<GraphIr>,
    pub graph_yaml: Option<String>,
    #[serde(default)]
    pub inputs: serde_json::Value,
    #[serde(default)]
    pub run_config: Option<horizons_graph::run::RunConfig>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct GraphExecuteResponse {
    pub output: serde_json::Value,
    pub usage_summary: serde_json::Value,
    pub validation_result: Option<serde_json::Value>,
    pub execution_trace: serde_json::Value,
    pub graph_hash: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct GraphRegistryResponse {
    pub graph_ids: Vec<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
struct GraphErrorBody {
    error: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    diagnostics: Option<Vec<horizons_graph::ir::Diagnostic>>,
}

fn graph_error_to_response(err: horizons_graph::GraphError) -> Response {
    match err {
        horizons_graph::GraphError::BadRequest(msg) => (
            StatusCode::BAD_REQUEST,
            Json(GraphErrorBody {
                error: msg,
                diagnostics: None,
            }),
        )
            .into_response(),
        horizons_graph::GraphError::InvalidGraph { diagnostics } => (
            StatusCode::BAD_REQUEST,
            Json(GraphErrorBody {
                error: "invalid graph".to_string(),
                diagnostics: Some(diagnostics),
            }),
        )
            .into_response(),
        horizons_graph::GraphError::Unavailable(msg) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(GraphErrorBody {
                error: msg,
                diagnostics: None,
            }),
        )
            .into_response(),
        horizons_graph::GraphError::Internal(msg) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(GraphErrorBody {
                error: msg,
                diagnostics: None,
            }),
        )
            .into_response(),
    }
}

fn resolve_graph_ir(
    graph_id: Option<&str>,
    graph_ir: Option<GraphIr>,
    graph_yaml: Option<&str>,
) -> std::result::Result<GraphIr, Response> {
    let count = graph_id.is_some() as u8 + graph_ir.is_some() as u8 + graph_yaml.is_some() as u8;
    if count != 1 {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(GraphErrorBody {
                error: "must provide exactly one of graph_id, graph_ir, or graph_yaml".to_string(),
                diagnostics: None,
            }),
        )
            .into_response());
    }

    if let Some(ir) = graph_ir {
        return Ok(ir);
    }

    let yaml = if let Some(yaml) = graph_yaml {
        yaml.to_string()
    } else {
        let id = graph_id.unwrap_or_default();
        graph_registry::get_builtin_graph_yaml(id).ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(GraphErrorBody {
                    error: format!("unknown built-in graph_id '{id}'"),
                    diagnostics: None,
                }),
            )
                .into_response()
        })?.to_string()
    };

    let graph_value: serde_json::Value = serde_yaml::from_str(&yaml).map_err(|e| {
        (
            StatusCode::BAD_REQUEST,
            Json(GraphErrorBody {
                error: format!("invalid graph_yaml: {e}"),
                diagnostics: None,
            }),
        )
            .into_response()
    })?;

    Ok(GraphIr {
        schema_version: GRAPH_IR_SCHEMA_VERSION.to_string(),
        graph: Some(graph_value),
        metadata: None,
        nodes: Vec::new(),
        edges: Vec::new(),
        entrypoints: Vec::new(),
    })
}

#[tracing::instrument(level = "debug", skip_all)]
pub async fn validate(
    OrgIdHeader(_org_id): OrgIdHeader,
    ProjectIdHeader(_project_id): ProjectIdHeader,
    Extension(_state): Extension<Arc<AppState>>,
    Json(req): Json<GraphValidateRequest>,
) -> std::result::Result<Json<GraphValidateResponse>, Response> {
    let graph_ir = resolve_graph_ir(req.graph_id.as_deref(), req.graph_ir, req.graph_yaml.as_deref())?;
    let normalized = normalize_graph_ir(&graph_ir);
    let strictness = req.strictness.unwrap_or(Strictness::Strict);
    let validation = validate_graph_ir(&normalized, strictness);
    Ok(Json(GraphValidateResponse {
        ok: validation.ok(),
        diagnostics: validation.diagnostics,
    }))
}

#[tracing::instrument(level = "debug", skip_all)]
pub async fn normalize(
    OrgIdHeader(_org_id): OrgIdHeader,
    ProjectIdHeader(_project_id): ProjectIdHeader,
    Extension(_state): Extension<Arc<AppState>>,
    Json(req): Json<GraphNormalizeRequest>,
) -> std::result::Result<Json<GraphNormalizeResponse>, Response> {
    let graph_ir = resolve_graph_ir(req.graph_id.as_deref(), req.graph_ir, req.graph_yaml.as_deref())?;
    let normalized = normalize_graph_ir(&graph_ir);
    let canonical = canonicalize_graph_ir(&normalized);
    Ok(Json(GraphNormalizeResponse {
        graph_ir_canonical: canonical,
    }))
}

#[tracing::instrument(level = "info", skip_all)]
pub async fn execute(
    OrgIdHeader(org_id): OrgIdHeader,
    ProjectIdHeader(project_id): ProjectIdHeader,
    Extension(state): Extension<Arc<AppState>>,
    Json(req): Json<GraphExecuteRequest>,
) -> std::result::Result<Json<GraphExecuteResponse>, Response> {
    let graph_ir = resolve_graph_ir(req.graph_id.as_deref(), req.graph_ir, req.graph_yaml.as_deref())?;

    // Include project scoping in graph inputs so context-ingestion pipelines and
    // remote tool executors can reliably key work to the right org/project.
    let mut inputs = req.inputs;
    let horizon_ctx = serde_json::json!({
        "org_id": org_id.to_string(),
        "project_id": project_id.as_ref().map(|p| p.to_string()),
    });
    match inputs.as_object_mut() {
        Some(map) => {
            map.entry("_horizons".to_string())
                .or_insert(horizon_ctx);
        }
        None => {
            inputs = serde_json::json!({
                "input": inputs,
                "_horizons": horizon_ctx,
            });
        }
    }

    let run = state
        .graph_engine
        .execute_graph_ir(&graph_ir, inputs, req.run_config)
        .await
        .map_err(graph_error_to_response)?;

    Ok(Json(GraphExecuteResponse {
        output: run.output,
        usage_summary: run.usage_summary,
        validation_result: run.validation_result,
        execution_trace: run.execution_trace,
        graph_hash: run.graph_hash,
    }))
}

#[tracing::instrument(level = "debug", skip_all)]
pub async fn list_registry(
    OrgIdHeader(_org_id): OrgIdHeader,
    ProjectIdHeader(_project_id): ProjectIdHeader,
    Extension(_state): Extension<Arc<AppState>>,
) -> Result<Json<GraphRegistryResponse>, ApiError> {
    let ids = graph_registry::list_builtin_graphs()
        .into_iter()
        .map(|s| s.to_string())
        .collect::<Vec<_>>();
    Ok(Json(GraphRegistryResponse { graph_ids: ids }))
}

#[tracing::instrument(level = "debug", skip_all)]
pub async fn get_registry_yaml(
    OrgIdHeader(_org_id): OrgIdHeader,
    ProjectIdHeader(_project_id): ProjectIdHeader,
    Extension(_state): Extension<Arc<AppState>>,
    axum::extract::Path(graph_id): axum::extract::Path<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let yaml = graph_registry::get_builtin_graph_yaml(&graph_id).ok_or_else(|| {
        ApiError::Core(horizons_core::Error::NotFound(format!(
            "unknown built-in graph_id '{graph_id}'"
        )))
    })?;
    Ok(Json(serde_json::json!({ "graph_id": graph_id, "yaml": yaml })))
}
