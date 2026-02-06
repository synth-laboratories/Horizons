use crate::error::ApiError;
use crate::extract::{OrgIdHeader, ProjectIdHeader};
use crate::server::AppState;
use axum::Extension;
use axum::Json;
use axum::routing::post;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

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
