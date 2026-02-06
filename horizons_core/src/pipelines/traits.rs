use crate::Result;
use crate::models::AgentIdentity;
use crate::pipelines::models::{PipelineRun, PipelineSpec};
use async_trait::async_trait;
use serde_json::Value;

#[async_trait]
pub trait PipelineRunner: Send + Sync {
    async fn run(
        &self,
        spec: &PipelineSpec,
        inputs: Value,
        identity: &AgentIdentity,
    ) -> Result<PipelineRun>;

    async fn get_run(&self, run_id: &str) -> Result<Option<PipelineRun>>;

    async fn approve_step(
        &self,
        run_id: &str,
        step_id: &str,
        identity: &AgentIdentity,
    ) -> Result<()>;

    async fn cancel(&self, run_id: &str) -> Result<()>;
}

#[async_trait]
pub trait Subagent: Send + Sync {
    async fn run(
        &self,
        role: &str,
        input: Value,
        tools: Vec<String>,
        context: Value,
    ) -> Result<Value>;
}

/// Graph execution adapter for pipeline steps.
///
/// This lives in `horizons_core` so the pipeline engine can remain decoupled
/// from any particular graph implementation (e.g. `horizons_graph`).
#[async_trait]
pub trait GraphRunner: Send + Sync {
    async fn run_graph(
        &self,
        graph_id: &str,
        inputs: Value,
        context: Value,
        identity: &AgentIdentity,
    ) -> Result<Value>;
}
