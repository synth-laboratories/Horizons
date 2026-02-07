use crate::admin;
use crate::auth::CentralDbApiKeyAuth;
use crate::extract::AuthProviderExt;
use crate::routes;
use axum::routing::get;
use axum::{Extension, Router};
use horizons_core::context_refresh::traits::ContextRefresh;
use horizons_core::core_agents::executor::CoreAgentsExecutor;
use horizons_core::core_agents::mcp_gateway::McpGateway;
use horizons_core::engine::sandbox_runtime::SandboxRuntime;
#[cfg(feature = "evaluation")]
use horizons_core::evaluation::engine::EvaluationEngine;
use horizons_core::events::traits::EventBus;
#[cfg(feature = "memory")]
use horizons_core::memory::traits::HorizonsMemory;
use horizons_core::onboard::secrets::CredentialManager;
use horizons_core::onboard::traits::{
    Cache, CentralDb, Filestore, GraphStore, ProjectDb, VectorStore,
};
#[cfg(all(feature = "memory", feature = "optimization", feature = "evaluation"))]
use horizons_core::optimization::continual::ContinualLearningEngine;
#[cfg(feature = "optimization")]
use horizons_core::optimization::engine::OptimizationEngine;
use horizons_core::pipelines::traits::PipelineRunner;
use horizons_graph::GraphEngine;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;
use tower_http::cors::{Any, CorsLayer};
use tower_http::request_id::{MakeRequestUuid, SetRequestIdLayer};
use tower_http::trace::TraceLayer;

#[derive(Clone)]
pub struct AppState {
    pub central_db: Arc<dyn CentralDb>,
    pub project_db: Arc<dyn ProjectDb>,
    pub filestore: Arc<dyn Filestore>,
    pub cache: Arc<dyn Cache>,
    pub graph_store: Arc<dyn GraphStore>,
    pub vector_store: Arc<dyn VectorStore>,
    pub graph_engine: Arc<GraphEngine>,
    pub event_bus: Arc<dyn EventBus>,
    pub context_refresh: Arc<dyn ContextRefresh>,
    pub core_agents: Arc<CoreAgentsExecutor>,
    pub pipelines: Arc<dyn PipelineRunner>,
    /// Optional credential manager for `$cred:` resolution and credential CRUD routes.
    pub credential_manager: Option<Arc<CredentialManager>>,
    /// MCP gateway for tool proxying (optional; configured via env).
    pub mcp_gateway: Option<Arc<McpGateway>>,
    /// Sandbox runtime for executing coding agents in containers.
    /// None if the sandbox engine is not configured.
    pub sandbox_runtime: Option<Arc<SandboxRuntime>>,
    #[cfg(feature = "memory")]
    pub memory: Arc<dyn HorizonsMemory>,
    #[cfg(feature = "optimization")]
    pub optimization: Arc<OptimizationEngine>,
    #[cfg(feature = "evaluation")]
    pub evaluation: Arc<EvaluationEngine>,
    #[cfg(all(feature = "memory", feature = "optimization", feature = "evaluation"))]
    pub continual_learning: Arc<ContinualLearningEngine>,
    pub started_at: Instant,
}

impl AppState {
    #[tracing::instrument(level = "debug", skip_all)]
    pub fn new(
        central_db: Arc<dyn CentralDb>,
        project_db: Arc<dyn ProjectDb>,
        filestore: Arc<dyn Filestore>,
        cache: Arc<dyn Cache>,
        graph_store: Arc<dyn GraphStore>,
        vector_store: Arc<dyn VectorStore>,
        graph_engine: Arc<GraphEngine>,
        event_bus: Arc<dyn EventBus>,
        context_refresh: Arc<dyn ContextRefresh>,
        core_agents: Arc<CoreAgentsExecutor>,
        pipelines: Arc<dyn PipelineRunner>,
        credential_manager: Option<Arc<CredentialManager>>,
        mcp_gateway: Option<Arc<McpGateway>>,
        sandbox_runtime: Option<Arc<SandboxRuntime>>,
        #[cfg(feature = "memory")] memory: Arc<dyn HorizonsMemory>,
        #[cfg(feature = "optimization")] optimization: Arc<OptimizationEngine>,
        #[cfg(feature = "evaluation")] evaluation: Arc<EvaluationEngine>,
        #[cfg(all(feature = "memory", feature = "optimization", feature = "evaluation"))]
        continual_learning: Arc<ContinualLearningEngine>,
    ) -> Self {
        Self {
            central_db,
            project_db,
            filestore,
            cache,
            graph_store,
            vector_store,
            graph_engine,
            event_bus,
            context_refresh,
            core_agents,
            pipelines,
            credential_manager,
            mcp_gateway,
            sandbox_runtime,
            #[cfg(feature = "memory")]
            memory,
            #[cfg(feature = "optimization")]
            optimization,
            #[cfg(feature = "evaluation")]
            evaluation,
            #[cfg(all(feature = "memory", feature = "optimization", feature = "evaluation"))]
            continual_learning,
            started_at: Instant::now(),
        }
    }
}

#[tracing::instrument(level = "debug", skip_all)]
pub fn router(state: AppState) -> Router {
    let state = Arc::new(state);
    // Provide Bearer-token auth support via `AuthenticatedIdentity` extractor.
    // Enforcement is controlled by env vars in `extract.rs` (dev headers remain default).
    let auth_provider =
        AuthProviderExt(Arc::new(CentralDbApiKeyAuth::new(state.central_db.clone())));
    Router::new()
        .merge(routes::router())
        .merge(admin::router())
        .route("/health", get(routes::health::get_health))
        .layer(Extension(state))
        .layer(Extension(auth_provider))
        .layer(SetRequestIdLayer::new(
            axum::http::HeaderName::from_static("x-request-id"),
            MakeRequestUuid,
        ))
        .layer(TraceLayer::new_for_http())
        .layer(
            CorsLayer::new()
                .allow_origin(Any)
                .allow_methods(Any)
                .allow_headers(Any),
        )
}

#[tracing::instrument(level = "info", skip_all)]
pub async fn serve(addr: SocketAddr, state: AppState) -> anyhow::Result<()> {
    // If sandbox execution is configured, run a background crash monitor for long-running runs.
    if let Some(rt) = state.sandbox_runtime.clone() {
        let monitor = horizons_core::engine::health_monitor::SandboxHealthMonitor::new(
            rt,
            state.event_bus.clone(),
        );
        let cancel = tokio_util::sync::CancellationToken::new();
        tokio::spawn(async move {
            monitor.run(cancel).await;
        });
    }

    let app = router(state);
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}
