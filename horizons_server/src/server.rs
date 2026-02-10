use crate::admin;
use crate::auth::CentralDbApiKeyAuth;
use crate::extract::{AuthConfig, AuthConfigExt, AuthProviderExt};
use crate::routes;
use axum::routing::get;
use axum::{Extension, Router};
use horizons_core::context_refresh::traits::ContextRefresh;
use horizons_core::core_agents::executor::CoreAgentsExecutor;
use horizons_core::core_agents::mcp_gateway::McpGateway;
use horizons_core::core_agents::scheduler::CoreScheduler;
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
    pub core_scheduler: Arc<CoreScheduler>,
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
        core_scheduler: Arc<CoreScheduler>,
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
            core_scheduler,
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
    router_with_auth_config(state, AuthConfig::default())
}

#[tracing::instrument(level = "debug", skip_all)]
pub fn router_with_auth_config(state: AppState, auth_cfg: AuthConfig) -> Router {
    let state = Arc::new(state);
    // Provide Bearer-token auth support via `AuthenticatedIdentity`/`OrgIdHeader` extractors.
    let auth_provider =
        AuthProviderExt(Arc::new(CentralDbApiKeyAuth::new(state.central_db.clone())));
    let auth_cfg = AuthConfigExt(auth_cfg);

    Router::new()
        .merge(routes::router())
        .merge(admin::router())
        .route("/health", get(routes::health::get_health))
        .layer(Extension(state))
        .layer(Extension(auth_provider))
        .layer(Extension(auth_cfg))
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

    // Self-driving scheduler background loop (opt-in).
    let scheduler_enabled = std::env::var("HORIZONS_CORE_SCHEDULER_ENABLED")
        .ok()
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(true);
    if scheduler_enabled {
        let tick_ms: u64 = std::env::var("HORIZONS_CORE_SCHEDULER_TICK_MS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(5_000);
        let max_runs_per_tick: usize = std::env::var("HORIZONS_CORE_SCHEDULER_MAX_RUNS_PER_TICK")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(3);

        let scheduler = state.core_scheduler.clone();
        let central_db = state.central_db.clone();
        let project_db = state.project_db.clone();

        tokio::spawn(async move {
            tracing::info!(
                tick_ms,
                max_runs_per_tick,
                "core scheduler background loop started"
            );
            let mut interval = tokio::time::interval(std::time::Duration::from_millis(tick_ms));
            loop {
                interval.tick().await;
                let now = chrono::Utc::now();

                // List orgs (best-effort pagination).
                let mut offset = 0usize;
                loop {
                    let orgs = match central_db
                        .list_orgs(horizons_core::onboard::models::ListQuery { limit: 100, offset })
                        .await
                    {
                        Ok(v) => v,
                        Err(e) => {
                            tracing::warn!(error = %e, "scheduler failed listing orgs");
                            break;
                        }
                    };
                    if orgs.is_empty() {
                        break;
                    }

                    let org_count = orgs.len();
                    for org in orgs {
                        // List projects for org (best-effort pagination).
                        let mut poffset = 0usize;
                        loop {
                            let projects = match project_db
                                .list_projects(
                                    org.org_id,
                                    horizons_core::onboard::models::ListQuery {
                                        limit: 200,
                                        offset: poffset,
                                    },
                                )
                                .await
                            {
                                Ok(v) => v,
                                Err(e) => {
                                    tracing::warn!(org_id = %org.org_id, error = %e, "scheduler failed listing projects");
                                    break;
                                }
                            };
                            if projects.is_empty() {
                                break;
                            }

                            let project_count = projects.len();
                            for p in projects {
                                let _ = scheduler
                                    .tick_project(org.org_id, p.project_id, now, max_runs_per_tick)
                                    .await;
                            }

                            poffset += project_count;
                        }
                    }

                    offset += org_count;
                }
            }
        });
    } else {
        tracing::info!(
            "core scheduler background loop disabled (HORIZONS_CORE_SCHEDULER_ENABLED=false)"
        );
    }

    let app = router(state);
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}
