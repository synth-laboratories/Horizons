use axum::Router;
use axum::middleware;
use axum::routing::get;

pub mod actions;
pub mod agents;
pub mod assets;
pub mod audit;
pub mod config;
pub mod context_refresh;
pub mod core_agents;
pub mod credentials;
pub mod engine;
#[cfg(feature = "evaluation")]
pub mod evaluation;
pub mod events;
pub mod filestore;
pub mod graph;
pub mod health;
pub mod mcp;
#[cfg(feature = "memory")]
pub mod memory;
pub mod onboard;
#[cfg(feature = "optimization")]
pub mod optimization;
pub mod orgs;
pub mod pipelines;
pub mod projects;
pub mod tick;

#[tracing::instrument(level = "debug", skip_all)]
pub fn router() -> Router {
    Router::new()
        .merge(events::inbound_router())
        .merge(api_v1_router())
}

#[tracing::instrument(level = "debug", skip_all)]
fn api_v1_router() -> Router {
    Router::new().nest(
        "/api/v1",
        Router::new()
            .route("/health", get(health::get_health))
            .merge(orgs::router())
            .merge(projects::router())
            .merge(onboard::router())
            .merge(filestore::router())
            .merge(graph::router())
            .merge(events::api_router())
            .merge(context_refresh::router())
            .merge(agents::router())
            .merge(core_agents::router())
            .merge(tick::router())
            .merge(actions::router())
            .merge(pipelines::router())
            .merge(engine::router())
            .merge(mcp::router())
            .merge(feature_routers())
            .merge(audit::router())
            .merge(assets::router())
            .merge(credentials::router())
            .merge(config::router())
            .layer(middleware::from_fn(
                crate::middleware::require_auth_for_mutating,
            )),
    )
}

#[tracing::instrument(level = "debug", skip_all)]
fn feature_routers() -> Router {
    let r = Router::new();
    #[cfg(feature = "memory")]
    let r = r.merge(memory::router());
    #[cfg(feature = "optimization")]
    let r = r.merge(optimization::router());
    #[cfg(feature = "evaluation")]
    let r = r.merge(evaluation::router());
    r
}
