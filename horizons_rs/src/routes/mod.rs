use axum::Router;

pub mod actions;
pub mod agents;
pub mod audit;
pub mod config;
pub mod context_refresh;
pub mod engine;
#[cfg(feature = "evaluation")]
pub mod evaluation;
pub mod events;
pub mod filestore;
pub mod graph;
pub mod health;
#[cfg(feature = "memory")]
pub mod memory;
pub mod onboard;
#[cfg(feature = "optimization")]
pub mod optimization;
pub mod projects;

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
            .merge(projects::router())
            .merge(onboard::router())
            .merge(filestore::router())
            .merge(graph::router())
            .merge(events::api_router())
            .merge(context_refresh::router())
            .merge(agents::router())
            .merge(actions::router())
            .merge(engine::router())
            .merge(feature_routers())
            .merge(audit::router())
            .merge(config::router()),
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
