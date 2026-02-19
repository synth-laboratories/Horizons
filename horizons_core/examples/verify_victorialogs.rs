use horizons_core::engine::models::{AgentKind, SandboxBackendKind};
use horizons_core::engine::victoria_logs::{VictoriaLogsEmitter, VictoriaSessionCtx};
use std::collections::HashMap;
use std::sync::Arc;

#[tokio::main]
async fn main() {
    let Some(emitter) = VictoriaLogsEmitter::from_env() else {
        eprintln!(
            "VictoriaLogsEmitter not enabled. Set HORIZONS_VICTORIA_LOGS_URL (or SMR_VICTORIA_LOGS_URL)."
        );
        std::process::exit(2);
    };

    let mut tags = HashMap::new();
    tags.insert("run_id".to_string(), "run_verify_123".to_string());
    tags.insert("project_id".to_string(), "proj_verify_123".to_string());
    tags.insert("tenant_id".to_string(), "tenant_verify_123".to_string());
    tags.insert("component".to_string(), "verify".to_string());

    let ctx = VictoriaSessionCtx {
        sandbox_id: "sandbox_verify_123".to_string(),
        sandbox_backend: SandboxBackendKind::Docker,
        sandbox_agent_url: "http://127.0.0.1:2468".to_string(),
        host_port: 2468,
        session_id: "session_verify_123".to_string(),
        agent: AgentKind::Codex,
        tags: Arc::new(tags),
    };

    for (i, et) in ["session.created", "turn.started", "turn.completed"]
        .into_iter()
        .enumerate()
    {
        let event = serde_json::json!({
            "type": et,
            "time": chrono::Utc::now().to_rfc3339(),
            "data": { "n": i + 1 }
        });
        emitter.enqueue_event(&ctx, (i as u64) + 1, &event);
    }

    emitter.enqueue_session_completed(&ctx, true, 0.123, None);

    // Give the background ingest loop a moment to flush.
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    println!("ok");
}
