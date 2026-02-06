//! System 2/Shared: sandboxed execution via a Rhodes-compatible adapter,
//! plus a full sandbox runtime for running Codex/Claude/OpenCode agents
//! inside Docker or Daytona containers via sandbox-agent.

pub mod daytona_backend;
pub mod docker_backend;
pub mod health_monitor;
pub mod local_adapter;
pub mod models;
pub mod sandbox_agent_client;
pub mod sandbox_runtime;
pub mod traits;
