<div align="center">
  <h1>Horizons</h1>
  <p>Self-hostable agent platform. Define agents, give them tools, run them in sandboxes, govern their actions.</p>
  <p>
    <a href="https://github.com/synth-laboratories/Horizons/actions/workflows/ci.yml?query=branch%3Adev"><img alt="CI" src="https://github.com/synth-laboratories/Horizons/actions/workflows/ci.yml/badge.svg?branch=dev"></a>
    <a href="LICENSE.md"><img alt="License" src="https://img.shields.io/badge/license-FSL--1.1--Apache--2.0-blue"></a>
    <a href="https://crates.io/crates/horizons-ai"><img alt="crates.io" src="https://img.shields.io/crates/v/horizons-ai"></a>
    <a href="https://pypi.org/project/horizons/"><img alt="PyPI" src="https://img.shields.io/pypi/v/horizons"></a>
    <a href="https://www.npmjs.com/package/@horizons-ai/sdk"><img alt="npm" src="https://img.shields.io/npm/v/@horizons-ai/sdk"></a>
  </p>
  <p>
    <a href="#quickstart">Quickstart</a> ·
    <a href="#features">Features</a> ·
    <a href="#install-the-sdks">Install the SDKs</a> ·
    <a href="#from-source">From source</a> ·
    <a href="#api-overview">API overview</a> ·
    <a href="#repo-layout">Repo layout</a>
  </p>
</div>

---

## Features

### Agents and actions

Register agents as declarative specs (name, sandbox image, allowed tools, schedule). Run them on-demand or on cron. Agents propose actions; actions go through approval gates (auto-approve, AI review, or human review) before execution. Every action is recorded in an append-only audit log.

### MCP gateway with auth

Built-in [MCP](https://modelcontextprotocol.io) gateway supporting stdio and HTTP transports. Scope-based authorization per tool, per agent. Agents call your internal APIs and services through MCP — the gateway enforces what each agent is allowed to do.

- Configure MCP servers: `POST /api/v1/mcp/config`
- List available tools: `GET /api/v1/mcp/tools`
- Call a tool: `POST /api/v1/mcp/call`

### Secrets management

Per-org encrypted credential storage (AES-256-GCM). Store API keys, database credentials, service tokens. Secrets are injected at runtime via token resolution (`$cred:connector_id.key`) — agents never see raw credentials in config.

### Sandboxed execution

Agents run in isolated containers via the Rhodes adapter. Supports Docker (local dev) and Daytona (cloud, with snapshotted images for ~3s provisioning). Filesystem overrides let you inject files (AGENTS.md, skills files, config) and environment variables into the sandbox before the agent starts.

### Event-driven orchestration

Bidirectional event bus (Redis pub/sub + Postgres event store). Publish/subscribe with routing rules, retry policies, dead-letter queues, and webhook delivery. Agents, connectors, and pipelines communicate through events.

### Context refresh

Connectors ingest external data into durable, per-org context stores. Agents read from these stores at runtime. Built-in connectors for common services; write your own as a simple trait impl.

### Graph execution engine

DAG-based execution engine for structured agent workflows. LLM nodes, Python function nodes, tool-call nodes. Built-in verifier graph registry (rubric scoring, contrastive verification, few-shot, RLM). Define graphs in YAML, execute via API.

### Multi-tenant by default

All data and operations are scoped by `x-org-id`. Tenant isolation is enforced at the trait level — every storage backend, event bus, and agent execution path is org-scoped.

### API-first, SDKs in three languages

REST API (Axum). SDKs for Python, TypeScript, and Rust. No UI required — everything is programmable.

---

## Quickstart

**Docker Compose** (recommended):

```bash
docker compose up
```

Starts the Horizons server on http://localhost:8000 with persistent local storage. No external services required.

```bash
curl http://localhost:8000/health
```

## Install the SDKs

### Python

```bash
pip install horizons
```

### TypeScript / JavaScript

```bash
npm install @horizons-ai/sdk
```

### Rust

```bash
cargo add horizons-ai
```

## From source

Prerequisites: **Rust 1.85+** (edition 2024 support).

```bash
cargo build --release -p horizons_server --features all
cargo run --release -p horizons_server --features all -- serve
```

Starts on http://localhost:8000 with dev backends (SQLite + local filesystem). No external services required.

Python execution backends:

- Default: local `python3` subprocess.
- Optional: embedded interpreter via [pydantic/monty](https://github.com/pydantic/monty) (requires `--features graph_monty` and `HORIZONS_GRAPH_PYTHON_BACKEND=monty`).

## API overview

All endpoints require an `x-org-id` header for tenant isolation. Most also accept `x-project-id`.

| Area | Endpoints | Description |
|------|-----------|-------------|
| **Agents** | `POST /api/v1/agents/run`, `POST /api/v1/agents/chat` (SSE), `GET /api/v1/agents` | Run agents, stream agent chat, list registered agents |
| **Actions** | `POST /api/v1/actions/propose`, `POST /api/v1/actions/:id/approve`, `POST /api/v1/actions/:id/deny`, `GET /api/v1/actions/pending` | Action proposal and approval lifecycle |
| **MCP** | `POST /api/v1/mcp/config`, `GET /api/v1/mcp/tools`, `POST /api/v1/mcp/call` | MCP server configuration and tool execution |
| **Engine** | `POST /api/v1/engine/run`, `POST /api/v1/engine/start`, `GET /api/v1/engine/:id/events` (SSE) | Sandbox provisioning and agent execution |
| **Events** | `POST /api/v1/events/publish`, `POST /api/v1/events/subscribe`, `GET /api/v1/events/stream` (SSE) | Event bus pub/sub |
| **Graph** | `POST /api/v1/graph/execute`, `POST /api/v1/graph/validate`, `GET /api/v1/graph/registry` | DAG execution and verifier registry |
| **Context** | `POST /api/v1/context_refresh/connectors`, `POST /api/v1/context_refresh/sync` | Connector registration and data sync |
| **Pipelines** | `POST /api/v1/pipelines/run`, `GET /api/v1/pipelines/:id/status` | Multi-step pipeline orchestration |
| **Storage** | `POST /api/v1/filestore/upload`, `GET /api/v1/filestore/:key` | Per-org file storage |
| **Projects** | `POST /api/v1/projects`, `GET /api/v1/projects` | Project management |
| **Audit** | `GET /api/v1/audit/log` | Append-only audit trail |
| **Config** | `GET /api/v1/config`, `PUT /api/v1/config` | Runtime configuration |

Optional feature-gated endpoints (compile with `--features memory`, `optimization`, `evaluation`, or `all`):

| Feature | Endpoints | Description |
|---------|-----------|-------------|
| **Memory** | `/api/v1/memory/*` | Embed, retrieve, rank (Voyager) |
| **Optimization** | `/api/v1/optimization/*` | Prompt/policy optimization (MIPRO v2) |
| **Evaluation** | `/api/v1/evaluation/*` | Reward verification (RLM) |

## Repo layout

### Crates

| Crate | Version | Description |
|-------|---------|-------------|
| `horizons_server` | 0.1.0 | Axum HTTP API server |
| `horizons-ai` (`horizons_rs/`) | 0.1.0 | Rust SDK client — [crates.io](https://crates.io/crates/horizons-ai) |
| `horizons_core` | 0.1.0 | Core domain models and backend traits (events, projects DB, agents/actions, pipelines, sandbox runtime) |
| `horizons_graph` | 0.1.0 | DAG execution engine (LLM/tool/python nodes) with built-in verifier graph registry |
| `horizons_integrations` | 0.1.0 | Infrastructure adapters (vector store, queue backends, observability sinks) |
| `voyager` | 0.1.0 | Memory — embed/retrieve/rank |
| `mipro_v2` | 0.1.0 | Optimization — prompt/policy optimization engine |
| `rlm` | 0.1.0 | Evaluation — reward signals, weighted scoring, pass/fail verification |

All crates use Rust **edition 2024**.

### SDKs

| SDK | Version | Path |
|-----|---------|------|
| Python (`horizons`) | 0.1.0 | `horizons_py/` — [PyPI](https://pypi.org/project/horizons/) |
| TypeScript (`@horizons-ai/sdk`) | 0.1.0 | `horizons_ts/` — [npm](https://www.npmjs.com/package/@horizons-ai/sdk) |
| Rust (`horizons-ai`) | 0.1.0 | `horizons_rs/` — [crates.io](https://crates.io/crates/horizons-ai) |

## Testing

Horizons keeps test code out of the repo. See [synth-laboratories/testing](https://github.com/synth-laboratories/testing) (`horizons-tests/`) for unit, property-based, mocked integration, and opt-in live-LLM tests.

## License

[FSL-1.1-Apache-2.0](LICENSE.md) (the Sentry license) — Copyright 2026 Synth Incorporated.

Free to use, modify, and redistribute for any purpose except building a competing product. Converts to Apache 2.0 after two years.
