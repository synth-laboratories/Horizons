<div align="center">
  <h1>Horizons</h1>
  <p>Self-hostable agent platform. Define agents, give them tools, run them in sandboxes, govern their actions.</p>
  <p><a href="https://www.usesynth.ai">https://www.usesynth.ai</a></p>
  <p>
    <a href="https://github.com/synth-laboratories/Horizons/actions/workflows/ci.yml?query=branch%3Adev"><img alt="CI" src="https://github.com/synth-laboratories/Horizons/actions/workflows/ci.yml/badge.svg?branch=dev"></a>
    <a href="LICENSE.md"><img alt="License" src="https://img.shields.io/badge/license-FSL--1.1--Apache--2.0-blue"></a>
    <a href="https://crates.io/crates/horizons-ai"><img alt="crates.io" src="https://img.shields.io/crates/v/horizons-ai"></a>
    <a href="https://pypi.org/project/horizons/"><img alt="PyPI" src="https://img.shields.io/pypi/v/horizons"></a>
    <a href="https://www.npmjs.com/package/@horizons-ai/sdk"><img alt="npm" src="https://img.shields.io/npm/v/@horizons-ai/sdk"></a>
  </p>
  <p>
    <a href="#quickstart">Quickstart</a> ·
    <a href="#built-on-horizons">Built on Horizons</a> ·
    <a href="#features">Features</a> ·
    <a href="#install-the-sdks">Install the SDKs</a> ·
    <a href="#from-source">From source</a> ·
    <a href="#api-overview">API overview</a> ·
    <a href="#repo-layout">Repo layout</a>
  </p>
</div>

---

## SDLC Usage (Internal Development)

For internal PR workflows, run SDLC gates from `../synth-bazel`:

```bash
cd ../synth-bazel
./scripts/ci_dev_gate.sh
```

If integration dependencies are unavailable locally:

```bash
./scripts/ci_dev_gate.sh --skip-integration
```

## Features

### Agents and actions

Register agents as declarative specs (name, sandbox image, allowed tools, schedule). Run them on-demand or on cron. Agents propose actions; actions go through approval gates (auto-approve, AI review, or human review) before execution. Every action is recorded in an append-only audit log.

### MCP gateway (stdio + HTTP)

Built-in [MCP](https://modelcontextprotocol.io) gateway supporting stdio and HTTP transports. Requests to `POST /api/v1/mcp/call` derive identity from verified auth (Bearer API keys) by default.

Note: v0.x has the plumbing for scope checks, but does not yet have full RBAC-backed scope assignment and enforcement end-to-end.

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

All data and operations are org-scoped. In local dev you can use `x-org-id` (and optional `x-project-id`) headers; in secure mode, org + identity are derived from `Authorization: Bearer ...` API keys (recommended).

### API-first, SDKs in three languages

REST API (Axum). SDKs for Python, TypeScript, and Rust. No UI required — everything is programmable.

---

## Install

### Homebrew (macOS & Linux)

```bash
brew install synth-laboratories/tap/horizons
```

### Docker

```bash
docker compose up
```

Starts the Horizons server on http://localhost:8000 with persistent local storage. No external services required.

To create an API key for mutating `/api/v1/*` calls:

```bash
ORG_ID=$(uuidgen)
docker compose exec horizons horizons create-api-key --data-dir /data --org-id "$ORG_ID" --name dev
```

### Observability (OTLP, Laminar)

Horizons emits OpenTelemetry traces (OTLP). You can point it at any OTLP collector, including a self-hosted Laminar stack.

- Laminar self-host guide: `docs/LAMINAR.md`

### From source

Prerequisites: **Rust 1.85+** (edition 2024 support).

```bash
cargo build --release -p horizons_server --features all
./target/release/horizons_server serve
```

### Pre-built binaries

Download from [GitHub Releases](https://github.com/synth-laboratories/Horizons/releases). Binaries are available for:

- `x86_64-apple-darwin` (Intel Mac)
- `aarch64-apple-darwin` (Apple Silicon)
- `x86_64-unknown-linux-gnu`
- `aarch64-unknown-linux-gnu`

---

## Quickstart

```bash
# Install
brew install synth-laboratories/tap/horizons

# Start the server (local dev mode — SQLite + local filesystem, no external services)
horizons serve

# Verify
curl http://localhost:8000/health
```

### Golden path: first agent + approval + audit

Use this flow to validate the core execution loop in minutes:

1. Start server and create an API key
2. Create a project
3. Run an agent
4. Propose an action
5. Approve the action
6. Inspect pending actions and audit trail

```bash
# Set tenant + auth
ORG_ID=$(uuidgen)
horizons create-api-key --org-id "$ORG_ID" --name dev
HORIZONS_API_KEY="hzn_..."

# Create project
PROJECT_JSON=$(curl -sS -X POST "http://localhost:8000/api/v1/projects" \
  -H "Authorization: Bearer $HORIZONS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{}')
PROJECT_ID=$(printf '%s' "$PROJECT_JSON" | jq -r '.project_id')

# Run a simple agent
curl -sS -X POST "http://localhost:8000/api/v1/agents/run" \
  -H "Authorization: Bearer $HORIZONS_API_KEY" \
  -H "x-project-id: $PROJECT_ID" \
  -H "x-agent-id: agent:demo" \
  -H "Content-Type: application/json" \
  -d '{"agent_id":"dev.noop","inputs":{"prompt":"hello"}}'

# Propose an action
ACTION_ID=$(curl -sS -X POST "http://localhost:8000/api/v1/actions/propose" \
  -H "Authorization: Bearer $HORIZONS_API_KEY" \
  -H "x-project-id: $PROJECT_ID" \
  -H "x-agent-id: agent:demo" \
  -H "Content-Type: application/json" \
  -d '{
    "agent_id":"agent:demo",
    "action_type":"demo.notify",
    "payload":{"message":"hello from readme"},
    "risk_level":"low",
    "context":{"source":"readme"},
    "dedupe_key":"readme-demo-action-v1"
  }' | jq -r '.action_id')

# Approve action
curl -sS -X POST "http://localhost:8000/api/v1/actions/$ACTION_ID/approve" \
  -H "Authorization: Bearer $HORIZONS_API_KEY" \
  -H "x-project-id: $PROJECT_ID" \
  -H "Content-Type: application/json" \
  -d '{"reason":"readme demo"}'

# Verify queue + audit
curl -sS "http://localhost:8000/api/v1/actions/pending?limit=20" \
  -H "Authorization: Bearer $HORIZONS_API_KEY" \
  -H "x-project-id: $PROJECT_ID"
curl -sS "http://localhost:8000/api/v1/audit?limit=20" \
  -H "Authorization: Bearer $HORIZONS_API_KEY" \
  -H "x-project-id: $PROJECT_ID"
```

### Auth (recommended for any mutating API calls)

By default, mutating requests under `/api/v1/*` (POST/PUT/PATCH/DELETE) require verified auth.

Create an org + API key in the dev Central DB and use it as a Bearer token:

```bash
ORG_ID=$(uuidgen)
horizons create-api-key --org-id "$ORG_ID" --name dev

# Use the printed token (format: hzn_<uuid>.<secret>)
HORIZONS_API_KEY="hzn_..."

curl -sS -X POST "http://localhost:8000/api/v1/events/publish" \
  -H "Authorization: Bearer $HORIZONS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "direction": "outbound",
    "topic": "demo.hello",
    "source": "readme",
    "payload": { "hello": "world" },
    "dedupe_key": "demo.hello.v1"
  }'
```

### Insecure local dev (escape hatch)

If you want to use header-based org/identity for mutating endpoints in local dev, set:
- `HORIZONS_ALLOW_INSECURE_HEADERS=true`
- `HORIZONS_ALLOW_INSECURE_MUTATING_REQUESTS=true`

### CLI commands

```
horizons serve              # Start the HTTP server (default)
horizons migrate            # Run database migrations (Postgres)
horizons create-api-key     # Create an API key in the central DB (Bearer auth)
horizons validate-graph     # Validate a graph definition (YAML/JSON)
horizons list-graphs        # List built-in graphs from the registry
horizons config             # Print current configuration (secrets redacted)
horizons check              # Health check configured backends
```

### Python execution backends

- Default: local `python3` subprocess.
- Optional: embedded interpreter via [pydantic/monty](https://github.com/pydantic/monty) (requires `--features graph_monty` and `HORIZONS_GRAPH_PYTHON_BACKEND=monty`).

---

## Built on Horizons

Real applications are already running on top of Horizons as the control plane.

### OpenRevenue (public)

Revenue operations app built on Horizons traits and APIs:
- signal ingestion + scoring
- context refresh connectors
- agent sessions with streaming progress
- action approvals and audit workflow

Repo: [OpenRevenue](https://github.com/synth-laboratories/OpenRevenue)

### Dhakka (private/internal)

Personal ops + communication app built as a separate workspace on Horizons:
- iMessage/email/calendar connectors
- permissioned actions via approvals
- outbox/event-driven execution
- one-command local startup (`./run.sh`)

Local repo: `../Dhakka` (see `README.md` there)

---

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

## API overview

Horizons is org-scoped:
- If you use `Authorization: Bearer ...`, org/identity are derived from the token.
- For local dev/read-only usage, many endpoints also accept `x-org-id` (and optional `x-project-id`) headers.
- Mutating `/api/v1/*` endpoints require verified auth by default (see Quickstart above).

| Area | Endpoints | Description |
|------|-----------|-------------|
| **Agents** | `POST /api/v1/agents/run`, `POST /api/v1/agents/chat` (SSE), `GET /api/v1/agents` | Run agents, stream agent chat, list registered agents |
| **Actions** | `POST /api/v1/actions/propose`, `POST /api/v1/actions/:id/approve`, `POST /api/v1/actions/:id/deny`, `GET /api/v1/actions/pending` | Action proposal and approval lifecycle |
| **MCP** | `POST /api/v1/mcp/config`, `GET /api/v1/mcp/tools`, `POST /api/v1/mcp/call` | MCP server configuration and tool execution |
| **Engine** | `POST /api/v1/engine/run`, `POST /api/v1/engine/start`, `GET /api/v1/engine/:id/events` (SSE) | Sandbox provisioning and agent execution |
| **Events** | `POST /api/v1/events/publish`, `GET /api/v1/events`, `POST /api/v1/subscriptions`, `GET /api/v1/subscriptions`, `DELETE /api/v1/subscriptions/{id}` | Event bus publish/query + subscription lifecycle |
| **Graph** | `POST /api/v1/graph/execute`, `POST /api/v1/graph/validate`, `GET /api/v1/graph/registry` | DAG execution and verifier registry |
| **Context Refresh** | `POST /api/v1/connectors`, `GET /api/v1/connectors`, `POST /api/v1/context-refresh/run`, `GET /api/v1/context-refresh/status` | Connector source registration + refresh runs |
| **Pipelines** | `POST /api/v1/pipelines/run`, `GET /api/v1/pipelines/runs/{id}`, `POST /api/v1/pipelines/runs/{id}/approve/{step_id}`, `POST /api/v1/pipelines/runs/{id}/cancel` | Multi-step pipeline orchestration |
| **Project DB** | `POST /api/v1/projects/{id}/query`, `POST /api/v1/projects/{id}/execute` | Raw SQL query/execute against a project DB (guardrailed; writes disabled by default) |
| **Files** | `PUT /api/v1/files/{*key}`, `GET /api/v1/files/{*key}`, `DELETE /api/v1/files/{*key}` | Per-org file storage |
| **Projects** | `POST /api/v1/projects`, `GET /api/v1/projects` | Project provisioning + listing |
| **Audit** | `GET /api/v1/audit` | Append-only audit trail |
| **Assets** | `POST /api/v1/assets/resources`, `GET /api/v1/assets/resources`, `POST /api/v1/assets/operations`, `GET /api/v1/assets/operations` | Managed resources + operations (non-UI) |
| **Credentials** | `GET /api/v1/credentials`, `PUT /api/v1/credentials/{connector_id}` | Encrypted credential storage (non-UI) |
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

## Specs

Horizons platform specs and implementation plans live in the separate `specifications/` repo under `horizons/`.

## License

[FSL-1.1-Apache-2.0](LICENSE.md) (the Sentry license) — Copyright 2026 Synth Incorporated.

Free to use, modify, and redistribute for any purpose except building a competing product. Converts to Apache 2.0 after two years.
