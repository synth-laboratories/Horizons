<div align="center">
  <h1>Horizons</h1>
  <p>Rust-first runtime for shipping agent systems: event-driven orchestration, project-scoped state, graph execution, and auditable actions.</p>
  <p>
    <a href="https://github.com/synth-laboratories/Horizons/actions/workflows/ci.yml?query=branch%3Adev"><img alt="CI" src="https://github.com/synth-laboratories/Horizons/actions/workflows/ci.yml/badge.svg?branch=dev"></a>
    <a href="LICENSE.md"><img alt="License" src="https://img.shields.io/badge/license-FSL--1.1--Apache--2.0-blue"></a>
    <a href="https://crates.io/crates/horizons-ai"><img alt="crates.io" src="https://img.shields.io/crates/v/horizons-ai"></a>
    <a href="https://pypi.org/project/horizons/"><img alt="PyPI" src="https://img.shields.io/pypi/v/horizons"></a>
    <a href="https://www.npmjs.com/package/@horizons-ai/sdk"><img alt="npm" src="https://img.shields.io/npm/v/@horizons-ai/sdk"></a>
  </p>
  <p>
    <a href="#quickstart">Quickstart</a> ·
    <a href="#install-the-sdks">Install the SDKs</a> ·
    <a href="#from-source">From source</a> ·
    <a href="#graph-api">Graph API</a> ·
    <a href="#repo-layout">Repo layout</a>
  </p>
</div>

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

## Graph API

Horizons exposes the graph engine under `/api/v1/graph/*`.

List built-in graphs:

```bash
curl -sS \
  -H "x-org-id: $ORG_ID" \
  -H "x-project-id: $PROJECT_ID" \
  "http://localhost:8000/api/v1/graph/registry"
```

Validate a built-in graph:

```bash
curl -sS \
  -H "x-org-id: $ORG_ID" \
  -H "x-project-id: $PROJECT_ID" \
  -X POST "http://localhost:8000/api/v1/graph/validate" \
  -d '{"graph_id":"verifier_rubric_single","strictness":"strict"}'
```

Execute a graph from YAML:

```bash
curl -sS \
  -H "x-org-id: $ORG_ID" \
  -H "x-project-id: $PROJECT_ID" \
  -X POST "http://localhost:8000/api/v1/graph/execute" \
  -d '{
    "graph_yaml": "name: hello\nstart_nodes: [n1]\nend_nodes: [n1]\nnodes:\n  n1:\n    name: n1\n    type: DagNode\n    input_mapping: \"{}\"\n    implementation:\n      type: python_function\n      fn_name: main\n      fn_str: |\n        def main():\n            return {\"ok\": True}\ncontrol_edges:\n  n1: []\n",
    "inputs": {}
  }'
```

Notes:

- The HTTP layer injects `"_horizons": {"org_id": "...", "project_id": "..."}` into graph inputs.
- LLM nodes resolve API keys from `GRAPH_LLM_API_KEY` and provider-specific env vars (e.g. `OPENAI_API_KEY`).
- Tool calls can be routed to a remote executor via `GRAPH_TOOL_EXECUTOR_URL` (optional).

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
