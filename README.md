# Horizons

Horizons is a Rust-first runtime for shipping agent systems: event-driven orchestration, project-scoped state, graph execution, and auditable actions.

## Components

- `horizons_rs`: Axum HTTP API server.
- `horizons_core`: core domain models and backend traits (events, onboarding/project DB, agents/actions, pipelines, sandbox runtime).
- `horizons_graph`: DAG execution engine (LLM/tool/python nodes) with a built-in verifier graph registry.
- `horizons_integrations`: infrastructure adapters (e.g. vector store, queue backends, observability sinks).
- `voyager`: memory primitives (embed/retrieve/rank).
- `mipro_v2`: prompt/policy optimization engine.
- `rlm`: reward-signal evaluation engine.

## Quickstart

```bash
docker compose up
curl http://localhost:8000/health
```

## Run From Source

Prerequisites:

- Rust toolchain with edition 2024 support
- `python3` in `PATH` (used by graph `python_function` nodes)

```bash
cargo build --release -p horizons_rs --features all
cargo run --release -p horizons_rs --features all -- serve
```

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

## SDKs

- Python SDK: `horizons_py/` (`pip install -e horizons_py`)
- TypeScript SDK: `horizons_ts/` (`npm install && npm run build`)

## License

[FSL-1.1-Apache-2.0](LICENSE.md) (the Sentry license) — Copyright 2026 Synth Incorporated.

Free to use, modify, and redistribute for any purpose except building a competing product. Converts to Apache 2.0 after two years.

