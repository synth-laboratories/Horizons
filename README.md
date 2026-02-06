# Horizons

Platform for building, evaluating, and optimizing AI agents in production. Provides structured event routing, long-term memory, prompt optimization, automated evaluation, and human-in-the-loop action approval.

## Crates

| Crate | Version | Description |
|-------|---------|-------------|
| `horizons_core` | 0.0.0 | Core domain — events, context refresh, agent actions, onboarding, project DB |
| `horizons_rs` | 0.0.0 | HTTP API server (Axum) with dev-mode in-memory backends |
| `horizons_integrations` | 0.0.0 | Connectors (Jira, LinkedIn, Langfuse) and queue backends (SQS, RabbitMQ) |
| `voyager` | 0.1.0 | Agent memory — store, retrieve, and rank episodic and semantic memories |
| `mipro_v2` | 0.1.0 | Prompt optimization — dataset splits, candidate generation, early stopping |
| `rlm` | 0.1.0 | Evaluation — reward signals, weighted scoring, pass/fail verification |

All crates use Rust edition 2024.

## SDKs

| SDK | Version | Path |
|-----|---------|------|
| Python (`horizons`) | 0.0.6 | `horizons_py/` |
| TypeScript (`@horizons/sdk`) | 0.0.6 | `horizons_ts/` |

## Getting Started

### Docker Compose (recommended)

```bash
docker compose up
```

Starts the Horizons server on `http://localhost:8000` with persistent local storage. No external services required.

Verify:

```bash
curl http://localhost:8000/health
```

### From source

Prerequisites: Rust 1.85+ (edition 2024 support).

```bash
cargo build --release -p horizons_rs --features all
cargo run --release -p horizons_rs --features all -- serve
```

Starts on `http://localhost:8000` with dev backends (SQLite + local filesystem). No external services required.

### Install the Python SDK

```bash
cd horizons_py
pip install -e .
```

### Install the TypeScript SDK

```bash
cd horizons_ts
npm install
npm run build
```

## Architecture

```
horizons_rs (HTTP API)
├── horizons_core
│   ├── events        — publish/subscribe on dot-delimited topics, glob matching, retry + DLQ
│   ├── context_refresh — pull from external sources on cron or event triggers
│   ├── core_agents   — action proposals, risk levels, review policies (auto/AI/human)
│   └── onboard       — project DB (Turso/Postgres/S3), user roles, audit log
├── horizons_integrations
│   ├── connectors    — Jira, LinkedIn
│   ├── queue_backends — SQS, RabbitMQ
│   └── langfuse      — trace export
├── voyager           — episodic memory with relevance/recency/importance ranking
├── mipro_v2          — MiPRO prompt optimization with holdout evaluation
└── rlm               — reward signals (exact match, contains, LLM rubric), weighted scoring
```

## License

[FSL-1.1-Apache-2.0](LICENSE.md) (the Sentry license) — Copyright 2026 Synth Incorporated.

Free to use, modify, and redistribute for any purpose except building a competing product. Converts to Apache 2.0 after two years.
