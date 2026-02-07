# Laminar (lmnr) Tracing

Horizons emits traces using OpenTelemetry (OTLP). Laminar can ingest OTLP, so you can use a self-hosted Laminar instance as Horizons' tracing backend.

## 1. Run Laminar (self-hosted)

Follow Laminar's self-host instructions in `lmnr-ai/lmnr` (their `docker-compose.yml` starts the full stack).

Laminar defaults (self-host):

- OTLP HTTP/protobuf endpoint: `http://localhost:8000` (POSTs to `/v1/traces`)
- OTLP gRPC endpoint: `http://localhost:8001`

## 2. Configure Horizons

Horizons supports standard OpenTelemetry env vars, and also provides Laminar convenience env vars.

### Option A: Laminar convenience env vars (recommended)

```bash
export HORIZONS_LAMINAR_SELF_HOSTED=1
export LMNR_PROJECT_API_KEY="lmnr_project_..."

# Optional overrides:
export HORIZONS_LAMINAR_OTLP_ENDPOINT="http://localhost:8000"
export HORIZONS_LAMINAR_OTLP_PROTOCOL="http/protobuf" # or "grpc"
```

### Option B: Standard OpenTelemetry env vars

```bash
export OTEL_EXPORTER_OTLP_ENDPOINT="http://localhost:8000"
export OTEL_EXPORTER_OTLP_PROTOCOL="http/protobuf" # or "grpc"
export OTEL_EXPORTER_OTLP_HEADERS="authorization=Bearer lmnr_project_..."
```

## Notes

- Laminar auth uses an `Authorization: Bearer ...` header. Horizons will auto-inject this header when `HORIZONS_LAMINAR_SELF_HOSTED` or `HORIZONS_LAMINAR_OTLP_ENDPOINT` is set and a project key is available via `LMNR_PROJECT_API_KEY` (or `HORIZONS_LAMINAR_PROJECT_API_KEY`).
- If you set `OTEL_EXPORTER_OTLP_*` explicitly, those take precedence over the Laminar convenience env vars.

