# Horizons Helm Chart

Deploy the [Horizons](https://github.com/synth-laboratories/Horizons) agent
execution platform on Kubernetes.

## Prerequisites

- Kubernetes 1.26+
- Helm 3.12+
- A container image built from the repo `Dockerfile`
  (`ghcr.io/synth-laboratories/horizons` or your own registry)

## Quick start (dev mode — in-cluster Postgres + Redis)

```bash
helm install horizons-dev ./charts/horizons \
  -f charts/horizons/ci/dev-values.yaml
```

This spins up single-replica Postgres (pgvector) and Redis inside the cluster
alongside the Horizons server. Good for demos and development.

Access the API:

```bash
kubectl port-forward svc/horizons-dev 8000:80
curl http://localhost:8000/health
```

## Production install (external Postgres + Redis)

Create your own `prod.yaml` (see `ci/prod-values.yaml` for a full example):

```yaml
externalPostgres:
  url: "postgresql://horizons:PASSWORD@rds-host:5432/horizons"

externalRedis:
  url: "redis://:PASSWORD@elasticache-host:6379"

secrets:
  masterKey: "<openssl rand -base64 32>"

ingress:
  enabled: true
  className: nginx
  hosts:
    - host: horizons.example.com
      paths:
        - path: /
          pathType: Prefix
  tls:
    - secretName: horizons-tls
      hosts:
        - horizons.example.com
```

```bash
helm install horizons ./charts/horizons -f prod.yaml
```

### Using ExternalSecrets / SealedSecrets

Set `existingSecret` to the name of a Secret you manage externally and the
chart will reference it instead of creating its own:

```yaml
existingSecret: my-horizons-secrets
```

The Secret must contain at minimum `DATABASE_URL` and `REDIS_URL`.

## Migrations

Database migrations run automatically as a Helm **pre-install / pre-upgrade**
Job when `migrations.enabled: true` (the default). The Job invokes
`horizons migrate` against the configured `DATABASE_URL`.

To run migrations manually:

```bash
kubectl run horizons-migrate --rm -it --restart=Never \
  --image=ghcr.io/synth-laboratories/horizons:0.1.0 \
  --env="DATABASE_URL=postgresql://..." \
  -- migrate
```

## Architecture

```
┌──────────────┐       ┌───────────┐
│  Ingress     │──────▶│  Service   │
│  (optional)  │       │  :80      │
└──────────────┘       └─────┬─────┘
                             │
                  ┌──────────▼──────────┐
                  │  Deployment         │
                  │  horizons-server    │
                  │  :8000              │
                  └──┬──────────────┬───┘
                     │              │
              ┌──────▼───┐   ┌─────▼────┐
              │ Postgres  │   │  Redis   │
              │ (ext/int) │   │ (ext/int)│
              └───────────┘   └──────────┘
```

## Configuration reference

| Parameter | Description | Default |
|-----------|-------------|---------|
| `server.replicaCount` | Number of server replicas | `1` |
| `server.image.repository` | Image repository | `ghcr.io/synth-laboratories/horizons` |
| `server.image.tag` | Image tag | `Chart.appVersion` |
| `server.port` | Container port | `8000` |
| `server.resources` | CPU/memory requests and limits | `250m/256Mi — 2/1Gi` |
| `service.type` | Kubernetes Service type | `ClusterIP` |
| `service.port` | Service port | `80` |
| `ingress.enabled` | Create Ingress resource | `false` |
| `autoscaling.enabled` | Enable HPA | `false` |
| `podDisruptionBudget.enabled` | Enable PDB | `false` |
| `networkPolicy.enabled` | Enable NetworkPolicy | `false` |
| `externalPostgres.url` | External Postgres URL | `""` |
| `externalRedis.url` | External Redis URL | `""` |
| `postgres.enabled` | Deploy in-cluster Postgres | `false` |
| `redis.enabled` | Deploy in-cluster Redis | `false` |
| `secrets.masterKey` | AES-GCM master key (base64) | `""` (auto-generated) |
| `objectStorage.bucket` | S3-compatible bucket | `""` |
| `migrations.enabled` | Run migrations on deploy | `true` |
| `persistence.enabled` | PVC for /data (dev mode) | `false` |
| `existingSecret` | Use pre-existing Secret | `""` |

See `values.yaml` for the complete set of configurable parameters.

## Mapping from docker-compose.prod.yml

| docker-compose service | Helm equivalent |
|------------------------|-----------------|
| `horizons` | `server.*` (Deployment) |
| `postgres` | `postgres.enabled: true` or `externalPostgres.url` |
| `redis` | `redis.enabled: true` or `externalRedis.url` |
| `.env` file | `values.yaml` + Secret |
| `horizons_data` volume | `persistence.enabled: true` or `emptyDir` |

## Upgrade notes

The chart uses semantic versioning. Breaking changes in `values.yaml` schema
will bump the chart major version. Check the chart `CHANGELOG` before upgrading
across major versions.
