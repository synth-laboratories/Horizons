-- Horizons central platform schema (Postgres).
-- All tables are tenant-scoped by org_id.

CREATE TABLE IF NOT EXISTS orgs (
  org_id UUID PRIMARY KEY,
  name TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS users (
  org_id UUID NOT NULL REFERENCES orgs(org_id) ON DELETE CASCADE,
  user_id UUID NOT NULL,
  email TEXT NOT NULL,
  display_name TEXT,
  role TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (org_id, user_id),
  UNIQUE (org_id, email),
  CONSTRAINT users_role_check CHECK (role IN ('admin', 'member', 'read_only'))
);

CREATE TABLE IF NOT EXISTS projects (
  org_id UUID NOT NULL REFERENCES orgs(org_id) ON DELETE CASCADE,
  project_id UUID NOT NULL,
  slug TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (org_id, project_id),
  UNIQUE (org_id, slug)
);

CREATE INDEX IF NOT EXISTS projects_org_created_at_idx ON projects(org_id, created_at DESC);

CREATE TABLE IF NOT EXISTS platform_config (
  org_id UUID PRIMARY KEY REFERENCES orgs(org_id) ON DELETE CASCADE,
  data JSONB NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS audit_log (
  id UUID PRIMARY KEY,
  org_id UUID NOT NULL REFERENCES orgs(org_id) ON DELETE CASCADE,
  project_id UUID,
  actor JSONB NOT NULL,
  action TEXT NOT NULL,
  payload JSONB NOT NULL,
  outcome TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS audit_log_org_created_at_idx ON audit_log(org_id, created_at DESC);

CREATE TABLE IF NOT EXISTS connector_credentials (
  org_id UUID NOT NULL REFERENCES orgs(org_id) ON DELETE CASCADE,
  connector_id TEXT NOT NULL,
  ciphertext BYTEA NOT NULL,
  created_at TIMESTAMPTZ NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (org_id, connector_id)
);

-- Sync state is split into two tables to ensure uniqueness semantics for optional project_id.
CREATE TABLE IF NOT EXISTS sync_state_org (
  org_id UUID NOT NULL REFERENCES orgs(org_id) ON DELETE CASCADE,
  connector_id TEXT NOT NULL,
  scope TEXT NOT NULL,
  cursor JSONB NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (org_id, connector_id, scope)
);

CREATE TABLE IF NOT EXISTS sync_state_project (
  org_id UUID NOT NULL REFERENCES orgs(org_id) ON DELETE CASCADE,
  project_id UUID NOT NULL,
  connector_id TEXT NOT NULL,
  scope TEXT NOT NULL,
  cursor JSONB NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (org_id, project_id, connector_id, scope)
);

CREATE TABLE IF NOT EXISTS project_dbs (
  org_id UUID NOT NULL REFERENCES orgs(org_id) ON DELETE CASCADE,
  project_id UUID NOT NULL,
  connection_url TEXT NOT NULL,
  auth_token TEXT,
  created_at TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (org_id, project_id)
);

-- ----------------------------------------------------------------------------
-- Core Agents (System 2): durable agent registry + scheduling state.
-- ----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS core_agents (
  org_id UUID NOT NULL REFERENCES orgs(org_id) ON DELETE CASCADE,
  project_id UUID NOT NULL,
  agent_id TEXT NOT NULL,
  name TEXT NOT NULL,
  sandbox_image TEXT,
  tools JSONB NOT NULL,
  schedule_type TEXT,
  schedule JSONB,
  enabled BOOLEAN NOT NULL,
  next_run_at TIMESTAMPTZ,
  config JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (org_id, project_id, agent_id)
);

CREATE INDEX IF NOT EXISTS core_agents_org_project_idx ON core_agents(org_id, project_id);

CREATE TABLE IF NOT EXISTS core_agent_event_cursors (
  org_id UUID NOT NULL REFERENCES orgs(org_id) ON DELETE CASCADE,
  project_id UUID NOT NULL,
  agent_id TEXT NOT NULL,
  topic TEXT NOT NULL,
  last_seen_received_at TIMESTAMPTZ NOT NULL,
  last_seen_event_id TEXT NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (org_id, project_id, agent_id, topic)
);

CREATE INDEX IF NOT EXISTS core_agent_event_cursors_org_project_idx
  ON core_agent_event_cursors(org_id, project_id);

CREATE TABLE IF NOT EXISTS source_configs (
  org_id UUID NOT NULL REFERENCES orgs(org_id) ON DELETE CASCADE,
  source_id TEXT NOT NULL,
  project_id UUID NOT NULL,
  connector_id TEXT NOT NULL,
  scope TEXT NOT NULL,
  enabled BOOLEAN NOT NULL,
  project_db_url TEXT NOT NULL,
  project_db_token TEXT,
  schedule_expr TEXT,
  schedule_next_run_at TIMESTAMPTZ,
  event_triggers JSONB NOT NULL,
  processor JSONB NOT NULL DEFAULT '{"type":"connector"}',
  settings JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (org_id, source_id)
);

CREATE INDEX IF NOT EXISTS source_configs_org_project_idx ON source_configs(org_id, project_id);

CREATE TABLE IF NOT EXISTS refresh_runs (
  run_id UUID PRIMARY KEY,
  org_id UUID NOT NULL REFERENCES orgs(org_id) ON DELETE CASCADE,
  project_id UUID NOT NULL,
  source_id TEXT NOT NULL,
  connector_id TEXT NOT NULL,
  trigger JSONB NOT NULL,
  status TEXT NOT NULL,
  started_at TIMESTAMPTZ NOT NULL,
  finished_at TIMESTAMPTZ,
  records_pulled BIGINT NOT NULL,
  entities_stored BIGINT NOT NULL,
  error_message TEXT,
  cursor JSONB,
  CONSTRAINT refresh_runs_status_check CHECK (status IN ('running', 'succeeded', 'failed'))
);

CREATE INDEX IF NOT EXISTS refresh_runs_org_started_at_idx ON refresh_runs(org_id, started_at DESC);

-- ----------------------------------------------------------------------------
-- Managed assets: Resources + Operations (Retool-style non-UI core)
--
-- These power governed outbound/integration calls (e.g. Event Sync subscriptions)
-- and provide a stable registry for endpoints, env separation, and auditing.
-- ----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS resources (
  org_id UUID NOT NULL REFERENCES orgs(org_id) ON DELETE CASCADE,
  resource_id TEXT NOT NULL,
  resource_type TEXT NOT NULL,
  config JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (org_id, resource_id)
);

CREATE TABLE IF NOT EXISTS operations (
  org_id UUID NOT NULL REFERENCES orgs(org_id) ON DELETE CASCADE,
  operation_id TEXT NOT NULL,
  resource_id TEXT NOT NULL,
  operation_type TEXT NOT NULL,
  config JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (org_id, operation_id)
);

CREATE INDEX IF NOT EXISTS operations_org_resource_idx ON operations(org_id, resource_id);

CREATE TABLE IF NOT EXISTS operation_runs (
  id UUID PRIMARY KEY,
  org_id UUID NOT NULL REFERENCES orgs(org_id) ON DELETE CASCADE,
  operation_id TEXT NOT NULL,
  source_event_id TEXT,
  status TEXT NOT NULL,
  error TEXT,
  output JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS operation_runs_org_created_at_idx ON operation_runs(org_id, created_at DESC);

-- ----------------------------------------------------------------------------
-- API keys (bearer tokens) for authentication.
--
-- Token format (recommended): `hzn_<key_id>.<secret>` where:
-- - key_id is a UUID
-- - secret is a random opaque string (stored hashed)
-- ----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS api_keys (
  key_id UUID PRIMARY KEY,
  org_id UUID NOT NULL REFERENCES orgs(org_id) ON DELETE CASCADE,
  name TEXT NOT NULL,
  actor JSONB NOT NULL,
  scopes JSONB NOT NULL,
  secret_hash TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL,
  expires_at TIMESTAMPTZ,
  last_used_at TIMESTAMPTZ,
  UNIQUE (org_id, name)
);

CREATE INDEX IF NOT EXISTS api_keys_org_idx ON api_keys(org_id);
