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

