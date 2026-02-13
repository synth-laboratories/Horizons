use crate::server::AppState;
use axum::http::StatusCode;
use axum::response::{Html, IntoResponse, Response};
use axum::routing::get;
use axum::{Extension, Router};
use leptos::{Children, IntoView, component, view};
use std::sync::Arc;

// ---------------------------------------------------------------------------
// Router
// ---------------------------------------------------------------------------

pub fn router() -> Router {
    Router::new()
        .route("/admin", get(dashboard))
        .route("/admin/", get(dashboard))
        .route("/admin/agents", get(agents))
        .route("/admin/graphs", get(graphs))
        .route("/admin/backends", get(backends))
}

// ---------------------------------------------------------------------------
// Auth guard
// ---------------------------------------------------------------------------

fn admin_guard(headers: &axum::http::HeaderMap) -> Result<(), Box<Response>> {
    let token = std::env::var("HORIZONS_ADMIN_TOKEN").ok();
    let Some(expected) = token.filter(|t| !t.trim().is_empty()) else {
        return Ok(());
    };

    let mut got: Option<String> = None;

    if let Some(v) = headers
        .get("x-horizons-admin-token")
        .and_then(|v| v.to_str().ok())
    {
        got = Some(v.to_string());
    }

    if got.is_none() {
        if let Some(v) = headers
            .get(axum::http::header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
        {
            got = v
                .strip_prefix("Bearer ")
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty());
        }
    }

    if got.as_deref() != Some(expected.trim()) {
        return Err(Box::new(
            (StatusCode::UNAUTHORIZED, "unauthorized").into_response(),
        ));
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Axum handlers
// ---------------------------------------------------------------------------

async fn dashboard(
    Extension(state): Extension<Arc<AppState>>,
    headers: axum::http::HeaderMap,
) -> Response {
    if let Err(resp) = admin_guard(&headers) {
        return *resp;
    }

    let version = env!("CARGO_PKG_VERSION").to_string();
    let uptime_s = state.started_at.elapsed().as_secs();
    let mcp_ok = state.mcp_gateway.is_some();
    let sandbox_ok = state.sandbox_runtime.is_some();
    let agent_ids = state.core_agents.list_registered_agent_ids().await;
    let agent_count = agent_ids.len();
    let graph_count = horizons_graph::registry::list_builtin_graphs().len();

    let body = leptos::ssr::render_to_string(move || {
        view! {
            <DashboardPage
                version=version
                uptime_s=uptime_s
                mcp_ok=mcp_ok
                sandbox_ok=sandbox_ok
                agent_count=agent_count
                graph_count=graph_count
            />
        }
    });

    render_page("Dashboard", "dashboard", &body)
}

async fn agents(
    Extension(state): Extension<Arc<AppState>>,
    headers: axum::http::HeaderMap,
) -> Response {
    if let Err(resp) = admin_guard(&headers) {
        return *resp;
    }

    let mut ids = state.core_agents.list_registered_agent_ids().await;
    ids.sort();

    let body = leptos::ssr::render_to_string(move || {
        view! { <AgentsPage ids=ids /> }
    });

    render_page("Agents", "agents", &body)
}

async fn graphs(headers: axum::http::HeaderMap) -> Response {
    if let Err(resp) = admin_guard(&headers) {
        return *resp;
    }

    let mut items: Vec<String> = horizons_graph::registry::list_builtin_graphs()
        .into_iter()
        .map(|s| s.to_string())
        .collect();
    items.sort();

    let body = leptos::ssr::render_to_string(move || {
        view! { <GraphsPage items=items /> }
    });

    render_page("Graphs", "graphs", &body)
}

async fn backends(
    Extension(state): Extension<Arc<AppState>>,
    headers: axum::http::HeaderMap,
) -> Response {
    if let Err(resp) = admin_guard(&headers) {
        return *resp;
    }

    let db_url = std::env::var("DATABASE_URL")
        .or_else(|_| std::env::var("HORIZONS_CENTRAL_DB_URL"))
        .ok()
        .map(|u| redact_url(&u));
    let redis_url = std::env::var("REDIS_URL")
        .or_else(|_| std::env::var("HORIZONS_REDIS_URL"))
        .ok()
        .map(|u| redact_url(&u));
    let vector_url = std::env::var("HORIZONS_VECTOR_DB_URL")
        .ok()
        .map(|u| redact_url(&u));
    let helix_url = std::env::var("HORIZONS_HELIX_URL").ok();
    let s3_bucket = std::env::var("HORIZONS_S3_BUCKET").ok();
    let sandbox_backend = std::env::var("HORIZONS_SANDBOX_BACKEND").ok();
    let mcp_ok = state.mcp_gateway.is_some();
    let sandbox_ok = state.sandbox_runtime.is_some();

    let body = leptos::ssr::render_to_string(move || {
        view! {
            <BackendsPage
                db_url=db_url
                redis_url=redis_url
                vector_url=vector_url
                helix_url=helix_url
                s3_bucket=s3_bucket
                sandbox_backend=sandbox_backend
                mcp_ok=mcp_ok
                sandbox_ok=sandbox_ok
            />
        }
    });

    render_page("Backends", "backends", &body)
}

// ---------------------------------------------------------------------------
// Render helpers
// ---------------------------------------------------------------------------

fn redact_url(s: &str) -> String {
    // Show scheme + host, hide credentials and path details.
    if let Some(at) = s.find('@') {
        let scheme_end = s.find("://").map(|i| i + 3).unwrap_or(0);
        format!("{}***@{}", &s[..scheme_end], &s[at + 1..])
    } else if s.len() > 20 {
        format!("{}...{}", &s[..10], &s[s.len() - 8..])
    } else {
        s.to_string()
    }
}

fn render_page(title: &str, active: &str, body: &str) -> Response {
    let shell = page_shell(title, active, body);
    Html(shell).into_response()
}

// ---------------------------------------------------------------------------
// Page shell (plain HTML — wraps the Leptos-rendered body)
// ---------------------------------------------------------------------------

fn page_shell(title: &str, active: &str, body: &str) -> String {
    let nav_items = [
        ("dashboard", "Dashboard", "/admin"),
        ("agents", "Agents", "/admin/agents"),
        ("graphs", "Graphs", "/admin/graphs"),
        ("backends", "Backends", "/admin/backends"),
    ];

    let nav_html: String = nav_items
        .iter()
        .map(|(id, label, href)| {
            let cls = if *id == active {
                "nav-link active"
            } else {
                "nav-link"
            };
            format!(r#"<a class="{cls}" href="{href}">{label}</a>"#)
        })
        .collect::<Vec<_>>()
        .join("\n        ");

    format!(
        r#"<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width,initial-scale=1" />
  <title>Horizons | {title}</title>
  {STYLES}
</head>
<body>
  <div class="shell">
    <header class="topbar">
      <div class="topbar-inner">
        <a href="/admin" class="logo">
          <svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
            <circle cx="12" cy="12" r="10"/>
            <path d="M12 2a15.3 15.3 0 0 1 4 10 15.3 15.3 0 0 1-4 10 15.3 15.3 0 0 1-4-10 15.3 15.3 0 0 1 4-10z"/>
            <path d="M2 12h20"/>
          </svg>
          <span>Horizons</span>
        </a>
        <nav class="nav">
          {nav_html}
          <a class="nav-link" href="/health" target="_blank">/health</a>
        </nav>
      </div>
    </header>
    <main class="main">
      {body}
    </main>
  </div>
</body>
</html>"#
    )
}

// ---------------------------------------------------------------------------
// Leptos components
// ---------------------------------------------------------------------------

#[component]
fn StatusDot(ok: bool) -> impl IntoView {
    let cls = if ok { "dot ok" } else { "dot warn" };
    view! { <span class=cls></span> }
}

#[component]
fn Card(title: &'static str, children: Children) -> impl IntoView {
    view! {
        <div class="card">
            <div class="card-header">{title}</div>
            <div class="card-body">
                {children()}
            </div>
        </div>
    }
}

#[component]
fn Kv(label: &'static str, children: Children) -> impl IntoView {
    view! {
        <div class="kv">
            <span class="kv-label">{label}</span>
            <span class="kv-value">{children()}</span>
        </div>
    }
}

#[component]
fn Stat(label: &'static str, value: String) -> impl IntoView {
    view! {
        <div class="stat">
            <div class="stat-value">{value}</div>
            <div class="stat-label">{label}</div>
        </div>
    }
}

#[component]
fn EmptyState(message: &'static str) -> impl IntoView {
    view! {
        <div class="empty">
            <p>{message}</p>
        </div>
    }
}

// ---------------------------------------------------------------------------
// Pages
// ---------------------------------------------------------------------------

#[component]
fn DashboardPage(
    version: String,
    uptime_s: u64,
    mcp_ok: bool,
    sandbox_ok: bool,
    agent_count: usize,
    graph_count: usize,
) -> impl IntoView {
    let uptime_display = format_uptime(uptime_s);

    view! {
        <div class="page-header">
            <h1>"Dashboard"</h1>
            <p class="subtitle">"Server overview and quick status."</p>
        </div>

        <div class="stats-row">
            <Stat label="Version" value=version.clone() />
            <Stat label="Uptime" value=uptime_display />
            <Stat label="Agents" value=agent_count.to_string() />
            <Stat label="Graphs" value=graph_count.to_string() />
        </div>

        <div class="grid-2">
            <Card title="Server">
                <Kv label="Version">
                    <code>{version}</code>
                </Kv>
                <Kv label="MCP Gateway">
                    <StatusDot ok=mcp_ok />
                    {if mcp_ok { " configured" } else { " not configured" }}
                </Kv>
                <Kv label="Sandbox Runtime">
                    <StatusDot ok=sandbox_ok />
                    {if sandbox_ok { " configured" } else { " not configured" }}
                </Kv>
            </Card>

            <Card title="Quick Links">
                <div class="link-grid">
                    <a class="link-card" href="/admin/agents">
                        <span class="link-card-title">"Agents"</span>
                        <span class="link-card-desc">"View registered agents"</span>
                    </a>
                    <a class="link-card" href="/admin/graphs">
                        <span class="link-card-title">"Graphs"</span>
                        <span class="link-card-desc">"Builtin verifier registry"</span>
                    </a>
                    <a class="link-card" href="/admin/backends">
                        <span class="link-card-title">"Backends"</span>
                        <span class="link-card-desc">"Database & service config"</span>
                    </a>
                    <a class="link-card" href="/health" target="_blank">
                        <span class="link-card-title">"Health"</span>
                        <span class="link-card-desc">"JSON health endpoint"</span>
                    </a>
                </div>
            </Card>
        </div>

        <Card title="Observability (OTLP / Laminar)">
            <p class="muted">"Horizons emits OpenTelemetry traces. Point them at any OTLP collector or self-hosted Laminar:"</p>
            <div class="env-list">
                <div class="env-item">
                    <code>"HORIZONS_LAMINAR_SELF_HOSTED=1"</code>
                    <span class="muted">"enable Laminar defaults"</span>
                </div>
                <div class="env-item">
                    <code>"LMNR_PROJECT_API_KEY=..."</code>
                    <span class="muted">"auth header"</span>
                </div>
                <div class="env-item">
                    <code>"HORIZONS_LAMINAR_OTLP_ENDPOINT=http://localhost:8000"</code>
                    <span class="muted">"endpoint override"</span>
                </div>
            </div>
        </Card>
    }
}

#[component]
fn AgentsPage(ids: Vec<String>) -> impl IntoView {
    let count = ids.len();

    view! {
        <div class="page-header">
            <h1>"Agents"</h1>
            <p class="subtitle">{format!("{count} registered in this server process.")}</p>
        </div>

        {if ids.is_empty() {
            view! { <EmptyState message="No agents registered. Register agents via the SDK or by implementing the AgentSpec trait." /> }.into_view()
        } else {
            view! {
                <div class="table-card">
                    <table>
                        <thead>
                            <tr>
                                <th>"#"</th>
                                <th>"Agent ID"</th>
                                <th>"Status"</th>
                            </tr>
                        </thead>
                        <tbody>
                            {ids.into_iter().enumerate().map(|(i, id)| {
                                view! {
                                    <tr>
                                        <td class="num">{i + 1}</td>
                                        <td><code>{id}</code></td>
                                        <td><StatusDot ok=true />" registered"</td>
                                    </tr>
                                }
                            }).collect::<Vec<_>>()}
                        </tbody>
                    </table>
                </div>
            }.into_view()
        }}
    }
}

#[component]
fn GraphsPage(items: Vec<String>) -> impl IntoView {
    let count = items.len();

    view! {
        <div class="page-header">
            <h1>"Builtin Graphs"</h1>
            <p class="subtitle">{format!("{count} graphs in the verifier registry.")}</p>
        </div>

        {if items.is_empty() {
            view! { <EmptyState message="No builtin graphs found. Compile with --features all to include the verifier graph registry." /> }.into_view()
        } else {
            view! {
                <div class="table-card">
                    <table>
                        <thead>
                            <tr>
                                <th>"#"</th>
                                <th>"Graph ID"</th>
                                <th>"Source"</th>
                            </tr>
                        </thead>
                        <tbody>
                            {items.into_iter().enumerate().map(|(i, id)| {
                                view! {
                                    <tr>
                                        <td class="num">{i + 1}</td>
                                        <td><code>{id}</code></td>
                                        <td class="muted">"builtin"</td>
                                    </tr>
                                }
                            }).collect::<Vec<_>>()}
                        </tbody>
                    </table>
                </div>
            }.into_view()
        }}
    }
}

#[component]
fn BackendsPage(
    db_url: Option<String>,
    redis_url: Option<String>,
    vector_url: Option<String>,
    helix_url: Option<String>,
    s3_bucket: Option<String>,
    sandbox_backend: Option<String>,
    mcp_ok: bool,
    sandbox_ok: bool,
) -> impl IntoView {
    // Pre-compute display values so we don't move Options into multiple closures.
    let db_ok = db_url.is_some();
    let db_display = match &db_url {
        Some(u) => format!(" {u}"),
        None => " not configured (using SQLite)".to_string(),
    };
    let redis_ok = redis_url.is_some();
    let redis_display = match &redis_url {
        Some(u) => format!(" {u}"),
        None => " not configured (using in-memory)".to_string(),
    };
    let vector_ok = vector_url.is_some() || db_url.is_some();
    let vector_display = match &vector_url {
        Some(u) => format!(" {u}"),
        None if db_url.is_some() => " using central DB (pgvector)".to_string(),
        None => " not configured (using in-memory)".to_string(),
    };
    let helix_ok = helix_url.is_some();
    let helix_display = match &helix_url {
        Some(u) => format!(" {u}"),
        None => " not configured".to_string(),
    };
    let s3_ok = s3_bucket.is_some();
    let s3_display = match &s3_bucket {
        Some(b) => format!(" s3://{b}"),
        None => " not configured (using local filesystem)".to_string(),
    };
    let sandbox_display = match &sandbox_backend {
        Some(b) => format!(" {b}"),
        None => " not configured".to_string(),
    };

    view! {
        <div class="page-header">
            <h1>"Backends"</h1>
            <p class="subtitle">"Configured storage and service backends."</p>
        </div>

        <div class="grid-2">
            <Card title="Databases">
                <Kv label="Central DB (PG)">
                    <StatusDot ok=db_ok />
                    {db_display}
                </Kv>
                <Kv label="Redis">
                    <StatusDot ok=redis_ok />
                    {redis_display}
                </Kv>
                <Kv label="Vector Store">
                    <StatusDot ok=vector_ok />
                    {vector_display}
                </Kv>
            </Card>

            <Card title="Services">
                <Kv label="MCP Gateway">
                    <StatusDot ok=mcp_ok />
                    {if mcp_ok { " active" } else { " not configured" }}
                </Kv>
                <Kv label="Sandbox Runtime">
                    <StatusDot ok=sandbox_ok />
                    {sandbox_display}
                </Kv>
                <Kv label="Helix (Graph DB)">
                    <StatusDot ok=helix_ok />
                    {helix_display}
                </Kv>
                <Kv label="Object Storage">
                    <StatusDot ok=s3_ok />
                    {s3_display}
                </Kv>
            </Card>
        </div>

        <Card title="Environment Reference">
            <p class="muted">"Key environment variables for backend configuration:"</p>
            <div class="env-list">
                <div class="env-item">
                    <code>"DATABASE_URL"</code>
                    <span class="muted">"Postgres connection string"</span>
                </div>
                <div class="env-item">
                    <code>"REDIS_URL"</code>
                    <span class="muted">"Redis connection string"</span>
                </div>
                <div class="env-item">
                    <code>"HORIZONS_VECTOR_DB_URL"</code>
                    <span class="muted">"pgvector DB (defaults to DATABASE_URL)"</span>
                </div>
                <div class="env-item">
                    <code>"HORIZONS_S3_BUCKET"</code>
                    <span class="muted">"S3-compatible bucket for filestore"</span>
                </div>
                <div class="env-item">
                    <code>"HORIZONS_SANDBOX_BACKEND"</code>
                    <span class="muted">"docker | daytona"</span>
                </div>
                <div class="env-item">
                    <code>"HORIZONS_MCP_CONFIG"</code>
                    <span class="muted">"MCP server config JSON"</span>
                </div>
            </div>
        </Card>
    }
}

// ---------------------------------------------------------------------------
// Utility
// ---------------------------------------------------------------------------

fn format_uptime(secs: u64) -> String {
    if secs < 60 {
        format!("{secs}s")
    } else if secs < 3600 {
        format!("{}m {}s", secs / 60, secs % 60)
    } else if secs < 86400 {
        format!("{}h {}m", secs / 3600, (secs % 3600) / 60)
    } else {
        format!("{}d {}h", secs / 86400, (secs % 86400) / 3600)
    }
}

// ---------------------------------------------------------------------------
// Styles (embedded CSS)
// ---------------------------------------------------------------------------

const STYLES: &str = r#"<style>
  :root {
    --bg0: #0a0e14;
    --bg1: #0d1219;
    --surface: rgba(255, 255, 255, 0.04);
    --surface2: rgba(255, 255, 255, 0.07);
    --surface3: rgba(255, 255, 255, 0.10);
    --border: rgba(255, 255, 255, 0.08);
    --border2: rgba(255, 255, 255, 0.12);
    --text: rgba(255, 255, 255, 0.92);
    --muted: rgba(255, 255, 255, 0.55);
    --faint: rgba(255, 255, 255, 0.35);
    --accent: #63e6be;
    --accent2: #74c0fc;
    --danger: #ff6b6b;
    --warning: #ffd43b;
    --radius: 12px;
    --radius-sm: 8px;
  }

  *, *::before, *::after { box-sizing: border-box; }
  html, body { height: 100%; margin: 0; }

  body {
    color: var(--text);
    background: var(--bg0);
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
    font-size: 14px;
    line-height: 1.5;
    -webkit-font-smoothing: antialiased;
  }

  a { color: inherit; text-decoration: none; }

  code {
    font-family: "SF Mono", SFMono-Regular, ui-monospace, Menlo, Monaco, Consolas, monospace;
    font-size: 12.5px;
    background: rgba(0, 0, 0, 0.3);
    border: 1px solid var(--border);
    padding: 2px 6px;
    border-radius: 6px;
    color: var(--accent2);
  }

  .muted { color: var(--muted); }

  /* Shell */
  .shell { min-height: 100vh; display: flex; flex-direction: column; }

  /* Top bar */
  .topbar {
    position: sticky;
    top: 0;
    z-index: 100;
    background: rgba(10, 14, 20, 0.85);
    backdrop-filter: blur(16px) saturate(180%);
    -webkit-backdrop-filter: blur(16px) saturate(180%);
    border-bottom: 1px solid var(--border);
  }
  .topbar-inner {
    max-width: 1200px;
    margin: 0 auto;
    padding: 0 24px;
    height: 56px;
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 24px;
  }
  .logo {
    display: flex;
    align-items: center;
    gap: 10px;
    font-weight: 700;
    font-size: 16px;
    letter-spacing: 0.3px;
    color: var(--text);
    flex-shrink: 0;
  }
  .logo svg { color: var(--accent); }
  .nav { display: flex; gap: 2px; flex-wrap: wrap; }
  .nav-link {
    padding: 6px 14px;
    border-radius: 8px;
    font-size: 13px;
    font-weight: 500;
    color: var(--muted);
    transition: all 0.15s ease;
  }
  .nav-link:hover { color: var(--text); background: var(--surface2); text-decoration: none; }
  .nav-link.active {
    color: var(--text);
    background: var(--surface3);
  }

  /* Main content */
  .main {
    flex: 1;
    max-width: 1200px;
    width: 100%;
    margin: 0 auto;
    padding: 32px 24px 64px;
  }

  /* Page header */
  .page-header { margin-bottom: 28px; }
  .page-header h1 {
    margin: 0 0 4px;
    font-size: 24px;
    font-weight: 700;
    letter-spacing: -0.3px;
  }
  .page-header .subtitle {
    margin: 0;
    color: var(--muted);
    font-size: 14px;
  }

  /* Stats row */
  .stats-row {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(140px, 1fr));
    gap: 12px;
    margin-bottom: 24px;
  }
  .stat {
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: var(--radius);
    padding: 18px 16px;
    text-align: center;
  }
  .stat-value {
    font-size: 22px;
    font-weight: 700;
    letter-spacing: -0.5px;
    color: var(--text);
    font-family: "SF Mono", SFMono-Regular, ui-monospace, monospace;
  }
  .stat-label {
    font-size: 12px;
    color: var(--muted);
    text-transform: uppercase;
    letter-spacing: 0.8px;
    margin-top: 4px;
  }

  /* Grid */
  .grid-2 {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 16px;
    margin-bottom: 16px;
  }
  @media (max-width: 768px) { .grid-2 { grid-template-columns: 1fr; } }

  /* Card */
  .card {
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: var(--radius);
    overflow: hidden;
    margin-bottom: 16px;
  }
  .card-header {
    padding: 12px 18px;
    font-size: 11px;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 1px;
    color: var(--faint);
    background: rgba(0, 0, 0, 0.2);
    border-bottom: 1px solid var(--border);
  }
  .card-body { padding: 18px; }

  /* Key-value rows */
  .kv {
    display: flex;
    align-items: center;
    gap: 12px;
    padding: 10px 0;
    border-bottom: 1px solid rgba(255, 255, 255, 0.04);
  }
  .kv:last-child { border-bottom: none; }
  .kv-label {
    flex-shrink: 0;
    width: 140px;
    font-size: 13px;
    color: var(--muted);
  }
  .kv-value {
    font-size: 13px;
    display: flex;
    align-items: center;
    gap: 6px;
  }

  /* Status dot */
  .dot {
    display: inline-block;
    width: 8px;
    height: 8px;
    border-radius: 50%;
    flex-shrink: 0;
  }
  .dot.ok {
    background: var(--accent);
    box-shadow: 0 0 0 3px rgba(99, 230, 190, 0.15);
  }
  .dot.warn {
    background: var(--danger);
    box-shadow: 0 0 0 3px rgba(255, 107, 107, 0.15);
  }

  /* Link grid */
  .link-grid {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 10px;
  }
  .link-card {
    display: flex;
    flex-direction: column;
    gap: 4px;
    padding: 14px;
    border-radius: var(--radius-sm);
    border: 1px solid var(--border);
    background: var(--surface);
    transition: all 0.15s ease;
  }
  .link-card:hover {
    background: var(--surface2);
    border-color: var(--border2);
    text-decoration: none;
  }
  .link-card-title { font-weight: 600; font-size: 13px; }
  .link-card-desc { font-size: 12px; color: var(--muted); }

  /* Env list */
  .env-list { display: flex; flex-direction: column; gap: 8px; margin-top: 12px; }
  .env-item {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 16px;
    padding: 10px 14px;
    border-radius: var(--radius-sm);
    background: rgba(0, 0, 0, 0.2);
    border: 1px solid var(--border);
  }

  /* Table */
  .table-card {
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: var(--radius);
    overflow: hidden;
  }
  table { width: 100%; border-collapse: collapse; }
  thead { background: rgba(0, 0, 0, 0.2); }
  th {
    padding: 10px 16px;
    text-align: left;
    font-size: 11px;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.8px;
    color: var(--faint);
    border-bottom: 1px solid var(--border);
  }
  td {
    padding: 12px 16px;
    font-size: 13px;
    border-bottom: 1px solid rgba(255, 255, 255, 0.03);
  }
  tr:last-child td { border-bottom: none; }
  tr:hover td { background: rgba(255, 255, 255, 0.02); }
  td.num { color: var(--faint); font-size: 12px; width: 40px; }

  /* Empty state */
  .empty {
    text-align: center;
    padding: 48px 24px;
    color: var(--muted);
  }
  .empty p { margin: 0; }
</style>"#;
