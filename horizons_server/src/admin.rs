use crate::server::AppState;
use axum::http::StatusCode;
use axum::response::{Html, IntoResponse, Response};
use axum::routing::get;
use axum::{Extension, Router};
use std::sync::Arc;

pub fn router() -> Router {
    Router::new()
        .route("/admin", get(index))
        .route("/admin/", get(index))
        .route("/admin/agents", get(agents))
        .route("/admin/graphs", get(graphs))
}

fn admin_guard(headers: &axum::http::HeaderMap) -> Result<(), Response> {
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
        return Err((StatusCode::UNAUTHORIZED, "unauthorized").into_response());
    }

    Ok(())
}

fn page_shell(title: &str, body: String) -> String {
    // Keep this self-contained: no external assets, no WASM/hydration required.
    format!(
        r#"<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width,initial-scale=1" />
  <title>{title}</title>
  <style>
    :root {{
      --bg0: #0c1016;
      --bg1: #0f1722;
      --panel: rgba(255, 255, 255, 0.06);
      --panel2: rgba(255, 255, 255, 0.09);
      --line: rgba(255, 255, 255, 0.12);
      --text: rgba(255, 255, 255, 0.92);
      --muted: rgba(255, 255, 255, 0.68);
      --faint: rgba(255, 255, 255, 0.52);
      --accent: #63e6be;
      --accent2: #74c0fc;
      --danger: #ff6b6b;
      --radius: 16px;
    }}
    html, body {{ height: 100%; }}
    body {{
      margin: 0;
      color: var(--text);
      background:
        radial-gradient(900px 500px at 20% 10%, rgba(116, 192, 252, 0.25), transparent 60%),
        radial-gradient(700px 400px at 80% 20%, rgba(99, 230, 190, 0.22), transparent 55%),
        linear-gradient(180deg, var(--bg0), var(--bg1));
      font-family: ui-sans-serif, system-ui, -apple-system, "Segoe UI", sans-serif;
      letter-spacing: 0.2px;
    }}
    a {{ color: inherit; text-decoration: none; }}
    a:hover {{ text-decoration: underline; }}
    .wrap {{ max-width: 1180px; margin: 0 auto; padding: 28px 22px 54px; }}
    .top {{
      display: flex;
      align-items: flex-end;
      justify-content: space-between;
      gap: 16px;
      padding: 10px 6px 20px;
    }}
    .brand {{
      font-family: ui-serif, "Iowan Old Style", "Palatino Linotype", Palatino, serif;
      font-size: 28px;
      font-weight: 700;
      letter-spacing: 0.3px;
      margin: 0;
    }}
    .sub {{ margin: 6px 0 0; color: var(--muted); font-size: 14px; }}
    .nav {{
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
      justify-content: flex-end;
    }}
    .pill {{
      border: 1px solid var(--line);
      background: var(--panel);
      padding: 8px 11px;
      border-radius: 999px;
      font-size: 13px;
      color: var(--muted);
    }}
    .pill strong {{ color: var(--text); font-weight: 650; }}
    .grid {{
      display: grid;
      grid-template-columns: 320px 1fr;
      gap: 18px;
      align-items: start;
    }}
    @media (max-width: 920px) {{
      .grid {{ grid-template-columns: 1fr; }}
    }}
    .card {{
      border: 1px solid var(--line);
      background: linear-gradient(180deg, rgba(255,255,255,0.07), rgba(255,255,255,0.045));
      border-radius: var(--radius);
      overflow: hidden;
      box-shadow: 0 20px 60px rgba(0,0,0,0.38);
    }}
    .card h2 {{
      margin: 0;
      font-size: 13px;
      letter-spacing: 0.8px;
      text-transform: uppercase;
      color: var(--faint);
      padding: 16px 16px 12px;
      border-bottom: 1px solid rgba(255,255,255,0.08);
      background: rgba(0,0,0,0.18);
    }}
    .card .content {{ padding: 16px; }}
    .kvs {{ display: grid; grid-template-columns: 140px 1fr; gap: 10px 14px; }}
    .kvs .k {{ color: var(--faint); font-size: 13px; }}
    .kvs .v {{ color: var(--text); font-size: 13px; overflow-wrap: anywhere; }}
    .list {{
      margin: 0;
      padding: 0;
      list-style: none;
      display: grid;
      gap: 10px;
    }}
    .item {{
      border: 1px solid rgba(255,255,255,0.10);
      background: rgba(255,255,255,0.05);
      border-radius: 14px;
      padding: 12px 12px;
      display: flex;
      justify-content: space-between;
      gap: 12px;
      align-items: center;
    }}
    code {{
      background: rgba(0,0,0,0.25);
      border: 1px solid rgba(255,255,255,0.10);
      padding: 2px 6px;
      border-radius: 10px;
      color: rgba(255,255,255,0.9);
      font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", monospace;
      font-size: 12px;
    }}
    .muted {{ color: var(--muted); }}
    .btnrow {{ display: flex; gap: 10px; flex-wrap: wrap; margin-top: 12px; }}
    .btn {{
      display: inline-flex;
      align-items: center;
      gap: 8px;
      border-radius: 12px;
      padding: 10px 12px;
      border: 1px solid rgba(255,255,255,0.12);
      background: rgba(255,255,255,0.06);
      color: var(--text);
      font-size: 13px;
    }}
    .btn:hover {{
      background: rgba(255,255,255,0.09);
      text-decoration: none;
    }}
    .dot {{
      width: 10px; height: 10px; border-radius: 999px;
      background: var(--accent);
      box-shadow: 0 0 0 6px rgba(99,230,190,0.12);
    }}
    .dot.warn {{
      background: var(--danger);
      box-shadow: 0 0 0 6px rgba(255,107,107,0.14);
    }}
  </style>
</head>
<body>
  <div class="wrap">
    <div class="top">
      <div>
        <h1 class="brand">Horizons Admin</h1>
        <div class="sub">SSR panel (Leptos). No client JS required.</div>
      </div>
      <div class="nav">
        <a class="pill" href="/admin"><strong>Dashboard</strong></a>
        <a class="pill" href="/admin/agents">Agents</a>
        <a class="pill" href="/admin/graphs">Graphs</a>
        <a class="pill" href="/health">Health</a>
      </div>
    </div>
    {body}
  </div>
</body>
</html>"#
    )
}

fn render(title: &str, body: String) -> Html<String> {
    Html(page_shell(title, body))
}

async fn index(
    Extension(state): Extension<Arc<AppState>>,
    headers: axum::http::HeaderMap,
) -> Response {
    if let Err(resp) = admin_guard(&headers) {
        return resp;
    }

    let uptime = state.started_at.elapsed();
    let uptime_s = uptime.as_secs();

    let mcp_configured = state.mcp_gateway.is_some();
    let sandbox_configured = state.sandbox_runtime.is_some();

    let mcp = status_badge(mcp_configured, "configured", "not configured");
    let sandbox = status_badge(sandbox_configured, "configured", "not configured");

    render(
        "Horizons Admin",
        format!(
            r#"
<div class="grid">
  <div class="card">
    <h2>Server</h2>
    <div class="content">
      <div class="kvs">
        <div class="k">Version</div>
        <div class="v"><code>{}</code></div>
        <div class="k">Uptime</div>
        <div class="v">{}s</div>
        <div class="k">MCP gateway</div>
        <div class="v">{}</div>
        <div class="k">Sandbox runtime</div>
        <div class="v">{}</div>
      </div>

      <div class="btnrow">
        <a class="btn" href="/admin/agents">Browse agents</a>
        <a class="btn" href="/admin/graphs">Browse graphs</a>
        <a class="btn" href="/health">Open /health</a>
      </div>
    </div>
  </div>

  <div class="card">
    <h2>Tracing (Laminar / OTLP)</h2>
    <div class="content">
      <p class="muted">Horizons emits OpenTelemetry traces. For self-hosted Laminar (lmnr), set:</p>
      <ul class="list">
        <li class="item"><span><code>HORIZONS_LAMINAR_SELF_HOSTED=1</code></span><span class="muted">enable defaults</span></li>
        <li class="item"><span><code>LMNR_PROJECT_API_KEY=...</code></span><span class="muted">auth header</span></li>
        <li class="item"><span><code>HORIZONS_LAMINAR_OTLP_ENDPOINT=http://localhost:8000</code></span><span class="muted">override</span></li>
      </ul>
      <div class="btnrow">
        <a class="btn" href="https://github.com/lmnr-ai/lmnr" target="_blank" rel="noreferrer">Laminar repo</a>
      </div>
    </div>
  </div>
</div>
"#,
            env!("CARGO_PKG_VERSION"),
            uptime_s,
            mcp,
            sandbox
        ),
    )
    .into_response()
}

async fn agents(
    Extension(state): Extension<Arc<AppState>>,
    headers: axum::http::HeaderMap,
) -> Response {
    if let Err(resp) = admin_guard(&headers) {
        return resp;
    }

    let ids = state.core_agents.list_registered_agent_ids().await;
    let mut ids = ids;
    ids.sort();
    let count = ids.len();

    let list_html = if ids.is_empty() {
        r#"<p class="muted">No agents registered in this server process.</p>"#.to_string()
    } else {
        let items = ids
            .into_iter()
            .map(|id| {
                format!(
                    r#"<li class="item"><span><code>{}</code></span><span class="muted">registered</span></li>"#,
                    html_escape(&id)
                )
            })
            .collect::<Vec<_>>()
            .join("\n");
        format!(r#"<ul class="list">{}</ul>"#, items)
    };

    render(
        "Horizons Admin | Agents",
        format!(
            r#"
<div class="grid">
  <div class="card">
    <h2>Navigation</h2>
    <div class="content">
      <div class="btnrow">
        <a class="btn" href="/admin">Back to dashboard</a>
        <a class="btn" href="/admin/graphs">Graphs</a>
      </div>
    </div>
  </div>
  <div class="card">
    <h2>Agents ({})</h2>
    <div class="content">{}</div>
  </div>
</div>
"#,
            count, list_html
        ),
    )
    .into_response()
}

async fn graphs(headers: axum::http::HeaderMap) -> Response {
    if let Err(resp) = admin_guard(&headers) {
        return resp;
    }

    let mut graphs: Vec<String> = horizons_graph::registry::list_builtin_graphs()
        .into_iter()
        .map(|s| s.to_string())
        .collect();
    graphs.sort();
    let count = graphs.len();

    let items = graphs
        .into_iter()
        .map(|id| {
            format!(
                r#"<li class="item"><span><code>{}</code></span><span class="muted">builtin</span></li>"#,
                html_escape(&id)
            )
        })
        .collect::<Vec<_>>()
        .join("\n");

    render(
        "Horizons Admin | Graphs",
        format!(
            r#"
<div class="grid">
  <div class="card">
    <h2>Navigation</h2>
    <div class="content">
      <div class="btnrow">
        <a class="btn" href="/admin">Back to dashboard</a>
        <a class="btn" href="/admin/agents">Agents</a>
      </div>
    </div>
  </div>

  <div class="card">
    <h2>Builtin graphs ({})</h2>
    <div class="content">
      <ul class="list">{}</ul>
    </div>
  </div>
</div>
"#,
            count, items
        ),
    )
    .into_response()
}

fn status_badge(ok: bool, ok_text: &str, bad_text: &str) -> String {
    if ok {
        format!(
            r#"<span><span class="dot"></span> {}</span>"#,
            html_escape(ok_text)
        )
    } else {
        format!(
            r#"<span><span class="dot warn"></span> {}</span>"#,
            html_escape(bad_text)
        )
    }
}

fn html_escape(s: &str) -> String {
    // Minimal escaping for values we interpolate into HTML.
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#x27;")
}
