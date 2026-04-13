use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{Html, IntoResponse, Json};
use axum::routing::{get, post};
use axum::Router;
use serde::Serialize;
use tokio::net::TcpListener;
use tower_http::trace::TraceLayer;

use crate::node_runtime::NodeRuntime;

#[derive(Debug, Serialize)]
struct HealthResponse {
    status: &'static str,
}

#[derive(Debug, Serialize)]
struct CommandResponse {
    status: &'static str,
    message: &'static str,
}

pub async fn serve(
    runtime: Arc<NodeRuntime>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let bind_addr = runtime.api_bind_addr();
    let app = Router::new()
        .route("/", get(dashboard))
        .route("/dashboard", get(dashboard))
        .route("/health", get(health))
        .route("/api/status", get(status))
        .route("/api/commands", post(commands_placeholder))
        .layer(TraceLayer::new_for_http())
        .with_state(runtime);

    let listener = TcpListener::bind(bind_addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

async fn health() -> Json<HealthResponse> {
    Json(HealthResponse { status: "ok" })
}

async fn status(
    State(runtime): State<Arc<NodeRuntime>>,
) -> Json<crate::node_runtime::NodeStatusSnapshot> {
    Json(runtime.snapshot().await)
}

async fn commands_placeholder() -> impl IntoResponse {
    (
        StatusCode::NOT_IMPLEMENTED,
        Json(CommandResponse {
            status: "not_implemented",
            message: "Prompt-driven operator commands are not wired yet. Start with the read-only dashboard and status API.",
        }),
    )
}

async fn dashboard(State(runtime): State<Arc<NodeRuntime>>) -> Html<String> {
    let snapshot = runtime.snapshot().await;
    let latest_height = snapshot
        .latest_block_height
        .map(|value| value.to_string())
        .unwrap_or_else(|| "unknown".to_string());
    let latest_hash = snapshot
        .latest_block_hash
        .clone()
        .unwrap_or_else(|| "unknown".to_string());
    let db_size = snapshot
        .db_size_bytes
        .map(format_bytes)
        .unwrap_or_else(|| "unknown".to_string());
    let peer_addresses = if snapshot.peer_addresses.is_empty() {
        "<li>No active peers yet</li>".to_string()
    } else {
        snapshot
            .peer_addresses
            .iter()
            .map(|addr| format!("<li>{}</li>", addr))
            .collect::<Vec<_>>()
            .join("")
    };
    let data_gaps = snapshot
        .data_gaps
        .iter()
        .map(|item| format!("<li>{}</li>", item))
        .collect::<Vec<_>>()
        .join("");

    Html(format!(
        r#"<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Kumquat Node Dashboard</title>
  <style>
    :root {{
      color-scheme: light;
      --bg: #f4efe4;
      --panel: #fffaf2;
      --ink: #1d1a17;
      --accent: #d96b1f;
      --border: #d8c6ab;
      --muted: #6f665c;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
      color: var(--ink);
      background:
        radial-gradient(circle at top left, rgba(217,107,31,0.14), transparent 28%),
        linear-gradient(180deg, #fbf6ec 0%, var(--bg) 100%);
    }}
    main {{
      max-width: 1100px;
      margin: 0 auto;
      padding: 32px 20px 48px;
    }}
    h1, h2 {{
      margin: 0 0 12px;
      letter-spacing: 0.02em;
    }}
    p {{
      margin: 0;
      color: var(--muted);
    }}
    .grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
      gap: 16px;
      margin-top: 24px;
    }}
    .panel {{
      background: var(--panel);
      border: 1px solid var(--border);
      border-radius: 18px;
      padding: 18px;
      box-shadow: 0 16px 40px rgba(29,26,23,0.05);
    }}
    .label {{
      display: block;
      margin-bottom: 8px;
      color: var(--muted);
      font-size: 12px;
      text-transform: uppercase;
    }}
    .value {{
      font-size: 28px;
      color: var(--accent);
      word-break: break-word;
    }}
    .stack {{
      display: grid;
      gap: 16px;
      margin-top: 16px;
    }}
    ul {{
      margin: 0;
      padding-left: 18px;
      color: var(--muted);
    }}
    code {{
      color: var(--accent);
    }}
  </style>
</head>
<body>
  <main>
    <section class="panel">
      <h1>{node_name}</h1>
      <p>Embedded Rust control plane for the Kumquat node. JSON status is available at <code>/api/status</code>.</p>
    </section>
    <section class="grid">
      <article class="panel"><span class="label">Block Height</span><div class="value">{latest_height}</div></article>
      <article class="panel"><span class="label">Mining</span><div class="value">{mining_status}</div></article>
      <article class="panel"><span class="label">Peers</span><div class="value">{peer_count}</div></article>
      <article class="panel"><span class="label">Mempool</span><div class="value">{mempool_size}</div></article>
      <article class="panel"><span class="label">Uptime</span><div class="value">{uptime_seconds}s</div></article>
      <article class="panel"><span class="label">Database Size</span><div class="value">{db_size}</div></article>
    </section>
    <section class="grid">
      <article class="panel">
        <h2>Node</h2>
        <div class="stack">
          <p>Chain identity: <code>{chain_identity}</code></p>
          <p>Chain ID: <code>{chain_id}</code></p>
          <p>Expected genesis: <code>{expected_genesis_hash}</code></p>
          <p>Active genesis: <code>{active_genesis_hash}</code></p>
          <p>Configured genesis: <code>{configured_genesis_hash}</code></p>
          <p>Genesis config: <code>{genesis_config_path}</code></p>
          <p>API: <code>{api_bind_addr}</code></p>
          <p>P2P: <code>{network_bind_addr}</code></p>
          <p>Data dir: <code>{data_dir}</code></p>
          <p>DB path: <code>{db_path}</code></p>
          <p>Sync status: <code>{sync_status}</code></p>
          <p>Sync progress: <code>{sync_progress}</code></p>
        </div>
      </article>
      <article class="panel">
        <h2>Latest Block</h2>
        <div class="stack">
          <p>Height: <code>{latest_height}</code></p>
          <p>Hash: <code>{latest_hash}</code></p>
          <p>Timestamp: <code>{latest_timestamp}</code></p>
          <p>Last mined height: <code>{last_mined_height}</code></p>
        </div>
      </article>
    </section>
    <section class="grid">
      <article class="panel">
        <h2>Active Peers</h2>
        <ul>{peer_addresses}</ul>
      </article>
      <article class="panel">
        <h2>Missing Instrumentation</h2>
        <ul>{data_gaps}</ul>
      </article>
    </section>
  </main>
</body>
</html>"#,
        node_name = snapshot.node_name,
        latest_height = latest_height,
        mining_status = snapshot.mining.status,
        peer_count = snapshot.peer_count,
        mempool_size = snapshot.mempool_size,
        uptime_seconds = snapshot.uptime_seconds,
        db_size = db_size,
        chain_identity = snapshot.chain_identity,
        chain_id = snapshot.chain_id,
        expected_genesis_hash = snapshot.expected_genesis_hash,
        active_genesis_hash = snapshot
            .active_genesis_hash
            .clone()
            .unwrap_or_else(|| "unknown".to_string()),
        configured_genesis_hash = snapshot
            .configured_genesis_hash
            .clone()
            .unwrap_or_else(|| "not pinned in config".to_string()),
        genesis_config_path = snapshot.genesis_config_path,
        api_bind_addr = snapshot.api_bind_addr,
        network_bind_addr = snapshot.network_bind_addr,
        data_dir = snapshot.data_dir,
        db_path = snapshot.db_path,
        sync_status = snapshot.sync.status,
        sync_progress = snapshot
            .sync
            .progress_percent
            .map(|value| format!(
                "{value:.1}% ({}/{})",
                snapshot.sync.current_height, snapshot.sync.target_height
            ))
            .unwrap_or_else(|| format!(
                "{}/{}",
                snapshot.sync.current_height, snapshot.sync.target_height
            )),
        latest_hash = latest_hash,
        latest_timestamp = snapshot
            .latest_block_timestamp
            .map(|value| value.to_string())
            .unwrap_or_else(|| "unknown".to_string()),
        last_mined_height = snapshot
            .mining
            .last_mined_block_height
            .map(|value| value.to_string())
            .unwrap_or_else(|| "unknown".to_string()),
        peer_addresses = peer_addresses,
        data_gaps = data_gaps,
    ))
}

fn format_bytes(bytes: u64) -> String {
    const KB: f64 = 1024.0;
    const MB: f64 = KB * 1024.0;
    const GB: f64 = MB * 1024.0;
    let value = bytes as f64;

    if value >= GB {
        format!("{value:.2} GB", value = value / GB)
    } else if value >= MB {
        format!("{value:.2} MB", value = value / MB)
    } else if value >= KB {
        format!("{value:.2} KB", value = value / KB)
    } else {
        format!("{bytes} B")
    }
}
