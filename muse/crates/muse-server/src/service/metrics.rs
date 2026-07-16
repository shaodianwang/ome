//! Fleet metrics: a background refresher polls each cluster's Prometheus for the
//! curated dashboard query set — one poll per interval, regardless of how many
//! consoles are open — builds a snapshot, and publishes it. The console subscribes
//! to `GET /v1/metrics/stream` (SSE) and never polls; `GET /v1/metrics` serves the
//! latest snapshot for initial load.
//!
//! MUSE holds no Prometheus credential: each query rides the cluster's own kube
//! client via the apiserver `services/proxy` subresource, so access is exactly the
//! RBAC that client already has. The target Prometheus (`namespace/name:port`) and
//! the query set are both config-driven (`--prom-service`, `--metrics-queries-file`).

use std::{convert::Infallible, sync::Arc};

use axum::{
    extract::State,
    http::Request,
    response::sse::{Event as SseEvent, KeepAlive, Sse},
    Json,
};
use serde_json::{json, Value};
use tokio_stream::{wrappers::WatchStream, StreamExt};

use crate::{
    app_context::AppState,
    config::{PromTarget, QueryDef, QueryType},
    informers::now_secs,
};

/// `GET /v1/metrics` — the latest metrics snapshot (initial load / non-SSE clients).
pub async fn snapshot(State(st): State<AppState>) -> Json<Value> {
    Json((*st.metrics_snapshot()).clone())
}

/// `GET /v1/metrics/stream` — SSE: the current snapshot on connect, then a fresh
/// one on every background refresh, so the console updates without polling. The
/// `watch` receiver yields the current value immediately (flushing the response
/// headers), then each subsequent snapshot.
pub async fn stream(
    State(st): State<AppState>,
) -> Sse<impl tokio_stream::Stream<Item = Result<SseEvent, Infallible>>> {
    let live = WatchStream::new(st.metrics_subscribe()).map(|snap| {
        let sse = SseEvent::default()
            .json_data(&*snap)
            .unwrap_or_else(|_| SseEvent::default().comment("encode error"));
        Ok::<_, Infallible>(sse)
    });
    // Emit a comment immediately so a buffering proxy in front (the gateway) flushes
    // the response headers and the client's `onopen` fires right away, instead of
    // holding the first snapshot — mirrors the activity stream (`events::stream`).
    let hello = tokio_stream::once(Ok::<_, Infallible>(
        SseEvent::default().comment("connected"),
    ));
    Sse::new(hello.chain(live)).keep_alive(KeepAlive::default())
}

/// Poll every cluster's Prometheus for the curated query set and publish one
/// snapshot. Called on a timer by the background refresher in `server::serve`. A
/// cluster or query that fails contributes an `{ "error": … }` entry rather than
/// failing the whole snapshot, so one bad target never blanks the dashboard.
pub async fn refresh(st: &AppState) {
    let Some(prom) = st.prom.clone() else {
        return;
    };
    let queries = st.metrics_queries();
    let mut clusters = serde_json::Map::new();
    for cluster in st.clusters.iter() {
        let mut results = serde_json::Map::new();
        for def in queries.iter() {
            results.insert(def.id.clone(), run_query(&cluster.client, &prom, def).await);
        }
        clusters.insert(cluster.name.clone(), Value::Object(results));
    }
    st.publish_metrics(Arc::new(
        json!({ "updatedAt": now_secs(), "clusters": clusters }),
    ));
}

/// Run one query definition against a cluster's Prometheus, returning the raw
/// Prometheus JSON — or an `{ "error": … }` object on failure (never panics).
async fn run_query(client: &kube::Client, prom: &PromTarget, def: &QueryDef) -> Value {
    let proxy = |sub: &str, qs: &str| {
        format!(
            "/api/v1/namespaces/{}/services/{}:{}/proxy/api/v1/{sub}?{qs}",
            prom.namespace, prom.service, prom.port
        )
    };
    let path = match def.kind {
        QueryType::Instant => {
            let now = now_secs().to_string();
            match encode_params(&[("query", def.promql.as_str()), ("time", &now)]) {
                Ok(qs) => proxy("query", &qs),
                Err(e) => return json!({ "error": e }),
            }
        }
        QueryType::Range => {
            let (Some(window), Some(step)) = (def.window, def.step) else {
                return json!({ "error": format!("range query '{}' has no window or step", def.id) });
            };
            let end = now_secs();
            let start = end.saturating_sub(window);
            let step = format!("{step}s");
            match encode_params(&[
                ("query", def.promql.as_str()),
                ("start", &start.to_string()),
                ("end", &end.to_string()),
                ("step", &step),
            ]) {
                Ok(qs) => proxy("query_range", &qs),
                Err(e) => return json!({ "error": e }),
            }
        }
    };
    match prom_get(client, &path).await {
        Ok(v) => v,
        Err(e) => json!({ "error": e }),
    }
}

/// GET an apiserver service-proxy path on a cluster's client, decoding the
/// Prometheus JSON body.
async fn prom_get(client: &kube::Client, path: &str) -> Result<Value, String> {
    let req = Request::builder()
        .method("GET")
        .uri(path)
        .body(Vec::new())
        .map_err(|e| format!("build metrics request: {e}"))?;
    client
        .request::<Value>(req)
        .await
        .map_err(|e| format!("prometheus query failed: {e}"))
}

/// Percent-encode PromQL params into a query string, reusing reqwest's URL encoder
/// (already a dependency) rather than pulling in a separate one.
fn encode_params(pairs: &[(&str, &str)]) -> Result<String, String> {
    let url = reqwest::Url::parse_with_params("http://prom.invalid/", pairs)
        .map_err(|e| format!("encode query: {e}"))?;
    Ok(url.query().unwrap_or_default().to_owned())
}
