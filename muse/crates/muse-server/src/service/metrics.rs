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
use kube::{Api, Error as KubeError};
use ome_client::k8s_openapi::api::core::v1::Service;
use serde::Serialize;
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
/// missing cluster target contributes one structured source state and no query
/// results; a query failure contributes an `{ "error": … }` entry. One unconfigured
/// cluster therefore cannot look like every query independently failed.
pub async fn refresh(st: &AppState) {
    let Some(prom) = st.prom.clone() else {
        return;
    };
    let queries = st.metrics_queries();
    let mut clusters = serde_json::Map::new();
    let mut sources = serde_json::Map::new();
    for cluster in st.clusters.iter() {
        let mut results = serde_json::Map::new();
        if queries.is_empty() {
            clusters.insert(cluster.name.clone(), Value::Object(results));
            continue;
        }
        if let Err(failure) = discover_prom_service(&cluster.client, &prom).await {
            log_failure(&cluster.name, &prom, &failure);
            sources.insert(cluster.name.clone(), failure.to_source_value(&prom));
            clusters.insert(cluster.name.clone(), Value::Object(results));
            continue;
        }
        sources.insert(cluster.name.clone(), available_source_value(&prom));
        for def in queries.iter() {
            results.insert(def.id.clone(), run_query(&cluster.client, &prom, def).await);
        }
        clusters.insert(cluster.name.clone(), Value::Object(results));
    }
    st.publish_metrics(Arc::new(
        json!({ "updatedAt": now_secs(), "clusters": clusters, "sources": sources }),
    ));
}

#[derive(Clone, Debug)]
struct MetricsFailure {
    status: &'static str,
    code: &'static str,
    message: String,
    http_status: Option<u16>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct MetricsSourceError<'a> {
    code: &'a str,
    message: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    http_status: Option<u16>,
}

impl MetricsFailure {
    fn new(status: &'static str, code: &'static str, message: String) -> Self {
        Self {
            status,
            code,
            message,
            http_status: None,
        }
    }

    fn to_source_value(&self, prom: &PromTarget) -> Value {
        json!({
            "status": self.status,
            "target": prom_target(prom),
            "error": MetricsSourceError {
                code: self.code,
                message: &self.message,
                http_status: self.http_status,
            },
        })
    }
}

fn available_source_value(prom: &PromTarget) -> Value {
    json!({ "status": "available", "target": prom_target(prom) })
}

fn prom_target(prom: &PromTarget) -> String {
    format!("{}/{}:{}", prom.namespace, prom.service, prom.port)
}

async fn discover_prom_service(
    client: &kube::Client,
    prom: &PromTarget,
) -> Result<(), MetricsFailure> {
    let services: Api<Service> = Api::namespaced(client.clone(), &prom.namespace);
    let service = services
        .get(&prom.service)
        .await
        .map_err(|error| service_discovery_failure(prom, &error))?;
    validate_prom_service(&service, prom)
}

fn validate_prom_service(service: &Service, prom: &PromTarget) -> Result<(), MetricsFailure> {
    let exposes_port = service
        .spec
        .as_ref()
        .and_then(|spec| spec.ports.as_ref())
        .is_some_and(|ports| {
            ports
                .iter()
                .any(|service_port| service_port.port == i32::from(prom.port))
        });
    if exposes_port {
        Ok(())
    } else {
        let target = prom_target(prom);
        Err(MetricsFailure::new(
            "unconfigured",
            "prometheus_service_port_not_found",
            format!(
                "Prometheus Service {target} does not expose port {}",
                prom.port
            ),
        ))
    }
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

fn service_discovery_failure(prom: &PromTarget, error: &KubeError) -> MetricsFailure {
    let (status, code, http_status) = match error {
        KubeError::Api(status) if status.code == 401 || status.is_forbidden() => (
            "unavailable",
            "prometheus_service_discovery_forbidden",
            Some(status.code),
        ),
        KubeError::Api(status) if status.is_not_found() => (
            "unconfigured",
            "prometheus_service_not_found",
            Some(status.code),
        ),
        KubeError::Api(status) => (
            "unavailable",
            "prometheus_service_discovery_failed",
            (status.code != 0).then_some(status.code),
        ),
        _ => ("unavailable", "prometheus_service_discovery_failed", None),
    };
    let target = prom_target(prom);
    let mut failure = MetricsFailure::new(
        status,
        code,
        format!("failed to discover Prometheus Service {target}: {error}"),
    );
    failure.http_status = http_status;
    failure
}

fn log_failure(cluster: &str, prom: &PromTarget, failure: &MetricsFailure) {
    let target = prom_target(prom);
    tracing::warn!(
        cluster,
        target,
        error_code = failure.code,
        http_status = failure.http_status,
        error = %failure.message,
        "Prometheus metrics refresh failed"
    );
}

/// Percent-encode PromQL params into a query string, reusing reqwest's URL encoder
/// (already a dependency) rather than pulling in a separate one.
fn encode_params(pairs: &[(&str, &str)]) -> Result<String, String> {
    let url = reqwest::Url::parse_with_params("http://prom.invalid/", pairs)
        .map_err(|e| format!("encode query: {e}"))?;
    Ok(url.query().unwrap_or_default().to_owned())
}

#[cfg(test)]
mod tests {
    use kube::{error::Status, Error as KubeError};
    use ome_client::k8s_openapi::api::core::v1::{Service, ServicePort, ServiceSpec};

    use super::{service_discovery_failure, validate_prom_service};
    use crate::config::PromTarget;

    fn target() -> PromTarget {
        PromTarget {
            namespace: "ome".to_owned(),
            service: "ome-prometheus".to_owned(),
            port: 9090,
        }
    }

    fn service_with_port(port: i32) -> Service {
        Service {
            spec: Some(ServiceSpec {
                ports: Some(vec![ServicePort {
                    port,
                    ..ServicePort::default()
                }]),
                ..ServiceSpec::default()
            }),
            ..Service::default()
        }
    }

    #[test]
    fn service_discovery_validates_the_configured_port() {
        let prom = target();
        assert!(validate_prom_service(&service_with_port(9090), &prom).is_ok());

        let failure = validate_prom_service(&service_with_port(8080), &prom).unwrap_err();
        let source = failure.to_source_value(&prom);
        assert_eq!(source["status"], "unconfigured");
        assert_eq!(source["target"], "ome/ome-prometheus:9090");
        assert_eq!(source["error"]["code"], "prometheus_service_port_not_found");
        assert!(source["error"].get("httpStatus").is_none());
    }

    #[test]
    fn discovery_rbac_errors_are_unavailable() {
        let prom = target();
        let error = KubeError::Api(
            Status::failure("services is forbidden", "Forbidden")
                .with_code(403)
                .boxed(),
        );
        let source = service_discovery_failure(&prom, &error).to_source_value(&prom);
        assert_eq!(source["status"], "unavailable");
        assert_eq!(source["error"]["httpStatus"], 403);
    }

    #[test]
    fn missing_services_are_unconfigured() {
        let prom = target();
        let error = KubeError::Api(
            Status::failure("services \"ome-prometheus\" not found", "NotFound")
                .with_code(404)
                .boxed(),
        );
        let source = service_discovery_failure(&prom, &error).to_source_value(&prom);
        assert_eq!(source["status"], "unconfigured");
        assert_eq!(source["error"]["code"], "prometheus_service_not_found");
    }
}
