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

use std::{
    convert::Infallible,
    future::Future,
    num::NonZeroUsize,
    sync::Arc,
    time::{Duration, Instant},
};

use axum::{
    extract::State,
    http::Request,
    response::{sse::Event as SseEvent, Response},
    Json,
};
use kube::{Api, Error as KubeError};
use ome_client::k8s_openapi::api::core::v1::Service;
use serde::Serialize;
use serde_json::{json, Value};
use tokio::task::JoinSet;
use tokio_stream::{wrappers::WatchStream, StreamExt};

use crate::{
    app_context::AppState,
    config::{PromTarget, QueryDef, QueryType},
    informers::now_secs,
    service::sse,
};

/// `GET /v1/metrics` — the latest metrics snapshot (initial load / non-SSE clients).
pub async fn snapshot(State(st): State<AppState>) -> Json<Value> {
    Json((*st.metrics_snapshot()).clone())
}

/// `GET /v1/metrics/stream` — SSE: the current snapshot on connect, then a fresh
/// one on every background refresh, so the console updates without polling. The
/// `watch` receiver yields the current value immediately (flushing the response
/// headers), then each subsequent snapshot.
pub async fn stream(State(st): State<AppState>) -> Response {
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
    let keep_alive = st.sse_keep_alive_interval();
    sse::response(hello.chain(live), keep_alive)
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
    let started = Instant::now();
    let queries = st.metrics_queries();
    let concurrency = st.metrics_query_concurrency();
    let request_timeout = st.metrics_request_timeout();
    let mut clusters = st
        .clusters
        .iter()
        .map(|cluster| (cluster.name.clone(), json!({})))
        .collect::<serde_json::Map<_, _>>();
    let mut sources = serde_json::Map::new();
    let mut query_jobs = Vec::new();

    if !queries.is_empty() {
        let discovery_prom = prom.clone();
        let discovery_jobs = st
            .clusters
            .iter()
            .map(|cluster| (cluster.name.clone(), cluster.client.clone()))
            .collect::<Vec<_>>()
            .into_iter();
        let discoveries =
            collect_with_concurrency(discovery_jobs, concurrency, move |(name, client)| {
                let prom = discovery_prom.clone();
                async move {
                    let result = discover_prom_service(&client, &prom, request_timeout).await;
                    (name, client, result)
                }
            })
            .await;

        for (name, client, discovery) in discoveries {
            if let Err(failure) = discovery {
                log_failure(&name, &prom, &failure);
                sources.insert(name, failure.to_source_value(&prom));
                continue;
            }
            sources.insert(name.clone(), available_source_value(&prom));
            query_jobs.extend(
                queries
                    .iter()
                    .cloned()
                    .map(|query| (name.clone(), client.clone(), query)),
            );
        }
    }

    let evaluation_time = now_secs();
    let query_count = query_jobs.len();
    let query_prom = prom.clone();
    let query_results = collect_with_concurrency(
        query_jobs.into_iter(),
        concurrency,
        move |(cluster, client, query)| {
            let prom = query_prom.clone();
            async move {
                let query_id = query.id.clone();
                let value =
                    run_query(&client, &prom, &query, evaluation_time, request_timeout).await;
                (cluster, query_id, value)
            }
        },
    )
    .await;

    let mut failed_queries = 0usize;
    for (cluster, query_id, value) in query_results {
        failed_queries += usize::from(value.get("error").is_some());
        if let Some(results) = clusters.get_mut(&cluster).and_then(Value::as_object_mut) {
            results.insert(query_id, value);
        }
    }

    st.publish_metrics(Arc::new(
        json!({ "updatedAt": now_secs(), "clusters": clusters, "sources": sources }),
    ));
    let elapsed = started.elapsed();
    ::metrics::gauge!("muse_metrics_refresh_duration_seconds").set(elapsed.as_secs_f64());
    tracing::info!(
        duration_seconds = elapsed.as_secs_f64(),
        clusters = st.clusters.len(),
        queries = query_count,
        failed_queries,
        "Prometheus metrics snapshot refreshed"
    );
}

/// Run serially when no concurrency policy is configured. Otherwise, run at most
/// `concurrency` jobs at once without allocating one Tokio task per job; each
/// completed slot admits exactly one pending job.
async fn collect_with_concurrency<I, F, Fut, T>(
    mut items: I,
    concurrency: Option<NonZeroUsize>,
    operation: F,
) -> Vec<T>
where
    I: Iterator + Send,
    I::Item: Send + 'static,
    F: Fn(I::Item) -> Fut + Clone + Send + 'static,
    Fut: Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    let Some(concurrency) = concurrency else {
        let mut results = Vec::new();
        for item in items {
            results.push(operation(item).await);
        }
        return results;
    };

    let mut tasks = JoinSet::new();
    for _ in 0..concurrency.get() {
        let Some(item) = items.next() else {
            break;
        };
        let operation = operation.clone();
        tasks.spawn(operation(item));
    }

    let mut results = Vec::new();
    while let Some(joined) = tasks.join_next().await {
        match joined {
            Ok(result) => results.push(result),
            Err(error) => {
                tracing::error!(error = %error, "metrics refresh worker failed");
            }
        }
        if let Some(item) = items.next() {
            let operation = operation.clone();
            tasks.spawn(operation(item));
        }
    }
    results
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
    request_timeout: Option<Duration>,
) -> Result<(), MetricsFailure> {
    let services: Api<Service> = Api::namespaced(client.clone(), &prom.namespace);
    let service = match await_with_timeout(request_timeout, services.get(&prom.service)).await {
        Ok(result) => result.map_err(|error| service_discovery_failure(prom, &error))?,
        Err(timeout) => return Err(service_discovery_timeout_failure(prom, timeout)),
    };
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
async fn run_query(
    client: &kube::Client,
    prom: &PromTarget,
    def: &QueryDef,
    evaluation_time: u64,
    request_timeout: Option<Duration>,
) -> Value {
    let path = match query_path(prom, def, evaluation_time) {
        Ok(path) => path,
        Err(error) => return json!({ "error": error }),
    };
    match prom_get(client, &path, request_timeout).await {
        Ok(v) => v,
        Err(e) => json!({ "error": e }),
    }
}

fn query_path(prom: &PromTarget, def: &QueryDef, evaluation_time: u64) -> Result<String, String> {
    let proxy = |sub: &str, qs: &str| {
        format!(
            "/api/v1/namespaces/{}/services/{}:{}/proxy/api/v1/{sub}?{qs}",
            prom.namespace, prom.service, prom.port
        )
    };
    let path = match def.kind {
        QueryType::Instant => {
            let at = evaluation_time.to_string();
            let qs = encode_params(&[("query", def.promql.as_str()), ("time", &at)])?;
            proxy("query", &qs)
        }
        QueryType::Range => {
            let (Some(window), Some(step)) = (def.window, def.step) else {
                return Err(format!("range query '{}' has no window or step", def.id));
            };
            let end = evaluation_time;
            let start = end.saturating_sub(window);
            let step = format!("{step}s");
            let qs = encode_params(&[
                ("query", def.promql.as_str()),
                ("start", &start.to_string()),
                ("end", &end.to_string()),
                ("step", &step),
            ])?;
            proxy("query_range", &qs)
        }
    };
    Ok(path)
}

/// GET an apiserver service-proxy path on a cluster's client, decoding the
/// Prometheus JSON body.
async fn prom_get(
    client: &kube::Client,
    path: &str,
    request_timeout: Option<Duration>,
) -> Result<Value, String> {
    let req = Request::builder()
        .method("GET")
        .uri(path)
        .body(Vec::new())
        .map_err(|e| format!("build metrics request: {e}"))?;
    match await_with_timeout(request_timeout, client.request::<Value>(req)).await {
        Ok(result) => result.map_err(|e| format!("prometheus query failed: {e}")),
        Err(timeout) => Err(format!(
            "prometheus query timed out after {}s",
            timeout.as_secs()
        )),
    }
}

/// Await normally when no deployment deadline is configured. A timeout returns
/// the configured duration so callers can retain it in their structured error.
async fn await_with_timeout<T>(
    timeout: Option<Duration>,
    future: impl Future<Output = T>,
) -> Result<T, Duration> {
    match timeout {
        Some(timeout) => tokio::time::timeout(timeout, future)
            .await
            .map_err(|_| timeout),
        None => Ok(future.await),
    }
}

fn service_discovery_timeout_failure(
    prom: &PromTarget,
    request_timeout: Duration,
) -> MetricsFailure {
    MetricsFailure::new(
        "unavailable",
        "prometheus_service_discovery_timeout",
        format!(
            "timed out discovering Prometheus Service {} after {}s",
            prom_target(prom),
            request_timeout.as_secs()
        ),
    )
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
    use std::{
        num::NonZeroUsize,
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
        time::Duration,
    };

    use kube::{error::Status, Error as KubeError};
    use ome_client::k8s_openapi::api::core::v1::{Service, ServicePort, ServiceSpec};

    use super::{
        collect_with_concurrency, query_path, service_discovery_failure,
        service_discovery_timeout_failure, validate_prom_service,
    };
    use crate::config::{PromTarget, QueryDef, QueryType};

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

    #[test]
    fn discovery_timeouts_are_reported_as_unavailable() {
        let prom = target();
        let source = service_discovery_timeout_failure(&prom, Duration::from_secs(15))
            .to_source_value(&prom);
        assert_eq!(source["status"], "unavailable");
        assert_eq!(
            source["error"]["code"],
            "prometheus_service_discovery_timeout"
        );
        assert!(source["error"]["message"]
            .as_str()
            .is_some_and(|message| message.contains("after 15s")));
    }

    #[test]
    fn query_paths_share_the_supplied_evaluation_time() {
        let mut query = QueryDef {
            id: "requests".to_owned(),
            promql: "up".to_owned(),
            kind: QueryType::Instant,
            window: None,
            step: None,
        };

        let instant = query_path(&target(), &query, 1_700_000_000).unwrap();
        assert_eq!(
            instant,
            "/api/v1/namespaces/ome/services/ome-prometheus:9090/proxy/api/v1/query?query=up&time=1700000000"
        );

        query.kind = QueryType::Range;
        query.window = Some(300);
        query.step = Some(30);
        let range = query_path(&target(), &query, 1_700_000_000).unwrap();
        assert_eq!(
            range,
            "/api/v1/namespaces/ome/services/ome-prometheus:9090/proxy/api/v1/query_range?query=up&start=1699999700&end=1700000000&step=30s"
        );
    }

    async fn observed_concurrency(limit: Option<NonZeroUsize>) -> (usize, Vec<i32>) {
        let active = Arc::new(AtomicUsize::new(0));
        let maximum = Arc::new(AtomicUsize::new(0));
        let task_active = active.clone();
        let task_maximum = maximum.clone();

        let mut results = collect_with_concurrency(0..12, limit, move |item| {
            let active = task_active.clone();
            let maximum = task_maximum.clone();
            async move {
                let current = active.fetch_add(1, Ordering::SeqCst) + 1;
                maximum.fetch_max(current, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(5)).await;
                active.fetch_sub(1, Ordering::SeqCst);
                item
            }
        })
        .await;
        results.sort_unstable();
        assert_eq!(active.load(Ordering::SeqCst), 0);
        (maximum.load(Ordering::SeqCst), results)
    }

    #[tokio::test]
    async fn configured_concurrency_bounds_work_and_absence_is_serial() {
        let (serial_maximum, serial_results) = observed_concurrency(None).await;
        let (bounded_maximum, bounded_results) = observed_concurrency(NonZeroUsize::new(3)).await;

        assert_eq!(serial_results, (0..12).collect::<Vec<_>>());
        assert_eq!(bounded_results, serial_results);
        assert_eq!(serial_maximum, 1);
        assert_eq!(bounded_maximum, 3);
    }
}
