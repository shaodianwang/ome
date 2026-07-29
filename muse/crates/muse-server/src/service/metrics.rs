//! On-demand Prometheus metrics through the PromQL Query Gateway.
//!
//! Callers select either fleet scope (when supported by the catalog) or one exact
//! InferenceService. The catalog owns every expression; request input can only
//! fill the reserved cluster/pod matchers and choose a configured range.
//! A bounded per-replica cache coalesces identical misses, while one semaphore
//! caps all upstream work across concurrent browser requests.

use std::{
    collections::HashMap,
    future::Future,
    num::NonZeroUsize,
    sync::Arc,
    time::{Duration, Instant},
};

use axum::{
    extract::{Query, State},
    Json,
};
use reqwest::header::{CONTENT_TYPE, RETRY_AFTER};
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::{
    sync::{Mutex, OnceCell, Semaphore},
    task::JoinSet,
};
use tokio_stream::StreamExt;

use crate::{
    app_context::AppState,
    config::{MetricsGatewayConfig, QueryDef, QueryScope, QueryType},
    informers::now_secs,
    protocol::MetricsTimeRanges,
    service::error::ApiError,
};

#[derive(Clone, Debug, PartialEq, Eq)]
enum MetricsScope {
    Fleet,
    Deployment {
        namespace: String,
        inferenceservice: String,
        pods: Vec<ScopedPod>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ScopedPod {
    name: String,
    component: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct SelectedMetricsRange {
    window_seconds: u64,
    step_seconds: u64,
}

impl MetricsScope {
    fn catalog_scope(&self) -> QueryScope {
        match self {
            Self::Fleet => QueryScope::Fleet,
            Self::Deployment { .. } => QueryScope::Deployment,
        }
    }
}

#[derive(Debug, Default, Deserialize)]
pub struct MetricsParams {
    cluster: Option<String>,
    namespace: Option<String>,
    inferenceservice: Option<String>,
    range: Option<String>,
}

fn request_scope(params: &MetricsParams) -> Result<MetricsScope, ApiError> {
    match (&params.cluster, &params.namespace, &params.inferenceservice) {
        (_, None, None) => Ok(MetricsScope::Fleet),
        (Some(_), Some(namespace), Some(inferenceservice))
            if !namespace.trim().is_empty() && !inferenceservice.trim().is_empty() =>
        {
            Ok(MetricsScope::Deployment {
                namespace: namespace.clone(),
                inferenceservice: inferenceservice.clone(),
                pods: Vec::new(),
            })
        }
        _ => Err(ApiError::BadRequest(
            "deployment metrics require cluster, namespace, and inferenceservice together"
                .to_owned(),
        )),
    }
}

fn selected_range(
    requested: Option<&str>,
    ranges: Option<&MetricsTimeRanges>,
) -> Result<Option<SelectedMetricsRange>, ApiError> {
    let Some(ranges) = ranges else {
        return if requested.is_some() {
            Err(ApiError::BadRequest(
                "metrics time ranges are not configured".to_owned(),
            ))
        } else {
            Ok(None)
        };
    };
    let id = requested.unwrap_or(&ranges.default);
    ranges
        .options
        .iter()
        .find(|option| option.id == id)
        .map(|option| {
            Some(SelectedMetricsRange {
                window_seconds: option.seconds,
                step_seconds: option.step_seconds,
            })
        })
        .ok_or_else(|| ApiError::BadRequest(format!("unknown metrics time range: {id}")))
}

/// `GET /v1/metrics` executes or reuses the exact requested scope.
pub async fn snapshot(
    State(st): State<AppState>,
    Query(params): Query<MetricsParams>,
) -> Result<Json<Value>, ApiError> {
    let mut scope = request_scope(&params)?;
    let backend = st
        .metrics_backend()
        .ok_or_else(|| ApiError::Unavailable("metrics are not configured".to_owned()))?;
    if matches!(scope, MetricsScope::Fleet) && !backend.supports_fleet_scope() {
        return Err(ApiError::Unavailable(
            "fleet metrics are not configured".to_owned(),
        ));
    }
    let range = selected_range(params.range.as_deref(), st.metrics_time_ranges())?;
    let clusters = match params.cluster.as_deref() {
        Some(name) => {
            let selected = st.select(Some(name))?;
            if let MetricsScope::Deployment {
                namespace,
                inferenceservice,
                pods,
            } = &mut scope
            {
                let exists = selected[0]
                    .inference_services
                    .list()
                    .iter()
                    .any(|candidate| {
                        candidate.metadata.name.as_deref() == Some(inferenceservice.as_str())
                            && candidate.metadata.namespace.as_deref() == Some(namespace.as_str())
                    });
                if !exists {
                    return Err(ApiError::NotFound(format!(
                        "InferenceService {namespace}/{inferenceservice} was not found in cluster {name}"
                    )));
                }
                let all_pods = selected[0].pods.list();
                let mut name_counts = HashMap::new();
                for pod in &all_pods {
                    if let Some(name) = pod.metadata.name.as_deref() {
                        *name_counts.entry(name.to_owned()).or_insert(0usize) += 1;
                    }
                }
                let mut deployment_pods = selected[0]
                    .pods
                    .list_namespaced(namespace.as_str())
                    .iter()
                    .filter(|pod| {
                        pod.metadata
                            .labels
                            .as_ref()
                            .and_then(|labels| labels.get("ome.io/inferenceservice"))
                            .is_some_and(|owner| owner == inferenceservice.as_str())
                    })
                    .filter_map(|pod| {
                        let name = pod.metadata.name.as_ref()?;
                        if name_counts.get(name).copied() != Some(1) {
                            return None;
                        }
                        Some(ScopedPod {
                            name: name.clone(),
                            component: pod
                                .metadata
                                .labels
                                .as_ref()
                                .and_then(|labels| labels.get("component"))
                                .cloned(),
                        })
                    })
                    .collect::<Vec<_>>();
                deployment_pods.sort_by(|left, right| left.name.cmp(&right.name));
                if deployment_pods.is_empty() {
                    return Err(ApiError::Unavailable(format!(
                        "InferenceService {namespace}/{inferenceservice} has no unambiguous cached pods"
                    )));
                }
                if deployment_pods.len() > backend.max_scope_pods() {
                    return Err(ApiError::BadRequest(format!(
                        "InferenceService {namespace}/{inferenceservice} has {} pods, exceeding the configured metrics scope limit of {}",
                        deployment_pods.len(),
                        backend.max_scope_pods()
                    )));
                }
                *pods = deployment_pods;
            }
            selected
                .into_iter()
                .map(|cluster| cluster.name.clone())
                .collect()
        }
        None if matches!(scope, MetricsScope::Fleet) => st
            .select(None)?
            .into_iter()
            .map(|cluster| cluster.name.clone())
            .collect(),
        None => {
            return Err(ApiError::BadRequest(
                "deployment metrics require a cluster".to_owned(),
            ))
        }
    };
    let value = backend.snapshot(clusters, scope, range).await;
    Ok(Json((*value).clone()))
}

pub struct MetricsBackend {
    client: reqwest::Client,
    base_url: reqwest::Url,
    query_routing: u8,
    cluster_ids: Arc<HashMap<String, String>>,
    queries: Arc<Vec<QueryDef>>,
    semaphore: Arc<Semaphore>,
    request_timeout: Duration,
    max_response_bytes: usize,
    max_series: NonZeroUsize,
    max_scope_pods: NonZeroUsize,
    cache: MetricsCache,
}

impl MetricsBackend {
    pub fn new(config: MetricsGatewayConfig, queries: Arc<Vec<QueryDef>>) -> anyhow::Result<Self> {
        let mut builder = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(config.request_timeout);
        if let Some(path) = config.ca_cert.as_deref() {
            let pem = std::fs::read(path).map_err(|error| {
                anyhow::anyhow!("read metrics gateway CA {}: {error}", path.display())
            })?;
            let certificates = reqwest::Certificate::from_pem_bundle(&pem).map_err(|error| {
                anyhow::anyhow!("parse metrics gateway CA {}: {error}", path.display())
            })?;
            if certificates.is_empty() {
                anyhow::bail!("metrics gateway CA {} has no certificates", path.display());
            }
            for certificate in certificates {
                builder = builder.add_root_certificate(certificate);
            }
        }
        let client = builder.build()?;
        Ok(Self {
            client,
            base_url: config.url,
            query_routing: config.query_routing,
            cluster_ids: Arc::new(config.cluster_ids),
            queries,
            semaphore: Arc::new(Semaphore::new(config.query_concurrency.get())),
            request_timeout: config.request_timeout,
            max_response_bytes: config.max_response_bytes,
            max_series: config.max_series,
            max_scope_pods: config.max_scope_pods,
            cache: MetricsCache::new(config.cache_ttl, config.cache_max_entries),
        })
    }

    pub fn max_scope_pods(&self) -> usize {
        self.max_scope_pods.get()
    }

    pub fn supports_fleet_scope(&self) -> bool {
        self.queries
            .iter()
            .any(|query| query.scopes.contains(&QueryScope::Fleet))
    }

    async fn snapshot(
        self: &Arc<Self>,
        clusters: Vec<String>,
        scope: MetricsScope,
        range: Option<SelectedMetricsRange>,
    ) -> Arc<Value> {
        let key = cache_key(&clusters, &scope, range);
        let backend = self.clone();
        self.cache
            .get_or_compute(key, move || async move {
                Arc::new(backend.execute(clusters, scope, range).await)
            })
            .await
    }

    async fn execute(
        self: &Arc<Self>,
        clusters_requested: Vec<String>,
        scope: MetricsScope,
        range: Option<SelectedMetricsRange>,
    ) -> Value {
        let started = Instant::now();
        let evaluation_time = now_secs();
        let mut clusters = clusters_requested
            .iter()
            .map(|cluster| (cluster.clone(), json!({})))
            .collect::<serde_json::Map<_, _>>();
        let mut sources = serde_json::Map::new();
        let mut jobs = JoinSet::new();
        let catalog_scope = scope.catalog_scope();
        let mut query_count = 0usize;

        for cluster in clusters_requested {
            let Some(cluster_id) = self.cluster_ids.get(&cluster).cloned() else {
                sources.insert(
                    cluster,
                    json!({
                        "status": "unconfigured",
                        "target": self.base_url.as_str(),
                        "error": {
                            "code": "metrics_cluster_id_missing",
                            "message": "no metrics cluster ID is configured"
                        }
                    }),
                );
                continue;
            };
            sources.insert(
                cluster.clone(),
                json!({ "status": "available", "target": self.base_url.as_str() }),
            );
            for query in self
                .queries
                .iter()
                .filter(|query| query.scopes.contains(&catalog_scope))
                .cloned()
            {
                query_count += 1;
                let backend = self.clone();
                let cluster = cluster.clone();
                let cluster_id = cluster_id.clone();
                let scope = scope.clone();
                jobs.spawn(async move {
                    let query_id = query.id.clone();
                    let value = match backend
                        .run_query(&query, &cluster_id, &scope, evaluation_time, range)
                        .await
                    {
                        Ok(value) => value,
                        Err(error) => json!({ "error": error }),
                    };
                    (cluster, query_id, value)
                });
            }
        }

        let mut failed_queries = 0usize;
        while let Some(result) = jobs.join_next().await {
            match result {
                Ok((cluster, query_id, value)) => {
                    failed_queries += usize::from(value.get("error").is_some());
                    if let Some(results) = clusters.get_mut(&cluster).and_then(Value::as_object_mut)
                    {
                        results.insert(query_id, value);
                    }
                }
                Err(error) => {
                    failed_queries += 1;
                    tracing::error!(error = %error, "metrics query worker failed");
                }
            }
        }

        let elapsed = started.elapsed();
        ::metrics::histogram!("muse_metrics_query_snapshot_duration_seconds")
            .record(elapsed.as_secs_f64());
        ::metrics::counter!("muse_metrics_queries_total", "outcome" => "success")
            .increment((query_count.saturating_sub(failed_queries)) as u64);
        ::metrics::counter!("muse_metrics_queries_total", "outcome" => "failure")
            .increment(failed_queries as u64);
        tracing::info!(
            duration_seconds = elapsed.as_secs_f64(),
            queries = query_count,
            failed_queries,
            "Query Gateway metrics snapshot evaluated"
        );
        json!({
            "updatedAt": now_secs(),
            "evaluatedAt": evaluation_time,
            "clusters": clusters,
            "sources": sources
        })
    }

    async fn run_query(
        &self,
        definition: &QueryDef,
        cluster_id: &str,
        scope: &MetricsScope,
        evaluation_time: u64,
        requested_range: Option<SelectedMetricsRange>,
    ) -> Result<Value, String> {
        let promql = render_promql(&definition.promql, cluster_id, scope)?;
        let (path, params) = query_parameters(
            definition,
            &promql,
            evaluation_time,
            self.query_routing,
            requested_range,
        )?;
        let url = self
            .base_url
            .join(path)
            .map_err(|error| format!("build Query Gateway URL: {error}"))?;
        let body = encode_params(&params)?;
        let _permit = self
            .semaphore
            .acquire()
            .await
            .map_err(|_| "metrics query concurrency limiter closed".to_owned())?;
        let response = tokio::time::timeout(
            self.request_timeout,
            self.client
                .post(url)
                .header(CONTENT_TYPE, "application/x-www-form-urlencoded")
                .body(body)
                .send(),
        )
        .await
        .map_err(|_| {
            format!(
                "Query Gateway request timed out after {}s",
                self.request_timeout.as_secs()
            )
        })?
        .map_err(|error| format!("Query Gateway request failed: {error}"))?;

        if !response.status().is_success() {
            let status = response.status();
            let retry_after = response
                .headers()
                .get(RETRY_AFTER)
                .and_then(|value| value.to_str().ok())
                .map(|value| format!("; Retry-After: {value}"))
                .unwrap_or_default();
            return Err(format!("Query Gateway returned HTTP {status}{retry_after}"));
        }
        if response
            .content_length()
            .is_some_and(|length| length > self.max_response_bytes as u64)
        {
            return Err(format!(
                "Query Gateway response exceeds the configured {} byte limit",
                self.max_response_bytes
            ));
        }

        let mut bytes = Vec::new();
        let mut stream = response.bytes_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|error| format!("read Query Gateway response: {error}"))?;
            if bytes.len().saturating_add(chunk.len()) > self.max_response_bytes {
                return Err(format!(
                    "Query Gateway response exceeds the configured {} byte limit",
                    self.max_response_bytes
                ));
            }
            bytes.extend_from_slice(&chunk);
        }
        let value: Value = serde_json::from_slice(&bytes)
            .map_err(|error| format!("decode Query Gateway response: {error}"))?;
        let value = validate_gateway_response(value, self.max_series)?;
        normalize_gateway_response(value, scope)
    }
}

type QueryParameters = (&'static str, Vec<(&'static str, String)>);

fn query_parameters(
    definition: &QueryDef,
    promql: &str,
    evaluation_time: u64,
    query_routing: u8,
    requested_range: Option<SelectedMetricsRange>,
) -> Result<QueryParameters, String> {
    match definition.kind {
        QueryType::Instant => Ok((
            "query",
            vec![
                ("query", promql.to_owned()),
                ("time", evaluation_time.to_string()),
                ("query_routing", query_routing.to_string()),
            ],
        )),
        QueryType::Range => {
            let (Some(max_window), Some(minimum_step)) = (definition.window, definition.step)
            else {
                return Err(format!(
                    "range query '{}' has no window or step",
                    definition.id
                ));
            };
            let (window, step) = requested_range
                .map(|range| {
                    (
                        range.window_seconds.min(max_window),
                        range.step_seconds.max(minimum_step),
                    )
                })
                .unwrap_or((max_window, minimum_step));
            Ok((
                "query_range",
                vec![
                    ("query", promql.to_owned()),
                    ("start", evaluation_time.saturating_sub(window).to_string()),
                    ("end", evaluation_time.to_string()),
                    ("step", format!("{step}s")),
                    ("query_routing", query_routing.to_string()),
                ],
            ))
        }
    }
}

fn encode_params(params: &[(&str, String)]) -> Result<String, String> {
    let url = reqwest::Url::parse_with_params("http://prom.invalid/", params)
        .map_err(|error| format!("encode query: {error}"))?;
    Ok(url.query().unwrap_or_default().to_owned())
}

fn promql_string_value(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for character in value.chars() {
        match character {
            '\\' => escaped.push_str("\\\\"),
            '"' => escaped.push_str("\\\""),
            '\n' => escaped.push_str("\\n"),
            '\r' => escaped.push_str("\\r"),
            '\t' => escaped.push_str("\\t"),
            character => escaped.push(character),
        }
    }
    escaped
}

fn prometheus_regex_escape(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for character in value.chars() {
        if matches!(
            character,
            '\\' | '.' | '^' | '$' | '|' | '?' | '*' | '+' | '(' | ')' | '[' | ']' | '{' | '}'
        ) {
            escaped.push('\\');
        }
        escaped.push(character);
    }
    escaped
}

fn prometheus_exact_set_regex(values: &[&str]) -> String {
    let first = values[0];
    let mut prefix_len = first.len();
    for value in &values[1..] {
        prefix_len = first
            .as_bytes()
            .iter()
            .zip(value.as_bytes())
            .take(prefix_len)
            .take_while(|(left, right)| left == right)
            .count();
        while !first.is_char_boundary(prefix_len) {
            prefix_len -= 1;
        }
    }
    if values.iter().all(|value| *value == first) {
        return format!("^{}$", prometheus_regex_escape(first));
    }

    let prefix = prometheus_regex_escape(&first[..prefix_len]);
    let alternatives = values
        .iter()
        .map(|value| prometheus_regex_escape(&value[prefix_len..]))
        .collect::<Vec<_>>()
        .join("|");
    format!("^{prefix}({alternatives})$")
}

fn render_promql(template: &str, cluster_id: &str, scope: &MetricsScope) -> Result<String, String> {
    if !template.contains("{{cluster_id}}") || !template.contains("{{pod_matcher}}") {
        return Err("metrics query is missing a reserved scope placeholder".to_owned());
    }
    let pod_matcher = match scope {
        MetricsScope::Fleet => String::new(),
        MetricsScope::Deployment { pods, .. } => {
            if pods.is_empty() {
                return Err("deployment metrics scope has no pods".to_owned());
            }
            let names = pods.iter().map(|pod| pod.name.as_str()).collect::<Vec<_>>();
            let regex = prometheus_exact_set_regex(&names);
            format!(",pod=~\"{}\"", promql_string_value(&regex))
        }
    };
    let rendered = template
        .replace("{{cluster_id}}", &promql_string_value(cluster_id))
        .replace("{{pod_matcher}}", &pod_matcher);
    if rendered.contains("{{") {
        return Err("metrics query contains an unsupported placeholder".to_owned());
    }
    Ok(rendered)
}

fn normalize_gateway_response(mut value: Value, scope: &MetricsScope) -> Result<Value, String> {
    let MetricsScope::Deployment {
        namespace,
        inferenceservice,
        pods,
    } = scope
    else {
        return Ok(value);
    };
    let result = value
        .pointer_mut("/data/result")
        .and_then(Value::as_array_mut)
        .ok_or_else(|| "Query Gateway response has no mutable result array".to_owned())?;
    for series in result {
        let metric = series
            .get_mut("metric")
            .and_then(Value::as_object_mut)
            .ok_or_else(|| "Query Gateway result series has no metric labels".to_owned())?;
        metric.insert("namespace".to_owned(), Value::String(namespace.clone()));
        metric.insert(
            "inferenceservice".to_owned(),
            Value::String(inferenceservice.clone()),
        );
        metric.remove("component");
        if let Some(pod_name) = metric.get("pod").and_then(Value::as_str).map(str::to_owned) {
            let pod = pods
                .iter()
                .find(|pod| pod.name == pod_name)
                .ok_or_else(|| {
                    format!("Query Gateway returned pod outside the requested scope: {pod_name}")
                })?;
            if let Some(component) = &pod.component {
                metric.insert("component".to_owned(), Value::String(component.clone()));
            }
        }
    }
    Ok(value)
}

fn validate_gateway_response(value: Value, max_series: NonZeroUsize) -> Result<Value, String> {
    if value.get("status").and_then(Value::as_str) != Some("success") {
        return Err("Query Gateway returned a non-success Prometheus response".to_owned());
    }
    let data = value
        .get("data")
        .and_then(Value::as_object)
        .ok_or_else(|| "Query Gateway response has no Prometheus data object".to_owned())?;
    if data.get("resultType").and_then(Value::as_str).is_none() {
        return Err("Query Gateway response has no Prometheus resultType".to_owned());
    }
    let result = data
        .get("result")
        .and_then(Value::as_array)
        .ok_or_else(|| "Query Gateway response has no Prometheus result array".to_owned())?;
    if result.len() > max_series.get() {
        return Err(format!(
            "Query Gateway response returned {} series, exceeding the configured limit of {}",
            result.len(),
            max_series
        ));
    }
    Ok(value)
}

fn cache_key(
    clusters: &[String],
    scope: &MetricsScope,
    range: Option<SelectedMetricsRange>,
) -> String {
    format!("{clusters:?}|{scope:?}|{range:?}")
}

#[derive(Clone)]
struct MetricsCache {
    state: Arc<Mutex<HashMap<String, MetricsCacheEntry>>>,
    ttl: Duration,
    max_entries: NonZeroUsize,
}

struct MetricsCacheEntry {
    inserted_at: Instant,
    completed_at: Option<Instant>,
    value: Arc<OnceCell<Arc<Value>>>,
}

impl MetricsCache {
    fn new(ttl: Duration, max_entries: NonZeroUsize) -> Self {
        Self {
            state: Arc::new(Mutex::new(HashMap::new())),
            ttl,
            max_entries,
        }
    }

    async fn get_or_compute<F, Fut>(&self, key: String, compute: F) -> Arc<Value>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Arc<Value>>,
    {
        let cell = {
            let mut state = self.state.lock().await;
            state.retain(|_, entry| {
                entry
                    .completed_at
                    .is_none_or(|completed_at| completed_at.elapsed() < self.ttl)
            });
            if let Some(entry) = state.get(&key) {
                ::metrics::counter!("muse_metrics_cache_requests_total", "outcome" => "hit")
                    .increment(1);
                entry.value.clone()
            } else {
                ::metrics::counter!("muse_metrics_cache_requests_total", "outcome" => "miss")
                    .increment(1);
                if state.len() >= self.max_entries.get() {
                    if let Some(oldest) = state
                        .iter()
                        .min_by_key(|(_, entry)| {
                            (
                                entry.completed_at.is_none(),
                                entry.completed_at.unwrap_or(entry.inserted_at),
                            )
                        })
                        .map(|(key, _)| key.clone())
                    {
                        state.remove(&oldest);
                    }
                }
                let value = Arc::new(OnceCell::new());
                state.insert(
                    key.clone(),
                    MetricsCacheEntry {
                        inserted_at: Instant::now(),
                        completed_at: None,
                        value: value.clone(),
                    },
                );
                value
            }
        };
        let result = cell.get_or_init(compute).await.clone();
        let mut state = self.state.lock().await;
        if let Some(entry) = state.get_mut(&key) {
            if Arc::ptr_eq(&entry.value, &cell) && entry.completed_at.is_none() {
                entry.completed_at = Some(Instant::now());
            }
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        num::NonZeroUsize,
        path::PathBuf,
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
        time::Duration,
    };

    use super::{
        normalize_gateway_response, query_parameters, render_promql, request_scope, selected_range,
        validate_gateway_response, MetricsBackend, MetricsCache, MetricsParams, MetricsScope,
        ScopedPod, SelectedMetricsRange,
    };
    use crate::{
        config::{MetricsGatewayConfig, QueryDef, QueryScope, QueryType},
        informers::now_secs,
        protocol::{MetricsTimeRangeOption, MetricsTimeRanges},
    };

    #[test]
    fn grafana_promql_rendering_scopes_every_reserved_matcher() {
        let promql = concat!(
            "sum(rate(sglang:generation_tokens_total{",
            "cluster=\"{{cluster_id}}\"",
            "{{pod_matcher}}}[5m]))"
        );
        let deployment = render_promql(
            promql,
            "123/us-east-2/cluster-a",
            &MetricsScope::Deployment {
                namespace: "tenant\\\"one".to_owned(),
                inferenceservice: "model-a".to_owned(),
                pods: vec![
                    ScopedPod {
                        name: "engine.0".to_owned(),
                        component: Some("engine".to_owned()),
                    },
                    ScopedPod {
                        name: "decode-(1)".to_owned(),
                        component: Some("decode".to_owned()),
                    },
                ],
            },
        )
        .unwrap();
        assert!(deployment.contains(
            "cluster=\"123/us-east-2/cluster-a\",pod=~\"^(engine\\\\.0|decode-\\\\(1\\\\))$\""
        ));
    }

    #[test]
    fn grafana_promql_rendering_compacts_large_deployment_pod_sets() {
        let promql = concat!(
            "sum(metric_a{cluster=\"{{cluster_id}}\"{{pod_matcher}}}) or ",
            "sum(metric_b{cluster=\"{{cluster_id}}\"{{pod_matcher}}}) or ",
            "sum(metric_c{cluster=\"{{cluster_id}}\"{{pod_matcher}}})"
        );
        let pods = (0..1_100)
            .map(|index| ScopedPod {
                name: format!("avocado-t4-9b-text-engine-{index}-default-1"),
                component: Some("engine".to_owned()),
            })
            .collect();
        let deployment = render_promql(
            promql,
            "msl-gcp-1-uwob1/us-central1/msl-gcp-usc1-1-tpu",
            &MetricsScope::Deployment {
                namespace: "inf-prod".to_owned(),
                inferenceservice: "avocado-t4-9b-text".to_owned(),
                pods,
            },
        )
        .unwrap();

        assert!(deployment.len() < 131_072);
        assert!(deployment
            .contains("pod=~\"^avocado-t4-9b-text-engine-(0-default-1|1-default-1|2-default-1|"));
    }

    #[test]
    fn grafana_promql_rendering_preserves_pods_named_as_another_pods_prefix() {
        let deployment = render_promql(
            "up{cluster=\"{{cluster_id}}\"{{pod_matcher}}}",
            "cluster-a",
            &MetricsScope::Deployment {
                namespace: "tenant-a".to_owned(),
                inferenceservice: "model-a".to_owned(),
                pods: vec![
                    ScopedPod {
                        name: "model-a-engine".to_owned(),
                        component: Some("engine".to_owned()),
                    },
                    ScopedPod {
                        name: "model-a-engine-0".to_owned(),
                        component: Some("engine".to_owned()),
                    },
                ],
            },
        )
        .unwrap();

        assert!(deployment.contains("pod=~\"^model-a-engine(|-0)$\""));
    }

    #[test]
    fn deployment_results_are_enriched_from_the_scoped_pod_cache() {
        let scope = MetricsScope::Deployment {
            namespace: "tenant-a".to_owned(),
            inferenceservice: "model-a".to_owned(),
            pods: vec![ScopedPod {
                name: "model-a-engine-0".to_owned(),
                component: Some("engine".to_owned()),
            }],
        };
        let response = serde_json::json!({
            "status": "success",
            "data": {
                "resultType": "vector",
                "result": [{
                    "metric": {"pod": "model-a-engine-0"},
                    "value": [1, "2"]
                }]
            }
        });

        let normalized = normalize_gateway_response(response, &scope).unwrap();
        let labels = &normalized["data"]["result"][0]["metric"];
        assert_eq!(labels["namespace"], "tenant-a");
        assert_eq!(labels["inferenceservice"], "model-a");
        assert_eq!(labels["component"], "engine");

        let outside_scope = serde_json::json!({
            "status": "success",
            "data": {
                "resultType": "vector",
                "result": [{"metric": {"pod": "other-pod"}, "value": [1, "2"]}]
            }
        });
        assert!(normalize_gateway_response(outside_scope, &scope).is_err());

        let scope_without_component = MetricsScope::Deployment {
            namespace: "tenant-a".to_owned(),
            inferenceservice: "model-a".to_owned(),
            pods: vec![ScopedPod {
                name: "model-a-router-0".to_owned(),
                component: None,
            }],
        };
        let untrusted_component = serde_json::json!({
            "status": "success",
            "data": {
                "resultType": "vector",
                "result": [{
                    "metric": {"pod": "model-a-router-0", "component": "untrusted"},
                    "value": [1, "2"]
                }]
            }
        });
        let normalized =
            normalize_gateway_response(untrusted_component, &scope_without_component).unwrap();
        assert!(normalized["data"]["result"][0]["metric"]
            .get("component")
            .is_none());
    }

    #[test]
    fn requested_range_selects_window_and_step_without_expanding_catalog_window() {
        let definition = QueryDef {
            id: "requests".to_owned(),
            promql: "up".to_owned(),
            kind: QueryType::Range,
            window: Some(21_600),
            step: Some(60),
            scopes: vec![QueryScope::Fleet],
        };
        let (_, short) = query_parameters(
            &definition,
            "up",
            10_000,
            2,
            Some(SelectedMetricsRange {
                window_seconds: 900,
                step_seconds: 60,
            }),
        )
        .unwrap();
        let (_, capped) = query_parameters(
            &definition,
            "up",
            30_000,
            2,
            Some(SelectedMetricsRange {
                window_seconds: 43_200,
                step_seconds: 300,
            }),
        )
        .unwrap();
        assert!(short.contains(&("start", "9100".to_owned())));
        assert!(short.contains(&("step", "60s".to_owned())));
        assert!(short.contains(&("query_routing", "2".to_owned())));
        assert!(capped.contains(&("start", "8400".to_owned())));
        assert!(capped.contains(&("step", "300s".to_owned())));
    }

    #[test]
    fn request_scope_rejects_partial_deployment_identity() {
        assert!(request_scope(&MetricsParams::default()).is_ok());
        assert!(request_scope(&MetricsParams {
            cluster: Some("cluster-a".to_owned()),
            namespace: Some("tenant".to_owned()),
            inferenceservice: None,
            range: None,
        })
        .is_err());
    }

    #[test]
    fn range_selection_is_limited_to_configured_options() {
        let ranges = MetricsTimeRanges {
            default: "15m".to_owned(),
            options: vec![MetricsTimeRangeOption {
                id: "15m".to_owned(),
                label: "15m".to_owned(),
                seconds: 900,
                step_seconds: 60,
            }],
        };
        assert_eq!(
            selected_range(None, Some(&ranges)).unwrap(),
            Some(SelectedMetricsRange {
                window_seconds: 900,
                step_seconds: 60,
            })
        );
        assert!(selected_range(Some("6h"), Some(&ranges)).is_err());
    }

    #[test]
    fn gateway_response_enforces_prometheus_shape_and_series_limit() {
        let valid = serde_json::json!({
            "status": "success",
            "data": { "resultType": "vector", "result": [{"metric": {}, "value": [1, "1"]}] }
        });
        assert!(validate_gateway_response(valid, NonZeroUsize::new(1).unwrap()).is_ok());

        let oversized = serde_json::json!({
            "status": "success",
            "data": { "resultType": "vector", "result": [{}, {}] }
        });
        assert!(validate_gateway_response(oversized, NonZeroUsize::new(1).unwrap()).is_err());
        assert!(validate_gateway_response(
            serde_json::json!({"status": "success"}),
            NonZeroUsize::new(1).unwrap()
        )
        .is_err());
    }

    #[tokio::test]
    async fn identical_cache_misses_are_computed_once() {
        let cache = MetricsCache::new(Duration::from_secs(30), NonZeroUsize::new(8).unwrap());
        let calls = Arc::new(AtomicUsize::new(0));
        let mut tasks = tokio::task::JoinSet::new();

        for _ in 0..8 {
            let cache = cache.clone();
            let calls = calls.clone();
            tasks.spawn(async move {
                cache
                    .get_or_compute("deployment-a".to_owned(), || async move {
                        calls.fetch_add(1, Ordering::SeqCst);
                        tokio::time::sleep(Duration::from_millis(10)).await;
                        Arc::new(serde_json::json!({"value": 1}))
                    })
                    .await
            });
        }

        while let Some(result) = tasks.join_next().await {
            assert_eq!(result.unwrap()["value"], 1);
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn cache_ttl_starts_after_the_snapshot_completes() {
        let cache = MetricsCache::new(Duration::from_millis(10), NonZeroUsize::new(2).unwrap());
        let calls = Arc::new(AtomicUsize::new(0));
        let first_calls = calls.clone();
        cache
            .get_or_compute("slow-scope".to_owned(), || async move {
                first_calls.fetch_add(1, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(20)).await;
                Arc::new(serde_json::json!({"value": 1}))
            })
            .await;
        let second_calls = calls.clone();
        cache
            .get_or_compute("slow-scope".to_owned(), || async move {
                second_calls.fetch_add(1, Ordering::SeqCst);
                Arc::new(serde_json::json!({"value": 2}))
            })
            .await;

        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    #[ignore = "requires a live Query Gateway route and explicit MUSE_LIVE_METRICS_* values"]
    async fn live_query_gateway_from_environment() {
        let required =
            |name: &str| std::env::var(name).unwrap_or_else(|_| panic!("missing {name}"));
        let gateway_url = required("MUSE_LIVE_METRICS_GATEWAY_URL");
        let ca_cert = required("MUSE_LIVE_METRICS_CA_CERT");
        let cluster_id = required("MUSE_LIVE_METRICS_CLUSTER_ID");
        let pod = required("MUSE_LIVE_METRICS_POD");
        let promql = required("MUSE_LIVE_METRICS_PROMQL");
        let backend = MetricsBackend::new(
            MetricsGatewayConfig {
                url: reqwest::Url::parse(&gateway_url).unwrap(),
                ca_cert: Some(PathBuf::from(ca_cert)),
                query_routing: 2,
                cluster_ids: HashMap::new(),
                query_concurrency: NonZeroUsize::new(1).unwrap(),
                request_timeout: Duration::from_secs(30),
                cache_ttl: Duration::from_secs(1),
                cache_max_entries: NonZeroUsize::new(1).unwrap(),
                max_response_bytes: 1_048_576,
                max_series: NonZeroUsize::new(100).unwrap(),
                max_scope_pods: NonZeroUsize::new(100).unwrap(),
            },
            Arc::new(Vec::new()),
        )
        .unwrap();
        let query = QueryDef {
            id: "live".to_owned(),
            promql,
            kind: QueryType::Instant,
            window: None,
            step: None,
            scopes: vec![QueryScope::Deployment],
        };
        let scope = MetricsScope::Deployment {
            namespace: "live-probe".to_owned(),
            inferenceservice: "live-probe".to_owned(),
            pods: vec![ScopedPod {
                name: pod,
                component: None,
            }],
        };

        let result = backend
            .run_query(&query, &cluster_id, &scope, now_secs(), None)
            .await
            .unwrap();
        assert_eq!(result["status"], "success");
    }
}
