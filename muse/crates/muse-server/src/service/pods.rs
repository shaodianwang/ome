//! `GET /v1/inferenceservices/{name}/pods` — one InferenceService's cached pods,
//! for the deployment diagnosis view.
//!
//! Pods are matched by the operator's ownership label within the service's
//! namespace and projected with their health and, when stuck, the reason
//! (scheduling failure, container waiting/terminated state, or not-Ready
//! condition). Problematic pods sort first so a large mostly-healthy deployment
//! still surfaces its few broken instances on top.

use std::{
    convert::Infallible,
    pin::Pin,
    task::{Context, Poll},
};

use axum::{
    extract::{Path, Query, State},
    response::{sse::Event as SseEvent, Response},
    Json,
};
use futures_util::io::AsyncBufReadExt as _;
use kube::{api::ListParams, api::LogParams, Api, Resource};
use ome_client::k8s_openapi::api::core::v1::{Event, Pod};
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::{sync::mpsc, task::JoinSet};
use tokio_stream::{wrappers::ReceiverStream, Stream, StreamExt};

use super::{error::ApiError, resources::lookup_resource, sse};
use crate::{
    app_context::AppState,
    config::PodInspectConfig,
    informers::ClusterState,
    protocol::{
        owned_by, pod_component, tagged, IsvcPodView, IsvcPodsView, PodInspectView, ViewParams,
    },
};

/// The cluster owning the named InferenceService and the service's namespace,
/// looked up across the selected clusters' caches; the canonical not-found
/// error when no selected cluster caches it.
fn find_isvc<'a>(
    st: &'a AppState,
    name: &str,
    cluster: Option<&str>,
    namespace: Option<&str>,
) -> Result<(&'a ClusterState, String), ApiError> {
    for c in st.select(cluster)? {
        if let Some(isvc) = lookup_resource(&c.inference_services, name, namespace) {
            return Ok((c, isvc.meta().namespace.clone().unwrap_or_default()));
        }
    }
    Err(ApiError::NotFound(format!(
        "inferenceservices/{name} not found"
    )))
}

pub async fn list(
    Path(name): Path<String>,
    Query(params): Query<ViewParams>,
    State(st): State<AppState>,
) -> Result<Json<Value>, ApiError> {
    let (c, namespace) = find_isvc(
        &st,
        &name,
        params.cluster.as_deref(),
        params.namespace.as_deref(),
    )?;
    let mut pods: Vec<IsvcPodView> = c
        .pods
        .list_namespaced(&namespace)
        .iter()
        .filter(|pod| owned_by(pod, &name))
        .map(|pod| IsvcPodView::of(pod))
        .collect();
    pods.sort_by(|a, b| {
        b.problematic()
            .cmp(&a.problematic())
            .then_with(|| a.name.cmp(&b.name))
    });
    let view = IsvcPodsView {
        namespace,
        name: name.clone(),
        count: pods.len(),
        pods,
    };
    Ok(Json(tagged(view, &c.name)))
}

/// Query params for the log-tail endpoint (`cluster`/`namespace` as everywhere,
/// plus which container, which run, and how many trailing lines).
#[derive(Deserialize)]
pub struct LogQuery {
    pub cluster: Option<String>,
    pub namespace: Option<String>,
    pub container: Option<String>,
    #[serde(default)]
    pub previous: bool,
    pub tail: Option<usize>,
}

/// Resolve one owned pod for the on-demand endpoints: the effective policy, the
/// owning cluster, and the pod's namespace. Ownership is enforced against the
/// cache (the pod must carry the service's label), so these endpoints can't be
/// used as a generic apiserver proxy.
fn resolve_owned_pod<'a>(
    st: &'a AppState,
    isvc: &str,
    pod: &str,
    cluster: Option<&str>,
    namespace: Option<&str>,
) -> Result<(PodInspectConfig, &'a ClusterState, String), ApiError> {
    let policy = st.pod_inspect().ok_or_else(|| {
        ApiError::Unavailable("pod inspection is not configured on this deployment".to_owned())
    })?;
    let (c, ns) = find_isvc(st, isvc, cluster, namespace)?;
    let owned = c
        .pods
        .list_namespaced(&ns)
        .iter()
        .any(|p| p.metadata.name.as_deref() == Some(pod) && owned_by(p, isvc));
    if !owned {
        return Err(ApiError::NotFound(format!(
            "pod {pod} is not a cached pod of inferenceservices/{isvc}"
        )));
    }
    Ok((policy, c, ns))
}

/// Map an on-demand apiserver call result: timeouts and transport failures are
/// upstream errors, never cache-state errors.
fn upstream<T>(
    result: Result<Result<T, kube::Error>, tokio::time::error::Elapsed>,
    what: &str,
) -> Result<T, ApiError> {
    match result {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(kube::Error::Api(e))) if e.code == 404 => {
            Err(ApiError::NotFound(format!("{what}: {}", e.message)))
        }
        Ok(Err(error)) => Err(ApiError::BadGateway(format!("{what}: {error}"))),
        Err(_) => Err(ApiError::BadGateway(format!(
            "{what}: timed out against the cluster apiserver"
        ))),
    }
}

/// `GET /v1/inferenceservices/{name}/pods/{pod}` — live pod status (full
/// condition/state messages) plus its recent Events, fetched per click.
pub async fn inspect(
    Path((name, pod)): Path<(String, String)>,
    Query(params): Query<ViewParams>,
    State(st): State<AppState>,
) -> Result<Json<Value>, ApiError> {
    let (policy, c, ns) = resolve_owned_pod(
        &st,
        &name,
        &pod,
        params.cluster.as_deref(),
        params.namespace.as_deref(),
    )?;
    let pods: Api<Pod> = Api::namespaced(c.client.clone(), &ns);
    let live = upstream(
        tokio::time::timeout(policy.timeout, pods.get(&pod)).await,
        "pod read",
    )?;
    // Events are the diagnosis narrative but remain best-effort: their RBAC or
    // availability must not take down the pod view itself.
    let event_api: Api<Event> = Api::namespaced(c.client.clone(), &ns);
    let field_selector = format!("involvedObject.name={pod},involvedObject.namespace={ns}");
    let events = tokio::time::timeout(
        policy.timeout,
        event_api.list(&ListParams::default().fields(&field_selector).limit(20)),
    )
    .await
    .ok()
    .and_then(Result::ok)
    .map(|list| list.items)
    .unwrap_or_default();
    Ok(Json(tagged(PodInspectView::of(&live, &events), &c.name)))
}

/// `GET /v1/inferenceservices/{name}/pods/{pod}/logs` — a bounded log tail of
/// the current or previous run, fetched per click.
pub async fn logs(
    Path((name, pod)): Path<(String, String)>,
    Query(params): Query<LogQuery>,
    State(st): State<AppState>,
) -> Result<Json<Value>, ApiError> {
    let (policy, c, ns) = resolve_owned_pod(
        &st,
        &name,
        &pod,
        params.cluster.as_deref(),
        params.namespace.as_deref(),
    )?;
    let tail = params
        .tail
        .unwrap_or(policy.log_tail_lines)
        .min(policy.log_tail_lines);
    let log_params = LogParams {
        container: params.container.clone(),
        previous: params.previous,
        tail_lines: Some(tail as i64),
        limit_bytes: Some(policy.log_max_bytes as i64),
        ..LogParams::default()
    };
    let pods: Api<Pod> = Api::namespaced(c.client.clone(), &ns);
    let text = upstream(
        tokio::time::timeout(policy.timeout, pods.logs(&pod, &log_params)).await,
        "log read",
    )?;
    Ok(Json(json!({
        "cluster": c.name,
        "namespace": ns,
        "pod": pod,
        "container": params.container,
        "previous": params.previous,
        "tail": tail,
        "text": text,
    })))
}

/// Query params for the live log stream: scope to a component's pods (or one
/// pod), optionally pre-filter lines server-side, and choose the starting tail.
#[derive(Deserialize)]
pub struct StreamQuery {
    pub cluster: Option<String>,
    pub namespace: Option<String>,
    pub component: Option<String>,
    pub pod: Option<String>,
    pub grep: Option<String>,
    pub tail: Option<usize>,
}

/// An SSE stream that owns its per-pod reader tasks: when the client
/// disconnects and the response body drops, the JoinSet drop aborts every
/// upstream log connection.
struct StreamGuard<S> {
    inner: S,
    _tasks: JoinSet<()>,
}

impl<S: Stream + Unpin> Stream for StreamGuard<S> {
    type Item = S::Item;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.inner).poll_next(cx)
    }
}

fn notice(message: String) -> SseEvent {
    SseEvent::default()
        .event("notice")
        .json_data(json!({ "message": message }))
        .unwrap_or_else(|_| SseEvent::default().comment("encode error"))
}

/// `GET /v1/inferenceservices/{name}/logs/stream` — follow logs live across a
/// component's pods (or one pod), multiplexed into `{pod, line}` SSE events.
/// Bounded on every axis: pods per session, wall-clock duration, per-connect
/// timeout, and the same ownership check as the other pod endpoints.
pub async fn stream_logs(
    Path(name): Path<String>,
    Query(q): Query<StreamQuery>,
    State(st): State<AppState>,
) -> Result<Response, ApiError> {
    let policy = st.pod_inspect().ok_or_else(|| {
        ApiError::Unavailable("pod inspection is not configured on this deployment".to_owned())
    })?;
    let stream_policy = st.pod_log_stream().ok_or_else(|| {
        ApiError::Unavailable("log streaming is not configured on this deployment".to_owned())
    })?;
    let (c, ns) = find_isvc(&st, &name, q.cluster.as_deref(), q.namespace.as_deref())?;
    // Only scheduled pods have a kubelet to stream from.
    let mut pods: Vec<String> = c
        .pods
        .list_namespaced(&ns)
        .iter()
        .filter(|p| {
            owned_by(p, &name)
                && q.component
                    .as_deref()
                    .is_none_or(|component| pod_component(p) == Some(component))
                && q.pod
                    .as_deref()
                    .is_none_or(|pod| p.metadata.name.as_deref() == Some(pod))
                && p.spec.as_ref().is_some_and(|s| s.node_name.is_some())
        })
        .filter_map(|p| p.metadata.name.clone())
        .collect();
    pods.sort();
    if pods.is_empty() {
        return Err(ApiError::NotFound(format!(
            "no running pods of inferenceservices/{name} match the requested scope"
        )));
    }
    let total = pods.len();
    pods.truncate(stream_policy.max_pods);

    let tail = q
        .tail
        .unwrap_or(policy.log_tail_lines)
        .min(policy.log_tail_lines);
    let grep = q.grep.as_ref().map(|g| g.to_lowercase());
    let deadline = tokio::time::Instant::now() + stream_policy.max_duration;
    let (tx, rx) = mpsc::channel::<SseEvent>(256);
    let mut tasks = JoinSet::new();
    for pod in &pods {
        let api: Api<Pod> = Api::namespaced(c.client.clone(), &ns);
        let tx = tx.clone();
        let grep = grep.clone();
        let pod = pod.clone();
        let log_params = LogParams {
            follow: true,
            tail_lines: Some(tail as i64),
            ..LogParams::default()
        };
        tasks.spawn(async move {
            let reader =
                match tokio::time::timeout(policy.timeout, api.log_stream(&pod, &log_params)).await
                {
                    Ok(Ok(reader)) => reader,
                    Ok(Err(error)) => {
                        let _ = tx.send(notice(format!("{pod}: {error}"))).await;
                        return;
                    }
                    Err(_) => {
                        let _ = tx.send(notice(format!("{pod}: connect timed out"))).await;
                        return;
                    }
                };
            let mut lines = reader.lines();
            loop {
                match tokio::time::timeout_at(deadline, lines.next()).await {
                    // Session duration cap: stop following.
                    Err(_) => break,
                    Ok(Some(Ok(line))) => {
                        if let Some(g) = &grep {
                            if !line.to_lowercase().contains(g) {
                                continue;
                            }
                        }
                        let event = SseEvent::default()
                            .event("line")
                            .json_data(json!({ "pod": pod, "line": line }))
                            .unwrap_or_else(|_| SseEvent::default().comment("encode error"));
                        // A failed send means the client disconnected.
                        if tx.send(event).await.is_err() {
                            break;
                        }
                    }
                    Ok(None) => {
                        let _ = tx.send(notice(format!("{pod}: log stream ended"))).await;
                        break;
                    }
                    Ok(Some(Err(error))) => {
                        let _ = tx.send(notice(format!("{pod}: {error}"))).await;
                        break;
                    }
                }
            }
        });
    }
    drop(tx);

    let preface = notice(format!(
        "streaming {} of {} matching pods{}",
        pods.len(),
        total,
        if total > pods.len() {
            " (per-session cap)"
        } else {
            ""
        },
    ));
    let events = tokio_stream::once(Ok::<_, Infallible>(preface))
        .chain(ReceiverStream::new(rx).map(Ok::<_, Infallible>));
    let guarded = StreamGuard {
        inner: events,
        _tasks: tasks,
    };
    Ok(sse::response(guarded, st.sse_keep_alive_interval()))
}
