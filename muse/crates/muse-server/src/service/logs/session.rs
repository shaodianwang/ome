//! The per-session fan-in engine. One task per (pod, container) follows that
//! container's log stream and feeds a bounded line channel — a full channel
//! blocks the tails, so a slow client bounds memory instead of growing a
//! queue. One aggregator drains the channel on a flush tick into batched
//! `log` frames, resyncs the roster against the pod cache (rollouts start new
//! tails, deletions stop them), rate-limits egress, and reports every anomaly
//! as a `notice` event rather than ending the stream. Only the configured
//! session lifetime — or the client going away — ends a session.

use std::{
    collections::{BTreeMap, BTreeSet},
    convert::Infallible,
    sync::Arc,
    time::Duration,
};

use axum::response::sse::Event as SseEvent;
use futures_util::{AsyncBufReadExt, TryStreamExt};
use kube::{api::LogParams, Api, Client};
use ome_client::{k8s_openapi::api::core::v1::Pod, listers::Lister};
use serde::{ser::SerializeSeq, Serialize, Serializer};
use tokio::{
    sync::{mpsc, OwnedSemaphorePermit, Semaphore},
    task::JoinHandle,
    time::{interval_at, Instant},
};
use tokio_stream::wrappers::ReceiverStream;

use super::{
    roster::{self, RosterFilter, RosterSelection, TailKey},
    throttle::TokenBucket,
    LogsConfig,
};

/// The validated caller inputs for one session.
pub(super) struct SessionRequest {
    pub namespace: String,
    pub isvc: String,
    pub component: Option<String>,
    pub pod: Option<String>,
    pub container: Option<String>,
    /// Already clamped to the configured cap by the endpoint.
    pub tail_lines: Option<i64>,
    pub since_seconds: Option<i64>,
}

/// Everything one session runs against, captured at admission time.
pub(super) struct SessionContext {
    pub config: LogsConfig,
    pub pods: Lister<Pod>,
    pub client: Client,
    pub tail_gate: Arc<Semaphore>,
    pub request: SessionRequest,
}

type EventSender = mpsc::Sender<Result<SseEvent, Infallible>>;

/// Spawn the session task; the returned stream is the SSE body. Dropping the
/// response (client disconnect) closes the channel, which tears the session
/// down — aborting its tails — and releases the admission permit.
pub(super) fn spawn(
    ctx: SessionContext,
    permit: OwnedSemaphorePermit,
) -> ReceiverStream<Result<SseEvent, Infallible>> {
    // One slot is intentional, not a tunable: frames are already batched
    // upstream, and a slow client should propagate backpressure to the tails
    // rather than grow another queue here.
    let (events, rx) = mpsc::channel(1);
    tokio::spawn(async move {
        let _session_slot = permit;
        run(ctx, events).await;
    });
    ReceiverStream::new(rx)
}

async fn run(ctx: SessionContext, events: EventSender) {
    let cfg = ctx.config.clone();
    let (lines_tx, mut lines_rx) = mpsc::channel::<TailEvent>(cfg.buffer_lines);

    let mut selection = current_roster(&ctx);
    if send_json(&events, "pods", &selection, None).await.is_err() {
        return;
    }
    if selection.capped {
        let notice = Notice::TruncatedPods {
            total: selection.total,
            shown: selection.pods.len(),
        };
        if send_notice(&events, &notice).await.is_err() {
            return;
        }
    }

    let mut components = component_index(&selection);
    let mut tails: BTreeMap<TailKey, JoinHandle<()>> = BTreeMap::new();
    let initial: Vec<TailKey> = roster::tail_keys(&selection, ctx.request.container.as_deref())
        .into_iter()
        .collect();
    start_tails(&ctx, &lines_tx, &mut tails, initial, TailStart::Initial);

    let mut bucket = TokenBucket::new(cfg.max_lines_per_sec);
    let mut dropped: u64 = 0;
    let session_deadline = tokio::time::sleep(cfg.session_max);
    tokio::pin!(session_deadline);
    let mut flush = interval_at(Instant::now() + cfg.flush_interval, cfg.flush_interval);
    let mut refresh = interval_at(Instant::now() + cfg.pod_refresh, cfg.pod_refresh);
    let mut last_refill = Instant::now();
    let mut last_drop_notice = Instant::now();

    loop {
        tokio::select! {
            () = &mut session_deadline => {
                let notice = Notice::SessionEnd { after_secs: cfg.session_max.as_secs() };
                let _ = send_notice(&events, &notice).await;
                break;
            }
            () = events.closed() => break,
            _ = flush.tick() => {
                let now = Instant::now();
                bucket.refill(now.duration_since(last_refill));
                last_refill = now;
                if flush_lines(
                    &events,
                    &mut lines_rx,
                    &mut bucket,
                    &mut dropped,
                    &components,
                    cfg.batch_max_lines,
                )
                .await
                .is_err()
                {
                    break;
                }
                if dropped > 0 && last_drop_notice.elapsed() >= cfg.notice_interval {
                    if send_notice(&events, &Notice::Dropped { count: dropped }).await.is_err() {
                        break;
                    }
                    dropped = 0;
                    last_drop_notice = Instant::now();
                }
            }
            _ = refresh.tick() => {
                let next = current_roster(&ctx);
                if next == selection {
                    continue;
                }
                if send_json(&events, "pods", &next, None).await.is_err() {
                    break;
                }
                if next.capped && !selection.capped {
                    let notice = Notice::TruncatedPods {
                        total: next.total,
                        shown: next.pods.len(),
                    };
                    if send_notice(&events, &notice).await.is_err() {
                        break;
                    }
                }
                selection = next;
                components = component_index(&selection);
                let desired = roster::tail_keys(&selection, ctx.request.container.as_deref());
                let active: BTreeSet<TailKey> = tails.keys().cloned().collect();
                let to_start: Vec<TailKey> = desired.difference(&active).cloned().collect();
                let to_stop: Vec<TailKey> = active.difference(&desired).cloned().collect();
                let mut client_gone = false;
                for key in to_stop {
                    if let Some(handle) = tails.remove(&key) {
                        handle.abort();
                    }
                    // A vanished pod ends its tail with a notice, so the
                    // console shows a deliberate stop instead of silence.
                    let notice = Notice::TailEnded {
                        pod: &key.pod,
                        container: &key.container,
                    };
                    if send_notice(&events, &notice).await.is_err() {
                        client_gone = true;
                        break;
                    }
                }
                if client_gone {
                    break;
                }
                start_tails(&ctx, &lines_tx, &mut tails, to_start, TailStart::Catchup);
            }
        }
    }
    for handle in tails.values() {
        handle.abort();
    }
}

fn current_roster(ctx: &SessionContext) -> RosterSelection {
    roster::select(
        &ctx.pods.list(),
        &RosterFilter {
            namespace: &ctx.request.namespace,
            isvc: &ctx.request.isvc,
            component: ctx.request.component.as_deref(),
            pod: ctx.request.pod.as_deref(),
        },
        ctx.config.max_pods_per_session,
    )
}

fn component_index(selection: &RosterSelection) -> BTreeMap<String, Option<String>> {
    selection
        .pods
        .iter()
        .map(|pod| (pod.name.clone(), pod.component.clone()))
        .collect()
}

/// Whether a batch of tails starts with the caller's history request (session
/// start) or the short catch-up depth (mid-session joiners).
enum TailStart {
    Initial,
    Catchup,
}

fn start_tails(
    ctx: &SessionContext,
    lines: &mpsc::Sender<TailEvent>,
    tails: &mut BTreeMap<TailKey, JoinHandle<()>>,
    keys: Vec<TailKey>,
    mode: TailStart,
) {
    let cfg = &ctx.config;
    for (index, key) in keys.into_iter().enumerate() {
        let (tail_lines, since_seconds) = match mode {
            // The caller's history request applies once, to the pods present
            // at session start; late joiners get a short catch-up so a
            // rollout never re-dumps the full history per new pod.
            TailStart::Initial => (ctx.request.tail_lines, ctx.request.since_seconds),
            TailStart::Catchup => (Some(cfg.catchup_lines), None),
        };
        let tail = Tail {
            client: ctx.client.clone(),
            namespace: ctx.request.namespace.clone(),
            key: key.clone(),
            gate: ctx.tail_gate.clone(),
            lines: lines.clone(),
            line_bytes_max: cfg.line_bytes_max,
            retry_backoff: cfg.tail_retry_backoff,
            catchup_lines: cfg.catchup_lines,
            start_delay: cfg.start_stagger * index as u32,
            tail_lines,
            since_seconds,
        };
        tails.insert(key, tokio::spawn(tail.run()));
    }
}

/// What a tail task pushes into the session's bounded line channel.
enum TailEvent {
    Line(TailLine),
    /// A stream ended (EOF or error); the tail retries after the backoff.
    Ended {
        pod: String,
        container: String,
        error: String,
    },
}

/// One parsed log line from a tail task.
struct TailLine {
    pod: String,
    container: String,
    ts: String,
    line: String,
    truncated: bool,
}

/// One (pod, container) follow stream that never gives up: it reopens the
/// stream after the backoff on any end or error, reporting each occurrence
/// through the line channel. The session aborts it when the pod leaves the
/// roster or the session ends.
struct Tail {
    client: Client,
    namespace: String,
    key: TailKey,
    gate: Arc<Semaphore>,
    lines: mpsc::Sender<TailEvent>,
    line_bytes_max: usize,
    retry_backoff: Duration,
    catchup_lines: i64,
    start_delay: Duration,
    tail_lines: Option<i64>,
    since_seconds: Option<i64>,
}

impl Tail {
    async fn run(mut self) {
        tokio::time::sleep(self.start_delay).await;
        // One per-cluster tail slot, held for this tail's whole lifetime.
        let Ok(_tail_slot) = self.gate.clone().acquire_owned().await else {
            return;
        };
        let api: Api<Pod> = Api::namespaced(self.client.clone(), &self.namespace);
        loop {
            let error = self.follow_once(&api).await;
            let ended = TailEvent::Ended {
                pod: self.key.pod.clone(),
                container: self.key.container.clone(),
                error,
            };
            if self.lines.send(ended).await.is_err() {
                return; // session is gone
            }
            tokio::time::sleep(self.retry_backoff).await;
            // Replaying history belongs to the first attempt only; a reopened
            // stream picks up with a short catch-up.
            self.tail_lines = Some(self.catchup_lines);
            self.since_seconds = None;
        }
    }

    /// Follow the stream until it ends; the return value says why.
    async fn follow_once(&self, api: &Api<Pod>) -> String {
        let params = LogParams {
            container: Some(self.key.container.clone()),
            follow: true,
            timestamps: true,
            tail_lines: self.tail_lines,
            since_seconds: self.since_seconds,
            ..LogParams::default()
        };
        let reader = match api.log_stream(&self.key.pod, &params).await {
            Ok(reader) => reader,
            Err(error) => return format!("log stream failed to open: {error}"),
        };
        let mut lines = Box::pin(reader.lines());
        loop {
            match lines.try_next().await {
                Ok(Some(raw)) => {
                    let line = parse_line(&self.key, &raw, self.line_bytes_max);
                    if self.lines.send(TailEvent::Line(line)).await.is_err() {
                        return "session closed".to_owned();
                    }
                }
                Ok(None) => return "log stream ended".to_owned(),
                Err(error) => return format!("log stream read failed: {error}"),
            }
        }
    }
}

/// Split the kubelet timestamp prefix and enforce the line byte cap.
fn parse_line(key: &TailKey, raw: &str, line_bytes_max: usize) -> TailLine {
    let (ts, rest) = split_timestamp(raw);
    let (kept, truncated) = truncate_utf8(rest, line_bytes_max);
    TailLine {
        pod: key.pod.clone(),
        container: key.container.clone(),
        ts: ts.to_owned(),
        line: kept.to_owned(),
        truncated,
    }
}

/// The kubelet prefixes every line with an RFC3339(Nano) timestamp when
/// `timestamps=true`. A line without a recognizable prefix (defensive; log
/// APIs of some runtimes misbehave) is kept whole with an empty timestamp.
fn split_timestamp(raw: &str) -> (&str, &str) {
    if let Some((ts, rest)) = raw.split_once(' ') {
        // Minimal RFC3339 is 20 bytes ("2024-01-02T03:04:05Z"); the dash and
        // 'T' positions distinguish a timestamp from an arbitrary first word.
        if ts.len() >= 20 && ts.as_bytes().get(4) == Some(&b'-') && ts.contains('T') {
            return (ts, rest);
        }
    }
    ("", raw)
}

/// Truncate to at most `max_bytes`, backing up to a UTF-8 char boundary.
fn truncate_utf8(s: &str, max_bytes: usize) -> (&str, bool) {
    if s.len() <= max_bytes {
        return (s, false);
    }
    let mut end = max_bytes;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    (&s[..end], true)
}

/// One SSE `log` event: a batch of lines from a single pod container.
#[derive(Debug, PartialEq, Serialize)]
struct LogFrame {
    /// Pod name.
    p: String,
    /// Container name.
    ct: String,
    /// Component label, when the pod carries one.
    #[serde(skip_serializing_if = "Option::is_none")]
    co: Option<String>,
    /// `[timestamp, line]` pairs; a third `true` element flags truncation.
    ls: Vec<FrameLine>,
}

#[derive(Clone, Debug, PartialEq)]
struct FrameLine {
    ts: String,
    line: String,
    truncated: bool,
}

impl Serialize for FrameLine {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let len = if self.truncated { 3 } else { 2 };
        let mut seq = serializer.serialize_seq(Some(len))?;
        seq.serialize_element(&self.ts)?;
        seq.serialize_element(&self.line)?;
        if self.truncated {
            seq.serialize_element(&true)?;
        }
        seq.end()
    }
}

/// Session anomalies surfaced to the client without ending the stream.
#[derive(Debug, Serialize)]
#[serde(
    tag = "kind",
    rename_all = "kebab-case",
    rename_all_fields = "camelCase"
)]
enum Notice<'a> {
    /// The roster exceeded the per-session pod cap; only `shown` are tailed.
    TruncatedPods { total: usize, shown: usize },
    /// Lines dropped by the egress rate limit since the previous notice.
    Dropped { count: u64 },
    /// One pod stream ended or failed; it reopens after the backoff.
    TailError {
        pod: &'a str,
        container: &'a str,
        error: &'a str,
    },
    /// A pod left the roster; its tail ended deliberately, with no retry.
    TailEnded { pod: &'a str, container: &'a str },
    /// The configured session lifetime elapsed; the stream closes cleanly.
    SessionEnd { after_secs: u64 },
}

/// Drain everything buffered, spend the rate allowance, and emit the admitted
/// lines as batched frames plus a notice per ended stream. `Err` means the
/// client is gone.
async fn flush_lines(
    events: &EventSender,
    lines_rx: &mut mpsc::Receiver<TailEvent>,
    bucket: &mut TokenBucket,
    dropped: &mut u64,
    components: &BTreeMap<String, Option<String>>,
    batch_max_lines: usize,
) -> Result<(), ()> {
    let mut pending: Vec<TailLine> = Vec::new();
    let mut ended: Vec<(String, String, String)> = Vec::new();
    while let Ok(event) = lines_rx.try_recv() {
        match event {
            TailEvent::Line(line) => pending.push(line),
            TailEvent::Ended {
                pod,
                container,
                error,
            } => ended.push((pod, container, error)),
        }
    }

    let admitted = bucket.admit(pending.len());
    *dropped += (pending.len() - admitted) as u64;
    pending.truncate(admitted);

    for frame in frames(pending, components, batch_max_lines) {
        let id = frame
            .ls
            .last()
            .map(|line| line.ts.clone())
            .filter(|ts| !ts.is_empty());
        send_json(events, "log", &frame, id.as_deref()).await?;
    }
    for (pod, container, error) in &ended {
        let notice = Notice::TailError {
            pod,
            container,
            error,
        };
        send_notice(events, &notice).await?;
    }
    Ok(())
}

/// Batch drained lines into per-(pod, container) frames of at most
/// `batch_max_lines` lines, preserving arrival order within each stream.
fn frames(
    lines: Vec<TailLine>,
    components: &BTreeMap<String, Option<String>>,
    batch_max_lines: usize,
) -> Vec<LogFrame> {
    let mut grouped: BTreeMap<(String, String), Vec<FrameLine>> = BTreeMap::new();
    for line in lines {
        grouped
            .entry((line.pod, line.container))
            .or_default()
            .push(FrameLine {
                ts: line.ts,
                line: line.line,
                truncated: line.truncated,
            });
    }
    let mut out = Vec::new();
    for ((pod, container), lines) in grouped {
        let component = components.get(&pod).cloned().flatten();
        let mut chunk: Vec<FrameLine> = Vec::new();
        for line in lines {
            chunk.push(line);
            if chunk.len() == batch_max_lines {
                out.push(LogFrame {
                    p: pod.clone(),
                    ct: container.clone(),
                    co: component.clone(),
                    ls: std::mem::take(&mut chunk),
                });
            }
        }
        if !chunk.is_empty() {
            out.push(LogFrame {
                p: pod,
                ct: container,
                co: component,
                ls: chunk,
            });
        }
    }
    out
}

async fn send_notice(events: &EventSender, notice: &Notice<'_>) -> Result<(), ()> {
    send_json(events, "notice", notice, None).await
}

async fn send_json(
    events: &EventSender,
    name: &str,
    data: &impl Serialize,
    id: Option<&str>,
) -> Result<(), ()> {
    let mut event = SseEvent::default().event(name);
    if let Some(id) = id {
        event = event.id(id);
    }
    let event = event
        .json_data(data)
        .unwrap_or_else(|_| SseEvent::default().comment("encode error"));
    events.send(Ok(event)).await.map_err(|_| ())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use serde_json::json;

    use super::{frames, split_timestamp, truncate_utf8, FrameLine, Notice, TailLine};

    fn line(pod: &str, container: &str, ts: &str, text: &str) -> TailLine {
        TailLine {
            pod: pod.to_owned(),
            container: container.to_owned(),
            ts: ts.to_owned(),
            line: text.to_owned(),
            truncated: false,
        }
    }

    #[test]
    fn timestamps_split_off_the_kubelet_prefix_only() {
        let (ts, rest) = split_timestamp("2024-01-02T03:04:05.123456789Z GET /healthz 200");
        assert_eq!(ts, "2024-01-02T03:04:05.123456789Z");
        assert_eq!(rest, "GET /healthz 200");

        // A first word that isn't a timestamp stays part of the line.
        assert_eq!(
            split_timestamp("ERROR something broke"),
            ("", "ERROR something broke")
        );
        assert_eq!(split_timestamp("no-space-line"), ("", "no-space-line"));
    }

    #[test]
    fn truncation_lands_on_a_utf8_boundary_and_is_flagged() {
        assert_eq!(truncate_utf8("short", 10), ("short", false));
        assert_eq!(truncate_utf8("exactly-10", 10), ("exactly-10", false));
        assert_eq!(truncate_utf8("0123456789ab", 10), ("0123456789", true));
        // "é" is two bytes; a cap in the middle backs up to the boundary.
        assert_eq!(truncate_utf8("aé", 2), ("a", true));
    }

    #[test]
    fn frames_group_by_stream_and_chunk_at_the_batch_cap() {
        let components: BTreeMap<String, Option<String>> = BTreeMap::from([
            ("pod-a".to_owned(), Some("engine".to_owned())),
            ("pod-b".to_owned(), None),
        ]);
        let lines = vec![
            line("pod-a", "main", "t1", "one"),
            line("pod-b", "main", "t2", "two"),
            line("pod-a", "main", "t3", "three"),
            line("pod-a", "main", "t4", "four"),
        ];

        let out = frames(lines, &components, 2);
        assert_eq!(out.len(), 3);
        // pod-a/main chunks at 2 lines per frame, ordered by arrival.
        assert_eq!(out[0].p, "pod-a");
        assert_eq!(out[0].co.as_deref(), Some("engine"));
        assert_eq!(out[0].ls.len(), 2);
        assert_eq!(out[0].ls[1].ts, "t3");
        assert_eq!(out[1].ls.len(), 1);
        assert_eq!(out[1].ls[0].ts, "t4");
        assert_eq!(out[2].p, "pod-b");
        assert_eq!(out[2].co, None);
    }

    #[test]
    fn frame_lines_serialize_as_pairs_with_a_truncation_flag() {
        let plain = FrameLine {
            ts: "t1".to_owned(),
            line: "hello".to_owned(),
            truncated: false,
        };
        assert_eq!(
            serde_json::to_value(&plain).unwrap(),
            json!(["t1", "hello"])
        );

        let cut = FrameLine {
            truncated: true,
            ..plain
        };
        assert_eq!(
            serde_json::to_value(&cut).unwrap(),
            json!(["t1", "hello", true])
        );

        let components = BTreeMap::from([("pod-a".to_owned(), Some("engine".to_owned()))]);
        let frame = &frames(vec![line("pod-a", "main", "t1", "hello")], &components, 10)[0];
        assert_eq!(
            serde_json::to_value(frame).unwrap(),
            json!({ "p": "pod-a", "ct": "main", "co": "engine", "ls": [["t1", "hello"]] })
        );
    }

    #[test]
    fn notices_serialize_with_kebab_case_kinds_and_camel_case_fields() {
        assert_eq!(
            serde_json::to_value(Notice::TruncatedPods {
                total: 60,
                shown: 48
            })
            .unwrap(),
            json!({ "kind": "truncated-pods", "total": 60, "shown": 48 })
        );
        assert_eq!(
            serde_json::to_value(Notice::Dropped { count: 12 }).unwrap(),
            json!({ "kind": "dropped", "count": 12 })
        );
        assert_eq!(
            serde_json::to_value(Notice::TailError {
                pod: "pod-a",
                container: "main",
                error: "forbidden",
            })
            .unwrap(),
            json!({ "kind": "tail-error", "pod": "pod-a", "container": "main", "error": "forbidden" })
        );
        assert_eq!(
            serde_json::to_value(Notice::TailEnded {
                pod: "pod-a",
                container: "main",
            })
            .unwrap(),
            json!({ "kind": "tail-ended", "pod": "pod-a", "container": "main" })
        );
        assert_eq!(
            serde_json::to_value(Notice::SessionEnd { after_secs: 3600 }).unwrap(),
            json!({ "kind": "session-end", "afterSecs": 3600 })
        );
    }
}
