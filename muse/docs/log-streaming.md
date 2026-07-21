# Deployment log streaming for MUSE

## 1. Summary

Add **stern-like multi-pod log tailing** to the MUSE deployment page: pick a
deployment (InferenceService), and muse-server tails every matching pod ×
container on the target cluster, aggregates the lines server-side, and delivers
them to the console as **one SSE stream**. This is a **read-plane** feature —
`pods/log` is a read verb, no write plane involved — and it is **config-gated
and fail-closed**: absent config ⇒ the Logs tab is hidden and no tailing code
path exists at runtime. It ships **disabled** (`enabled: false` in the chart),
and because one MUSE deployment aggregates many workload clusters, enablement
is **per cluster**: a cluster becomes tailable only when it is added to the
`config.logs.clusters` allowlist, after its `pods/log` RBAC grant has landed
(§8).

The approved UI design is `muse/docs/mockups/deployment-page.html` (runtime
card + Logs tab); the frontend summary is §9.

## 2. Motivation

Diagnosing a misbehaving InferenceService today means leaving MUSE: find the
pods (`kubectl get pods -l ome.io/inferenceservice=…` against the right
cluster), then juggle N `kubectl logs -f` terminals or install stern locally
with per-cluster kubeconfigs. At MSL scale the deployment an operator is
staring at in MUSE may span dozens of pods on a 2900-node cluster they have no
local credentials for.

MUSE already holds everything needed to do this well: per-cluster authenticated
clients, a cached Pod lister covering every workload cluster, and an SSE path
through the shared gateway that is already carved out for long-lived responses
(`/v1/stream`). Log tailing is the same shape as the existing read plane — fan
out per cluster, aggregate server-side, serve one stream the browser could
never assemble itself — applied to `pods/log` instead of list/watch.

## 3. Goals / Non-goals

### Goals

- **One SSE stream per session** carrying interleaved, timestamped lines from
  every pod × container of one deployment (optionally narrowed to one
  component, pod, or container).
- **Roster tracking**: pods that appear mid-session (rollouts, scale-out) join
  the tail automatically; pods that vanish end their tail with a notice, not a
  broken session.
- **Bounded resource usage, end to end**: session caps, per-cluster tail caps,
  bounded buffers, an egress rate cap, and a hard session lifetime — every
  bound config-supplied, none baked into the binary.
- **Graceful degradation**: a pod whose logs 403, a rotated pod, a kubelet EOF
  — each becomes a retried per-tail notice; one bad pod or cluster never kills
  the session.
- **Config-gated, fail-closed**: no `logs` config ⇒ feature off, tab hidden,
  `GET /v1/config` says so explicitly.

### Non-goals (this doc)

- **Log search / history.** This is live tailing plus a small catch-up window
  (`tail`/`sinceSecs`), not a log store. No indexing, no retention, no
  historical queries (see §10 — Loki-class systems solve a different problem).
- **Server-side grep in v1.** Filtering is client-side; the `grep` query
  parameter is reserved — accepted and ignored by the v1 server (§4.2) — so
  server-side grep can land later without a protocol change (§11).
- **Exactly-once / lossless delivery to the browser.** Delivery is lossless
  from kubelet to muse-server (blocking backpressure, §4.3); the session egress
  cap may drop lines toward a slow client, and every drop is **counted and
  surfaced** as a notice — never silent.
- **Writes of any kind.** No exec, no attach, no port-forward. `pods/log` get
  is the only new verb.

## 4. Architecture

### 4.1 Where it lives

A new module in `muse-server` beside the other read-plane sources, wired into
the axum router in `server.rs` and the shared per-cluster state. It reuses the
**retained per-cluster `kube::Client`s** that the informer layer already holds
(`informers.rs` keeps `pub client: Client` per cluster) — no new connection
machinery, no separate binary. Log tailing is not a process split: it is one
more read source feeding the same server, the same DTO conventions, the same
console.

### 4.2 Endpoint and gateway routing

```
GET /v1/logs/stream?cluster=&namespace=&isvc=&component=&pod=&container=&tail=&sinceSecs=&grep=
```

One **fixed path** with query parameters — deliberately not a REST-ish
`/v1/logs/{cluster}/{ns}/{isvc}` — because the shared-gateway HTTPRoute
(`charts/muse/templates/httproute.yaml`) matches its stream rules with
`type: Exact`. A parameterized path would need a `PathPrefix` stream rule,
which would widen the no-timeout carve-out beyond the streaming endpoints.
Instead the new path is appended to `route.streams.paths` in
`charts/muse/values.yaml`, inheriting the existing exact-match rule that sets
`timeouts.request: 0s` (SSE responses are intentionally long-lived; Envoy's
finite route timeout stays in force for everything else).

Parameters:

| Param | Meaning |
|---|---|
| `cluster` | Target cluster (required; must be a configured, **log-enabled** cluster — §5 `clusters`). |
| `namespace`, `isvc` | The deployment to tail (required). |
| `component` | Optional narrowing to one component (engine / decoder / router). |
| `pod`, `container` | Optional narrowing to a single pod and/or container. |
| `tail` | Catch-up lines per tail on join (≤ `tailLinesMax`). |
| `sinceSecs` | Alternative catch-up: lines newer than now−N seconds. Also the reconnect-resume knob (§4.4). |
| `grep` | **Reserved, ignored in v1**: the server accepts the parameter and does not apply it (no 4xx), so server-side filtering (§11) can land without a protocol change. |

Requests are validated against config before any tail starts: unknown or
non-log-enabled cluster, feature disabled, or over-cap values are a fast 4xx,
never a half-open stream. The reserved `grep` param is the one deliberate
exception to that strictness — accepted and ignored, per the table above.

### 4.3 Fan-in pipeline (per session)

One session = one HTTP request = one task tree. The flow, in order:

1. **Global session semaphore** (`maxSessions`). Full ⇒ immediate 429-class
   refusal. Sessions are the unit of accounting; everything below is owned by
   the session and torn down with it.
2. **Pod roster** from the **existing cached Pod lister**, selected by the
   `ome.io/inferenceservice` label (plus the `component` label when narrowing).
   No apiserver list in the request path. Labels already survive the
   aggressive `ome-client-rs` Pod prune (`informers.rs` drops annotations,
   spec, and status detail but keeps `metadata.labels` and container names), so
   **no prune widening is needed** — the roster costs nothing extra at
   ~10^5-pod cache scale. The roster is capped at `maxPodsPerSession`; overflow
   is reported via a `truncated-pods` notice (§4.5), not silently dropped.
3. **One tail per pod × container** via kube-rs
   `Api<Pod>::log_stream(LogParams { follow, tail_lines, timestamps, container,
   since_seconds })` on the retained per-cluster client. `timestamps: true`
   always — kubelet's RFC3339 stamps are the interleave key and the SSE event
   id (§4.5).
4. **Jittered starts + per-cluster tail semaphore.** Tail starts are staggered
   by `startStaggerMs` so a 48-pod session doesn't hit the apiserver with 48
   simultaneous `pods/log` upgrades, and every tail (across *all* sessions)
   holds a permit from a per-cluster semaphore (`maxTailsPerCluster`). Both are
   apiserver protection: at 2900-node scale the apiserver is the shared,
   precious resource, and `pods/log` streams hold an apiserver→kubelet
   connection each for their whole life.
5. **Bounded mpsc, BLOCKING send.** Each tail sends parsed lines into the
   session's bounded channel (`bufferLines` capacity, lines capped at
   `lineBytesMax`). When the channel is full the tail task **blocks** —
   lossless backpressure. The stall propagates naturally: the tail stops
   reading its HTTP body, kubelet's TCP window fills, and the kernel buffers
   absorb the burst. No unbounded queue, no drop at this stage.
6. **Session egress cap.** A token bucket (`maxLinesPerSec`) bounds what one
   session can push to one browser. Lines over the rate are dropped **counted**
   — the drop total is reported in periodic `dropped` notices — because toward
   a slow client, dropping visibly beats buffering unboundedly or stalling the
   roster machinery. This is the only lossy stage, and it is never silent.
7. **Batched SSE events.** Lines are flushed every `flushIntervalMs` or
   `batchMaxLines`, whichever first, as one `log` event per pod×container
   batch (§4.5). Batching is what keeps 2000 lines/s from being 2000
   `text/event-stream` frames/s.

### 4.4 Roster resync, failure behavior, session lifetime

**Roster resync.** A timer (`podRefreshSecs`) re-evaluates the label selector
against the cached lister and diffs it against the live tail set. New pods
(scale-out, rollout surge) get tails started automatically — with catch-up
capped at `catchupLines` so a mid-session join doesn't replay a pod's history —
and a `pods` roster event tells the console. Disappeared pods end their tail
with a `tail-ended` notice. Pods are deliberately **not** picked up from the
informer event feed (see §10): a 5-second lister diff is bounded, simple, and
fast enough for human log-watching.

**Per-tail failure isolation.** A tail that fails — 403 because the member
cluster hasn't granted `pods/log` yet, a rotated/deleted pod, a kubelet EOF or
connection reset — emits a `tail-error` notice and is retried with a flat
`tailRetryBackoffSecs` backoff (until the roster drops the pod). Notices are
rate-limited per session by `noticeIntervalSecs` so a crash-looping pod cannot
flood the stream with its own failure reports. **One bad pod or one bad
cluster never terminates the session**; the operator sees exactly which tails
are degraded and why.

**Session lifetime.** Every session is hard-capped at `sessionMaxSecs`, ending
with a `session-end` notice. This is the reclamation valve for the walked-away
browser tab: SSE half-open detection is unreliable behind the gateway, and a
leaked session would otherwise hold tail permits and an egress task forever.

**Deploys cut sessions — by design.** muse-server's shutdown grace is 2s
(`shutdownGraceSecs` in the chart), so every muse deploy severs all log
sessions. Recovery is **client-managed reconnect**, not the browser's native
`EventSource` retry: native auto-reconnect re-issues the original construction
URL verbatim, which would replay the session's entire initial catch-up window
(e.g. a full 15-minute `sinceSecs`) into the console ring buffer on every muse
deploy. Instead the console closes the `EventSource` in its error handler and
opens a new one with `tail` dropped and `sinceSecs` recomputed from the
timestamp of the last line it received (§9). The overlap is then bounded by
the reconnect gap plus timestamp rounding — a few seconds of duplicate lines,
not the whole catch-up window. That makes reconnect-with-small-overlap the
**expected, routine path**, not an edge case. The SSE `id:` field still
carries the last line timestamp from day one (§4.5) as a coarse global
cursor; §11 covers what it can and cannot buy for finer resume.

### 4.5 SSE protocol

The response is standard `text/event-stream`. First frame is the comment
`: connected` — it commits the HTTP status early (before the first tail
produces anything) and lets the console distinguish "stream open, logs quiet"
from "request stuck".

Three event types:

| Event | Payload | Notes |
|---|---|---|
| `log` | `{ "p": pod, "ct": container, "co": component, "ls": [[ts, line], …] }` | One batch of lines from one pod × container. Short keys are deliberate: at `maxLinesPerSec` the envelope overhead is material. `ts` is the kubelet RFC3339 timestamp; `line` is the raw line, truncated at `lineBytesMax`. |
| `pods` | roster snapshot `[{ pod, component, containers, state }, …]` | Sent once on session start and again on every roster change, so the console's pod filter chips and per-pod hues never drift from what is actually being tailed. |
| `notice` | `{ "kind": "truncated-pods" \| "tail-error" \| "tail-ended" \| "session-end" \| "dropped", … }` | Out-of-band session/tail conditions with kind-specific fields (pod, error class, drop count, retry-in). Rendered inline in the log view, rate-limited by `noticeIntervalSecs`. |

Every `log` event sets the SSE `id:` field to the timestamp of its last line.
This is a **coarse, global cursor**: it marks how far the *merged* stream has
progressed, not where each per-pod tail is. With N independent tails, a pod
whose last delivered line is older than the global cursor still has
undelivered lines *behind* it, so a single timestamp can only support
overlap-based resume — a finer-grained `sinceSecs`, not exact resume (§11).
v1 does not consume it at all: reconnect is client-managed via `sinceSecs`
(§4.4), and a client-constructed `EventSource` does not carry `Last-Event-ID`
anyway. It is stamped from day one so a future resume refinement can read it
without a protocol change.

### 4.6 Data flow (end to end)

```
kubelet ×N ──pods/log follow──► tail tasks (per pod×container, kube-rs log_stream)
                                   │  jittered starts, per-cluster tail semaphore
                                   ▼
                     bounded mpsc (bufferLines × lineBytesMax, BLOCKING send)
                                   │  lossless: full buffer ⇒ tail blocks ⇒ TCP backpressure
                                   ▼
                     session egress token bucket (maxLinesPerSec, counted drops)
                                   │
                                   ▼
                     batcher (flushIntervalMs / batchMaxLines)
                                   │
        roster resync timer ───────┤ pods / notice events
        (podRefreshSecs, lister diff)
                                   ▼
   GET /v1/logs/stream ── SSE ──► gateway (Exact route, request timeout 0s) ──► console Logs tab
```

## 5. Configuration

A `logs` block following the existing clap-flags-plus-env pattern in
`config.rs`, with the same discipline the chat proxy established
(`chat_proxy_config`): **zero in-binary defaults**. Every knob is a
`--logs-*` flag / `MUSE_LOGS_*` env var; the values live in
`charts/muse/values.yaml` under `config.logs.*` and render into the
Deployment. Enabling the feature without its required knobs is a configuration
error handled by **leaving the feature disabled with a logged reason** — no
value is invented in code. Absent config ⇒ the feature is off and the rest of
MUSE is untouched.

Chart-supplied values (the binary carries none of these):

| Key | Chart value | Meaning |
|---|---|---|
| `enabled` | `false` | Master switch for the environment (one muse-server deployment). Off ⇒ no routes, no tails, tab hidden. Ships `false` to match the disabled-by-default posture (§1); an environment flips it when its first cluster completes the RBAC checklist (§8). |
| `clusters` | `[]` | **Per-cluster allowlist** — the per-cluster half of the gate, since one MUSE instance aggregates many workload clusters. Only listed clusters can be tailed; requests for unlisted clusters are a fast 4xx (§4.2) and the console hides the Logs tab for them (§6). Validated at startup against the configured cluster names (unknown name ⇒ startup error), same discipline as the existing cluster-name checks. A cluster is added only after its `pods/log` grant is verified (§8). |
| `maxSessions` | `16` | Global concurrent session cap (the semaphore in §4.3). |
| `maxPodsPerSession` | `48` | Roster cap; overflow ⇒ `truncated-pods` notice. |
| `maxTailsPerCluster` | `256` | Cross-session cap on concurrent `pods/log` streams per cluster (apiserver protection). |
| `bufferLines` | `512` | Per-session bounded channel capacity. |
| `lineBytesMax` | `8192` | Per-line byte cap (longer lines truncated). |
| `maxLinesPerSec` | `2000` | Session egress token-bucket rate. |
| `tailLinesMax` | `5000` | Upper bound on the `tail` query param. |
| `catchupLines` | `10` | Catch-up lines for pods joining mid-session. |
| `podRefreshSecs` | `5` | Roster resync interval (lister diff). |
| `sessionMaxSecs` | `3600` | Hard session lifetime (reclamation valve). |
| `startStaggerMs` | `25` | Jitter between tail starts. |
| `noticeIntervalSecs` | `5` | Per-session notice rate limit. |
| `tailRetryBackoffSecs` | `10` | Backoff before retrying a failed tail. |
| `flushIntervalMs` | `200` | SSE batch flush interval. |
| `batchMaxLines` | `500` | Max lines per `log` event. |
| `clientBufferLines` | `5000` | Console ring-buffer size, served to the UI via `GET /v1/config` (§6). |
| `sinceOptionsJson` | `{"default":"15m","options":["5m","15m","1h","6h"]}` | The console's "since" selector. Parsed and validated at startup exactly like `metricsTimeRanges` (non-empty options, unique ids, default ∈ options); invalid ⇒ startup error, not a silent fallback. |

**Memory bound, validated at startup.** Worst-case buffered log memory is
`maxSessions × bufferLines × lineBytesMax` = 16 × 512 × 8 KiB ≈ **64 MiB**. A
cross-field check in `Config::validate()` (beside the existing latency-bucket
and cluster-name checks) computes this product against the configured pod
memory budget and rejects a combination that could not fit — the operator
finds out at deploy time, not as an OOMKill under load.

## 6. ConfigResp additions

The console never hardcodes these numbers; `GET /v1/config` (`ConfigResp` in
`protocol.rs`) is its **only source**, following the existing
`chat`/`metrics` pattern:

| Field | Meaning |
|---|---|
| `logs: bool` | Feature on/off. `false` ⇒ the Logs tab is not rendered at all. |
| `logsClusters` | The log-enabled cluster allowlist (§5 `clusters`). The console renders the Logs tab only for deployments on these clusters — a cluster whose grant hasn't landed shows **no tab**, not a permanently-403ing one. Omitted when disabled. |
| `logsCaps` | `{ maxPodsPerSession, tailLinesMax, maxLinesPerSec }` — the caps the UI must reflect (input clamping, truncation hints). Omitted when disabled. |
| `logsSinceOptions` | The validated `sinceOptionsJson` payload driving the "since" selector. Omitted when disabled. |
| `logsClientBufferLines` | Ring-buffer size for the console log view. Omitted when disabled. |

Disabled is an explicit `logs: false` — the same non-error signal shape the
catalog and chat features use — never a 404 the frontend has to interpret.

## 7. Security & privacy

- **Read-plane only.** `pods/log` get is a read verb; no write, exec, attach,
  or port-forward path exists. Log *content* transits muse-server but is never
  stored — buffers are bounded, in-memory, and die with the session.
- **Scoped access.** Sessions can only target configured, log-enabled
  clusters (§5 `clusters`), and only pods matching the deployment label
  selector. There is no free-form pod-name/namespace tailing beyond what the
  roster resolves.
- **No credentials in the stream.** Tail errors surface an error class
  (`403`, `gone`, `eof`) and the pod name — never tokens, kubeconfig paths, or
  raw apiserver responses.
- **Fail closed.** No config ⇒ no routes registered. Missing RBAC on a member
  cluster ⇒ per-pod `tail-error` notices (§4.4), never a crash and never an
  escalation path.

## 8. RBAC & rollout

The chart's ClusterRole (`charts/muse/templates/rbac.yaml`) covers only the
cluster muse-server runs in; today it grants `nodes`/`pods` `get, list, watch`.
Log tailing adds **`pods/log` (get)** there, gated on `config.logs.enabled` —
the same conditional pattern the metrics block uses for `services/proxy`.

Remote workload clusters are read via their mounted kubeconfigs, so **their**
RBAC lives in **their own GitOps**, not in this chart. Each member cluster must
grant `pods/log` get to the identity its kubeconfig presents.

Rollout checklist, per cluster:

1. Member-cluster GitOps: add `pods/log` (get) to the role bound to the
   kubeconfig identity MUSE uses for that cluster.
2. Verify from a muse-server pod: tail one pod on that cluster with the same
   credentials (any `pods/log` get succeeding is sufficient).
3. Hub GitOps: add the cluster to `config.logs.clusters` (and flip
   `config.logs.enabled` to `true` if this is the environment's first
   log-enabled cluster).
4. Confirm in the console: open a deployment on that cluster, Logs tab, no
   `tail-error kind: 403` notices.

**The allowlist is the rollout gate; 403 handling is the safety net.** A
cluster whose grant hasn't landed is simply absent from
`config.logs.clusters`: no tab (§6), no requests, no noise — this is what
makes the §1 posture (**disabled per cluster until the grant lands**)
actually expressible with one MUSE instance aggregating many clusters. The
403 `tail-error` path (§4.4) exists for drift, not for rollout: if a grant is
revoked after enablement, or an allowlist entry lands before the grant
finishes propagating, every tail on that cluster fails with a 403 notice
retried on `tailRetryBackoffSecs`. The operator sees "logs not yet authorized
on this cluster" in the stream itself; nothing crashes, other clusters are
unaffected.

## 9. Frontend

The deployment page (`web/src/panels/DeploymentDetail.tsx`) gains a **fourth
tab — Logs** — beside overview / monitoring / activity, implementing the
approved mockup (`muse/docs/mockups/deployment-page.html`):

- **Virtualized, bounded ring buffer** of `logsClientBufferLines` lines —
  memory-stable during hour-long sessions; oldest lines fall off, with a
  "buffer full, older lines dropped" affordance.
- **Pod-prefix hues as CSS classes** composed from `base.css` design tokens
  (the mockup's classes map 1:1 to `web/src/styles/*`) — stern-style visual
  separation without inline styles.
- **Client-side grep in v1**: the filter box narrows the ring buffer locally.
  The server `grep` query param is reserved (§11) so moving filtering
  server-side later changes no UI contract.
- **Filters restart the stream**: narrowing to a component / pod / container
  or changing the "since" selection closes the EventSource and reopens
  `/v1/logs/stream` with narrower server params — the server then tails less,
  rather than the client hiding lines the server still ships.
- **Client-managed reconnect** (§4.4): on stream error/close the console
  closes the `EventSource` — suppressing native auto-reconnect, which would
  replay the original catch-up window verbatim — and reopens
  `/v1/logs/stream` with `tail` omitted and `sinceSecs` recomputed from the
  timestamp of the last line received, bounding the duplicate overlap to the
  reconnect gap.
- **Per-cluster tab gating**: the Logs tab renders only when the deployment's
  cluster is in `logsClusters` (§6) — clusters awaiting their RBAC grant get
  no tab rather than a stream of 403 notices.
- Roster chips from `pods` events; notices rendered inline in the log flow.

All caps and options rendered by the UI come from `GET /v1/config` (§6) —
no magic numbers in TSX.

## 10. Alternatives considered, not chosen

- **WebSocket or NDJSON streaming transport.** SSE wins on every axis that
  matters here: the gateway's stream carve-out already exists for SSE paths
  (`route.streams.paths`), event framing (typed events + `id:`) comes for
  free, and the flow is strictly server→client — WebSocket's bidirectionality
  buys nothing. Reconnect is client-managed on any transport (§4.4 — native
  `EventSource` retry replays a stale URL, so its built-in reconnect is
  deliberately unused), which levels that axis; NDJSON over fetch-streaming
  would still reimplement event framing and the notice/roster channels by
  hand.
- **Watch-driven pod pickup.** Pods are deliberately excluded from the
  informer change-event feed (the feed drives live-query invalidation; pods at
  ~10^5 scale would swamp it), and roster changes are not latency-critical — a
  human is reading logs, not a machine. The `podRefreshSecs` lister diff is
  bounded, allocation-free against the cache, and arrives within seconds.
  Wiring pods into the event feed for this would tax every MUSE deployment to
  speed up a corner of one tab.
- **Server-embedded runtime summary in the log stream.** The mockup's runtime
  card (resolved runtime identity, image, engine args) is a separate read view,
  not a log-stream concern; mixing it into the SSE protocol would couple two
  lifecycles that update at wildly different rates. It stays a plain REST
  fetch; the real fix for its data quality is OME-side (§11).
- **Loki / external log store.** Solves retention, search, and cross-service
  aggregation — none of which are goals (§3). It requires per-cluster agents,
  storage, and an ingestion pipeline MUSE cannot assume on member clusters.
  Live tailing straight from kubelet needs zero new cluster infrastructure and
  one RBAC verb. If a log store materializes later, it complements rather than
  replaces the live tail.
- **Per-pod fan-out from the browser.** N `EventSource`s from the console to N
  per-pod endpoints breaks on every count: browsers cap concurrent connections
  per origin far below `maxPodsPerSession`, every pod would multiply gateway
  route traffic, interleaving/ordering moves into the client, and server-side
  aggregation limits (per-cluster tail semaphore, egress cap) become
  unenforceable. Server-side fan-in is the entire point of the design.

## 11. Follow-ups

- **Finer timestamp resume — still overlap-based.** The protocol already
  stamps `id:` with the last line timestamp (§4.5); a follow-up can have the
  client-managed reconnect (§4.4) pass that cursor explicitly (e.g. a
  `sinceTs` param) so the server resumes from the exact timestamp instead of
  a rounded-up `sinceSecs` — a smaller overlap, but **not exact resume**: a
  single global timestamp cannot exactly resume N independent per-pod tails,
  because a tail whose last delivered line predates the global cursor has
  undelivered lines behind it. True exact resume needs per-pod cursors in the
  event id — a protocol change, worth designing only if the overlap proves
  costly in practice.
- **Server-side grep.** If real sessions show bandwidth pressure from
  high-volume deployments filtered client-side, implement the reserved `grep`
  param server-side (filter before the egress token bucket, so filtered lines
  stop costing egress budget).
- **OME-side resolved-runtime status (OEP-sized).** The runtime card wants the
  *resolved* runtime identity (after auto-selection) as a first-class ISVC
  status field instead of MUSE re-deriving it from child resources. That is an
  OME API change with its own OEP, tracked separately from this read-plane
  feature.

## Status

Draft — 2026-07-20, author MUSE (design), pending review
