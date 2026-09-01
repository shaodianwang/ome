# OEP-0021.2: MUSE system status — an admin view of OME's own control plane

<!-- toc -->

## Summary

**Tl;dr:** MUSE today shows *workloads* — InferenceServices, models, runtimes,
capacity — but shows nothing about the OME control plane that reconciles them.
When the `ome-manager` controller is crash-looping, the scheduler is scaled to
zero, or the `model-agent` DaemonSet is missing on half the fleet, the operator's
first signal is a workload that silently stops progressing, with no place in the
console that says *why*. This OEP adds one read-only, admin-facing panel — **System**
— that reports the health of OME's own system components, per aggregated cluster,
served from the same informer caches the rest of MUSE reads.

This is a follow-up to OEP-0021. It changes no workload behavior and writes to no
cluster; it is strictly a new read projection plus its panel. Two design choices
are load-bearing and settled here:

- **It watches `apps/v1` Deployments and DaemonSets**, not just Pods, so it can
  report *desired*-vs-*ready* — including the two states a pod scan alone cannot
  see: a component scaled to zero, and a component **not deployed at all**.
- **The set of components is entirely config-driven.** Which objects count as "the
  controller" or "the scheduler" — their namespaces, kinds, and label selectors —
  is supplied by config with **no in-code default**, because those identifiers are
  install- and provider-specific and hardcoding them is a repo-wide hard blocker.

Define the load-bearing terms up front; the rest of the doc keeps them apart:

- **System component** — a piece of OME's *own* control plane: the `ome-manager`
  controller, the scheduler (`scheduler-plugins` or equivalent), the admission
  webhook, the `model-agent` DaemonSet. Contrast a **workload** — an
  InferenceService's serving pods. MUSE already surfaces workloads; this OEP is
  about components. The distinction matters because they fail differently and the
  audience is different: a workload owner cares about their InferenceService; only
  an operator cares whether the controller is up.
- **Component descriptor** — the config-supplied identity of one system component:
  a display name, a group, a target kind (`Deployment` | `DaemonSet`), a namespace,
  a label selector, and an optional cluster scope. Descriptors are the *only* thing
  that tells MUSE what to look for; absent descriptors, the panel is empty, not
  wrong.
- **Component health** — a derived status in a fixed lattice —
  `healthy` › `degraded` › `unavailable` › `missing` › `unknown` — computed from
  the object's desired-vs-ready counts and rollout conditions. Defined precisely in
  [Design Details](#design-details); "worst wins" when a descriptor resolves to
  more than one object or spans clusters.
- **The cache-only invariant** — MUSE serves every read from an informer cache and
  **never** makes a per-request apiserver call (OEP-0021's defining property). This
  view keeps that invariant: it adds informers, not request-time gets.

## Motivation

OEP-0021 built MUSE as a *workload* console: Fleet, Deployments, Models, Runtimes,
Capacity. Every panel answers "what is being served, and how healthy is the
serving?" None answers "is OME itself healthy?" — and the control plane is exactly
what turns a user's InferenceService into running pods (OEP-0019 split it into the
manager, and companion processes).

The failure mode we are designing *against* is the **silent-controller stall**: a
control-plane component is down or misconfigured, so no reconcile happens, so a
user's InferenceService sits in a pending state that looks — in every existing MUSE
panel — like a slow workload rather than a broken operator. Today an operator
diagnoses this by leaving MUSE entirely and running `kubectl -n <ome-ns> get
deploy,ds,pods` against each cluster by hand. That is slow, it does not aggregate
across the fleet, and it is the one thing MUSE's multi-cluster informer transport
is already positioned to make trivial.

Three concrete cases the current console cannot show, that this view must:

1. **The controller is crash-looping.** `ome-manager` has `readyReplicas: 0,
   replicas: 2`; a `Deployment` scan sees this immediately, a workload panel sees
   only that nothing is progressing.
2. **A component is scaled to zero.** Someone set the scheduler's `replicas: 0`
   during an incident and forgot to restore it. There are no pods to scan — the
   *desired* count is the only evidence, and it lives on the Deployment, not on any
   Pod.
3. **`model-agent` is missing on a member cluster.** The DaemonSet was never
   installed on that cluster, so models never localize there. Across a 20-cluster
   fleet this is invisible until a deploy to that cluster fails; a per-cluster
   descriptor with an expected cluster scope turns "absent from cache" into an
   explicit **missing** signal.

### Goals

- Add one read-only panel, **System**, that lists OME's configured system
  components with a per-component, per-cluster health rollup, plus a fleet-level
  roll-up ("3 of 4 components healthy across 20 clusters").
- Report **desired-vs-ready** and rollout conditions for Deployments and
  DaemonSets — i.e. distinguish `degraded` (partial) from `unavailable` (zero
  ready, some desired) from `missing` (expected but absent).
- Keep the **cache-only invariant**: all data comes from new informers, never a
  request-time apiserver call.
- Make the component set **100% config-driven**, with no in-code default, and
  degrade gracefully (empty panel, neutral state) when unconfigured.
- Fit the existing conventions: one bespoke aggregate endpoint (`GET /v1/system`),
  one new panel, the standard nav/route wiring — nothing bespoke to maintain.

### Non-Goals

- **Not a metrics or logs view.** Reconcile throughput, queue depth, error rates,
  and pod logs are out of scope; those belong to the existing Prometheus/SSE
  metrics path (OEP-0021) and to `kubectl`/observability tooling respectively. This
  view answers "is the component up and at its desired scale?", not "how fast is it
  reconciling?".
- **Not a write or remediation surface.** No restart, scale, or rollout-restart
  buttons. Writes are OEP-0021.1's domain (PR-per-write) and are deliberately kept
  out of the broadly-exposed read plane.
- **Not authn/authz.** The read plane has no identity layer today; "admin" here
  names the *audience and framing*, not an enforced role. Gating is an
  [Open Question](#open-questions), deferred to the gateway, not invented here.
- **Not a general Kubernetes workload browser.** The view shows only the components
  named by descriptors — it is not `kubectl get deploy -A` in a browser.
- **Not auto-discovery of components** (considered and rejected — see
  [Alternatives](#alternatives)).

## Proposal

### The shape, in one paragraph

Add a Deployment and a DaemonSet informer to each cluster's `ClusterState`. Add a
config field — a list of **component descriptors** — supplied via
`--system-components-json` / `MUSE_SYSTEM_COMPONENTS_JSON` / Helm
`config.systemComponents`, with no baked default. Add one handler,
`GET /v1/system[?cluster=]`, that, for each descriptor, resolves the matching
cached objects on each in-scope, cache-ready cluster, computes a health status, and
returns rows tagged with cluster — exactly the bespoke-aggregate pattern
`clusters.rs` and `rollouts.rs` already follow. Add one frontend panel, **System**,
following `Fleet.tsx`, and wire it into the nav and router. Expand the chart's
read-only ClusterRole to `get/list/watch` `apps/v1` Deployments and DaemonSets.

### User stories

- **As an OME operator**, when a deploy stops progressing, I open **System**, see
  `ome-manager` is `unavailable` on the hub cluster (`0/2 ready`,
  `Available=False`), and know to look at the controller before the workload.
- **As an operator running a 20-cluster fleet**, I open **System** and see
  `model-agent` is `healthy` on 19 clusters and `missing` on one — the fleet
  roll-up surfaces the one bad cluster without my scanning twenty.
- **As an operator during an incident**, I see the scheduler is `unavailable`
  because it is scaled to `replicas: 0`, a state no pod-only view could show.

### Risks and mitigations

- **RBAC surface grows.** The read plane's ServiceAccount currently gets
  `get/list/watch` on OME CRDs + nodes/pods only (`charts/muse/values.yaml`). Adding
  Deployments/DaemonSets widens that. *Mitigation:* the addition is `get/list/watch`
  on two apps/v1 kinds — read-only, no secrets, no arbitrary namespaces beyond what
  the informers watch. Scope the informers to the configured namespaces where
  practical (see [Design Details](#design-details)).
- **Misconfiguration reads as a healthy-empty panel.** With no descriptors, the
  panel is empty — indistinguishable at a glance from "everything is fine."
  *Mitigation:* render an explicit "no system components configured" empty state,
  and document the Helm values so a real install always ships descriptors.
- **Informer memory/watch cost.** Two more informers per cluster add watch load and
  cache memory. *Mitigation:* control-plane Deployments/DaemonSets are few (tens,
  not thousands) and, unlike Pods, low-churn; the marginal cost is small relative to
  the existing Node/Pod informers.
- **Descriptor drift from reality.** A descriptor's selector can go stale when a
  chart renames a component. *Mitigation:* a descriptor that resolves to zero
  objects on an in-scope cluster reports **missing**, which is visible and
  actionable rather than silent — the same signal as a genuinely absent component.
  We accept that this conflates "renamed" with "not installed"; the fix for both is
  the same (correct the descriptor or install the component).

## Design Details

### Data source: watch Deployments and DaemonSets

MUSE already watches all Pods per cluster (for capacity), so a pod-only health view
is tempting — but it cannot see two of the three motivating cases: a component
scaled to zero (no pods, but `spec.replicas` says it should exist) and a component
never installed (no pods, no Deployment). Both require the *controller* object, so
we watch it.

Add to `ClusterState` (`muse/crates/muse-server/src/informers.rs`) two listers,
wired through the same `warm::<K>(...)` helper the `Node`/`Pod` informers use, each
marking `Invalidations` on change (kinds `"Deployment"` and `"DaemonSet"`, so the
frontend's `useLive` can refresh):

```rust
pub deployments: Lister<Deployment>,   // apps/v1
pub daemon_sets:  Lister<DaemonSet>,   // apps/v1
```

These are core apps/v1 types (from `k8s-openapi`, like `Node`/`Pod`), so they need
no CRD-presence gate — but see the namespace-scoping note below. StatefulSet is
deliberately omitted from v1: no current OME system component is a StatefulSet, and
a descriptor kind can be added later without an API change if that changes.

**Optional, best-effort, and isolated (required, not optional-in-practice).** These
two informers are started only for the kinds some *applicable* descriptor uses on
that cluster — a cluster with no applicable descriptor watches neither, so an empty
`systemComponents` adds no watches and no RBAC requirement at all. Critically, their
readiness is **not** part of the cluster's cache-readiness gate: a `warm_system`
helper (distinct from `warm`) starts them without registering a readiness future.
The failure mode this guards against is concrete — an existing remote-cluster
credential may lack `get/list/watch` on apps/v1, and if that 403 marked the whole
cluster cache `failed`, it would take **Fleet, Capacity, and every workload view
offline**, not just System. With the informers isolated, such a failure degrades
*only* System (its lister stays empty), and the shared caches are untouched.

> **Namespace scoping (TBD, implementation detail).** The Node/Pod informers are
> cluster-wide. The control plane lives in one or a few namespaces, so we would
> prefer to scope the Deployment/DaemonSet informers to the configured namespaces
> to bound RBAC and watch cost. Whether the current `SharedInformerFactory` in
> `ome-client-rs` supports per-namespace informers cleanly is `TBD`; if not, v1
> watches cluster-wide (read-only) and we file a follow-up to narrow it. Either way
> the *projection* only ever exposes objects matched by a descriptor.

### Config: component descriptors, no in-code default

A **component descriptor** is the config-driven identity of one system component.
It is supplied as JSON, mirroring how `--dashboard-policy-json` threads a structured
policy through `Config → AppState → handler` (`config.rs`, `app_context.rs`):

```jsonc
// MUSE_SYSTEM_COMPONENTS_JSON — an array of descriptors. No default; absent ⇒ empty.
[
  {
    "name": "Controller manager",       // display name
    "group": "control-plane",           // optional grouping for the panel
    "kind": "Deployment",               // Deployment | DaemonSet
    "namespace": "ome",                 // where it lives
    "selector": { "control-plane": "ome-controller-manager" }, // matchLabels
    "clusters": ["hub"]                 // optional; omit ⇒ all aggregated clusters
  },
  {
    "name": "Model agent",
    "group": "data-plane",
    "kind": "DaemonSet",
    "namespace": "ome",
    "selector": { "app.kubernetes.io/name": "model-agent" }
    // no "clusters" ⇒ expected on every aggregated cluster
  }
]
```

Threaded exactly like the existing structured config:

- **Flag/env** in `config.rs`: `--system-components-json` /
  `MUSE_SYSTEM_COMPONENTS_JSON`, parsed with `serde_json` and **validated** at
  startup (non-empty name; kind ∈ {Deployment, DaemonSet}; non-empty namespace;
  non-empty selector) — a bad descriptor fails fast at boot, not at request time.
- **Helm** `charts/muse/values.yaml` → `config.systemComponents`, rendered into the
  env var in `templates/deployment.yaml`. The chart ships the real OME component set
  as *values* (GitOps), not the binary.
- **No in-code default.** Absent config ⇒ zero descriptors ⇒ the endpoint returns an
  empty listing and the panel renders "no system components configured." This
  satisfies the hard blocker: the default lives in the chart, and absent config
  degrades gracefully rather than injecting a magic namespace or label.

The `clusters` field carries the *expected* placement, which is what makes
**missing** meaningful: a descriptor scoped to `["hub"]` that resolves to nothing on
the hub is `missing` (a real problem); the same descriptor is simply not evaluated
on member clusters (not a problem).

### The endpoint: `GET /v1/system`

Add `service/system.rs` and register it in `service/mod.rs` **before** the
`/v1/{kind}` catch-all (matchit prefers static routes), matching `clusters.rs`:

```
GET /v1/system[?cluster=<name>]   →  Listing of SystemComponentRow
```

For each descriptor, for each in-scope cluster returned by `st.select(cluster)`
(which already filters to cache-ready clusters and enforces fleet-readiness), the
handler resolves the descriptor's objects from the `deployments`/`daemon_sets`
lister by namespace + selector, computes health, and emits one row per
(descriptor × cluster), tagged with `cluster` via `protocol::tagged`. Wire types go
in `protocol.rs` with the usual camelCase `serde` renames and `skip_serializing_if`:

```rust
struct SystemComponentRow {
    name: String,
    group: Option<String>,
    kind: String,                 // "Deployment" | "DaemonSet"
    namespace: String,
    cluster: String,              // added by tagged()
    status: String,               // healthy | degraded | unavailable | missing | unknown
    desired: i32,
    ready: i32,
    updated: Option<i32>,         // updatedReplicas / updatedNumberScheduled
    message: Option<String>,      // e.g. the Available/Progressing condition reason
}
```

### Health model

Health is derived, deterministic, and depends only on the object's status — never
on iteration order (the same discipline `pick_pod_runtime_name` follows in
`resources.rs`). The lattice, worst-first, with the rule and its rationale:

- **`missing`** — the descriptor is in scope for this cluster but resolves to zero
  objects. *Rationale:* the strongest actionable signal — the component operators
  expect is not there.
- **`unavailable`** — `desired > 0` and `ready == 0`. *Rationale:* the component
  exists but nothing is serving; a crash-loop or a failed rollout.
- **`degraded`** — `0 < ready < desired`, or a Deployment reporting
  `Available=False` / stuck `Progressing`. *Rationale:* partially up — often a
  rollout in flight or one bad replica.
- **`healthy`** — `ready == desired` and `desired > 0` (Deployment: `Available=True`;
  DaemonSet: `numberReady == desiredNumberScheduled`). *Rationale:* at desired
  scale.
- **`unknown`** — `desired == 0` with no clear intent, or a status MUSE cannot yet
  interpret. *Rationale:* honesty over a fabricated verdict. Note: an *intentional*
  `replicas: 0` is a judgment call — v1 reports it `unknown` with a message rather
  than guessing `healthy`; revisit if operators find that noisy.

**Worst-wins aggregation.** When a descriptor matches more than one object on a
cluster, or the fleet roll-up combines clusters, the reported status is the worst
in the lattice (`missing`/`unavailable` dominate `healthy`). *Rationale:* an
operator scanning a roll-up must never see `healthy` while one instance is down.

DaemonSet counts map to the same lattice via `status.desiredNumberScheduled`
(desired), `status.numberReady` (ready), and `status.updatedNumberScheduled`
(updated). A DaemonSet with `desiredNumberScheduled == 0` (no nodes match its
nodeSelector) is `unknown`, not `unavailable` — zero desired is not a failure.

### Frontend: the System panel

Follow the established panel pattern (the OEP-0021 exploration and `Fleet.tsx`):

- `web/src/api.ts` — a `SystemComponentRow` interface (mirroring the wire type) and
  `api.system = () => getJSON<Listing<SystemComponentRow>>("/v1/system")`.
- `web/src/panels/System.tsx` — grouped by `group`, each component a `Card` with a
  status `Dot`, `desired/ready` counts, cluster chips, and the `message`; a
  fleet-level roll-up header ("N of M healthy"). Reuse `viz`/`components` primitives
  (`StatTile`, `Dot`, `DataNotice`) for consistency and the loading/error states.
- Live updates via `useLive(api.system, { kinds: ["Deployment","DaemonSet"],
  intervalMs: 30_000 })` — the same SSE-invalidation hook every panel uses.
- Nav + routing: add `"system"` to `PanelKey` and `LABELS`, a `navItem(...)` with a
  new icon in `icons.tsx`, the `{panel === "system" && <System/>}` branch in
  `App.tsx`, and the slug mappings in `route.ts` (`PANEL_TO_SLUG` /
  `SLUG_TO_PANEL`) so the view is deep-linkable.

The panel is presented as an admin/operator view (framing and placement in the
nav), but — see Non-Goals — it is not access-gated in v1.

### RBAC

Expand the chart's read-only ClusterRole (`charts/muse/templates/rbac.yaml` /
`values.yaml`) with `get/list/watch` on `apps` `deployments` and `daemonsets`.
No writes, no secrets. This is the one privilege increase the feature requires, and
it is called out here explicitly per the review discipline.

## Test Plan

- **Rust unit tests** (`service/system.rs`, alongside the `resources.rs` test
  style): the health projection against table-driven fixtures — a healthy
  Deployment, a `0/2` unavailable one, a `1/2` degraded one, a scaled-to-zero
  `unknown` one, a DaemonSet with partial `numberReady`, a DaemonSet with
  `desiredNumberScheduled: 0`, and a descriptor that resolves to nothing
  (`missing`). Assert worst-wins on multi-object and multi-cluster aggregation, and
  determinism regardless of object order.
- **Config tests** (`config.rs`): `--system-components-json` parses a valid array,
  and each validation rule rejects its bad input (empty name, unknown kind, empty
  namespace/selector) at boot.
- **Frontend tests** (Vitest): status-derivation/roll-up helper unit tests
  (mirroring `clusterStatus.test.ts`), and a panel render test covering the empty
  ("no components configured"), error, and populated states.
- **Helm**: `make helm-lint` + a template render asserting descriptors reach the env
  var and the ClusterRole gains the apps/v1 rules.
- No new integration suite: the endpoint is a pure projection over informer caches,
  covered by the unit tests above; end-to-end wiring rides the existing MUSE image
  build (`make -C muse ci`).

## Graduation Criteria

- **Alpha:** endpoint + panel behind the standard MUSE image; descriptors supplied
  via Helm values in at least one real install; the three motivating cases
  (crash-loop, scaled-to-zero, missing-on-a-cluster) each render the expected
  status.
- **Beta:** namespace-scoped informers (the TBD above) resolved; operator feedback
  on the `replicas: 0 ⇒ unknown` call folded in; docs in `muse/README.md`'s
  Endpoints table.
- **Stable:** the panel is the operators' first stop for control-plane health in at
  least one production fleet, and the descriptor set ships as chart defaults in the
  GitOps repos that deploy OME.

## Drawbacks

- **More RBAC and more informers.** Two apps/v1 kinds and two watches per cluster —
  small, but non-zero, and the RBAC widening is real.
- **Config burden.** Every install must supply descriptors or the panel is empty;
  there is no zero-config "it just works," by design (the hard blocker forbids the
  baked-in default that would provide it).
- **A partial health picture.** "Deployment is at desired scale" is not "the
  controller is reconciling correctly" — a wedged-but-Ready controller reads
  `healthy` here. This view raises the floor (catches the down/missing/scaled-to-zero
  cases) without claiming to be a full liveness check; metrics remain the deeper
  signal.

## Alternatives

1. **Derive health from the existing Pod cache only.** *Tradeoff:* no new informer
   or RBAC, but structurally blind to a scaled-to-zero or never-installed component
   (no pods to observe) — which are two of the three cases we are building this for.
   Rejected: it cannot see the desired count, which lives on the controller object.
2. **Per-request apiserver gets** for the configured components. *Tradeoff:* no
   informer memory, always-fresh. Rejected: it breaks MUSE's defining cache-only
   invariant (OEP-0021), reintroduces request-time apiserver load and failure
   modes, and buys freshness the informer already provides within its resync window.
3. **Auto-discover components by a single OME label** (e.g.
   `app.kubernetes.io/part-of=ome`). *Tradeoff:* less config. Rejected for v1: the
   label is itself install-specific (still config, just a different field), display
   names and grouping become fuzzy, and — critically — auto-discovery cannot report
   **missing**, because it only ever sees what exists. The descriptor's *expected*
   scope is the whole point. A label-based descriptor kind can be added later as an
   additive option.
4. **Surface control-plane health in the existing Fleet panel** instead of a new
   panel. *Tradeoff:* one fewer nav entry. Rejected: Fleet is a workload-owner view;
   folding operator-only control-plane health into it muddies both audiences. A
   separate, clearly-admin panel keeps the concern separate.
5. **A Go/controller-side health CRD** that MUSE reads like any other kind.
   *Tradeoff:* richer, controller-authored status. Rejected as over-scoped for this
   need: it is new API surface and a new reconciler to maintain, when the
   information wanted is already on standard Deployment/DaemonSet status.

## Open Questions

- **Access control.** The read plane has no authn/authz today, so this "admin" view
  is not enforced as admin-only. Should it be gated (gateway-level identity, à la
  OEP-0021.1's ingress-injected `jwt_header`), and if so is that a MUSE concern or the
  gateway's? *Current lean:* defer to the gateway; do not invent an identity layer
  in the read plane for this.
- **Namespace-scoped informers.** Does the `ome-client-rs`
  `SharedInformerFactory` support per-namespace Deployment/DaemonSet informers
  cleanly, or must v1 watch cluster-wide? (Bounds RBAC and watch cost — TBD above.)
- **`replicas: 0` semantics.** Report intentional scale-to-zero as `unknown` (v1
  lean) or add an explicit `scaled-to-zero` status so operators can tell "off on
  purpose" from "I can't tell"? Decide from alpha feedback.
- **Cross-namespace / multi-instance descriptors.** Is one selector per descriptor
  enough, or do real installs need a descriptor to span namespaces or match by more
  than `matchLabels`? Start with single namespace + `matchLabels`; revisit if a real
  component needs more.

Next action: assign a reviewer/approver and confirm the alpha descriptor set with
the team that owns the OME Helm charts, since the descriptors — not the code — are
what make this view correct for a given install.
