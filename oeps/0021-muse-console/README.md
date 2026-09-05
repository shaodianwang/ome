# OEP-0021: MUSE — a management plane and console for OME

<!-- toc -->

## Summary

**Tl;dr:** MUSE ("Multi-cluster Unified Serving Engine") is one place — an API service plus a
web console — to deploy a model, onboard and convert its weights, move those
weights cross-cloud, and see where in the fleet a model can actually fit. It is a
**read + write layer on the multi-cluster control plane OME already runs**
(OEP-0018), not a new plane: deploying is "create one `InferenceService` on the
control plane and let placement fan it out," and the fleet/capacity view rides the
per-cluster informer transport the placement control plane already maintains.

Define the two halves up front, because they have different reliability profiles
and the rest of the doc keeps them apart:

- **Read plane** — the aggregation + query layer that watches every workload
  cluster and serves a unified view of OME workloads, nodes, and capacity. It is
  read-only and **out of the serving data path**: if MUSE is down, models keep
  serving and CR-driven deploys still work. MUSE is an operator convenience, never
  a serving dependency.
- **Write plane** — the API that creates/edits OME CRs (`InferenceService`,
  `BaseModel`) and kicks off asynchronous jobs (weight conversion, cross-cloud
  transfer). Writes are thin; the heavy lifting stays in OME controllers and
  `ome-agent`.

The console is "MUSE Console" riding the "MUSE" backend — one name, one product.

## Motivation

OME's web console lives outside the operator tree. Meanwhile OME has added the
features that most need a UI — PD-disaggregated serving, multi-cluster
placement (OEP-0018), canary/blue-green rollout (OEP-0015/0017) — and there is no
operator surface for any of it. The failure mode today is concrete:

- **Deploys are `kubectl` + tribal knowledge across N clusters.** An engineer who
  wants to serve a model hand-writes an `InferenceService`, guesses a runtime,
  guesses a target cluster, and applies it somewhere. There is no self-service
  path and no record of "who deployed what, where."
- **Capacity is invisible.** "Can this 405B model land on a TPU-v5e slice, and is
  there a free one?" has no answer short of `kubectl get nodes` against each
  cluster and reading provider-specific labels by hand. RDMA islands (on-prem) and
  TPU slices (GCP) are topology that vanilla Kubernetes does not surface.
- **Model onboarding is a pile of scripts.** Converting a checkpoint from PyTorch
  to safetensors, generating a valid `config.json`, and moving weights from
  on-prem storage to GCP so a model can be hosted on TPU are all manual,
  one-off, and unobservable.

None of this is a new control loop — it is the *visibility and self-service* layer
missing on top of control loops we already run. That framing is the whole point:
MUSE should be mostly assembly, with one genuinely new hard problem (the capacity
model, see Design Details).

### Goals

1. **One pane across clouds.** A single view of OME workloads + node/accelerator
   capacity across on-prem, GCP, OCI, and CoreWeave clusters — read-only, near-real-time.
2. **Self-service deploy.** Create an `InferenceService` from the console with
   runtime auto-selection and a placement choice; OME's existing placement
   controller does the fan-out. Record deploy provenance (who/when/what).
3. **Model onboarding pipeline.** Convert PyTorch→safetensors, generate
   `config.json`, and transfer weights on-prem→GCP, each as an observable async job
   with status surfaced in the console.
4. **Capacity inventory with topology.** Surface RDMA islands and TPU slices, not
   just raw GPU counts, so an operator (or, later, an automated check) can answer
   "where does this fit."

### Non-Goals

- **Not a serving-path component.** The read plane must never sit between a client
  and a model. If correctness or availability of serving depends on MUSE, that is
  a bug.
- **Not a scheduler.** Placement stays with OME's placement controller + Kueue +
  the Kubernetes scheduler. MUSE *shows* capacity and *requests* a deploy; it does
  not bin-pack. (Naming honesty: this is why it is not called a scheduler.)
- **Not a GitOps replacement.** Whether deploy intent is written to clusters
  directly or via Git is an open fork (Design Details); MUSE supports at least the
  direct path in v1 and does not mandate ripping out GitOps.
- **Not, in v1, a cost/chargeback tool or a fine-grained multi-tenant RBAC
  product.** Auth model is `TBD` (Open Questions); v1 assumes operator-level access.

## Proposal

### The shape, in one breath

MUSE runs in a **management-plane cluster** — in practice the same cluster that
already runs OME's `--multicluster-role=control-plane` (OEP-0018), or a peer
process beside it (OEP-0019 set the precedent that a named subset of the control
plane can be its own process). It reuses three things that already exist there:

1. the **WorkloadCluster registry** (the CRD + the per-cluster connection/kubeconfig
   transport),
2. the **per-cluster cached clients** and the **status watch-funnel** (OEP-0018.2),
   which already deliver cross-cluster object events, and
3. the **placement controller**, which already turns one source `InferenceService`
   into derived copies fanned out to matching clusters.

Given those, MUSE's read plane is "add Node/capacity to what the funnel already
watches and expose it as an API," and MUSE's write plane is "create the source CR
and let the existing loops run." That is the reuse story, and it is why this is an
OEP about a *layer*, not a new system.

### User stories

- *As an ML engineer*, I pick `llama-3.1-405b-instruct` from the registry, toggle
  "auto-select runtime," choose "let OME place it," and click Deploy; MUSE creates
  one `InferenceService` and I watch placement land it on a cluster with free
  H100s.
- *As an SRE*, I open Capacity, see that `gcp-tpu-us-east5` has two free `v5e-256`
  slices and `cw-h100-1` is at 95% H100 allocatable, and I deploy accordingly.
- *As a model owner*, I onboard a PyTorch checkpoint; MUSE converts it to
  safetensors, generates `config.json`, then transfers it on-prem→GCP, and I watch
  each stage with an ETA instead of tailing a script.

### Risks (named so they are reviewable)

- **R1 — Read-plane scale/footprint.** Watching Nodes (and Pods, if we are not
  careful) across many clusters can blow up cache memory. We have already been
  burned: the manager once OOMed by caching all Pods cluster-wide on a very large
  cluster. Mitigation in Design Details (scope the cache; do not cache Pods).
- **R2 — The capacity model is heterogeneous and not in the API.** RDMA islands
  and TPU slices come from provider-specific labels/CRDs, not standard Node fields.
  This is the one genuinely new, hard piece; getting the normalization wrong makes
  Capacity lie. (Open fork #1.)
- **R3 — Deploy provenance + blast radius.** A console "Deploy" button that writes
  to production clusters needs an audit trail and a safe-by-default scope, or it
  becomes an unaudited way to break serving. (Open fork #2: direct vs GitOps.)
- **R4 — Cross-cloud weight egress is a governance question, not a code one.**
  Moving model weights out of the on-prem network is gated by data-egress policy;
  the plumbing is the easy part. (Open fork #3.)

## Design Details

> Order within: read plane → write plane → where it runs → the three open forks.
> "What" (the contract) is kept separate from "how" (mechanism), and proposals are
> marked as proposals.

### Read plane: inventory + capacity

**What must be true.** The read plane serves a *normalized, near-real-time* view of
(a) OME workloads (`InferenceService` and their Components/Instances/placement
status) across all connected clusters, (b) node/accelerator capacity, and (c) the
onboarding/transfer job status. "Near-real-time" here means seconds, driven by the
watch-funnel's events, with a periodic safety re-list as backstop — i.e. the same
event-primary/poll-backstop contract OEP-0018.2 already established. It is
read-only and may be stale; it must never be authoritative for serving.

**How (mechanism, proposed).** Extend the existing per-cluster informer set the
funnel maintains to also watch `Node` (and the OME CRs MUSE renders), and project
each cluster's objects into an in-memory normalized store the API reads. Two
hazards to call out:

- **Do not cache Pods cluster-wide.** R1. Per-`InferenceService` pod detail, when a
  user expands a workload, is fetched on demand (a scoped live list by the
  `ome.io/inferenceservice` label), not held in cache. The manager's existing
  Pod-cache label-scoping is the precedent.
- **Strip and scope.** Cache only `Node` fields the capacity model needs
  (allocatable, the topology labels) with managedFields stripped, mirroring the
  manager's `DefaultTransform`.

### Write plane: deploy, convert, transfer

**Deploy.** The contract: a console deploy creates exactly one source
`InferenceService` on the control plane, stamped with provenance (actor, time,
request id), and returns; everything after is the placement controller's job. MUSE
does **not** write to workload clusters directly — that would duplicate and race
the placement loop. Runtime auto-selection reuses `pkg/runtimeselector`; the
console simply omits `spec.runtime` to opt in.

**Convert + transfer are asynchronous jobs, never request-path.** Both are minutes-to-hours
operations on multi-gigabyte artifacts; blocking an API call on them is a
non-starter.

- *Conversion* (PyTorch→safetensors + `config.json`) extends BaseModel onboarding.
  It is adjacent to the existing `ome-agent model-metadata` path (which already
  parses model config) and to the `BaseModel` importing/transit status states; the
  proposal is a new `ome-agent` subcommand run as a Job, with status surfaced via
  the BaseModel status the console already reads. *Open:* whether conversion is a
  first-class BaseModel lifecycle phase or a separate CR (Open Questions).
- *Transfer* (on-prem→GCP for TPU hosting) is, mechanically, what `ome-agent replica`
  (cross-region replication) already does. The new work is the source/sink
  (on-prem storage → GCS) and the governance gate (fork #3), not the copy loop.

### Where MUSE runs

Proposed: MUSE is a separate process in the control-plane cluster, **not** a new
controller folded into the OME manager. Rationale: MUSE is a request-serving API +
aggregation cache with a different scaling and deploy cadence than the
reconcilers, and OEP-0019 already established that the control plane is a set of
cooperating processes rather than one binary. It shares the WorkloadCluster
transport (same kubeconfig Secrets, same RBAC posture as the placement control
plane) so it does not invent a second cross-cluster credential path.

### Open fork #1 — where capacity *topology* comes from

This is the hard one. RDMA islands and TPU slices are not Node API fields; they
are conventions per provider. Options:

  a) **Node labels only.** Read provider-set labels (e.g. a topology/island label
     on on-prem nodes, the GKE TPU topology labels on GCP) and normalize.
     *Tradeoff:* zero new infrastructure, but entirely dependent on each
     provisioner labeling consistently — a missing/renamed label silently makes a
     pool invisible, and there is no place to attach island-level health.
  b) **A capacity CRD MUSE (or a per-cluster agent) populates.** Normalize into a
     first-class `CapacitySlice`/`Island` object per cluster. *Tradeoff:* a clean,
     queryable, health-bearing model — but it is new API surface and a new
     producer to keep correct, i.e. the thing OEP-0018-style work already shows is
     non-trivial.
  c) **Provider APIs** (GKE/TPU API, an on-prem fabric inventory service) queried directly.
     *Tradeoff:* the most accurate topology, but couples MUSE to N provider SDKs
     and their auth, and breaks the "everything comes through the WorkloadCluster
     transport" symmetry.

I lean **(a) for v1, with (b) as the graduation target**: start by normalizing
labels (cheapest, ships the view), and promote to a CRD once we know which
topology facts the deploy/capacity decisions actually need. This is a lean, not a
decision — flagged for reviewers.

### Open fork #2 — deploy: direct-write vs GitOps

  a) **Direct-write.** Console → create `InferenceService` on the control plane →
     placement fans out. *Tradeoff:* the fastest UX and the whole point of
     self-service, but the audit trail is whatever MUSE records, not Git history.
  b) **GitOps / PR-flow.** Console writes deploy intent to a Git repo; an existing
     GitOps reconciler applies it. *Tradeoff:* review, rollback, and audit come for
     free from Git, at the cost of latency and a "Deploy" button that opens a PR
     instead of deploying.

I recommend **direct-write for v1 with provenance as a first-class field**, and
GitOps as an opt-in *mode* later — because direct self-service is goal #2, and the
control plane is already the system of record for what is deployed. A reader will
object: "direct-write to prod with a button is how you get an unaudited outage."
Fair — which is why provenance + a per-environment guardrail (prod requires an
explicit confirm/scope) are in scope for v1, not deferred.

### Open fork #3 — on-prem→GCP weight egress

The transfer's blocker is policy, not code: model weights leaving the on-prem network
is a data-governance decision. v1 must treat the egress path as gated — an
allowlist of (model, destination) pairs and an approval record — rather than an
open copy. The mechanism is `TBD` and owned jointly with whoever owns the data-egress
policy.

## Test Plan

- **Read-plane correctness/scale.** Reuse the multi-cluster envtest harness
  (`tests/integration/multicluster/`, OEP-0018.2): assert the aggregated view
  matches the union of per-cluster state, and that the cache does not hold Pods.
  Add a capacity-normalization unit suite over synthetic node-label fixtures per
  provider.
- **Write-plane.** Unit-test the deploy mapping (console request → the exact
  `InferenceService` produced, including provenance stamping and runtime opt-out).
  Integration: a console deploy produces one source CR and placement fans it out
  (extends the existing placement specs).
- **Jobs.** Conversion and transfer get job-level tests with status surfaced
  through BaseModel status (the path the console reads).
- `TODO:` end-to-end console test strategy (likely a thin API contract test, not a
  browser test) — `TBD`.

## Graduation Criteria

- **Alpha:** read plane (workloads + capacity via fork-#1(a) labels) behind an
  internal flag; direct-write deploy with provenance; conversion + transfer jobs
  observable. Single environment.
- **Beta:** capacity model promoted toward fork-#1(b) where labels proved
  insufficient; prod guardrails on deploy; egress governance gate (fork #3) wired,
  not stubbed; multi-environment.
- **Stable:** auth/RBAC model resolved (Open Questions); SLOs for read-plane
  freshness; documented runbook. `TBD` thresholds.

## Drawbacks

- **A new always-on service with fleet-wide read credentials.** MUSE holds (via the
  shared WorkloadCluster transport) the ability to read every cluster; that is a
  blast radius worth stating even though it reuses the placement control plane's
  existing posture.
- **It is another thing to run.** The honest counter is that the alternative —
  no UI, `kubectl` across N clouds — is the status quo we are trying to retire.
- **Risk of scope creep into "platform."** Deploy + capacity + onboarding +
  transfer is already broad; the Non-Goals exist to keep MUSE a layer, not a
  second OME.

## Alternatives

1. **Revive the earlier OME web console.** Rejected: it predates PD, multi-cluster,
   and rollout, and has no concept of cross-cloud capacity or weight onboarding —
   the exact things we need. We would rewrite it anyway.
2. **No backend; a thin UI that talks straight to each cluster's apiserver.**
   *Tradeoff:* less to run, but it pushes N-cluster fan-out, credential handling,
   and capacity normalization into the browser, and re-implements the transport
   OEP-0018 already built. Rejected.
3. **Fold MUSE into the OME manager as another controller.** Rejected: a
   request-serving API + aggregation cache has a different scaling/deploy profile
   than reconcilers, and OEP-0019 already argued for splitting the control plane by
   role rather than one binary.

## Open Questions

- **Auth/identity model.** Who can deploy, to which environments/namespaces, and
  how does MUSE authenticate to clusters vs. authorize users? `TBD` — likely the
  v1 gate before any prod exposure. Owner: `TBD`.
- **Fork #1 resolution.** Which topology facts do deploy/capacity decisions
  actually consume? That answer decides labels-vs-CRD. Owner: `TBD`.
- **Conversion modeling.** Is conversion a BaseModel lifecycle phase or its own CR?
  Leaning lifecycle phase (reuses status + the console's existing read), but
  `TBD`.
- **Console/UI stack + design.** A first interactive mockup of the five panels
  (Fleet, Workloads, Models, Deploy, Capacity) lives at
  [`mockup.html`](./mockup.html) in this directory; the production UI stack is
  `TBD`.
- **Does MUSE share the control-plane process or run as its own?** OEP-0019
  precedent says its own; final call deferred until the read-plane footprint
  (R1) is measured.

See also: OEP-0018 (the control plane MUSE reuses), OEP-0018.2 (the funnel/cache
transport), OEP-0019 (control-plane process split), OEP-0009 (model distribution
context).
