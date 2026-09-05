# OEP-0019: Control-plane process split (webhook / serving / model)

<!-- toc -->
- [Summary](#summary)
- [Motivation](#motivation)
  - [Goals](#goals)
  - [Non-Goals](#non-goals)
- [Proposal](#proposal)
  - [Terminology](#terminology)
  - [The split axis: runtime contract, not domain](#the-split-axis-runtime-contract-not-domain)
  - [Where each controller and webhook lands](#where-each-controller-and-webhook-lands)
  - [Why this split and not the others](#why-this-split-and-not-the-others)
  - [What is new — and what is deliberately not](#what-is-new--and-what-is-deliberately-not)
  - [User stories](#user-stories)
- [Design Details](#design-details)
  - [Mode taxonomy and what each mode runs](#mode-taxonomy-and-what-each-mode-runs)
  - [The five wiring changes, each with its failure mode](#the-five-wiring-changes-each-with-its-failure-mode)
  - [Helm shape](#helm-shape)
  - [Migration / cutover](#migration--cutover)
- [Test Plan](#test-plan)
- [Graduation Criteria](#graduation-criteria)
- [Drawbacks](#drawbacks)
- [Alternatives](#alternatives)
- [Open Questions](#open-questions)
<!-- /toc -->

## Summary

**Tl;dr:** Today the entire OME control plane — every controller and every admission webhook — is **one binary, in one image, run as one Deployment** (`ome-controller-manager`, `replicaCount: 3`, args `--leader-elect --webhook`; `charts/ome-resources/templates/ome-controller/deployment.yaml`). That packs two workloads with opposite runtime contracts into the same process: webhooks are **synchronous, all-replicas-active, latency-critical, on the apiserver write path with `failurePolicy: Fail`**; controllers are **asynchronous, single-leader, memory-heavy** reconcilers. The coupling has one sharp consequence — a bad controller rollout or a controller OOM-loop degrades the very pods that serve admission, and because the pod mutator is `failurePolicy: Fail`, that can **block OME serving-pod creation cluster-wide**.

This OEP adds a `--mode={all,webhook,serving,model}` flag and a Helm toggle that render the control plane as **three right-sized Deployments** instead of one — with **no change to any CRD, API, or serving behavior**. `--mode=all` is the default and is exactly today's single process, so existing installs are untouched until an operator opts in.

The split axis is the **runtime contract, not the domain**: admission handlers → the **webhook** plane; anything that creates or watches Pods/workloads → the **serving** plane; anything that manages definitions/metadata (models, runtimes, revisions) → the **model** plane.

## Motivation

One process, three named failure modes:

1. **Correlated failure wedges admission.** The pod mutator is registered `failurePolicy=fail` (`pkg/webhook/admission/pod/mutator.go:17`; the rendered `MutatingWebhookConfiguration` confirms `failurePolicy: Fail` on both the ISVC defaulter and the pod-mutator, `charts/ome-resources/templates/ome-controller/webhooks/isvcmutator.yaml`). Because controllers and webhooks share one image and one process, a crashlooping reconciler — a bad ISVC reconcile, an OOM — flaps webhook readiness **on every replica at once** (the crash is correlated; it's the same image). With `failurePolicy: Fail` and the mutator's `objectSelector: ome.io/inferenceservice Exists`, every OME serving-pod CREATE in scope then fails admission. A reconciler bug becoming a serving-pod outage is the failure this OEP removes.
2. **Two workloads, one size.** A webhook wants **N active replicas** spread across zones for latency and HA; a controller wants **one leader + a warm standby**, and it carries the heavy cache. Coupled in one Deployment, every replica must be sized for the leader's worst case — 4Gi (`charts/ome-resources/values.yaml:94-100`) to hold the Pod informer it would start on failover — and you cannot scale the two apart. Worse: the BaseModel reconciler requeues every 5s during import (`pkg/controller/v1beta1/basemodel`), so model-import churn shares a process and a workqueue with the serving hot path.
3. **Coupled rollout cadence.** Any reconciler change redeploys the admission path; any webhook change redeploys the reconcilers. The two iterate at very different rates, and today neither can move without the other.

### Goals

- **A `--mode` flag** that selects which controllers and webhooks a process runs. `all` is the default and reproduces today's behavior exactly.
- **Three planes, right-sized:** a lean, all-active **webhook** plane; a single-leader **serving** plane (the *only* plane that runs the Pod informer); a single-leader **model/catalog** plane.
- **Isolate admission from reconciler failure:** a serving-plane crash or rollout must not degrade the webhook plane. This is the property the [blast-radius test](#test-plan) asserts.
- **No API/CRD/serving-behavior change.** No new user-facing surface; the serving runtime path is byte-for-byte unchanged.
- **One binary, one image.** The mode is a flag, mirroring the existing `--multicluster-role` precedent — no second build artifact, no version-skew risk.
- **A Helm `split.enabled` toggle, default `false`** (today's single Deployment), so the split is opt-in and reversible per cluster.

### Non-Goals

- **Splitting the serving controllers from each other.** ISVC, InferenceReplica, and the embedded traffic reconciler share the heavy cache (Pods, Deployments, LeaderWorkerSets, HPAs, ScaledObjects, revisions); splitting them just duplicates those informers across processes. Out of scope — see [Alternatives](#alternatives).
- **Per-webhook Deployments on day one.** v1 is one webhook plane for all handlers. Peeling the hot-path pod mutator into its own Deployment + HPA is a Beta option gated on measured QPS, not v1 — see [Open Questions](#open-questions).
- **Two binaries / two images.** Rejected for version skew; see [Alternatives](#alternatives).
- **Changing the OEP-0018 control-plane role.** The serving plane absorbs the WorkloadCluster + placement controllers; `--multicluster-role=control-plane` keeps its existing meaning (disables the local ISVC→pods reconcilers). The two flags compose; the matrix is documented in [Design Details](#mode-taxonomy-and-what-each-mode-runs).
- **Per-runnable leader election inside one process.** controller-runtime supports it, but it leaves blast radius and sizing unsolved (still one image, one process) — see [Alternatives](#alternatives).

## Proposal

### Terminology

- **Plane:** a process *role* selected by `--mode`. It is **not** a new deployment kind or a new binary — it is a flag on the same `cmd/manager` image that decides which controllers/webhooks that process wires up.
- **webhook plane** (`--mode=webhook`): runs only the admission handlers (`main.go:521-590`). No controllers, no leader election. Its cache holds CR informers only — never the Pod informer.
- **serving plane** (`--mode=serving`): runs the InferenceService (+ traffic), InferenceReplica, and multi-cluster (WorkloadCluster + placement) controllers. **The only plane that watches Pods** — the Pod informer is created solely by the InferenceReplica controller's `Watches(&corev1.Pod{})` (`pkg/controller/v1beta1/inferencereplica/manager.go:76`); the ISVC controller only `Owns` higher-level objects (Deployments, Services, HPAs, …; `pkg/controller/v1beta1/inferenceservice/controller.go:916-960`).
- **model plane** / **catalog plane** (`--mode=model`): runs the BaseModel/ClusterBaseModel reconcilers, the ServingRuntime inheritance reconciler, and the runtime-revision GC. Manages *definitions and metadata*; never watches Pods.
- **`failurePolicy: Fail`:** an admission-webhook policy meaning *if the webhook endpoint is unavailable, the apiserver rejects the matched write*. For the pod mutator, "rejects the matched write" = OME serving-pod CREATE fails. This is the property that turns a webhook outage into a serving outage.
- **Leader lease:** the `coordination.k8s.io/Lease` a leader-elected manager holds so exactly one replica reconciles (`LeaderLockName = "ome-controller-manager-leader-lock"`, `main.go:69`). Each leader-elected plane needs its **own** lease, or two planes contend one lock and one stalls.

### The split axis: runtime contract, not domain

The load-bearing rule:

> **Split by how a unit must run, not by what it is about.** Admission handlers, Pod-touching reconcilers, and definition reconcilers have three different availability/latency/cache contracts. Put each contract in its own process; do not group by subject matter.

The naive cut is by domain ("a model controller, a runtime controller, an ISVC controller"). That is the wrong axis: it would split ISVC from InferenceReplica even though they share the same heavy cache, and it would leave the latency-critical pod mutator in the same process as a memory-heavy reconciler. The right axis is the runtime contract. Concretely, the assignment rule is a single question per unit:

- **Is it an admission handler?** → **webhook** plane.
- **Does it create or watch Pods / workload objects?** → **serving** plane.
- **Does it only manage definitions/metadata (models, runtimes, revisions)?** → **model** plane.

### Where each controller and webhook lands

| Unit (from `main.go`) | Plane | Why |
|---|---|---|
| InferenceService reconciler (+ embedded traffic) | serving | Renders Deployments/LWS/HPA/ScaledObjects; the serving hot path |
| InferenceReplica reconciler | serving | The sole Pod watcher (`manager.go:76`); owns pod lifecycle |
| WorkloadCluster + placement (OEP-0018) | serving | Cross-cluster *workload* placement; already role-gated |
| BaseModel / ClusterBaseModel reconcilers | model | Model definitions + import status; 5s requeue loop; no Pod cache |
| ServingRuntime inheritance reconciler (OEP-0012) | model | Runtime *definitions*; no Pod cache |
| Runtime-revision GC (OEP-0014) | model | ControllerRevision bookkeeping; no Pod cache |
| Pod mutator `/mutate-pods` (`failurePolicy: Fail`) | webhook | Admission; the hot, sharp-blast-radius handler |
| ISVC defaulter + validator | webhook | Admission |
| ServingRuntime / ClusterServingRuntime validators + preset mutators | webhook | Admission |
| BaseModel / ClusterBaseModel validators | webhook | Admission |
| InferenceReplica validator | webhook | Admission |
| ControllerRevision immutability validator | webhook | Admission |

| Plane | Leader lease | Pod informer? | Shape |
|---|---|---|---|
| webhook | none | no | N active replicas, small memory, HA across zones |
| serving | `…-serving-leader-lock` | **yes** (leader only) | 1 leader + warm standby, sized for the Pod cache (~4Gi) |
| model | `…-model-leader-lock` | no | 1 leader + standby, modest memory |

Note: the serving plane still *reads* models and runtimes (the ISVC reconciler matches via `runtimeselector`), so those CR informers exist in more than one plane. That overlap is expected and cheap — the point of the split is to isolate the **reconcile workload and the Pod informer**, not to make caches disjoint (which is impossible: ISVC must read what the model plane reconciles).

### Why this split and not the others

- **webhook ⟂ controller is the primary cut**, because it is the one that removes the [failure mode #1](#motivation) blast radius: with the webhook plane in its own Deployment, a serving-controller crashloop cannot flap the `failurePolicy: Fail` admission endpoint. Nothing else delivers that.
- **model is the second cut**, because the model plane has a genuinely different contract: it never watches Pods, and its 5s import-requeue loop should not share a workqueue or a failure domain with the serving hot path. Pulling the ServingRuntime-inheritance and revision-GC controllers in with it keeps a clean rule — "no Pod cache here" — and keeps the heavy informers exclusively in serving.
- **We do not split the serving controllers further**, because ISVC/IR/traffic share the expensive cache; separating them duplicates the Pod/Deployment/LWS informers across processes (more apiserver watch load, more total memory) for no isolation win. This is the [Alternatives](#alternatives) "full per-controller split", rejected.

### What is new — and what is deliberately not

New:
- A `--mode` flag (default `all`) that gates controller setup, webhook registration, leader election, probes, and startup config-loading.
- Two new leader-lease names (serving, model) alongside the existing one.
- Three Helm Deployments behind a `split.enabled` toggle, with split ServiceAccounts/RBAC and a webhook Service/cert repoint.

Deliberately *not*:
- **No CRD or API change**, additive or otherwise. No new field on any type.
- **No change to the serving runtime path.** The pods OME renders, the reconcile logic, the webhooks' decisions — all identical.
- **No behavior change at `--mode=all`.** The default mode is the same single process running today; `split.enabled=false` renders the same single Deployment.

### User stories

1. *As a platform operator*, I set `split.enabled=true`. A bad serving-controller image then crashloops on rollout. Admission keeps working — the pod mutator is served by the separate webhook plane — so new serving pods still admit and only *reconciliation* pauses until I roll the serving plane back. Today that same bad image would have failed serving-pod admission cluster-wide.
2. *As a platform operator*, I right-size: 3 small webhook pods (no Pod cache), 2 serving pods (1 leader + standby at ~4Gi), 2 model pods (modest). I scale the webhook plane for admission QPS and the serving plane for object count, independently.
3. *As a small / dev-cluster user*, I leave `split.enabled=false` and run exactly one Deployment, exactly as today — the split is something I opt into, not something imposed.

## Design Details

### Mode taxonomy and what each mode runs

The precedent already exists: `--multicluster-role=control-plane` runs a controller **subset** — it disables the local ISVC→pods reconcilers (`main.go:387`) and runs the placement controller instead. `--mode` generalizes that one special case into the primary structuring knob. Each mode gates the corresponding setup calls in `main()`:

| Mode | Controllers wired | Webhooks | Leader election |
|---|---|---|---|
| `all` (default) | all of them | yes | one lease (today's `…-leader-lock`) |
| `webhook` | none | yes (`main.go:521-590`) | none |
| `serving` | ISVC (+traffic), InferenceReplica, WorkloadCluster + placement | none | serving lease |
| `model` | BaseModel/CBM, ServingRuntime inheritance, runtime-revision GC | none | model lease |

`--mode` composes with `--multicluster-role`: `--mode=serving --multicluster-role=control-plane` runs placement but **not** the local ISVC→pods reconcilers (the role still wins on that sub-decision). The full matrix is documented alongside the flag; the corner to get right is that the control-plane role is a *modifier* on the serving plane, not a fourth mode.

### The five wiring changes, each with its failure mode

1. **The flag + setup gating.** `--mode` (default `all`) wraps the existing `SetupWithManager` calls and the `enableWebhook` block. *Failure if wrong:* a mode that silently sets up the wrong controllers — mitigated by a unit test per mode asserting exactly the expected reconcilers/handlers are registered.
2. **Per-mode leader leases.** Today `LeaderLockName` is a single const (`main.go:69`). Split into serving + model lock names; the webhook plane disables leader election entirely. *Failure if unchanged:* serving and model share one lease, only one plane ever becomes leader, the other's reconcilers never run — a silent stall, not a crash.
3. **Per-mode health/readiness.** Both `healthz` and `readyz` are wired to `mgr.GetWebhookServer().StartedChecker()` (`main.go:593-604`). *Failure if unchanged:* in `serving`/`model` mode there is no webhook server, so the checker never reports started, `readyz` never passes, and the pods crashloop on readiness. Fix: a non-webhook readiness check (ping / cache-sync) in the controller modes.
4. **Per-mode startup config + translator.** `NewDeployConfig`/`NewIngressConfig`/`NewModelCacheConfig`/`NewOmeAgentConfig` and the traffic translator are loaded unconditionally and `os.Exit(1)` on error (`main.go:304-371`). *Failure if unchanged:* a model-plane pod dies at boot because, say, `IngressConfig` is absent — config it never needed. Fix: load only what the mode uses (webhook → model-cache name + runtime selector; model → model-cache + ome-agent; serving → all).
5. **Cache scoping is already correct by construction.** The Pod `ByObject` label scope (`main.go:277-279`) only matters where a Pod watch is registered — i.e. the serving plane. In `webhook`/`model` mode no controller registers `Watches(&corev1.Pod{})`, so no Pod informer starts and the scope is inert. The webhook plane reads the `inferenceservice-config` ConfigMap via the **Clientset** (uncached, request-time; `mutator.go:39`) and reads CRs via the cached Client (bounded informers for runtimes/models/FineTunedWeight). So the non-serving planes are CR-informer-dominated, not Pod-dominated — the lean-plane property holds **by construction**, not by tuning.

### Helm shape

A `split.enabled` toggle (default `false`) selects the topology:

```yaml
ome:
  split: { enabled: false }        # false -> one Deployment (today); true -> three
  webhook:         { replicaCount: 3, resources: {small} }    # --mode=webhook
  controller:      { replicaCount: 2, resources: {4Gi} }      # --mode=serving
  modelController: { replicaCount: 2, resources: {medium} }   # --mode=model
```

When `true`, the chart renders three Deployments from the same image with the corresponding `--mode` arg, and:
- **Repoints `ome-webhook-server-service`** (and the cert `ome-webhook-server-cert`, `cert-manager.io/inject-ca-from: ome/serving-cert`) at the **webhook** plane's pods via the label selector. *This is the one change that silently breaks admission if wrong* — the Service must select the plane that actually serves `:9443`.
- **Splits the ServiceAccount/RBAC**: webhook SA = read the validated CRs + ConfigMap, no writes; serving SA = the workload writer (Deployments/LWS/HPA/ScaledObjects/Services/pods/ISVC status); model SA = model/runtime status + ConfigMaps. Least-privilege as a bonus.

### Migration / cutover

The cutover from one Deployment to three must not blip admission. Ordering:

1. Bring up the **webhook** plane Deployment (it can run alongside the existing `all` Deployment — both serve `:9443`, the Service just fans out).
2. Repoint the webhook Service/cert to the webhook plane.
3. Bring up the **serving** and **model** planes.
4. Scale the `all` Deployment to zero.

`split.enabled` makes this a chart-level flip, but the **order is load-bearing**: repointing the Service before a healthy webhook plane exists would drop admission. Rollback is the reverse, or simply `split.enabled=false`.

## Test Plan

- **Unit:** one test per `--mode` asserting exactly the expected controllers/handlers are registered (and that `all` is unchanged); per-mode probe wiring; per-mode config-load gating (a model-mode boot does not require `IngressConfig`).
- **Integration (envtest):** boot `--mode=webhook` → admission works, no reconcilers run; `--mode=serving` → ISVC/IR reconcile, no webhook server; `--mode=model` → BaseModel reconciles, no Pod informer is created.
- **Helm:** `helm template` with `split.enabled=true` renders three Deployments + a repointed Service/cert + three SAs; with `split.enabled=false` renders the same single Deployment as today; `make helm-lint`.
- **KIND (authoritative — the whole point):** with the split enabled, kill the serving plane and assert the pod mutator **still admits** OME serving pods (the blast-radius property). The reverse: roll the webhook plane and assert reconciliation continues uninterrupted.

## Graduation Criteria

- **Alpha:** `--mode` lands with `all` as default (no behavior change); the chart ships `split.enabled` default `false`; the three modes pass the envtest + a KIND smoke. Opt-in only.
- **Beta:** `split.enabled=true` validated in a real cluster; the KIND blast-radius test (kill serving → admission survives) is green and gating; split RBAC/SAs documented; a recorded decision on whether the pod mutator gets its own Deployment + HPA.
- **Stable:** soak at fleet scale; flip the chart default to `split.enabled=true` once the split has run in production long enough to trust.

## Drawbacks

- **More moving parts.** Three Deployments, two leader leases, split RBAC/SAs, and the webhook Service/cert repoint, versus one Deployment today. Operational surface grows; `split.enabled=false` keeps the simple topology available for those who want it.
- **Duplicate CR watches.** The serving, webhook, and model planes each run informers for some shared CR types (runtimes, models), so the apiserver sees N watches per type instead of one. Bounded — these are small-count CRs, not Pods — but not zero.
- **Webhook cache can lag the controller's.** A split webhook plane validates against its own cache, which can be staler than the serving controller's by an informer-resync delta. Admission is point-in-time and eventually-consistent already, so this does not change correctness — but it is a real, if minor, difference from the shared-cache status quo.
- **Migration must repoint the Service/cert without a gap.** Mitigated by the [cutover ordering](#migration--cutover); the risk is real if an operator flips selectors by hand out of order.

## Alternatives

Presented as a menu with the cost of each, then the pick.

1. **Status quo — one process (`all`).** *Gives:* one Deployment, one lease, trivial ops. *Costs:* failure mode #1 (a controller bug wedges `failurePolicy: Fail` admission) and no independent sizing/scaling. **Not rejected — kept** as `--mode=all` / `split.enabled=false` for small and dev installs. It is the wrong *default* for a busy cluster, not a wrong *option*.
2. **Split webhook from controllers only (two processes); keep all controllers together.** *Gives:* the entire blast-radius win with the least change. *Costs:* the model-import 5s requeue still shares the serving process and workqueue. **This is "phase 1" of this OEP** — a legitimate place to stop if the model plane proves unnecessary. We go one step further because the model plane's "no Pod cache, different cadence" contract is clean and cheap to separate.
3. **Full per-controller split (one Deployment per controller).** *Gives:* maximal isolation. *Costs:* every process re-watches overlapping types (ISVC, ConfigMaps, runtimes), multiplying apiserver watch load and duplicating informer memory; N leases; N Deployments; harder local dev. **Rejected** — over-splitting; the benefit beyond the three-plane cut does not justify the cost.
4. **Two binaries / two images** (a `manager` and a `webhook` binary) instead of one binary + `--mode`. *Gives:* a smaller webhook image. *Costs:* two build/release artifacts and version-skew risk between them. **Rejected** — one binary + a flag mirrors the existing `--multicluster-role` precedent and removes skew.
5. **Per-runnable leader election inside one process.** controller-runtime lets individual runnables opt out of the shared lease. *Gives:* finer leader granularity. *Costs:* it is still one image and one process, so it solves neither blast radius nor independent sizing. **Rejected** — wrong layer.

## Open Questions

- **Pod mutator its own Deployment?** `/mutate-pods` is the only traffic-driven, `failurePolicy: Fail` handler; the rest are human/CD-paced CR admission. Should it get its own Deployment + HPA (and its own Service entry), peeled from the low-QPS validators? Decide at Beta from measured pod-admission QPS. Owner: TBD.
- **Webhook plane leader election.** None (the simple choice), or keep a lease for a future leader-only runnable (e.g. cert rotation)? Default to none until something needs it.
- **The `--mode` × `--multicluster-role` matrix.** `--mode=serving --multicluster-role=control-plane` must run placement but not local ISVC→pods. Confirm there is no third combination that silently mis-wires, and document the matrix in the flag help.
- **Naming.** `--mode` values (`webhook`/`serving`/`model`/`all`) and the chart value keys (`controller` vs `serving`, `modelController`) are a bikeshed — pick once and keep them consistent across the flag, the chart, and the lease names.
- **Pod-informer ownership is a standing invariant.** We verified the InferenceReplica controller is the *sole* registrant of `Watches(&corev1.Pod{})` today (`manager.go:76`), which is what makes the webhook/model planes Pod-free. If a future controller adds a Pod watch, it must land in the serving plane (or the invariant breaks) — worth a comment at the watch site and a test that fails if a non-serving mode starts a Pod informer.

See also: OEP-0018 (the `--multicluster-role` subset-execution precedent and the WorkloadCluster/placement controllers that land in the serving plane), OEP-0012 (ServingRuntime inheritance → model plane), OEP-0014 (runtime-revision GC → model plane).
