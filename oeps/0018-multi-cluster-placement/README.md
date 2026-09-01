# OEP-0018: OME Multi-Cluster Support (placement by fan-out + race)

<!-- toc -->
- [Summary](#summary)
- [Motivation](#motivation)
  - [Goals](#goals)
  - [Non-Goals](#non-goals)
- [Proposal](#proposal)
  - [Terminology](#terminology)
  - [The model: the ISVC is the unit; the control plane fans it out](#the-model-the-isvc-is-the-unit-the-control-plane-fans-it-out)
  - [Why fan-out + race (and why Kueue is mandatory)](#why-fan-out--race-and-why-kueue-is-mandatory)
  - [What is new — and what is deliberately not](#what-is-new--and-what-is-deliberately-not)
  - [Feasibility: gating OMENative pods](#feasibility-gating-omenative-pods)
  - [User stories](#user-stories)
- [Design Details](#design-details)
  - [Objects](#objects)
  - [Components on the control-plane cluster](#components-on-the-control-plane-cluster)
  - [Fan-out and winner selection](#fan-out-and-winner-selection)
  - [Cross-cluster transport](#cross-cluster-transport)
  - [Endpoint publication](#endpoint-publication)
  - [End-to-end flow](#end-to-end-flow)
  - [Failure modes and re-placement](#failure-modes-and-re-placement)
  - [Dependencies](#dependencies)
- [Test Plan](#test-plan)
- [Graduation Criteria](#graduation-criteria)
- [Drawbacks](#drawbacks)
- [Alternatives](#alternatives)
- [Open Questions](#open-questions)
<!-- /toc -->

## Summary

**Tl;dr:** OME runs one InferenceService (ISVC) well on one cluster, but it has **no way to place a service across clusters** and **no concept of GPU quota**. This OEP adds multi-cluster placement by **fan-out + race**: you submit a normal ISVC to a **control-plane cluster**; OME **clones it onto every candidate workload cluster**; each cluster's Kueue gates the cloned pods until that cluster has GPU room; the **first cluster whose Kueue admits wins**; OME deletes the clones on the losers and publishes a global endpoint at the winner. There is no central capacity calculation — distributed Kueue admission *is* the placement decision.

OME owns the control plane (the fan-out controller, the cross-cluster transport, the endpoint). **Kueue is mandatory** on every workload cluster, but only as the per-cluster GPU-quota ledger and pod-admission gate — it never appears in the user-facing API, and OME does **not** adopt MultiKueue (the ISVC is never turned into a Kueue job). This is "approach A′" from the design discussion that produced this OEP; the rejected "adopt MultiKueue" path is approach A in [Alternatives](#alternatives).

## Motivation

Placing inference across clusters is manual today. An operator decides, by hand or by editing a gitops repo, which cluster an ISVC lands on. Two failure modes follow:

1. **No global placement → chronic mis-utilization.** A fleet spans clusters across regions and providers with uneven GPU availability. With no placer, one cluster queues work it cannot run while another sits idle.
2. **No quota concept, even single-cluster.** OME has no `ClusterQueue`-equivalent: nothing caps a team's GPUs, nothing preempts. Two teams oversubscribe a cluster and the loser's pods sit `Pending` with no policy behind it.

The platform owner's constraint is explicit: **OME should own the multi-cluster control plane** — not hand the steering wheel to an external orchestrator — but OME should **not** reinvent GPU quota, which Kueue already does well. Fan-out + race threads that needle: OME owns *where to try* (clone to all candidates) and *what to keep* (first admitted wins); Kueue owns *whether a cluster has room* (admission). This OEP draws that line.

### Goals

- **Placement by fan-out + race.** Clone the ISVC to all capability-matched candidate clusters; the first whose Kueue admits is the home; the rest are deleted. **Single-active**: a service runs on exactly one cluster.
- **OME owns the control plane.** The fan-out controller, cross-cluster object lifecycle, status mirroring, and endpoint publication are OME's, driven directly against workload clusters via stored credentials — no gitops and no MultiKueue dispatch in the loop.
- **Quota via Kueue, mandatory but invisible.** Each workload cluster runs Kueue as the GPU-quota ledger and per-pod admission gate. OME relies on its admission outcome and never reads or computes capacity itself; Kueue vocabulary does **not** appear in `InferenceService` / `WorkloadCluster`.
- **No ISVC surgery.** The cloned ISVC is a normal, unmodified `InferenceService` (no `spec.managedBy`, no Kueue job integration, no adapter). OME only stamps the Kueue `queue-name` label + serving annotation onto the *pods* it renders, so each cluster's Kueue gates them.
- **Minimal new API.** One new OME CRD (`WorkloadCluster`, the cluster registry) plus an additive `status.placement` block on ISVC.

### Non-Goals

- **Adopting MultiKueue / turning the ISVC into a Kueue job.** That is approach A ([Alternatives](#alternatives)); it cedes orchestration to MultiKueue and requires deep ISVC surgery. Rejected.
- **Global *pooled* quota / cross-cluster borrowing / global fair-share.** v1 quota is **per cluster** (each Kueue `ClusterQueue` independently). Pooling GPUs across clusters into one borrowable budget is out — see [Alternatives](#alternatives) for the MultiKueue Cohort path if this becomes a hard requirement.
- **Multi-cluster *load-balanced serving*.** v1 is single-active: a service runs on one cluster, not spread across clusters behind one balanced endpoint. The global endpoint resolves to a single home.
- **DR / active-standby failover** as a first-class feature. Re-placement on failure exists (below); warm-standby and RTO/RPO guarantees are not in scope.
- **Model staging / weight replication.** Weights are assumed pre-staged on all candidate clusters (the existing BaseModel/model-agent + `cluster_cache` path). A cluster without the model is not a valid candidate; v1 does not pull weights at placement time.
- **Batch/Job semantics.** This is for long-running serving (ISVCs), not finishing jobs.

## Proposal

### Terminology

- **Control-plane cluster:** the cluster where multi-cluster ISVCs are submitted and where OME's fan-out controller runs. It has no GPU nodes and runs no serving pods; an ISVC here is an *intent + status* object, not a running workload.
- **Workload cluster:** a GPU cluster where ISVCs actually run. Runs OME (unchanged from single-cluster) and **Kueue**.
- **Candidate clusters:** the workload clusters whose capability labels satisfy an ISVC's requirements (e.g. `gpu=gb300`) and that are reachable. The fan-out targets exactly this set.
- **Derived ISVC:** the clone of the submitted ISVC that the fan-out controller creates on a candidate cluster. A normal, unmodified ISVC — the worker's OME runs it as if a human applied it. It carries an origin annotation for tracking + GC, and its pods carry the Kueue `queue-name` label so that cluster's Kueue gates them.
- **Admission gate:** Kueue's pod scheduling gate (`kueue.x-k8s.io/admission`). A gated pod is **queued, not stuck** — it consumes no GPU until that cluster's Kueue admits it.
- **Winner / race:** the candidate cluster whose Kueue admits the derived ISVC's pods first. Distributed admission across candidates *is* the placement decision; there is no central scoring.

### The model: the ISVC is the unit; the control plane fans it out

The central decision — the one that keeps the API small and avoids ISVC surgery — is that **a "multi-cluster ISVC" is not a new kind, and is never turned into a Kueue job.** It is the same `InferenceService`. The only difference is *where you submit it* and *what the control plane does with it*:

- Submit an ISVC to a **workload cluster** → it runs there, single-cluster, exactly as today. Unchanged.
- Submit the same ISVC to the **control-plane cluster** → OME's control plane runs in **fan-out mode**: it does not run the ISVC locally (no GPUs), it clones the ISVC onto every candidate cluster, lets the clusters' Kueues race to admit, keeps the winner, and deletes the rest.

The submitted ISVC is the control object: its `status.placement` reports the chosen cluster, mirrored readiness, and the global endpoint. The derived ISVC on the winner is the runtime. The two roles are disambiguated **structurally by cluster role** (control-plane = fan-out, workload = run), not by an overloaded field — which is what keeps it clean. This is the same local-object/remote-object split MultiKueue uses for `Workload` (manager) ↔ `Job` (worker); we reuse the *shape*, not MultiKueue itself.

### Why fan-out + race (and why Kueue is mandatory)

The load-bearing rule:

> **Fan-out + race works only because every candidate cluster has its own admission authority (Kueue) that independently decides whether it has room.** The race *is* distributed admission — no central brain reads or computes capacity.

Take Kueue away and fan-out breaks: with no per-cluster gate, cloning an ISVC onto all candidates would **start its pods on all of them**, not one. So Kueue is **mandatory** on every workload cluster. In exchange, OME builds no capacity reader, no scoring, and no quota engine — the placement decision is delegated to the clusters' Kueues, and over-subscription is structurally impossible (a cluster without room simply never ungates).

This is the deliberate inverse of "read capacity centrally, then pick one." Reading capacity centrally is eventually-consistent and needs re-placement on races; the race model is self-correcting (admission is the truth) at the cost of transiently creating a gated clone on every candidate (see [Drawbacks](#drawbacks)).

### What is new — and what is deliberately not

New:
- **`WorkloadCluster`** (CRD, cluster-scoped) — the registry: one per workload cluster. A first-class operational entity (`kubectl get workloadcluster` shows reachability) and the anchor for connection + health.
- **`status.placement`** on ISVC (additive status only — observed state, not spec surgery).
- **A control-plane fan-out controller + cross-cluster connection manager + endpoint publisher** (controllers, not API).

Deliberately *not*:
- **No ISVC spec surgery.** No `spec.managedBy`, no Kueue GenericJob integration (`Suspend`/`PodSets`/`Finished`), no `MultiKueueAdapter`. Those are required only to adopt MultiKueue (approach A); we don't. The derived ISVC is a plain ISVC whose *pods* are Kueue-gated by a label — nothing more.
- **No Kueue vocabulary in the user-facing API.** `WorkloadCluster` carries no `ClusterQueue` field; the `queue-name` the pods carry is resolved by convention/operator-config, not written by users.
- **No `ClusterPlacementPolicy` in v1.** Candidates are derived by capability matching (the ISVC's accelerator requirements vs `WorkloadCluster` labels). A selector-based policy for operator-imposed routing (partition pools, per-class rules) is a v2 extension, not needed for capacity pooling.

### Feasibility: gating OMENative pods

Fan-out hinges on one property: **OME's own pods must tolerate being held by Kueue's scheduling gate without OME's lifecycle misreading the gated pod as a failure.** OME creates serving pods directly via the OMENative workload engine (InferenceReplica → pods), not via a Deployment, so this was verified against the code, not assumed:

- **The gate rides through untouched.** The workload renderer builds the pod from a verbatim `PodSpec.DeepCopy()` (`workload/ops/render.go`), and `schedulingGates` is a first-class field on OME's PodSpec — a gate on the InferenceReplica runner template is preserved.
- **Labels/annotations propagate.** The renderer overlays only its own mandatory keys and copies the rest, so the Kueue `queue-name` label + serving annotation reach the pods with no collision.
- **A gated pod is not a failure.** A `SchedulingGated` pod sits in `Phase=Creating`, requeued normally. The stuck-pod escalator fires only on terminal kubelet *container* states (ImagePullBackOff/CrashLoop); a gated pod has no container statuses, so it is never escalated.
- **Clean teardown.** A gated pod is never in an EndpointSlice, so drain is instantly satisfied; no finalizer blocks deleting a never-admitted loser clone.

**One required dependency:** the `InstanceReadyTimeout` backstop must measure from *admission*, not from operation start — otherwise a pod Kueue legitimately holds gated (waiting for quota) would be failed at the timeout (default 30m). This is fixed by pausing the deadline clock while a pod is admission-gated; see [Dependencies](#dependencies). This is not multi-cluster-specific — it is a prerequisite for OME ↔ Kueue at all, single-cluster included.

**The one genuine risk** is multi-node: a Size>1 Instance (leader+worker, PD-disaggregated) already runs OME's scheduler-plugins `PodGroup` (gang); Kueue's pod-group is a *second* gang overlay. They can coexist (parallel annotations, stable identity), but multi-node gang admission is the corner that needs real verification — single-pod serving is trivially fine. See [Open Questions](#open-questions).

### User stories

1. *As a platform operator*, I register three GPU clusters with the control plane (three `WorkloadCluster` objects + their kubeconfig Secrets), each running Kueue. A team applies a normal ISVC to the control-plane cluster; OME clones it to the clusters with `gb300`, whichever cluster's Kueue admits first runs it, and I see `status.placement.cluster` + a global endpoint — without anyone choosing the cluster.
2. *As a service owner*, when my home cluster loses capacity (preempted, or the cluster goes unreachable), OME re-runs the fan-out and re-homes my service, repointing the endpoint — without me editing anything.
3. *As a single-cluster user*, nothing changes: I apply my ISVC to a workload cluster directly and it runs there, no placement involved.

## Design Details

### Objects

**Cluster registry — `WorkloadCluster` (cluster-scoped CRD).** Modeled on Kueue's `MultiKueueCluster`: a pluggable connection source, conditions-only status (no capacity, no quota vocabulary), capability labels for matching.

```yaml
apiVersion: ome.io/v1beta1
kind: WorkloadCluster              # cluster-scoped, shortName: wlc
metadata:
  name: cluster-a
  labels:                          # capability matching: an ISVC's requirements match THESE
    provider: aws
    region: us-east-2
    gpu: gb300
spec:
  clusterSource:                   # pluggable connection (learned from MultiKueueCluster.ClusterSource); exactly one of:
    kubeConfig:
      secretRef: {name: cluster-a-kubeconfig, namespace: ome-system}
    # -- or --
    # clusterProfileRef: {name: cluster-a}   # reference a SIG-Multicluster ClusterProfile (cluster-inventory-api)
status:
  conditions:                      # connection HEALTH only -- never capacity, never Kueue
    - {type: Ready, status: "True", reason: Reachable}
# +kubebuilder:printcolumn Reachable <- conditions[Ready].status ; Age
```

The referenced `Secret` is just the kubeconfig (rotated/RBAC'd independently); the `WorkloadCluster` is the registry entry operators reason about. There is intentionally no capacity field: the race delegates "does this cluster have room?" to that cluster's Kueue at admission time.

**Candidate selection (no CRD).** An ISVC's candidate set = reachable `WorkloadCluster`s whose labels satisfy the ISVC's accelerator requirements (OME already knows the accelerator from the model/runtime). This is plain matching, not a policy object. (`ClusterPlacementPolicy` for operator-imposed routing is a v2 extension — see [Open Questions](#open-questions).)

**ISVC — unchanged spec; additive status only:**

```yaml
status:
  placement:
    cluster: cluster-a
    endpoint: https://my-svc.global.example
    phase: FanningOut | Admitted | Running | RePlacing
    conditions: [...]              # mirrored readiness from the derived ISVC on the winner
```

### Components on the control-plane cluster

1. **Connection manager.** Watches `WorkloadCluster` objects, resolves each one's `clusterSource` (a kubeConfig Secret or a `ClusterProfile`), builds and caches one client + label-scoped watch per cluster, reconnects with backoff, and writes each `WorkloadCluster.status` reachability condition. *This is the one genuinely hard subsystem.* It is modeled on Kueue's MultiKueue remote-client (`pkg/controller/admissionchecks/multikueue/{multikueuecluster,remote_client}.go`) — the proven pattern (selectively-caching client, bounded watch-establishment backoff, per-cluster event funnel) — **without** taking the MultiKueue dependency.
2. **Fan-out controller.** The brain: reconciles ISVCs on the control-plane cluster, computes candidates, clones the derived ISVC onto all of them, watches for the first admission, deletes the losers, and runs re-placement. The algorithm below.
3. **Status mirror.** Watches the derived ISVC on the winning cluster and writes `status.placement`.
4. **Endpoint publisher.** Programs the global DNS/gateway to the winner's ingress; updates on re-placement.

### Fan-out and winner selection

```
1. Candidates = reachable WorkloadClusters whose labels satisfy the ISVC's
   accelerator requirements (e.g. gpu=gb300).
2. Create the DERIVED ISVC on EVERY candidate (origin annotation; pods stamped
   with the Kueue queue-name label + serving annotation). Each cluster's Kueue
   gates the pods until that cluster has room.
3. Watch all candidates. The FIRST cluster whose Kueue admits the pods
   (gate removed -> pods scheduling/running) is the winner.
4. Record the winner (status.placement.cluster, immutable once Running) and
   DELETE the derived ISVC on every other candidate (their pods were gated,
   never ran).
5. Mirror the winner's readiness to status.placement; publish the endpoint.
```

There is no scoring step and no capacity read: step 3's "first to admit" is the decision, made by the candidates' Kueues. If no candidate has room, all clones stay gated (queued) until one frees up — Kueue's queueing, not an OME timeout, governs the wait (see [Dependencies](#dependencies)).

### Cross-cluster transport

The control plane holds one kubeconfig Secret per workload cluster and talks to each apiserver directly. Cloning/patching/deleting the derived ISVC and watching its admission state are ordinary client calls. Derived ISVCs carry an origin annotation (`ome.io/placement-origin` + `ome.io/placement-origin-uid`); a periodic GC sweep (modeled on MultiKueue's label-scoped GC) deletes any whose control-plane ISVC is gone, and the loser-deletion in step 4 is the common-case cleanup.

**Authentication model.** The default kubeconfig shape is **static credentials**: a bearer token (ServiceAccount), client certificate, or inline CA. client-go reads these directly from the kubeconfig Secret; no external process runs. `exec` credential plugins and the deprecated `auth-provider` mechanism are **rejected by default** (following Kueue's `validateKubeconfig`), because an exec block runs a command in the controller pod — a stored kubeconfig becomes arbitrary-code-exec if the Secret is compromised.

**Opt-in exec credentials.** For workload clusters that require **short-lived cloud tokens** (EKS, GKE, AKS with pod-identity federation), the control-plane manager supports `--allow-exec-credentials` + `--exec-credential-allowed-commands=<comma-list>`. When enabled, client-go runs the allowlisted plugin (e.g., `aws eks get-token`, `gke-gcloud-auth-plugin`) and auto-refreshes tokens on expiry, removing the static-token rotation burden. Requirements: (1) **BYO manager image** — the default OME image ships no cloud CLIs; operators must bake the plugin binary into a derived image (see `config/samples/multicluster/Dockerfile.exec-example`); (2) **pod identity** granted to the manager pod (EKS: IRSA annotation on the manager ServiceAccount; GKE: Workload Identity binding) so the plugin can mint tokens. The allowlist bounds which binary runs; it does not sandbox the binary's behavior.

**Threat model.** An exec block in a kubeconfig Secret is **arbitrary code** (bounded only by the allowlist + the container's UID/capabilities). WorkloadCluster kubeconfig Secrets MUST be admin-only RBAC (no self-service creation). The `auth-provider` mechanism remains unsupported (deprecated upstream since client-go v1.26; use `exec` for all plugin-based auth).

**Security (failure mode).** Beyond the exec-credential attack surface, a stored kubeconfig with broad RBAC is a cross-cluster blast radius (a compromised Secret lets an attacker write to every workload cluster). Mitigation, not full fix: scope the credential to the minimum verbs on ISVCs (read/write InferenceService + read Pods/PodGroups for admission watches, nothing else); RBAC scoping is the operator's responsibility, OME validates the shape.

### Endpoint publication

OME owns the global endpoint (decided in design: not delegated to an external LB OME merely informs). On a winner, the publisher programs a global DNS record or multi-cluster Gateway route to the winner's ingress; on re-placement it repoints it. The concrete backend (which global-traffic system, and its API) is **TBD** and is the largest integration unknown — see [Open Questions](#open-questions).

### End-to-end flow

```
1. User applies ISVC to the CONTROL-PLANE cluster.
2. Fan-out controller computes candidates (gb300 + reachable) -> {cluster-a, cluster-b}.
3. Creates the DERIVED ISVC on BOTH; pods labeled queue-name=<lq>, serving=true.
4. Each cluster's Kueue gates the pods. cluster-b has free GB300 -> its Kueue
   admits first -> pods run there. cluster-a stays gated.
5. Controller records winner=cluster-b, DELETES the derived ISVC on cluster-a.
6. Status mirror writes ISVC.status.placement (cluster=cluster-b, Running).
7. Endpoint publisher points the global endpoint at cluster-b.
```

### Failure modes and re-placement

| What breaks | What happens | Mitigation (not always a full fix) |
|---|---|---|
| No candidate has room | All clones stay gated (queued by each Kueue) | They wait — Kueue queueing governs, NOT an OME timeout (deadline paused while gated; see Dependencies). When any cluster frees up, it admits and wins |
| Two candidates admit near-simultaneously | Brief window with pods running on two clusters | Controller picks the first observed as winner, deletes the other; endpoint only ever points at the winner. Minor transient |
| Winner cluster becomes unreachable | Connection manager flips it unhealthy | Re-run fan-out on healthy candidates; repoint endpoint. **Open:** grace window before re-homing a possibly-still-serving cluster |
| Higher-priority service needs the GPUs | Kueue preempts the derived ISVC's pods | Treated as "lost admission" -> re-fan-out. Disruptive (single-active has no second replica) |
| Autoscale-up exceeds remaining quota on the home cluster | New replicas' pods gated `Pending` | v1: size the `ClusterQueue` with headroom or admit at max replicas; graceful elastic scaling is Kueue's *alpha* `ElasticJobsViaWorkloadSlices`, out of scope |
| Two control-plane controllers (HA) double-fan-out | Conflicting derived ISVCs | Leader election on the control plane; derived-ISVC name is deterministic so a duplicate create conflicts rather than creating a second instance |

### Dependencies

- **`InstanceReadyTimeout` must measure from admission, not operation start.** The deadline backstop (the consumer of `InstanceReadyTimeout`) fails any transient Instance whose `Operation.Deadline` elapsed. Because the deadline is stamped at op start, a pod Kueue holds gated burns its timeout while merely queued — every gated OME workload would be failed at ~30m. Fix: pause the deadline clock while an Instance's pods are admission-gated, restart it from admission once they clear the gate (`workload.ReconcileGatedDeadlines` + `PodAdmissionGated`, wired into the IR aggregator before the expiry backstop). This is a prerequisite for OME ↔ Kueue generally (single-cluster too), landed alongside the `InstanceReadyTimeout` backstop wiring.
- **Kueue installed on every workload cluster**, with a `ResourceFlavor`/`ClusterQueue` describing its GPUs and a `LocalQueue` per serving namespace. The cluster operator owns this; OME assumes it.

## Test Plan

- **Unit:** candidate matching (ISVC requirements vs `WorkloadCluster` labels); derived-ISVC rendering incl. the Kueue label/annotation stamp; winner-selection (first-admitted) + loser-deletion; origin-GC predicate; `PodAdmissionGated` + the gated-deadline pause (done).
- **Integration (envtest, control-plane side):** fan-out creates a clone per candidate against fake remote clients; one admits -> losers deleted; none admit -> all stay alive (no timeout failure); remote-unreachable -> re-fan-out.
- **Multi-cluster e2e (KIND, >=2 clusters), authoritative:** real Kueue on each worker; submit ISVC to the control plane -> clones land on both candidates gated -> free quota on one -> it wins, the other clone is deleted, endpoint points at the winner; preempt the winner -> re-homes. The race, preemption, and gated-wait paths only reproduce against real Kueue. **Multi-node:** a Size>1 (leader+worker) ISVC fan-out, to verify OME's PodGroup and Kueue's pod-group cooperate.

## Graduation Criteria

- **Alpha:** single-active fan-out across >=2 clusters with real Kueue; gated pods wait without timeout failure; loser deletion + origin GC; endpoint publication for one concrete global-traffic backend; behind a `--enable-multicluster` flag.
- **Beta:** re-placement hardened (grace windows, preemption handling); HA control plane (leader election); per-cluster health surfaced; documented RBAC scoping for stored kubeconfigs; an `Incremental` fan-out mode to bound churn at scale.
- **Stable:** soak at fleet scale; a recorded decision on pooled quota (stay per-cluster, or adopt the MultiKueue Cohort path); multi-node gang verified.

## Drawbacks

- **Fan-out churn.** Each placement transiently creates a gated derived ISVC (and its gated pods + PodGroups) on *every* candidate, then deletes all but one. At 20 candidates that is real create/delete churn per placement. Mitigation deferred to Beta: an `Incremental` fan-out mode (clone to a few, widen only if none admit), mirroring MultiKueue's dispatcher modes.
- **OME owns the cross-cluster transport.** The connection manager, per-cluster cached watches, reconnect/backoff, and GC are the genuinely hard subsystem, and OME maintains them (vs getting them free from MultiKueue under approach A). Mitigated by modeling on MultiKueue's proven code — the pattern, not the dependency.
- **Kueue is mandatory.** Multi-cluster placement does not function without Kueue on every workload cluster; that is the price of the no-central-capacity-read race model.
- **Single-active has no in-flight resilience.** Preemption or cluster loss means a re-fan-out, i.e. a serving gap — there is no second cluster already serving. Acceptable for capacity pooling; not a serving-HA story.
- **Multi-node runs two gang abstractions.** OME's scheduler-plugins `PodGroup` and Kueue's pod-group both gate a Size>1 Instance's pods; they must cooperate. Verified-as-risk, not yet verified-as-working (Open Questions).

## Alternatives

Presented as a menu with the cost of each, then the pick.

1. **Adopt MultiKueue (approach A).** Control-plane cluster = MultiKueue manager; workload clusters = workers; `InferenceService` becomes a Kueue job (`spec.managedBy` + a `Suspend`/`PodSets`/`Finished` integration + a `MultiKueueAdapter`). *Gives:* the fan-out/race + transport + GC for free; global pooled quota (Cohort) available. *Costs:* OME is no longer the brain (MultiKueue owns orchestration; custom/serving-aware/graceful re-placement fights its dispatcher); deep surgery on the central ISVC type; coupling to a Beta feature gate. **Rejected** because the platform owner wants OME to own multi-cluster control and to leave the ISVC unsurgered. Making Kueue mandatory (which we accept) removes only A's *dependency* cost, not the surgery or the ceded-brain cost. The per-cluster Kueue this OEP requires is exactly what A needs, so A remains the graduation path if pooled quota becomes mandatory.
2. **Read capacity centrally, then pick one (the prior draft of this OEP).** OME reads each cluster's free GPUs (native node accounting, or Kueue `ClusterQueue.status`), scores, and creates the ISVC on one winner; re-places if that cluster's Kueue then declines. *Gives:* Kueue-optional (native capacity needs no Kueue); one clone, no fan-out churn. *Costs:* a central capacity reader over eventually-consistent data; scoring; a re-placement loop for races. **Rejected** in favor of fan-out because the race is self-correcting (admission is the truth, over-subscription structurally impossible) and needs no capacity-reading machinery — at the cost of transient N-clone churn.
3. **OME-native everything (approach B).** OME builds placement *and* its own quota engine (borrowing/preemption/fairness) *and* the transport. **Rejected** as too extreme — reinvents quota, which Kueue already solves.
4. **Registry as a labeled Secret (no CRD).** *Rejected:* the `WorkloadCluster` concept deserves a queryable kind; the first draft's mistake was capacity-in-status, fixed by keeping capacity out.
5. **Wrapper CRD `MultiClusterInferenceService`.** A global object embedding an ISVC template. **Rejected:** ~95% duplicates the ISVC spec; "the ISVC is the unit, the control plane fans it out" is cleaner.
6. **gitops as the transport.** The controller writes clones into a per-cluster GitOps repo. **Rejected:** couples multi-cluster control to gitops; the owner wants pure OME control.

## Open Questions

- **Global-traffic backend (largest unknown).** What concrete system does the endpoint publisher program — GSLB/DNS, a multi-cluster Gateway, a service mesh? Its API and failover semantics shape the publisher. Owner: TBD.
- **Multi-node gang interaction.** Do OME's scheduler-plugins `PodGroup` and Kueue's pod-group cooperate for a Size>1 Instance, or do two gang gates deadlock/double-count? Must be proven before Alpha for PD-disaggregated/multi-node. Owner: TBD.
- **Fan-out churn at scale.** When is `AllAtOnce` too noisy, and what is the `Incremental` widen policy (how many first, how long to wait before widening)?
- **`ClusterPlacementPolicy` (v2).** When operators need to partition pools or impose routing beyond capability matching, what does the selector-based policy look like, and how does an ISVC opt out of multi-cluster?
- **Re-placement grace window.** How long to wait before re-homing a service off a cluster that went unreachable but may still be serving — avoiding a flap that causes an unnecessary serving gap.
- **Pooled quota trigger.** What concrete need (a fairness incident?) flips us from per-cluster quota to the MultiKueue Cohort path (alternative 1)? Record the decision at Stable.

See also: OEP-0009 (model-weight locality, the assumption behind "models pre-staged"), OEP-0013 (per-cluster autoscaling, which interacts with per-cluster quota).
