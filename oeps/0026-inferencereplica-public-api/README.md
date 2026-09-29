# OEP-0026: InferenceReplica as a public API, InferenceService as composition

<!-- toc -->
- [Summary](#summary)
- [Motivation](#motivation)
  - [Goals](#goals)
  - [Non-Goals](#non-goals)
- [Terminology](#terminology)
- [Proposal](#proposal)
- [API](#api)
- [Design Details](#design-details)
- [Risks and Mitigations](#risks-and-mitigations)
- [Test Plan](#test-plan)
- [Graduation Criteria](#graduation-criteria)
- [Alternatives](#alternatives)
- [Migration](#migration)
- [Follow-ups](#follow-ups)
- [Open Questions](#open-questions)
<!-- /toc -->

## Summary

**Tl;dr:** The `InferenceReplica` (IR) is already a complete workload object. It
has its own controller (`pkg/controller/v1beta1/inferencereplica/`), its own
status, a scale subresource, ControllerRevisions, update strategies, pause, and
the frozen state machine of OEP-0020.1 behind every cell. Three things keep it
internal: only the `InferenceService` (ISVC) creates one; `spec.parentRef` and
`spec.component` assume a parent; and the ISVC controller writes its spec while
an admission webhook rejects everyone else. This OEP removes those three things.
The IR becomes user-creatable. The ISVC becomes the *composition* layer over pod
sets: traffic and router wiring, per-revision Services, prefill-to-decode
pairing, group rollouts and canary across sets, multi-cluster placement, status
aggregation. Every ISVC role gets two forms, never one: **inline** (today's
behavior, byte-for-byte) and **referenced** (bring your own IR). Phase 1 lands
the standalone form: a user creates an IR directly, and no ISVC names it; the
referenced form is Phase 3.

OEP-0007 said "OMENative introduces no new user-visible CRDs." This OEP revises
that one sentence. Nothing else in OEP-0007 changes.

## Motivation

Today's ISVC binds one pod set per role: one `spec.engine`, one `spec.decoder`,
one `spec.router`, each projected to exactly one IR named `<isvc>-<component>`
(`irprojector.InferenceReplicaName`). Three real deployments do not fit:

1. **Same model on two accelerator pools.** A Llama engine on H100 nodes and a
   second engine on A100 nodes, one endpoint. Today: two ISVCs and an external
   load balancer, with two rollouts nobody coordinates.
2. **Two runtimes side by side, long-lived.** vLLM and SGLang on the same model
   for a months-long comparison, not a canary that ends. Today's canary
   (OEP-0017) is a *progression* that finishes; it is not a steady state.
3. **One set per zone.** The same engine spec in three zones, each with its own
   replica count and its own rollout, one service in front. Today the ISVC has
   one `replicas` per role.

Each case is "several pod sets in one role." The engine can run them: an IR is a
pod set. The ISVC cannot *name* them.

A second motivation is platform teams. The IR controller and the workload engine
(OEP-0020.3) are the valuable part: gang creation, in-place update, drain against
EndpointSlices, migration, retry blocks, escalation. A team that wants that
lifecycle without OME's model/runtime catalog cannot get it today. The only door
is the ISVC, which requires a model or a runtime.

*Aside — why a public boundary matters beyond Kubernetes.* The provider
assessment of the workload engine (2026-09-21, the Slurm direction) found that
zero production files under `workload/` import the OME CRD package and that 22
of 33 Kubernetes touch points already sit behind one function or package. The
engine is close to being a library. Its only public input, though, is an ISVC. A
public IR gives that library a public spec: replicas, runners, lifecycle, pacing.
A future provider then has one object to target, not the ISVC's model catalog.

### Goals

- Make `InferenceReplica` a supported, documented, user-creatable API.
- Keep every existing ISVC unchanged: same IR name, same spec bytes, same pods.
- Let an ISVC role bind one *or more* IRs it does not own.
- Move the rendering pipeline (runtime piece selection, model config,
  accelerator match, template render) behind a boundary with two callers.
- Define which IR fields the ISVC may write, per ownership form, and enforce it.

### Non-Goals

- **No behavior inference.** The engine never substitutes a strategy the user
  did not choose. The owner's rule from OEP-0017: a shape the engine cannot
  execute is rejected at admission, not mis-executed silently. Concretely: a
  referenced IR without a rollout group rolls independently; the ISVC does not
  invent a group for it. (Same rule as `dispatchIRAutoscaler` in
  `components/base.go`: a policy failure "never substitutes a default HPA".)
- **No change to existing InferenceServices.** The inline form *is* today's
  projector path. The KIND suites that drive it (`irmanaged_kind`,
  `omenative_coordination_kind`, `omenative_perm_kind`) are the compatibility
  lock.
- **No new rollout engine.** `pkg/controller/v1beta1/workload`, including its
  replay driver, is unchanged.
- No new traffic data plane. Traffic across refs reuses per-revision Services
  and `TrafficMap` (OEP-0025).
- No change to `RawDeployment`, `MultiNode`, `PDDisaggregated` modes.

## Terminology

Each term is defined once; the consequence is part of the definition.

- **Pod set:** one IR — N Instances of the same rendered pod template(s), one
  revision history, one rollout. Two pod sets never share a ControllerRevision.
- **Role:** the slot an IR fills in an ISVC: `engine`, `decoder`, or `router`.
  Today `spec.component` names the role and is immutable.
- **Piece:** one of the three configs a ServingRuntime carries —
  `engineConfig`, `decoderConfig`, `routerConfig`
  (`servingruntime_types.go:219-226`). A runtime may carry any subset. An IR
  renders exactly one piece; which one is chosen by its role, never guessed.
- **Inline form:** the ISVC role carries a spec (`engine: {…}`); the ISVC
  creates and owns the IR. This is today's only form.
- **Projected replica:** the IR an inline role creates. It carries an ISVC
  controller owner reference and `spec.parentRef`. Either one marks it
  projected, so it stays projected after its ISVC is deleted with the orphan
  policy, which removes the owner reference.
- **Standalone replica:** an IR with neither an ISVC controller owner reference
  nor `spec.parentRef`. A user creates it directly. Its own name stands in for
  the parent name in its pod, Service and revision names and in the
  `ome.io/inferenceservice` pod label.
- **Name prefix:** the parent ISVC name for a projected IR, the IR's own name
  for a standalone IR; pod, Service and revision names start with it.
- **Referenced form:** the ISVC role names existing IRs
  (`engine: {replicaRefs: […]}`); the user owns them. A referenced IR is a
  standalone replica an ISVC names.
- **Owner:** the actor allowed to write an IR's user fields. Projected: the
  ISVC controller. Standalone and referenced: the user, meaning any principal
  RBAC lets write the IR except the ISVC controller; admission does not track
  which user created it. An IR has one owner: the ISVC controller or the user.
- **Rollout-control field:** an IR spec field a *composer* writes to steer a
  rollout across sets: partition, pacing, rollback target, paused/pauseMode.
- **Controller-only field:** an IR spec field only the ISVC controller writes,
  on any IR: `parentRef`, `placementExecution`, `placementReplicaLimit`. A spec
  field that is neither rollout-control nor controller-only is a **user
  field**.
- **Composer:** the ISVC controller when it orchestrates a rollout across the
  IRs of one ISVC. A composer is not an owner.

## Proposal

### 1. The IR becomes public by fixing three things

| Constraint | Where | Phase 1 change |
|---|---|---|
| Only the ISVC creates one | `irprojector.EnsureInferenceReplica` | Users may `kubectl apply` an IR. |
| `parentRef` and `component` assume a parent | `inferencereplica_types.go`; the IR controller reads the parent via `resolveParent` and derives label, Service and revision names from `parentRef.name` | `parentRef` becomes optional. Naming falls back to the IR's own name. |
| The webhook admits a spec write that carries the annotation `ome.io/controller-write=true`, which anyone with write RBAC can add | `pkg/webhook/admission/inferencereplica` | Ownership follows the form (section 3): the webhook recognizes the ISVC controller by the identity configured in `inferenceservice-config`, and RBAC governs a standalone IR (Design Details). |

The IR controller tolerates a missing parent: every parent read in `convert.go`
is behind a `parent != nil` guard, and `resolveParent` returns nil for a
standalone IR, so the IR's events land on the IR itself.

### 2. The InferenceService becomes composition

The ISVC keeps everything that connects pod sets and gives up nothing else:

- traffic and the router/gateway wiring (HTTPRoute, `spec.traffic`);
- per-revision Services (`coordination.EnsurePerRevisionServices`);
- prefill-to-decode pairing (`pairingProtocol`, `ServingPeers`, peer env);
- group rollouts and canary across sets (`spec.rollout.groups[]`);
- multi-cluster placement (derived ISVCs, OEP-0018) and `TrafficMap`;
- status aggregation (`status.components[c]`).

Two forms per role, never one:

```yaml
kind: InferenceService
spec:
  engine:  {replicaRefs: [llama-prefill-h100, llama-prefill-a100]}
  decoder: {replicaRefs: [llama-decode-h100]}
  router:  {replicaRef: llama-router}
  rollout: …   # groups name replica refs
  traffic: …   # weights across the refs in a role
```

The inline form is unchanged: `engine: {model…, runner…, lifecycle…}` still
creates `<isvc>-engine`.

### 3. Ownership follows the form

The webhook reads an IR's form from the object itself: a projected replica
carries an ISVC controller owner reference or `spec.parentRef`, and a
standalone replica carries neither.

| Form | Owner | ISVC may write | Enforcement |
|---|---|---|---|
| Projected (inline role) | ISVC controller | the whole spec (the projector) | admission by controller identity; anyone RBAC admits may still write metadata |
| Standalone | the user: any principal RBAC lets write it except the ISVC controller; admission does not track which user created it | nothing: the projector refuses an IR it does not project, and no ISVC names a standalone IR before Phase 3 | RBAC, plus the controller-only fields and the standalone rejections (Design Details) |
| Referenced (Phase 3) | the user | at most the rollout-control fields, only while a rollout group names the IR | admission by controller identity; which writes it admits depends on Open Question 1 |

Whether the composer may write even the rollout-control fields on a referenced
IR is **Open Question 1**. The alternative is that cross-set rollouts are driven
only by policy objects the user binds (`RolloutPolicy`, OEP-0017.1), and the ISVC
never writes a referenced IR at all.

### 4. Spec split

Same fields, documented as three groups. Phase 1 enforces the controller-only
group; the user/rollout-control split is enforced with the referenced form
(Phase 3).

| Group | Fields |
|---|---|
| User | `component` (set at create, immutable), `runners` (or `modelRef`/`runtimeRef`, section 5), `replicas`, `minReadySeconds`, `topologyKey`, `topologySpread`, `topologySpreadKey`, `lifecycle` (restart, update strategy, ready policy, migration), `revisionHistoryLimit`, `autoscaler` |
| Rollout-control | `pacing.partition`, `pacing.maxUnavailable`, `pacing.rollbackToRevision`, `paused`, `pauseMode`, `pairingProtocol` |
| Controller-only | `parentRef`, `placementExecution`, `placementReplicaLimit` |

The composer steers a rollout through `pacing` and never writes inside
`lifecycle`, which is the user's update strategy:

- The canary partition travels through `spec.pacing.partition`. The projector
  writes it, capped at `replicas`, and keeps the rest of the `pacing` block.
  The engine reads it ahead of `lifecycle.updateStrategy.rollingUpdate.partition`
  (`escalation.EffectivePartition`): an explicit `0` releases every Instance
  even when the user's strategy carries a partition, and an absent value
  defers to the user's partition.
- `pacing.maxUnavailable` is reserved and has no effect. Disruption during a
  rollout is paced by `lifecycle.updateStrategy.rollingUpdate.maxUnavailable`.
- `pacing.rollbackToRevision`: nothing checks the name at admission. The IR
  controller acts on it only when it names a ControllerRevision the IR
  controls; a foreign revision raises the Warning event
  `RollbackRevisionForeign` and is ignored, and a missing one falls back to
  the spec. The canary executor pins only revisions the IR controls.

### 5. Runtime integration moves to the IR, optionally

Two new optional fields: `spec.modelRef` and `spec.runtimeRef`. With a ref set,
`spec.component` selects **which piece** of the runtime the IR renders. A
runtime has three pieces; the IR's role picks one. The controller never infers
the piece from what the runtime happens to contain. Accelerator placement
comes from the runtime's pod spec (node selector, affinity, resources); a
second accelerator pool is a second runtime.

Validation is per piece:

- a `decoder` IR against a runtime with no `decoderConfig` is rejected;
- model format/architecture compatibility is checked per IR, with today's rule:
  a *named* runtime downgrades a declared-format mismatch to an advisory event
  (`inferenceservice/controller.go:459`); an *auto-selected* runtime must match.

`modelRef` without `runtimeRef` runs the existing selector
(`runtimeselector.Selector.SelectRuntime`) for that one piece. A bare-template IR
(no refs, `runners` set) is a plain pod set and is what Phase 1 delivers.

The rendering pipeline — runtime selection, model config, accelerator match
(`acceleratorclassselector`), the merge functions in
`inferenceservice/utils/merging.go` and the pod-spec assembly in `components/`
— is one package, `pkg/render`, with two callers: the IR controller, when refs
are set, and the ISVC inline form. The inline form keeps creating its IRs with
`runners`; its output is byte-identical across the extraction, and a golden of
the rendered runners plus the revision hash, captured before it, locks that.
Refs are the
standalone IR's feature. A refs IR carries no role overrides: it renders the
runtime piece named by `spec.component` and the model. Anything beyond that
uses `runners`. Open Question 2 is answered yes; the alternative, and why it
is worse, is in Alternatives.

### 6. What an IR cannot know alone: its peers

Router upstreams and prefill/decode pairing are wiring *between* sets. They stay
in the ISVC. The ISVC hands each IR the discovery inputs, as it does today:

- per-revision Service names via `EnsurePerRevisionServices`, called from the
  coordination reconciler and the canary dispatcher;
- peer endpoints via the render hook `core.ISVCRenderHook`, which injects
  `OME_<PEER>_ENDPOINT` from `coordination.ServingPeers(isvc)`;
- the pairing token via `spec.pairingProtocol`, folded into the revision hash.

A standalone engine IR serves alone. A standalone router IR is valid but idle:
it has no upstreams until an ISVC composes it.

### 7. Phases

Each phase leaves every existing ISVC untouched.

| Phase | Delivers | Locked by |
|---|---|---|
| 1 | Spec split documented as three groups, with the controller-only group enforced (the user/rollout-control split is enforced with the referenced form, Phase 3); owner rule enforced; `parentRef` optional; a bare-pod IR (`runners` only) is usable standalone | existing KIND suites through the inline path; workload goldens; webhook unit tests; **new** envtest suites `webhook/inferencereplica` (ownership rules per form) and `controller/inferencereplica_standalone` (naming, events, idle router, teardown); **new** KIND spec `standalone_replica`; the `ome-resources` render test (identity block, webhook configuration) |
| 2 | `modelRef`/`runtimeRef` on the IR; rendering library `pkg/render` extracted; the inline form keeps `runners`; a standalone IR renders from refs | a byte golden of the projector's rendered runners and revision hash, captured before the extraction; existing merge, component, projector, hash and placement unit tests; KIND suites through the inline path; **new** envtest specs (webhook: `runners` vs refs exclusivity, piece-missing rejection; controller: refs render the runtime image and model path, auto-selection, `RuntimePieceMissing`, re-render on runtime change); **new** KIND spec `standalone_replica_refs`; one unit test asserting the inline path and the refs path produce identical runners and hash |
| 3 | Referenced form on the ISVC, one ref per role | new KIND specs: ISVC over a pre-created IR, router over a referenced engine |
| 4 | Multiple refs per role: traffic across refs (`TrafficMap` as actuator), pairing across sets, groups naming refs | new KIND specs per case in Motivation; TrafficMap envtest |

## API

### InferenceReplica additions and changes

| Field | Type | Group | Validation | Note |
|---|---|---|---|---|
| `spec.parentRef` | `ParentReference` | controller-only | optional; set only by the controller, together with the ISVC controller owner reference; immutable | a set `parentRef` marks a projected replica; absent, with no ISVC controller owner reference, means standalone |
| `spec.component` | `engine\|decoder\|router` | user | required; immutable | selects the piece when refs are set |
| `spec.modelRef` | `ModelRef` (same type as ISVC `spec.model`) | user | optional; requires `runners` absent | `BaseModel` or `ClusterBaseModel`; `fineTunedWeights` and `overlays` render as on the ISVC |
| `spec.runtimeRef` | `ServingRuntimeRef` (same type as ISVC `spec.runtime`) | user | optional; runtime must carry the piece named by `component` | without `modelRef`, the runtime's piece is rendered as-is; the runtime's pod spec places the pods |
| `spec.runners` | `[]Runner` | user | required when neither ref is set; forbidden when a ref is set; a refs IR carries no `runners` and no role overrides | unchanged shape |
| `spec.pacing.partition`, `.maxUnavailable`, `.rollbackToRevision` | unchanged | rollout-control | unchanged: `partition <= replicas` | `partition` takes precedence over the user's rolling-update partition; `maxUnavailable` is reserved (section 4) |
| `spec.paused`, `spec.pauseMode` | unchanged | rollout-control | unchanged: `pauseMode` is `Recover` or `Freeze` | |
| `spec.pairingProtocol` | `*string` | rollout-control | a label value, at most 63 characters; accepted on every component | only engine and decoder replicas pair on it; a change on any replica mints a new revision; on a projected replica the composer copies `spec.rollout.pairingProtocol` onto engine and decoder, never onto the router |

Rules:
1. Exactly one of {`runners`, `modelRef`, `runtimeRef`-alone} is the template
   source. `modelRef` with `runtimeRef` names both; `modelRef` alone
   auto-selects the runtime as the ISVC does; `runtimeRef` alone renders the
   runtime's piece as-is. Mixing `runners` with a ref is rejected. Rationale:
   two sources with one winner is inference.
2. `runtimeRef` names a runtime without the piece for `component` ⇒ rejected at
   admission on create or when the reference changes, when the runtime is
   readable; otherwise a `Ready=False` condition with reason
   `RuntimePieceMissing`. A `runtimeRef` naming a runtime whose inherit-from
   parent does not exist is denied at admission with a message naming the
   parent. A disabled runtime or model is reported by the IR
   controller (`Ready=False` with reason `RuntimeDisabled` or `ModelDisabled`),
   not denied at admission: disabling is an operational flip the controller
   re-evaluates through its watches, and a write to the IR that keeps its
   references (a scale, a pause) must not wait on it. A `runtimeRef` that pins
   a runtime revision (`autoSync: false`) is rejected at admission: the IR
   controller renders the live runtime and creates no runtime snapshots. A
   model that cannot be found, or is not yet ready, yields `Ready=False` with
   reason `ModelNotFound`; the IR renders again when the model changes, through
   its watch, without a polling interval. A refs IR's own object annotations
   are inherited pod metadata excluded from the revision hash, as an ISVC's
   are: an annotation edit on the IR rolls no pods, and the same edit on an
   ISVC and on the IR that describes its workload yields one revision hash.
3. On a projected IR, a spec write by anyone but the ISVC controller is
   rejected. On any IR, a write to a controller-only field by anyone but the
   ISVC controller is rejected, and `parentRef` is immutable for every writer.
   On a referenced IR, a write to a user field by anyone but the owner is
   rejected (Phase 3). On any IR, a write to a rollout-control field by anyone
   but the owner or the composer is rejected (Phase 3). Phase 1 enforces the
   projected-IR and controller-only checks. Mechanism in Design Details.
4. A standalone IR's name is the prefix of its pod and Service names, so it
   must match the ISVC name pattern and must not collide with an ISVC. The IR
   webhook rejects a standalone IR named `<isvc>` or `<isvc>-<component>` for
   an existing ISVC; the ISVC webhook rejects a new ISVC when a standalone IR
   already uses its name or one of its projected IR names. Both checks read
   the API server directly, yet an ISVC and a standalone IR created at the same
   moment can both pass. If the IR took a projected IR name, the projector
   refuses to adopt it and the ISVC reconcile fails with an error that names
   it; the projector never overwrites an IR it does not project. If the IR took
   the ISVC's own name and a role the ISVC also fills, the two IRs of that role
   derive the same pod and Service names and contend for them (Follow-ups).

### InferenceService referenced form

| Field | Type | Validation |
|---|---|---|
| `spec.engine.replicaRefs` | `[]string` | 1..N names in the ISVC's namespace; each IR must have `component: engine`; mutually exclusive with every inline engine field |
| `spec.decoder.replicaRefs` | `[]string` | same, `component: decoder` |
| `spec.router.replicaRef` | `string` | one name, `component: router`; mutually exclusive with inline router fields |
| `spec.rollout.groups[].components` | today `[]ComponentType` | extended to accept `<role>/<irName>` when the role is referenced; a bare role means every ref in that role |
| `spec.traffic` | today LB policy | gains per-ref weights (shape **TBD**, Phase 4) |

A referenced IR may be named by at most one ISVC. Two ISVCs naming the same IR
would give it two composers; admission rejects the second.

Whether `router` allows a list is left at one ref: a router is the external
entrypoint and two of them is a gateway question, not an IR question.

## Design Details

### Ownership enforcement

Two mechanisms can implement rule 3 above. Both are stated; one is recommended.

- **(a) Admission by identity.** The IR webhook already exists and already sees
  `req.UserInfo`. Replace the annotation gate with: if the IR has a controller
  owner reference to an ISVC, user-field writes must come from the OME manager
  ServiceAccount; otherwise from anyone RBAC allows. Rollout-control writes are
  allowed from the manager when `status.coordinationGroupRef` or an ISVC
  `replicaRefs` entry names the IR. *Tradeoff:* the webhook must know the
  manager's identity, which is config (a ServiceAccount name from the Helm
  chart, not a literal in Go).
- **(b) Server-side-apply field managers.** The projector and the composer apply
  with distinct manager names; the webhook rejects a write whose managed-fields
  diff touches a user field under a non-owner manager. *Tradeoff:* every writer
  must move to SSA; `kubectl edit` and merge patches lose the field-manager
  signal, so a user can still clobber a controller field and only notice on the
  next reconcile.

I recommend **(a)**, and Phase 1 implements it. The webhook is in place, the
identity check is one comparison, and the failure is a clear denial at
`kubectl apply` rather than a silent overwrite. The gate (a) replaces is the
string `ome.io/controller-write=true` in annotations, and anyone with write RBAC
can add it: it is a convention, not a boundary. Option (a) makes the webhook the
boundary for spec writes on a projected IR; the annotation remains only as the
fallback when no identity is configured.

#### What Phase 1 implements

- **Identity.** The `inferenceReplica.controllerIdentity` block of the
  `inferenceservice-config` ConfigMap lists `usernames` and `groups`. A request
  comes from the controller when its username is listed or any of its groups
  is. The block parses strictly: an unknown or repeated key, content after the
  object, a blank or whitespace-padded entry, and an identity a whole class of
  callers holds (`system:anonymous`, `system:authenticated`,
  `system:unauthenticated`, `system:serviceaccounts`) are errors, so a
  malformed block never loads as an identity other than the one written. The
  `ome-resources` chart always lists the controller's own ServiceAccount,
  `system:serviceaccount:<release namespace>:ome-controller-manager`, and adds
  the names set in `ome.controller.inferenceReplica.controllerIdentity`.
- **Fallback.** With no identity configured, the webhook recognizes the
  controller by the `ome.io/controller-write=true` annotation the projector
  stamps.
- **Startup check.** Once elected leader, the manager asks the API server who
  it is (a SelfSubjectReview) and logs the admission mode, `identity` or
  `annotation`. If the configured identity does not name the manager, it logs
  an error and keeps running, and the webhook denies its projections until the
  block names it. If the review itself fails, it logs that the identity could
  not be verified, and the webhook's decision stands either way.
- **Installation.** The `ome-resources` chart installs the webhook
  configuration on every multicluster role: fail-closed, on CREATE and UPDATE
  of `inferencereplicas`, and skipping objects already being deleted. The
  `scale` subresource is not matched, so an autoscaler sets `spec.replicas`
  through it on either form without passing the webhook.

The webhook decides each request by the IR's form (section 3) and the
requester:

| Request | Projected IR | Standalone IR |
|---|---|---|
| Create | only the controller, and only with the ISVC controller owner reference: `parentRef` without it is refused for every writer | anyone RBAC admits; the placement fields need the controller; the standalone rejections below apply |
| Spec update | only the controller | anyone RBAC admits; the controller-only fields need the controller; the standalone rejections below apply, except the name checks, which run at create |
| Metadata-only update: labels, annotations, finalizers, keeping or removing the controller owner reference | anyone RBAC admits | anyone RBAC admits, except adding a migration request |
| Install or change a controller owner reference | only the controller | only the controller for an ISVC controller owner reference; anyone RBAC admits for another kind's |
| Change `parentRef` or `component` | refused for every writer | refused for every writer |
| Delete | not gated | not gated |

A standalone IR rejects settings it accepts in shape but nothing acts on
without an ISVC, for every writer including the controller:

- `spec.autoscaler.class` `HPA` or `KEDA`: only the ISVC controller creates
  those scalers. To autoscale a standalone IR, set class `External` and point
  your own HorizontalPodAutoscaler or ScaledObject at its scale subresource.
- `spec.lifecycle.minReadySeconds`: an IR reads `spec.minReadySeconds`, never
  the nested field.
- A name outside the ISVC name pattern, or one that collides with an ISVC
  (rule 4).
- A manual migration request (an `ome.io/migration-request-v1-` annotation): a
  migration surges through the per-revision Service only an ISVC creates.

Two rows need their reason stated. First, an update that keeps the spec and
installs no controller owner reference is decided without loading the
identity. Operator requests such as `ome.io/migration-request-v1-<uuid>`,
`ome.io/release-held-revision` and `ome.io/reset-instances` are annotations,
and the garbage collector patches owner references, so these writes must pass
a fail-closed webhook even when the ConfigMap is unreadable.
A create, a spec change or an owner-reference install needs the identity; if
the block cannot be loaded, the webhook answers with an internal error and the
write fails. Second, anyone may keep or remove a controller owner reference,
because the garbage collector removes it when it orphans an IR and clears
`blockOwnerDeletion` during foreground deletion; the projector re-stamps it
while the ISVC exists. Installing one is different: on a projected IR the
projector would refuse the IR under its new owner and the ISVC reconcile would
fail on every pass, and an ISVC owner on a standalone IR would attach it to an
ISVC. ISVC owner references match by group and kind, so every served version
counts.

**Orphaned projected IRs.** Deleting an ISVC with the orphan policy removes the
owner reference from its IRs and leaves `spec.parentRef` in place, so the IRs
stay projected. The webhook still admits spec writes only from the controller,
and the IR controller stops each pass before the workload pass: it cannot scope
the revision history of an IR that names a parent no ISVC controls. The pods
keep running as they are, and deleting the IR still runs its teardown.
Recreating an ISVC with the same name re-adopts the IRs. The new ISVC's UID
scopes a new revision history, so each IR moves its Instances onto a new
revision through its update strategy, even when the pod template is unchanged.

Composer writes on a referenced IR arrive with Phase 3, and their rule depends
on Open Question 1. Phase 1 has no composer writes: no ISVC names a standalone
IR, and the ISVC controller writes only the IRs it projects.

### Rendering library extraction

The pipeline is the package `pkg/render`. `Resolve` performs every cluster
read: the model and its overlays, the runtime (named, auto-selected, or
supplied pre-resolved by a caller that manages pins), the enabled check, and
the merge of the runtime with the service's role specs. `Prepare` applies
deployment modes and the operator's deploy defaults. `RenderComponent` is pure
given those inputs: it assembles the pod template for one role (fine-tuned
weights, object metadata, container merge, placeholders, model volumes and
env, node selector and affinity, resources, parallelism) and converts it to
`runners` with the same runner names, sizes and template metadata the
projector writes.

Two callers. The ISVC inline path: the component reconcilers call the same
functions in the same order and hand the result to the projector, which
writes `runners`; the golden, captured before the extraction, pins that
output. The IR controller: when a replica carries refs, it builds an
ISVC-shaped shell from the replica (the replica's name prefix, namespace,
labels and annotations; `spec.model` and `spec.runtime` from the refs; no
accelerator selector; an empty role spec for `spec.component` so the runtime
piece is the whole template), calls `Resolve`, `Prepare` and
`RenderComponent`, and feeds the runners into its workload pass in memory. The
replica's stored spec keeps the refs and never gains `runners`.

The revision hash is over the rendered runners, so both callers produce the
same hash for the same inputs. One unit test renders identical inputs through
both paths and asserts identical runners JSON and hash.

The library's input is still shaped like an InferenceService. A signature
over `(model, runtime piece, role overrides, accelerator selection)` is a
follow-up once the referenced form (Phase 3) decides what a role override on
a referenced IR means.

### Discovery hand-off

Nothing new is invented. Per-revision Services stay ISVC-created because their
selector is `{isvc, component, revision-hash, managed-by}`
(`perRevisionServiceSelector`) and their consumer is the HTTPRoute the ISVC owns.
For a referenced IR the `isvc` label value is the composing ISVC's name, stamped
by the composer onto the IR's pod template labels — which is a rollout-control
write. If Open Question 1 resolves to "the composer writes nothing", the
per-revision Service selector must instead key on the IR name, and the peer env
must be rendered from the IR's own label. That is the one place the two answers
to Open Question 1 produce different code.

Peer env today requires the parent ISVC object (`ISVCRenderHook(isvc)`). For a
referenced IR the IR controller resolves the composing ISVC through the
`replicaRefs` back-index (a field index on `spec.*.replicaRefs`) instead of
`parentRef`.

The IR's own drain Service needs no ISVC. Every IR creates a headless Service,
`<name prefix>-<component>-headless`, with the IR as its controller owner, and
the IR controller maps an EndpointSlice event to its IR through that owner
reference (`endpointSliceToIR`). A name parse cannot do this for a standalone
IR: it maps IR `demo`'s Service `demo-engine-headless` to `demo-engine`, an IR
that does not exist. The parse remains as the fallback when the Service
lookup fails or the Service has no IR controller owner
(`irNameFromDrainServiceName`); it covers the ISVC-owned per-revision
Services, whose prefix names the projected IR. When the lookup fails for a
standalone IR's Service, the parse usually yields a name no IR carries (at
worst an unrelated IR reconciles once), so the standalone IR misses that
event.

### Traffic across refs

`TrafficMap` (OEP-0025) has entries keyed by cluster (`entries[].cluster`).
Traffic across refs needs entries keyed by (cluster, IR). The publisher already
turns entries into weighted HTTPRoute backends, so the actuator exists; the key
change is the work. Weights come from `spec.traffic` per-ref weights when set,
else from ready capacity per IR (`status.readyReplicas`), the same
`min(admitted, ready) * factor` formula OEP-0025 uses per home.

### Pairing across sets

A prefill IR pairs with any decode IR carrying the same `pairingProtocol`. Today
that token is stamped by the projector from `spec.rollout.pairingProtocol` and
lands as a label on the per-revision routing Service. With several refs per role
the ISVC stamps the same token on every referenced engine and decoder IR. The
router receives one `OME_DECODER_ENDPOINT` per decode IR (**TBD**: one env var
per ref, or a single Service spanning the refs — the second changes the peer DNS
contract and needs its own decision).

## Risks and Mitigations

| Risk | What breaks | Mitigation |
|---|---|---|
| Two writers of one IR field | rollout flaps between the user's value and the composer's | the spec split plus admission rule 3; a field has one writer per form |
| A referenced IR is deleted while an ISVC names it | the role loses capacity; the ISVC cannot recreate what it does not own | `status.components[c]` reports `ReplicaRefMissing`; the ISVC does not recreate (no inference) |
| The canary partition and the user's rolling-update partition disagree | canary holds the wrong Instances | no field moves: the projector writes the canary partition to `spec.pacing.partition`, the engine reads it ahead of the user's partition (`escalation.EffectivePartition`, section 4), and a unit test pins that order |
| A user edits a projected IR | the projector would overwrite the edit on its next reconcile | the webhook denies a spec write by anyone but the controller identity, and the denial names the parent ISVC to edit instead |
| An ISVC is deleted with the orphan cascade | its IRs stay frozen under the projected rules: only the controller may write their spec, and the IR controller stops each pass before the workload pass, so the pods keep running with no rollout, scaling or pod replacement | recreate the ISVC, which re-adopts the IRs and moves their Instances onto a new revision, or delete the IRs; deletion runs the usual teardown |
| The configured identity does not name the manager | the webhook denies every projection, so no inline ISVC can create or update its IRs | the chart always lists the controller's own ServiceAccount; the startup check logs an error that names both identities; metadata-only updates never load the identity, so finalizers and garbage collection keep working |
| An ISVC and a standalone IR with colliding names are created at the same moment | both admission checks pass (rule 4) | the projector refuses an IR it does not project and never overwrites it; making the same-prefix case harmless is a follow-up |
| Inline path changes bytes | every existing ISVC re-rolls on upgrade | the projector's rendered runners and revision hash are pinned by a golden captured before the extraction and run on every change; the KIND coordination and irmanaged suites run the inline path |

## Test Plan

- **Existing suites, unchanged, run through the inline path each phase:**
  `irmanaged_kind`, `omenative_coordination_kind`, `omenative_perm_kind`,
  `rawdeployment_kind`.
- **Webhook unit tests** (`pkg/webhook/admission/inferencereplica`), Phase 1:
  the decision table per form in both admission modes, owner-reference
  installs and removals, orphaned projected IRs, the standalone rejections,
  the metadata-only path when the identity cannot be loaded, and
  `partition <= replicas`.
- **New envtest suite** `tests/integration/singlecluster/webhook/inferencereplica`,
  the webhook against a real API server. Phase 1: a user creates and edits a
  standalone IR but not its controller-only fields; a standalone IR carrying
  `parentRef` is refused; a projected IR is admitted only from the controller
  identity; a standalone IR named like an ISVC is refused. Phase 2 adds
  `runners` vs refs exclusivity and piece-missing rejection; Phase 3 adds
  second-composer rejection.
- **New envtest suite**
  `tests/integration/singlecluster/controller/inferencereplica_standalone`,
  Phase 1: a standalone IR derives its pod, revision, Service and selector
  names from its own name; its conditions and events land on the IR itself; a
  router IR runs with nothing to route to; deleting the IR tears down through
  its finalizer. Phase 2: a replica with `modelRef` and `runtimeRef` renders
  pods with the runtime's image and the model path; `modelRef` alone
  auto-selects; a runtime without the replica's piece yields
  `Ready=False`/`RuntimePieceMissing`; a runtime image change re-renders into
  a new revision.
- **New KIND spec** `standalone_replica` (Phase 1, classified in the KIND
  spec groups): a standalone IR reaches Ready without an ISVC and rolls to a
  new image.
- **New KIND spec** `standalone_replica_refs` (Phase 2, classified in the KIND
  spec groups): a standalone IR from refs reaches Ready and rolls when its
  runtime's image changes.
- **Chart render test** (`charts/ome-resources/tests/render_test.sh`), Phase 1:
  the identity block always lists the controller's ServiceAccount in the
  release namespace and adds the configured names; a malformed value fails the
  render; the webhook configuration renders on every multicluster role.
- **New KIND specs** for Phases 3-4 (each classified in the KIND spec groups):
  ISVC over a pre-created engine IR; two engine refs on two node pools with
  weights; two runtimes side by side; rollout group naming one ref while the
  other holds; pairing across two decode IRs.
- **Unit:** rendering library golden for each piece, both callers, same hash.

## Graduation Criteria

- **Alpha:** Phases 1-2 merged; inline path byte-identical (golden- and KIND-asserted);
  standalone IR usable. State: Phase 1 is merged and the standalone IR is
  usable; Phase 2 is pending, so alpha is not reached.
- **Beta:** Phase 3 merged; the three Motivation cases run in KIND; Open
  Questions 1 and 2 closed.
- **Stable:** Phase 4 merged; `TrafficMap` per-ref weights published; IR API
  documented on the reference site.

## Alternatives

1. **Keep the IR internal.** *Loses:* the three Motivation cases and every
   platform team; the engine stays reachable only through the model catalog.
2. **Make the ISVC the only public object, with a `pools` field per role.**
   *Loses:* the ISVC becomes the pod-set API *and* the composition API, so every
   IR field is mirrored into `pools[]` and projected back out — a second copy of
   the spec split, with the ISVC controller still the sole writer. The object
   that already is a pod set would exist and be hidden.
3. **A new CRD instead of promoting the IR.** *Loses:* a second workload object
   with the same controller behind it; two status shapes for one engine; and
   the migration of every existing `<isvc>-<component>` IR to the new kind.
4. **IR stays bare; composition users write pod templates (Open Question 2's
   "no").** *Loses:* a referenced engine cannot be auto-matched to a runtime,
   so case 2 (two runtimes) means hand-rendered templates that drift from the
   runtime catalog. Worse: model-format validation happens only
   in the ISVC, so a bare IR can run a template the catalog would have rejected.

## Migration

- Existing users: none. The inline form is today's path; IR names, specs and
  pods are unchanged.
- `kubectl edit` of a projected IR: a spec change is denied unless it comes
  from the controller identity, and the message names the parent ISVC to edit
  instead. An operator-managed `inferenceservice-config` without the
  `inferenceReplica` block leaves no identity configured, and the webhook
  falls back to the `ome.io/controller-write=true` annotation. Metadata stays
  writable by anyone RBAC admits. Rollout-control fields (`paused`, `pacing`)
  on a projected IR stay controller-written; operators keep using the ISVC
  annotation `ome.io/rollout-paused`.
- Upgrading a chart install registers the IR webhook, which only a kustomize
  install registered before Phase 1. From then on, a direct spec edit of a
  projected IR is denied at admission instead of being overwritten by the
  projector on its next reconcile.
- Deleting an ISVC with the orphan policy leaves its IRs frozen under the
  projected rules (Design Details). Recreate the ISVC to re-adopt them, which
  moves their Instances onto a new revision, or delete the IRs.
- A standalone IR rejects, for every writer, the settings nothing acts on
  without an ISVC: autoscaler class `HPA` or `KEDA`,
  `lifecycle.minReadySeconds`, a manual migration request, and a name outside
  the ISVC name pattern or colliding with an ISVC. Use class `External` with
  your own scaler, and `spec.minReadySeconds`. A new ISVC is rejected when a
  standalone IR already uses its name or one of its projected IR names; the
  check runs on create only, so existing ISVCs are unaffected.
- Operators who script against `spec.parentRef`: a set `parentRef` marks a
  projected IR, including one whose ISVC was deleted with the orphan policy
  and whose owner reference is therefore gone. A standalone IR carries neither
  `parentRef` nor an ISVC controller owner reference. `parentRef` stays set on
  inline IRs.

## Follow-ups

These are gaps Phase 1 leaves open or exposes, not decisions this OEP still
needs.

1. **Manual migration on a standalone IR.** A migration surges through the
   per-revision Service, which only an ISVC creates, so the webhook rejects a
   migration request on a standalone IR. The engine could fall back to the
   IR's headless Service when no per-revision Service exists.
2. **Make a name collision harmless, not merely unlikely.** Admission makes the
   collision in rule 4 unlikely but cannot rule it out. Two IRs that derive the
   same name prefix and role write the same headless Service, each setting
   itself as its controller owner (`ReconcileHeadlessService`), and can derive
   the same pod names. The IR controller should leave alone a pod or Service
   another IR controls.
3. **Name-keyed readers ignore an IR the ISVC does not control.** Readers such
   as `irprojector.ComponentIR` fetch `<isvc>-<component>` by name and do not
   check the controller owner reference, so a standalone IR that won the race
   in rule 4 is read as the ISVC's own, for example when the rollout run reads
   revision pairs, or when a projected peer resolves its pairing.
4. **KEDA scale-to-zero on the IR path.** A KEDA scaler idling at zero writes
   0 through the scale subresource. The projector re-stamps the component's
   `minReplicas` over a zero count, and the IR runs one Instance for a zero
   count, so the component never reaches zero pods. Scale-to-zero needs both
   to honor an explicit 0 while an autoscaler owns the count.
5. **Remove the annotation fallback.** The fallback trusts a convention anyone
   with write RBAC can forge. The `ome-resources` chart and the kustomize
   `inferenceservice-config` both carry the identity block; once every
   supported install does, the fallback, and the projector's annotation
   stamp, can be removed.
6. **Rust client drift.** Nothing checks `ome-client-rs` against the CRDs it
   mirrors. Phase 1 regenerated `inferencereplicas.rs` so a Rust consumer
   decodes an IR without `parentRef`; the other modules are not regenerated
   and may lag their CRDs. A CI drift check would keep them current.

## Open Questions

1. **May the composer write rollout-control fields on a referenced IR?**
   Yes: cross-set canary and pause work as today, at the cost of a second writer
   on a user-owned object. No: rollouts across referenced sets are driven only
   by `RolloutPolicy` objects the user binds, and the ISVC is read-only over
   referenced IRs. Design Details names the one code path that differs.
   *Deferred to Phase 3.* Phase 1 does not depend on it: no ISVC names a
   standalone IR, so no composer writes one.
2. **Does the rendering library move into the IR controller?** *Answered
   yes.* The package is `pkg/render` with two callers; the inline ISVC path
   keeps writing `runners` and stays byte-identical; a refs IR carries no
   role overrides. Alternative 4 is the "no" case.
3. **Name:** keep `InferenceReplica`, or rename now that it is public? A rename
   is a new kind with a conversion; it is cheapest before Phase 3.
   *Answered no for Phase 1:* the kind stays `InferenceReplica`. A rename
   changes the object every inline role creates, which breaks the
   byte-identical inline path.
4. **Should `spec.component` be renamed `role`?** The field now means "which
   piece this set renders and which slot it fills." `component` is the ISVC's
   word for the slot; `role` is the word this document needed.
   *Answered no for Phase 1:* the field stays `component`, for the same reason:
   a rename changes the spec bytes of every projected IR.

Next action: the owner decides Open Question 1 before Phase 3, whose
admission rules extend rule 3 to the composer.
