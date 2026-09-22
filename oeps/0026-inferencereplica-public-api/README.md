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
behavior, byte-for-byte) and **referenced** (bring your own IR).

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
- **No new rollout engine.** `pkg/controller/v1beta1/workload` and its 123
  replay goldens under `tests/rolloutspec/replay/golden` are unchanged.
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
- **Referenced form:** the ISVC role names existing IRs
  (`engine: {replicaRefs: […]}`); the user owns them.
- **Owner:** the actor allowed to write an IR's user fields. Inline: the ISVC
  controller. Referenced: whoever created the IR. Exactly one owner per IR.
- **Rollout-control field:** an IR spec field a *composer* writes to steer a
  rollout across sets: partition, pacing, rollback target, paused/pauseMode.
  Everything else in the spec is a **user field**.
- **Composer:** the ISVC controller when it orchestrates a rollout across the
  IRs of one ISVC. A composer is not an owner.

## Proposal

### 1. The IR becomes public by fixing three things

| What makes it internal today | Where | Change |
|---|---|---|
| Only the ISVC creates one | `irprojector.EnsureInferenceReplica` | Users may `kubectl apply` an IR. |
| `parentRef` and `component` assume a parent | `inferencereplica_types.go:64-71`; the IR controller reads the parent via `resolveParent` and derives label, Service and revision names from `parentRef.name` | `parentRef` becomes optional. Naming falls back to the IR's own name. |
| The ISVC controller writes the spec; the webhook denies writes without `ome.io/controller-write=true` | `pkg/webhook/admission/inferencereplica/validator.go:129` | Owner-based enforcement (Design Details). |

The IR controller already tolerates a missing parent: every parent read in
`convert.go` is behind a `parent != nil` guard. Standalone is closer than the
type comments suggest.

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

| Form | Owner | ISVC may write | Enforcement |
|---|---|---|---|
| Inline | ISVC controller | the whole spec (today's projector) | as today, plus owner identity |
| Referenced | the user | at most the rollout-control fields, only while a rollout group names the IR | admission or SSA field managers |

Whether the composer may write even the rollout-control fields on a referenced
IR is **Open Question 1**. The alternative is that cross-set rollouts are driven
only by policy objects the user binds (`RolloutPolicy`, OEP-0017.1), and the ISVC
never writes a referenced IR at all.

### 4. Spec split

Same fields, documented and validated as two groups.

| Group | Fields |
|---|---|
| User | `runners` (or `modelRef`/`runtimeRef`, section 5), `replicas`, `minReadySeconds`, `topologyKey`, `topologySpread`, `topologySpreadKey`, `lifecycle` (restart, update strategy, ready policy, migration), `revisionHistoryLimit`, `autoscaler` |
| Rollout-control | `pacing.partition`, `pacing.maxUnavailable`, `pacing.rollbackToRevision`, `paused`, `pauseMode`, `pairingProtocol` |

One fact from the code cuts across this table. The canary partition does **not**
travel through `spec.pacing.partition` today. `canary.EffectivePartition` stamps
it onto the merged component lifecycle, so it reaches the IR as
`spec.lifecycle.updateStrategy.rollingUpdate.partition` — a user field.
`pacing.partition` and `pacing.maxUnavailable` are copied into the engine input
and never read (`inferencereplica/convert.go:337`); only `rollbackToRevision` is
live. The split therefore requires one of: (a) route the canary partition through
`pacing.partition` and make the engine read it, or (b) declare
`lifecycle.updateStrategy.rollingUpdate.partition` a rollout-control field. I
recommend (a): a composer should never have to write inside `lifecycle`, which
is the user's update strategy. This is a behavior-preserving move only if the
engine treats the two sources identically; the goldens decide.

### 5. Runtime integration moves to the IR, optionally

Two new optional fields, `spec.modelRef` and `spec.runtimeRef`. With them set,
`spec.component` selects **which piece** of the runtime the IR renders. A
runtime has three pieces; the IR's role picks one. The controller never infers
the piece from what the runtime happens to contain.

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
`inferenceservice/utils/merging.go` and `components/base.go` — becomes a library
with two callers: the IR controller (when refs are set) and the ISVC inline form
(which creates an IR with the refs filled). Whether the library moves is **Open
Question 2**; the owner leans yes. The alternative, and why it is worse, is in
Alternatives.

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
| 1 | Spec split documented and validated; owner rule enforced; `parentRef` optional; a bare-pod IR (`runners` only) is usable standalone | existing KIND suites through the inline path; workload goldens; **new** envtest webhook suite `webhook/inferencereplica` for admission |
| 2 | `modelRef`/`runtimeRef`; rendering library extracted; inline form creates an IR with refs | KIND suites through the inline path (byte-equal IR spec is the assertion); goldens; new envtest specs for per-piece validation |
| 3 | Referenced form on the ISVC, one ref per role | new KIND specs: ISVC over a pre-created IR, router over a referenced engine |
| 4 | Multiple refs per role: traffic across refs (`TrafficMap` as actuator), pairing across sets, groups naming refs | new KIND specs per case in Motivation; TrafficMap envtest |

## API

### InferenceReplica additions and changes

| Field | Type | Group | Validation | Note |
|---|---|---|---|---|
| `spec.parentRef` | `ParentReference` | — | optional; immutable once set | today required. Absent means standalone. |
| `spec.component` | `engine\|decoder\|router` | user | required; immutable | selects the piece when refs are set |
| `spec.modelRef` | `ModelRef` (same type as ISVC `spec.model`) | user | optional; requires `runners` absent | `BaseModel` or `ClusterBaseModel` |
| `spec.runtimeRef` | `ServingRuntimeRef` (same type as ISVC `spec.runtime`) | user | optional; runtime must carry the piece named by `component` | without `modelRef`, the runtime's piece is rendered as-is |
| `spec.runners` | `[]Runner` | user | required when neither ref is set; forbidden when a ref is set | unchanged shape |
| `spec.pacing.partition`, `.maxUnavailable`, `.rollbackToRevision` | as today | rollout-control | `partition <= replicas` (today's rule) | partition made live (Proposal 4a) |
| `spec.paused`, `spec.pauseMode` | as today | rollout-control | unchanged | |
| `spec.pairingProtocol` | `*string` | rollout-control | `engine`/`decoder` only | the composer stamps the ISVC value |

Rules:
1. Exactly one of {`runners`, `modelRef`, `runtimeRef`-alone} is the template
   source. Mixing `runners` with a ref is rejected. Rationale: two sources with
   one winner is inference.
2. `runtimeRef` names a runtime without the piece for `component` ⇒ rejected at
   admission when the runtime is readable; otherwise a `Ready=False` condition
   with reason `RuntimePieceMissing`.
3. On a referenced IR, a write to a user field by anyone but the owner is
   rejected. On any IR, a write to a rollout-control field by anyone but the
   owner or the composer is rejected. Mechanism in Design Details.

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

I recommend **(a)**. The webhook is in place, the identity check is one
comparison, and the failure is a clear denial at `kubectl apply` rather than a
silent overwrite. Note what today's gate is: the string
`ome.io/controller-write=true` in annotations. Anyone with write RBAC can add it.
The package doc says so: RBAC is the boundary, the webhook is "discouragement".
Option (a) makes the webhook the boundary for user fields.

### Rendering library extraction

Today the pipeline runs inside the ISVC controller and the component
reconcilers: `controller.go` resolves the runtime (lines 440-500), the
`components/{engine,decoder,router}.go` reconcilers merge the runtime piece with
the ISVC role (`MergeEngineSpec`, `MergeDecoderSpec`, `MergeRouterSpec`), apply
accelerator affinity and node selectors, and hand the finished `PodSpec` to
`irprojector.Params`. The projector copies it into `spec.runners` verbatim.

The extraction keeps the same functions and changes their input type:
`(model, runtimePiece, roleOverrides, acceleratorSelection) -> []Runner`. Two
callers:

1. the ISVC inline path, which fills the role overrides from `spec.engine` and
   creates the IR with `runners` **or** with `modelRef`/`runtimeRef` (choice
   **TBD** in Phase 2; the first keeps the IR spec byte-identical, the second
   makes the IR self-describing);
2. the IR controller, which reads the refs and calls the library before
   `workload.Reconcile`.

The revision hash is over the rendered runners, so either caller produces the
same hash for the same inputs. That is the Phase 2 assertion.

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
| Partition move (Proposal 4a) changes engine behavior | canary holds the wrong Instances | goldens: the move ships only golden-identical, or with a re-record named per shape as OEP-0020.3 did |
| A user edits a controller-owned IR after Phase 1 | today the projector overwrites it next reconcile; after (a) the write is denied | denial message names the parent ISVC to edit instead (today's message already does) |
| Inline path changes bytes | every existing ISVC re-rolls on upgrade | Phase 2 assertion: IR spec byte-equal before and after, checked in the KIND suites |

## Test Plan

- **Existing suites, unchanged, run through the inline path each phase:**
  `irmanaged_kind`, `omenative_coordination_kind`, `omenative_perm_kind`,
  `rawdeployment_kind`; the workload replay goldens (123 files).
- **New envtest suite** `tests/integration/singlecluster/webhook/inferencereplica`:
  owner-identity rules per form; `runners` vs refs exclusivity; piece-missing
  rejection; `partition <= replicas`; second-composer rejection.
- **New envtest specs** in the IR controller suite: standalone bare IR reaches
  Ready with no parent; naming falls back to the IR name; router IR is idle.
- **New KIND specs** (each classified in `KIND_SPEC_GROUPS`): ISVC over a
  pre-created engine IR; two engine refs on two node pools with weights; two
  runtimes side by side; rollout group naming one ref while the other holds;
  pairing across two decode IRs.
- **Unit:** rendering library golden for each piece, both callers, same hash.

## Graduation Criteria

- **Alpha:** Phases 1-2 merged; inline path byte-identical (KIND-asserted);
  standalone IR usable.
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
   "no").** *Loses:* a referenced engine cannot be auto-matched to a runtime or
   accelerator, so case 2 (two runtimes) means hand-rendered templates that
   drift from the runtime catalog. Worse: model-format validation happens only
   in the ISVC, so a bare IR can run a template the catalog would have rejected.

## Migration

- Existing users: none. The inline form is today's path; IR names, specs and
  pods are unchanged.
- `kubectl edit` of an owned IR: today denied for lack of the annotation, or
  silently overwritten if the annotation is forged. After Phase 1: denied by
  identity, with the parent ISVC named in the message. Rollout-control fields
  (`paused`, `pacing`) on an owned IR stay controller-written; operators keep
  using the ISVC annotation `ome.io/rollout-paused`.
- Operators who script against `spec.parentRef` being present should read
  `metadata.ownerReferences` instead; `parentRef` stays set on inline IRs.

## Open Questions

1. **May the composer write rollout-control fields on a referenced IR?**
   Yes: cross-set canary and pause work as today, at the cost of a second writer
   on a user-owned object. No: rollouts across referenced sets are driven only
   by `RolloutPolicy` objects the user binds, and the ISVC is read-only over
   referenced IRs. Design Details names the one code path that differs.
2. **Does the rendering library move into the IR controller?** Owner leans yes.
   The "no" case is Alternative 4.
3. **Name:** keep `InferenceReplica`, or rename now that it is public? A rename
   is a new kind with a conversion; it is cheapest before Phase 3.
4. **Should `spec.component` be renamed `role`?** The field now means "which
   piece this set renders and which slot it fills." `component` is the ISVC's
   word for the slot; `role` is the word this document needed.

Next action: the owner decides Open Question 1 before Phase 1 admission rules are
written, since rule 3 is different under each answer.
