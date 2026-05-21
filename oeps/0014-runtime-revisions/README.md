# OEP-0014: Pinned ServingRuntime revisions for InferenceServices

<!-- toc -->
- [Summary](#summary)
- [Motivation](#motivation)
  - [Goals](#goals)
  - [Non-Goals](#non-goals)
- [Proposal](#proposal)
  - [Part A: storage — apps/v1 ControllerRevision in the OME namespace](#part-a-storage--appsv1-controllerrevision-in-the-ome-namespace)
  - [Part B: ISVC pinning fields + lazy revision creation](#part-b-isvc-pinning-fields--lazy-revision-creation)
  - [Part C: GC](#part-c-gc)
  - [User stories](#user-stories)
- [Design details](#design-details)
- [Implementation phases](#implementation-phases)
- [Drawbacks and trade-offs](#drawbacks-and-trade-offs)
- [Alternatives considered](#alternatives-considered)
<!-- /toc -->

## Summary

Today, every InferenceService reconcile rebuilds the Deployment's pod
template from the **current** ServingRuntime spec. The moment an
operator edits a runtime — even to fix a typo — every ISVC referencing
it rolls. This OEP introduces a per-ISVC pin so an ISVC stays on a
specific snapshot of the runtime until the operator (or researcher)
explicitly opts forward.

Three small pieces:

1. **Storage**: snapshots live in the built-in K8s type
   `apps/v1.ControllerRevision`, all in the OME system namespace.
   No new CRDs, no schema duplication; the opaque `Data` field holds
   the serialized fully-resolved `ServingRuntimeSpec` (post-OEP-0012
   Part B inheritance walk).
2. **ISVC fields**: `spec.runtime.autoSync: bool` (default `true`,
   preserves today's behavior) and `spec.runtime.revision: string`
   (optional explicit pin, enables rollback).
3. **Lazy creation**: revisions are written **only when an ISVC pins**,
   never eagerly on runtime edits. Hash dedup means a second ISVC
   pinning to the same content reuses the same revision; an unchanged
   runtime that nobody pins to writes nothing.

A small **GC** loop deletes ControllerRevisions older than retention
(default N=10 per source runtime) once no pinned ISVC references them.

End state: operators can edit runtimes freely; ISVCs roll only when
their owners ack. Deployment-deletion recovery rebuilds from the
**pinned** revision (true self-heal). Operators can roll a specific
ISVC back to a previous revision by name. Etcd storage cost scales
with *unique specs pinned by ISVCs*, not with runtime edit frequency.

## Motivation

OEP-0012 made runtimes shorter (one engine runtime + N ISVCs) but
also more *load-bearing per change*: a typo fix on a shared
ClusterServingRuntime now rolls every ISVC in every namespace that
references it. With shared runtimes (the OEP-0012 endpoint), the blast
radius of a runtime edit went from "1 ISVC" to "all consumers of that
engine".

Researchers and oncall both want **the ability to refuse to roll**
when a runtime changes — for the same reason no one runs production
on `:latest` image tags. Today there is no way to express this; the
ISVC controller reconciles from the live runtime spec on every loop.

Existing workarounds are all bad:
- Operators stop editing runtimes — defeats the point of the abstraction.
- Each model team forks the cluster runtime into a per-ISVC copy —
  reintroduces the YAML bloat OEP-0012 just eliminated.
- Operators schedule "freeze windows" and coordinate across teams —
  human-scaling problem.

We need versioning at the controller layer.

### Goals

- ISVCs can opt out of automatic re-roll on runtime changes
  (`spec.runtime.autoSync: false`).
- Pinning is **content-addressed** (via the resolved-spec hash), so
  it catches drift from any layer: literal runtime spec edits,
  Part B inheritance chain changes, profile updates.
- Pinned ISVCs can **rebuild from the pinned spec** if their Deployment
  is deleted (operator action, GC misfire). Pinning is a real freeze,
  not "freeze if everything stays normal".
- Operators can **roll a specific ISVC back to a previous revision by
  name** (`spec.runtime.revision: <name>`).
- Revisions GC automatically: N=10 per runtime + any referenced by a
  pinned ISVC.
- **Minimal surface**: no new CRDs, no new top-level controllers, no
  per-runtime status fields. The implementation is a small change to
  the ISVC controller plus a small GC loop.

### Non-Goals

- **GitOps replacement.** This OEP is not a substitute for keeping
  runtime YAMLs in git. Revisions are an operational-recovery
  primitive, not an audit log.
- **Cross-cluster rollback.** Revisions are per-cluster state.
- **Audit trail of every runtime version ever shipped.** Because
  creation is lazy, only specs that some ISVC actually pinned to
  exist as revisions. Operators who want a complete history use git.
- **Automatic ISVC bulk-ack.** For ISVCs that opt into pinning
  (`autoSync: false`), v1 ships with the strict semantics: each ISVC
  acks individually via an annotation bump. Bulk-ack primitives
  (cluster-level auto-ack policy, runtime-level "advance all pinning
  ISVCs") are explicit follow-ups, not in scope.
- **Per-ISVC snapshot stored on `InferenceService.status`.** Rejected
  again here: the resolved spec is fat (~5–15 KB); per-ISVC inflation
  is unacceptable. ControllerRevisions in the OME namespace carry the
  spec exactly once and are shared by every pinning ISVC.
- **Custom revision CRDs.** Considered (the original draft of this
  OEP). Rejected because defining a CRD whose spec embeds
  `ServingRuntimeSpec` generates ~13k lines of OpenAPI schema YAML
  (full + minimal × Helm copies × two scopes) — pure surface, zero
  behavioral benefit over `ControllerRevision`.

## Proposal

### Part A: storage — apps/v1 ControllerRevision in the OME namespace

`ControllerRevision` is a built-in `apps/v1` type that StatefulSets
and DaemonSets already use for rollback history. Shape:

```go
type ControllerRevision struct {
    metav1.TypeMeta
    metav1.ObjectMeta
    Data     runtime.RawExtension  // opaque
    Revision int64                 // monotonic counter
}
```

We use one ControllerRevision per `(source runtime, resolved-spec-hash)`
pair, stored in the OME system namespace (default `ome`, overridable
via `--ome-namespace`). The opaque `Data` field holds the serialized
`ServingRuntimeSpec` (the same spec OEP-0012 Part B's `Resolve*`
helpers return). `Revision` carries a monotonic counter for ordering
when audit display is wanted.

**Naming**:

| Source runtime kind | Namespace | Revision name |
|---|---|---|
| `ClusterServingRuntime srt-llama-pd` | — | `cr-srt-llama-pd-<shorthash>` |
| `ServingRuntime team-a/srt-foo` | `team-a` | `r-team-a-srt-foo-<shorthash>` |

All revisions live in the OME namespace regardless of source scope.
The `r-<orig-ns>-` prefix on namespaced-source revisions prevents
collisions between SR `team-a/srt-foo` and SR `team-b/srt-foo`.
`<shorthash>` is the first 8 hex chars of `sha256(json(resolved spec))`.

**Labels on each revision** (the writer sets these; the GC controller
queries by them):

| Label | Value | Purpose |
|---|---|---|
| `ome.io/runtime-of` | source runtime name (`srt-llama-pd`) | list-by-source |
| `ome.io/runtime-of-kind` | `ClusterServingRuntime` / `ServingRuntime` | scope split |
| `ome.io/runtime-of-namespace` | source SR namespace (empty for cluster scope) | scope split |
| `ome.io/revision-hash` | 8-char short hash | dedup lookup |

**Annotation** (gates the validating webhook so only the OME
controller can write):

| Annotation | Value |
|---|---|
| `ome.io/created-by` | `ome-controller` |

**Immutability** is enforced two ways: (a) the OME controller never
issues `Update` against an existing ControllerRevision (matches
StatefulSet/DaemonSet's contract — Create new, don't mutate), and
(b) a small validating webhook on `apps/v1/controllerrevisions` in
the OME namespace rejects user `Update` attempts targeting OME-created
revisions (filtered by label `ome.io/runtime-of` and the
`ome.io/created-by` annotation).

### Part B: ISVC pinning fields + lazy revision creation

Two new fields on `InferenceService.spec.runtime`:

```go
type ServingRuntimeRef struct {
    Name      string  `json:"name"`
    Namespace *string `json:"namespace,omitempty"`

    // AutoSync controls whether the ISVC follows runtime updates
    // automatically. Default true — preserves the pre-OEP-0014
    // behavior of re-rendering pod specs from the latest runtime
    // every reconcile. Set false to pin to a specific revision; the
    // ISVC then stays on the revision captured at first reconcile
    // until either:
    //   - the operator bumps `ome.io/runtime-sync` annotation
    //     (advances the pin to a fresh revision created from the
    //     runtime's current state), OR
    //   - the operator sets spec.runtime.revision explicitly
    //     (pins to a named ControllerRevision).
    // +optional
    // +kubebuilder:default=true
    AutoSync *bool `json:"autoSync,omitempty"`

    // Revision pins to a specific ControllerRevision by name (always
    // in the OME namespace). When set, overrides any first-reconcile
    // auto-pin behavior; useful for rollback to a known-good revision.
    // Setting to a non-existent revision surfaces a RuntimeDrifted
    // condition with Reason=RevisionMissing.
    // +optional
    Revision *string `json:"revision,omitempty"`
}
```

Status additions (small):

```go
type InferenceServiceStatus struct {
    // PinnedRevisionName is the ControllerRevision (in the OME ns)
    // the ISVC controller is currently using to build pod specs.
    // +optional
    PinnedRevisionName string `json:"pinnedRevisionName,omitempty"`

    // Conditions add a new type "RuntimeDrifted".
    // Reasons: "RevisionMismatch" | "RevisionMissing" | "PinAdvanced"
    Conditions []metav1.Condition `json:"conditions,omitempty"`
}
```

ISVC reconcile flow (only ISVC-side change; no new top-level
controller):

```
1. Fetch ISVC
2. Reconcile baseModel (existing)
3. Resolve target spec:
   a. If spec.runtime.revision is set explicitly →
        fetch that ControllerRevision from OME ns;
        if not found → RuntimeDrifted=True, Reason=RevisionMissing;
        return.
   b. Else if autoSync == true (default) →
        no pin needed; resolve live runtime via OEP-0012
        Resolve* helpers and reconcile (today's behavior).
   c. Else (autoSync == false; pinning):
        - if status.pinnedRevisionName != "":
            fetch that ControllerRevision;
            if missing → RuntimeDrifted=True, Reason=RevisionMissing;
        - else (first reconcile):
            resolve current spec, compute hash;
            look up existing rev by (runtime, hash) label;
            if exists → status.pinnedRevisionName = its name;
            else      → CREATE a new ControllerRevision and pin to it.
4. Detect drift:
   if autoSync == false:
     compute hash of current resolved live runtime spec;
     if hash != hash(pinned spec):
       set RuntimeDrifted=True (Reason=RevisionMismatch);
       if ome.io/runtime-sync annotation was bumped since last reconcile:
         create-or-find revision for current hash;
         set status.pinnedRevisionName = new revision name;
         clear RuntimeDrifted; fall through to step 5;
       else:
         status update; skip children; return.
5. Build merged specs from the pinned revision's spec (or live spec
   in the autoSync=true case); run engine/decoder/router/service/HPA
   reconcilers (existing flow).
6. Status update.
```

Two acknowledged behaviors:

- **Drift freezes all children**, not just the Deployment template.
  If the operator wants to scale `engine.minReplicas` while drifted,
  they must first ack the runtime drift.
- **Deployment-deletion recovery rebuilds from the pinned revision**.
  The whole point of having the revision content in etcd: when
  `kubectl delete deployment/...` happens to a pinned ISVC, the
  controller rebuilds from the pin, not the (possibly drifted) live
  runtime.

### Part C: GC

A small periodic loop in the ISVC controller (or a tiny dedicated
controller — implementation detail) iterates ControllerRevisions in
the OME namespace and deletes ones that satisfy ALL:

1. Labeled `ome.io/created-by=ome-controller` (we don't touch
   StatefulSet/DaemonSet-owned revisions).
2. No ISVC's `status.pinnedRevisionName` or `spec.runtime.revision`
   currently references the revision name.
3. Older than the source-runtime's N-th-most-recent revision (default
   `N=10`, configurable via `--runtime-revision-retention=N`).
4. Grace period elapsed (default 24h since became unreferenced) —
   gives in-flight ISVC reconciles a chance to settle before delete.

ISVCs whose pinned revision is GC'd surface `RuntimeDrifted=True`
with `Reason=RevisionMissing` — operator action required:
- Set `spec.runtime.revision` to a still-live revision, OR
- Clear `spec.runtime.revision` and bump `ome.io/runtime-sync` to
  pin to a fresh revision off the current runtime spec.

### User stories

**Story 1: researcher protects a model launch from runtime edits.**
Alice deploys an ISVC for a new model. She wants the runtime frozen
during the launch window — no surprise rolls if the platform team
edits a CSR. She sets `spec.runtime.autoSync: false`. On the next
reconcile, the controller computes the current resolved-runtime hash,
creates a ControllerRevision `cr-csr-sglang-pd-abc12345` in the OME
namespace, and pins Alice's ISVC to it. She stays there until she
bumps `ome.io/runtime-sync`.

**Story 2: platform team fixes a typo in a shared CSR.**
Platform pushes a one-character fix to `csr-sglang-pd`. Among the 50
ISVCs referencing it, the ~10 that opted into `autoSync: false`
surface `RuntimeDrifted=True`; the rest (default `autoSync: true`)
roll forward automatically using the live spec. Pinned-ISVC owners
review the diff (`kubectl get controllerrevision -n ome cr-csr-sglang-pd-def67890 -o yaml`)
and ack their own ISVCs at their own pace. No coordinated rollout
meeting.

**Story 3: bad runtime change rolled to one ISVC, operator rolls back.**
Bob's ISVC is on `autoSync: true` (the default). A bad runtime change
rolled out; his pods crash-loop. He sets `spec.runtime.autoSync:
false` and `spec.runtime.revision: cr-csr-sglang-pd-abc12345` to roll
back to the known-good revision (which exists because some other
pinned ISVC happened to reference it). Controller fetches that
revision, rebuilds the Deployment, pods recover. The runtime itself
is still drifted from rev `cr-csr-sglang-pd-def67890`; only Bob's
ISVC is on `abc12345`.

**Story 4: most ISVCs need no changes on upgrade.**
Carol's team trusts runtime changes; OEP-0014 lands and nothing
changes for her ISVCs. `autoSync` defaults to `true`, controller
reads from the live runtime every reconcile (today's behavior).
**No ControllerRevisions are created** for runtimes that have no
pinning consumers — zero etcd overhead.

**Story 5: Deployment deleted by accident.**
Dave's intern runs `kubectl delete deployment/dave-isvc-engine`.
Dave's ISVC has `autoSync: false`, pinned to `cr-csr-sglang-pd-abc12345`.
Live runtime is now drifted. Controller reconciles, sees Deployment
missing, **rebuilds from the pinned revision's spec** (not the live
drifted spec). Dave's pod is back, still on the version he tested
with.

## Design details

### Hash content addressing

`sha256(json.Marshal(resolved ServingRuntimeSpec))`, truncated to 8
hex chars for the name. The full 64-char hex isn't stored separately
— the short hash is the canonical identifier and dedup key; if a
collision is ever suspected, operators can recompute from the
revision's `Data` field.

JSON-canonical-form is not required: Go's `encoding/json` writes
struct field order deterministically, and map keys (in
Annotations/Labels/EnvVar valueFrom fields) are sorted. Same
serializer drives production reconciles; same scheme registration.

Collisions at 8 chars: birthday probability ~1 in 4 billion. If
encountered, the controller logs a hard error on Create (the
duplicate ControllerRevision name would be rejected by K8s with
`AlreadyExists`); operator workaround is to mutate the runtime
slightly to shift the hash. Acceptable for realistic fleet sizes.

### Why a single OME namespace for all revisions

- **Simpler than per-namespace storage**: one place to look, one RBAC
  surface for the writer + GC.
- **Cluster-scope-friendly**: ControllerRevision is namespaced-only
  in K8s. CSR-derived revisions need a namespace; the OME ns is the
  natural choice. SR-derived revisions could live in the source SR's
  namespace, but the operational cost of two locations (different
  RBAC stories, two places to grep) outweighs the visibility benefit.
- **ISVCs already cross-namespace-fetch** for ClusterBaseModel; this
  is the same pattern.
- **RBAC**: ISVC controller already has `controllerrevisions` read
  access (used in our existing flows? if not, a 1-line RBAC marker
  adds it). Writer needs Create + List in OME ns only.

### Lazy creation invariant

A ControllerRevision exists in the OME namespace if-and-only-if at
least one ISVC has, at some point, pinned to its content. Hash dedup
ensures a no-op runtime edit followed by ISVC pin doesn't create a
new revision (the hash matches an existing one).

The corollary: a runtime that no ISVC ever pins to never produces
any ControllerRevision. This is the design's primary etcd-cost win
over the eager-writer approach.

### Migration from pre-OEP-0014 ISVCs

Existing ISVCs without `spec.runtime.autoSync` set will default to
`true` on next admission — i.e., they preserve pre-OEP-0014
behavior. The ISVC controller reads from the live runtime exactly as
today. No revisions are created for them.

Teams that want pinning opt in by setting `autoSync: false` on the
ISVCs they care about. The first reconcile after that flip creates
the initial pin.

### Permissions

- **Writer + GC** (the OME controller): Create + List + Delete on
  `apps/v1/controllerrevisions` in the OME namespace.
- **Validating webhook** on `apps/v1/controllerrevisions` (namespace
  selector = OME ns): rejects user Update attempts on revisions
  carrying the `ome.io/created-by=ome-controller` annotation.
  StatefulSet/DaemonSet-owned revisions (no such annotation) are
  unaffected.
- **ISVC consumer**: Get on `apps/v1/controllerrevisions` in the
  OME namespace (cross-namespace read, same as today's CBM access).

### Status size on ISVC

Adds ~80 bytes per pinned ISVC (`PinnedRevisionName` + condition).
Negligible. Revision objects carry the full resolved spec but live
once per (runtime, hash) and are shared by all pinning ISVCs.

## Implementation phases

**Phase 1 (ISVC pin field + lazy creation + drift detection)**:
- ISVC `spec.runtime.autoSync` field (default `true`).
- ISVC controller resolves to either live runtime (autoSync=true) or
  pinned ControllerRevision (autoSync=false).
- Lazy revision creation in the ISVC controller (find-or-create by
  hash label).
- `ome.io/runtime-sync` annotation drives ack.
- Status field `PinnedRevisionName` + `RuntimeDrifted` condition.
- Validating webhook for revision immutability.

**Phase 2 (explicit revision pin for rollback)**:
- ISVC `spec.runtime.revision` field for explicit pinning.
- Validation: revision exists, matches runtime via labels.
- Error condition `RevisionMissing` when referenced revision is gone.

**Phase 3 (GC loop)**:
- Periodic GC of unreferenced revisions older than N=10 per source
  runtime.
- 24h grace period.
- Configurable retention via `--runtime-revision-retention=N`.

Each phase is independently mergeable. Phase 1 alone is the minimum
useful slice (pinning works; revisions just accumulate without GC
until Phase 3 lands). Phase 2 unlocks rollback. Phase 3 caps long-run
etcd growth.

## Drawbacks and trade-offs

- **All-or-nothing freeze**. When pinned and drifted, the ISVC
  refuses any update to children — including ones that have nothing
  to do with the runtime (e.g., scaling `engine.minReplicas`). v1
  accepts this; splitting the reconcile graph into runtime-driven vs
  isvc-driven children is a real follow-up if it bites researchers.
- **Default `autoSync: true` defers the benefit until each team opts in**.
  Teams that don't change anything keep the current "auto-roll on
  runtime edit" behavior. Pinning is opt-in per ISVC. Deliberate
  upgrade-safety choice; teams that *should* be pinned but aren't
  won't be. A namespace-level default (e.g., a
  `ServingRuntimePinPolicy` CR) is a follow-up if it becomes a
  common ask.
- **No audit trail of every runtime version**. Lazy creation means
  only specs that some ISVC pinned to exist as revisions. Operators
  who want a complete history use git (where runtime YAMLs should
  live anyway).
- **OME-namespace RBAC concentration**. All revisions live in one
  namespace; operators with broad write on that namespace can delete
  any revision and break pinned ISVCs. Mitigation: the validating
  webhook rejects Delete attempts by non-OME-controller principals if
  we want to harden further. v1 trusts ome-namespace RBAC.
- **Hash collisions** at 8-char truncation. Birthday probability is
  ~1 in 4 billion. K8s rejects the create with `AlreadyExists`; the
  controller logs loudly. Operator action would be to tweak the
  runtime to shift the hash.
- **Operator-deleted pinned revision** (kubectl-deleted out from under
  a pin). Surfaces `RuntimeDrifted=True / RevisionMissing`. Operator
  workaround: re-pin to a still-live revision or bump the sync
  annotation to create a fresh one. Documented as an operator footgun.

## Alternatives considered

### Custom ServingRuntimeRevision / ClusterServingRuntimeRevision CRDs (the original draft of this OEP)

The first draft of OEP-0014 defined two new CRDs whose `spec.spec`
field embedded `ServingRuntimeSpec`. controller-gen materializes the
full OpenAPI v3 schema inline for any embedded type, producing ~13k
lines of generated YAML (full + minimal × Helm copies × two scopes)
— pure surface for zero behavioral benefit over `ControllerRevision`,
which provides the same opaque-data storage with a built-in K8s
type. Rejected.

### Reuse ServingRuntime / ClusterServingRuntime with `disabled: true` + revision labels

Considered. Pros: zero new types. Cons: pollutes `kubectl get sr` /
`kubectl get csr` output with revision objects mixed in among
operator-authored runtimes, overloads the `disabled` field semantics,
and forces the OEP-0012 inheritance resolver to specifically skip
revision-labeled runtimes. `ControllerRevision` is cleaner because
it's a structurally separate concept.

### Snapshot the resolved runtime spec on ISVC.status

Rejected (in both this design and the original draft). The resolved
spec is fat (~5–15 KB serialized); per-ISVC inflation in a 50-ISVC
cluster adds substantial etcd cost with no sharing across pinning
ISVCs. ControllerRevisions share storage by hash.

### Eager revision creation (one per runtime spec change)

The original draft of OEP-0014. Wrote a new revision on every
runtime mutation. Pros: gives a full audit trail; rollback target
always exists for any past version. Cons: noisy runtimes generate
N=10×M revisions worth of etcd just for the option to pin. Most
runtimes never get pinned, so most revisions are wasted. Switched
to lazy creation as part of this revision.

### Hash-only pin with no revision storage

The very first design discussion's "Path 1". Pin would be just
`status.pinnedRuntimeHash`; controller would skip child reconcile
on hash mismatch, leaving K8s's existing `Deployment.spec.template`
in place as the de-facto snapshot. The fatal problem: if the
Deployment is deleted, the controller has no content to rebuild
from — the historical runtime spec is gone from etcd forever.
Soft-recovery from current spec defeats the pin's guarantee; a true
pin needs durable content storage. ControllerRevision is the
durable-content answer.

### GitOps as the source of truth for revisions

True for operators with mature GitOps. Not viable as the *only*
answer:
- Researchers who don't own runtimes can't roll their own ISVC back
  to a previous version via git (they don't have commit access).
- Self-heal on Deployment deletion still requires the controller to
  have content available **right now**, not "after a human reverts
  a git commit and ArgoCD syncs".

Revisions complement GitOps; they don't replace it.
