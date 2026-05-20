# OEP-0014: Pinned ServingRuntime revisions for InferenceServices

<!-- toc -->
- [Summary](#summary)
- [Motivation](#motivation)
  - [Goals](#goals)
  - [Non-Goals](#non-goals)
- [Proposal](#proposal)
  - [Part A: ServingRuntimeRevision + ClusterServingRuntimeRevision CRDs](#part-a-servingruntimerevision--clusterservingruntimerevision-crds)
  - [Part B: Revision-writer controller](#part-b-revision-writer-controller)
  - [Part C: ISVC pinning fields and reconcile semantics](#part-c-isvc-pinning-fields-and-reconcile-semantics)
  - [Part D: Revision GC controller](#part-d-revision-gc-controller)
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
it rolls. This OEP introduces **immutable runtime revisions** plus a
per-ISVC pin so an ISVC stays on a specific snapshot of the runtime
until the operator (or researcher) explicitly opts forward.

Three new pieces:

1. **`ServingRuntimeRevision` / `ClusterServingRuntimeRevision`** —
   immutable CRDs holding the fully-resolved runtime spec (post-OEP-0012
   Part B inheritance walk) at a moment in time. One per
   (runtime, resolved-spec-hash). Naming: `<runtime>-rev<N>-<shorthash>`.
2. **Revision-writer controller** — watches SR/CSR (and inheritance
   fan-out from OEP-0012), creates a new revision whenever the
   resolved spec changes, updates `runtime.status.latestRevision`.
3. **ISVC `spec.runtime.autoSync` field** (default **true**, preserves
   today's behavior) — set `false` to opt into pinning. When pinned,
   the ISVC controller fetches the **pinned** revision (not the live
   runtime) on every reconcile. Drift surfaces as a `RuntimeDrifted`
   condition; user acks via an annotation bump.

A **revision GC controller** retains the last N=10 revisions per
runtime plus any revision currently referenced by a pinned ISVC.

End state: operators can edit runtimes freely; ISVCs roll only when
their owners ack. Deployment-deletion recovery rebuilds from the
**pinned** revision (true self-heal). Operators can roll a specific
ISVC back to a previous revision by name.

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
  it catches drift from *any* layer: literal runtime spec edits,
  Part B inheritance chain changes, profile updates.
- Pinned ISVCs can **rebuild from the pinned spec** if their Deployment
  is deleted (operator action, GC misfire). Pinning is a real freeze,
  not "freeze if everything stays normal".
- Operators can **roll a specific ISVC back to a previous revision by
  name** (`spec.runtime.revision: <name>`).
- Revisions GC automatically: N=10 per runtime + any referenced by a
  pinned ISVC.

### Non-Goals

- **GitOps replacement.** This OEP is not a substitute for keeping
  runtime YAMLs in git. Revisions are an operational-recovery primitive,
  not an audit log.
- **Cross-cluster rollback.** Revisions are per-cluster state.
- **Automatic ISVC bulk-ack.** For ISVCs that opt into pinning
  (`autoSync: false`), v1 ships with the strict semantics: each ISVC
  acks individually via an annotation bump. Bulk-ack primitives
  (cluster-level auto-ack policy, runtime-level "advance all pinning
  ISVCs") are explicit follow-ups, not in scope.
- **Snapshot stored on the ISVC.** This OEP explicitly rejects storing
  the resolved runtime spec on `InferenceService.status`. The spec
  is fat (engine + decoder + router runners with full container,
  env, ports, probes); per-ISVC inflation is unacceptable. Revisions
  carry the spec exactly once and are referenced by name.

## Proposal

### Part A: ServingRuntimeRevision + ClusterServingRuntimeRevision CRDs

Two new CRDs, one per scope, mirroring `ServingRuntime` /
`ClusterServingRuntime`:

```yaml
apiVersion: ome.io/v1beta1
kind: ClusterServingRuntimeRevision
metadata:
  name: srt-llama-pd-rev5-abc12345
  # Owner ref to the source CSR — but DOES NOT cascade delete:
  # revisions outlive their source runtime so pinning ISVCs can
  # continue to fetch their snapshot after a misclick.
  labels:
    ome.io/runtime: srt-llama-pd      # for fast lookup by source runtime
    ome.io/revision-hash: abc12345    # short hash of spec.spec
spec:
  # Source runtime identity at write time. The revision writer
  # captures all three from data that already exists on the runtime:
  #   - name             ← source runtime's metadata.name
  #   - generation       ← source runtime's metadata.generation (k8s built-in)
  #   - inheritanceChain ← source runtime's status.inheritanceChain
  #                        (populated by OEP-0012 Part B's controller)
  # No new fields on ServingRuntime / ClusterServingRuntime are required.
  source:
    name: srt-llama-pd
    generation: 12
    inheritanceChain: [profile-cw-gb300-rdma-16, srt-llama-pd]
  # The fully-resolved spec, exactly as OEP-0012 Part B's
  # ResolveClusterRuntime would return it. Frozen at write time.
  spec:
    supportedModelFormats: [...]
    engineConfig: {...}
    decoderConfig: {...}
    routerConfig: {...}
    ... (full ServingRuntimeSpec)
status:
  # Controllers set this; consumers read it for fast revision listing.
  references: 3   # number of ISVCs currently pinning to this revision
  retainedUntil: 2026-05-25T00:00:00Z   # earliest GC eligibility
```

**Immutability**: `spec` is immutable post-create, enforced via
validating webhook. Status updates are controller-only.

**Naming**: `<runtime-name>-rev<N>-<shorthash>` where N is the
monotonic revision number for that runtime (status field on the
runtime tracks `nextRevisionNumber`) and `<shorthash>` is the first
8 hex chars of `sha256(spec)`. Examples:
- `srt-llama-pd-rev1-abc12345`
- `srt-llama-pd-rev2-def67890`
- `profile-cw-gb300-rdma-16-rev1-1a2b3c4d` (yes, profiles get
  revisions too — they affect inheritance fan-out)

**Scope**: a `ServingRuntimeRevision` lives in the same namespace as
its source `ServingRuntime`. `ClusterServingRuntimeRevision` is
cluster-scoped. ISVCs in any namespace can pin to a CSR revision.

### Part B: Revision-writer controller

A new controller in `pkg/controller/v1beta1/runtimerevision/`
watches:

1. `ServingRuntime` / `ClusterServingRuntime` create / update
2. Same events that drive OEP-0012's inheritance fan-out (because a
   profile change *should* produce new revisions for every runtime
   that inherits from it)

On reconcile of a runtime:

1. Resolve via `runtimeinheritance.ResolveClusterRuntime` /
   `ResolveNamespacedRuntime` (existing OEP-0012 helpers).
2. Compute `sha256(resolvedSpec)`. Short to 8 hex chars.
3. Look up existing revisions with label `ome.io/runtime=<name>` AND
   `ome.io/revision-hash=<shorthash>`. If one exists, no-op (dedup —
   no-op edits don't bloat revision count).
4. Else: read `runtime.status.nextRevisionNumber` (initialize to 1 if
   absent), populate the revision's `spec.source` block from the
   runtime's existing data (`metadata.name`, `metadata.generation`,
   `status.inheritanceChain`), create the revision, increment the
   counter, set `runtime.status.latestRevision = <created name>`.

Idempotent. Safe to restart mid-reconcile (dedup catches the case
where the revision was created but status wasn't updated yet).

Only one new field is added to `ServingRuntime` / `ClusterServingRuntime`
status: `latestRevision` (the name of the most recent revision) plus
`nextRevisionNumber` (monotonic counter). All other source identity
(`name`, `generation`, `inheritanceChain`) is read from existing fields
the runtime already carries.

### Part C: ISVC pinning fields and reconcile semantics

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
    //     (advances the pin to the runtime's latest revision), OR
    //   - the operator sets spec.runtime.revision explicitly
    //     (pins to a named revision).
    // +optional
    // +kubebuilder:default=true
    AutoSync *bool `json:"autoSync,omitempty"`

    // Revision pins to a specific *ServingRuntimeRevision by name.
    // When set, overrides any first-reconcile auto-pin behavior;
    // useful for rollback to a known-good revision.
    // Must reference a revision whose .spec.source.name matches
    // spec.runtime.name. Setting to a GC'd revision surfaces a
    // hard error condition.
    // +optional
    Revision *string `json:"revision,omitempty"`
}
```

Status additions:

```go
type InferenceServiceStatus struct {
    // PinnedRevisionName is the *ServingRuntimeRevision the ISVC
    // controller is currently using to build pod specs.
    // +optional
    PinnedRevisionName string `json:"pinnedRevisionName,omitempty"`

    // LatestObservedRevisionName tracks the runtime's
    // status.latestRevision at the time of the last reconcile.
    // When PinnedRevisionName != LatestObservedRevisionName,
    // the RuntimeDrifted condition is True.
    // +optional
    LatestObservedRevisionName string `json:"latestObservedRevisionName,omitempty"`

    // Conditions add a new type "RuntimeDrifted".
    // Reason: "RevisionMismatch" | "RevisionMissing" | "PinAdvanced"
    Conditions []metav1.Condition `json:"conditions,omitempty"`
}
```

ISVC reconcile flow (Part C is the only ISVC-side change):

```
1. Fetch ISVC
2. Reconcile baseModel (existing)
3. Determine target revision:
   a. If spec.runtime.revision is set → use that exact revision
      (fetch it; if not found → set RuntimeDrifted=True
       Reason=RevisionMissing; return)
   b. Else if autoSync == true → fetch runtime,
      read runtime.status.latestRevision, use that
   c. Else if status.pinnedRevisionName is set → use that (already pinned)
   d. Else (first reconcile, autoSync=false, no revision set) →
      fetch runtime, use runtime.status.latestRevision, persist as
      status.pinnedRevisionName
4. Update status.latestObservedRevisionName = runtime.status.latestRevision
5. If autoSync == false AND status.pinnedRevisionName != status.latestObservedRevisionName:
   - set RuntimeDrifted=True (Reason=RevisionMismatch)
   - if ome.io/runtime-sync annotation has changed since last reconcile:
       - advance status.pinnedRevisionName = status.latestObservedRevisionName
       - clear RuntimeDrifted
       - fall through to step 6
   - else: skip children-reconcile, status update, return
6. Fetch the pinned revision; use its spec.spec for MergeRuntimeSpecs
   (existing flow). Build children as usual.
7. Status update.
```

Two acknowledged behaviors:

- **Drift freezes all children**, not just the Deployment template.
  If the operator wants to scale `engine.minReplicas` while drifted,
  they must first ack the runtime drift. (Splitting "runtime-driven
  children" vs "isvc-driven children" was considered; the reconcile
  graph isn't cleanly bipartite. Documented for v1; revisit if it
  bites researchers.)
- **Deployment-deletion recovery rebuilds from the pinned revision**.
  This is the entire point of having the revision in etcd — true
  self-heal to the version the operator chose.

### Part D: Revision GC controller

A small controller in `pkg/controller/v1beta1/runtimerevision/`
(same package as the writer; one controller per logical concern):

Reconcile triggered by:
- `ServingRuntime` / `ClusterServingRuntime` revision count exceeding
  the retention limit (via `runtime.status.nextRevisionNumber` watcher)
- ISVC create / delete / pin change (updates `revision.status.references`)
- Periodic resync (default 5m) to handle missed events

Retention rule (per runtime):
1. **Always keep**: any revision currently referenced by `status.references > 0`.
2. **Always keep**: the latest N revisions (default `N=10`, configurable
   via `--runtime-revision-retention=N` controller flag).
3. **GC candidates**: everything else, oldest first. Delete after
   `revision.status.retainedUntil` (24h grace period from when it
   became unreferenced) to give in-flight ISVC reconciles a chance
   to settle.

ISVCs reading a GC'd revision surface `RuntimeDrifted=True` with
`Reason=RevisionMissing` — the operator must either:
- Set `spec.runtime.revision` to a still-live revision (rollback to
  a different known-good version), OR
- Clear `spec.runtime.revision` and bump `ome.io/runtime-sync` to
  advance to the runtime's latest revision.

### User stories

**Story 1: researcher protects a model launch from runtime edits.**
Alice deploys an ISVC for a new model. She wants the runtime frozen
during the launch window — no surprise rolls if the platform team
edits a CSR. She sets `spec.runtime.autoSync: false`. The ISVC pins
to the latest revision at first reconcile and stays there until she
explicitly bumps `ome.io/runtime-sync`.

**Story 2: platform team fixes a typo in a shared CSR.**
Platform pushes a one-character fix to `csr-sglang-pd`. Among the 50
ISVCs referencing it, the ~10 that opted into `autoSync: false`
surface `RuntimeDrifted=True`; the rest (default `autoSync: true`)
roll forward automatically. Pinned-ISVC owners review the diff
(`kubectl describe csrrev csr-sglang-pd-rev9-...`) and ack their
own ISVCs at their own pace. No coordinated rollout meeting.

**Story 3: bad runtime change rolled to one ISVC, operator rolls back.**
Bob's ISVC is on `autoSync: true` (the default). A bad runtime
change rolled out; his pods crash-loop. He sets
`spec.runtime.autoSync: false` and `spec.runtime.revision:
csr-sglang-pd-rev9-abc12345` to roll back to the known-good revision.
Controller fetches that revision, rebuilds the Deployment, pods
recover. The runtime itself is still at rev10 — only Bob's ISVC is
on rev9. Bob can drop the explicit revision later (and bump the
sync annotation) once platform has shipped a fix.

**Story 4: most ISVCs need no changes on upgrade.**
Carol's team trusts runtime changes; OEP-0014 lands and nothing
changes for her ISVCs. `autoSync` defaults to `true`, controller
fetches the runtime's latest revision every reconcile (effectively
today's behavior, now backed by an immutable revision instead of
the live runtime spec). She gets free benefits — `kubectl get
csrrev` shows an audit trail of every runtime version her ISVCs
have ever run on — without changing any of her YAML.

**Story 5: Deployment deleted by accident.**
Dave's intern runs `kubectl delete deployment/dave-isvc-engine`.
Dave's ISVC has `autoSync: false`, pinned at rev3. Runtime is now
at rev7. Controller reconciles, sees Deployment missing, rebuilds
**from rev3 (the pin)**. Dave's pod is back, still on the version
he tested with.

## Design details

### Revision content addressing

The hash is `sha256(JSON-marshaled resolved ServingRuntimeSpec)`,
truncated to 8 hex chars for the name. Full 64-char hex stored in
`spec.source.specHash` for collision auditing. Marshal-then-hash
gives deterministic output across controller restarts as long as the
Go scheme registration is stable (which it is — same scheme drives
production reconciles).

JSON-canonical-form is not required: the strategic-merge resolver
already normalizes field ordering, and `equality.Semantic.DeepEqual`
is the dedup check before write. Hash collisions on different specs
are astronomically unlikely at 8-char truncation for fleets of
hundreds of revisions; if it ever happens the writer will refuse
the create with a clear error and the platform team will rev the
hash length cluster-wide.

### Migration from pre-OEP-0014 ISVCs

Existing ISVCs without `spec.runtime.autoSync` set will default to
`true` on next admission — i.e., they preserve pre-OEP-0014
behavior. On the next reconcile:
- Writer controller (Phase 1) has already created the first revision
  for the referenced runtime and set `runtime.status.latestRevision`.
- ISVC controller fetches from that revision (not the live runtime).
- Because `autoSync == true`, the ISVC follows the runtime's
  `latestRevision` on every reconcile — effectively the same
  behavior as fetching the live runtime, except the content path
  goes through the revision CRD for audit and recovery.

There is no migration moment; no surprise `RuntimeDrifted`. Teams
that want pinning opt in by setting `autoSync: false` on the ISVCs
they care about.

### Permissions

- Revisions are OME-controller-only on Create / Update. A validating
  webhook rejects user Create attempts (`ome.io/created-by` annotation
  must equal `ome-controller`). Status mutations are
  controller-only via RBAC + subresource.
- ISVCs `spec.runtime.revision` is operator-set; the ISVC controller
  validates that the referenced revision exists and that its
  `spec.source.name` matches `spec.runtime.name`.

### Webhook impact

- New validating webhook on `ServingRuntimeRevision` /
  `ClusterServingRuntimeRevision` for immutability + writer-only creates.
- ISVC validator gains a check: when `spec.runtime.revision` is set,
  reject if the revision name doesn't match the runtime name pattern
  (`<runtime-name>-rev<N>-<hash>`). Full existence check is at reconcile
  (admission can't reliably read across cluster scope).

### Status size

Per-ISVC status grows by ~150 bytes (`PinnedRevisionName` +
`LatestObservedRevisionName` + condition). Negligible. Revision
objects carry the full resolved spec but live in their own object
and dedup by hash — a 50-ISVC namespace sharing one CSR runtime
stores **one** revision per spec change, not 50.

## Implementation phases

**Phase 1 (foundation, ~1 PR)**:
- New CRDs (`ServingRuntimeRevision` + `ClusterServingRuntimeRevision`) with
  immutability webhook.
- Revision-writer controller (creates revisions on runtime change,
  dedup by hash, updates `runtime.status.latestRevision`).
- No ISVC integration yet — revisions are created but not consumed.

**Phase 2 (ISVC pin field + drift detection, ~1 PR)**:
- ISVC `spec.runtime.autoSync` field (default `false`).
- ISVC controller fetches from revision (not live runtime) when
  pinned; sets `RuntimeDrifted` condition on mismatch.
- `ome.io/runtime-sync` annotation drives ack.
- Status fields `PinnedRevisionName` + `LatestObservedRevisionName`.

**Phase 3 (explicit revision pin for rollback, ~1 PR)**:
- ISVC `spec.runtime.revision` field for explicit pinning.
- Validation: revision name matches runtime name.
- Error condition `RevisionMissing` when referenced revision is gone.

**Phase 4 (GC controller, ~1 PR)**:
- GC of unreferenced revisions older than N=10 per runtime.
- 24h grace period.
- Configurable retention via controller flag.

Each phase is independently mergeable: Phase 1 ships value
(operators get a `kubectl get csrrev` audit log of runtime changes)
without any ISVC behavior change.

## Drawbacks and trade-offs

- **All-or-nothing freeze**. When pinned and drifted, the ISVC
  refuses any update to children — including ones that have nothing
  to do with the runtime (e.g., scaling `engine.minReplicas`). v1
  accepts this; documented in user stories. Splitting the reconcile
  graph into runtime-driven vs isvc-driven children is a real follow-up
  if it bites researchers in practice.
- **Default `autoSync: true` defers the benefit until each team opts in**.
  Teams that don't change anything keep the current "auto-roll on
  runtime edit" behavior. Pinning is opt-in per ISVC. This is a
  deliberate choice favoring upgrade safety (no surprise drift on
  unmodified ISVCs) over default strictness. Teams that *should* be
  pinned but aren't won't be — they have to know to set the flag.
  An optional namespace-level default (e.g., a `ServingRuntimePinPolicy`
  CR) is a follow-up if "pin by default for my team's namespace"
  becomes a common ask.
- **Revision proliferation on noisy runtimes**. A runtime that gets
  edited 100 times in a day generates 100 revisions (dedup catches
  no-op edits; non-no-op edits create real revisions). GC default
  N=10 caps this, but in the window between edit-100 and
  GC-eligibility, etcd carries 100 revisions. Acceptable; bounded
  by operator behavior. Cluster admins can lower N if needed.
- **Hash collisions** at 8-char truncation. Birthday probability is
  ~1 in 4 billion. We refuse the create on collision and log loudly.
  Operator action would be to bump the runtime to force a fresh hash
  prefix.
- **Operator action required on Deployment deletion + GC'd pin**.
  If Dave's ISVC is pinned to rev3, rev3 gets GC'd because no ISVC
  references it (oh wait — Dave's ISVC does reference it, so it
  wouldn't get GC'd; this case is handled). But if Dave deletes
  his pinned revision *manually* with `kubectl delete csrrev ...`,
  the ISVC enters `RuntimeDrifted=True / RevisionMissing` state
  and stops reconciling. Documented as an operator footgun.

## Alternatives considered

### Snapshot the resolved runtime spec on ISVC.status

Considered and explicitly rejected. The resolved spec is fat (engine
+ decoder + router with full container, env, port, probe specs —
~5–15KB serialized). Per-ISVC inflation in a 50-ISVC cluster adds
~500KB to etcd in the best case; with multi-component runtimes
realistic sizes are larger. Revisions in their own object share
storage across pinning ISVCs (one revision per spec change, N
pinning ISVCs).

### Hash-only pin with no revision storage

The "lightweight Path 1" from the design discussion. Pin would be
just `status.pinnedRuntimeHash`; controller would skip child
reconcile on hash mismatch, leaving K8s's existing
`Deployment.spec.template` in place as the de-facto snapshot. The
fatal problem: if the Deployment is deleted, the controller has no
content to rebuild from — the runtime gen=1 spec is gone from etcd
forever. Soft-recovery from current spec defeats the pin's
guarantee; a true pin needs durable content storage.

### Use `kubectl rollout` semantics via Deployment ReplicaSet history

K8s already stores past ReplicaSets for Deployments. Could we walk
those to find the "pinned" pod template?
- ReplicaSets are GC'd by their own retention policy
  (`spec.revisionHistoryLimit`, default 10).
- ReplicaSet snapshots are per-Deployment, not per-runtime — they
  don't help when multiple ISVCs share a runtime.
- Not a model anyone outside Kubernetes core expects to read from.

Rejected as fragile and indirect.

### Per-runtime `latestRevision` link without separate revision objects

Just track `runtime.status.latestRevisionHash` on the runtime
itself; ISVCs pin by hash; controller refuses to re-render on
mismatch.

Same Deployment-deletion failure mode as hash-only pinning. No
rollback target. Rejected for the same reasons.

### GitOps as the source of truth for revisions

"Operators keep runtime YAMLs in git; pin and rollback happen via
git revert + reapply, not via in-cluster CRDs."

True for operators with mature GitOps. Not viable as the *only*
answer:
- Researchers who don't own runtimes can't roll their own ISVC back
  to a previous version via git (they don't have commit access).
- Self-heal on Deployment deletion still requires the controller to
  have content available **right now**, not "after a human reverts a
  git commit and ArgoCD syncs".

Revisions complement GitOps; they don't replace it.
