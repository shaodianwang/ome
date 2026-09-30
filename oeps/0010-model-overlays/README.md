# OEP-0010: ISVC Model Overlays

<!-- toc -->
- [Summary](#summary)
- [Motivation](#motivation)
  - [Goals](#goals)
  - [Non-Goals](#non-goals)
- [Proposal](#proposal)
  - [User Stories](#user-stories)
  - [Notes/Constraints/Caveats](#notesconstraintscaveats)
  - [Risks and Mitigations](#risks-and-mitigations)
- [Design Details](#design-details)
  - [API change](#api-change)
  - [Env var injection](#env-var-injection)
  - [Volume mounts](#volume-mounts)
  - [Cross-distribution overlays](#cross-distribution-overlays)
  - [Webhook validation](#webhook-validation)
  - [Status surface](#status-surface)
  - [Failure modes](#failure-modes)
  - [Test Plan](#test-plan)
- [Implementation History](#implementation-history)
- [Drawbacks](#drawbacks)
- [Alternatives](#alternatives)
<!-- /toc -->

## Summary

This OEP adds `ModelRef.Overlays []ModelRef` to `InferenceService.spec.model`.
Each overlay is a reference to a `BaseModel` / `ClusterBaseModel` whose
contents the ISVC controller attaches to every serving pod alongside the
primary model. Overlays are **pre-positioned** — always mounted (PVC) or
made addressable via env var (Sharded) — and **not switched to
automatically**. Selection between the primary and an overlay is up to the
runtime container at request time, or to the operator via a deliberate
annotation flip.

This is **additive**: an ISVC with `spec.model.overlays = nil` (today's
shape) behaves exactly as it does now. Only ISVCs that declare overlays
get extra volumes / env vars on their pods.

## Motivation

Today, when a Sharded BaseModel becomes unhealthy at serving time (the
model-cache provider fails chunk fetches, the cache data plane is
partially degraded, S3 mtime drift mid-rollout), the operator's only
recovery path is editing `InferenceService.spec.model.name` to point at
a different BaseModel of the same architecture (e.g. a PVC-backed copy).
That edit:

1. mutates the desired state of the ISVC, which is awkward to undo
   cleanly when the primary recovers;
2. requires a full Deployment rollout — the new pods download the model
   from scratch via the new storage backend (PVC mount, OCI download, …),
   which for a ~200GB checkpoint can take many minutes;
3. drops in-flight requests because the rollout is a normal rolling
   restart, not a graceful traffic shift.

The natural-feeling answer — "the controller should detect primary
failure and auto-swap" — runs into a wall described at length in the
[Risks](#risks-and-mitigations) section: OME has no reliable way to
distinguish a 10-second hiccup from a permanent failure. Any
detection-based switching either flaps or is too slow to be useful.

The reframe in this proposal is to move "what should the engine do when
the primary is unavailable?" out of the OME control plane, where the
signal is poor and indirect, and into the pod, where the signal is
immediate and complete. The control plane's job becomes preparing the
pod with the alternatives already in place. Selection is local.

### Goals

- Operators can declare a primary BaseModel and one or more **overlay**
  BaseModels on a single ISVC. The overlays are referenced — not inlined —
  so their lifecycle (storage, status, parsed metadata) continues to be
  managed by the BaseModel reconciler.
- The ISVC controller mounts or env-injects each overlay's storage so it
  is reachable from the runner container without any additional Pod
  restart.
- Failover from primary to overlay is a **pod-internal decision**: the
  runner container chooses which `MODEL_PATH` to read based on its own
  health signals. OME does not detect, infer, or schedule failover.
- An operator can manually pin a specific overlay during an incident via
  a documented annotation. This is the only OME-driven failover surface.
- Overlay refs are validated at admission (exist, runtime is compatible,
  no env-var name collisions) so misconfigurations fail fast.

### Non-Goals

- **No automatic failover.** Detection and switching are out of scope.
  See [Alternatives](#alternatives) for why.
- **No traffic-splitting across primary and overlay simultaneously.**
  Each ISVC pod points at exactly one active model at a time.
- **No model composition / merging.** Overlays are alternates, not
  layers. LoRA / fine-tuned-weight delta composition is a separate
  feature (`ModelRef.FineTunedWeights`, already exists).
- **No probe / heartbeat machinery** for overlays. Overlays attached but
  unused incur no health checks, no status updates beyond
  resolved-or-not.
- **No cross-namespace mixing of namespaced BaseModel + cluster overlay
  in a way the existing primary doesn't already allow.** Same scoping
  rules as today.

## Proposal

Extend `InferenceServiceSpec.Model` with a new field:

```go
type ModelRef struct {
    Name             string   `json:"name"`
    Kind             *string  `json:"kind,omitempty"`
    APIGroup         *string  `json:"apiGroup,omitempty"`
    FineTunedWeights []string `json:"fineTunedWeights,omitempty"`

    // NEW — declared alternate models attached to the pod.
    Overlays []ModelRef `json:"overlays,omitempty"`
}
```

Each overlay is a recursive `ModelRef` (reuses the existing kind /
apiGroup / fine-tuned-weights fields). Overlays cannot themselves
declare nested overlays — webhook rejects depth > 1.

For each declared overlay, on every reconcile the ISVC controller:

1. **Resolves** the overlay's BaseModel via the same code path as the
   primary (`ReconcileBaseModelWithStatus`). Disabled or NotReady
   overlays are skipped with a status note.
2. **Sanitizes** the overlay's metadata name into an env-var-safe token:
   `strings.ToUpper(strings.ReplaceAll(name, "-", "_"))`.
3. **Computes the effective path** per the overlay's distribution:
   - `PVC` → `/opt/ml/model-overlays/<name>`, mounted from the PVC into
     every runner container (read-only).
   - `Sharded` → the raw `storage.storageUri` (e.g. `s3://…`).
     No mount; the runner is expected to fetch through the model-cache
     client envs that are already wired in.
   - `PerNode` (HostPath) → `spec.storage.path` value, mounted as
     HostPath identical to the primary HostPath flow.
4. **Injects** `OVERLAY_<SANITIZED_NAME>_MODEL_PATH=<effective_path>`
   on every container that already receives the primary's `MODEL_PATH`.
5. **Reports** the active selection + the list of mounted overlays via
   `InferenceService.status` (see [Status surface](#status-surface)).

The runner container decides at runtime which env var to read. A
common pattern for runtime images is a shell or Python wrapper that
tries `$MODEL_PATH` first and falls back to a configured overlay env on
specific error classes — but that logic lives in the runner image, not
in OME.

### User Stories

#### Story 1: Sharded primary, PVC overlay as runtime safety net

Operator runs `foo` via Sharded distribution (model-cache provider) for
performance. Wants the same checkpoint mounted from a PVC as a
last-resort path the engine can fall back to internally if the
model-cache provider misbehaves.

```yaml
spec:
  model:
    name: foo
    overlays:
      - name: foo-pvc
```

Pod gets `MODEL_PATH=s3://…` (primary, model-cache provider) and
`OVERLAY_FOO_PVC_MODEL_PATH=/opt/ml/model-overlays/foo-pvc`
(overlay, PVC mount). Engine wrapper script:

```sh
exec python -m sglang.launch_server \
    --model-path "${MODEL_PATH:-${OVERLAY_FOO_PVC_MODEL_PATH}}" \
    ...
```

If the model-cache provider degrades, operator can flip via annotation
without changing the spec:

```sh
kubectl annotate isvc foo ome.io/active-model=foo-pvc
```

Controller picks this up, sets `MODEL_PATH=/opt/ml/model-overlays/foo-pvc`
on the next reconcile, rolls pods. Files already on disk → ~30s pod
boot vs ~5min from a fresh PVC mount + load. Removing the annotation
restores the primary.

#### Story 2: hot debug / inspection access

Operator wants the PVC mounted purely for `kubectl exec` visibility
during incidents — no fallback logic in the engine. Declares the overlay
and does nothing else. Pods boot normally; on incident, operator can
`exec` in and read the PVC contents, compare checksums, etc.

#### Story 3: A/B candidate weights

Operator wants two candidate fine-tuned variants of the same base
checkpoint available in the pod for online comparison without rolling.
Both are declared as overlays; the engine wrapper picks one based on
a request header.

(Note: this is a degenerate use case — the more idiomatic shape is two
separate ISVCs behind a router with weighted traffic. Listed here so
the design's flexibility is visible.)

### Notes / Constraints / Caveats

- **Overlay BaseModel readiness is not gated.** A NotReady overlay is
  simply omitted from the pod spec; the pod still boots with the primary
  intact. If a strictly-required overlay is needed, an operator can use
  ISVC events + status conditions to alert.
- **One mount path per overlay, deterministic.** Overlay `foo-pvc`
  always mounts at `/opt/ml/model-overlays/foo-pvc`. No customization
  knob in v1 — keep the convention rigid for tooling and runner-script
  authors.
- **Engine cooperation is required for runtime failover.** Without a
  runner wrapper that knows to check overlay env vars, the only
  failover mechanism is the operator annotation flip.

### Risks and Mitigations

- **Detection is the real hard problem and this OEP intentionally
  skips it.** A future OEP can add an external controller that watches
  serving-level metrics and toggles `ome.io/active-model` based on
  rules — but that work is independent of the primitive proposed here.
  By not pretending OME can detect failure reliably, we avoid building
  an unhealthy controller that will flap or be too slow.
- **Cross-distribution overlays inflate the pod-spec generation
  surface.** A Sharded primary with a PVC overlay needs both `MODEL_PATH`
  env injection and PVC volume mounting. A PVC primary with a Sharded
  overlay needs model-cache env vars even though the primary doesn't.
  Mitigation: the controller derives env wiring per overlay
  independently, not from an "effective distribution" of the pod as a
  whole.
- **Env var name collisions** between overlays with names that sanitize
  to the same token (`foo-bar` and `foo_bar`). Mitigation: webhook
  rejects creates / updates whose overlay set produces duplicate
  sanitized names.

## Design Details

### API change

In `pkg/apis/ome/v1beta1/inference_service.go`:

```go
type ModelRef struct {
    Name string `json:"name"`

    // +kubebuilder:default="ClusterBaseModel"
    Kind *string `json:"kind,omitempty"`

    // +kubebuilder:default="ome.io"
    APIGroup *string `json:"apiGroup,omitempty"`

    // +optional
    // +listType=atomic
    FineTunedWeights []string `json:"fineTunedWeights,omitempty"`

    // +optional
    // +listType=map
    // +listMapKey=name
    Overlays []ModelOverlayRef `json:"overlays,omitempty"`
}

// ModelOverlayRef is intentionally a *separate, narrower* type from
// ModelRef. This prevents an overlay from declaring its own overlays
// at the type-system level — there's no "depth > 1" admission check
// to maintain, and controller-gen emits a fully-validated CRD schema
// for overlay items (a recursive `Overlays []ModelRef` collapses to
// `items: {}` because controller-gen can't recurse into the same type).
//
// The overlay surface also excludes FineTunedWeights: weight
// composition belongs on the primary ModelRef, not on every overlay.
type ModelOverlayRef struct {
    Name string `json:"name"`

    // +kubebuilder:default="ClusterBaseModel"
    Kind *string `json:"kind,omitempty"`

    // +kubebuilder:default="ome.io"
    APIGroup *string `json:"apiGroup,omitempty"`
}
```

The `+listType=map` + `+listMapKey=name` enables server-side-apply
field-level diffs on the list.

### Why not `Overlays []ModelRef`?

Reusing `ModelRef` for overlay items would have been one fewer type to
introduce, but creates three real problems:

1. **Unbounded nesting at the type level**: a `ModelRef` whose
   `Overlays` field references more `ModelRef`s with their own
   `Overlays` is a legal Go type. Preventing nesting requires
   admission-time enforcement of a rule the type system could express
   for free.
2. **Lost CRD schema validation**: controller-gen handles recursive
   types by emitting `items: {}` for the recursive level — kubebuilder
   field-level validation (required fields, defaults, kind enum) is
   silently dropped for the overlay items. Apiserver still accepts
   nonsense schemas the webhook would have to re-validate.
3. **Server-side apply ambiguity**: `+listType=map` is well-defined for
   one level of nesting; recursive types make per-field ownership
   tracking ambiguous.

The cost of a separate `ModelOverlayRef` type is small (three fields,
no behavior) and bought with it: type-system-enforced "no nested
overlays," full CRD validation on the overlay items, clean server-side
apply semantics, and freedom to evolve overlay-specific fields without
touching the primary ModelRef.

### Env var injection

For each resolved overlay `o`, on every container that already receives
the primary's `MODEL_PATH` env, append:

```
OVERLAY_<SANITIZED_NAME>_MODEL_PATH = <effective_path>
```

where:
- `SANITIZED_NAME = strings.ToUpper(strings.ReplaceAll(o.Name, "-", "_"))`
- `effective_path`:
  - PVC overlay: `/opt/ml/model-overlays/<o.Name>`
  - Sharded overlay: `o.Storage.StorageUri` verbatim
  - PerNode overlay: `*o.Storage.Path`

Name sanitization happens after webhook validation, which guarantees no
two overlays produce the same `SANITIZED_NAME`.

### Volume mounts

Only PVC and PerNode overlays produce volume mounts. Sharded overlays
do not; their data is fetched at runtime via the model-cache provider.

For each PVC overlay:
- Add a `Volume` with `PersistentVolumeClaim{ClaimName, ReadOnly: true}`
- Add a `VolumeMount` to every container that gets the primary
  `MODEL_PATH`, with `MountPath = /opt/ml/model-overlays/<name>` and
  `SubPath = <pvc-subpath-from-storageUri>`

For each PerNode overlay:
- Add a `Volume` with `HostPath{Path: *Storage.Path}`
- Add a `VolumeMount` at `/opt/ml/model-overlays/<name>`

Volumes are named `model-overlay-<name>` to make `kubectl get pod -o
yaml` self-explanatory.

### Cross-distribution overlays

Each overlay's wiring is computed independently from its own
distribution. The pod can therefore carry, e.g.:

| Primary | Overlay A | Overlay B | Resulting pod adds |
|---|---|---|---|
| Sharded | PVC | — | model-cache env (primary) + 1 PVC volume + 1 overlay env var |
| PVC | Sharded | — | PVC mount (primary) + model-cache env (overlay needs it) + 1 overlay env var |
| Sharded | Sharded | PVC | model-cache env + 1 PVC volume + 2 overlay env vars |

The model-cache env vars (`MODEL_CACHE_PROVIDER`,
`MODEL_CACHE_ENDPOINT`, `CLUSTER_CACHE_HEADLESS_SERVICE`,
`MODEL_CACHE_OPTIONS`) are injected if the **primary OR any overlay** is
Sharded. They are pod-level config, not per-model, so a single set
covers all Sharded references.

### Webhook validation

In `pkg/webhook/admission/isvc/`:

- **Existence**: each overlay's `Name` / `Kind` resolves to a
  reachable BaseModel/ClusterBaseModel. Same lookup rules as the
  primary. NotFound = reject.
- **No nesting**: enforced by the `ModelOverlayRef` type itself —
  overlays cannot declare their own `Overlays` field, so no admission
  check is needed.
- **No self-reference**: overlay `Name` ≠ primary `Name` (same kind +
  apiGroup).
- **No duplicate names**: two overlays with the same `Name` are
  rejected. Also rejects two distinct-name overlays whose
  `SANITIZED_NAME` collide (e.g. `foo-bar` and `foo_bar`).
- **Runtime compatibility**: each overlay model must be supportable
  by the chosen runtime. Same `runtimeselector.ValidateRuntime` rules
  as the primary. This is so a healthy primary can't paper over a
  silently-broken overlay.

### Status surface

Add two fields to `InferenceServiceStatus`:

```go
// ActiveModel reports which declared model is currently driving the
// pod's MODEL_PATH. Equal to spec.model.name in the steady state.
// Set to an overlay name when ome.io/active-model annotation pins one.
// +optional
ActiveModel *ActiveModelRef `json:"activeModel,omitempty"`

// MountedOverlays lists overlays that resolved and were included in
// the current pod spec. Excludes overlays that were skipped because
// the referenced BaseModel was disabled, NotReady, or NotFound — those
// surface as conditions on this status.
// +optional
// +listType=map
// +listMapKey=name
MountedOverlays []MountedOverlay `json:"mountedOverlays,omitempty"`
```

```go
type ActiveModelRef struct {
    Name       string      `json:"name"`
    SinceTime  metav1.Time `json:"sinceTime"`
    Reason     string      `json:"reason"`  // "PrimaryActive", "AnnotationPinned"
}

type MountedOverlay struct {
    Name          string `json:"name"`
    MountPath     string `json:"mountPath,omitempty"`     // for PVC / PerNode
    EnvVar        string `json:"envVar"`                  // OVERLAY_…_MODEL_PATH
    Distribution  string `json:"distribution"`            // PVC, Sharded, PerNode
}
```

Plus a new condition `OverlaysReady` reporting True iff every declared
overlay either resolved and was included or was deliberately skipped
because the operator pinned a different one.

### Failure modes

| Failure | Behaviour |
|---|---|
| Overlay BaseModel does not exist | Reject at webhook on create / update |
| Overlay BaseModel disabled or NotReady | Omit from pod spec; `OverlaysReady=False` with reason |
| Overlay-only namespace mismatch | Same handling as primary today (per Kind rules) |
| Overlay env var collision | Reject at webhook |
| Operator annotation `ome.io/active-model=<name>` references an overlay that's not declared / not Mounted | Reject the change (mutating webhook) |
| Primary becomes NotReady | Pod spec still rendered; overlays remain; this OEP does not change ISVC's gate on primary readiness |

### Test Plan

#### Unit Tests

- `ModelRef` deepcopy with overlays.
- Env-var name sanitization edge cases (empty, hyphens, uppercase
  already, mixed-case, leading-digits — and reject the leading-digit
  case per POSIX env var rules).
- Cross-distribution wiring: pod-spec snapshot for each of the
  3-row table in [Cross-distribution overlays](#cross-distribution-overlays).

#### Integration Tests

Add to `tests/integration/singlecluster/controller/inferenceservice/`:

- **Single PVC overlay, Sharded primary**: pod spec contains
  model-cache envs + PVC volume + `OVERLAY_<name>_MODEL_PATH` env;
  primary `MODEL_PATH` is the raw storage URI.
- **Single Sharded overlay, PVC primary**: pod spec contains PVC
  mount + model-cache envs (because of overlay) + `OVERLAY_<name>_MODEL_PATH`
  = the raw storage URI.
- **Two overlays, mixed types**: both env vars present, only PVC
  contributes a volume.
- **Disabled overlay**: omitted from pod spec;
  `MountedOverlays` excludes it; `OverlaysReady=False`.
- **Annotation flip**: setting `ome.io/active-model=<overlay>` swaps
  `MODEL_PATH` and triggers a pod restart; clearing reverts.
- **Webhook rejections**: nested overlays, name collision, sanitized-name
  collision, self-reference.

Add to `tests/integration/singlecluster/webhook/inferenceservice/`:
- Each rejection path above as its own spec.

## Implementation History

- 2026-05-18: OEP drafted.

## Drawbacks

- Adds API surface to the central `ISVC.spec.model` field. Operators who
  never care about overlays still see the field in `kubectl explain`.
- Introduces a convention (`/opt/ml/model-overlays/<name>` + env var
  naming) that runner-image authors have to know about. Documented in
  the OEP and printed in `kubectl get isvc -o yaml` status, but it's
  still a coupling.
- The annotation `ome.io/active-model` is a non-spec mechanism for what
  is effectively a config flag. We accept this because the *list* of
  candidates is in the spec; the annotation only selects among them.

## Alternatives

### Auto-failover based on CBM / pod / probe signals

Discussed at length in [Risks](#risks-and-mitigations). The detection
signals OME has access to (CBM status, pod CrashLoop, restart counters,
probe failures) are either too slow, too noisy, or fundamentally
unrelated to the actual runtime health of model-fetching code in the
engine. Building auto-failover on top of these signals will either flap
or be too sluggish to be useful.

### Two parallel ISVCs + external traffic-flip

Operator runs `foo-sharded` and `foo-pvc` as separate ISVCs. A
`HTTPRoute` or external DNS controls which receives traffic. Works
today with zero OME changes, but doubles the GPU footprint and offers
no in-pod failover for engines that could otherwise self-recover.

This OEP is complementary, not competitive — operators with strict
zero-downtime requirements should still consider the two-ISVC pattern.
The overlay pattern is for operators who want a single-ISVC unit of
deployment with a faster recovery path than `spec.model.name` edits.

### `BaseModel.spec.fallbacks` instead of `ISVC.spec.model.overlays`

Push the alternate-storage concept down to the BaseModel CRD: the
BaseModel reconciler picks one storage backend, ISVCs are unaware.

Rejected because:
- Cross-distribution fallbacks (Sharded primary, PVC secondary) make
  the BaseModel's "effective distribution" a moving target, which
  cascades into the ISVC controller's branch-on-distribution logic.
- Multiple ISVCs may want to opt into overlays selectively; pushing the
  policy to the BaseModel forces all-or-nothing.
- The ISVC is the natural envelope for "what's attached to this pod" —
  consistent with how `Engine.runner`, `Decoder.runner`, and existing
  `FineTunedWeights` already live at this layer.

### Env var / annotation as the declarative API

What the operator instinct reaches for first: "just let me set an env
var or annotation that names the fallback." Rejected because:
- No schema, no validation, no defaulting, no `kubectl explain`.
- Status reporting (`MountedOverlays`) has nowhere to live.
- Webhook can't catch typos or stale references.

Annotations are the right shape for *one-shot selection* among
already-declared options, not for declaring the options themselves.
Hence the split: spec holds the list; annotation pins one within it.
