# OEP-0028: Encoder-prefill-decode (EPD) disaggregation

<!-- toc -->
- [Summary](#summary)
- [Motivation](#motivation)
- [Terminology](#terminology)
- [Proposal](#proposal) — the (A) fixed role vs (B) stage list decision
- [Design Details](#design-details) — API, role table, data path, discovery and pairing, rollout, status, admission, IR, samples and KIND
- [Test Plan](#test-plan)
- [Graduation Criteria](#graduation-criteria)
- [Drawbacks](#drawbacks)
- [Alternatives](#alternatives)
- [Open Questions](#open-questions)
<!-- /toc -->

## Summary

**Tl;dr:** Multimodal models run an encoder (vision, audio) before the language
model sees the prompt. Today that encoder lives inside the prefill pod. This OEP
lets it run as its own pod set — `spec.encoder` on the InferenceService,
`encoderConfig` on the ServingRuntime, `encoder` as a fourth `ComponentType` —
wired to prefill the way prefill is wired to decode: a per-revision endpoint
pair in env, one opaque pairing token, a consumer-first rollout order. Moving
the embeddings between encoder and prefill stays the runtime's business, as KV
transfer is today. We add a fixed role rather than a general stage list, and we
make the 37 places that spell out `router/engine/decoder` read from one table,
so a later stage list changes that table, not 37 switches.

Nothing in this tree runs EPD yet: no runtime, no sample, no test. This is the
shape, checked against the PD code on main at `2720a40b`; the first runtime to
run under it is Open Question 1.

## Motivation

**The failure mode.** A vision-language model on a PD topology carries the
vision encoder inside the prefill pod because there is nowhere else to put it.
Encoder and prefill then share one accelerator class, one replica count, one
image and one rollout, and three things break:

1. **Wrong hardware.** An encoder is small (hundreds of millions to a few
   billion parameters) and compute-bound; prefill for a large model is
   memory-bound and wants the biggest device. Co-located, the encoder gets a
   GB300 it cannot fill.
2. **Wrong scaling.** Image-heavy traffic saturates the encoder while prefill
   idles; text-heavy traffic does the reverse. `spec.<role>.autoscaler`
   (OEP-0013) scales roles apart; an encoder that is not a role cannot be.
3. **Wrong rollout unit.** An encoder image fix re-rolls every prefill pod —
   minutes of warm-up per pod at 1M-token context.

PD solved the same three problems for decode by making it a role: per-role pod
sets (OEP-0007), per-revision Services and peer env (OEP-0007.1), groups over
roles (OEP-0017), the pair as the canary unit (OEP-0017.2), the pairing token
and gate (`coordination/pairing.go`), the proposed run-level cohort (OEP-0027
Proposal 1). The encoder needs one more edge in the same mechanism.

### Goals

- An encoder pod set that scales, schedules and rolls independently of prefill
  and decode.
- Encoder and prefill pods discover each other per revision through PD's env
  contract; an external router discovers encoder pods by label.
- The rules that keep a prefill from serving without a pairable decoder extend
  to the encoder.
- Every place that enumerates roles reads one table.

### Non-Goals

- How embeddings move from encoder to prefill: the runtime owns this, as it
  owns KV transfer.
- Sharing one encoder pod set across InferenceServices (Open Question 3).
- A general N-stage pipeline API (option (B)); this OEP prepares for it only.
- RawDeployment or MultiNode: everything here is OMENative (IR-managed), as PD
  coordination is.

## Terminology

Each term is defined once; the consequence is part of the definition.

- **Role (Component):** a slot on an InferenceService that becomes one pod set
  with its own revisions and rollout: `engine` (prefill in a PD topology),
  `decoder`, `router` (`inference_service_status.go:266-272`). This OEP adds
  `encoder`. An undeclared role has no pods.
- **Piece:** the per-role config a ServingRuntime carries — `engineConfig`,
  `decoderConfig`, `routerConfig` (`servingruntime_types.go:217-226`); this OEP
  adds `encoderConfig`. An IR renders exactly one piece, chosen by its role
  (OEP-0026 §5).
- **Edge:** an ordered pair of roles where one sends per-request state to the
  other over a runtime-owned protocol. PD has one edge, prefill→decode (KV); EPD
  adds encoder→prefill (embeddings). A **sender** with no compatible
  **receiver** fails the request; a receiver with no sender only idles.
- **Peer env:** `OME_<PEER>_ENDPOINT` (stable per-role Service) and
  `OME_<PEER>_REVISION_ENDPOINT` (the paired revision's Service), stamped on
  every container when the ISVC declares a rollout group
  (`coordination/discovery.go:14-63`); read once at startup, so a pod's pairing
  is fixed for its life.
- **Pairing token:** `spec.rollout.pairingProtocol`, an opaque string compared
  for equality, empty pairs with anything (`rollout_types.go:72-86`); rides the
  revision hash of every pairing role, stamped on pods and per-revision
  Services; never interpreted by OME.
- **Consumer-first order:** roll the receiver of an edge before its sender and
  drain the sender first — PD's "decoders in first, out last" (OEP-0027 Proposal 1).

## Proposal

### The decision: a fourth fixed role (A) or a list of stages (B)

**(A) Add `encoder` as a fourth fixed role.** API delta:

| Object | Field | Type |
|---|---|---|
| InferenceService | `spec.encoder` | `*EncoderSpec` — the `DecoderSpec` shape (`inference_service.go:330-346`) |
| ServingRuntime | `spec.encoderConfig` | `*EncoderSpec` |
| `ComponentType` | new value `encoder` | closed set becomes four |
| InferenceReplica | `spec.component` accepts `encoder` | picks `encoderConfig` |
| Autoscaler anchor | enum gains `encoder` | `autoscaler.go:136` |
| Rollout groups | `components[]` accepts `encoder`; `MaxItems` 3→4 | `rollout_types.go:101-118` |
| Status | `status.components.encoder`, condition `EncoderReady` | map key + one constant |

A PD manifest is a valid EPD manifest with no encoder; nothing written today
changes meaning.

**(B) Generalize roles to an ordered list of stages.**

```yaml
spec:
  stages:
    - {name: vision,  role: encoder, runner: …}
    - {name: prefill, role: prefill, runner: …}
    - {name: decode,  role: decode,  runner: …}
  router: …
  rollout:
    pairs: [[vision, prefill], [prefill, decode]]   # declared edges
```

Runtime pieces become `pieces: {<role>: …}`; discovery, pairing and rollout
read the declared edges. Every ISVC and runtime in every GitOps repo changes
shape, or a conversion layer carries both shapes indefinitely.

### What each option touches

The investigation found 37 enumeration sites. Grouped:

| Group | Sites (count) | (A) | (B) |
|---|---|---|---|
| Go types: `ComponentType`, per-role spec fields, runtime pieces, conditions, constants, workload mirror, converter | 1–4, 7–10 (9) | one value or field each | replace fields with a list; keep the old fields behind conversion |
| Controller wiring: runtime merge, mode resolution, three `New*` blocks, per-role reconcilers, status switch, ingress entrypoint, autoscaler switches, runtime/accelerator selectors, projector, placement, watch suffix lists, policy-ref lists | 11–24 (14) | one case each; one new or role-parametrized reconciler | same 14, rewritten over a list |
| Coordination and rollout: `ServingPeers`, pair gate, OMENative role collection, primary-preference lists (3 copies), canary units | 25–29 (5) | second edge added | edges read from `pairs` |
| Admission: `validComponents`, pairing-update switch, lockstep switch, per-role shape rules, webhook projections, runtime preset | 30–33 (4) | one arm each | rules rewritten over the list |
| Charts, CI, fixtures, UI (internal) | 34–37 (4) | one name each | new manifest shape everywhere |

Both options touch all 37. Under (A) a touch is one added arm; under (B) a
rewrite plus a compatibility path for three fields every user already has. The
number that matters is manifests in the world, and (B) changes all of them.

**Recommendation: (A) now, with the 37 sites made table-driven** ([The role
table](#the-role-table)). Then (B), if ever wanted, changes the table's source
from a constant to `spec.stages` and leaves the consumers alone.

### Why (B) alone is the wrong first step

1. **It prices a fourth role at the cost of an API migration.** Three fields
   and three pieces exist in every runtime catalog and ISVC. (B) breaks them or
   keeps them as aliases — the enumeration sites we set out to remove, kept
   forever.
2. **It generalizes over one data point.** We have one edge and are adding a
   second; we do not know whether a third is a chain, a fan-in (two encoders
   into one prefill) or a side call (a reranker). The wrong guess is another
   migration.
3. **It hides the real work.** The hard part is the second edge in discovery,
   the pair gate and the rollout order, not the field. (A) does that on a role
   we can test in KIND; (B) on an abstraction no runtime exists for.
4. **It does not remove the switches by itself.** Every consumer is rewritten
   under either option; spend that effort under the one that ships a feature.

The honest cost of (A): a fifth role is another round of the same additions.
The role table bounds that round to the table plus the per-role reconciler.

### User stories

1. **Different hardware per stage** — encoder on a smaller accelerator class via
   its own `acceleratorOverride` (`acceleratorclassselector/selector.go:184-232`).
2. **Independent scaling** — image-heavy traffic scales the encoder to 12 while
   prefill stays at 4, via `spec.encoder.autoscaler` and a `ratios` anchor.
3. **An encoder-only fix** — a new encoder image rolls as a single-role blueGreen
   group; prefill and decode do not re-render. If the embedding format changed,
   the operator bumps `pairingProtocol` and the group must name every pairing
   role ([Admission](#admission)).
4. **Bring your own encoder** — under OEP-0026 phase 3, `spec.encoder:
   {replicaRefs: [vit-h100]}` names a pre-created IR with `component: encoder`.

### Risks

- **A role without a runtime.** The API may fit no implementation. Mitigation:
  `provisional` until one runtime (Open Question 1) runs E+P+D end to end with
  the peer env as its only discovery input.
- **Two edges, one token.** An embedding-format bump re-rolls decode too
  (Open Question 4).
- **Three-way surge.** A `paired` blueGreen over three roles surges all three —
  OEP-0027's double-surge risk, one set larger.

## Design Details

### API

`EncoderSpec` is `DecoderSpec` under a new name, `Leader`/`Worker` included
(whether a multi-pod encoder is ever needed is Open Question 6).

| Field | Type | Validation | Note |
|---|---|---|---|
| `InferenceService.spec.encoder` | `*EncoderSpec` | requires `spec.engine` (mirror of `pkg/validation/isvc.go:77-82`) | inline form; OEP-0026 adds `replicaRefs` |
| `EncoderSpec.PodSpec`, `.ComponentExtensionSpec`, `.Runner`, `.Leader`, `.Worker` | as `DecoderSpec` | leader/worker pairing per role (`isvc.go:94-101`) | |
| `ServingRuntime.spec.encoderConfig` | `*EncoderSpec` | optional | merged by `MergeRuntimeSpecs` (`utils/reconciliation.go:110-140`) |
| `ComponentType` | `encoder` | closed set of four | `inference_service_status.go:266-272` |
| `InferenceReplica.spec.component` | accepts `encoder` | immutable (unchanged) | CRD is a plain string (`ome.io_inferencereplicas.yaml:570`); the webhook validates against the role table |
| `ComponentAutoscaler.ratios.anchor` | enum + `encoder` | | `autoscaler.go:136` |
| `RolloutGroup.components`, `.order` | `MaxItems=4` | | `rollout_types.go:105-118` |
| `spec.rollout.pairingProtocol` | unchanged | projected onto encoder, engine, decoder | `irprojector/projector.go:452-460` |
| Condition `EncoderReady` | `apis.ConditionType` | | beside `DecoderReady` (`inference_service_status.go:277-282`) |

Constants: `constants.Encoder` (`constants.go:374-379`) and
`workloadtypes.ComponentEncoder` (`workload/types/types.go:28-34`). The pod
label stays `component=encoder`: the render hook reads it back to find the
pod's role (`omenative/core/render.go:94`) and an external router selects on
it. Name collision, stated: `BaseModel` metadata has `textEncoder` in the
diffusion pipeline description (`model.go:72-74`); that describes a model,
`spec.encoder` a pod set.

### The role table

The table keeps (A) from being 37 hand edits per role.

```go
// pkg/apis/ome/v1beta1/roles.go
type Role struct {
    Type           ComponentType
    ReadyCondition apis.ConditionType
    Pairs          bool // participates in pairing edges (encoder, engine, decoder)
    Entrypoint     int  // "primary" preference: router 0, engine 1, decoder 2, encoder 3
    MultiPod       bool // may carry Leader/Worker
}
var Roles = []Role{ /* router, engine, decoder, encoder */ }
var Edges = [][2]ComponentType{{EncoderComponent, EngineComponent}, {EngineComponent, DecoderComponent}}
```

Plus two controller-side accessors, `SpecOf(isvc, ComponentType)` and
`PieceOf(runtime, ComponentType)`, returning a view (`PodSpec`,
`ComponentExtensionSpec`, `Runner`, `Leader`, `Worker`, `Labels`,
`Annotations`). Those two functions are the **only** place the four fields are
named. Consumers that collapse onto them, by investigation row: 13, 14, 16, 18,
19, 20, 21, 22, 23, 24, 25, 27, 28 (three copies), 30, 31, 32 — every switch and
literal list in the controller, coordination and admission groups.

What the table cannot remove: the Go fields (rows 2, 3, 7) — under (A) a new
role is a new field — and the per-role reconciler (row 15). `decoder.go` is
already `engine.go` minus the model-format input (`components/decoder.go:36-56`),
so I recommend one `stage.go` parametrized by role serving decoder and encoder,
leaving `engine.go` and `router.go` alone. The kubebuilder enum marker (row 5)
is static text: edit it per role, or drop it for webhook validation against
`Roles`. I lean to dropping it — an enum and a table that can drift are two
sources of truth.

Lock: a unit test that greps `pkg/` for `case v1beta1.EngineComponent` outside
`roles.go`, `SpecOf` and `PieceOf` and fails on any hit.

### Data path

Two shapes exist in public runtime designs; neither is in this tree, so this is
a contract, not an observation.

1. **Router-orchestrated:** router → encoder → prefill → decode. The router
   sends the multimodal input to an encoder, gets embeddings or a handle, and
   sends the request with it to a prefill, which continues as PD.
2. **Prefill-orchestrated:** router → prefill; the prefill calls an encoder with
   the raw input, gets embeddings, continues as PD.

The OEP supports both by giving every side the other's address and choosing
nothing: encoder pods get `OME_ENGINE_*` and `OME_ROUTER_*`; engine pods get
`OME_ENCODER_*` and `OME_DECODER_*`; router pods get all three. That is what
`ServingPeers` produces once `encoder` is a declared role
(`discovery.go:152-173`), and the `component=encoder` label lets a router that
discovers by selector — the only router this tree has seen
(`charts/ome-isvc/templates/vendors/cks-gb300-sglang-pd.yaml:281-292`) — list
encoders the same way. Which side calls the encoder (Open Question 2) and how
the bytes move (Open Question 1) are the runtime's. OME provides an endpoint
pair per edge and a token; more only when a runtime asks for a named thing.

### Discovery and pairing

**Discovery.** `ServingPeers` iterates `Roles` and returns every declared role
but the caller's. Peer revision resolution (`inferencereplica/peer_revisions.go:66-104`)
iterates `ServingPeers`, so the encoder→prefill edge gets per-revision pairing
for free: an engine pod minted for its roll target names the encoder's roll
target in `OME_ENCODER_REVISION_ENDPOINT`; an older engine pod names the encoder
revision current when it rendered. The encoder's per-revision Services are
created before the engine pod renders (`:129-156`) and carry the
`ome.io/pairing-protocol` label (`coordination/services.go:277-282`). The render
hold waits on three peers' projections instead of two: one more projector pass
on first render, no change to the rule.

**Pairing token.** One token for the chain, stamped on every role with
`Pairs: true` (`projector.go:456` becomes a table lookup). Cost: an
embedding-format bump re-mints decode revisions too. A per-edge token only
widens the field later (Open Question 4).

**Pair gate as a chain.** `CheckPairing` (`pairing.go:117-236`) hard-codes
engine×decoder. It becomes per-edge: for each edge in `Edges` that names
`ctx.Component` and whose both ends the group spans, run the existing simulation
with that edge's peer; deny if any edge would lose its last pairable serving
pair (`pairableServingPairExists` takes the two roles as arguments). The three
liveness escapes — no pair to protect, the 1×1 wall, the last hop — are
evaluated per edge; an escape on one edge does not excuse a denial on the other.
E+P without D has one edge and behaves as PD does.

**OEP-0027 `paired` over three roles.** Admission accepts any contiguous run of
the declared chain: `[engine, decoder]`, `[encoder, engine]`,
`[encoder, engine, decoder]`. The cohort is `digest(pairingProtocol, <every
member's target hash>)` in chain order, projected as `IR.spec.pairingProtocol`
on each member. The two denials become the consumer-first rule per edge: (a)
deny a sender step onto the new cohort while no new-cohort receiver serves on
that edge; (b) deny a receiver step that leaves the old cohort with no serving
receiver while any old-cohort sender serves. The pair Service
`<isvc>-<peer>-pair-<cohort>` exists per member; `OME_<PEER>_REVISION_ENDPOINT`
names it.

### Rollout

**Groups may name `encoder`** (`validComponents` reads `Roles`); a single-role
blueGreen group `[encoder]` rolls the encoder alone (user story 3).

**Order: decode in first, then prefill, then encoder; encoder out first, then
prefill, then decode.** From the data path: on the prefill→decode edge a
new-cohort prefill with no new-cohort decoder fails every request it takes — it
has KV to hand off and no one to take it — while a new-cohort decoder beside old
prefills only idles. On the encoder→prefill edge the encoder is the sender: a
new-format encoder with no prefill that reads the format fails the request; a
format-ready prefill beside old encoders idles. At each edge the receiver goes
first, and the chain order follows. The internal chart's PD default,
`order: [decode, prefill]` (`charts/ome-isvc/examples/values-cks-gb300-sglang-pd.yaml:106-112`),
is this rule for one edge; EPD extends it to `[decode, prefill, encode]`. Under
OEP-0017 the operator writes single-role blueGreen groups in that order and the
controller collapses the run to Sequential (`coordination/groups.go:67-95`).

**Primary.** The role a canary's step machine runs through becomes router >
engine > decoder > encoder, read from `Roles[].Entrypoint` (three copies today:
`pkg/rollout/rollout.go:82`, `rolloutrun.go:514`, `observe.go:122-126`); the
encoder is last because it is never the external entrypoint.

**Canary unit.** OEP-0017.2's engine unit, "engine plus decoder when declared",
becomes "engine plus every declared pairing role"; the rule at
`pkg/validation/canary.go:131-140` ("names either, must name both") becomes
"names any pairing role, must name all". `maintainRatio` already keys on
`ComponentType` (`coordination_types.go:311-321`); a three-role group keeps
three ratios.

### Status

- `status.components.encoder` appears when the role is declared; the map is
  keyed by `ComponentType` (`inference_service_status.go:37`). Per-role counters
  (`Lifecycle.Replicas`, `ReadyReplicas`, `ServingReplicas`), `RolloutPhase`,
  `LatestReadyRevision` and `Traffic[]` (with `pairingProtocol`,
  `traffic.go:43,95`) come from the same per-role paths.
- `EncoderReady` is derived by `TopLevelComponentReadyFromLifecycle` via the
  table (`status/top_level_condition.go:19-27`). The aggregate `Ready` depends
  on `IngressReady` and `EngineReady` only (`inference_service_status.go:433-436`),
  so an ISVC is `Ready` with every decoder down; `EncoderReady` follows
  `DecoderReady` (Open Question 5).

### Admission

| Shape | Allowed | Why |
|---|---|---|
| P, P+D, each ± router | yes | today's shapes, unchanged |
| E+P ± router | **yes** | a multimodal model without PD still wants the encoder apart; one edge, the gate works |
| E+P+D ± router | **yes** | the full chain |
| E alone | no | an encoder produces embeddings for a prefill; with none it serves nothing — "encoder cannot be specified without engine", the mirror of `isvc.go:77-82` |
| E+D | no | falls out of "decoder cannot be specified without engine" |

Rules that widen: the OMENative mode-match check (`isvc.go:275-296`) requires
every declared pairing role to resolve to one mode when any does;
`ValidatePairingProtocolUpdate` (`pkg/validation/coordination.go:135-165`)
requires the blueGreen or canary group to name every declared pairing role;
leader/worker and lifecycle rules run per role from the table. A runtime named
on the ISVC need not carry `encoderConfig` — inline `spec.encoder` is a complete
template, as inline `spec.decoder` is today — but an IR with `runtimeRef` must.

### InferenceReplica public API

Under OEP-0026 an encoder is one more referenced pod set. `spec.component:
encoder` selects `encoderConfig`; a runtime without that piece is rejected at
admission when readable, else `Ready=False` with `RuntimePieceMissing`
(OEP-0026 API rule 2). `spec.pairingProtocol` is a rollout-control field on
every pairing role, so the composer stamps it on encoder IRs. The referenced
form is `spec.encoder: {replicaRefs: […]}`, validated as decoder's is.

### Samples and KIND coverage

- **Samples (internal, never upstreamed):** the `ome-isvc` chart gains
  `servingMode: epd` with an `encode:` block mapping to `encoderConfig`
  (`charts/ome-isvc/templates/_helpers.tpl:71-82`, `servingruntime.yaml:42-46`,
  `inferenceservice.yaml:28-41`) and `examples/values-<vendor>-epd.yaml`; the
  vendor profile is Open Question 1. `config/samples/epd/` carries a CSR + ISVC
  pair once a runtime exists.
- **KIND, one topology spec per progression,** in the coordination suite with
  the fake-serving image (an encoder is a pod with a readiness endpoint):
  `epd_kind_test.go` (E+P+D and E+P converge; engine pods carry `OME_ENCODER_*`
  and `OME_DECODER_*`, encoder pods `OME_ENGINE_*`; per-revision Services for
  all three); `epd_bluegreen_kind_test.go` (three single-role groups in
  `[decoder, engine, encoder]` order roll sequentially; an `[encoder]`-only
  group re-renders nothing else); `epd_rolling_kind_test.go` (a three-role
  rollingUpdate group holds three ratios); `epd_canary_kind_test.go` (the
  three-role unit stages capacity on all three before traffic shifts;
  `[encoder, engine]` on an E+P+D ISVC is rejected); `epd_pairing_kind_test.go`
  (a `pairingProtocol` bump keeps a pairable pair on both edges; with `paired`,
  no new-cohort engine serves before a new-cohort decoder and no new-cohort
  encoder before a new-cohort engine). Fixture knobs `WithEncoder()`,
  `WithEncoderImage`, `WithEncoderReplicas` (`fixtures_test.go:107-260`); a new
  `epd-topology` group in `KIND_SPEC_GROUPS` (`.buildkite/generate_pipeline.py:118-121`).

## Test Plan

- **Unit.** Role-table completeness; the no-stray-switch grep test;
  `ServingPeers` for the five shapes; `CheckPairing` per edge, including an
  escape on one edge with a denial on the other; primary preference with
  encoder; `MergeRuntimeSpecs` with and without `encoderConfig`.
- **Envtest webhook** (`tests/integration/singlecluster/webhook/inferenceservice`):
  the shape table; a `pairingProtocol` change with a group missing the encoder;
  a partial canary unit; IR `component: encoder` against a runtime without the piece.
- **KIND.** The five specs above; `make test-kind GINKGO_FOCUS='EPD'`.
- **Replay goldens (OEP-0020.2).** The instance table does not change: the
  encoder is one more owner of the same rows and the gate emits the same
  `gate.deny[Pairing]`. A golden that moves means this OEP misjudged its scope.
- **Perf.** `tests/perf/omenative` at 4+4+4: the render hold costs one extra
  projector pass, not a chain of them.

## Graduation Criteria

**Alpha:** `spec.encoder`, `encoderConfig`, `encoder` in `ComponentType`, the
role table and the grep lock merged, all 37 sites reading the table or the two
accessors; `epd_kind_test.go` and `epd_bluegreen_kind_test.go` green in CI; one
runtime (Open Question 1) runs E+P+D end to end on a dev cluster with a real
encoder and the peer env as its only discovery input.

**Beta:** per-edge pair gate and OEP-0027 `paired` over three roles with
`epd_pairing_kind_test.go`; OEP-0026 referenced form for `encoder`; chart
`servingMode: epd` used by at least one internal deployment.

**Stable:** TBD — after two runtimes, and after Open Question 4 is decided.

## Drawbacks

- One more role, field, and condition; every future role pays again unless the
  table holds.
- One token for two edges couples the decode roll to encoder wire changes.
- Nothing runs this yet; a role with no runtime invites manifests that set it
  and expect something to happen.

## Alternatives

1. **Encoder as a sidecar in the prefill pod.** No API change. Rejected: it is
   today's failure mode with a container boundary added — same accelerator class
   (a pod schedules on one node), same replica count, same rollout. Users want
   the encoder apart for different hardware, independent scaling and sharing
   across models; a sidecar gives none of them.
2. **(B), the stage list, now.** The right second step, the wrong first one.
3. **Encoder as a separate InferenceService.** An engine-only ISVC serving an
   embeddings API, its URL handed to prefill in `spec.engine.runner.env`. Works
   today and answers sharing across ISVCs, but loses per-revision pairing, the
   pair gate and the paired rollout. Keep as the sharing bridge (Open Question
   3), not as the design.
4. **A generic `peers[]` field** per role letting the user draw the edges.
   Rejected for now: edges belong to the runtime's data path, and a user-drawn
   edge the runtime does not implement is a silent no-op.

## Open Questions

1. **Which runtime first, and who owns the embedding transfer protocol?** No
   runtime in this tree speaks EPD; public designs (a disaggregated encode mode
   in SGLang, encoder-separated serving discussed for vLLM) differ in transfer
   backend. The OEP treats the protocol as opaque; alpha needs a name. Owner: TBD.
2. **Router or prefill calls the encoder?** Both work through the peer env. A
   router-orchestrated runtime needs the external router image to learn the
   `encoder` label, outside this repo.
3. **Encoder sharing across InferenceServices.** OEP-0026's "at most one
   composer" rule forbids two ISVCs naming one encoder IR; Alternative 3 is the
   bridge. Decide after the first runtime.
4. **One pairing token or one per edge?** One matches PD; per edge avoids
   re-rolling decode on an embedding-format change. Field shape if per edge
   (`pairingProtocols: {encoderPrefill, prefillDecode}` or a list keyed by
   edge): TBD.
5. **Should pairing roles gate `Ready`?** `DecoderReady` does not today
   (`inference_service_status.go:433-436`). Decoder and encoder together, or
   neither.
6. **Multi-pod encoders.** `EncoderSpec` carries `Leader`/`Worker` for symmetry;
   if no encoder needs them, drop them before beta.

See also: OEP-0007 (pod sets, peer DNS), OEP-0007.1 (per-revision Services,
peer env), OEP-0017 (groups), OEP-0017.2 (the pair as the canary unit),
OEP-0026 (roles and pieces on the public IR), OEP-0027 Proposal 1 (the paired
cohort extended here to a chain). OMENative only; RawDeployment and MultiNode
are unaffected.
