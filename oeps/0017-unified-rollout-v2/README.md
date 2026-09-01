# OEP-0017: Unified InferenceService Rollout v2

**Tl;dr:** Today an InferenceService rollout is split across two mutually-exclusive
engines — `spec.rollout.coordination` (how Components move together) and
`spec.rollout.canary` (how traffic shifts) — so you can sequence Components *or*
canary, never both. This OEP collapses them into one model: an **ordered list of
rollout groups**, where each group is **polymorphic in its progression**
(`blueGreen | rollingUpdate | canary`) and traffic becomes a **service-level**
concern. The axes the current spec uses are right; the defect is that two of them
are welded to rival engines. The design was captured provisionally; the
[implementation triggers](#status-and-decision) have since fired, so it is now
**implementable and being built** — a breaking spec change with **no migration
shim** (justified below).

**Status:** implementable. The design was captured provisionally after shipping the
canary executor (OEP-0015.1); the implementation triggers below have
since fired, and the spec + engine rewiring is underway.

---

## Status and decision

This began as a provisional design with a single recommendation: **write it down,
build it when the triggers fire — not because we just designed it.** Those triggers
have since fired, so the recommendation is now a **decision: build v2.** The
reasoning that made the *shape* worth committing to still holds:

- **The shape fixes a real wart.** The old surface bundled *sequencing* and
  *progression* into one `policy` enum and split traffic across two engines —
  confusing for operators and maintainers alike (detailed below).
- **The missing capability is now wanted.** The one thing the old spec couldn't
  express — "sequence the Components *and* canary each one" — is the combination
  driving this work.
- **Fixing an API is cheapest at near-zero adoption — which is still now.** Every
  coordination config in-tree is a single `[engine, decoder]` group (see
  [Evidence](#evidence-the-multi-group-axis-is-unused)), so the break is cheap
  today and only gets costlier as adoption grows.

The cost is real and unchanged: v2 supersedes both `coordination` (already the
default) and `canary` (OEP-0015.1), and still needs a unified status, a merged
reconciler, and a full KIND re-test. It does **not** need a migration shim — the
legacy fields are removed outright (see [Conversion / migration](#conversion--migration)),
because near-zero adoption makes a one-release deprecation window more machinery than
the risk warrants.

---

## Summary

`spec.rollout` already separates rollout into the correct axes — they are just
distributed across two engines and one per-Component field, with overlapping
knobs. v2 keeps the axes and removes the welds:

- **Sequencing** (which Components roll, in what order) becomes the **position in
  an ordered `groups[]` list** — not a `Sequential` policy value.
- **Progression** (how a revision and its traffic advance) becomes a **per-group
  one-of**: `blueGreen | rollingUpdate | canary`. `canary` stops being a
  top-level sibling and becomes one progression a group can choose.
- **Traffic** becomes **service-level** (per-InferenceService), not per-Component.
- **Per-pod replacement** (`spec.<comp>.lifecycle.updateStrategy`) is **unchanged**
  — it is the layer beneath, and v2 must not duplicate it.
- The two reconcilers (coordination + canary) **merge** into one engine, removing
  the mutual-exclusion rule that exists only because two engines would otherwise
  race the same `RollingUpdate.Partition` (the per-pod staging field) and the same
  per-Component `status...traffic[]` writes.

---

## Motivation

### The failure mode (what breaks today)

`spec.rollout.coordination` and `spec.rollout.canary` are **mutually exclusive**
(`pkg/validation/canary.go`, in `ValidateCanary`): an InferenceService rolls by
one or the other, never both. (Code anchors in this doc are by file + symbol; any
bare line numbers are as of 2026-06-07 and will drift.) So a PD service that wants to "roll `[router, decoder, engine]`
in sequence, *and* canary each Component's traffic 10→50→100" **cannot be
expressed** — sequencing lives in `coordination` and progressive traffic lives in
`canary`, and you may not have both. That is the concrete capability gap.

The mutual exclusion is not conceptual; it is an **implementation artifact**. Both
reconcilers stamp the same `RollingUpdate.Partition` (per-pod staging) and both
write the same per-Component `status.components[c].traffic[]` (confirmed:
`canary/traffic.go` `applyTraffic()` and `coordination/reconciler.go`
`updateTrafficStatus()`, which assigns `cs.Traffic`). Running both at once would
race those writes — so the spec forbids declaring both. The user pays (a missing feature) for
an engine-layering decision.

Four smaller defects compound it:

1. **"BlueGreen" means two things.** It is a `CoordinationPolicy` (gang flip,
   const `CoordinationPolicyBlueGreen`) *and*, at the per-Component layer,
   `updateStrategy: SurgeThenDrain` is already a per-Component blue-green (const
   `UpdateStrategySurgeThenDrain`).
   One word, two layers — the wart OEP-0015 named and left in place
   (OEP-0015, "Today's gaps": `"BlueGreen" means two things`).
2. **`CoordinationPolicy` bundles two questions.** `BlueGreen|RollingUpdate|
   Sequential|Independent` encodes *both* sequencing (together vs one-at-a-time)
   *and* progression (flip vs rolling). Because progression is welded into the
   policy enum, canary — which *is* a progression — has nowhere to attach, so it
   had to become a separate engine.
3. **Surge is configurable in three places.** `lifecycle.updateStrategy.rollingUpdate.maxSurge`
   (per-Component), `coordination.pacing.maxSurge` (group ceiling), and canary's
   `steps[].newCapacity` all move pods up. There is a documented "group caps
   Component" rule (the `RollingUpdate.MaxSurge` doc: "the group-wide ceiling caps
   the per-Component MaxSurge"), but it is three knobs for one
   concept.
4. **Ratio-keeping exists twice.** `coordination.pacing: RatioBalanced` (during a
   roll) and `spec.scalingPolicy: Proportional` (steady state) both hold
   cross-Component replica ratios.

And one scope error: **traffic is modeled per-Component.** `status.components[c].traffic[]`
is a per-Component array, written by both engines. But a request traverses one
coherent pipeline (router→engine→decoder), so "10% of engine traffic but 50% of
decoder traffic" has no meaning — which weight would the service even advertise?
Traffic is a **service-level** quantity (one number: the fraction of requests on
the new revision) that the spec currently, wrongly, splits per-Component.

### Goals

- One model for InferenceService rollout: an ordered list of groups, each with a
  single progression. No `coordination`-vs-`canary` fork.
- Make `sequence + canary` (and any other relationship × progression combination)
  expressible.
- Lift traffic to service-level; collapse the duplicated surge and ratio surfaces.
- Preserve every behavior the current spec can express (no capability regression
  — see the [mapping](#how-todays-capabilities-map-to-v2)).
- Leave the per-Component `lifecycle.updateStrategy` layer untouched.

### Non-Goals

- **A migration shim for the legacy fields.** v2 removes
  `spec.rollout.{coordination,canary}` outright with no conversion path (see
  [Conversion / migration](#conversion--migration)); the break is intentional and,
  at near-zero adoption, cheap.
- **Internal weighted routing** (router → weighted split across engine
  revisions). v2 keeps the current mechanism where the realized split is driven by
  pod **capacity**; a true internal traffic plane is a separate, larger effort
  (out of scope, noted in [Open questions](#open-questions--next-steps)).
- **Metric-gated promotion / auto-rollback.** Orthogonal; a future increment.
- Changing `spec.scalingPolicy` (steady-state scaling) beyond noting its overlap
  with `RatioBalanced`.

---

## Terminology

- **Component** : one of `router | engine | decoder` — a member of the serving
  pipeline. *Consequence:* Components are coupled (a request flows through them in
  order), not independent services.
- **Instance** : an OMENative per-replica unit (one or more pods) managed via an
  `InferenceReplica` (IR). Rollout staging operates on Instances.
- **OMENative / IR-managed** : the deployment mode where the ISVC controller
  drives pods through an `InferenceReplica` + `ControllerRevision`, enabling
  per-revision pod selection and partition staging. *Consequence:* all rollout
  machinery here is OMENative-only; RawDeployment is unaffected.
- **Progression** (new term) : *how a revision and its traffic advance* — the
  answer to "does the new revision flip in atomically, roll gradually, or step
  through capacity/traffic checkpoints?" The three progressions are `blueGreen`,
  `rollingUpdate`, and `canary`. *Consequence:* progressions are mutually
  exclusive — a group sets **at most one**, and omitting all of them defaults to
  `blueGreen` (so a group that just lists its components is a blueGreen group).
- **Group** : a set of Components that roll **together** as one coupled unit, with
  one progression. *Consequence:* Components in the same group share a progression
  and surge/ratio budget; Components that must roll *separately* go in *different*
  groups.
- **Sequencing** : the order in which groups roll, expressed by **position in the
  `groups[]` list**, not a flag. *Consequence:* group N completes before group N+1
  begins; the last group has no `soak` (nothing follows it).
- **Per-pod swap** : `spec.<comp>.lifecycle.updateStrategy`
  (`SurgeThenDrain|RecreatePod|InPlace*` + `rollingUpdate.{partition,maxSurge,maxUnavailable}`).
  The layer *beneath* progression: how an individual Component's pods are
  physically replaced once the rollout decides to move it.
- **Service-level traffic** : a single weight per step = the fraction of
  InferenceService requests served by the new revision. *Consequence:* not a
  per-Component knob.

---

## Evidence: the multi-group axis is unused

The v2 model makes `groups[]` an **ordered** list (today it is an unordered set of
independent, parallel groups). That is safe only if nobody relies on
parallel-independent groups. They do not:

- Every coordination block in the sample manifests and rollout how-to examples
  declares **exactly one group**, always `[engine, decoder]`, and the
  permutation-test harness's case generator only ever emits a single group.
- `Independent` policy appears only in CRD enums and one doc example — never as
  live parallel orchestration. It is documented as "documentation only" (const
  `CoordinationPolicyIndependent`).
- Conceptually, Components are a coupled serving chain, not independent services
  you would roll on separate schedules.

So making `groups[]` ordered removes a capability **nothing uses**, and the
"sequence = list position" model is what deletes the awkward `Sequential` policy.
*This is an observation of current usage, not a guarantee* — if a future topology
genuinely needs concurrent independent group tracks, that is the one thing this
change forecloses (revisit via the [alternative](#alternatives) on parallel groups).

---

## Proposal

### The diagnosis (why the shape, not just the fields)

The current axes are correct: per-pod swap (axis 1), cross-Component relationship
(axis 2), progressive traffic (axis 3), steady-state scaling (axis 4). The defect
is **not** the axes — it is that axis 2 (`policy`) bundles *sequencing* with
*progression*, which (a) forces canary into a second engine and (b) makes
"BlueGreen" mean two things. Unbundle `policy` into *sequencing* (the list order)
and *progression* (a per-group one-of), and canary becomes "just another
progression," composing with any sequencing.

Concretely, the bundling and the fix:

```yaml
# v1 — sequencing AND progression fused in one enum value:
rollout:
  coordination:
    groups:
      - { components: [decoder, engine], policy: Sequential, order: [decoder, engine] }

# v2 — sequencing = list position; progression = per-group one-of:
rollout:
  groups:
    - { components: [decoder] }   # progression omitted → defaults to blueGreen
    - { components: [engine]  }
```

### The model

A group **composes** its lifecycle (`components`, `soak`, surge/ratio budget) and
is **polymorphic** in its `progression` (at most one of `canary|blueGreen|rollingUpdate`;
omitting it defaults to `blueGreen`).
Composition where things genuinely combine; polymorphism where they are
mutually-exclusive named behaviors. This mirrors Argo Rollouts, where
`spec.strategy` is a one-of union (`strategy.canary` xor `strategy.blueGreen`) —
the dominant prior art for progressive delivery — applied **per group** rather
than per Rollout, because an ISVC has multiple Components and Argo has one
workload.

```yaml
spec:
  rollout:
    groups:                                  # ORDERED list: position = sequence
      - components: [router]                 # group 1 rolls first
        canary:                              # one-of: canary | blueGreen | rollingUpdate
          steps:
            - { capacity: 25%,  traffic: 10 }   # capacity = pods (per-Component);
            - { capacity: 50%,  traffic: 50, pause: { duration: 10m } }
            - { capacity: 100%, traffic: 100 }  # traffic = SERVICE-level weight
          promotion: Manual                  # Manual | Auto
        soak: 5m                             # wait before the next group (inter-group only)
      - components: [engine, decoder]        # group 2: roll together (one coupled unit)
        blueGreen: {}                        # different progression per group is fine
        maintainRatio: { tolerance: 5 }      # cross-Component ratio guard (multi-Component groups)
```

Read top-to-bottom, it *is* the timeline: "canary the router 25→50→100 with manual
gates, soak 5m, then blue-green engine+decoder together keeping their ratio within
5%." No cross-product to recombine in your head — the cause of the legibility
complaint that motivated this in the first place.

What stays out of the group, by design:

- **Per-pod swap** remains `spec.<comp>.lifecycle.updateStrategy` — untouched. A
  group's progression decides *when/how-much* a Component moves; `updateStrategy`
  decides *how each pod is physically replaced*. Keeping them separate is why v2
  does not re-introduce `maxSurge`/`maxUnavailable` at the group level except as a
  budget ceiling.
- **`spec.traffic`** stays steady-state load-balancing (LB algorithm, consistent
  hash). The rollout's traffic *progression* lives in the group's `canary.steps`,
  not in `spec.traffic` — re-overloading `spec.traffic` is the exact mistake
  OEP-0015 undid.

### Capacity vs traffic stays decoupled (and that is deliberate)

`capacity` (pods, via partition) and `traffic` (service-level weight) are
**independent** per step. This is OME-specific — Argo couples them into one
`setWeight`, K8s has no traffic dimension, Knative has no capacity dimension — and
it is worth keeping: it expresses "capacity ahead of traffic" (run 50% of pods,
send 10% of traffic) for warm-up/validation at scale. *Caveat, stated plainly:*
on a vanilla cluster *today* the realized split is **determined by capacity, not
by the `traffic` weight** — more new-revision pods behind one Service catch more
round-robined requests. `traffic` is the weight a future weighted-routing consumer
*would* honor; nothing in-tree enforces it yet. v2 does **not** fix that (see
[Non-Goals](#non-goals)). This matters: an operator reading `traffic: 10` may
expect 90% of requests withheld from the new revision; they will see 100%
dispatched, just to fewer pods.

---

## Design details

### How today's capabilities map to v2

Every current capability maps; the gap-check found no expressibility loss
(deferrals are called out below).

| Today | v2 expression |
|---|---|
| `policy: BlueGreen` on `[e,d]` | one group `[engine,decoder]`, `blueGreen: {}` |
| `policy: RollingUpdate` | one group, `rollingUpdate: { maxSurge, maxUnavailable }` |
| `policy: Sequential`, `order: [d,e]` | **two ordered groups** `[decoder]` then `[engine]` (`soak` on the earlier one) |
| `policy: Independent` | omit the group (ungrouped Components roll independently) — see deferral |
| `pacing: RatioBalanced { ratioTolerancePercent }` | `maintainRatio: { tolerance }` on a multi-Component group |
| `spec.rollout.canary { steps, promotion }` | a group with `canary: { steps, promotion }` |
| `scaleDownDelaySeconds` | per-group field (drain window) |
| canary rollback (`RolledBackRevisionHash`/`RollbackToRevision`) | unchanged mechanism; canary-progression-only in v2-alpha (deferral) |
| per-Component `lifecycle.updateStrategy` | unchanged |

The **only new** thing is the combination the current API forbade:
`Sequential × canary` = three ordered single-Component groups, each with
`canary: {...}`.

### Naming (grounded in prior art; still open)

Prior-art survey (Argo Rollouts, Flagger, K8s Deployment, Knative):

- **Progression union** : presence-based one-of `canary | blueGreen | rollingUpdate`
  per group (Argo's `strategy.canary` xor `strategy.blueGreen` shape). No `type:`
  discriminator — it avoids the "set `type: Canary` but fill `rollingUpdate`"
  dead-config footgun; "exactly one set" is a webhook rule.
- **`steps[]`, `capacity`, `traffic`** : keep `steps[]` (Argo convention); keep
  capacity and traffic as independent fields (OME extension).
- **Ratio guard** : `maintainRatio { tolerance }` reads plainly; today's
  `pacing.ratioBalanced.ratioTolerancePercent` is the existing spelling.

*Open, not settled:* (a) the ordered list — `groups` (keep) vs `stages`/`waves`
(reads more "ordered"); (b) `maintainRatio` vs `ratioBalanced`. The author and a
reviewer disagreed on whether renaming `groups` is worth the churn; recorded as an
open question, not decided.

### Status v2

Today rollout status is split across `status.rolloutCoordination.groups[]`,
`status.canary`, and `status.components[c].{traffic[],rolloutPhase}` — written by
two engines. v2 unifies it under `status.rollout` (one engine writes it):

```
status.rollout:
  groups: [{ name, components, progression: { phase, canaryStep?, canaryWeight? }, ... }]
  traffic: [ ... ]            # service-level view
  phase: <unified enum>       # e.g. Idle | Rolling | Paused | Failed | RolledBack
```

Until `status.rollout` exists, the alpha keeps writing the existing
`status.components[c].{traffic[],rolloutPhase}` and `status.canary` /
`status.rolloutCoordination` blocks, so the HTTPRoute builder and any dashboards keep
working unchanged. *This is a sketch.* The v1→v2 phase-enum mapping is **TBD** — you
cannot write the merged reconciler (or the unified `status.rollout`) without it — and
is on the order of the OEP-0015 status work, not a quick rename.

### Conversion / migration

**Decision: no conversion path.** v2 removes `spec.rollout.{coordination,canary}`
from the schema outright; a stored ISVC carrying a legacy rollout block has it
silently pruned on the next write, and the operator re-expresses the rollout as
`spec.rollout.groups[]`. We take that break deliberately.

The question that would otherwise force a shim — *is there live production config
that must survive a cluster upgrade?* — is answered **no**: every coordination block
in-tree is a single `[engine, decoder]` group (see
[Evidence](#evidence-the-multi-group-axis-is-unused)), and per the OEP-0015 premise
OME has effectively no external API consumers. So the two paths we considered both
lose to doing nothing:

a) **Admission-time defaulter + one-release window** (translate legacy →
   `groups[]`, warn, then remove). *Tradeoff:* real code plus a deprecation window
   to protect config that, as far as we can tell, no cluster stores.
b) **CRD conversion webhook.** *Tradeoff:* storage-version machinery for durable
   old-schema reads we don't need.

Neither earns its keep at near-zero adoption. **Caveat, stated plainly:** if a
cluster *does* hold a legacy `coordination`/`canary` block, that rollout config is
lost without warning on upgrade — the mitigation is to land v2 before adoption grows,
not to build a shim. This is the same bet OEP-0015 made when it relocated
coordination at admission level.

### Deferrals and known gaps (v2-alpha)

Stated as deferrals, not solutions:

1. **`Independent` policy has no progression.** It was always "no coordination,"
   not a strategy. *Deferral:* deprecate it — ungrouped Components roll
   independently; forbid a multi-Component group with no progression.
2. **Canary on a multi-Component group is primary-driven, not lockstep.** The
   primary Component (router > engine > decoder) carries the traffic weight; the
   other Components gate on capacity (the secondary-capacity gate) but do not run
   their own step machine. True lockstep multi-Component canary has no defined
   semantics and stays out of scope; the primary-driven form is what ships, and
   admission requires every canary Component to be declared and OMENative.
3. **Rollback is canary-specific.** `rollingUpdate`/`blueGreen` revert by removing
   the trigger, not via the sticky-reject mechanism. *Deferral:* keep rollback
   canary-only in v2-alpha.
4. **Service-level traffic vs per-Component data plane.** The data plane still
   reads `status.components[c].traffic[]`; v2 *derives* those per-Component entries
   from the group's service-level weight (group at 50% → each member's
   `traffic[]` shows 50%). *No data-plane change required*, but the producer logic
   moves from per-Component to derived-from-group.

---

## Alternatives

a) **Keep `coordination` XOR `canary` (do nothing).** — *Tradeoff:* preserves the
   capability gap (`sequence + canary` stays impossible) and the two-engine race
   risk; the conceptual wart persists. Cheapest now, costliest later as adoption
   grows.

b) **Surgically let `canary` compose with `Sequential` only.** Add a narrow path
   so a Sequential group can also canary, without the full model. — *Tradeoff:*
   smaller change, but leaves two engines, duplicated surge/ratio, and the
   `policy` bundling intact; solves one combination, not the class.

c) **Two orthogonal fields that compose: keep `coordination` (groups/order) and
   `canary` (progression) as separate top-level blocks layered together.** (A
   reviewer favored this — "they are orthogonal axes.") — *Tradeoff:* they are
   *not* fully orthogonal: `BlueGreen`/`RollingUpdate` are themselves
   *progressions*, so a separate `canary` block still collides with the
   progression baked into `policy`. You would have to first strip progression out
   of `policy` anyway — at which point (d) is cleaner.

d) **Ordered groups, per-group progression one-of (this proposal).** —
   *Tradeoff:* a breaking spec change (no migration shim — see
   [Conversion / migration](#conversion--migration)) plus a merged reconciler;
   forecloses parallel-independent groups (unused today). Cleanest model; canary
   composes for free.

e) **`type × ordering` two-axis split** (`type: RollingUpdate|Canary|BlueGreen`
   × `ordering: Parallel|Sequential`). — *Tradeoff:* OEP-0015 rejected this as
   "illusory factoring" because it treated `BlueGreen` as both a type and an
   ordering. v2 (d) avoids that trap: ordering is the **list structure**, not an
   enum, and `BlueGreen` is purely a progression — so the contradiction
   (`BlueGreen + Sequential`) cannot be written.

I choose **(d)**, because it removes the engine weld (not just one combination),
puts ordering where ordering belongs (a list), and matches the Argo one-of-union
prior art at the group level. (b) is the fallback if only `sequence + canary` is
ever wanted.

---

## Anticipated objections

**Q. Why redesign a feature (canary) that shipped only recently?**
A. Because API shape is cheapest to fix at near-zero adoption, and that is now. The
redesign was captured as a target the moment canary landed — to stop accreting
features around the bundled `policy`/two-engine shape — and held until canary had
real use. That bar is met, so we build it while the break is still cheap, not after
adoption makes it expensive.

**Q. Isn't this the `type × ordering` split OEP-0015 already rejected?**
A. No. OEP-0015 rejected encoding ordering as an *enum axis* crossed with a *type
axis*, because `BlueGreen` was both. Here ordering is the **position in the
`groups[]` list** (structure, not enum), and `BlueGreen` is *only* a progression.
The combinations OEP-0015 called contradictory (`BlueGreen + Sequential`) are
simply unwriteable — you would put the Components in separate ordered groups, each
with its own progression.

**Q. Coordination (who rolls together) and canary (how traffic shifts) are
orthogonal — why fuse them into one `progression`?** (the reviewer's objection)
A. The *orthogonal* part — sequencing — we keep, as the group list order. What we
fuse is only the **progressions**: `blueGreen`, `rollingUpdate`, and `canary` are
rival answers to one question ("how does the revision + traffic advance?"). You
cannot atomically flip *and* step traffic 10→50→100 on the same group. Treating
them as a one-of is the honest model; the current spec hides that they compete by
putting two of them in `policy` and the third in a separate engine.

**Q. Does service-level traffic actually move requests, or is it still capacity?**
A. Still capacity, today — same as the current canary. v2 fixes the *spec* scoping
(traffic is one service-level number, not per-Component) but does **not** build
internal weighted routing. That honesty is in [Non-Goals](#non-goals); pretending
otherwise would be the kind of "validates a plan the controller never executes"
gap OEP-0015 called out.

**Q. What does v2 give up?** A. Parallel-independent groups (unused today, per
[Evidence](#evidence-the-multi-group-axis-is-unused)) and the `Independent` policy
marker. If a real topology needs concurrent independent tracks, that is the trigger
to revisit alternative (a)-on-parallel-groups before committing.

---

## Failure modes & mitigations

- **Two engines race `partition`/`traffic[]`** (today's reason for mutual
  exclusion). → *Mitigation:* v2 merges them into one reconciler that owns those
  writes; until merged, the mutual-exclusion validation stays. Mitigation, not a
  fix, until the merge lands.
- **Silent migration drift:** an operator's stored `coordination`/`canary` spec is
  pruned and stops being honored when the field is removed. → *Accepted, not
  mitigated:* v2 ships no conversion shim (see [Conversion / migration](#conversion--migration)).
  The bet is that near-zero adoption means there is no stored block to lose; the
  residual risk is carried by landing v2 early, not by a deprecation window.
- **Status consumers break** (HTTPRoute builder reads `status.components[c].traffic[]`).
  → *Mitigation:* the alpha keeps writing per-Component `status.components[c].traffic[]`
  (derived from each group's service-level weight, deferral #4), so the data plane
  needs no change.

---

## Open questions / next steps

Implementation is **underway** (spec + engine rewiring). The triggers that gated the
start have fired: canary (OEP-0015.1) has shipped and is in use; the `sequence + canary`
combination the old spec forbade is now wanted; and adoption is still low enough to
drop the legacy fields without a conversion window. Concrete open items that remain:

- **[TBD] Naming:** `groups` vs `stages`/`waves`; `maintainRatio` vs
  `ratioBalanced`. (Owner: design review.)
- **[TBD] Unified `status.rollout` phase enum** and the coordination/canary →
  unified phase mapping. Blocks the merged reconciler.
- **[TBD] Multi-Component canary semantics.** Today's canary is primary-driven on a
  single Component; lockstep multi-Component canary needs an explicit sub-design.
- **[TBD] Merge the two reconcilers.** Until the canary and coordination engines
  share one writer of `partition`/`traffic[]`, the spec accepts group combinations
  the engine runs concurrently rather than in declared order — cross-group
  sequencing is not yet enforced.

**Scope:** this applies to **OMENative / IR-managed** InferenceServices only;
RawDeployment has no coordination/canary surface and is unaffected.

**See also:** `oeps/0015-unified-rollout` (the `spec.rollout` home this builds on),
`oeps/0015.1-canary-executor` (the executor v2 would absorb),
`oeps/0007.1-omenative-coordinated-rollout` (coordination groups origin).
