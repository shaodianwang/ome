# Multicluster placement

An InferenceService declares multicluster placement with
`spec.placement.policy: ClusterAffinity` and an explicit supported `mode`.
An ordinary service without placement intent stays local. Placement does not
adopt an unrelated member service with the same name. Omitting affinity matches
every registration; empty or null affinity is invalid.

Legacy placement is unsupported. Migrate sources, derived copies, and writers
on a compatibility release before installing this schema or controller. The
[upgrade procedure](#retiring-legacy-placement) below describes that prerequisite.

`Single` races eligible members for admission and retains one winner. `All`
places the full service on every eligible member. `Split` assigns exact shares
of a fleet floor across all matching registrations. Readiness and quota gate
execution; they do not change those shares. `SplitByCapacity` uses nominal
hardware weights for the same apportionment and bounded transition.

Every `ClusterAffinity` member requires OMENative for its resolved Engine, Decoder,
and Router components. This includes modes selected by a runtime or a component
annotation. An unsupported backend sets the advisory
`PlacementBackendReady=False` condition and holds workload and mounted model
configuration updates. Existing serving resources remain intact; correcting the
runtime or service resumes reconciliation. VirtualDeployment cannot participate.
Member deletion still permits cleanup. Local services retain all deployment
modes and do not gain this condition. Origin-marked members without an execution
policy stop before workload writes, including during direct ownership handoff.

The source checks each target through its identified direct member connection
before granting workload updates, then verifies runtime dependencies and source
identity again at application. Missing runtime inputs produce
`PlacementBackendReady=Unknown`. Single and All continue on verified eligible
members; a standing Single winner whose backend is unverified remains held.
Static Split leaves a blocked member's assigned share pending. SplitByCapacity
holds the entire accepted allocation when any matched backend cannot be verified.
Backend conditions are advisory: standing serving health continues to be observed.
Hardware reports are not required for Single, All, or static Split. Upgrade the
member access role so the placement credential can read runtimes, models, pinned
ControllerRevisions, and the named operator ConfigMap. These reads grant no
runtime, model, quota, or configuration writes.

## Static shares

```yaml
spec:
  placement:
    policy: ClusterAffinity
    mode: Split
    clusterAffinity:
      - weight: 3
        matchExpressions:
          - key: region
            operator: In
            values: [west]
      - weight: 1
        matchExpressions:
          - key: region
            operator: In
            values: [east]
    split:
      replicas: 8
    maxSurge: 1
```

With one matching registration per region, this assigns six replicas to the
west member and two to the east member. Multiple registrations in either region
each receive that term's weight. Terms are ORed, and requirements within a term
are ANDed. When weights are present, all matching term weights add; an omitted
weight contributes one. With no explicit weights, every matching registration
has equal weight, including one matched by several terms.

`matchExpressions` supports `In`, `NotIn`, `Exists`, `DoesNotExist`, `Gt`, and
`Lt` with Kubernetes node-selector semantics. `matchFields` supports `In` and
`NotIn` against the actual `metadata.name`. Weights are valid only for `Split`
and have no upper bound. Omitting affinity is unconstrained. An explicit empty
or null affinity is invalid.

Shares use largest remainders, with cluster-name order breaking ties. Three
replicas across two equally weighted registrations produce two and one, never
two each. A zero share creates no member workload. An unreachable matched
registration retains its desired share; spare quota elsewhere cannot absorb it.

The fleet floor comes from `split.replicas`, or an explicitly positive
`engine.minReplicas` when that override is absent. An unresolved floor holds the
plan. One replica unit is a complete Engine instance; a multi-pod instance stays
together on its member. Router retains its own per-home scaling policy.

A declared Decoder follows the Engine in the ratio of their minimums: each
home's Decoder floor is its share of `decoder.minReplicas`, apportioned by
largest remainder over the homes' Engine shares so the per-home Decoder floors
sum to the declared minimum exactly; while a move holds more or fewer Engine
replicas than the fleet floor, the Decoder total follows the Engine total in
the same ratio, rounded up. When `split.replicas` sizes the fleet, the
Decoder minimum scales with it in the declared ratio, rounded up; without an
Engine minimum it is used as declared. A home with a positive Engine share always
receives at least one Decoder when the Decoder minimum is positive; a Decoder
minimum smaller than the number of homes with a positive share holds the plan
with a message that says so. A zero Decoder minimum places no Decoder. An
undeclared Decoder minimum pairs one Decoder with each Engine. The ratio is a
floor, not a cap: within their bands the two components scale independently on
the member, and placement does not react to live counts.

`split.maxReplicasPerCluster` is an optional local ceiling in Engine replicas.
A positive ceiling below any computed share rejects the entire new allocation.
Zero is uncapped. A Decoder ceiling is the Engine ceiling scaled by the same
ratio, rounded up. Member autoscaling otherwise retains its local policy.

## Moving replicas

All four modes use the shared surge and member acknowledgement protocol for
moves between homes. Single's initial admission race is separate from moving
an existing winner.

`Single` persists its nominated homes before creating workloads, then commits
the observed winner before cleaning up race losers. One admitted instance of
each declared component suffices to win; admission need not fill the requested
floor. Simultaneously observed winners are ordered by cluster name. A restart
retains accepted nominations and the committed winner. Only that winner can be
published for routing, even while loser resources await cleanup.

A matching Single winner remains sticky. An affinity change that excludes it
can start a bounded replacement race while the current winner keeps serving.
Unknown member health retains the winner's identity and allocation; a missing
connection does not prove that its workloads have stopped. A terminally failed
winner remains selected and reports Failed. Until the terminal condition clears,
later source changes are not applied to that winner.

`maxSurge` is a whole-replica allowance shared by placement and member rollout
surge. An omitted allowance blocks moves between homes. Zero permits no excess;
a move may wait indefinitely if it needs a replacement before removing a
serving replica.

Placement persists each allocation before applying it. Participating members
pause new rollout surge and defer autoscaler growth while a move owns the
allowance. Existing rollout reservations and occupied replicas consume that
same allowance. Placement-authorized floor growth and cleanup remain possible.
Members must acknowledge the current policy and their replica limits before
placement can spend capacity.

A replacement must be ready and routable before it can justify reducing a
serving home. Removing a home also requires publication of its zero traffic
weight for the exact plan. A missing or stale publication holds the move.
A settled positive allocation can release its pause without global routing;
this does not authorize any further replica movement.

### Single replacements

A replacement probe is a complete candidate home. Its full resolved floor must
fit the shared allowance: with an original floor of two and `maxSurge: 2`, only
one additional two-replica home can race at a time. A smaller allowance cannot
create a partial home. Existing rollout occupancy and reservations also consume
this allowance.

One admitted instance of every component selects a replacement. The original
winner remains protected until that replacement reaches its complete Engine,
Decoder, and Router floors and has published routing. The serving winner changes
when the original is requested to drain, or after its absence is verified.
Unselected probes receive no traffic. A cancelled replacement that already
serves retains its route until it can be safely drained. Physical cleanup of
the original and losing probes must finish before the surge pause can be released.

An optional positive `spec.placement.replacementTimeout` bounds how long a
probe may wait for component admission. Expired probes are retired and their
resources removed before another probe can spend their allowance. The persisted
start time survives controller restarts. This timeout does not apply to a
selected replacement waiting for full readiness. A terminally failed selected
replacement retains the original winner and reports `ReplacementFailed` until
its configuration is repaired or the move is cancelled.

If the original disappears during an accepted move, placement verifies that its
derived service, InferenceReplicas, and Pods are absent before releasing its
current allocation. Its original floor remains the budget baseline. Cancelling
the move can restore an eligible original home within the same allowance; a
serving replacement stays protected until the restored home is fully ready and
routable. Unknown inventory holds this recovery. After a handoff, complete loss
of every home requires verified empty inventories and the configured winner-loss
grace before a fresh initial race can begin.

## Full-policy homes

`All` retains the complete source policy on each eligible member; `Single`
applies it to nominated members and its retained winner. Placement
resolves component minimums using source, runtime, then operator configuration
precedence. It requires a nonnegative resolved floor for every component; Engine
and Decoder floors may differ and the Engine count is the home's replica unit,
with Router keeping its own floor. An unresolved floor holds that home's
allocation without changing local-service behavior, and the `PlacementConverged`
message names the home and the cause. Operator defaults require the configured
member operator namespace and an identified ConfigMap.

An unpaused zero-floor home retains its service and autoscaler through scale to
zero. Single still needs actual admission to select a winner; an idle established
winner remains selected. All keeps each eligible full home, including homes with
different inherited floors. Zero floors grant no ready or routable capacity.

Retiring a home or adding a positive All allocation after initial provisioning
requires positive component floors. `PositiveFloorRequired` retains the accepted
plan while those guarantees are absent. Configuring positive minimums refreshes
retained homes before shared movement accounting begins; observed autoscaled
replicas do not substitute for these minimums. Outside a move, a retained home's
minimum can change to or from zero. A zero-floor update during a paused move
holds that move until positive floors are restored.

The plan stores each current and desired home policy separately. A replacement
must fit its entire floor within the shared allowance: a two-replica home cannot
start with only one surge replica available. Outgoing homes retain their accepted
floors and templates until replacement readiness and traffic drain permit
removal. During a pause, inherited minimums are fixed to the accepted values;
applying the approved source policy restores inheritance.

Initial provisioning inventories standing member resources before allowing
retirement. Missing matched members can be retried independently while that
initial source intent stays unchanged. Unknown unmatched homes, changed intent
during incomplete inventory, and unidentified registrations hold movement.
`AwaitingHomeInputs` reports incomplete inventory or unresolved full-home floors.
Placement mode changes involving an accepted full-policy plan remain held.

## Capacity input contract

The capacity adapter reads each matched member through its identified direct
connection, resolves the declared Engine and Decoder from that member's runtime
and configuration, and joins the result to the management root's fleet report.
Report UID, version, flavor mapping, and registration UID must agree. An empty
member can supply a complete sample; admission and running workloads are not
prerequisites for computing nominal weights.

One unit is one Engine instance plus the Decoder demand that the ratio of their
minimums attaches to it, including all leader and worker pods; equal minimums
use one instance of each. Each required resource/flavor pool contributes its
whole-unit quotient of allocatable hardware divided by that demand; the smallest
quotient is the member's weight. Quota budgets, usage, routing weights, and
high-water marks do not enter this calculation. Ambiguous flavors, stale
reports, and unavailable members return no partial allocation or equal-share
fallback.

The adapter requires OMENative for every declared component because placement
observes InferenceReplica execution and shared surge accounting. Local services
retain their existing backend choices. Required node constraints are projected
onto the complete flavor catalog's label keys for nominal attribution; topology,
model readiness, and other scheduling constraints remain member admission
checks. These weights do not guarantee physical fit.

Configure the input adapter through
`ome.multicluster.config.placement` in the resources chart, emitted as
`multicluster.placement` in `inferenceservice-config`:

```yaml
memberOperatorNamespace: operator-system
capacity:
  rootName: capacity-root
  maxAge: 5m
  stabilityWindow: 1m
  refreshInterval: 10s
```

These are example values, not binary defaults. The block defaults to null; every
field is required when present, and `refreshInterval` must be shorter than
`maxAge`. The root name must match the management and member quota collectors.
All members must expose their configuration in
`multicluster.placement.memberOperatorNamespace`. This setting also locates
pinned runtimes and sharded-model configuration for Single, All, and static
Split. An absent namespace holds resolution when those dependencies are needed.
When upgrading, move it from `placement.capacity.memberOperatorNamespace` to
`placement.memberOperatorNamespace`.
The adapter uses the existing placement `fanoutTimeout` for remote operations.

The resources chart grants the control-plane manager read access to fleet quota
reports only when capacity placement is configured. Member access also requires
read access to quota roots, ResourceFlavors, and RuntimeClasses.

Capacity collection must provide identified, complete per-cluster flavor reports.
The placement credential needs reads for the local quota root, ResourceFlavors,
configuration, runtimes and their dependencies, and RuntimeClasses. Before
applying a contracted allocation, the controller resolves through the exact
write connection again and checks the accepted rendering, fresh hardware totals,
and root identity. A heartbeat alone may advance the report version without
invalidating an accepted allocation.

Set `spec.placement.mode: SplitByCapacity` and provide the fleet floor with
`split.replicas` or a positive Engine minimum. The first allocation can use a
complete fresh snapshot immediately. A hardware, flavor mapping, or member
identity change must remain consistent for `stabilityWindow` before the
controller accepts it. Unknown reports restart that observation window. A
controller restart also restarts an unaccepted window; the persisted allocation
remains authoritative throughout. Source demand changes on unchanged hardware
do not wait for a hardware stability window.

Hardware events enqueue participating services; heartbeat versions and usage
do not. `refreshInterval` bounds checks for stale reports, runtime/configuration
changes, and missed events. Missing configuration holds this mode pending.
An outgoing home's accepted floor can decrease while preserving its member
specification and rendering contract; retirement does not apply a new source
template or require hardware weights for a home outside the matched set.

Upgrade member CRDs, RBAC, and controllers before enabling contracted
allocations; missing support cannot acknowledge the accepted demand.

## Status and rollout

`PlacementInputValid` reports syntax and obsolete selector annotations. `PlacementSatisfied`
requires every assigned floor to be admitted; extra admission on one home does
not compensate for another home's shortfall. `PlacementConverged` reports
whether members have applied the allocation. Serving readiness remains
independent, so a held allocation can continue serving.

For valid affinity, the `PlacementInputValid` message lists zero-based term
indexes with no matching registered WorkloadCluster. An unready matching member
still counts as a match. Unmatched terms do not invalidate the request or change
allocation; they help distinguish inventory or selector mistakes from member
availability problems.

The accepted plan records source and cluster identities, desired and current
floors, matching terms, and weights. Unknown identity or member observations
hold existing authority. `PlacementCapacityFresh` is true when the matched
hardware reports and demand are usable, false with `CapacityUnknown` when any
required input is unavailable, and unknown when this reconcile has not checked
them. `CapacityStabilizing` holds a changed hardware snapshot through the
configured window. Both holds preserve the accepted plan and member floors;
capacity placement never falls back to equal shares.

Upgrade member CRDs and controllers before expecting moves to progress. Missing
fields or missing member acknowledgements hold movement. Complete or stop an
active transition before downgrading to a member binary that does not enforce
placement pauses and replica limits. Ordinary local services do not acquire
these guards.

Execution annotations use an explicit versioned envelope. Members can still
read existing flat policies, but reject unsupported versions or fields before
reconciliation writes. Upgrade members before the source emits envelopes;
flat-only policy readers cannot establish allocation authority from them.
Deleting a service can still follow finalizer cleanup, and ordinary local
services ignore placement execution annotations.

## Retiring Legacy placement

Complete migration using a compatibility release **before** installing this
schema or binary. Audit all sources, derived copies, and writers, including
resources managed outside GitOps. Update each source in one write:

- Set `policy: ClusterAffinity` and an explicit mode.
- Replace `requirements`, `clusterSelector`, and both selector annotations with
  `clusterAffinity`. Combine requirements that were ANDed in the same term.
- Remove `split.spread` and `split.minReplicasPerCluster`, including explicit
  false, zero, empty, or null values. Choose equal or weighted static shares.
  Packed placement has no equivalent.
- Move placement `capacityFactors` to `spec.routing.capacityFactors`.

Wait for accepted plans, placement convergence, and Ready status. Every derived
copy must retain its source ownership and carry `ome.io/placement-policy:
ClusterAffinity` plus a valid `ome.io/placement-execution` envelope. Compare ISVC,
InferenceReplica, and Pod identities to detect unintended replacement. Require
the explicit policy in writer validation so a later promotion cannot restore
Legacy intent.

The retired fields are absent from the schema. Strict field validation rejects
them; non-strict clients may have them pruned before admission sees them. Do not
rely on an upgrade to preserve or translate their restrictions. Selector
annotations are rejected, and policy and mode are required.

The policy cannot be removed from an existing source: changing placement
ownership requires draining and recreating it. Rollback must use a
ClusterAffinity-capable release. Restoring a schema does not restore removed
restrictions or make a Legacy writer safe.

Execution envelope versions 1, 2, and 3 and existing flat policies remain
readable. Status recovery and endpoint-publisher cleanup are separate
compatibility contracts. The policy metric now carries `policy` and `mode`;
selector/spread labels and the per-cluster minimum metric are removed.

See the [retirement decision](../oeps/0018.4-cluster-affinity-placement/README.md#legacy-retirement)
and the [compatibility migration procedure](../oeps/0018.4-cluster-affinity-placement/README.md#migration).
