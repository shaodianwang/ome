# Multicluster placement

An InferenceService selects the new contract with
`spec.placement.policy: ClusterAffinity` and an explicit `mode`. Without that
opt-in, selector strings and annotations retain Legacy behavior through upgrades.
An ordinary service without any placement intent stays local. Placement does
not adopt an unrelated member service with the same name.

The following sections describe ClusterAffinity. Legacy continues to support
implicit Single, typed-over-annotation selector precedence, Packed/spread Split,
anti-sliver settings, routing-factor aliases, updates, and failover. In Legacy,
no selectors means no matching clusters; in ClusterAffinity, omitted affinity
matches every registration.

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
Member deletion still permits cleanup. Local services and Legacy members retain
all deployment modes and do not gain this condition.

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
`NotIn` against the actual `metadata.name`. Weights are valid only for `Split`.
Omitting affinity is unconstrained. An explicit empty or null affinity is invalid.

Shares use largest remainders, with cluster-name order breaking ties. Three
replicas across two equally weighted registrations produce two and one, never
two each. A zero share creates no member workload. An unreachable matched
registration retains its desired share; spare quota elsewhere cannot absorb it.

The fleet floor comes from `split.replicas`, or an explicitly positive
`engine.minReplicas` when that override is absent. An unresolved floor holds the
plan. One replica contains a complete Engine instance and, when declared, a
complete Decoder instance. A multi-pod instance stays together on its member.
Router retains its own per-home scaling policy.

`split.maxReplicasPerCluster` is an optional local ceiling. A positive ceiling
below any computed share rejects the entire new allocation. Zero is uncapped.
Member autoscaling otherwise retains its local policy.

## Moving replicas

The transition coordination below applies to `All`, `Split`, and
`SplitByCapacity`. `Single` retains its admission-race behavior.

`Single` persists its nominated homes before creating workloads, then commits
the observed winner before cleaning up race losers. One admitted instance of
each declared component suffices to win; admission need not fill the requested
floor. Simultaneously observed winners are ordered by cluster name. A restart
retains accepted nominations and the committed winner. Only that winner can be
published for routing, even while loser resources await cleanup.

A matching Single winner remains sticky. Unknown member health holds its
identity; confirmed loss uses the configured recovery grace. Healthy affinity
changes currently report `MigrationBlocked` and retain the winner. The initial
admission race does not authorize a deliberate move of a healthy service.

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

## Full-policy homes

`All` retains the complete source policy on each eligible member; `Single`
applies it to nominated members and its retained winner. Placement
resolves component minimums using source, runtime, then operator configuration
precedence. It requires positive resolved floors, with equal Engine and Decoder
counts; Router has its own floor. A zero or unresolved floor holds that home's
allocation without changing local-service behavior. Operator defaults require
the configured member operator namespace and an identified ConfigMap.

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

One unit uses the combined Engine and Decoder demand, including all leader and
worker pods. Each required resource/flavor pool contributes its whole-replica
quotient of allocatable hardware divided by demand; the smallest quotient is the
member's weight. Quota budgets, usage, routing weights, and high-water marks do
not enter this calculation. Ambiguous flavors, stale reports, and unavailable
members return no partial allocation or equal-share fallback.

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

`PlacementInputValid` reports syntax and obsolete fields. `PlacementSatisfied`
requires every assigned floor to be admitted; extra admission on one home does
not compensate for another home's shortfall. `PlacementConverged` reports
whether members have applied the allocation. Serving readiness remains
independent, so a held allocation can continue serving.

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

Opt in one source deliberately after upgrading member controllers and retiring
old hub writers. Existing Legacy sources do not require migration for this
upgrade. Update the source in one write:

- Set `policy: ClusterAffinity`.
- Replace `requirements`, `clusterSelector`, and the corresponding placement
  annotations with `clusterAffinity` and an explicit mode.
- Remove `split.spread` and `split.minReplicasPerCluster`, including explicit
  false, zero, empty, or null values. Choose equal or weighted static shares.
  Packed placement has no equivalent.
- Move placement `capacityFactors` to `spec.routing.capacityFactors`.

Legacy restrictions remain honored until opt-in. ClusterAffinity rejects them,
including explicit empty/zero values preserved through storage round trips.
The policy cannot be removed from an existing source: draining and recreating
it is required to return to Legacy. Downgrading to an old hub binary also
requires migrating or restoring opted-in sources first.

Deploy compatibility support together with the affinity implementation. A build
that only rejects legacy intent holds old sources and stops their placement and
routing updates, even though existing member pods may keep serving.
