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
execution; they do not change those shares.

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

The transition coordination below applies to `Split`. `Single` and `All` retain
their existing admission and re-placement behavior.

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

## Status and rollout

`PlacementInputValid` reports syntax and obsolete fields. `PlacementSatisfied`
requires every assigned floor to be admitted; extra admission on one home does
not compensate for another home's shortfall. `PlacementConverged` reports
whether members have applied the allocation. Serving readiness remains
independent, so a held allocation can continue serving.

The accepted plan records source and cluster identities, desired and current
floors, matching terms, and weights. Unknown identity or member observations
hold existing authority. `SplitByCapacity` is reserved in the API; the controller
holds with `CapacityUnknown` until per-service demand and nominal capacity are
resolved, without falling back to equal shares.

Upgrade member CRDs and controllers before expecting moves to progress. Missing
fields or missing member acknowledgements hold movement. Complete or stop an
active transition before downgrading to a member binary that does not enforce
placement pauses and replica limits. Ordinary local services do not acquire
these guards.

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
