# OEP-0025.1: TrafficMap publication fallback

**Tl;dr:** Placement and `TrafficMap` continue to report current facts. An
installation may opt into retaining a publisher's last fully applied positive
plan when the current plan has no positive weight because placement was
confirmed lost or a ready home lost its address. The publisher reports this as
fallback, not as successful publication of the current generation.

## Motivation

OEP-0025 intentionally makes an empty or all-zero `TrafficMap` visible to every
consumer. Copying stale homes or weights back into placement or `TrafficMap`
would hide an outage and make stale state look current.

Some installations nevertheless prefer a last-known plan over an immediate
global zero during a narrow placement failure. That availability choice belongs
at the publisher boundary, where the desired/applied divergence can be explicit
and independently observable.

## Scope

This proposal:

- keeps placement status and `TrafficMap` factual;
- adds an installation-owned, opt-in fallback for compatible publishers;
- persists the exact last positive plan for restart-safe replay; and
- exposes fallback through `TrafficMap.status` and a metric.

It does not retain traffic for manual or probe drains, zero capacity,
definitively unready homes, deletion, routing opt-out, or source withdrawal. An
unknown home never authorizes replay; under `RetainLastPositive`, the publisher
leaves already-applied state untouched until observation becomes definitive.
`ApplyCurrent` continues to apply the current no-positive plan. The proposal
also does not prove that a retained endpoint is healthy or impose a fallback
expiry in the first version.

Terms used below:

- **Current plan:** the publisher's complete canonical `target -> integer
  weight` plan after target rendering and weight transformation.
- **No-positive plan:** a valid current plan with no weight greater than zero;
  it may be empty.
- **Last-positive snapshot:** the latest complete current plan that was fully
  applied and contained at least one positive weight.
- **Fallback publication:** applying that snapshot instead of the current
  no-positive plan.

## Proposal

### Policy and publisher capability

A compatible publisher accepts one installation-only option:

```yaml
routing:
  publisher:
    options:
      noPositivePlanPolicy: ApplyCurrent
```

`ApplyCurrent` preserves existing behavior. `RetainLastPositive` enables the
fallback. Installation configuration must provide one of these values; the
publisher rejects missing or unknown values and rejects this option in
per-InferenceService publisher options.

The shared publisher contract gains an optional capture/replay capability. It
is limited to publishers whose complete replay state is a bounded canonical
target/weight set and whose targets are covered by the existing claim journal.
Publishers without that capability reject `RetainLastPositive` and otherwise
keep normal OEP-0025 behavior. This makes the contract publisher-neutral without
pretending every backend has the same object model.

### Eligibility

Fallback is allowed only when all of the following are true:

1. The current plan is valid, not a withdrawal, and has no positive weight.
2. The `TrafficMap`, source, ownership, routing verdict, and observed
   generations are fresh and mutually consistent.
3. Current status proves one of these cases:
   - Single placement is `Pending`, retains only a last-winner identity, has
     Ready `Unknown/PlacementLost`, and `Routable=False/NotPlaced`; or
   - placement is `Placed`, has positive observed ready replicas but no address,
     and `Routable=False/NoAddressableHome`.
4. No traffic-drain annotation intent exists and the fresh `OverrideActive`
   condition is `False/NoOverrides`.
5. A valid snapshot exists for the same publisher, source generation,
   compatible plan options, and exact claim set.

The allowlist is closed. These states never authorize replay:

- `AllHomesUnready`, `NoRoutableCapacity`, probe `Drain`, or any manual-drain
  intent, including a pending or malformed override;
- initial placement, `Admitting`, `Failed`, non-Single total placement loss, or
  any `NotPlaced` state without `PlacementLost` provenance;
- stale or missing status, claim mismatch, deletion, routing opt-out, or
  publisher source withdrawal.

A fresh `PlacementUnknown` verdict also never authorizes replay, but it is an
incomplete observation rather than a definitive negative. The publisher
therefore preserves the snapshot and claims, reports fallback eligibility
Unknown, and performs no external mutation until placement supplies a
definitive verdict. This prevents a transient incomplete read during controller
restart from destroying the durable plan needed once `PlacementLost` returns.

Invalid ownership, provenance, options, or plans cause no external mutation;
the publisher reports Unknown and retries. Unclassifiable stale input behaves
the same way. An eligible state with no valid snapshot follows `ApplyCurrent`
only after normal validation and authority checks pass.

The placement controller prerequisite is simple: policy and readiness holds may
stop actuation but never stop observation. Confirmed Single-home loss becomes
non-routable `Pending/PlacementLost`; an incomplete member read becomes
`PlacementUnknown`. The publisher never manufactures this provenance.

### Durable snapshot

`TrafficMap.status.publisher` gains an optional `lastPositive` block:

```yaml
status:
  publisher:
    publisherName: example-publisher
    claimedTargets: [model-a/cluster-a, model-a/cluster-b]
    lastPositive:
      trafficMapGeneration: 12
      observedISVCGeneration: 42
      planCompatibilityDigest: sha256:<digest>
      targets:
        - {target: model-a/cluster-a, weight: 60}
        - {target: model-a/cluster-b, weight: 40}
```

The snapshot is the exact post-transform plan, not a copy of
`TrafficMap.spec.entries`. Its contract is:

| Field | Shape | Requirement |
|---|---|---|
| `lastPositive` | optional object | Absent means replay is unavailable. |
| `trafficMapGeneration` | `int64`, minimum 1 | Generation of the positive plan. |
| `observedISVCGeneration` | `int64`, minimum 1 | Source generation of that plan. |
| `planCompatibilityDigest` | `sha256:<hex>` | Covers options that affect target identity or weights. |
| `targets` | list-map keyed by `target`, 1-128 items | Exactly one item per claimed target. |
| `target` | string, 1-512 bytes | Canonical publisher target. |
| `weight` | non-negative `int64` | Exact applied integer weight; publisher-specific bounds still apply. |

Every claim appears exactly once, including retired or inactive targets at
weight zero, and at least one item has positive weight. A snapshot cannot
authorize a new claim. Missing, partial, malformed, transform-incompatible, or
claim-mismatched snapshots are invalid.

`planCompatibilityDigest` covers all effective publisher options except
`noPositivePlanPolicy`; changing only the fallback choice does not invalidate
the captured plan. The existing `observedOptionsDigest` still covers the full
effective configuration and advances after a successful fallback.

### State and crash ordering

| Current input | Action |
|---|---|
| Positive plan | Apply it; after full success replace the snapshot. |
| No-positive plan with `ApplyCurrent` | Clear the snapshot, then apply the current plan. |
| Eligible no-positive plan with `RetainLastPositive` and a valid snapshot | Re-apply the complete snapshot. |
| Otherwise-valid but ineligible no-positive plan, or invalid snapshot | Clear the snapshot, then apply the current plan. |
| Stale or unclassifiable input | Do not mutate externally; report Unknown and retry. |
| Invalid owner, provenance, options, or plan | Do not mutate externally; report Unknown and retry. |
| Withdrawal, opt-out, or deletion | Clear the snapshot, then follow normal cleanup. |

All paths preserve these invariants:

1. Before an external mutation, the controller durably sets
   `PublicationFallback=Unknown/Transitioning` and `Published=False`.
2. A claim-set, source-generation, or plan-compatibility change clears the old
   snapshot before mutation. Preclaim expansion and explicit zero, withdrawal,
   or finalization do the same.
3. A snapshot is committed only after the complete positive plan succeeds.
   Replay repeats normal target, collision, source-UID, and exact-owner checks,
   then applies the complete snapshot idempotently. A failed replay preserves
   the snapshot and claims for a full retry; partial success is never journaled
   as a new snapshot.
4. Finalization first records `PublicationFallback=False/Finalizing`,
   `Published=False`, and clears the snapshot. It then unpublishes every claim
   and removes the journal only after cleanup succeeds.

The claim journal remains the sole cleanup authority. Fallback acquires no new
claims and cannot bypass target ownership.

### Status and metrics

The publisher owns a new abnormal-true `PublicationFallback` condition:

| Result | `PublicationFallback` | `Published` | Realized generation |
|---|---|---|---|
| Current plan fully applied | `False` | `True` | Advance to current. |
| Snapshot fully replayed | `True/LastPositiveRetained` | `False/PublicationFallback` | Keep snapshot generation. |
| Transition, failure, or unknown eligibility | `Unknown` | `False` | Keep last confirmed value. |
| Withdrawal or finalization | `False` | `False` | Follow OEP-0025 cleanup semantics. |

Both conditions use the evaluated `TrafficMap` generation as
`observedGeneration`. The scalar realized generation continues to name what the
publisher actually applied. Thus `Routable=False`, `Published=False`, and
`PublicationFallback=True` may intentionally coexist.

The gauge
`ome_trafficmap_publication_fallback{namespace,trafficmap,publisher}` is `1`
only while fallback is True, otherwise `0`, and is removed with the
`TrafficMap`.

## Testing

- Compatibility: `ApplyCurrent` publishes empty/all-zero plans unchanged, and
  unsupported publishers reject retention mode.
- Eligibility: cover both allowed reasons and every explicit exclusion,
  including stale generations and drain intent.
- Snapshot: validate schema bounds, deep copy/round trip, exact claim equality,
  source generation, compatibility digest, and integer preservation.
- Crash safety: fail before and after external writes, including after apply
  succeeds but before the final status patch; restart and verify full
  idempotent replay without acquiring claims.
- Status: cover fallback entry, retry, positive recovery, explicit zero,
  withdrawal, and finalization; `Published` is never True during fallback.
- Integration: publish a multi-target positive plan, confirm placement loss
  through the real observation path, restart the manager, and verify exact
  replay while the factual `TrafficMap` remains empty or all zero.

Test fixtures in upstreamable paths use generic names and `example.com`
endpoints.

## Rollout and rollback

1. Land always-observe placement behavior.
2. Add the status API and optional capture/replay contract with no behavior
   change.
3. Implement it in a compatible publisher with `ApplyCurrent`, verify snapshot
   coverage and alerts, then opt into `RetainLastPositive`.

Rollback sets `noPositivePlanPolicy: ApplyCurrent`. The next no-positive
reconcile clears the snapshot before applying the factual plan. Publisher
replacement still uses OEP-0025's disable-clean-switch-enable sequence.

## Tradeoffs and alternatives

- A retained endpoint may be dead. The opt-in trades a guaranteed zero for an
  unverified chance of service, so status remains degraded and visible.
- Multi-target writes are not atomic. Complete idempotent replay makes partial
  failure recoverable, not impossible.
- The journal stores one bounded entry per target.

Freezing placement, copying stale entries into `TrafficMap`, skipping zero
writes without a journal, or reconstructing an old plan from current inputs are
rejected because each hides facts or cannot recover safely after restart.
Fallback for every zero reason is rejected because it would override explicit
drain, readiness, and capacity signals.

## Open question

Should a future version require `maxFallbackAge`? The first version remains
indefinite and relies on the condition and metric for alerting.

## See also

- [OEP-0025](../0025-multi-cluster-routing/README.md)
- [OEP-0018](../0018-multi-cluster-placement/README.md)
- [OEP-0018.3](../0018.3-placement-modes/README.md)
