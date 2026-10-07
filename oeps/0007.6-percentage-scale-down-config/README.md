# OEP-0007.6: Percentage scale-down configuration

## Summary

A fixed deletion budget releases capacity slowly for large Components while
allowing an entire small Component to disappear in one batch. Let the existing
`lifecycle.scaleDownPodBatchSize` ConfigMap setting accept a positive integer
or a percentage string. The chart default remains the integer `100`.

This controls how quickly OMENative executes an already-requested scale-down.
It does not change desired replicas or any CRD, and it does not add an ISVC or
ServingRuntime policy. RawDeployment and LWS-backed execution are unaffected.

## Motivation and terminology

A Component is an ISVC's engine, decoder, or router managed by an
InferenceReplica. An Instance is one replica, possibly a leader/worker gang of
several Pods. Its Pods must be deleted together to avoid partial gangs.

A budget of 100 Pods permits 100 singleton Instances per wave but only 12
intact eight-Pod gangs. Large reductions can therefore require many waves even
when the operator has control-plane headroom for larger batches. Percentages
let the same operator setting scale with each Component's size.

## Configuration

The `inferenceservice-config` ConfigMap accepts:

```yaml
data:
  lifecycle: |-
    {
      "scaleDownPodBatchSize": "10%",
      "scaleDownRequeueInterval": "5s"
    }
```

The corresponding Helm value is:

```yaml
ome:
  controller:
    lifecycle:
      scaleDownPodBatchSize: "10%"
```

Existing integer values retain their meaning. Percentages are whole numbers
from `1%` through `100%`. Zero, negative, fractional, malformed, and over-100%
percentages are rejected at manager startup. A quoted integer such as `"100"`
is rejected; use the JSON number `100`. An omitted or null field preserves
unbounded selection. The binary supplies no numeric default.

## Resolution and lifecycle

The current Pod-equivalent footprint counts authoritative owned live and
Terminating Pods, plus one unit for each Instance status with no observed Pod.
Cleanup-only orphan PodGroups contribute one unit each when no Pod or status
accounts for them. This prevents the percentage denominator from disappearing
before resource cleanup finishes.

For each Component observation:

```text
budget = ceil(current Pod-equivalent footprint * percentage / 100)
```

The denominator is neither desired replicas nor the requested reduction. A
2,000-to-1 singleton scale-down at `10%` starts with a 200-Pod budget. A
2,000-Instance Component with eight Pods per Instance starts with a 1,600-Pod
budget, admitting 200 intact gangs. Later waves shrink with the current
footprint. Rounding up ensures small Components still progress.

Resolution uses the existing authoritative snapshot, with no additional Pod
list. Arithmetic widens before multiplication to avoid overflow.

The bounded scale-down lifecycle remains authoritative:

1. Commit the selected deletion intents together before external effects.
2. Finish existing Delete-owned work before admitting any fresh victims.
3. Keep Instance and gang boundaries atomic; an oversized first gang proceeds
   alone so a budget smaller than one gang cannot deadlock deletion.
4. Retain cleanup and finalizer ordering, including after a replica rebound.

The same configuration applies during normal scale-down and teardown.
`scaleDownRequeueInterval` remains a polling cadence while work is pending;
watched resources can wake reconciliation sooner. Neither setting promises a
fixed deletion rate per second.

## Compatibility and operation

The manager reads the policy at startup. Helm's configuration checksum rolls
controller Pods after a value change. Changing only this pacing setting does
not change workload revision identity or restart model Pods. No CRD rollout or
model runtime edit is required.

A percentage is the operator's budget; there is no additional fixed ceiling.
For example, `10%` really permits 200 Pods on a 2,000-Pod Component. Multiple
Components may scale down concurrently, so operators must measure API-server
and etcd headroom before choosing a percentage for their installation. This
proposal does not select a production percentage.

Before downgrading to a binary that accepts only integers, restore an integer
ConfigMap value. The older parser rejects percentage strings.

Wave logs retain the resolved `podBudget` and add `configuredPodBudget` and
`podFootprint`. Existing metrics retain their Pod-equivalent units.

## Validation

Coverage must establish startup parsing, Helm JSON rendering, ceiling rounding,
large-count arithmetic, and unchanged integer/omitted behavior. Large singleton
and gang tests must check bounded effects, one status write for admission,
progress across shrinking waves, and the barrier against fresh admission while
cleanup remains active. Controller tests must exercise orphan PodGroup cleanup
through the same setting.

The singleton KIND scenario can exercise the percentage ConfigMap path with
`OME_TEST_SCALE_DOWN_POD_BATCH_SIZE=40%` and
`GINKGO_FOCUS='scales a singleton Component 5 -> 1'` when running
`make test-kind`. Use a throwaway `KUBECONFIG` for that local cluster.

## Alternatives and next steps

1. An ISVC or ServingRuntime field would make controller deletion pacing part
   of the workload API. The operator ConfigMap already owns this setting.
2. A fixed maximum alongside a percentage could limit control-plane bursts,
   but adds precedence and tuning rules. The percentage itself is the explicit
   operator choice; retain the absolute form when a fixed bound is required.
3. A desired-replica denominator would collapse a large reduction to a tiny
   budget and fail to address its convergence time.

Review the ConfigMap contract and validate measured control-plane headroom
before selecting a percentage for a deployment.
