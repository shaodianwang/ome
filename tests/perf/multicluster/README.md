# Multicluster placement benchmark

Run the CI scenario with:

```sh
make test-perf-multicluster \
  PERF_MC_GENERATOR=tests/perf/multicluster/configs/ci/generator.yaml \
  PERF_MC_RANGESPEC=tests/perf/multicluster/configs/ci/rangespec.yaml
```

Each comparison pass creates an independent control-plane envtest and one
member envtest per configured member. This isolates the measured pass from
retained plans and pending cleanup in the other pass. Startup for both
topologies contributes to the reported wall time.

Sources select OMENative and reference a namespaced runtime seeded on every
member. Each class supplies `runtimeName`, `image`, and a positive `minReplicas`.
The synthetic member controller writes owned InferenceReplicas and matching
single-Pod instances, admits every configured replica, acknowledges the execution
policy and parent generation,
and triggers the metadata admission funnel through IR updates. The member
summary can remain unchanged during admission. These fixtures
measure initial admission and placement; they do not simulate physical Pod
readiness, capacity normalization, rebalancing, or rollout surge. Use the
multicluster integration and KIND suites for those behaviors.

`placement.clientQPS` and `placement.clientBurst` configure the remote client
budget equally for both passes. The committed scenarios use 50 QPS and a burst
of 100 per member; zero for both selects client-go defaults. Runtime and
allocation fences use live reads, so these settings affect measured latency.
The runner logs the selected budget and reports remote operation counts,
including direct reads and writes. Control-plane direct reads are included in
local counts. The comparison thresholds live in each scenario's `rangespec.yaml`.

A timeout logs source placement status and conditions so unresolved backends,
identity holds, and delayed admission can be distinguished.
