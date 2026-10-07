# Release Notes

## Unreleased

### feat: SlicePolicy adapter — cloud-specific scheduling logic decoupled from core

**Breaking Changes:**
- The scheduler now handles an expanded set of failure reasons: `FAILED`, `SliceCreationFailed`,
  `SliceHealthProbeFailed` (previously only `UNHEALTHY` and `SliceCreationFailed`).
- A new backoff mechanism defers rescheduling to the slice-controller when `retry-on-failure=true`
  and partitions are healthy. This is controlled by `--reschedule-timeout` (default `10m`).
- The `UNHEALTHY` reason is no longer handled — it was never a real cloud controller reason.

**New Features:**
- `SlicePolicy` interface (`internal/policy/policy.go`) abstracts environment-specific Slice CR
  interpretation.
- `CloudPolicy` — cloud-aware scheduling with expanded failure reasons, retry-aware backoff,
  partition health checks, and `ACTIVE_DEGRADED` support.
- `--reschedule-timeout` flag (default `10m`) — timeout for healthy-partition backoff before
  forcing reschedule. Set to `0` to disable backoff entirely.
- `ShouldEvictPod(slice, partitionBroken bool)` — partition health is now passed into the policy
  so eviction decisions are fully encapsulated. Callers no longer need cloud-specific knowledge.
- `ShouldSkipReconcile` guard in the scheduling worker path — prevents scheduling slices with
  `provisioned-partition` annotation (controller is managing them).

**Bug Fixes:**
- Fixed `failureTimestamps` memory leak on slice deletion — entries are now cleaned up in
  `handleSliceDelete`.
- Fixed `rescheduleTimeout=0` acting as immediate timeout on second call — now correctly disables
  backoff entirely.
- Fixed `isPartitionBroken` being a no-op in PodReconciler — now correctly checks partition health
  via the cache.
- Fixed e2e Scenario A using non-existent `UNHEALTHY` reason — updated to use `FAILED`.

**Helm Chart:**
- Added `reschedule.timeout` value (default `10m`).
- Updated deployment template to pass `--reschedule-timeout` arg.

**Test Coverage:**
- 76+ test cases across `internal/policy`, `internal/scheduler`, `internal/pod_reconciler`.
- New `isPartitionBroken` integration tests (5 cases: broken, unknown, missing, healthy, empty).
- New backoff timeout integration tests with time manipulation.
- New `ShouldSkipReconcile` worker-path tests.

**Migration Guide:**
- Existing deployments with `retry-on-failure=true` annotation on slices will now see a 10-minute
  backoff before the scheduler forces a reschedule. Previously, the scheduler would reschedule
  immediately. To disable this behavior, set `reschedule.timeout: "0s"` in Helm values or
  `--reschedule-timeout=0` on the command line.

---

## v0.1.0 — Initial Release

- Slice-scheduler for GKE TPU Dynamic Slicing.
- PartitionTree-aware scheduling with modular generator/filter/scorer pipeline.
- PodReconciler for pod-to-node binding.
- Priority-based preemption.
- E2E tests on Kind with KWOK nodes.
