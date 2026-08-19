# omenative_coordination_kind

KIND-backed integration suite for the OMENative cross-Component rollout
coordination layer. Each spec spins up a single-node KIND cluster,
builds the `fake-serving` image, starts an in-process controller-runtime
manager pointed at the kind apiserver, and runs Ginkgo specs against
real kubelet + scheduler + containerd.

The suite covers contracts where real-kubelet timing matters: pod
lifecycle, drain races, kube-proxy EndpointSlice publication, real
ImagePullBackOff failures, and pod-UID rotation behavior under in-place
vs. recreate updates.

## What KIND covers

- Pod-lifecycle timing (drain races, recreate ordering, in-place
  patch convergence, post-recreate `serving=True` flip).
- Per-revision Service convergence (kube-proxy actually publishes
  EndpointSlices).
- Sequential active-Component handoff (the soak window is observable).
- RatioBalanced gate at scale (in-flight projection + parallel drains).
- Failure escalation through real `ImagePullBackOff`.
- Pod-UID rotation (recreate vs in-place).
- Pure state-machine transitions (per-Component dispatch, group phase).
- Per-Instance counter and condition surface.

## Spec map

| Contract | Spec |
|---|---|
| BlueGreen converge | bluegreen_kind |
| RollingUpdate + RecreatePod | rolling_recreatepod_kind (B-1/2/3) |
| BlueGreen + RecreatePod | recreate_pod_kind |
| BlueGreen + RatioBalanced small | ratiobalanced_kind (C-1/4) |
| BlueGreen + RatioBalanced load | loadtest_kind |
| RatioBalanced anchor snapshot + deliberate scale-down | ratio_anchor_scaledown_kind |
| RatioBalanced anchor for a late-joining group member | ratio_late_member_anchor_kind |
| Pause/resume Idle | pause_kind |
| Pause/resume mid-rollout | pause_midrollout_kind |
| InferenceReplica pause blocks pod recreation | inference_replica_pause_kind |
| Multi-pod topology PodGroup + worker affinity | topology_podgroup_kind |
| Single-replica safe default | single_replica_kind |
| In-place opt-in | inplace_kind |
| Sequential | sequential_kind |
| Independent | independent_kind |
| PerComponent maxSurge | percomponent_surge_kind (E-1) |
| Sequential.Soak wall-clock | sequential_soak_kind |
| PD-disaggregated isolation | pd_isolation_kind + pd_kind |
| PD three-way (Engine+Decoder+Router) | pd_three_way_kind |
| PD concurrent ops | pd_concurrent_kind (PDA-1/2) |
| Annotation-driven migration | migrate_kind (8 specs) |
| Whole-gang migration + gang expiry teardown | migration_gang_kind |
| ObservedGeneration + per-Instance counters | status_surface_kind + status_kind |
| Event recorder firing | events_kind (7 specs) |
| Pod-state watch filters (label, cross-ISVC) | watches_kind |
| Scale up/down + pod lifecycle | lifecycle_kind |
| Bounded scale-down waves + victim order | bounded_scale_down_kind |
| In-place update edge cases | update_kind |
| State-machine interleavings | interleave_kind (INT-5) + interleave_extra_kind (INT-1..4/6) |
| Runtime+model resolution (CBM/CSR stall, missing CSR) | runtime_resolution_kind (RT-4/5) |
| Partial failure recovery | partial_failure_kind (PF-1/2/3) |
| Bad-image failure → Failed | bad_image_kind |
| ISVC delete cascade (pods/Services/CRs) | cleanup_lifecycle_kind (LIFE-DEL) |
| Per-pod headless DNS + EndpointSlice hostname | peer_discovery_kind (PEER-DNS) |
| Per-revision Service emission + drift correction | producer_services_kind (SVC-1/2) |
| Status.Components.Traffic[] steady + mid-bump | producer_traffic_kind (TRAFFIC-1/2) |
| No RolloutCoordinationReady stamped without coord block | regression_kind (REG-1+10) |

## Running locally

```sh
make test-kind                                  # full suite
GINKGO_FOCUS='PD per-Component' make test-kind  # narrow to specific specs
make test-kind-scheduler                        # focused controller + ome-scheduler contract
make test-kind-clean                            # delete leftover clusters
```

`make test-kind` requires `docker` running on the host and a `kind`
binary on PATH (`go install sigs.k8s.io/kind@v0.27.0`). It sets
`KIND_KEEP_CLUSTER=true` by default so the cluster is reused across
runs — set `KIND_RECREATE=true` to force a fresh one.

Raw `go test` equivalent (when you need extra flags the make target
doesn't expose):

```sh
KIND_KEEP_CLUSTER=true \
  go test ./tests/integration/singlecluster/controller/omenative_coordination_kind/... \
  -v -timeout 30m
```

## Cross-suite dependencies

- The fake-serving image build is content-hash cached so re-runs skip
  the docker build when the source hasn't changed.
- `tests/integration/dep-crds/prometheus-operator/` PodMonitor CRD is
  applied at suite startup (the OMENative reconciler hard-requires it).
- `tests/integration/dep-crds/scheduler-plugins/` PodGroup CRD is
  applied so multi-pod topology and gang object wiring run end to end.
- This suite deliberately keeps KIND's default scheduler. The separate
  `ome_scheduler_kind` suite owns custom-scheduler admission and topology
  placement so the large rollout suite does not duplicate that setup.
- The suite uses the production controller code paths (no mocks /
  fakes other than the fake-serving HTTP server) — bugs caught here
  reflect real controller behavior.

## Adding a new spec

1. Use `newCoordFixture` + `applyFixture` + `deferFixtureCleanup` from
   `helpers_test.go`. The fixture wires the standard CBM/CSR/ISVC
   shape, plus the node-label fix
   (`kindutil.LabelClusterBaseModelReady`) for OME pod-injection
   scheduling.
2. Use `kindLongTimeout` / `kindMediumTimeout` / `kindPollInterval`
   from `timeouts_test.go` (5min / 2min / 100ms) — generous enough for
   slow pod startup on a single-node cluster + suite contention.
3. If the test images need to differ (in-place tests need distinct
   digests, not aliases), use `kindutil.ImageBuildSpec.BuildArgs`
   threaded through `suite_test.go` to give each tag a real content
   diff (see `inplace_kind_test.go` for the pattern).
