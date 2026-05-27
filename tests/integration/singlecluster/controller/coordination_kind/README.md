# coordination_kind

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
| BlueGreen converge | behavior_d_bluegreen D-5 |
| RollingUpdate + RecreatePod | behavior_b_recreatepod B-1/2/3 |
| BlueGreen + RecreatePod | recreate_pod RP-1/2/3 |
| BlueGreen + RatioBalanced small | behavior_c_ratiobalanced C-1/4 |
| BlueGreen + RatioBalanced load | behavior_h_loadtest H-1/2/3 |
| Pause/resume Idle | behavior_g_pause G-1/2 |
| Pause/resume mid-rollout | behavior_d_pause_midrollout D-7 |
| Single-replica safe default | behavior_i_single_replica I-1 |
| In-place opt-in | behavior_l_inplace L-1/2/3 |
| Sequential | behavior_d_sequential D-2 |
| Independent | behavior_d_independent D-4 |
| PerComponent maxSurge | behavior_e_percomponent_surge E-1 |
| Sequential.Soak wall-clock | behavior_g_sequential_soak G-4 |
| PD-disaggregated isolation | pd_isolation + pd PD-1/5/7 |
| PD three-way (Engine+Decoder+Router) | pd_three_way PDX-1..7 |
| PD concurrent ops | pd_concurrent PDA-1/2 |
| Annotation-driven migration | migrate (8 specs) |
| ObservedGeneration + per-Instance counters | status_surface + status |
| Event recorder firing | events (7 specs) |
| Pod-state watch filters (label, cross-ISVC) | watches |
| Scale up/down + pod lifecycle | lifecycle |
| In-place update edge cases | update |
| State-machine interleavings | interleave INT-5 + interleave_extra INT-1..4/6 |
| Runtime+model resolution (CBM/CSR stall, missing CSR) | runtime_resolution RT-4/5 |
| Partial failure recovery | partial_failure PF-1/2/3 |
| Bad-image failure → Failed | behavior_x_bad_image X-1 |
| ISVC delete cascade (pods/Services/CRs) | cleanup_lifecycle LIFE-DEL |
| Per-pod headless DNS + EndpointSlice hostname | peer_discovery PEER-DNS |
| Per-revision Service emission + drift correction | producer_services SVC-1/2 |
| Status.Components.Traffic[] steady + mid-bump | producer_traffic TRAFFIC-1/2 |
| No RolloutCoordinationReady stamped without coord block | regression REG-1+10 |

## Running locally

```sh
make test-kind                                  # full suite
GINKGO_FOCUS='PD per-Component' make test-kind  # narrow to specific specs
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
  go test ./tests/integration/singlecluster/controller/coordination_kind/... \
  -v -timeout 30m
```

## Cross-suite dependencies

- The fake-serving image build is content-hash cached so re-runs skip
  the docker build when the source hasn't changed.
- `tests/integration/dep-crds/prometheus-operator/` PodMonitor CRD is
  applied at suite startup (the OMENative reconciler hard-requires it).
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
   diff (see L-* for the pattern).
