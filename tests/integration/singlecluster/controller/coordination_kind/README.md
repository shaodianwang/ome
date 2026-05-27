# coordination_kind

KIND-backed twin of the envtest `coordination/` suite. Each spec spins
up a single-node KIND cluster, builds the `fake-serving` image, starts
an in-process controller-runtime manager pointed at the kind apiserver,
and runs Ginkgo specs against real kubelet + scheduler + containerd —
the things the envtest sibling's `PodKubeletSimulator` can't model.

## Why KIND in addition to envtest

The envtest suite is fast (~15s per spec, ~10 min total) and runs unit-
style state-machine assertions against a kube-apiserver+etcd plus the
`PodKubeletSimulator` (`tests/integration/util/simulators.go`) that
brings every pod to `ContainersReady=True` instantly. That's exactly
the right tool for testing pure-function state transitions but it hides
a class of bugs that only surface when pods take real time to start,
drain races have real windows, and kube-proxy actually publishes
EndpointSlices.

This suite covers the contracts where real-kubelet timing matters. Six
bugs were caught here while porting / writing specs:

| Bug | Fix PR |
|---|---|
| `ome.io/rollout-paused` in revision hash → spurious surge cycle | #229 |
| OMENative drain via headless Service (with `PublishNotReadyAddresses=true`) → in-place / recreate hang forever | #230 |
| Top-level `EngineReady` / `DecoderReady` / `RouterReady` conditions not propagated from OMENative sub-status | #228 |
| `podRuntimeImagesMatch` string-compares `fake-serving:v2` vs `docker.io/library/fake-serving:v2` → in-place update hangs | #234 |
| Sequential dispatcher gate missing → concurrent recreates ignored declared Order | #235 |
| Per-Instance failure not lifted to `Component.Failed` → BlueGreen state machine never marks `Phase=Failed` | #236 |

## When to pick envtest vs KIND

**envtest** is authoritative for:

- Pure state-machine transitions (`coordination/state_test.go`,
  `ratio_test.go`, `sequential_event_test.go`, `status_test.go`).
- Unit-style invariants on the dispatcher gates (`ops/dispatch_test.go`,
  `ops/timeouts_test.go`, `ops/update_test.go`).
- Webhook validation (`webhook/...` envtest suites).
- Anything that needs to inject specific status / failure conditions
  that real kubelet wouldn't produce naturally.

**KIND** is authoritative for:

- Pod-lifecycle timing (drain races, recreate ordering, in-place
  patch convergence, post-recreate `serving=True` flip).
- Per-revision Service convergence (kube-proxy actually publishes
  EndpointSlices).
- Sequential active-Component handoff (the soak window is observable).
- RatioBalanced gate at scale (in-flight projection + parallel drains).
- Failure escalation through real `ImagePullBackOff` (no simulator
  failure-injection hook needed).
- Pod-UID rotation (recreate vs in-place).

The envtest behavior_b/c/d/g/h/i/l files have direct KIND counterparts
here. We keep BOTH because envtest gives a faster feedback signal on
every PR (~15s/spec) and KIND catches the timing-sensitive class
envtest can't. Don't delete envtest specs that have KIND mirrors —
each spec's headline comment notes when KIND is the authoritative
coverage and envtest is the fast-loop smoke.

## Spec map

| Contract | envtest | KIND | Authoritative |
|---|---|---|---|
| BlueGreen converge | behavior_d_bluegreen D-5 | behavior_d_bluegreen_kind D-5 | KIND (timing) |
| RollingUpdate + RecreatePod | behavior_b_recreatepod B-1/2/3 | behavior_b_recreatepod_kind B-1/2/3 | KIND (timing) |
| BlueGreen + RecreatePod | — | recreate_pod_kind RP-1/2/3 | KIND |
| BlueGreen + RatioBalanced small | behavior_c_ratiobalanced C-1/4 | behavior_c_ratiobalanced_kind C-1/4 | KIND (timing) |
| BlueGreen + RatioBalanced load | behavior_h_loadtest H-1/2/3 | behavior_h_loadtest_kind H-1/2/3 | KIND (timing + scale) |
| Pause/resume Idle | behavior_g_pause G-1/2 | behavior_g_pause_kind G-1/2 | KIND (timing) |
| Pause/resume mid-rollout | — | behavior_d_pause_midrollout_kind D-7 | KIND |
| Single-replica safe default | behavior_i_single_replica I-1 | behavior_i_single_replica_kind I-1 | KIND (timing) |
| In-place opt-in | behavior_l_inplace L-1/2/3 | behavior_l_inplace_kind L-1/2/3 | KIND (UID rotation) |
| Sequential | (state-machine unit) | behavior_d_sequential_kind D-2 | KIND |
| Independent | (state-machine unit) | behavior_d_independent_kind D-4 | KIND |
| PerComponent maxSurge | (deferred) | behavior_e_percomponent_surge_kind E-1 | KIND |
| Sequential.Soak wall-clock | (state-machine unit) | behavior_g_sequential_soak_kind G-4 | KIND (PIt — soak-gate bug) |
| PD-disaggregated isolation | pd_test PD-2/3/4/8 | pd_isolation_kind | both (fast feedback in envtest, real timing in KIND) |
| ObservedGeneration + per-Instance counters | status_test | status_surface_kind | both |
| Partial failure recovery | partial_failure_test PF-1/2/3 | partial_failure_kind PF-1/2/3 | KIND (real pod-delete) |
| Bad-image failure → Failed | (PIt — sim limitation) | behavior_x_bad_image_kind X-1 | KIND only |
| Rapid spec bumps + CR retention | interleave_test INT-5 | interleave_kind INT-5 | both |
| Top-level conditions plumbing | (separate unit) | top_level_status_test STAT-1/2 | KIND |
| Per-Component URL plumbing | (separate fix needed) | top_level_status_test STAT-3 | KIND (PIt) |
| ISVC delete cascade (pods/Services/CRs) | — | cleanup_lifecycle_kind LIFE-DEL | KIND (kube-GC required) |
| Per-pod headless DNS + EndpointSlice hostname | — | peer_discovery_kind PEER-DNS | KIND (EndpointSlice controller required) |
| Per-revision Service emission + drift correction | — | producer_services_kind SVC-1/2 | KIND |
| Status.Components.Traffic[] steady + mid-bump | — | producer_traffic_kind TRAFFIC-1/2 | KIND (image-bump revision rotation) |
| No RolloutCoordinationReady stamped without coord block | — | regression_kind REG-1+10 | KIND |

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
  applied at suite startup (the OMENative reconciler hard-requires it
  after PR #231).
- The suite uses the production controller code paths (no mocks /
  fakes other than the fake-serving HTTP server) — bugs caught here
  reflect real controller behavior.

## Adding a new spec

1. Mirror the envtest sibling's file name (`behavior_<x>_kind_test.go`)
   so the cross-reference is obvious.
2. Use `newCoordFixture` + `applyFixture` + `deferFixtureCleanup` from
   `helpers_test.go`. The fixture wires the same CBM/CSR/ISVC shape
   the envtest suite uses, plus the node-label fix
   (`kindutil.LabelClusterBaseModelReady`) for OME pod-injection
   scheduling.
3. Use `kindLongTimeout` / `kindMediumTimeout` / `kindPollInterval`
   from `timeouts_test.go` (5min / 2min / 100ms) — generous enough for
   slow pod startup on a single-node cluster + suite contention.
4. If the test images need to differ (in-place tests need distinct
   digests, not aliases), use `kindutil.ImageBuildSpec.BuildArgs`
   threaded through `suite_test.go` to give each tag a real content
   diff (see L-* for the pattern).
