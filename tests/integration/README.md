# Integration tests

`make test-kind` runs only the coordination suite under
`tests/integration/singlecluster/controller/omenative_coordination_kind/`. It
needs Docker on the host and a `kind` binary on PATH or at `bin/kind` (install
the pinned version with `make kind`). `KIND_KEEP_CLUSTER=true` is
on by default so re-runs are fast; set `KIND_RECREATE=true` to force
a fresh cluster. Narrow with `GINKGO_FOCUS='regex'`.

## The suites

Integration suites live under `tests/integration/` and split into two halves:

```
tests/integration/
├── dep-crds/                              # vendored optional-CRD YAMLs (LWS, KEDA, Gateway API, scheduler-plugins, Envoy Gateway, Prometheus)
├── framework/                             # shared test scaffolding (envtest)
├── kindutil/                              # shared test scaffolding (KIND) + fake-serving image source
├── components/, util/                     # helpers
└── singlecluster/
    ├── webhook/{inferenceservice,basemodel,servingruntime,pod,runtimepreset,runtimerevision,acceleratorquota,acceleratorquota_cert}
    ├── controller/{inferenceservice,basemodel,servingruntime,runtimerevision,acceleratorquota,acceleratorquota_capacity,omenative_coordination_kind,alfred_kind}
    ├── clustercache/
    └── agents/metadata/
```

Each leaf directory is its own Ginkgo suite with a single `TestXxx(t)` entry that calls `util.RunSuite` (envtest) or `kindutil.RunKindSuite` (KIND).

### envtest suites — fast, hermetic, run by `make integration-test`. Use them for: webhooks, CRD/RBAC defaulters, BaseModel/ServingRuntime/RuntimeRevision reconcilers, ClusterCache, agent-side metadata. NOT used for OMENative reconciler behavior — that's KIND territory now (the envtest `coordination/` + `omenative/` suites were deleted because PodKubeletSimulator races made them chronically flaky).

```sh
make integration-test                               # full envtest suite
KUBEBUILDER_ASSETS="$(bin/setup-envtest use "$(cat .envtest-k8s-version)" -p path)" \
  go test ./tests/integration/singlecluster/webhook/inferenceservice/... -v   # focused
```

`KUBEBUILDER_ATTACH_CONTROL_PLANE_OUTPUT=true` forwards the envtest kube-apiserver/etcd stdout+stderr into test logs when debugging control-plane startup failures.

### KIND suite (`omenative_coordination_kind/`) — real kubelet + scheduler + containerd via Docker. Authoritative for: cross-Component rollout coordination, pod-lifecycle timing (drain races, recreate ordering, in-place patch convergence), per-revision Service convergence, Sequential handoff timing, RatioBalanced gate at scale, ImagePullBackOff failure escalation, pod-UID rotation, migration, PD-disaggregated topologies. ~80 spec files, ~5 min/file wall-clock.

```sh
make test-kind                                       # full suite (~30-60 min)
GINKGO_FOCUS='PD per-Component' make test-kind       # narrow to specific specs (~5-10 min)
GINKGO_FOCUS='RecreatePod' make test-kind            # narrow by It-string fragment
make test-kind-clean                                 # delete leftover KIND clusters
```

Requirements:
- Docker daemon running on the host (KIND brings up the cluster as a Docker container).
- `kind` binary on `PATH` or at `bin/kind`; run `make kind` to install the pinned version.
- A throwaway `KUBECONFIG` — these targets create the cluster in whatever the caller exports. See **Kubeconfig Hygiene** above.

`KIND_KEEP_CLUSTER=true` is on by default in `make test-kind` — the cluster persists between runs for fast iteration. Set `KIND_RECREATE=true` to force a fresh cluster (use after suspected dirty state).

Direct `go test` (when you need extra flags):

```sh
KIND_KEEP_CLUSTER=true \
  PATH="$(pwd)/bin:$PATH" \
  go test ./tests/integration/singlecluster/controller/omenative_coordination_kind/... \
  -v -timeout 30m -ginkgo.v -ginkgo.focus 'rollout converges'
```

KIND specs use generous timeouts (`kindShortTimeout=30s`, `kindMediumTimeout=2m`, `kindLongTimeout=5m`) and a 100ms poll interval — tuned for real kubelet latency, not envtest microseconds.

CI runs the suite through the hard-gating `integration-test-kind-gke` group. The unit-test lane compiles one exact-commit test binary, and named semantic jobs run disjoint file groups in separate three-node KIND clusters on GKE agents. The TW dind cannot provide the bridge networking KIND requires.

**Every new KIND spec file must be classified** into a group in `KIND_SPEC_GROUPS` in `.buildkite/generate_pipeline.py` — the generator enforces exact ownership and rejects unowned (or stale) specs, so an unclassified `*_kind_test.go` fails pipeline generation for every build. Validate locally with `python3 -m unittest discover -s .buildkite -p 'test_*.py'` before pushing.

### Alfred suite (`alfred_kind/`) — deploys the alfred image built from `.alfred-oss-ref` through the ome-resources alfred templates into a three-worker cluster with fake GPU nodes, and drives it black-box against the in-process controller: node-health and maintenance signals are patched onto Nodes, and the outcome is read back through alfred's metrics, events, recommendations and journal ConfigMaps, the ISVC migration annotations and InferenceReplica status. `make test-kind-alfred` (set `OME_ALFRED_IMAGE` to skip the local image build). New spec files must be classified in `ALFRED_KIND_SPEC_GROUPS` in `.buildkite/generate_pipeline.py`.

### Multi-cluster real-Kueue suite (`tests/integration/multicluster/`, gated by `OME_MC_REALKUEUE=1`) — validates OEP-0018 single-active placement against a *real* Kueue admission stack. The default `make integration-test` runs this suite's **sim** path (`admitDerivedOn`); with the env gate it stands up 1 control-plane + 2 member envtest apiservers in one process, each member running a full OME manager + Kueue (core controllers + scheduler + pod webhook), and drives fan-out → real quota-gated admission → race → loser-sweep for real (no kubelet — admission = Kueue scheduling-gate removal). Run it via:

```sh
make mc-realkueue                          # full suite, opt-in
GINKGO_FOCUS='S1|S3' make mc-realkueue     # narrow to specific scenarios
```

Opt-in / soft-fail (it's heavy: 3 apiservers + 3 OME managers + 2 Kueue managers in-process). Loser-sweep assertions check the placer's contract — derived **deleted or Terminating** — not full reaping, because the member's finalizer teardown (IR + Kueue-gated pods) is eventually-consistent and lags under load.
