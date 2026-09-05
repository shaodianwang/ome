# OEP-0020: Integration test framework v2 — multi-cluster, performance, conflict gate

<!-- toc -->
- [Summary](#summary)
- [Motivation](#motivation)
  - [The failure modes we are shipping today](#the-failure-modes-we-are-shipping-today)
  - [Goals](#goals)
  - [Non-Goals](#non-goals)
- [Proposal](#proposal)
  - [Part 1: local multi-cluster testing (N envtest in one process)](#part-1-local-multi-cluster-testing-n-envtest-in-one-process)
  - [Part 2: a local performance / scale harness](#part-2-a-local-performance--scale-harness)
  - [Part 3: builder / wrapper ergonomics](#part-3-builder--wrapper-ergonomics)
  - [Part 4: a write-conflict observed-logs gate](#part-4-a-write-conflict-observed-logs-gate)
  - [Part 5: KIND / CI hygiene](#part-5-kind--ci-hygiene)
- [Design Details](#design-details)
- [Test Plan](#test-plan)
- [Graduation Criteria](#graduation-criteria)
- [Drawbacks](#drawbacks)
- [Alternatives](#alternatives)
- [Open Questions](#open-questions)
<!-- /toc -->

## Summary

OME's integration test suite is strong at exactly one thing — single-cluster
controller behavior — and structurally blind everywhere else. This OEP brings the
suite up to the bar set by Kueue's test infrastructure (`sigs.k8s.io/kueue`), the
reference codebase for the same external-reconciler / multi-cluster shape OME is
adopting in OEP-0018.

The five parts, each tied to a failure mode named in the next section: (1) **local
multi-cluster testing** by running N envtest control planes in one process and
faking the cross-cluster connection with a `RestConfigToKubeConfig` kubeconfig-Secret
round-trip — so the remote transport finally runs against a real apiserver; (2) a
**local performance / scale harness** — envtest plus a stripped manager plus a
fake-executor reconciler plus a checked-in `rangespec.yaml` threshold checker — so a
reconcile-throughput regression fails a `go test` instead of a production rollout;
(3) **builder / wrapper ergonomics** — Status setters, `CreateAndWait` helpers,
domain condition matchers, a global `Eventually` default — so a new spec is ten lines,
not eighty; (4) a **write-conflict observed-logs gate** that turns a silent
optimistic-lock retry storm into a hard test failure; and (5) **KIND / CI hygiene** —
promote the IR-managed KIND suite into CI when Docker lands, prune dead `PIt`/`Skip`
stubs, config-drive the host-sensitive timing slacks, and codify the rule that new
pod-lifecycle assertions go to KIND, not envtest.

This is a test-infrastructure OEP. It adds no CRD, no API surface, and no controller
behavior. Its blast radius is the `tests/` tree and a small set of test-only
exported options on the controllers under test.

## Motivation

### The failure modes we are shipping today

A design doc must lead with what breaks. Here is what breaks.

**Failure mode 1 — the multi-cluster transport is unverified against any real
apiserver.** OEP-0018 adds two controllers — `placement` (the fan-out / race
controller, `pkg/controller/v1beta1/placement/`) and `workloadcluster` (the
cross-cluster connection manager, `pkg/controller/v1beta1/workloadcluster/`) —
totaling roughly 1.6k LOC. Both are tested **only** by package-local
fake-client unit tests: every test in `placement/controller_test.go` and
`workloadcluster/controller_test.go` builds its workers with
`fakeclient.NewClientBuilder()` (e.g. `placement/controller_test.go:75,100`;
`workloadcluster/controller_test.go:70`). A fake client is an in-memory map: it
never serializes a kubeconfig, never dials a server, never runs an admission
webhook, never returns a real optimistic-lock conflict, and never exercises the
`clientcmd` → `client.NewWithWatch` connection path the production code takes
(`workloadcluster/manager.go:105`). The remote transport — the single
highest-risk subsystem OEP-0018 introduces — has therefore **never run against a
real apiserver in any test**. OEP-0018's own Test Plan declares a
"Multi-cluster e2e (KIND, ≥2 clusters), **authoritative**" suite and makes it part
of the Alpha graduation bar (`oeps/0018-multi-cluster-placement/README.md:240,244`).
That suite does not exist. We are one cross-cluster serialization bug away from a
silent placement failure that no test can catch.

**Failure mode 2 — there is no performance or regression gate of any kind.** OME
has modest-scale correctness specs and a handful of unit-level `Benchmark`s, but
nothing that drives reconcile throughput at scale and asserts a latency or
utilization bound. The research baseline for this OEP (a 2026-06-26 code study
of Kueue's MultiKueue implementation against OME's multi-cluster controllers)
found the multi-cluster code ships at client-go default rate limits (5 QPS / 10
burst), placement at
`MaxConcurrentReconciles` 1, and a 30s status poll — exactly the class of scaling
choice a perf harness exists to catch *before* it reaches a 3-cluster production
fleet. Without a gate, a 2× reconcile-latency regression merges green and is
discovered by an operator, not by CI.

**Failure mode 3 — local OMENative pod-lifecycle testing rests on a flaky
simulator, and we already paid for it.** envtest has no kubelet, so OMENative pods
never reach `ContainersReady` on their own. The suite synthesizes that with
`PodKubeletSimulator` (`tests/integration/util/simulators.go`), a ~570-LOC,
100ms-poll goroutine that best-effort-stamps pod conditions via `Status().Update`
and swallows the resulting conflicts (`simulators.go:281`, "a conflict on the next
tick retries naturally"). That best-effort-poll-against-a-racing-reconciler shape
is inherently timing-dependent. It already cost us the envtest `coordination/` and
`omenative/` suites: they were **deleted** because the simulator's races made them
chronically flaky (the suite documentation records the deletion and its cause).
The consequence is a coverage cliff: authoritative
pod-lifecycle behavior now lives only in KIND, which is a soft-fail step outside the
default CI gate.

**Failure mode 4 — write conflicts are invisible.** OME's house guidance is to
merge cross-resource references at reconcile time, which makes optimistic-lock
conflicts a normal, expected event under contention. Today a controller that loses
a write race simply logs an error and retries; nothing in the suite asserts the
*absence* of a conflict storm. A reconciler that thrashes — two controllers writing
the same object every reconcile — passes every functional test while burning the
apiserver. We have no tripwire.

**Failure mode 5 — apparent coverage overstates real coverage.** The KIND suites
carry 13 `ginkgo.PIt` placeholders (pending specs that compile and report as specs
but execute nothing — e.g. the entire `bluegreen_kind_test.go` failure block,
`:95,98,100`) and 8 dead `Skip`s left behind by OEP-0017's removal of the Sequential
policy (e.g. `webhook/inferenceservice/coordination_test.go:239,246,253`). A reader
counting specs over-counts what is actually asserted. Separately, the KIND timing
slacks are magic literals baked into Go (`kindShortTimeout=30s` etc.), so a slow CI
host can only be accommodated by editing code — a hard-blocker under the
repository's developer guidelines (no behavioral magic values in the binary)
that this suite currently violates.

### Goals

- **Run the OEP-0018 multi-cluster control loop against real apiservers, locally,
  with no Docker.** Cover connection, kubeconfig validation, fan-out dispatch, and
  status flow-back — plus the negative paths (broken kubeconfig → expected
  `WorkloadCluster` condition).
- **Catch reconcile-performance regressions in CI** with a calibrated, checked-in
  threshold file, runnable as a plain `go test`.
- **Cut the cost of writing a new integration spec** to the point where the
  boilerplate (create object → poll → read a condition) is a single helper call.
- **Make a write-conflict storm a hard failure**, not a log line.
- **Stop inflating coverage**: prune dead specs, move timing knobs into config, and
  write down where each kind of assertion belongs.

### Non-Goals

- **No new CRD, API field, or controller behavior.** This OEP is test
  infrastructure. If a fix to the controllers under test is needed to make them
  testable (e.g. a swappable client source), it is the *minimum* test seam, not a
  behavior change — and it is called out explicitly.
- **Not the OEP-0018 performance *fixes* themselves.** This OEP builds the harness
  that *measures* placement throughput and the suite that *exercises* the transport;
  raising QPS, adding a watch-funnel, or wiring the selectively-caching client are
  OEP-0018 follow-ups. We build the ruler, not the thing it measures.
- **No full e2e against cloud clusters.** Real multi-region GSLB / Gateway endpoint
  publication (OEP-0018's largest open question) stays out; the envtest suite covers
  the control loop, not the global-traffic backend.
- **We are not deleting `PodKubeletSimulator`.** It is load-bearing for the suites
  that remain in envtest. The goal is to *bound* its use, not remove it.

## Proposal

Terms used throughout, defined once:

- **envtest** — controller-runtime's in-process test control plane: a real
  `kube-apiserver` + `etcd`, no kubelet, no scheduler. OME already depends on it
  (`KUBEBUILDER_ASSETS`).
- **control plane cluster / workload cluster** — OEP-0018's roles: the control
  plane runs the fan-out controller; a workload cluster runs the cloned ISVC and its
  Kueue gate. In a test, each is a separate envtest apiserver.
- **fake-executor** — a stand-in reconciler that completes a workload's lifecycle on
  a timer (flips readiness / admission) instead of running real pods, so a scheduler
  or placement loop can be driven at scale without a kubelet.
- **rangespec** — a checked-in YAML file of pass/fail performance bounds, compared
  against a harness run's emitted summary by a plain `go test`.

### Part 1: local multi-cluster testing (N envtest in one process)

**Why.** Failure mode 1: the cross-cluster transport is unverified. A fake client
cannot catch a kubeconfig-serialization bug, a missing-Secret handling bug, a CA
validation bug, or a status-flowback bug, because it bypasses the entire transport.

**The contract.** A new suite under `tests/integration/multicluster/` stands up
**three real envtest apiservers in one `go test` process** — one control plane, two
workload clusters — and drives the actual `placement` + `workloadcluster`
controllers across them. The cross-cluster connection is faked **at the kubeconfig
layer, not the client layer**: each workload cluster's `rest.Config` is serialized
to a kubeconfig, stored in a Secret, and a `WorkloadCluster` CR is pointed at that
Secret — so the production code loads it via `clientcmd` and `client.NewWithWatch`
exactly as it would in a real fleet. The remote transport runs for real; only the
*topology* (three apiservers in one process) is synthetic.

**The mechanism, lifted from Kueue.** Kueue's MultiKueue integration suite is the
direct template, and it is a near-drop-in fit because OME's framework already mirrors
Kueue's lifecycle shape (`framework.Init` / `SetupClient` / `StartManager` /
`Teardown`, `tests/integration/framework/framework.go:113,176,216`,
`manager.go:61`).

1. **N apiservers, one process.** Kueue's `suite_test.go` declares one `cluster`
   struct per apiserver (`managerTestCluster`, `worker1TestCluster`,
   `worker2TestCluster`, `multikueue/suite_test.go:90-95`), each holding its own
   `framework.Framework` and its own `cfg *rest.Config`
   (`multikueue/suite_test.go:71-78`), and boots them in parallel in `BeforeSuite`
   with a `sync.WaitGroup` (`:420-436`). OME's framework supports this unchanged: a
   `Framework{}` value owns one envtest, and OME already constructs them
   per-suite.
2. **The kubeconfig round-trip is ~6 LOC.** Kueue's `RestConfigToKubeConfig`
   serializes a `rest.Config` (host + CA + client cert/key) into kubeconfig bytes
   via `clientcmd.Write` (`pkg/util/testing/kubeconfig.go:102-107`); the MultiKueue
   suite calls it through `cluster.kubeConfigBytes()`
   (`multikueue/suite_test.go:80-82`). OME needs the same helper. *No-magic-values
   note:* the cluster/user/context **names** in that helper
   (`"default-cluster"`, `"default-user"`, `"default-context"`) are internal
   kubeconfig keys, not behavioral or user-facing values — they never leave the test
   process — so a fixed identifier is acceptable here. There is no production default
   to leak.
3. **The CR points at the Secret; the real code does the rest.** The test writes the
   kubeconfig bytes into a Secret and a `WorkloadCluster` referencing it, then asserts
   the controller connects, validates, and reports a healthy condition — the genuine
   connection path, not a fake.
4. **Negative paths get real validation.** Because the certs are real, broken
   kubeconfigs produce real errors. Kueue's `setup_test.go:221,449` deliberately
   feeds missing-Secret, CA-as-path, tokenFile, and insecure kubeconfigs and asserts
   the cluster CR's reason — which is exactly the threat model
   `workloadcluster/connection.go` already implements. The fake-client tests
   can never reach this code.

**The one required test seam (called out, not silent).** To wire `placement` +
`workloadcluster` into `tests/integration/components/` the way every other
controller is wired (`components.SetupInferenceService` and peers take functional
options today, `components/inferenceservice.go:55`), the `workloadcluster` manager
needs a **swappable client source** and its timing knobs (poll interval, per-call
timeout) as **functional options** — so a suite can inject the three in-process
apiservers and shrink the 30s poll to test cadence. This mirrors Kueue's
`WithGCInterval` / `WithWorkerLostTimeout(3s)` / `WithEventsBatchPeriod` options
(`multikueue/suite_test.go:398-404`). This is a test seam, not a behavior change: the
production default stays config-driven and absent options preserve current behavior.

**The gotcha to document.** Kueue guards `fwk.Init` + `SetupClient` with a package
`sync.Mutex` because scheme registration into a shared `runtime.Scheme` is not
thread-safe (`multikueue/suite_test.go:97-100,123-128`). A parallel three-apiserver
boot in OME must do the same or risk a data race on the scheme.

### Part 2: a local performance / scale harness

**Why.** Failure mode 2: no regression gate. A perf number is only useful if a
*machine* checks it on every change; a benchmark a human runs occasionally is not a
gate.

**The contract.** A harness under `tests/performance/` (mirroring Kueue's
`test/performance/scheduler/`) that: boots an envtest apiserver, runs a **stripped
OME manager** containing only the controller(s) under test, runs a **fake-executor**
that completes ISVC/Instance readiness on a timer (no kubelet, no real pods),
generates a configurable load, records per-class latency and throughput, writes a
summary, and is graded by a separate `go test` against a checked-in `rangespec.yaml`.

**The mechanism, lifted from Kueue.**

1. **envtest boot inside `main`, with raised apiserver limits.** Kueue's runner
   starts `envtest.Environment` directly in `main.go` and bumps
   `max-requests-inflight` / `max-mutating-requests-inflight` to 5000/2500 because
   the default 400/200 throttles a high-N run into `http2: client connection lost`
   (`test/performance/scheduler/runner/main.go:117-131`). OME's harness must do the
   same; the limits are flags, not literals.
2. **A stripped manager = the controllers, nothing else.** Kueue runs `minimalkueue`
   (`runner/minimalkueue/main.go`) — core controllers + scheduler only, no optional
   components — so a CPU profile is not flooded by unrelated reconcilers
   (`test/performance/scheduler/README.md:17-21`). OME's analog is an ISVC-reconciler
   harness main that wires only `InferenceServiceReconciler` (and, for the MC sweep,
   `placement`).
3. **A fake-executor reconciler flips state on a timer.** Kueue's runner controller
   watches Workloads and, on a `RequeueAfter` of the workload's configured running
   time, sets it Finished — no pods involved
   (`runner/controller/controller.go:111-155`). OME's fake-executor flips ISVC /
   Instance readiness the same way: it stands in for both the real kubelet *and* the
   `PodKubeletSimulator`, so the perf harness is immune to failure mode 3's flake.
4. **A bounded, drop-on-full recorder.** Kueue's recorder pushes events onto a
   buffered channel and **drops on full** rather than blocking a reconcile
   (`runner/recorder/recorder.go:152` buffer=1000; `:464-469` the `select`/`default`
   drop), then reports the dropped count in the summary (`:278`,
   `DroppedWLEvents`). This is the right shape: instrumentation must never throttle
   the thing it measures. OME's recorder copies it.
5. **A checked-in `rangespec.yaml`, checked by a plain `go test`.** Kueue's
   `checker_test.go` unmarshals the run summary + command stats + a `RangeSpec`, then
   fails if wall time exceeds `maxWallMs`, utilization is below a floor, or
   time-to-admission exceeds a ceiling
   (`test/performance/scheduler/checker/checker_test.go:44-117`). The thresholds
   carry their **calibration recipe in comments** — averaged over 5 trial runs, set
   at mean +20% for time bounds and roughly mean −7% for utilization floors
   (`configs/baseline/rangespec.yaml:1-26`). OME adopts both the checker and the
   recipe; the rangespec is config (checked-in YAML), so no threshold is baked into
   Go.

**What we measure first.** ISVC reconcile throughput at N services (single cluster),
and — once Part 1 lands — placement fan-out latency across the three in-process
clusters. The exact metrics and N are `TBD` until first calibration; the harness is
metric-agnostic by design (it records whatever the recorder is taught to record).

### Part 3: builder / wrapper ergonomics

**Why.** Not a correctness failure mode — a *velocity* one, with a correctness
side effect. Today every spec repeats Create → `Eventually` → `Get` →
`FindStatusCondition`. Boilerplate that long is copy-pasted, and copy-paste drifts:
two specs end up asserting "ready" slightly differently. Uniform helpers make specs
short *and* consistent.

**The contract.** Add to `tests/integration/util` the wrapper layer OME lacks,
modeled on Kueue's:

1. **Status setters on the builders.** OME's fluent ISVC/BaseModel builders cover
   spec but not status. Kueue's wrappers seed defaults so there are no nil-panics and
   expose status setters (`pkg/util/testing/v1beta2/wrappers.go`); OME's builders
   need the same so a test can assert against a known status without a manual
   `Status().Update`.
2. **`await` / `CreateAndWait` helpers.** A single helper that creates an object and
   blocks until it is Ready (or deleted). Kueue's `ExpectObjectToBeDeleted[…]`
   (`test/util/util.go:213`) is the model; OME adds the create-and-wait-ready dual.
3. **Domain condition matchers.** Kueue ships Gomega matchers
   `HaveConditionStatusTrue` and `HaveConditionStatusTrueAndReason`
   (`pkg/util/testing/condition_matchers.go:48,52`) and error matchers like
   `BeNotFoundError` (`pkg/util/testing/error_matchers.go:27`). OME's equivalents —
   `HaveISVCConditionTrue`, `BeNotFoundError` — turn a five-line condition scan into
   one matcher and produce a readable failure message via `FailureMessage`.
4. **A global `Eventually` default.** OME already defines three timeout tiers and a
   poll interval (`tests/integration/util/constants.go:16-30`) but each spec still
   passes them explicitly. Setting `gomega.SetDefaultEventuallyTimeout` /
   `…PollingInterval` once in `RunSuite` lets the common case drop the arguments
   entirely; Kueue pairs this with `GinkgoHelper()` in its helpers so failures point
   at the spec, not the helper (`test/util/util.go:364,434`). The default belongs in
   one place, config-driven from the existing constants — not re-typed per call.

### Part 4: a write-conflict observed-logs gate

**Why.** Failure mode 4: a conflict storm is invisible. OME's controller-side-merge
guidance makes optimistic-lock conflicts expected under contention, so the signal we
care about is not "did a conflict ever happen" but "did the suite *end* with a
controller thrashing." An end-of-suite assertion catches exactly that.

**The contract.** Capture the manager's logs during the run; at `Teardown`, fail the
suite if any error-level log line is a write-conflict.

**The mechanism, lifted from Kueue.** Kueue installs a `zaptest/observer` core in
`Framework.Init`, holds the captured logs on the framework (`ObservedLogs
*observer.ObservedLogs`, `test/integration/framework/framework.go:91`), and in
`Teardown` calls `util.VerifyLogs(f.ObservedLogs)`, which asserts no `≥Error` entry
matches `IsWriteConflictError` — a string match for the apiserver's
optimistic-lock message (`pkg/util/logging/logging.go:77-83`). OME's framework does
**not** capture logs today (no `ObservedLogs` field exists), so this is a genuinely
new seam: add the observer in `framework.Init`, add the field, and add the
`VerifyLogs` call in `framework.Teardown`. Threshold note: a single transient
conflict on a contended object is normal; the gate should fire on a sustained
pattern, and the exact policy (any error vs. a count threshold) is `TBD` pending the
first run against OME's controllers — start strict (any matching error) and relax
only with evidence.

### Part 5: KIND / CI hygiene

**Why.** Failure mode 5 (overstated coverage) plus a no-magic-values hard-blocker
(magic timing values).

**The contract — five concrete cleanups:**

1. **Promote `irmanaged_kind` to CI when Docker lands.** The IR-managed path is the
   production default, and it has a KIND suite
   (`tests/integration/singlecluster/controller/irmanaged_kind/`), but CI runs only
   `omenative_coordination_kind`, soft-fail, because the CI agents expose no Docker
   daemon. When the CI agents get Docker, the default path's KIND suite
   must become a gating step. *Integration note for the maintainer:* this is a
   CI-pipeline + Makefile wiring change, owned outside this OEP's file scope.
2. **Prune the 13 dead `PIt` and 8 dead `Skip` stubs.** A `ginkgo.PIt("…", func(){})`
   that asserts nothing (e.g. `bluegreen_kind_test.go:31-37`) inflates the spec count
   without adding coverage; convert each to either a real spec or a removed line. The
   8 `Skip`s left by OEP-0017's Sequential-policy removal
   (`webhook/inferenceservice/coordination_test.go:239,246,253,278`;
   `validation_test.go:554,650`) reference a check that no longer exists — delete
   them.
3. **Config-drive the KIND timing slacks.** `kindShortTimeout=30s` /
   `kindMediumTimeout=2m` / `kindLongTimeout=5m` and the 100ms poll are tuned for one
   host's kubelet latency and are baked into Go. Per the no-magic-values rule (no
   behavioral magic values in the binary), move them to an env-overridable knob with
   the current value
   as the *config* default (not an in-code literal that silently wins), so a slow CI
   host needs no code edit.
4. **Codify "new pod-lifecycle assertions go to KIND, not envtest."** The
   `PodKubeletSimulator` flake (failure mode 3) is structural; the durable mitigation
   is a written rule, in the developer documentation and the suite docs, that any
   *new* assertion
   about drain ordering, recreate timing, in-place convergence, or readiness-gate
   flips lands in a KIND suite. envtest keeps webhooks, defaulters, and
   non-pod-lifecycle reconcile logic.
5. **Document the simulator's boundary.** Leave `PodKubeletSimulator` in place for
   the suites that need it, but annotate its envtest-only constraint at the call
   sites so the next author does not reach for it in a new coordination spec.

## Design Details

**Directory layout** (new; nothing existing moves):

```
tests/
├── integration/
│   ├── util/
│   │   ├── kubeconfig.go      # RestConfigToKubeConfig (Part 1)
│   │   ├── matchers.go        # domain condition + error matchers (Part 3)
│   │   └── await.go           # CreateAndWaitReady, status setters (Part 3)
│   ├── components/
│   │   ├── placement.go       # SetupPlacement(...opts) (Part 1)
│   │   └── workloadcluster.go # SetupWorkloadCluster(...opts) (Part 1)
│   ├── framework/             # +ObservedLogs capture & VerifyLogs in Teardown (Part 4)
│   └── multicluster/          # 1 control + 2 workers suite (Part 1)
└── performance/
    └── isvc/
        ├── runner/main.go     # envtest + stripped manager + fake-executor (Part 2)
        ├── runner/recorder/   # bounded drop-on-full recorder (Part 2)
        ├── checker/checker_test.go
        └── configs/baseline/rangespec.yaml
```

**Why envtest-N-in-one-process and not KIND for the MC suite.** KIND gives a real
kubelet but costs a Docker daemon and ~minutes per suite, and CI has no Docker today.
The MC transport's risk is in *serialization, connection, validation, and status
flow-back* — none of which need a kubelet. envtest gives a real apiserver for all
four at ~5s boot and zero Docker. The kubelet-dependent MC behavior (real Kueue
admission racing real pods) stays the job of the future authoritative KIND e2e that
OEP-0018 already specifies; this OEP makes the *control loop* testable now, in CI,
without waiting on Docker.

**Why a fake-executor and not the `PodKubeletSimulator` in the perf harness.** The
simulator is a correctness aid (synthesize a kubelet); the perf harness needs a
*scale* aid (complete thousands of workloads cheaply on a timer). Reusing the
simulator would re-import its flake into the one place that must be deterministic to
produce a stable threshold. Kueue keeps these separate for the same reason — its
perf runner has its own `controller.go`, not its integration helpers.

**No-magic-values audit (repository hard-blocker).** Every tunable this OEP
introduces — apiserver inflight limits, harness load size, rangespec thresholds,
KIND timing slacks, MC poll cadence — is a **flag or a checked-in config file**, with
no in-code default that silently wins. The single fixed-string exception (kubeconfig
cluster/user/context keys in `RestConfigToKubeConfig`) is internal to the test
process and carries no production default; it is flagged here rather than hidden.

## Test Plan

This OEP *is* test plan, so "test plan" here means how we validate the harness itself.

- **Part 1 self-check.** The MC suite is its own proof: it fails if the transport
  is broken. A meta-assertion confirms all three apiservers actually started
  (distinct hosts) and that a deliberately-broken kubeconfig yields the expected
  `WorkloadCluster` failure condition — proving the certs are real, mirroring Kueue's
  negative setup (`setup_test.go:221`).
- **Part 2 self-check.** Run the harness at a small N in CI as a smoke (does it boot,
  generate, record, and write a summary), and run the checker against a known-good
  summary fixture to prove the rangespec comparison actually fails on a regression.
  The full-scale run is a periodic / on-demand job, not the per-PR gate, because its
  wall time is minutes.
- **Part 3 self-check.** The matchers and helpers get unit tests (a matcher must fail
  with a readable message on the negative case, as Kueue's do via `FailureMessage`).
- **Part 4 self-check.** A targeted test installs a controller that intentionally
  thrashes one object and asserts the gate fires; and a normal run asserts it does
  not (no false positive on an expected single conflict).

## Graduation Criteria

- **Alpha:** Parts 1, 3, and 4 land. The MC envtest suite covers connection +
  fan-out dispatch + status flow-back + at least one negative kubeconfig path, and
  runs in the standard envtest CI gate. The conflict gate is on in `Teardown`. The
  builder/matcher layer exists and at least one existing suite is migrated to it as a
  worked example.
- **Beta:** Part 2 lands with a calibrated `rangespec.yaml` for ISVC reconcile
  throughput and a CI job that runs the checker; Part 5's prunes are done and the
  KIND timing slacks are config-driven. The "pod-lifecycle → KIND" rule is in the
  developer documentation.
- **Stable:** the MC perf sweep (placement fan-out across the three in-process
  clusters) is calibrated and gating; `irmanaged_kind` runs in CI as a gating step
  (gated on Docker availability on the agents).

## Drawbacks

- **More test infrastructure to maintain.** Three new subsystems (MC suite, perf
  harness, wrapper layer) are code that can rot. Mitigation: each mirrors a Kueue
  pattern that is already maintained upstream, so drift is diffable against a
  reference.
- **The required test seams touch production controllers.** Part 1 needs a swappable
  client source + functional-option timing knobs on `workloadcluster`. That is a real
  (if minimal) change to non-test code. Mitigation: options default to current
  behavior; absent options change nothing — but it is not zero-footprint, and we say
  so.
- **A perf gate can flake or false-positive on a noisy CI host**, the classic
  scalability-CI failure: *threshold-too-tight*. Mitigation: Kueue's averaged-over-5,
  set-at-+20% calibration recipe, and keeping the full run off the per-PR path until
  the threshold is proven stable.
- **envtest MC is not real MC.** It proves serialization / connection / dispatch /
  status, not kubelet- or scheduler-dependent behavior. We do not claim it replaces
  the authoritative KIND e2e; it precedes it.

## Alternatives

1. **Skip envtest MC; wait for the KIND ≥2-cluster e2e (OEP-0018's plan).**
   *Rejected.* KIND needs Docker, which CI lacks today, so this leaves the transport
   untested *now* — exactly failure mode 1 — for an unbounded wait. The envtest suite
   covers four of the five risk surfaces immediately and in CI; the KIND e2e remains
   the right home for the fifth (real Kueue admission racing real pods).
2. **Two real KIND clusters for the MC suite instead of N envtest in one process.**
   *Rejected for the control-loop suite.* Higher fidelity (real kubelet) at the cost
   of Docker + minutes/suite + no current CI home. The transport's risk does not need
   a kubelet, so paying for one buys fidelity we are not testing here. Kueue made the
   same call: its MultiKueue *integration* suite is N-envtest, with KIND reserved for
   `test/e2e`.
3. **A perf benchmark a human runs occasionally instead of a CI-gated checker.**
   *Rejected.* A number nobody checks is not a gate; failure mode 2 is precisely the
   absence of a *machine* check. The checked-in `rangespec.yaml` + `go test` makes the
   bound enforceable.
4. **Fix `PodKubeletSimulator`'s flake and bring the deleted envtest coordination
   suites back.** *Rejected as the primary path.* The flake is structural to
   best-effort polling against a racing reconciler; KIND already exists and is
   authoritative for pod lifecycle. Investing in a written "pod-lifecycle → KIND"
   rule (Part 5) is cheaper and durable than re-hardening a simulator we already
   retreated from once.
5. **Build a bespoke OME test framework from scratch rather than porting Kueue's
   patterns.** *Rejected.* OME's framework already converges on Kueue's lifecycle
   shape (`Init`/`SetupClient`/`StartManager`/`Teardown`); porting specific,
   battle-tested pieces (the kubeconfig round-trip, the recorder, the conflict gate)
   is lower-risk than reinventing them, and keeps the two diffable.

## Open Questions

- **What is the first perf metric and N for the ISVC harness?** `TBD` — settled by
  the first calibration run; the harness is metric-agnostic until then. Owner: TBD.
- **Conflict-gate policy: any error, or a count threshold?** `TBD` — start strict
  (any `IsWriteConflictError` at error level fails the suite) and relax only if a
  legitimately-contended object produces unavoidable single conflicts. Owner: TBD.
- **Does the MC perf sweep belong in this OEP or fold into OEP-0018's perf
  follow-ups?** The harness is generic; the *placement* scenario and its thresholds
  arguably live with the placement work. Leaning: harness here, placement rangespec
  with OEP-0018. Owner: TBD.
- **Can the three-apiserver boot be pooled/reused across MC spec files** to amortize
  the ~5s×3 envtest boot, or does scheme-registration thread-safety
  (`multikueue/suite_test.go:97`) force per-suite isolation? `TBD`.
- **When does Docker land on the CI agents** (gating Part 5's `irmanaged_kind`
  promotion and OEP-0018's authoritative KIND e2e)? External dependency; owner: infra,
  TBD.
