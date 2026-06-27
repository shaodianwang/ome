# Kueue vs OME — multi-cluster + integration-test study

Source: a 14-reader + 2-synthesis agent sweep over kueue (`sigs.k8s.io/kueue`,
`v0.19-devel`) and OME, 2026-06-26. Reference = kueue MultiKueue; target = OME
OEP-0018 multi-cluster + the integration-test harness.

## TL;DR

1. OME's multi-cluster is a **faithful, defensively-coded alpha skeleton** of
   MultiKueue's "external reconciler" model — the *architecture is right*. What's
   missing is the entire **performance layer**.
2. The highest-leverage perf fixes are **already written in-tree and just not
   wired to the live path** — most notably the selectively-caching remote client.
   Several are low-effort / high-impact.
3. Status convergence is **30s poll-based**; MultiKueue is event-driven via a
   remote-watch→channel bridge. Remote clients run at **client-go defaults
   (5 QPS / 10 burst)**; placement runs at **concurrency 1**.
4. The endpoint publisher is **status-only** (no DNS/Gateway), so OEP-0018's own
   Alpha graduation bar is **unmet**.
5. OME has **ZERO** multi-cluster integration coverage — `placement` +
   `workloadcluster` (~1.6k LOC) are tested only by package-local fake-client
   unit tests; the remote transport has never run against a real apiserver.
6. Kueue's **N-envtest-in-one-process** pattern (1 manager + N workers, wired via
   a `RestConfigToKubeConfig` kubeconfig-Secret round-trip) is a **near-drop-in
   template** OME lacks — and the CRDs are already installed in OME's envtest.
7. OME has **no performance/scale harness**. Kueue's envtest + stripped-main +
   fake-executor + checked-in `rangespec.yaml` checker is the blueprint and is
   fully transferable (OME already has envtest assets).
8. Test ergonomics lag: no Status setters/await helpers/domain matchers, no global
   `Eventually` default, no write-conflict log gate.

---

## Part 1 — Multi-cluster performance & architecture

### Kueue practices worth adopting (evidence)
- **External-reconciler model**: all coordination manager-side; workers are vanilla, no agent. Remote objects tagged with an origin label for GC/ownership. (`multikueue/controllers.go:111`)
- **Cache is the exception**: cache only low-cardinality config/quota (ClusterQueue/LocalQueue), read high-churn objects (Workloads) **live**; the cache is even feature-gated off by default. (`remote_client.go:46` SelectivelyCachingClient; `multikueuecluster.go:187-190`)
- **Scope every remote watch + List server-side** by one indexed origin label, `AllowWatchBookmarks:true` → traffic ∝ this manager's footprint, not the worker's total load. (`multikueuecluster.go:321`, `:535`)
- **Remote-watch → Go channel → local-reconcile bridge**: resolve the remote event to the LOCAL key, push onto a tiny (buf=10) channel, consume via `source.Channel` with ~1s batch debounce → event-driven, backpressured. (`multikueuecluster.go:353,505`; `workload.go:998`)
- **Per-remote fault isolation**: registry RWMutex held only for O(1) lookup; all slow per-cluster work under a per-client lock; snapshot-iterate for GC. Cites kueue#11297 (one stuck remote stalling all). (`multikueuecluster.go:139,1031`)
- **Two backoffs**: `establishBackoff` (1m→10m) bounds a hung Watch; `retryAfter` (0→5m20s) paces reconnects. (`multikueuecluster.go:87-99`)
- **Graceful-failover teardown**: hard-remove on delete vs soft-disconnect that keeps the entry for a `workerLostTimeout` grace (15m/5m) before re-placement. (`multikueuecluster.go:648,660`; `workload.go:560`)
- **Two-layer cleanup + hard ownership gate**: event-driven delete + periodic origin-scoped GC, re-validating the origin label immediately before each Delete (empty origin ⇒ refuse), plus a deleted-object cache for synchronous terminal cleanup. (`multikueuecluster.go:526`; `jobframework/utils.go:279`)
- **Split breadth policy from executor**: AllAtOnce vs leader-only Incremental (StepSize=3) vs External — all just populate `status.nominatedClusterNames`. (`workload.go:770`; `incrementaldispatcher.go:47`)
- **Field indexer behind every reverse lookup** + ~1s AddAfter event batching; directory-level inotify with ref-counted fan-out for rotated kubeconfig mounts. (`indexer.go:96`; `fswatch.go`)

### OME current state (the good)
- External-reconciler model matched; connection layer is the **strongest-delivered subsystem** (one `client.NewWithWatch` per cluster, SHA-256 kubeconfig dedup, idempotent reconnect, credential-rotation eviction, lifetime-scoped contexts). (`workloadcluster/manager.go:105,166`)
- Security validation precisely implements the threat model (exec/token-file/CA-path/insecure rejected; exec opt-in allowlist). (`connection.go:49,73,126`)
- Fan-out/race logic built and in places **stricter than the OEP** (lowest-named AllComponentsAdmitted wins = PD-safe; sticky winner; origin+UID-guarded deletes; empty-live-set GC guard). (`placement/controller.go`, `gc.go`)
- The cross-cutting prereq is **DONE**: `InstanceReadyTimeout` measured from admission. (`workload/deadline.go:103`)

### Gaps (severity · evidence)
- **[high] SelectivelyCachingClient not wired** — `buildRemoteClient` hardcodes `NewNeverCachingClient`; every cross-cluster read is a direct uncached round-trip every 30s poll per ISVC. The cache (Pod-banned, tested) exists but only a rejection test calls it. (`workloadcluster/manager.go:192`; `remoteclient.go:141`)
- **[high] No remote watch-funnel** — nothing calls Watch/registers a handler; status observed only by re-GET on the next 30s poll. (`placement/controller.go:471`; `DefaultPlacementRequeue=30s`)
- **[high] Remote clients at client-go defaults** (5 QPS/10 burst); no QPS/Burst/RateLimiter override. (`manager.go:176`)
- **[high] Placement concurrency=1** — no `MaxConcurrentReconciles`, unlike every other heavy OME controller. (`placement/controller.go:468`)
- **[high] Endpoint publisher status-only** — no DNS/Gateway/GSLB; inverts the OEP's "OME owns the endpoint" decision; Alpha criterion unmet. (`placement/controller.go:398`; OEP README:204,244)
- **[high] No MC e2e/envtest tests** — the OEP calls the ≥2-cluster KIND e2e "authoritative"; it doesn't exist.
- **[medium]** Thundering herd: unscoped List of all ISVCs on any cluster change (no ISVC-by-candidate index). (`controller.go:500`)
- **[medium]** No per-cluster fault isolation / per-call timeout / exponential reconnect backoff in the hot path. (`controller.go:187`)
- **[medium]** No "winner lost" grace window — re-races immediately on a transient winner blip. (`controller.go:114-148`)
- **[medium]** AllAtOnce-only fan-out (N creates + N-1 deletes per placement; DeepCopy every 30s). (`controller.go:187`)
- **[medium]** Multi-node gang (PodGroup × Kueue pod-group) untouched — Alpha-blocking open question.
- **[low]** SHA-256-of-whole-kubeconfig dedup will force full cache re-sync on CA rotation once the cache lands; doc drift on `status.placement.phase`.

### Recommendations (effort/impact)
| # | rec | effort | impact |
|---|---|---|---|
| A1 | Wire `NewSelectivelyCachingClient` onto the live path (cached: derived ISVC only; keep Pod-ban; GC stays live/uncached) | medium | **high** |
| A2 | Raise remote client QPS/Burst (config-driven) + per-call timeouts + 2-backoff (establish 1m→10m, retryAfter 0→5m20s) | **low** | **high** |
| A3 | `--placement-max-concurrent-reconciles` + `.WithOptions(MaxConcurrentReconciles)` | **low** | medium |
| A4 | Remote watch-funnel (channel bridge, ~1s batch) → event-drive placement, drop 30s status poll | high | **high** |
| A5 | Per-cluster fault isolation in fan-out (no shared lock across remote I/O; per-cluster timeout; snapshot iterate) | medium | medium |
| A6 | "Winner lost" grace window before re-placement (reuse soft-disconnect state) | medium | medium |
| A7 | ISVC-by-candidate index to kill the thundering-herd List | medium | medium |
| A8 | Split nomination from execution; add Incremental dispatcher | high | medium |
| A9 | Real endpoint publisher for one concrete backend (DNS/Gateway) | high | **high** |
| A10 | Per-Delete ownership re-validation + skip GC for disconnected + deleted-object cache | low | low |

---

## Part 2 — Integration tests (local feature / multi-cluster / performance testing)

### Kueue practices worth adopting (evidence)
- **Framework split into lifecycle methods** (Init/SetupClient/StartManager/Teardown); manager NOT auto-started; suite supplies a `ManagerSetup` callback → per-Describe manager restarts + multi-manager-in-one-process. (`test/integration/framework/framework.go:94,140,193,259`)
- **N envtest control planes in ONE process** (1 manager + 2 workers); fake the cross-cluster connection by serializing each worker's `rest.Config` to a kubeconfig (`RestConfigToKubeConfig`, ~6 LOC), storing it in a Secret, pointing a CR at it. Real controller loads it via `clientcmd` + `NewWithWatch` → genuine connection/validation/dispatch/status-flowback coverage, no KIND/Docker. (`multikueue/suite_test.go:107,417`; `pkg/util/testing/kubeconfig.go:102`; `jobs_test.go:206`)
- **Negative cross-cluster paths** with deliberately broken kubeconfigs (missing Secret, CA-as-path, tokenFile, insecure) asserting the cluster CR's Active/Connected reason — real certs ⇒ real validation. (`setup_test.go:221,449`)
- **Local perf harness**: envtest + stripped standalone main (controllers-under-test only) + a fake "executor" reconciler that completes work on a timer; bounded drop-on-full recorder for per-class latency/utilization; assert thresholds with a plain `go test` reading a checked-in `rangespec.yaml` (mean×1.2 / mean−7% calibration recipe documented). (`test/performance/scheduler/runner/main.go:117`; `checker/checker_test.go:44`; `configs/baseline/rangespec.yaml`)
- **Timing knobs as functional options** (WithGCInterval, WithWorkerLostTimeout(3s), WithEventsBatchPeriod) + raised apiserver max-requests-inflight for high-N runs. (`suite_test.go:398-404`; `performance/.../main.go:129`)
- **One canonical builder per type** with seeded defaults (no nil-panics), fluent setters, `.Obj()`, composite helpers, panic-on-misuse. (`pkg/util/testing/v1beta2/wrappers.go`)
- **Domain Gomega matchers** (HaveConditionStatusTrue/AndReason, BeNotFoundError) + `Expect…ByKeys` wrapping `Eventually(...).Should(Succeed(), AssertMsg(...))` with whole-slice equality, GinkgoHelper(), shared Timeout/Interval tiers, lazy-YAML dumps, timestamp-ignoring cmpopts. (`test/util/util.go:538,174`; `condition_matchers.go:29`)
- **Write-conflict as a HARD failure**: capture zap logs in Init; in Teardown assert no `≥Error` log matches `IsWriteConflictError`. (`framework.go:95,259`; `util.go:1507`)

### OME current state
- envtest harness is **well-layered** and already mirrors many kueue patterns: `util.RunSuite` (3 lines), `components.Setup*` per controller, synchronous ManagerSetup + WaitForCacheSync + webhook-TLS-dial readiness, SkipNameValidation, metrics-off, restart-friendly Stop/StartManager, opt-in dep-CRD bundles, ~1100 LOC fluent builders.
- Two tiers: hermetic envtest (CI gate) + KIND with real kubelet (authoritative for pod-lifecycle). `kindutil.ManagerSetup` == `framework.ManagerSetup` so suites port by swapping only the entry point.
- Single-cluster behavior genuinely well-exercised (~475 specs).

### Gaps (severity · evidence)
- **[high] ZERO multi-cluster integration coverage.** WorkloadCluster + InferenceReplica CRDs are installed by the framework, but `placement`/`workloadcluster` are never wired into `tests/integration/components`; all ~1.6k LOC tested only by fake-client unit tests; the remote transport never runs against a real apiserver. Kueue's 3-envtest pattern is the missing template. (grep of components/framework returns no placement/workloadcluster ref)
- **[medium] No performance/scale harness.** Only modest-scale correctness specs + 4 unit-level Benchmarks; no reconcile-throughput / large-N sweep / threshold gate.
- **[medium] No write-conflict log gate** (directly relevant to OME's controller-side-merge guidance).
- **[medium] Builder ergonomics**: no Status setters, no CreateAndWaitReady, no domain matchers, no global Eventually default → Create + Eventually/Get/FindStatusCondition boilerplate repeats across hundreds of specs.
- **[medium] PodKubeletSimulator** (570 LOC, 100ms poll) constrains local OMENative pod-lifecycle testing to KIND; already forced deletion of the old envtest coordination/omenative suites.
- **[medium] KIND coverage largely outside CI** (only coordination_kind, soft-fail); irmanaged/rawdeployment/perm have no CI; 13 PIt + 8 dead Skips inflate apparent coverage; host-sensitive magic timing slacks.
- **[low] No intra-suite parallelism / pooled control plane** (~5s envtest boot per package).

### Recommendations (effort/impact)
| # | rec | effort | impact |
|---|---|---|---|
| B1 | MC envtest suite via N-envtest-in-one-process + `RestConfigToKubeConfig` Secret round-trip; wire placement+workloadcluster into components; assert dispatch + status flowback + negative kubeconfigs | medium | **high** |
| B2 | WorkloadCluster timing knobs as functional options + swappable client source (prereq for B1) | **low** | **high** |
| B3 | Local perf harness: envtest + stripped main + fake-executor + `rangespec.yaml` checker + run-*/test-* targets | high | medium |
| B4 | Write-conflict observed-logs gate in Teardown | **low** | medium |
| B5 | Builders: Status setters + CreateAndWait + domain matchers + global Eventually default | medium | medium |
| B6 | Promote irmanaged_kind to CI (when Docker); prune dead PIt/Skip; config-drive timing slacks | medium | medium |
| B7 | Codify "new pod-lifecycle assertions go to KIND, not envtest" | low | low |
