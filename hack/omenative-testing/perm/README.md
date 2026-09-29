# OMENative lifecycle & coordination playground — real cluster, CPU only

A real-cluster test matrix for the OME OMENative path: rollout strategies,
coordination policies, and (incrementally) migration / restart / partition /
scale behavior. Targets whatever cluster your kubeconfig points at — override with
`OME_TEST_KUBECONFIG` (else the ambient `KUBECONFIG`, else `~/.kube/config`) and
`OME_TEST_NS` (default `sandbox`, where the runtime fixtures below are applied).

> **Prerequisites:** a cluster with OME installed, and the runtime fixtures in
> `runtime/` applied to `$OME_TEST_NS` (the noop ClusterServingRuntime /
> ClusterBaseModel + PVC/seed Job the cases reference). `batch.py` and `run.py`
> apply them idempotently on each run, so this is only needed for a manual
> apply:
>
> ```sh
> python3 runtime_fixtures.py | kubectl apply -n "$OME_TEST_NS" -f -
> ```
>
> The fixtures are **templates, not literal YAML** — `kubectl apply -f runtime/`
> will not work. `runtime_fixtures.py` substitutes the namespace, storage class,
> PVC access mode, node architecture and images. The namespace has to be
> $OME_TEST_NS everywhere: a pod cannot mount a PVC from another namespace, and
> the ISVC validator rejects a model that points at one. Add `--kind` (or set
> `OME_EVAL_KIND=1`) to target a local KIND cluster; see
> [`../deploy-ome-kind.sh`](../deploy-ome-kind.sh).
>
> **Images are tokens, not literals.** The fixtures and the cases that embed
> their own runtime name `${OME_EVAL_NGINX_IMAGE}`, `${OME_EVAL_NGINX_ALT_IMAGE}`
> (the tag the runtime-image-bump cases roll onto) and
> `${OME_EVAL_METRICS_IMAGE}`; the defaults are the public Docker Hub names and
> a bare `canary-metrics` tag. KIND side-loads them. A cluster that pulls only
> through a registry mirror exports the three variables first — the CI tree
> keeps the mirror paths: `source .buildkite/eval_image_mirror.sh`.
>
> **Scheduler dimension.** `OME_EVAL_SCHEDULER` names the scheduler stamped on
> every fixture runtime (and on the runtimes the cases embed). Empty, the
> default, renders no `schedulerName` and the cluster default places the pods.
> `OME_EVAL_SCHEDULER=ome-scheduler` makes `deploy-ome-kind.sh` build, load and
> install the gang scheduler chart, and the runner then asserts invariant D on
> every case: each pod runs under that scheduler, each multi-pod Instance's pods
> share one PodGroup whose `minMember` equals the gang size, and a gang binds
> as a unit. On a real cluster the scheduler must already run under that name.
> CI runs every eval job twice, once per scheduler (`omenative-e2e-<feature>-<topology>`
> and `…-ome-scheduler`), and both gate the build; `GKE_EVAL_SCHEDULER_HARD_FAIL=0`
> is the emergency switch that makes the scheduler jobs advisory. A shard that
> never reaches its cases (the KIND cluster or the OME deploy failed on the
> agent) leaves with exit 125 and the pipeline reruns it; a red case is never
> retried.
>
> **Canary-analysis cases (`analysis=…`) additionally need:** the bundled
> `ome-prometheus` enabled in the cluster (chart `prometheus.enabled`, now the
> default) scraping `$OME_TEST_NS`, and the `canary-metrics` image (built from
> `dockerfiles/canary-metrics.Dockerfile` by the pipeline) pullable by the cluster.

The expansion roadmap (P1–P4, the feature taxonomy, deferred autoscaling) lives
in [`../DESIGN.md`](../DESIGN.md). This file is the how-to-run reference.

```
perm/
├── README.md              — this
├── run.py                 — single-case runner (state machine + verdict)
├── batch.py               — parallel fan-out over many cases (--conc, ranges)
├── gen_cases.py           — generator: feature cases (migration/restart/partition/scale/edge)
├── gen_mn.py              — generator: multi-node (gang) rollout cases → isvcs/rollout/
├── generate_cases.py      — generator: hot-core permutation corpus → isvcs/gen-*/ (byte-stable; --check)
├── runtime_fixtures.py    — renders runtime/ for a target cluster (ns, storage, arch, images)
├── runtime/               — fixture TEMPLATES (see runtime_fixtures.py; not directly appliable)
│   ├── csr.yaml            — single-pod ClusterServingRuntime (nginx + a startup sleep)
│   ├── csr-multinode.yaml  — gang ClusterServingRuntime (leader + N workers, LeaderWorkerSet)
│   ├── csr-metrics.yaml    — metrics-emitting ClusterServingRuntime (canary-analysis cases; Downward-API health)
│   ├── cbm.yaml            — ClusterBaseModel (noop format, PVC-backed)
│   └── pvc-and-seed.yaml   — 1Mi PVC + seed Job (writes config.json so the CBM validates)
├── isvcs/                 — generated case YAMLs (committed; `topo=` in each header)
│   ├── rollout/           — 96 rollout cases (topology × lifecycle × coordination)
│   ├── migration/ restart/ partition/ scale/ edge/  — feature cases (see ../DESIGN.md)
│   └── gen-rollout/ gen-fault/ gen-restart/  — generated hot core (see isvcs/MANIFEST.md)
└── results/
    └── <RUN_TS>/<feature>/<topo>/
        ├── pNN-<ts>.tsv   — one verdict row per case
        └── pNN.log        — full per-case log
    └── <RUN_TS>/summary.txt — the batch summary table for that run
```

## The runtime — why the startup sleep matters

Read `runtime/csr.yaml`. The critical line:

```yaml
command: ["sh", "-c", "trap 'exit 0' QUIT TERM INT; sleep ${TEST_STARTUP_DELAY:-15} & wait $! && … && exec nginx …"]   # TEST_STARTUP_DELAY=15
```

Plain nginx goes Ready in ~150 ms, so a pod sampler misses every transition. The
~15 s `sleep` stretches each pod's Ready transition so a rotation spans long
enough for the watcher to catch the **peak** concurrent pods per Component — which
is what proves a strategy respected its surge/unavailable budget. The `trap`
matters just as much: until the `exec`, `sh` is PID 1, and on pod deletion the
kubelet delivers the **image's declared stop signal — nginx images declare
`STOPSIGNAL SIGQUIT`, not SIGTERM**. A PID-1 shell ignores any signal it has no
trap for, so without the trap (or with a trap that misses QUIT) a pod deleted
during the startup sleep sits Terminating for its full termination grace (the
admission-defaulted grace is far longer than any teardown budget here) instead
of exiting immediately. After the `exec`, nginx's own SIGQUIT handling takes
over, so a Ready pod always drains fast.
`csr-multinode.yaml` applies the same startup delay to the gang runtime (leader +
workers), so multi-node cases catch their peaks too.

The CBM points at a 1 Mi PVC in `$OME_TEST_NS`; the seed Job writes a minimal
`config.json`, and OME's own metadata-extraction Job (built from the `omeAgent`
image) then reads it and marks the CBM Ready. That extraction always runs for a
PVC-backed model — there is no way to skip it — so the cases stay rejected until
the agent image is pullable by the cluster.

## Case layout: feature = folder, topology = `topo=` tag

Cases live in `isvcs/<feature>/`. The **feature folder** is the primary axis
(`rollout`, then `migration`/`restart`/… as P2–P4 land). **Topology is an
orthogonal `topo=` header tag** so the two compose (slice by feature *or*
topology *or* both) without deep nesting. Four values:

| `topo`      | shape                                    |
|-------------|------------------------------------------|
| `engine`    | engine-only, single-pod                  |
| `pd`        | PD (engine+decoder[+router]), single-pod |
| `engine-mn` | engine-only, multi-node gang             |
| `pd-mn`     | PD, multi-node gang                      |

`rollout/` today: **27 `engine` · 38 `pd` · 13 `engine-mn` · 18 `pd-mn`** (96).
The case-id (`pNN`, the filename stem) stays globally unique and is the result
key, independent of which folder/topology a case lives in. `run.py` discovers
`isvcs/**/<id>.yaml` at any depth. Uniqueness is **enforced**: a duplicate id
would be silently shadowed (alphabetically-first folder wins), so `run.py`
refuses to run any case while the tree contains one, and

```bash
python3 run.py --validate-only            # the committed tree
python3 run.py --validate-only /some/dir  # any case dir (e.g. a scratch copy)
```

checks the whole tree — id uniqueness plus the line-1 header schema (known
keys, known `mode=` values, per-mode params) — with **zero cluster access**
(no kubeconfig needed) and exits nonzero on any problem. Run it after editing
or generating cases; the generators run it themselves on exit.

Each case is a single ISVC YAML; **line 1 is a `key=value` header** carrying the
description, replica counts (`Ne/Nd/Nr`), peak ceilings (`ceil_e/ceil_d/ceil_r`),
`topo=`, and the optional parked-debt flags (see verdicts): `infeasible=1`
(requires a `# reason: <why unsatisfiable>` comment on line 2) and `notimpl=1`
(requires `issue=<github issue number>` — an XFAIL with no tracked issue is
invisible debt; validate-only rejects both omissions). A notimpl case whose
tracked bug is itself a continuous-invariant violation adds
`inv_xfail=A|B|C|all` (requires the line-2 `# reason:` too): exactly that
invariant's violations record as the case's `XFAIL (known bug: #N)` instead of
hardening the verdict — the invariant is still evaluated and printed (nothing
is blinded, unlike `inv_off`), and any other invariant still fails hard, so
unrelated regressions cannot hide behind the parking.

## How a case runs (`run.py`)

For each case:

1. `kubectl apply` the runtime (idempotent) and the ISVC.
2. Wait until the expected pods are all Ready (bounded).
3. Start a pod watcher streaming events with timestamps.
4. Trigger a rotation by patching `engine.annotations[perm.test/rev]: v2`.
5. Sample the trajectory until convergence or the budget expires (the budget
   scales with pod count and is extended for Sequential coordination).
6. Replay the trajectory → **peak** alive and **final** alive per Component +
   convergence time, then emit a verdict and clean up the ISVC.

## How to run

```bash
cd hack/omenative-testing/perm   # from the repo root

# whole rollout suite, single-pod tier @ conc 8 then multi-node tier @ conc 4:
python3 batch.py --conc 8 p01-p65
python3 batch.py --conc 4 p66-p96

# a single case (own watcher, prints its result path):
python3 run.py p72

# explicit cases / subsets:
python3 batch.py --conc 6 p26 p32 p72

# canary rollout suite (OEP-0015.1) — full topology × policy permutation:
python3 batch.py --conc 4 p300-p313
python3 run.py p300           # single Auto canary, own watcher
```

### Running under CI

Three flags make a sweep machine-consumable:

```bash
python3 batch.py --shard 0/4 --junit results/junit-0.xml p01-p96
```

- `--shard I/N` — this agent's slice, partitioned round-robin by index (0-based
  `I`) so the long families spread across shards instead of piling into one.
  Every case lands in exactly one shard.
- `--junit PATH` — one `testcase` per case. Parked debt (`XFAIL`) is recorded as
  **skipped**, not passed, so tracked debt reads as debt in the CI UI.
- **Exit status** — nonzero when any case ends in a verdict that is neither
  `PASS` nor an `XFAIL`. `FAIL`, `INIT_TIMEOUT`, `APPLY_FAIL`, `INFRA-FAIL` and
  an unreadable verdict all mean the sweep did not validate that case.

Interactive runs are unaffected: without these flags the behavior is unchanged.

### Canary (`mode=canary`, OEP-0015.1)

`isvcs/canary/` (p300–p313, generated by `gen_cases.py`) ship a
`spec.rollout.groups[]` canary group (OEP-0017 v2: `components`, `canary.{steps,promotion}`)
in the initial apply; the harness bumps the primary component's revision
(`perm.test/rev`) to trigger the staged canary, then drives + observes the step
machine via `status.canary` + `status.components.<primary>.rolloutPhase` /
`.traffic[]`. The matrix permutes:

- **topology** — `engine`, `pd`, `engine-mn`, `pd-mn` (single / PD / +multi-node);
  the primary the step machine drives is router > engine > decoder.
- **promotion** — `canary=auto` (advances on each step's `pause.duration` timer) and
  `canary=manual` (pauses until the harness sets
  `ome.io/rollout-promote=<canaryRevisionHash>`, re-promoting at every gate).
- plus a **3-step** (25/50/100) plan, a **capacity-ahead smoke** step (100% new
  capacity at 0% traffic, then cut over), and a **rollback** probe (`rollback=1`
  — sets `ome.io/rollout-rollback` mid-rollout and asserts the canary drains
  every Instance back to the stable revision, holds there at `RolledBack`, and
  records the rejected revision in `status.canary.rolledBackRevisionHash`).

The harness asserts the **status-level** contract (phase progression, per-step
traffic weights stepping to 100 on the canary revision) and **per-revision pod
capacity** (the primary fully rolls to the canary revision). Gateway data-plane
weighting is a downstream consumer (per-revision Services + weighted HTTPRoute)
and is intentionally out of scope here.

#### Canary units (`unit=`)

A **canary unit** is the smallest set of Components that advance through a
ladder together: the **router** alone, or **engine+decoder** (a P/D pair speaks
a pairing protocol to each other, so splitting it can leave a new-protocol
prefill with no pairable decode). A group must contain the entrypoint of every
unit it covers, and at most one group may drive a unit — but different units may
each carry their own run.

The primary is therefore the entrypoint of the **group's** unit, not the ISVC's:
an engine ladder behind a router is driven and observed on the *engine*. Run
state lives at `status.components.<unit>.canary`; the top-level `status.canary`
is an alias for whichever unit owns the ISVC entrypoint, so it is the wrong copy
to read once two units roll.

p314–p318 cover the shapes:

| case | shape | drives |
|------|-------|--------|
| p314 | engine unit behind a router, PD, auto | engine (router idle) |
| p315 | engine unit behind a router, PD, manual | engine (router idle) |
| p316 | engine unit behind a router, no decoder | engine (router idle) |
| p317 | a group per unit on one ISVC | engine (`unit=engine`) |
| p318 | a group per unit on one ISVC | router (`unit=router`) |

`unit=router|engine|decoder` names which unit's ladder to drive when the ISVC
carries more than one group. Every unit the case does **not** drive is asserted
idle: no run of its own on its entrypoint, and every Instance still on its stable
revision — the real-cluster proof that one ladder does not pull another along
ungated. The idle unit's `ceil_*` is set to N (no surge), so a stray rotation
fails the case rather than passing quietly.

#### Canary-state hooks (`during=`, `verb=`, `fault=`, `bump=`)

The step machine is a state × event table, and p319 + p366–p383 aim an event at
a named state instead of at "some time after the bump". Every one of them names
the grid cell it covers in `cell=canary/<State>/<event>` (the states are
`Staging`, `Serving`, `PreHold`, `Draining`, `Done`, `RollingBack`,
`RolledBack`, `Failed`).

- **`during=canary-<state>[@<step>]`** — when to act: `canary-staging`
  (`Pending`), `canary-serving` (`Canarying`/`Paused`), `canary-draining`
  (`Promoting`), `canary-rollingback`, `canary-failed`; `@2` narrows it to one
  step index. The verb polls the ISVC status every 2 s and fires once.
- **`verb=`** — the operator verb to fire at the hook: `pause` / `pause-freeze`
  (lifted after `verbhold=<s>`, default 45; the step index must not move while
  held), `promote` (the matching hash; with `skipsoak=<s>` the case asserts it
  beat a timed step's soak), `promote-stale` (a real hash token naming the
  stable revision; the step must not move for 20 s), `rollback`, `rebump` (a
  second revision bump of the bumped members), `rebump-fix` (a bump that also
  drops the runner image override), `timeout-clear`.
- **`fault=<kind>:canary-<state>`** — the existing faults (`podkill`,
  `ctrlrestart`, `quota:<n>`, `cordon:source`) fired at a canary hook instead
  of `rotating`/`settling`; `victim=canary|stable` aims a podkill at the newest
  canary pod or at an old-revision pod.
- **`bump=<members>`** — bump only these group members (an engine-only or
  decoder-only change under a router unit); `img=<ref>` gives the bumped
  revision an image so the capacity gate never opens. Name the harness's
  unpullable image for that, `example.com/canary-test/missing:none`
  (`runtime_fixtures.UNPULLABLE_IMAGE`): it is the one case image the deploy
  step never side-loads, so it fails to pull offline as well as online.
- **`terminal=Stable|RolledBack|Failed`** — the expected terminal phase when the
  derivation from `analysis=`/`rollback=` does not apply; `Failed` asserts the
  park (no canary traffic, not fully rolled, nothing rolled back).
- **`prearm=rollback`** — leave a rollback request on the idle service before
  the bump; **`nophase=<Phase>`** — a phase that must not be observed once the
  hook has fired.

Failure cases set `canary.readyTimeout: "90s"` in the group so the park lands
inside the case budget.

### Canary analysis (`analysis=…`, OEP-0015.2)

`isvcs/canary/` p330–p343 gate steps on a **Prometheus query** instead of a
timer/promote. They run the metrics workload (`runtime/csr-metrics.yaml`, image
`canary-metrics`) and use the bundled `ome-prometheus` as the analysis source.

The harness sets each canary's health at the revision bump via Downward-API pod
annotations (`perm.test/error-rate`, `perm.test/emit`), so one bump both rolls
the revision and makes the canary pods PASS, BREACH, or read INCONCLUSIVE. The
header `analysis=pass|breach|inconclusive` (+ `failurelimit`, `oninconclusive`)
selects the intent and the expected terminal:

| `analysis` | `oninconclusive` | expected terminal |
|------------|------------------|-------------------|
| pass | — | promote → Stable |
| breach | — | auto-rollback → RolledBack (rejected revision recorded) |
| inconclusive | Hold | escalate → Failed (stable keeps serving, no rollback) |
| inconclusive | Rollback | auto-rollback → RolledBack |

The query is `max(canary_error_rate{inferenceservice="{{.ISVCName}}",component="{{.Component}}"})`
— MAX picks the worst (canary) pod, since the bundled Prometheus emits no
revision label. Analysis-validation reject cases live in `isvcs/reject/`
(p324–p328). Run them like any case: `python3 batch.py --conc 4 p330-p343`.

### Scale & hung-Prometheus (`perf/`)

Two real-cluster perf harnesses for the async sampler:

- `perf/scale.py [K]` — launches K identical PASS analysis canaries at once and
  reports how many converge + p50/p95/max convergence. Run escalating K
  (10/20/30…) to find where the controller (`MaxConcurrentReconciles=1`) falls
  behind — the async sampler should keep analysis canaries at parity with plain
  rollouts. `python3 perf/scale.py 20`
- `perf/hung_prom.py [N]` — deploys a 30s delay-proxy in front of Prometheus, N
  analysis canaries pointed at it (each churns inconclusive, generating sustained
  background queries), and one control canary on a normal source. Asserts the
  control still reaches Stable within budget — i.e. the hung source did **not**
  stall the reconcile worker. `python3 perf/hung_prom.py 5`

Both assume OME (+ bundled prometheus) in ns `ome` (see `perf/prom-delay-proxy.yaml`).

### Pairing protocol (`mode=pairing`)

`isvcs/pairing/` (p800+, hand-written — outside both generators' sweep) covers
`spec.rollout.pairingProtocol`: the trigger is a **patch of the protocol
field** (not an annotation bump) — the token rides every engine+decoder
revision, so the patch alone re-mints and rolls both pairing Components
(never the router; isolation is asserted). Header knobs:

- `proto=<token|clear>` — the transition target; the starting value is
  whatever the case YAML declares (`""→X` adoption, `X→""` clear, `X→Y`
  coordinated swap). `proto2=` re-patches once ≥1 new-cohort pod is READY —
  a re-target over a partially-landed intermediate (converge on the FINAL
  cohort, no Instance stranded on the intermediate — the X-2 shape).
- `hold=1` — assert a `status…rolloutHold` with `gate=Pairing` is observed
  mid-transition. Only shapes where the denial spans a startup cycle can
  assert this: the acting Component must keep ≥2 serving instances through
  the denied window (that blocks all three gate escapes — no-pair, the 1×1
  wall, and last-hop), e.g. an in-place engine racing ahead of a surging
  decoder. Drain-first shapes shed to ≤1 serving and get admitted by the
  escapes in under a sampling tick — holds there are reported ⓘ only.
- `pkill=1` — delete the LAST old-cohort decoder pod mid-transition and
  assert full recovery + convergence. Such cases carry `inv_off=C` and
  `order_soft=1`, and the decoder's capacity floor is exempt in-handler (the
  harness, not the product, destroyed the pod); the engine floor stays hard.
- `scale_e=` / `scale_d=` — re-target a Component's replicas mid-transition.
- `expect=reject` + `mode=pairing` — an **update-time** admission negative:
  the CREATE is admitted and the Phase-2 protocol patch itself must be denied
  (`reason=` substring asserted). Plain `expect=reject` (apply-time) cannot
  express this — the webhook rule compares old vs new spec.

Verdicts on top of the usual rollout checks: every engine/decoder pod carries
`ome.io/pairing-protocol=<target>`, every live traffic target publishes
`pairingProtocol=<target>`, every routing Service named by live traffic
carries the label in metadata and NO Service ever carries it in
`spec.selector`. Continuous **invariant C (pair-alive)** arms for every
non-empty→non-empty transition: whenever both engine and decoder serve, a
pairable pair (equal or either-empty protocols) must exist. The gate's
liveness escapes are exempt: the 1×1 mutual wall explicitly, and the no-pair
and last-hop windows through the disarm rule (a side at zero serving drops
`both-present`, and the tracker re-arms only on the next pairable sample).
Waive with `inv_off=C` + a line-2 `# reason:` comment.

A `canary=auto` case with a canary group in the YAML drives the same protocol
patch through the staged canary (steps advance on their own `pause.duration`
timers; `canary=manual` is not supported in this mode).

### Negative cases (`expect=reject`)

`isvcs/reject/` (p320–p329, continued at p364+) are **admission-rejection**
cases: the ISVC spec is
deliberately invalid and the validating webhook must deny the `kubectl apply`.
These are the only end-to-end proof that the validation rules have teeth on the
*deployed* webhook — unit tests can't see it. A case with `expect=reject` in its
header skips the watcher/rollout entirely:

- **PASS** = the apply is denied, and (if `reason=<code>` is set) the denial
  message contains that reason code (e.g. `CanaryInvalid`, `SoakNotHonored`).
- **FAIL** = the apply is accepted (the rule didn't fire) — the ISVC is then
  deleted so it doesn't leak.

Current cases: canary group missing its unit entrypoint (p320 — `[decoder]`
without the engine), soak on a canary group (p321), soak on a non-collapsing
rollout (p322), maintainRatio on a canary group (p323), a canary group splitting
a declared P/D pair (p329 — `[router, engine]`), two canary groups driving
the same unit (p364), and two canary groups that never declared
`groupOrdering: Concurrent` (p365). Run them like any other case:
`python3 batch.py --conc 4 p320-p323 p329 p364 p365`.

p365 is the negative twin of p317/p318: those declare `Concurrent` and their
units roll at the same time *because the spec asked for it*. Strip the
declaration and the same shape is denied, so the concurrency in p317/p318 can
never be mistaken for ordering that was quietly unimplemented.

**Concurrency is bounded by the controller, not the cluster.** The OME manager
runs `MaxConcurrentReconciles=1` (controller-runtime default) behind leader
election — one reconcile at a time, cluster-wide — and the rollout state machine
is paced by a ~5 s requeue. Past ~12–16 concurrent rollouts the controller can't
service each within budget and cases falsely time out (`ROTATE_TIMEOUT` /
`INIT_TIMEOUT` / `NO_ROLLOUT`) even though the rollout logic is correct (a conc-16
full-matrix run produced ~19 % such timeouts; the same cases pass isolated). Rules
of thumb:

| batch | conc |
|-------|------|
| single-pod (`engine`/`pd`) | 8–10 |
| multi-node gangs / migrate / kill | 4–6 |
| biggest gangs (≥6 pods) | 2 |

Re-run any timed-out case isolated to tell contention from a real failure. (Bumping
the controller's `MaxConcurrentReconciles` would raise these ceilings — a real
production-scale improvement too.) Each `batch.py` invocation stamps one `RUN_TS`
shared by its subprocesses, so a run's results land together under
`results/<RUN_TS>/<feature>/<topo>/` with a `summary.txt`.

### Fault cases: quota isolation, cordon impact, leak remediation

- **`fault=quota` cases run serially.** A quota case creates a *namespace-wide*
  ResourceQuota sized from a live usage snapshot, so run concurrently it both
  blocks other cases' pod admissions and mis-sizes its own headroom.
  `batch.py` partitions automatically: all other cases keep the concurrent
  pool; quota cases run **serially after the pool drains** (the partition is
  printed in the run banner). Nothing to do — just don't hand-craft a parallel
  run that mixes a quota case with others.
- **`fault=cordon` cases briefly cordon a real node**, so on a shared cluster
  they momentarily affect scheduling for *everyone* in that node's pools.
  run.py stamps the node with the `perm.test/cordoned=<case>` label at cordon
  time and always uncordons in cleanup.
- **Leak remediation after a SIGKILL.** run.py's fault cleanup runs on PASS,
  FAIL and crash — but not on SIGKILL. `batch.py` sweeps both leak classes at
  end of run; to remediate by hand (only harness-marked objects are touched):

  ```bash
  kubectl -n $OME_TEST_NS delete resourcequota -l perm.test/fault --ignore-not-found
  kubectl get nodes -l perm.test/cordoned -o name | xargs -r -n1 kubectl uncordon
  ```

## Verdicts

| verdict        | meaning                                                          |
|----------------|-----------------------------------------------------------------|
| `PASS`         | peak ≤ ceiling for every Component, converged back to N in budget, and coordination behaved (see below) |
| `FAIL`         | a ceiling was exceeded, it didn't converge, or coordination misbehaved |
| `XFAIL`        | a `FAIL` on a case tagged `infeasible=1` (a known-unsatisfiable shape — e.g. a RatioBalanced band narrower than one pod under a drain-first strategy; the line-2 `# reason:` comment names why); softened so it doesn't mask real regressions |
| `XFAIL (known bug: #N)` | a `FAIL` on a case tagged `notimpl=1 issue=N` — parked on a tracked known bug; the verdict carries the issue so no sweep summary hides the debt. `batch.py` additionally prints a `PARKED DEBT` table listing every notimpl/infeasible case in the sweep, even when they all XFAIL |
| `INIT_TIMEOUT` | never reached initial Ready (often capacity/contention, not a controller bug) |
| `APPLY_FAIL`   | `kubectl apply` rejected the ISVC                                |

Coordination check: in a PD group with a coordination policy, the decoder is
expected to **surge with** the engine; an *independent* (un-grouped) decoder must
**not** surge from an engine-only bump. Single-pod cases additionally assert the
surviving pod's UID never rotated (skipped for gangs, where pods are recreated).

## Regenerating cases

Three generators (re-run after editing; all write into `isvcs/`):
- `gen_cases.py` — the feature matrix (migration/restart/partition/scale/edge),
  permuting topology × lifecycle × coordination.
- `gen_mn.py` — the multi-node (gang) rollout cases (`p66`–`p99`, `p500`+) →
  `isvcs/rollout/` (`p500`+ is the continuation block: `p100`+ belongs to
  `gen_cases.py`'s feature counters).
- `generate_cases.py` — the hot-core permutation corpus (`p1000`–`p1999`) →
  `isvcs/gen-rollout/`, `isvcs/gen-fault/`, `isvcs/gen-restart/`, plus
  `isvcs/MANIFEST.md` (per-dimension counts, constraint ledger, missing-hook
  catalog). The gen-* folders are generator-owned WHOLESALE: regeneration is
  byte-stable and `generate_cases.py --check` (run by `run_test.py` in CI)
  rejects any hand edit — promote a bug-finding case into a hand-owned folder
  under a fresh id instead of editing it in place.

Every generator finishes with `run.py --validate-only`, so a regeneration that
would reintroduce a duplicate (shadowed) case id or a malformed header fails
the generation itself. Hand-written cases parked in generator-owned folders
(`scale/p188`+`p189` — the renamed former `scale/p97`+`p98`, `runtime-rev/p416`)
are registered in each generator's `HAND_WRITTEN` set: the regen sweeps
preserve them and the generators refuse to mint their ids.

`isvcs/fault/` (`p600`+ — fault-injection cases: `fault=` header, plus the
`blocked=1` / `expect=fail` / `lift=1` contract knobs) is entirely
hand-written and outside both generators' sweep; the `p600`+ id block is
reserved for it. `isvcs/pairing/` (`p800`+ — the `mode=pairing` protocol
cases) is likewise hand-written, with the `p800`+ block reserved.

Coordination is emitted under `spec.rollout.groups[]` (OEP-0017 v2: BlueGreen ->
one `blueGreen` group; RollingUpdate -> a `rollingUpdate` group, +`maintainRatio`
for RatioBalanced; Sequential -> one single-Component `blueGreen` group per member
in order). Keep
the `topo=` tag and any `infeasible=1` / `notimpl=1` header flags so the harness
groups and scores correctly.
```

## Future knobs

Dimensions the generators do not permute yet. Each is a candidate axis
once its KIND spec settles; until then it stays hand-tested.

- `lifecycle.minReadySeconds` — the Ready-age window a rollout waits
  before draining or promoting past a new pod. The KIND gate covers the
  single-pod SurgeThenDrain shape (`minreadyseconds_kind_test.go`);
  gang surge, RecreatePod, and in-place under a window, and the
  cluster-wide `deploy.minReadySeconds` default, are not yet in the
  matrix.
