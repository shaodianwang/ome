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
├── casefiles.py           — the generators' shared write path: only minted ids, hand-maintained header layer kept, nothing deleted
├── gen_cases.py           — generator: feature cases (migration/restart/partition/scale/edge/canary; --check)
├── gen_mn.py              — generator: multi-node (gang) rollout cases → isvcs/rollout/ (--check)
├── gen_runtime.py         — generator: runtime-triggered cases → isvcs/runtime-rev/ (--check)
├── generate_cases.py      — generator: hot-core permutation corpus → isvcs/gen-*/ (byte-stable; --check)
├── coverage.py            — the coverage ledger: the scenario space rendered from the case headers (--json, --check)
├── runtime_fixtures.py    — renders runtime/ for a target cluster (ns, storage, arch, images)
├── runtime/               — fixture TEMPLATES (see runtime_fixtures.py; not directly applicable)
│   ├── csr.yaml            — single-pod ClusterServingRuntime (nginx + a startup sleep)
│   ├── csr-multinode.yaml  — gang ClusterServingRuntime (leader + N workers, LeaderWorkerSet)
│   ├── csr-metrics.yaml    — metrics-emitting ClusterServingRuntime (canary-analysis cases; Downward-API health)
│   ├── cbm.yaml            — ClusterBaseModel (noop format, PVC-backed)
│   └── pvc-and-seed.yaml   — 1Mi PVC + seed Job (writes config.json so the CBM validates)
├── isvcs/                 — generated case YAMLs (committed; `topo=` in each header)
│   ├── rollout/           — 96 rollout cases (topology × lifecycle × coordination)
│   ├── migration/ restart/ partition/ scale/ edge/  — feature cases (see ../DESIGN.md)
│   └── gen-rollout/ gen-fault/ gen-restart/ gen-crashloop/ gen-switch/ gen-crashloop-idle/ gen-crashloop-sibling/
│       gen-heal/ gen-imagepull/ gen-node/ gen-ctrlrestart/  — generated hot core (see isvcs/MANIFEST.md)
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
4. Trigger a rotation by patching `engine.annotations[perm.test/rev]: v2`
   (every coordination-group member when the case declares a group;
   `bump=<members>` names the members to bump instead, so an engine-only
   change can land under a group whose other members stay as negative
   controls). With
   `edit=env` the same patch also sets the bumped runner's `TEST_REV` env, a
   pod-template diff beyond container images: an in-place strategy then
   resolves to a recreate, UID rotation is expected, and the drain-first
   checks (ratio band, capacity floor) judge the roll.
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
the grid cells it proves in `cell=`, a comma-separated list of
`canary/<State>/<event>` ids (the states are `Idle`, `Staging`, `Serving`,
`PreHold`, `Draining`, `Done`, `RollingBack`, `RolledBack`, `Failed`; `Idle`
is reached through `prearm=`, below); a case that exercises a second row,
such as the recovery half of a podkill, carries both ids.

- **`during=canary-<state>[@<step>]`** — when to act: `canary-staging`
  (`Pending`), `canary-serving` (`Canarying`/`Paused`), `canary-prehold`
  (`Paused` with the pre-step hold set, which only a repin creates),
  `canary-draining` (`Promoting`), `canary-done` (`Stable` with the step at the
  ladder's end), `canary-rollingback`, `canary-rolledback`, `canary-failed`;
  `@2` narrows it to one step index. The verb polls the ISVC status every 2 s
  and fires once. **`verb2=` / `during2=`** fire a second verb at a second hook
  once the first has fired, which is how a case reaches PreHold (a
  `planedit-repin` at Serving, then the verb under test at `canary-prehold`).
  **`release=promote|resume`** fires that verb `releaseafter=<s>` seconds
  (default 20, `RELEASE_AFTER_S`) after the second verb, so a case whose
  second verb leaves the pre-step hold in place can end (needs `verb2=` and
  `during2=canary-prehold`); the verdict asserts the ladder was still held at
  the same step when the release fired, and that it then completes.
- **`verb=`** — the operator verb to fire at the hook: `pause` / `pause-freeze`
  (lifted after `verbhold=<s>`, default 45; the step index must not move while
  held), `promote` (the matching hash; with `skipsoak=<s>` the case asserts it
  beat a timed step's soak), `promote-stale` (a real hash token naming the
  stable revision; the step must not move for 20 s), `rollback`, `rebump` (a
  second revision bump of the bumped members), `rebump-fix` (a bump that also
  drops the runner image override), `timeout-clear` and `timeout-set` (writes
  `ome.io/rollout-ready-timeout=<timeout=>`), `repin` (`ome.io/rollout-repin=now`:
  the run keeps its identity and progress), `resume` (`ome.io/rollout-resume=<canary
  hash>`: a terminal canary re-enters the ladder at step 0), `planedit` (inserts
  a step at 75% into the driven group's ladder; a pinned run never serves it,
  and the verdict asserts so), `planedit-repin` (drops the ladder's first step
  and repins, so the clamped step would raise exposure and the run holds at
  PreHold), `groupremove` (empties `spec.rollout.groups` mid-run; the pinned
  plan keeps stepping), `scale` (see the PD knobs below), `runlost` (removes
  the pinned run from the status subresource, `status.rollout.activeRun`,
  which is how a lost run looks to the executor. At a live hook the verdict
  asserts the loss was acknowledged — a `CanaryRunLost` hold, the run
  layer's in-place adoption, or a run pinned again at the next sample, with
  a new id or the removed run's own id when a pass in flight at the removal
  wrote its snapshot back — and, over the samples that saw no run, that the
  step never moved and a re-pinned run resumed at that step; a run pinned
  again before any sample saw it missing leaves nothing to hold, and a run
  never pinned again ends the case as held after `RUNLOST_HOLD_S`. At
  Failed the terminal waits up to `RUNLOST_HOLD_S` for the run layer's
  re-pin and reports it; at RollingBack it waits the same way for the run
  layer to adopt the revert in place, and the RolledBack checks carry the
  claim; at Done and RolledBack the run is already closed and nothing is
  removed), `pairing`
  (patches `spec.rollout.pairingProtocol` to `pairto=<token|clear>` on a
  bump-triggered canary; needs `bump=`, Ne>0 and Nd>0; the verdict asserts a
  new canary hash followed the patch, nothing stays on a pre-re-target
  revision, and the pairing surface reads `pairto=` at the terminal),
  `quotalift` (lifts the case's `fault=quota:<n>`; a second verb waits for
  the lifted capacity to land, and the verdict asserts the park survived it:
  still Failed at the same step once the parked step's pods are Ready).
  `terminal=` decides
  the verdict contract: only `RolledBack` reads the rollback checks, so a case
  that rolls back and then resumes or re-targets is judged as `Stable`.
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
- **`prearm=<verb>`** — apply a verb to the idle service after the initial
  fleet settles and before the bump: the grid's Idle column, through the same
  writes the hook verbs use (a promote or resume names the stable revision's
  hash, the only revision an idle service has). Accepted: `rollback` (consumed
  with a `CanaryRollbackIgnored` event, reported), `pause` (the arm holds until
  the runner lifts the pause `verbhold=` seconds after the bump: no split
  served and no step past 0 before the lift), `promote` (a stale token that
  lingers and is ignored: the first timed hold soaks in full), `repin`
  (lingers until a run opens; the grid leaves the cell open, so the verdict
  records what the first run did with it and asserts only completion),
  `resume` (consumed: the annotation is gone at convergence and the ladder
  runs normally), `timeout-set` with `timeout=` (the arm inherits the budget:
  with a bad-image bump and `terminal=Failed` the park must arrive at the
  annotation's timeout, not the plan's `readyTimeout`), `planedit` (the next
  run pins the edited plan, so the inserted 75% step IS served: the inverse of
  a mid-run edit's verdict), `groupremove` (nothing arms: the bump is a plain
  rollout with no canary phase and no run), `scale` with `scale_e=`/`scale_d=`
  (the fleet is re-shaped and settles before the bump; the arm counts the new
  fleet). `runlost` is not accepted here: at Idle there is no run to lose, so
  the Idle/runLost cell needs an invalid policy fixture instead.
  **`nophase=<Phase>`** — a phase that must not be observed once the hook has
  fired (with `prearm=`, once the bump lands).

Failure cases set `canary.readyTimeout: "90s"` in the group so the park lands
inside the case budget.

#### PD knobs (`proto=`, `verb=scale`, `victimcomp=`, `imgcomp=`, invariant E)

The prefill/decode cases drive the pair through the canary and aim events at
one side of it:

- **`proto=<token|clear>`** — the trigger is the `spec.rollout.pairingProtocol`
  patch instead of the annotation bump (the token re-mints engine and
  decoder together, whatever the group lists; `bump=`/`img=` do not apply).
  Invariant C (pair-alive) arms exactly as in `mode=pairing` for a
  non-empty→non-empty transition, and the pairing surface (pod labels, live
  traffic targets, Service metadata) is asserted at the terminal for the
  protocol it implies: the target on `Stable`, the original on `RolledBack`.
  **`proto2=`** re-targets the protocol at the `during=` hook (at `during2=`
  when that hook has no `verb2=` of its own), or at the first Ready canary pod
  when no hook is named; convergence then requires every bumped pod past the
  pre-re-target revisions (nothing stranded).
- **`verb=scale`** with **`scale_e=`/`scale_d=`** — re-target one side's
  replicas at the `during=` hook; the convergence target moves with it and the
  capacity floor keeps `min(start, target)` as its basis.
- **`victimcomp=engine|decoder|router`** — narrow a `fault=podkill:*` (with
  or without `victim=`) to one Component. **`imgcomp=<member>`** — give only
  that bumped member the unpullable `img=`, so one side of the pair parks
  while the other's canary is up.
- **Invariant E (cohort ratio, informational)** — whenever both engine and
  decoder are bumped, every tick reads the canary cohort's serving
  engine:decoder ratio while both sides have canary capacity and records any
  sustained deviation beyond `ratio_tol=<percent>` (default 50) from the
  fleet's ratio. It is reported (`⚠`), never failed, until the contract is
  decided; the executor holds the ratio only at the traffic shift today.
- **`xcheck=sectraffic`** with `notimpl=1 issue=<n>` and a line-2 `# reason:`
  — a scoped park: the secondaries' traffic-target check records the tracked
  bug as the XFAIL, and every other check stays red. Plain `notimpl=1`
  softens every failure, which would hide a pair-alive violation behind the
  park.

A deliberately slow side needs no knob: the case YAML sets
`runner.env: [{ name: TEST_STARTUP_DELAY, value: "40" }]` on that Component
(the runner merges into the runtime container by env name), so its canary
pods turn Ready later than the other side's. Keep the delay under the fixture
runtime's startup probe budget (30 × 2 s = 60 s), or the kubelet restarts the
container before it binds. On a multi-node Component the override sits under
`leader.runner` and must carry `name: ome-container`; the CRD requires the
name there.

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

`health=breach` with `analysis=pass` is the transient shape: the canary pods
breach for their first `perm.test/decay` seconds of serving and then recover, so
the gated step must accrue failing samples under `failurelimit` and still
promote. `minfails=<n>` makes the runner assert that the peak of
`status…canary.analysisFailedChecks` reached n before the step advanced — the
promotion alone would also pass a run whose sampler never saw the breach. Pick
the step's `initialDelay`, `interval`, `failureLimit`, bake (`pause.duration`)
and `decay` against the two clocks involved: the step's clock starts when its
last canary pod turns Ready, and the bundled Prometheus scrapes every 15s.

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

`isvcs/pairing/` (p800+, hand-written; no generator mints into it) covers
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
liveness escapes are exempt where the window is unavoidable: the no-pair
and last-hop windows through the disarm rule (a side at zero serving drops
`both-present`, and the tracker re-arms only on the next pairable sample), and
the 1×1 mutual wall only when a pairing Component updates drain-first
(`RecreatePod`). Under a surge or in-place strategy both replacements come up
regardless and the drain-time half of the pair floor holds a source's drain
until the peer's target cohort serves, so an unpaired sample at 1×1 is a
violation; `p1817` to `p1819` (one side slow through `runner.env
TEST_STARTUP_DELAY`) and the symmetric `p810`/`p830` assert it. Waive with
`inv_off=C` + a line-2 `# reason:` comment.

A `canary=auto` case with a canary group in the YAML drives the same protocol
patch through the staged canary (steps advance on their own `pause.duration`
timers; `canary=manual` is not supported in this mode). For manual promotion,
verbs at canary hooks, rollbacks and the failure edges, use `mode=canary` with
`proto=` instead (see the PD knobs above).

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

### Mid-roll edits (`edit=strategy:<type>`, `edit=restartPolicy:<policy>`)

`edit=strategy:SurgeThenDrain|RecreatePod|InPlaceIfPossible|InPlaceOnly` and
`edit=restartPolicy:None|RecreateInstanceOnPodRestart` (rollout mode) patch
`spec.<member>.lifecycle` on every bumped member at a named hook —
`during=settling` (right after the bump lands) or `during=rotating` (the
first updated pod observed), the same rule the phase faults use — so a roll
that started under one strategy or policy finishes under another. The merge
patch changes only the named field: the `rollingUpdate` budget stays as the
case declared it, so it has to be valid for both strategies (a drain-first
strategy cannot roll under `maxUnavailable: 0`), and the header's ceilings
and floor must be stated for the worse of the two shapes. The usual rollout
checks judge the roll as a whole; the verdict adds that the edit was applied
at its hook (a refused patch is `INFRA-FAIL`) and that the live spec carries
it on every bumped member at the end. Across a strategy switch in-place UID
stability and surge-before-drain are informational, since the second strategy
legitimately breaks the first one's shape; convergence onto the new revision,
isolation, gang atomicity, index partition and the capacity floor stay hard.
A phase fault (`fault=podkill:rotating`) may share the case.

The generated `isvcs/gen-switch/` family (`p3800`+, `generate_cases.py`)
crosses the edit with the four topologies at one gang width: every ordered
pair of distinct strategies at both hooks, both restart-policy flips
(`None` to `RecreateInstanceOnPodRestart` and back) at both hooks under the
canonical surge budget, and on the PD topologies each cell again under a
`rollingUpdate` group, where the decoder carries the same start lifecycle
as the engine so the edit is valid on it too. One budget per pair serves
both strategies: `maxSurge: 1, maxUnavailable: 1` whenever `SurgeThenDrain`
is on either side, `maxSurge: 0, maxUnavailable: 1` otherwise; the ceiling
is N+1 whenever a surge step can be in flight on either side of the edit
and N otherwise, and the floor N-1 is the one the drain-first side needs,
asserted hard. No pair is skipped: the bump is an annotation diff, which
both in-place variants apply, and a gang runs either variant as a
whole-gang recreate. The family also carries the broken-while-switching
shape for the surge and recreate pair in both directions:
`fault=crashloop:old form=start` on the engine from the bump plus the
strategy edit at `rotating`, so the crashing Instance, reached last, is
fixed under the strategy the roll switched to (`floor_soft=1`, as on every
crashloop case).

### Fault cases: quota isolation, cordon impact, leak remediation

- **`fault=crashloop:old` with `form=start|load`** breaks one old-revision
  pod the moment the bump lands — the highest-index Instance's leader (or
  single) pod of the victim Component (`victimcomp=engine|decoder`, default
  engine), so the roll reaches it last and the crash is observed on a
  settled row for the whole roll. `form=start` points the live pod at the
  fixture's metrics image, which has no shell, so the fixture command dies at
  every start and the kubelet parks it in CrashLoopBackOff, never Ready again.
  `form=load` execs `kill 1` into the runner every
  `OME_TEST_CRASH_KILL_INTERVAL_S` (default 15) seconds while it is up, so it
  dies after Ready and comes back between crashes; the killer stops as soon as
  the pod leaves the old revision, is rebuilt (a new UID) or is gone. Only the
  image and an exec are available against a live pod, which is why neither
  form edits the template. On top of the usual rollout checks the verdict
  requires that Instance to end on the new revision with its pods serving,
  and no `RestartTriggered` event naming it after the bump: an Instance whose
  revision is not the roll target belongs to the roll, never to a repair on
  the revision it was running. A gang under SurgeThenDrain is replaced at a
  fresh Instance index and its source index retired, so the verdict follows
  the Instance across the gang surges by the start events the ISVC records —
  a `RecreateUpdateStarted` names the replacement index, a
  `GangSurgeAbandoned` retires one, and the starts the abandons do not cancel
  out are where the Instance went — and reads the index it ended at; the
  start is the record to follow because the source row is gone once the
  surge completes, so nothing in the status names the pair. A repair on any
  index of that chain fails the verdict. A victim on a Component the bump
  never touches (`victimcomp=decoder` with no group) has no new revision to
  reach, so the repair owns it instead: it must be rebuilt — no pod the fault
  hit survives — Ready and serving on its running revision, with a
  `RestartTriggered` event naming it; the usual isolation check still holds
  the sibling at N with nothing rotated. The generated `isvcs/gen-crashloop/`
  family crosses the verb with topology, strategy, maxUnavailable, restart
  policy and (on PD) a rolling group, so every path the closed-loop matrix in
  the workload package decides is also driven against a real kubelet. Two
  more generated families put the verb under every rollout-group kind
  (`co=none|bluegreen|rolling|seq|canary`; a canary group on a rollout case
  is only a group here, its ladder is not observed):
  `isvcs/gen-crashloop-idle/` breaks the engine, or the decoder,
  on the settled service (`during=idle`, below) and expects the repair on
  every shape, and `isvcs/gen-crashloop-sibling/` breaks the decoder as an
  engine-only bump lands (`bump=engine victimcomp=decoder`) and expects the
  engine roll to converge while the decoder's Instance is repaired. In both,
  the grouped cells are parked on the issue their header names, and
  `form=load` under `restartPolicy: None` is left out: a runner that dies
  after Ready is a container restart the kubelet owns under that policy, not
  a wedge, so no repair is promised there.

- **`fault=imagepull:old`** is the same break with an image that cannot be
  pulled: the victim's live pod is pointed at the harness's unpullable
  reference (`runtime_fixtures.UNPULLABLE_IMAGE`, the one image no deploy
  step side-loads), so the kubelet parks it in ImagePullBackOff on the
  running revision, never Ready again. It has only `form=start` (optional;
  an image that never runs has no after-Ready form), takes the same
  `victimcomp=` and `during=idle`, fires at the same hooks, and is judged by
  the same verdict: after a bump the Instance ends on the new revision
  serving with no repair recorded for it; at idle, `expect=repaired|held`.
  Nothing outlives the case: the patched pod belongs to the case's ISVC and
  goes with it at teardown. The generated `isvcs/gen-imagepull/` family
  fires it as the bump lands across topology, strategy, restart policy and
  (on PD) a rolling group, and on the idle service under both restart
  policies with `expect=repaired`.

- **`fault=configkey:missing` with `lift=after-settle|after-failed` and
  `expect=heals|parked`** (rollout mode, `during=idle` only) takes a cause
  away and gives it back. The case's victim Component reads a ConfigMap key
  through its runner env, and the case file carries that ConfigMap as a
  second document, so the apply creates it and the teardown removes it:

  ```yaml
  # on the victim Component (under leader: on a multi-node one, with name: ome-container)
  runner:
    env:
      - name: TEST_CONFIG_VALUE
        valueFrom: { configMapKeyRef: { name: p2500-config, key: value } }
  ---
  apiVersion: v1
  kind: ConfigMap
  metadata: { name: p2500-config }
  data: { value: "1" }
  ```

  The runner reads the reference off the victim's live pod (the break
  victim rule, `victimcomp=` as above), registers the restore, removes the
  key with a JSON-patch and restarts the runner with `kill 1`, so its next
  start waits with `CreateContainerConfigError` — on the same pod under
  `restartPolicy: None`, on the rebuilt one under
  `RecreateInstanceOnPodRestart`. `lift=after-settle` puts the key back
  once that wait has held for `LIFT_SETTLE_S` (20 s scaled); `lift=after-failed`
  once the victim's InferenceReplica row reads `Failed`, which needs the
  case's `lifecycle.instanceReadyTimeout` short enough to arrive inside the
  rotate budget (a row that never escalates fails the case). Then
  `expect=heals` asserts the Instance is Ready and serving again on its
  running revision within the budget, with no new revision and
  `ome.io/reset-instances` never written; `expect=parked` (with
  `lift=after-failed` and `issue=<n>`, the issue the self-heal is tracked
  under) asserts the documented behaviour instead: the row stays `Failed` and
  nothing of the Instance serves across the hold window after the lift, and
  a row that heals fails the case as a stale park, so the flip to
  `expect=heals` is forced the day the rule lands. `batch.py` lists
  `expect=parked` cases in its PARKED DEBT table. The restore also runs from
  the end-of-case cleanup (a no-op after the lift, harmless once the case's
  ConfigMap is gone with the case). The generated `isvcs/gen-heal/` family
  crosses the fault with topology, restart policy, lift phase and a rolling
  group; every case states the intended `expect=heals`, and the
  `lift=after-failed` half is parked (`notimpl=1` on the issue the headers
  name) until a Failed row gets its exit — `isvcs/MANIFEST.md` states per
  axis why the `after-settle` half heals as the controller stands.

- **`fault=node:notready` with `lift=<phase>` and
  `expect=relocated|repaired|parked`** (rollout mode, `during=idle` only)
  stops the kubelet of the KIND node under the victim and starts it again at
  the lift. The harness must run on the host whose docker owns the KIND
  nodes (the eval lanes do): the Node is a running container of the same
  name, and `docker exec <node> systemctl stop|start kubelet` is the fault
  and its lift (`OME_TEST_DOCKER` names another CLI). The capability is
  checked before anything is applied — docker on PATH and some Node a
  running container — and again when the victim is picked: the break victim
  rule (`victimcomp=` as above) runs over the pods on workers that are
  containers and host no manager pod (the control plane and the manager's
  node are never stopped). Either check failing records the verdict `SKIP`
  with the reason instead of a failure; `batch.py` counts it green and
  writes it as skipped in the JUnit report. The node is labelled
  `perm.test/kubelet-stopped=<case>` and the restart registered before the
  stop, so the end-of-case cleanup and `batch.py`'s end-of-run sweep both
  find it; node cases run serially (a dead node takes every pod on it). The
  fault has bitten once the Node reads NotReady and no pod of the Instance
  on that node serves (the node lifecycle controller's grace, 40 s by
  default, comes first; a gang whose other members sit on live nodes keeps
  them serving, so the bite is read on the stopped node's pods alone), and
  `expect=` says what the Instance's fate is under the restart policy and
  the force-delete policy as configured:
  `expect=relocated` with `lift=after-recover` — while the node is down the
  Instance comes back Ready and serving on new pods on other nodes (the
  force-delete policy frees its names), then the kubelet is restored;
  `expect=repaired` with `lift=after-settle|after-failed` — the kubelet is
  restored once the bite has held (or once the row reads `Failed`) and the
  Instance must be Ready and serving again within the budget, on its running
  revision, with no reset mailbox; `expect=parked` with `lift=after-settle` —
  across the hold window with the node down nothing rebuilds the Instance
  (no force delete configured), then the kubelet is restored and only the
  teardown is required to converge. A relocation may bring the replacement
  up beside the stuck pods, so the no-surge claim is informational there.
  The generated `isvcs/gen-node/` family crosses the verb with topology,
  restart policy, migration mode (Auto and Never) and an idle rolling
  group, and asserts the configured outcome in every cell: `parked` while
  the node is down and `repaired` once the kubelet is back, both at
  `lift=after-settle`. Without `lifecycle.forceDelete` in the lane's chart
  values nothing frees the names a dead node holds, so `relocated` is not a
  configured outcome there and the family does not emit it; neither
  migration mode moves a Ready Instance (Auto relocates on an expired
  attempt's deadline, which a Ready Instance never has), and each cell's
  line-2 reason says why its restart policy and group change nothing.

- **`fault=ctrlrestart:<hook>` under an operation in flight** is what the
  generated `isvcs/gen-ctrlrestart/` family drives: the manager pod is
  deleted mid-surge (`settling`), mid-drain (`rotating`), mid-migration
  (`mode=migrate`, `migrating`), mid-canary (`mode=canary`,
  `canary-serving`) and mid-scale-down (`mode=scaledown`, `settling`), on
  engine, pd and engine-mn under SurgeThenDrain and RecreatePod, with a
  rolling group crossed in on the PD rollout conditions. The usual verdict of
  each mode carries the claim: the operation resumes from status and
  converges, peaks stay within the ceiling, no index collides and every
  Component ends at its count. The fault is gated on
  `OME_TEST_ALLOW_CTRL_RESTART=1`, which the GKE eval lanes export; without
  it the case passes with a note and proves nothing. A restart while a
  crash-loop repair is open needs a second fault slot the runner lacks and
  is listed in the manifest's missing hooks.

- **`during=idle`** (rollout mode, with a persistent fault) fires the fault
  on the settled service and never bumps; the verdict reads what the product
  does about the break on its own, stated by `expect=`. `expect=repaired`:
  within the rotate budget the victim Instance is rebuilt (every pod new,
  Ready and serving) and a `RestartTriggered` event names it.
  `expect=held`: nothing rebuilds it across the hold window (`IDLE_HOLD_S`,
  90 s scaled by `OME_TEST_TIMEOUT_MULT`) and a `RepairHeld` event names it —
  the hold a rollout group keeps on repairs while no run is open; a silent
  hold fails. Absent, `expect=` is derived from the spec: held when the
  victim Component is a member of a `spec.rollout.groups` entry, repaired
  otherwise, so a family that asserts the repair on a grouped service states
  `expect=repaired` and parks it with `notimpl=1 issue=<n>` while the rule
  is missing (`isvcs/gen-crashloop-idle/` does). Either way no Component may
  ever exceed N alive: a repair rebuilds
  in place. `form=load` under `restartPolicy: None` is kept alive by the
  kubelet alone between kills; whether a repair opens there is the product's
  answer, and the killer stops the moment the pod is rebuilt.

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

Four generators write into `isvcs/`; re-run one after editing it:
- `gen_cases.py` — the feature matrix (migration/restart/partition/scale/edge/canary),
  permuting topology × lifecycle × coordination.
- `gen_mn.py` — the multi-node (gang) rollout cases (`p66`–`p99`, `p500`+) →
  `isvcs/rollout/` (`p500`+ is the continuation block: `p100`+ belongs to
  `gen_cases.py`'s feature counters).
- `gen_runtime.py` — the runtime-triggered cases (`p400`+) → `isvcs/runtime-rev/`.
- `generate_cases.py` — the hot-core permutation corpus (`p1000`–`p2799`
  and `p3000`–`p4999`) → `isvcs/gen-rollout/`, `isvcs/gen-fault/`,
  `isvcs/gen-restart/`, `isvcs/gen-crashloop/`, `isvcs/gen-switch/`,
  `isvcs/gen-crashloop-idle/`, `isvcs/gen-crashloop-sibling/`,
  `isvcs/gen-heal/`, `isvcs/gen-imagepull/`, `isvcs/gen-node/`,
  `isvcs/gen-ctrlrestart/`, plus `isvcs/MANIFEST.md` (per-dimension
  counts, constraint ledger, parked cells, missing-hook catalog).

A generator owns exactly the case ids it mints. Hand-written cases share the
feature folders with generated ones (`restart/`, `canary/`, `scale/` and
`runtime-rev/` all hold some) and need no registration: the shared write path
(`casefiles.py`) never deletes a file, writes only the ids it mints, and
refuses — writing nothing — to mint an id held by a case it does not
recognise as its own (same id and description on line 1). The header keys a
generator does not emit (`cell=`, `notimpl=`, `issue=`, `inv_xfail=`, …) and
the line-2 `# reason:` comment are the hand-maintained layer and ride through
a regeneration unchanged, each key where it sits on disk; the description, the
emitted keys and the body are generator-owned, so a hand edit to those is
drift. A case a generator stops minting stays on disk until removed by hand
(the regen summary lists every unminted id in its folders). Each generator's
`--check` regenerates in memory and byte-compares against the committed tree
without writing; `run_test.py` runs it in CI for `gen_cases.py`, `gen_mn.py`
and `generate_cases.py`, and regenerates a copy of the whole tree through the
real write paths to prove nothing changes and nothing is deleted.

The gen-* folders are generator-owned WHOLESALE: every case there carries the
`[gen]` mark on line 1, regeneration is byte-stable and `--check` rejects any
hand edit — promote a bug-finding case into a hand-owned folder under a fresh
id instead of editing it in place. A file without the mark in a gen-* folder
is hand-written: it stops the regeneration before anything is written or
swept, and must move to a hand-owned folder.

Every generator finishes with `run.py --validate-only`, so a regeneration that
would reintroduce a duplicate (shadowed) case id or a malformed header fails
the generation itself.

`isvcs/fault/` (`p600`+ — fault-injection cases: `fault=` header, plus the
`blocked=1` / `expect=fail` / `lift=1` contract knobs) is entirely
hand-written; no generator mints into it and the `p600`+ id block is
reserved for it. `isvcs/pairing/` (`p800`+ — the `mode=pairing` protocol
cases) is likewise hand-written, with the `p800`+ block reserved.

Coordination is emitted under `spec.rollout.groups[]` (OEP-0017 v2: BlueGreen ->
one `blueGreen` group; RollingUpdate -> a `rollingUpdate` group, +`maintainRatio`
for RatioBalanced; Sequential -> one single-Component `blueGreen` group per member
in order; a canary group, `co=canary`, on a rollout case whose ladder is not
driven). Keep
the `topo=` tag and any `infeasible=1` / `notimpl=1` header flags so the harness
groups and scores correctly.
```

## Coverage ledger (`coverage.py`)

`coverage.py` renders the scenario space the marathon tracks from the
committed cases: line 1 (the header) and line 2 (the `# reason:`) of every
case under `isvcs/`, plus the facts a header does not carry and the body
does (the engine's update strategy and budget, its restart policy, the gang
width, the rollout group kind, the canary steps).

```bash
python3 coverage.py            # the Markdown ledger on stdout
python3 coverage.py --cases    # ... plus one row per case
python3 coverage.py --json     # the same facts as JSON
python3 coverage.py --check    # nonzero on an unclassified header value or a story with no case
```

The ledger's cell is (shape family, operation, fault row):

- **shape family** — the Components present (from `Ne`/`Nd`/`Nr`: `engine`,
  `engine+router`, `engine+decoder+router`; `engine+decoder` is tabulated but
  outside the product) times `single` or `gang` (`topo=*-mn`), with the
  gang width and the Instance count (`1`, `2`, `many`) as details;
- **operation** — what the operator does, from `mode=` and its knobs:
  `steady state` (`mode=kill`, or a persistent fault at `during=idle`),
  `push` (a plain bump, `edit=env`, `mode=partition`, `mode=rebump`, a
  pairing-protocol patch), `rollback` (of a plain rollout; a canary's
  rollback is a step kind), `scale up` / `scale down` (`mode=scaledown` by
  its targets), `migrate`, `delete` (`mode=recreate`), `pause` / `freeze`
  (`pause=` under a kill, lifted by the harness), `canary`, `strategy or
  policy edit` (`edit=strategy:*` / `edit=restartPolicy:*`), `runtime edit`
  (`mode=runtimerev`). `admission reject` and `remove component` are counted
  but not crossed with the faults. A second operation landing on the first
  (a scale, pause or push at a canary hook; `during=rollout|migrate`) is
  the `operation+operation` overlap;
- **fault row** — (kind, form, side). `fault=crashloop:old` is `crash at
  start` / `crash after Ready` by `form=`, `imagepull:old` is `image pull`,
  `configkey:missing` is `config missing`, `node:notready` and
  `cordon:source` are `node`, `quota:<n>` is `quota`, `ctrlrestart` is
  `controller restart`; `mode=kill`, `podkill` and `pkill=1` are `pod
  deleted by hand` on a single-pod service and `gang member lost` on a gang;
  a canary's `img=` is `image pull` on the new revision, or `sibling broken`
  when `imgcomp=` names the decoder or the router; `break=stuck` is
  `readiness never passes`; a runtime-rev `expect=recover` is `image pull`
  on the new revision; `analysis=breach` is `metric breach` (one-shot when
  `health=breach` decays under `analysis=pass`); `analysis=inconclusive` is
  `analysis source unusable`. The form is `one-shot` when the harness takes
  the cause away again (`lift=`, `verb=quotalift`, a decaying breach) or
  the controller heals it once (a pod delete, a controller restart), else
  `persistent`. The side follows `victim=`, the break's `old` victim, and
  run.py's component-blind podkill rule (the newest new-revision pod dies
  mid-rotation under a replacement-first strategy, a settled pod otherwise).
  A case with two faults counts in both rows.

Cells the product does not need carry a written reason in `EXCLUDED_CELLS`
(a steady state with no fault asserts nothing; a single-pod Instance has no
gang member to lose; on a gang a hand-deleted pod is a lost member). The
report lists the empty cells biggest holes first (the fault rows,
operations and shape families with no case anywhere, then counts per axis
value, then every hole by name), crosses the operations and faults with the
group policy, update strategy, budget and restart policy, places every
event on its timing hook (`settling`, `rotating`, the canary states, `paused`,
...), tallies the overlaps and the canary step kinds, and lists the parked
cases with their issues.

The operator stories of the marathon goal live in `STORIES` at the top of
the script, one predicate over a parsed case per story; the report prints the
cases and cells that prove each, and `--check` fails on a story with no
case. `run_test.py` runs `coverage.py --check` beside the generator checks,
so a header value the ledger cannot classify (a new verb, a new fault kind)
fails CI until it is mapped to an axis there. What no header can express is
listed under "Not derivable from the headers" in the report (the scheduler
lane, production profiles, the `last Instance` and `held floor` timings,
auto versus manual migration, the webhook, apiserver and eviction faults).

## Future knobs

Dimensions the generators do not permute yet. Each is a candidate axis
once its KIND spec settles; until then it stays hand-tested.

- `lifecycle.minReadySeconds` — the Ready-age window a rollout waits
  before draining or promoting past a new pod. The KIND gate covers the
  single-pod SurgeThenDrain shape (`minreadyseconds_kind_test.go`);
  gang surge, RecreatePod, and in-place under a window, and the
  cluster-wide `deploy.minReadySeconds` default, are not yet in the
  matrix.
