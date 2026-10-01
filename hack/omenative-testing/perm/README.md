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
> **Force-delete dimension.** `OME_EVAL_FORCE_DELETE=1` makes
> `deploy-ome-kind.sh` set the controller's `lifecycle.forceDelete` policy
> (off by chart default): `OME_EVAL_FORCE_DELETE_THRESHOLD` (default `90s`)
> becomes `nodeUnreachableThreshold`, the age the node's unreachable evidence
> must reach before the policy acts, and `OME_EVAL_FORCE_DELETE_SLACK`
> (default `30s`) becomes `overdueSlack`, how long past a Terminating pod's own
> deadline before it counts as wedged. A node-loss case names the policy its
> outcome is read against with `needs=forcedelete|no-forcedelete`, and the
> runner reads the deployed `inferenceservice-config` lifecycle block before
> anything is applied: where it differs the case records `SKIP` with the
> reason. CI emits a policy-on twin (`omenative-e2e-<feature>-<topology>-forcedelete`,
> and its scheduler twin) for every lane that has `needs=forcedelete` cases,
> carrying exactly those cases; the plain lane keeps the rest. The threshold
> is longer than the settle window after which a `repaired` case starts the
> kubelet again (the node has read NotReady for 30–40 s by then, so the
> policy never acts on that half) and short enough that a relocation lands
> about three minutes after the stop, well inside the case budget.
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
│   ├── gen-rollout/ gen-fault/ gen-restart/ gen-crashloop/ gen-switch/ gen-crashloop-idle/ gen-crashloop-sibling/
│   │   gen-heal/ gen-imagepull/ gen-node/ gen-ctrlrestart/ gen-rollback/ gen-crashloop-new/ gen-crashloop-group/ gen-pause-repair/ gen-ops-held/ gen-runtime-edit-repair/ gen-migrate-bump/  — generated hot core (see isvcs/MANIFEST.md)
│   ├── gen-canary-crash/ gen-canary-second-rev/ gen-canary-podkill/ gen-canary-fault-old/ gen-canary-fault-new/  — generated operator stories driven through a canary
│   └── gen-size/ gen-router-gang/  — generated small fleets (one and two Instances per Component) and the engine gang behind a router
└── results/
    └── <RUN_TS>/<feature>/<topo>/
        ├── pNN-<ts>.tsv   — one verdict row per case
        └── pNN.log        — full per-case log
    └── <RUN_TS>/summary.txt — the batch summary table for that run
```

## The runtime — why the startup sleep matters

Read `runtime/csr.yaml`. The critical line:

```yaml
command: ["sh", "-c", "trap 'exit 0' QUIT TERM INT; sleep ${TEST_STARTUP_DELAY:-15} & wait $! && … || exit 1; [ -n \"${TEST_EXIT_AFTER}\" ] || exec nginx …; nginx … & sleep ${TEST_EXIT_AFTER} & wait $!; exit 1"]   # TEST_STARTUP_DELAY=15
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

`TEST_EXIT_AFTER` is the runtime's one fault switch. Unset (every fixture and
every case leaves it unset), the command execs nginx exactly as above. Set to
a number of seconds, the shell stays PID 1, serves through nginx for that long
after it is up, then exits 1 — every time the kubelet restarts the container —
so the pod passes readiness and crashes after it. Only `fault=crashloop:new
form=load` sets it, on the pushed revision's runner env; the trap still answers
a stop signal during the window, so such a pod drains fast too.

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

An engine-only shape may carry a single-pod router in front of it (`Nr>0`
with `Nd=0`, `topo=engine` or `engine-mn`); the router is never a bumped
member there, so the rollout mode holds it as a negative control and the
canary mode asserts it idle. The coverage ledger reads that as the
`engine+router` family.

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
`inv_xfail=A|B|C|D|F|G|H|all` (requires the line-2 `# reason:` too): exactly that
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
  **`release=promote|resume|rebump-fix`** fires that verb `releaseafter=<s>`
  seconds (default 20, `RELEASE_AFTER_S`) after the second verb, so a case
  whose second verb leaves a hold in place can end: a promote or resume lifts
  a pre-step hold (needs `verb2=` and `during2=canary-prehold`), a
  `rebump-fix` lifts a capacity gate held by an image that cannot start
  (needs `verb2=` and `img=`); the verdict asserts the ladder was still held
  at the same step, in the same state, when the release fired, and that it
  then completes.
- **`verb=`** — the operator verb to fire at the hook: `pause` / `pause-freeze`
  (lifted after `verbhold=<s>`, default 45; the step index must not move while
  held), `promote` (the matching hash; with `skipsoak=<s>` the case asserts it
  beat a timed step's soak), `promote-stale` (a real hash token naming the
  stable revision; the step must not move for 20 s), `rollback`, `rebump` (a
  second revision bump of the bumped members), `rebump-fix` (a bump that also
  drops the runner image override), `rtedit` (a material edit, the runner env
  the runtime-rev cases bump, of the case's dedicated runtime named by
  `rt=<dedicated CSR name>` and shipped as the case file's first document:
  the second change of a sync that applies the runtime after the service),
  `podkill` (deletes one pod by hand, aimed like the fault with
  `victim=stable|canary` and `victimcomp=`; the verdict waits for the pod that
  replaces it and holds it to label truth: it must run the image the pods of
  its revision ran when the deletion landed, never a newer template under the
  old label; under `canary=manual` the harness holds its promote `verbhold=`
  seconds after the deletion, so the replacement comes up while the roll is
  paused), `timeout-clear` and `timeout-set` (writes
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
  canary pod or at an old-revision pod. The break faults
  (`crashloop:canary-<state>` with `form=start|load`, `imagepull:canary-<state>`)
  break the stable side in that state: `victims=<n>` (default 1) breaks that
  many old-revision Instances of `victimcomp=` at once, highest index first,
  the storm shape. The verdict reads every victim as the rollout-mode break
  does (each ends on the canary revision serving, no `RestartTriggered` event
  names one after the bump), and the capacity floor is read with the victims
  allowed for: the healthy Instances must hold `N - maxUnavailable - n`.
  `fault=<kind>:idle` lands the same break on the settled service before the
  push: the push waits until the break has visibly bitten (`BREAK_BITE_S`,
  60 s scaled), so it meets a fleet that is already broken, and the verdict
  reads the victims as above. Under `canary=manual` the harness holds its
  promote `verbhold=` seconds (default 45) after a break fired at a hook, so
  the broken stable side is observed while the gate holds, as an operator
  would see it before promoting.
  Every re-target verb (`rebump`, `rebump-fix`, `rtedit`) additionally
  asserts, at a live hook, that the run open when it fired closed as
  `Superseded`, that no Instance is left on a pre-re-target revision at
  Stable, and that no weighted traffic target named a revision with no
  serving pod of the primary for `TRAFFIC_GAP_POLLS` consecutive polls (the
  same gap is reported, not failed, on every other canary case).
- **`bump=<members>`** — bump only these group members (an engine-only or
  decoder-only change under a router unit); `img=<ref>` gives the bumped
  revision an image so the capacity gate never opens. Name the harness's
  unpullable image for that, `example.com/canary-test/missing:none`
  (`runtime_fixtures.UNPULLABLE_IMAGE`): it is the one case image the deploy
  step never side-loads, so it fails to pull offline as well as online.
- **`fault=crashloop:new` with `form=start|load`, `fault=imagepull:new`** —
  the push itself brings a broken revision. The break is armed on the runner
  of one bumped member (`victimcomp=`, default engine) before the push lands,
  never injected into a running pod: `form=start` gives it the fixture's
  shell-less metrics image, so the fixture command dies at every start;
  `imagepull:new` gives it the unpullable image; `form=load` sets the fixture
  runtime's `TEST_EXIT_AFTER` switch to `exitafter=` seconds
  (`OME_TEST_CRASH_EXIT_AFTER_S`, default 20, when the header leaves it out),
  so the pod passes readiness and exits that long after it is up, every time
  the kubelet restarts it. `img=` and `victims=` do not apply. The exit is the
  case's verb at `during=canary-failed`: `rollback` (`terminal=RolledBack`) or
  `rebump-fix` (`terminal=Stable`; the fix push drops the runner override, env
  and image alike). The verdict is the operator's end state: the break bit (a
  pushed revision that never breaks is `INFRA-FAIL`), the ladder parked at the
  step where it bit with the unit's `canary.failed.reason` set, no traffic
  named the broken revision at the park while the stable side kept serving
  every Instance the broken revision had not replaced, nothing reached
  `Promoting` before the exit landed, and then the exit's own terminal checks
  (drained back with the rejected revision recorded, or every Instance on the
  fixed revision with nothing left on the broken one). The capacity floor is
  read with the Instances the broken revision reached allowed for. Under an
  analysis gate only the pull failure is armed: the metrics image is a static
  binary with no shell and no exit switch.
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

### Rollback of a plain rollout (`rollback=1`, `during=`, `break=stuck`)

`rollback=1` (rollout mode) re-applies the revision the run started from:
every bumped member's spec as it was before the bump, put back with a JSON
`replace` of `spec.<member>` at the named hook, so the controller sees the
starting revision's hash again as the target. `during=` names the moment:

- `settling` — the moment the bump has landed, before any Instance moved
  (two pushes in a row, the shape of a GitOps sync that applies two objects);
- `rotating` — the first pod of the new revision is observed;
- `draining` — the first pod of the starting revision is seen draining (a
  surge roll reaches it when its first Instance is promoted, a drain-first
  roll at once; a single-pod in-place roll never does, it patches in place);
- `last` — exactly one Instance of the bumped members is still off the new
  revision: every other one is promoted (full pod set on it, Ready and
  serving), read off the watcher so it is caught between two events;
- `held` — the roll is parked at its budget. Only with `break=stuck`, which
  gives the new revision a readiness probe it never passes (the fixture's
  probe aimed at a path the runner does not serve, under `runner` or the
  leader's runner), so its pods run, never turn Ready and never escalate;
  the hook fires once a new-revision pod has been up for `HELD_SETTLE_S`
  (30 s scaled) with none Ready. A probe is a diff beyond an in-place patch,
  so under an in-place strategy both the new revision and its rollback
  are recreated.

The verdict is the operator's end state. Every Instance of the bumped
members ends on the starting revision and serving (N/N, no pod of the new
revision alive), the InferenceReplica rows are settled (every Instance
Ready with no open operation, `currentRevision` and `updateRevision` both
the starting revision), and on a grouped service the run that targeted the
new revision closed as superseded (`RolloutRunClosed` … superseded; a
rollback that lands before any run opened leaves nothing to close and is
reported, an ungrouped service has no run model). The usual rollout checks
still judge the whole case: peak ceilings, the capacity floor (which covers
the roll back too), isolation, gang atomicity, index partition; an in-place
single-pod roll must come back in place (UIDs stable). A roll that
completes on the new revision before its hook is reached ends the case as
`HOOK_MISSED` and fails. The verdict also records, as a measurement and not
a failure, how many Instances that were still on the starting revision at
the rollback were rebuilt anyway (an Instance that never left it should
ideally be untouched); the counts decide whether it becomes an assertion.
The generated `isvcs/gen-rollback/` family crosses the hooks with topology,
strategy (budget 1, plus maxSurge 2 on the single-pod engine) and a
`rollingUpdate` group (what puts the roll under the run model, so the
engine-only shapes carry a single-member group); the starting revision
carries `perm.test/rev: "v1"` so the rollback is an annotation update, not
a removal. `rollback=1` in canary mode keeps its own meaning (the
`ome.io/rollout-rollback` probe).

### Small fleets and the engine behind a router (`gen-size/`, `gen-router-gang/`)

The engine resolves a percent budget as `ceil(replicas * percent / 100)`,
clamped to the replica count — the rule the `RollingUpdate.maxSurge` and
`maxUnavailable` fields document, which the canary step capacity shares —
so on one or two Instances `25%` and `50%` both resolve to one, and no
positive percent ever rounds to zero: a roll always starts. The capacity
floor the harness reads (`N - maxUnavailable`) resolves a percent the same
way, so a `50%` budget on one Instance reads as a floor of zero, the
configured outcome, not a breach.

The generated `isvcs/gen-size/` family (`p8000`+) runs one and two
Instances per Component (the router sized with the fleet) on the engine
and prefill/decode shapes:

- the plain push under each strategy and budget (`SurgeThenDrain` at
  `maxSurge` 1 and `25%`, `RecreatePod` at `maxUnavailable` 1 and `50%`,
  `InPlaceIfPossible` at 1). The surge pushes assert the floor
  instantaneously (`floor_hard=1`): one Instance surges to two and is never
  left with zero serving. Under `RecreatePod` the single Instance goes down
  and comes back, zero serving being the configured outcome, which the case
  says on line two;
- the push with the running revision crash-looping (`fault=crashloop:old`,
  both forms, both restart policies) under the integer budgets, where on one
  Instance the broken Instance is both the first and the last the roll
  reaches;
- a scale from one to two and from two to one under each strategy
  (`mode=scaledown`): index 0 keeps its pod and the floor is
  `min(start, target)`;
- a canary ladder on two Instances (`10/50/100` percent capacity and
  traffic, timed and manual gates, under the surge and the recreate
  strategy): the 10 percent step asks `ceil(0.2) = 1` Instance and the 50
  percent step asks the same one, so it moves traffic and adds no capacity;
  the stable revision keeps one Instance until the cutover's traffic write
  lands. A canary on a single Instance has no Instance to hold on the stable
  revision and stages in place; that shape waits for its own verdict.

`isvcs/gen-router-gang/` (`p8100`+) runs the engine as a gang (one leader
plus one or two workers, two Instances) behind a single-pod router — the
`engine+router gang` family of the coverage ledger, `topo=engine-mn` with
`Nr>0` — through a push under each strategy, a crash loop on the running
revision under the surge and recreate strategies, a scale to one and to
three gangs, and an engine-only canary ladder (the group names the engine
alone, as `p316` does without a decoder). The router is never bumped: every
cell holds it at its count on the same pods, and the gang invariants
(atomicity, index partition) judge the engine.

### Operations on a stuck Component (`stuck=held|failed`)

`stuck=held|failed` with `img=<unpullable image>` (`mode=scaledown` and
`mode=recreate`) lands the mode's operation on a Component that is stuck
on a revision it cannot pull. After the initial settle the runner pushes
every bumped member (the coordination group's members, else the engine)
onto `img=` — name the harness's unpullable reference,
`example.com/canary-test/missing:none` (`runtime_fixtures.UNPULLABLE_IMAGE`)
— and waits, reading the InferenceReplica: `stuck=failed` is the first
failed attempt (an Instance row reads `Failed`, or the retry block for the
pushed revision records a started attempt), `stuck=held` is the retry
ladder spent (a retry block in state `Held`, or the hold the status derives
from it). The ladder's length is the cluster's lifecycle configuration
(every attempt waits out the stuck-pod grace, then its backoff), so the
wait for `held` has its own budget (`STUCK_HELD_WAIT_S`, scaled). A push
that never reaches the named state within budget ends the case as `FAIL`
with the operation not applied.

Then the operation, judged on what the engine promises:

- **a scale-down** (`to_e=` below N; `to_d=` too when the decoder is a
  bumped member) removes the Instances that are not Ready first, highest
  index first within that tier, then Ready Instances highest index first,
  so a stuck Instance goes before a healthy one. The verdict snapshots the
  rows at the moment the scale lands and asserts exactly those Instances
  went and the rest stayed; that no survivor lost its running-revision pods
  to be rebuilt on the same revision; that under `held` no survivor was
  taken onto the pushed revision afterwards (the hold freezes the roll;
  under `failed` the ladder's next attempt may take one, reported `ⓘ`);
  and that the healthy survivors kept serving throughout (a floor of the
  Ready survivors, less the unavailability budget under `failed` on a
  drain-first strategy, debounced like the rollout floor).
- **a request for zero** (`to_e=0`, patched as `minReplicas: 0` with
  `maxReplicas` at one, the least the API admits) is kept as the API
  answers it: a plain Component runs one Instance for zero, so the verdict converges on the
  replica count the controller accepted (read from the InferenceReplica,
  never assumed) with the same victim order, which leaves the healthy
  Instance and takes the stuck ones.
- **a delete** (`mode=recreate`) is the recreate contract on a stuck fleet:
  every pod goes, stuck ones included, the teardown converges within its
  budget (no pod left Terminating, no finalizer wedge), and the same name
  comes back Ready on fresh pods carrying nothing of the push (no hold, no
  retry block, no Failed row).
- **a scale-up** (`to_e=` above N) while `held` starts nothing: the new
  index waits as an empty slot across the hold window, no pod is rendered
  from the running revision, and the survivors stay as they were. While
  `failed` the new Instance is admitted on the pushed revision when the
  retry window allows it, never on the running revision, and is stuck like
  its peers; the watch runs until the ladder is spent and reports whether
  the window admitted it.

On every operation the Components the push never touched (a PD decoder and
router with no group, the router with one) must stay as they were, and the
status must keep reporting the push that has not landed: the update
revision still differs from the current one and a hold, a retry block or a
Failed row says why; under `held` the hold itself must still be reported
(only a new revision or a release lifts it). The generated
`isvcs/gen-ops-held/` family crosses the two stuck kinds with the four
operations, the surge and the drain-first strategies, the engine, PD and
gang shapes, and a rolling group on PD.

### A runtime edit and a repair overlapping (`mode=runtimerev` with a break)

A break (`fault=crashloop:old form=start|load`, or `fault=imagepull:old`)
may ride a runtime edit that rolls (`expect=roll`), in either order:

- without `during=`, the break fires as the edit lands on the dedicated
  runtime, so the broken Instance is off the target from that moment and
  the roll owns it: it ends on the edited runtime's revision serving, with
  no `RestartTriggered` event naming it after the edit — the same verdict
  as a break under an ISVC bump;
- with **`during=repair`**, the break fires on the settled service and the
  runner waits for the product to open the repair (a `RestartTriggered`
  event, a `Restarting` row, or a rebuilt pod) and then for the repair's
  replacement pod to be up and not yet Ready — the one moment that is the
  same on every run — before editing the runtime. The last change wins:
  every Instance, the repaired one included, ends on the post-edit revision
  serving; after the edit the repaired Instance got no more new pods than
  it has pod slots (rebuilt at most once; more is the repair and the roll
  each rebuilding it) and no repair is recorded for it from then on.

Both orders add that every Instance row settles after the roll (Ready, no
operation open; polled for `ROWS_SETTLE_S`): no Instance is left
mid-operation. The generated `isvcs/gen-runtime-edit-repair/` family crosses
the order with the engine and PD shapes, the surge, drain-first and in-place
strategies, both restart policies, both crash forms and the image and env
edits; the edit-during-repair order skips a crash after Ready under
`restartPolicy: None`, where no repair opens.

### A move and a revision bump that overlap (`mode=migrate then=bump at=…`, `during=settling|rotating|last`)

An Instance is being moved to another node by request and, while the move
is in flight, the operator pushes a new revision; or the reverse, a roll is
in flight and a move is requested. Both are migrate cases, so the usual
migration checks apply (the ledger reaches Completed, the replacement
surges off the node it left, the source drains, the request is consumed,
the peak ceiling, the capacity floor, gang atomicity, index partition), and
one of two knobs says which operation comes first and when the other lands:

- **`then=bump at=accept|surge|drain`** — the move first. The request is
  filed on the settled service and the bump of the bumped members (the
  group members, else the engine) is filed at the named moment of the move:
  `accept` once the request is recorded (the controller's record in
  `status.migrations`, the ledger row or the consumed annotation, whichever
  shows first), `surge` once the replacement pod has appeared (a pod the
  fleet did not have, or the record at `SurgeReady`), `drain` once a source
  pod is being taken down or gone (or the record at `Draining`). The record
  is read every 2 s while the hook is armed, the pods at every tick.
- **`during=settling|rotating|last`** — the roll first. The bump lands, then
  the request is filed at the named moment of the roll: `settling` at once,
  `rotating` at the first pod of the new revision, `last` when exactly one
  Instance of the moved Component is still off the new revision (every
  other one promoted: full pod set on it, Ready and serving; a grouped
  decoder's own progress does not count). `last` is paired with
  **`inst=last`**: the request names the Instance the roll has not
  reached, resolved when the hook fires (a steady one before one
  mid-operation, the highest index first). `during=rollout` is the older
  spelling of `rotating`. A hook the roll or the move completes before is
  recorded as missed and fails the case; nothing else is judged then.

The verdict is the operator's end state, whichever operation finished first
(the order is reported, never asserted): the roll converges onto the new
revision on every Instance of the bumped members; the moved Instance (the
index the record names) ends among them, Ready and serving (a gang roll
under SurgeThenDrain may rebuild that index under another one, which is
reported); the controller's rows are settled with no open operation and
name the new revision; the move is recorded once and consumed once (one
ledger row for the request, no other row, one controller record);
Components outside the roll keep the same pods on their revision. The node
the replacement came up on is compared with the node it left and reported
(a one-node pool cannot satisfy it). Ceilings count the move's one
replacement against the roll's surge: `N + max(maxSurge, 1)` under
SurgeThenDrain, `N + 1` otherwise — the move and the roll together never
exceed the roll's budget. The generated `isvcs/gen-migrate-bump/` family
(`p6200`+) crosses the six moments with topology, strategy (budget 1, plus
maxSurge 2 on the single-pod engine, where `last` is not observable: a
surge of two promotes its last pair together) and a `rollingUpdate` group
(a single-member group on the engine-only shapes). The move is always by
request: the engine's own migration is a born-terminal relocation record
written when a lost Instance is rebuilt elsewhere, so only a node loss
provokes it, and that belongs to the node-loss family.

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
  the Instance across the gang surges and reads the index it ended at. Two
  records name the pairs. The InferenceReplica's status rows: a source row
  carries its replacement index from the claim until the row is removed as
  the surge completes, and the invariant poll, which reads the rows every
  few seconds, keeps every pair it sees claimed (a source seen again on its
  running revision without the claim was reset, and its pair is dropped).
  Then the ISVC's events: a `RecreateUpdateStarted` names the replacement
  index, a `GangSurgeAbandoned` retires one, and the starts the abandons do
  not cancel out are where the Instance went. The rows are read first
  because the Kubernetes event recorder drops an object's events past a
  burst, which a many-Instance fleet reaches before its surges start. A
  repair on any index of that chain fails the verdict. A victim on a Component the bump
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
  every cell runs live, grouped or not (an Instance that serves nothing is
  repaired without touching its Component's budget or a group's gate), and
  `form=load` under `restartPolicy: None` is left out: a runner that dies
  after Ready is a container restart the kubelet owns under that policy, not
  a wedge, so no repair is promised there.

- **`fixfirst=1`** (with a break on the running revision after a bump) adds
  the order claim to that verdict: the broken Instance is never the last of
  its Component to land the fix, read from the creation time of each
  Instance's first new-revision pod (so it judges the strategies that
  rebuild pods). Its capacity is already lost, so a roll that reaches it
  last leaves the operator short for the whole roll. The generated
  `isvcs/gen-crashloop-group/` family carries it: the running-revision crash
  under each group policy (blueGreen, Sequential, maintainRatio) on the
  shapes that have a peer for it, and under the zero budgets of a plain
  rolling group (maxUnavailable 0 with maxSurge 1, maxSurge 0 with
  maxUnavailable 1), crossed with both forms, both drain orders and both
  restart policies. Each case's second line says what the policy's own rule
  lets it assert: blueGreen flips traffic only once the whole new set is
  complete and cannot replace the broken Instance ahead of the set, so those
  cases assert the per-budget roll and the order, and read the flip as
  status only; Sequential's component order and maintainRatio's band are
  the ordinary ordering and ratio checks.

- **`fault=crashloop:new` with `form=start|load`** is the other side: the
  push itself brings a revision whose pods crash. The crash is armed on the
  bumped member's runner before the push lands, never injected into a
  running pod (`victimcomp=`, default engine, names a member the push
  bumps): `form=start` gives it the fixture's shell-less metrics image, so
  the fixture command dies at every start (an image-only diff, so an
  in-place strategy really patches in place); `form=load` sets the fixture
  runtime's `TEST_EXIT_AFTER` switch (`OME_TEST_CRASH_EXIT_AFTER_S`, default
  20), so the pod passes readiness, is promoted, and exits that many seconds
  later, every time the kubelet restarts it — a pod-template env diff, so an
  in-place strategy resolves to a recreate. The verdict is the operator's
  end state. While the crashing revision is in flight: no more Instances
  than the larger of maxSurge and maxUnavailable are off the running
  revision, which therefore keeps serving at that floor (debounced like the
  capacity floor), never more than that many Instances carry the crashing
  revision at once, status says why the roll stopped — a `rolloutHold` on
  the victim Component or one of its Instance rows at `Failed`, and a
  `RetryHeld` or `InstanceFailed` event naming it — within the rotate
  budget, and once it has, no further pod of the crashing revision appears
  across `IDLE_HOLD_S`; a sound group member converges on its own new
  revision meanwhile, or holds with a reason. Then the harness pushes
  `rebump-fix` (a `v3` bump that drops the runner override) and every bumped
  Instance must end on the fixed revision and serving within the rotate
  budget, with nothing rebuilding the crashing revision after the fix landed
  and no `rolloutHold` left in status. Peaks, isolation of the untouched
  Components and gang atomicity hold throughout. A pushed revision that
  never crashes is `INFRA-FAIL` (the arm failed, not the product). The
  generated `isvcs/gen-crashloop-new/` family crosses both forms with
  topology, strategy (SurgeThenDrain, RecreatePod and InPlaceIfPossible at
  their canonical budgets), restart policy and, on PD, a rolling group.

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
  the force-delete policy as configured, and `needs=` names that policy:
  `expect=relocated` with `lift=after-recover` and `needs=forcedelete` —
  while the node is down the policy frees the pod names it holds once the
  node's unreachable evidence is older than the threshold, and the Instance
  comes back Ready and serving on other nodes (every pod the node held
  replaced; members on live nodes may keep theirs) within the threshold plus
  one Instance's rotate budget; then the kubelet is restored, every pod the
  node held must leave the API within the hold window and the Instance must
  still serve on its running revision with no reset mailbox;
  `expect=repaired` with `lift=after-settle|after-failed` — the kubelet is
  restored once the bite has held (or once the row reads `Failed`) and the
  Instance must be Ready and serving again within the budget, on its running
  revision, with no reset mailbox; `expect=parked` with `lift=after-settle`
  and `needs=no-forcedelete` — across the hold window with the node down
  nothing rebuilds the Instance (no force delete configured), then the
  kubelet is restored and only the teardown is required to converge. A
  relocation may bring the replacement up beside the stuck pods, so the
  no-surge claim is informational there.
  The generated `isvcs/gen-node/` family crosses the verb with topology,
  restart policy, migration mode (Auto and Never) and an idle rolling
  group, in two halves that each run on the lane whose controller config
  they read: without the policy, `parked` while the node is down and
  `repaired` once the kubelet is back, both at `lift=after-settle`; with the
  policy (the force-delete twin), `relocated` at `lift=after-recover`.
  Neither migration mode plays a part (Auto relocates on an expired
  attempt's deadline, which a Ready Instance never has; the relocation is
  the force-delete sweep plus the ordinary rebuild), and each cell's line-2
  reason says what its restart policy and group do.

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
  otherwise; a family that asserts the repair on a grouped service states
  `expect=repaired` (`isvcs/gen-crashloop-idle/` does on every cell: an
  Instance that serves nothing is repaired without touching the budget or
  the gate, whatever group it sits in). Either way no Component may ever
  exceed N alive: a repair rebuilds in place. `form=load` under
  `restartPolicy: None` is kept alive by the
  kubelet alone between kills; whether a repair opens there is the product's
  answer, and the killer stops the moment the pod is rebuilt.

- **`pause=true|freeze` with `pauseat=before|during|after` and `push=fix`**
  (rollout mode, `during=idle` with a break fault) is a pause, or a freeze,
  around the repair of the broken Instance. `pause=true` is the plain pause:
  no fleet change starts or advances, but the restart policy keeps repairing
  Instances at the revision they run. `pause=freeze` is the deeper one: the
  repair pass is suspended too, except a repair already under way, which
  finishes its step. `pauseat=` says when the pause lands against the
  repair: `before` sets the annotation and then fires the break, `during`
  waits for the first sign of the repair (a `RestartTriggered` event naming
  the Instance, its row `Restarting`, or a pod of it gone or replaced) and
  pauses while it is in flight, `after` waits for the Instance to be rebuilt
  and serving first. `push=fix` then bumps the revision while the service is
  paused (the bump re-renders the broken pod from its template, so it is the
  fix); absent, nothing is pushed. The service is held for `IDLE_HOLD_S`,
  measured from the pause or the break, whichever came last, and unpaused.
  The verdict holds the product to what a pause promises: while paused no
  Instance other than the broken one gets a new pod and no pod moves to
  another revision; the broken Instance is repaired before the unpause
  under a plain pause and under a freeze that caught the repair in flight,
  and left as it stood under a freeze that landed first or once its repair
  was already done; after the unpause, with the fix pushed, every bumped
  Instance ends Ready and serving on the new revision within the roll's
  budget and the broken one is never repaired again on the revision it was
  running (the roll owns it); with nothing pushed, a freeze that landed first
  releases exactly one repair, recorded by its event, and every other cell
  moves nothing; in every cell the InferenceReplica rows end Ready with no
  operation open and peaks stay under the header ceilings. A repair that
  never opens, or never finishes, leaves the pause nothing to land on and
  fails the case; a repair the product announces it is holding (a
  `RepairHeld` event naming the Instance) ends that wait at once, with the
  hold's own words in the verdict. The generated `isvcs/gen-pause-repair/` family crosses
  the verb with the three single-gang shapes, both crash forms under the
  restart policies that promise a repair, both depths, the three moments,
  the push and a `rollingUpdate` group; the engine runs `maxUnavailable: 1`
  so the repair fits the unavailability budget, and the grouped cells run
  live: a repair of an Instance that serves nothing is admitted without the
  budget or the gate.

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

### Continuous invariants

Every case, whatever its verb, is also judged on conditions the verb's own
verdict cannot see. The watcher samples them every `INV_TICK_S` for the whole
case, from the first pod to the handler's last read; a condition has to hold
across consecutive ticks before it fires, and a fired one FAILs the verdict
unless the case parks exactly that class with `inv_xfail=` (or waives it with
`inv_off=` and a line-2 reason). Every fired violation names the pod or
Instance and the two values that disagree. The classes: **A status-truth**
and **B endpoints-membership** (the parking paragraph above), **C pair-alive**
(*Pairing protocol*), **D gang-scheduling** (*Scheduler dimension*), **E
cohort ratio** (the informational PD observer under *PD knobs*), and:

**F label-truth** — every live pod runs the template its
`ome.io/revision-hash` label names. The reading is the ControllerRevision of
that hash (OMENative stores each revision's `podSpec`, `podMeta` and, for a
gang, `workerPodSpec`; the list is fetched only while a live pod carries a
hash the ledger has not read, and re-listed a bounded number of times): the
pod's container images, its `perm.test/rev` annotation, and the literal env
values and resource requests of every template container must match,
template-is-a-subset-of-pod, so sidecars, admission defaults and the
renderer's `OME_*` env never count. Until a hash's revision is visible the
first pod seen under that hash stands in, so a pod rebuilt from a newer
template under an old label — what an operator deleting a pod under a held
or paused roll produces — still disagrees with the pods that carried the
label first. A pod whose spec the harness changed itself — the `crashloop`
and `imagepull` breaks point a live pod at another image — is not judged
and the verdict lists it; the replacement the engine builds is a new UID
and is judged. A violation reads `invariant F violated
(label-template-mismatch): engine/1 pod <name> carries
ome.io/revision-hash=<old> but runs a different template from its
ControllerRevision: container ome-container image: revision 'img:1' vs pod
'img:2'; annotation perm.test/rev: revision 'v1' vs pod 'v2'`. Limits: a
difference confined to fields outside the fingerprint (probes, volumes,
affinity) is invisible; so is a hash whose every pod and whose revision lie
together; and an admission webhook that rewrites images would make every pod
disagree with its revision. The settle window (`INV_SETTLE_S`, scaled by
`OME_TEST_TIMEOUT_MULT`) absorbs an in-place update's separate image, label
and annotation writes.

**G ready-serving** — an Instance the InferenceReplica reports `Ready`,
whose full pod set the watcher also sees container-Ready, has at least one
pod in rotation: the `ome.io/serving` readiness gate `True` and the pod a
Ready endpoint of a per-revision routed Service whose selector matches it.
The reading is the IR row (phase) plus the watcher's pods for that index
and the same Service and EndpointSlice lists invariant B reads; the endpoint
half is asserted only once the case has a routed Service at all (otherwise
only the gate is), and an Instance whose pod set is incomplete or not all
Ready is left to A. A violation reads `invariant G violated
(ready-not-serving): engine/2 Phase=Ready with all 1 pod(s) Ready, but no
pod has the ome.io/serving gate True` or `… pod(s) <name> serving but not a
Ready endpoint of any routed Service that selects them`. Limits: it says
nothing about an Instance the IR does not call Ready (a Ready pod under a
`Creating` or `Updating` row is a transition, not a lie), nothing about
traffic weights (a 0% canary step keeps its pods in rotation and passes),
and it reads the gate and the endpoints, not the request path. The settle
window absorbs the controller's serving flip and endpoint propagation after
a pod turns Ready.

**H superseded-revision** — once a Component's rollout reports done, no
Instance of it runs any other revision. "Done" is read two ways: a Component
the ISVC run model pins (any grouped Component — canary, blueGreen,
rollingUpdate) is done when `status.rollout.activeRun` is absent,
`lastRun.outcome` is `Completed`, its `rolloutPhase` (where a canary reports
one) is `Stable`, and the IR's `updateRevision` is the run's pinned target
for it; a Component outside the run model is done when its IR's
`currentRevision` has reached `updateRevision`. From then on every IR row's
`runningRevision` and every live pod's `ome.io/revision-hash` must be that
revision. A violation reads `invariant H violated (superseded-instance):
engine/2 runs revision <old> after the rollout reported done on <new> (IR
runningRevision=…, updateRevision=…)` or `(superseded-pod): engine/2 pod
<name> carries ome.io/revision-hash=<old> after the rollout reported done on
<new>`. It is not evaluated under `mode=partition`, whose own verdict owns
the staged split (k Instances rest on the old revision by design). Limits: a
rollout that never reports done (a `Failed` park, an open run, a
`RolledBack` hold) is outside its claim, as is the straggler the run close
itself waits for; a revision the IR calls `updateRevision` while the ISVC
spec already moved on is caught only once the next run opens and closes.
The settle window absorbs the run close and the next render.

## Regenerating cases

Four generators write into `isvcs/`; re-run one after editing it:
- `gen_cases.py` — the feature matrix (migration/restart/partition/scale/edge/canary),
  permuting topology × lifecycle × coordination.
- `gen_mn.py` — the multi-node (gang) rollout cases (`p66`–`p99`, `p500`+) →
  `isvcs/rollout/` (`p500`+ is the continuation block: `p100`+ belongs to
  `gen_cases.py`'s feature counters).
- `gen_runtime.py` — the runtime-triggered cases (`p400`+) → `isvcs/runtime-rev/`.
- `generate_cases.py` — the hot-core permutation corpus (`p1000`–`p2799`,
  `p3000`–`p4999`, `p5000`–`p5799`, `p5800`–`p5999` `p6000`–`p6199`, `p6200`–`p6399` and `p6800`–`p6999`) → `isvcs/gen-rollout/`, `isvcs/gen-fault/`,
  `isvcs/gen-restart/`, `isvcs/gen-crashloop/`, `isvcs/gen-switch/`,
  `isvcs/gen-crashloop-idle/`, `isvcs/gen-crashloop-sibling/`,
  `isvcs/gen-heal/`, `isvcs/gen-imagepull/`, `isvcs/gen-node/`,
  `isvcs/gen-ctrlrestart/`, `isvcs/gen-rollback/`, `isvcs/gen-crashloop-new/`, `isvcs/gen-crashloop-group/`, `isvcs/gen-pause-repair/`, `isvcs/gen-ops-held/`, `isvcs/gen-runtime-edit-repair/`, `isvcs/gen-migrate-bump/`, and the operator stories driven through a canary (`p5200`–`p5399`) →
  `isvcs/gen-canary-crash/` (a canary rolls the fix while the running
  revision breaks under it), `isvcs/gen-canary-second-rev/` (a second revision
  lands while the first canary is staging or serving) and
  `isvcs/gen-canary-podkill/` (a pod deleted by hand while the roll is paused
  or held), the step kinds crossed with the persistent faults (`p6400`–`p6799`) →
  `isvcs/gen-canary-fault-old/` (the stable side breaks before the push, at
  the capacity step, during a soak, at a manual gate, at an analysis gate and
  at the last gate before the cutover; the ladder still lands the fix on every
  broken Instance) and `isvcs/gen-canary-fault-new/` (the push brings a
  revision that crashes at start, crashes after Ready inside or past the first
  soak, or cannot be pulled; the ladder parks where it bites and a rollback or
  a fix push lands), the small fleets and the engine gang behind a router
  (`p8000`–`p8199`) → `isvcs/gen-size/` and `isvcs/gen-router-gang/`, plus
  `isvcs/MANIFEST.md` (per-dimension counts, the
  constraint ledger with every omitted cell and its reason, missing-hook
  catalog). The story families are canary lanes in CI: the pipeline gives
  every `gen-canary-*` folder the canary pod budget and Prometheus.

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
