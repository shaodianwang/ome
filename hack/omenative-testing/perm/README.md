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
> **Manager placement.** `OME_EVAL_MANAGER_ON_CONTROL_PLANE=1` makes
> `deploy-ome-kind.sh` pin the controller to the control-plane node: the node
> selector is the role label `OME_EVAL_CONTROL_PLANE_NODE_LABEL` (default
> `node-role.kubernetes.io/control-plane`) and its taint is tolerated. A
> node-loss case stops the kubelet only of a worker that hosts no controller
> pod, so with the controller off the workers every worker under the case is a
> candidate; the CI eval lanes set it. Off by default: a cluster of one node
> runs everything on its control plane anyway.
>
> **Canary-analysis cases (`analysis=…`) additionally need:** the bundled
> `ome-prometheus` enabled in the cluster (chart `prometheus.enabled`, now the
> default) scraping `$OME_TEST_NS`, and the `canary-metrics` image (built from
> `dockerfiles/canary-metrics.Dockerfile` by the pipeline) pullable by the cluster.
> The metric-fault cases (`fault=metric`) also reach a pod's metrics server
> and the bundled Prometheus through the apiserver proxy
> (`kubectl get --raw /api/v1/namespaces/<ns>/pods/<pod>:8080/proxy/...` and
> `.../services/<prometheus>:<port>/proxy/api/v1/query`), so the kubeconfig
> needs `pods/proxy` and `services/proxy` on top of the usual verbs; the
> source-fault cases (`fault=source`) read the store's EndpointSlices in the
> controller's namespace and create a Service and an EndpointSlice in the
> case namespace.

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
│   ├── csr-metrics-multinode.yaml — the same as a gang (leader + N workers, every pod emitting the gate metric)
│   ├── cbm.yaml            — ClusterBaseModel (noop format, PVC-backed)
│   └── pvc-and-seed.yaml   — 1Mi PVC + seed Job (writes config.json so the CBM validates)
├── isvcs/                 — generated case YAMLs (committed; `topo=` in each header)
│   ├── rollout/           — 96 rollout cases (topology × lifecycle × coordination)
│   ├── migration/ restart/ partition/ scale/ edge/  — feature cases (see ../DESIGN.md)
│   ├── gen-rollout/ gen-fault/ gen-restart/ gen-crashloop/ gen-switch/ gen-crashloop-idle/ gen-crashloop-sibling/
│   │   gen-heal/ gen-imagepull/ gen-node/ gen-ctrlrestart/ gen-rollback/ gen-crashloop-new/ gen-crashloop-group/ gen-pause-repair/ gen-ops-held/ gen-runtime-edit-repair/ gen-migrate-bump/  — generated hot core (see isvcs/MANIFEST.md)
│   ├── gen-canary-crash/ gen-canary-second-rev/ gen-canary-podkill/ gen-canary-fault-old/ gen-canary-fault-new/  — generated operator stories driven through a canary (the podkill and fault families also carry a canary beside the remaining faults on the six shape families)
│   ├── gen-canary-fault-gate/  — generated: what a step's gates read breaks (a metric breach on either side, an unusable analysis source, a broken sibling)
│   ├── gen-size/ gen-router-gang/  — generated small fleets (one and two Instances per Component) and the engine gang behind a router
│   ├── gen-readiness/ gen-gang-member/ gen-tight/ gen-evict/ gen-preempt/ gen-pending/  — generated fault families (see isvcs/MANIFEST.md)
│   ├── gen-packed-pool/   — generated: every node packed, a bad push then the fix, and the running revision broken then the fix
│   ├── gen-budget/ gen-inplaceonly/  — percent budgets as the engine resolves them; the strict in-place strategy on its own terms
│   ├── gen-fault-scale/ gen-fault-delete/  — generated: a scale or a delete on a service with a broken Instance
│   ├── gen-fault-move/ gen-fault-edit/  — generated faults crossed with a move and with a runtime edit
│   ├── gen-repeat/            — generated: faults that keep repeating until they are lifted (`repeat=<n>`)
│   ├── gen-webhook/ gen-conflict/  — generated window faults: the admission webhook unreachable, a competing writer
│   ├── gen-crash-once/  — generated: a crash that happens exactly once (once=1), at start or after Ready, on either revision, the engine's or a sibling's
│   ├── gen-lift/        — generated: a cause kept to the end of the case or taken away after a while (the config key, readiness and image pull, on either revision)
│   ├── gen-stale-record/ gen-canary-stale-record/  — generated: a stale revision record (the engine's rolled-out record written back to the revision before the one it runs) on the settled service and under every operation, and under a canary's gate on the shapes the hand-written cases leave
│   └── gen-pairing/     — generated: the pairing-protocol transition matrix of a prefill/decode service (shape x strategy pair x slow side x budget x sequence x grouping), the pairable floor hard on both sides
└── results/
    └── <RUN_TS>/<feature>/<topo>/
        ├── pNN-<ts>.tsv   — one verdict row per case
        └── pNN.log        — full per-case log
    └── <RUN_TS>/summary.txt — the batch summary table for that run
    └── <RUN_TS>/verdicts.json — the same verdicts for machines: one object per case
                                 (verdict, reason, seconds, fault, log) and the lane
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
workers), so multi-node cases catch their peaks too. Both fixtures pin
`terminationGracePeriodSeconds` on every component config (the gang's engine
config is folded into its leader and worker pods; its router config carries
its own), for the reason the generator states on the cases that pin it per
component: the eval judges a case well inside the cluster's default pod
grace, and a pod that ignores its stop signal must not outlive the verdict.

Readiness is an `exec` probe with two switches, each enough to fail it while
the startup and liveness probes (plain `httpGet /`) keep passing, so a pod can
run, leave rotation and never be restarted: the `/tmp/perm-unready` file (a
serving pod that stops passing readiness, `fault=readiness:old`) and the
`TEST_UNREADY` runner env (a revision whose pods never pass it). With neither
set the probe is an HTTP check of nginx on its port. `run.py` names both
switches (`READINESS_SWITCH_FILE`, `READINESS_SWITCH_ENV`); the metrics
runtime keeps its HTTP readiness probe, its image has no shell.

`TEST_EXIT_AFTER` is the runtime's one fault switch. Unset (every fixture and
every case leaves it unset), the command execs nginx exactly as above. Set to
a number of seconds, the shell stays PID 1, serves through nginx for that long
after it is up, then exits 1 — every time the kubelet restarts the container —
so the pod passes readiness and crashes after it. Only `fault=crashloop:new
form=load` sets it, on the pushed revision's runner env; the trap still answers
a stop signal during the window, so such a pod drains fast too.

Readiness is an `exec` probe with two switches, each enough to fail it while
the startup and liveness probes (plain `httpGet /`) keep passing, so a pod can
run, leave rotation and never be restarted: the `TEST_UNREADY` runner env (a
bumped revision whose pods never pass readiness, `fault=readiness:new`) and the
`/tmp/perm-unready` file (a serving pod that stops passing it,
`fault=readiness:old`). With neither set the probe is an HTTP check of nginx on
its port. `run.py` names both switches (`READINESS_SWITCH_ENV`,
`READINESS_SWITCH_FILE`); the metrics runtime keeps its HTTP readiness probe,
its image has no shell.

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
`inv_xfail=A|B|C|D|F|G|H|I|all` (requires the line-2 `# reason:` too): exactly that
invariant's violations record as the case's `XFAIL (known bug: #N)` instead of
hardening the verdict — the invariant is still evaluated and printed (nothing
is blinded, unlike `inv_off`), and any other invariant still fails hard, so
unrelated regressions cannot hide behind the parking. `inv_on=J,K` selects
the two opt-in invariants (J, no auto-recovery record in the audit ledger;
K, no node exclusion on a pod), which run on every case of a family whose
story forbids them and nowhere else (see *Continuous invariants*).
`inv_hard=L` (or `L,M`) turns an advisory reading hard for the case: a
sustained dip of the pairable floor (L) or a traffic target with no pod
behind it (M) then fails it instead of being reported, which is what the
generated pairing family asks for (see *Pairing protocol*).

## How a case runs (`run.py`)

For each case:

1. `kubectl apply` the runtime (idempotent) and the ISVC.
2. Wait until the expected pods are all Ready (bounded).
3. Start a pod watcher streaming events with timestamps.
4. Trigger a rotation by patching `engine.annotations[perm.test/rev]: v2`
   (every coordination-group member when the case declares a group;
   `bump=<members>` names the members to bump instead, so an engine-only
   change can land under a group whose other members stay as negative
   controls). A rollout edit rides the same patch as a runner override on
   every bumped member: `edit=image` (with `img=`) changes the runner image,
   the one diff an in-place strategy patches onto the live pod;
   `edit=env|resources|probe` is a pod-template diff beyond container
   images (the runner's `TEST_REV` env, its CPU request, its readiness
   period), so `InPlaceIfPossible` on a single-pod Component resolves it to
   a recreate (UID rotation is expected and the drain-first checks judge the
   roll), a gang is rebuilt whole under either in-place variant, and
   `InPlaceOnly` on a single-pod Component refuses it: such a case states
   `expect=held` (see *A push the strategy cannot apply*).
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
- `results/<RUN_TS>/verdicts.json` — always written beside `summary.txt`: one
  object per case (`verdict`, a one-line `reason`, `exit`, `seconds`, the
  header's `fault`, the `log` and `result` paths under the run directory) and
  the lane the run was (`build`, `job`, `step`, `lane`, `scheduler`, `commit`,
  null outside CI). The eval lanes upload it as its own artifact and the round
  record (`.buildkite/eval_verdicts.py`) reads it instead of the archive.
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

A canary group carries its body inline or names a same-namespace
`RolloutPolicy` through `policyRef`. The spec reader resolves the referenced
body before anything reads the plan, so both forms drive the same steps and
verdicts. The referenced twins of three inline cases (p1957, p1955 and p1956
mirror p1836, p1825 and p1820) ship the policy as the case file's second
document and name their cells with the `[policyRef]` variant. The generated
canary families carry the same twins for a few of their rows
(`CANARY_POLICY_TWINS` in `generate_cases.py`, two or three per family): the
inline case under its own header, fleet and reason, with the ladder moved
into a `RolloutPolicy` shipped as the second document, every cell id ending
in `[policyRef]` and `policyRef` closing the description; the twins are
minted after the family's other rows, so the inline ids keep their places.
A policy ladder with an analysis gate must name a metric provider bound in
the operator's configuration, which the inline rows do not carry, so the
twins stay on timed and manual ladders; the coverage ledger lists the form
as its own axis, where the analysis gate reads as empty under the referenced
form rather than covered.

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
  stable revision; the step must not move for 20 s), `promote-force` (the
  matching hash on `ome.io/rollout-promote-force`: the step it meets, gated
  or not, stops waiting for its capacity — no clock, no CapacityTimeout park,
  the partition still asks for the staged count and traffic still follows
  the pods that serve — and its gate, if any, opens; an ungated final step
  keeps its drain open under it; the verdict expects the step to move after
  the force and a `CanaryStepForced` event per force, and a forced final
  step to open into its drain, where `release=rebump-fix` lands the fixed
  image), `rollback`, `rebump` (a
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
  canary pod or at an old-revision pod. Under `canary=manual` a podkill aimed
  at the hold the ladder is at holds the harness's promote until the deleted
  pod's Instance is whole again on Ready pods (hold, kill, dip, capacity
  back, promote, cutover); the wait is bounded by `verbhold=` seconds after
  the deletion, after which the promote is issued anyway and the verdict
  notes the capacity was still down. The break faults
  (`crashloop:canary-<state>` with `form=start|load`, `imagepull:canary-<state>`)
  break the stable side in that state: `victims=<n>` (default 1) breaks that
  many old-revision Instances of `victimcomp=` at once, highest index first,
  the storm shape. The verdict reads every victim as the rollout-mode break
  does (each ends on the canary revision serving, no `RestartTriggered` event
  names one after the bump), and the capacity floor is read with the victims
  allowed for: the healthy Instances must hold `N - maxUnavailable - n`.
  `fault=<kind>:idle` lands the same break on the settled service before the
  push: the push waits until the break has visibly bitten (`BREAK_BITE_S`,
  60 s scaled) and the status has reacted to it (`BREAK_REACT_S`, 60 s
  scaled: under `RecreateInstanceOnPodRestart` every victim's repair is open
  — a `RestartTriggered` event names it, or its row reads `Restarting` or
  carries an operation — or declared held by a `RepairHeld`; under `None`,
  where no repair answers a broken pod, the break has been visible for
  `PREARM_SETTLE_S`), so it meets a fleet the product already knows is
  broken, and the verdict reads the victims as above. A repair is placed by
  its event's first record: one the product opened before the push and
  completed after it is not a repair after the push, whether or not its
  event was recorded again. Under `canary=manual` the harness holds its
  promote `verbhold=` seconds (default 45) after a break fired at a hook, so
  the broken stable side is observed while the gate holds, as an operator
  would see it before promoting. Under a canary `victimcomp=router` is
  admitted too: the router in front of an engine ladder is a sibling the
  ladder never rolls, so the repair owns its break (rebuilt, with a
  `RestartTriggered` naming it). **`victim=canary`** aims a break at a Ready
  pod of the canary revision instead (a canary-side sibling that breaks
  after it served; a repair of it is reported, never failed, since it is on
  the roll target). **`lift=after-settle`** on a break makes it heal itself:
  `LIFT_SETTLE_S` after it has bitten the harness takes its own break away
  (a `form=load` killer stops; a pod pointed at a break image is pointed
  back at the image it ran, when a repair has not replaced it first), and
  the verdict requires the Instance to be Ready and serving again on its
  revision, rebuilt or healed in place (both are reported). A gate fault, a
  self-healing break or a canary-side break whose hook the ladder never
  reaches fails the case; the older hook faults keep reporting a missed hook.
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
  step never side-loads, so it fails to pull offline as well as online. A
  member named that is outside the canary unit (`bump=engine,router` with
  the group naming the engine alone) rolls on its own beside the ladder;
  with `imgcomp=router` it is a **parked sibling**: the ladder is judged on
  the unit alone, and the sibling must never serve a pod of its unstartable
  revision, must say why its roll stopped (a `rolloutHold`, a Failed row, or
  a `RetryHeld`/`InstanceFailed` event naming it) and must end whole on its
  running revision; it is left parked, as its line two says.
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
  every Instance the broken revision had not replaced (an Instance the
  broken revision reached that has no pod of either revision at the park
  is mid-repair on it, not a stable Instance that stopped), nothing reached
  `Promoting` before the exit landed, and then the exit's own terminal checks
  (drained back with the rejected revision recorded, or every Instance on the
  fixed revision with nothing left on the broken one). The capacity floor is
  read with the Instances the broken revision reached allowed for. Under an
  analysis gate only the pull failure is armed: the metrics image is a static
  binary with no shell and no exit switch.
- **The remaining faults beside a canary.** The stable side also breaks as
  `fault=readiness:idle|canary-<state>[@step] form=persist|recover` (a
  serving pod of the running revision stops passing readiness; `recover`
  lifts the switch `LIFT_SETTLE_S` after it bit) and
  `fault=configkey:idle|canary-<state>[@step] lift=after-settle` (the key
  the engine runner reads removed and the runner restarted, put back
  `LIFT_SETTLE_S` after the wait showed); both are judged as the crash
  breaks are, the ladder landing the fix on the Instance. The key kept
  away (`fault=configkey:idle lift=never terminal=Failed`) is read by both
  revisions, so the push is never its fix: the verdict asks that the
  stable-side break did not hold the ladder (a pod of the pushed revision
  was placed and waited on the same key), that the ladder parked at that
  step naming the missing key (the run's failed reason or message, or a
  Failed row's recorded failure), that no traffic reached the step whose
  pod never came up, that the stable side kept serving every Instance the
  break and the ladder did not take, and that nothing promoted or rolled
  back; whether the ladder reached the broken Instance before the park is
  reported. `once=1` on `fault=crashloop:canary-<state> form=load` kills
  the runner of one Ready stable pod exactly once at that state, and on
  `fault=crashloop:new form=start|load` the pushed revision's first runner
  (at its first start, or once Ready); the verdict is the crash-once one
  over the ladder's end state. The pushed revision also breaks as
  `fault=configkey:new lift=never` and `fault=readiness:new` (the runner
  reads a key the ConfigMap lacks; the fixture's readiness switch env is
  set), parked and exited like the fault-new breaks, and as the one-shot
  forms `configkey:new lift=after-settle`, `readiness:new form=recover` and
  `imagepull:new lift=after-settle` (the key added, the switch lifted in the
  first pod, the first pod pointed back at its image, each `LIFT_SETTLE_S`
  after the bite; the step waits inside its bound and the same pod passes).
  `fault=podkill:canary-<state>[@step]` and `fault=evict:canary-<state>[@step]`
  take `repeat=<n>` (the deletion or the eviction again each time what it
  took is back; the window stays open until the repetitions are spent and
  the repeating-fault verdict reads it); an eviction at a manual gate holds
  the promote the way a deletion does. `fault=taint:all lift=after-settle|never`
  is armed before the push: the first step's canary pod waits with reason
  `Unschedulable` on its row, no row reads Failed and nothing settled is
  taken down; the lift lets the ladder complete, `never` parks it at the
  capacity step's ready timeout (`terminal=Failed`) with the wait still
  reported.
- **A window at a grid state.** `fault=webhook:canary-<state>[@step]
  lift=after-hold|after-window` and `fault=conflict:canary-<state>[@step]
  form=once|persist target=isvc|ir` open the window faults of the
  "Fault cases" section while the ladder is at that state; the window
  verdict is read beside the ladder's, with the step index never going back
  (see "A window at every hook").
- **A stable identity aged.** `fault=agerecord:idle|canary-<state>[@step]
  victimcomp=<member>` writes, through the status subresource, the member's
  stable identity back to the revision it rolled out before the current one
  (its `previousRolledoutRevision`; a member that never rolled fails the
  apparatus, so pair it with `preroll=`), in every record a rollback reads
  at that moment: the rolled-out record, and under a pinned run the run's
  pinned stable for the member and its unit's canary stable identity. At
  idle the verdict asks that the record is level with the revision the
  member stands on again by the end (the dispatcher levels an idle member's
  record with its replica) beside the idle-unit checks; at a hook, that the
  gate's rollback parked the unit on the empty stable revision
  (`StableRevisionMissing`, with a `CanaryStableRevisionEmpty` event naming
  the member) and that no pod of the member was rebuilt onto the aged
  revision. Beyond a canary the same fault lands on the settled service and
  under every other operation (see *A stale revision record under every
  operation* in the fault cases).
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
- **`preroll=<members>`** — history before the story: once the initial
  fleet has settled and its run has closed, the named members are bumped to
  a revision of their own and their ladders (which must advance on their
  own) are waited for, until every Instance stands whole on the new
  revision, the replica rows are steady and each unit reads the end of its
  ladder; the run closes again and the watcher starts over from that fleet
  (revisions, peaks, floors), so the case's own bump is judged on its own.
  The service then carries a previous rolled-out revision whose
  ControllerRevision is retained and a record naming the revision each
  member stands on, which is what a case about those records needs
  (`fault=agerecord`), and what makes a unit beside the driven one a
  settled unit rather than a fresh one (`unit=`).

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
— MAX picks the worst pod of the Component, stable and canary alike. The
bundled Prometheus stamps each pod's revision hash as `revision_hash`, so a
gate that must read the canary alone adds `revision_hash="{{.CanaryRevision}}"`
(`gen_cases.CANARY_QUERY`; the gate-fault family below runs on it, which is
what keeps a breach on the stable side out of the gate). The step's
`onInconclusive` policy is mirrored by `oninconclusive=hold|rollback|rollbackonstall`.
Analysis-validation reject cases live in `isvcs/reject/`
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

### What a step's gates read breaks (`fault=metric`, `fault=source`)

`isvcs/gen-canary-fault-gate/` (`p9050`+, `generate_cases.py`) breaks the
inputs of a step's gates while the ladder walks, on the ledger's six shape
families as the engine unit's ladder (the router, where present, is outside
the group), at three positions: before the push (`:idle`), the second step's
gate (`canary-serving@1`) and the last gate before the cutover
(`canary-serving@2`); gangs run the mid-ladder position alone. Every case
states the operator's end state on line two.

- **`fault=metric:new|idle|canary-<state>[@step]` with `form=persist|recover`
  and, at a hook, `victim=stable|canary`** — the health metric breaches the
  gate's threshold on one side. At a hook or at idle the harness sets the
  metrics image's live override (`GET /-/perm/error-rate?value=0.9[&for=45]`
  through the apiserver pod proxy) on every live pod of that side of
  `victimcomp=` (default engine): `persist` keeps it until the pod goes,
  `recover` lets it expire after `METRIC_BREACH_S` (45 s scaled), and the
  verdict reads the pods' own answer to confirm the lift while they are
  still there. `metric:new` is the push itself: the bump carries the breach
  (`health=breach`), decaying `METRIC_DECAY_S` (60 s) after the pod serves
  under `recover`. The breach must reach the gate's source: on the stable
  side Prometheus must show it on that revision (asked through the service
  proxy with the case's own query; never seen is `INFRA-FAIL`), on the canary
  side the gate must count a failing sample. Then the rows: on the **stable
  side** the gate reads the canary revision alone, so it must never count the
  breach (`analysisFailedChecks` stays where it was), never roll the healthy
  canary back, and the ladder reaches `Stable` (`nophase=RollingBack`); on the
  **canary side** nothing may have failed before the breach landed, and the
  case ends as its gate policy says: `analysis=breach health=pass` rolls back
  at the first failing sample (analysis-initiated, the rejected revision
  recorded), `analysis=pass health=pass minfails=1` with a `failurelimit`
  above the samples a 45 s breach can produce rides it out and promotes.
- **`fault=source:idle|canary-<state>[@step]` with `form=unreachable|garbage`
  and `lift=after-settle|never`** — the analysis source becomes unusable.
  A run pins its analysis source when it opens, so the fault never edits
  the address (an edit is inert on an open run and, made at idle, is pinned
  into the next one): before the push the case gets its own front for the
  bundled store, `perm-<case>-source` in the case namespace (a Service with
  no selector and the EndpointSlice that aims it at the store's pod, read
  off the Service the controller's `canaryAnalysis` block names), and the
  driven group's `canary.prometheus.serverAddress` is pointed at the front,
  which the run then pins. The fault breaks what is behind the front:
  `unreachable` aims it at nothing (every query is refused), `garbage` aims
  it at a running-revision pod of the unit, whose server answers no metrics
  API (every query is answered with something that is not a sample). The
  gate must report it in `metricResults` (an unevaluated sample with a
  message), hold the ladder at that step and count no breach.
  `lift=after-settle` puts the store back behind the front `LIFT_SETTLE_S`
  after the hold was observed (or once the gate has had `SOURCE_BITE_S` to
  report and did not, so the case still ends) and the ladder must resume to
  `Stable`; `lift=never` keeps it broken, and the step's `onInconclusive`
  decides the end: `Hold` parks `Failed` at that step with reason
  `AnalysisStalled` past the stall timeout (the group's `readyTimeout`,
  150 s in the family, with the stable revision still serving),
  `RollbackOnStall` rolls back instead (analysis-initiated). The end-of-case
  cleanup puts the store back and removes the front.
- **The sibling rows** reuse the break grammar on the router of an engine
  ladder and the decoder of a prefill/decode unit (admission makes engine and
  decoder one unit, so the ladder owns the decoder's running-side break and
  lands the fix on it; the router is never rolled, so the repair owns its
  break): for good (`form=start` and a pull failure under `None`, `form=load`
  under `RecreateInstanceOnPodRestart`), once (`lift=after-settle`: one kill,
  or the image put back), on the new side as the fault-new breaks of the
  decoder under both exits, as the router pushed beside the ladder onto an
  unpullable image (the parked sibling above: an Instance whose
  running-revision pod still serves is kept on that revision, whatever
  failed attempt sits beside it), and as one kill of a pod of the
  sibling's new revision (`victim=canary`). On every sibling row the
  Instance the break took is the fault's cost, on the running side and on
  the new side alike: the floor the sibling's other Instances must hold is
  read with it allowed for. The sibling rows run the plain fixture runtime
  and a timed ladder: a break needs a shell to kill or an image to swap, and
  the analysis gate changes nothing about a sibling.

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

`isvcs/pairing/` (p800+, hand-written) and the generated `isvcs/gen-pairing/`
(p13000+, the transition matrix below) cover
`spec.rollout.pairingProtocol`: the trigger is a **patch of the protocol
field** (not an annotation bump) — the token rides every engine+decoder
revision, so the patch alone re-mints and rolls both pairing Components
(never the router; isolation is asserted). Header knobs:

- `proto=<token|clear>` — the transition target; the starting value is
  whatever the case YAML declares (`""→X` adoption, `X→""` clear, `X→Y`
  coordinated swap). `proto2=` re-patches once ≥1 new-cohort pod is READY —
  a re-target over a partially-landed intermediate (converge on the FINAL
  cohort, no Instance stranded on the intermediate — the X-2 shape). A
  `proto2=` equal to the protocol the case starts on is a rollback
  mid-transition: the pods already on the new protocol are replaced, the
  pods that never moved stay, and the case converges when every pod
  carries the starting protocol and none runs a revision of the abandoned
  cohort (the starting revision may come back under its old hash, so the
  label tells the cohorts apart where the hash cannot).
- `hold=1` — assert a `status…rolloutHold` with `gate=Pairing` is observed
  mid-transition. Only shapes where the denial spans a startup cycle can
  assert this: the acting Component must keep ≥2 serving instances through
  the denied window (that blocks all three gate escapes — no-pair, the 1×1
  wall, and last-hop), e.g. an in-place engine racing ahead of a surging
  decoder. Drain-first shapes shed to ≤1 serving and get admitted by the
  escapes in under a sampling tick — holds there are reported ⓘ only.
- `hold_max=<s>` — every Pairing hold is a bounded wait: the harness samples
  `rolloutHold` every tick, keeps the span of each `gate=Pairing` hold per
  bumped Component, and fails the case when the longest span exceeds the
  bound or a hold is still reported once the transition converged (two
  more samples absorb the status writer's lag). Without it the longest
  hold is reported.
- `inv_hard=L` — the pairable floor (invariant L) is hard for the case: on
  every tick each side keeps desired minus maxUnavailable pairable
  serving pods, a pod being pairable when a serving pod of the other
  Component runs its protocol or either protocol is empty; a dip that
  lasts the floor debounce fails the case.
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
TEST_STARTUP_DELAY`) and the symmetric `p810`/`p830` assert it. The hold also
keeps a replacement that cannot pair yet from retiring its source while other
old pairs still serve: `p12106`/`p12107` (6 engines + 2 decoders rolled 3+1 at
a time, one side slow) carry `hold=1` because without it the first old
decoder, or three old engines, would leave rotation for nothing. Waive with
`inv_off=C` + a line-2 `# reason:` comment.

#### The transition matrix (`isvcs/gen-pairing/`)

`generate_cases.py` mints one case per cell of the matrix `coverage.py`
holds (its *Pairing transitions* table): the shape (1x1, 2x2, 6 engines + 2
decoders, 2 engines + 6 decoders) x the strategy pair (surge+surge,
surge+recreate, recreate+surge, recreate+recreate, in-place engine +
surging decoder) x the slow side (none, engine, decoder, both; a slow
side's pods start 40 s late through `runner.env TEST_STARTUP_DELAY`) x the
maxUnavailable of the sides that surge (0 or 1; a drain-first or in-place
side always runs under 1, a six-pod surging side moves three at a time) x
the sequence (v1 to v2; v1 to v2 to v3 once the first v2 pod is Ready; v1
to v2 then back to v1 once the first v2 pod is Ready; v1 to empty; empty to
v1) x the grouping (one blueGreen group over engine and decoder, or none).
Cells the product rejects or that have no meaning are removed with a
written reason (an ungrouped change between two protocols, which admission
denies; a budget of 0 with both sides drain-first; a slow in-place side; a
slow side under a change to or from the empty protocol, which nothing
gates), and the cells the hand-written pairing cases already drive are not
minted again (`HAND_PINNED_PAIRING`). Every generated case carries
`hold_max=240` and, wherever an order of steps can keep it, `inv_hard=L`;
the one shape where no order can is a one-pod drain-first side whose peer
keeps a floor above zero (the peer's pods cannot pair while that pod is
away), and there the floor is reported and the reason says so. `hold=1`
is set where a Pairing hold is certain to be seen for longer than a
sampling tick: a surging side whose replacement is Ready while the other
side's pods are still starting, the in-place engine's last relabel waiting
on a surging decoder, and a two-pod drain-first side's last recreate
waiting on a slow peer. Line 2 of each case states the expected behaviour
in one sentence: which side moves first, what holds and why, the floor
each side keeps, the bound on every hold and the end state. The family is
on the eval soft list while its first main rounds classify the cells where
today's gates keep a weaker floor than the matrix asks for (a side that
retires its last old pod while the other side's old pods still wait for
slow replacements leaves those pods unpairable).

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
`results/<RUN_TS>/<feature>/<topo>/` with a `summary.txt` and a `verdicts.json`.

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

Its continuation (`p11100`+, once `p3800`-`p4199` is full) puts a break
beside the edit on the six shape families, each break kind kept and
one-shot on either side of the push. On the running revision
`fault=crashloop:idle form=start|load`, `fault=imagepull:idle`,
`fault=configkey:missing lift=never|after-settle` or `fault=readiness:old
form=persist|recover` (`victimcomp=decoder|router` for a sibling nobody
bumped) lands on the settled service before the push, the status reacts
to it, the push follows and the edit fires at `during=settling|rotating`;
the one-shot forms (`lift=after-settle`, readiness `form=recover`) take
the cause away once the edit has fired. The `:idle` parameter is what
tells this break from the `:old` crash the family already pairs with an
edit, which is armed as the bump lands. The roll must complete under the
edit with every bumped Instance on the new revision serving, the rows
settled and nothing held; the broken Instance ends on the new revision
serving with no repair opened for it after the push (the roll owns an
Instance that is off the target), a sibling ends Ready and serving on its
running revision, rebuilt by a repair or, once healed, in place; the
capacity floor is lowered by the broken Instance, not waived. A healed
sibling's crash costs its ladder one attempt, and the block it wrote, with
the hold that names it, lasts the proven window after the healed set is
Ready again: the deployed `lifecycle.stuckPodGracePeriod`, or the
sibling's `minReadySeconds` if larger, counted from the pod's Ready
transition. The runner reads both off the pod and the controller's
ConfigMap and waits that window out (then `HEALED_WINDOW_SETTLE_S` for
the status to drop the hold) before it reads the status for a leftover
hold; a hold that stands past the window fails the case. A key kept
away (`configkey:missing lift=never`) is the one cause no revision starts
without: the push parks on its first pod with the reason (a Failed row at
the Instance ready timeout, an InstanceFailed event), nothing of the new
revision may serve, the hold is kept to the end and the edit is read on
the live spec meanwhile. Each row meets the edit at a moment of its own
(`SWITCH_FAULT_MOMENTS`): the surge and recreate flips at both hooks, an
in-place start flipped to the surge at its first patched pod so the broken
Instance, reached last, is surged rather than patched while it fails, and
the policy flip, two of them under a `rollingUpdate` group (a single-member
one on the engine-only shapes).

On the pushed revision `break=crash-start|crash-load|imagepull|configkey|stuck`
(the readiness probe the pushed revision never passes) rides the push with
the edit at `during=held` (once the roll is parked on the broken pod with
the status saying why, waited for up to `HOLD_REASON_WAIT_S` after the
break shows) or `during=rotating` (the first pushed pod); `victimcomp=decoder`
puts the break on the grouped decoder of a prefill/decode service, and
`bump=router victimcomp=router` on the router of a router-only push on the
engine+router shapes, where the engine is never rolled and the edit lands on
the router's own strategy. The Components the push does not bump must keep
their pods. After the edit the fix push, the runner override dropped (the victim runner put
back onto its present key for `configkey`), must land under the edited
spec on every bumped Instance, nothing of the broken revision may be
created again or left, no hold may be left, no repair may name a bumped
Instance after the fix (reported, not judged, where the edited policy is
`RecreateInstanceOnPodRestart`, which rebuilds a restarting pod), and
nothing of a revision that cannot start may ever have served. `once=1` is
each kind's one-shot form: the first runner killed once (the crash kinds),
the unpullable image or the missing key taken away once the wait has
settled, the readiness switch lifted once the drop has settled; the edit
at `held` fires as the break shows, no fix push follows, and the roll must
complete on its own with no hold behind it, the Instance Ready and serving
on the new revision and a rebuild reported where the restart policy
promises one, never required, never twice. The kept rows meet the surge
and recreate flips at both hooks and the policy flip at `held`
(`SWITCH_HELD_MOMENTS`); the one-shot rows meet the two strategy flips,
where a restart is handled under one policy. `lift=` does not combine
with `break=`; `rollback=1` and an edit never share a case.

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
a removal. Its continuation (`p10800`+) puts a break beside the rollback on
the six shape families: each running-revision break, kept and one-shot, at
each hook under a (strategy, group) pair of its own, the sibling forms at
the first hook, and the held-moment breaks under every strategy, every
kind once, and on the grouped decoder. `rollback=1` in canary mode keeps
its own meaning (the `ome.io/rollout-rollback` probe).

A break rides beside the rollback in two places. On the running revision,
`fault=crashloop:old form=start|load`, `fault=imagepull:old`,
`fault=configkey:missing lift=never|after-settle` or `fault=readiness:old
form=persist|recover` (`victimcomp=decoder|router` for a sibling) lands on
the settled service before the push, the status reacts to it, the push
follows and the starting revision is re-applied at
`during=settling|rotating|draining|last`; the one-shot forms
(`lift=after-settle`, readiness `form=recover`) take the cause away once
the rollback has fired. The key kept away reaches `settling` and
`rotating` only: the roll takes the broken Instance first and its
replacement cannot start without the key, so no pod of the starting
revision ever drains and neither the draining nor the last hook arrives
(the generated family takes those two moments at `rotating` under their
own strategy and group). The victim must end on the starting revision too:
Ready and serving (rebuilt by a repair or the roll back, or healed in
place; the repairs that named it are reported, not judged), unless its
cause outlives a rebuild, when it ends on the starting revision without
serving, its Component one short, and its row reporting the loss (a phase
other than Ready, or Ready with fewer serving pods than pods). The key kept
away outlives every rebuild. The readiness switch kept lives in the pod it
was set in, so it outlives the rollback only while that pod stands (a
rollback that fires before the roll reaches the dark Instance, or a
single-pod in-place roll, which patches the pod where it is), and the
victim is then held to that end state. The out-of-rotation Instance is the
roll's first step, so the roll may replace the victim's pod before the hook
(the first source drained under the surge strategy, the first gang
recreated under the gang in-place roll); the rollback then rebuilds the
Instance with a fresh pod that carries no switch, and the victim must end
Ready and serving on the starting revision like any other, the verdict
naming the rebuild and the pod the switch went with. The kept reading is
taken from the pods at each read, so the watch waits for the rebuild to
serve before it reads the rows. On the pushed revision,
`during=held break=crash-start|crash-load|imagepull|configkey` (beside
`break=stuck`) arms the bumped runner (`victimcomp=decoder` under a group
on a prefill/decode service; `bump=router victimcomp=router` for a
router-only push on the engine+router shapes, where the engine is never
rolled and the router alone comes back) so the roll parks on its first pod,
the Components the push does not bump keeping their pods: the
kubelet shows the break, the status says why (a rolloutHold gate from the
retry ladder or the budget, a Failed row, a RetryHeld or InstanceFailed
event; waited for up to `HOLD_REASON_WAIT_S` after the break shows), and
the starting revision is re-applied. `once=1` is each kind's one-shot
form: on a crash kind it kills the pushed revision's first runner exactly
once and rolls back as soon as the restart shows; on `imagepull`,
`configkey` and `stuck` it breaks the pushed revision's first pod once
(pointed at the unpullable image while its runner starts, reading a key
the ConfigMap lacks, the readiness switch set before it is ever Ready),
rolls back as soon as the kubelet shows the break (the readiness switch
once the pod has run `READINESS_NEW_BITE_S` without passing readiness),
and takes the cause away once the rollback has fired (the image put back,
the key put into the ConfigMap, the switch lifted; a pod the rollback has
already removed took the switch with it). The status need not have said
why under a one-shot cause. The verdict reads the held reason first, then
the rollback's end state, and adds that nothing of a revision that cannot
start ever served (asked of a kept break), the one-shot cause was taken
away, no pod of the withdrawn revision was created and no repair named a
bumped Instance once the rollback fired (a pod of it created within
`HELD_BREAK_REBUILD_RACE_S` of the fire, as the rebuild under way at the
fire creates one, whose Instance the roll then replaces, is reported and
not counted: that rebuild finishes on its revision and the roll takes the
row). `once=1` and `lift=` do not
combine with a rollback otherwise; `expect=` is the idle break's
vocabulary and does not apply.

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

`stuck=held|failed` with `img=<unpullable image>` or `break=<kind>`
(`mode=scaledown` and `mode=recreate`) lands the mode's operation on a
Component that is stuck on a broken revision. After the initial settle the
runner pushes every bumped member (`bump=`, else the coordination group's
members, else the engine) onto the broken revision: `img=` names the
harness's unpullable reference, `example.com/canary-test/missing:none`
(`runtime_fixtures.UNPULLABLE_IMAGE`);
`break=crash-start|crash-load|configkey|stuck` puts one of the held
moment's kinds on the runner of the member `victimcomp=` names (default:
the one bumped member, else the engine) — the shell-less image, the
fixture runtime's exit switch, a ConfigMap key the ConfigMap lacks (the
case gives that runner a `configMapKeyRef` env and carries the ConfigMap
as a second document) or the readiness switch — and the pushed member's
`instanceReadyTimeout` bounds each attempt so a revision that never comes
up parks its row `Failed` within the wait. The runner then waits, reading
the InferenceReplica: `stuck=failed` is the first
failed attempt (an Instance row the controller holds `Failed`: the word
`Failed`, or `Ready` with a failed replacement attempt kept on the row,
which the status publishes while the pods of the old revision still serve;
or the retry block for the pushed revision records a started attempt),
`stuck=held` is the retry ladder spent (a retry block in state `Held`, or
the hold the status derives from it). The ladder's length is the cluster's lifecycle configuration
(every attempt waits out the stuck-pod grace, then its backoff), so the
wait for `held` has its own budget (`STUCK_HELD_WAIT_S`, scaled). A push
that never reaches the named state within budget ends the case as `FAIL`
with the operation not applied.

Then the operation, judged on what the engine promises:

- **a scale-down** (`to_e=` below N; `to_d=` too when the decoder is a
  bumped member) removes the Instances that serve nothing first, then a
  stuck one that still serves through its pods, then the highest index, so
  a stuck Instance goes before a healthy one. A row's phase is read as the
  controller holds it: the status publishes `Ready` over an Instance whose
  replacement attempt failed while its old pods still serve, and that
  Instance still goes first. The verdict snapshots the
  rows at the moment the scale lands and asserts exactly those Instances
  went and the rest stayed; that no survivor lost its running-revision pods
  to be rebuilt on the same revision; that under `held` no survivor was
  taken onto the pushed revision afterwards (the hold freezes the roll;
  under `failed` the ladder's next attempt may take one, reported `ⓘ`);
  and that the healthy survivors kept serving throughout (a floor of the
  Instances serving the running revision at the scale, read by their pods
  and judged by the same reading — the Instances the push took are not
  capacity, Ready between two crashes or not — less the unavailability
  budget under `failed` on a drain-first strategy, debounced like the
  rollout floor).
- **a scale to one** (`to_e=1`, with `to_d=1` when the decoder is a bumped
  member) is the deepest scale the API admits: `minReplicas: 0` needs KEDA
  autoscaling and is refused otherwise, so no case asks for zero (the
  header check rejects a `to_*` of zero). Every Instance but one goes, in
  the same victim order, which takes the stuck Instances and leaves a
  healthy one; the verdict converges on the replica count the controller
  accepted (read from the InferenceReplica, never assumed). On the gang
  fleet of two, to one is down by one, so that cell is the scale-down row.
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

A scale that touches no bumped member — `bump=router victimcomp=router` in
front of an engine fleet, `bump=decoder victimcomp=decoder` on a
prefill/decode service, the engine scaled — is the sibling form: the
engine's scale is read as a plain one (a scale-down takes the highest
index, a scale-up's new Instance comes up on the running revision, the
only one the engine has, and the survivors keep their pods), the pushed
sibling's pod set stays as it was under `held` (the hold freezes its roll;
under `failed` the ladder may take another attempt, reported `ⓘ`), and
the sibling's push stays reported. A delete under either form takes
everything.

On every operation the Components the push never touched (a PD decoder and
router with no group, the router with one) must stay as they were, and the
status must keep reporting the push that has not landed: the update
revision still differs from the current one and a hold, a retry block or a
Failed row says why; under `held` the hold itself must still be reported
(only a new revision or a release lifts it). The generated
`isvcs/gen-ops-held/` family crosses the two stuck kinds with the four
operations, the surge and the drain-first strategies, the engine, PD and
gang shapes, and a rolling group on PD; its break rows add one case per
ledger cell for the held moment's other kinds under a scale-down, a
scale-up and a delete on the six shape families — each kind read at one
stuck state under one strategy (the crash at start held under the surge
strategy, the crash after Ready held under the drain-first one — on the
gang shapes under a readiness soak of `OPS_HELD_CRASH_LOAD_SOAK_S`, twice
the crash window, so the first pushed Instance crashes before the roll
takes the second and the hold keeps a running-revision survivor — the
missing key failed under the drain-first one, the readiness switch failed
under the surge one), so the rows meet both states under both strategies
— and the sibling form with the crash at start on the router or the
decoder, held, under the surge strategy.

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
  each rebuilding it) and no repair is opened for it from then on (a repair
  is placed by its event's first record, so the one the edit landed on does
  not count however late its event is recorded again).

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
ledger row for the request, no other move's row, one controller record;
the force-delete sweep's audit rows, written under the UID of a pod it
deleted on a dead node, are records and are not counted as moves);
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

### A push the strategy cannot apply (`edit=env|resources|probe expect=held`)

`InPlaceOnly` applies a diff confined to container images and nothing else;
on a single-pod Component any other template change is refused, and a gang
is rebuilt whole under either in-place variant, so it is never refused
there. A rollout case whose `edit=` is a template change states
`expect=held` when the strategy must refuse it, and the runner then judges
the operator's story in two halves. For the hold window (`IDLE_HOLD_S`,
90 s scaled by `OME_TEST_TIMEOUT_MULT`) after the push no pod of a bumped
Component may be created, deleted or patched onto the pushed revision
(same UIDs, no pod carrying the bumped annotation), every Instance must
stay ready (`floor_soft=1` waives this where the case broke an Instance
itself), and an `InPlaceUpdateNotPossible` event must name the Component:
the engine records one when it refuses the diff, and a hold with no reason
fails. Then the runner lands the corrective push, every bumped member's
runner put back to what it was before the push (removed where it had none)
with the annotation bump left in place, so the target differs from the
running revision by the annotation alone; the usual rollout checks judge
that roll, counted in place (same UIDs, the bumped annotation on every
pod), and the live spec must carry the pre-push runner at the end. The
engine's refusal leaves the rows untouched (nothing is stamped Updating),
and a repair of a broken Instance yields to a roll whose target it is not,
so a crash-looping Instance under a held push stays broken until the
corrective push patches it in place (`fault=crashloop:old form=start` with
`expect=held` reads that: the Instance ends on the corrective revision
serving with no repair recorded after the bump). A held push changes no
pod, so the fault hook and the hold are ticked between watch events. The
generated `isvcs/gen-inplaceonly/` family drives the three template edits,
the image edit, and the crash loop under each, on the single-pod and gang
shapes.
### A scale or a delete on a service with a broken Instance (`during=idle` in `mode=scaledown|recreate`)

`during=idle` in `mode=scaledown` and `mode=recreate` breaks one Instance of
the settled service first and lands the mode's operation once the status has
reported it. The break is one of `fault=crashloop:old form=start|load`,
`fault=imagepull:old`, `fault=readiness:old form=persist|recover` (the
fixture's readiness switch set in the live pod with `kubectl exec`, so the
pod keeps running, leaves rotation and is never restarted; `persist` keeps
the switch set until the case ends and the end-of-case cleanup lifts it) or
`fault=configkey:missing` (the ConfigMap key the runner reads removed and
the runner restarted, so it waits in `CreateContainerConfigError`).
`inst=<index>` aims the break at one Instance; the generated families use
Instance 0, the lowest index, so a scale-down's natural victim (the highest
index) and the broken Instance differ and the order of removal can be told
apart. `victimcomp=decoder|router` aims it at a sibling the operation does
not scale: the engine's scale then takes nothing of the broken Instance and
states its fate as a scale-up does, and the engine is the negative control
of the sibling's break.

The one-shot forms take the cause away right after the operation has
landed: `lift=after-settle` on a crash, a pull failure or the missing key
(the pod pointed back at its image, the killer stopped, the key put back)
and `form=recover` on the readiness switch. The status's first word about
the broken Instance is what the operator acts on there, so the operation
meets the Instance broken; a scale that does not take it then expects
`expect=heals`: Ready and serving again on its running revision once the
cause is gone, the same pod started again or rebuilt by a repair recorded
at most once. The key kept away under a scale-up (`fault=configkey:missing`
with no `lift=` and a target above the count) is read by the new Instance
too, so that scale-up states `expect=waits`: the new Instance waits on the
same missing key with `CreateContainerConfigError` on its pod and a row that
never reads Ready, as the broken Instance does, across the scale-up and the
hold window; the healthy Instances keep serving, nothing surges above the
target, and a repair that rebuilds a waiting pod is reported, not judged.
A gang waits through the pods rendered from the runner that reads the key;
its other members read none and run. Once the scale has landed the two
waiting rows read Failed naming `CreateContainerConfigError` — the broken
Instance's with its repair parked on it, the new Instance's with its create
disposed — and every other row settles Ready with no operation open.

A pending cause is armed on the settled service before the operation
instead of a break: `fault=taint:all` or `fault=quota:<n>` with
`lift=after-settle|never`, the pending vocabulary. A scale-up's new
Instance, or every pod of a delete's recreate, waits with the product's own
reason (`Unschedulable`, `QuotaExceeded`) on its row, no row reads Failed
and nothing settled is taken down to make room; the operation completes
once the cause is lifted, or holds with the reason across the hold window.
A scale-down places nothing, so it converges beside the cause and no row
may wait.

The operator acts as the status lets them: once the broken Instance's row
leaves Ready (a repair opening on it, which waits for the pod to be older
than the stuck-pod grace), or once it has reported fewer serving pods than it
has for `BROKEN_STATUS_WAIT_S` (120 s scaled) while staying Ready, which is
where readiness loss leaves it. A row that never reports the break fails the
case before the operation is judged: the status told the operator nothing.
Then the operation, judged on what the operator wanted:

- **a scale-down** (`to_e=` below N) removes the broken Instance first, then
  the highest healthy indexes. The verdict snapshots the rows as the scale
  lands and right after (a repair may open on the broken Instance between
  the patch and the engine's first look) and accepts the removal either
  snapshot calls for: the rows that existed at the scale and are still
  present must be exactly the kept set, with no pod left on a removed index.
  The survivors keep their pods (no UID changes) and keep serving throughout
  (never below the target Ready, debounced like the rollout floor), no
  Component surges above N, and every row settles Ready with no operation
  open within `BROKEN_ROWS_SETTLE_S`. A broken Instance that was already
  repaired when the scale landed is reported; the removal then follows the
  index order alone.
- **a scale-up** (`to_e=` above N) brings a new Instance up on the running
  revision, whole and serving; the healthy Instances keep their pods and
  never leave rotation (never below N-1 Ready); nothing surges above the new
  count; and the broken Instance meets `expect=`: `repaired` (rebuilt, every
  pod new, Ready and serving, with a `RestartTriggered` event naming it) or
  `left` (no new pod for its index across the scale-up and a hold of
  `IDLE_HOLD_S`, and no repair recorded). The generated family states
  `repaired` for a crash at start, a crash after Ready and a pull failure
  under both restart policies, since a pod the kubelet parks in a terminal
  waiting state (CrashLoopBackOff, ImagePullBackOff) past the stuck-pod grace
  is a wedge the crash-loop repair rebuilds under every policy, and `left`
  for readiness loss, the runtime's own out-of-rotation signal.
- **a delete** (`mode=recreate`) is the recreate contract on a fleet with a
  broken Instance: every pod goes within the teardown budget, the broken one
  included, no pod stays Terminating and no finalizer wedges, and the same
  name comes back Ready on fresh pods.

On every operation the Components the operator did not scale stay as they
were (peak at N, the same pods). The generated `isvcs/gen-fault-scale/` and
`isvcs/gen-fault-delete/` families cross the four breaks with both restart
policies (readiness loss under `None` only, since no policy rebuilds it), the
engine, PD and gang shapes, a rolling group on PD, and the operations down by
one, down by two, to one, up by one and delete; the engine runs under
`maxUnavailable: 1`, the budget that admits the repair a scale-up expects (a
budget of zero holds it). The further rows run one case per ledger cell on
the six shape families, under restart policy `None` and no group, on one
scale-down, the scale-up and the delete: the missing key kept away (no
scale-up: the new Instance would wait on the same key), the one-shot forms,
a taint or a quota armed before the operation (the quota kept to the end
under the scale-up and the delete too: the new Instance's row, or a row of
the recreate, waits with its reason across the hold window), and a crash at
start on the sibling (the decoder beside a prefill/decode engine, the router
in front of an engine fleet). `isvcs/MANIFEST.md` lists the cells left out
and why.

### A fault at a moment of a move (`mode=migrate at=accept|surge|drain`)

`at=` fires the case's fault at a moment of the move the request files:
`accept` once the request is recorded (the controller's record in
`status.migrations`, the ledger row or the consumed annotation, whichever
shows first), `surge` once the replacement pod has appeared (a pod the fleet
did not have, or the record at `SurgeReady`), `drain` once a source pod is
being taken down or gone (or the record at `Draining`). The record is read
every 2 s while the moment is armed. `victim=` names whose pod: `moved` (the
Instance being moved: its source pod at accept, its replacement at surge and
at drain; on a gang its leader), `peer` (the highest other Instance of the
moved Component still on its original pods) or `sibling` (the decoder's
highest Instance). The faults are the breaks (`fault=crashloop:old
form=start|load`, `fault=imagepull:old`, patched or killed as under a
rollout) and the source node's loss (`fault=node:notready`, always the node
under the moved Instance's source pod, with `lift=after-recover` and
`needs=forcedelete`: the kubelet is started again once the Instance has
settled, and the cell SKIPs off the force-delete lane as the node family's
do). A quota with no room met by the request (`fault=quota:0 expect=fail`,
the bounded-failure contract of the hand-written `fault/` cases; `lift=1`
lifts it after the bounded failure and a fresh request completes) runs on
the shapes those cases do not. A moment whose victim pod never shows within
the grace is recorded as missed and fails the case. A break lowers the capacity floor by the one
Instance it broke instead of waiving it; a node loss waives it (the dead
node takes every pod on it).

The verdict is the operator's end state. The move finishes (`Completed`) or
is abandoned with a reason its record carries (`Failed` with a reason; a
`Failed` without one, or no terminal record within budget, fails). It is
recorded once: one ledger row for the request, no other move's row (the
force-delete sweep's audit rows are records, not moves), one controller
record. Once the record is terminal the runner watches until the moved
Instance reads whole, Ready and serving on one pod set at the index the
record names (its replacement index when the move completed, its source
index otherwise), with no source pod alive after a completed move, no pod
the fault hit surviving unserving, a broken peer or sibling rebuilt whole and
serving on new pods, and the moved Component holding N whole
Instances on N distinct indices, sustained for the stability window within
the idle budget (the relocation budget for a node loss). A break on a `peer`
or a `sibling` must not fail the move, and that Instance must be rebuilt
(every pod new, Ready and serving) with a `RestartTriggered` event naming it.
The ceiling, the floor, index partition and gang atomicity are the migrate
checks; Components neither moved nor broken keep their pods (reported, not
asserted, under a node loss). A repair recorded for the moved Instance
itself is reported, not judged: the move or the repair may own it. The
generated `isvcs/gen-fault-move/` family (`p7800`+) crosses the three
moments with the three victims, the three breaks and the node loss over the
engine, PD and gang shapes; `isvcs/MANIFEST.md` lists what it leaves out and
why.

The same request runs beside the remaining faults (`p11700`+ of the same
family). A break at a moment may be healed again (`lift=after-settle`: the
killer stops or the pod is pointed back at its image once the break has
bitten for `LIFT_SETTLE_S`) or be one kill of the replacement's runner
(`once=1`, `victim=moved` at `surge` with `form=start` or at `drain` with
`form=load`; the one-shot verdict is added to the move's). The missing key
(`fault=configkey:missing lift=after-settle|never`) and the readiness
switch (`fault=readiness:old form=recover|persist`) fire at the moment on
the moved Instance, a peer or the decoder. A kept cause decides the path
the verdict reads: a cause on a pod the move does not need (a peer's or the
decoder's readiness switch, the decoder's key) leaves the move to complete
and that Instance not serving with the status saying so; the source's
switch at `accept` is drained with the source; a switch set on the
replacement at `drain` leaves the move complete and the moved Instance not
serving, the status saying so; a cause that holds the replacement itself
(the engine's key, which every new engine pod reads, or a replacement that
never passes readiness at `surge`) has the move abandoned with a reason or
waiting with the status saying why (its record's message, or a row of the
moved Component not Ready with its wait or failure recorded). A healed
cause, a key put back, a switch lifted or one kill asks the broken
Instance to serve again, rebuilt or healed where it stood; a rebuild by a
`RestartTriggered` repair is demanded only for a crash or a pull failure
the product keeps hitting. `fault=podkill:migrating` (with `kill=` on a
gang and `repeat=` for the persistent form) deletes a pod by hand at the
request; the move must complete, the floor lowered by one holds and the
repeating verdict reads the window. `fault=evict:migrating at=<moment>` and
`fault=preempt:migrating at=surge lift=after-settle` are the hook-op driver
aimed at the moment's pod: the move finishes or is abandoned with a reason,
its Instance whole and serving at its source index. `fault=taint:all
lift=after-settle|never` is the pending form: the taint is armed before the
request, the replacement waits with `Unschedulable` on its row, nothing
settled is taken down, and the move completes after the lift or the wait
holds across the hold window. `fault=node:notready at=accept|surge
lift=after-settle` is the transient node loss: the source node's kubelet is
stopped at the moment and started again once the node has read NotReady
with no pod of the moved Instance serving on it for `LIFT_SETTLE_S`, so the
move settles on the kubelet's return (no policy relocates anything and the
cell reads the same on either lane); the verdict adds that the kubelet was
started again once the bite had held and that a move that completed left
the moved Instance off the stopped node, beside the move's own checks. `stuck=held|failed` under `mode=migrate`
(with `img=`, or `break=crash-start|crash-load|configkey|stuck`; `bump=`
names the member the push bumps, `victimcomp=` the one whose runner carries
the break; `bump=router` with `img=` is the router's own push parked beside
the engine's move on the engine+router shapes) requests the move once the
push has reached the named state:
the move finishes or is abandoned with a reason within budget, recorded
once; the moved Instance ends whole and serving at the index the record
names (the revision its replacement runs is reported, not judged), or,
where the push promoted it onto the revision its ladder then held, keeps
the pod set the request found under a move rejected naming that revision
(its row's running revision under the Held block names it; serving between
its crashes is not judged); the
Instances that served the running revision at the request keep serving;
the Components neither
pushed nor moved keep their pods; and the status keeps reporting the push
that has not landed, the hold itself under `held`. On the single-pod
shapes the crash after Ready (`break=crash-load`) carries the ops-held
rows' readiness soak as the pushed engine's `minReadySeconds`
(`MOVE_STUCK_CRASH_LOAD_SOAK_S`, the same knob): under the surge strategy a
pushed pod that is Ready is promoted and its source retired before the
crash shows, so the floor the verdict holds could not be met; the soak
outlasts the crash window, so the roll never promotes the pod and the
running Instances keep serving at the floor. The gang shapes run unsoaked
(a crash-looping gang replacement is one pod the kubelet restarts under its
own growing backoff, so under the soak the ladder sees too few failed
attempts to reach `held` inside the case's budget) and carry `floor_slack=1`
instead: with no soak the roll promotes a pushed gang as soon as it is
Ready, so each crash costs the fleet that one Instance until the kubelet
starts it again, and the floor the verdict holds is `N - 1`, what the
product promises without a soak; the Instances the push held down at the
request count once against that slack, not on top of it. The header and
the reason line of each case say which it carries.

### A fault during a runtime edit (`mode=runtimerev` with a break, `expect=recover`)

A break (`fault=crashloop:old form=start|load`, `fault=imagepull:old`,
`victimcomp=engine|decoder`) may ride a runtime edit that rolls
(`expect=roll` with `edit=image img=…` or `edit=env`): it fires as the edit
lands on the dedicated runtime, so the broken Instance is off the target
from that moment and the roll owns it. On top of the usual roll checks the
verdict requires the broken Instance to end on the post-edit revision
serving with no `RestartTriggered` event naming it after the edit, the
healthy Instances to hold the floor lowered by the one broken Instance, and
every Instance row to settle (Ready, no operation open; polled for
`ROWS_SETTLE_S`) once the pods have converged. Every Component inherits the
runtime, so on a PD service the decoder (and the router, on its own default
surge) roll too; a decoder victim is the sibling that inherits the same
runtime.

The recovery guards (`expect=recover`: the runtime is pointed at an image
that cannot be pulled; `expect=recover break=stuck`: its readiness probe
never passes) read, before the corrective edit, whether the status says why
the pushed revision has not landed (a `Failed` row, a retry block or a
rollout hold on some Component, with the update revision still differing
from the current one), waiting up to `HOLD_REASON_WAIT_S` (90 s scaled):
hard for the pull failure, which the kubelet reports and the stuck-pod
breaker escalates, reported for the readiness that never passes, which
nothing escalates before its ready timeout. After the fix they add that
nothing exceeded its ceiling, that the old revision kept serving throughout
(`N - maxUnavailable`: N under a surge, N-1 under the drain-first strategy,
which takes one Instance into the attempt) and that no row is left
mid-operation. The generated `isvcs/gen-fault-edit/` family (`p7900`+)
crosses the breaks with the engine and decoder victims, the image and env
edits, the surge and drain-first strategies and the restart policies, and
runs both bad edits under both strategies, on the engine, PD and gang
shapes.

Two further forms land a break beside a runtime edit, one case per ledger
cell on the six shape families (the surge strategy, the env edit):

- **A break before the edit** (`mode=runtimerev during=idle` with
  `fault=crashloop:old form=start|load [lift=after-settle]`,
  `fault=imagepull:old [lift=after-settle]`, `fault=readiness:old
  form=persist|recover` or `fault=configkey:missing lift=after-settle|never`,
  `victimcomp=` for a sibling): the break lands on the settled service and
  the status reacts to it, then the runtime is edited, so every Component
  rolls with one Instance already broken; a one-shot cause is taken away
  once the edit has fired. The roll owns the broken Instance: it ends on
  the edited revision serving with no repair recorded for it after the
  edit, the healthy Instances hold the floor lowered by it, nothing exceeds
  its ceiling and no row is left mid-operation (a kept readiness switch
  goes with the pod the roll rebuilds). With the key kept away no revision
  starts without it: the roll parks with its reason and nothing of the
  edited revision serves.
- **A break the edited runtime carries** (`mode=runtimerev
  break=crash-start|crash-load|imagepull|configkey [once=1]
  [victimcomp=]`, `break=stuck once=1`): the edit puts the break on the
  victim Component's runners in the same patch that re-renders every
  Component (the shell-less or the unpullable image, the exit switch, a
  second env entry reading a key the ConfigMap lacks). Kept, the roll
  parks on its first broken pod with the status saying why and nothing of
  that revision serving; a corrective runtime edit (the break taken off,
  plus a distinct runner env) then lands on every Instance, nothing of the
  broken revision is created again or left, no hold is left and no repair
  is opened for a rolled Instance after the fix. `once=1` is each kind's
  one-shot form on the first pushed pod (the kill, the unpullable image
  pointed back, the key added, the readiness switch lifted): the roll
  completes with no hold behind it and nothing escalates the one bite.
  `break=stuck` kept stays the recovery guard (`expect=recover`).

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
  and no `RestartTriggered` event first recorded after the bump naming it
  (a repair opened before the bump may complete, and have its event recorded
  again, after it): an Instance whose revision is not the roll target
  belongs to the roll, never to a repair on the revision it was running. A
  gang under SurgeThenDrain is replaced at a
  fresh Instance index and its source index retired, so the verdict follows
  the Instance across the gang surges and reads the index it ended at. Every
  record that names a pair is read. The InferenceReplica's status rows: a
  source row carries its replacement index from the claim until the row is
  removed as the surge completes, the invariant poll, which reads the rows
  every few seconds, keeps every pair it sees claimed (a source seen again
  on its running revision without the claim was reset, and its pair is
  dropped), and the rows are read once more at the verdict. The ISVC's
  events: a `RecreateUpdateStarted` names the replacement index, a
  `RecreateUpdateCompleted` names it again as the surge completes (a
  completion is a surge that stood whether or not the recorder kept its
  start), a `GangSurgeAbandoned` retires one, a `MigrationRequestAccepted`
  names the index a move surges to, and the starts the abandons do not
  cancel out are where the Instance went. The migration ledger: a move's
  row names its replacement index once allocated. No one record is enough
  — the Kubernetes event recorder drops an object's events past a burst,
  which a many-Instance fleet reaches before its surges start, and a claim
  can land after the pods have converged — so while the chain ends at an
  index with no pod the records are re-read every `ROW_POLL_S` for up to
  `ROWS_SETTLE_S` before the verdict fails. A
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
  last leaves the operator short for the whole roll. The order needs two
  peers to be read: the break lands as the bump does, once the roll has
  taken its first Instance, so with a single peer the broken Instance's
  place is the break's timing, not the roll's order, and the verdict reports
  the claim as not measurable instead of failing it. The generated
  `isvcs/gen-crashloop-group/` family carries it: the running-revision crash
  under each group policy (blueGreen, Sequential, maintainRatio) on the
  shapes that have a peer for it, and under the zero budgets of a plain
  rolling group (maxUnavailable 0 with maxSurge 1, maxSurge 0 with
  maxUnavailable 1), crossed with both forms, both drain orders and both
  restart policies. The gang shape runs two gangs under the surge strategy
  and three under the drain-first one: with maxUnavailable 1 the dark gang
  waits for the slot the roll's first gang holds, so with two gangs the only
  peer precedes it by construction, while with three it is rebuilt ahead of
  the peer still waiting (the victim is the highest index, as on the
  single-pod shapes). Three gangs with the decoder and the router are 14
  pods, above the gang scheduler twin's node cap, so those cells run on the
  default scheduler only, as the family's gang surge cells already do. Each
  case's second line says what the policy's own rule lets it assert:
  blueGreen flips traffic only once the whole new set is complete and cannot
  replace the broken Instance ahead of the set, so those cases assert the
  per-budget roll and the order, and read the flip as status only;
  Sequential's component order and maintainRatio's band are the ordinary
  ordering and ratio checks. Under a break the band is read net of the
  broken Instance as well as raw, and each sample is the better of the two
  readings: the broken Instance's lost capacity is the fault's, not the
  gate's, and the gate paces the raw counts, so a deviation that either
  reading keeps within tolerance is not the gate's. The band is measured,
  not waived, so the gate's pacing of the healthy Instances stays asserted
  on the break cells.

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
  than the case's allowance are off the running revision, which therefore
  keeps serving at that floor (debounced like the capacity floor), and
  never more than that many Instances carry the crashing revision at once
  — the allowance is the larger of maxSurge and maxUnavailable, plus one
  with `form=load` under every strategy: the promoted crashing Instance
  still looks healthy when the roll admits the next start (a drain under
  a drain-first strategy, a surge under the surge one; minReadySeconds is
  zero), so two are off at once at the floor, as a Deployment reaches too
  — at most budget plus one Instances off the running revision, the extra
  one being that start — and on a fleet the surge strategy rolls whole
  inside the crash window the whole fleet is the allowance: each further
  Instance takes one startup to come up
  (`RUNNER_STARTUP_S`, the fixture's `TEST_STARTUP_DELAY`, 15) and the
  first crash shows `CRASH_EXIT_AFTER_S` (20) after the first promotion,
  so on a fleet of two the second promote and drain land before any
  restart is visible and the whole fleet is the allowance
  (`_surge_roll_fits_crash_window`: `(N - 1) × (startup + minReadySeconds)
  < crash exit`; the verdict's note states it), while a larger fleet,
  whose first crash is visible before the roll completes, keeps budget
  plus one; status says why the roll stopped — a `rolloutHold` on
  the victim Component from the retry ladder (`RetryBlock`, `Held`) or the
  budget (`Budget`), or one of its Instance rows at `Failed`, and a
  `RetryHeld` or `InstanceFailed` event naming it — within the rotate
  budget (a coordination gate's hold — `Plan`, `Pairing`, `Ratio`,
  `Sequential` — is the group opening or pacing its run, not the crash),
  and once it has, every further pod of the crashing revision is a retry
  the ladder announced: a `RetryBlock` backoff names its next attempt's
  time in the retry block (`nextRetryAt`), one attempt is expected at each
  announced time, and none once the ladder reached its bound (`Held`); the
  watch follows the ladder to that bound within the rotate budget, exits
  once status has said why and the story has been quiet for `IDLE_HOLD_S`
  with no retry pending, and a pod created outside those windows fails the
  case; a sound group member converges on its own new
  revision meanwhile, or holds with a reason. Then the harness pushes
  `rebump-fix` (a `v3` bump that drops the runner override) and every bumped
  Instance must end on the fixed revision and serving within the rotate
  budget, with nothing rebuilding the crashing revision after the fix landed
  and no `rolloutHold` left in status. Peaks, isolation of the untouched
  Components and gang atomicity hold throughout. A pushed revision that
  never crashes is `INFRA-FAIL` (the arm failed, not the product). The
  generated `isvcs/gen-crashloop-new/` family crosses both forms with
  topology, strategy (SurgeThenDrain, RecreatePod and InPlaceIfPossible at
  their canonical budgets), restart policy and, on PD, a rolling group. A
  sibling's pushed revision crash-looping — `victimcomp=decoder` with the
  decoder bumped beside the engine in a `rollingUpdate` group,
  `victimcomp=router bump=engine,router` — is judged the same way on the
  sibling (from p5072), the engine rolling to its own new revision beside it.

- **`once=1`** (with `fault=crashloop:old|new` and `form=start|load`, rollout
  mode) is a crash that happens exactly once: the runner of one pod is
  killed once with `kubectl exec … kill 1` — the fixture's startup shell
  before the pod is Ready, nginx after — and the kubelet starts the
  container again; nothing kills it twice and no template changes.
  `form=start` lands during the pod's first start (the container running,
  not yet Ready, never restarted) and `form=load` once it is Ready. On the
  running revision (`crashloop:old`) the victim is the break rule's pod,
  on the idle service (`during=idle`; a crash at start then fires while
  the initial fleet comes up, the only start the running revision has) or
  as a push lands (`settling`); on the pushed revision (`crashloop:new`)
  it is the first single or leader pod of `victimcomp=` the push brings
  up, in the state the form needs. The aimed pod is read on the clock,
  every `ONCE_POLL_S`, not on pod events (a starting pod raises none
  between its container running and Ready), and after the kill the
  victim is read live until its restart count rises, so a roll that later
  retires its gang at a fresh index cannot hide the restart. A kill that
  never finds a pod in that state is `INFRA-FAIL`, not a product failure,
  and the verdict says what the watcher first saw: the aimed pod already
  Ready at first sight (Ready before the harness could read it), or not
  Ready and still missed (the harness's own miss). A kill whose restart
  was never read live and that no repair named is the same apparatus
  failure; under a push the roll's own replacement of the Instance proves
  nothing, at idle a rebuild nobody asked for does. The verdict is the
  operator's end state after a fault that heals by itself: the Instance
  Ready and serving on the revision it should run (the running one at
  idle, the new one after a push on either side; read at the index its
  gang surge left it at), no `InstanceFailed`, `RepairHeld` or
  `RetryHeld` event naming it, every row settled with no operation open,
  and a repair only where the restart policy promises one
  — none for the running revision under a push (its revision is not the
  roll target, so the roll owns it), none under `restartPolicy: None`
  (the kubelet's restart is the whole story), and under
  `RecreateInstanceOnPodRestart` one rebuild, required for a settled pod
  that restarted after Ready, reported for a restart before its first
  Ready or on the pushed revision mid-roll, and never two. At idle the
  Instance must also come back on its own within the budget and then hold
  through `IDLE_HOLD_S` with no second generation of pods and no new
  revision; under a push `floor_slack=1` allows for the one Instance the
  kill takes while it restarts, and the usual rollout checks judge the
  roll. A sibling broken once at idle — `victimcomp=decoder|router`, the
  router behind the engine or the decoder of a prefill/decode service — is
  read the same way: nothing is pushed, so the sibling's own restart policy
  decides the repair and the engine must keep its pods. Under a push the
  sibling is read the same way when the push leaves it alone (`bump=engine`),
  and as the pushed revision's first pod when the push bumps it beside the
  engine (`crashloop:new once=1 victimcomp=decoder` in a `rollingUpdate`
  group, `victimcomp=router bump=engine,router`). The generated
  `isvcs/gen-crash-once/` family (`p8400`+) crosses
  both forms with the running revision at idle and under a push and the
  pushed revision under a push, the six shape families of the coverage
  ledger (the engine alone, behind a router and with a decoder and a
  router, single-pod and as a gang), both restart policies and the surge
  and drain-first strategies; the one-shot forms of the readiness, config-key
  and image-pull faults are the `isvcs/gen-lift/` family's (below).

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
  `lift=after-failed` and `issue=<n>`, the issue such a park is tracked
  under) asserts instead that the row stays `Failed` and nothing of the
  Instance serves across the hold window after the lift, and a row that
  heals fails the case as a stale park, so the flip to `expect=heals` is
  forced the day the rule lands. `batch.py` lists `expect=parked` cases in
  its PARKED DEBT table. The restore also runs from the end-of-case cleanup
  (a no-op after the lift, harmless once the case's ConfigMap is gone with
  the case). The generated `isvcs/gen-heal/` family crosses the fault with
  topology, restart policy, lift phase and a rolling group, and every case
  runs live with `expect=heals`: a repair parked at Failed resumes when its
  rebuilt set comes up (and re-arms on the update retry ladder otherwise),
  and a grouped engine's repair is admitted without the group's gate while
  the Instance serves nothing — `isvcs/MANIFEST.md` states per axis how
  each half heals.

- **`fault=configkey:missing` with `lift=never` and `expect=failed`**
  (rollout mode, `during=idle`) keeps the key away until the case ends: the
  same break, never restored by the harness (only by the end-of-case
  cleanup). The verdict needs the victim's row to reach `Failed` within the
  budget with a recorded reason (the case's `lifecycle.instanceReadyTimeout`
  brings the escalation inside it), to stay `Failed` with nothing of the
  Instance serving across `IDLE_HOLD_S`, no new revision, no reset mailbox
  and no Component above N alive. It is the persistent form of `config
  missing` on the running revision; `batch.py` does not list it as debt.

- **`fault=configkey:new` with `lift=never|after-settle`** (rollout mode,
  no `during=`) is the push itself: the bump points the bumped engine
  runner at `<key>-new`, next to the key the running revision reads (the
  reference and the env var are read off a live engine pod), so the pushed
  revision's first engine pod waits in `CreateContainerConfigError` from
  its creation. `lift=never` reads like `readiness:new`: the roll holds at
  the Instance ready timeout with the new Instance `Failed` and its reason,
  the old revision keeps serving at the floor, nothing of the broken
  revision serves, and a fix push back onto the present key lands on every
  Instance with nothing of the broken revision left. `lift=after-settle`
  adds the key once the wait has held for `LIFT_SETTLE_S`; the kubelet's
  own retry starts the container and the roll must complete with no hold
  behind it: no bumped Instance `Failed`, nothing escalated or held
  (`InstanceFailed`, `RetryHeld`, `RepairHeld`), the Instance Ready and
  serving on the new revision, every row settled; a repair is reported,
  never required, never twice. The watch and verdict are shared by the
  one-shot pushed-revision forms of readiness and image pull below.

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
  **`fault=node:<hook>` with `lift=after-settle|after-recover`** is the
  same loss at a hook of an operation in flight: `node:settling|rotating|draining`
  under a plain push or `mode=runtimerev`, `node:settling` beside
  `rollback=1 during=<hook>`, a mid-roll `edit=`, `mode=scaledown` or
  `mode=recreate` (the loss lands once the operation has, the rollback or
  the edit fired at its own hook first), and `node:canary-<state>[@step]`
  under `mode=canary`. The victim is the break rule's (the highest engine
  Instance still on its starting pods; under a scale-down the highest
  Instance the scale keeps, never one it takes). `lift=after-settle` is the
  transient loss: the kubelet is started again once the bite has held
  `LIFT_SETTLE_S`, the pods it never stopped serve again, and it holds on
  either lane. `lift=after-recover` keeps the node down until the Instance
  is Ready and serving on other nodes with every pod the node held
  replaced (under a delete, until those pods are gone), which is the
  force-delete policy's doing (`needs=forcedelete`, the policy twin); then
  the kubelet is started again. A pod a drain deleted before the node's
  evidence matured keeps its own termination grace plus the overdue slack
  before the sweep may act, so the watch stays open for that window within
  its budget, and a watch that ends inside it leaves the sweep unjudged
  rather than failed. That form runs under every operation: a
  rollback, an edit, a runtime edit, a scale-down and the canary keep the
  victim Instance and rebuild it elsewhere, a push and a scale-up rebuild
  it elsewhere on the revision they owe it, and a delete has the pods the
  node held freed so the re-apply lands elsewhere. Either way the operation must complete
  within its budget plus the loss's (the NotReady grace and the settle
  window, or the relocation budget), the rows settle, nothing is held and
  the reset mailbox stays unwritten; `expect=` has no place, the
  operation's completion is the verdict. The floor is reported rather than
  judged (`floor_soft=1`): the node takes every pod it ran, the victim's
  neighbours included; gang atomicity is not judged either, since the
  restart policy decides whether a member the node took is rebuilt alone
  or with its gang. The hook half of `isvcs/gen-node/` (from p4296) runs
  one case per ledger cell of those operations on the six shape families:
  transient on every operation, kept down on a rollback, a strategy edit,
  a runtime edit, a scale-down and the canary.

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
  **`fault=ctrlrestart:<hook>` beside the remaining operations** is the
  hook-op driver's: `ctrlrestart:settling` beside `rollback=1
  during=<hook>` or a mid-roll `edit=` (the manager is deleted as the
  rollback or the edit lands, once it has fired at its own hook), under
  `mode=recreate` (as the delete lands) and `mode=scaledown` with `to_e`
  above N (as the scale-up lands, the scale driver's), and
  `ctrlrestart:settling|rotating|draining` under `mode=runtimerev` (a hook
  of the roll the edit starts). The restart loses no pod, so no victim,
  lift or slack knob combines with it; the floor and the order are judged
  in full. The operation must complete within its budget plus the
  manager's return (`CTRL_READY_S`), the rows settle and nothing is held;
  the checks name the manager pods deleted and, for a one-shot restart,
  when the manager read Ready again under new names. A delete waits for
  the restarted manager's webhook to admit a server-side dry-run apply
  before the same-name recreate. The hook half of `isvcs/gen-ctrlrestart/`
  (from p4668) runs the one-shot form of a rollback, a strategy edit, a
  runtime edit, a scale-up and a delete on the six shape families; the
  persistent form is `repeat=` in `isvcs/gen-repeat/` (below).
  **`fault=ctrlrestart:idle` with `during=idle`** and no `repeat=` is the
  one-shot restart on the settled service: the manager pod is deleted once
  while nothing is in flight, and across the restart and a settle after it
  no pod of the service is created or deleted, every Instance stays Ready
  and serving, no repair is recorded, and every row ends Ready with no
  operation open and no hold; the idle half of `isvcs/gen-ctrlrestart/`
  (from p4698) runs it on the six shape families, and `repeat=<n>` is the
  same loop applied again each time the manager is Ready.

- **`fault=agerecord:<hook>` beyond a canary: a stale revision record under
  every operation.** The rolled-out record
  (`status.components.<c>.latestRolledoutRevision`) names the revision that
  owns a Component's traffic; a canary rollback reads it as the revision to
  return to. It goes stale when a status write is lost or overwritten (a
  conflict with another writer, a manager restarted between the pods'
  switch and the write, a status put back from a backup, a hand patch), and
  only the controller's own writers put it right, on a pass that sees the
  Component's pods on one revision. The fault writes the victim's record
  (`victimcomp=`, default engine) back to the revision it ran before the
  one it runs, which needs history: `preroll=` must name the victim, and
  the runner rolls it once before the story (every Instance on the new
  revision and serving, the rows settled, the record naming it with the old
  revision as its previous one), then starts the watch over from that
  fleet. The record stays wrong until the controller rewrites it, so the
  fault has one form, the persistent one. Where it lands:
  `agerecord:idle during=idle` on the settled service with nothing to
  follow (`mode=rollout`; a status write wakes no controller, so the runner
  waits `RECORD_OWN_PASS_S` for a pass of the controller's own, then sets an
  inert annotation on the ISVC, `perm.test/record-nudge`, so it looks once,
  and allows `RECORD_LEVEL_S` more) or ahead of a scale or a delete
  (`mode=scaledown|recreate`); `agerecord:settling|rotating|draining` under
  a push or a runtime edit (a hook of the roll) and `agerecord:settling` as
  a rollback or a mid-roll edit lands once it has fired at its own
  `during=` hook; `agerecord:migrating at=<moment>` at a moment of a move.
  The verdict: the operation completes within its budget with the usual
  floor, order, gang and index checks (a rollback ends on the starting
  revision, an edit on the live spec, a move's row Completed, a delete
  followed by the same-name recreate), no pod of the victim is ever seen on
  the aged revision, and the record names a revision the victim's pods run
  again (`RECORD_LEVEL_S` allowed after the operation for the write that
  levels it; after a delete the re-created service's record names the
  revision it runs). `lift=`, `once=`, `repeat=` and the break knobs do not
  combine with it. `isvcs/gen-stale-record/` runs one case per ledger cell
  on the six shape families; `isvcs/gen-canary-stale-record/` runs the
  canary form (the gate's rollback parks on the empty stable revision) on
  the four shapes the hand-written cases leave.

- **`fault=readiness:old` with `form=persist|recover`** (rollout mode,
  `during=idle` only) makes one serving pod of the running revision — the
  break victim, `victimcomp=` as above — stop passing its readiness probe
  while it keeps running: the runner sets the fixture's readiness switch
  file in the live pod (see the runtime section), so startup and liveness
  keep passing and the kubelet never restarts it. Readiness is the runtime's
  own out-of-rotation signal, not a failure, and no restart policy rebuilds
  such a pod, so the verdict asserts the pod stops reporting Ready and leaves
  the Ready endpoints of its routed Service within `READINESS_DROP_S`, that
  the Instance's row reports the drop (its serving count falls below its pod
  count, or its phase leaves Ready), and that nothing is rebuilt, surged or
  re-revisioned. `form=persist` holds that across the hold window
  (`IDLE_HOLD_S`); `form=recover` lifts the switch `READINESS_RECOVER_AFTER_S`
  after the drop and requires the same pod (same UID) to return Ready,
  serving and routed, with the row recovered. The lift also runs from the
  end-of-case cleanup.

- **`fault=readiness:new`** (rollout mode, no `during=`) is the bump itself:
  the trigger sets the switch in the bumped runners' env (`TEST_UNREADY`), so
  the pushed revision's pods run and never pass readiness. The roll must hold
  at the Instance ready timeout — a bumped Instance on the new revision reads
  `Failed` with its recorded reason, or an `InstanceFailed` event names it —
  while the old revision keeps serving at the floor and no pod of the broken
  revision ever serves; the runner then pushes a fix (a bump that drops the
  switch) and requires it to land on every Instance with nothing of the
  broken revision left. Cases set a short `lifecycle.instanceReadyTimeout` so
  the hold arrives inside the budget. The switch is a template change beyond
  images, so an in-place strategy recreates and its pods are counted by hash.
  **`form=recover`** is the one-shot form: the push is an ordinary bump and
  the harness sets the switch file in the push's first engine single or
  leader pod while its runner is still starting (running, not Ready, never
  restarted), lifting it once the pod has run `READINESS_NEW_BITE_S` without
  passing readiness and the drop has held for `LIFT_SETTLE_S`. The roll must
  complete with no hold behind it, the same pod (same UID) Ready and serving
  on the new revision and no `RestartTriggered` for the Instance under either
  restart policy; a pod read Ready before the lift is an apparatus verdict
  (the switch landed late), a pod restarted or replaced while it only failed
  readiness fails the case.

- **`fault=imagepull:new` with `lift=never`** is the pushed revision's pull
  failure kept: the bumped engine runs the unpullable reference, so the
  push's first pod is parked in `ImagePullBackOff` and never Ready; the roll
  must hold at the Instance ready timeout with the reason recorded while the
  old revision keeps serving and the broken one never serves, then a fix
  push that drops the image override must land on every Instance with
  nothing of the broken revision left (the kept key's verdict, read on the
  image). The generated `isvcs/gen-lift/` family runs it on the six shapes.
- **`fault=imagepull:old` with `during=idle lift=after-settle expect=heals`**
  and **`fault=imagepull:new` with `lift=after-settle`** are the one-shot
  forms of the image pull. At idle the running revision's victim is pointed
  at the unpullable reference as above. The pod is past the stuck-pod grace,
  which counts from its creation, so the crash-loop repair usually rebuilds
  it as soon as the kubelet's pull failure (`ImagePullBackOff` or
  `ErrImagePull`) is seen, before it has held for `LIFT_SETTLE_S`: the
  rebuilt pod (a new UID) or the Instance serving again ends the wait as
  healed before the lift, with nothing left to point back. Where the failure
  does hold for `LIFT_SETTLE_S` the pod is pointed back at the image it ran.
  Either way the heal verdict of `configkey` applies (Ready and serving again
  on the running revision, no new revision, no reset mailbox, no Component
  above N alive). Under a push the
  push's first engine pod is pointed at the unpullable reference while its
  runner is still starting and back once the failure has settled; the
  one-shot pushed-revision verdict applies, with one rebuild reported (the
  kubelet's restart of the container may be read as a runner restart by
  `RecreateInstanceOnPodRestart`), never required, never twice.
  **`fault=imagepull:old` with `lift=after-settle` and no `during=`** is the
  running revision's pull failure as the bump lands, pointed back once it
  has held for `LIFT_SETTLE_S` unless the roll has replaced the pod first:
  the Instance was off the target from the bump, so the roll owns it either
  way and it ends on the new revision serving with no repair recorded after
  the bump; `isvcs/gen-lift/` (from p9370) runs it on the six shape families
  under both restart policies.

- **`fault=podkill:<phase>` with `kill=leader|worker`** (gang topologies)
  aims the podkill at one gang member instead of the lowest gang's leader,
  and `victim=stable|canary` picks the gang: the lowest serving old-revision
  gang, or the newest gang on the new revision (the hook then waits for a
  pod of that member to exist). `podkill:draining` fires at the first
  old-revision pod observed on its way out. A gang already draining is never
  the victim. The verdict adds the gang-member contract: the killed pod is
  gone; under `RecreateInstanceOnPodRestart` the gang is rebuilt as a whole
  (a `RestartTriggered` names it) unless the roll took it first, under `None`
  no whole-gang repair is recorded; and while the gang is short a Ready
  member its row never counts every pod as serving (sampled once the gap has
  lasted `GANG_SHORT_REPORT_S`). `floor_slack=<k>` lowers the capacity floor
  by the `k` Instances the fault takes, so the untouched gangs stay a hard
  claim (`N - maxUnavailable - k`) instead of a `floor_soft=1` waiver.
  `restart=` in the header names the policy the verdict reads.

- **`fault=podkill:<hook>` beside the remaining operations** is the pod
  deleted by hand at a moment of an operation beyond the push:
  `podkill:settling` beside `rollback=1 during=<hook>` or a mid-roll
  `edit=` (a serving pod of the running revision, `victim=stable`, is
  deleted as the rollback or the edit lands, once it has fired at its own
  hook), `podkill:rotating` under `mode=runtimerev` (at the first pod of
  the new revision; any roll hook is accepted), `podkill:settling` under
  `mode=scaledown` with `to_e` above or below N (as the scale lands; the
  scale driver's, as the eviction under a scale is) and under
  `mode=recreate` (as the delete lands). The aim knobs apply as under a
  push: `kill=leader|worker` with `restart=` names the gang member and the
  policy the verdict reads, `victimcomp=engine` keeps the deletion on the
  engine of a prefill/decode service, `floor_slack=1` lowers the floor by
  the Instance the deletion takes (not under a delete, which judges no
  floor) and `order_soft=1` waives the surge-before-drain order the
  deletion itself breaks under a roll. The verdict: the deletion fired at
  the hook and names what it took; the first pod that replaces it runs the
  image its revision label names (the `verb=podkill` label-truth record,
  kept on the fault); a gang's lost member meets the gang-member contract
  above (the scale driver leaves that gang out of its partial-teardown
  check, since the restart policy decides what is rebuilt); the operation
  completes within its budget with every row settled and no hold left;
  under a delete nothing replaces the pod (a replacement before the
  teardown is noted) and the same-name recreate comes up on fresh pods.
  `repeat=<n>` is the persistent form beside the rollback, the edit, the
  runtime edit and the scale-down: the pod is deleted again each time its
  Instance is back, and the hook-op driver holds the operation's window
  open until the repetitions are spent, as it does for the manager
  restart; a delete brings nothing back, so it has the one-shot form only.
  The one-shot rows live in `isvcs/gen-fault/` (from p1520, the single-pod
  shapes) and `isvcs/gen-gang-member/` (from p7172, the gangs under restart
  policy `None`); the persistent rows in `isvcs/gen-repeat/` (from p8868,
  the gangs under `RecreateInstanceOnPodRestart`).

- **`fault=cordon:all`** cordons every schedulable worker (the control plane
  and nodes already cordoned are left alone), so nothing new can be placed
  until the lift; each node is labelled `perm.test/cordoned=<case>` and
  uncordoned in cleanup. **`lift=after-hold|never`** (rollout mode, with
  `fault=quota:<n>` or `fault=cordon:all`) squeezes a roll out of room: the
  verdict first requires the roll to report its wait on capacity — a bumped
  Instance's operation waiting on `QuotaExceeded` or `Unschedulable`, or an
  `InstanceQuotaBlocked` event — and prints whether the reason names the
  capacity; while the wait holds no more pods of the running gangs may leave
  than the strategy's own `maxUnavailable` budget allows (nothing is torn
  down to make room) and the floor holds. `after-hold` then lifts the squeeze
  once the wait has held `LIFT_SETTLE_S` and requires the roll to complete;
  `never` keeps the squeeze across `IDLE_HOLD_S`, requires the wait to still
  be reported at the end and the pod set to be still (no churn), and ends
  with the squeeze in place. A quota one pod short of a gang (`quota:<w>` for
  a `w+1`-pod gang) is how a surge gang is kept from being placed; a
  drain-first strategy frees its own room under that quota, so its cell is a
  plain roll that must complete.

- **`during=idle`** (rollout mode, with a persistent fault) fires the fault
  on the settled service and never bumps; the verdict reads what the product
  does about the break on its own, stated by `expect=`. `expect=repaired`:
  within the rotate budget the victim Instance is rebuilt (every pod new,
  Ready and serving) and a `RestartTriggered` event names it.
  `expect=held`: nothing rebuilds it across the hold window (`IDLE_HOLD_S`,
  90 s scaled by `OME_TEST_TIMEOUT_MULT`) and a `RepairHeld` event names it
  — the hold the budget or a rollout group's gate keeps on a repair that
  takes serving capacity offline; a silent hold fails. Absent, `expect=` is
  repaired: the break wedges the victim Instance's single or leader pod, so
  the Instance serves nothing and its repair is admitted without touching
  the budget or the gate, whatever group it sits in
  (`isvcs/gen-crashloop-idle/` states `expect=repaired` on every cell).
  Either way no Component may ever
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
  running (the roll owns it; a gang's roll replaces it at a fresh index and
  retires its own, so the Instance is followed through its gang surges and
  read where they left it); with nothing pushed, a freeze that landed first
  releases exactly one repair, recorded by its event, and every other cell
  moves nothing; in every cell the InferenceReplica rows end Ready with no
  operation open, waited for up to `ROWS_SETTLE_S` and held to the indexes
  0..N-1 only when nothing was pushed (a repair rebuilds in place), and
  peaks stay under the header ceilings. A repair that
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

- **`push=fix` alone** (rollout mode, `during=idle` with `fault=configkey:missing
  lift=never|after-settle` or `fault=readiness:old form=persist|recover`, no
  `pauseat=`) is the operator's next move after a break of the running
  revision, with no pause around it: the key is removed or the serving pod's
  readiness switch set as above, and once the break has shown on the
  Instance (its pod waiting on the key or out of Ready, a pod of it
  replaced, or its row off Ready) for `FIX_PUSH_SETTLE_S` the fix is pushed
  — a bump whose template no longer carries the cause: the runner given a
  literal value in place of the key it cannot read, a re-rendered pod with
  no switch file. The one-shot forms (`lift=after-settle`, `form=recover`)
  take the cause away as the fix lands, so the broken pod may come back on
  its running revision while the roll is already replacing it. The verdict
  is what the fix push promises: the roll lands on every bumped Instance
  within the rotate budget, the broken Instance Ready and serving on the new
  revision and never the last to land the fix (reported where the fleet has
  fewer than two peers), no `RestartTriggered` naming it after the push (the
  roll owns it), every row settled, no hold left, the peaks within the
  ceilings and the floor reported (`floor_soft=1`) for the one Instance the
  break took. `isvcs/gen-lift/` (from p9322) runs the four forms on the six
  shape families under both restart policies.

- **`via=evict` (mode=kill) and `fault=evict:<phase>` (rollout mode)**
  remove the pod through the Eviction API, the way a node drain does,
  instead of `kubectl delete`. The product keeps a PodDisruptionBudget on
  OMENative pods (`podDisruptionBudget.omeNative` in the chart, one
  disruption at a time), so the API may refuse an eviction while a pod is
  already down: the runner asks again every `FAULT_RETRY_S` seconds as a
  drain would, reports the refusals (under `repeat=<n>` the operation's
  window stays open while a refused repetition is asked again,
  `LIFT_SETTLE_S` from its first refusal, instead of closing on a fleet
  that is whole only because the eviction is being refused; where the
  budget keeps refusing while the operation holds an Instance down, the
  repetitions are those the budget admits, at least one, and the verdict
  names the refusals), and reds a
  case whose eviction is never admitted (at steady state a budget that
  refuses one eviction of a
  fully serving fleet through `LIFT_SETTLE_S` is `FAIL`; the apparatus is
  checked first, so an apiserver without `policy/v1` records `SKIP`). In
  kill mode the verdict is the usual restart-policy one: exactly the
  evicted pod (policy `None`) or the whole Instance
  (`RecreateInstanceOnPodRestart`) is rebuilt, nothing else moves, the
  fleet is back at N; in rollout mode the victim is the podkill rule's
  (`victim=`, `victimcomp=` apply) and the roll must still converge with
  every Instance on the new revision. The generated `isvcs/gen-evict/`
  family crosses both moments with topology, restart policy and (under the
  roll) strategy.

- **`via=preempt` (mode=kill) and `fault=preempt:<phase>` (rollout mode),
  with `lift=after-settle|never`** have the scheduler remove the pod: a
  PriorityClass one notch above the case's pods is created, fillers take
  the spare CPU of every node a case pod could be placed on (Ready,
  uncordoned, untainted), and one filler under that class, pinned to the
  node of the aimed pod (the kill target, or the podkill rule's victim at
  the hook), refuses to share that node with the aimed pod (a required pod
  anti-affinity on the identity labels the pod carries: service, Component,
  Instance index, gang runner, pod ordinal) and asks for that pod's own CPU
  — so the scheduler must preempt that pod and no other (left to size
  alone it picks its own victim among the pods below the press: those no
  disruption budget covers first, the youngest after, so a router or a
  sibling of the same size could go instead, or two pods once the spare had
  shrunk; the runner reads the victim back off the scheduler's
  `DisruptionTarget` condition or its `Preempted` event, by UID, since the
  controller brings the victim back under the same name within seconds —
  never off a drain). The fill is `INFRA-FAIL` when the aimed pod has left
  the node before the press, or when another pod there carries the same
  identity labels, so the press could not single it out. A canary press
  (`victim=canary`) lands on the first bound replacement of the Component
  `victimcomp=` names, or of any acting Component without it. Before the
  press lands, the harness's `perm.test/pending` NoSchedule taint goes on
  every one of those nodes (the fillers tolerate it): the fillers hold the room there
  was, and the taint keeps the case from the room its own pods free
  afterwards — what the preemption vacates and, under a roll, what each
  drain vacates, which a surge-then-drain roll would otherwise take one pod
  at a time. Nothing of the case can be placed anywhere until the lift:
  the Instance waits with the product's own reason (`operation.waiting`
  `Unschedulable` on its InferenceReplica row), and `lift=after-settle`
  removes the taint and drops the fillers once that wait has held
  `LIFT_SETTLE_S`, after which the repair (or the roll) must complete;
  `lift=never` keeps both to the end and judges the hold instead (below).
  The verb needs a cluster this
  harness owns, since it blocks every node (the same check as
  `node:notready`), the PriorityClass API, and an OME control plane that
  outranks the press: among the pods below the press on the pressed node
  the scheduler takes those no disruption budget covers first, and the
  case's pods are covered, so the manager (or anything else there that
  requests CPU) would go before them. The eval deploy runs every
  Deployment in the OME namespace under the class the fixtures name
  (`OME_EVAL_CONTROL_PLANE_PRIORITY_CLASS`, preempting nothing itself);
  the capability check creates that class when absent and reads the
  control plane's priority, and elsewhere the case is `SKIP` with the
  reason. The fill refuses to press (`INFRA-FAIL`) while any other pod on
  the pressed node sits below the press and requests CPU, and a
  control-plane pod the scheduler preempts anyway is `INFRA-FAIL`, never a
  product finding. Fillers carry `perm.test/filler=<case>` and the class
  `perm.test/fault=<case>`; both go at the end of the case, as does the
  taint, and in `batch.py`'s sweep. `isvcs/gen-preempt/` crosses both moments with
  topology, restart policy, lift and (under the roll) strategy. Both verbs
  also fire at a hook of the remaining operations (`fault=evict:settling` |
  `fault=preempt:settling lift=…` beside `rollback=1 during=<hook>`, a
  mid-roll `edit=`, `mode=runtimerev` — there at any roll hook —
  `mode=scaledown` for the preemption, the eviction under a scale being the
  scale driver's own, and `mode=recreate` for the eviction): the fault
  lands once the operation has (the rollback or the edit fired at its own
  hook first), the operation must still complete with every row settled
  and no hold left, the eviction's `repeat=` form applies it again each
  time what it took has come back, and the preemption's Instance waits
  with reason `Unschedulable` until the lift opens the nodes.
  `isvcs/gen-evict/` and `isvcs/gen-preempt/` run one case per ledger cell
  of those operations; `fault=node:settling` beside the same operations is
  the node loss's hook form (see `fault=node:<hook>` above). A pending
  case that fails prints, before teardown, each pod of the case with its
  `PodScheduled` and `Ready` conditions, gates and pod group, the
  PodGroups, the pod events since the trigger with their reporter, and the
  manager's (and the gang scheduler's) log lines naming the case.

- **`pool=packed` (rollout mode, with `fault=crashloop:new` or
  `fault=crashloop:idle`, `form=start|load`)** is the packed pool: once the
  service has settled, a filler takes the spare CPU of every node a case pod
  can be placed on (the preempt verb's filler, sized to the node, under no
  priority class), so the service's pods have exactly the room they occupy
  and a rebuilt pod can land only in the room a pod of the service vacates.
  Then the story, under the strategies that add no pod (RecreatePod and
  InPlaceIfPossible at `maxSurge: 0, maxUnavailable: 1`): with
  `crashloop:new` the push brings a revision whose pods crash, exactly as
  the crashloop:new verb drives it (the arm, the hold, the `rebump-fix`
  push); with `crashloop:idle` the running revision is broken on one
  Instance of the settled service (the break victim rule: the highest
  Instance's leader or single pod of `victimcomp=`), the push waits until
  the break has bitten (`BREAK_BITE_S`) and then lands the good revision.
  The verdict is the operator's end state, on top of the crashloop:new
  checks (the hold, the reason, the fix landing with nothing rebuilt on the
  crashing revision) or the running-revision break checks (the Instance
  ends on the good revision serving with no repair recorded after the
  push): never two Instances of a Component down at once from the push (or
  the break) to the end, sampled on every watch read and debounced like the
  capacity floor; the broken Instance lands the good revision first (read
  from when each Instance was first seen on it); it comes back in the room
  it vacated (its pods' nodes, as a multiset, or the same pod under an
  in-place patch) when the engine rolls alone, or in room any pod of a
  rolled Component vacated when engine and decoder roll together (a
  decoder's drain frees room an engine gang can take); for the mirror, no
  pod is created on the running revision after the good push, and a repair
  that opened before the push is reported. A crash after Ready
  (`form=load`) carries `floor_soft=1` and
  reports the one-down rule and the landing order instead of asserting
  them: a runner that exits after Ready can be Ready when the roll decides,
  so the roll need not read that Instance as serving nothing. The usual
  peaks, isolation, gang atomicity, index partition and capacity floor
  apply. The verb needs a cluster this harness owns (the same check as
  `node:notready`); elsewhere the case is `SKIP` with the reason. Fillers
  carry `perm.test/filler=<case>` and go at the end of the case and in
  `batch.py`'s sweep. The generated `isvcs/gen-packed-pool/` family
  (`p8600`+) crosses both stories and both forms with the six shape families
  (the engine, the engine behind a router, prefill/decode, single-pod at
  three Instances and as gangs at two), both strategies, both restart
  policies on the bad push (the mirror runs under `None`: its break lives in
  the live pod, so `RecreateInstanceOnPodRestart` heals it with its own
  rebuild before the push lands) and a `rollingUpdate` group on the
  prefill/decode shapes, whose decoder carries the same one-down budget;
  every cell selects invariants J and K (`inv_on=J,K`).

- **`repeat=<n>`** is the persistent form of the faults above that the
  controller heals once: the fault is applied again each time what it took
  has come back, `n` times in all, and then stops — the lift — so the case
  reads the window and the end state after it. It rides `mode=kill` (the
  pod deleted by hand, `via=evict` the eviction, `via=preempt lift=after-settle`
  the preemption; `kill=leader|worker` names the gang member), a
  `fault=podkill:<phase>` or `fault=evict:<phase>` under a push or a scale
  (`mode=scaledown`, up or down; `victim=stable` keeps the deleter on the
  running revision's pods, `victimcomp=engine` on the Component the
  operation works on), `fault=podkill:settling` beside `rollback=1` or a
  mid-roll `edit=` at their hook and `fault=podkill:<hook>` under
  `mode=runtimerev`, `fault=ctrlrestart:<phase>` under a push, a scale, a runtime edit
  or a delete (`mode=recreate`), beside `rollback=1` or a mid-roll `edit=`
  at their hook, `fault=ctrlrestart:migrating` under a move and
  `fault=ctrlrestart:canary-<state>` under a canary, or
  `fault=ctrlrestart:idle during=idle` for the manager restarting on the
  settled service. A repeating manager restart holds the operation's
  window open until its repetitions are spent (the hook-op, scale, roll,
  move and canary watches alike), so a short operation finishes under the
  first restart and the later ones land on its settled end, which must
  stay settled; a repeating pod deletion at a hook of a rollback, an edit
  or a runtime edit holds the hook-op watch the same way. A pod fault is due again once the Instance it took is
  whole, Ready and serving on new pods — the deleter takes the pod that came
  back, the same Instance, as long as it still fits the aim — or once the
  roll has retired that Instance (a gang surge abandoned, a source drained)
  with its Component back at N; the manager restart once the manager pods
  are Ready under new names; the preemption once the Instance it took is
  placed and serving again (the taint and the fillers are lifted after each
  wait, each repetition's fillers named after it; the scheduler chooses the pod it
  takes on the pressed node, so the wait, the allowance, the scope and the
  record follow that pod's Component, a router or decoder beside the
  engine the press was aimed at). The watch budgets grow by
  `REPEAT_CYCLE_S` per repetition; a repeating fault is ticked between pod
  events too, since a dead manager raises none.
  The verdict during the window: the operation's own checks (the roll
  converges on the new revision, the scale lands, the kill rebuilds exactly
  its policy's scope each time) and the floor the budget promises
  (`floor_slack=1` for the Instance each loss takes; `N-1` on the killed
  Component at steady state); while an Instance is short its row is sampled
  every `INV_TICK_S` and must report the loss (a phase other than Ready, or
  fewer serving pods than pods) and must never count the Instance whole once
  the gap has lasted `GANG_SHORT_REPORT_S` (a preemption's wait with reason
  `Unschedulable` is its report); every `RestartTriggered` after the first
  loss names an Instance the fault took, at most one per loss; the pods the
  controller creates across the window are bounded by what the operation
  and the repairs explain (the roll's or the scale's own pods, plus one
  repair scope per loss, plus `REPEAT_CHURN_SLACK` per loss) — the hot-loop
  bound, since the harness cannot read the manager's reconcile counter, and
  the service's event rate is printed beside it; and no pod comes back on a
  revision the service never pushed (at most one new revision per bumped
  Component, none elsewhere). The manager loop additionally requires that on
  the idle service no pod is created or deleted at all and every Instance
  stays Ready. After the lift: every row Ready with no operation open and no
  `rolloutHold` left on any Component, and the continuous invariants A to I
  as on every case. The generated `isvcs/gen-repeat/` family drives the four
  rows on the six shape families at steady state, under a push and under a
  scale-up, the manager restart under the remaining operations and the
  pod deleted again under a rollback, a strategy edit, a runtime edit and
  a scale-down; `isvcs/MANIFEST.md` lists what it leaves out and why.

- **`fault=taint:all` and `fault=quota:<n>` with `lift=after-settle|never`**
  (mode=kill, or rollout mode without `during=idle`) are the pending
  causes: a new pod cannot be placed (a `perm.test/pending` NoSchedule
  taint on every schedulable node; running pods are untouched) or admitted
  (a ResourceQuota short of the pod; `n<0` admits `n` fewer pods than run,
  so the pod a repair or a drain-first roll creates after one is gone is
  refused, and on a gang that leaves room for all but one member, so the
  gang cannot form). Both are armed before the kill or the bump. The case
  then reads the wait the product reports — `operation.waiting`
  `Unschedulable` from the scheduler, `QuotaExceeded` from admission (a
  quota short of one gang member may also surface as `Unschedulable` once
  the admitted members cannot bind without the last one) — and asserts that
  while the cause holds no Instance reads `Failed` (the configured
  `unschedulableGracePeriod` is longer than any hold here) and no Component
  drops below N minus its allowance (the kill's own Instance, or the roll's
  `maxUnavailable` plus a settled pod the fault itself took): nothing is
  taken down to make room. `lift=after-settle` removes the cause once the
  wait has held `LIFT_SETTLE_S` and then requires the repair to land
  (exact restart-policy scope, fleet at N) or the roll to converge with
  every Instance on the new revision; `lift=never` holds `IDLE_HOLD_S`
  after the wait appeared and requires the wait still reported at the end.
  An operation that completes with the cause in place and no wait reported
  is `INFRA-FAIL` (the cause never bound), never a pass. The taint needs a
  cluster this harness owns (`SKIP` elsewhere); the quota runs anywhere.
  `isvcs/gen-pending/` crosses both causes with both moments, topology,
  restart policy, lift and (under the roll) strategy, with the quota sized
  per cell as above; it also arms a quota with no room before a canary push
  on the shapes the hand-written canary cases do not run (`mode=canary
  fault=quota:0`): kept, the capacity step parks the ladder Failed at its
  ready timeout (`terminal=Failed`); lifted once the ladder has parked
  (`verb=quotalift during=canary-failed`), the capacity lands, the park
  holds and a resume (`verb2=resume`) walks the ladder to Stable. The same
  causes armed before a rollback (`rollback=1
  during=draining`, the roll drain-first: its first replacement is what
  waits, and the rollback then owes that Instance on the starting revision
  under the same cause), before a strategy edit landed as the push lands
  (`edit=strategy:<type> during=settling`) and before a runtime edit
  (`mode=runtimerev`, every Component rolling) are read with the same
  verdict: the operation's pod waits with its reason, the floor holds,
  nothing settled is taken down, and after the lift the operation completes
  (a rollback with every Instance back on the starting revision, an edit
  with the edit on the live spec); the family runs one case per ledger cell
  of those operations on the six shape families.

- **`fault=webhook:idle|settling` with `lift=after-hold|after-window`**
  (rollout mode with `during=idle`, rollout mode as a push lands, or
  `mode=scaledown` as a scale lands) makes the admission webhook
  unreachable. The manager pod serves admission itself behind the webhook
  Service (`ome-webhook-server-service` in the manager's namespace;
  `OME_TEST_WEBHOOK_SERVICE`, `OME_TEST_CTRL_NS`), and the apiserver keeps
  its connection to that pod warm, so a Service pointed at no pod keeps
  answering. The verb leaves the Service alone and points every webhook of
  the Validating/MutatingWebhookConfigurations that name it at a Service
  that does not exist (`perm-webhook-outage-<case>`, checked absent first),
  the manager still running: the apiserver fails to resolve the Service on
  every call, so every write the webhooks intercept with `failurePolicy:
  Fail` (an InferenceService or InferenceReplica spec write, the create of
  an InferenceService pod) is refused with the apiserver's own words until
  the webhooks are pointed back, while status writes pass. The service
  reference each webhook had rides the configuration's
  `perm.test/webhook-outage` annotation, so the end-of-case cleanup and
  `batch.py`'s end-of-run sweep both restore it. `idle` starts the outage on
  the settled service (nothing follows); `settling` starts it right after
  the push or the scale has landed. `lift=after-hold` points the Service
  back once the status has said why a write waits (held `LIFT_SETTLE_S`; or
  after `HOLD_REASON_WAIT_S` with nothing said; at idle after
  `LIFT_SETTLE_S`), `lift=after-window` keeps the webhook unreachable across
  `IDLE_HOLD_S` first. The verdict: admission was refused (a server-side
  dry-run patch of the ISVC, read every `INV_TICK_S`; still answering is
  `INFRA-FAIL`; a NotFound for an ISVC the operation deleted is an answer,
  not a refusal, and from then on a server-side dry-run apply of the
  case's manifest, the recreate's own create, is the probe); under a push
  or a scale the status said why in the
  webhook's own words — an event on the ISVC or one of its
  InferenceReplicas, a Component's rollout hold, a row's recorded failure
  or wait, or an ISVC condition naming the webhook (an operation that
  neither completes nor says why fails); no pod was created while
  admission refused (reported, since the deployed mutator decides it);
  nothing running left at idle or under a scale; the capacity floor held;
  the InferenceReplica rows followed every change of the pods in rotation
  within `STATUS_STALL_REQUEUES` of the controller's configured
  `lifecycle.requeue.operation` (never under `STATUS_STALL_MIN_S`); the
  controller's events on the ISVC and its InferenceReplicas stayed under
  `EVENT_RATE_CAP_PER_MIN` and its reconciles (the
  `controller_runtime_reconcile_total` counters of the OMENative
  controllers, read through the apiserver proxy of
  `OME_TEST_CTRL_METRICS_SERVICE`) under `RECONCILE_RATE_CAP_PER_MIN`, a
  rate judged over windows of `RATE_MIN_WINDOW_S` or more and reported
  otherwise; the webhook answered again once pointed back; then the push
  or the scale completed within the rotate budget with each pod slot
  created once (more is a double apply; the pods that appeared are counted
  against the slots the operation owns), the rows settled with no
  operation open, and no rollout hold, Held retry block or Failed row was
  left. An operation that completed under the outage, on writes admission
  does not gate (a delete's teardown), is read as such: "completed Ns into
  the outage; nothing for the lift to release", and it must still be done
  once the webhook is back. The generated `isvcs/gen-webhook/` family runs
  the steady state, the push (surge and drain-first) and a scale-up by one
  on every shape family of the coverage ledger. The outage is cluster-wide
  (every ISVC write is refused while it holds), so `batch.py` runs these
  cases alone.
- **`fault=conflict:idle|settling` with `form=once|persist` and
  `target=isvc|ir`** (the same modes and phases) runs a competing writer:
  a merge patch of the `perm.test/conflict-writer` annotation on the ISVC
  or on every InferenceReplica of the case, once at the phase, or every
  `1/OME_TEST_CONFLICT_RATE_HZ` seconds (default 2 per second) until the
  operation completes (at idle, across `IDLE_HOLD_S`), so the controller's
  next status write carries a stale resourceVersion and conflicts. The
  verdict is the window verdict above without the admission half: the
  writes were taken (none taken is `INFRA-FAIL`), nothing moved at idle,
  the rows followed the pods in rotation, the events stayed bounded (the
  reconcile counters are cluster-wide, so they are read on the webhook
  cases alone), the operation completed with each slot created once and
  nothing left held. The generated `isvcs/gen-conflict/` family crosses the
  form and the target with the same operations and shapes, except that the
  delete window runs against the InferenceReplicas alone: the controller
  removes the ISVC finalizer in the reconcile that sees the deletion, so a
  write on the ISVC as the delete lands has no window (the ids of those
  cells are kept empty, listed in `isvcs/MANIFEST.md`); the writer is per
  service, so these cases run in the concurrent pool.
- **A window at every hook.** Both window faults also open at
  `rotating` and `draining` under a push or a runtime edit
  (`mode=runtimerev`, `fault=<kind>:settling|rotating|draining`), as a
  rollback lands (`rollback=1 during=settling|rotating|draining|last`,
  `fault=<kind>:settling`), as a mid-roll edit lands
  (`edit=strategy:*|restartPolicy:* during=settling|rotating`), as a
  scale-down lands (`mode=scaledown` with a target below the count), at a
  moment of a move (`mode=migrate fault=<kind>:migrating at=accept|surge|drain`)
  and as a delete lands (`mode=recreate`). One handler reads them all with
  the window verdict above: the operation waits with a reason while the
  window holds and completes once it closes, each pod slot created once (a
  rollback: at most once in each direction; a scale-down creates nothing
  and may take down only the Instances it owns), the rows settled with
  nothing left held; a rollback ends on the starting revision, an edit is on
  the live spec, a move's ledger row reads Completed once with its
  annotation consumed and its Instance whole, and a delete comes back under
  the same name on fresh pods once the window has closed. A hook the
  operation passes before the window can open (the roll completes first)
  is `INFRA-FAIL`, never a pass. Under a canary (`mode=canary`,
  `fault=<kind>:canary-<state>[@step]`) the window opens at the grid state
  it names and the canary driver watches it beside the ladder: the webhook
  is pointed back by its lift rule, one competing write settles after
  `LIFT_SETTLE_S`, a writer that persists stops when the ladder ends; the
  window verdict is read with the ladder as the operation (each pod slot of
  the unit created once), and the ladder's step index must never go back
  while the window is open or after it (an outage holds the step where it
  is; a retried status write moves the step once). The generated
  `gen-webhook` and `gen-conflict` families open both windows at the second
  step's soak on the six shape families.
- **`fault=quota`, `fault=taint`, `fault=preempt`, `fault=webhook`, `via=preempt` and
  `pool=packed` cases run serially.** A quota case creates a *namespace-wide*
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
  kubectl -n $OME_TEST_NS delete pod -l perm.test/filler --ignore-not-found
  kubectl delete priorityclass -l perm.test/fault --ignore-not-found
  kubectl get nodes -o name | xargs -r -n1 kubectl taint nodes perm.test/pending:NoSchedule- 2>/dev/null
  # a webhook configuration left cut carries perm.test/webhook-outage with the service reference each webhook had;
  # batch.py's sweep restores it, or put the references back by hand
  kubectl get validatingwebhookconfiguration,mutatingwebhookconfiguration -o custom-columns='KIND:.kind,NAME:.metadata.name,OUTAGE:.metadata.annotations.perm\.test/webhook-outage'
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

Ceiling and floor: the ceiling is the header's `ceil_*` (in Instances); the
floor is `N - maxUnavailable`, read from the case body. A percent budget is
resolved the way the engine's per-Component budget resolves both budgets:
`N x percent / 100` rounded **up** and clamped to N, so `10%` of 4 is 1 and
`25%` of 8 is 2. The engine applies the same rounding to `maxSurge` and to
`maxUnavailable`; the Deployment convention of rounding `maxUnavailable`
down is not its rule, and a generator that states a percent case's ceiling
uses the same formula (`budget_count`). The generated `isvcs/gen-budget/`
family drives 10, 25 and 50 percent on both budgets at N=4 and N=8.

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
cohort ratio** (the informational PD observer under *PD knobs*), and the
four below (F, G, H, I). Two more readings, **L pairable-capacity** and
**M traffic-backed**, plus one extra kind each inside A and I, are advisory
for now unless a case's header turns them hard with `inv_hard=` (*Advisory
readings*, after I): they print a `⚠` line and never
move the verdict.

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

**I operation-progress** — every operation a status row holds has an owner
making progress. Four readings, each on every tick. (1) *A pod set behind
the operation*: a row that holds an operation (`Create`, `Update`,
`Restart`, `Migrate`, `Delete`) has a live pod of its Instance, or one
Pending, or one on its way out; a pod in a terminal phase (`Failed`,
`Succeeded`) is none of those. Sustained past `INV_I_NO_POD_S` (a minute,
scaled: a drain's replacement is one reconcile plus admission away) it
reads `invariant I violated (operation-without-pods): operation without a
pod set: engine/2 holds Restart with no live pod for 65s`. (2) *A pod
change inside the window*: an operation older than the Component's
`lifecycle.instanceReadyTimeout` — its own, else the deployed controller's
(`inferenceservice-config` lifecycle block, read once per case), else
`INV_I_WINDOW_DEFAULT_S` when neither names one — during which no pod of
the Instance (or of the surge replacement its operation names) was
created, deleted, changed phase, readiness, label or image, reads
`(operation-without-progress): operation without progress: engine/1 Update
open 70s with no pod change (window 60s)`; a deadline the engine restarted
from admission extends the window to it. Container restarts, the serving
gate and the engine's own clocks are not pod changes. (3) *The sweep on a
stopped node*: a Terminating pod on the node a node fault stopped, once the
force-delete policy is on and the pod is `overdueSlack` past its
deletionTimestamp and the node `nodeUnreachableThreshold` past reading
NotReady, is gone within `INV_I_SWEEP_S`; else `(terminating-outlives-
policy): terminating pod outlives the policy: engine-1 on worker-2 65s past
the boundary`. (4) *A hold names Instances that exist*: a persisted
`rolloutHold` whose reason names an Instance (`Instance 3 on revision …
not serving`) with no row and no pod, sustained past the settle window,
reads `(hold-names-gone-instance): hold names a gone Instance: engine hold
Held names 3`. Exempt, each because the case's own fault or operation
produces the shape: a row whose operation waits on an external authority
(`waiting` set: quota refused the create, nothing places the pod — on the
row or on its surge replacement's), a paused InferenceReplica (either pause
mode) and a settle after the pause lifts, a Component whose `rolloutPhase`
is `Paused` (a gate the operator or the ladder holds; reading 2 only), the
window in which the case's own fault has the manager down (a controller
restart until it is Ready again, bounded by its Ready budget, or a webhook
outage until lifted) plus a settle, and an Instance with a pod on the
stopped node while its kubelet is down (the node loss owns that wait;
reading 3 judges its Terminating pod). Reading 3 is also silent without
the policy (`needs=no-forcedelete`: the park is the fate) and once the
kubelet runs again. Limits: a foreign finalizer on a Terminating pod is
invisible to reading 3 (the policy cannot remove it either); a rollout
hold that names no Instance makes no claim; and the window of reading 2
is whatever the lane configures, so on a lane whose controller default is
long only Components that set their own `instanceReadyTimeout` are judged
inside a case. Waived and parked like the others (`inv_off=I`,
`inv_xfail=I`).

**Advisory readings (L, M, and one kind each in A and I)** — four
operator-level readings taken on every tick with the same sustain rules as
the classes above. They record like the others and print `⚠ invariant …
(advisory — reported, not failed)`, and they never move the verdict until a
full round has shown their noise level (`INV_ADVISORY` in `run.py` is the
switch; a reading that held is listed as `ⓘ invariant L … across N
tick(s)`). A case whose header names a class in `inv_hard=` (L, M or
both) fails on that class like on any other: its violations print `✗`, a
reading that held prints `✓ invariant L (pairable-capacity, hard)`, and
`inv_xfail=` scopes it like the rest.

**L pairable-capacity** — per Component, the Instances with a pod that is
alive, Ready, serving, in rotation (a Ready endpoint of a routed
per-revision Service, as B reads it) and pairable stay at or above the
capacity floor: N minus maxUnavailable, minus the Instances the case's own
fault broke and the header's `floor_slack`, the basis the capacity floor
uses (a scale moves it the same way). Pairable: no pairing-protocol
transition is in flight, or the pod's protocol label is empty, or some
serving pod of the other pairing Component (engine against decoder; the
router pairs with nothing) carries the same label or an empty one. A
Component arms on the first tick it stands at or above its floor, so the
service coming up is not a dip, and a dip then counts once it has lasted
`FLOOR_SUSTAIN_S`, like the floor. A reading: `invariant L
(pairable-capacity, advisory — reported, not failed): engine has 1
Instance(s) with a pairable pod in rotation, below the floor 2 (N 2 -
maxUnavailable 0)`. Limits: it reads the gate, the endpoints and the
labels, not the request path; without a routed Service it counts the gate
alone and says so; the ready-Instance floor and invariant C are unchanged.

**M traffic-backed** — on every tick, for every Component in the ISVC
status traffic list, every target with a weight above zero names a
revision with at least one alive, Ready, serving pod of that Component. It
is the canary handler's traffic-gap reading run on every handler and every
Component, sustained two ticks; the canary handler's own check is
unchanged. A reading: `invariant M (traffic-backed, advisory — reported,
not failed): engine: 10% of traffic names <isvc>-engine-rev-<hash> with no
alive, Ready, serving engine pod on it`.

**A serving-count-drift** — a row that publishes `servingPodCount` agrees
with the watcher's count of alive, Ready, serving pods of its Instance.
Rows in a transient phase are skipped like the rest of A, and the settle
window absorbs the controller's status write after a serving flip.

**I deadline-without-reason** — a row whose `operation.deadline` passed
more than a tick ago comes with a reason: a `waiting` token on the row or a
`rolloutHold` on the InferenceReplica. Read under the quiet windows I keeps
(manager down, webhook cut, a paused InferenceReplica, a rollout phase held
at a gate, a pod on the stopped node); a parked step (null deadline) has no
clock.

Two classes are opt-in, selected per case with `inv_on=J,K` (both, or one),
because their claim is a story's, not every case's: a node loss expects the
very record J forbids. They fire on a single tick (what they read is durable,
not a transition) and park and waive like the others (`inv_xfail=J`,
`inv_off=K`).

**J no-auto-recovery** — the service's audit ledger ConfigMap
(`<isvc>-ome-migration-audit`, key `history.json`) holds no entry whose
`reason` is `AutoRecover`: the record the engine writes when it gives up on
an Instance's node and steers the rebuild elsewhere, which also feeds the
node-exclusion memory. A case that selects J says nothing it does should
provoke that: a broken Instance in a packed pool is rebuilt where it stood.
The ledger is read once per tick; an absent ConfigMap is an empty ledger, an
unreadable one makes no observation. A violation reads `invariant J violated
(auto-recover): engine/2: the audit ledger records an AutoRecover entry
(fromNode=<node> revision=<hash> phase=Completed outcome=relocate-recreate)`.
Limits: it reads the ledger, not the pods; a relocation the engine decided
and never recorded is invisible to it (K reads the other half); and the
ledger ring-buffers its terminal rows, so a very churny service could age a
row out within a case.

**K no-node-exclusion** — no live pod of the service carries a required
`kubernetes.io/hostname NotIn` node affinity term. The engine writes one only
to keep an Instance's rebuild off a node it gave up on; a case that selects K
says no placement of its pods may be narrowed that way (in a packed pool
such a term would make the only room unusable). The reading is the pod spec
the watcher already carries (`excl` on every watcher record); a record
without the field is not evaluated. A violation reads `invariant K violated
(node-exclusion): engine/2 pod <name> carries a required kubernetes.io/hostname
NotIn [<node>] node affinity term`. Limits: a preferred (soft) term, an
`In` term that pins the pod elsewhere, and any other key are not exclusions
and pass; so does a term on a pod the watcher never saw alive.

## Regenerating cases

Four generators write into `isvcs/`; re-run one after editing it:
- `gen_cases.py` — the feature matrix (migration/restart/partition/scale/edge/canary),
  permuting topology × lifecycle × coordination.
- `gen_mn.py` — the multi-node (gang) rollout cases (`p66`–`p99`, `p500`+) →
  `isvcs/rollout/` (`p500`+ is the continuation block: `p100`+ belongs to
  `gen_cases.py`'s feature counters).
- `gen_runtime.py` — the runtime-triggered cases (`p400`+) → `isvcs/runtime-rev/`.
- `generate_cases.py` — the hot-core permutation corpus (`p1000`–`p2799`,
  `p3000`–`p4999`, `p5000`–`p5799`, `p5800`–`p5999` (continued at `p10800`–`p11099`), `p6000`–`p6199`, `p6200`–`p6399`, `p6800`–`p6999` and `p7400`–`p7599`) → `isvcs/gen-rollout/`, `isvcs/gen-fault/`,
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
  broken Instance; continued at `p11500`–`p11699` with the remaining
  stable-side breaks on the six shape families: a readiness loss kept or
  lifted, the key put back, a crash and a pull failure taken away again, one
  runner killed once, and the kept crash and pull failure on the gangs and
  the engine-behind-a-router shapes) and `isvcs/gen-canary-fault-new/` (the
  push brings a revision that crashes at start, crashes after Ready inside or
  past the first soak, or cannot be pulled; the ladder parks where it bites
  and a rollback or a fix push lands; then on the six shape families the
  missing key and the never-Ready revision under both exits, their one-shot
  forms and the pull failure that lifts, one runner killed once at start or
  after Ready, a taint armed before the push, and the kept crash and pull
  failure under both exits on the gangs and the engine-behind-a-router
  shapes), the deletion and the eviction under a canary
  once and again (`gen-canary-podkill`, the six shape families), what a step's
  gates read (`p9050`–`p9249`) →
  `isvcs/gen-canary-fault-gate/` (the health metric breaches on the stable or
  the canary side, once or for good; the analysis source becomes unreachable
  or answers garbage and comes back or never does; a sibling Component breaks
  once or for good on the running or the new side), the small fleets and the engine gang behind a router
  (`p8000`–`p8199`) → `isvcs/gen-size/` and `isvcs/gen-router-gang/`, the fault
  families (`p7000`–`p7399`) → `isvcs/gen-readiness/`, `isvcs/gen-gang-member/`,
  `isvcs/gen-tight/`, `isvcs/gen-evict/`, `isvcs/gen-preempt/` and `isvcs/gen-pending/` (continued at
  `p9500`–`p9799` and `p10700`–`p10799`), the packed pool
  (`p8600`–`p8699`) → `isvcs/gen-packed-pool/` (every node's spare CPU taken
  once the service has settled; a push whose revision crashes stops at one
  Instance down and the good push lands on that Instance first, in room the
  service's own pods vacated, then on the others one at a time; and the mirror, the running
  revision crashing on one Instance before the push), the percent budgets (`p8200`–`p8329`) →
  `isvcs/gen-budget/` (maxSurge and maxUnavailable at 10, 25 and 50 percent
  on fleets of 4 and 8, for a plain push, a push with a crash-looping
  Instance and a canary ladder; each case states the engine's rounding rule
  and the ceiling or floor it yields), and the strict in-place strategy
  (`p8330`–`p8399`) → `isvcs/gen-inplaceonly/` (an image change rolls in
  place with stable pod identities, an env, resources or probe change is
  refused on a single-pod Instance and rebuilt whole on a gang, and a crash
  loop under each is repaired or fixed the way the strategy allows), and the
  window faults (`p8900`–`p9049`, continued at `p10300`–`p10449` and
  `p10450`–`p10699`) → `isvcs/gen-webhook/` (the admission
  webhook unreachable on the settled service, as a push lands and as a
  scale-up lands, for a bounded time or across the hold window, and at the
  push's later hooks, as a rollback, a strategy edit, a scale-down, a
  runtime edit or a delete lands and as a move's request is recorded) and
  `isvcs/gen-conflict/` (a competing writer on the ISVC or the
  InferenceReplicas, once or until the operation completes, over the same
  operations and hooks), the
  faults crossed with a move and with a runtime edit (`p7800`–`p7999`) →
  `isvcs/gen-fault-move/` (a break, or the loss of the source node, at accept,
  at surge or at drain of a move, on the moved Instance, a peer or the decoder)
  and `isvcs/gen-fault-edit/` (a break of the running revision as the edit of
  the inherited runtime lands, and a bad edit followed by its fix), the
  crash that happens exactly once (`p8400`–`p8599`) → `isvcs/gen-crash-once/`
  (`once=1`: one kill of the runner, at start or after Ready, on the running
  revision at idle and under a push and on the pushed revision), the cause
  kept or lifted (`p9250`–`p9399`) → `isvcs/gen-lift/` (the config key kept
  away on the running revision or missing for the pushed one, the pushed
  revision's pod failing readiness for a while, the image that cannot be
  pulled for a while on either revision; the six ledger rows no other family
  filled, each on the six shape families; from p9322 the fix pushed after a
  key or a readiness loss at idle, with no pause around it, and the pull
  failure lifted again as the bump lands), and the pairing-protocol
  transition matrix (`p13000`–`p13999`) → `isvcs/gen-pairing/` (four
  prefill/decode shapes x five strategy pairs x the slow side x the surging
  sides' maxUnavailable x five sequences x grouped or not, the pairable
  floor hard on both sides and every Pairing hold bounded; see *The
  transition matrix* under *Pairing protocol*), plus
  `isvcs/MANIFEST.md` (per-dimension counts, the
  constraint ledger with every omitted cell and its reason, missing-hook
  catalog). The story families are canary lanes in CI: the pipeline gives
  every `gen-canary-*` folder the canary pod budget and Prometheus. Each
  `gen-canary-*` folder also carries the policy-referenced twins of a few
  of its rows (`CANARY_POLICY_TWINS`), minted after every other row of the
  folder (see *Canary*). The
  broken-Instance operations (`p7600`–`p7799`, continued at `p9400`–`p9499`
  and `p10100`–`p10299`) → `isvcs/gen-fault-scale/` (one Instance broken,
  then the engine scaled down by one, by two, to one, or up by one) and
  `isvcs/gen-fault-delete/` (the same breaks, then the service deleted and
  re-created under the same name). The repeating faults
  (`p8700`–`p8899`) → `isvcs/gen-repeat/` (a pod deleted by hand, a gang
  member lost, an eviction or a preemption applied again each time what it
  took has come back, and the manager deleted again each time it is Ready,
  at steady state, under a push and under a scale-up, the manager
  again under a rollback, a strategy edit, a runtime edit, a scale-down, a
  move, a delete and a canary from p8826, and the pod again under a
  rollback, a strategy edit, a runtime edit and a scale-down from p8868;
  the one-shot form of those moments sits with `isvcs/gen-fault/` from
  p1520 and `isvcs/gen-gang-member/` from p7172).

A generator owns exactly the case ids it mints. A gen-* folder whose id
block is full mints from a continuation block (`GEN_CONTINUATIONS` in
`generate_cases.py`, `p9400` and up); the folder owns the ids of both
blocks. Hand-written cases share the
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
cases) is likewise hand-written, with the `p800`+ block reserved; the
generated transition matrix lives in `isvcs/gen-pairing/` and skips the
cells those hand-written cases drive.

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
width, the rollout group kind, the canary steps). The operations with no
fault on the shapes the hand-written folders skip are generated as control
rows in the operation's own family (`gen-rollout`, `gen-rollback`,
`gen-switch`, `gen-fault-edit`, `gen-fault-delete`, `gen-fault-scale`,
`gen-fault-move`), so every (shape, operation, no fault) cell of the product
has a case; the single cells the shape axis left on the engine-behind-a-router
shapes (a node lost at steady state, the key put back, a pull failure at
steady state and under a push, a pod or a gang leader deleted under a push
and once at a canary step, a rollback at the held moment) are generated the
same way in the family that owns each verb. The ledger's last cells (a
router-only push parked by its own break, rolled back and edited at the held
moment and moved beside, on the engine+router shapes; the source node stopped
once at a moment of a move, the pushed revision's pull failure kept, and the
running revision's key kept away under a scale-up, on the six shapes) are
minted after every control row of every family, so no earlier id moves.

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
  its targets), `migrate`, `delete` (`mode=recreate`; with `during=idle` and
  a break either lands on a service with one broken Instance), `pause` / `freeze`
  (`pause=` under a kill, lifted by the harness), `canary`, `strategy or
  policy edit` (`edit=strategy:*` / `edit=restartPolicy:*`), `runtime edit`
  (`mode=runtimerev`). `admission reject` and `remove component` are counted
  but not crossed with the faults. A second operation landing on the first
  (a scale, pause or push at a canary hook; `during=rollout|migrate`) is
  the `operation+operation` overlap;
- **fault row** — (kind, form, side). `fault=crashloop:old` is `crash at
  start` / `crash after Ready` by `form=`, `imagepull:old` is `image pull`,
  `readiness:old` is `readiness never passes` on the running revision and
  `readiness:new` on the new one (one-shot under `form=recover`),
  `configkey:missing` is `config missing` on the running revision and
  `configkey:new` on the new one, `node:notready` and
  `cordon:source` are `node`, `quota:<n>` is `quota`, `ctrlrestart` is
  `controller restart`; `mode=kill`, `podkill` and `pkill=1` are `pod
  deleted by hand` on a single-pod service and `gang member lost` on a gang;
  a canary's or a stuck push's `img=` is `image pull` on the new revision,
  or `sibling broken` when `imgcomp=` names the decoder or the router (beside
  `edit=image` it is the image the edit lands, no fault), as is
  `imagepull:new` kept (`lift=never`); `break=stuck` is
  `readiness never passes`; a runtime-rev `expect=recover` is `image pull`
  on the new revision; `analysis=breach` is `metric breach` (one-shot when
  `health=breach` decays under `analysis=pass`), as is `fault=metric:*` (the
  side from `:new`, `:idle` or `victim=`, one-shot under `form=recover`);
  `analysis=inconclusive` and `fault=source:*` are `analysis source unusable`
  (one-shot under `lift=after-settle`). The form is `one-shot` when the
  harness takes the cause away again (`lift=`, `verb=quotalift`, a decaying
  or expiring breach, a break it heals) or the controller heals it once (a
  pod delete, a controller restart), else `persistent`; a pod delete, an
  eviction, a preemption or a controller restart with `repeat=<n>` is
  `persistent` (it keeps happening until the repetitions are spent). The
  side follows `victim=` (`canary` is the new side for a break too), the
  break's `old` victim, and run.py's component-blind podkill rule (the newest
  new-revision pod dies mid-rotation under a replacement-first strategy, a
  settled pod otherwise).
  A case with two faults counts in both rows. A fault at a moment of a move
  (`mode=migrate at=`) sits on the `migrating` timing, and `victim=sibling`
  there puts it on the decoder as a sibling break.

Cells the product does not need carry a written reason in `EXCLUDED_CELLS`,
and a cell counts under the first rule that matches: a steady state with no
fault asserts nothing; a single-pod Instance has no gang member to lose; on
a gang a hand-deleted pod is a lost member; an engine-only service has no
sibling Component to break; a pause or a freeze is a moment of another cell,
not an operation a fault crosses (the cell is proved by pausing while the
operation and the fault are in flight, or by the fault landing while paused,
the `paused` timing and the operation+operation overlap of the fault's own
cell), so the two columns keep only the kill family's pause-after-a-kill
cells; a fault on the new revision has no meaning under an operation that
creates no new revision (steady state); a metric breach and an unusable
analysis source are read by a canary step's analysis gate alone; a one-shot
fault on the new revision is over before a scale, a move or a delete can be
aimed at it, so only the persistent row stays under those (what the
operation meets is a plain roll, the operation+operation overlap); a
repeating node loss during a delete has no state left to lose after the
first loss (a pod deleted by hand, a lost gang member, an eviction or a
preemption repeats only once what it took has come back, and a delete
brings nothing back), so only the one-shot loss stays under a delete; a
stale revision record has no one-shot form (it is one wrong status entry
that stays until the controller rewrites it, not a cause that can be taken
away again), so only its persistent row stays. The
ledger prints the cells each reason removes; `--check` fails on a removed
cell that has a case and on a rule that removes nothing. The
report lists the empty cells biggest holes first (the fault rows,
operations and shape families with no case anywhere, then counts per axis
value, then every hole by name), crosses the operations and faults with the
group policy, update strategy, budget and restart policy, places every
event on its timing hook (`settling`, `rotating`, the canary states, `paused`,
...), tallies the overlaps and the canary step kinds, crosses the form a
canary group carries its ladder in (inline, or named through a
`RolloutPolicy`) with the folders and the step kinds, so a cell proved in
one form alone reads as empty in the other, and lists the parked cases with
their issues.

The operator stories of the marathon goal live in `STORIES` at the top of
the script, one predicate over a parsed case per story; the report prints the
cases and cells that prove each, and `--check` fails on a story with no
case. The ledger's *Pairing transitions* section is the pairing-protocol
transition matrix: shape x strategy pair x slow side x budget x sequence x
grouping (the axes, the removed cells and their reasons, and the floor each
side keeps live in `coverage.py`, and `generate_cases.py` mints the
`gen-pairing` family from the same product). A `mode=pairing` case on
single pods with no other knob is one cell, read off its header (`Ne`,
`Nd`, `proto=`, `proto2=`) and body (the two strategies and budgets, a
runner's `TEST_STARTUP_DELAY`, the starting protocol, the groups); any
other pairing case is listed outside the matrix with the reason. The
section prints the driven cells per shape and strategy pair for each
sequence, per axis value, the hand-written cases inside the matrix and
every empty cell; `--check` fails on a cell of the product with no case,
on a removed cell with one, and on a removal rule that removes nothing.
`run_test.py` runs `coverage.py --check` beside the generator checks,
so a header value the ledger cannot classify (a new verb, a new fault kind)
fails CI until it is mapped to an axis there. What no header can express is
listed under "Not derivable from the headers" in the report (the scheduler
lane, production profiles, the `last Instance` and `held floor` timings,
auto versus manual migration).

## Future knobs

Dimensions the generators do not permute yet. Each is a candidate axis
once its KIND spec settles; until then it stays hand-tested.

- `lifecycle.minReadySeconds` — the Ready-age window a rollout waits
  before draining or promoting past a new pod. The KIND gate covers the
  single-pod SurgeThenDrain shape (`minreadyseconds_kind_test.go`);
  gang surge, RecreatePod, and in-place under a window, and the
  cluster-wide `deploy.minReadySeconds` default, are not yet in the
  matrix. The ops-held crash-after-Ready gang rows and the fault-move
  crash-after-Ready single-pod stuck rows carry a fixed window as pacing
  (`OPS_HELD_CRASH_LOAD_SOAK_S`, `MOVE_STUCK_CRASH_LOAD_SOAK_S`), not as a
  permuted axis; the fault-move crash-after-Ready gang stuck rows carry no
  window and state the floor one Instance below N (`floor_slack=1`).
