# OMENative test playground — coverage expansion

Status: **P1 in progress** (foundation + categorization done). 2026-06-01.

Expands the real-cluster OMENative harness (any OME cluster; kubeconfig +
namespace via `OME_TEST_KUBECONFIG` / `OME_TEST_NS`, default ns `sandbox`) from
96 rollout-only cases to cover the whole lifecycle surface:
migration, restart, partition/canary, scale-down, and edge interplay. Built on
the existing harness (`run.py` state machine, `batch.py` fan-out, `gen_mn.py`
generator). This folder is a clone of `2026-06-01-omenative-pd-playground/`;
that original stays the stable runner for regression re-runs.

## Layout: feature = folder, topology = tag

```
perm/isvcs/
  rollout/      # 96 existing cases (topology × lifecycle × coordination)
  migration/    # P2 — MigrationPolicy Auto/Surge/Never, ProgressDeadline, MaxAutoRetries
  restart/      # P3 — RestartPolicy None / RecreateInstance
  partition/    # P4 — RollingUpdate.Partition canary holds
  scale/        # P4 — scale-down, X-2 re-bump mid-roll
  edge/         # P4 — mixed-strategy groups, %-budgets, ReadyPolicy, in-place tuning
```

**Feature is the folder** (primary navigation axis). **Topology is an orthogonal
`topo=` header tag**, not a second folder level — the two compose (slice by
feature OR topology OR both), and small feature folders don't fragment into
near-empty topology subdirs. Four `topo` values:

| `topo`      | shape                                  |
|-------------|----------------------------------------|
| `engine`    | engine-only, single-pod                |
| `pd`        | PD (engine+decoder[+router]), single-pod |
| `engine-mn` | engine-only, multi-node gang           |
| `pd-mn`     | PD, multi-node gang                    |

Classification rule (from the case header): contains `PD` → `pd` base else
`engine`; contains `MN` → `-mn` suffix.

**Case-id (`pNN`) stays globally unique** = the result/log key, independent of
which folder a case lives in. `run.py:find_isvc()` resolves `isvcs/**/<id>.yaml`
at any depth (flat layout still works).

## Harness changes

- `run.py find_isvc()` — recursive case discovery. **Done.**
- `batch.py` — `--feature <folder>` and `--topo <tag>` filters (glob the feature
  folder; filter on the `topo=` header). Composable:
  `batch.py --feature migration --topo pd-mn`.
- `run.py` per-feature verb modes:
  - **rollout** (default) — bump spec → assert rollout converges within budget.
  - **migrate** — set the migration-request annotation → assert surge to a
    *different* node + source drain + ledger reaches `Completed` (+
    ProgressDeadline timeout / MaxAutoRetries paths).
  - **kill** — delete a pod → assert RestartPolicy behavior (None = no recreate;
    RecreateInstance = gang/instance recreated). `pause=true|freeze` layers the
    rollout-paused circuit breaker over the kill: `true` must repair
    RecreateInstance instances while still paused (and hold policy None until
    unpause); `freeze` must hold everything, then repair after a downgrade to
    `true` without ever unpausing. Cases p148–p153.
  - **partition** — set `RollingUpdate.Partition` → assert canary count holds,
    rest stay on old revision.
  - **scaledown** / **rebump** — shrink replicas; bump revision twice (X-2)
    mid-roll → assert convergence to the latest.

## Phasing

- **P1** — categorize existing 96 into `rollout/` (pure move, `topo=` stamp;
  **done**) + scale-up: default N 2→3–4, W 1→2, add a few N=6–8 gang cases.
- **P2** — migration mode + `migration/` cases. Closes the biggest gap: the
  gang migrate operation and the IR-path migration wiring get e2e coverage.
- **P3** — restart mode + `restart/` cases (pod-kill trigger).
- **P4** — `partition/`, `scale/`, `edge/`: Partition canary, scale-down, X-2
  re-bump, mixed-strategy groups, IntOrString %-budgets, ReadyPolicy,
  in-place `GracePeriodSeconds` / `MarkNotReadyDuringLifecycle`.

## Deferred

**Autoscaling** (HPA/KEDA convergence) — needs a GitOps change to enable the
prometheus deployment in `sandbox`. Parked until that lands.

## CRD surface this targets (from `pkg/apis/ome/v1beta1/lifecycle_types.go`)

UpdateStrategy {SurgeThenDrain, RecreatePod, InPlaceIfPossible, InPlaceOnly} ·
RollingUpdate {MaxSurge, MaxUnavailable IntOrString, **Partition**} ·
Coordination {none, RollingUpdate(+RatioBalanced/PerComponent), Sequential,
BlueGreen} · **RestartPolicy** {None, RecreateInstanceOnPodRestart} ·
**MigrationPolicy** {Mode Auto/Surge/Never,
ProgressDeadlineSeconds, MaxAutoRetries} · **ReadyPolicy** {AllPodReady, None} ·
InPlaceUpdateStrategy {GracePeriodSeconds, MarkNotReadyDuringLifecycle}.

## Harness `mode=` framework (implementation status)

`run.py` dispatches on a per-case `mode=` header. Default (`rollout`) is the
original surge-budget path, untouched. Non-rollout modes go through
`run_mode_case`, sharing the apply→wait-Ready→watch scaffolding (`start_watcher`,
`aggregate`, per-instance counters) with a mode-specific Phase-2 trigger + verdict.

- **scaledown** — ✅ done + validated (p97/p98). Patch `minReplicas`/`maxReplicas`
  to header `to_e/to_d/to_r`; assert drain to target with peak ≤ ceiling (no
  surge-up). Cases in `isvcs/scale/`.
- **migrate** — recipe below (next).
- **kill / partition / rebump** — record `MODE_TODO` until implemented (never a
  silent PASS).

## Migration mode — concrete recipe (P2)

Source of truth: `tests/integration/.../coordination_kind/migrate_kind_test.go`.
Migration is **annotation-triggered** (safe on the shared cluster — no node
cordoning).

**Trigger** — set on the ISVC's `.metadata.annotations`:
- key: `ome.io/migration-request-v1-<uuid>` (random uuid per request)
- value (JSON): `{"schemaVersion":"v1","component":"<engine|decoder|router>","instance":<idx>,"from_node":"<nodeName>","reason":"perm-test"}`
- `instance` = the instance-index to migrate; `from_node` = that instance's
  current node, found via `kubectl get pod -l ome.io/inferenceservice=<id>,component=<c>,ome.io/instance-index=<idx> -o jsonpath={.items[0].spec.nodeName}`.

**Asserts** (post-trigger):
1. **Ledger Completed** — ConfigMap `<isvc>-ome-migration-audit`, data key
   `history.json` = `{"entries":[{requestUUID, component, sourceInstance,
   surgeInstance, phase, fromNode, startedAt, completedAt, outcome}]}`. Assert an
   entry with `requestUUID==<uuid>` and `phase=="Completed"`.
2. **Surge-to-different-node** — the surge instance (`surgeInstance` index from
   the ledger) scheduled on a node ≠ `from_node`. (The cluster is multi-node, so
   the off-source surge can schedule.)
3. **Source drained** — the `instance` pod(s) are gone.
4. **Annotation cleared** — `ome.io/migration-request-v1-<uuid>` removed after
   Completed (the controller's accept/dedup contract).

Events `MigrationRequestAccepted` + `MigrationCompleted` also fire (optional
assert). Cases in `isvcs/migration/`: MigrationPolicy `Mode` Auto / Surge / Never,
plus `ProgressDeadlineSeconds` (timeout → Failed ledger) and `MaxAutoRetries`.
