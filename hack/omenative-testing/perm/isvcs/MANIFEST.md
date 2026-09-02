# Generated hot-core corpus

Emitted by `generate_cases.py` — regenerate with `python3 generate_cases.py`; CI rejects hand edits via `python3 generate_cases.py --check` (run_test.py).
The gen-* folders are generator-owned wholesale; promote a bug-finding case to a hand-owned folder under a fresh id instead of editing it here.

Total generated cases: **579**. The whole-corpus per-PR cap (hand + generated) is enforced by run_test.py, not baked in here.

## Folders

| folder | cases | id range |
|---|---|---|
| gen-rollout | 72 | p1000..p1071 |
| gen-fault | 320 | p1200..p1519 |
| gen-restart | 187 | p1600..p1786 |

### By topology

| value | cases |
|---|---|
| engine | 60 |
| engine-mn | 211 |
| pd | 93 |
| pd-mn | 215 |

### By update strategy

| value | cases |
|---|---|
| InPlaceIfPossible | 140 |
| InPlaceOnly | 140 |
| RecreatePod | 156 |
| SurgeThenDrain | 143 |

### By failure verb / phase

| value | cases |
|---|---|
| kill | 187 |
| none | 72 |
| podkill:rotating | 160 |
| podkill:settling | 160 |

### By restartPolicy

| value | cases |
|---|---|
| (unset — baseline) | 72 |
| None | 237 |
| RecreateInstance | 270 |

### By pause

| value | cases |
|---|---|
| freeze | 38 |
| off | 465 |
| true | 76 |

### Parked cells (XFAIL on a tracked issue)

| case | folder | issue |
|---|---|---|
| p1600 | gen-restart | #1354 |
| p1605 | gen-restart | #1354 |
| p1610 | gen-restart | #1354 |
| p1615 | gen-restart | #1354 |
| p1617 | gen-restart | #1354 |
| p1622 | gen-restart | #1354 |
| p1627 | gen-restart | #1354 |
| p1632 | gen-restart | #1354 |

## Constraint ledger (tuples excluded during construction)

- `pause` multiplies only `mode=kill`: run.py consumes the pause header nowhere else, so a paused rollout case would carry a dead knob.
- `pause=freeze` pairs only with `RecreateInstanceOnPodRestart`: the freeze lift downgrades to a plain pause, which parks policy None's Create recovery — freeze+None could never repair while paused.
- In-place strategies on single-pod engine-only topologies are excluded from gen-fault: the component-blind podkill victim is the in-place pod itself and its recreate trips the UID-stability check by harness design (a missing component-scoped victim hook, see below).
- `fault=podkill:migrating` needs `mode=migrate` — out of scope here.
- Kill targets (`kill=leader|worker`) exist only on gang topologies.
- Update budgets are dormant under a steady-state kill: the kill family permutes strategies at their canonical budget only.
- Kill-family cells pinned by hand-written restart/ cases are excluded:

  - p130: engine SurgeThenDrain rp=None
  - p131: engine SurgeThenDrain rp=RecreateInstance
  - p132: pd SurgeThenDrain rp=None
  - p133: pd SurgeThenDrain rp=RecreateInstance
  - p138: engine-mn W1 SurgeThenDrain rp=RecreateInstance target=leader
  - p139: engine-mn W1 SurgeThenDrain rp=RecreateInstance target=worker
  - p141: engine-mn W2 SurgeThenDrain rp=RecreateInstance target=worker
  - p148: engine SurgeThenDrain rp=RecreateInstance pause=true
  - p149: engine SurgeThenDrain rp=None pause=true
  - p150: engine-mn W1 SurgeThenDrain rp=RecreateInstance pause=true target=worker
  - p151: engine-mn W1 SurgeThenDrain rp=RecreateInstance pause=freeze target=worker
  - p152: pd SurgeThenDrain rp=RecreateInstance pause=true
  - p153: pd-mn W1 SurgeThenDrain rp=RecreateInstance pause=freeze target=leader

## Missing runner verbs/hooks (future coverage, not emitted)

- container-exit failure verb (in-container crash vs pod delete) — needs a fake-serving crash endpoint or exec-kill runner verb
- kill-instance verb (atomic delete of a whole gang) — today only reachable as the RecreateInstanceOnPodRestart cascade from one pod death
- component-scoped podkill victim (fault=podkill:<phase>:<component>) — victim selection is component-blind, so PD podkill can never target the engine at settling, and single-pod in-place engine victims are unreachable
- mid-surge injection points beyond first-surge-observed (k-th surge, mid-drain, pre-settle tail) — FAULT_PHASES has settling/rotating/migrating only
- pause-under-rollout hook (ome.io/rollout-paused is only exercised inside mode=kill; no verb pauses an in-flight rotation)
- decoder-targeted mode=kill (the kill handler always picks the engine when one is configured)

## Deferred fill

`--fill` (t=3 covering array over coordination groups, canary promotion, and budget interactions) is documented in the generator and intentionally not emitted until the hot core has baked in the per-PR lane.
