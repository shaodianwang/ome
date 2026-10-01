# Generated hot-core corpus

Emitted by `generate_cases.py` — regenerate with `python3 generate_cases.py`; CI rejects hand edits via `python3 generate_cases.py --check` (run_test.py).
The gen-* folders are generator-owned wholesale; promote a bug-finding case to a hand-owned folder under a fresh id instead of editing it here.

Total generated cases: **1089**. The whole-corpus per-PR cap (hand + generated) is enforced by run_test.py, not baked in here.

## Folders

| folder | cases | id range |
|---|---|---|
| gen-rollout | 72 | p1000..p1071 |
| gen-fault | 320 | p1200..p1519 |
| gen-restart | 187 | p1600..p1786 |
| gen-crashloop | 216 | p2000..p2215 |
| gen-switch | 180 | p3800..p3979 |
| gen-crashloop-idle | 84 | p2400..p2483 |
| gen-crashloop-sibling | 30 | p2600..p2629 |

### By topology

| value | cases |
|---|---|
| engine | 138 |
| engine-mn | 289 |
| pd | 270 |
| pd-mn | 392 |

### By update strategy

| value | cases |
|---|---|
| InPlaceIfPossible | 224 |
| InPlaceOnly | 224 |
| RecreatePod | 246 |
| SurgeThenDrain | 395 |

### By failure verb / phase

| value | cases |
|---|---|
| crashloop:load | 108 |
| crashloop:load@idle | 28 |
| crashloop:load@sibling | 10 |
| crashloop:start | 108 |
| crashloop:start+edit:rotating | 12 |
| crashloop:start@idle | 56 |
| crashloop:start@sibling | 20 |
| edit:rotating | 84 |
| edit:settling | 84 |
| kill | 187 |
| none | 72 |
| podkill:rotating | 160 |
| podkill:settling | 160 |

### By restartPolicy

| value | cases |
|---|---|
| (unset — baseline) | 228 |
| None | 395 |
| RecreateInstance | 466 |

### By pause

| value | cases |
|---|---|
| freeze | 38 |
| off | 975 |
| true | 76 |

### By coordination

| value | cases |
|---|---|
| bluegreen | 24 |
| canary | 24 |
| none | 867 |
| rolling | 156 |
| seq | 18 |

### By mid-roll edit (gen-switch)

| value | cases |
|---|---|
| restartPolicy None to RecreateInstance at rotating | 6 |
| restartPolicy None to RecreateInstance at settling | 6 |
| restartPolicy RecreateInstance to None at rotating | 6 |
| restartPolicy RecreateInstance to None at settling | 6 |
| strategy InPlaceIfPossible to InPlaceOnly at rotating | 6 |
| strategy InPlaceIfPossible to InPlaceOnly at settling | 6 |
| strategy InPlaceIfPossible to RecreatePod at rotating | 6 |
| strategy InPlaceIfPossible to RecreatePod at settling | 6 |
| strategy InPlaceIfPossible to SurgeThenDrain at rotating | 6 |
| strategy InPlaceIfPossible to SurgeThenDrain at settling | 6 |
| strategy InPlaceOnly to InPlaceIfPossible at rotating | 6 |
| strategy InPlaceOnly to InPlaceIfPossible at settling | 6 |
| strategy InPlaceOnly to RecreatePod at rotating | 6 |
| strategy InPlaceOnly to RecreatePod at settling | 6 |
| strategy InPlaceOnly to SurgeThenDrain at rotating | 6 |
| strategy InPlaceOnly to SurgeThenDrain at settling | 6 |
| strategy RecreatePod to InPlaceIfPossible at rotating | 6 |
| strategy RecreatePod to InPlaceIfPossible at settling | 6 |
| strategy RecreatePod to InPlaceOnly at rotating | 6 |
| strategy RecreatePod to InPlaceOnly at settling | 6 |
| strategy RecreatePod to SurgeThenDrain at rotating | 6 |
| strategy RecreatePod to SurgeThenDrain at rotating while an Instance crashes | 6 |
| strategy RecreatePod to SurgeThenDrain at settling | 6 |
| strategy SurgeThenDrain to InPlaceIfPossible at rotating | 6 |
| strategy SurgeThenDrain to InPlaceIfPossible at settling | 6 |
| strategy SurgeThenDrain to InPlaceOnly at rotating | 6 |
| strategy SurgeThenDrain to InPlaceOnly at settling | 6 |
| strategy SurgeThenDrain to RecreatePod at rotating | 6 |
| strategy SurgeThenDrain to RecreatePod at rotating while an Instance crashes | 6 |
| strategy SurgeThenDrain to RecreatePod at settling | 6 |

### By break victim

| value | cases |
|---|---|
| (no break) | 759 |
| decoder | 60 |
| engine | 270 |

### Parked cells (XFAIL on a tracked issue)

| case | folder | issue |
|---|---|---|
| p2400 | gen-crashloop-idle | #1977 |
| p2401 | gen-crashloop-idle | #1977 |
| p2402 | gen-crashloop-idle | #1977 |
| p2403 | gen-crashloop-idle | #1977 |
| p2404 | gen-crashloop-idle | #1977 |
| p2405 | gen-crashloop-idle | #1977 |
| p2406 | gen-crashloop-idle | #1977 |
| p2407 | gen-crashloop-idle | #1977 |
| p2408 | gen-crashloop-idle | #1977 |
| p2409 | gen-crashloop-idle | #1977 |
| p2410 | gen-crashloop-idle | #1977 |
| p2411 | gen-crashloop-idle | #1977 |
| p2412 | gen-crashloop-idle | #1977 |
| p2413 | gen-crashloop-idle | #1977 |
| p2414 | gen-crashloop-idle | #1977 |
| p2415 | gen-crashloop-idle | #1977 |
| p2416 | gen-crashloop-idle | #1977 |
| p2417 | gen-crashloop-idle | #1977 |
| p2418 | gen-crashloop-idle | #1977 |
| p2419 | gen-crashloop-idle | #1977 |
| p2420 | gen-crashloop-idle | #1977 |
| p2421 | gen-crashloop-idle | #1977 |
| p2422 | gen-crashloop-idle | #1977 |
| p2423 | gen-crashloop-idle | #1977 |
| p2424 | gen-crashloop-idle | #1977 |
| p2425 | gen-crashloop-idle | #1977 |
| p2426 | gen-crashloop-idle | #1977 |
| p2427 | gen-crashloop-idle | #1977 |
| p2428 | gen-crashloop-idle | #1977 |
| p2429 | gen-crashloop-idle | #1977 |
| p2430 | gen-crashloop-idle | #1977 |
| p2431 | gen-crashloop-idle | #1977 |
| p2432 | gen-crashloop-idle | #1977 |
| p2433 | gen-crashloop-idle | #1977 |
| p2434 | gen-crashloop-idle | #1977 |
| p2435 | gen-crashloop-idle | #1977 |
| p2436 | gen-crashloop-idle | #1977 |
| p2437 | gen-crashloop-idle | #1977 |
| p2438 | gen-crashloop-idle | #1977 |
| p2439 | gen-crashloop-idle | #1977 |
| p2440 | gen-crashloop-idle | #1977 |
| p2441 | gen-crashloop-idle | #1977 |
| p2442 | gen-crashloop-idle | #1977 |
| p2443 | gen-crashloop-idle | #1977 |
| p2444 | gen-crashloop-idle | #1977 |
| p2445 | gen-crashloop-idle | #1977 |
| p2446 | gen-crashloop-idle | #1977 |
| p2447 | gen-crashloop-idle | #1977 |
| p2448 | gen-crashloop-idle | #1977 |
| p2449 | gen-crashloop-idle | #1977 |
| p2450 | gen-crashloop-idle | #1977 |
| p2451 | gen-crashloop-idle | #1977 |
| p2452 | gen-crashloop-idle | #1977 |
| p2453 | gen-crashloop-idle | #1977 |
| p2454 | gen-crashloop-idle | #1977 |
| p2455 | gen-crashloop-idle | #1977 |
| p2456 | gen-crashloop-idle | #1977 |
| p2457 | gen-crashloop-idle | #1977 |
| p2458 | gen-crashloop-idle | #1977 |
| p2459 | gen-crashloop-idle | #1977 |
| p2460 | gen-crashloop-idle | #1977 |
| p2461 | gen-crashloop-idle | #1977 |
| p2462 | gen-crashloop-idle | #1977 |
| p2463 | gen-crashloop-idle | #1977 |
| p2464 | gen-crashloop-idle | #1977 |
| p2465 | gen-crashloop-idle | #1977 |
| p2466 | gen-crashloop-idle | #1977 |
| p2467 | gen-crashloop-idle | #1977 |
| p2468 | gen-crashloop-idle | #1977 |
| p2469 | gen-crashloop-idle | #1977 |
| p2470 | gen-crashloop-idle | #1977 |
| p2471 | gen-crashloop-idle | #1977 |
| p2472 | gen-crashloop-idle | #1977 |
| p2473 | gen-crashloop-idle | #1977 |
| p2474 | gen-crashloop-idle | #1977 |
| p2475 | gen-crashloop-idle | #1977 |
| p2476 | gen-crashloop-idle | #1977 |
| p2477 | gen-crashloop-idle | #1977 |
| p2478 | gen-crashloop-idle | #1977 |
| p2479 | gen-crashloop-idle | #1977 |
| p2480 | gen-crashloop-idle | #1977 |
| p2481 | gen-crashloop-idle | #1977 |
| p2482 | gen-crashloop-idle | #1977 |
| p2483 | gen-crashloop-idle | #1977 |
| p2600 | gen-crashloop-sibling | #1977 |
| p2601 | gen-crashloop-sibling | #1977 |
| p2602 | gen-crashloop-sibling | #1977 |
| p2603 | gen-crashloop-sibling | #1977 |
| p2604 | gen-crashloop-sibling | #1977 |
| p2605 | gen-crashloop-sibling | #1977 |
| p2606 | gen-crashloop-sibling | #1977 |
| p2607 | gen-crashloop-sibling | #1977 |
| p2608 | gen-crashloop-sibling | #1977 |
| p2609 | gen-crashloop-sibling | #1977 |
| p2610 | gen-crashloop-sibling | #1977 |
| p2611 | gen-crashloop-sibling | #1977 |
| p2612 | gen-crashloop-sibling | #1977 |
| p2613 | gen-crashloop-sibling | #1977 |
| p2614 | gen-crashloop-sibling | #1977 |
| p2615 | gen-crashloop-sibling | #1977 |
| p2616 | gen-crashloop-sibling | #1977 |
| p2617 | gen-crashloop-sibling | #1977 |
| p2618 | gen-crashloop-sibling | #1977 |
| p2619 | gen-crashloop-sibling | #1977 |
| p2620 | gen-crashloop-sibling | #1977 |
| p2621 | gen-crashloop-sibling | #1977 |
| p2622 | gen-crashloop-sibling | #1977 |
| p2623 | gen-crashloop-sibling | #1977 |
| p2624 | gen-crashloop-sibling | #1977 |
| p2625 | gen-crashloop-sibling | #1977 |
| p2626 | gen-crashloop-sibling | #1977 |
| p2627 | gen-crashloop-sibling | #1977 |
| p2628 | gen-crashloop-sibling | #1977 |
| p2629 | gen-crashloop-sibling | #1977 |

## Constraint ledger (tuples excluded during construction)

- `pause` multiplies only `mode=kill`: run.py consumes the pause header nowhere else, so a paused rollout case would carry a dead knob.
- `pause=freeze` pairs only with `RecreateInstanceOnPodRestart`: the freeze lift downgrades to a plain pause, which parks policy None's Create recovery — freeze+None could never repair while paused.
- In-place strategies on single-pod engine-only topologies are excluded from gen-fault: the component-blind podkill victim is the in-place pod itself and its recreate trips the UID-stability check by harness design (a missing component-scoped victim hook, see below).
- `fault=podkill:migrating` needs `mode=migrate` — out of scope here.
- Kill targets (`kill=leader|worker`) exist only on gang topologies.
- Update budgets are dormant under a steady-state kill: the kill family permutes strategies at their canonical budget only.
- crashloop breaks an engine pod, so its coordination axis (co=none | co=rolling) exists only on the PD topologies; maxUnavailable=0 is emitted for SurgeThenDrain only, percent budgets run at the mid size where the ceil() rounding differs from the integer case, and both restart policies run under both crash forms.
- A mid-roll edit (gen-switch) is a merge patch of the lifecycle's type or policy, so the declared rollingUpdate budget must be valid under both strategies of a pair: (maxSurge 1, maxUnavailable 1) whenever SurgeThenDrain is on either side, (0, 1) otherwise. The header is stated for the worse of the two shapes: the ceiling is N+1 whenever a surge step can be in flight on either side of the edit, the floor N-1 is the one the drain-first side needs and stays hard. A strategy flip carries no restart policy; the restart-policy flips run under the canonical surge budget (floor N). The switch family runs one integer budget per pair and one gang width per topology, and its crashing variants pair the surge and recreate strategies only, at the rotating hook, with form=start.
- No strategy pair is skipped in gen-switch: the bump is an annotation diff, which both in-place variants apply, and a gang runs either variant as a whole-gang recreate, so a flip into InPlaceOnly has no diff it cannot apply.
- The steady-state family (`during=idle`) and the sibling family (`bump=engine victimcomp=decoder`) leave out `form=load` under `restartPolicy: None`: a runner that dies after Ready and comes back is a container restart the kubelet owns under that policy, not a wedge, so no repair is promised and neither family can read one.
- On the engine-only topologies a Sequential group as the generators express it (one single-Component blueGreen group per member) is one blueGreen group over the engine, so the steady-state family emits it as `co=bluegreen` only.
- The sibling family rolls the engine under the default strategy at its canonical budget: the sibling's break is independent of how the engine's own pods are replaced.
- The grouped cells of both families state the intended repair (`expect=repaired`; the engine converges and the decoder is repaired) and are parked, through `issue=`, on the issue that tracks it; the no-group cells run live.
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

- kill-instance verb (atomic delete of a whole gang) — today only reachable as the RecreateInstanceOnPodRestart cascade from one pod death
- component-scoped podkill victim (fault=podkill:<phase>:<component>) — victim selection is component-blind, so PD podkill can never target the engine at settling, and single-pod in-place engine victims are unreachable
- mid-surge injection points beyond first-surge-observed (k-th surge, mid-drain, pre-settle tail) — FAULT_PHASES has settling/rotating/migrating only
- pause-under-rollout hook (ome.io/rollout-paused is only exercised inside mode=kill; no verb pauses an in-flight rotation)
- decoder-targeted mode=kill (the kill handler always picks the engine when one is configured)

## Deferred fill

`--fill` (t=3 covering array over coordination groups, canary promotion, and budget interactions) is documented in the generator and intentionally not emitted until the hot core has baked in the per-PR lane.
