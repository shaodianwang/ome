# Generated hot-core corpus

Emitted by `generate_cases.py` — regenerate with `python3 generate_cases.py`; CI rejects hand edits via `python3 generate_cases.py --check` (run_test.py).
The gen-* folders are generator-owned wholesale; promote a bug-finding case to a hand-owned folder under a fresh id instead of editing it here.

Total generated cases: **1913**. The whole-corpus per-PR cap (hand + generated) is enforced by run_test.py, not baked in here.

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
| gen-heal | 32 | p3000..p3031 |
| gen-imagepull | 56 | p3400..p3455 |
| gen-node | 96 | p4200..p4295 |
| gen-ctrlrestart | 34 | p4600..p4633 |
| gen-canary-crash | 48 | p5200..p5247 |
| gen-canary-second-rev | 32 | p5250..p5281 |
| gen-canary-podkill | 16 | p5290..p5305 |
| gen-canary-fault-old | 188 | p6400..p6587 |
| gen-canary-fault-new | 52 | p6600..p6651 |
| gen-rollback | 126 | p5800..p5925 |
| gen-crashloop-new | 72 | p5000..p5071 |
| gen-crashloop-group | 72 | p5400..p5471 |

### By topology

| value | cases |
|---|---|
| engine | 424 |
| engine-mn | 383 |
| pd | 586 |
| pd-mn | 520 |

### By update strategy

| value | cases |
|---|---|
| (dormant: nothing rolls) | 40 |
| InPlaceIfPossible | 296 |
| InPlaceOnly | 236 |
| RecreatePod | 375 |
| SurgeThenDrain | 966 |

### By failure verb / phase

| value | cases |
|---|---|
| configkey:after-failed | 16 |
| configkey:after-settle | 16 |
| crashloop:load | 144 |
| crashloop:load@canary | 16 |
| crashloop:load@capacity | 16 |
| crashloop:load@gate | 4 |
| crashloop:load@idle | 28 |
| crashloop:load@prestart | 16 |
| crashloop:load@promote | 8 |
| crashloop:load@sibling | 10 |
| crashloop:load@traffic | 16 |
| crashloop:new:crashAfterSoak | 16 |
| crashloop:new:crashInSoak | 16 |
| crashloop:new:crashStart | 8 |
| crashloop:new:load | 36 |
| crashloop:new:start | 36 |
| crashloop:start | 144 |
| crashloop:start+edit:rotating | 12 |
| crashloop:start@canary | 16 |
| crashloop:start@capacity | 16 |
| crashloop:start@gate | 4 |
| crashloop:start@idle | 56 |
| crashloop:start@prestart | 16 |
| crashloop:start@promote | 8 |
| crashloop:start@sibling | 20 |
| crashloop:start@traffic | 16 |
| ctrlrestart:canary | 6 |
| ctrlrestart:drain | 8 |
| ctrlrestart:migration | 6 |
| ctrlrestart:scaledown | 6 |
| ctrlrestart:surge | 8 |
| edit:rotating | 84 |
| edit:settling | 84 |
| imagepull:idle | 8 |
| imagepull:new:imagePull | 12 |
| imagepull:settling | 48 |
| imagepull@analysis | 8 |
| imagepull@canary | 16 |
| imagepull@capacity | 16 |
| imagepull@gate | 4 |
| imagepull@prestart | 16 |
| imagepull@promote | 8 |
| imagepull@traffic | 16 |
| kill | 187 |
| node:parked | 32 |
| node:relocated | 32 |
| node:repaired | 32 |
| none | 72 |
| podkill:rotating | 160 |
| podkill:settling | 160 |
| podkill@gate | 4 |
| podkill@held | 4 |
| podkill@heldpaused | 4 |
| podkill@paused | 4 |
| rebump@canary-serving@0 | 8 |
| rebump@canary-staging | 8 |
| rollback:draining | 22 |
| rollback:held | 26 |
| rollback:last | 26 |
| rollback:rotating | 26 |
| rollback:settling | 26 |
| rtedit@canary-serving@0 | 8 |
| rtedit@canary-staging | 8 |

### By restartPolicy

| value | cases |
|---|---|
| (unset — baseline) | 388 |
| None | 743 |
| RecreateInstance | 782 |

### By pause

| value | cases |
|---|---|
| freeze | 38 |
| off | 1799 |
| true | 76 |

### By coordination

| value | cases |
|---|---|
| bluegreen | 48 |
| canary | 24 |
| none | 1448 |
| ratio | 16 |
| rolling | 343 |
| seq | 34 |

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
| (no break) | 1583 |
| decoder | 60 |
| engine | 270 |

### By migration mode (node family)

| value | cases |
|---|---|
| Auto | 48 |
| Never | 48 |

### By controller-restart condition

| value | cases |
|---|---|
| canary | 6 |
| drain | 8 |
| migration | 6 |
| scaledown | 6 |
| surge | 8 |

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
| p3002 | gen-heal | #1978 |
| p3003 | gen-heal | #1978 |
| p3006 | gen-heal | #1978 |
| p3007 | gen-heal | #1978 |
| p3010 | gen-heal | #1978 |
| p3011 | gen-heal | #1978 |
| p3014 | gen-heal | #1978 |
| p3015 | gen-heal | #1978 |
| p3018 | gen-heal | #1978 |
| p3019 | gen-heal | #1978 |
| p3022 | gen-heal | #1978 |
| p3023 | gen-heal | #1978 |
| p3026 | gen-heal | #1978 |
| p3027 | gen-heal | #1978 |
| p3030 | gen-heal | #1978 |
| p3031 | gen-heal | #1978 |

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
- heal breaks the engine runner, so its coordination axis is a rollingUpdate group over the engine alone or, where a decoder exists, over engine and decoder; nothing rolls, so the update strategy stays canonical and is not an axis. `lift=after-failed` carries `instanceReadyTimeout: 90s` on the victim so the row escalates inside the case budget.
- imagepull has one form (an image that never pulls has no after-Ready form) and runs each strategy at its canonical budget only; its idle half carries no group, since a grouped repair's hold is its own family.
- node:notready reads the fate of a Ready Instance on a dead node under the deployed force-delete policy, a lane dimension: without `lifecycle.forceDelete` (the chart default) nothing frees the pod names a silent node holds, so the outcomes are `parked` (nothing rebuilds it while the node is down) and `repaired` (the kubelet returns and the pods it never stopped serve again), both at `lift=after-settle`; with the policy the names are freed once the node's unreachable evidence is older than the threshold and the Instance is rebuilt elsewhere while the node is down, `relocated` at `lift=after-recover`. `needs=` pins parked to the plain lane and relocated to the force-delete lane (the runner SKIPs where the controller config differs); repaired holds under both. The migration axis is Auto (the default) and Never — the API has no mode that admits explicit requests but not relocation, and neither mode moves a Ready Instance (Auto relocates on an expired attempt's deadline, which a Ready Instance never has); restart policy and an idle rolling group change nothing either, and each cell says why. The update strategy is dormant at idle (one canonical budget).
- ctrlrestart has no hook inside a repair (one fault per case; the idle-break handler fires no second one), so a restart while a crash-loop repair is open is not emitted. The strategy axis is live for the two rollout conditions and dormant under a migration and a scale-down, where both strategies still run for the family's symmetry; a rolling group is crossed in on the PD rollout conditions only.
- The gen-canary-* families drive operator stories through a canary at small size: 8 single-pod Instances, SurgeThenDrain with the engine surge at 1 and 2, a four-step ladder (10/30/60/100 percent capacity and traffic) with a timed soak in place of an analysis gate, on the engine and prefill/decode shapes only (the stories run single-pod Instances; the gang shapes are the crashloop family's).
- gen-canary-crash breaks the stable side at the first split (canary-serving@0): every break kind and form, both restart policies, 1 and 3 victim Instances at once. The floor is read with the victims allowed for, not waived.
- gen-canary-second-rev lands the second revision as an ISVC re-bump or a runtime edit (a dedicated runtime per case), at staging and at the first split, with and without an analysis gate on every step before the cutover; the restart policy is None (a re-target repairs nothing).
- gen-canary-podkill deletes one old-revision engine pod at a manual gate, under the operator's pause, under a capacity gate held by an image that cannot start, and under that gate paused; the engine surge is 1 (the budget is dormant while the roll is held).
- gen-canary-fault-old breaks the stable side at the step kinds that differ in mechanism: before the push (the push meets a broken fleet), the capacity step ahead of traffic, the second step's timed soak, the same step at a manual gate (promoted 30s after the break), the same step at an analysis gate, and the last gate before the cutover. Left out, with the reason: the first split, which gen-canary-crash drives with the same faults, victims and policies; the cutover itself (Promoting), where at most the surge budget of stable Instances is left and already draining, so a break has no settled row to land on (the last gate carries that claim); a traffic step apart from its soak, since every traffic step of the story ladder but the cutover soaks; the surge budget at the gates, where no capacity builds while they hold (surge 1 only); three victims at the manual gate, whose storm is the first split's under another release; the crash forms under the analysis gate, since the metrics image is a static binary with no shell to exec a kill into and no fixture image fails to start under its entrypoint (the pull failure alone breaks it); the analysis gate crossed with the other step kinds, since the gate changes only what ends a hold, not how a break meets it; and a missing ConfigMap key, whose reference both revisions share, so a push is never its fix (the idle configkey family lifts the cause instead).
- gen-canary-fault-new arms the break on the pushed engine revision and crosses where it bites (a crash at start and a pull failure at staging; a crash after Ready inside the first soak and past it) with the exit at the park (a rollback, or a fix push that drops the broken runner), both shapes and both restart policies. Left out, with the reason: the surge budget where the ladder parks at its first capacity step, which asks one Instance (surge 1 only); the crash forms under the analysis gate (the metrics image carries no shell and no exit switch); the rollback exit under the analysis gate, whose terminal verdict requires an analysis sample a park before the first gate never yields (the fix exit walks the gated ladder); a missing ConfigMap key, which no verb arms on a template and whose bite is a container that never starts, the crash-at-start cell; and a manual policy, since a broken revision parks before any gate is reached.
- A rollback's `draining` hook is not emitted for an in-place strategy on a single-pod topology: an annotation-only diff is patched in place, so no pod of the starting revision ever drains and the hook never arrives (a gang falls back to a whole-gang recreate, which drains).
- crashloop:new arms the engine's new revision, so its group axis (co=none | co=rolling) exists only on the PD topologies; it runs each strategy at its canonical budget and leaves InPlaceOnly out: form=load's exit switch is a pod-template env diff in-place cannot carry (the InPlaceIfPossible cell documents the fallback) and form=start's image patch lands the same way under either in-place type.
- The group family runs a policy only where it has a peer: a one-member sequence is a blueGreen group and a ratio needs two Components, so engine-only runs blueGreen and the zero-budget rolling group alone; maintainRatio's tolerance is 50 at N=2 and 25 at N=4.
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

## gen-heal: which cells heal without the missing rule

The family asserts the intended behaviour, `expect=heals`, on every cell; the cells whose heal needs the exit a Failed row lacks are parked on #1978 and listed above.

- `lift=after-settle` (asserted): the key returns once the CreateContainerConfigError wait has held for the settle window, before any attempt escalates. Under `None` the crash-loop repair opens as soon as the wait is seen (the stuck-pod clock counts from the pod's creation and the victim is older than the grace), rebuilds the pod, and the rebuilt pod waits on the same key; under `RecreateInstanceOnPodRestart` the runner-restart repair does the same. Either way the kubelet starts the container once the key is back and the open repair promotes the Instance; where a group holds the crash-loop repair (`None` with `co=rolling`), the kubelet's retry on the untouched pod is the whole heal. The settle window (`LIFT_SETTLE_S`, 20 s before `OME_TEST_TIMEOUT_MULT`) has to stay inside the deployment's `stuckPodGracePeriod` (60 s in the chart): a multiplier of 3 or more turns these cells into the after-failed shape.
- `lift=after-failed` (parked on #1978): the repair keeps its operation when the row escalates, and a Failed row with an operation has no exit once its pods are runtime-ready, so the key's return starts the pods while the row stays Failed. The `None` x `co=rolling` cells additionally never escalate: the only repair a `None` policy opens is the crash-loop one, which the group holds while no run is open, so those four flip only when the grouped-repair hold is lifted as well.

## Missing runner verbs/hooks (future coverage, not emitted)

- kill-instance verb (atomic delete of a whole gang) — today only reachable as the RecreateInstanceOnPodRestart cascade from one pod death
- component-scoped podkill victim (fault=podkill:<phase>:<component>) — victim selection is component-blind, so PD podkill can never target the engine at settling, and single-pod in-place engine victims are unreachable
- mid-surge injection points beyond first-surge-observed (k-th surge, mid-drain, pre-settle tail) — FAULT_PHASES has settling/rotating/migrating only
- pause-under-rollout hook (ome.io/rollout-paused is only exercised inside mode=kill; no verb pauses an in-flight rotation)
- decoder-targeted mode=kill (the kill handler always picks the engine when one is configured)
- controller restart while a repair is open (a crash loop at idle with the manager deleted once the rebuild has started) — the runner fires one fault per case and the idle-break handler fires no second one

## Deferred fill

`--fill` (t=3 covering array over coordination groups, canary promotion, and budget interactions) is documented in the generator and intentionally not emitted until the hot core has baked in the per-PR lane.
