# Generated hot-core corpus

Emitted by `generate_cases.py` — regenerate with `python3 generate_cases.py`; CI rejects hand edits via `python3 generate_cases.py --check` (run_test.py).
The gen-* folders are generator-owned wholesale; promote a bug-finding case to a hand-owned folder under a fresh id instead of editing it here.

Total generated cases: **5087**. The whole-corpus per-PR cap (hand + generated) is enforced by run_test.py, not baked in here.

## Folders

| folder | cases | id range |
|---|---|---|
| gen-rollout | 72 | p1000..p1071 |
| gen-fault | 320 | p1200..p1519 |
| gen-restart | 187 | p1600..p1786 |
| gen-crashloop | 216 | p2000..p2215 |
| gen-switch | 180 | p3800..p3979 |
| gen-crashloop-idle | 114 | p2400..p2513 |
| gen-crashloop-sibling | 36 | p2600..p2635 |
| gen-heal | 32 | p3000..p3031 |
| gen-imagepull | 56 | p3400..p3455 |
| gen-node | 96 | p4200..p4295 |
| gen-ctrlrestart | 68 | p4600..p4667 |
| gen-canary-crash | 48 | p5200..p5247 |
| gen-canary-second-rev | 32 | p5250..p5281 |
| gen-canary-podkill | 16 | p5290..p5305 |
| gen-canary-fault-old | 188 | p6400..p6587 |
| gen-canary-fault-new | 52 | p6600..p6651 |
| gen-canary-fault-gate | 173 | p9050..p9222 |
| gen-rollback | 126 | p5800..p5925 |
| gen-crashloop-new | 72 | p5000..p5071 |
| gen-crashloop-group | 72 | p5400..p5471 |
| gen-pause-repair | 180 | p6000..p6179 |
| gen-size | 86 | p8000..p8085 |
| gen-router-gang | 30 | p8100..p8129 |
| gen-ops-held | 112 | p6800..p10011 |
| gen-runtime-edit-repair | 84 | p6900..p6983 |
| gen-migrate-bump | 154 | p6200..p6353 |
| gen-readiness | 60 | p7000..p7059 |
| gen-gang-member | 72 | p7100..p7171 |
| gen-tight | 28 | p7300..p7327 |
| gen-evict | 102 | p7400..p9561 |
| gen-preempt | 102 | p7440..p9641 |
| gen-pending | 210 | p7500..p10709 |
| gen-budget | 114 | p8200..p8313 |
| gen-inplaceonly | 45 | p8330..p8374 |
| gen-fault-scale | 292 | p7600..p10191 |
| gen-fault-delete | 118 | p7700..p10217 |
| gen-fault-move | 168 | p7800..p9867 |
| gen-fault-edit | 128 | p7900..p9927 |
| gen-packed-pool | 96 | p8600..p8695 |
| gen-repeat | 126 | p8700..p8825 |
| gen-webhook | 168 | p8900..p10407 |
| gen-conflict | 264 | p8960..p10623 |
| gen-crash-once | 120 | p8400..p8519 |
| gen-lift | 72 | p9250..p9321 |

### By topology

| value | cases |
|---|---|
| engine | 1466 |
| engine-mn | 1317 |
| pd | 1287 |
| pd-mn | 1017 |

### By update strategy

| value | cases |
|---|---|
| (dormant: nothing rolls) | 750 |
| InPlaceIfPossible | 458 |
| InPlaceOnly | 281 |
| RecreatePod | 1021 |
| SurgeThenDrain | 2577 |

### By failure verb / phase

| value | cases |
|---|---|
| bad-edit:imagepull | 12 |
| bad-edit:readiness | 12 |
| bump-at-accept | 26 |
| bump-at-drain | 26 |
| bump-at-surge | 26 |
| canary | 30 |
| canary:auto | 6 |
| canary:manual | 4 |
| configkey:after-failed | 16 |
| configkey:after-settle | 16 |
| configkey:never | 12 |
| configkey:new:after-settle | 12 |
| configkey:new:never | 12 |
| configkey:once@delete | 6 |
| configkey:once@down1 | 6 |
| configkey:once@up1 | 6 |
| configkey@delete | 6 |
| configkey@down1 | 6 |
| conflict:draining | 24 |
| conflict:idle | 24 |
| conflict:migrating | 24 |
| conflict:rotating | 24 |
| conflict:settling | 168 |
| cordon:after-hold | 8 |
| cordon:never | 8 |
| crashloop:load | 212 |
| crashloop:load:decoder:new:once@last | 2 |
| crashloop:load:decoder:new:once@mid | 3 |
| crashloop:load:decoder:once@idle | 2 |
| crashloop:load:decoder:once@last | 2 |
| crashloop:load:decoder:once@mid | 4 |
| crashloop:load:decoder@idle | 1 |
| crashloop:load:decoder@last | 1 |
| crashloop:load:decoder@mid | 2 |
| crashloop:load:once@delete | 6 |
| crashloop:load:once@down1 | 6 |
| crashloop:load:once@up1 | 6 |
| crashloop:load:router:new:once@last | 2 |
| crashloop:load:router:new:once@mid | 3 |
| crashloop:load:router:once@idle | 2 |
| crashloop:load:router:once@last | 2 |
| crashloop:load:router:once@mid | 4 |
| crashloop:load:router@idle | 1 |
| crashloop:load:router@last | 1 |
| crashloop:load:router@mid | 2 |
| crashloop:load@canary | 16 |
| crashloop:load@capacity | 16 |
| crashloop:load@delete | 16 |
| crashloop:load@down1 | 16 |
| crashloop:load@down2 | 8 |
| crashloop:load@edit | 52 |
| crashloop:load@gate | 4 |
| crashloop:load@idle | 101 |
| crashloop:load@move-accept | 16 |
| crashloop:load@move-drain | 16 |
| crashloop:load@move-surge | 16 |
| crashloop:load@prestart | 16 |
| crashloop:load@promote | 8 |
| crashloop:load@sibling | 12 |
| crashloop:load@toone | 8 |
| crashloop:load@traffic | 16 |
| crashloop:load@up1 | 16 |
| crashloop:new:crashAfterSoak | 16 |
| crashloop:new:crashInSoak | 16 |
| crashloop:new:crashStart | 8 |
| crashloop:new:decoder:crashAfterSoak | 2 |
| crashloop:new:decoder:crashInSoak | 2 |
| crashloop:new:decoder:crashStart | 2 |
| crashloop:new:load | 36 |
| crashloop:new:start | 36 |
| crashloop:start | 266 |
| crashloop:start+edit:rotating | 12 |
| crashloop:start+env@held | 4 |
| crashloop:start+env@recreate | 2 |
| crashloop:start+image | 6 |
| crashloop:start:decoder@idle | 1 |
| crashloop:start:decoder@last | 1 |
| crashloop:start:decoder@mid | 2 |
| crashloop:start:once@delete | 10 |
| crashloop:start:once@down1 | 10 |
| crashloop:start:once@up1 | 10 |
| crashloop:start:router@idle | 1 |
| crashloop:start:router@last | 1 |
| crashloop:start:router@mid | 2 |
| crashloop:start@canary | 16 |
| crashloop:start@capacity | 16 |
| crashloop:start@delete | 20 |
| crashloop:start@down1 | 20 |
| crashloop:start@down2 | 8 |
| crashloop:start@edit | 26 |
| crashloop:start@gate | 4 |
| crashloop:start@idle | 202 |
| crashloop:start@move-accept | 16 |
| crashloop:start@move-drain | 16 |
| crashloop:start@move-surge | 16 |
| crashloop:start@prestart | 16 |
| crashloop:start@promote | 8 |
| crashloop:start@sibling | 24 |
| crashloop:start@toone | 8 |
| crashloop:start@traffic | 16 |
| crashloop:start@up1 | 20 |
| crashonce:new:load | 24 |
| crashonce:new:start | 24 |
| crashonce:old:load | 36 |
| crashonce:old:start | 36 |
| ctrlrestart:canary | 12 |
| ctrlrestart:drain | 16 |
| ctrlrestart:migration | 12 |
| ctrlrestart:scaledown | 12 |
| ctrlrestart:surge | 16 |
| edit:rotating | 84 |
| edit:settling | 84 |
| env@held | 4 |
| env@recreate | 2 |
| evict:delete | 6 |
| evict:edit | 12 |
| evict:rollback | 12 |
| evict:rotating | 24 |
| evict:runtime | 12 |
| evict:scaledown | 12 |
| evict:scaleup | 6 |
| evict:steady | 18 |
| image | 6 |
| imagepull:decoder:once@idle | 1 |
| imagepull:decoder:once@last | 1 |
| imagepull:decoder:once@mid | 2 |
| imagepull:decoder@idle | 1 |
| imagepull:decoder@last | 1 |
| imagepull:decoder@mid | 2 |
| imagepull:idle | 8 |
| imagepull:idle:after-settle | 12 |
| imagepull:new:after-settle | 12 |
| imagepull:new:decoder:imagePull | 4 |
| imagepull:new:imagePull | 12 |
| imagepull:new:router:parked | 3 |
| imagepull:once@delete | 6 |
| imagepull:once@down1 | 6 |
| imagepull:once@up1 | 6 |
| imagepull:router:once@idle | 1 |
| imagepull:router:once@last | 1 |
| imagepull:router:once@mid | 2 |
| imagepull:router@idle | 1 |
| imagepull:router@last | 1 |
| imagepull:router@mid | 2 |
| imagepull:settling | 48 |
| imagepull@analysis | 8 |
| imagepull@canary | 16 |
| imagepull@capacity | 16 |
| imagepull@delete | 16 |
| imagepull@down1 | 16 |
| imagepull@down2 | 8 |
| imagepull@edit | 26 |
| imagepull@gate | 4 |
| imagepull@move-accept | 16 |
| imagepull@move-drain | 16 |
| imagepull@move-surge | 16 |
| imagepull@prestart | 16 |
| imagepull@promote | 8 |
| imagepull@toone | 8 |
| imagepull@traffic | 16 |
| imagepull@up1 | 16 |
| kill | 187 |
| metric:canary:persist@last | 3 |
| metric:canary:persist@mid | 6 |
| metric:canary:persist@new | 3 |
| metric:canary:recover@last | 3 |
| metric:canary:recover@mid | 6 |
| metric:canary:recover@new | 3 |
| metric:stable:persist@idle | 3 |
| metric:stable:persist@last | 3 |
| metric:stable:persist@mid | 6 |
| metric:stable:recover@idle | 3 |
| metric:stable:recover@last | 3 |
| metric:stable:recover@mid | 6 |
| move-at-last | 24 |
| move-at-rotating | 26 |
| move-at-settling | 26 |
| node:parked | 32 |
| node:relocated | 32 |
| node:repaired | 32 |
| node@move-accept | 12 |
| node@move-surge | 12 |
| none | 140 |
| packed:idle:load | 16 |
| packed:idle:start | 16 |
| packed:new:load | 32 |
| packed:new:start | 32 |
| pending:quota-kept@down1 | 6 |
| pending:quota:edit | 12 |
| pending:quota:roll | 42 |
| pending:quota:rollback | 12 |
| pending:quota:runtime | 12 |
| pending:quota:steady | 24 |
| pending:quota@delete | 6 |
| pending:quota@down1 | 6 |
| pending:quota@up1 | 6 |
| pending:taint:edit | 12 |
| pending:taint:roll | 48 |
| pending:taint:rollback | 12 |
| pending:taint:runtime | 12 |
| pending:taint:steady | 24 |
| pending:taint@delete | 12 |
| pending:taint@down1 | 12 |
| pending:taint@up1 | 12 |
| podkill:draining:leader | 12 |
| podkill:draining:worker | 12 |
| podkill:rotating | 160 |
| podkill:rotating:leader | 12 |
| podkill:rotating:worker | 12 |
| podkill:settling | 160 |
| podkill:settling:leader | 12 |
| podkill:settling:worker | 12 |
| podkill@gate | 4 |
| podkill@held | 4 |
| podkill@heldpaused | 4 |
| podkill@paused | 4 |
| preempt:edit | 6 |
| preempt:rollback | 6 |
| preempt:rotating | 48 |
| preempt:runtime | 6 |
| preempt:scaledown | 6 |
| preempt:scaleup | 6 |
| preempt:steady | 24 |
| probe@held | 4 |
| probe@recreate | 2 |
| push:failed | 56 |
| push:held | 56 |
| quota:after-hold | 4 |
| quota:completes | 4 |
| quota:never | 4 |
| readiness:new | 36 |
| readiness:new:recover | 12 |
| readiness:old:persist | 12 |
| readiness:old:recover | 12 |
| readiness:once@delete | 6 |
| readiness:once@down1 | 6 |
| readiness:once@up1 | 6 |
| readiness@delete | 8 |
| readiness@down1 | 8 |
| readiness@down2 | 4 |
| readiness@toone | 4 |
| readiness@up1 | 8 |
| rebump@canary-serving@0 | 8 |
| rebump@canary-staging | 8 |
| repeat:ctrlrestart:push | 12 |
| repeat:ctrlrestart:scale-up | 6 |
| repeat:ctrlrestart:steady | 6 |
| repeat:evict:push | 12 |
| repeat:evict:scale-up | 6 |
| repeat:evict:steady | 18 |
| repeat:podkill:push | 24 |
| repeat:podkill:scale-up | 18 |
| repeat:podkill:steady | 18 |
| repeat:preempt:steady | 6 |
| resources@held | 4 |
| resources@recreate | 2 |
| rollback:draining | 22 |
| rollback:held | 26 |
| rollback:last | 26 |
| rollback:rotating | 26 |
| rollback:settling | 26 |
| rtedit@canary-serving@0 | 8 |
| rtedit@canary-staging | 8 |
| scale:down | 8 |
| scale:up | 8 |
| source:garbage:after-settle@mid | 6 |
| source:garbage:never@mid | 6 |
| source:unreachable:after-settle@idle | 6 |
| source:unreachable:after-settle@last | 6 |
| source:unreachable:after-settle@mid | 9 |
| source:unreachable:never@idle | 6 |
| source:unreachable:never@last | 6 |
| source:unreachable:never@mid | 9 |
| webhook:draining | 24 |
| webhook:idle | 12 |
| webhook:migrating | 12 |
| webhook:rotating | 24 |
| webhook:settling | 96 |

### By restartPolicy

| value | cases |
|---|---|
| (unset — baseline) | 1458 |
| None | 2041 |
| RecreateInstance | 1588 |

### By pause

| value | cases |
|---|---|
| freeze | 110 |
| off | 4793 |
| true | 184 |

### By pause moment

| value | cases |
|---|---|
| (no pause around a repair) | 4907 |
| after | 36 |
| before | 72 |
| during | 72 |

### By push while paused

| value | cases |
|---|---|
| (no pause around a repair) | 4907 |
| fix | 90 |
| none | 90 |

### By coordination

| value | cases |
|---|---|
| bluegreen | 54 |
| canary | 30 |
| none | 4325 |
| ratio | 16 |
| rolling | 628 |
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
| (no break) | 3672 |
| decoder | 106 |
| engine | 1243 |
| router | 66 |

### By migration mode (node family)

| value | cases |
|---|---|
| Auto | 48 |
| Never | 48 |

### By controller-restart condition

| value | cases |
|---|---|
| canary | 12 |
| drain | 16 |
| migration | 12 |
| scaledown | 12 |
| surge | 16 |

### By Instances per Component (size and router-gang families)

| value | cases |
|---|---|
| 1 | 40 |
| 2 | 76 |

### By stuck kind (ops-held)

| value | cases |
|---|---|
| failed | 56 |
| held | 56 |

### By operation on the stuck Component (ops-held)

| value | cases |
|---|---|
| delete | 192 |
| down1 | 124 |
| down2 | 28 |
| edit | 78 |
| move | 36 |
| push | 144 |
| rollback | 78 |
| runtime | 78 |
| scale | 36 |
| scale-down | 32 |
| scale-one | 16 |
| scale-up | 32 |
| scaledown | 54 |
| scaleup | 12 |
| steady | 36 |
| toone | 28 |
| up1 | 112 |

### By overlap order (runtime-edit-repair)

| value | cases |
|---|---|
| crash-during-edit | 48 |
| edit-during-repair | 36 |

### By runtime edit (runtime-edit-repair and fault-edit families)

| value | cases |
|---|---|
| env | 42 |
| image | 42 |

### By lift (preempt, pending and webhook families)

| value | cases |
|---|---|
| after-hold | 84 |
| after-settle | 216 |
| after-window | 84 |
| never | 162 |

### By window operation (webhook and conflict families)

| value | cases |
|---|---|
| conflict delete | 24 |
| conflict edit | 24 |
| conflict move | 24 |
| conflict push | 24 |
| conflict push draining | 24 |
| conflict push rotating | 24 |
| conflict rollback | 24 |
| conflict runtime | 24 |
| conflict scale | 24 |
| conflict scaledown | 24 |
| conflict steady | 24 |
| webhook delete | 12 |
| webhook edit | 12 |
| webhook move | 12 |
| webhook push | 24 |
| webhook push draining | 24 |
| webhook push rotating | 24 |
| webhook rollback | 12 |
| webhook runtime | 12 |
| webhook scale | 12 |
| webhook scaledown | 12 |
| webhook steady | 12 |

### By operation under a pre-armed cause or a hook fault (pending, evict and preempt families)

| value | cases |
|---|---|
| evict delete | 6 |
| evict edit | 12 |
| evict rollback | 12 |
| evict runtime | 12 |
| evict scaledown | 12 |
| evict scaleup | 6 |
| pending edit | 24 |
| pending rollback | 24 |
| pending runtime | 24 |
| preempt edit | 6 |
| preempt rollback | 6 |
| preempt runtime | 6 |
| preempt scaledown | 6 |
| preempt scaleup | 6 |

### By competing-writer form (conflict)

| value | cases |
|---|---|
| once | 132 |
| persist | 132 |

### By competing-writer target (conflict)

| value | cases |
|---|---|
| ir | 132 |
| isvc | 132 |

### By percent budget (gen-budget)

| value | cases |
|---|---|
| maxSurge 10% | 19 |
| maxSurge 25% | 19 |
| maxSurge 50% | 19 |
| maxUnavailable 10% | 19 |
| maxUnavailable 25% | 19 |
| maxUnavailable 50% | 19 |

### By operation on a service with a broken Instance

| value | cases |
|---|---|
| delete | 192 |
| down1 | 124 |
| down2 | 28 |
| edit | 78 |
| move | 36 |
| push | 144 |
| rollback | 78 |
| runtime | 78 |
| scale | 36 |
| scale-down | 32 |
| scale-one | 16 |
| scale-up | 32 |
| scaledown | 54 |
| scaleup | 12 |
| steady | 36 |
| toone | 28 |
| up1 | 112 |

### By one-shot crash (gen-crash-once)

| value | cases |
|---|---|
| load on the pushed revision under a push | 24 |
| load on the running revision at idle | 12 |
| load on the running revision under a push | 24 |
| start on the pushed revision under a push | 24 |
| start on the running revision at idle | 12 |
| start on the running revision under a push | 24 |

### By ledger fault row (gen-lift)

| value | cases |
|---|---|
| config missing, one-shot, new | 12 |
| config missing, persistent, new | 12 |
| config missing, persistent, running | 12 |
| image pull, one-shot, new | 12 |
| image pull, one-shot, running | 12 |
| readiness never passes, one-shot, new | 12 |

### By move moment (fault-move)

| value | cases |
|---|---|
| accept | 60 |
| drain | 48 |
| surge | 60 |

### By move victim (fault-move)

| value | cases |
|---|---|
| moved | 78 |
| peer | 54 |
| sibling | 36 |

### By runtime edit (fault-edit)

| value | cases |
|---|---|
| env | 24 |
| image | 104 |

### By story (packed-pool)

| value | cases |
|---|---|
| bad push then fix | 64 |
| broken running then fix | 32 |

### By repeating fault and operation (gen-repeat)

| value | cases |
|---|---|
| ctrlrestart x3 at steady state | 6 |
| ctrlrestart x3 under a push | 12 |
| ctrlrestart x3 under a scale-up | 6 |
| evict x3 at steady state | 18 |
| evict x3 under a push | 12 |
| evict x3 under a scale-up | 6 |
| podkill x3 at steady state | 18 |
| podkill x3 under a push | 24 |
| podkill x3 under a scale-up | 18 |
| preempt x2 at steady state | 6 |

### By ledger fault row (canary-fault-gate)

| value | cases |
|---|---|
| analysis source unusable, one-shot | 27 |
| analysis source unusable, persistent | 27 |
| metric breach, one-shot, new | 12 |
| metric breach, one-shot, running | 12 |
| metric breach, persistent, new | 12 |
| metric breach, persistent, running | 12 |
| sibling broken, one-shot, new | 10 |
| sibling broken, one-shot, running | 24 |
| sibling broken, persistent, new | 13 |
| sibling broken, persistent, running | 24 |

### By shape family (canary-fault-gate)

| value | cases |
|---|---|
| engine gang | 8 |
| engine single | 26 |
| engine+decoder+router gang | 17 |
| engine+decoder+router single | 57 |
| engine+router gang | 16 |
| engine+router single | 49 |

## Constraint ledger (tuples excluded during construction)

- `pause` multiplies `mode=kill` and the pause-and-repair family (`pauseat=`); run.py consumes the pause header nowhere else, and a rollout case that pauses without `pauseat=` is rejected as a dead knob.
- `pause=freeze` pairs only with `RecreateInstanceOnPodRestart`: the freeze lift downgrades to a plain pause, which parks policy None's Create recovery — freeze+None could never repair while paused.
- In-place strategies on single-pod engine-only topologies are excluded from gen-fault: the component-blind podkill victim is the in-place pod itself and its recreate trips the UID-stability check by harness design (a missing component-scoped victim hook, see below).
- `fault=podkill:migrating` needs `mode=migrate` — out of scope here.
- A router in front of an engine fleet is a break victim under a canary alone: elsewhere the runner admits engine and decoder pods, so the sibling rows of the engine-behind-a-router shapes outside a canary (a move, a runtime edit, a push, the idle service) have no cell and the router is a negative control there.
- Kill targets (`kill=leader|worker`) exist only on gang topologies.
- Update budgets are dormant under a steady-state kill: the kill family permutes strategies at their canonical budget only.
- crashloop breaks an engine pod, so its coordination axis (co=none | co=rolling) exists only on the PD topologies; maxUnavailable=0 is emitted for SurgeThenDrain only, percent budgets run at the mid size where the ceil() rounding differs from the integer case, and both restart policies run under both crash forms.
- A mid-roll edit (gen-switch) is a merge patch of the lifecycle's type or policy, so the declared rollingUpdate budget must be valid under both strategies of a pair: (maxSurge 1, maxUnavailable 1) whenever SurgeThenDrain is on either side, (0, 1) otherwise. The header is stated for the worse of the two shapes: the ceiling is N+1 whenever a surge step can be in flight on either side of the edit, the floor N-1 is the one the drain-first side needs and stays hard. A strategy flip carries no restart policy; the restart-policy flips run under the canonical surge budget (floor N). The switch family runs one integer budget per pair and one gang width per topology, and its crashing variants pair the surge and recreate strategies only, at the rotating hook, with form=start.
- No strategy pair is skipped in gen-switch: the bump is an annotation diff, which both in-place variants apply, and a gang runs either variant as a whole-gang recreate, so a flip into InPlaceOnly has no diff it cannot apply.
- The steady-state family (`during=idle`) and the sibling family (`bump=engine victimcomp=decoder`) leave out `form=load` under `restartPolicy: None`: a runner that dies after Ready and comes back is a container restart the kubelet owns under that policy, not a wedge, so no repair is promised and neither family can read one.
- On the engine-only topologies a Sequential group as the generators express it (one single-Component blueGreen group per member) is one blueGreen group over the engine, so the steady-state family emits it as `co=bluegreen` only.
- The sibling family rolls the engine under the default strategy at its canonical budget: the sibling's break is independent of how the engine's own pods are replaced.
- A rollingUpdate group admits no push that names one member without the other, so the sibling family's `co=rolling` cells bump engine and decoder together (`bump=engine,decoder`, the decoder's ceiling N+1 for its own surge) and the decoder's broken Instance is judged as the roll's: it ends on the new revision serving with no repair recorded after the bump.
- Every cell of both families runs live and asserts the repair (`expect=repaired`; the engine converges and the decoder is repaired): an Instance that serves nothing is rebuilt without touching its Component's budget or a group's gate, whatever group it sits in.
- heal breaks the engine runner, so its coordination axis is a rollingUpdate group over the engine alone or, where a decoder exists, over engine and decoder; nothing rolls, so the update strategy stays canonical and is not an axis. `lift=after-failed` carries `instanceReadyTimeout: 90s` on the victim so the row escalates inside the case budget.
- imagepull has one form (an image that never pulls has no after-Ready form) and runs each strategy at its canonical budget only; its idle half carries no group, since a grouped repair's hold is its own family.
- node:notready reads the fate of a Ready Instance on a dead node under the deployed force-delete policy, a lane dimension: without `lifecycle.forceDelete` (the chart default) nothing frees the pod names a silent node holds, so the outcomes are `parked` (nothing rebuilds it while the node is down) and `repaired` (the kubelet returns and the pods it never stopped serve again), both at `lift=after-settle`; with the policy the names are freed once the node's unreachable evidence is older than the threshold and the Instance is rebuilt elsewhere while the node is down, `relocated` at `lift=after-recover`. `needs=` pins parked to the plain lane and relocated to the force-delete lane (the runner SKIPs where the controller config differs); repaired holds under both. The migration axis is Auto (the default) and Never — the API has no mode that admits explicit requests but not relocation, and neither mode moves a Ready Instance (Auto relocates on an expired attempt's deadline, which a Ready Instance never has); restart policy and an idle rolling group change nothing either, and each cell says why. The update strategy is dormant at idle (one canonical budget).
- The packed-pool family runs RecreatePod and InPlaceIfPossible at maxSurge 0, maxUnavailable 1 only: a surge needs room a packed pool has not got (the tight-cluster family's story), and InPlaceOnly refuses the after-Ready form's env diff and patches the at-start form as InPlaceIfPossible does. Its group axis is a rollingUpdate group on the prefill/decode shapes, whose decoder carries the same one-down budget; blueGreen and Sequential need a whole second set of pods. The mirror (fault=crashloop:idle) runs under restart policy None only: the break lives in the live pod, not the template, so RecreateInstanceOnPodRestart heals it with its own rebuild before the push lands. The after-Ready form carries floor_soft=1 and reports the one-down rule and the landing order: a runner that exits after Ready can be Ready when the roll decides.
- ctrlrestart has no hook inside a repair (one fault per case; the idle-break handler fires no second one), so a restart while a crash-loop repair is open is not emitted. The strategy axis is live for the two rollout conditions and dormant under a migration and a scale-down, where both strategies still run for the family's symmetry; a rolling group is crossed in on the PD rollout conditions only.
- The gen-canary-* families drive operator stories through a canary at small size: 8 single-pod Instances, SurgeThenDrain with the engine surge at 1 and 2, a four-step ladder (10/30/60/100 percent capacity and traffic) with a timed soak in place of an analysis gate, on the engine and prefill/decode shapes only (the stories run single-pod Instances; the gang shapes are the crashloop family's).
- gen-canary-crash breaks the stable side at the first split (canary-serving@0): every break kind and form, both restart policies, 1 and 3 victim Instances at once. The floor is read with the victims allowed for, not waived.
- gen-canary-second-rev lands the second revision as an ISVC re-bump or a runtime edit (a dedicated runtime per case), at staging and at the first split, with and without an analysis gate on every step before the cutover; the restart policy is None (a re-target repairs nothing).
- gen-canary-podkill deletes one old-revision engine pod at a manual gate, under the operator's pause, under a capacity gate held by an image that cannot start, and under that gate paused; the engine surge is 1 (the budget is dormant while the roll is held).
- gen-canary-fault-old breaks the stable side at the step kinds that differ in mechanism: before the push (the push meets a broken fleet), the capacity step ahead of traffic, the second step's timed soak, the same step at a manual gate (promoted 30s after the break), the same step at an analysis gate, and the last gate before the cutover. Left out, with the reason: the first split, which gen-canary-crash drives with the same faults, victims and policies; the cutover itself (Promoting), where at most the surge budget of stable Instances is left and already draining, so a break has no settled row to land on (the last gate carries that claim); a traffic step apart from its soak, since every traffic step of the story ladder but the cutover soaks; the surge budget at the gates, where no capacity builds while they hold (surge 1 only); three victims at the manual gate, whose storm is the first split's under another release; the crash forms under the analysis gate, since the metrics image is a static binary with no shell to exec a kill into and no fixture image fails to start under its entrypoint (the pull failure alone breaks it); the analysis gate crossed with the other step kinds, since the gate changes only what ends a hold, not how a break meets it; and a missing ConfigMap key, whose reference both revisions share, so a push is never its fix (the idle configkey family lifts the cause instead).
- gen-canary-fault-new arms the break on the pushed engine revision and crosses where it bites (a crash at start and a pull failure at staging; a crash after Ready inside the first soak and past it) with the exit at the park (a rollback, or a fix push that drops the broken runner), both shapes and both restart policies. Left out, with the reason: the surge budget where the ladder parks at its first capacity step, which asks one Instance (surge 1 only); the crash forms under the analysis gate (the metrics image carries no shell and no exit switch); the rollback exit under the analysis gate, whose terminal verdict requires an analysis sample a park before the first gate never yields (the fix exit walks the gated ladder); a missing ConfigMap key, which no verb arms on a template and whose bite is a container that never starts, the crash-at-start cell; and a manual policy, since a broken revision parks before any gate is reached.
- gen-canary-fault-gate drives the engine unit's ladder on the ledger's six shape families (the router, where present, is outside the group and idle; single-pod fleets at 8 Instances, gangs at 2 Instances of 1 leader + 1 worker, SurgeThenDrain surge 1) and breaks what a step's gates read. The metric and source rows gate every step but the cutover on the canary-scoped error rate (`revision_hash="{{.CanaryRevision}}"`, the label the bundled Prometheus stamps from the pod's revision hash), so a breach on the stable side never reaches the gate by construction: a query that reads the whole Component blames the stable side and is not emitted. The stable side breaches through the metrics image's live override (every stable pod of the engine, kept or for 45 s) before the push, at the second step's gate and at the last gate; the canary side from birth (the push carries the breach, kept or decaying after 60 s) and at the same two gates (the override on every canary pod). The source is pointed at a name that resolves nowhere at the three positions, under both lifts (restored 20 s after the hold is observed, or never) and both stall policies (Hold parks Failed with reason AnalysisStalled at the step; RollbackOnStall rolls back), and at a server that is not Prometheus at the mid-ladder gate under Hold alone: the gate reads a non-answer the same way wherever it lands. Left out, with the reason: the restart policy on the metric and source rows (nothing restarts under a breach or a lost source; None throughout); the ladder positions other than mid-ladder on the gangs (a fleet of two has no stable Instance left at the last gate, and the gate reads a gang as it reads a single pod); the canary side's breach before the push (no canary pod exists yet; its birth breach is the push); Rollback as the stall policy (it reverts on the first unreadable sample, so a blip would revert a healthy canary by configuration, and the hand-written analysis cases carry that cell).
- The sibling rows of gen-canary-fault-gate run on the shapes with a Component beside the engine unit: the router of an engine ladder (never rolled by the ladder, so the repair owns its break) and the decoder of a prefill/decode unit (admission makes engine and decoder one canary unit, so the ladder owns its running-side break and lands the fix on it). The running side breaks for good (a crash at start and a pull failure under None; a crash after Ready under RecreateInstanceOnPodRestart, the policy that repairs it) and once (`lift=after-settle`: a crash after Ready is one kill under both policies; a pull failure has its image put back 20 s after it bit, under None), at the three positions on single-pod fleets and mid-ladder on gangs, under the plain fixture runtime and timed soaks (a break needs a shell to kill or an image to swap; the analysis gate changes nothing about a sibling). The new side: the decoder of the unit carries the fault-new breaks (crash at start, pull failure, crash inside and past the first soak; a pull failure alone on the gang) under both exits; the router is pushed beside the ladder onto an image that cannot be pulled (`bump=<unit>,router imgcomp=router`), rolls on its own and is judged as a parked sibling while the ladder is judged on the unit; and a pod of the sibling's new revision (the decoder's canary, or a router pushed beside the ladder) is killed once mid-ladder and at the last gate under both restart policies. Left out, with the reason: a sibling on the engine-only shapes (there is none; the ledger removes those cells); a running-side crash after Ready under None for good (a container restart the kubelet owns, not a wedge: no repair is promised, as in every idle family); the decoder as a roll beside the ladder (admission names it in the engine's group); a fix push for the parked router (the crash-loop and fault-new families prove a fix lands on a parked roll); a router pushed beside the prefill/decode gang's ladder (that shape carries the unit's new-side break).
- A rollback's `draining` hook is not emitted for an in-place strategy on a single-pod topology: an annotation-only diff is patched in place, so no pod of the starting revision ever drains and the hook never arrives (a gang falls back to a whole-gang recreate, which drains).
- crashloop:new arms the engine's new revision, so its group axis (co=none | co=rolling) exists only on the PD topologies; it runs each strategy at its canonical budget and leaves InPlaceOnly out: form=load's exit switch is a pod-template env diff in-place cannot carry (the InPlaceIfPossible cell documents the fallback) and form=start's image patch lands the same way under either in-place type.
- The group family runs a policy only where it has a peer: a one-member sequence is a blueGreen group and a ratio needs two Components, so engine-only runs blueGreen and the zero-budget rolling group alone; maintainRatio's tolerance is 50 at N=2 and 25 at N=4.
- The pause-and-repair family leaves out `form=load` under `restartPolicy: None`: a runner that dies after Ready and comes back is a container restart the kubelet owns under that policy, not a wedge, so there is no repair to time a pause against.
- A freeze after the repair has finished holds nothing a pause does not, so the family emits only `pause=true` at `pauseat=after`.
- The pause-and-repair family runs the engine under SurgeThenDrain maxSurge 1 maxUnavailable 1: a fresh crash-loop repair is charged against the unavailability budget, so under maxUnavailable 0 the repair the pause is timed against never opens.
- The grouped pause-and-repair cells run live: a repair of an Instance that serves nothing is admitted without the budget or the gate, so the rollingUpdate group changes nothing.
- The engine resolves a percent budget as ceil(replicas * percent / 100), clamped to replicas (the per-Component budget's documented rule, which the canary step capacity shares), so at one and two Instances 25 and 50 percent both resolve to one and no budget rounds to zero. gen-size (1 and 2 Instances per Component, the router sized with the fleet) drives the percent budgets on the plain push, where the resolved budget is read off the peak and the floor, and runs the crash loop (both forms, both restart policies), the scale (one to two and two to one) and the canary (10/50/100 percent capacity and traffic, on 2 Instances) under the integer budgets they resolve to. The canary runs on two Instances only: a canary on one Instance has no Instance to hold on the stable revision and stages in place, a shape with its own verdict to write; on two the first two steps resolve to the same single Instance, so the second moves traffic and adds no capacity. Group policies are left out: the group pacing budget rounds the same way and the group axis belongs to the crashloop family.
- gen-router-gang runs the engine as a gang (W1 and W2) behind a single-pod router at 2 Instances: the push under each strategy, the crash loop under the surge and recreate strategies only (a gang runs the in-place variants as a whole-gang recreate, the recreate cell), and the scale and the canary under the surge strategy (the strategy is dormant under a scale; the gang canary under a recreate is the controller-restart family's shape). The router is never bumped: every cell asserts it stays in rotation on the same pods.
- ops-held pushes a revision that cannot be pulled and lands one operation on the stuck Component; a rolling group exists only where a decoder does (pd). The deepest scale asks for one, the least count the API admits (minReplicas 0 needs KEDA autoscaling and is refused otherwise), and the verdict reads the count the controller accepted rather than assuming it; on the gang fleet of two, to one is down by one, so that cell is the scale-down row.
- runtime-edit-repair lands an edit on an open repair only where a repair opens: a crash after Ready opens one only under RecreateInstanceOnPodRestart, so the edit-during-repair order skips form=load under policy None; the crash-during-edit order keeps that cell (the roll must still land on a pod the kubelet alone restarts). The PD shape carries no router, as the runtime-rev cases do: the edit rolls engine and decoder, each onto its own revision.
- The migrate-and-bump family moves one Instance by request only: the engine's own (automatic) migration is a born-terminal relocation record, written when an Instance whose node is gone is rebuilt elsewhere; it has no in-flight phase for a roll to overlap, and the one verb that provokes it, a node loss, belongs to the node-loss family.
- `during=last` is not emitted with maxSurge 2: a surge of two promotes its last pair together, so the moment with exactly one Instance still off the new revision is not reliably observable.
- readiness:old runs on the idle service at the canonical SurgeThenDrain budget: update budgets are dormant with no roll, so strategy multiplies only the readiness:new side; readiness:new has one form, since a revision that never passes readiness has no one-shot shape (a slow start is the startup delay, not a readiness failure).
- gang-member cells at steady state are the mode=kill cells gen-restart already emits (kill=leader|worker x restart policy x strategy); this family adds the mid-roll timings, each bound to the gang it aims at (settling: a settled gang; rotating: the gang forming on the new revision; draining: a serving gang beside the first one leaving).
- tight: RecreatePod under the quota squeeze has no lift axis — the drain frees the gang's own room, so the roll completes with no wait, and the cell asserts that instead.
- evict has no lift: the Eviction API is a one-shot removal, so the family crosses topology x restartPolicy (x strategy under the roll) only. Its steady-state cells are the kill verdict with the removal done by the Eviction API; the product's disruption budget (`podDisruptionBudget.omeNative` in the chart) may refuse it while a pod is already down, the runner asks again as a drain does, and a budget that never admits one eviction fails the case.
- preempt and pending cross `lift=after-settle|never` (the cause removed once the wait has held, or kept to the end so the hold is judged), run on one gang width, with `co=none` (a group changes nothing about where a pod can be placed) and under the two strategies a roll waits under (in-place strategies create no pod to place). Both verbs need a cluster the harness owns (every node is blocked); elsewhere the case is SKIP with the reason. The preempt roll cells waive the floor and the order (the preemption takes a pod the budget cannot absorb); the pending verdict keeps its own floor.
- a pending cell's quota admits one pod fewer than the operation needs: `quota:0` for a single-pod surge, `quota:<members-1>` for a surge gang (room for all but one member), and `quota:-1` (one pod fewer than run) under a repair or a drain-first roll, which create after a pod is gone; on a gang under RecreateInstance that leaves the whole gang short of one member. A gang surge roll held to the end by that quota is the tight-cluster family's cell, so the pending family leaves its `quota:<members-1> lift=never` cell out.
- under a rollback, a strategy edit, a runtime edit, a scale and a delete the three families run one case per ledger cell under restart policy None: a taint or a quota armed before a rollback (drain-first, the rollback at its draining hook: the roll's first replacement is what waits, and a roll that cannot place its first pod reaches no later hook; the surge roll under the same cause never drains, so it offers no rollback moment), before a strategy edit landed as the push lands and before a runtime edit; an eviction as each operation lands, once and, where what it took comes back, again (a delete brings nothing back, so its cell is one-shot; a scale-up's persistent form is the repeating family's); a preemption as each operation that places a pod lands, the nodes opened again once the wait has held (a delete places nothing until its recreate, so it meets the eviction alone).
- gen-budget drives the percent budgets the engine resolves: maxSurge under SurgeThenDrain and maxUnavailable under RecreatePod at 10%, 25%, 50% on fleets of 4 and 8 Instances, for a plain push, a push with one engine Instance crash-looping on the running revision (form=start, restart policy None: the roll owns the broken Instance under either policy) and the story families' four-step auto canary. The engine resolves a percent budget as N x percent rounded up and clamped to N, for both budgets alike (the Deployment convention of rounding maxUnavailable down is not its rule), and every case states the rule with its own numbers: at 10 percent both fleets round up to 1, where rounding down would leave 0 and no roll could start, so the roll converging is the proof; at 25 and 50 percent the ceiling or floor scales with N. Left out, with the reason: the in-place strategies, whose maxUnavailable the drain-first cells already read and whose own family is gen-inplaceonly; the prefill/decode gang at 8 Instances, a 34-pod fleet that would cost a lane shard per case while the rounding is a function of N and the percent, which the single-pod cells at 8 carry; the crash form load and the restart policy under the crash loop (gen-crashloop crosses both at 25 percent); and the gangs under the canary beyond the engine gang at 4 (the pod budget of a canary lane). The capacity floor is waived on the crash-loop cells as on every crash-loop case; the ceiling stays hard.
- gen-inplaceonly drives InPlaceOnly on what sets it apart. An image change is the one diff it patches onto the live pod: on a single-pod Instance the roll lands in place with UIDs stable; a gang runs every in-place strategy as a whole-gang recreate (eligibility reads the leader's spec alone), so it is rebuilt whole and never refused. An env, a resources or a probe change exceeds container images: on a single-pod Instance the push is refused (`expect=held`: nothing is recreated, patched or surged for the hold window, an InPlaceUpdateNotPossible event says why, then the corrective push drops the override and lands in place); on a gang it is a whole-gang recreate. A crash loop at idle is repaired by a rebuild, since a repair is not an update (form=load under restart policy None is left out, as in every idle family: no repair is promised there). A crash loop under a push belongs to the roll: an image push patches the crashing pod itself and it serves again; an env push is refused, the repair yields to the roll, and the Instance waits broken for the corrective push, which patches it in place. One fleet size (4), both restart policies everywhere (live under the crash loops, a negative control under the pushes), the canonical budget maxUnavailable 1.
- gen-fault-scale and gen-fault-delete break Instance 0 of the settled service, the lowest index, so a scale-down's natural victim (the highest index) and the broken Instance differ and the order of removal can be told apart. The engine runs under SurgeThenDrain with maxSurge 1 and maxUnavailable 1 in both families: nothing rolls, so the strategy is dormant, and the unavailability budget of one admits the repair that takes the broken Instance offline (the canonical budget of zero holds it). Readiness loss is repaired under neither restart policy, so its policy axis is collapsed to None; a crash at start, a crash after Ready and a pull failure run under both, and each scale-up cell states on line two what becomes of the broken Instance (expect=repaired|left) and why. On the gang fleet of two only down by one and up by one are emitted: to one is down by one there, and down by two asks for zero, which the API refuses (minReplicas 0 needs KEDA autoscaling). A rolling group is crossed in where a decoder exists; the delete family keeps the same axes.
- gen-fault-move fires the fault at a moment of a move (at=accept, the request recorded; at=surge, the replacement pod up; at=drain, the source going) on the moved Instance (its source pod at accept, its replacement at surge and at drain), on a peer (the highest other Instance) or on the decoder (victim=sibling, PD only). Left out, with the reason: pd-mn (the move copies a whole gang whatever the decoder does; the gang axis is engine-mn's, the sibling axis pd's); the update strategy (dormant: no roll is in flight, a move surges whatever the strategy and a repair rebuilds in place); a rollout group (a repair of an Instance that serves nothing is admitted without its gate; the group axis is the steady-state crash-loop family's); the restart policy on a crash at start and a pull failure (both reach the crash-loop repair under either policy, so they run under None) while the after-Ready form runs under RecreateInstanceOnPodRestart, the one policy that promises its repair; a node loss anywhere but the source node (the story), at drain (the source is already going, its node dying then is the sweep of a terminating pod, which the surge moment reads with the replacement in place), and on the lane without the force-delete policy (a source pod on a silent node is never freed, so the move cannot finish; that fate is the node family's parked cell); the readiness switch (fault=readiness:old is repaired under neither restart policy, so it is the broken-Instance families' break and is not fired at a move moment).
- gen-fault-edit edits the dedicated runtime every Component inherits (engine, decoder and router each roll onto their own revision) under the surge and the drain-first strategies, and breaks a pod of the running revision on the engine or the decoder as the edit lands; its bad-edit half pushes an image that cannot be pulled or a readiness that never passes and then the corrective edit. Left out, with the reason: the in-place strategies (the switch family's); the env edit beyond the engine shape (under these strategies both edits are one new revision; the pod-template difference matters to an in-place strategy alone); the restart policy on a crash at start and a pull failure (they open no runner-restart trigger, so the roll owns the Instance either way; the after-Ready form runs under both); any restart policy but None on the bad edit (a revision that never becomes Ready restarts no runner); pd-mn, a rollout group and a readiness loss on the running side, as in fault-move. The readiness-that-never-passes cell asserts the fix and the floor and reports whether the status said why: nothing escalates a pod that runs but never passes readiness before its ready timeout.
- gen-repeat is the persistent form of a pod deleted by hand, a gang member lost, an eviction or a preemption and a manager restart: the fault is applied again each time what it took has come back (3 times; 2 for the preemption, whose fill-and-press cycle is the slowest), then stops, and the case reads the window and the end state after the lift. It runs on the six shape families at steady state (mode=kill; the manager on the idle service), under a push with the surge and the drain-first strategy and under a scale-up by one. The loop takes the same Instance again once it is back (the deleter hits the pod that came back), the running revision's pods under the push (victim=stable), and the engine on the prefill/decode shapes (victimcomp=engine), so the fault and the operation meet on one Component. The kill cells run at the canonical surge budget, dormant at steady state, so the floor reads N minus the killed Instance. Left out, with the reason: a gang member lost under a push with restart policy None (the one-shot gang-member family drives it; the repeating loss stresses the rebuild-whole path) and the eviction loop under a push or a scale with RecreateInstanceOnPodRestart (the policy only widens what is rebuilt, which the steady-state eviction cells cross; the eviction's own question is the disruption budget admitting each removal); the preemption loop under a push (a repeating preemption needs the kill's fill-and-press cycle, which the roll handler does not run) and with lift=never (nothing comes back while the fillers stay); the manager restarting while a repair is open, as in the one-shot family. The cluster-wide forms (the manager restarts, the preemptions) run one at a time after the pool, as every such case does.
- gen-webhook points the admission webhook's configurations at a Service that does not exist on the settled service, as a push lands and as a scale-up lands, for a bounded time (lift=after-hold: pointed back once the status has said why a write waits, or after the settle window at idle) or across the hold window (lift=after-window), on every shape family of the coverage ledger at one gang width (the outage acts on the controller's writes, not on a gang's members). The push runs under the surge and the drain-first strategies: the outage refuses the pod a surge creates before anything is torn down, and the one a drain-first roll creates after a pod is gone; the steady state and the scale are dormant to the strategy. No rollout group: the engine is the only bumped member. The in-place strategies are left out: an annotation-only diff is patched onto the live pod and creates nothing for admission to refuse. A webhook case runs alone (the outage is cluster-wide: every ISVC write in the cluster is refused while it holds). The outage also opens at the push's later hooks (the first pod of the new revision, the first pod of the old one on its way out, under both strategies), as a rollback filed at the first new pod lands, as a strategy edit landed there lands, as a scale-down by one lands, as an env edit of the case's dedicated runtime lands (every Component rolls), as soon as a move's request is recorded, and as a delete lands (the same name is re-created once the window has closed); each reads the same window verdict, and the operation must complete once the webhook is back.
- gen-conflict runs a competing writer (an annotation patch of the ISVC or of every InferenceReplica of the service; target=isvc|ir) once or until the operation completes (form=once|persist) over the same operations, hooks and shapes, under the canonical surge budget alone: a status write conflicts the same way under every strategy. The writer is per service, so its cases run in the concurrent pool. The reconcile counters are read on the webhook cases only (they run alone); a conflict case bounds the controller's events on its own objects.
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

## gen-heal: how each half heals

The family asserts `expect=heals` on every cell, and every cell runs live. A rolling group changes nothing: a repair of an Instance that serves nothing is admitted without the group's gate, and the runner-restart repair is not put to it.

- `lift=after-settle`: the key returns once the CreateContainerConfigError wait has held for the settle window, before any attempt escalates. Under `None` the crash-loop repair opens as soon as the wait is seen (the stuck-pod clock counts from the pod's creation and the victim is older than the grace), rebuilds the pod, and the rebuilt pod waits on the same key; under `RecreateInstanceOnPodRestart` the runner-restart repair does the same. Either way the kubelet starts the container once the key is back and the open repair promotes the Instance. The settle window (`LIFT_SETTLE_S`, 20 s before `OME_TEST_TIMEOUT_MULT`) has to stay inside the deployment's `stuckPodGracePeriod` (60 s in the chart): a multiplier of 3 or more turns these cells into the after-failed shape.
- `lift=after-failed`: the same repair opens, and the rebuilt pod's wait outlives the stuck-pod grace (or the attempt its `instanceReadyTimeout`, `HEAL_FAILED_READY_TIMEOUT`), so the escalation parks the repair at Failed with its Restart kept. A parked repair keeps two exits of its own: the restart pass resumes its promote when the rebuilt set comes up, and re-arms it on `lifecycle.updateRetry` while the set stays wedged. The key returns once the row reads Failed, the kubelet's retry brings the rebuilt set up, and the resume promotes the Instance on its running revision with no reset mailbox and no new revision, whether or not a ladder is configured.

## gen-lift: what each form reads

One family fills the six ledger rows the other families left empty; every cell runs live. A kept cause (`lift=never`) is the persistent form, a lifted one (`lift=after-settle`) the one-shot form.

- `config missing, persistent, running`: `configkey:missing during=idle lift=never expect=failed`; the row reaches Failed with its reason inside the `instanceReadyTimeout: 90s` and stays Failed with nothing serving across the hold; both restart policies (which repair opens first differs).
- `config missing, persistent, new` and `config missing, one-shot, new`: `configkey:new` with `lift=never` (the roll holds at the ready timeout with its reason; a fix push back onto the present key lands) or `lift=after-settle` (the key is added and the roll completes with no hold behind it). The surge and the drain-first strategies decide which pod waits and the floor; one restart policy, since a pod that never started has no restart for a policy to read.
- `readiness never passes, one-shot, new`: `readiness:new form=recover`; the switch file is set in the push's first engine pod as it starts and lifted once the drop has settled; the same pod returns and no restart policy may rebuild it. Both restart policies under SurgeThenDrain (the surge pod is the first pushed pod and takes no capacity while it waits; drain-first is the same story at floor N-1).
- `image pull, one-shot, running`: `imagepull:old during=idle lift=after-settle expect=heals`; the image is pointed back once the pull failure has settled; the kubelet or a repair brings the Instance back, both restart policies.
- `image pull, one-shot, new`: `imagepull:new lift=after-settle`; the push's first engine pod is pointed at the unpullable image as it starts and back once the failure has settled; one rebuild may be reported under RecreateInstanceOnPodRestart, never required, never twice; both restart policies under SurgeThenDrain.
- No group axis: a kept cause changes no fleet for a group to pace, and a lifted one leaves the roll to finish as any plain push; the group families cover the gates.

## Missing runner verbs/hooks (future coverage, not emitted)

- kill-instance verb (atomic delete of a whole gang) — today only reachable as the RecreateInstanceOnPodRestart cascade from one pod death
- component-scoped podkill victim (fault=podkill:<phase>:<component>) — victim selection is component-blind, so PD podkill can never target the engine at settling, and single-pod in-place engine victims are unreachable
- mid-surge injection points beyond first-surge-observed (k-th surge, mid-drain, pre-settle tail) — FAULT_PHASES has settling/rotating/migrating only
- pause-under-rollout hook (ome.io/rollout-paused is exercised inside mode=kill and around a repair at idle, pauseat=; no verb pauses an in-flight rotation)
- decoder-targeted mode=kill (the kill handler always picks the engine when one is configured)
- controller restart while a repair is open (a crash loop at idle with the manager deleted once the rebuild has started) — the runner fires one fault per case and the idle-break handler fires no second one
- an automatic migration verb — the engine's own migration is a born-terminal relocation record written when a lost Instance is rebuilt elsewhere, so only a node loss provokes it and the migrate-and-bump family moves by request only
- a pending cause armed mid-roll (a taint or a quota at the rotating hook) — taint:all and quota:<n> are armed before the bump; only preempt fires at a hook
- a webhook outage or a competing writer at a canary step — the window faults open on the settled service, at a hook of a push or a runtime edit, as a rollback, a mid-roll edit, a scale or a delete lands, and at a moment of a move, not while a canary ladder is at a step

## Deferred fill

`--fill` (t=3 covering array over coordination groups, canary promotion, and budget interactions) is documented in the generator and intentionally not emitted until the hot core has baked in the per-PR lane.
