# Generated hot-core corpus

Emitted by `generate_cases.py` — regenerate with `python3 generate_cases.py`; CI rejects hand edits via `python3 generate_cases.py --check` (run_test.py).
The gen-* folders are generator-owned wholesale; promote a bug-finding case to a hand-owned folder under a fresh id instead of editing it here.

Total generated cases: **6992**. The whole-corpus per-PR cap (hand + generated) is enforced by run_test.py, not baked in here.

## Folders

| folder | cases | id range |
|---|---|---|
| gen-rollout | 73 | p1000..p1072 |
| gen-fault | 339 | p1200..p1538 |
| gen-restart | 187 | p1600..p1786 |
| gen-crashloop | 216 | p2000..p2215 |
| gen-switch | 584 | p3800..p11283 |
| gen-crashloop-idle | 114 | p2400..p2513 |
| gen-crashloop-sibling | 36 | p2600..p2635 |
| gen-heal | 34 | p3000..p3033 |
| gen-imagepull | 60 | p3400..p3459 |
| gen-node | 196 | p4200..p4395 |
| gen-ctrlrestart | 104 | p4600..p4703 |
| gen-canary-crash | 48 | p5200..p5247 |
| gen-canary-second-rev | 32 | p5250..p5281 |
| gen-canary-podkill | 55 | p5290..p5344 |
| gen-canary-fault-old | 343 | p6400..p11642 |
| gen-canary-fault-new | 154 | p6600..p6753 |
| gen-canary-fault-gate | 173 | p9050..p9222 |
| gen-rollback | 484 | p5800..p11083 |
| gen-crashloop-new | 88 | p5000..p5087 |
| gen-crashloop-group | 72 | p5400..p5471 |
| gen-pause-repair | 180 | p6000..p6179 |
| gen-size | 86 | p8000..p8085 |
| gen-router-gang | 30 | p8100..p8129 |
| gen-ops-held | 196 | p6800..p10095 |
| gen-runtime-edit-repair | 84 | p6900..p6983 |
| gen-migrate-bump | 154 | p6200..p6353 |
| gen-readiness | 60 | p7000..p7059 |
| gen-gang-member | 91 | p7100..p7190 |
| gen-tight | 28 | p7300..p7327 |
| gen-evict | 102 | p7400..p9561 |
| gen-preempt | 102 | p7440..p9641 |
| gen-pending | 219 | p7500..p10718 |
| gen-budget | 114 | p8200..p8313 |
| gen-inplaceonly | 45 | p8330..p8374 |
| gen-fault-scale | 300 | p7600..p10199 |
| gen-fault-delete | 128 | p7700..p10227 |
| gen-fault-move | 415 | p7800..p11914 |
| gen-fault-edit | 234 | p7900..p12033 |
| gen-packed-pool | 96 | p8600..p8695 |
| gen-repeat | 192 | p8700..p8891 |
| gen-webhook | 180 | p8900..p10419 |
| gen-conflict | 264 | p8960..p10635 |
| gen-crash-once | 168 | p8400..p8567 |
| gen-lift | 132 | p9250..p9381 |

### By topology

| value | cases |
|---|---|
| engine | 2090 |
| engine-mn | 1942 |
| pd | 1611 |
| pd-mn | 1349 |

### By update strategy

| value | cases |
|---|---|
| (dormant: nothing rolls) | 872 |
| InPlaceIfPossible | 602 |
| InPlaceOnly | 281 |
| RecreatePod | 1261 |
| SurgeThenDrain | 3976 |

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
| configMissing@before-edit | 6 |
| configMissing@edited-runtime | 6 |
| configMissingOnce@before-edit | 6 |
| configMissingOnce@edited-runtime | 6 |
| configkey kept@idle | 6 |
| configkey lifted@gate | 12 |
| configkey lifted@idle | 6 |
| configkey lifted@soak | 12 |
| configkey:after-failed | 16 |
| configkey:after-settle | 18 |
| configkey:after-settle:fixpush | 12 |
| configkey:never | 12 |
| configkey:never:fixpush | 12 |
| configkey:new:after-settle | 12 |
| configkey:new:configkey kept:rebump-fix | 6 |
| configkey:new:configkey kept:rollback | 6 |
| configkey:new:configkey lifted | 6 |
| configkey:new:never | 12 |
| configkey:once@delete | 6 |
| configkey:once@down1 | 6 |
| configkey:once@up1 | 6 |
| configkey@delete | 6 |
| configkey@down1 | 6 |
| configkey@move-accept:after-settle | 14 |
| configkey@move-accept:never | 8 |
| configkey@move-surge:after-settle | 6 |
| configkey@move-surge:never | 6 |
| conflict:canary-serving@1 | 12 |
| conflict:draining | 24 |
| conflict:idle | 24 |
| conflict:migrating | 24 |
| conflict:rotating | 24 |
| conflict:settling | 156 |
| cordon:after-hold | 8 |
| cordon:never | 8 |
| crashLoad@edited-runtime | 6 |
| crashLoadOnce@before-edit | 10 |
| crashLoadOnce@edited-runtime | 6 |
| crashStart@edited-runtime | 10 |
| crashStartOnce@before-edit | 6 |
| crashStartOnce@edited-runtime | 10 |
| crashloop:load | 212 |
| crashloop:load lifted@idle | 6 |
| crashloop:load lifted@soak | 12 |
| crashloop:load once@gate | 12 |
| crashloop:load once@soak | 12 |
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
| crashloop:load@move-accept:healed | 10 |
| crashloop:load@move-drain | 16 |
| crashloop:load@move-drain:once | 6 |
| crashloop:load@move-surge | 16 |
| crashloop:load@move-surge:healed | 6 |
| crashloop:load@prestart | 16 |
| crashloop:load@promote | 8 |
| crashloop:load@sibling | 12 |
| crashloop:load@soak | 8 |
| crashloop:load@toone | 8 |
| crashloop:load@traffic | 16 |
| crashloop:load@up1 | 16 |
| crashloop:new:crashAfterSoak | 16 |
| crashloop:new:crashInSoak | 16 |
| crashloop:new:crashInSoak:rebump-fix | 4 |
| crashloop:new:crashInSoak:rollback | 4 |
| crashloop:new:crashStart | 8 |
| crashloop:new:crashStart:rebump-fix | 4 |
| crashloop:new:crashStart:rollback | 4 |
| crashloop:new:crashloop:load once | 12 |
| crashloop:new:crashloop:start once | 12 |
| crashloop:new:decoder:crashAfterSoak | 2 |
| crashloop:new:decoder:crashInSoak | 2 |
| crashloop:new:decoder:crashStart | 2 |
| crashloop:new:load | 44 |
| crashloop:new:start | 44 |
| crashloop:start | 266 |
| crashloop:start lifted@idle | 6 |
| crashloop:start lifted@soak | 12 |
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
| crashloop:start@move-accept:healed | 10 |
| crashloop:start@move-drain | 16 |
| crashloop:start@move-surge | 16 |
| crashloop:start@move-surge:healed | 6 |
| crashloop:start@move-surge:once | 6 |
| crashloop:start@prestart | 16 |
| crashloop:start@promote | 8 |
| crashloop:start@sibling | 24 |
| crashloop:start@soak | 8 |
| crashloop:start@toone | 8 |
| crashloop:start@traffic | 16 |
| crashloop:start@up1 | 20 |
| crashonce:new:load | 32 |
| crashonce:new:start | 32 |
| crashonce:old:load | 52 |
| crashonce:old:start | 52 |
| ctrlrestart:canary | 12 |
| ctrlrestart:delete | 6 |
| ctrlrestart:drain | 16 |
| ctrlrestart:edit | 6 |
| ctrlrestart:idle | 6 |
| ctrlrestart:migration | 12 |
| ctrlrestart:rollback | 6 |
| ctrlrestart:runtime | 6 |
| ctrlrestart:scaledown | 12 |
| ctrlrestart:scaleup | 6 |
| ctrlrestart:surge | 16 |
| edit:held+break:configkey | 12 |
| edit:held+break:configkey:once | 6 |
| edit:held+break:crash-load | 12 |
| edit:held+break:crash-load:once | 6 |
| edit:held+break:crash-start | 14 |
| edit:held+break:crash-start:once | 8 |
| edit:held+break:imagepull | 12 |
| edit:held+break:imagepull:once | 6 |
| edit:held+break:stuck | 12 |
| edit:held+break:stuck:once | 6 |
| edit:rotating | 86 |
| edit:rotating+break:configkey | 6 |
| edit:rotating+break:configkey:once | 6 |
| edit:rotating+break:crash-load | 6 |
| edit:rotating+break:crash-load:once | 6 |
| edit:rotating+break:crash-start | 6 |
| edit:rotating+break:crash-start:once | 6 |
| edit:rotating+break:imagepull | 6 |
| edit:rotating+break:imagepull:once | 6 |
| edit:rotating+break:stuck | 6 |
| edit:rotating+break:stuck:once | 6 |
| edit:rotating+configkey:missing | 24 |
| edit:rotating+crashloop:idle | 48 |
| edit:rotating+imagepull:idle | 24 |
| edit:rotating+readiness:old | 24 |
| edit:settling | 84 |
| edit:settling+configkey:missing | 24 |
| edit:settling+crashloop:idle | 56 |
| edit:settling+imagepull:idle | 24 |
| edit:settling+readiness:old | 24 |
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
| evict@auto | 6 |
| evict@auto x2 | 6 |
| evict@manual | 6 |
| evict@move-accept | 6 |
| evict@move-drain | 6 |
| evict@move-surge | 6 |
| evict@move-surge x2 | 6 |
| image | 6 |
| imagePullOnce@before-edit | 6 |
| imagePullOnce@edited-runtime | 6 |
| imagepull lifted@idle | 3 |
| imagepull lifted@soak | 6 |
| imagepull:decoder:once@idle | 1 |
| imagepull:decoder:once@last | 1 |
| imagepull:decoder:once@mid | 2 |
| imagepull:decoder@idle | 1 |
| imagepull:decoder@last | 1 |
| imagepull:decoder@mid | 2 |
| imagepull:idle | 10 |
| imagepull:idle:after-settle | 12 |
| imagepull:new:after-settle | 12 |
| imagepull:new:decoder:imagePull | 4 |
| imagepull:new:imagePull | 12 |
| imagepull:new:imagePull:rebump-fix | 4 |
| imagepull:new:imagePull:rollback | 4 |
| imagepull:new:imagepull lifted | 6 |
| imagepull:new:router:parked | 3 |
| imagepull:old:after-settle:push | 12 |
| imagepull:once@delete | 6 |
| imagepull:once@down1 | 6 |
| imagepull:once@up1 | 6 |
| imagepull:router:once@idle | 1 |
| imagepull:router:once@last | 1 |
| imagepull:router:once@mid | 2 |
| imagepull:router@idle | 1 |
| imagepull:router@last | 1 |
| imagepull:router@mid | 2 |
| imagepull:settling | 50 |
| imagepull@analysis | 8 |
| imagepull@canary | 16 |
| imagepull@capacity | 16 |
| imagepull@delete | 16 |
| imagepull@down1 | 16 |
| imagepull@down2 | 8 |
| imagepull@edit | 26 |
| imagepull@gate | 4 |
| imagepull@move-accept | 16 |
| imagepull@move-accept:healed | 10 |
| imagepull@move-drain | 16 |
| imagepull@move-surge | 16 |
| imagepull@move-surge:healed | 6 |
| imagepull@prestart | 16 |
| imagepull@promote | 8 |
| imagepull@soak | 4 |
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
| node:canary | 12 |
| node:delete | 12 |
| node:edit | 12 |
| node:parked | 32 |
| node:push | 12 |
| node:relocated | 34 |
| node:repaired | 34 |
| node:rollback | 12 |
| node:runtime | 12 |
| node:scaledown | 12 |
| node:scaleup | 12 |
| node@move-accept | 12 |
| node@move-surge | 12 |
| none | 141 |
| none@delete | 4 |
| none@edit | 4 |
| none@move | 2 |
| none@scaledown | 1 |
| none@scaleup | 1 |
| packed:idle:load | 16 |
| packed:idle:start | 16 |
| packed:new:load | 32 |
| packed:new:start | 32 |
| pending:quota-kept@delete | 6 |
| pending:quota-kept@down1 | 6 |
| pending:quota-kept@up1 | 6 |
| pending:quota:canary:kept | 4 |
| pending:quota:canary:lifted | 5 |
| pending:quota:edit | 12 |
| pending:quota:roll | 42 |
| pending:quota:rollback | 12 |
| pending:quota:runtime | 12 |
| pending:quota:steady | 24 |
| pending:quota@delete | 6 |
| pending:quota@down1 | 6 |
| pending:quota@up1 | 6 |
| pending:taint:edit | 12 |
| pending:taint:move:after-settle | 6 |
| pending:taint:move:never | 6 |
| pending:taint:roll | 48 |
| pending:taint:rollback | 12 |
| pending:taint:runtime | 12 |
| pending:taint:steady | 24 |
| pending:taint@delete | 12 |
| pending:taint@down1 | 12 |
| pending:taint@up1 | 12 |
| podkill:delete | 6 |
| podkill:draining:leader | 12 |
| podkill:draining:worker | 12 |
| podkill:edit | 6 |
| podkill:migrating | 3 |
| podkill:migrating x2 | 3 |
| podkill:migrating:leader | 3 |
| podkill:migrating:leader x2 | 3 |
| podkill:migrating:worker | 3 |
| podkill:migrating:worker x2 | 3 |
| podkill:rollback | 6 |
| podkill:rotating | 160 |
| podkill:rotating:leader | 12 |
| podkill:rotating:worker | 12 |
| podkill:runtime | 6 |
| podkill:scaledown | 6 |
| podkill:scaleup | 6 |
| podkill:settling | 161 |
| podkill:settling:leader | 13 |
| podkill:settling:worker | 12 |
| podkill@auto | 3 |
| podkill@auto x2 | 18 |
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
| preempt@move-surge | 6 |
| probe@held | 4 |
| probe@recreate | 2 |
| push:failed | 56 |
| push:failed:configkey | 18 |
| push:failed:imagepull+move | 6 |
| push:failed:stuck | 18 |
| push:held | 56 |
| push:held:configkey+move | 6 |
| push:held:crash-load | 18 |
| push:held:crash-load+move | 6 |
| push:held:crash-start | 18 |
| push:held:crash-start+move | 6 |
| push:held:crash-start:decoder | 6 |
| push:held:crash-start:router | 6 |
| push:held:imagepull+move | 6 |
| push:held:imagepull:decoder+move | 2 |
| push:held:stuck+move | 6 |
| quota:after-hold | 4 |
| quota:completes | 4 |
| quota:never | 4 |
| quota@move:kept | 3 |
| quota@move:lifted | 4 |
| readiness kept@gate | 6 |
| readiness kept@idle | 3 |
| readiness kept@soak | 6 |
| readiness lifted@gate | 6 |
| readiness lifted@idle | 3 |
| readiness lifted@soak | 6 |
| readiness:new | 36 |
| readiness:new:readiness kept:rebump-fix | 6 |
| readiness:new:readiness kept:rollback | 6 |
| readiness:new:readiness lifted | 6 |
| readiness:new:recover | 12 |
| readiness:old:persist | 12 |
| readiness:old:persist:fixpush | 12 |
| readiness:old:recover | 12 |
| readiness:old:recover:fixpush | 12 |
| readiness:once@delete | 6 |
| readiness:once@down1 | 6 |
| readiness:once@up1 | 6 |
| readiness:persist@move-accept | 14 |
| readiness:persist@move-drain | 6 |
| readiness:persist@move-surge | 6 |
| readiness:recover@move-accept | 14 |
| readiness:recover@move-surge | 6 |
| readiness@delete | 8 |
| readiness@down1 | 8 |
| readiness@down2 | 4 |
| readiness@toone | 4 |
| readiness@up1 | 8 |
| readinessLost@before-edit | 6 |
| readinessLostOnce@before-edit | 6 |
| readinessLostOnce@edited-runtime | 6 |
| rebump@canary-serving@0 | 8 |
| rebump@canary-staging | 8 |
| repeat:ctrlrestart:canary | 6 |
| repeat:ctrlrestart:delete | 6 |
| repeat:ctrlrestart:edit | 6 |
| repeat:ctrlrestart:move | 6 |
| repeat:ctrlrestart:push | 12 |
| repeat:ctrlrestart:rollback | 6 |
| repeat:ctrlrestart:runtime | 6 |
| repeat:ctrlrestart:scale-up | 6 |
| repeat:ctrlrestart:scaledown | 6 |
| repeat:ctrlrestart:steady | 6 |
| repeat:evict:push | 12 |
| repeat:evict:scale-up | 6 |
| repeat:evict:steady | 18 |
| repeat:podkill:edit | 6 |
| repeat:podkill:push | 24 |
| repeat:podkill:rollback | 6 |
| repeat:podkill:runtime | 6 |
| repeat:podkill:scale-up | 18 |
| repeat:podkill:scaledown | 6 |
| repeat:podkill:steady | 18 |
| repeat:preempt:steady | 6 |
| resources@held | 4 |
| resources@recreate | 2 |
| rollback:draining | 22 |
| rollback:draining:configkey:missing | 6 |
| rollback:draining:crashloop:old | 24 |
| rollback:draining:imagepull:old | 12 |
| rollback:draining:readiness:old | 12 |
| rollback:held | 28 |
| rollback:held:configkey | 18 |
| rollback:held:configkey:once | 6 |
| rollback:held:crash-load | 18 |
| rollback:held:crash-load:once | 6 |
| rollback:held:crash-start | 20 |
| rollback:held:crash-start:once | 8 |
| rollback:held:imagepull | 18 |
| rollback:held:imagepull:once | 6 |
| rollback:held:stuck:once | 6 |
| rollback:last | 26 |
| rollback:last:configkey:missing | 6 |
| rollback:last:crashloop:old | 24 |
| rollback:last:imagepull:old | 12 |
| rollback:last:readiness:old | 12 |
| rollback:rotating | 28 |
| rollback:rotating:configkey:missing | 24 |
| rollback:rotating:crashloop:old | 24 |
| rollback:rotating:imagepull:old | 12 |
| rollback:rotating:readiness:old | 12 |
| rollback:settling | 26 |
| rollback:settling:configkey:missing | 12 |
| rollback:settling:crashloop:old | 24 |
| rollback:settling:crashloop:old:decoder | 4 |
| rollback:settling:crashloop:old:router | 4 |
| rollback:settling:imagepull:old | 12 |
| rollback:settling:readiness:old | 12 |
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
| taint:after-settle | 6 |
| taint:never | 6 |
| webhook:canary-serving@1 | 12 |
| webhook:draining | 24 |
| webhook:idle | 12 |
| webhook:migrating | 12 |
| webhook:rotating | 24 |
| webhook:settling | 96 |

### By restartPolicy

| value | cases |
|---|---|
| (unset — baseline) | 2407 |
| None | 2831 |
| RecreateInstance | 1754 |

### By pause

| value | cases |
|---|---|
| freeze | 110 |
| off | 6698 |
| true | 184 |

### By pause moment

| value | cases |
|---|---|
| (no pause around a repair) | 6812 |
| after | 36 |
| before | 72 |
| during | 72 |

### By push while paused

| value | cases |
|---|---|
| (no pause around a repair) | 6812 |
| fix | 90 |
| none | 90 |

### By coordination

| value | cases |
|---|---|
| bluegreen | 54 |
| canary | 30 |
| none | 5966 |
| ratio | 16 |
| rolling | 892 |
| seq | 34 |

### By mid-roll edit (gen-switch)

| value | cases |
|---|---|
| restartPolicy None to RecreateInstance at held by configkey | 6 |
| restartPolicy None to RecreateInstance at held by crash-load | 6 |
| restartPolicy None to RecreateInstance at held by crash-start | 6 |
| restartPolicy None to RecreateInstance at held by imagepull | 6 |
| restartPolicy None to RecreateInstance at held by stuck | 6 |
| restartPolicy None to RecreateInstance at rotating | 6 |
| restartPolicy None to RecreateInstance at settling | 6 |
| restartPolicy None to RecreateInstance at settling over configkey after-settle | 6 |
| restartPolicy None to RecreateInstance at settling over configkey never | 6 |
| restartPolicy None to RecreateInstance at settling over crashloop load | 6 |
| restartPolicy None to RecreateInstance at settling over crashloop load after-settle | 6 |
| restartPolicy None to RecreateInstance at settling over crashloop start | 6 |
| restartPolicy None to RecreateInstance at settling over crashloop start after-settle | 6 |
| restartPolicy None to RecreateInstance at settling over imagepull | 6 |
| restartPolicy None to RecreateInstance at settling over imagepull after-settle | 6 |
| restartPolicy None to RecreateInstance at settling over readiness persist | 6 |
| restartPolicy None to RecreateInstance at settling over readiness recover | 6 |
| restartPolicy RecreateInstance to None at rotating | 6 |
| restartPolicy RecreateInstance to None at settling | 6 |
| strategy InPlaceIfPossible to InPlaceOnly at rotating | 6 |
| strategy InPlaceIfPossible to InPlaceOnly at settling | 6 |
| strategy InPlaceIfPossible to RecreatePod at rotating | 6 |
| strategy InPlaceIfPossible to RecreatePod at settling | 6 |
| strategy InPlaceIfPossible to SurgeThenDrain at rotating | 6 |
| strategy InPlaceIfPossible to SurgeThenDrain at rotating over configkey after-settle | 6 |
| strategy InPlaceIfPossible to SurgeThenDrain at rotating over configkey never | 6 |
| strategy InPlaceIfPossible to SurgeThenDrain at rotating over crashloop load | 6 |
| strategy InPlaceIfPossible to SurgeThenDrain at rotating over crashloop load after-settle | 6 |
| strategy InPlaceIfPossible to SurgeThenDrain at rotating over crashloop start | 6 |
| strategy InPlaceIfPossible to SurgeThenDrain at rotating over crashloop start after-settle | 6 |
| strategy InPlaceIfPossible to SurgeThenDrain at rotating over imagepull | 6 |
| strategy InPlaceIfPossible to SurgeThenDrain at rotating over imagepull after-settle | 6 |
| strategy InPlaceIfPossible to SurgeThenDrain at rotating over readiness persist | 6 |
| strategy InPlaceIfPossible to SurgeThenDrain at rotating over readiness recover | 6 |
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
| strategy RecreatePod to SurgeThenDrain at rotating by configkey | 6 |
| strategy RecreatePod to SurgeThenDrain at rotating by configkey once | 6 |
| strategy RecreatePod to SurgeThenDrain at rotating by crash-load | 6 |
| strategy RecreatePod to SurgeThenDrain at rotating by crash-load once | 6 |
| strategy RecreatePod to SurgeThenDrain at rotating by crash-start | 6 |
| strategy RecreatePod to SurgeThenDrain at rotating by crash-start once | 6 |
| strategy RecreatePod to SurgeThenDrain at rotating by imagepull | 6 |
| strategy RecreatePod to SurgeThenDrain at rotating by imagepull once | 6 |
| strategy RecreatePod to SurgeThenDrain at rotating by stuck | 6 |
| strategy RecreatePod to SurgeThenDrain at rotating by stuck once | 6 |
| strategy RecreatePod to SurgeThenDrain at rotating over configkey after-settle | 6 |
| strategy RecreatePod to SurgeThenDrain at rotating over configkey never | 6 |
| strategy RecreatePod to SurgeThenDrain at rotating over crashloop load | 6 |
| strategy RecreatePod to SurgeThenDrain at rotating over crashloop load after-settle | 6 |
| strategy RecreatePod to SurgeThenDrain at rotating over crashloop start | 6 |
| strategy RecreatePod to SurgeThenDrain at rotating over crashloop start after-settle | 6 |
| strategy RecreatePod to SurgeThenDrain at rotating over imagepull | 6 |
| strategy RecreatePod to SurgeThenDrain at rotating over imagepull after-settle | 6 |
| strategy RecreatePod to SurgeThenDrain at rotating over readiness persist | 6 |
| strategy RecreatePod to SurgeThenDrain at rotating over readiness recover | 6 |
| strategy RecreatePod to SurgeThenDrain at rotating while an Instance crashes | 6 |
| strategy RecreatePod to SurgeThenDrain at settling | 6 |
| strategy SurgeThenDrain to InPlaceIfPossible at rotating | 6 |
| strategy SurgeThenDrain to InPlaceIfPossible at settling | 6 |
| strategy SurgeThenDrain to InPlaceOnly at rotating | 6 |
| strategy SurgeThenDrain to InPlaceOnly at settling | 6 |
| strategy SurgeThenDrain to RecreatePod at held by configkey | 6 |
| strategy SurgeThenDrain to RecreatePod at held by configkey once | 6 |
| strategy SurgeThenDrain to RecreatePod at held by crash-load | 6 |
| strategy SurgeThenDrain to RecreatePod at held by crash-load once | 6 |
| strategy SurgeThenDrain to RecreatePod at held by crash-start | 6 |
| strategy SurgeThenDrain to RecreatePod at held by crash-start on the decoder | 2 |
| strategy SurgeThenDrain to RecreatePod at held by crash-start once | 6 |
| strategy SurgeThenDrain to RecreatePod at held by crash-start once on the decoder | 2 |
| strategy SurgeThenDrain to RecreatePod at held by imagepull | 6 |
| strategy SurgeThenDrain to RecreatePod at held by imagepull once | 6 |
| strategy SurgeThenDrain to RecreatePod at held by stuck | 6 |
| strategy SurgeThenDrain to RecreatePod at held by stuck once | 6 |
| strategy SurgeThenDrain to RecreatePod at rotating | 8 |
| strategy SurgeThenDrain to RecreatePod at rotating while an Instance crashes | 6 |
| strategy SurgeThenDrain to RecreatePod at settling | 6 |
| strategy SurgeThenDrain to RecreatePod at settling over configkey after-settle | 6 |
| strategy SurgeThenDrain to RecreatePod at settling over configkey never | 6 |
| strategy SurgeThenDrain to RecreatePod at settling over crashloop load | 6 |
| strategy SurgeThenDrain to RecreatePod at settling over crashloop load after-settle | 6 |
| strategy SurgeThenDrain to RecreatePod at settling over crashloop start | 6 |
| strategy SurgeThenDrain to RecreatePod at settling over crashloop start after-settle | 6 |
| strategy SurgeThenDrain to RecreatePod at settling over crashloop start after-settle on the decoder | 2 |
| strategy SurgeThenDrain to RecreatePod at settling over crashloop start after-settle on the router | 2 |
| strategy SurgeThenDrain to RecreatePod at settling over crashloop start on the decoder | 2 |
| strategy SurgeThenDrain to RecreatePod at settling over crashloop start on the router | 2 |
| strategy SurgeThenDrain to RecreatePod at settling over imagepull | 6 |
| strategy SurgeThenDrain to RecreatePod at settling over imagepull after-settle | 6 |
| strategy SurgeThenDrain to RecreatePod at settling over readiness persist | 6 |
| strategy SurgeThenDrain to RecreatePod at settling over readiness recover | 6 |

### By break beside the mid-roll edit (gen-switch)

| value | cases |
|---|---|
| configkey after-settle before the push | 24 |
| configkey never before the push | 24 |
| configkey on the pushed revision | 18 |
| configkey once on the pushed revision | 12 |
| crash-load on the pushed revision | 18 |
| crash-load once on the pushed revision | 12 |
| crash-start on the decoder on the pushed revision | 2 |
| crash-start on the pushed revision | 18 |
| crash-start once on the decoder on the pushed revision | 2 |
| crash-start once on the pushed revision | 12 |
| crashloop load after-settle before the push | 24 |
| crashloop load before the push | 24 |
| crashloop start after-settle before the push | 24 |
| crashloop start after-settle before the push on the decoder | 2 |
| crashloop start after-settle before the push on the router | 2 |
| crashloop start before the push | 24 |
| crashloop start before the push on the decoder | 2 |
| crashloop start before the push on the router | 2 |
| imagepull after-settle before the push | 24 |
| imagepull before the push | 24 |
| imagepull on the pushed revision | 18 |
| imagepull once on the pushed revision | 12 |
| readiness persist before the push | 24 |
| readiness recover before the push | 24 |
| stuck on the pushed revision | 18 |
| stuck once on the pushed revision | 12 |

### By break victim

| value | cases |
|---|---|
| (no break) | 5080 |
| decoder | 166 |
| engine | 1630 |
| router | 116 |

### By migration mode (node family)

| value | cases |
|---|---|
| Auto | 52 |
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
| failed | 98 |
| held | 136 |

### By operation on the stuck Component (ops-held)

| value | cases |
|---|---|
| canary | 51 |
| delete | 248 |
| down1 | 124 |
| down2 | 28 |
| edit | 114 |
| move | 42 |
| push | 156 |
| rollback | 114 |
| runtime | 114 |
| scale | 36 |
| scale-down | 60 |
| scale-one | 16 |
| scale-up | 60 |
| scaledown | 85 |
| scaleup | 37 |
| steady | 36 |
| toone | 28 |
| up1 | 118 |

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
|  | 248 |
| after-hold | 84 |
| after-recover | 48 |
| after-settle | 476 |
| after-window | 84 |
| never | 232 |
| quotalift | 5 |

### By window operation (webhook and conflict families)

| value | cases |
|---|---|
| conflict canary | 12 |
| conflict delete | 12 |
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
| webhook canary | 12 |
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

### By operation under a pre-armed cause or a hook fault (pending, evict, preempt, node-hook and ctrlrestart-hook families)

| value | cases |
|---|---|
| ctrlrestart delete | 6 |
| ctrlrestart edit | 6 |
| ctrlrestart idle | 6 |
| ctrlrestart rollback | 6 |
| ctrlrestart runtime | 6 |
| ctrlrestart scaleup | 6 |
| evict delete | 6 |
| evict edit | 12 |
| evict rollback | 12 |
| evict runtime | 12 |
| evict scaledown | 12 |
| evict scaleup | 6 |
| node canary | 12 |
| node delete | 12 |
| node edit | 12 |
| node push | 12 |
| node rollback | 12 |
| node runtime | 12 |
| node scaledown | 12 |
| node scaleup | 12 |
| pending edit | 24 |
| pending rollback | 24 |
| pending runtime | 24 |
| podkill delete | 6 |
| podkill edit | 12 |
| podkill rollback | 12 |
| podkill runtime | 12 |
| podkill scaledown | 12 |
| podkill scaleup | 6 |
| preempt edit | 6 |
| preempt rollback | 6 |
| preempt runtime | 6 |
| preempt scaledown | 6 |
| preempt scaleup | 6 |
| quota canary | 9 |

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
| canary | 51 |
| delete | 248 |
| down1 | 124 |
| down2 | 28 |
| edit | 114 |
| move | 42 |
| push | 156 |
| rollback | 114 |
| runtime | 114 |
| scale | 36 |
| scale-down | 60 |
| scale-one | 16 |
| scale-up | 60 |
| scaledown | 85 |
| scaleup | 37 |
| steady | 36 |
| toone | 28 |
| up1 | 118 |

### By one-shot crash (gen-crash-once)

| value | cases |
|---|---|
| load on the pushed revision under a push | 24 |
| load on the pushed revision's decoder under a push | 4 |
| load on the pushed revision's router under a push | 4 |
| load on the running revision at idle | 12 |
| load on the running revision under a push | 24 |
| load on the running revision's decoder at idle | 4 |
| load on the running revision's decoder under an engine push | 4 |
| load on the running revision's router at idle | 4 |
| load on the running revision's router under an engine push | 4 |
| start on the pushed revision under a push | 24 |
| start on the pushed revision's decoder under a push | 4 |
| start on the pushed revision's router under a push | 4 |
| start on the running revision at idle | 12 |
| start on the running revision under a push | 24 |
| start on the running revision's decoder at idle | 4 |
| start on the running revision's decoder under an engine push | 4 |
| start on the running revision's router at idle | 4 |
| start on the running revision's router under an engine push | 4 |

### By ledger fault row (gen-lift)

| value | cases |
|---|---|
| config missing, one-shot, new | 12 |
| config missing, one-shot, running (fix push) | 12 |
| config missing, persistent, new | 12 |
| config missing, persistent, running | 12 |
| config missing, persistent, running (fix push) | 12 |
| image pull, one-shot, new | 12 |
| image pull, one-shot, running | 12 |
| image pull, one-shot, running (push) | 12 |
| readiness never passes, one-shot, new | 12 |
| readiness never passes, one-shot, running (fix push) | 12 |
| readiness never passes, persistent, running (fix push) | 12 |

### By move moment (fault-move)

| value | cases |
|---|---|
| accept | 223 |
| drain | 66 |
| surge | 126 |

### By move victim (fault-move)

| value | cases |
|---|---|
| any | 18 |
| moved | 267 |
| peer | 72 |
| sibling | 58 |

### By runtime edit (fault-edit)

| value | cases |
|---|---|
| env | 130 |
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
| ctrlrestart x3 under the canary | 6 |
| ctrlrestart x3 under the delete | 6 |
| ctrlrestart x3 under the move | 6 |
| ctrlrestart x3 under the roll under its edit | 6 |
| ctrlrestart x3 under the rollback | 6 |
| ctrlrestart x3 under the runtime edit | 6 |
| ctrlrestart x3 under the scale-down | 6 |
| evict x3 at steady state | 18 |
| evict x3 under a push | 12 |
| evict x3 under a scale-up | 6 |
| podkill x3 at steady state | 18 |
| podkill x3 under a push | 24 |
| podkill x3 under a scale-up | 18 |
| podkill x3 under the roll under its edit | 6 |
| podkill x3 under the rollback | 6 |
| podkill x3 under the runtime edit | 6 |
| podkill x3 under the scale-down | 6 |
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
| engine gang | 66 |
| engine single | 77 |
| engine+decoder+router gang | 74 |
| engine+decoder+router single | 109 |
| engine+router gang | 74 |
| engine+router single | 114 |

### Ids kept empty (cells of the matrix that are no case)

| id | folder | cell | reason |
|---|---|---|---|
| p10460 | gen-conflict | conflict delete engine N=4 once on isvc | a write on the ISVC as the delete lands has no window: the controller removes the ISVC finalizer in the reconcile that sees the deletion, so every write after it is refused NotFound; the InferenceReplicas outlive their pods under their own finalizer and are the only object a competing writer reaches during a teardown |
| p10461 | gen-conflict | conflict delete engine N=4 persist on isvc | a write on the ISVC as the delete lands has no window: the controller removes the ISVC finalizer in the reconcile that sees the deletion, so every write after it is refused NotFound; the InferenceReplicas outlive their pods under their own finalizer and are the only object a competing writer reaches during a teardown |
| p10492 | gen-conflict | conflict delete engine-mn W1 N=2 once on isvc | a write on the ISVC as the delete lands has no window: the controller removes the ISVC finalizer in the reconcile that sees the deletion, so every write after it is refused NotFound; the InferenceReplicas outlive their pods under their own finalizer and are the only object a competing writer reaches during a teardown |
| p10493 | gen-conflict | conflict delete engine-mn W1 N=2 persist on isvc | a write on the ISVC as the delete lands has no window: the controller removes the ISVC finalizer in the reconcile that sees the deletion, so every write after it is refused NotFound; the InferenceReplicas outlive their pods under their own finalizer and are the only object a competing writer reaches during a teardown |
| p10524 | gen-conflict | conflict delete engine+router N=4 once on isvc | a write on the ISVC as the delete lands has no window: the controller removes the ISVC finalizer in the reconcile that sees the deletion, so every write after it is refused NotFound; the InferenceReplicas outlive their pods under their own finalizer and are the only object a competing writer reaches during a teardown |
| p10525 | gen-conflict | conflict delete engine+router N=4 persist on isvc | a write on the ISVC as the delete lands has no window: the controller removes the ISVC finalizer in the reconcile that sees the deletion, so every write after it is refused NotFound; the InferenceReplicas outlive their pods under their own finalizer and are the only object a competing writer reaches during a teardown |
| p10556 | gen-conflict | conflict delete engine-mn W1+router N=2 once on isvc | a write on the ISVC as the delete lands has no window: the controller removes the ISVC finalizer in the reconcile that sees the deletion, so every write after it is refused NotFound; the InferenceReplicas outlive their pods under their own finalizer and are the only object a competing writer reaches during a teardown |
| p10557 | gen-conflict | conflict delete engine-mn W1+router N=2 persist on isvc | a write on the ISVC as the delete lands has no window: the controller removes the ISVC finalizer in the reconcile that sees the deletion, so every write after it is refused NotFound; the InferenceReplicas outlive their pods under their own finalizer and are the only object a competing writer reaches during a teardown |
| p10588 | gen-conflict | conflict delete pd N=4 once on isvc | a write on the ISVC as the delete lands has no window: the controller removes the ISVC finalizer in the reconcile that sees the deletion, so every write after it is refused NotFound; the InferenceReplicas outlive their pods under their own finalizer and are the only object a competing writer reaches during a teardown |
| p10589 | gen-conflict | conflict delete pd N=4 persist on isvc | a write on the ISVC as the delete lands has no window: the controller removes the ISVC finalizer in the reconcile that sees the deletion, so every write after it is refused NotFound; the InferenceReplicas outlive their pods under their own finalizer and are the only object a competing writer reaches during a teardown |
| p10620 | gen-conflict | conflict delete pd-mn W1 N=2 once on isvc | a write on the ISVC as the delete lands has no window: the controller removes the ISVC finalizer in the reconcile that sees the deletion, so every write after it is refused NotFound; the InferenceReplicas outlive their pods under their own finalizer and are the only object a competing writer reaches during a teardown |
| p10621 | gen-conflict | conflict delete pd-mn W1 N=2 persist on isvc | a write on the ISVC as the delete lands has no window: the controller removes the ISVC finalizer in the reconcile that sees the deletion, so every write after it is refused NotFound; the InferenceReplicas outlive their pods under their own finalizer and are the only object a competing writer reaches during a teardown |

## Constraint ledger (tuples excluded during construction)

- `pause` multiplies `mode=kill` and the pause-and-repair family (`pauseat=`); run.py consumes the pause header nowhere else, and a rollout case that pauses without `pauseat=` is rejected as a dead knob.
- `pause=freeze` pairs only with `RecreateInstanceOnPodRestart`: the freeze lift downgrades to a plain pause, which parks policy None's Create recovery — freeze+None could never repair while paused.
- In-place strategies on single-pod engine-only topologies are excluded from gen-fault: the component-blind podkill victim is the in-place pod itself and its recreate trips the UID-stability check by harness design (a missing component-scoped victim hook, see below).
- `fault=podkill:migrating` needs `mode=migrate` — out of scope here.
- A rollback over the key kept away on the running revision fires at `settling` and `rotating` only: the roll takes the broken Instance first and its replacement cannot start without the key, so no pod of the starting revision ever drains and neither the draining nor the last hook arrives; the kept-key row takes those two moments at `rotating` under their own strategy and group.
- A router in front of an engine fleet is a break victim under a canary alone: elsewhere the runner admits engine and decoder pods, so the sibling rows of the engine-behind-a-router shapes outside a canary (a move, a runtime edit, a push, the idle service) have no cell and the router is a negative control there.
- Kill targets (`kill=leader|worker`) exist only on gang topologies.
- Update budgets are dormant under a steady-state kill: the kill family permutes strategies at their canonical budget only.
- crashloop breaks an engine pod, so its coordination axis (co=none | co=rolling) exists only on the PD topologies; maxUnavailable=0 is emitted for SurgeThenDrain only, percent budgets run at the mid size where the ceil() rounding differs from the integer case, and both restart policies run under both crash forms.
- A mid-roll edit (gen-switch) is a merge patch of the lifecycle's type or policy, so the declared rollingUpdate budget must be valid under both strategies of a pair: (maxSurge 1, maxUnavailable 1) whenever SurgeThenDrain is on either side, (0, 1) otherwise. The header is stated for the worse of the two shapes: the ceiling is N+1 whenever a surge step can be in flight on either side of the edit, the floor N-1 is the one the drain-first side needs and stays hard. A strategy flip carries no restart policy; the restart-policy flips run under the canonical surge budget (floor N). The switch family runs one integer budget per pair and one gang width per topology, and its crashing variants pair the surge and recreate strategies only, at the rotating hook, with form=start. Its continuation puts a break beside the edit on the six shape families: a running-revision break landed before the push (fault=crashloop:idle, imagepull:idle, configkey:missing, readiness:old; the :old crash form beside an edit stays the break armed as the bump lands) meets the edit at a moment of its own, and a pushed-revision break (break=<kind>, once=1 for its one-shot form) meets it once the roll is parked or at the first pushed pod; the capacity floor is lowered by the broken Instance, not waived.
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
- ctrlrestart has no hook inside a repair (one fault per case; the idle-break handler fires no second one), so a restart while a crash-loop repair is open is not emitted. The strategy axis is live for the two rollout conditions and dormant under a migration and a scale-down, where both strategies still run for the family's symmetry; a rolling group is crossed in on the PD rollout conditions only. The hook half (from p4668) deletes the manager as a rollback, a strategy edit, a scale-up and a delete land and at the first pod of a runtime edit's new revision, on the six ledger shapes under the surge strategy (the edit's roll switches to RecreatePod): the restart loses no pod, so the floor and the order are judged in full and the operation must complete with nothing created or deleted twice. Its persistent form is the repeating family's.
- The gen-canary-* families drive operator stories through a canary at small size: 8 single-pod Instances, SurgeThenDrain with the engine surge at 1 and 2, a four-step ladder (10/30/60/100 percent capacity and traffic) with a timed soak in place of an analysis gate, on the engine and prefill/decode shapes only (the stories run single-pod Instances; the gang shapes are the crashloop family's).
- gen-canary-crash breaks the stable side at the first split (canary-serving@0): every break kind and form, both restart policies, 1 and 3 victim Instances at once. The floor is read with the victims allowed for, not waived.
- gen-canary-second-rev lands the second revision as an ISVC re-bump or a runtime edit (a dedicated runtime per case), at staging and at the first split, with and without an analysis gate on every step before the cutover; the restart policy is None (a re-target repairs nothing).
- gen-canary-podkill deletes one old-revision engine pod at a manual gate, under the operator's pause, under a capacity gate held by an image that cannot start, and under that gate paused; the engine surge is 1 (the budget is dormant while the roll is held).
- gen-canary-fault-old breaks the stable side at the step kinds that differ in mechanism: before the push (the push meets a broken fleet), the capacity step ahead of traffic, the second step's timed soak, the same step at a manual gate (promoted 30s after the break), the same step at an analysis gate, and the last gate before the cutover. Left out, with the reason: the first split, which gen-canary-crash drives with the same faults, victims and policies; the cutover itself (Promoting), where at most the surge budget of stable Instances is left and already draining, so a break has no settled row to land on (the last gate carries that claim); a traffic step apart from its soak, since every traffic step of the story ladder but the cutover soaks; the surge budget at the gates, where no capacity builds while they hold (surge 1 only); three victims at the manual gate, whose storm is the first split's under another release; the crash forms under the analysis gate, since the metrics image is a static binary with no shell to exec a kill into and no fixture image fails to start under its entrypoint (the pull failure alone breaks it); the analysis gate crossed with the other step kinds, since the gate changes only what ends a hold, not how a break meets it; and a missing ConfigMap key, whose reference both revisions share, so a push is never its fix (the idle configkey family lifts the cause instead).
- gen-canary-fault-new arms the break on the pushed engine revision and crosses where it bites (a crash at start and a pull failure at staging; a crash after Ready inside the first soak and past it) with the exit at the park (a rollback, or a fix push that drops the broken runner), both shapes and both restart policies. Left out, with the reason: the surge budget where the ladder parks at its first capacity step, which asks one Instance (surge 1 only); the crash forms under the analysis gate (the metrics image carries no shell and no exit switch); the rollback exit under the analysis gate, whose terminal verdict requires an analysis sample a park before the first gate never yields (the fix exit walks the gated ladder); a missing ConfigMap key, which no verb arms on a template and whose bite is a container that never starts, the crash-at-start cell; and a manual policy, since a broken revision parks before any gate is reached.
- The further rows of gen-canary-fault-old (continued at p11500), gen-canary-fault-new and gen-canary-podkill run the engine unit's ladder on the ledger's six shape families (the gate family's shapes). The stable side: a serving pod that stops passing readiness, kept or lifted; the key put back once its wait has shown; a crash and a pull failure taken away again; and the runner of one Ready stable pod killed exactly once, before the push (single-pod fleets), at the second step's soak and at the same step's manual gate. The pushed revision: a missing key and a revision that never passes readiness under both exits at the park; the key added, the switch lifted and the image put back once they have bitten; the runner of the first canary pod killed once at start and after Ready under both restart policies; and a taint on every node armed before the push, lifted after the wait has shown or kept to the park. The pod faults: an old-revision engine pod (a gang member on the gangs) deleted again each time it is back, and evicted once under a soak, once at a manual gate held until the Instance is whole, and again each time it is back. The running revision's key kept away before the push, on every shape family: both revisions read it, so the push is never its fix; the stable-side break does not hold the ladder, the pod placed for the first capacity step waits on the same key, and the ladder parks at that step naming the missing key, the stable side still serving and no traffic reaching the step whose pod never came up. The kept crash and pull failure, which the storm families drive on the single-pod engine and prefill/decode fleets, run on the gangs and the engine-behind-a-router shapes too: one stable Instance crashing at start or after Ready under both restart policies, or sitting in ImagePullBackOff, at the second step's soak; and the pushed engine revision crashing at start, crashing inside the first soak or unpullable, under both exits at the park. Left out, with the reason: one kill at start on the running side at a grid state (a running pod has no first start to catch; its one-shot form is the lifted crash); a preemption at a grid state (its press runs in the pending driver's own watch; the eviction fills the ledger row); the idle position on the gangs (as the gate family: a fleet of two reads a gang as a single pod); the manual gate for a lifted crash or pull failure (the gate's hold after a break is the fault-old gate step's); the readiness rows under RecreateInstanceOnPodRestart (readiness loss opens no repair under either policy); and the one-shot pushed breaks under RecreateInstanceOnPodRestart (a pod that never started has no restart to read).
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
- gang-member cells at steady state are the mode=kill cells gen-restart already emits (kill=leader|worker x restart policy x strategy); this family adds the mid-roll timings, each bound to the gang it aims at (settling: a settled gang; rotating: the gang forming on the new revision; draining: a serving gang beside the first one leaving), and, from p7172, a serving gang's leader deleted under restart policy None as a rollback, a strategy edit, a scale up or down and a delete land and at the first pod of a runtime edit's new revision, on the gang ledger shapes; gen-fault carries the same moments on the single-pod shapes from p1520 (the operation still completes, the replacement is labelled as what it runs, a delete takes the deleted pod with the rest), and gen-repeat the persistent form.
- tight: RecreatePod under the quota squeeze has no lift axis — the drain frees the gang's own room, so the roll completes with no wait, and the cell asserts that instead.
- evict has no lift: the Eviction API is a one-shot removal, so the family crosses topology x restartPolicy (x strategy under the roll) only. Its steady-state cells are the kill verdict with the removal done by the Eviction API; the product's disruption budget (`podDisruptionBudget.omeNative` in the chart) may refuse it while a pod is already down, the runner asks again as a drain does, and a budget that never admits one eviction fails the case.
- preempt and pending cross `lift=after-settle|never` (the cause removed once the wait has held, or kept to the end so the hold is judged), run on one gang width, with `co=none` (a group changes nothing about where a pod can be placed) and under the two strategies a roll waits under (in-place strategies create no pod to place). Both verbs need a cluster the harness owns (every node is blocked); elsewhere the case is SKIP with the reason. The preempt roll cells waive the floor and the order (the preemption takes a pod the budget cannot absorb); the pending verdict keeps its own floor.
- a pending cell's quota admits one pod fewer than the operation needs: `quota:0` for a single-pod surge, `quota:<members-1>` for a surge gang (room for all but one member), and `quota:-1` (one pod fewer than run) under a repair or a drain-first roll, which create after a pod is gone; on a gang under RecreateInstance that leaves the whole gang short of one member. A gang surge roll held to the end by that quota is the tight-cluster family's cell, so the pending family leaves its `quota:<members-1> lift=never` cell out. A quota with no room is also armed before a canary push on the shapes the hand-written canary cases do not run (minted after the other rows): kept, the capacity step parks the ladder Failed at its ready timeout; lifted once the ladder has parked, the capacity lands, the park holds and a resume walks the ladder to Stable.
- under a rollback, a strategy edit, a runtime edit, a scale and a delete the three families run one case per ledger cell under restart policy None: a taint or a quota armed before a rollback (drain-first, the rollback at its draining hook: the roll's first replacement is what waits, and a roll that cannot place its first pod reaches no later hook; the surge roll under the same cause never drains, so it offers no rollback moment), before a strategy edit landed as the push lands and before a runtime edit; an eviction as each operation lands, once and, where what it took comes back, again (a delete brings nothing back, so its cell is one-shot; a scale-up's persistent form is the repeating family's); a preemption as each operation lands, aimed at a pod the operation places or, under a rollback at its first surge pod and a scale-down, at a settled Instance it keeps, the nodes opened again once the wait has held (a delete places nothing until its recreate, so it meets the eviction alone).
- gen-budget drives the percent budgets the engine resolves: maxSurge under SurgeThenDrain and maxUnavailable under RecreatePod at 10%, 25%, 50% on fleets of 4 and 8 Instances, for a plain push, a push with one engine Instance crash-looping on the running revision (form=start, restart policy None: the roll owns the broken Instance under either policy) and the story families' four-step auto canary. The engine resolves a percent budget as N x percent rounded up and clamped to N, for both budgets alike (the Deployment convention of rounding maxUnavailable down is not its rule), and every case states the rule with its own numbers: at 10 percent both fleets round up to 1, where rounding down would leave 0 and no roll could start, so the roll converging is the proof; at 25 and 50 percent the ceiling or floor scales with N. Left out, with the reason: the in-place strategies, whose maxUnavailable the drain-first cells already read and whose own family is gen-inplaceonly; the prefill/decode gang at 8 Instances, a 34-pod fleet that would cost a lane shard per case while the rounding is a function of N and the percent, which the single-pod cells at 8 carry; the crash form load and the restart policy under the crash loop (gen-crashloop crosses both at 25 percent); and the gangs under the canary beyond the engine gang at 4 (the pod budget of a canary lane). The capacity floor is waived on the crash-loop cells as on every crash-loop case; the ceiling stays hard.
- gen-inplaceonly drives InPlaceOnly on what sets it apart. An image change is the one diff it patches onto the live pod: on a single-pod Instance the roll lands in place with UIDs stable; a gang runs every in-place strategy as a whole-gang recreate (eligibility reads the leader's spec alone), so it is rebuilt whole and never refused. An env, a resources or a probe change exceeds container images: on a single-pod Instance the push is refused (`expect=held`: nothing is recreated, patched or surged for the hold window, an InPlaceUpdateNotPossible event says why, then the corrective push drops the override and lands in place); on a gang it is a whole-gang recreate. A crash loop at idle is repaired by a rebuild, since a repair is not an update (form=load under restart policy None is left out, as in every idle family: no repair is promised there). A crash loop under a push belongs to the roll: an image push patches the crashing pod itself and it serves again; an env push is refused, the repair yields to the roll, and the Instance waits broken for the corrective push, which patches it in place. One fleet size (4), both restart policies everywhere (live under the crash loops, a negative control under the pushes), the canonical budget maxUnavailable 1.
- gen-fault-scale and gen-fault-delete break Instance 0 of the settled service, the lowest index, so a scale-down's natural victim (the highest index) and the broken Instance differ and the order of removal can be told apart. The engine runs under SurgeThenDrain with maxSurge 1 and maxUnavailable 1 in both families: nothing rolls, so the strategy is dormant, and the unavailability budget of one admits the repair that takes the broken Instance offline (the canonical budget of zero holds it). Readiness loss is repaired under neither restart policy, so its policy axis is collapsed to None; a crash at start, a crash after Ready and a pull failure run under both, and each scale-up cell states on line two what becomes of the broken Instance (expect=repaired|left) and why. On the gang fleet of two only down by one and up by one are emitted: to one is down by one there, and down by two asks for zero, which the API refuses (minReplicas 0 needs KEDA autoscaling). A rolling group is crossed in where a decoder exists; the delete family keeps the same axes. The quota is also kept to the end under the scale-up and the delete (minted after the other rows): the new Instance's row, or a row of the recreate, waits with its reason across the hold window and nothing settled is taken down to make room.
- gen-fault-move fires the fault at a moment of a move (at=accept, the request recorded; at=surge, the replacement pod up; at=drain, the source going) on the moved Instance (its source pod at accept, its replacement at surge and at drain), on a peer (the highest other Instance) or on the decoder (victim=sibling, PD only). Left out, with the reason: pd-mn (the move copies a whole gang whatever the decoder does; the gang axis is engine-mn's, the sibling axis pd's); the update strategy (dormant: no roll is in flight, a move surges whatever the strategy and a repair rebuilds in place); a rollout group (a repair of an Instance that serves nothing is admitted without its gate; the group axis is the steady-state crash-loop family's); the restart policy on a crash at start and a pull failure (both reach the crash-loop repair under either policy, so they run under None) while the after-Ready form runs under RecreateInstanceOnPodRestart, the one policy that promises its repair; a node loss anywhere but the source node (the story), at drain (the source is already going, its node dying then is the sweep of a terminating pod, which the surge moment reads with the replacement in place), and on the lane without the force-delete policy (a source pod on a silent node is never freed, so the move cannot finish; that fate is the node family's parked cell); the readiness switch (fault=readiness:old is repaired under neither restart policy, so it is the broken-Instance families' break and is not fired at a move moment as a break the product repairs). The family's later rows put the same request beside the remaining faults: a crash or a pull failure healed again once it has bitten (lift=after-settle) and a runner killed once on the replacement (once=1); the missing key put back or kept away and the readiness switch lifted or kept (victim=moved|peer, the decoder as sibling); a pod deleted by hand at the request, a gang member under kill=, again under repeat=; the moved Instance's pod evicted at each moment, again under repeat= at surge, and preempted as the replacement appears; a NoSchedule taint before the request; and a move on a push stuck on an unpullable image (held and failed), a crash at start, a crash after Ready, a missing key or a readiness that never passes (held), the decoder's push stuck beside the engine's move on the prefill/decode shapes. Left out: the router's push stuck beside a move (the router is bumped by no group, so a stuck router push has no driver) and the missing key or the readiness switch on the router (its image carries neither the fixture's ConfigMap env nor the readiness switch). The request also meets a quota with no room (fault=quota:0 expect=fail, the bounded- failure contract of the hand-written fault cases; lift=1 lifts it after the failure and a fresh request completes) on the shapes those cases do not run, minted after the other rows.
- gen-fault-edit edits the dedicated runtime every Component inherits (engine, decoder and router each roll onto their own revision) under the surge and the drain-first strategies, and breaks a pod of the running revision on the engine or the decoder as the edit lands; its bad-edit half pushes an image that cannot be pulled or a readiness that never passes and then the corrective edit. Its further rows run one case per ledger cell under the surge strategy and the env edit: a break of the running revision landed on the settled service before the edit (a crash at start, after Ready and a pull failure healed once the edit has fired, a readiness loss kept and lifted, the key put back and the key kept away, which parks the roll with its reason; a sibling's crash after Ready healed), and a break the edited runtime itself carries (a crash at start, after Ready and a missing key kept until the corrective edit, and each kind plus a pull failure and a readiness loss once on the first pushed pod; a sibling's crash at start kept and once). Left out, with the reason: the in-place strategies (the switch family's); the env edit beyond the engine shape (under these strategies both edits are one new revision; the pod-template difference matters to an in-place strategy alone); the restart policy on a crash at start and a pull failure (they open no runner-restart trigger, so the roll owns the Instance either way; the after-Ready form runs under both); any restart policy but None on the bad edit (a revision that never becomes Ready restarts no runner); a rollout group, as in fault-move; the restart policy on the further rows (one policy per cell). The readiness-that-never-passes cell asserts the fix and the floor and reports whether the status said why: nothing escalates a pod that runs but never passes readiness before its ready timeout.
- gen-repeat is the persistent form of a pod deleted by hand, a gang member lost, an eviction or a preemption and a manager restart: the fault is applied again each time what it took has come back (3 times; 2 for the preemption, whose fill-and-press cycle is the slowest), then stops, and the case reads the window and the end state after the lift. It runs on the six shape families at steady state (mode=kill; the manager on the idle service), under a push with the surge and the drain-first strategy and under a scale-up by one; the manager is deleted again each time it is Ready under a rollback, a strategy edit, a runtime edit, a scale-down, a move, a delete and a two-step canary too (from p8826), the window held open until the restarts are spent so the later ones land on the operation's settled end, and the pod (a gang's worker under the rebuild-whole policy) again under a rollback, a strategy edit, a runtime edit and a scale-down (from p8868); a delete brings nothing back, so the pod deleted as a delete lands has the one-shot form only. The loop takes the same Instance again once it is back (the deleter hits the pod that came back), the running revision's pods under the push (victim=stable), and the engine on the prefill/decode shapes (victimcomp=engine), so the fault and the operation meet on one Component. The kill cells run at the canonical surge budget, dormant at steady state, so the floor reads N minus the killed Instance. Left out, with the reason: a gang member lost under a push with restart policy None (the one-shot gang-member family drives it; the repeating loss stresses the rebuild-whole path) and the eviction loop under a push or a scale with RecreateInstanceOnPodRestart (the policy only widens what is rebuilt, which the steady-state eviction cells cross; the eviction's own question is the disruption budget admitting each removal); the preemption loop under a push (a repeating preemption needs the kill's fill-and-press cycle, which the roll handler does not run) and with lift=never (nothing comes back while the fillers stay); the manager restarting while a repair is open, as in the one-shot family. The cluster-wide forms (the manager restarts, the preemptions) run one at a time after the pool, as every such case does.
- gen-webhook points the admission webhook's configurations at a Service that does not exist on the settled service, as a push lands and as a scale-up lands, for a bounded time (lift=after-hold: pointed back once the status has said why a write waits, or after the settle window at idle) or across the hold window (lift=after-window), on every shape family of the coverage ledger at one gang width (the outage acts on the controller's writes, not on a gang's members). The push runs under the surge and the drain-first strategies: the outage refuses the pod a surge creates before anything is torn down, and the one a drain-first roll creates after a pod is gone; the steady state and the scale are dormant to the strategy. No rollout group: the engine is the only bumped member. The in-place strategies are left out: an annotation-only diff is patched onto the live pod and creates nothing for admission to refuse. A webhook case runs alone (the outage is cluster-wide: every ISVC write in the cluster is refused while it holds). The outage also opens at the push's later hooks (the first pod of the new revision, the first pod of the old one on its way out, under both strategies), as a rollback filed at the first new pod lands, as a strategy edit landed there lands, as a scale-down by one lands, as an env edit of the case's dedicated runtime lands (every Component rolls), as soon as a move's request is recorded, and as a delete lands (the same name is re-created once the window has closed); each reads the same window verdict, and the operation must complete once the webhook is back. It also opens while the second step of the engine unit's ladder soaks its split, on the six shape families as the gate family runs them: the step waits with a reason while admission refuses, the ladder completes once the webhook is back and its step index never goes back.
- gen-conflict runs a competing writer (an annotation patch of the ISVC or of every InferenceReplica of the service; target=isvc|ir) once or until the operation completes (form=once|persist) over the same operations, hooks and shapes, under the canonical surge budget alone: a status write conflicts the same way under every strategy. The writer is per service, so its cases run in the concurrent pool. The reconcile counters are read on the webhook cases only (they run alone); a conflict case bounds the controller's events on its own objects. The writer also runs over the engine unit's ladder at the second step's soak on the six shape families (one write, or writes until the ladder ends; the ISVC as the target): the status write is retried and the step moves once, never back.
- The plain operations on the shapes the hand-written folders do not run, one case per ledger cell with no fault, land in the family that owns the operation as the control row its faulted rows are read against, minted after every other row: a push on the engine behind a router (gen-rollout); a rollback and a strategy edit on both engine-behind-a- router shapes (gen-rollback, gen-switch); a runtime edit and a delete on the router shapes and the prefill/decode gang (gen-fault-edit, gen-fault-delete); a scale up and a scale down on the engine behind a router (gen-fault-scale); a move on both router shapes (gen-fault-move).
- The single cells the shape axis left on the engine-behind-a-router shapes, one case each in the family that owns the verb and minted after every other row: the node lost once and for good at steady state (gen-node); the key put back once its wait has shown, and a pull failure of the running revision at steady state and as a push lands (gen-heal, gen-imagepull); a pod deleted by hand under a push on the single-pod shape and a gang leader lost under a push on the gang (gen-fault, gen-gang-member); a stable pod or gang leader deleted once at the first split of a canary on the engine gang and both router shapes (gen-canary-podkill); and a rollback at the held moment of a push that never passes readiness (gen-rollback).
- The coverage ledger (coverage.py, `EXCLUDED_CELLS`) removes a cell of its product only with a written reason, and a cell counts under the first rule that matches; beside the four shape rules, four rules remove 1068 cells. The pause and freeze columns outside the kill family's own cells (540): a pause or a freeze is a moment of another cell, not an operation a fault crosses: the cell is proved by pausing while the operation and the fault are in flight, or by the fault landing while paused, which the ledger records as the `paused` timing and the operation+operation overlap of the fault's own cell.
- The new-side rows at steady state (80): a fault on the new revision has no meaning under an operation that creates no new revision.
- The metric-breach and analysis-source rows outside a canary (312): a metric breach and an unusable analysis source are read by a canary step's analysis gate alone; no other operation reads the health metric or the analysis source.
- The one-shot new-side rows under a scale, a move and a delete (136): a one-shot fault on the new revision is over before a scale, a move or a delete can be aimed at it: what the operation then meets is a plain roll, which is the operation+operation overlap, and a new revision that stays broken is the persistent row.
- A competing write on the ISVC as a delete lands (gen-conflict, `delete` on target=isvc) has no window: the controller removes the ISVC finalizer in the reconcile that sees the deletion, so every write after it is refused NotFound; the delete window runs against the InferenceReplicas alone, which outlive their pods under their own finalizer, and the ISVC cells' ids are kept empty (the table above) so the ids after them keep their places.
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
- `lift=after-failed`: the same repair opens, and the rebuilt pod's wait outlives the stuck-pod grace (or the attempt its `instanceReadyTimeout`, `HEAL_FAILED_READY_TIMEOUT`), so the escalation parks the repair at Failed with its Restart kept. A parked repair keeps two exits of its own: the restart pass resumes its promote when the rebuilt set comes up, and `lifecycle.updateRetry` re-arms it for a cause a fresh pod can clear (a crash loop, a run-container error, a deadline with no reason). A park whose cause a fresh pod cannot clear (a missing ConfigMap key, an unpullable or invalid image) owes no re-arm: the row stays Failed with its reason until the config or image is fixed, and recovers by itself when it is. The key returns once the row reads Failed, the kubelet's retry brings the rebuilt set up, and the resume promotes the Instance on its running revision with no reset mailbox and no new revision, whether or not a ladder is configured.

## gen-lift: what each form reads

One family fills the six ledger rows the other families left empty; every cell runs live. A kept cause (`lift=never`) is the persistent form, a lifted one (`lift=after-settle`) the one-shot form.

- `config missing, persistent, running`: `configkey:missing during=idle lift=never expect=failed`; the row reaches Failed with its reason inside the `instanceReadyTimeout: 90s` and, since a fresh pod cannot clear a missing key, is owed no re-arm: it stays Failed with its reason and nothing of the Instance serving across the hold, until the key is put back; both restart policies (which repair opens first differs).
- `config missing, persistent, new` and `config missing, one-shot, new`: `configkey:new` with `lift=never` (the roll holds at the ready timeout with its reason; a fix push back onto the present key lands) or `lift=after-settle` (the key is added and the roll completes with no hold behind it). The surge and the drain-first strategies decide which pod waits and the floor; one restart policy, since a pod that never started has no restart for a policy to read.
- `readiness never passes, one-shot, new`: `readiness:new form=recover`; the switch file is set in the push's first engine pod as it starts and lifted once the drop has settled; the same pod returns and no restart policy may rebuild it. Both restart policies under SurgeThenDrain (the surge pod is the first pushed pod and takes no capacity while it waits; drain-first is the same story at floor N-1).
- `image pull, one-shot, running`: `imagepull:old during=idle lift=after-settle expect=heals`; the image is pointed back once the pull failure has settled; the kubelet or a repair brings the Instance back, both restart policies.
- `image pull, one-shot, new`: `imagepull:new lift=after-settle`; the push's first engine pod is pointed at the unpullable image as it starts and back once the failure has settled; one rebuild may be reported under RecreateInstanceOnPodRestart, never required, never twice; both restart policies under SurgeThenDrain.
- `config missing, persistent, running (fix push)` and `config missing, one-shot, running (fix push)`: `configkey:missing during=idle lift=never|after-settle push=fix`; the key is removed on the running revision and, once the wait has shown for the settle window, the fix is pushed with no pause around it: a bump that gives the runner a literal in place of the key it cannot read; under lift=after-settle the key is put back as the fix lands. The roll lands on every Instance, the broken one Ready on the new revision and never the last, with no repair recorded for it after the push; both restart policies.
- `readiness never passes, persistent, running (fix push)` and `readiness never passes, one-shot, running (fix push)`: `readiness:old during=idle form=persist|recover push=fix`; a serving pod stops passing readiness and, once the drop has shown for the settle window, an ordinary bump is pushed, whose re-rendered pods carry no switch file; under form=recover the switch is lifted as the fix lands. The same roll verdict; both restart policies.
- `image pull, one-shot, running (push)`: `imagepull:old lift=after-settle` with no during=; the running revision's pod is pointed at the unpullable image as the bump lands and back once the failure has held for the settle window, unless the roll has replaced it first; the roll owns the Instance either way; both restart policies.
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

## Deferred fill

`--fill` (t=3 covering array over coordination groups, canary promotion, and budget interactions) is documented in the generator and intentionally not emitted until the hot core has baked in the per-PR lane.
