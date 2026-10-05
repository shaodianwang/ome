#!/usr/bin/env python3
"""generate_cases.py — the generated hot-core permutation corpus (isvcs/gen-*/).

Emits the constraint-filtered cross product of
    topology x updateStrategy x failure verb x injection phase x
    restartPolicy x pause
into the generator-owned folders below, using ONLY verbs and hooks the
runners (run.py + the KIND CI lane) already implement:

    gen-rollout/   p1000+  baseline revision-bump rollouts (failure=none)
    gen-fault/     p1200+  rollouts with fault=podkill:settling|rotating;
                          from p1520, a serving pod deleted by hand as a
                          rollback, a strategy edit, a scale up or down and a
                          delete land and at the first pod of a runtime
                          edit's new revision, on the single-pod ledger
                          shapes
    gen-restart/   p1600+  mode=kill (steady-state pod kill), pause off|true|freeze
    gen-crashloop/ p2000+  rollouts where one old-revision Instance crashes
                          (fault=crashloop:old, form=start|load) from the
                          moment the bump lands, crossed with topology x
                          strategy x maxUnavailable x restartPolicy x
                          coordination; the fix must land on that Instance
    gen-switch/    p3800+  rollouts whose update strategy or restart policy
                          is edited mid-flight (edit=strategy:<type> |
                          edit=restartPolicy:<policy> at
                          during=settling|rotating): topology x every
                          ordered pair of strategies x hook, topology x
                          both policy flips x hook, a rolling group on PD,
                          and the two most informative strategy pairs with
                          an old-revision Instance crashing while the
                          strategy flips; its continuation (p11100+) puts a
                          break beside the edit on the six shape families:
                          each running-revision break before the push, kept
                          and one-shot, at a moment of its own, and each
                          pushed-revision break, kept and once, with the
                          edit at the held moment or the first pushed pod
    gen-crashloop-idle/    p2400+  a crash loop on the settled service, no bump
                          (during=idle), crossed with topology x rollout-group
                          kind x break victim (engine, or decoder) x crash form
                          x restartPolicy; every cell expects the repair
    gen-crashloop-sibling/ p2600+  the decoder's running revision breaks as an
                          engine-only bump lands (bump=engine, victimcomp=
                          decoder), crossed with the PD topologies x group
                          kind x crash form x restartPolicy; the engine roll
                          converges and the decoder is repaired. A
                          rollingUpdate group admits no push that leaves a
                          member out, so under co=rolling the bump names
                          engine and decoder and the roll owns the broken
                          Instance
    gen-heal/      p3000+  a cause that disappears: the engine runner's
                          ConfigMap key is removed on the idle service and
                          put back at lift=after-settle|after-failed
                          (fault=configkey:missing), crossed with topology x
                          restartPolicy x lift x coordination; the Instance
                          must be Ready and serving again on its running
                          revision with no bump and no reset mailbox
    gen-imagepull/ p3400+  a running revision whose image stops pulling
                          (fault=imagepull:old): fired as the bump
                          lands, crossed with topology x strategy x
                          restartPolicy x coordination, the fix must land on
                          that Instance; and fired on the idle service
                          (during=idle), crossed with topology x restartPolicy,
                          the repair must rebuild it
    gen-node/      p4200+  a Ready Instance loses its node (fault=node:notready
                          at idle: the kubelet under it is stopped, then
                          started again at the lift), crossed with topology x
                          restartPolicy x migration mode x an idle rolling
                          group x the outcome: parked while the node is down
                          and repaired once it is back without a force-delete
                          policy (needs=no-forcedelete), relocated while the
                          node is down with one (needs=forcedelete, the
                          force-delete lane); each half SKIPs on the other
                          lane's controller config
                          and, from p4296, the same loss at a hook of an
                          operation in flight (fault=node:<hook>): a push, a
                          rollback, a strategy edit, a runtime edit, a scale
                          up or down, a delete and the canary ladder on the
                          six ledger shapes, transient (lift=after-settle) and
                          kept down until the policy relocates the Instance
                          (lift=after-recover, needs=forcedelete) on every one
    gen-ctrlrestart/ p4600+ the manager pod is deleted under an operation in
                          flight (fault=ctrlrestart:<hook>): mid-surge,
                          mid-drain, mid-migration, mid-canary and
                          mid-scale-down, crossed with topology x strategy
                          (x a rolling group on PD rolls); the operation
                          resumes from status and converges; and, from
                          p4668, the same restart as a rollback, a strategy
                          edit, a scale-up and a delete land and at the
                          first pod of a runtime edit's new revision, on the
                          six ledger shapes
    gen-canary-crash/    p5200+  a canary rolls the fix while the running
                          revision breaks under it (crashloop start|load,
                          imagepull, one or several Instances at once)
    gen-canary-second-rev/ p5250+  a second revision (an ISVC re-bump or a
                          runtime edit) lands while the first canary is
                          staging or serving, with and without analysis
    gen-canary-podkill/  p5290+  a pod deleted by hand while the roll is
                          paused or held; the replacement is labelled as
                          what it runs and the roll finishes
    gen-canary-fault-old/ p6400+  a persistent fault on the stable side at
                          each step kind of the ladder (before the push,
                          the capacity step, a soak, a manual gate, an
                          analysis gate, the last gate before the promote);
                          the ladder still lands the fix on every Instance;
                          continued at p11500+ with the remaining stable-side
                          breaks on the six shape families (a readiness loss
                          kept or lifted, the key, a crash and a pull failure
                          lifted, one runner killed once)
    gen-canary-fault-new/ p6600+  the push itself brings a broken revision
                          (crash at start, crash after Ready inside or past
                          the soak, an image that cannot be pulled); the
                          ladder stops where it bites, says why, and a
                          rollback or a fix push lands; then the missing key
                          and the never-Ready revision under the same exits,
                          the one-shot pushed breaks, one runner killed once,
                          and a taint armed before the push
    gen-canary-fault-gate/ p9050+ what a step's gates read breaks while the
                          ladder walks: the health metric breaches on the
                          stable side (the gate must not blame the canary)
                          or on the canary side (the gate aborts or rides it
                          out), the analysis source becomes unreachable or
                          answers garbage (the ladder holds with a reason
                          and resumes, parks or rolls back as the policy
                          says), and a sibling Component (the router beside
                          an engine ladder, the decoder of a prefill/decode
                          unit) breaks once or for good, on the running or
                          the new side; across the canary-capable shapes
                          and the ladder positions
    gen-rollback/  p5800+  rollouts rolled back: the starting revision is
                          re-applied at a named moment of the new revision's
                          roll (rollback=1 during=settling|rotating|draining|
                          last|held; held breaks the new revision with
                          break=stuck so the roll parks at its budget first),
                          crossed with topology x strategy x hook x group;
                          every Instance must end on the starting revision.
                          Continued at p10800+ with a break beside the
                          rollback on the six shape families: the running
                          revision broken before the push (a crash at start
                          or after Ready, an unpullable image, the key
                          removed, readiness lost; kept, or lifted once the
                          rollback has fired) and rolled back at each hook,
                          a sibling broken the same way, and the push held
                          by break=crash-start|crash-load|imagepull|configkey
                          (once=1 for one kill of the first runner; the
                          grouped decoder's revision too) and rolled back
    gen-crashloop-new/ p5000+  the push brings a revision whose pods crash
                          (fault=crashloop:new, form=start|load), crossed
                          with topology x strategy x restartPolicy x group;
                          the roll must stop within its budget, say why, and
                          land the fix push on every Instance
    gen-crashloop-group/ p5400+  one old-revision Instance crashes under a
                          group policy (blueGreen, Sequential, maintainRatio)
                          and under the zero budgets of a rolling group; the
                          fix lands within the policy's own rule and never
                          last on the broken Instance (fixfirst=1); the gang
                          drain-first cells run three gangs so that order
                          can be read
    gen-pause-repair/ p6000+  a crash loop on the settled service, then the
                          operator pauses or freezes the service before the
                          repair opens, while it is in flight or after it has
                          finished (pause=, pauseat=), pushes the fix while
                          paused or nothing (push=), and unpauses; crossed
                          with shape x crash form x restartPolicy x group;
                          nothing but the repair in flight moves while paused
                          and the fix lands on every Instance afterwards
    gen-size/      p8000+  one and two Instances per Component, where a
                          budget of one is the whole fleet, a percent rounds
                          up to one and a single Instance is both the first
                          and the last the roll reaches: the plain push under
                          each strategy and budget, the push with the running
                          revision crash-looping, a scale between one and two,
                          and a canary ladder on two (engine and prefill/decode)
    gen-router-gang/ p8100+ the engine as a gang (widths 1 and 2) behind a
                          router at two Instances: a push, a crash loop on the
                          running revision, a scale and a canary ladder; the
                          router is never rolled by an engine push and stays
                          in rotation, and the gang rules hold throughout
    gen-ops-held/  p6800+  a scale-down, a scale to one, a delete or a
                          scale-up lands on a Component stuck on a revision
                          that cannot be pulled (stuck=failed: the first
                          attempt has failed; stuck=held: the retry ladder
                          is spent), crossed with shape x strategy x group;
                          the stuck Instances go first, survivors stay
                          untouched, the unlanded push stays visible; the
                          break rows park the push on the held moment's
                          other kinds (break=crash-start|crash-load|
                          configkey|stuck) and, on the sibling rows, on
                          the router or decoder beside the engine the
                          operation works on
    gen-runtime-edit-repair/ p6900+  a repair of a crash-looping Instance and
                          an edit of the inherited runtime overlap, in both
                          orders (mode=runtimerev with a break: during=repair
                          lands the edit on the open repair; without it the
                          crash lands as the edit does), crossed with shape x
                          strategy x restartPolicy x crash form x edit verb;
                          the last change wins and nothing is rebuilt twice
    gen-migrate-bump/ p6200+  a move and a revision bump that overlap: an
                          Instance is moved by request while a revision
                          rolls (then=bump at=accept|surge|drain pushes the
                          revision at a moment of the move; during=settling|
                          rotating|last requests the move at a moment of the
                          roll), crossed with topology x strategy x moment x
                          group; both finish and every Instance ends on the
                          new revision, the moved one among them
    gen-readiness/ p7000+  a pod that runs but never passes readiness: on the
                          running revision at idle (fault=readiness:old,
                          form=persist|recover) and on a pushed revision
                          (fault=readiness:new, then a fix push), crossed
                          with shape x strategy x restartPolicy
    gen-gang-member/ p7100+  a gang's leader or worker deleted mid-roll
                          (fault=podkill:<phase> kill=<member>) while a gang
                          settles, forms on the new revision, or serves beside
                          a draining one, crossed with shape x strategy x
                          restartPolicy; from p7172, a serving gang's leader
                          deleted as a rollback, a strategy edit, a scale up
                          or down and a delete land and at the first pod of a
                          runtime edit's new revision, on the gang ledger
                          shapes
    gen-tight/     p7300+  a gang roll on a cluster with no room for its surge
                          gang (fault=quota:<w> or cordon:all) that must wait
                          with a reason, tear nothing down and complete once
                          the squeeze lifts (lift=after-hold|never)
    gen-evict/     p7400+  a pod of a Ready Instance is evicted through the
                          Eviction API, as a node drain would: at steady state
                          (mode=kill via=evict) and while a roll is in flight
                          (fault=evict:rotating), crossed with topology x
                          restartPolicy (x strategy under the roll)
    gen-preempt/   p7440+  a pod of a Ready Instance is preempted by a
                          higher-priority filler (via=preempt at steady state,
                          fault=preempt:rotating under a roll) with every
                          node's spare CPU taken and every node closed to the
                          case, so the Instance waits until the lift
                          (lift=after-settle) or to the end (lift=never)
    gen-pending/   p7500+  a new pod cannot be placed or admitted: a NoSchedule
                          taint on every node (fault=taint:all) or a quota
                          short of one pod (fault=quota:<n>; on a gang, room
                          for all but one member), under a repair at steady
                          state (mode=kill) and under a roll, lifted or never
    gen-budget/    p8200+  percent budgets as the engine resolves them:
                          maxSurge (SurgeThenDrain) and maxUnavailable
                          (RecreatePod) at 10, 25 and 50 percent on fleets of
                          4 and 8, single-pod and gang, for a plain push, a
                          push with one Instance crash-looping on the running
                          revision, and a canary ladder; each case states the
                          rounding rule and the ceiling or floor it yields
    gen-inplaceonly/ p8330+ the strict in-place strategy on its own terms: an
                          image change rolls in place with stable pod
                          identities; an env, resources or probe change is
                          refused on a single-pod Instance (expect=held: the
                          hold, its reason, then the corrective push lands
                          in place) and rebuilds a gang whole; a crash loop
                          at idle is repaired by a rebuild, and under a push
                          it is fixed in place or waits for the corrective
                          push
    gen-fault-scale/     p7600+  one Instance of the settled service is broken
                          (a crash at start or after Ready, an image that
                          cannot be pulled, readiness that stops passing) and
                          the engine is scaled down by one, by two, to one, or
                          up by one (mode=scaledown with during=idle, inst=0):
                          the broken Instance goes first on a scale-down and
                          is repaired or left as the policy says on a scale-up
    gen-fault-delete/    p7700+  the same breaks, then the service is deleted
                          (mode=recreate with during=idle): everything goes
                          within budget, the broken pod included, and the same
                          name comes back clean
    gen-repeat/    p8700+  a fault that keeps repeating until it is lifted
                          (repeat=<n>): a pod deleted by hand, a gang member
                          lost, an eviction or a preemption applied again each
                          time what it took has come back, and the manager
                          deleted again each time it is Ready, on the six
                          shape families at steady state (mode=kill, or the
                          manager on the idle service), under a push and
                          under a scale-up, the manager again under a
                          rollback, a strategy edit, a runtime edit, a
                          scale-down, a move, a delete and a canary, and the
                          pod (a gang's worker) again under a rollback, a
                          strategy edit, a runtime edit and a scale-down; the
                          service keeps its floor, the status says why,
                          nothing is rebuilt on a wrong revision, and after
                          the lift every Instance is Ready with no row or
                          hold left behind
    gen-fault-move/ p7800+  a fault at a moment of a move: an Instance is
                          moved by request and, as the request is accepted,
                          as its replacement pod appears or as its source
                          drains (at=accept|surge|drain), a pod breaks
                          (crash at start, crash after Ready, an image that
                          cannot be pulled) on the moved Instance, on a peer
                          or on the decoder (victim=moved|peer|sibling), or
                          the source node dies; the move finishes or is
                          abandoned with a reason, the Instance ends whole
                          and serving on one pod set, a broken peer is
                          repaired under its policy. The same request beside
                          the remaining faults (p11700+): a break healed again
                          (lift=after-settle) or a runner killed once (once=1),
                          the missing ConfigMap key and the readiness switch,
                          kept or lifted, a pod deleted by hand at the request
                          (kill=, repeat=), a pod evicted or preempted at a
                          moment (evict|preempt:migrating at=), a taint that
                          leaves the replacement nowhere to go (taint:all
                          lift=), and a move requested while a push is stuck
                          on a broken revision (stuck=held|failed with img= or
                          break=, bump=decoder for the sibling's push)
    gen-packed-pool/ p8600+ a packed pool: fillers take every node's spare
                          CPU once the service has settled, so the only room
                          is what a broken pod vacates; a push whose revision
                          crashes stops at one Instance down and the good push
                          lands on that Instance first, in the same room, then
                          on the others one at a time (fault=crashloop:new,
                          pool=packed), and the mirror, the running revision
                          crashing on one Instance before the push
                          (fault=crashloop:idle); crossed with the six shape
                          families x the drain-first and in-place strategies
                          at maxUnavailable 1 x crash form x restartPolicy x
                          group; every cell selects the ledger and
                          node-exclusion invariants (inv_on=J,K)
    gen-fault-edit/ p7900+  a fault during a runtime edit: the runtime every
                          Component inherits gets a new image or runner env
                          and, as the edit lands, a pod of the running
                          revision breaks on the engine or on the decoder
                          (the roll owns it: it ends on the edited revision
                          with no repair); and the edit itself is bad (an
                          image that cannot be pulled, a readiness that
                          never passes): it holds within budget with a
                          reason, the old revision keeps serving, and a
                          corrective edit lands
    gen-webhook/   p8900+  the admission webhook made unreachable
                          (fault=webhook:idle|settling: its webhooks pointed
                          at a Service that does not exist) on the settled
                          service, as a push lands and
                          as a scale-up lands, for a bounded time
                          (lift=after-hold) or across the hold window
                          (lift=after-window), on every shape family: the
                          controller keeps the floor, says why a write waits,
                          creates nothing twice, and completes once the
                          webhook is back
    gen-conflict/  p8960+  a competing writer bumping the resourceVersion of
                          the ISVC or of the service's InferenceReplicas
                          (fault=conflict:idle|settling target=isvc|ir), once
                          (form=once) or until the operation completes
                          (form=persist), over the same three operations and
                          shapes: the controller's status writes conflict and
                          are retried without a lost decision, a double apply
                          or a hot loop
    gen-crash-once/ p8400+  a crash that happens exactly once (once=1): a
                          pod's runner is killed once through exec and the
                          kubelet starts it again, at its first start
                          (form=start) or after it is Ready (form=load), on
                          the running revision at steady state and under a
                          push and on the pushed revision's first pod under
                          a push, crossed with six shape families (the
                          engine alone, behind a router, with a decoder and
                          a router; single-pod and gang) x restartPolicy x
                          the surge and drain-first strategies; the Instance
                          ends Ready and serving on the revision it should
                          run, nothing is escalated or held, the push lands,
                          and a repair runs only where the policy promises it
    gen-lift/      p9250+  the lift axis of the config-key, readiness and
                          image-pull faults: a cause kept to the end of the
                          case (lift=never) or taken away after a bounded
                          time (lift=after-settle), on the running revision
                          at steady state and on the pushed revision under a
                          push, across the six shape families; a kept cause
                          parks the Instance Failed with its reason, a
                          lifted one leaves the Instance serving with no
                          hold behind it

Ownership and determinism:
  - The gen-* folders belong ENTIRELY to this generator: every case there
    carries the `[gen]` mark on line 1, and a regen sweeps and rewrites the
    marked files wholesale. A file without the mark is hand-written, whatever
    its id: it stops the regen before anything is written or swept, and must
    move to a hand-owned folder. When a generated case finds a bug worth
    pinning, copy it into a hand-owned folder under a fresh hand id; never
    edit the generated file.
  - Output is a pure function of this file: no timestamps, no environment.
    `--check` regenerates in memory and byte-compares against the committed
    tree (run_test.py runs it in CI), so hand-edits to generated files are
    rejected — edit the generator and regenerate instead.
  - Case ids are positional (allocation order inside each folder block). A
    matrix change renumbers ids after the edit point; result history is keyed
    per sweep, and cross-sweep tracking belongs to hand-pinned cases.

Constraint ledger (why the raw product is filtered — derived from run.py):
  - pause= is consumed only by mode=kill (_kill): a pause needs a held repair
    to observe, so pause multiplies only the kill family.
  - pause=freeze pairs only with RecreateInstanceOnPodRestart: the freeze lift
    downgrades to a plain pause, and plain pause parks policy None's Create
    recovery pass — freeze+None could never recover while paused.
  - fault=podkill victim selection is component-blind (_fault_pick_pod): on
    single-pod in-place engine-only cases the victim is the in-place-updated
    pod itself, whose recreate trips the in-place UID-stability check by
    harness design — those tuples are excluded, not waived.
  - podkill:migrating needs mode=migrate (out of scope here); kill targets
    (leader|worker) exist only on gang topologies; update budgets are dormant
    under a steady-state kill, so the kill family uses canonical budgets only.
  - Cells already pinned by hand-written restart/ cases are excluded
    (HAND_PINNED_KILL) so the corpus grows coverage instead of echoing pins.
  - ops-held pushes a revision that cannot be pulled; a rolling group exists
    only where a decoder does (pd), as in the crashloop family. The deepest
    scale asks for one: the API admits no count below one for a Component
    without KEDA autoscaling (minReplicas 0 is refused), and the case reads
    the count the controller accepted instead of assuming it. On the gang
    fleet of two, to one is down by one, so that cell is the scale-down row.
    The break rows fill one case per ledger cell (shape x operation x
    kind): each kind is read at one stuck state under one strategy,
    chosen so the rows meet both states under both strategies; the
    sibling rows take the crash at start, as the rollback and edit
    families do, and scale or delete the engine beside the sibling's
    parked push.
  - runtime-edit-repair lands an edit on an open repair only where a repair
    opens: a crash at start reaches the crash-loop repair under every restart
    policy, a crash after Ready reaches the runner-restart trigger only under
    RecreateInstanceOnPodRestart, so the edit-during-repair order skips
    form=load under policy None (the crash-during-edit order keeps it: the
    roll must still land on a pod the kubelet alone keeps restarting).
  - crashloop breaks an engine pod, so its coordination axis rolls engine and
    decoder together only where a decoder exists (pd, pd-mn): co=none and
    co=rolling. maxUnavailable=0 is a SurgeThenDrain budget only: a
    drain-first strategy can never roll under it. Percent budgets run at the
    mid size, where the ceil() rounding differs from the integer case. Both
    restart policies run under both crash forms: a crash at start reaches the
    crash-loop repair under every policy, a crash after Ready reaches the
    runner-restart trigger only under RecreateInstance, and the roll must win
    over each.
  - A mid-roll edit is a merge patch of the lifecycle's type or policy, so
    the rollingUpdate budget the case declares must be valid under BOTH
    strategies of a pair: a surge step needs headroom above N and a
    drain-first or in-place step needs headroom below it, so a pair with
    SurgeThenDrain on either side runs under (maxSurge 1, maxUnavailable 1)
    and every other pair under (0, 1). The header is stated for the worse
    of the two shapes: the ceiling is N+1 whenever a surge step can be in
    flight on either side of the edit, and the floor N-1 (hard) is the one
    the drain-first side needs. The bump is an annotation diff, which every
    in-place variant applies, and a gang runs either variant as a
    whole-gang recreate, so no strategy pair is impossible and none is
    skipped; a strategy flip carries no restart policy and the policy
    flips run under the canonical surge budget, where the floor is N.
  - The steady-state and sibling families leave out form=load under
    restartPolicy None: a runner that dies after Ready and comes back is a
    container restart the kubelet owns under that policy, not a wedge, so no
    repair is promised and neither family can read one. On the engine-only
    topologies a Sequential group as the generators express it (one
    single-Component blueGreen group per member) is one blueGreen group over
    the engine, so the steady-state family emits it as co=bluegreen only. The
    sibling family rolls the engine under the default strategy at its canonical
    budget: the sibling's break is independent of how the engine's own pods
    are replaced. Every cell of both families runs live and asserts the
    repair: an Instance that serves nothing is rebuilt without touching its
    Component's budget or a group's gate, whatever group it sits in.
  - heal breaks the engine runner, so its coordination axis is a rollingUpdate
    group over the engine alone or, where a decoder exists, over engine and
    decoder, whose gate admits a repair of an Instance that serves nothing
    without a consult. Nothing rolls, so the update strategy stays canonical
    and is not an axis.
    lift=after-failed waits for the row to escalate, so those cases carry a
    short instanceReadyTimeout on the victim (HEAL_FAILED_READY_TIMEOUT).
  - imagepull has one form (an image that never pulls has no after-Ready
    form) and runs each strategy at its canonical budget only; the idle half
    carries no group, since a repair under a group is the steady-state
    crash-loop family's axis.
  - node:notready reads the fate of a Ready Instance on a dead node under
    the deployed force-delete policy, which the lanes set as a dimension.
    Without lifecycle.forceDelete (the chart default) nothing frees the pod
    names a silent node holds: parked (nothing rebuilds it while the node is
    down) and repaired (the kubelet returns and the pods it never stopped
    serve again), both at lift=after-settle. With the policy the names are
    freed once the node's unreachable evidence is older than its threshold
    and the Instance is rebuilt elsewhere while the node is down: relocated
    at lift=after-recover. needs= pins each cell to the config it reads, so
    the other lane SKIPs it. The migration axis renders as Auto (the default) and Never:
    the API has no mode that admits explicit requests but not relocation,
    and neither mode moves a Ready Instance (Auto relocates on an expired
    attempt's deadline, which a Ready Instance never has). Restart policy
    and an idle rolling group change nothing either, and each cell says why.
    At a hook of an operation in flight (fault=node:<hook>) the operation
    must complete whichever form the loss takes: transient, the kubelet is
    started again once the bite has held and the pods it never stopped serve
    again; kept down, the policy frees the pods it held and the Instance is
    rebuilt elsewhere before the kubelet returns. The floor is reported, not
    judged, since the node takes every pod it ran. The move's node loss stays
    the move family's (at=); its transient form is not emitted.
  - ctrlrestart has no hook inside a repair: the runner fires one fault per
    case and the idle-break handler fires no second one, so a restart while
    a crash-loop repair is open is not emitted (MISSING_HOOKS). The update
    strategy is dormant under a migration and a scale-down; those cells run
    under both strategies for the family's symmetry and say so.
  - A rollback's draining hook is not emitted for an in-place strategy on a
    single-pod topology: an annotation-only diff is patched in place, so no
    pod of the starting revision ever drains and the hook never arrives (a
    gang falls back to a whole-gang recreate, which drains).
  - A rollback's draining and last hooks are not emitted over the missing
    key on the running revision, kept away or put back once the rollback
    has fired: the roll takes the broken Instance first and its replacement
    cannot start without the key, so no pod of the starting revision ever
    drains and neither hook arrives; both rows take those two moments at
    rotating instead, under their own strategy and group
    (ROLLBACK_KEPT_KEY_HOOKS).
  - crashloop:new arms the engine's new revision (victimcomp defaults to
    engine), so its group axis exists only where a sound member rolls beside
    it (pd, pd-mn: co=rolling); InPlaceOnly is left out, since form=load's
    exit switch is a pod-template env diff that in-place cannot carry (the
    InPlaceIfPossible cell documents the fallback instead) and form=start's
    image patch lands the same way under either in-place type; canonical
    budgets only (the budget axis is the running-revision family's).
  - the group family runs every policy on the PD shapes only where the
    policy has a peer to speak of: a one-member sequence is a blueGreen
    group and a ratio needs two Components, so engine-only runs blueGreen
    and the zero-budget rolling group alone. maintainRatio's tolerance is 50
    at N=2 (a crashing Instance alone is half the fleet) and 25 at N=4.
  - The pause-and-repair family leaves out form=load under restartPolicy
    None: a runner that dies after Ready and comes back is a container
    restart the kubelet owns under that policy, not a wedge, so no repair is
    promised and there is no repair to time a pause against. A freeze after
    the repair has finished is a pause: nothing is left for the deeper hold
    to suspend, so only pause=true is emitted at pauseat=after. The engine
    runs SurgeThenDrain maxSurge 1 maxUnavailable 1: a fresh crash-loop
    repair is charged against the unavailability budget, so under
    maxUnavailable 0 the repair the pause is timed against never opens. The
    grouped cells run live: a repair of an Instance that serves nothing is admitted without the budget or the gate,
    so the group changes nothing.
  - The engine resolves a percent budget as ceil(replicas * percent / 100),
    clamped to replicas (the per-Component budget's documented rule, which
    the canary step capacity shares), so at one and two Instances 25 and 50
    percent both resolve to one and no budget rounds to zero. gen-size drives
    the percent budgets on the plain push, where the resolved budget is read
    off the peak and the floor, and runs the crash loop, the scale and the
    canary under the integer budgets they resolve to. Its canary runs on two
    Instances only: a canary on one Instance has no Instance to hold on the
    stable revision and stages in place, a shape with its own verdict to
    write. Group policies are left out: the group pacing budget rounds the
    same way and the group axis belongs to the crashloop family.
  - gen-router-gang crosses the crash loop with the surge and recreate
    strategies only (a gang runs the in-place variants as a whole-gang
    recreate, the recreate cell) and runs the scale and the canary under the
    surge strategy: the strategy is dormant under a scale, and the gang
    canary under a recreate is the controller-restart family's shape.
  - The migrate-and-bump family moves one Instance by request only: the
    engine's own (automatic) migration is a born-terminal relocation record,
    written when an Instance whose node is gone is rebuilt elsewhere; it has
    no in-flight phase for a roll to overlap, and the one verb that provokes
    it, a node loss, belongs to the node-loss family. during=last is not
    emitted with maxSurge 2: a surge of two promotes its last pair together,
    so the moment with exactly one Instance still off the new revision is not
    reliably observable.
  - evict has no lift: the Eviction API is a one-shot removal, so the family
    crosses topology x restartPolicy (x strategy under the roll) only. Its
    steady-state cells are the kill verdict (the restart policy's exact
    recreate scope) with the removal done by the Eviction API, which the
    product's disruption budget may refuse while a pod is already down; the
    runner retries as a drain does, and a budget that never admits one
    eviction fails the case.
  - preempt and pending cross lift=after-settle|never: the cause is removed
    once the wait has held, or kept to the end so the hold itself is judged.
    Both run on one gang width (the pod removed or refused is one member
    whatever the width), with co=none (a group changes nothing about where a
    pod can be placed) and with the two strategies a roll waits under
    (in-place strategies create no pod to place). The preempt roll cells
    waive the floor and the order: the preemption itself takes a pod the
    budget cannot absorb; the pending verdict keeps its own floor.
  - the quota a pending cell arms depends on the pod the operation creates:
    a surge roll creates before anything is gone, so quota:0 (single-pod) or
    quota:<members-1> (a gang: room for all but one member) refuses it; a
    repair and a drain-first roll create after a pod is gone, so quota:-1
    (one pod fewer than run) is what refuses the replacement, and on a gang
    under RecreateInstance that is the whole gang short of one member. A gang
    surge roll held to the end by that quota is the tight-cluster family's
    cell, so the pending family leaves its quota:<members-1> lift=never cell
    out.
  - Under the remaining operations the pending causes and the pod faults run
    one case per ledger cell, under restart policy None. A cause armed before
    a rollback runs drain-first with the rollback at its draining hook: the
    roll's first replacement is what waits, and a roll that cannot place its
    first pod reaches no later hook, while the surge roll under the same
    cause never drains and so offers no rollback moment the cause can hold.
    The strategy edit lands as the push does, switching the surge strategy to
    the drain-first one. The eviction's persistent form (repeat=) needs a pod
    that comes back: a delete brings none back, so its cell has the one-shot
    form only, and a scale-up's persistent form is the repeating family's
    cell. A preemption holds a pod the operation places or, where it places
    none (a rollback at its first surge pod withdraws that pod, a scale-down
    takes an Instance), the repair of a settled Instance it keeps, which the
    press is aimed at; a delete places nothing until its recreate, so the
    delete meets the eviction alone.
  - The broken-Instance families aim the break at Instance 0, the lowest
    index, so a scale-down's natural victim (the highest index) and the
    broken Instance differ and the order of removal can be told apart. The
    engine runs under maxUnavailable 1 in both families: nothing rolls, so
    the strategy is dormant, and a budget of one admits the repair that
    takes a broken Instance offline (a budget of zero holds it). Readiness
    loss is repaired under neither policy, so its
    restart-policy axis is collapsed to None; the other breaks run under
    both. A gang fleet of two runs down by one and up by one only: "to one"
    is "down by one" there, and "down by two" asks for zero, which the API
    refuses (minReplicas 0 needs KEDA autoscaling). A rolling group is
    crossed in where a decoder exists, as in the crashloop family. The
    further rows run one case per ledger cell: the missing key, the
    one-shot forms (the cause taken away as the operation lands), a
    pending cause armed before the operation, and a break on the sibling
    the scale does not touch, each on one scale-down, the scale-up and the
    delete, under restart policy None and no group. The missing key kept
    away across a scale-up has no cell: the new Instance waits on the same
    key, so the scale cannot complete while the case runs.
  - fault-move runs one gang width (the move copies the whole gang) and no
    pd-mn shape (the gang axis is engine-mn's, the sibling axis pd's); the
    update strategy is dormant (no roll is in flight: a move surges whatever
    the strategy and a repair rebuilds in place), so one canonical budget;
    no rollout group (a repair of an Instance that serves nothing is admitted
    without a group's gate; the group axis is the steady-state crash-loop
    family's). A crash at start and a pull failure reach the
    crash-loop repair under either restart policy and run under None; the
    after-Ready form is the one the policy decides and runs under
    RecreateInstanceOnPodRestart, the policy that promises its repair. The
    node loss is the source node's (the story), at accept and at surge (at
    drain the source is already going: its node dying then is the sweep of
    a terminating pod, which the surge moment reads with the replacement in
    place), on the force-delete lane only (a source pod on a silent node is
    never freed without the policy, so the move cannot finish; that fate is
    the node family's parked cell). The readiness switch (fault=readiness:old,
    a pod that keeps running and leaves rotation) is not fired at a move
    moment: it is repaired under neither restart policy, so it is the
    broken-Instance families' break, not one a move answers.
  - fault-edit rolls engine, decoder and router (every Component inherits
    the runtime) under the surge and the drain-first strategies; the in-place
    variants are the switch family's. The image edit runs on every shape and
    the env edit on the engine shape only: under these two strategies both
    are one new revision, and the pod-template difference between them
    matters to an in-place strategy alone. A crash at start and a pull
    failure open no runner-restart trigger, so they run under restart policy
    None while the roll owns the Instance; the after-Ready form runs under
    both policies, since under RecreateInstanceOnPodRestart the trigger
    competes with the roll. The bad-edit cells run under None: a revision
    that never becomes Ready restarts no runner. One gang width, no pd-mn,
    no group, no readiness loss on the running side, as in fault-move.
  - The window families (webhook, conflict) run every shape family of the
    coverage ledger at one gang width: a webhook outage and a competing
    writer act on the controller's writes, not on a gang's members, so a
    second width adds nothing. The push runs under the surge and the
    drain-first strategies for the webhook (the outage refuses the pod a
    surge creates before anything is torn down, and the one a drain-first
    roll creates after a pod is gone) and under the surge strategy alone
    for the writer (a status write conflicts the same way under every
    strategy); the steady state and the scale are dormant to the strategy
    and run under the canonical surge budget. No rollout group: the engine
    is the only bumped member and the decoder and router are negative
    controls, so a group's gate has nothing to pace.

  - The packed-pool family runs the strategies that add no pod: RecreatePod
    and InPlaceIfPossible at maxSurge 0, maxUnavailable 1 (a surge needs
    room a packed pool has not got, which is the tight-cluster family's
    story; InPlaceOnly refuses the after-Ready form's env diff and patches
    the at-start form exactly as InPlaceIfPossible does). Single-pod shapes
    run three Instances, the smallest fleet where "the broken one first,
    then the others one at a time" is an order; gangs run two at one width.
    The group axis is a rollingUpdate group on the prefill/decode shapes,
    whose decoder carries the same one-down budget so its roll fits the
    pool; blueGreen and Sequential need a whole second set of pods and have
    no room. The mirror (the running revision crashing before the push)
    runs under restart policy None only: the break lives in the live pod,
    not the template, so under RecreateInstanceOnPodRestart the policy's
    own rebuild heals it before the push lands. The after-Ready form
    carries floor_soft=1 and reports the one-down rule and the landing
    order instead of asserting them: a runner that exits after Ready can be
    Ready when the roll decides.

The t=3 covering fill over the deferred dimensions (coordination/rollout
groups, canary promotion, budget x coordination interactions) is the --fill
knob: documented, deliberately not emitted until this lane has baked in.
"""
import argparse
import functools
import math
import subprocess
import sys
from pathlib import Path

import gen_cases as lib   # single source of truth for YAML body rendering
import gen_runtime        # the dedicated per-case ClusterServingRuntime a runtime edit patches
import runtime_fixtures   # the one image no deploy step side-loads

HERE = Path(__file__).parent.resolve()
ISVCS = HERE / "isvcs"

# Generator-owned folders and their id blocks (inclusive start, exclusive end).
GEN_BLOCKS = {
    "gen-rollout": (1000, 1200),
    "gen-fault": (1200, 1600),
    "gen-restart": (1600, 2000),
    "gen-crashloop": (2000, 2400),
    "gen-switch": (3800, 4200),
    "gen-crashloop-idle": (2400, 2600),
    "gen-crashloop-sibling": (2600, 2800),
    "gen-heal": (3000, 3400),
    "gen-imagepull": (3400, 3800),
    "gen-node": (4200, 4600),
    "gen-ctrlrestart": (4600, 5000),
    "gen-canary-crash": (5200, 5250),
    "gen-canary-second-rev": (5250, 5290),
    "gen-canary-podkill": (5290, 5400),
    "gen-canary-fault-old": (6400, 6600),
    "gen-canary-fault-new": (6600, 6800),
    "gen-canary-fault-gate": (9050, 9250),
    "gen-rollback": (5800, 6000),
    "gen-crashloop-new": (5000, 5200),
    "gen-crashloop-group": (5400, 5800),
    "gen-pause-repair": (6000, 6200),
    "gen-size": (8000, 8100),
    "gen-router-gang": (8100, 8200),
    "gen-ops-held": (6800, 6900),
    "gen-runtime-edit-repair": (6900, 7000),
    "gen-migrate-bump": (6200, 6400),
    "gen-readiness": (7000, 7100),
    "gen-gang-member": (7100, 7300),
    "gen-tight": (7300, 7400),
    "gen-evict": (7400, 7440),
    "gen-preempt": (7440, 7500),
    "gen-pending": (7500, 7600),
    "gen-budget": (8200, 8330),
    "gen-inplaceonly": (8330, 8400),
    "gen-fault-scale": (7600, 7700),
    "gen-fault-delete": (7700, 7800),
    "gen-fault-move": (7800, 7900),
    "gen-fault-edit": (7900, 8000),
    "gen-packed-pool": (8600, 8700),
    "gen-repeat": (8700, 8900),
    "gen-webhook": (8900, 8960),
    "gen-conflict": (8960, 9050),
    "gen-crash-once": (8400, 8600),
    "gen-lift": (9250, 9400),
}
# A folder mints from its block until the block is full, then from its
# continuation block; a folder owns an id that lies in either.
GEN_CONTINUATIONS = {
    "gen-fault-scale": [(9400, 9500), (10100, 10200)],
    "gen-fault-delete": (10200, 10300),
    "gen-evict": (9500, 9600),
    "gen-preempt": (9600, 9700),
    "gen-pending": [(9700, 9800), (10700, 10800)],
    "gen-fault-move": [(9800, 9900), (11700, 12000)],
    "gen-fault-edit": [(9900, 10000), (12000, 12100)],
    "gen-ops-held": (10000, 10100),
    "gen-webhook": (10300, 10450),
    "gen-conflict": (10450, 10700),
    "gen-rollback": (10800, 11100),
    "gen-switch": (11100, 11500),
    "gen-canary-fault-old": (11500, 11700),
}


def id_blocks(folder):
    """The id blocks a folder mints from, in minting order."""
    blocks = [GEN_BLOCKS[folder]]
    cont = GEN_CONTINUATIONS.get(folder)
    if cont:
        blocks.extend(cont if isinstance(cont, list) else [cont])
    return blocks


def all_id_blocks():
    """Every (folder, lo, hi) a generated id may come from."""
    return [(f, lo, hi) for f in GEN_BLOCKS for lo, hi in id_blocks(f)]


GEN_ID_LO = min(lo for _f, lo, _hi in all_id_blocks())
GEN_ID_HI = max(hi for _f, _lo, hi in all_id_blocks())
MANIFEST_REL = "MANIFEST.md"
# Every generated description starts with this mark; it is what tells a
# generated file in a gen-* folder from a hand-written one.
GEN_MARK = "[gen]"

RP_FULL = {"None": "None", "RecreateInstance": "RecreateInstanceOnPodRestart"}
RESTARTS = ["None", "RecreateInstance"]
PHASES = ["settling", "rotating"]
PAUSES = ["", "true", "freeze"]
IN_PLACE = {"InPlaceIfPossible", "InPlaceOnly"}

# The crashloop family: how the old-revision pod fails, the budgets each
# strategy is crossed with (maxSurge, maxUnavailable), both restart policies,
# the shapes (one gang width per topology; the crash is on the leader, so the
# width adds nothing), and the coordination axis per topology.
CRASH_FORMS = ["start", "load"]
CRASH_BUDGETS = {
    "SurgeThenDrain": [(1, 0), (1, 1), (1, "25%")],
    "RecreatePod": [(0, 1), (0, "25%")],
    "InPlaceIfPossible": [(0, 1), (0, "25%")],
    "InPlaceOnly": [(0, 1), (0, "25%")],
}
CRASH_SHAPES = [("engine", 4, 0), ("pd", 4, 0), ("engine-mn", 2, 1), ("pd-mn", 2, 1)]
CRASH_COORDS = {"engine": ["none"], "engine-mn": ["none"], "pd": ["none", "rolling"], "pd-mn": ["none", "rolling"]}

# The switch family: a roll whose update strategy, or restart policy, is
# edited on every bumped member at a hook. Every ordered pair of distinct
# strategies at both hooks; both restart-policy flips under the canonical
# surge budget; the same one-width shapes and coordination axis as the
# crashloop family (the flip is about the strategy, a gang's width adds
# nothing); and the two most informative pairs with an old-revision Instance
# crashing while the strategy flips at the first updated pod.
SWITCH_SHAPES = CRASH_SHAPES
SWITCH_COORDS = CRASH_COORDS
SWITCH_STRATS = list(CRASH_BUDGETS)
SWITCH_HOOKS = PHASES
SWITCH_POLICY_STRAT, SWITCH_POLICY_BUDGET = "SurgeThenDrain", (1, 0)
SWITCH_CRASH_PAIRS = [("SurgeThenDrain", "RecreatePod"), ("RecreatePod", "SurgeThenDrain")]
SWITCH_CRASH_HOOK = "rotating"
SWITCH_WHEN = {"settling": "right after the bump lands", "rotating": "at the first updated pod"}


def switch_budget(a, b):
    """(maxSurge, maxUnavailable) valid under both strategies of a pair: a
    surge step needs headroom above N, a drain-first or in-place step needs
    headroom below it, and the merge patch keeps the budget as declared."""
    return (1 if "SurgeThenDrain" in (a, b) else 0, 1)
# The pause-and-repair family: the engine's highest Instance breaks on the
# settled service, the operator pauses or freezes the service before the
# repair opens, while it is in flight or after it has finished, pushes the
# fix while paused or nothing, and unpauses; with no group or under a
# rollingUpdate group over the Components a bump would roll. The engine runs
# SurgeThenDrain maxSurge 1 maxUnavailable 1 so the repair the pause is
# timed against is admitted by the unavailability budget. A crash after
# Ready under restartPolicy None is a container restart the kubelet owns,
# not a wedge: no repair is promised there. A freeze after the repair has
# finished holds nothing a pause does not, so only the plain pause is
# emitted at that moment.
PAUSE_SHAPES = [("engine", 4, 0), ("pd", 4, 0), ("engine-mn", 2, 1)]
PAUSE_DEPTHS = ["true", "freeze"]
PAUSE_MOMENTS = ["before", "during", "after"]
PAUSE_PUSHES = ["", "fix"]
PAUSE_GROUPS = ["none", "rolling"]
PAUSE_BUDGET = (1, 1)
UNPROMISED_REPAIR = {("load", "None")}


# The steady-state and sibling crash-loop families. A service carries no group
# or one of the four group kinds, Sequential and canary as the other generators
# express them (one single-Component blueGreen group per member; a two-step
# timed canary over every Component); the break victim is the engine, or the
# decoder where one exists. A crash after Ready under restartPolicy None is a
# container restart the kubelet owns, not a wedge: no repair is promised there,
# so neither family emits that (form, policy) pair.
BREAK_GROUPS = ["none", "bluegreen", "rolling", "seq", "canary"]
BREAK_VICTIMS = {"engine": ["engine"], "engine-mn": ["engine"], "pd": ["engine", "decoder"], "pd-mn": ["engine", "decoder"]}
SIBLING_SHAPES = [("pd", 4, 0), ("pd-mn", 2, 1)]
# The engine behind a router, single-pod and as a gang: the router is the
# sibling that breaks there. (topo, N, W, routers.)
SIBLING_ROUTER_SHAPES = [("engine", 4, 0, lib.NR), ("engine-mn", 2, 1, lib.NR)]
UNPROMISED_REPAIR = {("load", "None")}
GROUP_NAMES = {"bluegreen": "blueGreen", "rolling": "rollingUpdate", "seq": "Sequential", "canary": "canary"}
IDLE_PHASE = "idle"
IDLE_FAULT_HOOK = "idle"
CRASHLOOP_NEW_HOOK = "new"

# The heal family: the engine runner's ConfigMap key is removed on the idle
# service and put back at the lift. Shapes as the crashloop family; nothing
# rolls, so the strategy stays canonical; the coordination axis is a
# rollingUpdate group over the victim engine (with the decoder where one
# exists), whose gate a repair of an Instance that serves nothing is admitted
# without.
HEAL_LIFTS = ["after-settle", "after-failed"]
HEAL_COORDS = ["none", "rolling"]
# The ConfigMap each heal case carries beside its InferenceService: named
# after the case, one key, read by the engine runner through this env var.
HEAL_CONFIG_SUFFIX = "-config"
HEAL_CONFIG_KEY = "value"
HEAL_CONFIG_VALUE = "1"
HEAL_CONFIG_VAR = "TEST_CONFIG_VALUE"
# lift=after-failed waits for the victim's row to escalate, which the open
# repair does at StartedAt + instanceReadyTimeout (or at the stuck-pod grace
# where the deployment configures one). run.py watches the whole case inside
# the rotate budget, ROTATE_BASE_S + ROTATE_PER_POD_S per pod (180 s + 100 s
# per pod before OME_TEST_TIMEOUT_MULT scales it), so the smallest shape here
# has 580 s: 90 s, the readyTimeout the canary failure cases park under, puts
# the escalation well inside that and leaves the heal watch after the lift
# its own full budget.
HEAL_FAILED_READY_TIMEOUT = "90s"

# The node family: the kubelet under the highest-index engine Instance of a
# settled service is stopped, and the lift starts it again. The outcome is
# the configured one, read at lift=after-settle: parked holds the node down
# across the hold window and asserts nothing rebuilds the Instance (no
# force-delete policy frees its names); repaired lifts once the bite has held
# and asserts the Instance serves again on the pods the kubelet never
# stopped. Both restart policies, both migration modes a running Instance can
# carry (Auto is the default) and an idle rolling group are crossed in; none
# of them changes the fate, and every cell states why.
NODE_PARAM = "notready"
NODE_SHAPES = [("engine", 4, 0), ("pd", 4, 0), ("engine-mn", 2, 1), ("pd-mn", 2, 1)]
NODE_MIGRATIONS = ["Auto", "Never"]
NODE_COORDS = ["none", "rolling"]
NODE_OUTCOMES = [("parked", "after-settle"), ("repaired", "after-settle")]
# The relocation half, emitted after the first: the force-delete lane's
# outcome, read once the Instance serves again on other nodes.
NODE_RELOCATION = ("relocated", "after-recover")
# The deployed force-delete policy each outcome is read against (run.py
# SKIPs the cell where the controller config differs); repaired holds under
# both and names none.
NODE_NEEDS = {"parked": "no-forcedelete", "relocated": "forcedelete"}
NODE_RELOCATION_REBUILD = {
    "None": ("the pod the node held is recreated at the same index on another node, and members on live "
             "nodes keep theirs"),
    "RecreateInstance": ("the lost pod re-arms the whole-Instance rebuild on other nodes"),
}
NODE_NO_TRIGGER = {
    "None": "a pod that is NotReady but neither restarted nor Failed is no repair trigger under restart policy None",
    "RecreateInstance": ("a NotReady pod is still present, so RecreateInstanceOnPodRestart reads neither a runner "
                         "restart nor a lost gang member"),
}
NODE_NO_RELOCATION = {
    "Auto": ("migration mode Auto relocates only when an open attempt's deadline expires, and a Ready Instance "
             "has no attempt open"),
    "Never": "migration mode Never allows no migration at all",
}

# The controller-restart family: the manager pod is deleted at a hook inside
# an operation, and the operation must resume from status and converge with
# nothing created twice (ceilings, index partition) and nothing deleted twice
# (every Instance ends at its count). Each condition is one runner mode and
# one hook; the strategy axis is live for the two rollout conditions and
# dormant under a migration and a scale-down. The six shape families of the
# coverage ledger at one gang width; on a canary every Component is a group
# member, the router included. (topo, N, W, routers on an engine-only shape.)
CTRL_SHAPES = [("engine", 4, 0, 0), ("pd", 4, 0, 0), ("engine-mn", 2, 1, 0),
               ("engine", 4, 0, lib.NR), ("engine-mn", 2, 1, lib.NR), ("pd-mn", 2, 1, 0)]
CTRL_STRATS = [("SurgeThenDrain", 1, 0), ("RecreatePod", 0, 1)]
CTRL_CONDITIONS = ["surge", "drain", "migration", "canary", "scaledown"]
CTRL_FAULTS = {"surge": "ctrlrestart:settling", "drain": "ctrlrestart:rotating",
               "migration": "ctrlrestart:migrating", "canary": "ctrlrestart:canary-serving",
               "scaledown": "ctrlrestart:settling"}
CTRL_ROLL_CONDITIONS = {"surge", "drain"}
CTRL_CANARY_STEPS = (("50%", 50), ("100%", 100))
CTRL_CANARY_CELL = "canary/Serving/ctrlRestart[gen]"


# The rollback family: revision A serves, B is pushed, and A's spec is
# re-applied at a named moment of B's roll. One gang width per topology; the
# budget is 1 throughout, plus maxSurge 2 on the single-pod engine; a
# rollingUpdate group is what puts the roll under the run model, so the
# engine-only shapes carry a single-member group on that axis. The starting
# revision carries the trigger annotation at its first value, so the rollback
# is an annotation update the in-place path can apply, not a removal.
ROLLBACK_SHAPES = [("engine", 4, 0), ("pd", 4, 0), ("engine-mn", 2, 1), ("pd-mn", 2, 1)]
ROLLBACK_BUDGETS = {
    "SurgeThenDrain": [(1, 0)],
    "RecreatePod": [(0, 1)],
    "InPlaceIfPossible": [(0, 1)],
}
ROLLBACK_ENGINE_BUDGETS = {"SurgeThenDrain": [(1, 0), (2, 0)]}
ROLLBACK_HOOKS = ["settling", "rotating", "draining", "last", "held"]
ROLLBACK_COORDS = ["none", "rolling"]
ROLLBACK_START_ANN = {"perm.test/rev": "v1"}
ROLLBACK_MOMENT = {
    "settling": "the moment the bump has landed, before any Instance moved",
    "rotating": "the first pod of the new revision",
    "draining": "the first pod of the starting revision draining",
    "last": "the last Instance, every other one already on the new revision",
    "held": "the held floor: the new revision never turns Ready, so the roll parks at its budget",
}

# The new-revision crash family: the push itself carries the crash, under
# each strategy at its canonical budget (maxSurge, maxUnavailable), both
# restart policies, both forms, the crash shapes and their group axis.
NEW_CRASH_STRATS = [("SurgeThenDrain", 1, 0), ("RecreatePod", 0, 1), ("InPlaceIfPossible", 0, 1)]

# The group family: a running-revision crash under each group policy on the
# shapes that have a peer for it, the two drain orders, and the zero-budget
# cells of a plain rolling group (maxUnavailable 0 with maxSurge 1, maxSurge
# 0 with maxUnavailable 1) on the single-pod shapes. The gang shape runs two
# gangs under the surge strategy and three under the drain-first one: the
# break lands as the bump does, once the roll has taken its first gang, and
# the dark gang then waits for that one slot (maxUnavailable 1), so with two
# gangs the only peer precedes the broken gang by construction and the order
# claim has nothing to read; with three the dark gang is rebuilt ahead of
# the peer still waiting, and the victim is the highest index as on the
# single-pod shapes (index 3 of 4). Three gangs with the decoder and the
# router are 14 pods, above the gang scheduler twin's node cap, so those
# cells run on the default scheduler only, as the family's gang surge cells
# (also 14 pods at their peak) already do.
GROUP_CRASH_SHAPES = [("engine", 4, 0), ("pd", 4, 0), ("pd-mn", 2, 1)]
GROUP_CRASH_MN_DRAIN_N = 3
GROUP_CRASH_POLICIES = {"engine": ["bluegreen"], "pd": ["bluegreen", "seq", "ratio"], "pd-mn": ["bluegreen", "seq", "ratio"]}
GROUP_CRASH_STRATS = [("SurgeThenDrain", 1, 0), ("RecreatePod", 0, 1)]
BUDGET_ZERO_SHAPES = [("engine", 4, 0), ("pd", 4, 0)]
# maintainRatio tolerance by fleet size: at N=2 one crashing Instance is
# already half the engine, so the band must admit 1:2; at N=3 the roll holds
# one healthy gang in the drain slot while the broken one is dark, so the
# healthy engine runs 1 of 2 against a full decoder and the band must admit
# 1:2 there too.
RATIO_TOL_BY_N = {2: 50, 3: 50, 4: 25}

# The ops-held family: an operation lands on a Component stuck on a revision
# that cannot be pulled. One size per shape (the engine's victim order and
# its hold do not depend on the fleet size), the strategy that surges and
# the one that drains first, both stuck kinds, every operation, and a
# rolling group where a decoder exists (a one-member group has nobody to
# pace with, as in the crashloop family). The deepest scale asks for one,
# the least count the API admits (minReplicas 0 needs KEDA autoscaling);
# on the gang fleet of two that is down by one, the scale-down row.
# The six shape families of the coverage ledger: singles at N=3, gangs at
# N=2 with one worker. (topo, N, W, routers on an engine-only shape.)
OPS_HELD_SHAPES = [("engine", 3, 0, 0), ("pd", 3, 0, 0), ("engine-mn", 2, 1, 0),
                   ("engine", 3, 0, lib.NR), ("engine-mn", 2, 1, lib.NR), ("pd-mn", 2, 1, 0)]
OPS_HELD_STUCK = ["failed", "held"]
OPS_HELD_OPS = ["scale-down", "scale-one", "delete", "scale-up"]
OPS_HELD_STRATS = {"SurgeThenDrain": (1, 0), "RecreatePod": (0, 1)}
OPS_HELD_GROUPS = {"engine": ["none"], "pd": ["none", "rolling"], "engine-mn": ["none"], "pd-mn": ["none", "rolling"]}

# The runtime-edit-repair family: a repair of a crash-looping Instance and
# an edit of the runtime every Component inherits overlap. One fleet size
# (the interplay does not depend on it), the strategies that surge, drain or
# patch, both restart policies, both crash forms, the two edit verbs, and
# the order: the edit lands on the open repair, or the crash lands as the
# edit does. A PD shape here has no router, as in the runtime-rev cases: the
# edit rolls engine and decoder, each onto its own new revision.
EDIT_REPAIR_SHAPES = [("engine", 3), ("pd", 3)]
EDIT_REPAIR_STRATS = {"SurgeThenDrain": (1, 0), "RecreatePod": (0, 1), "InPlaceIfPossible": (0, 1)}
EDIT_REPAIR_ORDERS = ["edit-during-repair", "crash-during-edit"]
EDIT_REPAIR_EDITS = ["image", "env"]
ALT_IMAGE_TOKEN = "${OME_EVAL_NGINX_ALT_IMAGE}"


# The migrate-and-bump family: an Instance is moved by request while a
# revision rolls. then=bump at=<moment of the move> pushes the revision once
# the move is accepted, once its replacement pod has appeared, or once its old
# pod is being taken down; during=<moment of the roll> files the request once
# the bump has landed, at the first pod of the new revision, or when exactly
# one Instance of the moved Component is still off it (inst=last names that
# Instance). One gang
# width per topology (the move copies the whole gang); budget 1 per strategy,
# plus maxSurge 2 on the single-pod engine; a rollingUpdate group puts the
# roll under the run model (a single-member group on the engine-only shapes).
# The move always surges one replacement, so the ceiling is N plus the larger
# of the roll's surge and that one replacement: the move and the roll together
# never exceed the roll's budget.
MIGRATE_BUMP_SHAPES = [("engine", 4, 0), ("pd", 4, 0), ("engine-mn", 2, 1), ("pd-mn", 2, 1)]
MIGRATE_BUMP_BUDGETS = {
    "SurgeThenDrain": [(1, 0)],
    "RecreatePod": [(0, 1)],
    "InPlaceIfPossible": [(0, 1)],
}
MIGRATE_BUMP_ENGINE_BUDGETS = {"SurgeThenDrain": [(1, 0), (2, 0)]}
# (the operation that starts first, the moment of it the second is filed at)
MIGRATE_BUMP_ORDERS = [("move", "accept"), ("move", "surge"), ("move", "drain"),
                       ("roll", "settling"), ("roll", "rotating"), ("roll", "last")]
MIGRATE_BUMP_COORDS = ["none", "rolling"]
MIGRATE_BUMP_MOMENT = {
    ("move", "accept"): "the revision is pushed as the move is accepted, before its replacement exists",
    ("move", "surge"): "the revision is pushed as the move's replacement pod starts",
    ("move", "drain"): "the revision is pushed as the move takes its old pod down",
    ("roll", "settling"): "the move is requested as the bump lands, before any Instance moved",
    ("roll", "rotating"): "the move is requested at the first pod of the new revision",
    ("roll", "last"): "the move is requested when the roll has one Instance of the moved Component left, and names that Instance",
}

# The readiness family: the running side at idle (form = whether the switch
# stays or is lifted) and the new side (a pushed revision that never passes
# readiness, then a fix push) with a short Instance ready timeout so the hold
# arrives inside the case budget; shapes with one gang width (the switch is
# on the leader, so the width adds nothing).
# The six shape families of the coverage ledger, in the batches the ids are
# minted in: a batch's running-side cells come before its new-side cells.
# (topo, N, W, routers on an engine-only shape.)
READINESS_BATCHES = [[("engine", 4, 0, 0), ("pd", 4, 0, 0), ("engine-mn", 2, 1, 0)],
                     [("engine", 4, 0, lib.NR), ("engine-mn", 2, 1, lib.NR), ("pd-mn", 2, 1, 0)]]
READINESS_STRATS = ["SurgeThenDrain", "RecreatePod", "InPlaceIfPossible"]
READINESS_FORMS = ["persist", "recover"]
READINESS_NEW_TIMEOUT = "90s"

# The gang-member family: which member dies, when in the roll (and which gang:
# a settled one, the one forming on the new revision, a serving neighbour of
# the first gang on its way out), under which strategy and restart policy.
# N=3 engine gangs give the untouched-gang floor a value under RecreatePod.
GANG_MEMBER_SHAPES = [("engine-mn", 3, 1), ("engine-mn", 3, 2), ("pd-mn", 2, 1)]
GANG_MEMBER_STRATS = ["SurgeThenDrain", "RecreatePod"]
GANG_MEMBER_TIMINGS = [("settling", "stable"), ("rotating", "canary"), ("draining", "stable")]
GANG_MEMBERS = ["leader", "worker"]

# The tight-cluster family: gang shapes at two widths, the squeeze (a quota
# one pod short of a gang, or every node cordoned) and whether it lifts.
TIGHT_SHAPES = [("engine-mn", 2, 1), ("engine-mn", 2, 2), ("pd-mn", 2, 1), ("pd-mn", 2, 2)]
TIGHT_STRATS = ["SurgeThenDrain", "RecreatePod"]
TIGHT_SQUEEZES = ["quota", "cordon"]
TIGHT_LIFTS = ["after-hold", "never"]

# The eviction, preemption and pending families: the six shape families of
# the coverage ledger at one gang width, the two strategies a roll waits
# under, the rotating hook for the in-flight moment, and for the causes that
# can be lifted whether they are. A pending cell's quota admits one pod
# fewer than the operation needs (see _pending_quota). (topo, N, W, routers
# on an engine-only shape.)
DISRUPT_SHAPES = [("engine", 4, 0, 0), ("pd", 4, 0, 0), ("engine-mn", 2, 1, 0),
                  ("engine", 4, 0, lib.NR), ("engine-mn", 2, 1, lib.NR), ("pd-mn", 2, 1, 0)]
# The repeating preemption at steady state runs on the single-pod shapes
# and the engine gang, at the disruption families' sizes.
REPEAT_PREEMPT_SHAPES = [("engine", 4, 0), ("pd", 4, 0), ("engine-mn", 2, 1)]
DISRUPT_STRATS = [("SurgeThenDrain", 1, 0), ("RecreatePod", 0, 1)]
DISRUPT_PHASE = "rotating"
PENDING_CAUSES = ["taint", "quota"]
PENDING_LIFTS = ["after-settle", "never"]
PENDING_TOKEN = {"taint": "Unschedulable", "quota": "QuotaExceeded"}

# The packed-pool family: once the service has settled, fillers take every
# node's spare CPU, so the only room is what a broken pod vacates. The six
# shape families (the engine, the engine behind a router, prefill/decode,
# each single-pod and as a gang), the strategies that add no pod at
# maxUnavailable 1, both crash forms, and the side the crash is on: the
# push brings it (fault=crashloop:new, then the good push), or the running
# revision crashes before any push (fault=crashloop:idle, then the good
# push). The restart policy axis is live on the bad push; the mirror runs
# under None (see the module docstring). Every cell selects the ledger and
# node-exclusion invariants.
PACKED_SHAPES = [
    # (topo, N, W, a router in front of an engine-only shape)
    ("engine", 3, 0, False), ("engine", 3, 0, True),
    ("engine-mn", 2, 1, False), ("engine-mn", 2, 1, True),
    ("pd", 3, 0, False), ("pd-mn", 2, 1, False),
]
PACKED_STRATS = [("RecreatePod", 0, 1), ("InPlaceIfPossible", 0, 1)]
PACKED_SIDES = ["new", "idle"]
PACKED_RESTARTS = {"new": RESTARTS, "idle": ["None"]}
PACKED_GROUPS = {"engine": ["none"], "engine-mn": ["none"], "pd": ["none", "rolling"], "pd-mn": ["none", "rolling"]}
PACKED_POOL = "packed"
PACKED_INVARIANTS = "J,K"

# The repeating-fault family: the persistent form of a pod deleted by hand,
# a gang member lost, an eviction or a preemption and a manager restart. The
# fault is applied again each time what it took has come back (repeat=<n>),
# then stops. The six shape families at the sizes the other families use
# (singles at 4 Instances; the engine gang at 3, so the floor under a
# drain-first roll has a value; the gang behind a router and the PD gang at
# 2), under the three operations the loop rides: the steady state, a push
# under the surge and the drain-first strategy, and a scale-up by one.
REPEAT_SHAPES = [
    # (label, topo, N, W, a router in front)
    ("engine", "engine", 4, 0, False),
    ("engine gang", "engine-mn", 3, 1, False),
    ("engine behind a router", "engine", 4, 0, True),
    ("engine gang behind a router", "engine-mn", 2, 1, True),
    ("pd", "pd", 4, 0, False),
    ("pd gang", "pd-mn", 2, 1, False),
]
REPEAT_N = 3
REPEAT_PREEMPT_N = 2
REPEAT_STRATS = [("SurgeThenDrain", 1, 0), ("RecreatePod", 0, 1)]
REPEAT_PUSH_HOOK = "rotating"
REPEAT_SCALE_HOOK = "settling"
REPEAT_IDLE_HOOK = "idle"
REPEAT_OPS = ["steady", "push", "scale-up"]

# Canonical shapes: (topo, N, W). Gangs at N=2 with widths 1 and 2; singles at
# N=4 (pd: engine+decoder N, router lib.NR). Percentage budgets additionally
# run a mid-size variant (MID_SHAPE) so the ceil() rounding actually differs.
SHAPES = [
    ("engine", 4, 0),
    ("pd", 4, 0),
    ("engine-mn", 2, 1),
    ("engine-mn", 2, 2),
    ("pd-mn", 2, 1),
    ("pd-mn", 2, 2),
]
MID_SHAPE = {("engine", 0): 8, ("pd", 0): 8, ("engine-mn", 1): 4, ("pd-mn", 1): 4}

# updateStrategy x budget variants. The first budget of each strategy is its
# canonical one (the only budget the kill family uses); the trailing percent
# budget is the one that gets the mid-size variant in gen-fault.
STRATS = [
    ("SurgeThenDrain", [(1, 0), (2, 0), ("25%", 0)]),
    ("RecreatePod", [(0, 1), (0, 2), (0, "50%")]),
    ("InPlaceIfPossible", [(0, 1), (0, 2), (0, "50%")]),
    ("InPlaceOnly", [(0, 1), (0, 2), (0, "50%")]),
]

# The imagepull family: as the bump lands, one old-revision engine pod is
# pointed at the harness's unpullable image (one form: an image that never
# pulls has no after-Ready form), at each strategy's canonical budget, under
# both restart policies, with the crashloop coordination axis; and on the
# idle service under both restart policies with no group, where the
# crash-loop repair must rebuild it.
IMAGEPULL_BUDGETS = [(strat, budgets[0]) for strat, budgets in STRATS]

# Kill-family cells already pinned by hand-written restart/ cases (size-agnostic:
# the recreate-scope contract does not depend on N). Key:
# (topo, W, strat, restart, pause, target) -> hand case id.
HAND_PINNED_KILL = {
    ("engine", 0, "SurgeThenDrain", "None", "", None): "p130",
    ("engine", 0, "SurgeThenDrain", "RecreateInstance", "", None): "p131",
    ("pd", 0, "SurgeThenDrain", "None", "", None): "p132",
    ("pd", 0, "SurgeThenDrain", "RecreateInstance", "", None): "p133",
    ("engine-mn", 1, "SurgeThenDrain", "RecreateInstance", "", "leader"): "p138",
    ("engine-mn", 1, "SurgeThenDrain", "RecreateInstance", "", "worker"): "p139",
    ("engine-mn", 2, "SurgeThenDrain", "RecreateInstance", "", "worker"): "p141",
    ("engine", 0, "SurgeThenDrain", "RecreateInstance", "true", None): "p148",
    ("engine", 0, "SurgeThenDrain", "None", "true", None): "p149",
    ("engine-mn", 1, "SurgeThenDrain", "RecreateInstance", "true", "worker"): "p150",
    ("engine-mn", 1, "SurgeThenDrain", "RecreateInstance", "freeze", "worker"): "p151",
    ("pd", 0, "SurgeThenDrain", "RecreateInstance", "true", None): "p152",
    ("pd-mn", 1, "SurgeThenDrain", "RecreateInstance", "freeze", "leader"): "p153",
}

# Cells parked on tracked product bugs: cell key -> (issue number, reason,
# invariant scope). A parked cell emits notimpl=1 issue=N and carries the
# reason as its line-2 rationale, so the runners record XFAIL (known bug: #N)
# instead of red and the PARKED DEBT table keeps the debt visible. The scope
# ("A"|"B"|"C"|"all", or "" when the bug fails an ordinary check) additionally
# emits inv_xfail=<scope>: it names the continuous invariant whose violation
# IS the tracked bug, which would otherwise fail the verdict after the
# notimpl softening. A cell goes quiet only through this table: entries
# require a filed issue number, and a key that matches no emitted cell fails
# the regen (stale parking is invisible debt); the manifest lists every
# parked case. Cell keys are the per-family tuples the emitters build:
#   ("rollout", topo, w, strat, surge, unavail)
#   ("podkill", phase, topo, w, size, strat, surge, unavail, rp)
#   ("kill", topo, w, strat, rp, pause, target)
#   ("crashloop", form, topo, w, size, strat, surge, unavail, rp, co)
#   ("switch", "strategy", hook, topo, w, n, from, to, co)
#   ("switch", "restartPolicy", hook, topo, w, n, from, to, co)
#   ("switch-crash", hook, topo, w, n, from, to, co)
#   ("crashloop-idle", form, topo, w, size, co, victim, rp, routers)
#   ("crashloop-sibling", form, topo, w, size, co, rp)
#   ("heal", topo, w, rp, lift, co)
#   ("imagepull", "settling", topo, w, strat, rp, co) / ("imagepull", "idle", topo, w, rp)
#   ("rollback", hook, topo, w, strat, surge, unavail, co)
#   ("pause-repair", topo, w, form, rp, co, pause, moment, push)
#   ("migrate-bump", first, hook, topo, w, strat, surge, unavail, co)
#   ("fault-move", kind, form, at, victim, topo, w, rp, routers)
#   ("fault-edit", kind, form, victim, edit, topo, w, strat, rp, routers) / ("bad-edit", kind, topo, w, strat, routers)
# (routers is the router count of an engine-only shape, 0 when it has none.)
# A park is added here as (key): (issue, reason, invariant scope) and
# rendered into the header.
PARKED = {}

# Runner verbs/hooks this corpus needs but the runners do not implement yet.
# Kept in the manifest so the follow-up surface stays visible next to the
# corpus it would grow.
MISSING_HOOKS = [
    "kill-instance verb (atomic delete of a whole gang) — today only reachable "
    "as the RecreateInstanceOnPodRestart cascade from one pod death",
    "component-scoped podkill victim (fault=podkill:<phase>:<component>) — "
    "victim selection is component-blind, so PD podkill can never target the "
    "engine at settling, and single-pod in-place engine victims are unreachable",
    "mid-surge injection points beyond first-surge-observed (k-th surge, "
    "mid-drain, pre-settle tail) — FAULT_PHASES has settling/rotating/migrating "
    "only",
    "pause-under-rollout hook (ome.io/rollout-paused is exercised inside "
    "mode=kill and around a repair at idle, pauseat=; no verb pauses an "
    "in-flight rotation)",
    "decoder-targeted mode=kill (the kill handler always picks the engine when "
    "one is configured)",
    "controller restart while a repair is open (a crash loop at idle with the "
    "manager deleted once the rebuild has started) — the runner fires one fault "
    "per case and the idle-break handler fires no second one",
    "an automatic migration verb — the engine's own migration is a born-terminal "
    "relocation record written when a lost Instance is rebuilt elsewhere, so only a "
    "node loss provokes it and the migrate-and-bump family moves by request only",
    "a pending cause armed mid-roll (a taint or a quota at the rotating hook) — "
    "taint:all and quota:<n> are armed before the bump; only preempt fires at a "
    "hook",
]


def is_pd(topo):
    return topo.startswith("pd")


def is_mn(topo):
    return topo.endswith("-mn")


def budget_count(n, value):
    """A rolling-update budget as the engine resolves it against n
    Instances: an int as is, a percent as n x percent / 100 rounded up and
    clamped to n, for maxSurge and maxUnavailable alike."""
    if isinstance(value, str) and value.endswith("%"):
        return min(n, math.ceil(n * int(value[:-1]) / 100))
    return value


def surge_count(n, s):
    return budget_count(n, s)


def budget_tag(strat, surge, unavail):
    v = surge if strat == "SurgeThenDrain" else unavail
    p = "s" if strat == "SurgeThenDrain" else "u"
    return f"{p}{v}".replace("%", "pct")


def shape_tag(topo, w, nr=None):
    tag = f"{topo} W{w}" if is_mn(topo) else topo
    return tag + ("+router" if nr and not is_pd(topo) else "")


def router_count(topo, nr=None):
    """The router replicas a shape carries: a prefill/decode service always
    has lib.NR; an engine-only topo has what its shape list names, 0 when it
    names none."""
    if is_pd(topo):
        return lib.NR
    return nr or 0


def shape_words(topo, w, nr=None):
    """The shape family in words, for a reason line."""
    base = "prefill/decode" if is_pd(topo) else ("engine behind a router" if nr else "engine alone")
    return base + (f" as gangs of 1 leader + {w} worker{'s' if w > 1 else ''}" if is_mn(topo) else ", single-pod")


# The keys the hot-core families emit come first, in their order; the canary
# families' keys follow the closing co= topo= pair, so the hot-core headers
# read as they always have.
GEN_HDR_ORDER = ["mode", "Ne", "Nd", "Nr", "ceil_e", "ceil_d", "ceil_r",
                 "restart", "kill", "pause", "fault", "pool", "floor_soft", "floor_hard", "order_soft",
                 "co", "topo",
                 "canary", "bump", "img", "form", "victims", "exitafter", "during", "verb", "verbhold",
                 "verb2", "during2", "victim", "victimcomp", "release", "releaseafter", "rt",
                 "analysis", "failurelimit", "terminal", "nophase", "inv_on", "cell"]


def header(name, desc, kv):
    parts = [f"{k}={kv[k]}" for k in GEN_HDR_ORDER if k in kv]
    parts += [f"{k}={v}" for k, v in kv.items() if k not in GEN_HDR_ORDER]
    return f"# {name}: {desc}  " + " ".join(parts)


def counts(topo, n, nr=None):
    if is_pd(topo):
        return dict(Ne=n, Nd=n, Nr=lib.NR)
    return dict(Ne=n, Nd=0, Nr=router_count(topo, nr))


def ceilings(topo, n, strat, surge, nr=None):
    ce = n + surge_count(n, surge) if strat == "SurgeThenDrain" else n
    if is_pd(topo):
        return dict(ceil_e=ce, ceil_d=n, ceil_r=lib.NR)
    return dict(ceil_e=ce, ceil_d=0, ceil_r=router_count(topo, nr))


def comps(topo, n, w, e_life, d_life=None, r_life=None, nr=None):
    """The fleet of one shape: the engine, the decoder of a prefill/decode
    service, and the router where the shape carries one (always on a
    prefill/decode service, on an engine-only shape when nr names it)."""
    cw = w if is_mn(topo) else 0
    cs = [lib.comp("engine", n, cw, e_life)]
    if is_pd(topo):
        cs.append(lib.comp("decoder", n, cw, d_life if d_life is not None else lib.lifecycle()))
    if router_count(topo, nr):
        cs.append(lib.comp("router", router_count(topo, nr), 0, r_life if r_life is not None else lib.lifecycle()))
    return cs


def case_yaml(name, desc, kv, reason, body):
    lines = [header(name, desc, kv), f"# reason: {reason}"]
    return "\n".join(lines) + "\n" + body


# ── gen-rollout: failure=none baselines ─────────────────────────────────────
def emit_rollout(add):
    for topo, n, w in SHAPES:
        for strat, budgets in STRATS:
            for surge, unavail in budgets:
                kv = dict(**counts(topo, n), **ceilings(topo, n, strat, surge),
                          co="none", topo=topo)
                meta = dict(strat=strat, verb="none")
                desc = (f"{GEN_MARK} rollout {shape_tag(topo, w)} N={n} {strat} "
                        f"{budget_tag(strat, surge, unavail)}")
                if strat == "SurgeThenDrain":
                    reason = (f"plain bump: rolls within the surge budget "
                              f"(peak <= {kv['ceil_e']}), never below N ready")
                elif strat == "RecreatePod":
                    reason = ("plain bump: drain-first roll bounded by the "
                              "unavailable budget, no surge above N")
                elif is_mn(topo):
                    reason = ("plain bump: gang in-place falls back to whole-gang "
                              "recreate within the unavailable budget, no surge")
                else:
                    reason = ("plain bump: annotation change patches pods in "
                              "place (UIDs stable), no surge above N")
                if is_pd(topo):
                    reason += "; decoder/router are negative controls (co=none)"
                life = lib.lifecycle(strat=strat, surge=surge, unavail=unavail)
                add("gen-rollout", desc, kv, comps(topo, n, w, life), reason, meta,
                    cell=("rollout", topo, w, strat, surge, unavail))


# ── gen-fault: podkill during a rollout ─────────────────────────────────────
def _podkill_waivers(topo, strat, phase):
    """floor_soft/order_soft per the deterministic _fault_pick_pod victim.
    PD topologies never need one: the victim is a decoder pod (or, mid-rotation
    under a surge strategy, an engine new-revision pod), so the bumped engine's
    floor/order checks are untouched."""
    if is_pd(topo):
        return {}
    if topo == "engine":
        if strat == "SurgeThenDrain":
            return {} if phase == "rotating" else dict(floor_soft=1, order_soft=1)
        if strat == "RecreatePod":
            return {} if phase == "rotating" else dict(floor_soft=1)
        return {}
    # engine-mn: the victim is always a settled gang leader.
    if strat == "SurgeThenDrain":
        return dict(floor_soft=1, order_soft=1)
    return dict(floor_soft=1)


def _podkill_victim(topo, strat, phase):
    if topo == "engine-mn":
        return "the lowest settled gang's leader"
    if topo == "pd-mn":
        return "the decoder gang leader (component-blind, name-sorted victim)"
    if topo == "engine":
        if phase == "rotating" and strat in ("SurgeThenDrain", "RecreatePod"):
            return "the newest new-revision pod"
        return "a settled pod"
    # pd single-pod
    if phase == "rotating" and strat in ("SurgeThenDrain", "RecreatePod"):
        return "the newest engine new-revision pod"
    return "a decoder pod (component-blind, name-sorted victim)"


def _podkill_reason(topo, strat, phase, waivers):
    victim = _podkill_victim(topo, strat, phase)
    if is_pd(topo) and "decoder" in victim:
        out = (f"kill {victim}: repair must stay isolated (decoder never "
               f"surges/rotates) while the engine roll converges")
    elif "new-revision" in victim:
        out = (f"kill {victim}: the controller replaces it and the roll still "
               f"converges; settled capacity is untouched")
    else:
        out = f"kill {victim}: repair overlaps the in-flight roll, both must complete"
    if waivers.get("floor_soft"):
        out += ("; floor waived — the kill itself costs settled capacity the "
                "update budget cannot absorb while the repair restarts")
    if waivers.get("order_soft"):
        out += "; order waived — the fault-induced deletion is a drain with no preceding surge"
    return out


def emit_fault(add):
    for topo, n, w in SHAPES:
        mid = MID_SHAPE.get((topo, w))
        for strat, budgets in STRATS:
            if topo == "engine" and strat in IN_PLACE:
                continue   # constraint ledger: in-place single-pod engine victim
            for surge, unavail in budgets:
                pct = isinstance(surge, str) or isinstance(unavail, str)
                sizes = [n] + ([mid] if pct and mid else [])
                for size in sizes:
                    for phase in PHASES:
                        for rp in RESTARTS:
                            waivers = _podkill_waivers(topo, strat, phase)
                            kv = dict(**counts(topo, size),
                                      **ceilings(topo, size, strat, surge),
                                      fault=f"podkill:{phase}", **waivers,
                                      co="none", topo=topo)
                            meta = dict(strat=strat, verb=f"podkill:{phase}", rp=rp)
                            desc = (f"{GEN_MARK} podkill {phase} {shape_tag(topo, w)} "
                                    f"N={size} {strat} "
                                    f"{budget_tag(strat, surge, unavail)} rp {rp}")
                            reason = _podkill_reason(topo, strat, phase, waivers)
                            life = lib.lifecycle(strat=strat, surge=surge,
                                                 unavail=unavail,
                                                 restart=RP_FULL[rp])
                            d_life = (lib.lifecycle(restart=RP_FULL[rp])
                                      if is_pd(topo) else None)
                            add("gen-fault", desc, kv,
                                comps(topo, size, w, life, d_life=d_life),
                                reason, meta,
                                cell=("podkill", phase, topo, w, size, strat,
                                      surge, unavail, rp))


# ── gen-restart: steady-state kill x pause x strategy ───────────────────────
def _kill_reason(topo, rp, pause, target):
    scope = ("the whole instance (leader+workers) recreates"
             if rp == "RecreateInstance" and is_mn(topo)
             else "exactly the killed pod is recreated"
             if rp == "None" and is_mn(topo)
             else "the instance (one pod) recreates")
    tgt = f" from a dead {target}" if target else ""
    if pause == "":
        return f"steady-state kill{tgt}: {scope}, scoped to the killed instance, fleet returns to N"
    if pause == "true" and rp == "RecreateInstance":
        return (f"pause suspends fleet changes, not the RestartPolicy: {scope}{tgt} "
                f"while rollout-paused stays set")
    if pause == "true":
        return (f"plain pause parks the Create recovery pass: holds degraded{tgt} "
                f"through the hold window, recovers only after unpause")
    return (f"freeze holds every repair{tgt}; downgrading to a plain pause must "
            f"repair without ever unpausing")


def emit_restart(add):
    for topo, n, w in SHAPES:
        for strat, budgets in STRATS:
            surge, unavail = budgets[0]   # canonical budget: dormant under a kill
            for pause in PAUSES:
                for rp in RESTARTS:
                    if pause == "freeze" and rp == "None":
                        continue   # constraint ledger: freeze lift needs RecreateInstance
                    targets = ["leader", "worker"] if is_mn(topo) else [None]
                    for target in targets:
                        if (topo, w, strat, rp, pause, target) in HAND_PINNED_KILL:
                            continue
                        kv = dict(mode="kill", **counts(topo, n),
                                  ceil_e=n, ceil_d=(n if is_pd(topo) else 0),
                                  ceil_r=(lib.NR if is_pd(topo) else 0),
                                  restart=rp)
                        if target:
                            kv["kill"] = target
                        if pause:
                            kv["pause"] = pause
                        kv.update(co="none", topo=topo)
                        meta = dict(strat=strat, verb="kill", rp=rp,
                                    pause=pause or "off")
                        strat_note = "" if strat == "SurgeThenDrain" else f" ({strat} dormant)"
                        desc = (f"{GEN_MARK} kill {shape_tag(topo, w)} N={n} {strat} "
                                f"rp {rp}"
                                + (f" target {target}" if target else "")
                                + (f" pause {pause}" if pause else "")
                                + strat_note)
                        reason = _kill_reason(topo, rp, pause, target)
                        life = lib.lifecycle(strat=strat, surge=surge,
                                             unavail=unavail, restart=RP_FULL[rp])
                        d_life = (lib.lifecycle(restart=RP_FULL[rp])
                                  if is_pd(topo) else None)
                        add("gen-restart", desc, kv,
                            comps(topo, n, w, life, d_life=d_life), reason, meta,
                            cell=("kill", topo, w, strat, rp, pause, target))


# ── gen-crashloop: one old-revision Instance crashes while the roll lands ───
def _crash_how(form, pod="one old-revision engine pod"):
    """How the crashloop verb breaks the old-revision pod, per form."""
    return (f"{pod} is pointed at an image with no shell as the bump "
            "lands, so it crashes at every start and is never Ready again"
            if form == "start" else
            f"{pod} has its runner killed every few seconds while it "
            "is up, so it dies after Ready and comes back between crashes")


# How each strategy lands the fix on a crashing Instance.
CRASH_LANDS = {"SurgeThenDrain": "the surge brings the fixed replacement up beside it and drains it",
               "RecreatePod": "the recreate drains it and rebuilds the Instance at the fixed revision",
               "InPlaceIfPossible": "the patch lands the fixed image in place",
               "InPlaceOnly": "the patch lands the fixed image in place"}


def _crash_reason(form, strat, co):
    how = _crash_how(form)
    lands = CRASH_LANDS[strat]
    out = (f"{how}; its Instance is off the target from that moment, so the roll owns it: "
           f"{lands}, admitted within the same budget as its peers, and no repair is recorded "
           f"for it after the bump; every Instance ends on the new revision")
    if co != "none":
        out += "; the group paces engine and decoder together as on any coordinated roll"
    out += "; floor waived — the crash itself costs settled capacity for the length of the roll"
    return out


def emit_crashloop(add):
    for topo, n, w in CRASH_SHAPES:
        mid = MID_SHAPE.get((topo, w))
        for strat, budgets in CRASH_BUDGETS.items():
            for surge, unavail in budgets:
                size = mid if (isinstance(unavail, str) and mid) else n
                for rp, form in ((rp, form) for rp in RESTARTS for form in CRASH_FORMS):
                    for co in CRASH_COORDS[topo]:
                        kv = dict(**counts(topo, size), **ceilings(topo, size, strat, surge),
                                  fault="crashloop:old", form=form, floor_soft=1,
                                  co=co, topo=topo)
                        if co != "none":
                            # The group rolls the decoder too, on its own surge.
                            kv["ceil_d"] = size + 1
                        meta = dict(strat=strat, verb=f"crashloop:{form}", rp=rp, victim="engine")
                        desc = (f"[gen] crashloop {form} {shape_tag(topo, w)} N={size} {strat} "
                                f"{budget_tag(strat, surge, unavail)} rp {rp} co {co}")
                        life = lib.lifecycle(strat=strat, surge=surge, unavail=unavail,
                                             restart=RP_FULL[rp])
                        d_life = (lib.lifecycle(restart=RP_FULL[rp])
                                  if is_pd(topo) else None)
                        add("gen-crashloop", desc, kv,
                            comps(topo, size, w, life, d_life=d_life),
                            _crash_reason(form, strat, co), meta,
                            cell=("crashloop", form, topo, w, size, strat, surge, unavail, rp, co),
                            coord=lib.coord_for(co) if co != "none" else None)


# ── gen-switch: the strategy or restart policy changes while the roll lands ─
def _switch_shape(topo, n, surge):
    """Ceiling and floor for the worse of a pair's two shapes: N+1 when a
    surge step can be in flight on either side of the edit, else N; the
    floor is left to the budget's maxUnavailable (hard, never waived)."""
    return ceilings(topo, n, "SurgeThenDrain", surge)


def _switch_strategy_reason(topo, from_, to, hook, co):
    surge, unavail = switch_budget(from_, to)
    out = (f"every bumped member's updateStrategy is patched from {from_} to {to} {SWITCH_WHEN[hook]}; "
           f"a step already in flight finishes under {from_} and every step that starts afterwards "
           f"runs under {to}; the budget (maxSurge {surge}, maxUnavailable {unavail}) is valid under both")
    out += ("; the ceiling is N+1 because a surge step can be in flight on either side of the edit"
            if surge else "; neither side surges, so the ceiling is N")
    out += ("; maxUnavailable 1 gives either side the one slot it needs to drain or patch, so the floor "
            "N-1 holds throughout")
    if is_mn(topo) and IN_PLACE & {from_, to}:
        out += "; a gang runs either in-place variant as a whole-gang recreate"
    if co != "none":
        out += "; the group paces engine and decoder together and both carry the edit"
    out += f"; every Instance ends on the new revision and the live spec carries {to}"
    return out


def _switch_policy_reason(from_, to, hook, co):
    surge, unavail = SWITCH_POLICY_BUDGET
    out = (f"the roll runs under {SWITCH_POLICY_STRAT} (maxSurge {surge}, maxUnavailable {unavail}) and "
           f"every bumped member's restartPolicy is patched from {RP_FULL[from_]} to {RP_FULL[to]} "
           f"{SWITCH_WHEN[hook]}; the policy decides what a pod restart rebuilds and nothing restarts "
           f"here, so the roll surges within its ceiling N+1 with the floor at N and converges on the "
           f"new revision")
    if co != "none":
        out += "; the group paces engine and decoder together and both carry the edit"
    out += f"; the live spec carries {RP_FULL[to]}"
    return out


def _switch_crash_reason(from_, to, co):
    surge, unavail = switch_budget(from_, to)
    out = (f"{_crash_how('start')}; every bumped member's updateStrategy is patched from {from_} to {to} "
           f"{SWITCH_WHEN[SWITCH_CRASH_HOOK]}, so the crashing Instance, which the roll reaches last, is "
           f"handled under {to}: {CRASH_LANDS[to]}, admitted within the budget both strategies share "
           f"(maxSurge {surge}, maxUnavailable {unavail}); it ends on the new revision serving with no "
           f"repair recorded for it after the bump, and every other Instance ends on the new revision")
    if co != "none":
        out += "; the group paces engine and decoder together and both carry the edit"
    out += "; floor waived — the crash itself costs settled capacity for the length of the roll"
    return out


def emit_switch(add):
    for topo, n, w in SWITCH_SHAPES:
        for co in SWITCH_COORDS[topo]:
            coord = lib.coord_for(co) if co != "none" else None
            # Strategy flips: every ordered pair of distinct strategies at
            # both hooks, under the one budget valid for both.
            for from_ in SWITCH_STRATS:
                for to in SWITCH_STRATS:
                    if to == from_:
                        continue
                    surge, unavail = switch_budget(from_, to)
                    life = lib.lifecycle(strat=from_, surge=surge, unavail=unavail)
                    # A grouped decoder rolls too, so it carries the same
                    # start strategy and budget: the edit must be valid on it.
                    d_life = life if co != "none" else None
                    for hook in SWITCH_HOOKS:
                        kv = dict(**counts(topo, n), **_switch_shape(topo, n, surge), co=co, topo=topo,
                                  edit=f"strategy:{to}", during=hook)
                        if co != "none":
                            kv["ceil_d"] = n + surge
                        meta = dict(strat=from_, verb=f"edit:{hook}",
                                    edit=f"strategy {from_} to {to} at {hook}")
                        desc = (f"{GEN_MARK} switch {hook} {shape_tag(topo, w)} N={n} {from_} to {to}"
                                + (f" co {co}" if co != "none" else ""))
                        add("gen-switch", desc, kv, comps(topo, n, w, life, d_life=d_life),
                            _switch_strategy_reason(topo, from_, to, hook, co), meta,
                            cell=("switch", "strategy", hook, topo, w, n, from_, to, co), coord=coord)
            # Restart-policy flips under the canonical surge budget.
            surge, unavail = SWITCH_POLICY_BUDGET
            for from_, to in ((a, b) for a in RESTARTS for b in RESTARTS if a != b):
                life = lib.lifecycle(strat=SWITCH_POLICY_STRAT, surge=surge, unavail=unavail,
                                     restart=RP_FULL[from_])
                d_life = (life if co != "none" else
                          lib.lifecycle(restart=RP_FULL[from_]) if is_pd(topo) else None)
                for hook in SWITCH_HOOKS:
                    kv = dict(**counts(topo, n), **_switch_shape(topo, n, surge), co=co, topo=topo,
                              edit=f"restartPolicy:{RP_FULL[to]}", during=hook)
                    if co != "none":
                        kv["ceil_d"] = n + surge
                    meta = dict(strat=SWITCH_POLICY_STRAT, verb=f"edit:{hook}", rp=from_,
                                edit=f"restartPolicy {from_} to {to} at {hook}")
                    desc = (f"{GEN_MARK} switch {hook} {shape_tag(topo, w)} N={n} {SWITCH_POLICY_STRAT} "
                            f"rp {from_} to {to}" + (f" co {co}" if co != "none" else ""))
                    add("gen-switch", desc, kv, comps(topo, n, w, life, d_life=d_life),
                        _switch_policy_reason(from_, to, hook, co), meta,
                        cell=("switch", "restartPolicy", hook, topo, w, n, from_, to, co), coord=coord)
            # Broken while switching: an old-revision Instance crashes from
            # the bump and the strategy flips at the first updated pod.
            for from_, to in SWITCH_CRASH_PAIRS:
                surge, unavail = switch_budget(from_, to)
                life = lib.lifecycle(strat=from_, surge=surge, unavail=unavail)
                d_life = life if co != "none" else None
                kv = dict(**counts(topo, n), **_switch_shape(topo, n, surge), fault="crashloop:old",
                          floor_soft=1, co=co, topo=topo, edit=f"strategy:{to}", during=SWITCH_CRASH_HOOK,
                          form="start")
                if co != "none":
                    kv["ceil_d"] = n + surge
                meta = dict(strat=from_, verb=f"crashloop:start+edit:{SWITCH_CRASH_HOOK}",
                            edit=f"strategy {from_} to {to} at {SWITCH_CRASH_HOOK} while an Instance crashes")
                desc = (f"{GEN_MARK} switch {SWITCH_CRASH_HOOK} crashloop start {shape_tag(topo, w)} N={n} "
                        f"{from_} to {to}" + (f" co {co}" if co != "none" else ""))
                add("gen-switch", desc, kv, comps(topo, n, w, life, d_life=d_life),
                    _switch_crash_reason(from_, to, co), meta,
                    cell=("switch-crash", SWITCH_CRASH_HOOK, topo, w, n, from_, to, co), coord=coord)
    _emit_switch_faults(add)
    _emit_switch_held_breaks(add)


# The switch family under a break, on the six shape families (the engine
# alone, behind a router, and with a decoder and router; single and gang).
# A break of the running revision lands before the push, kept and one-shot
# for each kind; the edit fires at a moment of its own (hook, edited field,
# group) and the roll completes under it, except that a key kept away parks
# the push with its reason. The pushed revision's break, kept and once for
# each kind, meets the edit once the roll is parked on it or at the first
# pushed pod, and the fix push lands under the edit. The sibling forms break
# the decoder or the router nobody bumped before the push, and the grouped
# decoder the push breaks.
SWITCH_FAULT_SHAPES = [("engine", 4, 0, False), ("engine-mn", 2, 1, False), ("engine", 4, 0, True),
                       ("engine-mn", 2, 1, True), ("pd", 4, 0, True), ("pd-mn", 2, 1, True)]
SWITCH_FAULT_ROWS = [
    ("crashloop:idle", "start", ""), ("crashloop:idle", "start", "after-settle"),
    ("crashloop:idle", "load", ""), ("crashloop:idle", "load", "after-settle"),
    ("imagepull:idle", "", ""), ("imagepull:idle", "", "after-settle"),
    ("configkey:missing", "", "never"), ("configkey:missing", "", "after-settle"),
    ("readiness:old", "persist", ""), ("readiness:old", "recover", ""),
]
# (hook, edited field, from, to, group) for each running-revision row,
# cycling the family's own axes. The in-place start is edited at the first
# patched pod, so the broken Instance, which the roll reaches last, is
# surged rather than patched while it fails.
SWITCH_FAULT_MOMENTS = [("settling", "strategy", "SurgeThenDrain", "RecreatePod", "none"),
                        ("rotating", "strategy", "RecreatePod", "SurgeThenDrain", "none"),
                        ("rotating", "strategy", "InPlaceIfPossible", "SurgeThenDrain", "rolling"),
                        ("settling", "restartPolicy", "None", "RecreateInstance", "rolling")]
SWITCH_SIBLING_ROWS = [("crashloop:idle", "start", ""), ("crashloop:idle", "start", "after-settle")]
SWITCH_SIBLING_MOMENT = SWITCH_FAULT_MOMENTS[0]
SWITCH_HELD_BREAKS = ["crash-start", "crash-load", "imagepull", "configkey", "stuck"]
# (hook, edited field, from, to) for each pushed-revision row; the one-shot
# rows meet the strategy flips, where a restart is handled under one policy.
SWITCH_HELD_MOMENTS = [("held", "strategy", "SurgeThenDrain", "RecreatePod"),
                       ("rotating", "strategy", "RecreatePod", "SurgeThenDrain"),
                       ("held", "restartPolicy", "None", "RecreateInstance")]
SWITCH_HELD_ONCE_MOMENTS = SWITCH_HELD_MOMENTS[:2]
SWITCH_HELD_SIBLING_SHAPES = [("pd", 4, 0, True), ("pd-mn", 2, 1, True)]
# The bumped members' Instance ready timeout under a kept break of the
# pushed revision, and under a key kept away before the push: a revision
# that never comes up parks its row Failed within the wait the runner gives
# the status to say why.
SWITCH_HELD_READY_TIMEOUT = READINESS_NEW_TIMEOUT
SWITCH_HELD_WORDS = {"crash-start": "a runner that dies at every start", "crash-load": "a runner that exits after Ready",
                     "imagepull": "an image that cannot be pulled", "configkey": "a ConfigMap key the ConfigMap lacks",
                     "stuck": "a readiness probe it never passes"}
SWITCH_EDIT_WHEN = dict(SWITCH_WHEN, held="once the roll is parked on the broken pod")


def _switch_fault_kept(fault, lift):
    """True when the running revision's cause outlives the roll: the key
    kept away, which no revision starts without."""
    return fault == "configkey:missing" and lift == "never"


def _switch_fault_oneshot(fault, form, lift):
    return lift == "after-settle" or (fault == "readiness:old" and form == "recover")


def _switch_moment_life(field, from_, to, ready_timeout=None):
    """(lifecycle, maxSurge, maxUnavailable) a roll starts under at a moment:
    the pair's shared budget for a strategy flip, the canonical surge budget
    with the starting policy for a policy flip."""
    if field == "strategy":
        surge, unavail = switch_budget(from_, to)
        return lib.lifecycle(strat=from_, surge=surge, unavail=unavail, ready_timeout=ready_timeout), surge, unavail
    surge, unavail = SWITCH_POLICY_BUDGET
    return (lib.lifecycle(strat=SWITCH_POLICY_STRAT, surge=surge, unavail=unavail, restart=RP_FULL[from_],
                          ready_timeout=ready_timeout), surge, unavail)


def _switch_edit_value(field, to):
    return f"strategy:{to}" if field == "strategy" else f"restartPolicy:{RP_FULL[to]}"


def _switch_edit_words(field, from_, to):
    if field == "strategy":
        return f"every bumped member's updateStrategy is patched from {from_} to {to}"
    return f"every bumped member's restartPolicy is patched from {RP_FULL[from_]} to {RP_FULL[to]}"


def _switch_budget_words(field, from_, to, surge, unavail):
    if field == "strategy":
        out = (f"the budget (maxSurge {surge}, maxUnavailable {unavail}) is valid under both strategies and the "
               f"ceiling is {'N+1, since a surge step can be in flight on either side of the edit' if surge else 'N'}")
    else:
        out = f"the roll runs under {SWITCH_POLICY_STRAT} (maxSurge {surge}, maxUnavailable {unavail}), so the ceiling is N+1"
    return out + "; the capacity floor is lowered by the broken Instance"


def _switch_fault_kv(topo, n, router, surge, co, extra):
    kv = dict(**_crash_once_counts(topo, n, router), **_crash_once_ceilings(topo, n, router, "SurgeThenDrain", surge),
              co=co, topo=topo)
    if co != "none" and is_pd(topo):
        kv["ceil_d"] = kv["ceil_e"]
    kv.update(extra)
    return kv


def _switch_fault_comps(topo, n, w, router, life, co, cm):
    """The fleet on a six-family shape: the engine (its runner reading a key
    of the ConfigMap cm names, where the case has one), the decoder of a
    prefill/decode service (sharing the engine's lifecycle under a group),
    and the router where the shape carries one."""
    cw = w if is_mn(topo) else 0
    runner = lib.configmap_env(cm, HEAL_CONFIG_KEY, HEAL_CONFIG_VAR, gang=is_mn(topo)) if cm else None
    cs = [lib.comp("engine", n, cw, life, runner=runner)]
    if is_pd(topo):
        cs.append(lib.comp("decoder", n, cw, life if co != "none" else lib.lifecycle()))
    if router_count(topo, lib.NR if router else 0):
        cs.append(lib.comp("router", lib.NR, 0, lib.lifecycle()))
    return cs


def _switch_fault_body(name, topo, n, w, router, life, co, configkey, coord):
    """The case body as a function of the minted name: where the break needs
    a ConfigMap key, the engine runner reads one of a ConfigMap named after
    the case, carried as a second document."""
    cm = f"{name}{HEAL_CONFIG_SUFFIX}" if configkey else None
    cs = _switch_fault_comps(topo, n, w, router, life, co, cm)
    docs = [lib.configmap_doc(cm, {HEAL_CONFIG_KEY: HEAL_CONFIG_VALUE})] if cm else []
    return lib.render(name, cs, coord=coord, docs=docs)


def _switch_fault_words(fault, form, lift, victim, topo):
    pod = "leader pod" if is_mn(topo) else "pod"
    where = f"the {pod} of the {victim}'s highest Instance"
    what = {("crashloop:idle", "start"): f"{where} is pointed at an image whose runner dies at every start",
            ("crashloop:idle", "load"): f"{where} has its runner killed every few seconds once it is up",
            ("imagepull:idle", ""): f"{where} is pointed at an image that cannot be pulled",
            ("configkey:missing", ""): f"the ConfigMap key the {victim} runner reads is removed and {where} restarted, "
                                       f"so it waits in CreateContainerConfigError",
            ("readiness:old", form): f"{where} gets the readiness switch and stops passing readiness"}
    key = (fault, form if fault in ("crashloop:idle", "readiness:old") else "")
    out = what[key] + " on the settled service, and the push follows once the status has reacted to it"
    if _switch_fault_oneshot(fault, form, lift):
        out += "; the cause is taken away once the edit has fired"
    else:
        out += "; the cause stays for the whole case"
    return out


def _switch_fault_reason(fault, form, lift, hook, field, from_, to, co, topo, router, victim, surge, unavail):
    kept = _switch_fault_kept(fault, lift)
    out = (f"the operator edits a roll over a fleet whose {victim} is already broken: "
           f"{_switch_fault_words(fault, form, lift, victim, topo)}; {_switch_edit_words(field, from_, to)} "
           f"{SWITCH_EDIT_WHEN[hook]}")
    if kept:
        out += (f"; no revision starts without the key, so the push parks on its first pod with the reason "
                f"(a Failed row at the Instance ready timeout, an InstanceFailed event), nothing of the new revision "
                f"serves and the hold is kept; the edit is on the live spec meanwhile")
    else:
        pacing = (f"a step already in flight finishes under {from_} and every step afterwards runs under {to}"
                  if field == "strategy" else "the policy decides what a pod restart rebuilds and the roll's pacing is unchanged")
        out += (f"; {pacing}, so the roll completes with every Instance of the bumped members on the new revision serving, "
                f"every row settled, nothing held and the live spec carrying {to if field == 'strategy' else RP_FULL[to]}")
        if victim == "engine":
            out += (f"; the broken Instance is off the target from the moment of the push, so the roll owns it: it ends "
                    f"on the new revision serving with no repair opened for it after the push")
        else:
            out += (f"; the {victim} is not bumped, so it ends Ready and serving on its running revision, "
                    + ("healed in place or rebuilt by a repair" if _switch_fault_oneshot(fault, form, lift)
                       else "rebuilt by a repair that names it"))
    out += "; " + _switch_budget_words(field, from_, to, surge, unavail)
    if is_mn(topo) and field == "strategy" and IN_PLACE & {from_, to}:
        out += "; a gang runs the in-place variant as a whole-gang recreate"
    if co != "none":
        out += ("; the group paces engine and decoder together and both carry the edit" if is_pd(topo)
                else "; the single-member group puts the roll under the run model")
    return out


def _switch_held_reason(kind, once, hook, field, from_, to, co, topo, router, victim, surge, unavail):
    pod = "leader pod" if is_mn(topo) else "pod"
    edit = _switch_edit_words(field, from_, to)
    when = "as the break shows" if hook == "held" else "at the first pushed pod"
    if once:
        how = {"crash-start": "has its runner killed once during its first start, before it was ever Ready, and the kubelet restarts it",
               "crash-load": "has its runner killed once after it is Ready and serving, and the kubelet restarts it",
               "imagepull": "is pointed at an image that cannot be pulled while its runner is starting and pointed back once the pull failure has settled",
               "configkey": "reads a key the ConfigMap lacks, which is added once the wait has settled",
               "stuck": "gets the readiness switch while its runner is starting and loses it once the drop has settled"}[kind]
        out = (f"the operator edits a roll whose push breaks once: the push's first {victim} {pod} {how}; {edit} {when}; "
               f"the roll completes on its own under the edit with no hold behind it, the Instance Ready and serving on "
               f"the new revision, every row settled, nothing escalated, and the live spec carries the edit; a rebuild is "
               f"reported where the restart policy promises one, never required, never twice")
    else:
        out = (f"the operator edits a roll whose push cannot start: the push gives the {victim} runner {SWITCH_HELD_WORDS[kind]}, "
               f"so the roll parks on its first {pod}: the kubelet shows the break and the status says why (a rolloutHold "
               f"gate from the retry ladder or the budget, a Failed row, a RetryHeld or InstanceFailed event); {edit} "
               f"{SWITCH_EDIT_WHEN[hook]}; then the fix push, the override dropped, lands under the edit on every Instance "
               f"of the bumped members, nothing of the broken revision is created again or left, no hold is left and the "
               f"live spec carries the edit")
        if kind != "crash-load":
            out += "; nothing of the broken revision ever served"
    if field == "strategy":
        out += f"; the budget (maxSurge {surge}, maxUnavailable {unavail}) is valid under both strategies"
    else:
        out += f"; the roll runs under {SWITCH_POLICY_STRAT} (maxSurge {surge}, maxUnavailable {unavail}) and the policy decides what a pod restart rebuilds"
    if victim != "engine":
        out += f"; the engine's own roll converges or holds meanwhile and the fix lands on it too"
    if co != "none":
        out += "; the group paces engine and decoder together and both carry the edit"
    return out


def _emit_switch_faults(add):
    def one(topo, n, w, router, fault, form, lift, hook, field, from_, to, co, victim):
        shape = _lift_shape(topo, w, router)
        configkey = fault == "configkey:missing"
        life, surge, unavail = _switch_moment_life(field, from_, to,
                                                   ready_timeout=(SWITCH_HELD_READY_TIMEOUT if configkey else None))
        extra = dict(fault=fault, edit=_switch_edit_value(field, to), during=hook)
        if form:
            extra["form"] = form
        if lift:
            extra["lift"] = lift
        if victim != "engine":
            extra["victimcomp"] = victim
        kv = _switch_fault_kv(topo, n, router, surge, co, extra)
        words = fault.partition(":")[0] + (f" {form}" if form else "") + (f" {lift}" if lift else "")
        strat = from_ if field == "strategy" else SWITCH_POLICY_STRAT
        meta = dict(strat=strat, verb=f"edit:{hook}+{fault}", hook=hook, fault=fault, form=form, lift=lift,
                    edit=f"{field} {from_} to {to} at {hook} over {words}" + (f" on the {victim}" if victim != "engine" else ""),
                    switchbreak=f"{words} before the push" + (f" on the {victim}" if victim != "engine" else ""))
        if field == "restartPolicy":
            meta["rp"] = from_
        desc = (f"{GEN_MARK} switch {hook} over {words}" + (f" on the {victim}" if victim != "engine" else "")
                + f" {shape} N={n} {field} {from_} to {to} co {co}")
        coord = lib.coord_for(co, comps=("engine", "decoder") if is_pd(topo) else ("engine",)) if co != "none" else None
        add("gen-switch", desc, kv, None,
            _switch_fault_reason(fault, form, lift, hook, field, from_, to, co, topo, router, victim, surge, unavail),
            meta, cell=("switch-fault", fault, form, lift, hook, field, from_, to, co, topo, w, router, victim),
            body=functools.partial(_switch_fault_body, topo=topo, n=n, w=w, router=router, life=life, co=co,
                                   configkey=configkey, coord=coord))

    for topo, n, w, router in SWITCH_FAULT_SHAPES:
        for fault, form, lift in SWITCH_FAULT_ROWS:
            for hook, field, from_, to, co in SWITCH_FAULT_MOMENTS:
                one(topo, n, w, router, fault, form, lift, hook, field, from_, to, co, "engine")
    hook, field, from_, to, co = SWITCH_SIBLING_MOMENT
    for topo, n, w, router in SWITCH_FAULT_SHAPES:
        victim = "decoder" if is_pd(topo) else ("router" if router else "")
        if not victim:
            continue
        for fault, form, lift in SWITCH_SIBLING_ROWS:
            one(topo, n, w, router, fault, form, lift, hook, field, from_, to, co, victim)


def _emit_switch_held_breaks(add):
    def one(topo, n, w, router, kind, once, hook, field, from_, to, co, victim):
        shape = _lift_shape(topo, w, router)
        life, surge, unavail = _switch_moment_life(field, from_, to,
                                                   ready_timeout=(None if once else SWITCH_HELD_READY_TIMEOUT))
        extra = {"break": kind, "edit": _switch_edit_value(field, to), "during": hook}
        if once:
            extra["once"] = 1
        if victim != "engine":
            extra["victimcomp"] = victim
        kv = _switch_fault_kv(topo, n, router, surge, co, extra)
        words = kind + (" once" if once else "") + (f" on the {victim}" if victim != "engine" else "")
        strat = from_ if field == "strategy" else SWITCH_POLICY_STRAT
        meta = dict(strat=strat, verb=f"edit:{hook}+break:{kind}" + (":once" if once else ""), hook=hook,
                    held_break=kind, once=once, edit=f"{field} {from_} to {to} at {hook} by {words}",
                    switchbreak=f"{words} on the pushed revision")
        if field == "restartPolicy":
            meta["rp"] = from_
        desc = f"{GEN_MARK} switch {hook} by {words} {shape} N={n} {field} {from_} to {to} co {co}"
        coord = lib.coord_for(co, comps=("engine", "decoder") if is_pd(topo) else ("engine",)) if co != "none" else None
        add("gen-switch", desc, kv, None,
            _switch_held_reason(kind, once, hook, field, from_, to, co, topo, router, victim, surge, unavail), meta,
            cell=("switch-held", kind, once, hook, field, from_, to, co, topo, w, router, victim),
            body=functools.partial(_switch_fault_body, topo=topo, n=n, w=w, router=router, life=life, co=co,
                                   configkey=(kind == "configkey"), coord=coord))

    for topo, n, w, router in SWITCH_FAULT_SHAPES:
        for kind in SWITCH_HELD_BREAKS:
            for hook, field, from_, to in SWITCH_HELD_MOMENTS:
                one(topo, n, w, router, kind, False, hook, field, from_, to, "none", "engine")
            for hook, field, from_, to in SWITCH_HELD_ONCE_MOMENTS:
                one(topo, n, w, router, kind, True, hook, field, from_, to, "none", "engine")
    hook, field, from_, to = SWITCH_HELD_MOMENTS[0]
    for topo, n, w, router in SWITCH_HELD_SIBLING_SHAPES:
        one(topo, n, w, router, "crash-start", False, hook, field, from_, to, "rolling", "decoder")
        one(topo, n, w, router, "crash-start", True, hook, field, from_, to, "rolling", "decoder")


# ── gen-pause-repair: a pause or a freeze around the repair of a crash loop ─
def pause_group_for(co, topo):
    """The spec.rollout.groups block a pause-and-repair case carries: a
    rollingUpdate group over engine and decoder, or over the engine alone
    where there is no decoder."""
    if co == "none":
        return None
    return lib.coord_for(co, comps=(["engine", "decoder"] if is_pd(topo) else ["engine"]))


def _pause_reason(form, pause, moment, push, co):
    word = "frozen" if pause == "freeze" else "paused"
    how = ("the engine's highest Instance's pod on the settled service is pointed at an image with no shell, "
           "so it crashes at every start and is never Ready again"
           if form == "start" else
           "the engine's highest Instance's pod on the settled service has its runner killed every few seconds "
           "while it is up, so it dies after Ready and comes back between crashes")
    when = {"before": f"the service is {word} before the repair opens",
            "during": f"the service is {word} while the repair is in flight",
            "after": f"the service is {word} after the repair has finished"}[moment]
    if moment == "after":
        held = f"while {word} nothing is rebuilt"
    elif pause == "freeze" and moment == "before":
        held = "while frozen nothing is rebuilt, the broken Instance included"
    elif pause == "freeze":
        held = ("while frozen the repair already open finishes, its new pods come up and serve, and nothing "
                "else is rebuilt")
    else:
        held = ("while paused the broken Instance is still repaired on the revision it runs, since a pause holds "
                "the fleet and not the repair, and nothing else is rebuilt")
    if push == "fix":
        held += "; the fix pushed while paused lands on no Instance until the unpause"
        after = ("after the unpause the fix lands on every Instance, the broken one included, within the roll's "
                 "budget; nothing repairs that Instance again on the revision it was running, and no Instance is "
                 "left mid-operation")
    elif pause == "freeze" and moment == "before":
        after = ("after the unpause the repair opens and rebuilds the broken Instance Ready and serving on its "
                 "running revision, and everything else stays as it was")
    else:
        after = ("after the unpause nothing moves: every Instance keeps serving its running revision and no "
                 "Instance is left mid-operation")
    out = f"{how}; {when}; {held}; {after}"
    if co != "none":
        out += ("; the rollingUpdate group lists the broken Component and changes none of this: a repair of an "
                "Instance that serves nothing is admitted without the budget or the gate")
    if push == "fix":
        out += "; floor waived — the crash itself costs settled capacity"
    return out


def emit_pause_repair(add):
    surge, unavail = PAUSE_BUDGET
    for topo, n, w in PAUSE_SHAPES:
        for rp in RESTARTS:
            for form in CRASH_FORMS:
                if (form, rp) in UNPROMISED_REPAIR:
                    continue   # constraint ledger: no repair is promised there
                for co in PAUSE_GROUPS:
                    for moment in PAUSE_MOMENTS:
                        for pause in PAUSE_DEPTHS:
                            if pause == "freeze" and moment == "after":
                                continue   # constraint ledger: nothing left for a freeze to hold
                            for push in PAUSE_PUSHES:
                                if push:
                                    kv = dict(**counts(topo, n), **ceilings(topo, n, "SurgeThenDrain", surge))
                                    if co != "none" and is_pd(topo):
                                        kv["ceil_d"] = n + 1   # the group rolls the decoder too
                                else:
                                    kv = dict(**counts(topo, n), ceil_e=n, ceil_d=(n if is_pd(topo) else 0),
                                              ceil_r=(lib.NR if is_pd(topo) else 0))
                                kv.update(pause=pause, fault="crashloop:old")
                                if push:
                                    kv["floor_soft"] = 1
                                kv.update(co=co, topo=topo, form=form, during="idle", victimcomp="engine",
                                          expect="repaired", pauseat=moment)
                                if push:
                                    kv["push"] = push
                                meta = dict(strat="SurgeThenDrain", verb=f"crashloop:{form}@idle", rp=rp,
                                            pause=pause, moment=moment, push=push or "none", victim="engine")
                                desc = (f"{GEN_MARK} {'freeze' if pause == 'freeze' else 'pause'} {moment} repair "
                                        f"crashloop {form} {shape_tag(topo, w)} N={n} rp {rp} co {co} "
                                        f"push {push or 'none'}")
                                life = lib.lifecycle(strat="SurgeThenDrain", surge=surge, unavail=unavail,
                                                     restart=RP_FULL[rp])
                                d_life = lib.lifecycle(restart=RP_FULL[rp]) if is_pd(topo) else None
                                add("gen-pause-repair", desc, kv, comps(topo, n, w, life, d_life=d_life),
                                    _pause_reason(form, pause, moment, push, co), meta,
                                    cell=("pause-repair", topo, w, form, rp, co, pause, moment, push),
                                    coord=pause_group_for(co, topo))


# ── gen-crashloop-idle / gen-crashloop-sibling: a break under a rollout group ─
def group_for(co, topo):
    """The spec.rollout.groups block a crash-loop family case carries: the
    coordination helpers over engine and decoder (the engine alone without a
    decoder), or a two-step timed canary over every Component."""
    if co == "none":
        return None
    members = ["engine", "decoder"] if is_pd(topo) else ["engine"]
    if co == "canary":
        comps_ = members + (["router"] if is_pd(topo) else [])
        return lib.canary_block(lib.canary_steps("auto", ("50%", 50), ("100%", 100)), components=comps_)
    return lib.coord_for(co, comps=members)


def _break_how(form, victim, where=""):
    pod = f"the {victim}'s highest Instance's pod" + (f" {where}" if where else "")
    if form == "start":
        return f"{pod} is pointed at an image with no shell, so it crashes at every start and is never Ready again"
    return (f"{pod} has its runner killed every few seconds while it is up, so it dies after Ready and comes "
            f"back between crashes")


def _idle_reason(form, victim, co):
    out = (f"{_break_how(form, victim, 'on the settled service')}; no bump follows, and an Instance that "
           f"serves nothing is repaired without touching its Component's budget or a group's gate, whatever "
           f"group it sits in: it is rebuilt (every pod new) Ready and serving on its running revision "
           f"within the rotate budget, a RestartTriggered event names it, and nothing surges above N")
    if co != "none":
        out += (f"; the {GROUP_NAMES[co]} group lists the {victim}, and its gate holds every consult while "
                f"no run is open, but a rebuild that takes no serving capacity offline is not a consult, so "
                f"the group holds nothing")
    return out


def _sibling_reason(form, co, sibling="decoder"):
    if co == "rolling":
        return (f"the rollingUpdate group lists both Components and admits no push that names one without the "
                f"other, so engine and decoder are bumped together and, as the bump lands, "
                f"{_break_how(form, 'decoder')}; both rolls converge within their budgets with every Instance on "
                f"the new revision, and the decoder's broken Instance is the roll's to land, not a repair's: it "
                f"ends on the new revision serving and no RestartTriggered event names it after the bump, while "
                f"the group's gate paces the two members as one run; the broken decoder's own capacity is the one "
                f"Instance the floor allows while the roll takes it first (floor_slack=1): no healthy decoder "
                f"Instance is drained below N-1 while its replacement surges within the ceiling")
    out = (f"the engine is bumped alone and, as the bump lands, {_break_how(form, sibling)}; the engine "
           f"roll converges within its budget with every engine Instance on the new revision, while the "
           f"{sibling}, which nothing bumped, keeps N with nothing rotated and its broken Instance is rebuilt "
           f"by a repair (every pod new, Ready and serving on its running revision) that a RestartTriggered "
           f"event names")
    if sibling == "router":
        out += ("; the router sits outside every rollout group and no roll or scale of the engine owns its "
                "break, so the repair is the only thing that may touch it")
    if co != "none":
        out += (f"; the {GROUP_NAMES[co]} group lists both Components, so the engine's run is open while the "
                f"decoder breaks, and a rebuild of an Instance that serves nothing takes no capacity offline "
                f"and is put to neither the decoder's budget nor the group's gate: the repair opens beside "
                f"the open run, and the engine's roll completes beside the repair")
    return out


# The idle crash-loop family's shapes: the crash-loop shapes and the engine
# behind a router, single-pod and as a gang. The router is outside every
# group and never the victim (outside a canary the runner admits engine and
# decoder pods as break victims). (topo, N, W, routers on an engine-only shape.)
CRASHLOOP_IDLE_SHAPES = [(topo, n, w, 0) for topo, n, w in CRASH_SHAPES] + [("engine", 4, 0, lib.NR), ("engine-mn", 2, 1, lib.NR)]


def emit_crashloop_idle(add):
    for topo, n, w, nr in CRASHLOOP_IDLE_SHAPES:
        for co in BREAK_GROUPS:
            if co == "seq" and not is_pd(topo):
                continue   # constraint ledger: one member makes Sequential a blueGreen group
            for victim in BREAK_VICTIMS[topo]:
                for rp in RESTARTS:
                    for form in CRASH_FORMS:
                        if (form, rp) in UNPROMISED_REPAIR:
                            continue   # constraint ledger: no repair is promised there
                        kv = dict(**counts(topo, n, nr), ceil_e=n, ceil_d=(n if is_pd(topo) else 0),
                                  ceil_r=router_count(topo, nr),
                                  fault="crashloop:old", co=co, topo=topo, form=form, during=IDLE_PHASE,
                                  victimcomp=victim, expect="repaired")
                        if co == "canary":
                            kv["canary"] = "auto"
                        meta = dict(strat="SurgeThenDrain", verb=f"crashloop:{form}@idle", rp=rp, victim=victim)
                        desc = (f"{GEN_MARK} crashloop idle {form} {shape_tag(topo, w, nr)} N={n} co {co} "
                                f"victim {victim} rp {rp}")
                        life = lib.lifecycle(restart=RP_FULL[rp])
                        add("gen-crashloop-idle", desc, kv,
                            comps(topo, n, w, life, d_life=(life if is_pd(topo) else None), nr=nr),
                            _idle_reason(form, victim, co) + ("; the router keeps its pods" if nr else ""), meta,
                            cell=("crashloop-idle", form, topo, w, n, co, victim, rp, nr),
                            coord=group_for(co, topo))
    # The router as the broken sibling on the engine+router shapes: outside
    # every group, so the plain service alone; the engine is the negative
    # control.
    for topo, n, w, nr in CRASHLOOP_IDLE_SHAPES:
        if not nr:
            continue
        for rp in RESTARTS:
            for form in CRASH_FORMS:
                if (form, rp) in UNPROMISED_REPAIR:
                    continue
                kv = dict(**counts(topo, n, nr), ceil_e=n, ceil_d=0, ceil_r=router_count(topo, nr),
                          fault="crashloop:old", co="none", topo=topo, form=form, during=IDLE_PHASE,
                          victimcomp="router", expect="repaired")
                meta = dict(strat="SurgeThenDrain", verb=f"crashloop:{form}@idle", rp=rp, victim="router")
                desc = f"{GEN_MARK} crashloop idle {form} {shape_tag(topo, w, nr)} N={n} co none victim router rp {rp}"
                life = lib.lifecycle(restart=RP_FULL[rp])
                add("gen-crashloop-idle", desc, kv, comps(topo, n, w, life, r_life=lib.lifecycle(restart=RP_FULL[rp]), nr=nr),
                    _idle_reason(form, "router", "none") + "; the engine, which nothing broke, keeps its pods and serves N",
                    meta, cell=("crashloop-idle", form, topo, w, n, "none", "router", rp, nr))


def emit_crashloop_sibling(add):
    for topo, n, w in SIBLING_SHAPES:
        for co in BREAK_GROUPS:
            for rp in RESTARTS:
                for form in CRASH_FORMS:
                    if (form, rp) in UNPROMISED_REPAIR:
                        continue   # constraint ledger: no repair is promised there
                    # A rollingUpdate group's lockstep rule denies a push that
                    # names one member: the bump then carries both, and the
                    # decoder rolls under its own surge beside the engine.
                    bump = "engine,decoder" if co == "rolling" else "engine"
                    kv = dict(**counts(topo, n), **ceilings(topo, n, "SurgeThenDrain", 1),
                              fault="crashloop:old", co=co, topo=topo, form=form,
                              victimcomp="decoder", bump=bump)
                    if co == "rolling":
                        kv["ceil_d"] = n + 1
                        # The decoder rolls too, so its floor is read under
                        # the roll: the broken Instance's own capacity is the
                        # one the floor allows while the roll takes it first.
                        kv["floor_slack"] = 1
                    if co == "canary":
                        kv["canary"] = "auto"
                    meta = dict(strat="SurgeThenDrain", verb=f"crashloop:{form}@sibling", rp=rp, victim="decoder")
                    desc = f"{GEN_MARK} crashloop sibling {form} {shape_tag(topo, w)} N={n} co {co} rp {rp}"
                    life = lib.lifecycle(strat="SurgeThenDrain", surge=1, unavail=0, restart=RP_FULL[rp])
                    add("gen-crashloop-sibling", desc, kv,
                        comps(topo, n, w, life, d_life=lib.lifecycle(restart=RP_FULL[rp])),
                        _sibling_reason(form, co), meta,
                        cell=("crashloop-sibling", form, topo, w, n, co, rp),
                        coord=group_for(co, topo))
    # The router as the broken sibling on the engine+router shapes: the
    # engine is bumped alone, the router (outside every group) breaks as the
    # bump lands and is a repair's to rebuild.
    for topo, n, w, nr in SIBLING_ROUTER_SHAPES:
        for rp in RESTARTS:
            for form in CRASH_FORMS:
                if (form, rp) in UNPROMISED_REPAIR:
                    continue
                kv = dict(**counts(topo, n, nr), **ceilings(topo, n, "SurgeThenDrain", 1, nr),
                          fault="crashloop:old", co="none", topo=topo, form=form, victimcomp="router", bump="engine")
                meta = dict(strat="SurgeThenDrain", verb=f"crashloop:{form}@sibling", rp=rp, victim="router")
                desc = f"{GEN_MARK} crashloop sibling {form} {shape_tag(topo, w, nr)} N={n} co none rp {rp}"
                life = lib.lifecycle(strat="SurgeThenDrain", surge=1, unavail=0, restart=RP_FULL[rp])
                add("gen-crashloop-sibling", desc, kv,
                    comps(topo, n, w, life, r_life=lib.lifecycle(restart=RP_FULL[rp]), nr=nr),
                    _sibling_reason(form, "none", sibling="router"), meta,
                    cell=("crashloop-sibling", form, topo, w, n, "none", rp, nr))


# ── gen-heal: a cause that disappears ───────────────────────────────────────
def _heal_coord(topo, co):
    if co == "none":
        return None
    return lib.coord_for(co, comps=("engine", "decoder") if is_pd(topo) else ("engine",))


def _heal_body(name, topo, n, w, life, d_life, co):
    """The case body: the engine runner reads one key of a ConfigMap named
    after the case, and that ConfigMap rides in the file as a second document,
    so the apply creates it and the teardown removes it with the case."""
    cm = f"{name}{HEAL_CONFIG_SUFFIX}"
    cs = comps(topo, n, w, life, d_life=d_life)
    cs[0]["runner"] = lib.configmap_env(cm, HEAL_CONFIG_KEY, HEAL_CONFIG_VAR, gang=is_mn(topo))
    return lib.render(name, cs, coord=_heal_coord(topo, co),
                      docs=[lib.configmap_doc(cm, {HEAL_CONFIG_KEY: HEAL_CONFIG_VALUE})])


def _heal_reason(rp, co, lift, topo):
    where = "gang leader" if is_mn(topo) else "pod"
    out = (f"the engine runner's ConfigMap key is removed on the highest Instance's {where} and the "
           f"runner restarted, so its next start waits in CreateContainerConfigError")
    if rp == "None":
        out += ("; the crash-loop repair opens as soon as the wait is seen (the stuck-pod clock counts from "
                "the pod's creation, and the victim is older than the grace) and rebuilds the pod, which "
                "waits on the same key")
        if co != "none":
            out += ("; the rolling group lists the engine, but a repair of an Instance that serves nothing "
                    "is admitted without its gate, so the group holds nothing")
    else:
        out += "; the runner-restart repair rebuilds the pod, which waits on the same key"
        if co != "none":
            out += ("; the group gates only a fresh crash-loop repair of a pod set that still serves, and "
                    "the runner-restart repair is not put to it, so it opens as with no group")
    if lift == "after-settle":
        out += ("; the key is put back once that wait has held for the settle window, before any attempt "
                "escalates, and the kubelet's own retry starts the container on the rebuilt pod")
        if rp == "None":
            out += ", or on the victim itself where the key returns before the repair opens"
        out += "; an open repair promotes the Instance"
    else:
        out += ("; the wait outlives the stuck-pod grace, or the attempt its Instance ready timeout, and "
                "the escalation parks the repair at Failed with its Restart kept; the key is put back once "
                "the row reads Failed, the kubelet's own retry starts the container, and the parked repair "
                "resumes when its rebuilt set comes up (a set that stayed wedged would be rebuilt again on "
                "the update retry ladder, where one is configured) and promotes the Instance")
    out += ("; the Instance is Ready and serving again on its running revision with no new revision "
            "and no reset mailbox")
    return out


def emit_heal(add):
    for topo, n, w in CRASH_SHAPES:
        for rp in RESTARTS:
            for lift in HEAL_LIFTS:
                for co in HEAL_COORDS:
                    kv = dict(**counts(topo, n), ceil_e=n, ceil_d=(n if is_pd(topo) else 0),
                              ceil_r=(lib.NR if is_pd(topo) else 0),
                              fault="configkey:missing", co=co, topo=topo,
                              during="idle", lift=lift, expect="heals")
                    meta = dict(strat="(dormant: nothing rolls)", verb=f"configkey:{lift}", rp=rp)
                    desc = (f"{GEN_MARK} heal configkey {shape_tag(topo, w)} N={n} rp {rp} "
                            f"lift {lift} co {co}")
                    timeout = HEAL_FAILED_READY_TIMEOUT if lift == "after-failed" else None
                    life = lib.lifecycle(restart=RP_FULL[rp], ready_timeout=timeout)
                    d_life = lib.lifecycle(restart=RP_FULL[rp]) if is_pd(topo) else None
                    add("gen-heal", desc, kv, None, _heal_reason(rp, co, lift, topo), meta,
                        cell=("heal", topo, w, rp, lift, co),
                        body=functools.partial(_heal_body, topo=topo, n=n, w=w, life=life,
                                               d_life=d_life, co=co))


# ── gen-imagepull: a running revision whose image stops pulling ───
def _imagepull_reason(strat, co):
    lands = {"SurgeThenDrain": "the surge brings the fixed replacement up beside it and drains it",
             "RecreatePod": "the recreate drains it and rebuilds the Instance at the fixed revision",
             "InPlaceIfPossible": "the patch lands the fixed image in place",
             "InPlaceOnly": "the patch lands the fixed image in place"}[strat]
    out = ("one old-revision engine pod is pointed at an image that cannot be pulled as the bump lands, "
           "so the kubelet parks it in ImagePullBackOff and it is never Ready again; its Instance is off "
           f"the target from that moment, so the roll owns it: {lands}, admitted within the same budget "
           "as its peers, and no repair is recorded for it after the bump; every Instance ends on the "
           "new revision")
    if co != "none":
        out += "; the group paces engine and decoder together as on any coordinated roll"
    out += "; floor waived — the broken pod costs settled capacity for the length of the roll"
    return out


def _imagepull_idle_reason(topo):
    scope = "the whole gang" if is_mn(topo) else "the pod"
    return ("one engine pod of the idle service is pointed at an image that cannot be pulled, so the "
            "kubelet parks it in ImagePullBackOff; nothing bumps the service, so the crash-loop repair "
            "owns it under either restart policy: once the pod has sat past the stuck-pod grace the "
            f"Instance is rebuilt in place ({scope}, every pod new, Ready and serving on the running "
            "revision) and a RestartTriggered event names it; no Component ever exceeds N alive")


def emit_imagepull(add):
    for topo, n, w in CRASH_SHAPES:
        for strat, (surge, unavail) in IMAGEPULL_BUDGETS:
            for rp in RESTARTS:
                for co in CRASH_COORDS[topo]:
                    kv = dict(**counts(topo, n), **ceilings(topo, n, strat, surge),
                              fault="imagepull:old", floor_soft=1, co=co, topo=topo)
                    if co != "none":
                        # The group rolls the decoder too, on its own surge.
                        kv["ceil_d"] = n + 1
                    meta = dict(strat=strat, verb="imagepull:settling", rp=rp)
                    desc = (f"{GEN_MARK} imagepull {shape_tag(topo, w)} N={n} {strat} "
                            f"{budget_tag(strat, surge, unavail)} rp {rp} co {co}")
                    life = lib.lifecycle(strat=strat, surge=surge, unavail=unavail, restart=RP_FULL[rp])
                    d_life = lib.lifecycle(restart=RP_FULL[rp]) if is_pd(topo) else None
                    add("gen-imagepull", desc, kv, comps(topo, n, w, life, d_life=d_life),
                        _imagepull_reason(strat, co), meta,
                        cell=("imagepull", "settling", topo, w, strat, rp, co),
                        coord=lib.coord_for(co) if co != "none" else None)
    for topo, n, w in CRASH_SHAPES:
        for rp in RESTARTS:
            kv = dict(**counts(topo, n), ceil_e=n, ceil_d=(n if is_pd(topo) else 0),
                      ceil_r=(lib.NR if is_pd(topo) else 0),
                      fault="imagepull:old", co="none", topo=topo,
                      during="idle", expect="repaired")
            meta = dict(strat="(dormant: nothing rolls)", verb="imagepull:idle", rp=rp)
            desc = f"{GEN_MARK} imagepull idle {shape_tag(topo, w)} N={n} rp {rp}"
            life = lib.lifecycle(restart=RP_FULL[rp])
            d_life = lib.lifecycle(restart=RP_FULL[rp]) if is_pd(topo) else None
            add("gen-imagepull", desc, kv, comps(topo, n, w, life, d_life=d_life),
                _imagepull_idle_reason(topo), meta, cell=("imagepull", "idle", topo, w, rp))


# ── gen-node: a Ready Instance loses its node ───────────────────────────────
def _node_group(topo, co):
    """The idle rolling group a node cell carries: engine and decoder on PD,
    the engine alone otherwise."""
    if co == "none":
        return None
    return lib.coord_for(co, comps=("engine", "decoder") if is_pd(topo) else ("engine",))


def _node_reason(outcome, rp, mig, co):
    if outcome == "relocated":
        out = (f"the kubelet under the highest-index engine Instance is stopped; the node reads NotReady and no "
               f"pod of the Instance on it serves; once the node's unreachable evidence is older than the deployed "
               f"force-delete threshold the policy frees the pod names it holds and the Instance is rebuilt on other "
               f"nodes while the node is still down: {NODE_RELOCATION_REBUILD[rp]}; it serves on its running revision "
               f"with no new revision and no reset mailbox; then the kubelet is started again and every pod the node "
               f"held is gone; the move is the force-delete sweep plus the ordinary rebuild, not a migration, so "
               f"migration mode {mig} plays no part")
        if co != "none":
            out += "; a rebuild that recovers capacity already lost opens no new outage, so the rolling group does not hold it"
        return out
    still = f"{NODE_NO_TRIGGER[rp]}; {NODE_NO_RELOCATION[mig]}"
    if co != "none":
        still += ("; the rolling group plays no part: no repair opens, and one for an Instance that serves "
                  "nothing would be admitted without its gate")
    if outcome == "parked":
        return (f"the kubelet under the highest-index engine Instance is stopped; the node reads NotReady and "
                f"no pod of the Instance on it serves; nothing may rebuild the Instance while the node is down: "
                f"{still}; with no force-delete policy configured (the chart default) nothing frees its pod names, "
                f"so across the hold window no new pod appears for it; then the kubelet is started again and only "
                f"the teardown must converge; a rebuild elsewhere while the node is down is the force-delete path "
                f"and fails this cell as configured")
    return (f"the kubelet under the highest-index engine Instance is stopped and, once the node has read NotReady "
            f"with no pod of the Instance on it serving for the settle window, started again; the pods it ran "
            f"were never stopped, so the Instance returns to Ready and serving on its running revision within the "
            f"budget, with no new revision and no reset mailbox; nothing else moves while the node is down: {still}")


def emit_node(add):
    for topo, n, w in NODE_SHAPES:
        for rp in RESTARTS:
            for mig in NODE_MIGRATIONS:
                life = lib.lifecycle(restart=RP_FULL[rp], mig={"mode": mig})
                d_life = lib.lifecycle(restart=RP_FULL[rp], mig={"mode": mig}) if is_pd(topo) else None
                for co in NODE_COORDS:
                    for outcome, lift in NODE_OUTCOMES:
                        _add_node_cell(add, topo, n, w, rp, mig, co, outcome, lift, life, d_life)


def _add_node_cell(add, topo, n, w, rp, mig, co, outcome, lift, life, d_life):
    # Nobody bumps the service: every ceiling is N.
    kv = dict(**counts(topo, n), **ceilings(topo, n, "SurgeThenDrain", 0),
              fault=f"node:{NODE_PARAM}", during="idle", lift=lift, expect=outcome, co=co, topo=topo)
    if outcome in NODE_NEEDS:
        kv["needs"] = NODE_NEEDS[outcome]
    meta = dict(strat="SurgeThenDrain", verb=f"node:{outcome}", rp=rp, mig=mig)
    desc = f"{GEN_MARK} node notready {shape_tag(topo, w)} N={n} rp {rp} mig {mig} co {co} {outcome}"
    add("gen-node", desc, kv, comps(topo, n, w, life, d_life=d_life),
        _node_reason(outcome, rp, mig, co), meta,
        cell=("node", outcome, topo, w, rp, mig, co), coord=_node_group(topo, co))


def emit_node_relocation(add):
    """The force-delete lane's half of the node family, after the other."""
    outcome, lift = NODE_RELOCATION
    for topo, n, w in NODE_SHAPES:
        for rp in RESTARTS:
            for mig in NODE_MIGRATIONS:
                life = lib.lifecycle(restart=RP_FULL[rp], mig={"mode": mig})
                d_life = lib.lifecycle(restart=RP_FULL[rp], mig={"mode": mig}) if is_pd(topo) else None
                for co in NODE_COORDS:
                    _add_node_cell(add, topo, n, w, rp, mig, co, outcome, lift, life, d_life)


# ── gen-node, the hook half: a node loss under an operation in flight ────────
# The operations the hook families drive (a push, a rollback, a mid-roll
# edit, a runtime edit, a scale, a delete) on the six ledger shapes, and the
# canary ladder on the gate shapes, each with the kubelet under the victim
# stopped at the operation's hook (fault=node:<hook>). The transient loss
# (lift=after-settle: the kubelet is started again once the bite has held,
# so the pods it ran serve again) runs on every operation and holds on
# either lane; the loss the node does not come back from until the
# force-delete policy has relocated the Instance (lift=after-recover,
# needs=forcedelete) runs on every operation too: under a rollback, an
# edit, a runtime edit, a scale-down and the canary the victim Instance is
# kept and rebuilt elsewhere; under a push and a scale-up it is rebuilt
# elsewhere on the revision the operation owes it; under a delete the pods
# the node held are freed and the re-apply lands elsewhere. The push,
# scale-up and delete rows of that form are minted after the family's
# other rows, so those keep their ids.
NODE_HOOK_OPS = ["push", "rollback", "edit", "runtime", "scaledown", "scaleup", "delete"]
NODE_HOOK_PERSISTENT_OPS = {"rollback", "edit", "runtime", "scaledown", "canary"}
NODE_HOOK_PERSISTENT_MORE_OPS = ["push", "scaleup", "delete"]
NODE_HOOK_FORMS = [("one-shot", "after-settle"), ("persistent", "after-recover")]
# The hook each operation offers the loss: the first pod of the new revision
# under a push or a runtime edit (the roll is visibly in flight), the first
# traffic step's soak under the canary, the moment the others land.
NODE_HOOK_AT = {"push": "rotating", "runtime": "rotating", "canary": "canary-serving@0"}
NODE_HOOK_ROLLS = {"push", "rollback", "edit", "runtime"}


def _node_hook_kv(op, topo, n, nr):
    if op == "push":
        strat, surge, unavail = HOOK_STRAT
        return dict(**counts(topo, n, nr), **ceilings(topo, n, strat, surge, nr))
    return _op_kv(op, topo, n, nr, False)


def _node_hook_comps(op, topo, n, w, nr):
    if op == "push":
        strat, surge, unavail = HOOK_STRAT
        return comps(topo, n, w, lib.lifecycle(strat=strat, surge=surge, unavail=unavail), nr=nr)
    return _op_comps(op, topo, n, w, nr, False)


def _node_hook_reason(op, form, shape, topo, gang):
    does = {"push": "the engine is bumped under SurgeThenDrain",
            "canary": "the engine is bumped under the story ladder with automatic promotion"}.get(op) or _op_does(op, False, topo)
    at = {"push": "at the first pod of the new revision", "runtime": "at the first pod of the new revision",
          "canary": "while the first step soaks its split"}.get(op, "as it lands")
    victim = {"scaledown": "the highest engine Instance the scale keeps", "scaleup": "the highest engine Instance",
              "delete": "the highest engine Instance", "canary": "the highest stable-side engine Instance"
              }.get(op, "the highest engine Instance still on the starting revision")
    out = (f"{does} on {shape}, and {at} the kubelet of the node under {victim} is stopped; the node reads NotReady "
           f"on the cluster's own grace and no pod of the Instance on it serves, every other pod that node ran going "
           f"with it, so the capacity floor is reported rather than judged")
    if form == "one-shot" and op == "delete":
        out += ("; once that has held for the settle window the kubelet is started again and the pods it held, already "
                "on their way out, leave with the teardown")
    elif form == "one-shot":
        out += ("; nothing rebuilds the Instance elsewhere in the meantime, and once that has held for the settle window "
                "the kubelet is started again: the pods it ran were never stopped, so they serve again on their own pods")
    elif op == "delete":
        out += ("; the node stays down: the pods it held are stuck on their way out until its unreachable evidence is "
                "older than the deployed force-delete threshold and the policy frees their names, so the teardown "
                "completes with nothing of the service left in the API and the re-apply comes up whole on other nodes "
                "while the node is still down; then the kubelet is started again with nothing of the service left on it")
    else:
        out += ("; the node stays down: once its unreachable evidence is older than the deployed force-delete threshold "
                "the policy frees the pod names it holds and the controller rebuilds the Instance on other nodes, Ready "
                "and serving with every pod the node held replaced, while the node is still down; then the kubelet is "
                "started again with nothing of the Instance left on it")
    end = {"push": "the roll completes with every Instance on the new revision and serving",
           "canary": "the ladder walks every step to Stable with every Instance on the canary revision and serving"
           }.get(op) or _op_end(op)
    out += f"; {end}, with every row settled, no hold left behind and the reset mailbox never written"
    if gang:
        out += ("; each Instance is a gang, so the loss takes the member the node ran and the restart policy decides "
                "whether that member or the whole gang is rebuilt")
    return out


def _add_node_hook(add, topo, n, w, nr, op, form, lift):
    """One row of the hook half: the operation with the kubelet under the
    victim stopped at its hook, in the form the lift names."""
    strat, surge, unavail = HOOK_STRAT if op == "push" else _op_strategy(op, False)
    hook = NODE_HOOK_AT.get(op, HOOK_PHASE)
    kv = dict(_node_hook_kv(op, topo, n, nr), fault=f"node:{hook}", lift=lift, co="none", topo=topo)
    if op != "delete":
        kv["floor_soft"] = 1
    if op in NODE_HOOK_ROLLS:
        kv["order_soft"] = 1
    if form == "persistent":
        kv["needs"] = NODE_NEEDS["relocated"]
    desc = (f"{GEN_MARK} node {op} {shape_tag(topo, w, nr)} N={n}"
            + (f" {strat} {budget_tag(strat, surge, unavail)}" if op in NODE_HOOK_ROLLS else "")
            + f" {form} lift {lift}")
    meta = dict(strat=strat if op in NODE_HOOK_ROLLS else "(dormant: nothing rolls)", verb=f"node:{op}",
                faultop=f"node {op}", op=op, form=form, lift=lift, hook=hook)
    add("gen-node", desc, kv, _node_hook_comps(op, topo, n, w, nr),
        _node_hook_reason(op, form, shape_words(topo, w, nr), topo, is_mn(topo)) + _op_controls(topo, nr, op),
        meta, cell=("node-hook", op, form, topo, w, nr), dedicated=(op == "runtime"))


def emit_node_hooks(add):
    """The hook half of gen-node, minted after the idle halves."""
    for topo, n, w, nr in DISRUPT_SHAPES:
        for op in NODE_HOOK_OPS:
            for form, lift in NODE_HOOK_FORMS:
                if form == "persistent" and op not in NODE_HOOK_PERSISTENT_OPS:
                    continue
                _add_node_hook(add, topo, n, w, nr, op, form, lift)
    for family, topo, n, w, nr in GATE_SHAPES:
        for form, lift in NODE_HOOK_FORMS:
            hook = NODE_HOOK_AT["canary"]
            kv = dict(gate_kv(topo, n, nr), canary="auto", fault=f"node:{hook}", lift=lift, floor_soft=1, co="none", topo=topo)
            if form == "persistent":
                kv["needs"] = NODE_NEEDS["relocated"]
            desc = f"{GEN_MARK} node canary {_canary_shape(topo, w, nr)} N={n} {form} lift {lift}"
            meta = dict(strat="SurgeThenDrain", verb="node:canary", faultop="node canary", op="canary", form=form,
                        lift=lift, hook=hook, shape=family)
            add("gen-node", desc, kv, gate_comps(topo, n, w, nr, GATE_RP),
                _node_hook_reason("canary", form, _gate_shape_words(family, n, w), topo, is_mn(topo)), meta,
                cell=("node-hook", "canary", form, topo, w, nr),
                coord=lib.canary_block(story_ladder("auto"), components=gate_group(topo)))
    form, lift = NODE_HOOK_FORMS[1]
    for topo, n, w, nr in DISRUPT_SHAPES:
        for op in NODE_HOOK_PERSISTENT_MORE_OPS:
            _add_node_hook(add, topo, n, w, nr, op, form, lift)


# ── gen-ctrlrestart: the manager restarts under an operation in flight ──────
def _ctrl_reason(cond, strat, topo, co, nr=0):
    resumes = ("the restarted manager carries nothing in memory and resumes the operation from status; "
               "nothing is created twice (peaks within the ceiling, no index collision) and nothing is "
               "deleted twice (every Component ends at its count)")
    if cond == "surge":
        at = ("right after the bump, while the first replacement is being created"
              if strat == "SurgeThenDrain" else "right after the bump, while the first source is being drained")
    elif cond == "drain":
        at = ("at the first replacement observed, while its source drains and the next replacement is due"
              if strat == "SurgeThenDrain" else "at the first rebuilt pod observed, while the next source is due to drain")
    elif cond == "migration":
        at = "at the migration accept"
    elif cond == "canary":
        at = "while the first step serves its canary"
    else:
        at = "as the drain of the tail Instances starts"
    out = f"the manager pod is deleted {at}; {resumes}"
    if cond in CTRL_ROLL_CONDITIONS:
        out += ("; the roll finishes within the same surge budget, every source drained once"
                if strat == "SurgeThenDrain" else
                "; the roll finishes within the same unavailable budget, each drained index rebuilt once")
        out += " and every Instance ends on the new revision"
        if is_pd(topo):
            out += ("; the rolling group paces engine and decoder together and its run resumes with the roll"
                    if co != "none" else "; decoder and router are negative controls (co=none)")
        out += _router_control(topo, nr)
    elif cond == "migration":
        out += ("; the replacement comes up on another node at a distinct index, the source drains once, the "
                "request is consumed exactly once and the ledger reaches Completed; the update strategy is "
                "dormant under a migration")
    elif cond == "canary":
        out += ("; the step clock and the run are status, so the ladder resumes at the same step, the timed "
                "hold elapses and the run promotes to Stable with every member on the canary revision; "
                "capacity steps run under the member's strategy: ")
        out += ("a surge of one beside the stable Instances" if strat == "SurgeThenDrain"
                else "each stepped Instance rebuilt in place, no surge above N")
        if nr and not is_pd(topo):
            out += "; the router is a group member on its default surge lifecycle and walks the ladder too"
    else:
        out += ("; exactly the tail is torn down, the survivors are untouched and never below the target; "
                "the update strategy is dormant under a scale-down")
        if nr and not is_pd(topo):
            out += "; the router keeps its pods"
    if cond == "migration" and nr and not is_pd(topo):
        out += "; the router keeps its pods"
    return out


def emit_ctrlrestart(add):
    for topo, n, w, nr in CTRL_SHAPES:
        for strat, surge, unavail in CTRL_STRATS:
            life = lib.lifecycle(strat=strat, surge=surge, unavail=unavail)
            for cond in CTRL_CONDITIONS:
                coords = ["none", "rolling"] if cond in CTRL_ROLL_CONDITIONS and is_pd(topo) else ["none"]
                for co in coords:
                    kv, coord, d_life = dict(**counts(topo, n, nr)), None, None
                    case_comps = None
                    if cond in CTRL_ROLL_CONDITIONS:
                        kv.update(ceilings(topo, n, strat, surge, nr))
                        if co != "none":
                            # The group rolls the decoder too, under the same strategy.
                            d_life = life
                            kv["ceil_d"] = n + surge
                            coord = lib.coord_for(co)
                    elif cond == "migration":
                        # The replacement surges at a new index whatever the strategy.
                        kv.update(mode="migrate", **ceilings(topo, n, "SurgeThenDrain", 1, nr), mig="Auto", inst=0)
                    elif cond == "canary":
                        kv.update(mode="canary", **ceilings(topo, n, strat, surge, nr), canary="auto",
                                  cell=CTRL_CANARY_CELL)
                        if is_pd(topo):
                            # Decoder and router are group members on their default surge lifecycle.
                            kv.update(ceil_d=n + 1, ceil_r=lib.NR + 1)
                        elif nr:
                            kv.update(ceil_r=nr + 1)
                        case_comps = comps(topo, n, w, life, nr=nr)
                        coord = lib.canary_block(lib.canary_steps("auto", *CTRL_CANARY_STEPS),
                                                 components=[c["comp"] for c in case_comps])
                    else:
                        kv.update(mode="scaledown", **ceilings(topo, n, "SurgeThenDrain", 0, nr), to_e=n // 2)
                    kv.update(fault=CTRL_FAULTS[cond], co=co, topo=topo)
                    meta = dict(strat=strat, verb=f"ctrlrestart:{cond}", cond=cond)
                    desc = (f"{GEN_MARK} ctrlrestart {cond} {shape_tag(topo, w, nr)} N={n} {strat} "
                            f"{budget_tag(strat, surge, unavail)} co {co}")
                    add("gen-ctrlrestart", desc, kv,
                        case_comps if case_comps is not None else comps(topo, n, w, life, d_life=d_life, nr=nr),
                        _ctrl_reason(cond, strat, topo, co, nr), meta,
                        cell=("ctrlrestart", cond, topo, w, strat, co, nr), coord=coord)


# ── gen-ctrlrestart, the hook half: the manager restarts under the remaining operations ──
# The operations the hook families drive beyond the push, the scale-down,
# the move and the canary the first half owns: a rollback and a mid-roll
# edit once they have fired, a runtime edit, a scale-up and a delete, on the
# six ledger shapes, each with the manager pod deleted at the operation's
# hook (fault=ctrlrestart:<hook>): the first pod of the new revision under
# a runtime edit, the moment the others land. The restart loses no pod, so
# the operation must resume from status and complete with nothing created
# or deleted twice. The persistent form (repeat=) is the repeating
# family's, beside its push and scale-up rows.
CTRL_HOOK_OPS = ["rollback", "edit", "runtime", "scaleup", "delete"]
CTRL_HOOK_AT = {"runtime": "rotating", "move": "migrating", "canary": "canary-serving"}
CTRL_HOOK_ROLLS = {"rollback", "edit", "runtime"}
CTRL_HOOK_WORDS = {"rollback": "the rollback", "edit": "the roll under its edit", "runtime": "the runtime edit",
                   "scaledown": "the scale-down", "move": "the move", "delete": "the delete", "canary": "the canary"}
# The repeating restart under the remaining operations, on the repeating
# family's shapes: the first half's push and scale-up rows plus these.
REPEAT_CTRL_OPS = ["rollback", "edit", "runtime", "scaledown", "move", "delete", "canary"]


def _ctrl_hook_end(op):
    """What the operation owes once the restarted manager has resumed it."""
    if op == "move":
        return ("the replacement comes up at a distinct index, the source drains once, the request is consumed "
                "exactly once and the ledger reaches Completed")
    if op == "canary":
        return ("the step clock and the run are status, so the ladder resumes at the same step, the timed hold "
                "elapses and the run promotes to Stable with every member on the canary revision")
    return _op_end(op)


def _ctrl_hook_reason(op, topo, w, nr, n=None):
    """What the operator does, where the manager pod is deleted and what the
    controller promises; n names the repetitions of the persistent form."""
    resumes = ("the restarted manager carries nothing in memory and resumes the operation from status: nothing is "
               "created twice (peaks within the ceiling, no index collision) and nothing is deleted twice (every "
               "Component ends at its count)")
    at = {"runtime": "at the first pod of the new revision", "move": "at the migration accept",
          "canary": "while the first step serves its canary"}.get(op, "as it lands")
    does = {"move": "engine Instance 0 is moved by request",
            "canary": "the engine is bumped under a two-step canary ladder with automatic promotion"}.get(op) \
        or _op_does(op, False, topo)
    out = f"{does} on {shape_words(topo, w, nr)}, and {at} the manager pod is deleted"
    if n:
        out += (f", and again each time it is Ready, {n} times in all, the window held open until the restarts are "
                f"spent so the later ones land on the operation's settled end")
    out += f"; {resumes}; {_ctrl_hook_end(op)}"
    if n:
        out += ("; across the window no repair opens, the pods created are those the operation explains and no pod "
                "comes back on a revision the service never pushed; after the last restart every row is Ready with "
                "no operation open and no hold")
    else:
        out += ", with every row settled and no hold left behind"
    if is_mn(topo):
        out += "; each Instance is a gang, created and drained whole"
    if op == "canary":
        if is_pd(topo):
            out += "; decoder and router are group members on their default surge lifecycle and walk the ladder too"
        elif nr:
            out += "; the router is a group member on its default surge lifecycle and walks the ladder too"
        return out
    return out + _op_controls(topo, nr, op)


def emit_ctrlrestart_hooks(add):
    """The hook half of gen-ctrlrestart, minted after the first half."""
    for topo, n, w, nr in DISRUPT_SHAPES:
        for op in CTRL_HOOK_OPS:
            strat, surge, unavail = _op_strategy(op, False)
            hook = CTRL_HOOK_AT.get(op, HOOK_PHASE)
            kv = dict(_op_kv(op, topo, n, nr, False), fault=f"ctrlrestart:{hook}", co="none", topo=topo)
            desc = (f"{GEN_MARK} ctrlrestart {op} {shape_tag(topo, w, nr)} N={n}"
                    + (f" {strat} {budget_tag(strat, surge, unavail)}" if op in CTRL_HOOK_ROLLS else ""))
            meta = dict(strat=strat if op in CTRL_HOOK_ROLLS else "(dormant: nothing rolls)", verb=f"ctrlrestart:{op}",
                        faultop=f"ctrlrestart {op}", op=op, hook=hook, half="hook")
            add("gen-ctrlrestart", desc, kv, _op_comps(op, topo, n, w, nr, False), _ctrl_hook_reason(op, topo, w, nr),
                meta, cell=("ctrl-hook", op, topo, w, nr), dedicated=(op == "runtime"))


# ── gen-ctrlrestart, the idle half: the manager restarts once on the settled service ──
# The one-shot form of the manager restart with no operation in flight, on
# the six ledger shapes: the manager pod is deleted once while nothing moves
# (fault=ctrlrestart:idle during=idle, no repeat=). A restarted manager
# reads a settled fleet from status and must start nothing. The persistent
# form (repeat=) is the repeating family's steady-state row.
def _ctrl_idle_reason(topo, w, nr):
    out = (f"nothing is pushed on {shape_words(topo, w, nr)}; the manager pod is deleted once while the service is "
           f"settled: no pod of the service is created or deleted, every Instance stays Ready and serving on its "
           f"running revision and no repair is recorded; the restarted manager carries nothing in memory and reads "
           f"a fleet with no operation open from status, so it starts none; after the restart every row is Ready "
           f"with no operation open and no hold")
    if is_mn(topo):
        out += "; each Instance is a gang and stays whole"
    return out


def emit_ctrlrestart_idle(add):
    """The idle half of gen-ctrlrestart, minted after the hook half."""
    for topo, n, w, nr in CTRL_SHAPES:
        kv = dict(**counts(topo, n, nr), **ceilings(topo, n, "SurgeThenDrain", 0, nr),
                  fault=f"ctrlrestart:{IDLE_PHASE}", co="none", topo=topo, during=IDLE_PHASE)
        meta = dict(strat="(dormant: nothing rolls)", verb=f"ctrlrestart:{IDLE_PHASE}", faultop="ctrlrestart idle",
                    half="idle")
        add("gen-ctrlrestart", f"{GEN_MARK} ctrlrestart idle once {shape_tag(topo, w, nr)} N={n}", kv,
            comps(topo, n, w, lib.lifecycle(), nr=nr), _ctrl_idle_reason(topo, w, nr), meta,
            cell=("ctrl-idle", topo, w, nr))
# ── gen-canary-*: operator stories driven through a canary ──────────────────
# Every story family walks the same ladder: four steps of capacity ahead of
# traffic with a timed soak in place of an analysis gate, on a
# fleet of STORY_N single-pod Instances, the engine's surge budget at one
# and two, on the engine-only and the prefill/decode shape.
STORY_LADDER = [("10%", 10), ("30%", 30), ("60%", 60), ("100%", 100)]
STORY_N = 8
STORY_SURGES = [1, 2]
STORY_TOPOS = ["engine", "pd"]
# The stable side breaks while the canary walks: each break kind and form,
# this many Instances at once, at the first step's split.
STORM_BREAKS = [("crashloop", "start"), ("crashloop", "load"), ("imagepull", "start")]
STORM_VICTIMS = [1, 3]
STORM_HOOK = "canary-serving@0"
# A second revision while the first canary is in flight: the change that
# lands it and the state it lands in, with and without an analysis gate.
RETARGET_CHANGES = ["rebump", "rtedit"]
RETARGET_HOOKS = ["canary-staging", "canary-serving@0"]
RETARGET_GATES = [None, "pass"]
# A pod deleted by hand while the roll is paused or held: at a manual gate,
# under the operator's pause, held by an image that cannot start, and held
# that way under a pause.
PODKILL_SHAPES = ["gate", "paused", "held", "heldpaused"]
# The image a healthy second revision runs in the podkill family, so a
# replacement rebuilt from the new template under the old label shows.
ALT_IMAGE = "${OME_EVAL_NGINX_ALT_IMAGE}"
# The capacity-gate bound where the podkill case wants the park (held) and
# where the gate must outlast the pause and the fix (heldpaused); how long
# the pause and the manual gate hold after the deletion, and the delay of
# the fix after the deletion under the pause.
HELD_READY_TIMEOUT, HELD_PAUSED_READY_TIMEOUT = "90s", "180s"
PODKILL_PAUSE_HOLD_S, PODKILL_GATE_HOLD_S, PODKILL_RELEASE_AFTER_S = 45, 30, 60

CSR_SINGLE_TEXT = (HERE / "runtime" / "csr.yaml").read_text()
CSR_GANG_TEXT = (HERE / "runtime" / "csr-multinode.yaml").read_text()
CSR_METRICS_TEXT = (HERE / "runtime" / "csr-metrics.yaml").read_text()
METRICS_ENV_HEAD = '      env:\n        - { name: TEST_STARTUP_DELAY, value: "15" }'


def dedicated_runtime(name, metrics, gang=False):
    """The fixture runtime under a per-case name, so a runtime edit touches
    one case: the gang runtime for a gang shape, else the single-pod one.
    The metrics runtime gets TEST_REV first in its env list: the entry the
    runtime env edit rewrites."""
    if gang:
        return CSR_GANG_TEXT.replace(lib.CSR_MN, name)
    if not metrics:
        return CSR_SINGLE_TEXT.replace(lib.CSR, name)
    text = CSR_METRICS_TEXT.replace("ome-coordtest-metrics", name)
    if METRICS_ENV_HEAD not in text:
        sys.exit("csr-metrics.yaml: the runner env list does not start as the generator expects")
    return text.replace(METRICS_ENV_HEAD,
                        '      env:\n        - { name: TEST_REV,           value: "v1" }\n'
                        '        - { name: TEST_STARTUP_DELAY, value: "15" }', 1)


def story_comps(topo, surge, rp, ann=None):
    """The story fleet: STORY_N single-pod Instances per Component, the
    engine surging `surge`, the restart policy on engine and decoder, the
    router on the fixture default."""
    e_life = lib.lifecycle(strat="SurgeThenDrain", surge=surge, unavail=0, restart=RP_FULL[rp])
    d_life = lib.lifecycle(strat="SurgeThenDrain", surge=1, unavail=0, restart=RP_FULL[rp]) if is_pd(topo) else None
    cs = comps(topo, STORY_N, 0, e_life, d_life=d_life)
    if ann:
        for c in cs:
            c["ann"] = dict(ann)
    return cs


def story_kv(topo, surge):
    n = STORY_N
    return dict(mode="canary", **counts(topo, n), ceil_e=n + surge,
                ceil_d=(n + 1 if is_pd(topo) else 0), ceil_r=(lib.NR + 1 if is_pd(topo) else 0))


def story_ladder(policy="auto", gate=None):
    """The four-step ladder with timed soaks (auto), manual gates, or an
    analysis gate on every step but the cutover."""
    if gate:
        return ([lib.astep(nc, tw, lib.analysis_spec()) for nc, tw in STORY_LADDER[:-1]]
                + [lib.astep(*STORY_LADDER[-1])])
    return lib.canary_steps(policy, *STORY_LADDER)


def story_group(case_comps, steps, ready_timeout=None):
    return lib.canary_block(steps, components=[c["comp"] for c in case_comps], ready_timeout=ready_timeout)


def _storm_label(kind, form):
    return kind if kind == "imagepull" else f"{kind}:{form}"


def _storm_cell(kind, form):
    return {("crashloop", "start"): "crashStart", ("crashloop", "load"): "crashLoad",
            ("imagepull", "start"): "imagePull"}[(kind, form)]


def _storm_reason(kind, form, victims, rp):
    how = {("crashloop", "start"): "crash at every start",
           ("crashloop", "load"): "die after Ready and come back between crashes",
           ("imagepull", "start"): "sit in ImagePullBackOff"}[(kind, form)]
    return (f"{victims} old-revision engine Instance(s) {how} from the first split on, under restart policy "
            f"{RP_FULL[rp]}; the canary walks all four steps to Stable inside its budgets, every broken Instance "
            f"ends on the canary revision serving, no repair is recorded for one after the bump, and the healthy "
            f"Instances keep serving above N-{victims}")


def emit_canary_crash(add):
    for topo in STORY_TOPOS:
        for surge in STORY_SURGES:
            for kind, form in STORM_BREAKS:
                for rp in RESTARTS:
                    for victims in STORM_VICTIMS:
                        kv = story_kv(topo, surge)
                        kv.update(canary="auto", fault=f"{kind}:{STORM_HOOK}")
                        if kind == "crashloop":
                            kv["form"] = form
                        kv.update(victims=victims, nophase="Failed",
                                  cell=f"canary/Serving/stableBroken[{_storm_cell(kind, form)}]",
                                  co="none", topo=topo)
                        case_comps = story_comps(topo, surge, rp)
                        desc = (f"{GEN_MARK} canary crash storm {_storm_label(kind, form)} {shape_tag(topo, 0)} "
                                f"N={STORY_N} s{surge} rp {rp} victims {victims}")
                        meta = dict(strat="SurgeThenDrain", verb=f"{_storm_label(kind, form)}@canary", rp=rp)
                        add("gen-canary-crash", desc, kv, case_comps, _storm_reason(kind, form, victims, rp), meta,
                            cell=("storm", kind, form, topo, surge, rp, victims),
                            coord=story_group(case_comps, story_ladder("auto")))


def _retarget_reason(change, hook, gate):
    what = "a second service revision" if change == "rebump" else "a runtime edit on the dedicated runtime"
    when = ("before the first canary pod is Ready" if hook.startswith("canary-staging")
            else "while the first canary serves its first split")
    out = (f"{what} lands {when}; the second change wins: the run re-targets to it (a new canary hash follows), "
           f"the run that was open closes as Superseded, no Instance is left on the intermediate revision after "
           f"Stable, and traffic never names a revision with no serving pod")
    if gate:
        out += "; every step before the cutover is gated on a passing analysis"
    return out


def emit_canary_retarget(add):
    for topo in STORY_TOPOS:
        for surge in STORY_SURGES:
            for change in RETARGET_CHANGES:
                for hook in RETARGET_HOOKS:
                    for gate in RETARGET_GATES:
                        kv = story_kv(topo, surge)
                        state = "Staging" if hook.startswith("canary-staging") else "Serving"
                        kv.update(canary="auto", during=hook, verb=change)
                        if gate:
                            kv.update(analysis=gate, failurelimit=1)
                        kv.update(cell=f"canary/{state}/newTarget[{'isvc' if change == 'rebump' else 'runtime'}]",
                                  co="none", topo=topo)
                        case_comps = story_comps(topo, surge, "None", ann=(lib.METRICS_ANN if gate else None))
                        desc = (f"{GEN_MARK} canary retarget {change} at {hook.partition('@')[0].removeprefix('canary-')} "
                                f"{shape_tag(topo, 0)} N={STORY_N} s{surge}" + (" analysis" if gate else ""))
                        meta = dict(strat="SurgeThenDrain", verb=f"{change}@{hook}", rp="None")
                        add("gen-canary-second-rev", desc, kv, case_comps, _retarget_reason(change, hook, gate), meta,
                            cell=("retarget", change, hook, topo, surge, gate),
                            coord=story_group(case_comps, story_ladder("auto", gate)),
                            runtime=(lib.CSR_METRICS if gate else None), dedicated=(change == "rtedit"))


def _deleted_pod_reason(shape):
    where = {"gate": "while the ladder waits at its first manual gate",
             "paused": "while the operator's pause holds the first split",
             "held": "while the new revision is held at the capacity gate by an image that cannot start",
             "heldpaused": "while the new revision is held by an image that cannot start and the operator has paused the roll"}[shape]
    resume = {"gate": "the promote", "paused": "the lift of the pause", "held": "a fixed image once the gate has parked",
              "heldpaused": "a fixed image under the pause and then its lift"}[shape]
    return (f"one old-revision engine pod is deleted by hand {where}; its replacement comes back labelled as what it "
            f"runs (the image its revision's pods ran, never the new template under the old label), and after {resume} "
            f"the roll finishes with every Instance on the final revision")


def emit_canary_podkill(add):
    for topo in STORY_TOPOS:
        for rp in RESTARTS:
            for shape in PODKILL_SHAPES:
                kv = story_kv(topo, 1)
                case_comps = story_comps(topo, 1, rp)
                held = shape in ("held", "heldpaused")
                kv.update(canary=("manual" if shape == "gate" else "auto"),
                          bump=",".join(c["comp"] for c in case_comps),
                          img=(runtime_fixtures.UNPULLABLE_IMAGE if held else ALT_IMAGE))
                if shape == "gate":
                    kv.update(during=STORM_HOOK, verb="podkill", verbhold=PODKILL_GATE_HOLD_S)
                elif shape == "paused":
                    kv.update(during=STORM_HOOK, verb="pause", verbhold=PODKILL_PAUSE_HOLD_S,
                              verb2="podkill", during2="canary-serving")
                elif shape == "held":
                    kv.update(during="canary-staging", verb="podkill", verb2="rebump-fix", during2="canary-failed",
                              terminal="Stable")
                else:
                    kv.update(during="canary-staging", verb="pause", verbhold=PODKILL_PAUSE_HOLD_S,
                              verb2="podkill", during2="canary-staging",
                              release="rebump-fix", releaseafter=PODKILL_RELEASE_AFTER_S, terminal="Stable")
                kv.update(victim="stable", victimcomp="engine",
                          cell=f"canary/{'Staging' if held else 'Serving'}/capacityDropped[stable]",
                          co="none", topo=topo)
                ready_timeout = {"held": HELD_READY_TIMEOUT, "heldpaused": HELD_PAUSED_READY_TIMEOUT}.get(shape)
                desc = f"{GEN_MARK} canary podkill {shape} {shape_tag(topo, 0)} N={STORY_N} rp {rp}"
                meta = dict(strat="SurgeThenDrain", verb=f"podkill@{shape}", rp=rp)
                add("gen-canary-podkill", desc, kv, case_comps, _deleted_pod_reason(shape), meta,
                    cell=("podkill", shape, topo, rp),
                    coord=story_group(case_comps, story_ladder("manual" if shape == "gate" else "auto"),
                                         ready_timeout=ready_timeout))
    _emit_canary_pod_faults(add)


# ── gen-canary-fault-old: the stable side breaks at each step kind ───────────
# The step kinds that differ in mechanism, each a hook of the story ladder:
#   prestart  the break lands on the idle service and the push meets a fleet
#             that is already broken (fault=<kind>:idle);
#   capacity  the first step builds canary capacity ahead of any traffic
#             (Pending);
#   traffic   the second step soaks its split (auto): part of the stable
#             side is already replaced;
#   gate      the second step waits at a manual gate, which the harness
#             promotes BREAK_GATE_HOLD_S after the break, as an operator would;
#   analysis  the second step is gated on a passing analysis (the metrics
#             runtime);
#   promote   the last gate (manual, the third step): the promote after the
#             break is the cutover that drains what is left of the stable side.
# (name, fault hook, promotion, analysis gate)
STABLE_BREAK_STEPS = [
    ("prestart", "idle", "auto", None),
    ("capacity", "canary-staging", "auto", None),
    ("traffic", "canary-serving@1", "auto", None),
    ("gate", "canary-serving@1", "manual", None),
    ("analysis", "canary-serving@1", "auto", "pass"),
    ("promote", "canary-serving@2", "manual", None),
]
# The surge budget is live where capacity builds right after the break (the
# push on a broken fleet, the capacity step, a soak whose end opens the next
# step) and dormant while a gate holds; the storm shape (three victims)
# matters where the stable side's remaining capacity does, which the manual
# gate at the second step does not change against the first split's storm.
STABLE_BREAK_SURGES = {"prestart": [1, 2], "capacity": [1, 2], "traffic": [1, 2],
                       "gate": [1], "analysis": [1], "promote": [1]}
STABLE_BREAK_VICTIMS = {"prestart": [1, 3], "capacity": [1, 3], "traffic": [1, 3],
                        "gate": [1], "analysis": [1, 3], "promote": [1, 3]}
# The break kinds per gate. The metrics runtime can be broken only by a pull
# failure: its image is a static binary with no shell to exec a kill into,
# and no fixture image fails to start under its entrypoint.
STABLE_BREAK_KINDS = {None: STORM_BREAKS, "pass": [("imagepull", "start")]}
BREAK_GATE_HOLD_S = 30
STABLE_BREAK_STATE = {"prestart": "Idle", "capacity": "Staging", "traffic": "Serving[soak]",
                      "gate": "Serving[gate]", "analysis": "Serving[analysis]", "promote": "Serving[lastGate]"}


def _stable_break_when(step, victims):
    when = {"prestart": "before the push lands (the break bites on the idle service and the status reacts to it first)",
            "capacity": "while the first step builds canary capacity ahead of any traffic",
            "traffic": "while the second step soaks its split",
            "gate": f"while the second step waits at its manual gate, promoted {BREAK_GATE_HOLD_S}s after the break",
            "analysis": "while the second step's analysis gate samples",
            "promote": "while the last gate waits for the promote that cuts over"}[step]
    if step == "promote" and victims == 3:
        when += (", so every stable Instance left is broken and the split names a revision with no serving pod "
                 "until the cutover (reported, not failed)")
    return when


def _stable_break_reason(step, kind, form, victims, rp, gate):
    how = {("crashloop", "start"): "crash at every start",
           ("crashloop", "load"): "die after Ready and come back between crashes",
           ("imagepull", "start"): "sit in ImagePullBackOff"}[(kind, form)]
    out = (f"{victims} old-revision engine Instance(s) {how} {_stable_break_when(step, victims)}, under restart policy "
           f"{RP_FULL[rp]}; the ladder walks every step to Stable inside its budgets, every broken Instance ends on "
           f"the canary revision serving, no repair is opened for one after the push, and the healthy Instances "
           f"keep serving above N-{victims}")
    if gate:
        out += "; every step before the cutover is gated on a passing analysis, which the broken stable pods do not feed"
    return out


def emit_canary_fault_old(add):
    for step, hook, policy, gate in STABLE_BREAK_STEPS:
        for topo in STORY_TOPOS:
            for surge in STABLE_BREAK_SURGES[step]:
                for kind, form in STABLE_BREAK_KINDS[gate]:
                    for rp in RESTARTS:
                        for victims in STABLE_BREAK_VICTIMS[step]:
                            kv = story_kv(topo, surge)
                            kv.update(canary=policy, fault=f"{kind}:{hook}")
                            if kind == "crashloop":
                                kv["form"] = form
                            kv["victims"] = victims
                            if policy == "manual":
                                kv["verbhold"] = BREAK_GATE_HOLD_S
                            if gate:
                                kv.update(analysis=gate, failurelimit=1)
                            kv.update(nophase="Failed",
                                      cell=f"canary/{STABLE_BREAK_STATE[step]}/stableBroken[{_storm_cell(kind, form)}]",
                                      co="none", topo=topo)
                            case_comps = story_comps(topo, surge, rp, ann=(lib.METRICS_ANN if gate else None))
                            desc = (f"{GEN_MARK} canary fault-old {_storm_label(kind, form)} at {step} {shape_tag(topo, 0)} "
                                    f"N={STORY_N} s{surge} rp {rp} victims {victims}" + (" analysis" if gate else ""))
                            meta = dict(strat="SurgeThenDrain", verb=f"{_storm_label(kind, form)}@{step}", rp=rp)
                            add("gen-canary-fault-old", desc, kv, case_comps,
                                _stable_break_reason(step, kind, form, victims, rp, gate), meta,
                                cell=("fault-old", step, kind, form, topo, surge, rp, victims, gate),
                                coord=story_group(case_comps, story_ladder(policy, gate)),
                                runtime=(lib.CSR_METRICS if gate else None))
    _emit_canary_stable_breaks(add)


# ── the remaining stable-side breaks under a canary ─────────────────────────
# On the ledger's six shape families as the engine unit's ladder (the gate
# family's shapes): a serving stable pod that stops passing readiness, kept
# or lifted; the running revision's ConfigMap key put back once its wait has
# bitten; a crash and a pull failure taken away again; and the runner of one
# Ready stable pod killed exactly once. Positions: before the push (single-pod
# fleets), the second step's timed soak, and the same step at a manual gate
# promoted BREAK_GATE_HOLD_S after the break. A key kept away is read by both
# revisions, so the push is never its fix: the ladder parks at the step whose
# pod waits on the key, and that park is the case's exit (before the push,
# on every shape family).
STABLE_POSITIONS = {"idle": IDLE_FAULT_HOOK, "soak": "canary-serving@1", "gate": "canary-serving@1"}
STABLE_POSITION_POLICY = {"idle": "auto", "soak": "auto", "gate": "manual"}
STABLE_POSITION_STATE = {"idle": "Idle", "soak": "Serving[soak]", "gate": "Serving[gate]"}
STABLE_POSITIONS_GANG = ["soak", "gate"]
# (label, kind, form, lift, cell variant, restart policies, positions)
STABLE_ROWS = [
    ("readiness kept", "readiness", "persist", None, "readinessLost", ["None"], ["idle", "soak", "gate"]),
    ("readiness lifted", "readiness", "recover", None, "readinessLostOnce", ["None"], ["idle", "soak", "gate"]),
    ("configkey lifted", "configkey", None, "after-settle", "configMissingOnce", RESTARTS, ["idle", "soak", "gate"]),
    ("crashloop:start lifted", "crashloop", "start", "after-settle", "crashStartOnce", RESTARTS, ["idle", "soak"]),
    ("crashloop:load lifted", "crashloop", "load", "after-settle", "crashLoadOnce", RESTARTS, ["idle", "soak"]),
    ("imagepull lifted", "imagepull", None, "after-settle", "imagePullOnce", ["None"], ["idle", "soak"]),
    ("crashloop:load once", "crashloop", "load", "once", "restartOnce", RESTARTS, ["soak", "gate"]),
]
# The ladder's bound on a capacity step while a lifted cause is still away.
LIFTED_READY_TIMEOUT = "240s"
LEDGER_KIND = {"configkey": "config missing", "readiness": "readiness never passes", "imagepull": "image pull",
               ("crashloop", "start"): "crash at start", ("crashloop", "load"): "crash after Ready"}


def _ledger_row(kind, form, oneshot, side):
    name = LEDGER_KIND[(kind, form)] if kind == "crashloop" else LEDGER_KIND[kind]
    return f"{name}, {'one-shot' if oneshot else 'persistent'}, {side}"


def _canary_shape(topo, w, nr):
    return shape_tag(topo, w) + (" router" if nr and not is_pd(topo) else "")


def _canary_configkey_body(name, topo, n, w, nr, rp, coord, ready_timeout=None):
    """The body of a canary case whose engine runner reads one key of a
    ConfigMap named after the case, carried as a second document."""
    cm = f"{name}{HEAL_CONFIG_SUFFIX}"
    cs = gate_comps(topo, n, w, nr, rp)
    cs[0]["runner"] = lib.configmap_env(cm, HEAL_CONFIG_KEY, HEAL_CONFIG_VAR, gang=is_mn(topo))
    return lib.render(name, cs, coord=coord, docs=[lib.configmap_doc(cm, {HEAL_CONFIG_KEY: HEAL_CONFIG_VALUE})])


def _stable_position_words(pos):
    return {"idle": "before the push lands (the break bites on the idle service and the status reacts to it first)",
            "soak": "while the second step soaks its split",
            "gate": f"while the second step waits at its manual gate, promoted {BREAK_GATE_HOLD_S}s after the break"}[pos]


def _stable_row_reason(label, kind, form, lift, pos, rp, family, n, w):
    shape = _gate_shape_words(family, n, w)
    how = {"readiness": "stops passing readiness while it keeps running",
           "configkey": "waits in CreateContainerConfigError once its ConfigMap key is removed and its runner restarted",
           "imagepull": "sits in ImagePullBackOff",
           ("crashloop", "start"): "crashes at every start",
           ("crashloop", "load"): "dies after Ready and comes back between crashes"}
    does = how[(kind, form)] if kind == "crashloop" and lift != "once" else how.get(kind, "")
    if lift == "once":
        return (f"the runner of one Ready old-revision engine pod is killed exactly once {_stable_position_words(pos)} on "
                f"{shape}, under restart policy {RP_FULL[rp]}; the kubelet starts it again and the ladder walks every "
                f"step to Stable with the Instance ending on the canary revision serving, nothing escalates or holds the "
                f"one restart, no repair is opened for it after the push (its revision is not the roll target), and the "
                f"healthy Instances keep serving above N-1")
    heal = {"readiness": f"the switch is lifted {lib_seconds('LIFT_SETTLE_S')}s after the pod left rotation",
            "configkey": f"the key is put back {lib_seconds('LIFT_SETTLE_S')}s after the wait showed",
            "imagepull": f"the image is put back {lib_seconds('LIFT_SETTLE_S')}s after the break bit",
            "crashloop": (f"the killer stops {lib_seconds('LIFT_SETTLE_S')}s after the first kill" if form == "load"
                          else f"the image is put back {lib_seconds('LIFT_SETTLE_S')}s after the break bit")}[kind]
    kept = lift is None and form == "persist"
    cause = "the switch stays set until the case ends" if kept else heal
    return (f"one old-revision engine pod {does} {_stable_position_words(pos)} on {shape}, under restart policy "
            f"{RP_FULL[rp]}; {cause}; the ladder walks every step to Stable inside its budgets, the broken Instance ends "
            f"on the canary revision serving, no repair is opened for it after the push, and the healthy Instances keep "
            f"serving above N-1")


def _emit_canary_stable_breaks(add):
    for family, topo, n, w, nr in GATE_SHAPES:
        shape = _canary_shape(topo, w, nr)
        group = gate_group(topo)
        for label, kind, form, lift, variant, rps, positions in STABLE_ROWS:
            for pos in positions:
                if is_mn(topo) and pos not in STABLE_POSITIONS_GANG:
                    continue
                policy = STABLE_POSITION_POLICY[pos]
                for rp in rps:
                    kv = gate_kv(topo, n, nr)
                    kv.update(canary=policy, fault=f"{kind}:{STABLE_POSITIONS[pos]}", co="none", topo=topo)
                    if form:
                        kv["form"] = form
                    if kind in ("crashloop", "imagepull") and lift != "once":
                        kv["victims"] = 1
                    if policy == "manual":
                        kv["verbhold"] = BREAK_GATE_HOLD_S
                    if lift == "once":
                        kv["once"] = 1
                    elif lift:
                        kv["lift"] = lift
                    kv.update(nophase="Failed", cell=f"canary/{STABLE_POSITION_STATE[pos]}/stableBroken[{variant}]")
                    oneshot = lift is not None or form == "recover"
                    desc = f"{GEN_MARK} canary fault-old {label} at {pos} {shape} N={n} rp {rp}"
                    meta = dict(strat="SurgeThenDrain", verb=f"{label}@{pos}", rp=rp, shape=family,
                                row=_ledger_row(kind, form, oneshot, "running"))
                    timeout = LIFTED_READY_TIMEOUT if kind == "configkey" else None
                    coord = lib.canary_block(story_ladder(policy), components=group, ready_timeout=timeout)
                    body = None
                    if kind == "configkey":
                        body = functools.partial(_canary_configkey_body, topo=topo, n=n, w=w, nr=nr, rp=rp, coord=coord)
                    add("gen-canary-fault-old", desc, kv, None if body else gate_comps(topo, n, w, nr, rp),
                        _stable_row_reason(label, kind, form, lift, pos, rp, family, n, w), meta,
                        cell=("stable-break", label, pos, topo, nr, rp), coord=coord, body=body)
    # Minted after every other stable row, so the rows above keep their ids.
    for family, topo, n, w, nr in GATE_SHAPES:
        _emit_canary_kept_key(add, family, topo, n, w, nr, _canary_shape(topo, w, nr), gate_group(topo))
    _emit_canary_kept_stable_breaks(add)


def _stable_kept_key_reason(family, n, w):
    shape = _gate_shape_words(family, n, w)
    return (f"the operator pushes a canary while the ConfigMap key one old-revision engine pod reads is kept away on "
            f"{shape} (the pod waits in CreateContainerConfigError and the status reacted to it before the push); both "
            f"revisions read the key, so the push cannot fix it: the stable-side break does not hold the ladder, the pod "
            f"placed for the first capacity step waits on the same key, the ladder parks Failed at that step naming the "
            f"missing key, the stable side keeps serving every Instance the break and the ladder did not take, no "
            f"traffic reaches the step whose pod never came up, and nothing rolls back")


def _emit_canary_kept_key(add, family, topo, n, w, nr, shape, group):
    """The running revision's key kept away before the push: the ladder is
    not held by the stable-side break and parks at the step whose pod waits
    on the key (terminal=Failed), on every shape family."""
    kv = gate_kv(topo, n, nr)
    kv.update(canary="auto", fault=f"configkey:{STABLE_POSITIONS['idle']}", lift="never", terminal="Failed", co="none",
              topo=topo, cell=f"canary/{STABLE_POSITION_STATE['idle']}/stableBroken[configMissing]")
    coord = lib.canary_block(story_ladder("auto"), components=group, ready_timeout=HELD_READY_TIMEOUT)
    desc = f"{GEN_MARK} canary fault-old configkey kept at idle {shape} N={n} rp {GATE_RP}"
    meta = dict(strat="SurgeThenDrain", verb="configkey kept@idle", rp=GATE_RP, shape=family,
                row=_ledger_row("configkey", None, False, "running"))
    add("gen-canary-fault-old", desc, kv, None, _stable_kept_key_reason(family, n, w), meta,
        cell=("stable-break", "configkey kept", "idle", topo, nr, GATE_RP), coord=coord,
        body=functools.partial(_canary_configkey_body, topo=topo, n=n, w=w, nr=nr, rp=GATE_RP, coord=coord))


# ── gen-canary-fault-new: the push brings a broken revision ─────────────────
# Where the break bites is the axis. A crash at start and an image that cannot
# be pulled bite at staging (the capacity gate never fills); a crash after
# Ready bites inside the first step's soak (the runner exits before the soak
# ends) or past it (it outlives the soak and dies while the next step's
# capacity builds). A pod's deaths after it served are the canary's across
# steps: the first restarts the soak and the second parks the ladder where it
# stands, so a crash past the soak parks at the step of the pod's second
# death, the step of its first death or the next one when the restarted soak
# elapsed between them. The exit at the park is a rollback or a fix push that
# drops the broken runner. (name, kind, form, exitafter)
STORY_SOAK_S = int(lib.CANARY_PAUSE["auto"]["duration"].rstrip("s"))
CRASH_EXIT_IN_SOAK_S, CRASH_EXIT_AFTER_SOAK_S = 10, 40
NEW_BREAKS = [
    ("crashStart", "crashloop", "start", None),
    ("imagePull", "imagepull", None, None),
    ("crashInSoak", "crashloop", "load", CRASH_EXIT_IN_SOAK_S),
    ("crashAfterSoak", "crashloop", "load", CRASH_EXIT_AFTER_SOAK_S),
]
NEW_BREAK_EXITS = [("rollback", "RolledBack"), ("rebump-fix", "Stable")]
# The surge budget is dormant where the ladder parks at its first capacity
# step, which asks one Instance, and live once the step advanced. The metrics
# runtime carries only the pull failure (see STABLE_BREAK_KINDS), and only
# with the fix exit: a park before the first gate yields no analysis sample,
# which the rollback terminal's verdict requires, while the fix push walks
# the gated ladder.
NEW_BREAK_SURGES = {"crashStart": [1], "imagePull": [1], "crashInSoak": [1, 2], "crashAfterSoak": [1, 2]}
NEW_BREAK_GATES = {"crashStart": [None], "imagePull": [None, "pass"], "crashInSoak": [None], "crashAfterSoak": [None]}
# The state the break bites in, the cell's row. A crash past the soak bites
# while the next step stages and parks at the step of the pod's second death,
# that step or the one after it.
NEW_BREAK_STATE = {"crashStart": "Staging", "imagePull": "Staging", "crashInSoak": "Serving", "crashAfterSoak": "Staging[next]"}
NEW_BREAK_EXIT_CELL = {"rollback": "canary/Failed/rollback", "rebump-fix": "canary/Failed/newTarget"}


def _new_break_reason(name, exit_verb, rp, gate):
    how = {"crashStart": "the pushed engine revision dies at every start",
           "imagePull": "the pushed engine revision's image cannot be pulled",
           "crashInSoak": (f"the pushed engine revision passes readiness and exits {CRASH_EXIT_IN_SOAK_S}s later, inside "
                           f"the first step's {STORY_SOAK_S}s soak, every time the kubelet restarts it"),
           "crashAfterSoak": (f"the pushed engine revision passes readiness and exits {CRASH_EXIT_AFTER_SOAK_S}s later, "
                              f"after the first step's {STORY_SOAK_S}s soak has advanced the ladder, every time the "
                              f"kubelet restarts it")}[name]
    then = {"rollback": "a rollback drains the broken revision and holds at RolledBack with the rejected revision recorded",
            "rebump-fix": ("a fix push that drops the broken runner lands on every Instance, serving, with nothing left "
                           "on the broken revision")}[exit_verb]
    stop = ("the first death restarts the soak and the second parks the ladder at the step it lands on, the step where "
            "the break bit or the next one, with a reason" if name == "crashAfterSoak"
            else "the ladder stops at the step where the break bites and parks with a reason")
    out = (f"{how}, under restart policy {RP_FULL[rp]}; {stop}, no traffic names the broken revision at the park while "
           f"the stable side keeps serving every Instance the broken revision had not replaced, nothing promotes the "
           f"broken revision, and then {then}")
    if gate:
        out += "; the steps before the cutover are gated on a passing analysis, which the park precedes and the fix walks"
    return out


def emit_canary_fault_new(add):
    for name, kind, form, exit_after in NEW_BREAKS:
        for topo in STORY_TOPOS:
            for surge in NEW_BREAK_SURGES[name]:
                for gate in NEW_BREAK_GATES[name]:
                    for exit_verb, terminal in NEW_BREAK_EXITS:
                        if gate and exit_verb != "rebump-fix":
                            continue
                        for rp in RESTARTS:
                            kv = story_kv(topo, surge)
                            kv.update(canary="auto", fault=f"{kind}:new")
                            if form:
                                kv["form"] = form
                            if exit_after:
                                kv["exitafter"] = exit_after
                            if gate:
                                kv.update(analysis=gate, failurelimit=1)
                            kv.update(during="canary-failed", verb=exit_verb, victimcomp="engine", terminal=terminal,
                                      cell=f"canary/{NEW_BREAK_STATE[name]}/canaryBroken[{name}],{NEW_BREAK_EXIT_CELL[exit_verb]}",
                                      co="none", topo=topo)
                            case_comps = story_comps(topo, surge, rp, ann=(lib.METRICS_ANN if gate else None))
                            desc = (f"{GEN_MARK} canary fault-new {name} exit {exit_verb} {shape_tag(topo, 0)} "
                                    f"N={STORY_N} s{surge} rp {rp}" + (" analysis" if gate else ""))
                            meta = dict(strat="SurgeThenDrain", verb=f"{kind}:new:{name}", rp=rp)
                            add("gen-canary-fault-new", desc, kv, case_comps, _new_break_reason(name, exit_verb, rp, gate), meta,
                                cell=("fault-new", name, exit_verb, topo, surge, rp, gate),
                                coord=story_group(case_comps, story_ladder("auto", gate), ready_timeout=HELD_READY_TIMEOUT),
                                runtime=(lib.CSR_METRICS if gate else None))
    _emit_canary_pushed_breaks(add)


# ── the remaining pushed-revision breaks under a canary, and a taint ────────
# On the six shape families: the pushed engine revision reads a ConfigMap key
# the ConfigMap lacks or never passes readiness (kept, under both exits at
# the park; or taken away again once it has bitten), its image cannot be
# pulled for a while, the runner of its first pod is killed exactly once at
# start or after Ready, and a taint on every node armed before the push
# holds the first capacity step, lifted after the wait has shown or kept.
# (label, kind, form, lift, once, exits, cell variant, grid state, restart policies)
PUSHED_ROWS = [
    ("configkey kept", "configkey", None, "never", False, True, "configMissing", "Staging", ["None"]),
    ("configkey lifted", "configkey", None, "after-settle", False, False, "configMissingOnce", "Staging", ["None"]),
    ("readiness kept", "readiness", None, None, False, True, "readinessLost", "Staging", ["None"]),
    ("readiness lifted", "readiness", "recover", None, False, False, "readinessLostOnce", "Staging", ["None"]),
    ("imagepull lifted", "imagepull", None, "after-settle", False, False, "imagePullOnce", "Staging", ["None"]),
    ("crashloop:start once", "crashloop", "start", None, True, False, "restartOnce", "Staging", RESTARTS),
    ("crashloop:load once", "crashloop", "load", None, True, False, "restartOnce", "Serving", RESTARTS),
]
PENDING_CANARY_LIFTS = [("after-settle", "capacityReached[unschedulableLifted]", LIFTED_READY_TIMEOUT),
                        ("never", "readyTimeout[unschedulable]", HELD_READY_TIMEOUT)]


def _pushed_row_reason(label, kind, form, lift, once, exit_verb, rp, family, n, w):
    shape = _gate_shape_words(family, n, w)
    settle = lib_seconds("LIFT_SETTLE_S")
    if once:
        when = "at its first start, before it is Ready" if form == "start" else "once it is Ready, inside the first soak"
        return (f"the runner of the pushed engine revision's first pod is killed exactly once {when} on {shape}, under "
                f"restart policy {RP_FULL[rp]}; the kubelet starts it again, nothing escalates or holds the one restart, "
                f"the ladder walks every step to Stable and every Instance ends on the canary revision serving, with a "
                f"rebuild only where the restart policy promises one")
    if exit_verb:
        how = {"configkey": "reads a ConfigMap key the ConfigMap lacks, so its pod waits in CreateContainerConfigError",
               "readiness": "runs and never passes readiness"}[kind]
        then = {"rollback": "a rollback drains the broken revision and holds at RolledBack with the rejected revision recorded",
                "rebump-fix": ("a fix push that drops the runner override lands on every Instance, serving, with nothing "
                               "left on the broken revision")}[exit_verb]
        return (f"the pushed engine revision {how} on {shape}, under restart policy {RP_FULL[rp]}; the ladder stops at "
                f"the step where the break bites and parks with a reason, no traffic names the broken revision at the "
                f"park while the stable side keeps serving every Instance the broken revision had not replaced, nothing "
                f"promotes the broken revision, and then {then}")
    how = {"configkey": (f"reads a ConfigMap key the ConfigMap lacks, so its first pod waits in CreateContainerConfigError "
                         f"until the key is added {settle}s after the wait showed"),
           "readiness": (f"has the readiness switch set in its first pod as it starts, so the pod runs without passing "
                         f"readiness until the switch is lifted {settle}s after the drop has settled"),
           "imagepull": (f"has its first pod pointed at an image that cannot be pulled as it starts, and pointed back at "
                         f"its image {settle}s after the pull failure showed")}[kind]
    return (f"the pushed engine revision {how} on {shape}, under restart policy {RP_FULL[rp]}; the capacity step waits "
            f"inside its bound and never parks, the same pod passes once the cause is gone, nothing escalates or holds "
            f"it, and the ladder walks every step to Stable with every Instance on the canary revision serving")


def _pending_canary_reason(lift, family, n, w):
    shape = _gate_shape_words(family, n, w)
    out = (f"every schedulable node carries a NoSchedule taint before the push lands on {shape}, so the first step's "
           f"canary pod cannot be placed; the Instance waits with reason Unschedulable on its row, no row reads Failed "
           f"and nothing settled is taken down to make room")
    if lift == "after-settle":
        return out + (f"; the taint is lifted {lib_seconds('LIFT_SETTLE_S')}s after the wait showed and the ladder walks "
                      f"every step to Stable with every Instance on the canary revision serving")
    return out + ("; the taint stays, so the capacity step parks the ladder Failed at its ready timeout with the wait "
                  "still reported, the stable revision keeps serving every Instance and nothing rolls back")


def _emit_canary_pushed_breaks(add):
    for family, topo, n, w, nr in GATE_SHAPES:
        shape = _canary_shape(topo, w, nr)
        group = gate_group(topo)
        for label, kind, form, lift, once, kept, variant, state, rps in PUSHED_ROWS:
            exits = NEW_BREAK_EXITS if kept else [(None, None)]
            for exit_verb, terminal in exits:
                for rp in rps:
                    kv = gate_kv(topo, n, nr)
                    kv.update(canary="auto", fault=f"{kind}:{CRASHLOOP_NEW_HOOK}", co="none", topo=topo)
                    if form:
                        kv["form"] = form
                    if lift:
                        kv["lift"] = lift
                    if once:
                        kv["once"] = 1
                    if kind != "configkey":
                        kv["victimcomp"] = "engine"
                    if exit_verb:
                        kv.update(during="canary-failed", verb=exit_verb, terminal=terminal,
                                  cell=f"canary/{state}/canaryBroken[{variant}],{NEW_BREAK_EXIT_CELL[exit_verb]}")
                        timeout = HELD_READY_TIMEOUT
                    else:
                        kv.update(nophase="Failed", cell=f"canary/{state}/canaryBroken[{variant}]")
                        timeout = LIFTED_READY_TIMEOUT
                    desc = (f"{GEN_MARK} canary fault-new {label}" + (f" exit {exit_verb}" if exit_verb else "")
                            + f" {shape} N={n} rp {rp}")
                    meta = dict(strat="SurgeThenDrain", verb=f"{kind}:new:{label}" + (f":{exit_verb}" if exit_verb else ""),
                                rp=rp, shape=family, row=_ledger_row(kind, form, not kept, "new"))
                    coord = lib.canary_block(story_ladder("auto"), components=group, ready_timeout=timeout)
                    body = None
                    if kind == "configkey":
                        body = functools.partial(_canary_configkey_body, topo=topo, n=n, w=w, nr=nr, rp=rp, coord=coord)
                    add("gen-canary-fault-new", desc, kv, None if body else gate_comps(topo, n, w, nr, rp),
                        _pushed_row_reason(label, kind, form, lift, once, exit_verb, rp, family, n, w), meta,
                        cell=("pushed-break", label, exit_verb, topo, nr, rp), coord=coord, body=body)
        for lift, cell, timeout in PENDING_CANARY_LIFTS:
            kv = gate_kv(topo, n, nr)
            kv.update(canary="auto", fault="taint:all", lift=lift, co="none", topo=topo, cell=f"canary/Staging/{cell}")
            if lift == "never":
                kv["terminal"] = "Failed"
            else:
                kv["nophase"] = "Failed"
            desc = f"{GEN_MARK} canary fault-new taint lift {lift} {shape} N={n}"
            meta = dict(strat="SurgeThenDrain", verb=f"taint:{lift}", rp=GATE_RP, shape=family,
                        row=f"pod Pending, {'one-shot' if lift == 'after-settle' else 'persistent'}")
            add("gen-canary-fault-new", desc, kv, gate_comps(topo, n, w, nr, GATE_RP),
                _pending_canary_reason(lift, family, n, w), meta, cell=("pushed-pending", lift, topo, nr),
                coord=lib.canary_block(story_ladder("auto"), components=group, ready_timeout=timeout))
    _emit_canary_kept_pushed_breaks(add)


# ── the kept crash and pull failure under a canary on the remaining shapes ──
# The storm families break the stable side for good, and push a revision
# that crashes or cannot be pulled, on the single-pod engine and
# prefill/decode fleets; these rows run the same breaks as the engine unit's
# ladder on the gangs and the engine-behind-a-router shapes: one stable
# Instance broken at the second step's soak (a crash at start and after
# Ready under both restart policies, a pull failure under None), and the
# pushed engine revision broken at start, inside the first soak and by a
# pull failure, under both exits at the park. Minted after the families'
# other rows, so those keep their ids.
KEPT_CANARY_STORY_FAMILIES = ("engine single", "engine+decoder+router single")
KEPT_STABLE_ROWS = [("crashloop", "start", RESTARTS), ("crashloop", "load", RESTARTS), ("imagepull", None, ["None"])]
KEPT_PUSHED_NAMES = ["crashStart", "crashInSoak", "imagePull"]
KEPT_POSITION = "soak"


def _kept_stable_reason(kind, form, rp, family, n, w):
    shape = _gate_shape_words(family, n, w)
    how = {("crashloop", "start"): "crashes at every start",
           ("crashloop", "load"): "dies after Ready and comes back between crashes",
           ("imagepull", None): "sits in ImagePullBackOff"}[(kind, form)]
    pod = "the leader pod of one old-revision engine Instance" if w else "one old-revision engine pod"
    return (f"{pod} {how} {_stable_position_words(KEPT_POSITION)} on {shape}, under restart policy {RP_FULL[rp]}; the "
            f"cause stays until the ladder replaces the Instance; the ladder walks every step to Stable inside its "
            f"budgets, the broken Instance ends on the canary revision serving, no repair is opened for it after the "
            f"push, and the healthy Instances keep serving above N-1")


def _kept_pushed_reason(name, exit_verb, family, n, w):
    shape = _gate_shape_words(family, n, w)
    where = ", where the break lands on each gang Instance's leader runner" if w else ""
    return f"on {shape}{where}: {_new_break_reason(name, exit_verb, GATE_RP, None)}"


def kept_canary_shapes():
    """The gate family's shapes the storm families do not run on."""
    return [s for s in GATE_SHAPES if s[0] not in KEPT_CANARY_STORY_FAMILIES]


def _emit_canary_kept_stable_breaks(add):
    for family, topo, n, w, nr in kept_canary_shapes():
        shape = _canary_shape(topo, w, nr)
        group = gate_group(topo)
        for kind, form, rps in KEPT_STABLE_ROWS:
            label = _storm_label(kind, form or "start")
            for rp in rps:
                kv = gate_kv(topo, n, nr)
                kv.update(canary="auto", fault=f"{kind}:{STABLE_POSITIONS[KEPT_POSITION]}", victims=1, co="none", topo=topo)
                if form:
                    kv["form"] = form
                kv.update(nophase="Failed",
                          cell=f"canary/{STABLE_POSITION_STATE[KEPT_POSITION]}/stableBroken[{_storm_cell(kind, form or 'start')}]")
                desc = f"{GEN_MARK} canary fault-old {label} kept at {KEPT_POSITION} {shape} N={n} rp {rp}"
                meta = dict(strat="SurgeThenDrain", verb=f"{label}@{KEPT_POSITION}", rp=rp, shape=family,
                            row=_ledger_row(kind, form, False, "running"))
                add("gen-canary-fault-old", desc, kv, gate_comps(topo, n, w, nr, rp),
                    _kept_stable_reason(kind, form, rp, family, n, w), meta,
                    cell=("kept-stable-break", kind, form, topo, nr, rp),
                    coord=lib.canary_block(story_ladder("auto"), components=group))


def _emit_canary_kept_pushed_breaks(add):
    for family, topo, n, w, nr in kept_canary_shapes():
        shape = _canary_shape(topo, w, nr)
        group = gate_group(topo)
        for name, kind, form, exit_after in NEW_BREAKS:
            if name not in KEPT_PUSHED_NAMES:
                continue
            for exit_verb, terminal in NEW_BREAK_EXITS:
                kv = gate_kv(topo, n, nr)
                kv.update(canary="auto", fault=f"{kind}:{CRASHLOOP_NEW_HOOK}", victimcomp="engine",
                          during="canary-failed", verb=exit_verb, terminal=terminal, co="none", topo=topo)
                if form:
                    kv["form"] = form
                if exit_after:
                    kv["exitafter"] = exit_after
                kv["cell"] = f"canary/{NEW_BREAK_STATE[name]}/canaryBroken[{name}],{NEW_BREAK_EXIT_CELL[exit_verb]}"
                desc = f"{GEN_MARK} canary fault-new {name} kept exit {exit_verb} {shape} N={n} rp {GATE_RP}"
                meta = dict(strat="SurgeThenDrain", verb=f"{kind}:new:{name}:{exit_verb}", rp=GATE_RP, shape=family,
                            row=_ledger_row(kind, form, False, "new"))
                add("gen-canary-fault-new", desc, kv, gate_comps(topo, n, w, nr, GATE_RP),
                    _kept_pushed_reason(name, exit_verb, family, n, w), meta,
                    cell=("kept-pushed-break", name, exit_verb, topo, nr),
                    coord=lib.canary_block(story_ladder("auto"), components=group, ready_timeout=HELD_READY_TIMEOUT))


# ── the deletion and the eviction under a canary, once and again ────────────
# On the six shape families at the first split: an old-revision engine pod
# (a gang member on the gangs) deleted by hand again each time it is back,
# under both restart policies; evicted once under a timed soak and under a
# manual gate held until the dip closes, and evicted again each time it is
# back (the eviction loop runs under policy None).
POD_FAULT_HOOK = "canary-serving@0"


def _pod_fault_reason(kind, repeat, policy, member, rp, family, n, w):
    shape = _gate_shape_words(family, n, w)
    what = (f"the {member} pod of one old-revision engine gang" if member else "one old-revision engine pod")
    took = "deleted by hand" if kind == "podkill" else "evicted through the Eviction API (as a node drain would)"
    where = ("while the ladder waits at its first manual gate, which the harness promotes once the Instance is whole again"
             if policy == "manual" else "while the first step soaks its split")
    if repeat:
        return (f"{what} is {took} {where} on {shape}, and again each time what it took is back, {repeat} times in all, "
                f"under restart policy {RP_FULL[rp]}; each loss is reported on its row and repaired within the policy's "
                f"scope, the controller creates no more pods than the roll and the repairs explain, no pod comes back on "
                f"a revision the service never pushed, and the ladder walks every step to Stable with every Instance on "
                f"the canary revision serving")
    return (f"{what} is {took} {where} on {shape}, under restart policy {RP_FULL[rp]}; the step keeps its state across "
            f"the capacity dip, the replacement comes back on the revision it ran, and the ladder walks every step to "
            f"Stable with every Instance on the canary revision serving")


def _emit_canary_pod_faults(add):
    for family, topo, n, w, nr in GATE_SHAPES:
        shape = _canary_shape(topo, w, nr)
        group = gate_group(topo)
        members = GANG_MEMBERS if is_mn(topo) else [None]
        rows = [("podkill", HOOK_REPEAT_N, "auto", rp, member) for rp in RESTARTS for member in members]
        rows += [("evict", 0, "auto", GATE_RP, None), ("evict", 0, "manual", GATE_RP, None),
                 ("evict", HOOK_REPEAT_N, "auto", GATE_RP, None)]
        for kind, repeat, policy, rp, member in rows:
            kv = gate_kv(topo, n, nr)
            kv.update(canary=policy, fault=f"{kind}:{POD_FAULT_HOOK}", victim="stable", victimcomp="engine",
                      co="none", topo=topo)
            if member:
                kv["kill"] = member
            if repeat:
                kv["repeat"] = repeat
            if policy == "manual":
                kv["verbhold"] = BREAK_GATE_HOLD_S
            variant = {("podkill", True): "gangRepeat" if is_mn(topo) else "stableRepeat",
                       ("evict", True): "evictedRepeat", ("evict", False): "evicted"}[(kind, bool(repeat))]
            kv.update(nophase="Failed",
                      cell=f"canary/Serving{'[M]' if policy == 'manual' else ''}/capacityDropped[{variant}]")
            desc = (f"{GEN_MARK} canary podkill {kind} {'again' if repeat else 'once'} at {policy} {shape} N={n} rp {rp}"
                    + (f" target {member}" if member else ""))
            lost = "gang member lost" if is_mn(topo) else "pod deleted by hand"
            row = (f"{lost}, persistent" if kind == "podkill"
                   else f"evicted or preempted, {'persistent' if repeat else 'one-shot'}")
            meta = dict(strat="SurgeThenDrain", verb=f"{kind}@{policy}" + (f" x{repeat}" if repeat else ""), rp=rp,
                        shape=family, row=row)
            add("gen-canary-podkill", desc, kv, gate_comps(topo, n, w, nr, rp),
                _pod_fault_reason(kind, repeat, policy, member, rp, family, n, w), meta,
                cell=("pod-fault", kind, repeat, policy, topo, nr, rp, member),
                coord=lib.canary_block(story_ladder(policy), components=group))


# ── gen-canary-fault-gate: what a step's gates read breaks ──────────────────
# The canary-capable shapes the family runs: the ledger's six families, each
# as the engine unit's ladder (the router, where present, is outside the
# group and idle). Single-pod fleets run the story size; gangs run two
# Instances of one leader and one worker, where the ladder's 60 percent step
# already asks for the whole fleet.
# (ledger family, topo, Instances, gang width, router count)
GATE_SHAPES = [
    ("engine single", "engine", STORY_N, 0, 0),
    ("engine gang", "engine-mn", 2, 1, 0),
    ("engine+router single", "engine", STORY_N, 0, lib.NR),
    ("engine+router gang", "engine-mn", 2, 1, lib.NR),
    ("engine+decoder+router single", "pd", STORY_N, 0, lib.NR),
    ("engine+decoder+router gang", "pd-mn", 2, 1, lib.NR),
]
# Ladder positions: before the push (the fault meets the idle service), the
# second step's analysis gate (mid-ladder) and the third step's, the last
# gate before the cutover. A gang fleet of two has one stable Instance left
# at the second step and none at the third, so gangs run the mid-ladder
# position alone.
GATE_POSITIONS = {"idle": IDLE_FAULT_HOOK, "mid": "canary-serving@1", "last": "canary-serving@2"}
GATE_POSITIONS_GANG = ["mid"]
GATE_STATE = {"idle": "Idle", "new": "Staging", "mid": "Serving[analysis]", "last": "Serving[lastGate]"}
# The sibling rows run a timed ladder: the same positions are a soak.
SOAK_STATE = {"idle": "Idle", "new": "Staging", "mid": "Serving[soak]", "last": "Serving[lastSoak]"}
# The metric rows: the side whose metrics breach and whether the breach is
# kept or lifts (the override expires in the pod; a pushed revision's own
# breach decays). The canary side's breach from birth is the push itself
# (fault=metric:new).
METRIC_SIDES = ["stable", "canary"]
METRIC_FORMS = ["persist", "recover"]
# Failing samples a one-shot canary breach may accrue before it lifts: the
# gate samples every 20 s, the override lasts 45 s (METRIC_BREACH_S) and a
# scrape may lag by one interval, so the budget stays above three.
METRIC_RECOVER_LIMIT, METRIC_DECAY_LIMIT = 4, 6
# The source rows: how the source becomes unusable, whether it comes back,
# and what the step's policy does with a stall.
SOURCE_FORMS = ["unreachable", "garbage"]
SOURCE_LIFTS = ["after-settle", "never"]
SOURCE_POLICIES = ["Hold", "RollbackOnStall"]
# The stall timeout (the group's readyTimeout) a persistent outage parks
# at, inside the case budget, and the one a restored source never reaches.
SOURCE_PARK_READY_TIMEOUT, SOURCE_RESUME_READY_TIMEOUT = "150s", "240s"
# The sibling rows: the break kinds a running-side sibling takes for good
# (a crash after Ready under the policy that repairs it) and once (healed
# by the harness LIFT_SETTLE_S after it bit), and the new-side breaks of a
# prefill/decode unit's decoder (the fault-new kinds under the fix and the
# rollback exits).
SIBLING_PERSIST_BREAKS = [("crashloop", "start", "None"), ("imagepull", None, "None"),
                          ("crashloop", "load", "RecreateInstance")]
SIBLING_ONESHOT_BREAKS = [("crashloop", "load", "None"), ("crashloop", "load", "RecreateInstance"),
                          ("imagepull", None, "None")]
SIBLING_NEW_GANG_BREAKS = ["imagePull"]
GATE_RP = "None"


def gate_shape_sibling(nr, topo):
    """The sibling Component a shape has beside the engine unit: the decoder
    of a prefill/decode unit, else the router in front of an engine ladder;
    None on an engine-only shape."""
    if is_pd(topo):
        return "decoder"
    return "router" if nr else None


def gate_comps(topo, n, w, nr, rp, ann=None, router_ann=False):
    """The fleet of one shape: the engine (and a decoder) surging one, the
    router on the fixture default lifecycle under the same restart policy;
    `ann` goes on the unit's pods (the metrics knobs), on the router too
    when router_ann is set (a router the case rolls)."""
    e_life = lib.lifecycle(strat="SurgeThenDrain", surge=1, unavail=0, restart=RP_FULL[rp])
    cs = [lib.comp("engine", n, w, e_life, ann=dict(ann) if ann else None)]
    if is_pd(topo):
        cs.append(lib.comp("decoder", n, w, e_life, ann=dict(ann) if ann else None))
    if nr:
        cs.append(lib.comp("router", nr, 0, lib.lifecycle(restart=RP_FULL[rp]),
                           ann=(dict(ann) if ann and router_ann else None)))
    return cs


def gate_group(topo):
    """The engine unit's group: engine and decoder together on a
    prefill/decode service (the pair rolls as one unit), the engine alone
    otherwise; the router stays outside."""
    return ["engine", "decoder"] if is_pd(topo) else ["engine"]


def gate_kv(topo, n, nr, router_rolls=False):
    kv = dict(mode="canary", Ne=n, Nd=(n if is_pd(topo) else 0), Nr=nr,
              ceil_e=n + 1, ceil_d=(n + 1 if is_pd(topo) else 0),
              ceil_r=(nr + 1 if router_rolls else nr))
    return kv


def gate_ladder(failure_limit=1, on_inconclusive=None):
    """The story ladder gated on the canary-scoped error rate on every step
    but the cutover."""
    spec = lib.analysis_spec(failure_limit=failure_limit, on_inconclusive=on_inconclusive,
                             metrics=[lib.err_metric(query=lib.CANARY_QUERY)])
    return [lib.astep(nc, tw, spec) for nc, tw in STORY_LADDER[:-1]] + [lib.astep(*STORY_LADDER[-1])]


def gate_runtime(topo, metrics):
    if not metrics:
        return None
    return lib.CSR_METRICS_MN if is_mn(topo) else lib.CSR_METRICS


def _gate_positions(topo):
    return GATE_POSITIONS_GANG if is_mn(topo) else list(GATE_POSITIONS)


def _gate_when(pos, gated=True):
    if gated:
        return {"idle": "before the push lands", "new": "from the pushed revision's first sample",
                "mid": "while the second step's analysis gate samples",
                "last": "while the last gate before the cutover samples"}[pos]
    return {"idle": "before the push lands", "new": "with the push",
            "mid": "while the second step soaks", "last": "while the last step before the cutover soaks"}[pos]


def _gate_shape_words(family, n, w):
    return f"{family} ({n} Instance{'s' if n > 1 else ''}" + (f" of 1 leader + {w} worker" if w else "") + ")"


def _metric_reason(side, form, pos, family, n, w):
    shape = _gate_shape_words(family, n, w)
    lasts = {"persist": "for the rest of the case", "recover": f"for {lib_seconds('METRIC_BREACH_S')}s and then recovers"}[form]
    if side == "stable":
        return (f"the running revision's error rate breaches the gate's threshold {_gate_when(pos)} ({lasts}) on "
                f"{shape}; the gate reads the canary revision alone, so it never counts the stable side's breach, "
                f"never rolls the healthy canary back, and the ladder walks every gated step to Stable with every "
                f"Instance on the canary revision serving")
    if pos == "new":
        if form == "persist":
            return (f"the pushed revision's error rate breaches the gate's threshold from its first sample on {shape}; "
                    f"the first failing sample aborts the ladder: the canary drains back to the stable revision, which "
                    f"kept serving throughout, and the rejected revision is recorded")
        return (f"the pushed revision's error rate breaches the gate's threshold at first and recovers "
                f"{lib_seconds('METRIC_DECAY_S')}s after it serves on {shape}; the gate counts the failing samples under its "
                f"budget, never rolls back, and promotes once the samples pass: every Instance ends on the canary revision")
    if form == "persist":
        return (f"the canary revision's error rate breaches the gate's threshold {_gate_when(pos)} on {shape}, with no "
                f"failing sample before it; the first failing sample aborts the ladder: the canary drains back to the "
                f"stable revision, which kept serving throughout, and the rejected revision is recorded")
    return (f"the canary revision's error rate breaches the gate's threshold {_gate_when(pos)} for "
            f"{lib_seconds('METRIC_BREACH_S')}s and recovers on {shape}; the gate counts the failing samples under its "
            f"budget, never rolls back, and promotes once the samples pass again: every Instance ends on the canary revision")


def lib_seconds(name):
    """A runner constant's unscaled value, for the words on line two."""
    return {"METRIC_BREACH_S": 45, "METRIC_DECAY_S": 60, "LIFT_SETTLE_S": 20}[name]


def _source_reason(form, lift, policy, pos, family, n, w):
    shape = _gate_shape_words(family, n, w)
    # A run pins its analysis source when it opens, so the fault breaks the
    # store behind the address the run pinned (the case's own front for the
    # bundled store), never the address.
    how = {"unreachable": "is unreachable (the case's front for the store, which the run pinned, is aimed at nothing)",
           "garbage": ("answers garbage (the case's front for the store, which the run pinned, is aimed at a pod of the "
                       "service that answers no metrics)")}[form]
    out = (f"the analysis source {how} {_gate_when(pos)} on {shape}; the gate reports that it cannot read health, "
           f"holds the ladder at its step and counts no breach")
    if lift == "after-settle":
        return out + (f"; the store is back behind the source {lib_seconds('LIFT_SETTLE_S')}s after the hold was observed "
                      f"and the ladder resumes: every Instance ends on the canary revision serving"
                      + (" (a blip shorter than the stall timeout never rolls back)" if policy == "RollbackOnStall" else ""))
    if policy == "Hold":
        return out + ("; the store never comes back, so past the stall timeout the ladder parks Failed at that step with "
                      "reason AnalysisStalled while the stable revision keeps serving every Instance the canary has "
                      "not replaced, and nothing rolls back")
    return out + ("; the store never comes back, so past the stall timeout the fail-safe policy rolls the canary back "
                  "instead of parking it: the canary drains to the stable revision and the rejected revision is recorded")


def _sibling_how(kind, form):
    return {("crashloop", "start"): "crashes at every start", ("crashloop", "load"): "dies after Ready",
            ("imagepull", None): "sits in ImagePullBackOff"}[(kind, form)]


def _sibling_running_reason(sib, kind, form, rp, pos, oneshot, family, n, w, unit):
    shape = _gate_shape_words(family, n, w)
    how = _sibling_how(kind, form)
    when = _gate_when(pos, gated=False)
    if oneshot:
        heal = {"load": "the runner is killed once and left alone",
                None: f"the image is put back {lib_seconds('LIFT_SETTLE_S')}s after the break bit"}.get(form,
                                                                                                        f"the image is put back {lib_seconds('LIFT_SETTLE_S')}s after the break bit")
        comes = ("rebuilt by the restart policy" if rp == "RecreateInstance"
                 else "brought back by the kubelet alone, or rebuilt by the repair where one opened")
        return (f"one running-revision {sib} pod {how} once {when} ({heal}) on {shape}, under restart policy "
                f"{RP_FULL[rp]}; the engine ladder walks every step to Stable, the {sib} Instance is Ready and serving "
                f"again on its running revision ({comes}), and the status reported the break while it lasted")
    if unit:
        return (f"one running-revision {sib} Instance {how} {when} on {shape}, under restart policy {RP_FULL[rp]}; the "
                f"{sib} is part of the engine unit, so the ladder lands the fix on it: it ends on the canary revision "
                f"serving with no repair recorded for it after the push, and the healthy Instances keep serving above N-1")
    return (f"one running-revision {sib} Instance {how} {when} on {shape}, under restart policy {RP_FULL[rp]}; the "
            f"{sib} is not rolled by the engine ladder, so the repair owns it: it is rebuilt, Ready and serving on its "
            f"running revision with a RestartTriggered event naming it, the other {sib} Instance keeps serving, and the "
            f"ladder walks every step to Stable")


def _sibling_new_reason(name, exit_verb, family, n, w):
    shape = _gate_shape_words(family, n, w)
    how = {"crashStart": "dies at every start", "imagePull": "cannot be pulled",
           "crashInSoak": f"passes readiness and exits {CRASH_EXIT_IN_SOAK_S}s later, inside the first soak",
           "crashAfterSoak": f"passes readiness and exits {CRASH_EXIT_AFTER_SOAK_S}s later, past the first soak"}[name]
    then = {"rollback": "a rollback drains both sides back and holds at RolledBack with the rejected revision recorded",
            "rebump-fix": "a fix push that drops the broken runner lands on engine and decoder alike, with nothing left on the broken revision"}[exit_verb]
    park = ("parks at the step of the decoder pod's second death after serving, the step where it bit or the next one, "
            "with a reason naming the gate" if name == "crashAfterSoak" else "parks where it bit with a reason naming the gate")
    return (f"the pushed decoder revision {how} while the engine's canary is up on {shape}; the unit's step waits on "
            f"the decoder's capacity and {park}, no traffic names the broken revision at the park while the stable "
            f"side keeps serving, nothing promotes it, and then {then}")


def _sibling_park_reason(family, n, w):
    shape = _gate_shape_words(family, n, w)
    return (f"the router is pushed beside the engine ladder onto an image that cannot be pulled on {shape}; the "
            f"router rolls on its own, never promotes a pod of that revision, keeps every router Instance serving on "
            f"its running revision (an Instance whose running-revision pod still serves is kept on it, whatever failed "
            f"attempt sits beside it) and says why it stopped (a hold, a Failed row or a RetryHeld or InstanceFailed event "
            f"naming it), while the engine ladder walks every step to Stable unaffected; the router is left parked, as "
            f"the status says")


def _sibling_new_oneshot_reason(sib, rp, pos, family, n, w, router_rolls):
    shape = _gate_shape_words(family, n, w)
    side = ("the router's new revision, pushed beside the ladder," if router_rolls
            else "the decoder's canary revision")
    return (f"one pod of {side} is killed once {_gate_when(pos, gated=False)} on {shape}, under restart policy {RP_FULL[rp]}; "
            f"{'the restart policy rebuilds the Instance' if rp == 'RecreateInstance' else 'the kubelet restarts the runner'} "
            f"and the step keeps its state across the capacity dip: the ladder walks every step to Stable, every "
            f"Instance of every bumped member ends on its new revision serving, and the {sib}'s other Instances hold "
            f"the floor read with the killed Instance allowed for")


def _gate_cell(pos, event, variant, gated=True):
    return f"canary/{(GATE_STATE if gated else SOAK_STATE)[pos]}/{event}[{variant}]"


def emit_canary_fault_gate(add):
    for family, topo, n, w, nr in GATE_SHAPES:
        positions = _gate_positions(topo)
        group = gate_group(topo)
        shape = shape_tag(topo, w) + (" router" if nr and not is_pd(topo) else "")
        # The metric rows: both sides, both forms, every position.
        for side in METRIC_SIDES:
            for form in METRIC_FORMS:
                for pos in positions + (["new"] if side == "canary" and not is_mn(topo) else []):
                    if side == "canary" and pos == "idle":
                        continue   # the canary side does not exist before the push; its birth breach is :new
                    kv = gate_kv(topo, n, nr)
                    hook = {"new": CRASHLOOP_NEW_HOOK, "idle": IDLE_FAULT_HOOK}.get(pos, GATE_POSITIONS.get(pos))
                    kv.update(canary="auto", fault=f"metric:{hook}", form=form, co="none", topo=topo)
                    if pos not in ("new", "idle"):
                        kv["victim"] = side
                    if side == "stable":
                        kv.update(analysis="pass", health="pass", failurelimit=1, nophase="RollingBack")
                    elif form == "persist":
                        kv.update(analysis="breach", failurelimit=1)
                        if pos != "new":
                            kv["health"] = "pass"
                    else:
                        limit = METRIC_DECAY_LIMIT if pos == "new" else METRIC_RECOVER_LIMIT
                        kv.update(analysis="pass", failurelimit=limit, minfails=1)
                        if pos != "new":
                            kv["health"] = "pass"
                    kv["cell"] = _gate_cell(pos, "metricBreach", side)
                    case_comps = gate_comps(topo, n, w, nr, GATE_RP, ann=lib.METRICS_ANN)
                    limit = kv["failurelimit"]
                    desc = f"{GEN_MARK} canary gate metric {side} {form} at {pos} {shape} N={n}"
                    meta = dict(strat="SurgeThenDrain", verb=f"metric:{side}:{form}@{pos}", rp=GATE_RP,
                                gate=f"metric breach, {'one-shot' if form == 'recover' else 'persistent'}, "
                                     f"{'running' if side == 'stable' else 'new'}", shape=family)
                    add("gen-canary-fault-gate", desc, kv, case_comps, _metric_reason(side, form, pos, family, n, w), meta,
                        cell=("gate-metric", side, form, pos, topo, nr),
                        coord=lib.canary_block(gate_ladder(limit), components=group),
                        runtime=gate_runtime(topo, True))
        # The source rows: unreachable at every position under both lifts and
        # both stall policies; garbage at mid-ladder under both lifts (the
        # gate reads a non-answer the same way, whatever the position).
        for form in SOURCE_FORMS:
            for pos in (positions if form == "unreachable" else ["mid"]):
                for lift in SOURCE_LIFTS:
                    for policy in (SOURCE_POLICIES if form == "unreachable" and not is_mn(topo) else ["Hold"]):
                        kv = gate_kv(topo, n, nr)
                        hook = IDLE_FAULT_HOOK if pos == "idle" else GATE_POSITIONS[pos]
                        kv.update(canary="auto", fault=f"source:{hook}", form=form, lift=lift, analysis="pass",
                                  health="pass", failurelimit=1, oninconclusive=policy.lower(), co="none", topo=topo)
                        if lift == "after-settle":
                            kv["nophase"] = "RollingBack" if policy == "RollbackOnStall" else "Failed"
                            ready_timeout = SOURCE_RESUME_READY_TIMEOUT
                        else:
                            kv["terminal"] = "RolledBack" if policy == "RollbackOnStall" else "Failed"
                            ready_timeout = SOURCE_PARK_READY_TIMEOUT
                        kv["cell"] = _gate_cell(pos, "sourceUnusable", form)
                        case_comps = gate_comps(topo, n, w, nr, GATE_RP, ann=lib.METRICS_ANN)
                        desc = (f"{GEN_MARK} canary gate source {form} {lift} {policy} at {pos} {shape} N={n}")
                        meta = dict(strat="SurgeThenDrain", verb=f"source:{form}:{lift}@{pos}", rp=GATE_RP,
                                    gate=f"analysis source unusable, {'one-shot' if lift == 'after-settle' else 'persistent'}",
                                    shape=family)
                        add("gen-canary-fault-gate", desc, kv, case_comps,
                            _source_reason(form, lift, policy, pos, family, n, w), meta,
                            cell=("gate-source", form, lift, policy, pos, topo, nr),
                            coord=lib.canary_block(gate_ladder(1, policy), components=group, ready_timeout=ready_timeout),
                            runtime=gate_runtime(topo, True))
        # The sibling rows: a Component beside the engine's ladder breaks.
        sib = gate_shape_sibling(nr, topo)
        if not sib:
            continue
        unit = sib in group
        for pos in positions:
            hook = IDLE_FAULT_HOOK if pos == "idle" else GATE_POSITIONS[pos]
            for oneshot, breaks in ((False, SIBLING_PERSIST_BREAKS), (True, SIBLING_ONESHOT_BREAKS)):
                for kind, form, rp in breaks:
                    kv = gate_kv(topo, n, nr)
                    kv.update(canary="auto", fault=f"{kind}:{hook}", victimcomp=sib, victims=1, nophase="Failed",
                              co="none", topo=topo)
                    if form:
                        kv["form"] = form
                    if oneshot:
                        kv["lift"] = "after-settle"
                    label = kind if kind == "imagepull" else f"{kind}:{form}"
                    kv["cell"] = _gate_cell(pos, "siblingBroken", f"{sib}{'Once' if oneshot else ''}", gated=False)
                    case_comps = gate_comps(topo, n, w, nr, rp)
                    desc = (f"{GEN_MARK} canary gate sibling {sib} {label} {'once' if oneshot else 'persists'} at {pos} "
                            f"{shape} N={n} rp {rp}")
                    meta = dict(strat="SurgeThenDrain", verb=f"{label}:{sib}{':once' if oneshot else ''}@{pos}", rp=rp,
                                gate=f"sibling broken, {'one-shot' if oneshot else 'persistent'}, running", shape=family)
                    add("gen-canary-fault-gate", desc, kv, case_comps,
                        _sibling_running_reason(sib, kind, form, rp, pos, oneshot, family, n, w, unit), meta,
                        cell=("gate-sibling-running", sib, kind, form, rp, pos, oneshot, topo, nr),
                        coord=lib.canary_block(story_ladder("auto"), components=group))
        # The new side. A prefill/decode unit's decoder breaks on the pushed
        # revision (the fault-new kinds, under both exits); the router is
        # pushed beside the ladder onto an image that cannot be pulled.
        if unit:
            names = SIBLING_NEW_GANG_BREAKS if is_mn(topo) else [b[0] for b in NEW_BREAKS]
            for name, kind, form, exit_after in NEW_BREAKS:
                if name not in names:
                    continue
                for exit_verb, terminal in NEW_BREAK_EXITS:
                    kv = gate_kv(topo, n, nr)
                    kv.update(canary="auto", fault=f"{kind}:{CRASHLOOP_NEW_HOOK}", victimcomp="decoder",
                              during="canary-failed", verb=exit_verb, terminal=terminal, co="none", topo=topo)
                    if form:
                        kv["form"] = form
                    if exit_after:
                        kv["exitafter"] = exit_after
                    kv["cell"] = f"canary/{NEW_BREAK_STATE[name]}/siblingBroken[{name}],{NEW_BREAK_EXIT_CELL[exit_verb]}"
                    case_comps = gate_comps(topo, n, w, nr, GATE_RP)
                    desc = f"{GEN_MARK} canary gate sibling decoder pushed {name} exit {exit_verb} {shape} N={n}"
                    meta = dict(strat="SurgeThenDrain", verb=f"{kind}:new:decoder:{name}", rp=GATE_RP,
                                gate="sibling broken, persistent, new", shape=family)
                    add("gen-canary-fault-gate", desc, kv, case_comps, _sibling_new_reason(name, exit_verb, family, n, w),
                        meta, cell=("gate-sibling-new", name, exit_verb, topo),
                        coord=lib.canary_block(story_ladder("auto"), components=group, ready_timeout=HELD_READY_TIMEOUT))
        if nr and not is_pd(topo) or (nr and not is_mn(topo)):
            kv = gate_kv(topo, n, nr, router_rolls=True)
            kv.update(canary="auto", bump=",".join(group + ["router"]), img=runtime_fixtures.UNPULLABLE_IMAGE,
                      imgcomp="router", terminal="Stable", nophase="Failed", co="none", topo=topo,
                      cell=_gate_cell("new", "siblingBroken", "routerParked", gated=False))
            case_comps = gate_comps(topo, n, w, nr, GATE_RP)
            desc = f"{GEN_MARK} canary gate sibling router pushed unpullable beside the ladder {shape} N={n}"
            meta = dict(strat="SurgeThenDrain", verb="imagepull:new:router:parked", rp=GATE_RP,
                        gate="sibling broken, persistent, new", shape=family)
            add("gen-canary-fault-gate", desc, kv, case_comps, _sibling_park_reason(family, n, w), meta,
                cell=("gate-sibling-park", topo),
                coord=lib.canary_block(story_ladder("auto"), components=group))
        # The new side, once: a pod of the sibling's new revision is killed
        # once mid-ladder (the decoder's canary; a router pushed beside the
        # ladder) and comes back under the restart policy.
        for pos in [p for p in positions if p != "idle"]:
            for rp in (RESTARTS if not is_mn(topo) else [GATE_RP]):
                router_rolls = not unit
                kv = gate_kv(topo, n, nr, router_rolls=router_rolls)
                kv.update(canary="auto", fault=f"crashloop:{GATE_POSITIONS[pos]}", form="load", victim="canary",
                          victimcomp=sib, lift="after-settle", nophase="Failed", co="none", topo=topo,
                          cell=_gate_cell(pos, "siblingBroken", f"{sib}NewOnce", gated=False))
                if router_rolls:
                    kv["bump"] = ",".join(group + ["router"])
                case_comps = gate_comps(topo, n, w, nr, rp)
                desc = f"{GEN_MARK} canary gate sibling {sib} new pod killed once at {pos} {shape} N={n} rp {rp}"
                meta = dict(strat="SurgeThenDrain", verb=f"crashloop:load:{sib}:new:once@{pos}", rp=rp,
                            gate="sibling broken, one-shot, new", shape=family)
                add("gen-canary-fault-gate", desc, kv, case_comps,
                    _sibling_new_oneshot_reason(sib, rp, pos, family, n, w, router_rolls), meta,
                    cell=("gate-sibling-new-once", sib, rp, pos, topo),
                    coord=lib.canary_block(story_ladder("auto"), components=group))


# ── gen-rollback: the starting revision re-applied mid-roll ─────────────────
def _rollback_budgets(topo, strat):
    if topo == "engine" and strat in ROLLBACK_ENGINE_BUDGETS:
        return ROLLBACK_ENGINE_BUDGETS[strat]
    return ROLLBACK_BUDGETS[strat]


def _rollback_reachable(topo, strat, hook):
    """draining never arrives for a single-pod in-place roll: an
    annotation-only diff is patched in place and nothing drains."""
    return not (hook == "draining" and strat in IN_PLACE and not is_mn(topo))


def _rollback_comps(topo, n, w, life, co):
    """The engine (and, under a group, the decoder) start on a revision that
    carries the trigger annotation; a grouped decoder shares the engine's
    strategy so the hooks and ceilings read the same on both members."""
    cw = w if is_mn(topo) else 0
    cs = [lib.comp("engine", n, cw, life, ann=ROLLBACK_START_ANN)]
    if is_pd(topo):
        d_life = life if co != "none" else lib.lifecycle()
        cs.append(lib.comp("decoder", n, cw, d_life, ann=ROLLBACK_START_ANN))
        cs.append(lib.comp("router", lib.NR, 0, lib.lifecycle()))
    return cs


def _rollback_reason(hook, strat, topo, co):
    out = (f"the starting revision is re-applied at {ROLLBACK_MOMENT[hook]}; every Instance ends on it "
           f"and serving, no pod of the new revision remains, the capacity floor holds throughout and "
           f"no Instance is left mid-operation")
    if hook == "held" and strat in IN_PLACE:
        out += ("; the readiness probe is a diff beyond an in-place patch, so the new revision and its "
                "rollback are recreated within the unavailable budget")
    elif strat == "SurgeThenDrain":
        out += "; Instances already on the new revision come back under the surge budget"
    elif strat == "RecreatePod":
        out += "; Instances already on the new revision come back drain-first within the unavailable budget"
    elif is_mn(topo):
        out += "; a gang in-place roll comes back as a whole-gang recreate within the unavailable budget"
    else:
        out += "; an in-place roll comes back in place (UIDs stable)"
    if co != "none":
        out += "; the run that targeted the new revision closes as superseded and a fresh run brings the group back"
    return out


# A break beside the rollback, on the six shape families. The running
# revision breaks before the push (crash at start or after Ready, an image
# that cannot be pulled, the ConfigMap key removed, readiness that stops
# passing; kept, or taken away once the rollback has fired) and the roll is
# rolled back at each of its hooks, one (strategy, group) pair per hook so
# the family's axes are all touched; a sibling nobody bumped (the router of
# an engine fleet, the decoder outside a group) breaks the same way at the
# first new pod. The pushed revision breaks at the held moment (break=
# beyond stuck) under each strategy, every kind also once (a crash kind as
# one kill of the first runner; a pull failure, a missing key or the
# readiness switch on the first pod, taken away once the rollback has
# fired), and on a grouped prefill/decode service the decoder's pushed
# revision breaks instead of the engine's.
ROLLBACK_FAULT_SHAPES = [("engine", 4, 0, False), ("engine-mn", 2, 1, False), ("engine", 4, 0, True),
                         ("engine-mn", 2, 1, True), ("pd", 4, 0, True), ("pd-mn", 2, 1, True)]
ROLLBACK_FAULT_ROWS = [
    ("crashloop:old", "start", ""), ("crashloop:old", "start", "after-settle"),
    ("crashloop:old", "load", ""), ("crashloop:old", "load", "after-settle"),
    ("imagepull:old", "", ""), ("imagepull:old", "", "after-settle"),
    ("configkey:missing", "", "never"), ("configkey:missing", "", "after-settle"),
    ("readiness:old", "persist", ""), ("readiness:old", "recover", ""),
]
ROLLBACK_FAULT_MOMENTS = [("settling", "SurgeThenDrain", 1, 0, "none"), ("rotating", "RecreatePod", 0, 1, "none"),
                          ("draining", "SurgeThenDrain", 1, 0, "rolling"), ("last", "InPlaceIfPossible", 0, 1, "rolling")]
ROLLBACK_SIBLING_ROWS = [("crashloop:old", "start", ""), ("crashloop:old", "start", "after-settle")]
ROLLBACK_SIBLING_MOMENT = ROLLBACK_FAULT_MOMENTS[0][:4]
ROLLBACK_HELD_BREAKS = ["crash-start", "crash-load", "imagepull", "configkey"]
ROLLBACK_HELD_STRATS = [("SurgeThenDrain", 1, 0), ("RecreatePod", 0, 1), ("InPlaceIfPossible", 0, 1)]
ROLLBACK_HELD_ONCE = ["crash-start", "crash-load"]
# The one-shot forms beyond the crash kinds, minted after the family's
# other rows so every earlier id keeps its case.
ROLLBACK_HELD_ONCE_MORE = ["imagepull", "configkey", "stuck"]
ROLLBACK_HELD_SIBLING_SHAPES = [("pd", 4, 0, True), ("pd-mn", 2, 1, True)]
# The bumped members' Instance ready timeout under a pushed-revision break:
# a revision that never comes up parks its row Failed within the wait the
# runner gives the status to say why.
ROLLBACK_HELD_READY_TIMEOUT = READINESS_NEW_TIMEOUT
ROLLBACK_BREAK_WORDS = {"crash-start": "a runner that dies at every start", "crash-load": "a runner that exits after Ready",
                        "imagepull": "an image that cannot be pulled", "configkey": "a ConfigMap key the ConfigMap lacks",
                        "stuck": "a readiness probe it never passes"}
# How the one-shot form of each held-moment break beyond the crash kinds
# lands on the pushed revision's first pod, and how the cause goes away
# once the rollback has fired.
ROLLBACK_HELD_ONCE_WORDS = {
    "imagepull": ("is pointed at an image that cannot be pulled while its runner is still starting, before it was "
                  "ever Ready, and the kubelet reports the pull failure", "the image is put back"),
    "configkey": ("reads a ConfigMap key the ConfigMap lacks, so it waits on the key and the kubelet reports it "
                  "missing", "the key is put into the ConfigMap"),
    "stuck": ("has its readiness switch set while its runner is still starting, so it runs without ever turning "
              "Ready", "the switch is lifted"),
}


def _rollback_fault_kept(fault, form, lift):
    return (fault == "configkey:missing" and lift == "never") or (fault == "readiness:old" and form == "persist")


# A missing key parks the roll on its victim: the roll takes the broken
# Instance first and its replacement cannot start without the key, so no pod
# of the starting revision ever drains and neither the draining nor the last
# hook arrives. The key put back once the rollback has fired is away until
# then just as the kept key is, so those two moments of both rows fire at
# rotating instead, under their own strategy and group.
ROLLBACK_KEPT_KEY_HOOKS = {"draining": "rotating", "last": "rotating"}


def _rollback_fault_hook(fault, form, lift, hook):
    """The hook a running-revision break's rollback fires at: the moment's
    own, or the reachable one the missing key leaves it (ROLLBACK_KEPT_KEY_HOOKS)."""
    if fault == "configkey:missing":
        return ROLLBACK_KEPT_KEY_HOOKS.get(hook, hook)
    return hook


def _rollback_fault_oneshot(fault, form, lift):
    return lift == "after-settle" or (fault == "readiness:old" and form == "recover")


def _rollback_fault_comps(topo, n, w, router, life, co, cm):
    """The rollback family's fleet on a six-family shape: the engine on the
    annotated starting revision (its runner reading a key of the ConfigMap
    cm names, where the case has one), the decoder of a prefill/decode
    service (sharing the engine's strategy under a group), and the router
    where the shape carries one."""
    cw = w if is_mn(topo) else 0
    runner = lib.configmap_env(cm, HEAL_CONFIG_KEY, HEAL_CONFIG_VAR, gang=is_mn(topo)) if cm else None
    cs = [lib.comp("engine", n, cw, life, ann=ROLLBACK_START_ANN, runner=runner)]
    if is_pd(topo):
        cs.append(lib.comp("decoder", n, cw, life if co != "none" else lib.lifecycle(), ann=ROLLBACK_START_ANN))
    if router_count(topo, lib.NR if router else 0):
        cs.append(lib.comp("router", lib.NR, 0, lib.lifecycle()))
    return cs


def _rollback_fault_body(name, topo, n, w, router, life, co, configkey, coord):
    """The case body as a function of the minted name: where the break needs
    a ConfigMap key, the engine runner reads one of a ConfigMap named after
    the case, carried as a second document so the apply creates it and the
    teardown removes it with the case."""
    cm = f"{name}{HEAL_CONFIG_SUFFIX}" if configkey else None
    cs = _rollback_fault_comps(topo, n, w, router, life, co, cm)
    docs = [lib.configmap_doc(cm, {HEAL_CONFIG_KEY: HEAL_CONFIG_VALUE})] if cm else []
    return lib.render(name, cs, coord=coord, docs=docs)


def _rollback_fault_words(fault, form, lift, victim, topo):
    pod = "leader pod" if is_mn(topo) else "pod"
    where = f"the {pod} of the {victim}'s highest Instance"
    what = {("crashloop:old", "start"): f"{where} is pointed at an image whose runner dies at every start",
            ("crashloop:old", "load"): f"{where} has its runner killed every few seconds once it is up",
            ("imagepull:old", ""): f"{where} is pointed at an image that cannot be pulled",
            ("configkey:missing", ""): f"the ConfigMap key the {victim} runner reads is removed and {where} restarted, "
                                       f"so it waits in CreateContainerConfigError",
            ("readiness:old", form if fault == "readiness:old" else ""): f"{where} gets the readiness switch and stops passing readiness"}
    key = (fault, form) if fault != "readiness:old" else ("readiness:old", form)
    out = what[key] + " on the settled service, and the push follows once the status has reacted to it"
    if _rollback_fault_oneshot(fault, form, lift):
        out += "; the cause is taken away once the rollback has fired"
    elif _rollback_fault_kept(fault, form, lift):
        out += "; the cause stays for the whole case"
    return out


def _rollback_fault_reason(fault, form, lift, hook, strat, co, topo, router, victim):
    kept = _rollback_fault_kept(fault, form, lift)
    out = (f"the operator pushes a revision over a fleet whose {victim} is already broken: "
           f"{_rollback_fault_words(fault, form, lift, victim, topo)}; the starting revision is re-applied at "
           f"{ROLLBACK_MOMENT[hook]}; every Instance of the bumped members ends on the starting revision, no pod of the "
           f"new revision remains, the capacity floor (lowered by the broken Instance) holds throughout and no Instance "
           f"is left mid-operation")
    if kept:
        out += (f"; the broken {victim} Instance cannot serve while its cause stays, so it ends on the starting revision "
                f"without serving, its Component one Instance short, and its row reads other than Ready")
    else:
        out += (f"; the broken {victim} Instance ends Ready and serving on the starting revision, rebuilt by a repair or "
                f"by the roll back or healed in place (the repairs that named it are reported, not judged)")
    if victim != "engine":
        out += f"; the {victim} is not bumped, so only a repair or the heal brings its Instance back"
    if strat == "SurgeThenDrain":
        out += "; Instances already on the new revision come back under the surge budget"
    elif strat == "RecreatePod":
        out += "; Instances already on the new revision come back drain-first within the unavailable budget"
    elif is_mn(topo):
        out += "; a gang in-place roll comes back as a whole-gang recreate within the unavailable budget"
    else:
        out += "; an in-place roll comes back in place (UIDs stable)"
    if co != "none":
        out += "; the run that targeted the new revision closes as superseded and a fresh run brings the group back"
    return out


def _rollback_held_reason(kind, once, strat, co, topo, router, victim):
    pod = "leader pod" if is_mn(topo) else "pod"
    if once and kind in ROLLBACK_HELD_ONCE_WORDS:
        broken, lifted = ROLLBACK_HELD_ONCE_WORDS[kind]
        how = (f"the push's first {victim} {pod} {broken}; the starting revision is re-applied as soon as the break "
               f"shows and {lifted} once the rollback has fired")
    elif once:
        how = (f"the push's first {victim} {pod} has its runner killed once, "
               f"{'during its first start, before it was ever Ready' if kind == 'crash-start' else 'once it is Ready and serving'}, "
               f"and the kubelet restarts it; the starting revision is re-applied as soon as the restart shows")
    else:
        how = (f"the push gives the {victim} runner {ROLLBACK_BREAK_WORDS[kind]}, so the roll parks on its first {pod}: the "
               f"kubelet shows the break, the status says why (a rolloutHold gate from the retry ladder or the budget, a "
               f"Failed row, a RetryHeld or InstanceFailed event), and only then is the starting revision re-applied")
    out = (f"the operator rolls back a push its first pod cannot carry: {how}; every Instance of the bumped members ends "
           f"on the starting revision and serving, no pod of the withdrawn revision remains, none is created after the "
           f"rollback fired and no repair names a bumped Instance after it; the capacity floor holds throughout")
    if kind in ("crash-start", "imagepull", "configkey") and not once:
        out += "; nothing of the broken revision ever served"
    if strat == "InPlaceIfPossible":
        out += ("; the runner change is patched in place where the strategy allows and recreated otherwise, and the "
                "rollback comes back the same way within the unavailable budget")
    elif strat == "RecreatePod":
        out += "; the first Instance drained is rebuilt on the broken revision and comes back drain-first within the unavailable budget"
    else:
        out += "; the broken surge Instance never joins rotation and is withdrawn under the surge budget"
    if victim != "engine":
        out += f"; the engine's own roll converges or holds meanwhile and comes back with the {victim}"
    if co != "none":
        out += "; the run that targeted the new revision closes as superseded and a fresh run brings the group back"
    return out


def _emit_rollback_faults(add):
    for topo, n, w, router in ROLLBACK_FAULT_SHAPES:
        shape = _lift_shape(topo, w, router)
        for fault, form, lift in ROLLBACK_FAULT_ROWS:
            for hook, strat, surge, unavail, co in ROLLBACK_FAULT_MOMENTS:
                hook = _rollback_fault_hook(fault, form, lift, hook)
                kv = dict(**_crash_once_counts(topo, n, router), **_crash_once_ceilings(topo, n, router, strat, surge),
                          fault=fault, co=co, topo=topo, rollback=1, during=hook)
                if form:
                    kv["form"] = form
                if lift:
                    kv["lift"] = lift
                if co != "none" and is_pd(topo):
                    kv["ceil_d"] = kv["ceil_e"]
                words = fault.partition(":")[0] + (f" {form}" if form else "") + (f" {lift}" if lift else "")
                meta = dict(strat=strat, verb=f"rollback:{hook}:{fault}", hook=hook, fault=fault, form=form, lift=lift)
                desc = (f"{GEN_MARK} rollback at {hook} over {words} {shape} N={n} {strat} "
                        f"{budget_tag(strat, surge, unavail)} co {co}")
                configkey = fault == "configkey:missing"
                life = lib.lifecycle(strat=strat, surge=surge, unavail=unavail,
                                     ready_timeout=(HEAL_FAILED_READY_TIMEOUT if configkey else None))
                coord = lib.coord_for(co, comps=("engine", "decoder") if is_pd(topo) else ("engine",)) if co != "none" else None
                add("gen-rollback", desc, kv, None, _rollback_fault_reason(fault, form, lift, hook, strat, co, topo, router, "engine"),
                    meta, cell=("rollback-fault", fault, form, lift, hook, strat, co, topo, w, router),
                    body=functools.partial(_rollback_fault_body, topo=topo, n=n, w=w, router=router, life=life, co=co,
                                           configkey=configkey, coord=coord))
    hook, strat, surge, unavail = ROLLBACK_SIBLING_MOMENT
    for topo, n, w, router in ROLLBACK_FAULT_SHAPES:
        victim = "decoder" if is_pd(topo) else ("router" if router else "")
        if not victim:
            continue
        shape = _lift_shape(topo, w, router)
        for fault, form, lift in ROLLBACK_SIBLING_ROWS:
            kv = dict(**_crash_once_counts(topo, n, router), **_crash_once_ceilings(topo, n, router, strat, surge),
                      fault=fault, form=form, co="none", topo=topo, rollback=1, during=hook, victimcomp=victim)
            if lift:
                kv["lift"] = lift
            words = f"crashloop {form}" + (f" {lift}" if lift else "")
            meta = dict(strat=strat, verb=f"rollback:{hook}:{fault}:{victim}", hook=hook, fault=fault, form=form, lift=lift)
            desc = f"{GEN_MARK} rollback at {hook} over {words} on the {victim} {shape} N={n} {strat} {budget_tag(strat, surge, unavail)} co none"
            life = lib.lifecycle(strat=strat, surge=surge, unavail=unavail)
            add("gen-rollback", desc, kv, None, _rollback_fault_reason(fault, form, lift, hook, strat, "none", topo, router, victim),
                meta, cell=("rollback-fault", fault, form, lift, hook, topo, w, victim),
                body=functools.partial(_rollback_fault_body, topo=topo, n=n, w=w, router=router, life=life, co="none",
                                       configkey=False, coord=None))


def _emit_rollback_held_breaks(add):
    def one(topo, n, w, router, kind, once, strat, surge, unavail, co, victim):
        shape = _lift_shape(topo, w, router)
        kv = dict(**_crash_once_counts(topo, n, router), **_crash_once_ceilings(topo, n, router, strat, surge),
                  co=co, topo=topo, rollback=1, during="held")
        kv["break"] = kind
        if once:
            kv["once"] = 1
        if victim != "engine":
            kv["victimcomp"] = victim
        if co != "none" and is_pd(topo):
            kv["ceil_d"] = kv["ceil_e"]
        words = kind + (" once" if once else "") + (f" on the {victim}" if victim != "engine" else "")
        meta = dict(strat=strat, verb=f"rollback:held:{kind}" + (":once" if once else ""), hook="held", held_break=kind, once=once)
        desc = f"{GEN_MARK} rollback at held by {words} {shape} N={n} {strat} {budget_tag(strat, surge, unavail)} co {co}"
        life = lib.lifecycle(strat=strat, surge=surge, unavail=unavail, ready_timeout=(None if once else ROLLBACK_HELD_READY_TIMEOUT))
        coord = lib.coord_for(co, comps=("engine", "decoder") if is_pd(topo) else ("engine",)) if co != "none" else None
        add("gen-rollback", desc, kv, None, _rollback_held_reason(kind, once, strat, co, topo, router, victim), meta,
            cell=("rollback-held", kind, once, strat, co, topo, w, router, victim),
            body=functools.partial(_rollback_fault_body, topo=topo, n=n, w=w, router=router, life=life, co=co,
                                   configkey=(kind == "configkey"), coord=coord))

    for topo, n, w, router in ROLLBACK_FAULT_SHAPES:
        for kind in ROLLBACK_HELD_BREAKS:
            for strat, surge, unavail in ROLLBACK_HELD_STRATS:
                one(topo, n, w, router, kind, False, strat, surge, unavail, "none", "engine")
        for kind in ROLLBACK_HELD_ONCE:
            strat, surge, unavail = ROLLBACK_HELD_STRATS[0]
            one(topo, n, w, router, kind, True, strat, surge, unavail, "none", "engine")
    for topo, n, w, router in ROLLBACK_HELD_SIBLING_SHAPES:
        strat, surge, unavail = ROLLBACK_HELD_STRATS[0]
        one(topo, n, w, router, "crash-start", False, strat, surge, unavail, "rolling", "decoder")
        one(topo, n, w, router, "crash-start", True, strat, surge, unavail, "rolling", "decoder")
    for topo, n, w, router in ROLLBACK_FAULT_SHAPES:
        for kind in ROLLBACK_HELD_ONCE_MORE:
            strat, surge, unavail = ROLLBACK_HELD_STRATS[0]
            one(topo, n, w, router, kind, True, strat, surge, unavail, "none", "engine")


def emit_rollback(add):
    for topo, n, w in ROLLBACK_SHAPES:
        for strat in ROLLBACK_BUDGETS:
            for surge, unavail in _rollback_budgets(topo, strat):
                for hook in ROLLBACK_HOOKS:
                    if not _rollback_reachable(topo, strat, hook):
                        continue
                    for co in ROLLBACK_COORDS:
                        kv = dict(**counts(topo, n), **ceilings(topo, n, strat, surge), co=co, topo=topo,
                                  rollback=1, during=hook)
                        if co != "none" and is_pd(topo):
                            # The grouped decoder rolls under the engine's strategy and budget.
                            kv["ceil_d"] = kv["ceil_e"]
                        if hook == "held":
                            kv["break"] = "stuck"
                        meta = dict(strat=strat, verb=f"rollback:{hook}", hook=hook)
                        desc = (f"{GEN_MARK} rollback at {hook} {shape_tag(topo, w)} N={n} {strat} "
                                f"{budget_tag(strat, surge, unavail)} co {co}")
                        life = lib.lifecycle(strat=strat, surge=surge, unavail=unavail)
                        coord = None
                        if co != "none":
                            coord = lib.coord_for(co, comps=("engine", "decoder") if is_pd(topo) else ("engine",))
                        add("gen-rollback", desc, kv, _rollback_comps(topo, n, w, life, co),
                            _rollback_reason(hook, strat, topo, co), meta,
                            cell=("rollback", hook, topo, w, strat, surge, unavail, co), coord=coord)
    _emit_rollback_faults(add)
    _emit_rollback_held_breaks(add)


# ── gen-crashloop-new: the push brings a revision whose pods crash ──────────
# Under form=load the roll admits the next start while the promoted crashing
# Instance still looks healthy (minReadySeconds is zero): a drain under a
# drain-first strategy, a surge under the surge one; so the case allows one
# Instance beyond the budget under every strategy, as a Deployment would
# reach too. The surge strategy may roll a small fleet whole inside the
# crash window instead: each further Instance takes one startup to come up
# and the first crash shows CRASH_EXIT_AFTER_S after the first promotion,
# so a fleet of two is replaced before any restart is visible and the case
# allows the whole fleet. The timings are the fixture runtime's startup
# delay (TEST_STARTUP_DELAY in runtime/csr*.yaml) and run.py's crash exit
# default.
RUNNER_STARTUP_S, CRASH_EXIT_AFTER_S = 15, 20


def _surge_roll_fits_crash_window(n):
    return n > 1 and (n - 1) * RUNNER_STARTUP_S < CRASH_EXIT_AFTER_S


def _crash_new_allowance(form, strat, budget, n):
    """How many Instances may be off the running revision at once under a
    push whose revision crashes: the budget; one more when the promoted
    pods are Ready then die, whatever the strategy (the start admitted
    while the first promoted one still looked healthy); the whole fleet
    where the surge strategy rolls it inside the crash window."""
    if form != "load":
        return budget
    if strat == "SurgeThenDrain" and _surge_roll_fits_crash_window(n):
        return max(budget + 1, n)
    return budget + 1


def _crash_new_reason(form, strat, co, budget, n):
    allowance = _crash_new_allowance(form, strat, budget, n)
    how = ("the pushed engine revision dies at every start (its runner image has no shell for the fixture command)"
           if form == "start" else
           "the pushed engine revision passes readiness, is promoted, and exits a few seconds later, "
           "every time the kubelet restarts it (Ready counted for the budget: the new pod was promoted, then crashed)")
    admitted = ("; promoted, it admits the next Instance's start before it crashes (minReadySeconds is zero), so "
                "two Instances are off at once at the floor, as a Deployment would have it, and the start after "
                "that waits for a promotion that holds")
    whole = (f"the promoted Instance is still healthy when the surge replacement of the {n - 1} Instance(s) after it "
             f"is promoted, about {RUNNER_STARTUP_S}s later and inside its {CRASH_EXIT_AFTER_S}s crash window "
             f"(minReadySeconds is zero), so the whole fleet of {n} is off the running revision before any restart "
             f"is visible to withhold the last drain on")
    lands = {"SurgeThenDrain": ("the surge replacement never joins rotation, so no source is drained"
                                if form == "start" else whole if _surge_roll_fits_crash_window(n) else
                                "the surge replacement is promoted and its source drained" + admitted),
             "RecreatePod": ("the first Instance drained is rebuilt on the crashing revision and stays the only one off"
                             if form == "start" else
                             "the first Instance drained is rebuilt on the crashing revision" + admitted),
             "InPlaceIfPossible": ("the image patches the first Instance's pod in place and it crash-loops there"
                                   if form == "start" else
                                   "the exit switch is a pod-template env diff, so in-place resolves to a recreate "
                                   "of the first Instance" + admitted)}[strat]
    plural = "Instances are" if allowance != 1 else "Instance is"
    floor = (f"which keeps serving at N-{allowance}" if allowance < n
             else "which then has no Instance left serving until the fix lands")
    out = (f"{how}; {lands}: at most {allowance} {plural} off the running revision, {floor}; "
           f"status says why (a rolloutHold from the retry ladder or the budget, or a Failed row, and a "
           f"RetryHeld or InstanceFailed event) and every further attempt at that revision is a retry the ladder "
           f"announced, none once it has held; the fix push then lands on every Instance, serving, "
           f"and nothing rebuilds the crashing revision")
    if co != "none":
        out += "; the decoder rolls to its own sound revision meanwhile (converges, or holds with a reason)"
    return out


# The pushed sibling revision that crash-loops: the decoder bumped beside
# the engine in a rollingUpdate group, the router by name beside the engine.
# (topo, N, W, routers on an engine shape, the sibling.)
CRASH_NEW_SIBLINGS = [("engine", 4, 0, lib.NR, "router"), ("engine-mn", 2, 1, lib.NR, "router"),
                      ("pd", 4, 0, 0, "decoder"), ("pd-mn", 2, 1, 0, "decoder")]


def _crash_new_sibling_reason(sibling, form, n_sibling):
    allowance = _crash_new_allowance(form, "SurgeThenDrain", 1, n_sibling)
    how = (f"the pushed {sibling} revision dies at every start (its runner image has no shell for the fixture command)"
           if form == "start" else
           f"the pushed {sibling} revision passes readiness, is promoted, and exits a few seconds later, every time "
           f"the kubelet restarts it (Ready counted for the budget: the new pod was promoted, then crashed)")
    plural = "Instances are" if allowance != 1 else "Instance is"
    floor = (f"which keeps serving at N-{allowance}" if allowance < n_sibling
             else "which then has no Instance left serving until the fix lands")
    engine = ("the engine, a sound member of the group, converges on its own new revision meanwhile or holds with "
              "the group's reason" if sibling == "decoder" else
              "the engine rolls to its own new revision meanwhile, since nothing pairs it with the router")
    return (f"{_crash_once_sibling_bumped_words(sibling)}; {how}; at most {allowance} {sibling} {plural} off the running "
            f"revision, {floor}; status says why (a rolloutHold on the {sibling} from the retry ladder or the budget, "
            f"or a Failed {sibling} row, and a RetryHeld or InstanceFailed event naming it) and every further attempt "
            f"at that revision is a retry the ladder announced, none once it has held; {engine}; the fix push then "
            f"lands on every bumped Instance, serving, and nothing rebuilds the crashing revision")


def emit_crashloop_new(add):
    for topo, n, w in CRASH_SHAPES:
        for strat, surge, unavail in NEW_CRASH_STRATS:
            for rp, form in ((rp, form) for rp in RESTARTS for form in CRASH_FORMS):
                for co in CRASH_COORDS[topo]:
                    kv = dict(**counts(topo, n), **ceilings(topo, n, strat, surge),
                              fault="crashloop:new", form=form, co=co, topo=topo)
                    if co != "none":
                        # The group rolls the decoder too, on its own surge.
                        kv["ceil_d"] = n + 1
                    meta = dict(strat=strat, verb=f"crashloop:new:{form}", rp=rp)
                    desc = (f"{GEN_MARK} crashloop:new {form} {shape_tag(topo, w)} N={n} {strat} "
                            f"{budget_tag(strat, surge, unavail)} rp {rp} co {co}")
                    life = lib.lifecycle(strat=strat, surge=surge, unavail=unavail, restart=RP_FULL[rp])
                    d_life = lib.lifecycle(restart=RP_FULL[rp]) if is_pd(topo) else None
                    add("gen-crashloop-new", desc, kv, comps(topo, n, w, life, d_life=d_life),
                        _crash_new_reason(form, strat, co, max(surge, unavail), n), meta,
                        cell=("crashloop-new", form, topo, w, n, strat, surge, unavail, rp, co),
                        coord=lib.coord_for(co) if co != "none" else None)
    strat, surge, unavail = NEW_CRASH_STRATS[0]
    for topo, n, w, nr, sibling in CRASH_NEW_SIBLINGS:
        for rp in RESTARTS:
            for form in CRASH_FORMS:
                kv = dict(**counts(topo, n, nr), **ceilings(topo, n, strat, surge, nr),
                          fault="crashloop:new", form=form, topo=topo, victimcomp=sibling)
                if sibling == "decoder":
                    kv.update(co="rolling", bump="engine,decoder", ceil_d=n + surge)
                    coord = lib.coord_for("rolling")
                else:
                    kv.update(co="none", bump="engine,router", ceil_r=nr + surge)
                    coord = None
                meta = dict(strat=strat, verb=f"crashloop:new:{form}", rp=rp, victim=sibling)
                desc = (f"{GEN_MARK} crashloop:new {form} {sibling} {shape_tag(topo, w, nr)} N={n} {strat} "
                        f"{budget_tag(strat, surge, unavail)} rp {rp}")
                life = lib.lifecycle(strat=strat, surge=surge, unavail=unavail, restart=RP_FULL[rp])
                add("gen-crashloop-new", desc, kv,
                    comps(topo, n, w, life, d_life=(life if is_pd(topo) else None), r_life=(life if sibling == "router" else None), nr=nr),
                    _crash_new_sibling_reason(sibling, form, lib.NR if sibling == "router" else n), meta,
                    cell=("crashloop-new", form, topo, w, n, strat, surge, unavail, rp, sibling), coord=coord)


# ── gen-crashloop-group: a running-revision crash under the group policies ──
def _group_rule(co, strat, tol):
    if co == "bluegreen":
        return ("blueGreen flips traffic only once every member's new set is complete, and the broken Instance "
                "is rebuilt as part of that set within the per-member budget; the flip cannot replace it ahead of "
                "the set, so the case asserts the per-budget roll and that the broken Instance is never the last "
                "one rebuilt, and reads the traffic flip as status only")
    if co == "seq":
        return ("Sequential rolls the engine to completion before the decoder starts (the ordering check), and "
                "inside the engine the broken Instance lands first or at least never last")
    if co == "ratio":
        return (f"maintainRatio keeps engine:decoder serving within {tol}% (the ratio check, read net of the broken "
                f"Instance as well as raw: its lost capacity is the fault's, not the gate's); the broken Instance "
                f"is already out of the serving count, so it comes back first or at least never last")
    zero = ("maxUnavailable 0 with maxSurge 1: every drain follows a Ready surge replacement"
            if strat == "SurgeThenDrain" else
            "maxSurge 0 with maxUnavailable 1: one Instance down at a time")
    return f"a rolling group under the zero budget ({zero}), and the broken Instance, already down, is rebuilt first or at least never last"


def _group_reason(form, co, strat, tol, three_gangs=False):
    how = ("one old-revision engine pod is pointed at an image with no shell as the bump lands, so it crashes at "
           "every start and is never Ready again"
           if form == "start" else
           "one old-revision engine pod has its runner killed every few seconds while it is up, so it dies after "
           "Ready and comes back between crashes")
    gangs = ("; three gangs, because the drain-first roll has taken its first gang before the break lands and the "
             "dark gang then waits for that one slot: with two gangs the only peer precedes the broken gang by "
             "construction, with three the dark gang must be rebuilt ahead of the peer still waiting"
             if three_gangs else "")
    return (f"{how}; the bump is the fix and lands on every Instance within the group's own rule: {_group_rule(co, strat, tol)}"
            f"{gangs}; "
            f"the healthy peers never leave rotation beyond the budget, no repair is recorded for the broken Instance, "
            f"and it ends on the new revision serving; floor waived — the crash itself costs settled capacity "
            f"for the length of the roll")


def _group_for(co, n):
    members = ["engine", "decoder"]
    if co == "ratio":
        return lib.coord_group(members, "RollingUpdate",
                               {"type": "RatioBalanced", "ratioTolerancePercent": RATIO_TOL_BY_N[n]})
    return lib.coord_for(co, comps=members)


def _group_n(topo, n, strat):
    """The fleet size a group cell runs: the shape's own, except the gang
    shape under a drain-first strategy, which needs three gangs for the
    order claim to be readable (see GROUP_CRASH_MN_DRAIN_N)."""
    return GROUP_CRASH_MN_DRAIN_N if is_mn(topo) and strat != "SurgeThenDrain" else n


def _emit_group_cell(add, topo, n, w, strat, surge, unavail, rp, form, co):
    members = ["engine", "decoder"] if is_pd(topo) else ["engine"]
    three_gangs = is_mn(topo) and _group_n(topo, n, strat) != n
    n = _group_n(topo, n, strat)
    kv = dict(**counts(topo, n), **ceilings(topo, n, strat, surge),
              fault="crashloop:old", form=form, floor_soft=1, fixfirst=1, co=co, topo=topo)
    if is_pd(topo):
        # The group rolls the decoder under the same strategy.
        kv["ceil_d"] = n + surge_count(n, surge)
    tol = RATIO_TOL_BY_N[n] if co == "ratio" else None
    meta = dict(strat=strat, verb=f"crashloop:{form}", rp=rp)
    desc = (f"{GEN_MARK} crashloop {form} group {co} {shape_tag(topo, w)} N={n} {strat} "
            f"{budget_tag(strat, surge, unavail)} rp {rp}")
    life = lib.lifecycle(strat=strat, surge=surge, unavail=unavail, restart=RP_FULL[rp])
    coord = _group_for(co, n) if is_pd(topo) else lib.coord_for(co, comps=tuple(members))
    add("gen-crashloop-group", desc, kv, comps(topo, n, w, life, d_life=life if is_pd(topo) else None),
        _group_reason(form, co, strat, tol, three_gangs=three_gangs), meta,
        cell=("crashloop-group", form, topo, w, n, strat, surge, unavail, rp, co), coord=coord)


def emit_crashloop_group(add):
    for topo, n, w in GROUP_CRASH_SHAPES:
        for co in GROUP_CRASH_POLICIES[topo]:
            for strat, surge, unavail in GROUP_CRASH_STRATS:
                for rp, form in ((rp, form) for rp in RESTARTS for form in CRASH_FORMS):
                    _emit_group_cell(add, topo, n, w, strat, surge, unavail, rp, form, co)
    for topo, n, w in BUDGET_ZERO_SHAPES:
        for strat, surge, unavail in GROUP_CRASH_STRATS:
            for rp, form in ((rp, form) for rp in RESTARTS for form in CRASH_FORMS):
                _emit_group_cell(add, topo, n, w, strat, surge, unavail, rp, form, "rolling")


# ── gen-size: one and two Instances per Component ────────────────────────────
# A budget of one is the whole fleet, a percent rounds up to one, and a
# single Instance is both the first and the last the roll reaches. The
# engine resolves a percent budget as ceil(replicas * percent / 100), clamped
# to replicas, so at these sizes 25 and 50 percent both resolve to one and no
# budget rounds to zero: the percent budgets are driven on the plain push,
# where the resolved budget is read off the peak and the floor, and the crash
# loop, the scale and the canary run under the integer budget they resolve
# to. The router is sized with the fleet, so a stray router rotation shows.
SIZE_NS = [1, 2]
SIZE_TOPOS = ["engine", "pd"]
SIZE_BUDGETS = {
    "SurgeThenDrain": [(1, 0), ("25%", 0)],
    "RecreatePod": [(0, 1), (0, "50%")],
    "InPlaceIfPossible": [(0, 1)],
}
SIZE_SCALES = [(1, 2), (2, 1)]
# The ladder on two Instances: the first two steps resolve to the same single
# Instance, so the second moves traffic and adds no capacity.
SIZE_LADDER = [("10%", 10), ("50%", 50), ("100%", 100)]
SIZE_CANARY_N = 2
SIZE_CANARIES = [("SurgeThenDrain", "auto"), ("SurgeThenDrain", "manual"), ("RecreatePod", "auto")]
SIZE_CANARY_CELL = "canary/Idle/newTarget[twoInstances]"


def resolved_budget(value, n):
    """The Instance count the engine resolves a budget to against n: an
    integer as it is, a percent rounded up and clamped to the fleet."""
    if isinstance(value, str) and value.endswith("%"):
        return min(n, surge_count(n, value))
    return value


def instances(n):
    """A count of Instances in words: one Instance, 2 Instances."""
    return "one Instance" if n == 1 else f"{n} Instances"


def budget_phrase(name, value, n):
    """A budget as the case's reason states it, with its resolution at n."""
    if isinstance(value, str) and value.endswith("%"):
        return f"{name} {value} ({value[:-1]} percent of {n} rounds up to {resolved_budget(value, n)})"
    return f"{name} {value}"


def size_counts(topo, n):
    """Every Component of the shape at n Instances, the router included."""
    return dict(Ne=n, Nd=n, Nr=n) if is_pd(topo) else dict(Ne=n, Nd=0, Nr=0)


def size_ceilings(topo, n, strat, surge):
    """Ceilings in Instances: n plus the resolved surge under SurgeThenDrain,
    n otherwise; the decoder and the router hold at n."""
    ce = n + resolved_budget(surge, n) if strat == "SurgeThenDrain" else n
    return dict(ceil_e=ce, ceil_d=(n if is_pd(topo) else 0), ceil_r=(n if is_pd(topo) else 0))


def size_comps(topo, n, e_life, d_life=None, r_life=None):
    cs = [lib.comp("engine", n, 0, e_life)]
    if is_pd(topo):
        cs.append(lib.comp("decoder", n, 0, d_life if d_life is not None else lib.lifecycle()))
        cs.append(lib.comp("router", n, 0, r_life if r_life is not None else lib.lifecycle()))
    return cs


def _size_push_reason(topo, n, strat, surge, unavail):
    one = n == 1
    if strat == "SurgeThenDrain":
        s = resolved_budget(surge, n)
        out = (f"a plain push on {instances(n)} per Component with {budget_phrase('maxSurge', surge, n)}: the new "
               f"revision comes up beside the running {'one' if one else 'ones'} (peak {n + s}) and "
               f"{'the single Instance' if one else 'each Instance'} is drained only once its replacement serves, so "
               f"capacity never drops below {n} and "
               f"{'the service is never left with zero serving' if one else 'at least one Instance always serves'}; "
               f"every Instance ends on the new revision")
    elif strat == "RecreatePod":
        u = resolved_budget(unavail, n)
        if one:
            out = (f"a plain push on one Instance with {budget_phrase('maxUnavailable', unavail, n)}: the single Instance "
                   f"is drained and rebuilt at the new revision, so zero serving while it is down is the configured "
                   f"outcome of this budget at this size; it comes back Ready and serving on the new revision with no "
                   f"surge above 1")
        else:
            out = (f"a plain push on {n} Instances with {budget_phrase('maxUnavailable', unavail, n)}: one Instance is "
                   f"drained and rebuilt while the other keeps serving (never below {n - u} ready), then the other "
                   f"follows; every Instance ends on the new revision with no surge above {n}")
    else:
        out = (f"a plain push on {instances(n)} with maxUnavailable {unavail}: the bump is an annotation diff, so the "
               f"engine patches {'the pod' if one else 'each pod'} in place (same UID, no surge above {n}); the budget "
               f"lets {'the single Instance' if one else 'one Instance at a time'} be out of rotation while it is "
               f"patched, and {'it' if one else 'every Instance'} ends Ready on the new revision")
    if is_pd(topo):
        out += "; decoder and router are negative controls (co=none), sized with the fleet"
    return out


def _size_crash_reason(topo, n, strat, form, rp):
    where = ("its Instance is the only one, so it is both the first and the last the roll reaches"
             if n == 1 else "its Instance is the highest index, the last the roll reaches")
    out = (f"{_crash_how(form)}; {where}, and the roll owns it from that moment: {CRASH_LANDS[strat]}, no repair is "
           f"recorded for it after the bump under restart policy {RP_FULL[rp]}, and "
           f"{'it' if n == 1 else 'every Instance'} ends on the new revision serving")
    if n == 1 and strat == "SurgeThenDrain":
        out += "; the service serves again as soon as the fix is Ready, before the broken Instance is drained"
    elif n == 1:
        out += ("; the single Instance serves nothing from the break until its rebuild is Ready, the configured "
                "outcome at this size")
    if is_pd(topo):
        out += "; decoder and router are negative controls (co=none)"
    out += "; floor waived — the crash itself costs settled capacity for the length of the roll"
    return out


def _size_scale_reason(topo, start, target, strat):
    out = (f"the fleet is scaled from {start} to {target} Instances per Component: only index "
           f"{max(start, target) - 1} is {'added' if target > start else 'removed'}; index 0 keeps its pod (same UID) "
           f"and serves throughout, so capacity never drops below min(start, target) = {min(start, target)} and "
           f"nothing surges above {max(start, target)}; the update strategy ({strat}) is dormant under a scale and "
           f"rebuilds nothing")
    if is_pd(topo):
        out += "; engine and decoder scale together and the router holds at its count"
    return out


def _size_canary_reason(topo, strat, policy):
    n = SIZE_CANARY_N
    members = "engine, decoder and router" if is_pd(topo) else "the engine"
    out = (f"a canary ladder on {n} Instances per member ({members}): the 10 percent step asks ceil(0.2) = 1 new "
           f"Instance; the 50 percent step asks ceil(1.0) = 1, the same Instance, so it moves traffic and adds no "
           f"capacity; the stable revision keeps one Instance until the cutover's traffic write lands, then the last "
           f"Instance rolls; traffic steps 10/50/100 on the canary revision")
    out += (f"; capacity steps surge one Instance beside the stable one (peak {n + 1}) and the floor stays at {n}"
            if strat == "SurgeThenDrain" else
            f"; each stepped Instance is rebuilt in place (no surge above {n}; one Instance always serving)")
    out += ("; the operator promotes at each of the two gates" if policy == "manual" else "; each gate is a timed soak")
    out += "; every member ends on the canary revision serving with nothing left on the stable one"
    return out


def emit_size(add):
    for n in SIZE_NS:
        for topo in SIZE_TOPOS:
            for strat, budgets in SIZE_BUDGETS.items():
                for surge, unavail in budgets:
                    kv = dict(**size_counts(topo, n), **size_ceilings(topo, n, strat, surge), co="none", topo=topo)
                    if strat == "SurgeThenDrain":
                        kv["floor_hard"] = 1   # never below N, instantaneously: the promise at this size
                    meta = dict(strat=strat, verb="none", size=n)
                    desc = f"{GEN_MARK} size N={n} push {topo} {strat} {budget_tag(strat, surge, unavail)}"
                    life = lib.lifecycle(strat=strat, surge=surge, unavail=unavail)
                    add("gen-size", desc, kv, size_comps(topo, n, life),
                        _size_push_reason(topo, n, strat, surge, unavail), meta,
                        cell=("size-push", topo, n, strat, surge, unavail))
            for strat, budgets in SIZE_BUDGETS.items():
                surge, unavail = budgets[0]
                for form in CRASH_FORMS:
                    for rp in RESTARTS:
                        kv = dict(**size_counts(topo, n), **size_ceilings(topo, n, strat, surge),
                                  fault="crashloop:old", form=form, floor_soft=1, co="none", topo=topo)
                        meta = dict(strat=strat, verb=f"crashloop:{form}", rp=rp, victim="engine", size=n)
                        desc = (f"{GEN_MARK} size N={n} crashloop {form} {topo} {strat} "
                                f"{budget_tag(strat, surge, unavail)} rp {rp}")
                        life = lib.lifecycle(strat=strat, surge=surge, unavail=unavail, restart=RP_FULL[rp])
                        d_life = lib.lifecycle(restart=RP_FULL[rp]) if is_pd(topo) else None
                        add("gen-size", desc, kv, size_comps(topo, n, life, d_life=d_life),
                            _size_crash_reason(topo, n, strat, form, rp), meta,
                            cell=("size-crash", topo, n, strat, form, rp))
    for start, target in SIZE_SCALES:
        for topo in SIZE_TOPOS:
            for strat, budgets in SIZE_BUDGETS.items():
                surge, unavail = budgets[0]
                peak = max(start, target)
                kv = dict(mode="scaledown", **size_counts(topo, start), ceil_e=peak,
                          ceil_d=(peak if is_pd(topo) else 0), ceil_r=(start if is_pd(topo) else 0),
                          co="none", topo=topo, to_e=target)
                if is_pd(topo):
                    kv.update(to_d=target, to_r=start)
                meta = dict(strat=strat, verb=f"scale:{'up' if target > start else 'down'}", size=start)
                desc = f"{GEN_MARK} size scale {topo} {start}->{target} {strat}"
                life = lib.lifecycle(strat=strat, surge=surge, unavail=unavail)
                add("gen-size", desc, kv, size_comps(topo, start, life),
                    _size_scale_reason(topo, start, target, strat), meta,
                    cell=("size-scale", topo, start, target, strat))
    for topo in SIZE_TOPOS:
        for strat, policy in SIZE_CANARIES:
            n = SIZE_CANARY_N
            surge, unavail = SIZE_BUDGETS[strat][0]
            life = lib.lifecycle(strat=strat, surge=surge, unavail=unavail)
            case_comps = size_comps(topo, n, life, d_life=life, r_life=life)
            kv = dict(mode="canary", **size_counts(topo, n), **size_ceilings(topo, n, strat, surge),
                      canary=policy, cell=SIZE_CANARY_CELL, co="none", topo=topo)
            if is_pd(topo) and strat == "SurgeThenDrain":
                kv.update(ceil_d=n + 1, ceil_r=n + 1)   # every group member surges one
            meta = dict(strat=strat, verb=f"canary:{policy}", size=n)
            desc = f"{GEN_MARK} size N={n} canary {policy} {topo} {strat} {budget_tag(strat, surge, unavail)}"
            add("gen-size", desc, kv, case_comps, _size_canary_reason(topo, strat, policy), meta,
                cell=("size-canary", topo, strat, policy),
                coord=lib.canary_block(lib.canary_steps(policy, *SIZE_LADDER),
                                       components=[c["comp"] for c in case_comps]))


# ── gen-router-gang: the engine as a gang behind a router ────────────────────
# Two gang Instances (one leader and W workers each) fronted by a single-pod
# router. An engine push never rolls the router: it stays in rotation on the
# same pods, and the gang rules (a gang moves whole, no two gangs share an
# index) hold through a push, a crash loop on the running revision, a scale
# and a canary ladder.
ROUTER_GANG_N = 2
ROUTER_GANG_WIDTHS = [1, 2]
ROUTER_GANG_TOPO = "engine-mn"
ROUTER_GANG_STRATS = [("SurgeThenDrain", 1, 0), ("RecreatePod", 0, 1), ("InPlaceIfPossible", 0, 1)]
ROUTER_GANG_CRASH_STRATS = ROUTER_GANG_STRATS[:2]
ROUTER_GANG_SCALES = [1, 3]
ROUTER_GANG_CANARIES = ["auto", "manual"]
ROUTER_GANG_CANARY_CELL = "canary/Idle/newTarget[routerIdleGang]"
ROUTER_HOLDS = f"the router is not bumped: it never rolls (peak {lib.NR}, the same pods) and stays in rotation throughout"
GANG_RULES = "no gang is ever split across revisions and no two gangs share an index"
GANG_LANDS = {
    "SurgeThenDrain": "each gang surges whole at a fresh index and its source gang is drained once the new one serves",
    "RecreatePod": "each gang is drained and rebuilt whole at the new revision, one gang at a time",
    "InPlaceIfPossible": ("a gang cannot be patched in place, so each gang is rebuilt whole at the new revision, "
                          "one gang at a time"),
}


def router_gang_counts():
    return dict(Ne=ROUTER_GANG_N, Nd=0, Nr=lib.NR)


def router_gang_ceilings(strat, surge, peak=None):
    n = ROUTER_GANG_N
    ce = peak if peak is not None else (n + surge if strat == "SurgeThenDrain" else n)
    return dict(ceil_e=ce, ceil_d=0, ceil_r=lib.NR)


def router_gang_comps(w, e_life, r_life=None):
    return [lib.comp("engine", ROUTER_GANG_N, w, e_life),
            lib.comp("router", lib.NR, 0, r_life if r_life is not None else lib.lifecycle())]


def _gang_words(w):
    return f"{ROUTER_GANG_N} gangs of 1 leader + {w} worker{'s' if w > 1 else ''}"


def _router_gang_push_reason(w, strat, surge):
    peak = ROUTER_GANG_N + surge if strat == "SurgeThenDrain" else ROUTER_GANG_N
    return (f"the engine ({_gang_words(w)}) is bumped alone; {GANG_LANDS[strat]} within its budget (peak {peak} "
            f"gangs); {GANG_RULES}; {ROUTER_HOLDS}; every engine Instance ends on the new revision")


def _router_gang_crash_reason(w, strat, form, rp):
    leader = "the highest gang's old-revision leader pod"
    return (f"{_crash_how(form, leader)}; its gang is the last the roll "
            f"reaches, and the roll owns it: {GANG_LANDS[strat]}, no repair is recorded for it after the bump under "
            f"restart policy {RP_FULL[rp]}, and every engine Instance ({_gang_words(w)}) ends on the new revision "
            f"serving; {GANG_RULES}; {ROUTER_HOLDS}; floor waived — the crash itself costs settled capacity for the "
            f"length of the roll")


def _router_gang_scale_reason(w, target):
    n = ROUTER_GANG_N
    return (f"the engine is scaled from {n} to {target} gang{'s' if target > 1 else ''} of 1 leader + {w} "
            f"worker{'s' if w > 1 else ''}: only index {max(n, target) - 1} is {'added' if target > n else 'removed'}, "
            f"whole; the surviving gang{'s keep their' if target > 2 else ' keeps its'} pods (same UIDs) and "
            f"{'serve' if target > 2 else 'serves'} throughout, so capacity never drops below min(start, target) = "
            f"{min(n, target)} and nothing surges above {max(n, target)}; no gang is left partial; the router holds "
            f"at {lib.NR} and stays in rotation")


def _router_gang_canary_reason(w, policy):
    n = ROUTER_GANG_N
    return (f"an engine-only ladder behind a router on {_gang_words(w)}: the group names the engine alone, so the "
            f"router is idle (no run of its own, every router pod on its revision, peak {lib.NR}) and stays in "
            f"rotation; the 10 percent step asks ceil(0.2) = 1 new gang, the 50 percent step asks ceil(1.0) = 1, the "
            f"same gang, so it moves traffic and adds no capacity; the stable revision keeps one gang until the "
            f"cutover's traffic write lands, then the last gang rolls; each step's gang surges whole beside the "
            f"stable one (peak {n + 1}); "
            f"{'the operator promotes at each of the two gates' if policy == 'manual' else 'each gate is a timed soak'}; "
            f"every engine Instance ends on the canary revision serving with nothing left on the stable one")


def emit_router_gang(add):
    topo, n = ROUTER_GANG_TOPO, ROUTER_GANG_N
    for w in ROUTER_GANG_WIDTHS:
        for strat, surge, unavail in ROUTER_GANG_STRATS:
            kv = dict(**router_gang_counts(), **router_gang_ceilings(strat, surge), co="none", topo=topo)
            meta = dict(strat=strat, verb="none", size=n)
            desc = f"{GEN_MARK} router gang push {shape_tag(topo, w)} N={n} {strat} {budget_tag(strat, surge, unavail)}"
            life = lib.lifecycle(strat=strat, surge=surge, unavail=unavail)
            add("gen-router-gang", desc, kv, router_gang_comps(w, life), _router_gang_push_reason(w, strat, surge),
                meta, cell=("router-gang-push", w, strat))
        for strat, surge, unavail in ROUTER_GANG_CRASH_STRATS:
            for form in CRASH_FORMS:
                for rp in RESTARTS:
                    kv = dict(**router_gang_counts(), **router_gang_ceilings(strat, surge), fault="crashloop:old",
                              form=form, floor_soft=1, co="none", topo=topo)
                    meta = dict(strat=strat, verb=f"crashloop:{form}", rp=rp, victim="engine", size=n)
                    desc = (f"{GEN_MARK} router gang crashloop {form} {shape_tag(topo, w)} N={n} {strat} "
                            f"{budget_tag(strat, surge, unavail)} rp {rp}")
                    life = lib.lifecycle(strat=strat, surge=surge, unavail=unavail, restart=RP_FULL[rp])
                    add("gen-router-gang", desc, kv,
                        router_gang_comps(w, life, r_life=lib.lifecycle(restart=RP_FULL[rp])),
                        _router_gang_crash_reason(w, strat, form, rp), meta,
                        cell=("router-gang-crash", w, strat, form, rp))
        strat, surge, unavail = ROUTER_GANG_STRATS[0]
        life = lib.lifecycle(strat=strat, surge=surge, unavail=unavail)
        for target in ROUTER_GANG_SCALES:
            kv = dict(mode="scaledown", **router_gang_counts(),
                      **router_gang_ceilings(strat, surge, peak=max(n, target)),
                      co="none", topo=topo, to_e=target, to_r=lib.NR)
            meta = dict(strat=strat, verb=f"scale:{'up' if target > n else 'down'}", size=n)
            desc = f"{GEN_MARK} router gang scale {shape_tag(topo, w)} {n}->{target} {strat}"
            add("gen-router-gang", desc, kv, router_gang_comps(w, life), _router_gang_scale_reason(w, target),
                meta, cell=("router-gang-scale", w, target))
        for policy in ROUTER_GANG_CANARIES:
            kv = dict(mode="canary", **router_gang_counts(), **router_gang_ceilings(strat, surge), canary=policy,
                      cell=ROUTER_GANG_CANARY_CELL, co="none", topo=topo)
            meta = dict(strat=strat, verb=f"canary:{policy}", size=n)
            desc = (f"{GEN_MARK} router gang canary {policy} {shape_tag(topo, w)} N={n} {strat} "
                    f"{budget_tag(strat, surge, unavail)}")
            add("gen-router-gang", desc, kv, router_gang_comps(w, life), _router_gang_canary_reason(w, policy),
                meta, cell=("router-gang-canary", w, policy),
                coord=lib.canary_block(lib.canary_steps(policy, *SIZE_LADDER), components=["engine"]))


# ── gen-ops-held: an operation lands on a stuck Component ───────────────────
def _ops_held_target(op, n):
    """The replica count the operation asks for; None for a delete."""
    return {"scale-down": n - 1, "scale-one": 1, "scale-up": n + 1}.get(op)


def _ops_held_reason(stuck, op, strat, topo, co, nr=0):
    how = ("the push lands a revision whose image cannot be pulled; " +
           ("its first attempt has failed (an Instance row reads Failed, the retry ladder is still running)"
            if stuck == "failed" else
            "every attempt of the retry ladder has failed and the push is held (only a new revision or a release lifts it)"))
    members = "engine and decoder are" if co != "none" else "the engine is"
    if strat == "SurgeThenDrain":
        shape = "the running Instances kept serving beside the failed replacements"
    else:
        shape = "the Instance each attempt drained is dark on the pushed revision"
    if op == "scale-down":
        what = (f"then {members} scaled down by one: the Instances that are not Ready go first, then the highest "
                f"index, so a stuck Instance is removed before a healthy one; the survivors keep their pods on the "
                f"running revision")
    elif op == "scale-one":
        what = (f"then {members} asked for one, the least count the API admits (a Component without KEDA "
                f"autoscaling may not ask for zero): the fleet converges on one Instance, the count the controller "
                f"accepted, with the stuck Instances gone first and no pod left behind on a removed index")
    elif op == "delete":
        what = ("then the service is deleted: every pod goes, stuck ones included, no pod stays Terminating and no "
                "finalizer wedges, and the same name comes back clean on fresh pods with nothing of the push left")
    else:
        what = (f"then {members} scaled up by one: " +
                ("while held nothing starts (the new index waits as an empty slot) and nothing is rendered from the "
                 "running revision" if stuck == "held" else
                 "the new Instance is admitted on the pushed revision when the retry window allows it, never on the "
                 "running revision, and is stuck like its peers") +
                "; the survivors stay as they were")
    out = f"{how}; {shape}; {what}"
    if op != "delete":
        out += "; the healthy survivors keep serving throughout and the status keeps reporting the push that has not landed"
    if is_pd(topo) and co == "none":
        out += "; decoder and router are neither pushed nor scaled and stay as they were"
    elif is_pd(topo) or nr:
        out += "; the router is neither pushed nor scaled and stays as it is"
    if is_mn(topo):
        out += "; each Instance is a gang, counted whole"
    return out


def emit_ops_held(add):
    for topo, n, w, nr in OPS_HELD_SHAPES:
        for strat, (surge, unavail) in OPS_HELD_STRATS.items():
            for co in OPS_HELD_GROUPS[topo]:
                for stuck in OPS_HELD_STUCK:
                    for op in OPS_HELD_OPS:
                        target = _ops_held_target(op, n)
                        if op == "scale-one" and target == n - 1:
                            continue   # constraint ledger: to one is down by one on a fleet of two
                        bump = surge if strat == "SurgeThenDrain" else 0
                        reach = max(n, target or 0)
                        kv = dict(**counts(topo, n, nr), ceil_e=reach + bump,
                                  ceil_d=((reach + bump if co != "none" else n) if is_pd(topo) else 0),
                                  ceil_r=router_count(topo, nr))
                        if op == "delete":
                            kv["mode"] = "recreate"
                        else:
                            kv["mode"] = "scaledown"
                            kv["to_e"] = target
                            if co != "none":
                                kv["to_d"] = target
                        kv.update(stuck=stuck, img=runtime_fixtures.UNPULLABLE_IMAGE, co=co, topo=topo)
                        meta = dict(strat=strat, verb=f"push:{stuck}", stuck=stuck, op=op)
                        desc = f"{GEN_MARK} stuck {stuck} {op} {shape_tag(topo, w, nr)} N={n} {strat} group {co}"
                        life = lib.lifecycle(strat=strat, surge=surge, unavail=unavail)
                        d_life = lib.lifecycle(strat=strat, surge=surge, unavail=unavail) if co != "none" else None
                        add("gen-ops-held", desc, kv, comps(topo, n, w, life, d_life=d_life, nr=nr),
                            _ops_held_reason(stuck, op, strat, topo, co, nr), meta,
                            cell=("ops-held", stuck, op, topo, w, strat, co, nr),
                            coord=lib.coord_for(co) if co != "none" else None)
    _emit_ops_held_breaks(add)


# The break rows of the ops-held family: the push carries one of the held
# moment's other kinds (stuck= with break=) and the operation lands on the
# parked push. One case per ledger cell (shape x operation x kind): (kind,
# the stuck state it is read at, the strategy), chosen so the rows meet
# both states under both strategies. The sibling row pushes the member
# beside the engine (the router in front of an engine fleet, the decoder
# of a prefill/decode service) onto the crash at start, as the rollback
# and edit families do, and lands the operation on the engine.
OPS_HELD_BREAKS = [("crash-start", "held", "SurgeThenDrain"), ("crash-load", "held", "RecreatePod"),
                   ("configkey", "failed", "RecreatePod"), ("stuck", "failed", "SurgeThenDrain")]
OPS_HELD_BREAK_OPS = ["scale-down", "delete", "scale-up"]
OPS_HELD_SIBLING_BREAK = ("crash-start", "held", "SurgeThenDrain")
OPS_HELD_CONFIG_SUFFIX = "-config"
# The pushed member's Instance ready timeout under a break: a revision that
# never comes up parks its row Failed within the stuck wait.
OPS_HELD_BREAK_READY_TIMEOUT = READINESS_NEW_TIMEOUT
OPS_HELD_BREAK_HOW = {"crash-start": "an image with no shell, so its pods crash at every start",
                      "crash-load": ("the fixture runtime's exit switch, so its pods pass readiness, are promoted and exit "
                                     "a few seconds later, every time the kubelet starts them"),
                      "configkey": "a ConfigMap key the ConfigMap lacks, so its pods wait in CreateContainerConfigError",
                      "stuck": "a readiness probe that never passes, so its pods run and never become Ready"}


def _ops_held_sibling(topo, nr):
    """The member beside the engine a shape carries: the decoder of a
    prefill/decode service, the router in front of an engine fleet, None
    on the engine alone."""
    return "decoder" if is_pd(topo) else ("router" if nr else None)


def _ops_held_break_reason(kind, stuck, op, strat, topo, nr, victim):
    sibling = victim != "engine"
    how = (f"the {victim} is pushed onto a revision whose runner carries {OPS_HELD_BREAK_HOW[kind]}; " +
           ("its first attempt has failed (an Instance row reads Failed, the retry ladder is still running)"
            if stuck == "failed" else
            "every attempt of the retry ladder has failed and the push is held (only a new revision or a release lifts it)"))
    if sibling:
        fleet = "the engine, which the push never touched, keeps every Instance serving on its running revision"
    elif strat == "SurgeThenDrain":
        fleet = "the running Instances kept serving beside the broken replacements"
    elif kind == "crash-load":
        fleet = "the Instance each attempt drained served briefly on the pushed revision and crash-loops there"
    else:
        fleet = "the Instance each attempt drained is dark on the pushed revision"
    if op == "delete":
        what = ("then the service is deleted: every pod goes, broken ones included, no pod stays Terminating and no "
                "finalizer wedges, and the same name comes back clean on fresh pods with nothing of the push left")
    elif sibling and op == "scale-down":
        what = (f"then the engine is scaled down by one: the highest index goes, as on a healthy fleet, the survivors "
                f"keep their pods on the running revision, and the {victim}'s parked push is not the scale's to take: "
                f"its pods stay as they were under the hold")
    elif sibling:
        what = (f"then the engine is scaled up by one: the new Instance comes up on the engine's running revision, the "
                f"only one it has, and serves; the survivors stay as they were and the {victim}'s parked push is not "
                f"the scale's to take: its pods stay as they were under the hold")
    elif op == "scale-down":
        what = ("then the engine is scaled down by one: the Instances that are not Ready go first, then the highest "
                "index, so a broken Instance is removed before a healthy one; the survivors keep their pods on the "
                "running revision")
    else:
        what = ("then the engine is scaled up by one: " +
                ("while held nothing starts (the new index waits as an empty slot) and nothing is rendered from the "
                 "running revision" if stuck == "held" else
                 "the new Instance is admitted on the pushed revision when the retry window allows it, never on the "
                 "running revision, and is broken like its peers") +
                "; the survivors stay as they were")
    out = f"{how}; {fleet}; {what}"
    if op != "delete":
        out += "; the healthy survivors keep serving throughout and the status keeps reporting the push that has not landed"
        if stuck == "held":
            out += ", the hold itself"
    if is_pd(topo) and not sibling:
        out += "; decoder and router are neither pushed nor scaled and stay as they were"
    elif is_pd(topo) or (nr and not sibling):
        out += "; the router is neither pushed nor scaled and stays as it is"
    if is_mn(topo):
        out += "; each Instance is a gang, counted whole"
    return out


def _ops_held_configkey_body(name, topo, n, w, nr, life):
    """The case body of a missing-key row: the engine runner reads one key of
    a ConfigMap named after the case, carried as a second document."""
    cm = f"{name}{OPS_HELD_CONFIG_SUFFIX}"
    cs = comps(topo, n, w, life, nr=nr)
    cs[0]["runner"] = lib.configmap_env(cm, HEAL_CONFIG_KEY, HEAL_CONFIG_VAR, gang=is_mn(topo))
    return lib.render(name, cs, docs=[lib.configmap_doc(cm, {HEAL_CONFIG_KEY: HEAL_CONFIG_VALUE})])


def _emit_ops_held_breaks(add):
    def one(topo, n, w, nr, kind, stuck, strat, op, victim):
        surge, unavail = OPS_HELD_STRATS[strat]
        target = _ops_held_target(op, n)
        reach = max(n, target or 0)
        sibling = victim != "engine"
        kv = dict(**counts(topo, n, nr), ceil_e=reach + (0 if sibling else surge),
                  ceil_d=((n + surge) if victim == "decoder" else (n if is_pd(topo) else 0)),
                  ceil_r=router_count(topo, nr) + (surge if victim == "router" else 0))
        if op == "delete":
            kv["mode"] = "recreate"
        else:
            kv["mode"] = "scaledown"
            kv["to_e"] = target
        kv.update(stuck=stuck, co="none", topo=topo)
        kv["break"] = kind
        if sibling:
            kv.update(bump=victim, victimcomp=victim)
        meta = dict(strat=strat, verb=f"push:{stuck}:{kind}" + (f":{victim}" if sibling else ""),
                    stuck=stuck, op=op, kind=kind, victim=victim)
        desc = (f"{GEN_MARK} stuck {stuck} {kind}" + (f" on {victim}" if sibling else "")
                + f" {op} {shape_tag(topo, w, nr)} N={n} {strat}")
        broken = lib.lifecycle(strat=strat, surge=surge, unavail=unavail, ready_timeout=OPS_HELD_BREAK_READY_TIMEOUT)
        life = lib.lifecycle() if sibling else broken
        d_life = broken if victim == "decoder" else None
        r_life = broken if victim == "router" else None
        reason = _ops_held_break_reason(kind, stuck, op, strat, topo, nr, victim)
        cell = ("ops-held-break", kind, stuck, op, topo, w, nr, victim)
        if kind == "configkey":
            add("gen-ops-held", desc, kv, None, reason, meta, cell=cell,
                body=functools.partial(_ops_held_configkey_body, topo=topo, n=n, w=w, nr=nr, life=life))
        else:
            add("gen-ops-held", desc, kv, comps(topo, n, w, life, d_life=d_life, r_life=r_life, nr=nr), reason, meta, cell=cell)

    for topo, n, w, nr in OPS_HELD_SHAPES:
        for kind, stuck, strat in OPS_HELD_BREAKS:
            for op in OPS_HELD_BREAK_OPS:
                one(topo, n, w, nr, kind, stuck, strat, op, "engine")
        sibling = _ops_held_sibling(topo, nr)
        if sibling:
            kind, stuck, strat = OPS_HELD_SIBLING_BREAK
            for op in OPS_HELD_BREAK_OPS:
                one(topo, n, w, nr, kind, stuck, strat, op, sibling)


# ── gen-runtime-edit-repair: a repair and a runtime edit overlap ────────────
def _edit_repair_opens(order, form, rp):
    """Whether the cell exists: the edit-during-repair order needs a repair
    to land on, which a crash after Ready opens only under RecreateInstance."""
    return not (order == "edit-during-repair" and form == "load" and rp == "None")


def _edit_repair_reason(order, form, edit, strat, topo):
    how = ("one engine pod is pointed at an image with no shell, so it crashes at every start and "
           "is never Ready again" if form == "start" else
           "one engine pod has its runner killed every few seconds while it is up, so it dies after "
           "Ready and comes back between crashes")
    what = ("the runtime every Component inherits gets a new image" if edit == "image" else
            "the runtime every Component inherits gets a new runner env")
    if order == "edit-during-repair":
        out = (f"{how}; the product opens a repair of its Instance, and while the repair's replacement pod "
               f"is coming up {what}; the last change wins: every Instance, the repaired one included, ends on the "
               f"edited runtime's revision and serving, the repaired Instance is rebuilt at most once "
               f"after the edit with no repair opened for it from then on, and no Instance is left "
               f"mid-operation")
    else:
        out = (f"{what} and, as the edit lands, {how}; its Instance is off the target from that moment, "
               f"so the roll owns it: it ends on the edited runtime's revision and serving with no repair "
               f"recorded for it, every other Instance converges the same way, and no Instance is left "
               f"mid-operation")
    if strat in IN_PLACE and edit == "env":
        out += "; the env edit is a pod-template change beyond the image, so the in-place strategy rebuilds instead of patching"
    elif strat in IN_PLACE:
        out += "; the image edit is patched in place on every pod that is up"
    if is_pd(topo):
        out += "; engine and decoder each roll onto their own new revision"
    return out


def emit_runtime_edit_repair(add):
    for topo, n in EDIT_REPAIR_SHAPES:
        for strat, (surge, unavail) in EDIT_REPAIR_STRATS.items():
            for rp in RESTARTS:
                for form in CRASH_FORMS:
                    for order in EDIT_REPAIR_ORDERS:
                        for edit in EDIT_REPAIR_EDITS:
                            if not _edit_repair_opens(order, form, rp):
                                continue
                            kv = dict(mode="runtimerev", Ne=n, Nd=(n if is_pd(topo) else 0), Nr=0,
                                      ceil_e=(n + surge if strat == "SurgeThenDrain" else n),
                                      ceil_d=((n + surge if strat == "SurgeThenDrain" else n) if is_pd(topo) else 0),
                                      ceil_r=0, fault="crashloop:old", co="none", topo=topo,
                                      expect="roll", edit=edit, form=form)
                            if edit == "image":
                                kv["img"] = ALT_IMAGE_TOKEN
                            if order == "edit-during-repair":
                                kv["during"] = "repair"
                            meta = dict(strat=strat, verb=f"crashloop:{form}", rp=rp, order=order, edit=edit)
                            desc = (f"{GEN_MARK} runtime-edit {order} {form} {topo} N={n} {strat} "
                                    f"rp {rp} edit {edit}")
                            life = lib.lifecycle(strat=strat, surge=surge, unavail=unavail, restart=RP_FULL[rp])
                            case_comps = [lib.comp("engine", n, 0, life)]
                            if is_pd(topo):
                                case_comps.append(lib.comp("decoder", n, 0, life))
                            add("gen-runtime-edit-repair", desc, kv, case_comps,
                                _edit_repair_reason(order, form, edit, strat, topo), meta,
                                cell=("runtime-edit-repair", order, edit, form, topo, strat, rp),
                                edit_runtime=True)


# ── gen-migrate-bump: a move and a revision bump that overlap ───────────────
def _migrate_bump_budgets(topo, strat):
    if topo == "engine" and strat in MIGRATE_BUMP_ENGINE_BUDGETS:
        return MIGRATE_BUMP_ENGINE_BUDGETS[strat]
    return MIGRATE_BUMP_BUDGETS[strat]


def _migrate_bump_reachable(first, hook, surge):
    """A surge of two promotes its last pair together, so the moment with
    exactly one Instance still off the new revision is not reliably seen."""
    return not (first == "roll" and hook == "last" and isinstance(surge, int) and surge > 1)


def _migrate_bump_extra(n, strat, surge):
    """Pods above N the move and the roll may hold together: the move's one
    replacement, or the roll's surge when that is larger."""
    return max(surge_count(n, surge), 1) if strat == "SurgeThenDrain" else 1


def _migrate_bump_comps(topo, n, w, life, co):
    """The engine is moved and rolled; a grouped decoder shares the engine's
    strategy so the group paces both on the same budget."""
    cw = w if is_mn(topo) else 0
    cs = [lib.comp("engine", n, cw, life)]
    if is_pd(topo):
        cs.append(lib.comp("decoder", n, cw, life if co != "none" else lib.lifecycle()))
        cs.append(lib.comp("router", lib.NR, 0, lib.lifecycle()))
    return cs


def _migrate_bump_reason(first, hook, topo, strat, extra, co):
    out = (f"{MIGRATE_BUMP_MOMENT[(first, hook)]}; both finish: every Instance ends on the new revision, "
           f"the moved one among them and off the node it left, no Instance is left mid-operation, the move "
           f"is recorded once and consumed once, the capacity floor holds throughout and the peak never "
           f"exceeds N+{extra} (the move and the roll together stay within the roll's budget)")
    if strat == "SurgeThenDrain" and is_mn(topo):
        out += "; a gang roll may rebuild the moved Instance under another index"
    elif strat == "SurgeThenDrain":
        out += "; the roll's surge and the move's replacement share the headroom"
    elif strat == "RecreatePod":
        out += "; the roll drains first within the unavailable budget, the move is the only surge"
    elif is_mn(topo):
        out += "; the gang roll falls back to a whole-gang recreate, the move is the only surge"
    else:
        out += "; the roll patches in place, the move is the only surge"
    if co != "none":
        out += "; the roll runs under a rollingUpdate group" + (
            " that paces engine and decoder together" if is_pd(topo) else "")
    return out


def emit_migrate_bump(add):
    for topo, n, w in MIGRATE_BUMP_SHAPES:
        for strat in MIGRATE_BUMP_BUDGETS:
            for surge, unavail in _migrate_bump_budgets(topo, strat):
                for first, hook in MIGRATE_BUMP_ORDERS:
                    if not _migrate_bump_reachable(first, hook, surge):
                        continue
                    for co in MIGRATE_BUMP_COORDS:
                        extra = _migrate_bump_extra(n, strat, surge)
                        kv = dict(mode="migrate", **counts(topo, n), ceil_e=n + extra,
                                  ceil_d=(n if is_pd(topo) else 0), ceil_r=(lib.NR if is_pd(topo) else 0),
                                  co=co, topo=topo, mig="Auto")
                        if co != "none" and is_pd(topo) and strat == "SurgeThenDrain":
                            kv["ceil_d"] = n + surge_count(n, surge)   # the grouped decoder rolls on its own surge
                        if first == "move":
                            kv.update(inst=0, then="bump", at=hook)
                        else:
                            kv.update(inst=("last" if hook == "last" else 0), during=hook)
                        meta = dict(strat=strat, verb=f"{'bump' if first == 'move' else 'move'}-at-{hook}",
                                    first=first, hook=hook, surge=surge)
                        desc = (f"{GEN_MARK} {'move then bump' if first == 'move' else 'bump then move'} at {hook} "
                                f"{shape_tag(topo, w)} N={n} {strat} {budget_tag(strat, surge, unavail)} co {co}")
                        life = lib.lifecycle(strat=strat, surge=surge, unavail=unavail)
                        coord = None
                        if co != "none":
                            coord = lib.coord_for(co, comps=("engine", "decoder") if is_pd(topo) else ("engine",))
                        add("gen-migrate-bump", desc, kv, _migrate_bump_comps(topo, n, w, life, co),
                            _migrate_bump_reason(first, hook, topo, strat, extra, co), meta,
                            cell=("migrate-bump", first, hook, topo, w, strat, surge, unavail, co), coord=coord)


# ── gen-readiness: a pod that runs but never passes readiness ────────────────
def _canonical_budget(strat):
    return dict(STRATS)[strat][0]


def _readiness_old_reason(form, topo, nr=0):
    victim = ("the leader pod of the engine's highest gang" if is_mn(topo)
              else "the pod of the engine's highest Instance")
    out = (f"{victim} stops passing its readiness probe while it keeps running; readiness is "
           f"the runtime's own out-of-rotation signal, not a failure, so no restart policy rebuilds it: the "
           f"Instance leaves the Ready endpoints at once, its row reports the drop (serving count below pod "
           f"count), and nothing is rebuilt or surged")
    if form == "persist":
        out += "; the pod stays out of rotation and reported across the hold window, until an operator acts"
    else:
        out += ("; when the probe passes again the same pod returns to rotation and the row recovers, with no "
                "rebuild and no new revision")
    if is_pd(topo):
        out += "; decoder and router are untouched"
    elif nr:
        out += "; the router is untouched"
    return out


def _readiness_new_reason(strat, topo, nr=0):
    how = {"SurgeThenDrain": "the surge Instance is the one that fails and nothing drains (floor N)",
           "RecreatePod": "the first drained Instance's replacement is the one that fails (floor N-1)",
           "InPlaceIfPossible": ("the switch is a template change beyond images, so the strategy recreates and "
                                 "the first drained Instance's replacement is the one that fails (floor N-1)")}[strat]
    out = ("the pushed revision carries the readiness switch, so its pods run and never pass readiness: the roll "
           "holds at the Instance ready timeout with the new Instance failed and the reason recorded, the old "
           "revision keeps serving at the floor and the broken revision never serves; a fix push without the "
           f"switch then lands on every Instance; {how}")
    if is_pd(topo):
        out += "; decoder and router are untouched"
    elif nr:
        out += "; the router is untouched"
    return out


def emit_readiness(add):
    for shapes in READINESS_BATCHES:
        for topo, n, w, nr in shapes:
            pd = is_pd(topo)
            for form in READINESS_FORMS:
                for rp in RESTARTS:
                    kv = dict(**counts(topo, n, nr), ceil_e=n, ceil_d=(n if pd else 0), ceil_r=router_count(topo, nr),
                              restart=rp, fault="readiness:old", co="none", topo=topo, form=form, during="idle")
                    desc = f"{GEN_MARK} readiness lost {form} {shape_tag(topo, w, nr)} N={n} rp {rp}"
                    life = lib.lifecycle(strat="SurgeThenDrain", surge=1, unavail=0, restart=RP_FULL[rp])
                    d_life = lib.lifecycle(restart=RP_FULL[rp]) if pd else None
                    add("gen-readiness", desc, kv, comps(topo, n, w, life, d_life=d_life, nr=nr),
                        _readiness_old_reason(form, topo, nr),
                        dict(strat="SurgeThenDrain", verb=f"readiness:old:{form}", rp=rp),
                        cell=("readiness", "old", form, topo, w, rp, nr))
        for topo, n, w, nr in shapes:
            pd = is_pd(topo)
            for strat in READINESS_STRATS:
                surge, unavail = _canonical_budget(strat)
                for rp in RESTARTS:
                    kv = dict(**counts(topo, n, nr), **ceilings(topo, n, strat, surge, nr),
                              restart=rp, fault="readiness:new", co="none", topo=topo)
                    desc = (f"{GEN_MARK} readiness never {shape_tag(topo, w, nr)} N={n} {strat} "
                            f"{budget_tag(strat, surge, unavail)} rp {rp}")
                    life = lib.lifecycle(strat=strat, surge=surge, unavail=unavail, restart=RP_FULL[rp],
                                         ready_timeout=READINESS_NEW_TIMEOUT)
                    d_life = lib.lifecycle(restart=RP_FULL[rp]) if pd else None
                    add("gen-readiness", desc, kv, comps(topo, n, w, life, d_life=d_life, nr=nr),
                        _readiness_new_reason(strat, topo, nr),
                        dict(strat=strat, verb="readiness:new", rp=rp),
                        cell=("readiness", "new", topo, w, strat, rp, nr))


# ── gen-gang-member: a gang's leader or worker dies mid-roll ────────────────
def _gang_member_reason(phase, member, strat, rp):
    when = {"settling": "from a settled gang the moment the bump lands",
            "rotating": "from the gang forming on the new revision, at its first pod of that member",
            "draining": "from a serving gang while the first gang is on its way out"}[phase]
    scope = ("the engine rebuilds the gang as a whole (a RestartTriggered names it) unless the roll takes it "
             "first, in which case every pod of it is new on the target"
             if rp == "RecreateInstance" else
             f"only the lost {member} comes back; no whole-gang rebuild and no RestartTriggered")
    out = (f"the {member} is deleted {when}: {scope}; the gang is never counted as serving while a member "
           f"is missing, the untouched gangs keep serving (floor N-maxUnavailable-1), and the roll ends with "
           f"every gang whole on the new revision")
    if strat == "SurgeThenDrain":
        out += "; order waived — the deletion is a drain with no preceding surge"
    return out


def emit_gang_member(add):
    for topo, n, w in GANG_MEMBER_SHAPES:
        for strat in GANG_MEMBER_STRATS:
            surge, unavail = _canonical_budget(strat)
            for phase, victim in GANG_MEMBER_TIMINGS:
                for member in GANG_MEMBERS:
                    for rp in RESTARTS:
                        kv = dict(**counts(topo, n), **ceilings(topo, n, strat, surge), restart=rp, kill=member,
                                  fault=f"podkill:{phase}", co="none", topo=topo, victim=victim, floor_slack=1)
                        if strat == "SurgeThenDrain":
                            kv["order_soft"] = 1
                        desc = f"{GEN_MARK} gang {member} lost {phase} {shape_tag(topo, w)} N={n} {strat} rp {rp}"
                        life = lib.lifecycle(strat=strat, surge=surge, unavail=unavail, restart=RP_FULL[rp])
                        d_life = lib.lifecycle(restart=RP_FULL[rp]) if is_pd(topo) else None
                        add("gen-gang-member", desc, kv, comps(topo, n, w, life, d_life=d_life),
                            _gang_member_reason(phase, member, strat, rp),
                            dict(strat=strat, verb=f"podkill:{phase}:{member}", rp=rp),
                            cell=("gang-member", phase, member, topo, w, strat, rp))


# ── gen-tight: a gang roll with no room for its surge gang ──────────────────
def _tight_reason(strat, squeeze, lift, w):
    gang = f"{w + 1}-pod gang"
    squeezed = (f"the quota admits {w} more pod(s), one short of a {gang}" if squeeze == "quota"
                else "every schedulable node is cordoned")
    if strat == "RecreatePod" and squeeze == "quota":
        return (f"{squeezed}; the first gang drains within its own budget and its {gang} replacement is admitted "
                f"once the old pods are gone (at most a brief quota refusal while they terminate): the roll "
                f"completes with no wait, at the floor N-1, and no second gang drains")
    if strat == "SurgeThenDrain":
        waits = (f"so the surge {gang} cannot be placed: the roll waits with the Instance's operation naming the "
                 f"capacity, no running gang drains (floor N) and every running gang keeps serving")
    else:
        waits = (f"so once the first gang drains within its own budget its {gang} replacement has no placement: "
                 f"the roll waits at N-1 serving with the operation naming the capacity and drains no second gang")
    ends = ("once the squeeze is lifted the gang is placed and the roll lands on every Instance"
            if lift == "after-hold" else
            "the wait holds across the window with no churn and nothing torn down; the case ends with the squeeze in place")
    return f"{squeezed}, {waits}; {ends}"


def emit_tight(add):
    for topo, n, w in TIGHT_SHAPES:
        for strat in TIGHT_STRATS:
            surge, unavail = _canonical_budget(strat)
            life = lib.lifecycle(strat=strat, surge=surge, unavail=unavail)
            for squeeze in TIGHT_SQUEEZES:
                fault = f"quota:{w}" if squeeze == "quota" else "cordon:all"
                base = dict(**counts(topo, n), **ceilings(topo, n, strat, surge), fault=fault, co="none", topo=topo)
                if strat == "RecreatePod" and squeeze == "quota":
                    desc = f"{GEN_MARK} tight {squeeze} completes {shape_tag(topo, w)} N={n} {strat}"
                    add("gen-tight", desc, base, comps(topo, n, w, life), _tight_reason(strat, squeeze, None, w),
                        dict(strat=strat, verb=f"{squeeze}:completes"),
                        cell=("tight", topo, w, strat, squeeze, "completes"))
                    continue
                for lift in TIGHT_LIFTS:
                    desc = f"{GEN_MARK} tight {squeeze} {lift} {shape_tag(topo, w)} N={n} {strat}"
                    add("gen-tight", desc, dict(base, lift=lift), comps(topo, n, w, life),
                        _tight_reason(strat, squeeze, lift, w),
                        dict(strat=strat, verb=f"{squeeze}:{lift}"),
                        cell=("tight", topo, w, strat, squeeze, lift))


# ── gen-evict: a pod of a Ready Instance is evicted ─────────────────────────
def _recreate_scope(topo, rp):
    """What the restart policy rebuilds after one pod of an Instance is gone."""
    if rp == "RecreateInstance" and is_mn(topo):
        return "the whole Instance (leader and workers) is rebuilt"
    if is_mn(topo):
        return "exactly the removed pod is recreated"
    return "the Instance (one pod) is recreated"


def _router_control(topo, nr):
    """The router's part on an engine-only shape behind one: a negative
    control the operation never touches; nothing on the other shapes."""
    return "; the router is a negative control (co=none)" if nr and not is_pd(topo) else ""


def _evict_steady_reason(topo, rp, target, nr=0):
    who = f"the {target} pod of one gang" if target else "one engine pod"
    return (f"{who} is evicted through the Eviction API, as a node drain would; the disruption budget admits it with "
            f"every Instance serving, {_recreate_scope(topo, rp)} under restart policy {RP_FULL[rp]}, no other Instance "
            f"is touched and the fleet is back at N serving; nothing surges on a service nobody bumped"
            + ("; the router keeps its pods" if nr and not is_pd(topo) else ""))


def _evict_roll_reason(topo, strat, waivers, nr=0):
    victim = _podkill_victim(topo, strat, DISRUPT_PHASE)
    out = (f"once the roll is in flight {victim} is evicted through the Eviction API, as a node drain would, asked again "
           f"while the disruption budget refuses it under {strat}; the controller replaces it, the roll converges with "
           f"every Instance on the new revision and no pod comes back under the old label")
    if is_pd(topo):
        out += "; decoder and router are negative controls (co=none)"
    out += _router_control(topo, nr)
    if waivers.get("floor_soft"):
        out += "; floor waived — the eviction itself costs settled capacity the update budget cannot absorb"
    if waivers.get("order_soft"):
        out += "; order waived — the eviction is a drain with no preceding surge"
    return out


def _kill_counts(topo, n, nr=0):
    return dict(**counts(topo, n, nr), ceil_e=n, ceil_d=(n if is_pd(topo) else 0), ceil_r=router_count(topo, nr))


def emit_evict(add):
    for topo, n, w, nr in DISRUPT_SHAPES:
        for rp in RESTARTS:
            life = lib.lifecycle(restart=RP_FULL[rp])
            d_life = lib.lifecycle(restart=RP_FULL[rp]) if is_pd(topo) else None
            for target in (["leader", "worker"] if is_mn(topo) else [None]):
                kv = dict(mode="kill", **_kill_counts(topo, n, nr), restart=rp)
                if target:
                    kv["kill"] = target
                kv.update(co="none", topo=topo, via="evict")
                meta = dict(strat="SurgeThenDrain", verb="evict:steady", rp=rp)
                desc = (f"{GEN_MARK} evict steady {shape_tag(topo, w, nr)} N={n} rp {rp}"
                        + (f" target {target}" if target else ""))
                add("gen-evict", desc, kv, comps(topo, n, w, life, d_life=d_life, nr=nr),
                    _evict_steady_reason(topo, rp, target, nr), meta,
                    cell=("evict", "steady", topo, w, rp, target, nr))
            for strat, surge, unavail in DISRUPT_STRATS:
                waivers = _podkill_waivers(topo, strat, DISRUPT_PHASE)
                kv = dict(**counts(topo, n, nr), **ceilings(topo, n, strat, surge, nr),
                          fault=f"evict:{DISRUPT_PHASE}", **waivers, co="none", topo=topo)
                meta = dict(strat=strat, verb=f"evict:{DISRUPT_PHASE}", rp=rp)
                desc = (f"{GEN_MARK} evict {DISRUPT_PHASE} {shape_tag(topo, w, nr)} N={n} {strat} "
                        f"{budget_tag(strat, surge, unavail)} rp {rp}")
                roll_life = lib.lifecycle(strat=strat, surge=surge, unavail=unavail, restart=RP_FULL[rp])
                add("gen-evict", desc, kv, comps(topo, n, w, roll_life, d_life=d_life, nr=nr),
                    _evict_roll_reason(topo, strat, waivers, nr), meta,
                    cell=("evict", DISRUPT_PHASE, topo, w, strat, rp, nr))
    _emit_evict_ops(add)


# ── gen-preempt: a pod of a Ready Instance is preempted ─────────────────────
def _preempt_reason(moment, topo, rp, lift, strat=None, nr=0):
    base = ("fillers take the spare CPU of every node, the pending taint closes every node to the case, and a "
            "higher-priority filler presses on the node of the aimed pod, so the scheduler preempts one pod of the "
            "case there")
    if moment == "steady":
        out = (f"{base}; {_recreate_scope(topo, rp)} under restart policy {RP_FULL[rp]} and its new pod waits with "
               f"reason Unschedulable while every other Instance keeps serving and nothing is taken down to make room")
    else:
        out = (f"{base} while the roll is in flight under {strat}; the roll's next pod waits with reason Unschedulable, "
               f"nothing settled is taken down to make room and the capacity floor holds")
    if lift == "after-settle":
        out += ("; once the lift opens the nodes and drops the fillers the Instance is placed again and serves, back at N"
                if moment == "steady"
                else "; once the lift opens the nodes and drops the fillers the roll converges with every Instance on "
                     "the new revision")
    else:
        out += ("; the taint and the fillers stay to the end, so the wait holds with its reason across the hold window "
                "and no Instance reads Failed")
    if is_pd(topo) and moment != "steady":
        out += "; decoder and router are negative controls (co=none)"
    if moment != "steady":
        out += _router_control(topo, nr)
        out += "; floor and order waived — the preemption itself takes a pod the update budget cannot absorb"
    elif nr and not is_pd(topo):
        out += "; the router keeps its pods"
    return out


def emit_preempt(add):
    for topo, n, w, nr in DISRUPT_SHAPES:
        for rp in RESTARTS:
            d_life = lib.lifecycle(restart=RP_FULL[rp]) if is_pd(topo) else None
            for lift in PENDING_LIFTS:
                kv = dict(mode="kill", **_kill_counts(topo, n, nr), restart=rp, co="none", topo=topo, via="preempt", lift=lift)
                meta = dict(strat="SurgeThenDrain", verb="preempt:steady", rp=rp, lift=lift)
                desc = f"{GEN_MARK} preempt steady {shape_tag(topo, w, nr)} N={n} rp {rp} lift {lift}"
                add("gen-preempt", desc, kv, comps(topo, n, w, lib.lifecycle(restart=RP_FULL[rp]), d_life=d_life, nr=nr),
                    _preempt_reason("steady", topo, rp, lift, nr=nr), meta,
                    cell=("preempt", "steady", topo, w, rp, lift, nr))
                for strat, surge, unavail in DISRUPT_STRATS:
                    kv = dict(**counts(topo, n, nr), **ceilings(topo, n, strat, surge, nr),
                              fault=f"preempt:{DISRUPT_PHASE}", floor_soft=1, order_soft=1, co="none", topo=topo, lift=lift)
                    meta = dict(strat=strat, verb=f"preempt:{DISRUPT_PHASE}", rp=rp, lift=lift)
                    desc = (f"{GEN_MARK} preempt {DISRUPT_PHASE} {shape_tag(topo, w, nr)} N={n} {strat} "
                            f"{budget_tag(strat, surge, unavail)} rp {rp} lift {lift}")
                    roll_life = lib.lifecycle(strat=strat, surge=surge, unavail=unavail, restart=RP_FULL[rp])
                    add("gen-preempt", desc, kv, comps(topo, n, w, roll_life, d_life=d_life, nr=nr),
                        _preempt_reason(DISRUPT_PHASE, topo, rp, lift, strat, nr), meta,
                        cell=("preempt", DISRUPT_PHASE, topo, w, strat, rp, lift, nr))
    _emit_preempt_ops(add)


# ── gen-packed-pool: the only room is what a broken pod vacates ─────────────
def packed_counts(topo, n, router):
    if is_pd(topo):
        return dict(Ne=n, Nd=n, Nr=lib.NR)
    return dict(Ne=n, Nd=0, Nr=lib.NR if router else 0)


def packed_ceilings(topo, n, router):
    """No strategy here adds a pod: every Component peaks at its count."""
    if is_pd(topo):
        return dict(ceil_e=n, ceil_d=n, ceil_r=lib.NR)
    return dict(ceil_e=n, ceil_d=0, ceil_r=lib.NR if router else 0)


def packed_comps(topo, n, w, router, life):
    """The engine, the decoder where one exists under the same one-down
    lifecycle (a grouped decoder roll has to fit the pool too), and the
    router of a prefill/decode shape or of an engine-only shape behind one."""
    cw = w if is_mn(topo) else 0
    cs = [lib.comp("engine", n, cw, life)]
    if is_pd(topo):
        cs.append(lib.comp("decoder", n, cw, life))
    if is_pd(topo) or router:
        cs.append(lib.comp("router", lib.NR, 0, lib.lifecycle()))
    return cs


def _packed_reason(side, form, strat, rp, co, router):
    pool = ("fillers take every node's spare CPU once the service has settled, so the only room a pod can be "
            "placed in is what a pod of the service vacates")
    # The engine rolling alone comes back in its own room; with the decoder
    # rolling beside it, room either Component's pods vacated is the pool's.
    room = ("in the room it vacated" if co == "none" else
            "in room this service's own pods vacated (the decoder rolls beside the engine, so its room counts too)")
    if side == "new":
        how = ("the pushed engine revision dies at every start (its runner image has no shell for the fixture command)"
               if form == "start" else
               "the pushed engine revision passes readiness, is promoted, and exits a few seconds later, every time "
               "the kubelet restarts it")
        lands = {"RecreatePod": "the first Instance drained is rebuilt on the crashing revision in the room its pod vacated",
                 "InPlaceIfPossible": ("the image patches the first Instance's pod in place and it crash-loops there"
                                       if form == "start" else
                                       "the exit switch is a pod-template env diff, so in-place resolves to a recreate of "
                                       "the first Instance in the room its pod vacated")}[strat]
        out = (f"{pool}; {how}; {lands} and the roll stops there, never two Instances down at once; status says why "
               f"(a rolloutHold or a Failed row, and a RetryHeld or InstanceFailed event) and no further attempt at that "
               f"revision follows; the good push then lands on the broken Instance first, {room}, then on the "
               f"others one at a time, every Instance ends serving on it and nothing rebuilds the crashing revision")
        if rp == "RecreateInstance":
            out += f"; every rebuild RecreateInstanceOnPodRestart opens on the crashing Instance lands {room}"
    else:
        how = ("one engine pod of the running revision is pointed at an image with no shell on the settled service, "
               "so it crashes at every start and is never Ready again"
               if form == "start" else
               "one engine pod of the running revision has its runner killed every few seconds while it is up, so "
               "it dies after Ready and comes back between crashes")
        out = (f"{pool}; {how}; once the break has bitten the good push lands: the broken Instance, which serves "
               f"nothing, is rolled first and comes back {room}, the others follow one at a time, "
               f"never two Instances down at once, every Instance ends serving on the good revision, no repair is "
               f"recorded for the broken Instance after the push and nothing rebuilds the running revision")
    if form == "load":
        out += ("; the one-down rule and the landing order are reported, not asserted: a runner that exits after "
                "Ready can be Ready when the roll decides, so the roll need not read that Instance as serving nothing")
    out += "; the audit ledger records no AutoRecover row and no pod carries a hostname NotIn term (invariants J and K)"
    if co != "none":
        out += "; the decoder rolls under the same one-down budget beside the engine (converges, or holds with a reason)"
    if router:
        out += "; the router in front is never bumped and stays in rotation"
    return out


def emit_packed_pool(add):
    for topo, n, w, router in PACKED_SHAPES:
        for side in PACKED_SIDES:
            for strat, surge, unavail in PACKED_STRATS:
                for form in CRASH_FORMS:
                    for rp in PACKED_RESTARTS[side]:
                        for co in PACKED_GROUPS[topo]:
                            kv = dict(**packed_counts(topo, n, router), **packed_ceilings(topo, n, router),
                                      fault=f"crashloop:{side}", pool=PACKED_POOL, co=co, topo=topo, form=form,
                                      inv_on=PACKED_INVARIANTS)
                            if form == "load":
                                kv["floor_soft"] = 1
                            story = "bad push then fix" if side == "new" else "broken running then fix"
                            meta = dict(strat=strat, verb=f"packed:{side}:{form}", rp=rp, side=story)
                            desc = (f"{GEN_MARK} packed pool {story} {form} {shape_tag(topo, w)}"
                                    f"{' +router' if router else ''} N={n} {strat} {budget_tag(strat, surge, unavail)} "
                                    f"rp {rp} co {co}")
                            life = lib.lifecycle(strat=strat, surge=surge, unavail=unavail, restart=RP_FULL[rp])
                            add("gen-packed-pool", desc, kv, packed_comps(topo, n, w, router, life),
                                _packed_reason(side, form, strat, rp, co, router), meta,
                                cell=("packed-pool", side, form, topo, w, router, strat, rp, co),
                                coord=lib.coord_for(co) if co != "none" else None)


# ── gen-repeat: a fault that keeps repeating until it is lifted ────────────
def _repeat_counts(shape):
    _label, topo, n, _w, router = shape
    if is_pd(topo):
        return counts(topo, n)
    return dict(Ne=n, Nd=0, Nr=(lib.NR if router else 0))


def _repeat_ceilings(shape, strat, surge, to_e=None):
    c = _repeat_counts(shape)
    ce = to_e if to_e is not None else (c["Ne"] + surge if strat == "SurgeThenDrain" else c["Ne"])
    return dict(ceil_e=ce, ceil_d=c["Nd"], ceil_r=c["Nr"])


def _repeat_comps(shape, e_life, d_life=None, r_life=None):
    _label, topo, n, w, router = shape
    cw = w if is_mn(topo) else 0
    cs = [lib.comp("engine", n, cw, e_life)]
    if is_pd(topo):
        cs.append(lib.comp("decoder", n, cw, d_life if d_life is not None else lib.lifecycle()))
    if is_pd(topo) or router:
        cs.append(lib.comp("router", lib.NR, 0, r_life if r_life is not None else lib.lifecycle()))
    return cs


def _repeat_shape_tag(shape):
    _label, topo, _n, w, router = shape
    return shape_tag(topo, w) + (" +router" if router else "")


def _repeat_what(kind, member):
    pod = f"the {member} of one engine gang" if member else "one engine pod"
    return {"podkill": f"{pod} is deleted by hand",
            "evict": f"{pod} is evicted through the Eviction API, the way a drain removes it,",
            "preempt": ("one engine pod is preempted by a higher-priority filler (every node's spare CPU taken and "
                        "every node closed to the case)")}[kind]


REPEAT_WINDOW_CLAIMS = ("the status reports each loss and never counts the short Instance whole, every repair "
                        "recorded is for an Instance the fault took, the pods created are those repairs{plus} and no "
                        "more (no hot loop), no pod comes back on a revision the service never pushed")
REPEAT_END_CLAIM = "after the lift every Instance is Ready and serving on its revision with no row mid-operation and no hold left behind"
# An eviction the disruption budget refuses is asked again, and the
# operation's window waits for the repetition instead of closing on a fleet
# that is whole only because the eviction is being refused.
REPEAT_EVICT_CLAIM = ("; an eviction the disruption budget refuses is asked again every few seconds and the operation's "
                      "window waits for that repetition; under a budget that keeps refusing while the operation holds an "
                      "Instance down, the repetitions are those the budget admits, at least one")


def _repeat_steady_reason(kind, shape, rp, member, n):
    _label, topo = shape[0], shape[1]
    return (f"{_repeat_what(kind, member)} again each time its Instance is back and serving, {n} times, then left alone: "
            f"each time {_recreate_scope(topo, rp)} under restart policy {RP_FULL[rp]} within the budget and nothing else "
            f"moves; across the window the other Instances keep serving (floor N-1), "
            + REPEAT_WINDOW_CLAIMS.format(plus="") + " and nothing surges on a service nobody bumped; " + REPEAT_END_CLAIM
            + (REPEAT_EVICT_CLAIM if kind == "evict" else ""))


def _repeat_preempt_reason(shape, rp, n):
    topo = shape[1]
    return (f"{_repeat_what('preempt', None)} again each time its Instance is placed and serving, {n} times, then left "
            f"alone: each time the Instance waits with reason Unschedulable until the lift opens the nodes and drops "
            f"the fillers, then "
            f"{_recreate_scope(topo, rp)} under restart policy {RP_FULL[rp]}, nothing is taken down to make room and the "
            f"fleet is back at N; across the window " + REPEAT_WINDOW_CLAIMS.format(plus="") + "; " + REPEAT_END_CLAIM)


def _repeat_push_reason(kind, shape, strat, rp, member, n):
    topo = shape[1]
    out = (f"{_repeat_what(kind, member)} again each time its Instance is back and serving while the roll lands under "
           f"{strat}, {n} times, then left alone: the roll converges with every Instance on the new revision; across the "
           f"window the untouched Instances hold the floor (N-maxUnavailable-1), "
           + REPEAT_WINDOW_CLAIMS.format(plus=" and the roll's own pods") + "; " + REPEAT_END_CLAIM)
    if rp == "RecreateInstance" and is_mn(topo):
        out += "; each loss rebuilds the gang whole unless the roll takes it first"
    if is_pd(topo):
        out += "; decoder and router are negative controls (co=none)"
    if strat == "SurgeThenDrain":
        out += "; order waived — each deletion is a drain with no preceding surge"
    if kind == "evict":
        out += REPEAT_EVICT_CLAIM
    return out


def _repeat_scale_reason(kind, shape, rp, member, n):
    _label, topo, size = shape[0], shape[1], shape[2]
    return (f"the engine grows from {size} to {size + 1} while {_repeat_what(kind, member)} again each time its Instance "
            f"is back and serving, {n} times, then left alone: the new Instance comes up and the scale lands at {size + 1} "
            f"with every Instance Ready; across the window the other Instances keep serving (floor N-1 on the engine, N "
            f"elsewhere), " + REPEAT_WINDOW_CLAIMS.format(plus=" and the scale's new Instance") + "; " + REPEAT_END_CLAIM
            + (f"; {_recreate_scope(topo, rp)} each time under restart policy {RP_FULL[rp]}")
            + (REPEAT_EVICT_CLAIM if kind == "evict" else ""))


def _repeat_ctrl_reason(op, shape, strat, n):
    topo, size = shape[1], shape[2]
    resumes = ("a restarted manager carries nothing in memory and resumes from status, so nothing is created twice "
               "(peaks within the ceiling, pods within what the operation explains, no index collision) and nothing "
               "is built on a third revision")
    if op == "steady":
        out = (f"the manager is deleted again each time it is Ready, {n} times, on the settled service: no pod of the "
               f"service is created or deleted, every Instance stays Ready and serving and no repair is recorded; "
               f"{resumes}; after the last restart every row is Ready with no operation open and no hold")
    elif op == "push":
        out = (f"the manager is deleted right after the bump and again each time it is Ready, {n} times, while the roll "
               f"lands under {strat}: the roll resumes from status after every restart and converges with every Instance "
               f"on the new revision; {resumes}; after the last restart every row is Ready with no operation open and no hold")
        if is_pd(topo):
            out += "; decoder and router are negative controls (co=none)"
    else:
        out = (f"the engine grows from {size} to {size + 1} while the manager is deleted right after the scale and again "
               f"each time it is Ready, {n} times: the scale resumes from status and lands at {size + 1}, the survivors "
               f"keep their pods; {resumes}; after the last restart every row is Ready with no operation open and no hold")
    return out


def emit_repeat_ctrl_ops(add):
    """The manager deleted again each time it is Ready under a rollback, a
    mid-roll edit, a runtime edit, a scale-down, a move, a delete and a
    canary, minted after the repeating family's first pass."""
    n = REPEAT_N
    for shape in REPEAT_SHAPES:
        label, topo, size, w, router = shape
        nr = lib.NR if router else 0
        tag = _repeat_shape_tag(shape)
        for op in REPEAT_CTRL_OPS:
            strat, surge, unavail = _op_strategy(op, False)
            hook = CTRL_HOOK_AT.get(op, HOOK_PHASE)
            life = lib.lifecycle(strat=strat, surge=surge, unavail=unavail)
            coord = None
            if op == "move":
                # The replacement surges at a new index whatever the strategy.
                kv = dict(mode="migrate", **counts(topo, size, nr), **ceilings(topo, size, "SurgeThenDrain", 1, nr),
                          mig="Auto", inst=0)
                case_comps = comps(topo, size, w, life, nr=nr)
            elif op == "canary":
                # Decoder and router are group members on their default surge lifecycle.
                kv = dict(mode="canary", **counts(topo, size, nr), **ceilings(topo, size, "SurgeThenDrain", 1, nr),
                          canary="auto")
                if is_pd(topo):
                    kv.update(ceil_d=size + 1, ceil_r=lib.NR + 1)
                elif nr:
                    kv.update(ceil_r=nr + 1)
                case_comps = comps(topo, size, w, life, nr=nr)
                coord = lib.canary_block(lib.canary_steps("auto", *CTRL_CANARY_STEPS),
                                         components=[c["comp"] for c in case_comps])
            else:
                kv = _op_kv(op, topo, size, nr, False)
                case_comps = _op_comps(op, topo, size, w, nr, False)
            kv.update(fault=f"ctrlrestart:{hook}", co="none", topo=topo, repeat=n)
            rolls = op in CTRL_HOOK_ROLLS or op == "canary"
            desc = (f"{GEN_MARK} repeat ctrlrestart {op} {tag} N={size}"
                    + (f" {strat} {budget_tag(strat, surge, unavail)}" if op in CTRL_HOOK_ROLLS else "") + f" x{n}")
            add("gen-repeat", desc, kv, case_comps, _ctrl_hook_reason(op, topo, w, nr, n=n),
                dict(strat=strat if rolls else "(dormant: nothing rolls)", verb=f"repeat:ctrlrestart:{op}",
                     loop=f"ctrlrestart x{n} under {CTRL_HOOK_WORDS[op]}", op=op, hook=hook),
                cell=("repeat", "ctrlrestart", op, label, strat if op in CTRL_HOOK_ROLLS else None, None, None),
                coord=coord, dedicated=(op == "runtime"))


def emit_repeat(add):
    n = REPEAT_N
    for shape in REPEAT_SHAPES:
        label, topo, size, w, _router = shape
        members = GANG_MEMBERS if is_mn(topo) else [None]
        tag = _repeat_shape_tag(shape)
        victim_kv = dict(victimcomp="engine") if is_pd(topo) else {}
        for rp in RESTARTS:
            e_life = lib.lifecycle(restart=RP_FULL[rp])
            d_life = lib.lifecycle(restart=RP_FULL[rp]) if is_pd(topo) else None
            steady_comps = _repeat_comps(shape, e_life, d_life)
            for member in members:
                # Steady state: deleted by hand, or evicted, again each time it is back.
                for kind, via in (("podkill", "delete"), ("evict", "evict")):
                    kv = dict(mode="kill", **_repeat_counts(shape), **_repeat_ceilings(shape, "SurgeThenDrain", 0),
                              restart=rp, co="none", topo=topo, repeat=n)
                    if member:
                        kv["kill"] = member
                    if via != "delete":
                        kv["via"] = via
                    desc = (f"{GEN_MARK} repeat {kind} steady {tag} N={size} rp {rp} x{n}"
                            + (f" target {member}" if member else ""))
                    add("gen-repeat", desc, kv, steady_comps, _repeat_steady_reason(kind, shape, rp, member, n),
                        dict(strat="SurgeThenDrain", verb=f"repeat:{kind}:steady", rp=rp, loop=f"{kind} x{n} at steady state"),
                        cell=("repeat", kind, "steady", label, None, rp, member))
                # A scale-up by one while the pod keeps being deleted (the eviction loop runs under policy None).
                for kind in ("podkill", "evict"):
                    if kind == "evict" and (rp != "None" or member == "leader"):
                        continue
                    kv = dict(mode="scaledown", **_repeat_counts(shape), **_repeat_ceilings(shape, "SurgeThenDrain", 0, to_e=size + 1),
                              fault=f"{kind}:{REPEAT_SCALE_HOOK}", co="none", topo=topo, to_e=size + 1, repeat=n, floor_slack=1,
                              **victim_kv)
                    if member and kind == "podkill":
                        kv["kill"] = member
                    desc = (f"{GEN_MARK} repeat {kind} scale-up {tag} {size}->{size + 1} rp {rp} x{n}"
                            + (f" target {member}" if member and kind == "podkill" else ""))
                    add("gen-repeat", desc, kv, steady_comps, _repeat_scale_reason(kind, shape, rp, member if kind == "podkill" else None, n),
                        dict(strat="SurgeThenDrain", verb=f"repeat:{kind}:scale-up", rp=rp, loop=f"{kind} x{n} under a scale-up"),
                        cell=("repeat", kind, "scale-up", label, None, rp, member if kind == "podkill" else None))
            # A push while the pod keeps being deleted (a gang's member under the
            # rebuild-whole policy only) or evicted (under policy None only).
            for strat, surge, unavail in REPEAT_STRATS:
                roll_life = lib.lifecycle(strat=strat, surge=surge, unavail=unavail, restart=RP_FULL[rp])
                roll_comps = _repeat_comps(shape, roll_life, d_life)
                for kind in ("podkill", "evict"):
                    if kind == "evict" and rp != "None":
                        continue
                    if kind == "podkill" and is_mn(topo) and rp != "RecreateInstance":
                        continue
                    for member in (members if kind == "podkill" else [None]):
                        kv = dict(**_repeat_counts(shape), **_repeat_ceilings(shape, strat, surge),
                                  fault=f"{kind}:{REPEAT_PUSH_HOOK}", co="none", topo=topo, victim="stable", repeat=n,
                                  floor_slack=1, **victim_kv)
                        if member:
                            kv["kill"] = member
                        if strat == "SurgeThenDrain":
                            kv["order_soft"] = 1
                        desc = (f"{GEN_MARK} repeat {kind} push {tag} N={size} {strat} {budget_tag(strat, surge, unavail)} rp {rp} x{n}"
                                + (f" target {member}" if member else ""))
                        add("gen-repeat", desc, kv, roll_comps, _repeat_push_reason(kind, shape, strat, rp, member, n),
                            dict(strat=strat, verb=f"repeat:{kind}:push", rp=rp, loop=f"{kind} x{n} under a push"),
                            cell=("repeat", kind, "push", label, strat, rp, member))
        # The manager restarting again and again: on the idle service, under a push, under a scale-up.
        life = lib.lifecycle()
        kv = dict(**_repeat_counts(shape), **_repeat_ceilings(shape, "SurgeThenDrain", 0),
                  fault=f"ctrlrestart:{REPEAT_IDLE_HOOK}", co="none", topo=topo, during=REPEAT_IDLE_HOOK, repeat=n)
        add("gen-repeat", f"{GEN_MARK} repeat ctrlrestart steady {tag} N={size} x{n}", kv, _repeat_comps(shape, life),
            _repeat_ctrl_reason("steady", shape, None, n),
            dict(strat="SurgeThenDrain", verb="repeat:ctrlrestart:steady", loop=f"ctrlrestart x{n} at steady state"),
            cell=("repeat", "ctrlrestart", "steady", label, None, None, None))
        for strat, surge, unavail in REPEAT_STRATS:
            roll_life = lib.lifecycle(strat=strat, surge=surge, unavail=unavail)
            kv = dict(**_repeat_counts(shape), **_repeat_ceilings(shape, strat, surge),
                      fault=f"ctrlrestart:{REPEAT_SCALE_HOOK}", co="none", topo=topo, repeat=n)
            add("gen-repeat", f"{GEN_MARK} repeat ctrlrestart push {tag} N={size} {strat} {budget_tag(strat, surge, unavail)} x{n}",
                kv, _repeat_comps(shape, roll_life), _repeat_ctrl_reason("push", shape, strat, n),
                dict(strat=strat, verb="repeat:ctrlrestart:push", loop=f"ctrlrestart x{n} under a push"),
                cell=("repeat", "ctrlrestart", "push", label, strat, None, None))
        kv = dict(mode="scaledown", **_repeat_counts(shape), **_repeat_ceilings(shape, "SurgeThenDrain", 0, to_e=size + 1),
                  fault=f"ctrlrestart:{REPEAT_SCALE_HOOK}", co="none", topo=topo, to_e=size + 1, repeat=n)
        add("gen-repeat", f"{GEN_MARK} repeat ctrlrestart scale-up {tag} {size}->{size + 1} x{n}", kv, _repeat_comps(shape, life),
            _repeat_ctrl_reason("scale-up", shape, None, n),
            dict(strat="SurgeThenDrain", verb="repeat:ctrlrestart:scale-up", loop=f"ctrlrestart x{n} under a scale-up"),
            cell=("repeat", "ctrlrestart", "scale-up", label, None, None, None))
    # The scheduler taking the pod again each time it lands: the preempt
    # shapes, at steady state, the fillers dropped once the wait has held.
    for topo, size, w in REPEAT_PREEMPT_SHAPES:
        shape = (shape_tag(topo, w), topo, size, w, False)
        for rp in RESTARTS:
            d_life = lib.lifecycle(restart=RP_FULL[rp]) if is_pd(topo) else None
            kv = dict(mode="kill", **_repeat_counts(shape), **_repeat_ceilings(shape, "SurgeThenDrain", 0), restart=rp,
                      co="none", topo=topo, via="preempt", lift="after-settle", repeat=REPEAT_PREEMPT_N)
            add("gen-repeat", f"{GEN_MARK} repeat preempt steady {shape_tag(topo, w)} N={size} rp {rp} x{REPEAT_PREEMPT_N}", kv,
                _repeat_comps(shape, lib.lifecycle(restart=RP_FULL[rp]), d_life), _repeat_preempt_reason(shape, rp, REPEAT_PREEMPT_N),
                dict(strat="SurgeThenDrain", verb="repeat:preempt:steady", rp=rp, lift="after-settle",
                     loop=f"preempt x{REPEAT_PREEMPT_N} at steady state"),
                cell=("repeat", "preempt", "steady", shape[0], None, rp, None))


# ── gen-pending: a new pod cannot be placed or admitted ─────────────────────
def _pending_quota(moment, strat, w):
    """The quota headroom that refuses exactly the pod the operation creates:
    a surge roll creates before anything is gone (room for all but one of the
    pods it adds); a repair and a drain-first roll create after a pod is
    gone (one pod fewer than run)."""
    if moment == "roll" and strat == "SurgeThenDrain":
        return w   # a gang surge adds 1 + w pods; a single-pod surge adds one
    return -1


def _pending_cause(cause, topo, moment, strat, rp):
    """The cause in the operator's words. On a gang the quota is what keeps
    the gang from forming, except under a repair with restart policy None,
    where only the one deleted pod is recreated and refused."""
    if cause == "taint":
        return "a NoSchedule taint on every schedulable node"
    if not is_mn(topo):
        return "a quota with no room for the one pod"
    if moment == "roll" and strat == "SurgeThenDrain":
        return "a quota with room for all but one member of the surge gang (the gang cannot form)"
    if moment == "roll":
        return "a quota one pod short of the recreated gang (all members but one admitted, the gang cannot form)"
    if rp == "RecreateInstance":
        return "a quota one pod short of the rebuilt gang (all members but one admitted, the gang cannot form)"
    return "a quota one pod short (the leader's replacement alone is refused)"


def _pending_reason(cause, moment, topo, rp, lift, strat, w, nr=0):
    token = PENDING_TOKEN[cause]
    what = _pending_cause(cause, topo, moment, strat, rp)
    how = "placed" if cause == "taint" else "admitted"
    if moment == "steady":
        pod = ("the leader's replacement" if is_mn(topo) and rp == "None"
               else "the rebuilt gang" if is_mn(topo) else "the replacement pod")
        out = (f"{what} is in place when one engine pod is deleted; {pod} cannot be {how}, so the Instance waits with "
               f"reason {token} inside its unschedulable grace, nothing is taken down to make room, every other "
               f"Instance keeps serving and no row reads Failed")
        if lift == "after-settle":
            out += f"; once the cause is lifted the repair completes: {_recreate_scope(topo, rp)} and the fleet is back at N serving"
        else:
            out += "; the cause stays to the end, so the wait holds with its reason across the hold window"
        return out + ("; the router keeps its pods" if nr and not is_pd(topo) else "")
    pod = "the surge" if strat == "SurgeThenDrain" else "the drained Instance's replacement"
    out = (f"{what} is in place when the bump lands under {strat}; {pod} cannot be {how}, so the roll waits with reason "
           f"{token} inside its unschedulable grace, the capacity floor (N minus maxUnavailable) holds and nothing settled "
           f"is taken down to make room")
    if lift == "after-settle":
        out += "; once the cause is lifted the roll converges with every Instance on the new revision"
    else:
        out += "; the cause stays to the end, so the roll holds with its reason across the hold window and no Instance reads Failed"
    if is_pd(topo):
        out += "; decoder and router are negative controls (co=none)"
    return out + _router_control(topo, nr)


def emit_pending(add):
    for topo, n, w, nr in DISRUPT_SHAPES:
        for rp in RESTARTS:
            d_life = lib.lifecycle(restart=RP_FULL[rp]) if is_pd(topo) else None
            for cause in PENDING_CAUSES:
                for lift in PENDING_LIFTS:
                    fault = "taint:all" if cause == "taint" else f"quota:{_pending_quota('steady', None, w)}"
                    kv = dict(mode="kill", **_kill_counts(topo, n, nr), restart=rp)
                    if is_mn(topo):
                        kv["kill"] = "leader"
                    kv.update(fault=fault, co="none", topo=topo, lift=lift)
                    meta = dict(strat="SurgeThenDrain", verb=f"pending:{cause}:steady", rp=rp, lift=lift)
                    desc = f"{GEN_MARK} pending {cause} steady {shape_tag(topo, w, nr)} N={n} rp {rp} lift {lift}"
                    add("gen-pending", desc, kv, comps(topo, n, w, lib.lifecycle(restart=RP_FULL[rp]), d_life=d_life, nr=nr),
                        _pending_reason(cause, "steady", topo, rp, lift, None, w, nr), meta,
                        cell=("pending", cause, "steady", topo, w, rp, lift, nr))
                    for strat, surge, unavail in DISRUPT_STRATS:
                        if cause == "quota" and lift == "never" and is_mn(topo) and strat == "SurgeThenDrain":
                            continue   # constraint ledger: the tight-cluster family's cell
                        fault = "taint:all" if cause == "taint" else f"quota:{_pending_quota('roll', strat, w)}"
                        kv = dict(**counts(topo, n, nr), **ceilings(topo, n, strat, surge, nr), fault=fault,
                                  co="none", topo=topo, lift=lift)
                        meta = dict(strat=strat, verb=f"pending:{cause}:roll", rp=rp, lift=lift)
                        desc = (f"{GEN_MARK} pending {cause} roll {shape_tag(topo, w, nr)} N={n} {strat} "
                                f"{budget_tag(strat, surge, unavail)} rp {rp} lift {lift}")
                        roll_life = lib.lifecycle(strat=strat, surge=surge, unavail=unavail, restart=RP_FULL[rp])
                        add("gen-pending", desc, kv, comps(topo, n, w, roll_life, d_life=d_life, nr=nr),
                            _pending_reason(cause, "roll", topo, rp, lift, strat, w, nr), meta,
                            cell=("pending", cause, "roll", topo, w, strat, rp, lift, nr))
    _emit_pending_ops(add)
    _emit_pending_canary(add)


# ── a quota under a canary ───────────────────────────────────────────────────
# A quota with room for no further pod is in place when the canary push
# lands: the first step's canary pod cannot be admitted, so the capacity
# step parks the ladder Failed at its ready timeout with the wait still
# reported (kept), or the quota is lifted once the ladder has parked, the
# capacity the step asked for lands, the park holds anyway and a resume
# walks the ladder to Stable (lifted). The engine unit's ladder on the
# gate family's shapes the hand-written canary cases do not run; minted
# after the family's other rows.
PENDING_CANARY_KEPT_SKIP = ("engine single", "engine+decoder+router single")
PENDING_CANARY_LIFTED_SKIP = ("engine single",)
PENDING_CANARY_KEPT_CELL = "canary/Staging/readyTimeout[quota]"
PENDING_CANARY_LIFTED_CELL = "canary/Failed/capacityReached,canary/Failed/resume[afterCapacity]"


def _pending_canary_quota_reason(lifted, family, n, w):
    shape = _gate_shape_words(family, n, w)
    out = (f"a quota with room for no further pod is in place when the canary push lands on {shape}, so the first "
           f"step's canary pod{' (a gang that cannot form)' if w else ''} cannot be admitted; the capacity step waits "
           f"with reason QuotaExceeded on its row, nothing settled is taken down to make room, the stable revision keeps "
           f"serving every Instance and no traffic names the canary")
    if not lifted:
        return out + ("; the quota stays, so the step parks the ladder Failed at its ready timeout with the wait still "
                      "reported and nothing rolls back")
    return out + ("; the step parks the ladder Failed at its ready timeout, then the quota is lifted: the capacity the "
                  "parked step asked for lands and the park holds anyway (still Failed at the same step with the capacity "
                  "there, since capacity that arrives after the deadline does not un-park a canary), and a resume "
                  "re-enters the ladder at its first step and walks every step to Stable with every Instance on the "
                  "canary revision serving")


def _emit_pending_canary(add):
    for family, topo, n, w, nr in GATE_SHAPES:
        shape = _canary_shape(topo, w, nr)
        group = gate_group(topo)
        for lifted, skip in ((False, PENDING_CANARY_KEPT_SKIP), (True, PENDING_CANARY_LIFTED_SKIP)):
            if family in skip:
                continue
            kv = gate_kv(topo, n, nr)
            kv.update(canary="auto", fault="quota:0", co="none", topo=topo)
            if lifted:
                kv.update(terminal="Stable", during="canary-failed", verb="quotalift", verb2="resume",
                          during2="canary-failed", cell=PENDING_CANARY_LIFTED_CELL)
            else:
                kv.update(terminal="Failed", cell=PENDING_CANARY_KEPT_CELL)
            label = "lifted" if lifted else "kept"
            desc = f"{GEN_MARK} pending quota canary {label} {shape} N={n}"
            meta = dict(strat="SurgeThenDrain", verb=f"pending:quota:canary:{label}", faultop="quota canary", op="canary",
                        rp=GATE_RP, lift=("quotalift" if lifted else "never"), shape=family)
            add("gen-pending", desc, kv, gate_comps(topo, n, w, nr, GATE_RP), _pending_canary_quota_reason(lifted, family, n, w),
                meta, cell=("pending-canary", label, topo, nr),
                coord=lib.canary_block(story_ladder("auto"), components=group, ready_timeout=HELD_READY_TIMEOUT))


# ── The pending causes and the pod faults under the remaining operations ────
# A pending cause armed before a rollback, a strategy edit and a runtime
# edit, and an eviction or a preemption at a hook of those and of a scale and
# a delete, one case per ledger cell on the six shape families. The rollback
# under a pre-armed cause runs drain-first and fires at its draining hook:
# the roll's first replacement is what waits, and a roll that cannot place
# its first pod reaches no later hook; the surge roll under the same cause
# never drains, so it has no rollback moment the cause can hold. The edit
# lands as the push does and switches the surge strategy to the drain-first
# one under the budget valid for both. The drain-first replacement is
# created after a pod is gone, so quota:-1 refuses it; the runtime edit
# surges, so its quota is the surge's. The hook faults land as the operation
# lands, the rollback and the edit having fired at the first new pod.
PENDING_OPS = ["rollback", "edit", "runtime"]
PENDING_ROLLBACK = ("RecreatePod", "draining")
PENDING_EDIT = ("SurgeThenDrain", "RecreatePod", "settling")
HOOK_OPS = ["rollback", "edit", "runtime", "scaledown", "scaleup", "delete"]
HOOK_PHASE = "settling"
HOOK_ROLLBACK = ("SurgeThenDrain", "rotating")
HOOK_EDIT = ("SurgeThenDrain", "RecreatePod", "rotating")
HOOK_STRAT = ("SurgeThenDrain", 1, 0)
# The persistent form of the eviction: applied again once what it took has
# come back. A scale-up's cell exists in the repeating family; a delete
# brings nothing back the eviction could take again.
HOOK_REPEAT_N = 2
HOOK_EVICT_REPEATS = ["rollback", "edit", "runtime", "scaledown"]
# The preemption lands at the hooks of the operations that place a pod the
# press can hold, or that keep settled Instances whose repair it holds (a
# rollback at its first surge pod withdraws that pod, a scale-down takes an
# Instance: the press aims at one each keeps); a delete places nothing
# until its recreate.
HOOK_PREEMPT_OPS = ["rollback", "edit", "runtime", "scaledown", "scaleup"]
OP_WORDS = {"rollback": "the rollback", "edit": "the roll under its edit", "runtime": "the runtime edit",
            "scaledown": "the scale-down", "scaleup": "the scale-up", "delete": "the delete"}


def _op_strategy(op, pending):
    """(strategy, maxSurge, maxUnavailable) of the roll an operation runs, or
    the dormant canonical budget where nothing rolls."""
    if op == "rollback":
        return (PENDING_ROLLBACK[0], 0, 1) if pending else HOOK_STRAT
    if op == "edit":
        start, end = (PENDING_EDIT if pending else HOOK_EDIT)[:2]
        return (start,) + switch_budget(start, end)
    return HOOK_STRAT


def _op_kv(op, topo, n, nr, pending):
    """The header of an operation case on a disruption shape: its mode and
    knobs, counts and ceilings (every Component rolls on a runtime edit,
    the decoder and the router under their default surge of one)."""
    strat, surge, unavail = _op_strategy(op, pending)
    routers = router_count(topo, nr)
    kv = dict(**counts(topo, n, nr), **ceilings(topo, n, strat, surge, nr))
    if op == "rollback":
        kv.update(rollback=1, during=(PENDING_ROLLBACK if pending else HOOK_ROLLBACK)[1])
    elif op == "edit":
        edit = PENDING_EDIT if pending else HOOK_EDIT
        kv.update(edit=f"strategy:{edit[1]}", during=edit[2])
    elif op == "runtime":
        kv.update(mode="runtimerev", edit="env", expect="roll",
                  ceil_d=(n + 1 if is_pd(topo) else 0), ceil_r=(routers + 1 if routers else 0))
    elif op == "scaledown":
        kv.update(mode="scaledown", to_e=n - 1, ceil_e=n)
    elif op == "scaleup":
        kv.update(mode="scaledown", to_e=n + 1, ceil_e=n + 1)
    elif op == "delete":
        kv.update(mode="recreate", ceil_e=n)
    return kv


def _op_comps(op, topo, n, w, nr, pending):
    strat, surge, unavail = _op_strategy(op, pending)
    return comps(topo, n, w, lib.lifecycle(strat=strat, surge=surge, unavail=unavail), nr=nr)


def _op_does(op, pending, topo):
    """What the operator does, in the reason's words."""
    if op == "rollback":
        if pending:
            return ("the engine is bumped under the drain-first strategy and the starting revision re-applied as its "
                    "first pod goes")
        return "the engine is bumped and the starting revision re-applied at the first pod of the new revision"
    if op == "edit":
        when = "as the push lands" if pending else "at the first pod of the new revision"
        return f"the engine is bumped under SurgeThenDrain and its strategy edited to RecreatePod {when}"
    if op == "runtime":
        return ("the dedicated runtime every Component inherits gets a new runner env, so every Component rolls onto "
                "its own new revision")
    return {"scaledown": "the engine is scaled from N to N-1", "scaleup": "the engine is scaled from N to N+1",
            "delete": "the service is deleted"}[op]


def _op_end(op):
    """What the operation owes once it is free to complete."""
    return {"rollback": ("the rollback completes with every Instance back on the starting revision and serving, no pod "
                         "of the new revision left"),
            "edit": "the roll completes under the edited strategy with every Instance on the new revision and the edit on the live spec",
            "runtime": "the edit lands on every Instance of every Component, each on its own new revision and serving",
            "scaledown": "the scale completes with the engine at N-1 and the survivors on their own pods",
            "scaleup": "the scale completes with the new Instance whole and serving and the survivors on their own pods",
            "delete": ("the delete completes with nothing of the service left, and the same manifest re-applied under "
                       "the same name comes up whole on fresh pods")}[op]


def _op_controls(topo, nr, op):
    if op == "runtime":
        return ""
    if is_pd(topo):
        return "; decoder and router are negative controls (co=none)"
    if nr:
        return "; the router is a negative control (co=none)"
    return ""


def _pending_op_cause(cause, op, topo):
    if cause == "taint":
        return "a NoSchedule taint on every schedulable node"
    if op == "runtime":
        return ("a quota with room for all but one member of a surge gang (no gang can form)" if is_mn(topo)
                else "a quota with no room for a single surge pod")
    return ("a quota one pod short of the recreated gang (all members but one admitted, the gang cannot form)"
            if is_mn(topo) else "a quota with no room for the one replacement pod")


def _pending_op_reason(op, cause, lift, topo, w, nr):
    token, how = PENDING_TOKEN[cause], ("placed" if cause == "taint" else "admitted")
    waits = {"rollback": ("the drained Instance's replacement cannot be {how}, so the roll waits with reason {token}; "
                          "the rollback then owes that Instance on the starting revision and its pod cannot be {how} "
                          "either, so the rollback waits with the same reason"),
             "edit": ("the surge cannot be {how}, and once the edit switches the roll to the drain-first strategy the "
                      "drained Instance's replacement cannot be {how} either, so the roll waits with reason {token} "
                      "under the edited strategy"),
             "runtime": "the first pod of each Component's roll cannot be {how}, so the roll waits with reason {token}"}
    floor = "N" if op == "runtime" else "N minus the drained Instance"
    out = (f"{_pending_op_cause(cause, op, topo)} is armed on the settled service ({shape_words(topo, w, nr)}), then "
           f"{_op_does(op, True, topo)}: {waits[op].format(how=how, token=token)} inside its unschedulable grace, the "
           f"capacity floor ({floor}) holds, nothing settled is taken down to make room and no row reads Failed")
    if lift == "after-settle":
        out += f"; once the cause is lifted {_op_end(op)}"
    else:
        out += "; the cause stays to the end, so the wait holds with its reason across the hold window"
    if is_mn(topo):
        out += "; each Instance is a gang, placed whole"
    return out + _op_controls(topo, nr, op)


def _evict_op_reason(op, form, topo, w, nr):
    victim = ("a settled pod of the starting revision" if op in ("rollback", "edit") else
              "a settled engine pod" if op in ("scaledown", "scaleup", "runtime") else "one of its pods")
    at = {"rollback": "as the rollback lands", "edit": "as the edit lands", "runtime": "as the edit lands",
          "scaledown": "as the scale lands", "scaleup": "as the scale lands", "delete": "as the delete lands"}[op]
    out = (f"{_op_does(op, False, topo)} on {shape_words(topo, w, nr)}, and {at} {victim} is evicted through the "
           f"Eviction API, as a node drain would, asked again while the disruption budget refuses it")
    if form == "persistent":
        out += (f", and again once what it took has come back, {HOOK_REPEAT_N} times in all; while an Instance is short "
                f"the status says so and never counts it whole, every repair is for an Instance the eviction took, and "
                f"the controller creates no more pods than the operation and the repairs explain")
    if op == "delete":
        out += "; the eviction is one more pod on its way out: every pod goes within the teardown budget, " + _op_end(op)
    else:
        out += (f"; the controller replaces what it took, the floor lowered by that one Instance holds, and "
                f"{_op_end(op)}, with every row settled and no hold left behind")
    if is_mn(topo):
        out += "; each Instance is a gang, rebuilt whole"
    return out + _op_controls(topo, nr, op)


def _preempt_op_reason(op, topo, w, nr):
    at = {"rollback": "as the rollback lands", "edit": "as the edit lands", "runtime": "as the edit lands",
          "scaledown": "once the scale has retired the Instance it takes", "scaleup": "as the scale lands"}[op]
    # A rollback at its first surge pod withdraws that pod and a scale-down
    # takes an Instance (a non-serving one first, whatever its index):
    # neither places a pod, so the press aims at a settled Instance the
    # operation keeps and holds its repair; a surge roll keeps only its
    # replacements, so the press aims at a bound one.
    surge = "the node of a bound pod of the new revision, which the roll keeps (the source it drains is no target)"
    aim = {"rollback": "the node of a settled pod of the starting revision, which the rollback keeps (the surge pod "
                       "it withdraws is no target)",
           "scaledown": "the node of a pod of the highest Instance the scale keeps (the Instance it takes is no "
                        "target)",
           "edit": surge, "runtime": surge}.get(op, "the node of the aimed pod")
    out = (f"{_op_does(op, False, topo)} on {shape_words(topo, w, nr)}, and {at} fillers take the spare CPU of every "
           f"node, the pending taint closes every node to the case, and a higher-priority filler presses on {aim}, "
           f"so the scheduler preempts one pod of the case there; the Instance waits with reason "
           f"Unschedulable, nothing settled is taken down to make room and no row reads Failed; once the lift opens the "
           f"nodes and drops the fillers {_op_end(op)}; floor and order waived, since the preemption itself takes a pod "
           f"the budget cannot absorb")
    if is_mn(topo):
        out += "; each Instance is a gang, placed whole"
    return out + _op_controls(topo, nr, op)


def _emit_pending_ops(add):
    """A pending cause armed before a rollback, a strategy edit and a runtime
    edit; appended after the first pass so the committed ids keep their
    places."""
    for topo, n, w, nr in DISRUPT_SHAPES:
        for op in PENDING_OPS:
            strat, surge, unavail = _op_strategy(op, True)
            for cause in PENDING_CAUSES:
                for lift in PENDING_LIFTS:
                    if cause == "taint":
                        fault = "taint:all"
                    else:
                        fault = f"quota:{_pending_quota('roll', strat, w) if op == 'runtime' else -1}"
                    kv = dict(_op_kv(op, topo, n, nr, True), fault=fault, co="none", topo=topo, lift=lift)
                    desc = (f"{GEN_MARK} pending {cause} {op} {shape_tag(topo, w, nr)} N={n} {strat} "
                            f"{budget_tag(strat, surge, unavail)} lift {lift}")
                    add("gen-pending", desc, kv, _op_comps(op, topo, n, w, nr, True),
                        _pending_op_reason(op, cause, lift, topo, w, nr),
                        dict(strat=strat, verb=f"pending:{cause}:{op}", lift=lift, faultop=f"pending {op}", op=op),
                        cell=("pending", cause, op, topo, w, nr, lift), dedicated=(op == "runtime"))


def _emit_evict_ops(add):
    """An eviction as a rollback, an edit, a runtime edit, a scale or a
    delete lands, once and (where what it took comes back) again."""
    for topo, n, w, nr in DISRUPT_SHAPES:
        for op in HOOK_OPS:
            strat, surge, unavail = _op_strategy(op, False)
            for form in (["one-shot", "persistent"] if op in HOOK_EVICT_REPEATS else ["one-shot"]):
                kv = dict(_op_kv(op, topo, n, nr, False), fault=f"evict:{HOOK_PHASE}", co="none", topo=topo)
                if op in ("rollback", "edit"):
                    kv["victim"] = "stable"
                if is_pd(topo) and op != "delete":
                    kv["victimcomp"] = "engine"
                if op != "delete":
                    kv["floor_slack"] = 1
                if op in ("rollback", "edit", "runtime"):
                    kv["order_soft"] = 1
                if form == "persistent":
                    kv["repeat"] = HOOK_REPEAT_N
                desc = (f"{GEN_MARK} evict {op} {shape_tag(topo, w, nr)} N={n}"
                        + (f" {strat} {budget_tag(strat, surge, unavail)}" if op in ("rollback", "edit", "runtime") else "")
                        + (f" x{HOOK_REPEAT_N}" if form == "persistent" else ""))
                add("gen-evict", desc, kv, _op_comps(op, topo, n, w, nr, False), _evict_op_reason(op, form, topo, w, nr),
                    dict(strat=strat if op in ("rollback", "edit", "runtime") else "(dormant: nothing rolls)",
                         verb=f"evict:{op}", faultop=f"evict {op}", op=op, form=form),
                    cell=("evict", op, form, topo, w, nr), dedicated=(op == "runtime"))


def _emit_preempt_ops(add):
    """A preemption as a rollback, an edit, a runtime edit or a scale lands,
    the nodes opened again once the wait has held. The rollback's press is
    aimed at a settled pod of the starting revision: the surge pod the
    rollback withdraws is the one pod it takes away. The edit's and the
    runtime edit's press is aimed at a bound replacement of the new revision
    (victim=canary): the pods a surge roll keeps, while it drains every
    source. The scale-down's press lands once the scale has retired the
    Instance it takes."""
    for topo, n, w, nr in DISRUPT_SHAPES:
        for op in HOOK_PREEMPT_OPS:
            strat, surge, unavail = _op_strategy(op, False)
            kv = dict(_op_kv(op, topo, n, nr, False), fault=f"preempt:{HOOK_PHASE}", floor_soft=1, order_soft=1,
                      co="none", topo=topo, lift="after-settle")
            if op == "rollback":
                kv["victim"] = "stable"
            elif op in ("edit", "runtime"):
                kv["victim"] = "canary"
            if is_pd(topo):
                kv["victimcomp"] = "engine"
            desc = (f"{GEN_MARK} preempt {op} {shape_tag(topo, w, nr)} N={n}"
                    + (f" {strat} {budget_tag(strat, surge, unavail)}" if op in ("rollback", "edit", "runtime") else "")
                    + " lift after-settle")
            add("gen-preempt", desc, kv, _op_comps(op, topo, n, w, nr, False), _preempt_op_reason(op, topo, w, nr),
                dict(strat=strat if op in ("rollback", "edit", "runtime") else "(dormant: nothing rolls)",
                     verb=f"preempt:{op}", faultop=f"preempt {op}", op=op, lift="after-settle"),
                cell=("preempt", op, topo, w, nr), dedicated=(op == "runtime"))


# ── a pod deleted by hand at the remaining moments ──────────────────────────
# A serving pod of the running revision (a gang's member under kill=) is
# deleted by hand as a rollback or a mid-roll edit lands, at the first pod
# of a runtime edit's new revision, as a scale up or down lands and as a
# delete lands. The one-shot form on the single-pod shapes lives with
# gen-fault, on the gangs with gen-gang-member; the persistent form
# (repeat=) with gen-repeat. The controller replaces what the deletion took,
# labelled as what it runs, and the operation still completes; a delete
# brings nothing back, so it has no persistent form. The gang rows name the
# restart policy the verdict reads: the one-shot loss under None (the lost
# member alone comes back), the repeating loss under the rebuild-whole
# policy, as the repeating family's push rows do.
PODKILL_HOOK_OPS = ["rollback", "edit", "runtime", "scaleup", "scaledown", "delete"]
PODKILL_HOOK_AT = {"runtime": "rotating"}
PODKILL_HOOK_ROLLS = {"rollback", "edit", "runtime"}
PODKILL_REPEAT_OPS = ["rollback", "edit", "runtime", "scaledown"]
PODKILL_HOOK_MEMBER = {"one-shot": ("leader", "None"), "persistent": ("worker", "RecreateInstance")}
PODKILL_HOOK_FOLDERS = {"one-shot": {False: "gen-fault", True: "gen-gang-member"},
                        "persistent": {False: "gen-repeat", True: "gen-repeat"}}


def _podkill_hook_kv(op, topo, n, nr, form, member, rp, hook):
    kv = dict(_op_kv(op, topo, n, nr, False), fault=f"podkill:{hook}", co="none", topo=topo)
    if op in PODKILL_HOOK_ROLLS:
        kv.update(victim="stable", order_soft=1)
    if op != "delete":
        kv["floor_slack"] = 1
    if is_pd(topo):
        kv["victimcomp"] = "engine"
    if member:
        kv.update(kill=member, restart=rp)
    if form == "persistent":
        kv["repeat"] = REPEAT_N
    return kv


def _podkill_hook_comps(op, topo, n, w, nr, rp):
    """The fleet of a podkill hook case: the operation's own on the
    single-pod shapes, the gangs under the restart policy the row names."""
    if rp is None:
        return _op_comps(op, topo, n, w, nr, False)
    strat, surge, unavail = _op_strategy(op, False)
    life = lib.lifecycle(strat=strat, surge=surge, unavail=unavail, restart=RP_FULL[rp])
    return comps(topo, n, w, life, d_life=(lib.lifecycle(restart=RP_FULL[rp]) if is_pd(topo) else None), nr=nr)


def _podkill_hook_reason(op, form, topo, w, nr, member, rp, n):
    """What the operator does, which pod is deleted by hand and what the
    controller promises; n names the repetitions of the persistent form."""
    at = "at the first pod of the new revision" if op == "runtime" else "as it lands"
    if op == "delete":
        pod = f"the {member} of one engine gang" if member else "one engine pod"
    else:
        pod = f"the {member} of one serving engine gang" if member else "one serving engine pod of the running revision"
    out = f"{_op_does(op, False, topo)} on {shape_words(topo, w, nr)}, and {at} {pod} is deleted by hand"
    if form == "persistent":
        out += f", and again each time its Instance is back and serving, {n} times in all, then left alone"
    if op == "delete":
        out += ("; the deleted pod is one more on its way out: nothing is rebuilt on a service being deleted, every pod "
                f"goes within the teardown budget and {_op_end(op)}")
    else:
        out += "; the controller replaces what the deletion took"
        if form == "one-shot":
            out += (", and the replacement is labelled as what it runs (the image its revision's pods ran when the pod "
                    "was deleted)")
        out += f", the floor lowered by that one Instance holds, and {_op_end(op)}"
        if op in ("scaleup", "scaledown"):
            out += " (the Instance the deletion took rebuilt by design)"
        if form == "persistent":
            out += ("; across the window " + REPEAT_WINDOW_CLAIMS.format(plus=" and the operation's own pods") + "; "
                    + REPEAT_END_CLAIM)
        else:
            out += ", with every row settled and no hold left behind"
        if member:
            if rp == "RecreateInstance":
                out += ("; under restart policy RecreateInstanceOnPodRestart the gang is rebuilt as a whole (a "
                        "RestartTriggered names it) unless the operation takes it first")
            else:
                out += f"; under restart policy None only the lost {member} comes back and no whole-gang repair is recorded"
            out += ", and the gang is never counted as serving while a member is missing"
    if op in PODKILL_HOOK_ROLLS:
        out += "; order waived — each deletion is a drain with no preceding surge"
    return out + _op_controls(topo, nr, op)


def emit_podkill_hooks(add):
    """A pod deleted by hand at the remaining moments, minted after each
    family's first pass: the one-shot rows on the disruption shapes, the
    persistent rows on the repeating family's."""
    persistent_shapes = [(topo, size, w, lib.NR if router else 0) for _label, topo, size, w, router in REPEAT_SHAPES]
    for form, ops, shapes in (("one-shot", PODKILL_HOOK_OPS, DISRUPT_SHAPES),
                              ("persistent", PODKILL_REPEAT_OPS, persistent_shapes)):
        member, rp = PODKILL_HOOK_MEMBER[form]
        for topo, n, w, nr in shapes:
            gang = is_mn(topo)
            for op in ops:
                strat, surge, unavail = _op_strategy(op, False)
                hook = PODKILL_HOOK_AT.get(op, HOOK_PHASE)
                kv = _podkill_hook_kv(op, topo, n, nr, form, member if gang else None, rp, hook)
                desc = (f"{GEN_MARK} {'repeat ' if form == 'persistent' else ''}podkill {op} {shape_tag(topo, w, nr)} N={n}"
                        + (f" {strat} {budget_tag(strat, surge, unavail)}" if op in PODKILL_HOOK_ROLLS else "")
                        + (f" rp {rp} target {member}" if gang else "")
                        + (f" x{REPEAT_N}" if form == "persistent" else ""))
                meta = dict(strat=strat if op in PODKILL_HOOK_ROLLS else "(dormant: nothing rolls)",
                            verb=(f"repeat:podkill:{op}" if form == "persistent" else f"podkill:{op}"),
                            faultop=f"podkill {op}", op=op, hook=hook, form=form, half="hook")
                if gang:
                    meta.update(rp=rp, kill=member)
                if form == "persistent":
                    meta["loop"] = f"podkill x{REPEAT_N} under {OP_WORDS[op]}"
                add(PODKILL_HOOK_FOLDERS[form][gang], desc, kv, _podkill_hook_comps(op, topo, n, w, nr, rp if gang else None),
                    _podkill_hook_reason(op, form, topo, w, nr, member if gang else None, rp, REPEAT_N), meta,
                    cell=("podkill-hook", form, op, topo, w, nr), dedicated=(op == "runtime"))


# ── gen-budget: percent budgets as the engine resolves them ─────────────────
# The engine resolves a percent budget, maxSurge and maxUnavailable alike, as
# N x percent / 100 rounded up and clamped to N (the per-Component budget of
# the escalation package); the Deployment convention of rounding
# maxUnavailable down is not its rule. Each strategy spends one budget:
# SurgeThenDrain its maxSurge above N, RecreatePod its maxUnavailable below
# N. Three percents at two fleet sizes: 10 percent rounds up to 1 at both
# (rounded down it would be 0 and no roll could start), 25 and 50 percent
# scale with N. The gang shapes run at width 1; the prefill/decode gang
# stays at the small fleet (its 34-pod fleet at 8 would cost a lane shard
# per case, and the rounding is a function of N and the percent, which the
# single-pod cells at 8 carry). The crash-loop push runs under restart
# policy None (the roll owns the broken Instance under either policy); the
# canary ladder is the story families' four-step auto ladder.
BUDGET_PERCENTS = ["10%", "25%", "50%"]
BUDGET_AXES = [("SurgeThenDrain", "maxSurge"), ("RecreatePod", "maxUnavailable")]
BUDGET_SHAPES = [("engine", 4, 0), ("engine", 8, 0), ("pd", 4, 0), ("pd", 8, 0),
                 ("engine-mn", 4, 1), ("engine-mn", 8, 1), ("pd-mn", 4, 1)]
BUDGET_CRASH_RP = "None"
BUDGET_LADDER_SHAPES = [("engine", 4, 0), ("engine", 8, 0), ("pd", 4, 0), ("pd", 8, 0), ("engine-mn", 4, 1)]


def _budget_split(strat, pct):
    """(maxSurge, maxUnavailable) with the percent on the budget the strategy spends."""
    return (pct, 0) if strat == "SurgeThenDrain" else (0, pct)


def _budget_rule(n, pct, strat):
    """The rounding rule, with this cell's numbers, for the case's second line."""
    which = "maxSurge" if strat == "SurgeThenDrain" else "maxUnavailable"
    out = (f"the engine resolves a percent budget as N x percent rounded up and clamped to N, so "
           f"{which} {pct} of {n} is {budget_count(n, pct)}")
    if n * int(pct[:-1]) < 100:
        out += " (rounded down it would be 0 and the roll could not start)"
    return out


def _budget_bounds(n, pct, strat, gang):
    k = budget_count(n, pct)
    if strat == "SurgeThenDrain":
        out = f"the surge keeps the peak at or under N+{k} = {n + k} Instances and never drops below N ready"
    else:
        out = f"the drain-first roll keeps at least N-{k} = {n - k} Instances ready and never surges above N"
    if gang:
        out += ", each gang Instance replaced or rebuilt whole"
    return out


def _budget_push_reason(topo, n, pct, strat):
    out = (f"plain bump; {_budget_rule(n, pct, strat)}; {_budget_bounds(n, pct, strat, is_mn(topo))}; "
           f"every Instance ends on the new revision")
    if is_pd(topo):
        out += "; decoder and router are negative controls (co=none)"
    return out


def _budget_crash_reason(topo, n, pct, strat):
    k = budget_count(n, pct)
    out = (f"{_crash_how('start')}; its Instance is off the target from that moment, so the roll owns it: "
           f"{CRASH_LANDS[strat]}, admitted within the same budget as its peers, and no repair is recorded for it "
           f"after the bump; {_budget_rule(n, pct, strat)}; ")
    if strat == "SurgeThenDrain":
        out += f"the peak stays at or under N+{k} = {n + k}"
    else:
        out += f"nothing surges above N and at most {k} Instance(s) are drained at once"
    out += "; every Instance ends on the new revision; floor waived — the crash itself costs settled capacity for the length of the roll"
    if is_pd(topo):
        out += "; decoder and router are negative controls (co=none)"
    return out


def _budget_ladder_reason(topo, n, pct, strat):
    k = budget_count(n, pct)
    steps = "/".join(str(tw) for _nc, tw in STORY_LADDER)
    out = (f"a {len(STORY_LADDER)}-step auto canary ({steps} percent capacity and traffic, timed soaks); "
           f"{_budget_rule(n, pct, strat)}; ")
    if strat == "SurgeThenDrain":
        out += (f"each capacity step builds its Instances at most {k} at a time, so the peak stays at or under "
                f"N+{k} = {n + k} and the fleet never drops below N ready")
    else:
        out += (f"each capacity step rebuilds its Instances drain-first at most {k} at a time, so at least "
                f"N-{k} = {n - k} stay ready and nothing surges above N")
    out += "; the ladder walks every step to Stable with every Instance on the canary revision"
    if is_pd(topo):
        out += "; decoder and router are group members on their canonical surge budget"
    return out


def emit_budget(add):
    for topo, n, w in BUDGET_SHAPES:
        for strat, which in BUDGET_AXES:
            for pct in BUDGET_PERCENTS:
                surge, unavail = _budget_split(strat, pct)
                kv = dict(**counts(topo, n), **ceilings(topo, n, strat, surge), co="none", topo=topo)
                meta = dict(strat=strat, verb="none", budget=f"{which} {pct}")
                desc = f"{GEN_MARK} budget push {shape_tag(topo, w)} N={n} {strat} {budget_tag(strat, surge, unavail)}"
                life = lib.lifecycle(strat=strat, surge=surge, unavail=unavail)
                add("gen-budget", desc, kv, comps(topo, n, w, life), _budget_push_reason(topo, n, pct, strat), meta,
                    cell=("budget", "push", topo, w, n, strat, pct))
    for topo, n, w in BUDGET_SHAPES:
        for strat, which in BUDGET_AXES:
            for pct in BUDGET_PERCENTS:
                surge, unavail = _budget_split(strat, pct)
                kv = dict(**counts(topo, n), **ceilings(topo, n, strat, surge), fault="crashloop:old", form="start",
                          floor_soft=1, co="none", topo=topo)
                meta = dict(strat=strat, verb="crashloop:start", rp=BUDGET_CRASH_RP, victim="engine", budget=f"{which} {pct}")
                desc = (f"{GEN_MARK} budget crashloop start {shape_tag(topo, w)} N={n} {strat} "
                        f"{budget_tag(strat, surge, unavail)} rp {BUDGET_CRASH_RP}")
                life = lib.lifecycle(strat=strat, surge=surge, unavail=unavail, restart=RP_FULL[BUDGET_CRASH_RP])
                d_life = lib.lifecycle(restart=RP_FULL[BUDGET_CRASH_RP]) if is_pd(topo) else None
                add("gen-budget", desc, kv, comps(topo, n, w, life, d_life=d_life),
                    _budget_crash_reason(topo, n, pct, strat), meta,
                    cell=("budget", "crashloop", topo, w, n, strat, pct))
    for topo, n, w in BUDGET_LADDER_SHAPES:
        for strat, which in BUDGET_AXES:
            for pct in BUDGET_PERCENTS:
                surge, unavail = _budget_split(strat, pct)
                kv = dict(mode="canary", **counts(topo, n), **ceilings(topo, n, strat, surge), canary="auto",
                          co="none", topo=topo)
                if is_pd(topo):
                    # Decoder and router are group members on their canonical surge lifecycle.
                    kv.update(ceil_d=n + 1, ceil_r=lib.NR + 1)
                meta = dict(strat=strat, verb="canary", budget=f"{which} {pct}")
                desc = f"{GEN_MARK} budget canary {shape_tag(topo, w)} N={n} {strat} {budget_tag(strat, surge, unavail)}"
                case_comps = comps(topo, n, w, lib.lifecycle(strat=strat, surge=surge, unavail=unavail))
                add("gen-budget", desc, kv, case_comps, _budget_ladder_reason(topo, n, pct, strat), meta,
                    cell=("budget", "canary", topo, w, n, strat, pct),
                    coord=story_group(case_comps, story_ladder("auto")))


# ── gen-inplaceonly: the strict in-place strategy on its own terms ──────────
# InPlaceOnly applies a diff confined to container images and nothing else.
# On a single-pod Instance any other template change is refused: the engine
# records an InPlaceUpdateNotPossible event, stamps nothing, and the push
# holds until a push it can apply lands (expect=held drives the hold, its
# reason and the corrective push). A gang runs every in-place strategy as a
# whole-gang recreate (eligibility reads the leader's spec alone), so the
# strategy refuses nothing on it. A repair is not an update, so a crash loop
# at idle is repaired by a rebuild whatever the strategy; under a push the
# roll owns the broken Instance and a repair yields to it, so the fix lands
# in place (an image change patches the crashing pod itself) or waits for
# the corrective push. Shapes at one fleet size, both restart policies: the
# policy is live under the crash loops and a negative control under the
# pushes (nothing restarts); the budget is the canonical maxUnavailable 1.
IPO_STRAT = "InPlaceOnly"
IPO_SHAPES = [("engine", 4, 0), ("pd", 4, 0), ("engine-mn", 4, 1)]
IPO_TEMPLATE_EDITS = ["env", "resources", "probe"]
IPO_EDIT_WHAT = {"env": "runner env", "resources": "runner CPU request", "probe": "runner readiness period"}
IPO_IDLE_CRASHES = [("start", "None"), ("start", "RecreateInstance"), ("load", "RecreateInstance")]
IPO_GANG_RULE = ("a gang runs every in-place strategy as a whole-gang recreate (eligibility reads the leader's spec "
                 "alone, and a patched leader beside unpatched workers would split the gang), so InPlaceOnly refuses "
                 "nothing on it")
IPO_FLOOR_WAIVED = "floor waived — the crash itself costs settled capacity for the length of the roll"


def _ipo_lifecycles(topo, rp):
    life = lib.lifecycle(strat=IPO_STRAT, surge=0, unavail=1, restart=RP_FULL[rp])
    return life, (lib.lifecycle(restart=RP_FULL[rp]) if is_pd(topo) else None)


def _ipo_controls(topo):
    return "; decoder and router are negative controls (co=none)" if is_pd(topo) else ""


def _ipo_image_reason(topo, rp):
    if is_mn(topo):
        out = (f"the push changes the runner image; {IPO_GANG_RULE}: each gang is rebuilt at the new revision within "
               f"maxUnavailable 1 (floor N-1), never surging above N, UIDs rotate")
    else:
        out = ("the push changes the runner image, the one diff InPlaceOnly patches onto the live pod: every engine pod "
               "is patched in place (UIDs stable) and ends running the new image, one at a time within maxUnavailable 1 "
               "(floor N-1), never surging above N")
    return out + f"; restart policy {RP_FULL[rp]} plays no part, nothing restarts{_ipo_controls(topo)}; every Instance ends on the new revision"


def _ipo_held_reason(topo, edit, rp):
    what = IPO_EDIT_WHAT[edit]
    if is_mn(topo):
        out = (f"the push changes the {what}, a diff beyond container images; {IPO_GANG_RULE}: each gang is rebuilt at "
               f"the new revision within maxUnavailable 1 (floor N-1), never surging above N")
    else:
        out = (f"the push changes the {what}, a diff beyond container images, which InPlaceOnly refuses on a single-pod "
               f"Instance: for the hold window nothing is recreated, patched or surged, every Instance keeps serving on "
               f"the running revision at N, and an InPlaceUpdateNotPossible event says why; then the corrective push "
               f"drops the override and, an annotation-only diff, lands in place on every Instance with UIDs stable")
    return out + f"; restart policy {RP_FULL[rp]} plays no part, nothing restarts{_ipo_controls(topo)}"


def _ipo_idle_reason(topo, form, rp):
    trigger = ("the crash-loop repair (a pod stuck past the grace)" if rp == "None"
               else "the runner-restart repair (RecreateInstanceOnPodRestart)")
    scope = "the whole gang" if is_mn(topo) else "the pod"
    return (f"{_break_how(form, 'engine', 'on the settled service')} under InPlaceOnly; a repair is not an update, so "
            f"the update strategy plays no part and {trigger} rebuilds the Instance in place ({scope}, every pod new, "
            f"Ready and serving on the running revision) within the budget, a RestartTriggered event names it, and "
            f"nothing surges above N")


def _ipo_crash_image_reason(topo, rp):
    if is_mn(topo):
        lands = f"{IPO_GANG_RULE}, so the crashing gang is rebuilt whole at the new revision"
    else:
        lands = ("the diff is images only, so the in-place patch lands the new image on the crashing pod itself, which "
                 "starts and serves (same UID)")
    return (f"{_crash_how('start')}; its Instance is off the target from that moment, so the roll owns it: {lands}, "
            f"admitted within maxUnavailable 1, and no repair is recorded for it after the bump under restart policy "
            f"{RP_FULL[rp]}; every Instance ends on the new revision; {IPO_FLOOR_WAIVED}{_ipo_controls(topo)}")


def _ipo_crash_held_reason(topo, rp):
    if is_mn(topo):
        return (f"{_crash_how('start')} as a push that changes the runner env lands; {IPO_GANG_RULE}: the crashing gang "
                f"is rebuilt whole at the new revision within maxUnavailable 1 with no repair recorded for it after the "
                f"bump under restart policy {RP_FULL[rp]}; every Instance ends on the new revision; {IPO_FLOOR_WAIVED}")
    return (f"{_crash_how('start')} as a push that changes the runner env lands, a diff InPlaceOnly refuses: the roll "
            f"owns the broken Instance from the bump and a repair yields to the roll, so under restart policy "
            f"{RP_FULL[rp]} the Instance stays broken through the hold while the healthy Instances keep serving and an "
            f"InPlaceUpdateNotPossible event says why; the corrective push drops the env override and lands in place on "
            f"every Instance, the broken one included (its image is patched back, same UID), with no repair recorded "
            f"for it after the bump; floor waived — the crash itself costs settled capacity until the corrective push "
            f"lands{_ipo_controls(topo)}")


def emit_inplaceonly(add):
    for topo, n, w in IPO_SHAPES:
        gang = is_mn(topo)
        landing = "recreate" if gang else "held"
        base = dict(**counts(topo, n), **ceilings(topo, n, IPO_STRAT, 0), co="none", topo=topo)
        for rp in RESTARTS:
            life, d_life = _ipo_lifecycles(topo, rp)
            add("gen-inplaceonly", f"{GEN_MARK} inplaceonly image {shape_tag(topo, w)} N={n} rp {rp}",
                dict(base, edit="image", img=ALT_IMAGE), comps(topo, n, w, life, d_life=d_life),
                _ipo_image_reason(topo, rp), dict(strat=IPO_STRAT, verb="image", rp=rp),
                cell=("inplaceonly", "image", topo, w, rp))
            for edit in IPO_TEMPLATE_EDITS:
                kv = dict(base, edit=edit)
                if not gang:
                    kv["expect"] = "held"
                add("gen-inplaceonly", f"{GEN_MARK} inplaceonly {edit} {shape_tag(topo, w)} N={n} rp {rp}", kv,
                    comps(topo, n, w, life, d_life=d_life), _ipo_held_reason(topo, edit, rp),
                    dict(strat=IPO_STRAT, verb=f"{edit}@{landing}", rp=rp),
                    cell=("inplaceonly", edit, topo, w, rp))
        for form, rp in IPO_IDLE_CRASHES:
            life, d_life = _ipo_lifecycles(topo, rp)
            kv = dict(base, fault="crashloop:old", form=form, during=IDLE_PHASE, expect="repaired")
            add("gen-inplaceonly", f"{GEN_MARK} inplaceonly crashloop idle {form} {shape_tag(topo, w)} N={n} rp {rp}", kv,
                comps(topo, n, w, life, d_life=d_life), _ipo_idle_reason(topo, form, rp),
                dict(strat=IPO_STRAT, verb=f"crashloop:{form}@idle", rp=rp, victim="engine"),
                cell=("inplaceonly", "idle", form, topo, w, rp))
        for rp in RESTARTS:
            life, d_life = _ipo_lifecycles(topo, rp)
            crash = dict(base, fault="crashloop:old", form="start", floor_soft=1)
            add("gen-inplaceonly", f"{GEN_MARK} inplaceonly crashloop start image {shape_tag(topo, w)} N={n} rp {rp}",
                dict(crash, edit="image", img=ALT_IMAGE), comps(topo, n, w, life, d_life=d_life),
                _ipo_crash_image_reason(topo, rp), dict(strat=IPO_STRAT, verb="crashloop:start+image", rp=rp, victim="engine"),
                cell=("inplaceonly", "crash-image", topo, w, rp))
            kv = dict(crash, edit="env")
            if not gang:
                kv["expect"] = "held"
            add("gen-inplaceonly", f"{GEN_MARK} inplaceonly crashloop start env {shape_tag(topo, w)} N={n} rp {rp}", kv,
                comps(topo, n, w, life, d_life=d_life), _ipo_crash_held_reason(topo, rp),
                dict(strat=IPO_STRAT, verb=f"crashloop:start+env@{landing}", rp=rp, victim="engine"),
                cell=("inplaceonly", "crash-env", topo, w, rp))
# ── gen-fault-scale / gen-fault-delete: an operation on a service with a broken Instance ──
# One Instance of the settled service is broken and the operator scales the
# engine, or deletes the service. The break hits Instance 0, the lowest
# index, so a scale-down's natural victim (the highest index) and the broken
# Instance differ and the family can tell the order of removal apart.
# The six shape families of the coverage ledger: singles at N=4, gangs at
# N=2 with one worker; a rolling group where a decoder exists. The engine
# runs under maxUnavailable 1 everywhere: nothing rolls, so the strategy is
# dormant, and a budget of one admits the repair that takes the broken
# Instance offline (a budget of zero holds it). (topo, N, W, routers on an
# engine-only shape.)
BROKEN_SHAPES = [("engine", 4, 0, 0), ("pd", 4, 0, 0), ("engine-mn", 2, 1, 0),
                 ("engine", 4, 0, lib.NR), ("engine-mn", 2, 1, lib.NR), ("pd-mn", 2, 1, 0)]
BROKEN_GROUPS = {"engine": ["none"], "pd": ["none", "rolling"], "engine-mn": ["none"], "pd-mn": ["none", "rolling"]}
BROKEN_FAULTS = [("crashloop", "start"), ("crashloop", "load"), ("imagepull", None), ("readiness", "persist")]
BROKEN_INST = 0
BROKEN_BUDGET = (1, 1)
# Readiness loss is repaired under neither policy, so its restart-policy axis
# is collapsed; every other break runs under both.
BROKEN_RESTARTS = {"readiness": ["None"]}
# The operations per fleet size: (label, target). On a fleet of two, "to
# one" is "down by one" and "down by two" asks for zero, which the API
# refuses (minReplicas 0 needs KEDA autoscaling), so the gang runs down by
# one and up by one.
BROKEN_OPS = {4: [("down1", 3), ("down2", 2), ("toone", 1), ("up1", 5)], 2: [("down1", 1), ("up1", 3)]}


def _broken_label(kind, form):
    return kind if form is None or kind == "readiness" else f"{kind}:{form}"


def _broken_how(kind, form, topo):
    victim = ("the leader pod of the engine's gang 0" if is_mn(topo) else "the pod of the engine's Instance 0")
    if kind == "readiness":
        return f"{victim} stops passing its readiness probe while it keeps running, so it leaves rotation"
    if kind == "imagepull":
        return (f"{victim} is pointed at an image that cannot be pulled on the settled service, so the kubelet parks "
                f"it in ImagePullBackOff and it is never Ready again")
    if form == "start":
        return (f"{victim} is pointed at an image with no shell on the settled service, so it crashes at every start "
                f"and is never Ready again")
    return (f"{victim} has its runner killed every few seconds while it is up, so it dies after Ready and comes back "
            f"between crashes")


def _broken_fate(kind):
    """What a scale-up leaves the broken Instance at: readiness loss is the
    runtime's own out-of-rotation signal and nothing rebuilds it; every other
    break parks the pod in a terminal kubelet waiting state, which the
    crash-loop repair rebuilds under every restart policy."""
    return "left" if kind == "readiness" else "repaired"


def _broken_fate_words(kind, form, rp):
    if kind == "readiness":
        return ("left as the fault left it: no new pod for its index and no repair recorded, since readiness is the "
                "runtime's own out-of-rotation signal and no restart policy rebuilds a pod that runs")
    out = "rebuilt, every pod new, Ready and serving on its running revision, and a RestartTriggered event names it: "
    if kind == "imagepull":
        out += ("a pod parked in ImagePullBackOff past the stuck-pod grace is a wedge the crash-loop repair rebuilds "
                "under every restart policy")
        if rp == "RecreateInstance":
            out += ("; under RecreateInstanceOnPodRestart the kubelet's kill of the running container ahead of the "
                    "failed pull reads as a runner restart after Ready and triggers it at once")
        return out
    if form == "start":
        out += ("a pod that dies at every start is parked in CrashLoopBackOff, a wedge the crash-loop repair rebuilds "
                "under every restart policy once the pod is past the stuck-pod grace")
        if rp == "RecreateInstance":
            out += "; under RecreateInstanceOnPodRestart the runner's restart after Ready triggers it at once"
        return out
    if rp == "RecreateInstance":
        return out + "the first runner restart after Ready triggers the rebuild under RecreateInstanceOnPodRestart"
    return out + ("the policy promises nothing for a runner that dies after Ready and comes back, but the kubelet "
                  "backs off its restarts and parks the pod in CrashLoopBackOff between them, a wedge the crash-loop "
                  "repair rebuilds under every restart policy once the pod is past the stuck-pod grace")


def _broken_removal_words(n, target):
    """The order a scale-down from n to target removes Instances in, with the
    broken Instance 0 going first and the highest healthy indexes after it."""
    extra = list(range(n - 1, 0, -1))[:n - target - 1]
    kept = [i for i in range(n) if i not in extra and i != BROKEN_INST]
    kept_words = (f"Instance {kept[0]} stays" if len(kept) == 1
                  else f"Instances {', '.join(str(i) for i in kept[:-1])} and {kept[-1]} stay")
    if not extra:
        return f"the broken Instance {BROKEN_INST} goes and {kept_words}"
    then = (f"the highest healthy Instance {extra[0]}" if len(extra) == 1
            else f"the highest healthy Instances {' and '.join(str(i) for i in extra)}")
    return f"the broken Instance {BROKEN_INST} goes first, then {then}; {kept_words}"


def _broken_tail(topo, co, op="a scale", repaired_up=False, nr=0):
    out = ""
    if is_pd(topo):
        out += "; decoder and router are neither broken nor scaled and stay as they were"
    elif nr:
        out += "; the router is neither broken nor scaled and keeps its pods"
    if co != "none":
        out += f"; the rollingUpdate group lists engine and decoder, and {op} is not a run it paces"
        if repaired_up:
            out += ", nor is the repair of an Instance that serves nothing a consult it may hold"
    if is_mn(topo):
        out += "; each Instance is a gang of a leader and a worker, counted whole"
    return out


def _broken_scale_reason(kind, form, rp, topo, co, n, target, nr=0):
    how = _broken_how(kind, form, topo)
    if target < n:
        return (f"{how}; once the status reports Instance {BROKEN_INST} broken the engine is scaled from {n} to {target}: "
                f"{_broken_removal_words(n, target)}; the survivors keep their pods and keep serving throughout (never "
                f"below {target} Ready), nothing is rebuilt during the scale, no Component surges, and every row "
                f"settles Ready with no operation open" + _broken_tail(topo, co, nr=nr))
    fate = _broken_fate(kind)
    return (f"{how}; once the status reports Instance {BROKEN_INST} broken the engine is scaled from {n} to {target}: a "
            f"new Instance {n} comes up on the running revision and serves, the healthy Instances keep their pods and "
            f"never leave rotation (never below {n - 1} Ready), nothing surges above {target}, and the broken Instance "
            f"is {_broken_fate_words(kind, form, rp)}" + _broken_tail(topo, co, repaired_up=(fate == "repaired"), nr=nr))


def _broken_delete_reason(kind, form, topo, co, nr=0):
    return (f"{_broken_how(kind, form, topo)}; once the status reports Instance {BROKEN_INST} broken the service is "
            f"deleted: every pod goes within the teardown budget, the broken one included, no pod stays Terminating "
            f"and no finalizer wedges, and the same name comes back Ready on fresh pods with nothing of the break left"
            + _broken_tail(topo, co, op="a delete", nr=nr))


def _broken_kv(mode, topo, n, kind, form, co, ceil_e, nr=0):
    kv = dict(mode=mode, **counts(topo, n, nr), ceil_e=ceil_e, ceil_d=(n if is_pd(topo) else 0),
              ceil_r=router_count(topo, nr), fault=f"{kind}:old")
    if form is not None:
        kv["form"] = form
    kv.update(during=IDLE_PHASE, co=co, topo=topo)
    return kv


def _broken_comps(topo, n, w, rp, nr=0):
    surge, unavail = BROKEN_BUDGET
    life = lib.lifecycle(strat="SurgeThenDrain", surge=surge, unavail=unavail, restart=RP_FULL[rp])
    d_life = lib.lifecycle(restart=RP_FULL[rp]) if is_pd(topo) else None
    return comps(topo, n, w, life, d_life=d_life, nr=nr)


# The further rows, one case per ledger cell: the operations each row runs
# (one scale-down, the scale-up, the delete), the restart policy, and the
# sibling a break may hit per shape (the decoder beside a prefill/decode
# engine, the router in front of an engine fleet).
BROKEN_MORE_OPS = {4: [("down1", 3), ("up1", 5)], 2: [("down1", 1), ("up1", 3)]}
BROKEN_MORE_RP = "None"
BROKEN_SIBLING = {"pd": "decoder", "pd-mn": "decoder", "engine": "router", "engine-mn": "router"}
BROKEN_CONFIG_SUFFIX = "-config"
# The one-shot breaks: (kind, form), lifted with lift=after-settle except
# readiness, whose switch form=recover lifts.
BROKEN_ONCE = [("crashloop", "start"), ("crashloop", "load"), ("imagepull", None), ("readiness", "recover"),
               ("configkey", None)]
# The pending causes before an operation: (label, the fault per operation,
# the lifts). A scale-up meets a quota with no room for one more pod; a
# delete's recreate meets one a pod short of the fleet; a scale-down
# places nothing and meets either as a negative control.
BROKEN_PENDING = [("taint", {"down1": "taint:all", "up1": "taint:all", "delete": "taint:all"}, ["after-settle", "never"]),
                  ("quota", {"down1": "quota:0", "up1": "quota:0", "delete": "quota:-1"}, ["after-settle"]),
                  ("quota-kept", {"down1": "quota:0"}, ["never"])]
# The quota kept to the end under the scale-up and the delete, minted after
# the family's other rows so those keep their ids.
BROKEN_PENDING_KEPT_MORE = {"up1": "quota:0", "delete": "quota:-1"}
BROKEN_PENDING_TOKEN = {"taint": "Unschedulable", "quota": "QuotaExceeded"}


def _broken_more_label(kind, form):
    return _broken_label(kind, form) if kind != "configkey" else "configkey"


def _broken_victim_words(topo, victim):
    """The pod a break hits, in words, per Component."""
    if victim == "engine":
        return "the leader pod of the engine's gang 0" if is_mn(topo) else "the pod of the engine's Instance 0"
    if victim == "decoder":
        return "the leader pod of the decoder's gang 0" if is_mn(topo) else "the pod of the decoder's Instance 0"
    return "the pod of the router's Instance 0"


def _broken_more_how(kind, form, topo, victim="engine"):
    pod = _broken_victim_words(topo, victim)
    if kind == "configkey":
        return (f"the ConfigMap key the runner of {pod} reads is removed and the runner restarted, so its next start "
                f"waits in CreateContainerConfigError and it serves nothing")
    if kind == "readiness":
        return f"{pod} stops passing its readiness probe while it keeps running, so it leaves rotation"
    if kind == "imagepull":
        return (f"{pod} is pointed at an image that cannot be pulled on the settled service, so the kubelet parks it in "
                f"ImagePullBackOff and it is never Ready again")
    if form == "start":
        return f"{pod} is pointed at an image with no shell on the settled service, so it crashes at every start and is never Ready again"
    return f"{pod} has its runner killed every few seconds while it is up, so it dies after Ready and comes back between crashes"


def _broken_once_lift_words(kind, form):
    if kind == "configkey":
        return "the key is put back and the kubelet's own retry starts the container"
    if kind == "readiness":
        return "the readiness switch is lifted and the same pod passes readiness again"
    if kind == "imagepull" or form == "start":
        return "the pod is pointed back at the image it ran, where a repair has not rebuilt it first"
    return "the killer stops and the kubelet's restart of the runner is the last"


def _broken_heal_words(kind, form):
    return (f"the broken Instance is Ready and serving again on its running revision once the cause is gone, the same "
            f"pod started again or rebuilt by a repair that is recorded at most once, with no new revision")


def _broken_more_op_words(n, target, scomp="engine"):
    if target < n:
        return f"the {scomp} is scaled from {n} to {target}"
    return f"the {scomp} is scaled from {n} to {target}"


def _broken_once_reason(kind, form, topo, n, target, op, nr):
    how = _broken_more_how(kind, form, topo)
    lift = _broken_once_lift_words(kind, form)
    if op == "delete":
        return (f"{how}; once the status reports Instance {BROKEN_INST} broken the service is deleted and {lift}: every pod "
                f"goes within the teardown budget, the broken one included, no pod stays Terminating and no finalizer "
                f"wedges, and the same name comes back Ready on fresh pods with nothing of the break left"
                + _broken_tail(topo, "none", op="a delete", nr=nr))
    if target < n:
        return (f"{how}; once the status first reports Instance {BROKEN_INST} broken {_broken_more_op_words(n, target)} and, "
                f"once the scale has landed, {lift}: {_broken_removal_words(n, target)}; the survivors keep their pods and keep "
                f"serving throughout (never below {target} Ready), no Component surges, and every row settles Ready with "
                f"no operation open" + _broken_tail(topo, "none", nr=nr))
    return (f"{how}; once the status first reports Instance {BROKEN_INST} broken {_broken_more_op_words(n, target)} and, "
            f"once the scale has landed, {lift}: a new Instance {n} comes up on the running revision and serves, the healthy "
            f"Instances keep their pods and never leave rotation (never below {n - 1} Ready), nothing surges above "
            f"{target}, and {_broken_heal_words(kind, form)}" + _broken_tail(topo, "none", nr=nr))


def _broken_configkey_reason(topo, n, target, op, nr):
    how = _broken_more_how("configkey", None, topo)
    repair = ("the crash-loop repair opens as soon as the wait is seen and rebuilds the pod, which waits on the same key, "
              "so the Instance serves nothing while the key is away")
    if op == "delete":
        return (f"{how}; {repair}; once the status reports Instance {BROKEN_INST} broken the service is deleted: every pod "
                f"goes within the teardown budget, the waiting one included, no pod stays Terminating and no finalizer "
                f"wedges, and the same name comes back Ready on fresh pods with the key the re-apply carries"
                + _broken_tail(topo, "none", op="a delete", nr=nr))
    return (f"{how}; {repair}; once the status reports Instance {BROKEN_INST} broken {_broken_more_op_words(n, target)}: "
            f"{_broken_removal_words(n, target)}; the survivors keep their pods and keep serving throughout (never below "
            f"{target} Ready), no Component surges, and every row settles Ready with no operation open; the key returns "
            f"only with the case's teardown" + _broken_tail(topo, "none", nr=nr))


def _broken_sibling_reason(kind, form, topo, n, target, op, victim, once, nr):
    how = _broken_more_how(kind, form, topo, victim)
    whose = f"the {victim}'s Instance {BROKEN_INST}"
    if once:
        fate = (f"once the scale has landed, {_broken_once_lift_words(kind, form)} and {whose} is Ready and serving again on its "
                f"running revision, the same pod started again or rebuilt by a repair recorded at most once")
    else:
        fate = (f"{whose} is rebuilt, every pod new, Ready and serving on its running revision, by the crash-loop repair a "
                f"RestartTriggered event names (a pod parked in CrashLoopBackOff past the stuck-pod grace is a wedge it "
                f"rebuilds under every restart policy)")
    if op == "delete":
        return (f"{how}; once the status reports {whose} broken the service is deleted: every pod goes within the "
                f"teardown budget, the broken one included, no pod stays Terminating and no finalizer wedges, and the "
                f"same name comes back Ready on fresh pods with nothing of the break left"
                + ("; each engine Instance is a gang of a leader and a worker, counted whole" if is_mn(topo) else ""))
    if target < n:
        act = (f"the engine is scaled from {n} to {target}: the highest engine Instance {n - 1} goes and the rest stay, since "
               f"the broken Instance is the {victim}'s and not the scale's to take")
    else:
        act = (f"the engine is scaled from {n} to {target}: a new engine Instance {n} comes up on the running revision and "
               f"serves, nothing surges above {target}")
    return (f"{how}; once the status reports {whose} broken {act}; the engine's healthy Instances keep their pods and keep "
            f"serving throughout, the {victim}'s other Instances keep theirs (never below {n - 1 if victim != 'router' else nr - 1} "
            f"Ready there) and nothing rotates or surges on the {victim}; {fate}; every row settles Ready with no operation "
            f"open; the engine is the negative control of the {victim}'s break: the scale is its only change"
            + ("; each engine Instance is a gang of a leader and a worker, counted whole" if is_mn(topo) else ""))


def _broken_pending_reason(label, fault, lift, topo, n, target, op, nr):
    cause = ("a NoSchedule taint on every schedulable node" if label == "taint"
             else "a quota with room for no further pod" if fault == "quota:0"
             else "a quota one pod short of the running fleet")
    token = BROKEN_PENDING_TOKEN[label.split("-")[0]]
    how = "placed" if label == "taint" else "admitted"
    gone = ("once the cause is lifted" if lift == "after-settle" else "the cause stays to the end, so")
    if op == "delete":
        short = "its last pod" if label.startswith("quota") else "its pods"
        out = (f"{cause} is armed on the settled service, then the service is deleted: every pod goes within the teardown "
               f"budget and the same name is re-applied, but {short} cannot be {how}, so a row of the recreate waits with "
               f"reason {token} and no row reads Failed")
        out += (f"; {gone} the recreate comes back Ready on fresh pods" if lift == "after-settle"
                else f"; {gone} the wait holds with its reason across the hold window and the service comes back only once the cause is gone")
    elif target < n:
        out = (f"{cause} is armed on the settled service, then the engine is scaled from {n} to {target}: nothing is placed or "
               f"admitted, so the scale-down converges beside the cause, the highest engine Instance {n - 1} goes, no row "
               f"waits on {token}, the survivors keep their pods and every row settles Ready with no operation open")
        out += ("; the cause is lifted once the scale has converged and nothing moves for it" if lift == "after-settle"
                else "; the cause stays to the end of the case and nothing waits on it")
    else:
        out = (f"{cause} is armed on the settled service, then the engine is scaled from {n} to {target}: the new Instance's "
               f"pod cannot be {how}, so its row waits with reason {token}, nothing settled is taken down to make room, "
               f"the healthy Instances keep their pods and keep serving at {n}, and no row reads Failed")
        out += (f"; {gone} the new Instance is {how} and serves, the fleet is at {target} and every row settles"
                if lift == "after-settle" else f"; {gone} the wait holds with its reason across the hold window")
    if is_pd(topo):
        out += "; decoder and router are neither scaled nor waiting and stay as they were"
    elif nr:
        out += "; the router is neither scaled nor waiting and keeps its pods"
    if is_mn(topo):
        out += "; each Instance is a gang of a leader and a worker, counted whole"
    return out


def _broken_configkey_body(name, topo, n, w, nr):
    """The case body of a missing-key cell: the engine runner reads one key of
    a ConfigMap named after the case, carried as a second document."""
    cm = f"{name}{BROKEN_CONFIG_SUFFIX}"
    cs = _broken_comps(topo, n, w, BROKEN_MORE_RP, nr)
    cs[0]["runner"] = lib.configmap_env(cm, HEAL_CONFIG_KEY, HEAL_CONFIG_VAR, gang=is_mn(topo))
    return lib.render(name, cs, docs=[lib.configmap_doc(cm, {HEAL_CONFIG_KEY: HEAL_CONFIG_VALUE})])


def _broken_more_kv(mode, topo, n, kind, form, ceil_e, nr, op, target):
    kv = _broken_kv(mode, topo, n, kind if kind != "configkey" else "crashloop", form, "none", ceil_e, nr)
    if kind == "configkey":
        kv["fault"] = "configkey:missing"
        kv.pop("form", None)
    if op != "delete":
        kv.update(to_e=target, inst=BROKEN_INST)
    else:
        kv["inst"] = BROKEN_INST
    return kv


def emit_fault_more(add):
    """The further rows of gen-fault-scale and gen-fault-delete, one case per
    ledger cell (see BROKEN_MORE_OPS and the manifest)."""
    rp = BROKEN_MORE_RP
    for topo, n, w, nr in BROKEN_SHAPES:
        ops = BROKEN_MORE_OPS[n] + [("delete", None)]
        for op, target in ops:
            mode = "recreate" if op == "delete" else "scaledown"
            folder = "gen-fault-delete" if op == "delete" else "gen-fault-scale"
            up = target is not None and target > n
            ceil_e = max(n, target or n)
            tag = f"{shape_tag(topo, w, nr)} N={n} rp {rp}"
            # The missing key, kept away: a scale-down and a delete.
            if not up:
                kv = _broken_more_kv(mode, topo, n, "configkey", None, ceil_e, nr, op, target)
                add(folder, f"{GEN_MARK} broken configkey {op} {tag}", kv, None,
                    _broken_configkey_reason(topo, n, target, op, nr),
                    dict(strat="(dormant: nothing rolls)", verb=f"configkey@{op}", rp=rp, op=op, victim="engine"),
                    cell=("broken-more", "configkey", None, topo, w, op, nr),
                    body=functools.partial(_broken_configkey_body, topo=topo, n=n, w=w, nr=nr))
            # The one-shot forms: the cause taken away as the operation lands.
            for kind, form in BROKEN_ONCE:
                kv = _broken_more_kv(mode, topo, n, kind, form if kind != "readiness" else "recover", ceil_e, nr, op, target)
                if kind != "readiness":
                    kv["lift"] = "after-settle"
                if up:
                    kv["expect"] = "heals"
                label = _broken_more_label(kind, form)
                body = functools.partial(_broken_configkey_body, topo=topo, n=n, w=w, nr=nr) if kind == "configkey" else None
                add(folder, f"{GEN_MARK} broken {label} once {op} {tag}", kv, None if body else _broken_comps(topo, n, w, rp, nr),
                    _broken_once_reason(kind, form, topo, n, target, op, nr),
                    dict(strat="(dormant: nothing rolls)", verb=f"{label}:once@{op}", rp=rp, op=op, victim="engine"),
                    cell=("broken-more", kind, form, "once", topo, w, op, nr), body=body)
            # A pending cause armed before the operation.
            for label, faults, lifts in BROKEN_PENDING:
                if op not in faults:
                    continue
                for lift in lifts:
                    kv = dict(mode=mode, **counts(topo, n, nr), ceil_e=ceil_e, ceil_d=(n if is_pd(topo) else 0),
                              ceil_r=router_count(topo, nr), fault=faults[op], during=IDLE_PHASE, co="none", topo=topo,
                              lift=lift)
                    if op != "delete":
                        kv["to_e"] = target
                    add(folder, f"{GEN_MARK} broken pending {label} {op} {tag} lift {lift}", kv, _broken_comps(topo, n, w, rp, nr),
                        _broken_pending_reason(label, faults[op], lift, topo, n, target, op, nr),
                        dict(strat="(dormant: nothing rolls)", verb=f"pending:{label}@{op}", rp=rp, op=op, lift=lift),
                        cell=("broken-more", "pending", label, lift, topo, w, op, nr))
            # The sibling's break: the decoder beside a prefill/decode
            # engine, the router in front of an engine fleet.
            victim = BROKEN_SIBLING[topo] if (is_pd(topo) or nr) else None
            if victim is None:
                continue
            for once in (False, True):
                kv = _broken_more_kv(mode, topo, n, "crashloop", "start", ceil_e, nr, op, target)
                kv["victimcomp"] = victim
                if once:
                    kv["lift"] = "after-settle"
                if op != "delete":
                    kv["expect"] = "heals" if once else "repaired"
                add(folder, f"{GEN_MARK} broken sibling {victim} crashloop:start{' once' if once else ''} {op} {tag}", kv,
                    _broken_comps(topo, n, w, rp, nr),
                    _broken_sibling_reason("crashloop", "start", topo, n, target, op, victim, once, nr),
                    dict(strat="(dormant: nothing rolls)", verb=f"crashloop:start{':once' if once else ''}@{op}", rp=rp, op=op,
                         victim=victim),
                    cell=("broken-more", "sibling", victim, once, topo, w, op, nr))
    for topo, n, w, nr in BROKEN_SHAPES:
        for op, target in [row for row in BROKEN_MORE_OPS[n] if row[0] == "up1"] + [("delete", None)]:
            mode = "recreate" if op == "delete" else "scaledown"
            folder = "gen-fault-delete" if op == "delete" else "gen-fault-scale"
            ceil_e = max(n, target or n)
            fault, lift = BROKEN_PENDING_KEPT_MORE[op], "never"
            kv = dict(mode=mode, **counts(topo, n, nr), ceil_e=ceil_e, ceil_d=(n if is_pd(topo) else 0),
                      ceil_r=router_count(topo, nr), fault=fault, during=IDLE_PHASE, co="none", topo=topo, lift=lift)
            if op != "delete":
                kv["to_e"] = target
            tag = f"{shape_tag(topo, w, nr)} N={n} rp {rp}"
            add(folder, f"{GEN_MARK} broken pending quota-kept {op} {tag} lift {lift}", kv, _broken_comps(topo, n, w, rp, nr),
                _broken_pending_reason("quota-kept", fault, lift, topo, n, target, op, nr),
                dict(strat="(dormant: nothing rolls)", verb=f"pending:quota-kept@{op}", rp=rp, op=op, lift=lift),
                cell=("broken-more", "pending", "quota-kept", lift, topo, w, op, nr))


def emit_fault_scale(add):
    for topo, n, w, nr in BROKEN_SHAPES:
        for co in BROKEN_GROUPS[topo]:
            for kind, form in BROKEN_FAULTS:
                for rp in BROKEN_RESTARTS.get(kind, RESTARTS):
                    for op, target in BROKEN_OPS[n]:
                        kv = _broken_kv("scaledown", topo, n, kind, form, co, max(n, target), nr)
                        kv.update(to_e=target, inst=BROKEN_INST)
                        if target > n:
                            kv["expect"] = _broken_fate(kind)
                        meta = dict(strat="(dormant: nothing rolls)", verb=f"{_broken_label(kind, form)}@{op}",
                                    rp=rp, op=op, victim="engine")
                        desc = (f"{GEN_MARK} broken {_broken_label(kind, form)} {op} {shape_tag(topo, w, nr)} "
                                f"N={n} rp {rp} co {co}")
                        add("gen-fault-scale", desc, kv, _broken_comps(topo, n, w, rp, nr),
                            _broken_scale_reason(kind, form, rp, topo, co, n, target, nr), meta,
                            cell=("broken-scale", kind, form, topo, w, co, rp, op, nr),
                            coord=lib.coord_for(co) if co != "none" else None)


def emit_fault_delete(add):
    for topo, n, w, nr in BROKEN_SHAPES:
        for co in BROKEN_GROUPS[topo]:
            for kind, form in BROKEN_FAULTS:
                for rp in BROKEN_RESTARTS.get(kind, RESTARTS):
                    kv = _broken_kv("recreate", topo, n, kind, form, co, n, nr)
                    kv["inst"] = BROKEN_INST
                    meta = dict(strat="(dormant: nothing rolls)", verb=f"{_broken_label(kind, form)}@delete",
                                rp=rp, op="delete", victim="engine")
                    desc = (f"{GEN_MARK} broken {_broken_label(kind, form)} delete {shape_tag(topo, w, nr)} "
                            f"N={n} rp {rp} co {co}")
                    add("gen-fault-delete", desc, kv, _broken_comps(topo, n, w, rp, nr),
                        _broken_delete_reason(kind, form, topo, co, nr), meta,
                        cell=("broken-delete", kind, form, topo, w, co, rp, nr),
                        coord=lib.coord_for(co) if co != "none" else None)
# ── gen-fault-move: a fault at a moment of a move ────────────────────────────
# An Instance is moved by request and, at a moment of the move, a pod breaks
# or the source node dies. The six shape families of the coverage ledger at
# one gang width (the move copies the whole gang), the canonical surge budget
# (no roll is in flight; the strategy is dormant), no group (a repair of an
# Instance that serves nothing is admitted without a group's gate; the group
# axis is the steady-state crash-loop family's). (topo, N, W, routers on an
# engine-only shape.)
MOVE_SHAPES = [("engine", 4, 0, 0), ("pd", 4, 0, 0), ("engine-mn", 2, 1, 0),
               ("engine", 4, 0, lib.NR), ("engine-mn", 2, 1, lib.NR), ("pd-mn", 2, 1, 0)]
MOVE_MOMENTS = ["accept", "surge", "drain"]
# (kind, form, the restart policy the cell runs under): a crash at start and
# a pull failure reach the crash-loop repair under either policy; the
# after-Ready form is the one the policy decides, so it runs under the policy
# that promises its repair.
MOVE_BREAKS = [("crashloop", "start", "None"), ("imagepull", None, "None"), ("crashloop", "load", "RecreateInstance")]
# The sibling a break may hit is the decoder; the router in front of an
# engine fleet is this family's negative control.
MOVE_VICTIMS = {"engine": ["moved", "peer"], "pd": ["moved", "peer", "sibling"], "engine-mn": ["moved", "peer"],
                "pd-mn": ["moved", "peer", "sibling"]}
# The source node dies at accept or at surge (at drain the source is already
# going); read on the force-delete lane, under both restart policies.
MOVE_NODE_MOMENTS = ["accept", "surge"]
MOVE_WHEN = {"accept": "as soon as the request is recorded, before the replacement exists",
             "surge": "as the replacement pod appears, before it is Ready",
             "drain": "as the source is being taken down, the replacement Ready and serving"}
MOVE_BREAK_HOW = {("crashloop", "start"): "is pointed at an image with no shell, so it crashes at every start and is never Ready again",
                  ("crashloop", "load"): "has its runner killed every few seconds while it is up, so it dies after Ready and comes back between crashes",
                  ("imagepull", None): "is pointed at an image that cannot be pulled, so the kubelet parks it in ImagePullBackOff and it is never Ready again"}
MOVE_REPAIR_HOW = {"None": "an Instance that serves nothing is rebuilt by the crash-loop repair once its pod has sat past the stuck-pod grace",
                   "RecreateInstance": "a runner that dies after Ready re-arms the whole-Instance rebuild"}


def _move_fault_pod(at, victim, sibling="decoder"):
    """(when, whose pod) the fault hits, in words."""
    if victim == "sibling":
        return MOVE_WHEN[at], f"the {sibling}'s highest Instance's pod"
    if victim == "peer":
        return MOVE_WHEN[at], "the pod of the moved Component's highest other Instance"
    return {"accept": ("as soon as the request is recorded, before its replacement exists", "the moved Instance's source pod"),
            "surge": ("as soon as it appears, before it is Ready", "the moved Instance's replacement pod"),
            "drain": ("as the source is being taken down", "the moved Instance's replacement pod, Ready and serving,")}[at]


def _move_fault_reason(kind, form, at, victim, rp, topo, nr=0, sibling="decoder"):
    when, pod = _move_fault_pod(at, victim, sibling)
    out = (f"{when}, {pod} {MOVE_BREAK_HOW[(kind, form)]}; the move finishes, or is abandoned with a reason its "
           f"record carries, within budget and is recorded once (one ledger row, one controller record); the moved "
           f"Instance ends whole, Ready and serving on one pod set, the engine holds N whole Instances on distinct "
           f"indices, and nothing exceeds N+1 alive")
    if victim == "moved":
        out += (f"; no pod the fault hit survives unserving: the move or the repair rebuilds it under restart policy "
                f"{RP_FULL[rp]} ({MOVE_REPAIR_HOW[rp]})")
        if at == "surge":
            out += "; a replacement that never comes up is the move's to abandon or to retry, and either way the Instance keeps serving on exactly one pod set"
    else:
        out += (f"; the move completes (a broken {'peer' if victim == 'peer' else 'sibling'} is not the move's to care "
                f"about) and the broken Instance is rebuilt, every pod new, Ready and serving on its running revision, "
                f"by a repair a RestartTriggered event names, under restart policy {RP_FULL[rp]} "
                f"({MOVE_REPAIR_HOW[rp]}); the capacity floor, lowered by that one Instance, holds throughout")
    if victim == "sibling":
        out += f"; the {sibling} is otherwise untouched: nothing rotates and nothing surges there"
    if sibling == "router":
        out += "; the engine's other Instances are the negative control: nothing rotates and nothing surges there beyond the move"
    elif is_pd(topo) or nr:
        out += "; the router keeps its pods"
    if is_mn(topo):
        out += "; each Instance is a gang, moved and rebuilt whole"
    return out


def _move_node_reason(at, rp, topo):
    out = (f"the kubelet of the node under the moved Instance's source pod is stopped {MOVE_WHEN[at]}; the node reads "
           f"NotReady and the source serves nothing; with the force-delete policy the pod names the dead node holds are "
           f"freed once its unreachable evidence is older than the threshold: the move's replacement, surging "
           f"elsewhere, takes over and the sweep drains the source, so the move finishes, or is abandoned with a reason "
           f"its record carries, and either way the Instance ends whole, Ready and serving off the dead node, recorded "
           f"once; under restart policy {RP_FULL[rp]} {NODE_RELOCATION_REBUILD[rp]}; the floor is waived: the dead "
           f"node takes every pod on it, peers and siblings included, and the same sweep rebuilds those; then the "
           f"kubelet is started again")
    if is_mn(topo):
        out += "; the gang's members on live nodes may keep their pods"
    return out


def emit_fault_move(add):
    for topo, n, w, nr in MOVE_SHAPES:
        base = dict(mode="migrate", **counts(topo, n, nr), ceil_e=n + 1, ceil_d=(n if is_pd(topo) else 0),
                    ceil_r=router_count(topo, nr))
        for at in MOVE_MOMENTS:
            for victim in MOVE_VICTIMS[topo]:
                for kind, form, rp in MOVE_BREAKS:
                    kv = dict(base, fault=f"{kind}:old", co="none", topo=topo)
                    if form:
                        kv["form"] = form
                    kv.update(victim=victim, mig="Auto", inst=0, at=at)
                    meta = dict(strat="SurgeThenDrain", verb=f"{_storm_label(kind, form)}@move-{at}", rp=rp,
                                victim=("decoder" if victim == "sibling" else "engine"), at=at, who=victim)
                    desc = (f"{GEN_MARK} fault-move {_storm_label(kind, form)} at {at} on {victim} "
                            f"{shape_tag(topo, w, nr)} N={n} rp {rp}")
                    life = lib.lifecycle(restart=RP_FULL[rp])
                    d_life = lib.lifecycle(restart=RP_FULL[rp]) if is_pd(topo) else None
                    add("gen-fault-move", desc, kv, comps(topo, n, w, life, d_life=d_life, nr=nr),
                        _move_fault_reason(kind, form, at, victim, rp, topo, nr), meta,
                        cell=("fault-move", kind, form, at, victim, topo, w, rp, nr))
        for at in MOVE_NODE_MOMENTS:
            for rp in RESTARTS:
                kv = dict(base, fault=f"node:{NODE_PARAM}", floor_soft=1, co="none", topo=topo)
                kv.update(victim="moved", mig="Auto", inst=0, at=at, lift="after-recover", needs=NODE_NEEDS["relocated"])
                meta = dict(strat="SurgeThenDrain", verb=f"node@move-{at}", rp=rp, victim="engine", at=at, who="moved")
                desc = f"{GEN_MARK} fault-move node at {at} on moved {shape_tag(topo, w, nr)} N={n} rp {rp}"
                life = lib.lifecycle(restart=RP_FULL[rp])
                d_life = lib.lifecycle(restart=RP_FULL[rp]) if is_pd(topo) else None
                add("gen-fault-move", desc, kv, comps(topo, n, w, life, d_life=d_life, nr=nr),
                    _move_node_reason(at, rp, topo), meta,
                    cell=("fault-move", "node", None, at, "moved", topo, w, rp, nr))
    # The router as the broken sibling on the engine+router shapes: a break
    # the move is not to care about, rebuilt by a repair beside it.
    for topo, n, w, nr in MOVE_SHAPES:
        if not nr:
            continue
        base = dict(mode="migrate", **counts(topo, n, nr), ceil_e=n + 1, ceil_d=0, ceil_r=router_count(topo, nr))
        for at in MOVE_MOMENTS:
            for kind, form, rp in MOVE_BREAKS:
                kv = dict(base, fault=f"{kind}:old", co="none", topo=topo)
                if form:
                    kv["form"] = form
                kv.update(victim="sibling", victimcomp="router", mig="Auto", inst=0, at=at)
                meta = dict(strat="SurgeThenDrain", verb=f"{_storm_label(kind, form)}@move-{at}", rp=rp,
                            victim="router", at=at, who="sibling")
                desc = (f"{GEN_MARK} fault-move {_storm_label(kind, form)} at {at} on sibling router "
                        f"{shape_tag(topo, w, nr)} N={n} rp {rp}")
                life = lib.lifecycle(restart=RP_FULL[rp])
                add("gen-fault-move", desc, kv, comps(topo, n, w, life, r_life=lib.lifecycle(restart=RP_FULL[rp]), nr=nr),
                    _move_fault_reason(kind, form, at, "sibling", rp, topo, nr, sibling="router"), meta,
                    cell=("fault-move", kind, form, at, "sibling", "router", topo, w, rp, nr))
    _emit_fault_move_more(add)


# ── the move under the remaining faults ──────────────────────────────────────
# The same six shapes and the same request, beside the faults the family's
# first rows left out: a break of the running revision healed again or a
# runner killed once, the missing ConfigMap key and the readiness switch
# (kept or lifted), a pod deleted by hand at the request (a gang member
# under kill=, again under repeat=), a pod evicted or preempted at a moment
# of the move, a taint that leaves the replacement nowhere to go, and a move
# requested while a push is stuck on a broken revision.
MOVE_MORE_MOMENTS = ["accept", "surge"]
# A runner killed once: the replacement at its first start, or Ready and
# serving as the source is taken down.
MOVE_ONCE_MOMENTS = [("start", "surge", "None"), ("load", "drain", "RecreateInstance")]
MOVE_KEY_LIFTS = ["after-settle", "never"]
MOVE_READINESS_FORMS = ["recover", "persist"]
MOVE_READINESS_KEPT_MOMENTS = ["accept", "surge", "drain"]
MOVE_HOOK_MOMENTS = ["accept", "surge", "drain"]
MOVE_EVICT_REPEAT_AT = "surge"
MOVE_PREEMPT_AT = "surge"
MOVE_REPEAT_N = 2
MOVE_PENDING_LIFTS = ["after-settle", "never"]
# (the break the stuck push carries, the stuck states it is read at): the
# unpullable image under both, the other kinds under the spent ladder.
MOVE_STUCK_BREAKS = [("imagepull", ["held", "failed"]), ("crash-start", ["held"]), ("crash-load", ["held"]),
                     ("configkey", ["held"]), ("stuck", ["held"])]
MOVE_STUCK_STRAT = ("SurgeThenDrain", 1, 0)
MOVE_STUCK_READY_TIMEOUT = READINESS_NEW_TIMEOUT
MOVE_CONFIG_SUFFIX = "-config"
MOVE_STUCK_HOW = {"imagepull": "an image that cannot be pulled",
                  "crash-start": "a runner image with no shell, so its pods crash at every start",
                  "crash-load": "an exit switch, so its runner dies a few seconds after Ready, every time the kubelet starts it",
                  "configkey": "a ConfigMap key the ConfigMap lacks, so its pods wait in CreateContainerConfigError",
                  "stuck": "a readiness probe that never passes, so its pods run and never become Ready"}
MOVE_STUCK_STATE = {"held": "the retry ladder is spent (a retry block in state Held)",
                    "failed": "its first attempt has failed (an Instance row reads Failed, the ladder still running)"}
MOVE_SIBLING = {"engine": "router", "engine-mn": "router", "pd": "decoder", "pd-mn": "decoder"}


def is_move_more_row(kv):
    """True for a gen-fault-move row of the move beside the remaining
    faults: a stuck push, a pod taken or pending, the missing key, the
    readiness switch, a break healed again, a runner killed once or a quota
    the request meets."""
    kind = str(kv.get("fault", "")).partition(":")[0]
    plain = "fault" not in kv and not kv.get("stuck")
    return bool(plain or kv.get("stuck") or kv.get("once") or kv.get("lift") == "after-settle"
                or kind in ("configkey", "readiness", "podkill", "taint", "evict", "preempt", "quota"))


def _move_request_words(topo, w, nr):
    return f"a move of engine Instance 0 is requested on {shape_words(topo, w, nr)}"


def _move_promise():
    return ("the move finishes, or is abandoned with a reason its record carries, within budget and is recorded once "
            "(one ledger row, one controller record), the engine holds N whole Instances on distinct indices, "
            "nothing exceeds N+1 alive and the floor, lowered by the one Instance the fault took, holds")


def _move_victim_words(at, victim, sibling):
    when, pod = _move_fault_pod(at, victim, sibling)
    return when, pod


def _move_configkey_body(name, topo, n, w, nr, life, d_life, victim_comp):
    """The case body of a missing-key cell at a move: the victim Component's
    runner reads one key of a ConfigMap named after the case, carried as a
    second document."""
    cm = f"{name}{MOVE_CONFIG_SUFFIX}"
    cs = comps(topo, n, w, life, d_life=d_life, nr=nr)
    slot = 1 if victim_comp == "decoder" else 0
    cs[slot]["runner"] = lib.configmap_env(cm, HEAL_CONFIG_KEY, HEAL_CONFIG_VAR, gang=is_mn(topo))
    return lib.render(name, cs, docs=[lib.configmap_doc(cm, {HEAL_CONFIG_KEY: HEAL_CONFIG_VALUE})])


def _move_healed_reason(kind, form, at, victim, topo, w, nr, sibling):
    when, pod = _move_victim_words(at, victim, sibling)
    out = (f"{_move_request_words(topo, w, nr)} and {when}, {pod} {MOVE_BREAK_HOW[(kind, form)]}; once the break has "
           f"bitten for a settle window the harness takes it away again (the killer stops, or the pod is pointed back "
           f"at the image it ran); {_move_promise()}")
    if victim == "moved":
        out += "; the moved Instance ends whole, Ready and serving on one pod set, healed where it stood or rebuilt by the move"
    else:
        out += (f"; the move completes (a broken {victim} is not the move's to care about) and the broken Instance "
                f"ends Ready and serving again, healed where it stood or rebuilt by a repair, which is reported, not demanded")
    return out


def _move_once_reason(form, at, topo, w, nr, rp):
    pod = "the moved Instance's replacement pod" if at == "surge" else "the moved Instance's replacement pod, Ready and serving,"
    moment = "at its first start, before it is Ready" if form == "start" else "once it serves, as the source is being taken down"
    out = (f"{_move_request_words(topo, w, nr)} and the runner of {pod} is killed exactly once {moment}; the kubelet "
           f"starts it again and nothing kills it twice; {_move_promise()}; the moved Instance ends whole, Ready and "
           f"serving on the running revision, nothing escalates the one restart to Failed or a hold, every row settles, "
           f"and a rebuild is recorded only where restart policy {RP_FULL[rp]} promises one")
    if rp == "RecreateInstance":
        out += " (at most one, reported for a replacement that restarted mid-move)"
    else:
        out += " (none: the kubelet's restart is enough)"
    return out


def _move_key_reason(at, victim, lift, topo, w, nr):
    when, pod = _move_victim_words(at, victim, "decoder")
    comp = "decoder" if victim == "sibling" else "engine"
    out = (f"{_move_request_words(topo, w, nr)} and {when}, the ConfigMap key {pod.replace(',', '')} reads is removed "
           f"and its runner restarted, so it waits in CreateContainerConfigError, as does every new pod of the {comp} "
           f"while the key is away")
    if lift == "after-settle":
        out += (f"; once the wait has bitten for a settle window the key is put back; {_move_promise()}; the moved "
                f"Instance ends whole, Ready and serving on one pod set")
        if victim != "moved":
            out += (f"; the move completes and the broken {victim} ends Ready and serving again, healed where it stood "
                    f"or rebuilt, which is reported, not demanded")
        return out
    out += "; the key stays away to the end of the case"
    if victim == "sibling":
        out += (f"; the move completes (the decoder's key is not the engine's) and the decoder's Instance ends not "
                f"serving with the status saying so; the move is recorded once and the engine holds N whole Instances")
    else:
        out += (f"; the engine's replacement cannot start either, so the move is abandoned with a reason its record "
                f"carries, or waits with the status saying why (a row not Ready with its wait or failure recorded); "
                f"the move is recorded once, nothing exceeds N+1 alive and the broken pod serves nothing")
    return out


def _move_readiness_reason(at, victim, form, topo, w, nr):
    when, pod = _move_victim_words(at, victim, "decoder")
    out = (f"{_move_request_words(topo, w, nr)} and {when}, {pod} stops passing readiness while it keeps running "
           f"(the readiness switch set in it)")
    if form == "recover":
        out += (f"; once the drop has bitten for a settle window the switch is lifted; {_move_promise()}; the moved "
                f"Instance ends whole, Ready and serving on one pod set")
        if victim != "moved":
            out += f"; the move completes and the {victim} serves again where it stood, no repair demanded (readiness is repaired under neither restart policy)"
        return out
    out += "; the switch stays set to the end of the case"
    if victim != "moved":
        out += (f"; the move completes (a {victim} that stops serving is not the move's to care about) and that "
                f"Instance ends not serving with the status saying so, no repair demanded; the move is recorded once "
                f"and the engine holds N Instances on distinct indices")
    elif at == "accept":
        out += (f"; the source is drained by the move anyway, so {_move_promise()} and the moved Instance ends whole, "
                f"Ready and serving on its replacement")
    elif at == "surge":
        out += (f"; the replacement never passes readiness, so the move is abandoned with a reason its record carries, "
                f"or waits with the status saying why (the replacement's row not Ready with its wait recorded); the move "
                f"is recorded once, the source keeps serving and nothing exceeds N+1 alive")
    else:
        out += (f"; the drain is already decided, so the move completes and the moved Instance ends not serving with "
                f"the status saying so; the move is recorded once and the engine holds N Instances on distinct indices")
    return out


def _move_podkill_reason(member, repeat, topo, w, nr, rp):
    pod = f"the {member} of one engine gang" if member else "one engine pod"
    out = (f"{_move_request_words(topo, w, nr)} and as soon as the request is recorded {pod} is deleted by hand")
    if repeat:
        out += f", and again each time its Instance is back and serving, {repeat} times in all, then left alone"
    out += (f"; the move completes end to end and is recorded once, the Instance the kill took is rebuilt under restart "
            f"policy {RP_FULL[rp]} ({_recreate_scope(topo, rp)}), the floor lowered by that one Instance holds, "
            f"nothing exceeds N+1 alive and no two gangs share an index")
    if repeat:
        out += ("; across the window the status reports each loss and never counts the short Instance whole, every "
                "repair is for an Instance the kill took, the controller creates no more pods than the move and the "
                "repairs explain, and after the lift every row is Ready with no operation open and no hold left behind")
    return out


def _move_taint_reason(lift, topo, w, nr):
    out = (f"a NoSchedule taint on every schedulable node is in place when {_move_request_words(topo, w, nr)}; the "
           f"replacement cannot be placed, so the move waits with the Instance's operation reporting Unschedulable "
           f"inside its unschedulable grace, nothing settled is taken down to make room, the engine keeps N serving and "
           f"no row reads Failed")
    if lift == "after-settle":
        out += ("; once the taint is lifted the replacement is placed, the move completes with its record Completed, "
                "the request consumed and the moved Instance whole, Ready and serving on its new placement")
    else:
        out += "; the taint stays to the end, so the wait holds with its reason across the hold window"
    if is_mn(topo):
        out += "; each Instance is a gang, placed whole"
    return out


def _move_evict_reason(at, repeat, topo, w, nr):
    when, pod = _move_victim_words(at, "moved", "decoder")
    out = (f"{_move_request_words(topo, w, nr)} and {when}, {pod} is evicted through the Eviction API, as a node "
           f"drain would, asked again while the disruption budget refuses it")
    if repeat:
        out += f", and again once what it took has come back, {repeat} times in all"
    out += (f"; the move finishes, or is abandoned with a reason its record carries and the Instance kept whole and "
            f"serving at its source index; the floor lowered by the one Instance the eviction took holds, nothing is "
            f"left mid-operation and nothing exceeds N+1 alive")
    if repeat:
        out += ("; while an Instance is short the status says so and never counts it whole, every repair is for an "
                "Instance the eviction took, and the controller creates no more pods than the move and the repairs explain")
    if is_mn(topo):
        out += "; each Instance is a gang, rebuilt whole"
    return out


def _move_preempt_reason(topo, w, nr):
    out = (f"{_move_request_words(topo, w, nr)} and as the replacement pod appears fillers take the spare CPU of every "
           f"node, the pending taint closes every node to the case, and a higher-priority filler presses on the "
           f"replacement's node, so the scheduler preempts it; the replacement waits with reason Unschedulable, nothing "
           f"settled is taken down to make room and no row reads Failed; once the lift opens the nodes and drops the "
           f"fillers the replacement is placed and the move completes with the moved Instance whole, Ready and serving; "
           f"floor and order waived, since the preemption itself takes a pod the budget cannot absorb")
    if is_mn(topo):
        out += "; each Instance is a gang, placed whole"
    return out


def _move_stuck_reason(kind, stuck, topo, w, nr, sibling):
    pushed = f"the {sibling}" if sibling else "the engine"
    out = (f"{pushed} is pushed onto a revision whose runner carries {MOVE_STUCK_HOW[kind]}; the push is left until "
           f"{MOVE_STUCK_STATE[stuck]}; then {_move_request_words(topo, w, nr)}: the move finishes, or is abandoned "
           f"with a reason its record carries, within budget and is recorded once; the moved Instance ends whole, Ready "
           f"and serving on one pod set at the index the record names (the revision its replacement runs is reported, "
           f"not judged); the Instances that served when the request was filed keep serving; the Components neither "
           f"pushed nor moved keep their pods; and the status keeps reporting the push that has not landed"
           + (", the hold itself" if stuck == "held" else ""))
    if is_mn(topo):
        out += "; each Instance is a gang, moved whole"
    return out


def _emit_fault_move_more(add):
    for topo, n, w, nr in MOVE_SHAPES:
        base = dict(mode="migrate", **counts(topo, n, nr), ceil_e=n + 1, ceil_d=(n if is_pd(topo) else 0),
                    ceil_r=router_count(topo, nr), co="none", topo=topo, mig="Auto", inst=0)
        sibling = MOVE_SIBLING[topo] if (is_pd(topo) or nr) else None
        tag = shape_tag(topo, w, nr)

        def lives(rp="None"):
            life = lib.lifecycle(restart=RP_FULL[rp])
            d_life = lib.lifecycle(restart=RP_FULL[rp]) if is_pd(topo) else None
            r_life = lib.lifecycle(restart=RP_FULL[rp]) if nr else None
            return life, d_life, r_life

        def shape_comps(rp="None"):
            life, d_life, r_life = lives(rp)
            return comps(topo, n, w, life, d_life=d_life, r_life=r_life, nr=nr)

        # A break of the running revision healed again: on the moved Instance
        # at accept and at surge, on the sibling at accept.
        for kind, form, _rp in MOVE_BREAKS:
            victims = [(at, "moved") for at in MOVE_MORE_MOMENTS] + ([("accept", "sibling")] if sibling else [])
            for at, victim in victims:
                kv = dict(base, fault=f"{kind}:old", victim=victim, at=at, lift="after-settle")
                if form:
                    kv["form"] = form
                if victim == "sibling" and sibling == "router":
                    kv["victimcomp"] = "router"
                meta = dict(strat="SurgeThenDrain", verb=f"{_storm_label(kind, form)}@move-{at}:healed", rp="None",
                            victim=(sibling if victim == "sibling" else "engine"), at=at, who=victim)
                desc = f"{GEN_MARK} fault-move {_storm_label(kind, form)} healed at {at} on {victim} {tag} N={n}"
                add("gen-fault-move", desc, kv, shape_comps(), _move_healed_reason(kind, form, at, victim, topo, w, nr, sibling),
                    meta, cell=("fault-move-healed", kind, form, at, victim, topo, w, nr))
        # A runner killed once on the moved Instance's replacement.
        for form, at, rp in MOVE_ONCE_MOMENTS:
            kv = dict(base, fault="crashloop:old", form=form, victim="moved", at=at, once=1)
            meta = dict(strat="SurgeThenDrain", verb=f"crashloop:{form}@move-{at}:once", rp=rp, victim="engine", at=at, who="moved")
            desc = f"{GEN_MARK} fault-move crashloop:{form} once at {at} on moved {tag} N={n} rp {rp}"
            add("gen-fault-move", desc, kv, shape_comps(rp), _move_once_reason(form, at, topo, w, nr, rp), meta,
                cell=("fault-move-once", form, at, topo, w, nr))
        # The missing ConfigMap key: put back, or kept away.
        key_rows = [(at, "moved", lift) for lift in MOVE_KEY_LIFTS for at in MOVE_MORE_MOMENTS] + [("accept", "peer", "after-settle")]
        if is_pd(topo):
            key_rows += [("accept", "sibling", lift) for lift in MOVE_KEY_LIFTS]
        for at, victim, lift in key_rows:
            kv = dict(base, fault="configkey:missing", victim=victim, at=at, lift=lift)
            victim_comp = "decoder" if victim == "sibling" else "engine"
            meta = dict(strat="SurgeThenDrain", verb=f"configkey@move-{at}:{lift}", rp="None", victim=victim_comp, at=at, who=victim)
            desc = f"{GEN_MARK} fault-move configkey {lift} at {at} on {victim} {tag} N={n}"
            life, d_life, _r = lives()
            add("gen-fault-move", desc, kv, None, _move_key_reason(at, victim, lift, topo, w, nr), meta,
                cell=("fault-move-key", at, victim, lift, topo, w, nr),
                body=lambda name, vc=victim_comp, life=life, d_life=d_life: _move_configkey_body(name, topo, n, w, nr, life, d_life, vc))
        # The readiness switch: lifted again, or kept.
        ready_rows = [(at, "moved", "recover") for at in MOVE_MORE_MOMENTS] + [("accept", "peer", "recover")]
        ready_rows += [(at, "moved", "persist") for at in MOVE_READINESS_KEPT_MOMENTS] + [("accept", "peer", "persist")]
        if is_pd(topo):
            ready_rows += [("accept", "sibling", form) for form in MOVE_READINESS_FORMS]
        for at, victim, form in ready_rows:
            kv = dict(base, fault="readiness:old", form=form, victim=victim, at=at)
            victim_comp = "decoder" if victim == "sibling" else "engine"
            meta = dict(strat="SurgeThenDrain", verb=f"readiness:{form}@move-{at}", rp="None", victim=victim_comp, at=at, who=victim)
            desc = f"{GEN_MARK} fault-move readiness {form} at {at} on {victim} {tag} N={n}"
            add("gen-fault-move", desc, kv, shape_comps(), _move_readiness_reason(at, victim, form, topo, w, nr), meta,
                cell=("fault-move-readiness", at, victim, form, topo, w, nr))
        # A pod deleted by hand at the request: once, and again under repeat=.
        members = GANG_MEMBERS if is_mn(topo) else [None]
        rp = "RecreateInstance" if is_mn(topo) else "None"
        for member in members:
            for repeat in (None, MOVE_REPEAT_N):
                kv = dict(base, fault="podkill:migrating")
                if member:
                    kv["kill"] = member
                if repeat:
                    kv["repeat"] = repeat
                meta = dict(strat="SurgeThenDrain", verb="podkill:migrating" + (f":{member}" if member else "") + (f" x{repeat}" if repeat else ""),
                            rp=rp, victim="engine", at="accept", who="any")
                desc = (f"{GEN_MARK} fault-move podkill at accept {tag} N={n} rp {rp}"
                        + (f" target {member}" if member else "") + (f" x{repeat}" if repeat else ""))
                add("gen-fault-move", desc, kv, shape_comps(rp), _move_podkill_reason(member, repeat, topo, w, nr, rp), meta,
                    cell=("fault-move-podkill", member, repeat, topo, w, nr))
        # A taint before the request: the replacement has nowhere to go.
        for lift in MOVE_PENDING_LIFTS:
            kv = dict(base, fault="taint:all", lift=lift)
            meta = dict(strat="SurgeThenDrain", verb=f"pending:taint:move:{lift}", rp="None", victim="engine", at="accept", who="moved", lift=lift)
            desc = f"{GEN_MARK} fault-move pending taint {tag} N={n} lift {lift}"
            add("gen-fault-move", desc, kv, shape_comps(), _move_taint_reason(lift, topo, w, nr), meta,
                cell=("fault-move-pending", "taint", lift, topo, w, nr))
        # The moved Instance's pod evicted at each moment, again under repeat=, and preempted as it appears.
        for at in MOVE_HOOK_MOMENTS:
            for repeat in ([None, MOVE_REPEAT_N] if at == MOVE_EVICT_REPEAT_AT else [None]):
                kv = dict(base, fault="evict:migrating", victim="moved", at=at)
                if repeat:
                    kv["repeat"] = repeat
                meta = dict(strat="SurgeThenDrain", verb=f"evict@move-{at}" + (f" x{repeat}" if repeat else ""), rp="None",
                            victim="engine", at=at, who="moved")
                desc = f"{GEN_MARK} fault-move evict at {at} on moved {tag} N={n}" + (f" x{repeat}" if repeat else "")
                add("gen-fault-move", desc, kv, shape_comps(), _move_evict_reason(at, repeat, topo, w, nr), meta,
                    cell=("fault-move-evict", at, repeat, topo, w, nr))
        kv = dict(base, fault="preempt:migrating", victim="moved", at=MOVE_PREEMPT_AT, lift="after-settle")
        meta = dict(strat="SurgeThenDrain", verb=f"preempt@move-{MOVE_PREEMPT_AT}", rp="None", victim="engine",
                    at=MOVE_PREEMPT_AT, who="moved", lift="after-settle")
        desc = f"{GEN_MARK} fault-move preempt at {MOVE_PREEMPT_AT} on moved {tag} N={n} lift after-settle"
        add("gen-fault-move", desc, kv, shape_comps(), _move_preempt_reason(topo, w, nr), meta,
            cell=("fault-move-preempt", MOVE_PREEMPT_AT, topo, w, nr))
        # A move requested while a push is stuck on a broken revision.
        strat, surge, unavail = MOVE_STUCK_STRAT
        for kind, states in MOVE_STUCK_BREAKS:
            for stuck in states:
                kv = dict(base, ceil_e=n + 1 + surge, stuck=stuck)
                if kind == "imagepull":
                    kv["img"] = runtime_fixtures.UNPULLABLE_IMAGE
                else:
                    kv["break"] = kind
                ready_timeout = MOVE_STUCK_READY_TIMEOUT if kind == "stuck" else None
                life = lib.lifecycle(strat=strat, surge=surge, unavail=unavail, ready_timeout=ready_timeout)
                d_life = lib.lifecycle() if is_pd(topo) else None
                meta = dict(strat=strat, verb=f"push:{stuck}:{kind}+move", rp="None", stuck=stuck, victim="engine", at="accept", who="moved")
                desc = f"{GEN_MARK} fault-move stuck {stuck} {kind} {tag} N={n} {strat} {budget_tag(strat, surge, unavail)}"
                if kind == "configkey":
                    add("gen-fault-move", desc, kv, None, _move_stuck_reason(kind, stuck, topo, w, nr, None), meta,
                        cell=("fault-move-stuck", kind, stuck, topo, w, nr),
                        body=lambda name, life=life, d_life=d_life: _move_configkey_body(name, topo, n, w, nr, life, d_life, "engine"))
                else:
                    add("gen-fault-move", desc, kv, comps(topo, n, w, life, d_life=d_life, nr=nr),
                        _move_stuck_reason(kind, stuck, topo, w, nr, None), meta,
                        cell=("fault-move-stuck", kind, stuck, topo, w, nr))
        if is_pd(topo):
            # The decoder's push stuck beside the engine's move.
            kv = dict(base, ceil_d=n + surge, stuck="held", img=runtime_fixtures.UNPULLABLE_IMAGE, bump="decoder")
            d_life = lib.lifecycle(strat=strat, surge=surge, unavail=unavail)
            meta = dict(strat=strat, verb="push:held:imagepull:decoder+move", rp="None", stuck="held", victim="decoder", at="accept", who="sibling")
            desc = f"{GEN_MARK} fault-move stuck held imagepull on decoder {tag} N={n} {strat} {budget_tag(strat, surge, unavail)}"
            add("gen-fault-move", desc, kv, comps(topo, n, w, lib.lifecycle(), d_life=d_life, nr=nr),
                _move_stuck_reason("imagepull", "held", topo, w, nr, "decoder"), meta,
                cell=("fault-move-stuck", "imagepull", "held", "decoder", topo, w, nr))
    _emit_fault_move_quota(add)


# ── a quota under the move ──────────────────────────────────────────────────
# The request meets a quota with room for no further pod: the replacement
# cannot be admitted, so the move waits on the quota and ends Failed at its
# record deadline with the source still serving (kept), and once the quota
# is lifted the Instance is moved (lifted). The product bounds that wait by
# the Instance ready timeout, so the cells set it to the stuck cells' knob
# and the deadline fires inside the harness budget. The shapes the
# hand-written fault cases do not run, as (topology, routers on an
# engine-only shape); minted after the family's other rows.
MOVE_QUOTA_KEPT = [("pd-mn", 0), ("engine-mn", lib.NR), ("engine", lib.NR)]
MOVE_QUOTA_LIFTED = [("pd-mn", 0), ("pd", 0), ("engine-mn", lib.NR), ("engine", lib.NR)]
MOVE_QUOTA_READY_TIMEOUT = MOVE_STUCK_READY_TIMEOUT


def _move_quota_reason(lifted, topo, w, nr):
    gang = ", a gang that moves whole, so the quota keeps its replacement gang from forming" if w else ""
    out = (f"{_move_request_words(topo, w, nr)} with a quota with room for no further pod in place{gang}: the replacement "
           f"cannot be admitted, so the move waits on the quota with its row naming the wait and ends Failed within its "
           f"bound, at its record deadline (the Instance ready timeout), with the source Instance still Ready and "
           f"serving, the request annotation consumed and its ledger entry terminal (the slot freed); nothing exceeds "
           f"N+1 alive and nothing settled is taken down to make room")
    if not lifted:
        out += "; the quota stays to the end, so the source keeps serving and no further move is attempted"
    else:
        out += ("; then the quota is lifted and a fresh request moves the Instance (the waiting one, if the lift lands "
                "first): it completes, the source drains and the Instance ends whole on its replacement, still within "
                "N+1 alive")
    if is_pd(topo):
        out += "; decoder and router are negative controls (co=none)"
    elif nr:
        out += "; the router is a negative control (co=none)"
    return out


def _emit_fault_move_quota(add):
    for topo, n, w, nr in MOVE_SHAPES:
        for lifted, shapes in ((False, MOVE_QUOTA_KEPT), (True, MOVE_QUOTA_LIFTED)):
            if (topo, nr) not in shapes:
                continue
            kv = dict(mode="migrate", **counts(topo, n, nr), ceil_e=n + 1, ceil_d=(n if is_pd(topo) else 0),
                      ceil_r=router_count(topo, nr), co="none", topo=topo, mig="Auto", inst=0, fault="quota:0", expect="fail")
            if lifted:
                kv["lift"] = 1
            label = "lifted" if lifted else "kept"
            meta = dict(strat="SurgeThenDrain", verb=f"quota@move:{label}", rp="None", victim="engine", at="accept", who="moved")
            desc = f"{GEN_MARK} fault-move quota {label} {shape_tag(topo, w, nr)} N={n}"
            life = lib.lifecycle(ready_timeout=MOVE_QUOTA_READY_TIMEOUT)
            add("gen-fault-move", desc, kv, comps(topo, n, w, life, d_life=(lib.lifecycle() if is_pd(topo) else None), nr=nr),
                _move_quota_reason(lifted, topo, w, nr), meta, cell=("fault-move-quota", label, topo, w, nr))


# ── gen-webhook / gen-conflict: the admission webhook unreachable, a competing writer ──
# A window fault holds a cluster condition over the settled service, a push
# or a scale-up: the admission webhook unreachable (every webhook that names
# its Service pointed at a Service that does not exist, so every write the
# webhooks intercept is refused until they are pointed back) or a competing
# writer bumping the resourceVersion the controller's status writes carry.
# Every shape family of the coverage ledger at one gang width: the engine
# alone, the engine behind a router, the prefill/decode pair, each
# single-pod and as gangs. (topo, N, W, routers on an engine-only shape.)
WINDOW_SHAPES = [("engine", 4, 0, 0), ("engine-mn", 2, 1, 0), ("engine", 4, 0, lib.NR), ("engine-mn", 2, 1, lib.NR),
                 ("pd", 4, 0, 0), ("pd-mn", 2, 1, 0)]
WINDOW_OPS = ["steady", "push", "scale"]
# The push under the webhook runs both pod-rebuilding strategies: the outage
# refuses the pod a surge creates before anything is torn down, and the one
# a drain-first roll creates after a pod is gone. The writer's push, the
# steady state and the scale run under the canonical surge budget.
WEBHOOK_PUSH_STRATS = [("SurgeThenDrain", 1, 0), ("RecreatePod", 0, 1)]
WINDOW_STRAT = ("SurgeThenDrain", 1, 0)
WEBHOOK_LIFTS = ["after-hold", "after-window"]
CONFLICT_FORMS = ["once", "persist"]
CONFLICT_TARGETS = ["isvc", "ir"]
# The delete window runs against the InferenceReplicas alone; the ISVC cells
# of that row are minted and kept empty so the ids after them keep their places.
CONFLICT_DELETE_ISVC_WHY = ("a write on the ISVC as the delete lands has no window: the controller removes the ISVC "
                            "finalizer in the reconcile that sees the deletion, so every write after it is refused "
                            "NotFound; the InferenceReplicas outlive their pods under their own finalizer and are the "
                            "only object a competing writer reaches during a teardown")
WINDOW_SCALE_UP = 1
# The hooks beyond the settled service, the push as it lands and the
# scale-up: (operation, the hook the window opens at). The push at its
# later hooks (the first pod of the new revision, the first pod of the old
# one on its way out); a rollback filed at the first new pod and a strategy
# edit landed there, each opening the window as it lands; a scale-down by
# one; a runtime edit of the dedicated runtime every Component inherits; a
# move opened at accept, the whole move still ahead of the window; a
# delete, then the same name re-created once the window has closed.
WINDOW_MORE_OPS = [("push", "rotating"), ("push", "draining"), ("rollback", "settling"), ("edit", "settling"),
                   ("scaledown", "settling"), ("runtime", "settling"), ("move", "migrating"), ("delete", "settling")]
WINDOW_ROLLBACK_HOOK = "rotating"
WINDOW_EDIT = ("strategy", "RecreatePod", "rotating")
WINDOW_MOVE_AT = "accept"
WINDOW_SCALE_DOWN = 1


def _window_counts(topo, n, nr):
    c = counts(topo, n)
    if nr and not is_pd(topo):
        c["Nr"] = nr
    return c


def _window_ceilings(topo, n, nr, strat, surge, peak_e=None):
    c = _window_counts(topo, n, nr)
    ce = peak_e if peak_e is not None else (n + surge_count(n, surge) if strat == "SurgeThenDrain" else n)
    return dict(ceil_e=ce, ceil_d=c["Nd"], ceil_r=c["Nr"])


def _window_comps(topo, n, w, nr, life):
    cs = comps(topo, n, w, life)
    if nr and not is_pd(topo):
        cs.append(lib.comp("router", nr, 0, lib.lifecycle()))
    return cs


def _window_shape(topo, w, nr):
    """The shape family in words, for the reason."""
    base = "prefill/decode" if is_pd(topo) else ("engine behind a router" if nr else "engine alone")
    return base + (f" as gangs of 1 leader + {w} worker{'s' if w > 1 else ''}" if is_mn(topo) else ", single-pod")


def _window_tag(topo, w, nr):
    return shape_tag(topo, w) + ("+router" if nr and not is_pd(topo) else "")


def _window_controls(topo, nr):
    if is_pd(topo):
        return "; decoder and router are negative controls (co=none)"
    if nr:
        return "; the router is a negative control (co=none)"
    return ""


def _window_claims(op):
    """What every window case asserts of the controller while the fault holds."""
    if op == "steady":
        return ("nothing may move: no pod created or taken down, every Instance keeps serving at N, the rows follow "
                "every change of the pods in rotation within the configured requeues and the manager's events "
                "stay bounded")
    if op == "push":
        return ("the capacity floor (N minus maxUnavailable) holds, the rows follow every change of the pods in "
                "rotation within the configured requeues, and the manager's events stay bounded")
    return ("nothing running may be taken down, the N Instances keep serving, the rows follow every change of the "
            "pods in rotation within the configured requeues, and the manager's events stay bounded")


def _window_end(op):
    """What every window case asserts once the fault is gone."""
    if op == "steady":
        return "the rows still read Ready with no operation open and no hold, retry block or Failed row left"
    if op == "push":
        return ("the roll completes with every Instance on the new revision, each pod slot created once (more is "
                "a double apply), the rows settled with no hold, retry block or Failed row left")
    return ("the new Instance comes up whole and serving, created once, the survivors keep their pods, and the rows "
            "settle with no hold, retry block or Failed row left")


def _webhook_window(op, lift):
    if lift == "after-window":
        return "the webhook stays unreachable across the hold window, then its webhooks are pointed back"
    if op == "steady":
        return "the webhooks are pointed back after the settle window"
    return "the webhooks are pointed back once the status has said why (or the reason wait has run out)"


def _webhook_reason(op, lift, topo, w, nr, strat):
    shape = _window_shape(topo, w, nr)
    if op == "steady":
        out = (f"the admission webhook is made unreachable on the settled service ({shape}): its webhooks are pointed "
               f"at a Service that does not exist, so every write the webhooks intercept is refused while status writes pass; "
               f"{_window_claims(op)}; the manager's reconciles stay bounded; {_webhook_window(op, lift)}, admission must "
               f"answer again and {_window_end(op)}")
    elif op == "push":
        pod = ("the surge beside a running Instance" if strat == "SurgeThenDrain"
               else "the replacement of the drained Instance")
        out = (f"the bump lands under {strat} on {shape}, then the admission webhook is made unreachable: the next pod "
               f"the roll creates ({pod}) is refused by admission, so the roll must wait and the status must say why in "
               f"the webhook's own words (an event on the ISVC or an InferenceReplica, a row failure or wait, a hold or "
               f"a condition) while {_window_claims(op)}; the manager's reconciles stay bounded; {_webhook_window(op, lift)}, "
               f"and {_window_end(op)}")
    else:
        out = (f"the engine ({shape}) is scaled from N to N+{WINDOW_SCALE_UP}, then the admission webhook is made "
               f"unreachable: the new Instance's pod is refused by admission, so the scale must wait with the reason "
               f"stated in the webhook's own words while {_window_claims(op)}; the manager's reconciles stay bounded; "
               f"{_webhook_window(op, lift)}, and {_window_end(op)}")
    return out + _window_controls(topo, nr)


def _conflict_writes(op, form, target):
    what = "the ISVC" if target == "isvc" else "every InferenceReplica of the service"
    when = {"steady": "on the settled service", "push": "as the bump lands", "scale": "as the scale lands"}[op]
    if form == "once":
        return f"a competing writer patches an annotation of {what} once {when}"
    if op == "steady":
        return f"a competing writer patches an annotation of {what} every few hundred milliseconds across the hold window"
    return f"a competing writer patches an annotation of {what} every few hundred milliseconds from the moment the operation lands until it completes"


def _conflict_reason(op, form, target, topo, w, nr):
    shape = _window_shape(topo, w, nr)
    lead = (f"{_conflict_writes(op, form, target)} ({shape}), so the controller's next status write carries a stale "
            f"resourceVersion and conflicts; the conflict must be retried without a lost decision")
    if op == "steady":
        out = f"{lead}: {_window_claims(op)} (a conflict storm is a hot loop); once the writer stops, {_window_end(op)}"
    elif op == "push":
        out = (f"{lead}: {_window_claims(op)} (a lost write is a stale row, a conflict retried as a fresh decision is a "
               f"double apply, a conflict storm is a hot loop); {_window_end(op)}")
    else:
        out = f"{lead}: {_window_claims(op)}; {_window_end(op)}"
    return out + _window_controls(topo, nr)


def _window_kv(kind, op, topo, n, nr, strat, surge, knobs):
    """The header of one window case: the counts and ceilings of its
    operation, the fault at its phase, and the kind's own knobs."""
    if op == "steady":
        kv = dict(**_window_counts(topo, n, nr), **_window_ceilings(topo, n, nr, strat, 0, peak_e=n))
        kv.update(fault=f"{kind}:idle", co="none", topo=topo, during="idle")
    elif op == "push":
        kv = dict(**_window_counts(topo, n, nr), **_window_ceilings(topo, n, nr, strat, surge))
        kv.update(fault=f"{kind}:settling", co="none", topo=topo)
    else:
        kv = dict(mode="scaledown", **_window_counts(topo, n, nr),
                  **_window_ceilings(topo, n, nr, strat, 0, peak_e=n + WINDOW_SCALE_UP))
        kv.update(fault=f"{kind}:settling", co="none", topo=topo, to_e=n + WINDOW_SCALE_UP)
    kv.update(knobs)
    return kv


def emit_webhook(add):
    for topo, n, w, nr in WINDOW_SHAPES:
        for op in WINDOW_OPS:
            strats = WEBHOOK_PUSH_STRATS if op == "push" else [WINDOW_STRAT]
            for strat, surge, unavail in strats:
                for lift in WEBHOOK_LIFTS:
                    kv = _window_kv("webhook", op, topo, n, nr, strat, surge, dict(lift=lift))
                    meta = dict(strat=strat if op == "push" else "(dormant: nothing rolls)",
                                verb=f"webhook:{'idle' if op == 'steady' else 'settling'}", winop=f"webhook {op}",
                                lift=lift, op=op)
                    desc = (f"{GEN_MARK} webhook {op} {_window_tag(topo, w, nr)} N={n}"
                            + (f" {strat} {budget_tag(strat, surge, unavail)}" if op == "push" else "")
                            + f" lift {lift}")
                    life = lib.lifecycle(strat=strat, surge=surge, unavail=unavail)
                    add("gen-webhook", desc, kv, _window_comps(topo, n, w, nr, life),
                        _webhook_reason(op, lift, topo, w, nr, strat), meta,
                        cell=("webhook", op, topo, w, nr, strat, lift))
    _emit_more_windows(add, "webhook")
    _emit_canary_windows(add, "webhook")


def emit_conflict(add):
    strat, surge, unavail = WINDOW_STRAT
    life = lib.lifecycle(strat=strat, surge=surge, unavail=unavail)
    for topo, n, w, nr in WINDOW_SHAPES:
        for op in WINDOW_OPS:
            for target in CONFLICT_TARGETS:
                for form in CONFLICT_FORMS:
                    kv = _window_kv("conflict", op, topo, n, nr, strat, surge, dict(form=form, target=target))
                    meta = dict(strat=strat if op == "push" else "(dormant: nothing rolls)",
                                verb=f"conflict:{'idle' if op == 'steady' else 'settling'}", winop=f"conflict {op}",
                                cform=form, target=target, op=op)
                    desc = (f"{GEN_MARK} conflict {op} {_window_tag(topo, w, nr)} N={n}"
                            + (f" {strat} {budget_tag(strat, surge, unavail)}" if op == "push" else "")
                            + f" {form} on {target}")
                    add("gen-conflict", desc, kv, _window_comps(topo, n, w, nr, life),
                        _conflict_reason(op, form, target, topo, w, nr), meta,
                        cell=("conflict", op, topo, w, nr, form, target))
    _emit_more_windows(add, "conflict")
    _emit_canary_windows(add, "conflict")


# ── a window at a canary step ───────────────────────────────────────────────
# The webhook outage and the competing writer open while the second step of
# the engine unit's ladder soaks its split, on the ledger's six shape
# families (the gate family's shapes): the step waits with a reason while
# admission refuses and the ladder completes once the webhook is back; a
# competing write is retried and the step moves once. Minted after each
# family's other rows, so those keep their ids.
CANARY_WINDOW_HOOK = "canary-serving@1"
CANARY_WINDOW_STATE = "Serving[soak]"
CANARY_WINDOW_KNOBS = {"webhook": [("after-hold", "afterHold", dict(lift="after-hold")),
                                   ("after-window", "afterWindow", dict(lift="after-window"))],
                       "conflict": [("once", "once", dict(form="once", target="isvc")),
                                    ("persist", "persist", dict(form="persist", target="isvc"))]}
CANARY_WINDOW_EVENT = {"webhook": "webhookDown", "conflict": "competingWriter"}


def _canary_window_promise():
    return ("the ladder walks every step to Stable with the step index never going back, each pod slot of the unit "
            "created once, every Instance on the canary revision serving, the rows following the pods in rotation "
            "within the configured requeues, the manager's events bounded and nothing left held")


def _canary_webhook_reason(lift, family, n, w):
    shape = _gate_shape_words(family, n, w)
    back = ("the webhook stays unreachable across the hold window, then its webhooks are pointed back"
            if lift == "after-window" else
            "the webhooks are pointed back once the status has said why (or the reason wait has run out)")
    return (f"the admission webhook is made unreachable while the second step of the engine unit's ladder soaks its "
            f"split on {shape}: its webhooks are pointed at a Service that does not exist, so every write the webhooks "
            f"intercept is refused while status writes pass; the next pod the ladder creates is refused by admission, so "
            f"the step must wait and the status must say why in the webhook's own words (an event on the ISVC or an "
            f"InferenceReplica, a row failure or wait, a hold or a condition) while the stable side keeps serving and the "
            f"rows follow the pods in rotation; the manager's reconciles stay bounded; {back}, admission must answer again, "
            f"and {_canary_window_promise()}")


def _canary_conflict_reason(form, family, n, w):
    shape = _gate_shape_words(family, n, w)
    writes = ("one competing write bumps the ISVC's resourceVersion while the second step of the engine unit's ladder "
              "soaks its split" if form == "once" else
              "a competing writer bumps the ISVC's resourceVersion every few hundred milliseconds from the second "
              "step's soak until the ladder ends")
    return (f"{writes} on {shape}, so the controller's status writes carry a stale resourceVersion, conflict and are "
            f"retried; {_canary_window_promise()}")


def _emit_canary_windows(add, kind):
    folder = f"gen-{kind}"
    for family, topo, n, w, nr in GATE_SHAPES:
        shape = _canary_shape(topo, w, nr)
        group = gate_group(topo)
        for label, variant, knobs in CANARY_WINDOW_KNOBS[kind]:
            kv = gate_kv(topo, n, nr)
            kv.update(canary="auto", fault=f"{kind}:{CANARY_WINDOW_HOOK}", co="none", topo=topo, nophase="Failed",
                      cell=f"canary/{CANARY_WINDOW_STATE}/{CANARY_WINDOW_EVENT[kind]}[{variant}]")
            kv.update(knobs)
            reason = (_canary_webhook_reason(label, family, n, w) if kind == "webhook"
                      else _canary_conflict_reason(label, family, n, w))
            desc = f"{GEN_MARK} {kind} canary at soak {shape} N={n} {label}"
            meta = dict(strat="SurgeThenDrain", verb=f"{kind}:{CANARY_WINDOW_HOOK}", winop=f"{kind} canary", op="canary",
                        hook=CANARY_WINDOW_HOOK, rp=GATE_RP, shape=family)
            if kind == "conflict":
                meta.update(cform=label, target="isvc")
            add(folder, desc, kv, gate_comps(topo, n, w, nr, GATE_RP), reason, meta,
                cell=("canary-window", kind, label, topo, nr),
                coord=lib.canary_block(story_ladder("auto"), components=group))


def _more_window_kv(kind, op, hook, topo, n, nr, strat, surge, unavail, knobs):
    """The header and the fleet of a window case of the second pass: the
    operation's mode and knobs, its counts and ceilings, the fault at its
    hook, and the kind's own knobs."""
    counts_ = _window_counts(topo, n, nr)
    routers = counts_["Nr"]
    life = lib.lifecycle(strat=strat, surge=surge, unavail=unavail)
    kv = dict(**counts_, **_window_ceilings(topo, n, nr, strat, surge))
    kv.update(fault=f"{kind}:{hook}", co="none", topo=topo)
    if op == "rollback":
        kv.update(rollback=1, during=WINDOW_ROLLBACK_HOOK)
    elif op == "edit":
        kv.update(edit=f"{WINDOW_EDIT[0]}:{WINDOW_EDIT[1]}", during=WINDOW_EDIT[2])
    elif op == "scaledown":
        kv.update(mode="scaledown", to_e=n - WINDOW_SCALE_DOWN,
                  **_window_ceilings(topo, n, nr, strat, 0, peak_e=n))
    elif op == "runtime":
        # Every Component rolls on the edit: the decoder and the router under
        # their default surge of one.
        kv.update(mode="runtimerev", edit="env", expect="roll",
                  ceil_d=(n + 1 if is_pd(topo) else 0), ceil_r=(routers + 1 if routers else 0))
    elif op == "move":
        kv.update(mode="migrate", mig="Auto", inst=0, at=WINDOW_MOVE_AT,
                  **_window_ceilings(topo, n, nr, strat, 0, peak_e=n + 1))
    elif op == "delete":
        kv.update(mode="recreate", **_window_ceilings(topo, n, nr, strat, 0, peak_e=n))
    kv.update(knobs)
    return kv, _window_comps(topo, n, w_of(topo), nr, life)


def w_of(topo):
    """The gang width the window shapes carry for a topology."""
    return next(w for t, _n, w, _nr in WINDOW_SHAPES if t == topo)


MORE_WINDOW_DOES = {
    "rollback": "the engine is bumped and the starting revision re-applied at the first pod of the new revision",
    "edit": "the engine is bumped under SurgeThenDrain and its strategy edited to RecreatePod at the first pod of the new revision",
    "scaledown": "the engine is scaled from N to N-1",
    "runtime": "the dedicated runtime every Component inherits gets a new runner env, so every Component rolls onto its own new revision",
    "move": "one engine Instance is asked to move",
    "delete": "the service is deleted",
}
MORE_WINDOW_HOOK = {
    ("push", "rotating"): "at the first pod of the new revision",
    ("push", "draining"): "as the first pod of the old revision goes",
    ("rollback", "settling"): "as the rollback lands",
    ("edit", "settling"): "as the edit lands",
    ("scaledown", "settling"): "as the scale-down lands",
    ("runtime", "settling"): "as the edit lands",
    ("move", "migrating"): "as soon as the move's request is recorded, the whole move still ahead",
    ("delete", "settling"): "as the delete lands",
}
MORE_WEBHOOK_REFUSES = {
    ("push", "rotating"): "the next pod the roll creates is refused by admission, so the roll must wait",
    ("push", "draining"): "the replacement the roll creates for the pod that went is refused by admission, so the roll must wait",
    ("rollback", "settling"): "the pods the rollback creates to bring its Instances back are refused by admission, so the rollback must wait",
    ("edit", "settling"): "the pods the roll creates under the edited strategy are refused by admission, so the roll must wait",
    ("scaledown", "settling"): "the InferenceReplica spec write that shrinks the Component is refused by admission, so the scale must wait",
    ("runtime", "settling"): "the pods the edit's roll creates are refused by admission, so the roll must wait",
    ("move", "migrating"): "the replacement pod the move creates is refused by admission, so the move must wait",
    ("delete", "settling"): "the InferenceService update that releases its finalizer is refused by admission, so the delete must wait (or finish on the writes admission does not gate)",
}
MORE_WINDOW_CLAIMS = {
    "push": _window_claims("push"),
    "rollback": _window_claims("push"),
    "edit": _window_claims("push"),
    "runtime": ("the capacity floor of every Component holds, the rows follow every change of the pods in rotation "
                "within the configured requeues, and the manager's events stay bounded"),
    "scaledown": ("no Instance below the target is taken down, the survivors keep serving, the rows follow every change "
                  "of the pods in rotation within the configured requeues, and the manager's events stay bounded"),
    "move": ("the N Instances keep serving (the source until its replacement serves), the rows follow every change of "
             "the pods in rotation within the configured requeues, and the manager's events stay bounded"),
    "delete": "the rows follow the pods on their way out and the manager's events stay bounded",
}
MORE_WINDOW_END = {
    "push": _window_end("push"),
    "rollback": ("the rollback completes with every Instance back on the starting revision and serving, no pod of the "
                 "new revision left, each pod slot created at most once in each direction, the rows settled with no "
                 "hold, retry block or Failed row left"),
    "edit": ("the roll completes under the edited strategy with every Instance on the new revision and the edit on "
             "the live spec, each pod slot created at most once, the rows settled with no hold, retry block or Failed "
             "row left"),
    "scaledown": ("the scale completes with the engine at N-1, the survivors on their own pods, nothing created, and "
                  "the rows settled with no hold, retry block or Failed row left"),
    "runtime": ("the edit lands on every Instance of every Component, each on its own new revision and serving, each "
                "pod slot created once, the rows settled with no hold, retry block or Failed row left"),
    "move": ("the move completes: its ledger row reads Completed once, the request annotation is consumed, the moved "
             "Instance ends whole, Ready and serving off its source pod and created once, the engine holds N whole "
             "Instances, and the rows settle with no hold, retry block or Failed row left"),
    "delete": ("the delete completes with no pod, InferenceReplica, Service or InferenceService left, and the same "
               "manifest re-applied under the same name comes up whole on fresh pods"),
}


def _more_webhook_reason(op, hook, lift, topo, w, nr, strat):
    shape = _window_shape(topo, w, nr)
    does = MORE_WINDOW_DOES.get(op, f"the bump lands under {strat}")
    out = (f"{does} on {shape}, then the admission webhook is made unreachable {MORE_WINDOW_HOOK[(op, hook)]}: "
           f"{MORE_WEBHOOK_REFUSES[(op, hook)]} and the status must say why in the webhook's own words (an event on "
           f"the ISVC or an InferenceReplica, a row failure or wait, a hold or a condition) while "
           f"{MORE_WINDOW_CLAIMS[op]}; the manager's reconciles stay bounded; {_webhook_window(op, lift)}, and "
           f"{MORE_WINDOW_END[op]}")
    return out + ("" if op == "runtime" else _window_controls(topo, nr))


def _more_conflict_reason(op, hook, form, target, topo, w, nr):
    shape = _window_shape(topo, w, nr)
    what = "the ISVC" if target == "isvc" else "every InferenceReplica of the service"
    does = MORE_WINDOW_DOES.get(op, "the engine is bumped under the canonical surge budget")
    if form == "once":
        writes = f"a competing writer patches an annotation of {what} once {MORE_WINDOW_HOOK[(op, hook)]}"
    else:
        writes = (f"a competing writer patches an annotation of {what} every few hundred milliseconds from "
                  f"{MORE_WINDOW_HOOK[(op, hook)]} until the operation completes")
    out = (f"{does} on {shape}, and {writes}, so the controller's next status write carries a stale "
           f"resourceVersion and conflicts; the conflict must be retried without a lost decision: "
           f"{MORE_WINDOW_CLAIMS[op]} (a lost write is a stale row, a conflict retried as a fresh decision is a "
           f"double apply, a conflict storm is a hot loop); {MORE_WINDOW_END[op]}")
    return out + ("" if op == "runtime" else _window_controls(topo, nr))


def _emit_more_windows(add, kind):
    """The second pass of a window family: every (operation, hook) of
    WINDOW_MORE_OPS on every shape, under the kind's own knobs; appended
    after the first pass so the committed ids keep their places."""
    for topo, n, w, nr in WINDOW_SHAPES:
        for op, hook in WINDOW_MORE_OPS:
            if op == "push" and kind == "webhook":
                strats = WEBHOOK_PUSH_STRATS
            elif op == "edit":
                strats = [("SurgeThenDrain",) + switch_budget("SurgeThenDrain", WINDOW_EDIT[1])]
            else:
                strats = [WINDOW_STRAT]
            for strat, surge, unavail in strats:
                knob_sets = ([dict(lift=lift) for lift in WEBHOOK_LIFTS] if kind == "webhook"
                             else [dict(form=form, target=target) for target in CONFLICT_TARGETS for form in CONFLICT_FORMS])
                for knobs in knob_sets:
                    kv, case_comps = _more_window_kv(kind, op, hook, topo, n, nr, strat, surge, unavail, knobs)
                    label = f"{op} {hook}" if op == "push" else op
                    meta = dict(strat=strat if op in ("push", "rollback", "edit") else "(dormant: nothing rolls)",
                                verb=f"{kind}:{hook}", winop=f"{kind} {label}", op=op, hook=hook, **knobs)
                    if kind == "conflict":
                        meta["cform"] = meta.pop("form")
                    desc = (f"{GEN_MARK} {kind} {label} {_window_tag(topo, w, nr)} N={n}"
                            + (f" {strat} {budget_tag(strat, surge, unavail)}" if op in ("push", "edit") else "")
                            + (f" lift {knobs['lift']}" if kind == "webhook" else f" {knobs['form']} on {knobs['target']}"))
                    reason = (_more_webhook_reason(op, hook, knobs["lift"], topo, w, nr, strat) if kind == "webhook"
                              else _more_conflict_reason(op, hook, knobs["form"], knobs["target"], topo, w, nr))
                    no_window = kind == "conflict" and op == "delete" and knobs["target"] == "isvc"
                    add(f"gen-{kind}", desc, kv, case_comps, reason, meta,
                        cell=(kind, op, hook, topo, w, nr, strat) + tuple(knobs[k] for k in sorted(knobs)),
                        dedicated=(op == "runtime"), empty_why=CONFLICT_DELETE_ISVC_WHY if no_window else None)


# ── gen-fault-edit: a fault during a runtime edit ────────────────────────────
# The runtime every Component inherits is edited, so engine, decoder and
# router each roll onto their own new revision. As the edit lands a pod of
# the running revision breaks (the roll owns it), or the edit itself is bad
# (it holds with a reason, the old revision keeps serving, a corrective edit
# lands). The six shape families of the coverage ledger at one gang width;
# the surge and the drain-first strategies. (topo, N, W, routers on an
# engine-only shape.) Two further halves run one case per ledger cell under
# the surge strategy and the env edit: a break of the running revision
# landed on the settled service before the edit (during=idle; each kind
# once and kept, a sibling once), and a break the edited runtime itself
# carries (break=<kind>; kept with the corrective edit, and once).
EDIT_SHAPES = [("engine", 4, 0, 0), ("pd", 4, 0, 0), ("engine-mn", 2, 1, 0),
               ("engine", 4, 0, lib.NR), ("engine-mn", 2, 1, lib.NR), ("pd-mn", 2, 1, 0)]
EDIT_STRATS = {"SurgeThenDrain": (1, 0), "RecreatePod": (0, 1)}
# (kind, form, the restart policies the cell runs under): a crash at start
# and a pull failure open no runner-restart trigger; the after-Ready form
# does, so it runs under both.
EDIT_BREAKS = [("crashloop", "start", ["None"]), ("imagepull", None, ["None"]),
               ("crashloop", "load", ["None", "RecreateInstance"])]
# The break victims per topo: the decoder is the sibling a break may hit
# (outside a canary the runner admits engine and decoder pods), so a router
# rolls on the edit as a negative control and is never the victim.
EDIT_VICTIMS = {"engine": ["engine"], "pd": ["engine", "decoder"], "engine-mn": ["engine"], "pd-mn": ["engine", "decoder"]}
# The image edit on every shape; the env edit on the single-pod engine
# shapes as the control (the same new revision under these strategies).
EDIT_VERBS = {"engine": ["image", "env"], "pd": ["image"], "engine-mn": ["image"], "pd-mn": ["image"]}
# The bad edit: an image that cannot be pulled (expect=recover) and a
# readiness that never passes (expect=recover break=stuck).
BAD_EDITS = [("imagepull", None), ("readiness", "stuck")]
EDIT_RP_FOR_BAD = "None"
EDIT_LANDS = {"SurgeThenDrain": "the surge brings the fixed replacement up beside it and drains it",
              "RecreatePod": "the recreate drains it and rebuilds the Instance at the edited revision"}


def _edit_break_reason(kind, form, victim, edit, strat, rp, topo, nr=0):
    what = "a new image" if edit == "image" else "a new runner env"
    rolls = ("engine, decoder and router roll, each onto its own new revision" if is_pd(topo)
             else "engine and router roll, each onto its own new revision" if nr
             else "the engine rolls onto a new revision")
    out = (f"the runtime every Component inherits gets {what}, so {rolls}; as the edit lands "
           f"{_break_how(form or 'start', victim)}; its "
           f"Instance is off the target from that moment, so the roll owns it: {EDIT_LANDS[strat]}, admitted within "
           f"the same budget as its peers, and no repair is recorded for it after the edit; every Instance of every "
           f"Component ends on the edited revision serving, the healthy Instances hold the floor lowered by the one "
           f"broken Instance, and no Instance row is left mid-operation")
    if form == "load" and rp == "None":
        out += ("; under restart policy None a runner that dies after Ready is the kubelet's restart, and the roll "
                "must still land on a pod the kubelet alone keeps restarting")
    elif form == "load":
        out += ("; under RecreateInstanceOnPodRestart the runner-restart trigger competes with the roll, and the roll "
                "must win")
    if victim != "engine":
        out += f"; the engine's own roll converges untouched by the {victim}'s break"
    if is_mn(topo):
        out += "; each Instance is a gang, replaced whole"
    return out


def _bad_edit_reason(kind, strat, topo):
    floor = ("the floor N under the surge strategy" if strat == "SurgeThenDrain" else
             "the floor N-1 under the drain-first strategy, which takes one Instance into the attempt")
    if kind == "imagepull":
        out = (f"the runtime every Component inherits is pointed at an image that cannot be pulled, so every "
               f"Component's new revision parks in ImagePullBackOff; within budget the status says why the push has "
               f"not landed (a Failed row, a retry block or a hold), the old revision keeps serving on every Instance "
               f"the attempt did not take ({floor}), and nothing promotes the broken revision; then a corrective edit "
               f"(the good image back, plus a runner env change for a distinct revision) lands on every Instance of "
               f"every Component, serving, with no row left mid-operation")
    else:
        out = (f"the runtime every Component inherits gets a readiness probe that never passes and a new runner env, "
               f"so every Component's new revision runs but is never Ready; the old revision keeps serving on every "
               f"Instance the attempt did not take ({floor}), nothing promotes the broken revision, and whether the "
               f"status says why within budget is reported (a pod that runs but never passes readiness escalates "
               f"nothing before its ready timeout); then a corrective edit (the probe restored, plus a distinct runner "
               f"env) lands on every Instance of every Component, serving, with no row left mid-operation")
    out += "; the restart policy is None: a revision that never becomes Ready restarts no runner, so it decides nothing here"
    if is_mn(topo):
        out += "; each Instance is a gang, attempted and replaced whole"
    return out


def _edit_kv(topo, n, strat, surge, nr=0):
    """Counts and ceilings of an edit case: engine and decoder roll under the
    case's strategy, the router under its default surge of one."""
    ce = n + surge if strat == "SurgeThenDrain" else n
    routers = router_count(topo, nr)
    return dict(mode="runtimerev", Ne=n, Nd=(n if is_pd(topo) else 0), Nr=routers,
                ceil_e=ce, ceil_d=(ce if is_pd(topo) else 0), ceil_r=(routers + 1 if routers else 0))


def emit_fault_edit(add):
    for topo, n, w, nr in EDIT_SHAPES:
        for strat, (surge, unavail) in EDIT_STRATS.items():
            for kind, form, policies in EDIT_BREAKS:
                for victim in EDIT_VICTIMS[topo]:
                    for edit in EDIT_VERBS[topo]:
                        for rp in policies:
                            kv = dict(_edit_kv(topo, n, strat, surge, nr), fault=f"{kind}:old", co="none", topo=topo)
                            if form:
                                kv["form"] = form
                            if victim != "engine":
                                kv["victimcomp"] = victim
                            kv.update(expect="roll", edit=edit)
                            if edit == "image":
                                kv["img"] = ALT_IMAGE
                            meta = dict(strat=strat, verb=f"{_storm_label(kind, form)}@edit", rp=rp, victim=victim,
                                        rtedit=edit)
                            desc = (f"{GEN_MARK} fault-edit {_storm_label(kind, form)} on {victim} edit {edit} "
                                    f"{shape_tag(topo, w, nr)} N={n} {strat} rp {rp}")
                            life = lib.lifecycle(strat=strat, surge=surge, unavail=unavail, restart=RP_FULL[rp])
                            add("gen-fault-edit", desc, kv, comps(topo, n, w, life, d_life=life, nr=nr),
                                _edit_break_reason(kind, form, victim, edit, strat, rp, topo, nr), meta,
                                cell=("fault-edit", kind, form, victim, edit, topo, w, strat, rp, nr), dedicated=True)
            for kind, brk in BAD_EDITS:
                kv = dict(_edit_kv(topo, n, strat, surge, nr), co="none", topo=topo, expect="recover")
                if brk:
                    kv["break"] = brk
                meta = dict(strat=strat, verb=f"bad-edit:{kind}", rp=EDIT_RP_FOR_BAD, rtedit="image")
                desc = f"{GEN_MARK} bad-edit {kind} {shape_tag(topo, w, nr)} N={n} {strat}"
                life = lib.lifecycle(strat=strat, surge=surge, unavail=unavail, restart=RP_FULL[EDIT_RP_FOR_BAD])
                add("gen-fault-edit", desc, kv, comps(topo, n, w, life, d_life=life, nr=nr),
                    _bad_edit_reason(kind, strat, topo), meta,
                    cell=("bad-edit", kind, topo, w, strat, nr), dedicated=True)
    # The router as the broken sibling on the engine+router shapes: it rolls
    # on the edit like every Component, so the roll owns its broken Instance;
    # the engine's own roll is the negative control.
    for topo, n, w, nr in EDIT_SHAPES:
        if not nr:
            continue
        for strat, (surge, unavail) in EDIT_STRATS.items():
            for kind, form, policies in EDIT_BREAKS:
                for edit in EDIT_VERBS[topo]:
                    for rp in policies:
                        kv = dict(_edit_kv(topo, n, strat, surge, nr), fault=f"{kind}:old", co="none", topo=topo)
                        if form:
                            kv["form"] = form
                        kv.update(victimcomp="router", expect="roll", edit=edit)
                        if edit == "image":
                            kv["img"] = ALT_IMAGE
                        meta = dict(strat=strat, verb=f"{_storm_label(kind, form)}@edit", rp=rp, victim="router", rtedit=edit)
                        desc = (f"{GEN_MARK} fault-edit {_storm_label(kind, form)} on router edit {edit} "
                                f"{shape_tag(topo, w, nr)} N={n} {strat} rp {rp}")
                        life = lib.lifecycle(strat=strat, surge=surge, unavail=unavail, restart=RP_FULL[rp])
                        add("gen-fault-edit", desc, kv, comps(topo, n, w, life, nr=nr),
                            _edit_break_reason(kind, form, "router", edit, strat, rp, topo, nr), meta,
                            cell=("fault-edit", kind, form, "router", edit, topo, w, strat, rp, nr), dedicated=True)
    _emit_fault_edit_idle_breaks(add)
    _emit_fault_edit_carried_breaks(add)


# The further rows of gen-fault-edit: one case per ledger cell, the surge
# strategy (surge 1), the env edit, restart policy None. (label, kind, the
# fault's side token, form, lift) of a break of the running revision landed
# on the settled service before the edit; the sibling row takes the crash
# after Ready, lifted, on the decoder or the router.
EDIT_IDLE_BREAKS = [
    ("crashStartOnce", "crashloop", "old", "start", "after-settle"),
    ("crashLoadOnce", "crashloop", "old", "load", "after-settle"),
    ("imagePullOnce", "imagepull", "old", None, "after-settle"),
    ("readinessLostOnce", "readiness", "old", "recover", None),
    ("readinessLost", "readiness", "old", "persist", None),
    ("configMissingOnce", "configkey", "missing", None, "after-settle"),
    ("configMissing", "configkey", "missing", None, "never"),
]
EDIT_IDLE_SIBLING_BREAK = ("crashLoadOnce", "crashloop", "old", "load", "after-settle")
# (label, break kind, once) of a break the edited runtime itself carries on
# the victim's runners; the sibling rows take the crash at start, kept and
# once, on the decoder or the router.
EDIT_CARRIED_BREAKS = [
    ("crashStart", "crash-start", False), ("crashStartOnce", "crash-start", True),
    ("crashLoad", "crash-load", False), ("crashLoadOnce", "crash-load", True),
    ("imagePullOnce", "imagepull", True),
    ("configMissing", "configkey", False), ("configMissingOnce", "configkey", True),
    ("readinessLostOnce", "stuck", True),
]
EDIT_CARRIED_SIBLING_BREAKS = [("crashStart", "crash-start", False), ("crashStartOnce", "crash-start", True)]
EDIT_ROW_RP = "None"
# The ledger row a further gen-fault-edit case fills: the crash kinds by
# form, the rest by kind.
EDIT_ROW_KIND = {"crash-start": ("crashloop", "start"), "crash-load": ("crashloop", "load"),
                 "imagepull": ("imagepull", None), "configkey": ("configkey", None), "stuck": ("readiness", None)}


def _edit_row(kind, form, oneshot, side, victim):
    """The ledger row a further gen-fault-edit case fills: the kind's row on
    the engine, the sibling row on a decoder or a router."""
    row = _ledger_row(kind, form, oneshot, side)
    return row if victim == "engine" else "sibling broken, " + row.split(", ", 1)[1]


def _edit_sibling(topo, nr):
    """The sibling a shape has beside the engine: the decoder of a
    prefill/decode service, the router in front of an engine fleet."""
    return gate_shape_sibling(nr, topo)


def _edit_idle_kv(topo, n, nr, kind, side, form, lift, victim):
    kv = dict(_edit_kv(topo, n, "SurgeThenDrain", 1, nr), fault=f"{kind}:{side}", during=IDLE_FAULT_HOOK,
              expect="roll", edit="env", co="none", topo=topo)
    if form:
        kv["form"] = form
    if lift:
        kv["lift"] = lift
    if victim != "engine":
        kv["victimcomp"] = victim
    return kv


def _edit_configkey_body(name, topo, n, w, nr, life):
    """The body of a runtime-edit case whose engine runner reads one key of
    a ConfigMap named after the case: the ISVC on its dedicated runtime and
    the ConfigMap as a second document (the runtime itself is prepended by
    the writer)."""
    cm = f"{name}{HEAL_CONFIG_SUFFIX}"
    cs = comps(topo, n, w, life, d_life=life, nr=nr)
    cs[0]["runner"] = lib.configmap_env(cm, HEAL_CONFIG_KEY, HEAL_CONFIG_VAR, gang=is_mn(topo))
    return lib.render(name, cs, runtime=f"ome-coordtest-rt-{name}",
                      docs=[lib.configmap_doc(cm, {HEAL_CONFIG_KEY: HEAL_CONFIG_VALUE})])


def _edit_rolls_words(topo, nr):
    return ("engine, decoder and router each roll onto their own new revision" if is_pd(topo)
            else "engine and router each roll onto their own new revision" if nr
            else "the engine rolls onto a new revision")


def _edit_idle_reason(kind, form, lift, victim, topo, w, nr):
    shape = shape_tag(topo, w, nr)
    does = {("crashloop", "start"): "crashes at every start", ("crashloop", "load"): "dies after Ready and comes back between crashes",
            ("imagepull", None): "sits in ImagePullBackOff",
            ("readiness", "recover"): "stops passing readiness while it keeps running",
            ("readiness", "persist"): "stops passing readiness while it keeps running",
            ("configkey", None): "waits in CreateContainerConfigError once its ConfigMap key is removed and its runner restarted"}[(kind, form)]
    settle = lib_seconds("LIFT_SETTLE_S")
    heal = {("crashloop", "start"): f"the image is put back {settle}s after the edit has fired",
            ("crashloop", "load"): f"the killer stops {settle}s after the edit has fired",
            ("imagepull", None): f"the image is put back {settle}s after the edit has fired",
            ("readiness", "recover"): f"the switch is lifted {settle}s after the edit has fired",
            ("configkey", None): f"the key is put back {settle}s after the edit has fired"}.get((kind, form))
    who = f"one old-revision {victim} pod"
    rolls = _edit_rolls_words(topo, nr)
    if kind == "configkey" and lift == "never":
        return (f"the operator edits the runtime every Component inherits (a new runner env) while the ConfigMap key "
                f"{who} reads is kept away on {shape} (the pod {does}; the status reacted before the edit); both "
                f"revisions read the key, so the edit cannot fix it: the roll parks with its reason (a Failed row at "
                f"the Instance ready timeout) and nothing of the edited revision serves while the key is away, the "
                f"healthy Instances keep serving")
    cause = "the switch stays set, so the roll's rebuild is what clears it" if (kind, form) == ("readiness", "persist") else heal
    out = (f"the operator edits the runtime every Component inherits (a new runner env) while {who} {does} on {shape}, "
           f"the break landed on the settled service and the status reacted to it first; {cause}; {rolls}, and the roll "
           f"owns the broken Instance: it ends on the edited revision serving with no repair recorded for it after the "
           f"edit, the healthy Instances hold the floor lowered by the one broken Instance, nothing exceeds its ceiling "
           f"and no Instance row is left mid-operation")
    if victim != "engine":
        out += f"; the engine's own roll converges untouched by the {victim}'s break"
    if is_mn(topo):
        out += "; each Instance is a gang, replaced whole"
    return out


def _edit_carried_reason(kind, once, victim, topo, w, nr):
    shape = shape_tag(topo, w, nr)
    rolls = _edit_rolls_words(topo, nr)
    carries = {"crash-start": "the shell-less image, so its pods crash at every start",
               "crash-load": "the fixture runtime's exit switch, so its pods serve and then exit",
               "imagepull": "an image that cannot be pulled",
               "configkey": "a second env entry reading a key the case's ConfigMap lacks, so its pods wait in CreateContainerConfigError",
               "stuck": "the readiness switch"}[kind]
    if once:
        bite = {"crash-start": "its runner is killed once at its first start and the kubelet starts it again",
                "crash-load": "its runner is killed once after Ready and the kubelet starts it again",
                "imagepull": f"it is pointed at an image that cannot be pulled as it starts and back {lib_seconds('LIFT_SETTLE_S')}s after the pull failure has shown",
                "configkey": f"it waits on a key the case's ConfigMap lacks until the key is added {lib_seconds('LIFT_SETTLE_S')}s after the wait has shown",
                "stuck": f"the readiness switch is set in it as it starts and lifted {lib_seconds('LIFT_SETTLE_S')}s after the drop has settled"}[kind]
        out = (f"the operator's runtime edit (a new runner env) re-renders every Component on {shape} and the first "
               f"pushed {victim} pod breaks once: {bite}; the roll completes with no hold behind it: nothing escalates "
               f"the one bite to Failed or holds it, the Instance ends Ready and serving on the edited revision, {rolls}, "
               f"nothing exceeds its ceiling and every row settles")
    else:
        served = ("the pod serves and then exits, so the roll's promotion is what the status must take back"
                  if kind == "crash-load" else "nothing of a revision that cannot start serves")
        out = (f"the operator's runtime edit carries a break on the {victim} runner ({carries}) while every Component "
               f"re-renders on {shape}; the roll parks on the first broken {victim} pod with the status saying why, "
               f"{served}, and the old revision keeps serving; then a corrective runtime edit (the break taken off, "
               f"plus a distinct runner env) lands on every Instance of every Component, nothing of the broken "
               f"revision is created again or left, no hold is left in status and no repair is opened for a rolled "
               f"Instance after the fix")
    if victim != "engine":
        out += f"; the engine's own roll is the negative control beside the {victim}'s break"
    if is_mn(topo):
        out += "; each Instance is a gang, attempted and replaced whole"
    return out


def _emit_fault_edit_idle_breaks(add):
    """A break of the running revision landed on the settled service before
    the runtime edit: each kind once and kept on the six shape families, and
    a sibling broken once where the shape has one."""
    life = lib.lifecycle(strat="SurgeThenDrain", surge=1, unavail=0, restart=RP_FULL[EDIT_ROW_RP])
    key_life = lib.lifecycle(strat="SurgeThenDrain", surge=1, unavail=0, restart=RP_FULL[EDIT_ROW_RP],
                             ready_timeout=SWITCH_HELD_READY_TIMEOUT)
    for topo, n, w, nr in EDIT_SHAPES:
        rows = [(label, kind, side, form, lift, "engine") for label, kind, side, form, lift in EDIT_IDLE_BREAKS]
        sibling = _edit_sibling(topo, nr)
        if sibling:
            label, kind, side, form, lift = EDIT_IDLE_SIBLING_BREAK
            rows.append((label, kind, side, form, lift, sibling))
        for label, kind, side, form, lift, victim in rows:
            kv = _edit_idle_kv(topo, n, nr, kind, side, form, lift, victim)
            oneshot = lift == "after-settle" or form == "recover"
            meta = dict(strat="SurgeThenDrain", verb=f"{label}@before-edit", rp=EDIT_ROW_RP, victim=victim, rtedit="env",
                        row=_edit_row(kind, form, oneshot, "running", victim))
            desc = f"{GEN_MARK} fault-edit {label} on {victim} before the edit {shape_tag(topo, w, nr)} N={n} SurgeThenDrain"
            case_life = key_life if kind == "configkey" else life
            case_comps = comps(topo, n, w, case_life, d_life=case_life, nr=nr)
            body = functools.partial(_edit_configkey_body, topo=topo, n=n, w=w, nr=nr, life=case_life) if kind == "configkey" else None
            add("gen-fault-edit", desc, kv, case_comps, _edit_idle_reason(kind, form, lift, victim, topo, w, nr), meta,
                cell=("fault-edit-idle", label, victim, topo, w, nr), dedicated=True, body=body)


def _emit_fault_edit_carried_breaks(add):
    """A break the edited runtime itself carries: each kind kept (with the
    corrective edit) and once on the six shape families, and the crash at
    start on the sibling, kept and once, where the shape has one."""
    for topo, n, w, nr in EDIT_SHAPES:
        rows = [(label, kind, once, "engine") for label, kind, once in EDIT_CARRIED_BREAKS]
        sibling = _edit_sibling(topo, nr)
        if sibling:
            rows += [(label, kind, once, sibling) for label, kind, once in EDIT_CARRIED_SIBLING_BREAKS]
        for label, kind, once, victim in rows:
            kv = dict(_edit_kv(topo, n, "SurgeThenDrain", 1, nr), expect="roll", edit="env", co="none", topo=topo)
            kv["break"] = kind
            if once:
                kv["once"] = 1
            if victim != "engine":
                kv["victimcomp"] = victim
            row_kind, row_form = EDIT_ROW_KIND[kind]
            meta = dict(strat="SurgeThenDrain", verb=f"{label}@edited-runtime", rp=EDIT_ROW_RP, victim=victim, rtedit="env",
                        row=_edit_row(row_kind, row_form, once, "new", victim))
            desc = f"{GEN_MARK} fault-edit {label} carried on {victim} {shape_tag(topo, w, nr)} N={n} SurgeThenDrain"
            life = lib.lifecycle(strat="SurgeThenDrain", surge=1, unavail=0, restart=RP_FULL[EDIT_ROW_RP],
                                 ready_timeout=(None if once else SWITCH_HELD_READY_TIMEOUT))
            case_comps = comps(topo, n, w, life, d_life=life, nr=nr)
            body = functools.partial(_edit_configkey_body, topo=topo, n=n, w=w, nr=nr, life=life) if kind == "configkey" else None
            add("gen-fault-edit", desc, kv, case_comps, _edit_carried_reason(kind, once, victim, topo, w, nr), meta,
                cell=("fault-edit-carried", label, victim, topo, w, nr), dedicated=True, body=body)


# ── gen-crash-once: a crash that happens exactly once ──────────────────────
# A pod's runner is killed once through exec and the kubelet starts it again:
# at its first start (form=start, before it was ever Ready) or after it is
# Ready (form=load). The running revision's pod at steady state and under a
# push, and the pushed revision's first pod under a push. Six shape
# families (the engine alone, behind a router, and with a decoder and a
# router, each single-pod and as a gang at one width), both restart
# policies, the surge and the drain-first strategies under a push (an
# in-place push starts no pod of the new revision to crash at start), no
# group (a one-shot restart is no fleet change for a group to pace). The
# operator's end state is the verdict: the Instance is Ready and serving
# again on the revision it should run, nothing was escalated to Failed or
# held, the push landed, and a repair ran only where the restart policy
# promises one.
CRASH_ONCE_SHAPES = [("engine", 4, 0, False), ("engine-mn", 2, 1, False),
                     ("engine", 4, 0, True), ("engine-mn", 2, 1, True),
                     ("pd", 4, 0, True), ("pd-mn", 2, 1, True)]
CRASH_ONCE_SIDES = ["old", "new"]
CRASH_ONCE_STRATS = [("SurgeThenDrain", 1, 0), ("RecreatePod", 0, 1)]
CRASH_ONCE_IDLE_STRAT = CRASH_ONCE_STRATS[0]


def _crash_once_counts(topo, n, router):
    if is_pd(topo):
        return counts(topo, n)
    return dict(Ne=n, Nd=0, Nr=(lib.NR if router else 0))


def _crash_once_ceilings(topo, n, router, strat, surge):
    ce = n + surge_count(n, surge) if strat == "SurgeThenDrain" else n
    if is_pd(topo):
        return dict(ceil_e=ce, ceil_d=n, ceil_r=lib.NR)
    return dict(ceil_e=ce, ceil_d=0, ceil_r=(lib.NR if router else 0))


def _crash_once_comps(topo, n, w, router, life, rp):
    cs = comps(topo, n, w, life, d_life=(lib.lifecycle(restart=RP_FULL[rp]) if is_pd(topo) else None))
    if router and not is_pd(topo):
        cs.append(lib.comp("router", lib.NR, 0, lib.lifecycle()))
    return cs


# The sibling broken once: the router behind the engine on the router
# shapes, the decoder on the prefill/decode ones; the engine is the
# negative control. (topo, N, W, a router on an engine shape, the sibling.)
CRASH_ONCE_SIBLINGS = [("engine", 4, 0, True, "router"), ("engine-mn", 2, 1, True, "router"),
                       ("pd", 4, 0, True, "decoder"), ("pd-mn", 2, 1, True, "decoder")]


def _crash_once_sibling_comps(topo, n, w, router, life, rp, sibling):
    """The crash-once fleet with the sibling under the case's restart policy,
    the Component whose repair the verdict reads."""
    cs = _crash_once_comps(topo, n, w, router, life, rp)
    if sibling == "router":
        cs[-1] = lib.comp("router", lib.NR, 0, lib.lifecycle(restart=RP_FULL[rp]))
    return cs


def _crash_once_sibling_reason(sibling, form, rp, topo):
    pod = "the leader pod of" if is_mn(topo) and sibling != "router" else "the pod of"
    when = ("during its first start, before it was ever Ready" if form == "start"
            else "once it is Ready and serving")
    out = (f"{pod} the {sibling}'s highest Instance has its runner killed once, {when}; the kubelet starts the "
           f"container again and nothing kills it twice; nothing is pushed, so the {sibling}'s own restart policy "
           f"decides the repair: {_crash_once_policy_words('old', form, rp, 'idle')}")
    out += (f"; at the end the {sibling} Instance is Ready and serving on its running revision, no row reads Failed, "
            f"no hold or retry ladder was opened for it, the rows settle with no operation open, and every pod "
            f"carries the label of what it runs")
    if sibling == "decoder":
        return out + "; the engine and the router, which nothing broke, keep their pods and serve N"
    return out + "; the engine, which nothing broke, keeps its pods and serves N"


def _crash_once_bumped_sibling_comps(topo, n, w, router, life, rp, sibling):
    """The crash-once fleet with the sibling bumped beside the engine: it
    rolls under the same strategy and restart policy."""
    cs = _crash_once_comps(topo, n, w, router, life, rp)
    if sibling == "router":
        cs[-1] = lib.comp("router", lib.NR, 0, life)
    return cs


def _crash_once_sibling_bumped_words(sibling):
    return ("the rollingUpdate group lists engine and decoder, so the bump carries both and the group paces them as one run"
            if sibling == "decoder" else
            "the engine and the router are bumped together by name and roll side by side, each under its own surge")


def _crash_once_sibling_push_reason(sibling, side, form, rp, topo):
    pod = "the leader pod of" if is_mn(topo) and sibling != "router" else "the pod of"
    when = ("during its first start, before it was ever Ready" if form == "start" else "once it is Ready and serving")
    end = ("no row reads Failed, no hold or retry ladder was opened for it, the rows settle with no operation open, and "
           "the floor allows for the one Instance the kill takes while it restarts")
    if side == "old":
        return (f"the engine is bumped alone and, as the bump lands, {pod} the {sibling}'s highest Instance has its runner "
                f"killed once, {when}; the kubelet starts the container again and nothing kills it twice; the roll never "
                f"touches the {sibling}, so its own restart policy decides the repair: "
                f"{_crash_once_policy_words('old', form, rp, 'idle')}; the SurgeThenDrain roll lands the new revision on "
                f"every engine Instance within its budget; at the end the {sibling} Instance is Ready and serving on its "
                f"running revision, {end}")
    return (f"{_crash_once_sibling_bumped_words(sibling)}; {pod} the first {sibling} Instance the push brings up has its "
            f"runner killed once, {when}; the kubelet starts the container again and nothing kills it twice; "
            f"{_crash_once_policy_words('new', form, rp, 'push')}; every bumped Instance ends on the new revision serving "
            f"within the roll's budget, the killed one included, {end}")


def _crash_once_victim(side, form, topo):
    pod = "the leader pod of" if is_mn(topo) else "the pod of"
    who = (f"{pod} the first engine Instance the push brings up" if side == "new"
           else f"{pod} the engine's highest Instance")
    when = ("during its first start, before it was ever Ready" if form == "start"
            else "once it is Ready and serving")
    return f"{who} has its runner killed once, {when}; the kubelet starts the container again and nothing kills it twice"


def _crash_once_policy_words(side, form, rp, op):
    if op == "push" and side == "old":
        return ("the Instance is off the target from the bump, so the roll owns it: no repair is recorded for it "
                "after the bump under either restart policy and it ends on the new revision like its peers")
    if rp == "None":
        return ("restart policy None promises no repair for a container the kubelet restarts: no RestartTriggered "
                "event names the Instance and its pod keeps its identity")
    if form == "load":
        return ("RecreateInstanceOnPodRestart reads the runner's restart after Ready as its trigger: the Instance is "
                "rebuilt whole exactly once, named by one RestartTriggered event, and never rebuilt again")
    return ("RecreateInstanceOnPodRestart reads a runner restart, and whether one before the first Ready counts is "
            "reported: a rebuild is allowed once, named by a RestartTriggered event, never twice")


def _crash_once_untouched(topo, router):
    if is_pd(topo):
        return "decoder and router stay as they were"
    return "the router stays as it was" if router else ""


def _crash_once_reason(side, form, rp, op, strat, topo, router):
    out = _crash_once_victim(side, form, topo)
    if op == "idle":
        out += "; nothing is pushed"
    else:
        out += f"; the {strat} roll lands the new revision on every engine Instance within its budget"
        if side == "old":
            out += " and the restarted pod is the last the roll reaches"
    out += f"; {_crash_once_policy_words(side, form, rp, op)}"
    out += ("; at the end the Instance is Ready and serving on the revision it should run, no row reads Failed, "
            "no hold or retry ladder was opened for it, the rows settle with no operation open, and every pod "
            "carries the label of what it runs")
    if op == "push":
        out += "; the floor allows for the one Instance the kill takes while it restarts"
    untouched = _crash_once_untouched(topo, router)
    if untouched:
        out += f"; {untouched}"
    return out


def emit_crash_once(add):
    for topo, n, w, router in CRASH_ONCE_SHAPES:
        shape = shape_tag(topo, w) + ("+router" if router and not is_pd(topo) else "")
        for form in CRASH_FORMS:
            for rp in RESTARTS:
                strat, surge, unavail = CRASH_ONCE_IDLE_STRAT
                kv = dict(**_crash_once_counts(topo, n, router), **_crash_once_ceilings(topo, n, router, strat, 0),
                          restart=rp, fault="crashloop:old", form=form, once=1, during=IDLE_PHASE, co="none", topo=topo)
                meta = dict(strat="(dormant: nothing rolls)", verb=f"crashonce:old:{form}", rp=rp, victim="engine",
                            oneshot=f"{form} on the running revision at idle")
                desc = f"{GEN_MARK} crash once {form} idle {shape} N={n} rp {rp}"
                life = lib.lifecycle(strat=strat, surge=surge, unavail=unavail, restart=RP_FULL[rp])
                add("gen-crash-once", desc, kv, _crash_once_comps(topo, n, w, router, life, rp),
                    _crash_once_reason("old", form, rp, "idle", strat, topo, router), meta,
                    cell=("crash-once", "idle", "old", form, topo, w, router, rp))
        for side in CRASH_ONCE_SIDES:
            for form in CRASH_FORMS:
                for rp in RESTARTS:
                    for strat, surge, unavail in CRASH_ONCE_STRATS:
                        kv = dict(**_crash_once_counts(topo, n, router),
                                  **_crash_once_ceilings(topo, n, router, strat, surge),
                                  restart=rp, fault=f"crashloop:{side}", form=form, once=1, floor_slack=1,
                                  co="none", topo=topo)
                        meta = dict(strat=strat, verb=f"crashonce:{side}:{form}", rp=rp, victim="engine",
                                    oneshot=f"{form} on the {'pushed' if side == 'new' else 'running'} revision under a push")
                        desc = (f"{GEN_MARK} crash once {form} {side} {shape} N={n} {strat} "
                                f"{budget_tag(strat, surge, unavail)} rp {rp}")
                        life = lib.lifecycle(strat=strat, surge=surge, unavail=unavail, restart=RP_FULL[rp])
                        add("gen-crash-once", desc, kv, _crash_once_comps(topo, n, w, router, life, rp),
                            _crash_once_reason(side, form, rp, "push", strat, topo, router), meta,
                            cell=("crash-once", "push", side, form, topo, w, router, rp, strat))
    # The sibling broken once on the settled service: the router on the
    # router shapes, the decoder on the prefill/decode ones; nothing is
    # pushed and the restart policy of the sibling decides the repair.
    for topo, n, w, router, sibling in CRASH_ONCE_SIBLINGS:
        shape = shape_tag(topo, w) + ("+router" if router and not is_pd(topo) else "")
        strat, surge, unavail = CRASH_ONCE_IDLE_STRAT
        for form in CRASH_FORMS:
            for rp in RESTARTS:
                kv = dict(**_crash_once_counts(topo, n, router), **_crash_once_ceilings(topo, n, router, strat, 0),
                          restart=rp, fault="crashloop:old", form=form, once=1, during=IDLE_PHASE, victimcomp=sibling,
                          co="none", topo=topo)
                meta = dict(strat="(dormant: nothing rolls)", verb=f"crashonce:old:{form}", rp=rp, victim=sibling,
                            oneshot=f"{form} on the running revision's {sibling} at idle")
                desc = f"{GEN_MARK} crash once {form} idle {sibling} {shape} N={n} rp {rp}"
                life = lib.lifecycle(strat=strat, surge=surge, unavail=unavail, restart=RP_FULL[rp])
                add("gen-crash-once", desc, kv, _crash_once_sibling_comps(topo, n, w, router, life, rp, sibling),
                    _crash_once_sibling_reason(sibling, form, rp, topo), meta,
                    cell=("crash-once", "idle", "old", form, topo, w, router, rp, sibling))
    # The sibling broken once under a push: on the running revision, which an
    # engine-only bump leaves alone, so the sibling's own restart policy
    # decides the repair; and on the pushed revision, the sibling bumped
    # beside the engine (the decoder in a rollingUpdate group, the router by
    # name) and its first new pod killed once.
    for topo, n, w, router, sibling in CRASH_ONCE_SIBLINGS:
        shape = shape_tag(topo, w) + ("+router" if router and not is_pd(topo) else "")
        strat, surge, unavail = CRASH_ONCE_IDLE_STRAT
        for form in CRASH_FORMS:
            for rp in RESTARTS:
                life = lib.lifecycle(strat=strat, surge=surge, unavail=unavail, restart=RP_FULL[rp])
                kv = dict(**_crash_once_counts(topo, n, router), **_crash_once_ceilings(topo, n, router, strat, surge),
                          restart=rp, fault="crashloop:old", form=form, once=1, floor_slack=1, co="none", topo=topo,
                          victimcomp=sibling, bump="engine")
                meta = dict(strat=strat, verb=f"crashonce:old:{form}", rp=rp, victim=sibling,
                            oneshot=f"{form} on the running revision's {sibling} under an engine push")
                desc = f"{GEN_MARK} crash once {form} old {sibling} {shape} N={n} {strat} {budget_tag(strat, surge, unavail)} rp {rp}"
                add("gen-crash-once", desc, kv, _crash_once_sibling_comps(topo, n, w, router, life, rp, sibling),
                    _crash_once_sibling_push_reason(sibling, "old", form, rp, topo), meta,
                    cell=("crash-once", "push", "old", form, topo, w, router, rp, strat, sibling))
                kv = dict(**_crash_once_counts(topo, n, router), **_crash_once_ceilings(topo, n, router, strat, surge),
                          restart=rp, fault="crashloop:new", form=form, once=1, floor_slack=1, topo=topo, victimcomp=sibling)
                if sibling == "decoder":
                    kv.update(co="rolling", bump="engine,decoder", ceil_d=n + surge)
                    coord = lib.coord_for("rolling")
                else:
                    kv.update(co="none", bump="engine,router", ceil_r=lib.NR + surge)
                    coord = None
                meta = dict(strat=strat, verb=f"crashonce:new:{form}", rp=rp, victim=sibling,
                            oneshot=f"{form} on the pushed revision's {sibling} under a push")
                desc = f"{GEN_MARK} crash once {form} new {sibling} {shape} N={n} {strat} {budget_tag(strat, surge, unavail)} rp {rp}"
                add("gen-crash-once", desc, kv, _crash_once_bumped_sibling_comps(topo, n, w, router, life, rp, sibling),
                    _crash_once_sibling_push_reason(sibling, "new", form, rp, topo), meta,
                    cell=("crash-once", "push", "new", form, topo, w, router, rp, strat, sibling), coord=coord)


# ── gen-lift: a cause kept to the end, or taken away after a while ──────────
# The forms the ledger's fault rows lacked, on the crash-once shape list (the
# six shape families): lift=never keeps the cause until the case ends,
# lift=after-settle takes it away once it has visibly bitten for the settle
# window. The running-revision rows fire on the settled service; the
# pushed-revision rows ride a push under the surge and the drain-first
# strategies (an in-place push starts no pod of its own to break).
LIFT_SHAPES = CRASH_ONCE_SHAPES
LIFT_PUSH_STRATS = CRASH_ONCE_STRATS
LIFT_IDLE_STRAT = CRASH_ONCE_IDLE_STRAT
# The ledger row each form fills, named as coverage.py prints it.
LIFT_ROW_CONFIG_RUNNING_KEPT = "config missing, persistent, running"
LIFT_ROW_CONFIG_NEW_KEPT = "config missing, persistent, new"
LIFT_ROW_CONFIG_NEW_LIFTED = "config missing, one-shot, new"
LIFT_ROW_READINESS_NEW_LIFTED = "readiness never passes, one-shot, new"
LIFT_ROW_IMAGEPULL_RUNNING_LIFTED = "image pull, one-shot, running"
LIFT_ROW_IMAGEPULL_NEW_LIFTED = "image pull, one-shot, new"
# The push rows: the fix pushed after a break of the running revision at
# idle, with no pause around it, and the pull failure as the bump lands,
# lifted once it has shown.
FIX_PUSH = "fix"
LIFT_ROW_CONFIG_RUNNING_KEPT_PUSH = "config missing, persistent, running (fix push)"
LIFT_ROW_CONFIG_RUNNING_LIFTED_PUSH = "config missing, one-shot, running (fix push)"
LIFT_ROW_READINESS_RUNNING_KEPT_PUSH = "readiness never passes, persistent, running (fix push)"
LIFT_ROW_READINESS_RUNNING_LIFTED_PUSH = "readiness never passes, one-shot, running (fix push)"
LIFT_ROW_IMAGEPULL_RUNNING_LIFTED_PUSH = "image pull, one-shot, running (push)"
# The pushed-revision rows run under one restart policy: a pod that never
# started has no restart for a policy to read.
LIFT_PUSH_RP = "None"


def _lift_shape(topo, w, router):
    return shape_tag(topo, w) + ("+router" if router and not is_pd(topo) else "")


def _lift_untouched(topo, router):
    if is_pd(topo):
        return "decoder and router stay as they were"
    return "the router stays as it was" if router else ""


def _lift_configkey_body(name, topo, n, w, router, life, rp):
    """The case body of a config-key cell: the engine runner reads one key of
    a ConfigMap named after the case, carried as a second document so the
    apply creates it and the teardown removes it with the case."""
    cm = f"{name}{HEAL_CONFIG_SUFFIX}"
    cs = _crash_once_comps(topo, n, w, router, life, rp)
    cs[0]["runner"] = lib.configmap_env(cm, HEAL_CONFIG_KEY, HEAL_CONFIG_VAR, gang=is_mn(topo))
    return lib.render(name, cs, docs=[lib.configmap_doc(cm, {HEAL_CONFIG_KEY: HEAL_CONFIG_VALUE})])


def _lift_configkey_kept_reason(rp, topo, router):
    where = "gang leader" if is_mn(topo) else "pod"
    repair = ("the crash-loop repair opens as soon as the wait is seen and rebuilds the pod, which waits on the same key"
              if rp == "None" else "the runner-restart repair rebuilds the pod, which waits on the same key")
    out = (f"the engine runner's ConfigMap key is removed on the highest Instance's {where} and the runner restarted, "
           f"so its next start waits in CreateContainerConfigError, and the key is not put back while the case runs; "
           f"{repair}; the wait outlives the attempt's Instance ready timeout and the escalation parks the repair at "
           f"Failed with the reason recorded on the row; the row stays Failed with nothing of the Instance serving "
           f"across the hold window; no new revision appears, the reset mailbox is never written and no Component "
           f"exceeds N alive; the key returns only with the case's teardown")
    untouched = _lift_untouched(topo, router)
    return out + (f"; {untouched}" if untouched else "")


def _lift_first_pod(strat):
    return {"SurgeThenDrain": "the surge Instance's pod (nothing drains; floor N)",
            "RecreatePod": "the first drained Instance's replacement (floor N-1)"}[strat]


def _lift_configkey_new_reason(lift, strat, topo, router):
    pod = "leader pod" if is_mn(topo) else "pod"
    out = (f"the push points the engine runner at a ConfigMap key the ConfigMap lacks, so the pushed revision's first "
           f"engine {pod}, {_lift_first_pod(strat)}, waits in CreateContainerConfigError from its creation and is never "
           f"Ready")
    if lift == "never":
        out += ("; the key is not added: the roll holds at the Instance ready timeout with the new Instance Failed and "
                "the reason recorded, the old revision keeps serving at the floor and the broken revision never serves; "
                "a fix push back onto the present key then lands on every Instance with nothing of the broken revision left")
    else:
        out += (f"; once that wait has held for the settle window the key is added, the kubelet's own retry starts the "
                f"container and the Instance comes up on its own; the {strat} roll lands the new revision on every "
                f"Instance within its budget with no hold behind it: no row reads Failed, nothing is escalated or held, "
                f"the Instance serves on the new revision, a repair is reported if one ran and never twice, every row settles")
    untouched = _lift_untouched(topo, router)
    return out + (f"; {untouched}" if untouched else "")


def _lift_readiness_new_reason(topo, router):
    pod = "leader pod" if is_mn(topo) else "pod"
    out = (f"the push's first engine {pod}, the surge Instance's, gets the fixture's readiness switch file while its "
           f"runner is still starting, before it was ever Ready, so it runs and does not pass readiness; the surge "
           f"waits (nothing drains; floor N) and nothing rebuilds or restarts the pod, under either restart policy, "
           f"because readiness is the runtime's own out-of-rotation signal and not a failure; once the pod has run "
           f"the bite window without passing and the drop has held for the settle window the switch is lifted and "
           f"the same pod passes readiness; the roll then lands the new revision on every Instance within its budget "
           f"with no hold behind it: no row reads Failed, nothing is escalated or held, no repair is recorded, the "
           f"pod keeps its identity and serves on the new revision, every row settles")
    untouched = _lift_untouched(topo, router)
    return out + (f"; {untouched}" if untouched else "")


def _lift_imagepull_idle_reason(rp, topo, router):
    where = "leader pod" if is_mn(topo) else "pod"
    back = ("where the wait does hold for the settle window the image is pointed back and the kubelet starts the "
            "container again"
            if rp == "None" else
            "where the wait does hold for the settle window the image is pointed back and the kubelet starts the "
            "container again, a restart the policy may read as a runner restart and answer with a rebuild")
    out = (f"the {where} of the engine's highest Instance is pointed at an image that cannot be pulled, so the kubelet "
           f"stops its container and parks it in ImagePullBackOff; the stuck-pod grace counts from the pod's creation "
           f"and a long-serving pod is past it, so the crash-loop repair rebuilds the Instance under either restart "
           f"policy as soon as the wait is seen, usually before the settle window ends, with nothing left to lift; "
           f"{back}; either way the Instance is Ready and serving again on its running revision within the budget, "
           f"with no new revision and no reset mailbox, and no Component exceeds N alive")
    untouched = _lift_untouched(topo, router)
    return out + (f"; {untouched}" if untouched else "")


def _lift_imagepull_new_reason(rp, topo, router):
    pod = "leader pod" if is_mn(topo) else "pod"
    policy = ("restart policy None reads nothing into the kubelet's restart of the container, so no repair is recorded"
              if rp == "None" else
              "RecreateInstanceOnPodRestart may read the kubelet's restart of the container as a runner restart, so one "
              "rebuild is reported, never required, never twice")
    out = (f"the push's first engine {pod}, the surge Instance's, is pointed at an image that cannot be pulled while its "
           f"runner is still starting, before it was ever Ready, so the kubelet stops the container and parks the pod in "
           f"ImagePullBackOff; the surge waits (nothing drains; floor N); once the pull failure has held for the settle "
           f"window the pod is pointed back at the image it ran and the kubelet starts the container; the roll then lands "
           f"the new revision on every Instance within its budget with no hold behind it: no row reads Failed, nothing is "
           f"escalated or held, the Instance serves on the new revision, every row settles; {policy}")
    untouched = _lift_untouched(topo, router)
    return out + (f"; {untouched}" if untouched else "")


def _fix_push_tail(topo, router):
    out = ("; the SurgeThenDrain roll lands the new revision on every engine Instance within its budget, the broken "
           "Instance Ready and serving on the new revision and never the last to land the fix, since its capacity is "
           "already lost; no repair is recorded for it after the push, because its revision stopped being the target "
           "at the bump and the roll owns it; every row settles with no operation open and no hold is left behind; "
           "the floor is reported, not judged, for the one Instance the break took")
    untouched = _lift_untouched(topo, router)
    return out + (f"; {untouched}" if untouched else "")


def _fix_push_configkey_reason(lift, rp, topo, router):
    where = "gang leader" if is_mn(topo) else "pod"
    waits = ("the crash-loop repair opens as soon as the wait is seen and rebuilds the pod, which waits on the same key"
             if rp == "None" else "the runner-restart repair rebuilds the pod, which waits on the same key")
    out = (f"the engine runner's ConfigMap key is removed on the highest Instance's {where} and the runner restarted, "
           f"so its next start waits in CreateContainerConfigError; {waits}; once the wait has shown for the settle "
           f"window the operator pushes the fix with no pause around it: a bump that gives the engine runner a literal "
           f"value in place of the key it cannot read, so no pod of the new revision waits on the ConfigMap")
    if lift == "never":
        out += "; the key stays away, so the broken Instance's pod waits until the roll replaces it"
    else:
        out += ("; the key is put back as the fix lands, so the broken pod may start again on its running revision "
                "while the roll is already replacing it, and the roll still owns it")
    return out + _fix_push_tail(topo, router)


def _fix_push_readiness_reason(form, rp, topo, router):
    where = "gang leader" if is_mn(topo) else "pod"
    out = (f"the fixture's readiness switch file is set in the highest engine Instance's {where}, so it keeps running "
           f"and stops passing readiness: it leaves the Ready endpoints and its row reports the drop, and restart policy "
           f"{RP_FULL[rp]} rebuilds nothing, since readiness is the runtime's own out-of-rotation signal and not a "
           f"failure; once the drop has shown for the settle window the operator pushes the fix with no pause around "
           f"it: an ordinary bump, whose re-rendered pods carry no switch file")
    if form == "persist":
        out += "; the switch stays set, so the broken pod stays out of rotation until the roll replaces it"
    else:
        out += ("; the switch is lifted as the fix lands, so the same pod may return to rotation while the roll is "
                "already replacing it, and the roll still owns it")
    return out + _fix_push_tail(topo, router)


def _lift_imagepull_push_reason(rp, topo, router):
    pod = "leader pod" if is_mn(topo) else "pod"
    out = (f"the {pod} of the engine's highest Instance is pointed at an image that cannot be pulled as the bump lands, "
           f"so the kubelet stops its container and parks it in ImagePullBackOff while the roll starts; once the pull "
           f"failure has held for the settle window the pod is pointed back at the image it ran, unless the roll has "
           f"already replaced it; the Instance was off the target from the bump, so the roll owns it under restart "
           f"policy {RP_FULL[rp]}: it ends on the new revision serving with no repair recorded for it after the bump, "
           f"the SurgeThenDrain roll converges within its budget with every engine Instance on the new revision, and "
           f"the floor is reported, not judged, for the one Instance the break took")
    untouched = _lift_untouched(topo, router)
    return out + (f"; {untouched}" if untouched else "")


def emit_lift(add):
    for topo, n, w, router in LIFT_SHAPES:
        shape = _lift_shape(topo, w, router)
        strat, surge, unavail = LIFT_IDLE_STRAT
        for rp in RESTARTS:
            kv = dict(**_crash_once_counts(topo, n, router), **_crash_once_ceilings(topo, n, router, strat, 0),
                      restart=rp, fault="configkey:missing", co="none", topo=topo, during=IDLE_PHASE,
                      lift="never", expect="failed")
            meta = dict(strat="(dormant: nothing rolls)", verb="configkey:never", rp=rp,
                        liftrow=LIFT_ROW_CONFIG_RUNNING_KEPT)
            desc = f"{GEN_MARK} configkey kept {shape} N={n} rp {rp}"
            life = lib.lifecycle(strat=strat, surge=surge, unavail=unavail, restart=RP_FULL[rp],
                                 ready_timeout=HEAL_FAILED_READY_TIMEOUT)
            add("gen-lift", desc, kv, None, _lift_configkey_kept_reason(rp, topo, router), meta,
                cell=("lift", "configkey", "kept", "old", topo, w, router, rp),
                body=functools.partial(_lift_configkey_body, topo=topo, n=n, w=w, router=router, life=life, rp=rp))
    for topo, n, w, router in LIFT_SHAPES:
        shape = _lift_shape(topo, w, router)
        for lift, row in (("never", LIFT_ROW_CONFIG_NEW_KEPT), ("after-settle", LIFT_ROW_CONFIG_NEW_LIFTED)):
            for strat, surge, unavail in LIFT_PUSH_STRATS:
                rp = LIFT_PUSH_RP
                kv = dict(**_crash_once_counts(topo, n, router), **_crash_once_ceilings(topo, n, router, strat, surge),
                          restart=rp, fault="configkey:new", co="none", topo=topo, lift=lift)
                meta = dict(strat=strat, verb=f"configkey:new:{lift}", rp=rp, liftrow=row)
                desc = (f"{GEN_MARK} configkey new {'kept' if lift == 'never' else 'lifted'} {shape} N={n} {strat} "
                        f"{budget_tag(strat, surge, unavail)} rp {rp}")
                timeout = READINESS_NEW_TIMEOUT if lift == "never" else None
                life = lib.lifecycle(strat=strat, surge=surge, unavail=unavail, restart=RP_FULL[rp], ready_timeout=timeout)
                add("gen-lift", desc, kv, None, _lift_configkey_new_reason(lift, strat, topo, router), meta,
                    cell=("lift", "configkey", lift, "new", topo, w, router, strat),
                    body=functools.partial(_lift_configkey_body, topo=topo, n=n, w=w, router=router, life=life, rp=rp))
    for topo, n, w, router in LIFT_SHAPES:
        shape = _lift_shape(topo, w, router)
        strat, surge, unavail = LIFT_IDLE_STRAT
        for rp in RESTARTS:
            kv = dict(**_crash_once_counts(topo, n, router), **_crash_once_ceilings(topo, n, router, strat, surge),
                      restart=rp, fault="readiness:new", co="none", topo=topo, form="recover")
            meta = dict(strat=strat, verb="readiness:new:recover", rp=rp, liftrow=LIFT_ROW_READINESS_NEW_LIFTED)
            desc = f"{GEN_MARK} readiness new lifted {shape} N={n} {strat} {budget_tag(strat, surge, unavail)} rp {rp}"
            life = lib.lifecycle(strat=strat, surge=surge, unavail=unavail, restart=RP_FULL[rp])
            add("gen-lift", desc, kv, _crash_once_comps(topo, n, w, router, life, rp),
                _lift_readiness_new_reason(topo, router), meta,
                cell=("lift", "readiness", "after-settle", "new", topo, w, router, rp))
    for topo, n, w, router in LIFT_SHAPES:
        shape = _lift_shape(topo, w, router)
        strat, surge, unavail = LIFT_IDLE_STRAT
        for rp in RESTARTS:
            kv = dict(**_crash_once_counts(topo, n, router), **_crash_once_ceilings(topo, n, router, strat, 0),
                      restart=rp, fault="imagepull:old", co="none", topo=topo, during=IDLE_PHASE,
                      lift="after-settle", expect="heals")
            meta = dict(strat="(dormant: nothing rolls)", verb="imagepull:idle:after-settle", rp=rp,
                        liftrow=LIFT_ROW_IMAGEPULL_RUNNING_LIFTED)
            desc = f"{GEN_MARK} imagepull lifted idle {shape} N={n} rp {rp}"
            life = lib.lifecycle(strat=strat, surge=surge, unavail=unavail, restart=RP_FULL[rp])
            add("gen-lift", desc, kv, _crash_once_comps(topo, n, w, router, life, rp),
                _lift_imagepull_idle_reason(rp, topo, router), meta,
                cell=("lift", "imagepull", "after-settle", "old", topo, w, router, rp))
    for topo, n, w, router in LIFT_SHAPES:
        shape = _lift_shape(topo, w, router)
        strat, surge, unavail = LIFT_IDLE_STRAT
        for rp in RESTARTS:
            kv = dict(**_crash_once_counts(topo, n, router), **_crash_once_ceilings(topo, n, router, strat, surge),
                      restart=rp, fault="imagepull:new", co="none", topo=topo, lift="after-settle")
            meta = dict(strat=strat, verb="imagepull:new:after-settle", rp=rp, liftrow=LIFT_ROW_IMAGEPULL_NEW_LIFTED)
            desc = f"{GEN_MARK} imagepull new lifted {shape} N={n} {strat} {budget_tag(strat, surge, unavail)} rp {rp}"
            life = lib.lifecycle(strat=strat, surge=surge, unavail=unavail, restart=RP_FULL[rp])
            add("gen-lift", desc, kv, _crash_once_comps(topo, n, w, router, life, rp),
                _lift_imagepull_new_reason(rp, topo, router), meta,
                cell=("lift", "imagepull", "after-settle", "new", topo, w, router, rp))
    # The fix pushed after a break of the running revision at idle, with no
    # pause around it: the key gone or a serving pod out of readiness, kept
    # or taken away as the fix lands, then push=fix once the break has shown.
    for topo, n, w, router in LIFT_SHAPES:
        shape = _lift_shape(topo, w, router)
        strat, surge, unavail = LIFT_IDLE_STRAT
        for rp in RESTARTS:
            life = lib.lifecycle(strat=strat, surge=surge, unavail=unavail, restart=RP_FULL[rp])
            for lift, row in (("never", LIFT_ROW_CONFIG_RUNNING_KEPT_PUSH), ("after-settle", LIFT_ROW_CONFIG_RUNNING_LIFTED_PUSH)):
                kv = dict(**_crash_once_counts(topo, n, router), **_crash_once_ceilings(topo, n, router, strat, surge),
                          restart=rp, fault="configkey:missing", floor_soft=1, co="none", topo=topo, during=IDLE_PHASE,
                          lift=lift, push=FIX_PUSH)
                meta = dict(strat=strat, verb=f"configkey:{lift}:fixpush", rp=rp, liftrow=row)
                desc = (f"{GEN_MARK} configkey {'kept' if lift == 'never' else 'lifted'} fix push {shape} N={n} {strat} "
                        f"{budget_tag(strat, surge, unavail)} rp {rp}")
                add("gen-lift", desc, kv, None, _fix_push_configkey_reason(lift, rp, topo, router), meta,
                    cell=("lift", "configkey", lift, "old", "fixpush", topo, w, router, rp),
                    body=functools.partial(_lift_configkey_body, topo=topo, n=n, w=w, router=router, life=life, rp=rp))
            for form, row in (("persist", LIFT_ROW_READINESS_RUNNING_KEPT_PUSH), ("recover", LIFT_ROW_READINESS_RUNNING_LIFTED_PUSH)):
                kv = dict(**_crash_once_counts(topo, n, router), **_crash_once_ceilings(topo, n, router, strat, surge),
                          restart=rp, fault="readiness:old", floor_soft=1, co="none", topo=topo, form=form,
                          during=IDLE_PHASE, push=FIX_PUSH)
                meta = dict(strat=strat, verb=f"readiness:old:{form}:fixpush", rp=rp, liftrow=row)
                desc = (f"{GEN_MARK} readiness {'kept' if form == 'persist' else 'lifted'} fix push {shape} N={n} {strat} "
                        f"{budget_tag(strat, surge, unavail)} rp {rp}")
                add("gen-lift", desc, kv, _crash_once_comps(topo, n, w, router, life, rp),
                    _fix_push_readiness_reason(form, rp, topo, router), meta,
                    cell=("lift", "readiness", form, "old", "fixpush", topo, w, router, rp))
    # The image that cannot be pulled, pointed at the running revision's pod
    # as the bump lands and back once the failure has shown.
    for topo, n, w, router in LIFT_SHAPES:
        shape = _lift_shape(topo, w, router)
        strat, surge, unavail = LIFT_IDLE_STRAT
        for rp in RESTARTS:
            kv = dict(**_crash_once_counts(topo, n, router), **_crash_once_ceilings(topo, n, router, strat, surge),
                      restart=rp, fault="imagepull:old", floor_soft=1, co="none", topo=topo, lift="after-settle")
            meta = dict(strat=strat, verb="imagepull:old:after-settle:push", rp=rp, liftrow=LIFT_ROW_IMAGEPULL_RUNNING_LIFTED_PUSH)
            desc = f"{GEN_MARK} imagepull lifted push {shape} N={n} {strat} {budget_tag(strat, surge, unavail)} rp {rp}"
            life = lib.lifecycle(strat=strat, surge=surge, unavail=unavail, restart=RP_FULL[rp])
            add("gen-lift", desc, kv, _crash_once_comps(topo, n, w, router, life, rp),
                _lift_imagepull_push_reason(rp, topo, router), meta,
                cell=("lift", "imagepull", "after-settle", "old", "push", topo, w, router, rp))


# ── assembly ─────────────────────────────────────────────────────────────────
def build_outputs():
    """{relative path under isvcs/: file text} — the entire owned output set."""
    outputs = {}
    counters = {f: lo for f, (lo, _hi) in GEN_BLOCKS.items()}
    rows = []   # (folder, case id, kv, meta) for the manifest
    kept_empty = []   # (folder, case id, description, why) of the ids minted with no case
    parked_used = set()

    def mint(folder):
        """The next id of a folder: from its block, then from its
        continuation block once the block is full."""
        cid = counters[folder]
        for lo, hi in id_blocks(folder):
            if cid < lo:
                cid = lo
            if cid < hi:
                counters[folder] = cid + 1
                return cid
        sys.exit(f"{folder}: id blocks exhausted at p{cid} (the last block ends before p{hi})")

    def add(folder, desc, kv, case_comps, reason, meta, cell, coord=None, body=None,
            runtime=None, dedicated=False, edit_runtime=False, empty_why=None):
        """body: the case body as a function of the minted name, for a family
        whose fixtures carry the case id; the plain render otherwise. A
        dedicated case ships its own runtime under a per-case name, so a
        runtime edit touches this case alone; edit_runtime ships the
        runtime-rev generator's ClusterServingRuntime for the case's topology
        under that name, the one the runtime-edit verbs patch. empty_why
        names why a cell of the matrix is no case: its id is minted and kept
        empty, so the ids after it keep their places."""
        cid = mint(folder)
        name = f"p{cid}"
        if empty_why:
            kept_empty.append((folder, name, desc[len(GEN_MARK) + 1:], empty_why))
            return
        issue = inv_scope = None
        if cell in PARKED:
            parked_used.add(cell)
            issue, reason, inv_scope = PARKED[cell]
        if issue:
            kv = dict(kv, notimpl=1, issue=issue)
            if inv_scope:
                kv["inv_xfail"] = inv_scope
            meta = dict(meta, parked=issue)
        before = None
        if dedicated or edit_runtime:
            rt_name = f"ome-coordtest-rt-{name}"
            kv = dict(kv, rt=rt_name)
            before = (gen_runtime.dedicated_csr(kv["topo"], rt_name) if edit_runtime
                      else dedicated_runtime(rt_name, metrics=(runtime == lib.CSR_METRICS),
                                             gang=any(c["w"] > 0 for c in case_comps or [])))
            runtime = rt_name
        text = body(name) if body else lib.render(name, case_comps, coord=coord, runtime=runtime)
        if before:
            text = before + "\n---\n" + text
        outputs[f"{folder}/{name}.yaml"] = case_yaml(name, desc, kv, reason, text)
        rows.append((folder, name, dict(kv), dict(meta)))

    emit_rollout(add)
    emit_fault(add)
    emit_restart(add)
    emit_crashloop(add)
    emit_switch(add)
    emit_crashloop_idle(add)
    emit_crashloop_sibling(add)
    emit_heal(add)
    emit_imagepull(add)
    emit_node(add)
    emit_node_relocation(add)
    emit_node_hooks(add)
    emit_ctrlrestart(add)
    emit_ctrlrestart_hooks(add)
    emit_ctrlrestart_idle(add)
    emit_canary_crash(add)
    emit_canary_retarget(add)
    emit_canary_podkill(add)
    emit_canary_fault_old(add)
    emit_canary_fault_new(add)
    emit_canary_fault_gate(add)
    emit_rollback(add)
    emit_crashloop_new(add)
    emit_crashloop_group(add)
    emit_pause_repair(add)
    emit_size(add)
    emit_router_gang(add)
    emit_ops_held(add)
    emit_runtime_edit_repair(add)
    emit_migrate_bump(add)
    emit_readiness(add)
    emit_gang_member(add)
    emit_tight(add)
    emit_evict(add)
    emit_preempt(add)
    emit_pending(add)
    emit_budget(add)
    emit_inplaceonly(add)
    emit_fault_scale(add)
    emit_fault_delete(add)
    emit_fault_more(add)
    emit_fault_move(add)
    emit_fault_edit(add)
    emit_packed_pool(add)
    emit_repeat(add)
    emit_repeat_ctrl_ops(add)
    emit_podkill_hooks(add)
    emit_webhook(add)
    emit_conflict(add)
    emit_crash_once(add)
    emit_lift(add)
    _emit_plain_ops(add)
    _emit_leftover_singles(add)
    stale = set(PARKED) - parked_used
    if stale:
        sys.exit("PARKED keys match no emitted cell (stale after a matrix change): "
                 + "; ".join(repr(k) for k in sorted(stale)))
    outputs[MANIFEST_REL] = manifest(rows, kept_empty)
    return outputs, rows


# ── the plain operations on the shapes the hand-written folders skip ───────
# One case per ledger cell with no fault: a push on the engine behind a
# router; a rollback and a strategy edit on both engine-behind-a-router
# shapes; a runtime edit and a delete on the router shapes and the
# prefill/decode gang; a scale up and a scale down on the engine behind a
# router; a move on both router shapes. Each lands in the family that owns
# the operation, minted after every other row of every family, as the
# control row the faulted rows are read against.
PLAIN_STRAT = ("SurgeThenDrain", 1, 0)
PLAIN_ROUTER_SHAPES = [("engine", 4, 0, lib.NR), ("engine-mn", 2, 1, lib.NR)]
PLAIN_WIDE_SHAPES = [("pd", 4, 0, 0), ("pd-mn", 2, 1, 0)] + PLAIN_ROUTER_SHAPES


def _plain_controls(topo, nr):
    if is_pd(topo):
        return "; decoder and router are negative controls (co=none)"
    return "; the router is a negative control (co=none)" if nr else ""


def _plain_gang(topo):
    return "; each engine Instance is a gang of a leader and a worker, moved and rolled whole" if is_mn(topo) else ""


def _emit_plain_ops(add):
    strat, surge, unavail = PLAIN_STRAT
    life = lib.lifecycle(strat=strat, surge=surge, unavail=unavail)
    topo, n, w, nr = PLAIN_ROUTER_SHAPES[0]
    kv = dict(**counts(topo, n, nr), **ceilings(topo, n, strat, surge, nr), co="none", topo=topo)
    add("gen-rollout", f"{GEN_MARK} rollout {shape_tag(topo, w, nr)} N={n} {strat} {budget_tag(strat, surge, unavail)}", kv,
        comps(topo, n, w, life, nr=nr),
        f"plain bump of the engine behind a router: rolls within the surge budget (peak <= {kv['ceil_e']}), never below N "
        f"ready, and the roll completes with every engine Instance on the new revision and serving{_plain_controls(topo, nr)}",
        dict(strat=strat, verb="none", plain=True), cell=("plain", "push", topo, w, nr))
    for topo, n, w, nr in PLAIN_ROUTER_SHAPES:
        shape = shape_tag(topo, w, nr)
        kv = dict(**_crash_once_counts(topo, n, True), **_crash_once_ceilings(topo, n, True, strat, surge),
                  co="none", topo=topo, rollback=1, during="rotating")
        add("gen-rollback", f"{GEN_MARK} rollback at rotating {shape} N={n} {strat} {budget_tag(strat, surge, unavail)} co none",
            kv, None, _rollback_reason("rotating", strat, topo, "none") + _plain_controls(topo, nr),
            dict(strat=strat, verb="rollback:rotating", hook="rotating", plain=True), cell=("plain", "rollback", topo, w, nr),
            body=functools.partial(_rollback_fault_body, topo=topo, n=n, w=w, router=True, life=life, co="none",
                                   configkey=False, coord=None))
        to = "RecreatePod"
        esurge, eunavail = switch_budget(strat, to)
        elife = lib.lifecycle(strat=strat, surge=esurge, unavail=eunavail)
        kv = _switch_fault_kv(topo, n, True, esurge, "none", dict(edit=f"strategy:{to}", during="rotating"))
        add("gen-switch", f"{GEN_MARK} switch rotating {shape} N={n} {strat} to {to}", kv, None,
            _switch_strategy_reason(topo, strat, to, "rotating", "none") + _plain_controls(topo, nr),
            dict(strat=strat, verb="edit:rotating", edit=f"strategy {strat} to {to} at rotating", plain=True),
            cell=("plain", "edit", topo, w, nr),
            body=functools.partial(_switch_fault_body, topo=topo, n=n, w=w, router=True, life=elife, co="none",
                                   configkey=False, coord=None))
        kv = dict(mode="migrate", **counts(topo, n, nr), ceil_e=n + 1, ceil_d=0, ceil_r=router_count(topo, nr),
                  co="none", topo=topo, mig="Auto", inst=0)
        add("gen-fault-move", f"{GEN_MARK} fault-move plain {shape} N={n}", kv, comps(topo, n, w, lib.lifecycle(), nr=nr),
            f"{_move_request_words(topo, w, nr)} with nothing in its way: the move completes end to end and is recorded "
            f"once, the replacement comes up whole elsewhere and the source drains, the Instance ends Ready and serving on "
            f"its replacement, nothing exceeds N+1 alive, the other Instances keep serving throughout and every row "
            f"settles{_plain_gang(topo)}{_plain_controls(topo, nr)}",
            dict(strat=strat, verb="none@move", rp="None", victim="engine", at="accept", who="moved", plain=True),
            cell=("plain", "move", topo, w, nr))
    for topo, n, w, nr in PLAIN_WIDE_SHAPES:
        shape = shape_tag(topo, w, nr)
        kv = dict(_edit_kv(topo, n, strat, surge, nr), co="none", topo=topo, expect="roll", edit="env")
        add("gen-fault-edit", f"{GEN_MARK} fault-edit plain edit env {shape} N={n} {strat}", kv,
            comps(topo, n, w, life, d_life=life, nr=nr),
            f"the operator gives the dedicated runtime every Component inherits a new runner env on {shape_words(topo, w, nr)} "
            f"under {strat}, so every Component rolls onto its own new revision: {_op_end('runtime')}, within the surge "
            f"budget of every Component, with every row settled and nothing left held{_plain_gang(topo)}",
            dict(strat=strat, verb="none@edit", rp="None", rtedit="env", row="none", plain=True),
            cell=("plain", "runtime", topo, w, nr), dedicated=True)
        kv = dict(_op_kv("delete", topo, n, nr, False), co="none", topo=topo)
        add("gen-fault-delete", f"{GEN_MARK} plain delete {shape} N={n}", kv, _op_comps("delete", topo, n, w, nr, False),
            f"{_op_does('delete', False, topo)} on {shape_words(topo, w, nr)} with nothing broken: every pod goes within "
            f"the teardown budget, no pod stays Terminating and no finalizer wedges, and {_op_end('delete')}"
            f"{_plain_gang(topo)}{_plain_controls(topo, nr)}",
            dict(strat="(dormant: nothing rolls)", verb="none@delete", rp="None", op="delete", plain=True),
            cell=("plain", "delete", topo, w, nr))
    topo, n, w, nr = PLAIN_ROUTER_SHAPES[0]
    for op in ("scaleup", "scaledown"):
        kv = dict(_op_kv(op, topo, n, nr, False), co="none", topo=topo)
        add("gen-fault-scale", f"{GEN_MARK} plain {op} {shape_tag(topo, w, nr)} N={n}", kv, _op_comps(op, topo, n, w, nr, False),
            f"{_op_does(op, False, topo)} on {shape_words(topo, w, nr)} with nothing broken: {_op_end(op)}, nothing surges "
            f"above the target, the untouched Instances keep their pods and keep serving, and every row settles Ready with no "
            f"operation open{_plain_controls(topo, nr)}",
            dict(strat="(dormant: nothing rolls)", verb=f"none@{op}", rp="None", op=op, plain=True),
            cell=("plain", op, topo, w, nr))


# ── the single cells the shape axis left on the router shapes ──────────────
# One case per ledger cell on the engine-behind-a-router shapes (single-pod
# and gang), each in the family that owns the verb and minted after every
# other row: the node lost once and for good at steady state (gen-node); the
# ConfigMap key put back once its wait has shown, and a pull failure of the
# running revision at steady state and as a push lands (gen-heal,
# gen-imagepull); a pod deleted by hand under a push on the single-pod shape
# and a gang leader lost under a push on the gang (gen-fault,
# gen-gang-member); a stable pod or gang leader deleted once at a canary
# step on the engine gang and both router shapes (gen-canary-podkill); and a
# rollback at the held moment of a push that never passes readiness
# (gen-rollback).
SINGLE_ROUTER_SHAPES = [("engine", 4, 0, lib.NR), ("engine-mn", 2, 1, lib.NR)]
SINGLE_RP = "None"
SINGLE_MIG = "Auto"
SINGLE_NODE_OUTCOMES = [("repaired", "after-settle"), NODE_RELOCATION]


def _single_router_control():
    return "; the router is a negative control (co=none)"


def _emit_leftover_singles(add):
    rp, mig = SINGLE_RP, SINGLE_MIG
    strat, surge, unavail = PLAIN_STRAT
    for topo, n, w, nr in SINGLE_ROUTER_SHAPES:
        tag = shape_tag(topo, w, nr)
        gang = is_mn(topo)
        # A Ready Instance loses its node: once (the kubelet started again
        # once the bite has held) and for good (relocated by the policy).
        life = lib.lifecycle(restart=RP_FULL[rp], mig={"mode": mig})
        for outcome, lift in SINGLE_NODE_OUTCOMES:
            kv = dict(**counts(topo, n, nr), **ceilings(topo, n, "SurgeThenDrain", 0, nr),
                      fault=f"node:{NODE_PARAM}", during="idle", lift=lift, expect=outcome, co="none", topo=topo)
            if outcome in NODE_NEEDS:
                kv["needs"] = NODE_NEEDS[outcome]
            add("gen-node", f"{GEN_MARK} node notready {tag} N={n} rp {rp} mig {mig} co none {outcome}", kv,
                comps(topo, n, w, life, nr=nr), _node_reason(outcome, rp, mig, "none") + _single_router_control(),
                dict(strat="SurgeThenDrain", verb=f"node:{outcome}", rp=rp, mig=mig, single=True),
                cell=("single", "node", outcome, topo, w, nr))
        # The running revision's key removed and put back once its wait has shown.
        life = lib.lifecycle(restart=RP_FULL[rp])

        def heal_body(name, topo=topo, n=n, w=w, nr=nr, life=life):
            cm = f"{name}{HEAL_CONFIG_SUFFIX}"
            cs = comps(topo, n, w, life, nr=nr)
            cs[0]["runner"] = lib.configmap_env(cm, HEAL_CONFIG_KEY, HEAL_CONFIG_VAR, gang=is_mn(topo))
            return lib.render(name, cs, docs=[lib.configmap_doc(cm, {HEAL_CONFIG_KEY: HEAL_CONFIG_VALUE})])

        kv = dict(**counts(topo, n, nr), ceil_e=n, ceil_d=0, ceil_r=nr, fault="configkey:missing", co="none", topo=topo,
                  during="idle", lift="after-settle", expect="heals")
        add("gen-heal", f"{GEN_MARK} heal configkey {tag} N={n} rp {rp} lift after-settle co none", kv, None,
            _heal_reason(rp, "none", "after-settle", topo) + _single_router_control(),
            dict(strat="(dormant: nothing rolls)", verb="configkey:after-settle", rp=rp, single=True),
            cell=("single", "heal", topo, w, nr), body=heal_body)
        # A pull failure of the running revision: at steady state, and as a push lands.
        kv = dict(**counts(topo, n, nr), ceil_e=n, ceil_d=0, ceil_r=nr, fault="imagepull:old", co="none", topo=topo,
                  during="idle", expect="repaired")
        add("gen-imagepull", f"{GEN_MARK} imagepull idle {tag} N={n} rp {rp}", kv, comps(topo, n, w, life, nr=nr),
            _imagepull_idle_reason(topo) + _single_router_control(),
            dict(strat="(dormant: nothing rolls)", verb="imagepull:idle", rp=rp, single=True),
            cell=("single", "imagepull", "idle", topo, w, nr))
        kv = dict(**counts(topo, n, nr), **ceilings(topo, n, strat, surge, nr), fault="imagepull:old", floor_soft=1,
                  co="none", topo=topo)
        add("gen-imagepull", f"{GEN_MARK} imagepull {tag} N={n} {strat} {budget_tag(strat, surge, unavail)} rp {rp} co none", kv,
            comps(topo, n, w, lib.lifecycle(strat=strat, surge=surge, unavail=unavail, restart=RP_FULL[rp]), nr=nr),
            _imagepull_reason(strat, "none") + _single_router_control(),
            dict(strat=strat, verb="imagepull:settling", rp=rp, single=True),
            cell=("single", "imagepull", "settling", topo, w, nr))
        # A pod, or a gang's leader, deleted by hand as the push lands.
        roll_life = lib.lifecycle(strat=strat, surge=surge, unavail=unavail, restart=RP_FULL[rp])
        if gang:
            kv = dict(**counts(topo, n, nr), **ceilings(topo, n, strat, surge, nr), restart=rp, kill="leader",
                      fault="podkill:settling", co="none", topo=topo, victim="stable", victimcomp="engine",
                      floor_slack=1, order_soft=1)
            add("gen-gang-member", f"{GEN_MARK} gang leader lost settling {tag} N={n} {strat} rp {rp}", kv,
                comps(topo, n, w, roll_life, nr=nr), _gang_member_reason("settling", "leader", strat, rp) + _single_router_control(),
                dict(strat=strat, verb="podkill:settling:leader", rp=rp, single=True),
                cell=("single", "gang-member", topo, w, nr))
        else:
            waivers = _podkill_waivers(topo, strat, "settling")
            kv = dict(**counts(topo, n, nr), **ceilings(topo, n, strat, surge, nr), fault="podkill:settling",
                      victimcomp="engine", **waivers, co="none", topo=topo)
            add("gen-fault", f"{GEN_MARK} podkill settling {tag} N={n} {strat} {budget_tag(strat, surge, unavail)} rp {rp}", kv,
                comps(topo, n, w, roll_life, nr=nr), _podkill_reason(topo, strat, "settling", waivers) + _single_router_control(),
                dict(strat=strat, verb="podkill:settling", rp=rp, single=True), cell=("single", "podkill", topo, w, nr))
        # A rollback at the held moment of a push that never passes readiness.
        kv = dict(**_crash_once_counts(topo, n, True), **_crash_once_ceilings(topo, n, True, strat, surge),
                  co="none", topo=topo, rollback=1, during="held")
        kv["break"] = "stuck"
        add("gen-rollback", f"{GEN_MARK} rollback at held {tag} N={n} {strat} {budget_tag(strat, surge, unavail)} co none", kv, None,
            _rollback_reason("held", strat, topo, "none") + _single_router_control(),
            dict(strat=strat, verb="rollback:held", hook="held", single=True), cell=("single", "rollback-held", topo, w, nr),
            body=functools.partial(_rollback_fault_body, topo=topo, n=n, w=w, router=True,
                                   life=lib.lifecycle(strat=strat, surge=surge, unavail=unavail), co="none",
                                   configkey=False, coord=None))
    # A stable pod, or a gang's leader, deleted once at the first split of
    # the engine unit's ladder: the engine gang and both router shapes.
    for family, topo, n, w, nr in GATE_SHAPES:
        if family not in ("engine gang", "engine+router single", "engine+router gang"):
            continue
        kv = gate_kv(topo, n, nr)
        kv.update(canary="auto", fault=f"podkill:{POD_FAULT_HOOK}", victim="stable", victimcomp="engine", co="none", topo=topo)
        member = "leader" if is_mn(topo) else None
        if member:
            kv["kill"] = member
        variant = "gangOnce" if is_mn(topo) else "stableOnce"
        kv.update(nophase="Failed", cell=f"canary/Serving/capacityDropped[{variant}]")
        lost = "gang member lost" if is_mn(topo) else "pod deleted by hand"
        add("gen-canary-podkill", f"{GEN_MARK} canary podkill podkill once at auto {_canary_shape(topo, w, nr)} N={n} rp {rp}"
            + (f" target {member}" if member else ""), kv, gate_comps(topo, n, w, nr, rp),
            _pod_fault_reason("podkill", 0, "auto", member, rp, family, n, w),
            dict(strat="SurgeThenDrain", verb="podkill@auto", rp=rp, shape=family, row=f"{lost}, one-shot", single=True),
            cell=("single", "canary-podkill", topo, nr),
            coord=lib.canary_block(story_ladder("auto"), components=gate_group(topo)))


def manifest(rows, kept_empty=()):
    def tally(key, where=lambda kv, m: True):
        t = {}
        for _f, _n, kv, meta in rows:
            if not where(kv, meta):
                continue
            k = key(kv, meta)
            t[k] = t.get(k, 0) + 1
        return t

    def table(title, t):
        out = [f"### {title}", "", "| value | cases |", "|---|---|"]
        out += [f"| {k} | {v} |" for k, v in sorted(t.items())]
        return out + [""]

    def table_if(title, key):
        """A table over the cases that carry `key` in their meta; nothing
        when no family emits it."""
        t = {}
        for _f, _n, _kv, meta in rows:
            if key in meta:
                t[meta[key]] = t.get(meta[key], 0) + 1
        return table(title, t) if t else []

    per_folder = {}
    id_range = {}
    for f, n, _kv, _m in rows:
        per_folder[f] = per_folder.get(f, 0) + 1
        lo, hi = id_range.get(f, (n, n))
        id_range[f] = (min(lo, n, key=lambda x: int(x[1:])),
                       max(hi, n, key=lambda x: int(x[1:])))

    lines = [
        "# Generated hot-core corpus",
        "",
        "Emitted by `generate_cases.py` — regenerate with"
        " `python3 generate_cases.py`; CI rejects hand edits via"
        " `python3 generate_cases.py --check` (run_test.py).",
        "The gen-* folders are generator-owned wholesale; promote a bug-finding"
        " case to a hand-owned folder under a fresh id instead of editing it here.",
        "",
        f"Total generated cases: **{len(rows)}**. The whole-corpus per-PR cap"
        " (hand + generated) is enforced by run_test.py, not baked in here.",
        "",
        "## Folders",
        "",
        "| folder | cases | id range |",
        "|---|---|---|",
    ]
    for f in GEN_BLOCKS:
        if f in per_folder:
            lo, hi = id_range[f]
            lines.append(f"| {f} | {per_folder[f]} | {lo}..{hi} |")
    lines += [""]
    lines += table("By topology", tally(lambda kv, m: kv["topo"]))
    lines += table("By update strategy", tally(lambda kv, m: m["strat"]))
    lines += table("By failure verb / phase", tally(lambda kv, m: m["verb"]))
    lines += table("By restartPolicy", tally(lambda kv, m: m.get("rp", "(unset — baseline)")))
    lines += table("By pause", tally(lambda kv, m: m.get("pause", "off")))
    lines += table("By pause moment", tally(lambda kv, m: m.get("moment", "(no pause around a repair)")))
    lines += table("By push while paused", tally(lambda kv, m: m.get("push", "(no pause around a repair)")))
    lines += table("By coordination", tally(lambda kv, m: kv.get("co", "none")))
    edits = {}
    for _f, _n, _kv, m in rows:
        if "edit" in m and _f == "gen-switch":
            edits[m["edit"]] = edits.get(m["edit"], 0) + 1
    if edits:
        lines += table("By mid-roll edit (gen-switch)", edits)
    lines += table_if("By break beside the mid-roll edit (gen-switch)", "switchbreak")
    lines += table("By break victim", tally(lambda kv, m: m.get("victim", "(no break)")))
    lines += table("By migration mode (node family)",
                   tally(lambda kv, m: m["mig"], where=lambda kv, m: "mig" in m))
    lines += table("By controller-restart condition",
                   tally(lambda kv, m: m["cond"], where=lambda kv, m: "cond" in m))
    lines += table("By Instances per Component (size and router-gang families)",
                   tally(lambda kv, m: m["size"], where=lambda kv, m: "size" in m))
    lines += table_if("By stuck kind (ops-held)", "stuck")
    lines += table_if("By operation on the stuck Component (ops-held)", "op")
    lines += table_if("By overlap order (runtime-edit-repair)", "order")
    lines += table("By runtime edit (runtime-edit-repair and fault-edit families)",
                   tally(lambda kv, m: m["edit"], where=lambda kv, m: "edit" in m and kv.get("mode") == "runtimerev"))
    lines += table("By lift (preempt, pending and webhook families)",
                   tally(lambda kv, m: m["lift"], where=lambda kv, m: "lift" in m))
    lines += table_if("By window operation (webhook and conflict families)", "winop")
    lines += table_if("By operation under a pre-armed cause or a hook fault (pending, evict, preempt, node-hook and "
                      "ctrlrestart-hook families)", "faultop")
    lines += table_if("By competing-writer form (conflict)", "cform")
    lines += table_if("By competing-writer target (conflict)", "target")
    lines += table("By percent budget (gen-budget)",
                   tally(lambda kv, m: m["budget"], where=lambda kv, m: "budget" in m))
    lines += table("By operation on a service with a broken Instance",
                   tally(lambda kv, m: m["op"], where=lambda kv, m: "op" in m))
    lines += table_if("By one-shot crash (gen-crash-once)", "oneshot")
    lines += table_if("By ledger fault row (gen-lift)", "liftrow")
    lines += table("By move moment (fault-move)", tally(lambda kv, m: m["at"], where=lambda kv, m: "at" in m))
    lines += table("By move victim (fault-move)", tally(lambda kv, m: m["who"], where=lambda kv, m: "who" in m))
    lines += table("By runtime edit (fault-edit)", tally(lambda kv, m: m["rtedit"], where=lambda kv, m: "rtedit" in m))
    lines += table_if("By story (packed-pool)", "side")
    lines += table_if("By repeating fault and operation (gen-repeat)", "loop")
    lines += table_if("By ledger fault row (canary-fault-gate)", "gate")
    lines += table("By shape family (canary-fault-gate)",
                   tally(lambda kv, m: m["shape"], where=lambda kv, m: "shape" in m))
    parked_rows = [(f, n, m["parked"]) for f, n, _kv, m in rows if "parked" in m]
    if parked_rows:
        lines += ["### Parked cells (XFAIL on a tracked issue)", "",
                  "| case | folder | issue |", "|---|---|---|"]
        lines += [f"| {n} | {f} | #{i} |" for f, n, i in parked_rows]
        lines += [""]
    if kept_empty:
        lines += ["### Ids kept empty (cells of the matrix that are no case)", "",
                  "| id | folder | cell | reason |", "|---|---|---|---|"]
        lines += [f"| {n} | {f} | {d} | {why} |" for f, n, d, why in kept_empty]
        lines += [""]
    lines += [
        "## Constraint ledger (tuples excluded during construction)",
        "",
        "- `pause` multiplies `mode=kill` and the pause-and-repair family"
        " (`pauseat=`); run.py consumes the pause header nowhere else, and a"
        " rollout case that pauses without `pauseat=` is rejected as a dead knob.",
        "- `pause=freeze` pairs only with `RecreateInstanceOnPodRestart`: the"
        " freeze lift downgrades to a plain pause, which parks policy None's"
        " Create recovery — freeze+None could never repair while paused.",
        "- In-place strategies on single-pod engine-only topologies are excluded"
        " from gen-fault: the component-blind podkill victim is the in-place"
        " pod itself and its recreate trips the UID-stability check by harness"
        " design (a missing component-scoped victim hook, see below).",
        "- `fault=podkill:migrating` needs `mode=migrate` — out of scope here.",
        "- A rollback over the missing key on the running revision, kept away or"
        " put back once the rollback has fired, fires at `settling` and `rotating`"
        " only: the roll takes the broken Instance first and its replacement"
        " cannot start without the key, so no pod of the starting revision ever"
        " drains and neither the draining nor the last hook arrives; both rows"
        " take those two moments at `rotating` under their own strategy and group.",
        "- A router in front of an engine fleet is a break victim under a canary"
        " alone: elsewhere the runner admits engine and decoder pods, so the"
        " sibling rows of the engine-behind-a-router shapes outside a canary"
        " (a move, a runtime edit, a push, the idle service) have no cell and"
        " the router is a negative control there.",
        "- Kill targets (`kill=leader|worker`) exist only on gang topologies.",
        "- Update budgets are dormant under a steady-state kill: the kill family"
        " permutes strategies at their canonical budget only.",
        "- crashloop breaks an engine pod, so its coordination axis (co=none |"
        " co=rolling) exists only on the PD topologies; maxUnavailable=0 is"
        " emitted for SurgeThenDrain only, percent budgets run at the mid"
        " size where the ceil() rounding differs from the integer case, and"
        " both restart policies run under both crash forms.",
        "- A mid-roll edit (gen-switch) is a merge patch of the lifecycle's type"
        " or policy, so the declared rollingUpdate budget must be valid under"
        " both strategies of a pair: (maxSurge 1, maxUnavailable 1) whenever"
        " SurgeThenDrain is on either side, (0, 1) otherwise. The header is"
        " stated for the worse of the two shapes: the ceiling is N+1 whenever a"
        " surge step can be in flight on either side of the edit, the floor"
        " N-1 is the one the drain-first side needs and stays hard. A strategy"
        " flip carries no restart policy; the restart-policy flips run under"
        " the canonical surge budget (floor N). The switch family runs one"
        " integer budget per pair and one gang width per topology, and its"
        " crashing variants pair the surge and recreate strategies only, at"
        " the rotating hook, with form=start. Its continuation puts a break"
        " beside the edit on the six shape families: a running-revision"
        " break landed before the push (fault=crashloop:idle, imagepull:idle,"
        " configkey:missing, readiness:old; the :old crash form beside an edit"
        " stays the break armed as the bump lands) meets the edit at a moment"
        " of its own, and a pushed-revision break (break=<kind>, once=1 for"
        " its one-shot form) meets it once the roll is parked or at the first"
        " pushed pod; the capacity floor is lowered by the broken Instance,"
        " not waived.",
        "- No strategy pair is skipped in gen-switch: the bump is an annotation"
        " diff, which both in-place variants apply, and a gang runs either"
        " variant as a whole-gang recreate, so a flip into InPlaceOnly has no"
        " diff it cannot apply.",
        "- The steady-state family (`during=idle`) and the sibling family"
        " (`bump=engine victimcomp=decoder`) leave out `form=load` under"
        " `restartPolicy: None`: a runner that dies after Ready and comes back"
        " is a container restart the kubelet owns under that policy, not a"
        " wedge, so no repair is promised and neither family can read one.",
        "- On the engine-only topologies a Sequential group as the generators"
        " express it (one single-Component blueGreen group per member) is one"
        " blueGreen group over the engine, so the steady-state family emits it"
        " as `co=bluegreen` only.",
        "- The sibling family rolls the engine under the default strategy at its"
        " canonical budget: the sibling's break is independent of how the"
        " engine's own pods are replaced.",
        "- A rollingUpdate group admits no push that names one member without"
        " the other, so the sibling family's `co=rolling` cells bump engine and"
        " decoder together (`bump=engine,decoder`, the decoder's ceiling N+1 for"
        " its own surge) and the decoder's broken Instance is judged as the"
        " roll's: it ends on the new revision serving with no repair recorded"
        " after the bump.",
        "- Every cell of both families runs live and asserts the repair"
        " (`expect=repaired`; the engine converges and the decoder is repaired):"
        " an Instance that serves nothing is rebuilt without touching its"
        " Component's budget or a group's gate, whatever group it sits in.",
        "- heal breaks the engine runner, so its coordination axis is a"
        " rollingUpdate group over the engine alone or, where a decoder"
        " exists, over engine and decoder; nothing rolls, so the update"
        " strategy stays canonical and is not an axis. `lift=after-failed`"
        f" carries `instanceReadyTimeout: {HEAL_FAILED_READY_TIMEOUT}` on the"
        " victim so the row escalates inside the case budget.",
        "- imagepull has one form (an image that never pulls has no after-Ready"
        " form) and runs each strategy at its canonical budget only; its idle"
        " half carries no group, since a grouped repair's hold is its own"
        " family.",
        "- node:notready reads the fate of a Ready Instance on a dead node under"
        " the deployed force-delete policy, a lane dimension: without"
        " `lifecycle.forceDelete` (the chart default) nothing frees the pod names"
        " a silent node holds, so the outcomes are `parked` (nothing rebuilds it"
        " while the node is down) and `repaired` (the kubelet returns and the"
        " pods it never stopped serve again), both at `lift=after-settle`; with"
        " the policy the names are freed once the node's unreachable evidence"
        " is older than the threshold and the Instance is rebuilt elsewhere while"
        " the node is down, `relocated` at `lift=after-recover`. `needs=`"
        " pins parked to the plain lane and relocated to the force-delete lane"
        " (the runner SKIPs where the controller config differs); repaired"
        " holds under both. The migration axis is Auto (the default) and Never —"
        " the API has no mode that admits explicit requests but not relocation,"
        " and neither mode moves a Ready Instance (Auto relocates on an expired"
        " attempt's deadline, which a Ready Instance never has); restart policy"
        " and an idle rolling group change nothing either, and each cell says"
        " why. The update strategy is dormant at idle (one canonical budget).",
        "- The packed-pool family runs RecreatePod and InPlaceIfPossible at"
        " maxSurge 0, maxUnavailable 1 only: a surge needs room a packed pool"
        " has not got (the tight-cluster family's story), and InPlaceOnly"
        " refuses the after-Ready form's env diff and patches the at-start"
        " form as InPlaceIfPossible does. Its group axis is a rollingUpdate"
        " group on the prefill/decode shapes, whose decoder carries the same"
        " one-down budget; blueGreen and Sequential need a whole second set"
        " of pods. The mirror (fault=crashloop:idle) runs under restart"
        " policy None only: the break lives in the live pod, not the"
        " template, so RecreateInstanceOnPodRestart heals it with its own"
        " rebuild before the push lands. The after-Ready form carries"
        " floor_soft=1 and reports the one-down rule and the landing order:"
        " a runner that exits after Ready can be Ready when the roll decides.",
        "- ctrlrestart has no hook inside a repair (one fault per case; the"
        " idle-break handler fires no second one), so a restart while a"
        " crash-loop repair is open is not emitted. The strategy axis is live"
        " for the two rollout conditions and dormant under a migration and a"
        " scale-down, where both strategies still run for the family's symmetry;"
        " a rolling group is crossed in on the PD rollout conditions only. The"
        " hook half (from p4668) deletes the manager as a rollback, a strategy"
        " edit, a scale-up and a delete land and at the first pod of a runtime"
        " edit's new revision, on the six ledger shapes under the surge"
        " strategy (the edit's roll switches to RecreatePod): the restart"
        " loses no pod, so the floor and the order are judged in full and the"
        " operation must complete with nothing created or deleted twice. Its"
        " persistent form is the repeating family's.",
        "- The gen-canary-* families drive operator stories through a canary at small size:"
        f" {STORY_N} single-pod Instances, SurgeThenDrain with the engine surge"
        f" at {' and '.join(str(s) for s in STORY_SURGES)}, a four-step ladder"
        f" ({'/'.join(str(tw) for _nc, tw in STORY_LADDER)} percent capacity and"
        " traffic) with a timed soak in place of an analysis gate, on the"
        " engine and prefill/decode shapes only (the stories run single-pod"
        " Instances; the gang shapes are the crashloop family's).",
        "- gen-canary-crash breaks the stable side at the first split"
        f" ({STORM_HOOK}): every break kind and form, both restart policies,"
        f" {' and '.join(str(v) for v in STORM_VICTIMS)} victim Instances at once."
        " The floor is read with the victims allowed for, not waived.",
        "- gen-canary-second-rev lands the second revision as an ISVC re-bump or a"
        " runtime edit (a dedicated runtime per case), at staging and at the"
        " first split, with and without an analysis gate on every step before"
        " the cutover; the restart policy is None (a re-target repairs nothing).",
        "- gen-canary-podkill deletes one old-revision engine pod at a manual"
        " gate, under the operator's pause, under a capacity gate held by an"
        " image that cannot start, and under that gate paused; the engine surge"
        " is 1 (the budget is dormant while the roll is held).",
        "- gen-canary-fault-old breaks the stable side at the step kinds that"
        " differ in mechanism: before the push (the push meets a broken fleet),"
        " the capacity step ahead of traffic, the second step's timed soak, the"
        " same step at a manual gate (promoted"
        f" {BREAK_GATE_HOLD_S}s after the break), the same step at an analysis"
        " gate, and the last gate before the cutover. Left out, with the reason:"
        " the first split, which gen-canary-crash drives with the same faults,"
        " victims and policies; the cutover itself (Promoting), where at most"
        " the surge budget of stable Instances is left and already draining, so"
        " a break has no settled row to land on (the last gate carries that"
        " claim); a traffic step apart from its soak, since every traffic step"
        " of the story ladder but the cutover soaks; the surge budget at the"
        " gates, where no capacity builds while they hold (surge 1 only); three"
        " victims at the manual gate, whose storm is the first split's under"
        " another release; the crash forms under the analysis gate, since the"
        " metrics image is a static binary with no shell to exec a kill into"
        " and no fixture image fails to start under its entrypoint (the pull"
        " failure alone breaks it); the analysis gate crossed with the other"
        " step kinds, since the gate changes only what ends a hold, not how a"
        " break meets it; and a missing ConfigMap key, whose reference both"
        " revisions share, so a push is never its fix (the idle configkey"
        " family lifts the cause instead).",
        "- gen-canary-fault-new arms the break on the pushed engine revision"
        " and crosses where it bites (a crash at start and a pull failure at"
        " staging; a crash after Ready inside the first soak and past it) with"
        " the exit at the park (a rollback, or a fix push that drops the broken"
        " runner), both shapes and both restart policies. Left out, with the"
        " reason: the surge budget where the ladder parks at its first capacity"
        " step, which asks one Instance (surge 1 only); the crash forms under"
        " the analysis gate (the metrics image carries no shell and no exit"
        " switch); the rollback exit under the analysis gate, whose terminal"
        " verdict requires an analysis sample a park before the first gate"
        " never yields (the fix exit walks the gated ladder); a missing"
        " ConfigMap key, which no verb arms on a template and whose bite is a"
        " container that never starts, the crash-at-start cell; and a manual"
        " policy, since a broken revision parks before any gate is reached.",
        "- The further rows of gen-canary-fault-old (continued at p11500), gen-canary-fault-new"
        " and gen-canary-podkill run the engine unit's ladder on the ledger's six shape"
        " families (the gate family's shapes). The stable side: a serving pod that stops"
        " passing readiness, kept or lifted; the key put back once its wait has shown; a"
        " crash and a pull failure taken away again; and the runner of one Ready stable"
        " pod killed exactly once, before the push (single-pod fleets), at the second"
        " step's soak and at the same step's manual gate. The pushed revision: a missing"
        " key and a revision that never passes readiness under both exits at the park;"
        " the key added, the switch lifted and the image put back once they have bitten;"
        " the runner of the first canary pod killed once at start and after Ready under"
        " both restart policies; and a taint on every node armed before the push, lifted"
        " after the wait has shown or kept to the park. The pod faults: an old-revision"
        " engine pod (a gang member on the gangs) deleted again each time it is back, and"
        " evicted once under a soak, once at a manual gate held until the Instance is"
        " whole, and again each time it is back. The running revision's key kept away"
        " before the push, on every shape family: both revisions read it, so the push is"
        " never its fix; the stable-side break does not hold the ladder, the pod placed"
        " for the first capacity step waits on the same key, and the ladder parks at that"
        " step naming the missing key, the stable side still serving and no traffic"
        " reaching the step whose pod never came up. The kept crash and pull failure,"
        " which the storm families drive on the single-pod engine and prefill/decode"
        " fleets, run on the gangs and the engine-behind-a-router shapes too: one stable"
        " Instance crashing at start or after Ready under both restart policies, or"
        " sitting in ImagePullBackOff, at the second step's soak; and the pushed engine"
        " revision crashing at start, crashing inside the first soak or unpullable, under"
        " both exits at the park. Left out, with the reason: one kill"
        " at start on the running side at a grid state (a running pod has"
        " no first start to catch; its one-shot form is the lifted crash); a preemption at"
        " a grid state (its press runs in the pending driver's own watch; the eviction"
        " fills the ledger row); the idle position on the gangs (as the gate family: a"
        " fleet of two reads a gang as a single pod); the manual gate for a lifted crash"
        " or pull failure (the gate's hold after a break is the fault-old gate step's);"
        " the readiness rows under RecreateInstanceOnPodRestart (readiness loss opens no"
        " repair under either policy); and the one-shot pushed breaks under"
        " RecreateInstanceOnPodRestart (a pod that never started has no restart to read).",
        "- gen-canary-fault-gate drives the engine unit's ladder on the ledger's six"
        " shape families (the router, where present, is outside the group and idle;"
        f" single-pod fleets at {STORY_N} Instances, gangs at 2 Instances of 1 leader + 1"
        " worker, SurgeThenDrain surge 1) and breaks what a step's gates read. The"
        " metric and source rows gate every step but the cutover on the canary-scoped"
        " error rate (`revision_hash=\"{{.CanaryRevision}}\"`, the label the bundled"
        " Prometheus stamps from the pod's revision hash), so a breach on the stable"
        " side never reaches the gate by construction: a query that reads the whole"
        " Component blames the stable side and is not emitted. The stable side"
        " breaches through the metrics image's live override (every stable pod of"
        " the engine, kept or for 45 s) before the push, at the second step's gate"
        " and at the last gate; the canary side from birth (the push carries the"
        " breach, kept or decaying after 60 s) and at the same two gates (the"
        " override on every canary pod). The source is pointed at a name that"
        " resolves nowhere at the three positions, under both lifts (restored 20 s"
        " after the hold is observed, or never) and both stall policies (Hold parks"
        " Failed with reason AnalysisStalled at the step; RollbackOnStall rolls"
        " back), and at a server that is not Prometheus at the mid-ladder gate under"
        " Hold alone: the gate reads a non-answer the same way wherever it lands."
        " Left out, with the reason: the restart policy on the metric and source"
        " rows (nothing restarts under a breach or a lost source; None throughout);"
        " the ladder positions other than mid-ladder on the gangs (a fleet of two"
        " has no stable Instance left at the last gate, and the gate reads a gang"
        " as it reads a single pod); the canary side's breach before the push (no"
        " canary pod exists yet; its birth breach is the push); Rollback as the"
        " stall policy (it reverts on the first unreadable sample, so a blip would"
        " revert a healthy canary by configuration, and the hand-written analysis"
        " cases carry that cell).",
        "- The sibling rows of gen-canary-fault-gate run on the shapes with a"
        " Component beside the engine unit: the router of an engine ladder (never"
        " rolled by the ladder, so the repair owns its break) and the decoder of a"
        " prefill/decode unit (admission makes engine and decoder one canary unit,"
        " so the ladder owns its running-side break and lands the fix on it). The"
        " running side breaks for good (a crash at start and a pull failure under"
        " None; a crash after Ready under RecreateInstanceOnPodRestart, the policy"
        " that repairs it) and once (`lift=after-settle`: a crash after Ready is"
        " one kill under both policies; a pull failure has its image put back 20 s"
        " after it bit, under None), at the three positions on single-pod fleets"
        " and mid-ladder on gangs, under the plain fixture runtime and timed soaks"
        " (a break needs a shell to kill or an image to swap; the analysis gate"
        " changes nothing about a sibling). The new side: the decoder of the unit"
        " carries the fault-new breaks (crash at start, pull failure, crash inside"
        " and past the first soak; a pull failure alone on the gang) under both"
        " exits; the router is pushed beside the ladder onto an image that cannot"
        " be pulled (`bump=<unit>,router imgcomp=router`), rolls on its own and is"
        " judged as a parked sibling while the ladder is judged on the unit; and a"
        " pod of the sibling's new revision (the decoder's canary, or a router"
        " pushed beside the ladder) is killed once mid-ladder and at the last gate"
        " under both restart policies. Left out, with the reason: a sibling on the"
        " engine-only shapes (there is none; the ledger removes those cells); a"
        " running-side crash after Ready under None for good (a container restart"
        " the kubelet owns, not a wedge: no repair is promised, as in every idle"
        " family); the decoder as a roll beside the ladder (admission names it in"
        " the engine's group); a fix push for the parked router (the crash-loop and"
        " fault-new families prove a fix lands on a parked roll); a router pushed"
        " beside the prefill/decode gang's ladder (that shape carries the unit's"
        " new-side break).",
        "- A rollback's `draining` hook is not emitted for an in-place strategy on a"
        " single-pod topology: an annotation-only diff is patched in place, so no"
        " pod of the starting revision ever drains and the hook never arrives (a"
        " gang falls back to a whole-gang recreate, which drains).",
        "- crashloop:new arms the engine's new revision, so its group axis"
        " (co=none | co=rolling) exists only on the PD topologies; it runs"
        " each strategy at its canonical budget and leaves InPlaceOnly out:"
        " form=load's exit switch is a pod-template env diff in-place cannot"
        " carry (the InPlaceIfPossible cell documents the fallback) and"
        " form=start's image patch lands the same way under either in-place"
        " type.",
        "- The group family runs a policy only where it has a peer: a"
        " one-member sequence is a blueGreen group and a ratio needs two"
        " Components, so engine-only runs blueGreen and the zero-budget"
        " rolling group alone; maintainRatio's tolerance is 50 at N=2 and 25"
        " at N=4.",
        "- The pause-and-repair family leaves out `form=load` under"
        " `restartPolicy: None`: a runner that dies after Ready and comes back"
        " is a container restart the kubelet owns under that policy, not a"
        " wedge, so there is no repair to time a pause against.",
        "- A freeze after the repair has finished holds nothing a pause does"
        " not, so the family emits only `pause=true` at `pauseat=after`.",
        "- The pause-and-repair family runs the engine under SurgeThenDrain"
        " maxSurge 1 maxUnavailable 1: a fresh crash-loop repair is charged"
        " against the unavailability budget, so under maxUnavailable 0 the"
        " repair the pause is timed against never opens.",
        "- The grouped pause-and-repair cells run live: a repair of an Instance that serves nothing is admitted without the budget or the gate,"
        " so the rollingUpdate group changes nothing.",
        "- The engine resolves a percent budget as ceil(replicas * percent / 100),"
        " clamped to replicas (the per-Component budget's documented rule, which"
        " the canary step capacity shares), so at one and two Instances 25 and"
        " 50 percent both resolve to one and no budget rounds to zero. gen-size"
        f" ({' and '.join(str(n) for n in SIZE_NS)} Instances per Component, the router sized with the fleet)"
        " drives the percent budgets on the plain push, where the resolved budget"
        " is read off the peak and the floor, and runs the crash loop (both forms,"
        " both restart policies), the scale (one to two and two to one) and the"
        f" canary ({'/'.join(str(tw) for _nc, tw in SIZE_LADDER)} percent capacity and traffic, on"
        f" {SIZE_CANARY_N} Instances) under the integer budgets they resolve to. The"
        " canary runs on two Instances only: a canary on one Instance has no"
        " Instance to hold on the stable revision and stages in place, a shape"
        " with its own verdict to write; on two the first two steps resolve to"
        " the same single Instance, so the second moves traffic and adds no"
        " capacity. Group policies are left out: the group pacing budget rounds"
        " the same way and the group axis belongs to the crashloop family.",
        f"- gen-router-gang runs the engine as a gang ({' and '.join(f'W{w}' for w in ROUTER_GANG_WIDTHS)})"
        f" behind a single-pod router at {ROUTER_GANG_N} Instances: the push under each"
        " strategy, the crash loop under the surge and recreate strategies only"
        " (a gang runs the in-place variants as a whole-gang recreate, the"
        " recreate cell), and the scale and the canary under the surge strategy"
        " (the strategy is dormant under a scale; the gang canary under a"
        " recreate is the controller-restart family's shape). The router is"
        " never bumped: every cell asserts it stays in rotation on the same pods.",
        "- ops-held pushes a revision that cannot be pulled and lands one"
        " operation on the stuck Component; a rolling group exists only where"
        " a decoder does (pd). The deepest scale asks for one, the least count"
        " the API admits (minReplicas 0 needs KEDA autoscaling and is refused"
        " otherwise), and the verdict reads the count the controller accepted"
        " rather than assuming it; on the gang fleet of two, to one is down by"
        " one, so that cell is the scale-down row.",
        "- runtime-edit-repair lands an edit on an open repair only where a"
        " repair opens: a crash after Ready opens one only under"
        " RecreateInstanceOnPodRestart, so the edit-during-repair order skips"
        " form=load under policy None; the crash-during-edit order keeps that"
        " cell (the roll must still land on a pod the kubelet alone restarts)."
        " The PD shape carries no router, as the runtime-rev cases do: the"
        " edit rolls engine and decoder, each onto its own revision.",
        "- The migrate-and-bump family moves one Instance by request only: the"
        " engine's own (automatic) migration is a born-terminal relocation record,"
        " written when an Instance whose node is gone is rebuilt elsewhere; it has"
        " no in-flight phase for a roll to overlap, and the one verb that provokes"
        " it, a node loss, belongs to the node-loss family.",
        "- `during=last` is not emitted with maxSurge 2: a surge of two promotes its"
        " last pair together, so the moment with exactly one Instance still off the"
        " new revision is not reliably observable.",
        "- readiness:old runs on the idle service at the canonical SurgeThenDrain"
        " budget: update budgets are dormant with no roll, so strategy multiplies"
        " only the readiness:new side; readiness:new has one form, since a revision"
        " that never passes readiness has no one-shot shape (a slow start is the"
        " startup delay, not a readiness failure).",
        "- gang-member cells at steady state are the mode=kill cells gen-restart"
        " already emits (kill=leader|worker x restart policy x strategy); this"
        " family adds the mid-roll timings, each bound to the gang it aims at"
        " (settling: a settled gang; rotating: the gang forming on the new"
        " revision; draining: a serving gang beside the first one leaving), and,"
        " from p7172, a serving gang's leader deleted under restart policy None"
        " as a rollback, a strategy edit, a scale up or down and a delete land"
        " and at the first pod of a runtime edit's new revision, on the gang"
        " ledger shapes; gen-fault carries the same moments on the single-pod"
        " shapes from p1520 (the operation still completes, the replacement is"
        " labelled as what it runs, a delete takes the deleted pod with the"
        " rest), and gen-repeat the persistent form.",
        "- tight: RecreatePod under the quota squeeze has no lift axis — the drain"
        " frees the gang's own room, so the roll completes with no wait, and the"
        " cell asserts that instead.",
        "- evict has no lift: the Eviction API is a one-shot removal, so the"
        " family crosses topology x restartPolicy (x strategy under the roll)"
        " only. Its steady-state cells are the kill verdict with the removal"
        " done by the Eviction API; the product's disruption budget"
        " (`podDisruptionBudget.omeNative` in the chart) may refuse it while a"
        " pod is already down, the runner asks again as a drain does, and a"
        " budget that never admits one eviction fails the case.",
        "- preempt and pending cross `lift=after-settle|never` (the cause"
        " removed once the wait has held, or kept to the end so the hold is"
        " judged), run on one gang width, with `co=none` (a group changes"
        " nothing about where a pod can be placed) and under the two"
        " strategies a roll waits under (in-place strategies create no pod to"
        " place). Both verbs need a cluster the harness owns (every node is"
        " blocked); elsewhere the case is SKIP with the reason. The preempt"
        " roll cells waive the floor and the order (the preemption takes a pod"
        " the budget cannot absorb); the pending verdict keeps its own floor.",
        "- a pending cell's quota admits one pod fewer than the operation"
        " needs: `quota:0` for a single-pod surge, `quota:<members-1>` for a"
        " surge gang (room for all but one member), and `quota:-1` (one pod"
        " fewer than run) under a repair or a drain-first roll, which create"
        " after a pod is gone; on a gang under RecreateInstance that leaves"
        " the whole gang short of one member. A gang surge roll held to the"
        " end by that quota is the tight-cluster family's cell, so the pending"
        " family leaves its `quota:<members-1> lift=never` cell out. A quota"
        " with no room is also armed before a canary push on the shapes the"
        " hand-written canary cases do not run (minted after the other rows):"
        " kept, the capacity step parks the ladder Failed at its ready timeout;"
        " lifted once the ladder has parked, the capacity lands, the park holds"
        " and a resume walks the ladder to Stable.",
        "- under a rollback, a strategy edit, a runtime edit, a scale and a delete"
        " the three families run one case per ledger cell under restart policy"
        " None: a taint or a quota armed before a rollback (drain-first, the"
        " rollback at its draining hook: the roll's first replacement is what"
        " waits, and a roll that cannot place its first pod reaches no later"
        " hook; the surge roll under the same cause never drains, so it offers no"
        " rollback moment), before a strategy edit landed as the push lands and"
        " before a runtime edit; an eviction as each operation lands, once and,"
        " where what it took comes back, again (a delete brings nothing back, so"
        " its cell is one-shot; a scale-up's persistent form is the repeating"
        " family's); a preemption as each operation lands, aimed at a pod the"
        " operation places or, under a rollback at its first surge pod and a"
        " scale-down, at a settled Instance it keeps, the nodes opened again"
        " once the wait has held (a delete places nothing until its recreate,"
        " so it meets the eviction alone).",
        "- gen-budget drives the percent budgets the engine resolves: maxSurge under"
        " SurgeThenDrain and maxUnavailable under RecreatePod at"
        f" {', '.join(BUDGET_PERCENTS)} on fleets of 4 and 8 Instances, for a plain"
        " push, a push with one engine Instance crash-looping on the running"
        " revision (form=start, restart policy None: the roll owns the broken"
        " Instance under either policy) and the story families' four-step auto"
        " canary. The engine resolves a percent budget as N x percent rounded up"
        " and clamped to N, for both budgets alike (the Deployment convention of"
        " rounding maxUnavailable down is not its rule), and every case states"
        " the rule with its own numbers: at 10 percent both fleets round up to 1,"
        " where rounding down would leave 0 and no roll could start, so the roll"
        " converging is the proof; at 25 and 50 percent the ceiling or floor"
        " scales with N. Left out, with the reason: the in-place strategies,"
        " whose maxUnavailable the drain-first cells already read and whose own"
        " family is gen-inplaceonly; the prefill/decode gang at 8 Instances, a"
        " 34-pod fleet that would cost a lane shard per case while the rounding"
        " is a function of N and the percent, which the single-pod cells at 8"
        " carry; the crash form load and the restart policy under the crash"
        " loop (gen-crashloop crosses both at 25 percent); and the gangs under"
        " the canary beyond the engine gang at 4 (the pod budget of a canary"
        " lane). The capacity floor is waived on the crash-loop cells as on"
        " every crash-loop case; the ceiling stays hard.",
        "- gen-inplaceonly drives InPlaceOnly on what sets it apart. An image"
        " change is the one diff it patches onto the live pod: on a single-pod"
        " Instance the roll lands in place with UIDs stable; a gang runs every"
        " in-place strategy as a whole-gang recreate (eligibility reads the"
        " leader's spec alone), so it is rebuilt whole and never refused. An"
        " env, a resources or a probe change exceeds container images: on a"
        " single-pod Instance the push is refused (`expect=held`: nothing is"
        " recreated, patched or surged for the hold window, an"
        " InPlaceUpdateNotPossible event says why, then the corrective push"
        " drops the override and lands in place); on a gang it is a whole-gang"
        " recreate. A crash loop at idle is repaired by a rebuild, since a"
        " repair is not an update (form=load under restart policy None is left"
        " out, as in every idle family: no repair is promised there). A crash"
        " loop under a push belongs to the roll: an image push patches the"
        " crashing pod itself and it serves again; an env push is refused, the"
        " repair yields to the roll, and the Instance waits broken for the"
        " corrective push, which patches it in place. One fleet size (4), both"
        " restart policies everywhere (live under the crash loops, a negative"
        " control under the pushes), the canonical budget maxUnavailable 1.",
        "- gen-fault-scale and gen-fault-delete break Instance 0 of the settled"
        " service, the lowest index, so a scale-down's natural victim (the"
        " highest index) and the broken Instance differ and the order of"
        " removal can be told apart. The engine runs under SurgeThenDrain with"
        f" maxSurge {BROKEN_BUDGET[0]} and maxUnavailable {BROKEN_BUDGET[1]} in"
        " both families: nothing rolls, so the strategy is dormant, and the"
        " unavailability budget of one admits the repair that takes the broken"
        " Instance offline (the canonical budget of zero holds it). Readiness"
        " loss is repaired under neither restart policy, so its policy axis is"
        " collapsed to None; a crash at start, a crash after Ready and a pull"
        " failure run under both, and each scale-up cell states on line two"
        " what becomes of the broken Instance (expect=repaired|left) and why."
        " On the gang fleet of two only down by one and up by one are emitted:"
        " to one is down by one there, and down by two asks for zero, which the"
        " API refuses (minReplicas 0 needs KEDA autoscaling). A rolling group is"
        " crossed in where a"
        " decoder exists; the delete family keeps the same axes. The quota is"
        " also kept to the end under the scale-up and the delete (minted after"
        " the other rows): the new Instance's row, or a row of the recreate,"
        " waits with its reason across the hold window and nothing settled is"
        " taken down to make room.",
        "- gen-fault-move fires the fault at a moment of a move (at=accept, the"
        " request recorded; at=surge, the replacement pod up; at=drain, the"
        " source going) on the moved Instance (its source pod at accept, its"
        " replacement at surge and at drain), on a peer (the highest other"
        " Instance) or on the decoder (victim=sibling, PD only). Left out, with"
        " the reason: pd-mn (the move copies a whole gang whatever the decoder"
        " does; the gang axis is engine-mn's, the sibling axis pd's); the update"
        " strategy (dormant: no roll is in flight, a move surges whatever the"
        " strategy and a repair rebuilds in place); a rollout group (a repair"
        " of an Instance that serves nothing is admitted without its gate; the"
        " group axis is the steady-state crash-loop family's); the"
        " restart policy on a crash at start and a pull failure (both reach the"
        " crash-loop repair under either policy, so they run under None) while"
        " the after-Ready form runs under RecreateInstanceOnPodRestart, the one"
        " policy that promises its repair; a node loss anywhere but the source"
        " node (the story), at drain (the source is already going, its node dying"
        " then is the sweep of a terminating pod, which the surge moment reads"
        " with the replacement in place), and on the lane without the"
        " force-delete policy (a source pod on a silent node is never freed, so"
        " the move cannot finish; that fate is the node family's parked cell);"
        " the readiness switch (fault=readiness:old is repaired under neither"
        " restart policy, so it is the broken-Instance families' break and is"
        " not fired at a move moment as a break the product repairs). The"
        " family's later rows put the same request beside the remaining"
        " faults: a crash or a pull failure healed again once it has bitten"
        " (lift=after-settle) and a runner killed once on the replacement"
        " (once=1); the missing key put back or kept away and the readiness"
        " switch lifted or kept (victim=moved|peer, the decoder as sibling);"
        " a pod deleted by hand at the request, a gang member under kill=,"
        " again under repeat=; the moved Instance's pod evicted at each"
        " moment, again under repeat= at surge, and preempted as the"
        " replacement appears; a NoSchedule taint before the request; and a"
        " move on a push stuck on an unpullable image (held and failed), a"
        " crash at start, a crash after Ready, a missing key or a readiness"
        " that never passes (held), the decoder's push stuck beside the"
        " engine's move on the prefill/decode shapes. Left out: the router's"
        " push stuck beside a move (the router is bumped by no group, so a"
        " stuck router push has no driver) and the missing key or the"
        " readiness switch on the router (its image carries neither the"
        " fixture's ConfigMap env nor the readiness switch). The request also"
        " meets a quota with no room (fault=quota:0 expect=fail, the bounded-"
        " failure contract of the hand-written fault cases; lift=1 lifts it"
        " after the failure and a fresh request completes) on the shapes those"
        " cases do not run, minted after the other rows.",
        "- gen-fault-edit edits the dedicated runtime every Component inherits"
        " (engine, decoder and router each roll onto their own revision) under"
        " the surge and the drain-first strategies, and breaks a pod of the"
        " running revision on the engine or the decoder as the edit lands; its"
        " bad-edit half pushes an image that cannot be pulled or a readiness"
        " that never passes and then the corrective edit. Its further rows run"
        " one case per ledger cell under the surge strategy and the env edit: a"
        " break of the running revision landed on the settled service before the"
        " edit (a crash at start, after Ready and a pull failure healed once the"
        " edit has fired, a readiness loss kept and lifted, the key put back and"
        " the key kept away, which parks the roll with its reason; a sibling's"
        " crash after Ready healed), and a break the edited runtime itself"
        " carries (a crash at start, after Ready and a missing key kept until the"
        " corrective edit, and each kind plus a pull failure and a readiness loss"
        " once on the first pushed pod; a sibling's crash at start kept and"
        " once). Left out, with the"
        " reason: the in-place strategies (the switch family's); the env edit"
        " beyond the engine shape (under these strategies both edits are one new"
        " revision; the pod-template difference matters to an in-place strategy"
        " alone); the restart policy on a crash at start and a pull failure"
        " (they open no runner-restart trigger, so the roll owns the Instance"
        " either way; the after-Ready form runs under both); any restart policy"
        " but None on the bad edit (a revision that never becomes Ready restarts"
        " no runner); a rollout group, as in fault-move; the restart policy on"
        " the further rows (one policy per cell). The readiness-that-never-passes cell asserts the"
        " fix and the floor and reports whether the status said why: nothing"
        " escalates a pod that runs but never passes readiness before its ready"
        " timeout.",
        "- gen-repeat is the persistent form of a pod deleted by hand, a gang member"
        " lost, an eviction or a preemption and a manager restart: the fault is"
        f" applied again each time what it took has come back ({REPEAT_N} times;"
        f" {REPEAT_PREEMPT_N} for the preemption, whose fill-and-press cycle is the"
        " slowest), then stops, and the case reads the window and the end state"
        " after the lift. It runs on the six shape families at steady state"
        " (mode=kill; the manager on the idle service), under a push with the"
        " surge and the drain-first strategy and under a scale-up by one; the"
        " manager is deleted again each time it is Ready under a rollback, a"
        " strategy edit, a runtime edit, a scale-down, a move, a delete and a"
        " two-step canary too (from p8826), the window held open until the"
        " restarts are spent so the later ones land on the operation's settled"
        " end, and the pod (a gang's worker under the rebuild-whole policy)"
        " again under a rollback, a strategy edit, a runtime edit and a"
        " scale-down (from p8868); a delete brings nothing back, so the pod"
        " deleted as a delete lands has the one-shot form only. The"
        " loop takes the same Instance again once it is back (the deleter hits"
        " the pod that came back), the running revision's pods under the push"
        " (victim=stable), and the engine on the prefill/decode shapes"
        " (victimcomp=engine), so the fault and the operation meet on one"
        " Component. The kill cells run at the canonical surge budget, dormant at"
        " steady state, so the floor reads N minus the killed Instance. Left out,"
        " with the reason: a gang member lost under a push with restart policy"
        " None (the one-shot gang-member family drives it; the repeating loss"
        " stresses the rebuild-whole path) and the eviction loop under a push or"
        " a scale with RecreateInstanceOnPodRestart (the policy only widens what"
        " is rebuilt, which the steady-state eviction cells cross; the eviction's"
        " own question is the disruption budget admitting each removal); the"
        " preemption loop under a push (a repeating preemption needs the kill's"
        " fill-and-press cycle, which the roll handler does not run) and with"
        " lift=never (nothing comes back while the fillers stay); the manager"
        " restarting while a repair is open, as in the one-shot family. The"
        " cluster-wide forms (the manager restarts, the preemptions) run one at a"
        " time after the pool, as every such case does.",
        "- gen-webhook points the admission webhook's configurations at a Service"
        " that does not exist on the settled service, as a push lands and as a"
        " scale-up lands, for a"
        " bounded time (lift=after-hold: pointed back once the status has said"
        " why a write waits, or after the settle window at idle) or across the"
        " hold window (lift=after-window), on every shape family of the"
        " coverage ledger at one gang width (the outage acts on the"
        " controller's writes, not on a gang's members). The push runs under"
        " the surge and the drain-first strategies: the outage refuses the pod"
        " a surge creates before anything is torn down, and the one a"
        " drain-first roll creates after a pod is gone; the steady state and"
        " the scale are dormant to the strategy. No rollout group: the engine"
        " is the only bumped member. The in-place strategies are left out: an"
        " annotation-only diff is patched onto the live pod and creates"
        " nothing for admission to refuse. A webhook case runs alone (the"
        " outage is cluster-wide: every ISVC write in the cluster is refused"
        " while it holds). The outage also opens at the push's later hooks"
        " (the first pod of the new revision, the first pod of the old one on"
        " its way out, under both strategies), as a rollback filed at the first"
        " new pod lands, as a strategy edit landed there lands, as a scale-down"
        " by one lands, as an env edit of the case's dedicated runtime lands"
        " (every Component rolls), as soon as a move's request is recorded,"
        " and as a delete lands (the same name is re-created once the window"
        " has closed); each reads the same window verdict, and the operation"
        " must complete once the webhook is back. It also opens while the"
        " second step of the engine unit's ladder soaks its split, on the six"
        " shape families as the gate family runs them: the step waits with a"
        " reason while admission refuses, the ladder completes once the"
        " webhook is back and its step index never goes back.",
        "- gen-conflict runs a competing writer (an annotation patch of the"
        " ISVC or of every InferenceReplica of the service; target=isvc|ir)"
        " once or until the operation completes (form=once|persist) over the"
        " same operations, hooks and shapes, under the canonical surge budget"
        " alone: a status write conflicts the same way under every strategy."
        " The writer is per service, so its cases run in the concurrent pool."
        " The reconcile counters are read on the webhook cases only (they run"
        " alone); a conflict case bounds the controller's events on its own"
        " objects. The writer also runs over the engine unit's ladder at the"
        " second step's soak on the six shape families (one write, or writes"
        " until the ladder ends; the ISVC as the target): the status write is"
        " retried and the step moves once, never back.",
        "- The plain operations on the shapes the hand-written folders do not run,"
        " one case per ledger cell with no fault, land in the family that owns the"
        " operation as the control row its faulted rows are read against, minted"
        " after every other row: a push on the engine behind a router"
        " (gen-rollout); a rollback and a strategy edit on both engine-behind-a-"
        " router shapes (gen-rollback, gen-switch); a runtime edit and a delete on"
        " the router shapes and the prefill/decode gang (gen-fault-edit,"
        " gen-fault-delete); a scale up and a scale down on the engine behind a"
        " router (gen-fault-scale); a move on both router shapes (gen-fault-move).",
        "- The single cells the shape axis left on the engine-behind-a-router"
        " shapes, one case each in the family that owns the verb and minted after"
        " every other row: the node lost once and for good at steady state"
        " (gen-node); the key put back once its wait has shown, and a pull failure"
        " of the running revision at steady state and as a push lands (gen-heal,"
        " gen-imagepull); a pod deleted by hand under a push on the single-pod"
        " shape and a gang leader lost under a push on the gang (gen-fault,"
        " gen-gang-member); a stable pod or gang leader deleted once at the first"
        " split of a canary on the engine gang and both router shapes"
        " (gen-canary-podkill); and a rollback at the held moment of a push that"
        " never passes readiness (gen-rollback).",
        "- The coverage ledger (coverage.py, `EXCLUDED_CELLS`) removes a cell of"
        " its product only with a written reason, and a cell counts under the"
        " first rule that matches; beside the four shape rules, four rules"
        " remove 1068 cells. The pause and freeze columns outside the kill"
        " family's own cells (540): a pause or a freeze is a moment of another"
        " cell, not an operation a fault crosses: the cell is proved by pausing"
        " while the operation and the fault are in flight, or by the fault"
        " landing while paused, which the ledger records as the `paused` timing"
        " and the operation+operation overlap of the fault's own cell.",
        "- The new-side rows at steady state (80): a fault on the new revision"
        " has no meaning under an operation that creates no new revision.",
        "- The metric-breach and analysis-source rows outside a canary (312): a"
        " metric breach and an unusable analysis source are read by a canary"
        " step's analysis gate alone; no other operation reads the health metric"
        " or the analysis source.",
        "- The one-shot new-side rows under a scale, a move and a delete (136):"
        " a one-shot fault on the new revision is over before a scale, a move or"
        " a delete can be aimed at it: what the operation then meets is a plain"
        " roll, which is the operation+operation overlap, and a new revision"
        " that stays broken is the persistent row.",
        "- A competing write on the ISVC as a delete lands (gen-conflict, `delete`"
        " on target=isvc) has no window: the controller removes the ISVC"
        " finalizer in the reconcile that sees the deletion, so every write"
        " after it is refused NotFound; the delete window runs against the"
        " InferenceReplicas alone, which outlive their pods under their own"
        " finalizer, and the ISVC cells' ids are kept empty (the table above)"
        " so the ids after them keep their places.",
        "- Kill-family cells pinned by hand-written restart/ cases are excluded:",
        "",
    ]
    for key, pin in sorted(HAND_PINNED_KILL.items(), key=lambda x: x[1]):
        topo, w, strat, rp, pause, target = key
        cell = f"{topo} W{w}" if w else topo
        lines.append(f"  - {pin}: {cell} {strat} rp={rp}"
                     + (f" pause={pause}" if pause else "")
                     + (f" target={target}" if target else ""))
    lines += heal_notes()
    lines += lift_notes()
    lines += [
        "",
        "## Missing runner verbs/hooks (future coverage, not emitted)",
        "",
    ]
    lines += [f"- {h}" for h in MISSING_HOOKS]
    lines += [
        "",
        "## Deferred fill",
        "",
        "`--fill` (t=3 covering array over coordination groups, canary"
        " promotion, and budget interactions) is documented in the generator"
        " and intentionally not emitted until the hot core has baked in the"
        " per-PR lane.",
        "",
    ]
    return "\n".join(lines)


def lift_notes():
    """What each form of gen-lift reads, and the axes it does not cross."""
    return [
        "",
        "## gen-lift: what each form reads",
        "",
        "One family fills the six ledger rows the other families left empty; every"
        " cell runs live. A kept cause (`lift=never`) is the persistent form, a"
        " lifted one (`lift=after-settle`) the one-shot form.",
        "",
        f"- `{LIFT_ROW_CONFIG_RUNNING_KEPT}`: `configkey:missing during=idle lift=never"
        " expect=failed`; the row reaches Failed with its reason inside the"
        f" `instanceReadyTimeout: {HEAL_FAILED_READY_TIMEOUT}` and, since a fresh pod cannot"
        " clear a missing key, is owed no re-arm: it stays Failed with its reason and nothing"
        " of the Instance serving across the hold, until the key is put back; both restart"
        " policies (which repair opens first differs).",
        f"- `{LIFT_ROW_CONFIG_NEW_KEPT}` and `{LIFT_ROW_CONFIG_NEW_LIFTED}`:"
        " `configkey:new` with `lift=never` (the roll holds at the ready timeout with its"
        " reason; a fix push back onto the present key lands) or `lift=after-settle` (the"
        " key is added and the roll completes with no hold behind it). The surge and the"
        " drain-first strategies decide which pod waits and the floor; one restart policy,"
        " since a pod that never started has no restart for a policy to read.",
        f"- `{LIFT_ROW_READINESS_NEW_LIFTED}`: `readiness:new form=recover`; the switch"
        " file is set in the push's first engine pod as it starts and lifted once the drop"
        " has settled; the same pod returns and no restart policy may rebuild it. Both"
        " restart policies under SurgeThenDrain (the surge pod is the first pushed pod and"
        " takes no capacity while it waits; drain-first is the same story at floor N-1).",
        f"- `{LIFT_ROW_IMAGEPULL_RUNNING_LIFTED}`: `imagepull:old during=idle"
        " lift=after-settle expect=heals`; the image is pointed back once the pull failure"
        " has settled; the kubelet or a repair brings the Instance back, both restart policies.",
        f"- `{LIFT_ROW_IMAGEPULL_NEW_LIFTED}`: `imagepull:new lift=after-settle`; the push's"
        " first engine pod is pointed at the unpullable image as it starts and back once the"
        " failure has settled; one rebuild may be reported under"
        " RecreateInstanceOnPodRestart, never required, never twice; both restart policies"
        " under SurgeThenDrain.",
        f"- `{LIFT_ROW_CONFIG_RUNNING_KEPT_PUSH}` and `{LIFT_ROW_CONFIG_RUNNING_LIFTED_PUSH}`:"
        " `configkey:missing during=idle lift=never|after-settle push=fix`; the key is removed"
        " on the running revision and, once the wait has shown for the settle window, the fix"
        " is pushed with no pause around it: a bump that gives the runner a literal in place of"
        " the key it cannot read; under lift=after-settle the key is put back as the fix lands."
        " The roll lands on every Instance, the broken one Ready on the new revision and never"
        " the last, with no repair recorded for it after the push; both restart policies.",
        f"- `{LIFT_ROW_READINESS_RUNNING_KEPT_PUSH}` and `{LIFT_ROW_READINESS_RUNNING_LIFTED_PUSH}`:"
        " `readiness:old during=idle form=persist|recover push=fix`; a serving pod stops passing"
        " readiness and, once the drop has shown for the settle window, an ordinary bump is"
        " pushed, whose re-rendered pods carry no switch file; under form=recover the switch is"
        " lifted as the fix lands. The same roll verdict; both restart policies.",
        f"- `{LIFT_ROW_IMAGEPULL_RUNNING_LIFTED_PUSH}`: `imagepull:old lift=after-settle` with no"
        " during=; the running revision's pod is pointed at the unpullable image as the bump"
        " lands and back once the failure has held for the settle window, unless the roll has"
        " replaced it first; the roll owns the Instance either way; both restart policies.",
        "- No group axis: a kept cause changes no fleet for a group to pace, and a lifted"
        " one leaves the roll to finish as any plain push; the group families cover the gates.",
    ]


def heal_notes():
    """How each half of gen-heal heals, read off the controller (the restart
    trigger, the stuck-pod evidence, the escalation pass and the exits a
    parked repair keeps)."""
    return [
        "",
        "## gen-heal: how each half heals",
        "",
        "The family asserts `expect=heals` on every cell, and every cell runs"
        " live. A rolling group changes nothing: a repair of an Instance that"
        " serves nothing is admitted without the group's gate, and the"
        " runner-restart repair is not put to it.",
        "",
        "- `lift=after-settle`: the key returns once the"
        " CreateContainerConfigError wait has held for the settle window,"
        " before any attempt escalates. Under `None` the crash-loop repair"
        " opens as soon as the wait is seen (the stuck-pod clock counts from"
        " the pod's creation and the victim is older than the grace), rebuilds"
        " the pod, and the rebuilt pod waits on the same key; under"
        " `RecreateInstanceOnPodRestart` the runner-restart repair does the"
        " same. Either way the kubelet starts the container once the key is"
        " back and the open repair promotes the Instance. The settle window"
        " (`LIFT_SETTLE_S`, 20 s before `OME_TEST_TIMEOUT_MULT`) has to stay"
        " inside the deployment's `stuckPodGracePeriod` (60 s in the chart): a"
        " multiplier of 3 or more turns these cells into the after-failed"
        " shape.",
        "- `lift=after-failed`: the same repair opens, and the rebuilt pod's"
        " wait outlives the stuck-pod grace (or the attempt its"
        " `instanceReadyTimeout`, `HEAL_FAILED_READY_TIMEOUT`), so the"
        " escalation parks the repair at Failed with its Restart kept. A parked"
        " repair keeps two exits of its own: the restart pass resumes its"
        " promote when the rebuilt set comes up, and `lifecycle.updateRetry`"
        " re-arms it for a cause a fresh pod can clear (a crash loop, a"
        " run-container error, a deadline with no reason). A park whose cause a"
        " fresh pod cannot clear (a missing ConfigMap key, an unpullable or"
        " invalid image) owes no re-arm: the row stays Failed with its reason"
        " until the config or image is fixed, and recovers by itself when it"
        " is. The key returns"
        " once the row reads Failed, the kubelet's retry brings the rebuilt set"
        " up, and the resume promotes the Instance on its running revision with"
        " no reset mailbox and no new revision, whether or not a ladder is"
        " configured.",
    ]


# ── modes ────────────────────────────────────────────────────────────────────
def hand_written(path):
    """A yaml in a gen-* folder is hand-written when line 1 lacks the mark."""
    with open(path) as f:
        return not f.readline().startswith(f"# {path.stem}: {GEN_MARK} ")


def owned_files_on_disk(root=ISVCS):
    files = {}
    for folder in GEN_BLOCKS:
        d = Path(root) / folder
        if d.exists():
            for f in sorted(d.glob("*.yaml")):
                files[f"{folder}/{f.name}"] = f
    mf = Path(root) / MANIFEST_REL
    if mf.exists():
        files[MANIFEST_REL] = mf
    return files


def check(outputs, root=ISVCS):
    disk = owned_files_on_disk(root)
    problems = []
    for rel, text in sorted(outputs.items()):
        if rel not in disk:
            problems.append(f"missing on disk (regenerate): isvcs/{rel}")
        elif rel.endswith(".yaml") and hand_written(disk[rel]):
            problems.append(f"hand-written case at a generated id (the regen refuses to run "
                            f"over it; move it to a hand-owned folder): isvcs/{rel}")
        elif disk[rel].read_text() != text:
            problems.append(f"drifted from generator output (hand-edited?): isvcs/{rel}")
    for rel in sorted(set(disk) - set(outputs)):
        if rel.endswith(".yaml") and hand_written(disk[rel]):
            problems.append(f"hand-written case in a generator-owned folder (the regen refuses "
                            f"to run over it; move it to a hand-owned folder): isvcs/{rel}")
        else:
            problems.append(f"stale generated file (regenerate to sweep it): isvcs/{rel}")
    if problems:
        for p in problems:
            print(p, file=sys.stderr)
        print(f"\ngenerate_cases.py --check: FAIL — {len(problems)} problem(s); "
              f"run `python3 generate_cases.py` and commit the result",
              file=sys.stderr)
        return 1
    print(f"generate_cases.py --check: OK — {len(outputs)} generated file(s) byte-stable")
    return 0


def guard_foreign(outputs, root=ISVCS):
    """Refuse to run over anything that is not this generator's: a yaml in a
    gen-* folder without the `[gen]` mark (hand-written, whatever its id —
    never overwritten, never swept), and a generated id used by a case
    outside the gen-* folders. Exits before anything is written."""
    problems = []
    for folder in GEN_BLOCKS:
        d = Path(root) / folder
        for f in sorted(d.glob("*.yaml")) if d.exists() else []:
            if hand_written(f):
                problems.append(f"{f}: hand-written case in a generator-owned folder — move "
                                f"it to a hand-owned folder; nothing was written or swept")
    owned_ids = {Path(rel).stem for rel in outputs if rel.endswith(".yaml")}
    for f in sorted(Path(root).glob("**/*.yaml")):
        if f.parent.name in GEN_BLOCKS:
            continue
        if f.stem in owned_ids:
            problems.append(f"{f}: case id {f.stem} collides with the generated id block — "
                            f"renumber the hand-written case out of p{GEN_ID_LO}-p{GEN_ID_HI - 1}")
    if problems:
        sys.exit("\n".join(problems))


def write(outputs, root=ISVCS):
    guard_foreign(outputs, root)
    for folder in GEN_BLOCKS:
        d = Path(root) / folder
        d.mkdir(parents=True, exist_ok=True)
        for f in sorted(d.glob("*.yaml")):
            if f"{folder}/{f.name}" not in outputs:
                f.unlink()   # stale: the guard proved every file here carries the mark
    for rel, text in sorted(outputs.items()):
        path = Path(root) / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--check", action="store_true",
                    help="regenerate in memory and byte-compare against the "
                         "committed tree; nonzero on any drift")
    ap.add_argument("--fill", action="store_true",
                    help="t=3 covering fill over the deferred dimensions "
                         "(documented follow-up; refuses to emit this round)")
    args = ap.parse_args()
    if args.fill:
        print("--fill: the t=3 covering fill (coordination groups, canary "
              "promotion, budget interactions) is deliberately not emitted "
              "until the hot core has baked in the per-PR lane.", file=sys.stderr)
        return 2

    outputs, rows = build_outputs()
    if args.check:
        return check(outputs)

    write(outputs)
    per_folder = {}
    for f, n, _kv, _d in rows:
        per_folder.setdefault(f, []).append(n)
    print(f"generated {len(rows)} cases:")
    for f in GEN_BLOCKS:
        names = per_folder.get(f, [])
        if names:
            print(f"  {f:22s} {len(names):3d}  {names[0]}..{names[-1]}")
    # A regen must never reintroduce a duplicate/shadowed case id or a
    # malformed header — fail the generation, not the next harness run.
    rc = subprocess.run([sys.executable, str(HERE / "run.py"), "--validate-only"]).returncode
    if rc:
        sys.exit(rc)
    return 0


if __name__ == "__main__":
    sys.exit(main())
