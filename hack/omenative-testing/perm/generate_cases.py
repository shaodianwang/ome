#!/usr/bin/env python3
"""generate_cases.py — the generated hot-core permutation corpus (isvcs/gen-*/).

Emits the constraint-filtered cross product of
    topology x updateStrategy x failure verb x injection phase x
    restartPolicy x pause
into the generator-owned folders below, using ONLY verbs and hooks the
runners (run.py + the KIND CI lane) already implement:

    gen-rollout/   p1000+  baseline revision-bump rollouts (failure=none)
    gen-fault/     p1200+  rollouts with fault=podkill:settling|rotating
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
                          strategy flips
    gen-crashloop-idle/    p2400+  a crash loop on the settled service, no bump
                          (during=idle), crossed with topology x rollout-group
                          kind x break victim (engine, or decoder) x crash form
                          x restartPolicy; every cell expects the repair
    gen-crashloop-sibling/ p2600+  the decoder's running revision breaks as an
                          engine-only bump lands (bump=engine, victimcomp=
                          decoder), crossed with the PD topologies x group
                          kind x crash form x restartPolicy; the engine roll
                          converges and the decoder is repaired
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
    gen-ctrlrestart/ p4600+ the manager pod is deleted under an operation in
                          flight (fault=ctrlrestart:<hook>): mid-surge,
                          mid-drain, mid-migration, mid-canary and
                          mid-scale-down, crossed with topology x strategy
                          (x a rolling group on PD rolls); the operation
                          resumes from status and converges
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
                          the ladder still lands the fix on every Instance
    gen-canary-fault-new/ p6600+  the push itself brings a broken revision
                          (crash at start, crash after Ready inside or past
                          the soak, an image that cannot be pulled); the
                          ladder stops where it bites, says why, and a
                          rollback or a fix push lands
    gen-rollback/  p5800+  rollouts rolled back: the starting revision is
                          re-applied at a named moment of the new revision's
                          roll (rollback=1 during=settling|rotating|draining|
                          last|held; held breaks the new revision with
                          break=stuck so the roll parks at its budget first),
                          crossed with topology x strategy x hook x group;
                          every Instance must end on the starting revision
    gen-crashloop-new/ p5000+  the push brings a revision whose pods crash
                          (fault=crashloop:new, form=start|load), crossed
                          with topology x strategy x restartPolicy x group;
                          the roll must stop within its budget, say why, and
                          land the fix push on every Instance
    gen-crashloop-group/ p5400+  one old-revision Instance crashes under a
                          group policy (blueGreen, Sequential, maintainRatio)
                          and under the zero budgets of a rolling group; the
                          fix lands within the policy's own rule and never
                          last on the broken Instance (fixfirst=1)
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
    gen-ops-held/  p6800+  a scale-down, a request for zero, a delete or a
                          scale-up lands on a Component stuck on a revision
                          that cannot be pulled (stuck=failed: the first
                          attempt has failed; stuck=held: the retry ladder
                          is spent), crossed with shape x strategy x group;
                          the stuck Instances go first, survivors stay
                          untouched, the unlanded push stays visible
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
                          restartPolicy
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
                          node's spare CPU taken, so the Instance waits until
                          the fillers are dropped (lift=after-settle) or to
                          the end (lift=never)
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
                          repaired under its policy
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
    only where a decoder does (pd), as in the crashloop family. A request
    for zero is kept: the API runs one Instance for it, and the case reads
    the count the controller accepted instead of assuming either answer.
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
  - ctrlrestart has no hook inside a repair: the runner fires one fault per
    case and the idle-break handler fires no second one, so a restart while
    a crash-loop repair is open is not emitted (MISSING_HOOKS). The update
    strategy is dormant under a migration and a scale-down; those cells run
    under both strategies for the family's symmetry and say so.
  - A rollback's draining hook is not emitted for an in-place strategy on a
    single-pod topology: an annotation-only diff is patched in place, so no
    pod of the starting revision ever drains and the hook never arrives (a
    gang falls back to a whole-gang recreate, which drains).
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
    does not run as zero. A rolling group is crossed in where a decoder
    exists, as in the crashloop family.
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
}
GEN_ID_LO = min(lo for lo, _hi in GEN_BLOCKS.values())
GEN_ID_HI = max(hi for _lo, hi in GEN_BLOCKS.values())
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
UNPROMISED_REPAIR = {("load", "None")}
GROUP_NAMES = {"bluegreen": "blueGreen", "rolling": "rollingUpdate", "seq": "Sequential", "canary": "canary"}
IDLE_PHASE = "idle"

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
# dormant under a migration and a scale-down.
CTRL_SHAPES = [("engine", 4, 0), ("pd", 4, 0), ("engine-mn", 2, 1)]
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
# 0 with maxUnavailable 1) on the single-pod shapes.
GROUP_CRASH_SHAPES = [("engine", 4, 0), ("pd", 4, 0), ("pd-mn", 2, 1)]
GROUP_CRASH_POLICIES = {"engine": ["bluegreen"], "pd": ["bluegreen", "seq", "ratio"], "pd-mn": ["bluegreen", "seq", "ratio"]}
GROUP_CRASH_STRATS = [("SurgeThenDrain", 1, 0), ("RecreatePod", 0, 1)]
BUDGET_ZERO_SHAPES = [("engine", 4, 0), ("pd", 4, 0)]
# maintainRatio tolerance by fleet size: at N=2 one crashing Instance is
# already half the engine, so the band must admit 1:2.
RATIO_TOL_BY_N = {2: 50, 4: 25}

# The ops-held family: an operation lands on a Component stuck on a revision
# that cannot be pulled. One size per shape (the engine's victim order and
# its hold do not depend on the fleet size), the strategy that surges and
# the one that drains first, both stuck kinds, every operation, and a
# rolling group where a decoder exists (a one-member group has nobody to
# pace with, as in the crashloop family).
OPS_HELD_SHAPES = [("engine", 3, 0), ("pd", 3, 0), ("engine-mn", 2, 1)]
OPS_HELD_STUCK = ["failed", "held"]
OPS_HELD_OPS = ["scale-down", "scale-zero", "delete", "scale-up"]
OPS_HELD_STRATS = {"SurgeThenDrain": (1, 0), "RecreatePod": (0, 1)}
OPS_HELD_GROUPS = {"engine": ["none"], "pd": ["none", "rolling"], "engine-mn": ["none"]}

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
READINESS_SHAPES = [("engine", 4, 0), ("pd", 4, 0), ("engine-mn", 2, 1)]
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

# The eviction, preemption and pending families: the shapes the fault list
# names at one gang width, the two strategies a roll waits under, the
# rotating hook for the in-flight moment, and for the causes that can be
# lifted whether they are. A pending cell's quota admits one pod fewer than
# the operation needs (see _pending_quota).
DISRUPT_SHAPES = [("engine", 4, 0), ("pd", 4, 0), ("engine-mn", 2, 1)]
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
#   ("crashloop-idle", form, topo, w, size, co, victim, rp)
#   ("crashloop-sibling", form, topo, w, size, co, rp)
#   ("heal", topo, w, rp, lift, co)
#   ("imagepull", "settling", topo, w, strat, rp, co) / ("imagepull", "idle", topo, w, rp)
#   ("rollback", hook, topo, w, strat, surge, unavail, co)
#   ("pause-repair", topo, w, form, rp, co, pause, moment, push)
#   ("migrate-bump", first, hook, topo, w, strat, surge, unavail, co)
#   ("fault-move", kind, form, at, victim, topo, w, rp)
#   ("fault-edit", kind, form, victim, edit, topo, w, strat, rp) / ("bad-edit", kind, topo, w, strat)
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


def shape_tag(topo, w):
    return f"{topo} W{w}" if is_mn(topo) else topo


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


def counts(topo, n):
    if is_pd(topo):
        return dict(Ne=n, Nd=n, Nr=lib.NR)
    return dict(Ne=n, Nd=0, Nr=0)


def ceilings(topo, n, strat, surge):
    ce = n + surge_count(n, surge) if strat == "SurgeThenDrain" else n
    if is_pd(topo):
        return dict(ceil_e=ce, ceil_d=n, ceil_r=lib.NR)
    return dict(ceil_e=ce, ceil_d=0, ceil_r=0)


def comps(topo, n, w, e_life, d_life=None, r_life=None):
    cw = w if is_mn(topo) else 0
    cs = [lib.comp("engine", n, cw, e_life)]
    if is_pd(topo):
        cs.append(lib.comp("decoder", n, cw, d_life if d_life is not None else lib.lifecycle()))
        cs.append(lib.comp("router", lib.NR, 0, r_life if r_life is not None else lib.lifecycle()))
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


def _sibling_reason(form, co):
    out = (f"the engine is bumped alone and, as the bump lands, {_break_how(form, 'decoder')}; the engine "
           f"roll converges within its budget with every engine Instance on the new revision, while the "
           f"decoder, which nothing bumped, keeps N with nothing rotated and its broken Instance is rebuilt "
           f"by a repair (every pod new, Ready and serving on its running revision) that a RestartTriggered "
           f"event names")
    if co != "none":
        out += (f"; the {GROUP_NAMES[co]} group lists both Components, so the engine's run is open while the "
                f"decoder breaks, and a rebuild of an Instance that serves nothing takes no capacity offline "
                f"and is put to neither the decoder's budget nor the group's gate: the repair opens beside "
                f"the open run, and the engine's roll completes beside the repair")
    return out


def emit_crashloop_idle(add):
    for topo, n, w in CRASH_SHAPES:
        for co in BREAK_GROUPS:
            if co == "seq" and not is_pd(topo):
                continue   # constraint ledger: one member makes Sequential a blueGreen group
            for victim in BREAK_VICTIMS[topo]:
                for rp in RESTARTS:
                    for form in CRASH_FORMS:
                        if (form, rp) in UNPROMISED_REPAIR:
                            continue   # constraint ledger: no repair is promised there
                        kv = dict(**counts(topo, n), ceil_e=n, ceil_d=(n if is_pd(topo) else 0),
                                  ceil_r=(lib.NR if is_pd(topo) else 0),
                                  fault="crashloop:old", co=co, topo=topo, form=form, during=IDLE_PHASE,
                                  victimcomp=victim, expect="repaired")
                        if co == "canary":
                            kv["canary"] = "auto"
                        meta = dict(strat="SurgeThenDrain", verb=f"crashloop:{form}@idle", rp=rp, victim=victim)
                        desc = (f"{GEN_MARK} crashloop idle {form} {shape_tag(topo, w)} N={n} co {co} "
                                f"victim {victim} rp {rp}")
                        life = lib.lifecycle(restart=RP_FULL[rp])
                        add("gen-crashloop-idle", desc, kv,
                            comps(topo, n, w, life, d_life=(life if is_pd(topo) else None)),
                            _idle_reason(form, victim, co), meta,
                            cell=("crashloop-idle", form, topo, w, n, co, victim, rp),
                            coord=group_for(co, topo))


def emit_crashloop_sibling(add):
    for topo, n, w in SIBLING_SHAPES:
        for co in BREAK_GROUPS:
            for rp in RESTARTS:
                for form in CRASH_FORMS:
                    if (form, rp) in UNPROMISED_REPAIR:
                        continue   # constraint ledger: no repair is promised there
                    kv = dict(**counts(topo, n), **ceilings(topo, n, "SurgeThenDrain", 1),
                              fault="crashloop:old", co=co, topo=topo, form=form,
                              victimcomp="decoder", bump="engine")
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


# ── gen-ctrlrestart: the manager restarts under an operation in flight ──────
def _ctrl_reason(cond, strat, topo, co):
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
    else:
        out += ("; exactly the tail is torn down, the survivors are untouched and never below the target; "
                "the update strategy is dormant under a scale-down")
    return out


def emit_ctrlrestart(add):
    for topo, n, w in CTRL_SHAPES:
        for strat, surge, unavail in CTRL_STRATS:
            life = lib.lifecycle(strat=strat, surge=surge, unavail=unavail)
            for cond in CTRL_CONDITIONS:
                coords = ["none", "rolling"] if cond in CTRL_ROLL_CONDITIONS and is_pd(topo) else ["none"]
                for co in coords:
                    kv, coord, d_life = dict(**counts(topo, n)), None, None
                    case_comps = None
                    if cond in CTRL_ROLL_CONDITIONS:
                        kv.update(ceilings(topo, n, strat, surge))
                        if co != "none":
                            # The group rolls the decoder too, under the same strategy.
                            d_life = life
                            kv["ceil_d"] = n + surge
                            coord = lib.coord_for(co)
                    elif cond == "migration":
                        # The replacement surges at a new index whatever the strategy.
                        kv.update(mode="migrate", **ceilings(topo, n, "SurgeThenDrain", 1), mig="Auto", inst=0)
                    elif cond == "canary":
                        kv.update(mode="canary", **ceilings(topo, n, strat, surge), canary="auto",
                                  cell=CTRL_CANARY_CELL)
                        if is_pd(topo):
                            # Decoder and router are group members on their default surge lifecycle.
                            kv.update(ceil_d=n + 1, ceil_r=lib.NR + 1)
                        case_comps = comps(topo, n, w, life)
                        coord = lib.canary_block(lib.canary_steps("auto", *CTRL_CANARY_STEPS),
                                                 components=[c["comp"] for c in case_comps])
                    else:
                        kv.update(mode="scaledown", **ceilings(topo, n, "SurgeThenDrain", 0), to_e=n // 2)
                    kv.update(fault=CTRL_FAULTS[cond], co=co, topo=topo)
                    meta = dict(strat=strat, verb=f"ctrlrestart:{cond}", cond=cond)
                    desc = (f"{GEN_MARK} ctrlrestart {cond} {shape_tag(topo, w)} N={n} {strat} "
                            f"{budget_tag(strat, surge, unavail)} co {co}")
                    add("gen-ctrlrestart", desc, kv,
                        case_comps if case_comps is not None else comps(topo, n, w, life, d_life=d_life),
                        _ctrl_reason(cond, strat, topo, co), meta,
                        cell=("ctrlrestart", cond, topo, w, strat, co), coord=coord)
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
    when = {"prestart": "before the push lands (the break bites on the idle service first)",
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
           f"the canary revision serving, no repair is recorded for one after the push, and the healthy Instances "
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


# ── gen-canary-fault-new: the push brings a broken revision ─────────────────
# Where the break bites is the axis. A crash at start and an image that cannot
# be pulled bite at staging (the capacity gate never fills); a crash after
# Ready bites inside the first step's soak (the runner exits before the soak
# ends) or past it (it outlives the soak and dies while the next step's
# capacity builds). The exit at the park is a rollback or a fix push that
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
    out = (f"{how}, under restart policy {RP_FULL[rp]}; the ladder stops at the step where the break bites and parks "
           f"with a reason, no traffic names the broken revision at the park while the stable side keeps serving every "
           f"Instance the broken revision had not replaced, nothing promotes the broken revision, and then {then}")
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


# ── gen-crashloop-new: the push brings a revision whose pods crash ──────────
def _crash_new_reason(form, strat, co, allowance):
    how = ("the pushed engine revision dies at every start (its runner image has no shell for the fixture command)"
           if form == "start" else
           "the pushed engine revision passes readiness, is promoted, and exits a few seconds later, "
           "every time the kubelet restarts it (Ready counted for the budget: the new pod was promoted, then crashed)")
    lands = {"SurgeThenDrain": ("the surge replacement never joins rotation, so no source is drained"
                                if form == "start" else
                                "the promoted Instance crashes, so no further source is drained"),
             "RecreatePod": "the first Instance drained is rebuilt on the crashing revision and stays the only one off",
             "InPlaceIfPossible": ("the image patches the first Instance's pod in place and it crash-loops there"
                                   if form == "start" else
                                   "the exit switch is a pod-template env diff, so in-place resolves to a recreate "
                                   "of the first Instance")}[strat]
    out = (f"{how}; {lands}: at most {allowance} Instance is off the running revision, which keeps serving at "
           f"N-{allowance}; status says why (a rolloutHold or a Failed row, and a RetryHeld or InstanceFailed event) "
           f"and no further attempt at that revision follows; the fix push then lands on every Instance, serving, "
           f"and nothing rebuilds the crashing revision")
    if co != "none":
        out += "; the decoder rolls to its own sound revision meanwhile (converges, or holds with a reason)"
    return out


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
                        _crash_new_reason(form, strat, co, max(surge, unavail)), meta,
                        cell=("crashloop-new", form, topo, w, n, strat, surge, unavail, rp, co),
                        coord=lib.coord_for(co) if co != "none" else None)


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
        return (f"maintainRatio keeps engine:decoder serving within {tol}% (the ratio check); the broken Instance "
                f"is already out of the serving count, so it comes back first or at least never last")
    zero = ("maxUnavailable 0 with maxSurge 1: every drain follows a Ready surge replacement"
            if strat == "SurgeThenDrain" else
            "maxSurge 0 with maxUnavailable 1: one Instance down at a time")
    return f"a rolling group under the zero budget ({zero}), and the broken Instance, already down, is rebuilt first or at least never last"


def _group_reason(form, co, strat, tol):
    how = ("one old-revision engine pod is pointed at an image with no shell as the bump lands, so it crashes at "
           "every start and is never Ready again"
           if form == "start" else
           "one old-revision engine pod has its runner killed every few seconds while it is up, so it dies after "
           "Ready and comes back between crashes")
    return (f"{how}; the bump is the fix and lands on every Instance within the group's own rule: {_group_rule(co, strat, tol)}; "
            f"the healthy peers never leave rotation beyond the budget, no repair is recorded for the broken Instance, "
            f"and it ends on the new revision serving; floor waived — the crash itself costs settled capacity "
            f"for the length of the roll")


def _group_for(co, n):
    members = ["engine", "decoder"]
    if co == "ratio":
        return lib.coord_group(members, "RollingUpdate",
                               {"type": "RatioBalanced", "ratioTolerancePercent": RATIO_TOL_BY_N[n]})
    return lib.coord_for(co, comps=members)


def _emit_group_cell(add, topo, n, w, strat, surge, unavail, rp, form, co):
    members = ["engine", "decoder"] if is_pd(topo) else ["engine"]
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
        _group_reason(form, co, strat, tol), meta,
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
    return {"scale-down": n - 1, "scale-zero": 0, "scale-up": n + 1}.get(op)


def _ops_held_reason(stuck, op, strat, topo, co):
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
    elif op == "scale-zero":
        what = (f"then {members} asked for zero: the API runs one Instance for that request, so the fleet "
                f"converges on the count the controller accepted with the stuck Instances gone first and no pod left "
                f"behind on a removed index")
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
    elif is_pd(topo):
        out += "; the router is neither pushed nor scaled and stays as it is"
    if is_mn(topo):
        out += "; each Instance is a gang, counted whole"
    return out


def emit_ops_held(add):
    for topo, n, w in OPS_HELD_SHAPES:
        for strat, (surge, unavail) in OPS_HELD_STRATS.items():
            for co in OPS_HELD_GROUPS[topo]:
                for stuck in OPS_HELD_STUCK:
                    for op in OPS_HELD_OPS:
                        target = _ops_held_target(op, n)
                        bump = surge if strat == "SurgeThenDrain" else 0
                        reach = max(n, target or 0)
                        kv = dict(**counts(topo, n), ceil_e=reach + bump,
                                  ceil_d=((reach + bump if co != "none" else n) if is_pd(topo) else 0),
                                  ceil_r=(lib.NR if is_pd(topo) else 0))
                        if op == "delete":
                            kv["mode"] = "recreate"
                        else:
                            kv["mode"] = "scaledown"
                            kv["to_e"] = target
                            if co != "none":
                                kv["to_d"] = target
                        kv.update(stuck=stuck, img=runtime_fixtures.UNPULLABLE_IMAGE, co=co, topo=topo)
                        meta = dict(strat=strat, verb=f"push:{stuck}", stuck=stuck, op=op)
                        desc = f"{GEN_MARK} stuck {stuck} {op} {shape_tag(topo, w)} N={n} {strat} group {co}"
                        life = lib.lifecycle(strat=strat, surge=surge, unavail=unavail)
                        d_life = lib.lifecycle(strat=strat, surge=surge, unavail=unavail) if co != "none" else None
                        add("gen-ops-held", desc, kv, comps(topo, n, w, life, d_life=d_life),
                            _ops_held_reason(stuck, op, strat, topo, co), meta,
                            cell=("ops-held", stuck, op, topo, w, strat, co),
                            coord=lib.coord_for(co) if co != "none" else None)


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
               f"after the edit with no repair recorded for it from then on, and no Instance is left "
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


def _readiness_old_reason(form, topo):
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
    return out


def _readiness_new_reason(strat, topo):
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
    return out


def emit_readiness(add):
    for topo, n, w in READINESS_SHAPES:
        pd = is_pd(topo)
        for form in READINESS_FORMS:
            for rp in RESTARTS:
                kv = dict(**counts(topo, n), ceil_e=n, ceil_d=(n if pd else 0), ceil_r=(lib.NR if pd else 0),
                          restart=rp, fault="readiness:old", co="none", topo=topo, form=form, during="idle")
                desc = f"{GEN_MARK} readiness lost {form} {shape_tag(topo, w)} N={n} rp {rp}"
                life = lib.lifecycle(strat="SurgeThenDrain", surge=1, unavail=0, restart=RP_FULL[rp])
                d_life = lib.lifecycle(restart=RP_FULL[rp]) if pd else None
                add("gen-readiness", desc, kv, comps(topo, n, w, life, d_life=d_life),
                    _readiness_old_reason(form, topo),
                    dict(strat="SurgeThenDrain", verb=f"readiness:old:{form}", rp=rp),
                    cell=("readiness", "old", form, topo, w, rp))
    for topo, n, w in READINESS_SHAPES:
        pd = is_pd(topo)
        for strat in READINESS_STRATS:
            surge, unavail = _canonical_budget(strat)
            for rp in RESTARTS:
                kv = dict(**counts(topo, n), **ceilings(topo, n, strat, surge),
                          restart=rp, fault="readiness:new", co="none", topo=topo)
                desc = (f"{GEN_MARK} readiness never {shape_tag(topo, w)} N={n} {strat} "
                        f"{budget_tag(strat, surge, unavail)} rp {rp}")
                life = lib.lifecycle(strat=strat, surge=surge, unavail=unavail, restart=RP_FULL[rp],
                                     ready_timeout=READINESS_NEW_TIMEOUT)
                d_life = lib.lifecycle(restart=RP_FULL[rp]) if pd else None
                add("gen-readiness", desc, kv, comps(topo, n, w, life, d_life=d_life),
                    _readiness_new_reason(strat, topo),
                    dict(strat=strat, verb="readiness:new", rp=rp),
                    cell=("readiness", "new", topo, w, strat, rp))


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


def _evict_steady_reason(topo, rp, target):
    who = f"the {target} pod of one gang" if target else "one engine pod"
    return (f"{who} is evicted through the Eviction API, as a node drain would; the disruption budget admits it with "
            f"every Instance serving, {_recreate_scope(topo, rp)} under restart policy {RP_FULL[rp]}, no other Instance "
            f"is touched and the fleet is back at N serving; nothing surges on a service nobody bumped")


def _evict_roll_reason(topo, strat, waivers):
    victim = _podkill_victim(topo, strat, DISRUPT_PHASE)
    out = (f"once the roll is in flight {victim} is evicted through the Eviction API, as a node drain would, asked again "
           f"while the disruption budget refuses it under {strat}; the controller replaces it, the roll converges with "
           f"every Instance on the new revision and no pod comes back under the old label")
    if is_pd(topo):
        out += "; decoder and router are negative controls (co=none)"
    if waivers.get("floor_soft"):
        out += "; floor waived — the eviction itself costs settled capacity the update budget cannot absorb"
    if waivers.get("order_soft"):
        out += "; order waived — the eviction is a drain with no preceding surge"
    return out


def _kill_counts(topo, n):
    return dict(**counts(topo, n), ceil_e=n, ceil_d=(n if is_pd(topo) else 0), ceil_r=(lib.NR if is_pd(topo) else 0))


def emit_evict(add):
    for topo, n, w in DISRUPT_SHAPES:
        for rp in RESTARTS:
            life = lib.lifecycle(restart=RP_FULL[rp])
            d_life = lib.lifecycle(restart=RP_FULL[rp]) if is_pd(topo) else None
            for target in (["leader", "worker"] if is_mn(topo) else [None]):
                kv = dict(mode="kill", **_kill_counts(topo, n), restart=rp)
                if target:
                    kv["kill"] = target
                kv.update(co="none", topo=topo, via="evict")
                meta = dict(strat="SurgeThenDrain", verb="evict:steady", rp=rp)
                desc = (f"{GEN_MARK} evict steady {shape_tag(topo, w)} N={n} rp {rp}"
                        + (f" target {target}" if target else ""))
                add("gen-evict", desc, kv, comps(topo, n, w, life, d_life=d_life),
                    _evict_steady_reason(topo, rp, target), meta,
                    cell=("evict", "steady", topo, w, rp, target))
            for strat, surge, unavail in DISRUPT_STRATS:
                waivers = _podkill_waivers(topo, strat, DISRUPT_PHASE)
                kv = dict(**counts(topo, n), **ceilings(topo, n, strat, surge),
                          fault=f"evict:{DISRUPT_PHASE}", **waivers, co="none", topo=topo)
                meta = dict(strat=strat, verb=f"evict:{DISRUPT_PHASE}", rp=rp)
                desc = (f"{GEN_MARK} evict {DISRUPT_PHASE} {shape_tag(topo, w)} N={n} {strat} "
                        f"{budget_tag(strat, surge, unavail)} rp {rp}")
                roll_life = lib.lifecycle(strat=strat, surge=surge, unavail=unavail, restart=RP_FULL[rp])
                add("gen-evict", desc, kv, comps(topo, n, w, roll_life, d_life=d_life),
                    _evict_roll_reason(topo, strat, waivers), meta,
                    cell=("evict", DISRUPT_PHASE, topo, w, strat, rp))


# ── gen-preempt: a pod of a Ready Instance is preempted ─────────────────────
def _preempt_reason(moment, topo, rp, lift, strat=None):
    base = ("fillers take the spare CPU of every node and a higher-priority filler presses on the node of the aimed "
            "pod, so the scheduler preempts one pod of the case there")
    if moment == "steady":
        out = (f"{base}; {_recreate_scope(topo, rp)} under restart policy {RP_FULL[rp]} and its new pod waits with "
               f"reason Unschedulable while every other Instance keeps serving and nothing is taken down to make room")
    else:
        out = (f"{base} while the roll is in flight under {strat}; the roll's next pod waits with reason Unschedulable, "
               f"nothing settled is taken down to make room and the capacity floor holds")
    if lift == "after-settle":
        out += ("; once the fillers are dropped the Instance is placed again and serves, back at N" if moment == "steady"
                else "; once the fillers are dropped the roll converges with every Instance on the new revision")
    else:
        out += ("; the fillers stay to the end, so the wait holds with its reason across the hold window and no "
                "Instance reads Failed")
    if is_pd(topo) and moment != "steady":
        out += "; decoder and router are negative controls (co=none)"
    if moment != "steady":
        out += "; floor and order waived — the preemption itself takes a pod the update budget cannot absorb"
    return out


def emit_preempt(add):
    for topo, n, w in DISRUPT_SHAPES:
        for rp in RESTARTS:
            d_life = lib.lifecycle(restart=RP_FULL[rp]) if is_pd(topo) else None
            for lift in PENDING_LIFTS:
                kv = dict(mode="kill", **_kill_counts(topo, n), restart=rp, co="none", topo=topo, via="preempt", lift=lift)
                meta = dict(strat="SurgeThenDrain", verb="preempt:steady", rp=rp, lift=lift)
                desc = f"{GEN_MARK} preempt steady {shape_tag(topo, w)} N={n} rp {rp} lift {lift}"
                add("gen-preempt", desc, kv, comps(topo, n, w, lib.lifecycle(restart=RP_FULL[rp]), d_life=d_life),
                    _preempt_reason("steady", topo, rp, lift), meta,
                    cell=("preempt", "steady", topo, w, rp, lift))
                for strat, surge, unavail in DISRUPT_STRATS:
                    kv = dict(**counts(topo, n), **ceilings(topo, n, strat, surge),
                              fault=f"preempt:{DISRUPT_PHASE}", floor_soft=1, order_soft=1, co="none", topo=topo, lift=lift)
                    meta = dict(strat=strat, verb=f"preempt:{DISRUPT_PHASE}", rp=rp, lift=lift)
                    desc = (f"{GEN_MARK} preempt {DISRUPT_PHASE} {shape_tag(topo, w)} N={n} {strat} "
                            f"{budget_tag(strat, surge, unavail)} rp {rp} lift {lift}")
                    roll_life = lib.lifecycle(strat=strat, surge=surge, unavail=unavail, restart=RP_FULL[rp])
                    add("gen-preempt", desc, kv, comps(topo, n, w, roll_life, d_life=d_life),
                        _preempt_reason(DISRUPT_PHASE, topo, rp, lift, strat), meta,
                        cell=("preempt", DISRUPT_PHASE, topo, w, strat, rp, lift))


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
               f"revision follows; the good push then lands on the broken Instance first, in the same room, then on the "
               f"others one at a time, every Instance ends serving on it and nothing rebuilds the crashing revision")
        if rp == "RecreateInstance":
            out += "; every rebuild RecreateInstanceOnPodRestart opens on the crashing Instance lands in that same room"
    else:
        how = ("one engine pod of the running revision is pointed at an image with no shell on the settled service, "
               "so it crashes at every start and is never Ready again"
               if form == "start" else
               "one engine pod of the running revision has its runner killed every few seconds while it is up, so "
               "it dies after Ready and comes back between crashes")
        out = (f"{pool}; {how}; once the break has bitten the good push lands: the broken Instance, which serves "
               f"nothing, is rolled first and comes back in the room it vacated, the others follow one at a time, "
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


def _pending_reason(cause, moment, topo, rp, lift, strat, w):
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
        return out
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
    return out


def emit_pending(add):
    for topo, n, w in DISRUPT_SHAPES:
        for rp in RESTARTS:
            d_life = lib.lifecycle(restart=RP_FULL[rp]) if is_pd(topo) else None
            for cause in PENDING_CAUSES:
                for lift in PENDING_LIFTS:
                    fault = "taint:all" if cause == "taint" else f"quota:{_pending_quota('steady', None, w)}"
                    kv = dict(mode="kill", **_kill_counts(topo, n), restart=rp)
                    if is_mn(topo):
                        kv["kill"] = "leader"
                    kv.update(fault=fault, co="none", topo=topo, lift=lift)
                    meta = dict(strat="SurgeThenDrain", verb=f"pending:{cause}:steady", rp=rp, lift=lift)
                    desc = f"{GEN_MARK} pending {cause} steady {shape_tag(topo, w)} N={n} rp {rp} lift {lift}"
                    add("gen-pending", desc, kv, comps(topo, n, w, lib.lifecycle(restart=RP_FULL[rp]), d_life=d_life),
                        _pending_reason(cause, "steady", topo, rp, lift, None, w), meta,
                        cell=("pending", cause, "steady", topo, w, rp, lift))
                    for strat, surge, unavail in DISRUPT_STRATS:
                        if cause == "quota" and lift == "never" and is_mn(topo) and strat == "SurgeThenDrain":
                            continue   # constraint ledger: the tight-cluster family's cell
                        fault = "taint:all" if cause == "taint" else f"quota:{_pending_quota('roll', strat, w)}"
                        kv = dict(**counts(topo, n), **ceilings(topo, n, strat, surge), fault=fault,
                                  co="none", topo=topo, lift=lift)
                        meta = dict(strat=strat, verb=f"pending:{cause}:roll", rp=rp, lift=lift)
                        desc = (f"{GEN_MARK} pending {cause} roll {shape_tag(topo, w)} N={n} {strat} "
                                f"{budget_tag(strat, surge, unavail)} rp {rp} lift {lift}")
                        roll_life = lib.lifecycle(strat=strat, surge=surge, unavail=unavail, restart=RP_FULL[rp])
                        add("gen-pending", desc, kv, comps(topo, n, w, roll_life, d_life=d_life),
                            _pending_reason(cause, "roll", topo, rp, lift, strat, w), meta,
                            cell=("pending", cause, "roll", topo, w, strat, rp, lift))


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
# Singles at N=4, the gang at N=2 with one worker; a rolling group where a
# decoder exists. The engine runs under maxUnavailable 1 everywhere: nothing
# rolls, so the strategy is dormant, and a budget of one admits the repair
# that takes the broken Instance offline (a budget of zero holds it).
BROKEN_SHAPES = [("engine", 4, 0), ("pd", 4, 0), ("engine-mn", 2, 1)]
BROKEN_GROUPS = {"engine": ["none"], "pd": ["none", "rolling"], "engine-mn": ["none"]}
BROKEN_FAULTS = [("crashloop", "start"), ("crashloop", "load"), ("imagepull", None), ("readiness", "persist")]
BROKEN_INST = 0
BROKEN_BUDGET = (1, 1)
# Readiness loss is repaired under neither policy, so its restart-policy axis
# is collapsed; every other break runs under both.
BROKEN_RESTARTS = {"readiness": ["None"]}
# The operations per fleet size: (label, target). On a fleet of two, "to
# one" is "down by one" and "down by two" asks for zero, which the API does
# not run as zero, so the gang runs down by one and up by one.
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


def _broken_tail(topo, co, op="a scale", repaired_up=False):
    out = ""
    if is_pd(topo):
        out += "; decoder and router are neither broken nor scaled and stay as they were"
    if co != "none":
        out += f"; the rollingUpdate group lists engine and decoder, and {op} is not a run it paces"
        if repaired_up:
            out += ", nor is the repair of an Instance that serves nothing a consult it may hold"
    if is_mn(topo):
        out += "; each Instance is a gang of a leader and a worker, counted whole"
    return out


def _broken_scale_reason(kind, form, rp, topo, co, n, target):
    how = _broken_how(kind, form, topo)
    if target < n:
        return (f"{how}; once the status reports Instance {BROKEN_INST} broken the engine is scaled from {n} to {target}: "
                f"{_broken_removal_words(n, target)}; the survivors keep their pods and keep serving throughout (never "
                f"below {target} Ready), nothing is rebuilt during the scale, no Component surges, and every row "
                f"settles Ready with no operation open" + _broken_tail(topo, co))
    fate = _broken_fate(kind)
    return (f"{how}; once the status reports Instance {BROKEN_INST} broken the engine is scaled from {n} to {target}: a "
            f"new Instance {n} comes up on the running revision and serves, the healthy Instances keep their pods and "
            f"never leave rotation (never below {n - 1} Ready), nothing surges above {target}, and the broken Instance "
            f"is {_broken_fate_words(kind, form, rp)}" + _broken_tail(topo, co, repaired_up=(fate == "repaired")))


def _broken_delete_reason(kind, form, topo, co):
    return (f"{_broken_how(kind, form, topo)}; once the status reports Instance {BROKEN_INST} broken the service is "
            f"deleted: every pod goes within the teardown budget, the broken one included, no pod stays Terminating "
            f"and no finalizer wedges, and the same name comes back Ready on fresh pods with nothing of the break left"
            + _broken_tail(topo, co, op="a delete"))


def _broken_kv(mode, topo, n, kind, form, co, ceil_e):
    kv = dict(mode=mode, **counts(topo, n), ceil_e=ceil_e, ceil_d=(n if is_pd(topo) else 0),
              ceil_r=(lib.NR if is_pd(topo) else 0), fault=f"{kind}:old")
    if form is not None:
        kv["form"] = form
    kv.update(during=IDLE_PHASE, co=co, topo=topo)
    return kv


def _broken_comps(topo, n, w, rp):
    surge, unavail = BROKEN_BUDGET
    life = lib.lifecycle(strat="SurgeThenDrain", surge=surge, unavail=unavail, restart=RP_FULL[rp])
    d_life = lib.lifecycle(restart=RP_FULL[rp]) if is_pd(topo) else None
    return comps(topo, n, w, life, d_life=d_life)


def emit_fault_scale(add):
    for topo, n, w in BROKEN_SHAPES:
        for co in BROKEN_GROUPS[topo]:
            for kind, form in BROKEN_FAULTS:
                for rp in BROKEN_RESTARTS.get(kind, RESTARTS):
                    for op, target in BROKEN_OPS[n]:
                        kv = _broken_kv("scaledown", topo, n, kind, form, co, max(n, target))
                        kv.update(to_e=target, inst=BROKEN_INST)
                        if target > n:
                            kv["expect"] = _broken_fate(kind)
                        meta = dict(strat="(dormant: nothing rolls)", verb=f"{_broken_label(kind, form)}@{op}",
                                    rp=rp, op=op, victim="engine")
                        desc = (f"{GEN_MARK} broken {_broken_label(kind, form)} {op} {shape_tag(topo, w)} "
                                f"N={n} rp {rp} co {co}")
                        add("gen-fault-scale", desc, kv, _broken_comps(topo, n, w, rp),
                            _broken_scale_reason(kind, form, rp, topo, co, n, target), meta,
                            cell=("broken-scale", kind, form, topo, w, co, rp, op),
                            coord=lib.coord_for(co) if co != "none" else None)


def emit_fault_delete(add):
    for topo, n, w in BROKEN_SHAPES:
        for co in BROKEN_GROUPS[topo]:
            for kind, form in BROKEN_FAULTS:
                for rp in BROKEN_RESTARTS.get(kind, RESTARTS):
                    kv = _broken_kv("recreate", topo, n, kind, form, co, n)
                    kv["inst"] = BROKEN_INST
                    meta = dict(strat="(dormant: nothing rolls)", verb=f"{_broken_label(kind, form)}@delete",
                                rp=rp, op="delete", victim="engine")
                    desc = f"{GEN_MARK} broken {_broken_label(kind, form)} delete {shape_tag(topo, w)} N={n} rp {rp} co {co}"
                    add("gen-fault-delete", desc, kv, _broken_comps(topo, n, w, rp),
                        _broken_delete_reason(kind, form, topo, co), meta,
                        cell=("broken-delete", kind, form, topo, w, co, rp),
                        coord=lib.coord_for(co) if co != "none" else None)
# ── gen-fault-move: a fault at a moment of a move ────────────────────────────
# An Instance is moved by request and, at a moment of the move, a pod breaks
# or the source node dies. One gang width (the move copies the whole gang),
# the canonical surge budget (no roll is in flight; the strategy is dormant),
# no group (a repair of an Instance that serves nothing is admitted without a
# group's gate; the group axis is the steady-state crash-loop family's).
MOVE_SHAPES = [("engine", 4, 0), ("pd", 4, 0), ("engine-mn", 2, 1)]
MOVE_MOMENTS = ["accept", "surge", "drain"]
# (kind, form, the restart policy the cell runs under): a crash at start and
# a pull failure reach the crash-loop repair under either policy; the
# after-Ready form is the one the policy decides, so it runs under the policy
# that promises its repair.
MOVE_BREAKS = [("crashloop", "start", "None"), ("imagepull", None, "None"), ("crashloop", "load", "RecreateInstance")]
MOVE_VICTIMS = {"engine": ["moved", "peer"], "pd": ["moved", "peer", "sibling"], "engine-mn": ["moved", "peer"]}
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


def _move_fault_pod(at, victim):
    """(when, whose pod) the fault hits, in words."""
    if victim == "sibling":
        return MOVE_WHEN[at], "the decoder's highest Instance's pod"
    if victim == "peer":
        return MOVE_WHEN[at], "the pod of the moved Component's highest other Instance"
    return {"accept": ("as soon as the request is recorded, before its replacement exists", "the moved Instance's source pod"),
            "surge": ("as soon as it appears, before it is Ready", "the moved Instance's replacement pod"),
            "drain": ("as the source is being taken down", "the moved Instance's replacement pod, Ready and serving,")}[at]


def _move_fault_reason(kind, form, at, victim, rp, topo):
    when, pod = _move_fault_pod(at, victim)
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
        out += "; the decoder is otherwise untouched: nothing rotates and nothing surges there"
    if is_pd(topo):
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
    for topo, n, w in MOVE_SHAPES:
        base = dict(mode="migrate", **counts(topo, n), ceil_e=n + 1, ceil_d=(n if is_pd(topo) else 0),
                    ceil_r=(lib.NR if is_pd(topo) else 0))
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
                            f"{shape_tag(topo, w)} N={n} rp {rp}")
                    life = lib.lifecycle(restart=RP_FULL[rp])
                    d_life = lib.lifecycle(restart=RP_FULL[rp]) if is_pd(topo) else None
                    add("gen-fault-move", desc, kv, comps(topo, n, w, life, d_life=d_life),
                        _move_fault_reason(kind, form, at, victim, rp, topo), meta,
                        cell=("fault-move", kind, form, at, victim, topo, w, rp))
        for at in MOVE_NODE_MOMENTS:
            for rp in RESTARTS:
                kv = dict(base, fault=f"node:{NODE_PARAM}", floor_soft=1, co="none", topo=topo)
                kv.update(victim="moved", mig="Auto", inst=0, at=at, lift="after-recover", needs=NODE_NEEDS["relocated"])
                meta = dict(strat="SurgeThenDrain", verb=f"node@move-{at}", rp=rp, victim="engine", at=at, who="moved")
                desc = f"{GEN_MARK} fault-move node at {at} on moved {shape_tag(topo, w)} N={n} rp {rp}"
                life = lib.lifecycle(restart=RP_FULL[rp])
                d_life = lib.lifecycle(restart=RP_FULL[rp]) if is_pd(topo) else None
                add("gen-fault-move", desc, kv, comps(topo, n, w, life, d_life=d_life),
                    _move_node_reason(at, rp, topo), meta,
                    cell=("fault-move", "node", None, at, "moved", topo, w, rp))


# ── gen-fault-edit: a fault during a runtime edit ────────────────────────────
# The runtime every Component inherits is edited, so engine, decoder and
# router each roll onto their own new revision. As the edit lands a pod of
# the running revision breaks (the roll owns it), or the edit itself is bad
# (it holds with a reason, the old revision keeps serving, a corrective edit
# lands). One gang width; the surge and the drain-first strategies.
EDIT_SHAPES = [("engine", 4, 0), ("pd", 4, 0), ("engine-mn", 2, 1)]
EDIT_STRATS = {"SurgeThenDrain": (1, 0), "RecreatePod": (0, 1)}
# (kind, form, the restart policies the cell runs under): a crash at start
# and a pull failure open no runner-restart trigger; the after-Ready form
# does, so it runs under both.
EDIT_BREAKS = [("crashloop", "start", ["None"]), ("imagepull", None, ["None"]),
               ("crashloop", "load", ["None", "RecreateInstance"])]
EDIT_VICTIMS = {"engine": ["engine"], "pd": ["engine", "decoder"], "engine-mn": ["engine"]}
# The image edit on every shape; the env edit on the engine shape as the
# control (the same new revision under these strategies).
EDIT_VERBS = {"engine": ["image", "env"], "pd": ["image"], "engine-mn": ["image"]}
# The bad edit: an image that cannot be pulled (expect=recover) and a
# readiness that never passes (expect=recover break=stuck).
BAD_EDITS = [("imagepull", None), ("readiness", "stuck")]
EDIT_RP_FOR_BAD = "None"
EDIT_LANDS = {"SurgeThenDrain": "the surge brings the fixed replacement up beside it and drains it",
              "RecreatePod": "the recreate drains it and rebuilds the Instance at the edited revision"}


def _edit_break_reason(kind, form, victim, edit, strat, rp, topo):
    what = "a new image" if edit == "image" else "a new runner env"
    rolls = ("engine, decoder and router roll, each onto its own new revision" if is_pd(topo)
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
    if victim == "decoder":
        out += "; the engine's own roll converges untouched by the decoder's break"
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


def _edit_kv(topo, n, strat, surge):
    """Counts and ceilings of an edit case: engine and decoder roll under the
    case's strategy, the router under its default surge of one."""
    ce = n + surge if strat == "SurgeThenDrain" else n
    return dict(mode="runtimerev", Ne=n, Nd=(n if is_pd(topo) else 0), Nr=(lib.NR if is_pd(topo) else 0),
                ceil_e=ce, ceil_d=(ce if is_pd(topo) else 0), ceil_r=(lib.NR + 1 if is_pd(topo) else 0))


def emit_fault_edit(add):
    for topo, n, w in EDIT_SHAPES:
        for strat, (surge, unavail) in EDIT_STRATS.items():
            for kind, form, policies in EDIT_BREAKS:
                for victim in EDIT_VICTIMS[topo]:
                    for edit in EDIT_VERBS[topo]:
                        for rp in policies:
                            kv = dict(_edit_kv(topo, n, strat, surge), fault=f"{kind}:old", co="none", topo=topo)
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
                                    f"{shape_tag(topo, w)} N={n} {strat} rp {rp}")
                            life = lib.lifecycle(strat=strat, surge=surge, unavail=unavail, restart=RP_FULL[rp])
                            add("gen-fault-edit", desc, kv, comps(topo, n, w, life, d_life=life),
                                _edit_break_reason(kind, form, victim, edit, strat, rp, topo), meta,
                                cell=("fault-edit", kind, form, victim, edit, topo, w, strat, rp), dedicated=True)
            for kind, brk in BAD_EDITS:
                kv = dict(_edit_kv(topo, n, strat, surge), co="none", topo=topo, expect="recover")
                if brk:
                    kv["break"] = brk
                meta = dict(strat=strat, verb=f"bad-edit:{kind}", rp=EDIT_RP_FOR_BAD, rtedit="image")
                desc = f"{GEN_MARK} bad-edit {kind} {shape_tag(topo, w)} N={n} {strat}"
                life = lib.lifecycle(strat=strat, surge=surge, unavail=unavail, restart=RP_FULL[EDIT_RP_FOR_BAD])
                add("gen-fault-edit", desc, kv, comps(topo, n, w, life, d_life=life),
                    _bad_edit_reason(kind, strat, topo), meta,
                    cell=("bad-edit", kind, topo, w, strat), dedicated=True)


# ── assembly ─────────────────────────────────────────────────────────────────
def build_outputs():
    """{relative path under isvcs/: file text} — the entire owned output set."""
    outputs = {}
    counters = {f: lo for f, (lo, _hi) in GEN_BLOCKS.items()}
    rows = []   # (folder, case id, kv, meta) for the manifest
    parked_used = set()

    def add(folder, desc, kv, case_comps, reason, meta, cell, coord=None, body=None,
            runtime=None, dedicated=False, edit_runtime=False):
        """body: the case body as a function of the minted name, for a family
        whose fixtures carry the case id; the plain render otherwise. A
        dedicated case ships its own runtime under a per-case name, so a
        runtime edit touches this case alone; edit_runtime ships the
        runtime-rev generator's ClusterServingRuntime for the case's topology
        under that name, the one the runtime-edit verbs patch."""
        lo, hi = GEN_BLOCKS[folder]
        cid = counters[folder]
        if cid >= hi:
            sys.exit(f"{folder}: id block exhausted at p{cid} (block ends before p{hi})")
        counters[folder] += 1
        name = f"p{cid}"
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
    emit_ctrlrestart(add)
    emit_canary_crash(add)
    emit_canary_retarget(add)
    emit_canary_podkill(add)
    emit_canary_fault_old(add)
    emit_canary_fault_new(add)
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
    emit_fault_move(add)
    emit_fault_edit(add)
    emit_packed_pool(add)
    stale = set(PARKED) - parked_used
    if stale:
        sys.exit("PARKED keys match no emitted cell (stale after a matrix change): "
                 + "; ".join(repr(k) for k in sorted(stale)))
    outputs[MANIFEST_REL] = manifest(rows)
    return outputs, rows


def manifest(rows):
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
    lines += table("By lift (preempt and pending families)",
                   tally(lambda kv, m: m["lift"], where=lambda kv, m: "lift" in m))
    lines += table("By percent budget (gen-budget)",
                   tally(lambda kv, m: m["budget"], where=lambda kv, m: "budget" in m))
    lines += table("By operation on a service with a broken Instance",
                   tally(lambda kv, m: m["op"], where=lambda kv, m: "op" in m))
    lines += table("By move moment (fault-move)", tally(lambda kv, m: m["at"], where=lambda kv, m: "at" in m))
    lines += table("By move victim (fault-move)", tally(lambda kv, m: m["who"], where=lambda kv, m: "who" in m))
    lines += table("By runtime edit (fault-edit)", tally(lambda kv, m: m["rtedit"], where=lambda kv, m: "rtedit" in m))
    lines += table_if("By story (packed-pool)", "side")
    parked_rows = [(f, n, m["parked"]) for f, n, _kv, m in rows if "parked" in m]
    if parked_rows:
        lines += ["### Parked cells (XFAIL on a tracked issue)", "",
                  "| case | folder | issue |", "|---|---|---|"]
        lines += [f"| {n} | {f} | #{i} |" for f, n, i in parked_rows]
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
        " the rotating hook, with form=start.",
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
        " a rolling group is crossed in on the PD rollout conditions only.",
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
        " a decoder does (pd). A request for zero is kept: the API runs one"
        " Instance for it, and the verdict reads the count the controller"
        " accepted rather than assuming either answer.",
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
        " revision; draining: a serving gang beside the first one leaving).",
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
        " family leaves its `quota:<members-1> lift=never` cell out.",
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
        " API does not run as zero. A rolling group is crossed in where a"
        " decoder exists; the delete family keeps the same axes.",
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
        " not fired at a move moment).",
        "- gen-fault-edit edits the dedicated runtime every Component inherits"
        " (engine, decoder and router each roll onto their own revision) under"
        " the surge and the drain-first strategies, and breaks a pod of the"
        " running revision on the engine or the decoder as the edit lands; its"
        " bad-edit half pushes an image that cannot be pulled or a readiness"
        " that never passes and then the corrective edit. Left out, with the"
        " reason: the in-place strategies (the switch family's); the env edit"
        " beyond the engine shape (under these strategies both edits are one new"
        " revision; the pod-template difference matters to an in-place strategy"
        " alone); the restart policy on a crash at start and a pull failure"
        " (they open no runner-restart trigger, so the roll owns the Instance"
        " either way; the after-Ready form runs under both); any restart policy"
        " but None on the bad edit (a revision that never becomes Ready restarts"
        " no runner); pd-mn, a rollout group and a readiness loss on the running"
        " side, as in fault-move. The readiness-that-never-passes cell asserts the"
        " fix and the floor and reports whether the status said why: nothing"
        " escalates a pod that runs but never passes readiness before its ready"
        " timeout.",
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
        " promote when the rebuilt set comes up, and re-arms it on"
        " `lifecycle.updateRetry` while the set stays wedged. The key returns"
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
