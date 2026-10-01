#!/usr/bin/env python3
"""generate_cases.py — the generated hot-core permutation corpus (isvcs/gen-*/).

Emits the constraint-filtered cross product of
    topology x updateStrategy x failure verb x injection phase x
    restartPolicy x pause
into nineteen generator-owned folders, using ONLY verbs and hooks the runners
(run.py + the KIND CI lane) already implement:

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
    decoder: the hold a group keeps on repairs while no run is open. Nothing
    rolls, so the update strategy stays canonical and is not an axis.
    lift=after-failed waits for the row to escalate, so those cases carry a
    short instanceReadyTimeout on the victim (HEAL_FAILED_READY_TIMEOUT).
  - imagepull has one form (an image that never pulls has no after-Ready
    form) and runs each strategy at its canonical budget only; the idle half
    carries no group, since a grouped repair's hold is its own family.
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
import runtime_fixtures

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
# exists), the hold a group keeps on repairs while no run is open.
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
# The issue the self-heal of a Failed row is tracked under: a repair that
# escalates keeps its operation, and a Failed row with an operation has no
# exit once its pods are runtime-ready, so the key's return starts the pods
# but the row stays Failed. Every lift=after-failed cell is parked on it.
HEAL_ISSUE = 1978

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
# A park is added here as (key): (issue, reason, invariant scope) and
# rendered into the header. The heal family's lift=after-failed cells are
# parked on HEAL_ISSUE with a per-axis reason, filled in next to its emitter.
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
    "pause-under-rollout hook (ome.io/rollout-paused is only exercised inside "
    "mode=kill; no verb pauses an in-flight rotation)",
    "decoder-targeted mode=kill (the kill handler always picks the engine when "
    "one is configured)",
    "controller restart while a repair is open (a crash loop at idle with the "
    "manager deleted once the rebuild has started) — the runner fires one fault "
    "per case and the idle-break handler fires no second one",
]


def is_pd(topo):
    return topo.startswith("pd")


def is_mn(topo):
    return topo.endswith("-mn")


def surge_count(n, s):
    if isinstance(s, str) and s.endswith("%"):
        return math.ceil(n * int(s[:-1]) / 100)
    return s


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
                 "restart", "kill", "pause", "fault", "floor_soft", "order_soft",
                 "co", "topo",
                 "canary", "bump", "img", "form", "victims", "exitafter", "during", "verb", "verbhold",
                 "verb2", "during2", "victim", "victimcomp", "release", "releaseafter", "rt",
                 "analysis", "failurelimit", "terminal", "nophase", "cell"]


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
def _crash_how(form):
    """How the crashloop verb breaks the old-revision pod, per form."""
    return ("one old-revision engine pod is pointed at an image with no shell as the bump "
            "lands, so it crashes at every start and is never Ready again"
            if form == "start" else
            "one old-revision engine pod has its runner killed every few seconds while it "
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


def _heal_reason(rp, co, topo):
    where = "gang leader" if is_mn(topo) else "pod"
    out = (f"the engine runner's ConfigMap key is removed on the highest Instance's {where} and the "
           f"runner restarted, so its next start waits in CreateContainerConfigError; the key is put "
           f"back once that wait has held for the settle window, before any attempt escalates, and "
           f"the kubelet's own retry starts the container")
    if rp == "None" and co == "none":
        out += (" on that pod, or on the one the crash-loop repair rebuilt when it opened first (the "
                "stuck-pod clock counts from the pod's creation, and the victim is older than the grace)")
    elif rp == "None":
        out += (" on that pod: the group holds the crash-loop repair while no run is open, which "
                "leaves the pod to the kubelet")
    else:
        out += " on the pod the runner-restart repair rebuilt, and the open repair promotes it"
        if co != "none":
            out += ("; the group holds only a fresh crash-loop repair, and the runner-restart repair "
                    "is not put to it, so it opens as with no group")
    out += ("; the Instance is Ready and serving again on its running revision with no new revision "
            "and no reset mailbox")
    return out


def _heal_parked_reason(rp, co):
    out = ("parked: the repair that rebuilt the Instance keeps its operation when the row escalates "
           "to Failed, and a Failed row with an operation has no exit once its pods are runtime-ready, "
           "so the key's return starts the pods while the row stays Failed with nothing serving until "
           "the reset mailbox")
    if rp == "None" and co != "none":
        out += ("; under a group with no run open the row does not even escalate: the only repair a "
                "None policy opens is the crash-loop one, which the group holds, so the lift never "
                "arrives and the park covers both rules")
    return out


PARKED.update({
    ("heal", topo, w, rp, "after-failed", co): (HEAL_ISSUE, _heal_parked_reason(rp, co), "")
    for topo, _n, w in CRASH_SHAPES for rp in RESTARTS for co in HEAL_COORDS
})


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
                    add("gen-heal", desc, kv, None, _heal_reason(rp, co, topo), meta,
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
        still += "; the rolling group would hold a repair while no run is open, and none opens"
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
CSR_METRICS_TEXT = (HERE / "runtime" / "csr-metrics.yaml").read_text()
METRICS_ENV_HEAD = '      env:\n        - { name: TEST_STARTUP_DELAY, value: "15" }'


def dedicated_runtime(name, metrics):
    """The fixture runtime under a per-case name, so a runtime edit touches
    one case. The metrics runtime gets TEST_REV first in its env list: the
    entry the runtime env edit rewrites."""
    if not metrics:
        return CSR_SINGLE_TEXT.replace("ome-coordtest-cpu", name)
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


# ── assembly ─────────────────────────────────────────────────────────────────
def build_outputs():
    """{relative path under isvcs/: file text} — the entire owned output set."""
    outputs = {}
    counters = {f: lo for f, (lo, _hi) in GEN_BLOCKS.items()}
    rows = []   # (folder, case id, kv, meta) for the manifest
    parked_used = set()

    def add(folder, desc, kv, case_comps, reason, meta, cell, coord=None, body=None,
            runtime=None, dedicated=False):
        """body: the case body as a function of the minted name, for a family
        whose fixtures carry the case id; the plain render otherwise. A
        dedicated case ships its own runtime under a per-case name, so a
        runtime edit touches this case alone."""
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
        if dedicated:
            rt_name = f"ome-coordtest-rt-{name}"
            kv = dict(kv, rt=rt_name)
            before = dedicated_runtime(rt_name, metrics=(runtime == lib.CSR_METRICS))
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
    lines += table("By coordination", tally(lambda kv, m: kv.get("co", "none")))
    edits = {}
    for _f, _n, _kv, m in rows:
        if "edit" in m:
            edits[m["edit"]] = edits.get(m["edit"], 0) + 1
    if edits:
        lines += table("By mid-roll edit (gen-switch)", edits)
    lines += table("By break victim", tally(lambda kv, m: m.get("victim", "(no break)")))
    lines += table("By migration mode (node family)",
                   tally(lambda kv, m: m["mig"], where=lambda kv, m: "mig" in m))
    lines += table("By controller-restart condition",
                   tally(lambda kv, m: m["cond"], where=lambda kv, m: "cond" in m))
    parked_rows = [(f, n, m["parked"]) for f, n, _kv, m in rows if "parked" in m]
    if parked_rows:
        lines += ["### Parked cells (XFAIL on a tracked issue)", "",
                  "| case | folder | issue |", "|---|---|---|"]
        lines += [f"| {n} | {f} | #{i} |" for f, n, i in parked_rows]
        lines += [""]
    lines += [
        "## Constraint ledger (tuples excluded during construction)",
        "",
        "- `pause` multiplies only `mode=kill`: run.py consumes the pause header"
        " nowhere else, so a paused rollout case would carry a dead knob.",
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
    """Why each half of gen-heal is asserted or parked, read off the controller
    (the restart trigger, the stuck-pod evidence and the escalation pass)."""
    return [
        "",
        "## gen-heal: which cells heal without the missing rule",
        "",
        "The family asserts the intended behaviour, `expect=heals`, on every"
        " cell; the cells whose heal needs the exit a Failed row lacks are"
        f" parked on #{HEAL_ISSUE} and listed above.",
        "",
        "- `lift=after-settle` (asserted): the key returns once the"
        " CreateContainerConfigError wait has held for the settle window,"
        " before any attempt escalates. Under `None` the crash-loop repair"
        " opens as soon as the wait is seen (the stuck-pod clock counts from"
        " the pod's creation and the victim is older than the grace), rebuilds"
        " the pod, and the rebuilt pod waits on the same key; under"
        " `RecreateInstanceOnPodRestart` the runner-restart repair does the"
        " same. Either way the kubelet starts the container once the key is"
        " back and the open repair promotes the Instance; where a group holds"
        " the crash-loop repair (`None` with `co=rolling`), the kubelet's retry"
        " on the untouched pod is the whole heal. The settle window"
        " (`LIFT_SETTLE_S`, 20 s before `OME_TEST_TIMEOUT_MULT`) has to stay"
        " inside the deployment's `stuckPodGracePeriod` (60 s in the chart): a"
        " multiplier of 3 or more turns these cells into the after-failed"
        " shape.",
        f"- `lift=after-failed` (parked on #{HEAL_ISSUE}): the repair keeps its"
        " operation when the row escalates, and a Failed row with an operation"
        " has no exit once its pods are runtime-ready, so the key's return"
        " starts the pods while the row stays Failed. The `None` x `co=rolling`"
        " cells additionally never escalate: the only repair a `None` policy"
        " opens is the crash-loop one, which the group holds while no run is"
        " open, so those four flip only when the grouped-repair hold is lifted"
        " as well.",
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
