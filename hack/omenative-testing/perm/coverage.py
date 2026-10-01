#!/usr/bin/env python3
"""coverage.py — the coverage ledger of the perm corpus.

Reads line 1 (the header) and line 2 (the reason) of every case under
isvcs/, plus the facts a header does not carry and the body does (update
strategy and budget, restart policy, gang width, rollout group kind, canary
step shapes), and renders the scenario space: the axes of every case, the
matrix with counts, the empty cells by name with the biggest holes first,
the parked cells with their issues, and the operator stories with the cells
that prove them.

  python3 coverage.py            # the Markdown ledger on stdout
  python3 coverage.py --cases    # ... plus the per-case table
  python3 coverage.py --json     # the same facts as JSON
  python3 coverage.py --check    # nonzero when a story has no cell or a
                                 # header carries a key or value the parser
                                 # does not know (run_test.py runs this)

The tables at the top are the editable part: STORIES maps each operator
story to a predicate over a parsed case, and the vocabularies name every
header value the ledger can place on an axis. A new header value is added
to its vocabulary and mapped to an axis value here, so the ledger never
guesses; until then `--check` fails on it.
"""
import argparse
import json
import re
import sys
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass, field
from pathlib import Path

HERE = Path(__file__).parent.resolve()
ISVCS = HERE / "isvcs"


# ── The operator stories (the owner approves this list) ─────────────────────
# Each story names a predicate over a parsed Case (its fields are listed on
# the dataclass below). A story is proved by every case whose predicate is
# true; the report prints the cells (shape family, operation, fault row)
# those cases occupy, and `--check` fails on a story no case proves.

SIDE_OPERATIONS = ("scale up", "scale down", "migrate", "pause", "freeze", "delete")
ROLL_OPERATIONS = ("push", "canary", "rollback", "runtime edit", "strategy or policy edit")

STORIES = [
    (1, "I push a new revision and it rolls out, under each strategy and each group policy.",
     lambda c: c.operation == "push" and not c.faults and not c.second_operation),
    (2, "The running revision breaks (crash, OOM, pull failure, missing config, dead node) and I push "
        "the fix; the fix lands on every Instance, the broken one first, and nothing rebuilds the "
        "broken revision.",
     lambda c: c.operation in ("push", "runtime edit") and c.has_fault(side="running", form="persistent")),
    (3, "The new revision is broken; the rollout stops, tells me why, holds capacity on the old "
        "revision, and my next push or rollback lands.",
     lambda c: c.has_fault(side="new", form="persistent")
     and (c.operation == "rollback" or c.second_operation == "push" or c.outcome == "expect=recover"
          or "rollback mid-ladder" in c.canary_steps or "re-target mid-ladder" in c.canary_steps)),
    (4, "Two changes land in a row or overlap (a runtime edit after an ISVC edit, two pushes during one "
        "canary, a scale or a migration while a roll is in flight, a pause and an unpause around a "
        "repair). The last change wins and nothing is left half-done.",
     lambda c: "operation+operation" in c.overlap or "two revisions in a row" in c.overlap),
    (5, "Something breaks while a roll is in flight (a pod, a node, a sibling Component, the "
        "controller, the webhook, the apiserver connection). The roll finishes or holds with a "
        "reason; it never wedges.",
     lambda c: bool(c.faults) and c.operation in ROLL_OPERATIONS),
    (6, "A canary walks its ladder: capacity before traffic, each step kind, analysis, manual gates, "
        "abort, rollback at every step, re-target mid-ladder, with and without a broken stable side.",
     lambda c: c.operation == "canary"),
    (7, "I scale, migrate, pause, freeze, delete, at any point of any of the above.",
     lambda c: c.operation in SIDE_OPERATIONS or c.second_operation in SIDE_OPERATIONS),
    (8, "A fault heals by itself; the engine notices without me.",
     lambda c: c.fault_lifted),
    (9, "An operator deletes a pod by hand; what comes back is labelled as what it runs.",
     lambda c: c.has_fault(kind="pod deleted by hand") or c.has_fault(kind="gang member lost")),
    (10, "Status tells the truth: Ready means serving, every wait has a reason, every state change "
         "has an event, no Instance is left behind after a run reports Stable.",
     lambda c: not c.invariants_off),
]


# ── Axis vocabularies (section 2 of the marathon goal) ──────────────────────
# Shape families the ledger's product is built over; the corpus also holds
# engine+decoder services with no router, tabulated but outside the product.
SHAPE_FAMILIES = [
    "engine single", "engine gang",
    "engine+router single", "engine+router gang",
    "engine+decoder+router single", "engine+decoder+router gang",
]
SIZE_CLASSES = ["1", "2", "many"]

OPERATIONS = [
    "steady state", "push", "rollback", "scale up", "scale down", "migrate", "delete",
    "pause", "freeze", "canary", "strategy or policy edit", "runtime edit",
]
# Operations the corpus drives that section 2 does not list; counted, not crossed.
OTHER_OPERATIONS = ["admission reject", "remove component"]

# Faults bound to a revision carry a side (the running revision or the new
# one); the rest do not. Every kind has a one-shot and a persistent form.
REVISION_BOUND_FAULTS = [
    "crash at start", "crash after Ready", "image pull", "config missing",
    "readiness never passes", "metric breach", "sibling broken",
]
UNBOUND_FAULTS = [
    "pod deleted by hand", "gang member lost", "pod Pending", "node", "evicted or preempted",
    "controller restart", "webhook unavailable", "apiserver conflict", "quota",
    "analysis source unusable",
]
FAULT_FORMS = ["one-shot", "persistent"]
FAULT_SIDES = ["running", "new"]
NO_FAULT = ("none", "-", "-")

GROUPS = ["none", "blueGreen", "rollingUpdate", "rollingUpdate+maintainRatio", "Sequential", "canary"]
STRATEGIES = ["SurgeThenDrain", "RecreatePod", "InPlaceIfPossible", "InPlaceOnly", "unset"]
BUDGETS = ["0", "1", "2+", "percent", "unset"]
RESTART_POLICIES = ["None", "RecreateInstanceOnPodRestart", "unset"]
TIMINGS = [
    "-", "steady state", "settling", "rotating", "migrating", "draining", "paused",
    "canary staging", "canary step", "canary soak", "canary analysis", "canary pre-hold",
    "canary draining", "canary rolling back", "canary rolled back", "canary done", "canary failed",
    "last Instance", "held floor",
]
OVERLAPS = ["operation+fault", "operation+operation", "two revisions in a row"]
CANARY_STEP_KINDS = [
    "capacity ahead of traffic", "traffic step", "manual gate", "timed soak", "analysis gate",
    "promote", "abort", "rollback mid-ladder", "re-target mid-ladder", "resume", "plan or run edit",
]
# Axis values no header can express today; they stay empty until a verb exists.
NOT_DERIVABLE = [
    "the scheduler (default or the gang scheduler): an environment setting of the lane, not a header key",
    "production profiles: no header marks a case as a shipped service's lifecycle fixture",
    "the timings `last Instance` and `held floor`: no hook fires at them",
    "migration auto versus manual: `mode=migrate` files an explicit request; `mig=` is the policy",
    "the faults `webhook unavailable`, `apiserver conflict`, `evicted or preempted`, `pod Pending` "
    "(other than through a quota): no verb injects them",
]


# Cells removed from the product. A cell leaves the product only with a
# written reason; a predicate over (shape family, operation, fault row)
# names the cells it removes.
EXCLUDED_CELLS = [
    ("no operation and no fault leaves nothing to assert",
     lambda family, operation, row: operation == "steady state" and row == NO_FAULT),
    ("a single-pod Instance has no gang member to lose",
     lambda family, operation, row: row[0] == "gang member lost" and family.endswith("single")),
    ("on a gang a pod deleted by hand is a lost gang member",
     lambda family, operation, row: row[0] == "pod deleted by hand" and family.endswith("gang")),
]


# ── Header vocabulary (what the keys may say; mirrors run.py's validator) ───
COMPONENTS = ("engine", "decoder", "router")
CANARY_HOOKS = {"canary-staging", "canary-serving", "canary-prehold", "canary-draining", "canary-done",
                "canary-rollingback", "canary-rolledback", "canary-failed"}
FAULT_PHASES = {"settling", "rotating", "migrating"}
CANARY_VERBS = {"pause", "pause-freeze", "promote", "promote-stale", "rebump", "rebump-fix",
                "rollback", "timeout-clear", "timeout-set", "scale", "repin", "resume",
                "planedit", "planedit-repin", "groupremove", "runlost", "pairing", "quotalift"}
PREARM_VERBS = {"rollback", "pause", "promote", "repin", "resume", "timeout-set",
                "planedit", "groupremove", "scale", "planinvalid"}
UPDATE_STRATEGIES = {"SurgeThenDrain", "RecreatePod", "InPlaceIfPossible", "InPlaceOnly"}
RESTART_POLICY_NAMES = {"None", "RecreateInstanceOnPodRestart"}
IDLE_EXPECTS = {"repaired", "held", "heals", "parked", "relocated"}

HEADER_VALUES = {
    "mode": {"rollout", "scaledown", "partition", "rebump", "migrate", "kill", "canary", "runtimerev",
             "recreate", "compremove", "pairing"},
    "topo": {"engine", "pd", "engine-mn", "pd-mn"},
    "co": {"none", "rolling", "ratio", "percomp", "seq", "bluegreen", "canary"},
    "canary": {"auto", "manual"},
    "unit": set(COMPONENTS),
    "analysis": {"pass", "breach", "inconclusive"},
    "health": {"pass", "breach", "inconclusive"},
    "oninconclusive": {"hold", "rollback"},
    "expect": {"reject", "roll", "noroll", "recover", "fail"} | IDLE_EXPECTS,
    "break": {"stuck"},
    "remove": {"decoder", "router"},
    "mig": {"Auto", "Surge", "Never"},
    "restart": {"None", "RecreateInstance"},
    "kill": {"leader", "worker"},
    "pause": {"true", "freeze"},
    "terminal": {"Stable", "RolledBack", "Failed", "Held"},
    "victim": {"stable", "canary"},
    "victimcomp": set(COMPONENTS),
    "imgcomp": set(COMPONENTS),
    "form": {"start", "load"},
    "nophase": {"Pending", "Canarying", "Paused", "Promoting", "RollingBack", "RolledBack", "Failed"},
    "inv_off": {"A", "B", "C", "D", "F", "G", "H", "all"},
    "inv_xfail": {"A", "B", "C", "D", "F", "G", "H", "all"},
    "xcheck": {"sectraffic"},
    "verb": CANARY_VERBS,
    "verb2": CANARY_VERBS,
    "prearm": PREARM_VERBS,
    "release": {"promote", "resume"},
    # The deployed controller config a cell is read against; the runner SKIPs
    # the cell where the config differs.
    "needs": {"forcedelete", "no-forcedelete"},
}


def _count(v):
    return isinstance(v, int) and v >= 0


def _flag(v):
    return v == 1


def _text(v):
    return bool(str(v))


def _hook(v):
    hook, _, step = str(v).partition("@")
    return hook in CANARY_HOOKS and (not step or step.isdigit())


def _fault(v):
    kind, _, param = str(v).partition(":")
    if kind == "quota":
        return param.isdigit()
    if kind in ("podkill", "ctrlrestart"):
        return param in FAULT_PHASES or _hook(param) or (kind == "ctrlrestart" and param == "idle")
    return {"cordon": "source", "crashloop": "old", "imagepull": "old",
            "configkey": "missing", "node": "notready"}.get(kind) == param


def _edit(v):
    kind, sep, value = str(v).partition(":")
    if not sep:
        return v in {"material", "noop", "image", "resources", "env"}
    return value in {"strategy": UPDATE_STRATEGIES, "restartPolicy": RESTART_POLICY_NAMES}.get(kind, set())


def _during(v):
    return v in {"rollout", "migrate", "idle", "settling", "rotating"} or _hook(v)


def _lift(v):
    return v == 1 or v in {"after-settle", "after-failed", "after-recover"}


def _int_list(v):
    return all(part.isdigit() for part in str(v).split(","))


def _number(v):
    try:
        float(v)
        return True
    except (TypeError, ValueError):
        return False


def _duration(v):
    return re.match(r"^[0-9]+(s|m|h)$", str(v)) is not None


def _cells(v):
    return all(re.match(r"^canary/[A-Za-z]+(\[[A-Za-z]+\])?/[A-Za-z0-9]+(\[[A-Za-z]+\])?$", c)
               for c in str(v).split(","))


def _members(v):
    return bool(str(v)) and all(m in COMPONENTS for m in str(v).split(","))


HEADER_CHECKS = {
    "Ne": _count, "Nd": _count, "Nr": _count, "ceil_e": _count, "ceil_d": _count, "ceil_r": _count,
    "to_e": _count, "to_d": _count, "to_r": _count, "partition": _count, "failurelimit": _count,
    "minfails": _count, "verbhold": _count, "releaseafter": _count, "skipsoak": _count,
    "ratio_tol": _count, "scale_e": _count, "scale_d": _count, "issue": _count,
    "rollback": _flag, "readd": _flag, "dup": _flag, "hint": _flag, "hold": _flag, "pkill": _flag,
    "blocked": _flag, "infeasible": _flag, "notimpl": _flag, "floor_hard": _flag, "floor_soft": _flag,
    "order_soft": _flag,
    "fault": _fault, "during": _during, "during2": _hook, "edit": _edit, "lift": _lift,
    "inst": _int_list, "errorrate": _number, "timeout": _duration, "cell": _cells, "bump": _members,
    "img": _text, "rt": _text, "reason": _text, "proto": _text, "proto2": _text, "pairto": _text,
}
HEADER_KEYS = set(HEADER_VALUES) | set(HEADER_CHECKS)
REQUIRED_KEYS = ("Ne", "Nd", "Nr", "ceil_e", "ceil_d", "ceil_r")


# ── One parsed case ─────────────────────────────────────────────────────────
@dataclass
class Case:
    id: str
    folder: str
    description: str
    header: dict
    reason: str
    components: tuple
    shape: str
    pods: str
    gang_width: int
    size: int
    size_class: str
    shape_family: str
    mode: str
    operation: str
    operation_detail: str
    second_operation: str
    faults: list            # (kind, form, side, component) tuples, the primary first
    fault_lifted: bool
    group: str
    strategy: str
    budget: str
    restart_policy: str
    timing: str
    overlap: list
    canary_steps: list
    canary_cells: list
    parked: str
    issue: int
    outcome: str
    invariants_off: bool
    problems: list = field(default_factory=list)

    @property
    def fault_kind(self):
        return self.faults[0][0] if self.faults else "none"

    @property
    def fault_form(self):
        return self.faults[0][1] if self.faults else "-"

    @property
    def fault_side(self):
        return self.faults[0][2] if self.faults else "-"

    def has_fault(self, kind=None, form=None, side=None):
        return any((kind is None or f[0] == kind) and (form is None or f[1] == form)
                   and (side is None or f[2] == side) for f in self.faults)

    def fault_rows(self):
        """The matrix rows this case occupies: a revision-bound fault keeps
        its side, any other fault drops it, no fault is the `none` row."""
        if not self.faults:
            return [NO_FAULT]
        return sorted({(k, f, s if k in REVISION_BOUND_FAULTS else "-") for k, f, s, _c in self.faults})


# ── Parsing: line 1, line 2, and the body facts ─────────────────────────────
def coerce(raw):
    try:
        return int(raw)
    except ValueError:
        return raw


def parse_header(line, case_id):
    """(description, fields, problems) of a header line. The key region is
    walked backward from the end of the line, as run.py does, so description
    words that look like key=value (maxSurge=2, N=8) stay description."""
    words = line.lstrip("#").strip().split()
    fields = {}
    end = len(words)
    while end > 0:
        word = words[end - 1]
        if "=" not in word or word.startswith("("):
            break
        key, _, raw = word.partition("=")
        if key not in HEADER_KEYS or key in fields:
            break
        fields[key] = coerce(raw)
        end -= 1
    description = " ".join(words[:end])
    if description.startswith(case_id + ":"):
        description = description[len(case_id) + 1:].strip()
    return description, fields, header_problems(fields)


def header_problems(fields):
    problems = [f"header missing {key}=" for key in REQUIRED_KEYS if key not in fields]
    for key, value in fields.items():
        if key in HEADER_VALUES and value not in HEADER_VALUES[key]:
            problems.append(f"unknown value {key}={value}")
        elif key in HEADER_CHECKS and not HEADER_CHECKS[key](value):
            problems.append(f"malformed value {key}={value}")
    return problems


COMPONENT_LINE = re.compile(r"^  (engine|decoder|router):\s*$")
STEP_LINE = re.compile(r"^\s+- (\{ )?capacity:")
GROUP_LINE = re.compile(r"^\s+- components:")
FIELD = {
    "type": re.compile(r"\btype:\s*\"?([A-Za-z]+)"),
    "surge": re.compile(r"maxSurge:\s*\"?([0-9]+%?)"),
    "unavailable": re.compile(r"maxUnavailable:\s*\"?([0-9]+%?)"),
    "restart": re.compile(r"restartPolicy:\s*\"?([A-Za-z]+)"),
    "width": re.compile(r"worker:\s*\{\s*size:\s*([0-9]+)"),
}
GROUP_KIND = re.compile(r"^\s+(blueGreen|rollingUpdate|maintainRatio|canary):")


def body_facts(text):
    """The facts the header does not carry: per-Component strategy, budgets
    and restart policy, the widest gang, the rollout groups' kinds and the
    canary steps. A Component section runs from its `  <name>:` line to the
    next line indented by less than four spaces."""
    facts = {"strategy": {}, "surge": {}, "unavailable": {}, "restart": {}, "width": 0,
             "groups": [], "steps": []}
    component = None
    for line in text.split("\n"):
        opened = COMPONENT_LINE.match(line)
        if opened:
            component = opened.group(1)
            continue
        if not line.startswith("    "):
            component = None
        if component:
            read_component_line(facts, component, line)
        elif GROUP_LINE.match(line):
            facts["groups"].append(set())
        elif STEP_LINE.match(line):
            facts["steps"].append(line)
        elif GROUP_KIND.match(line) and facts["groups"]:
            facts["groups"][-1].add(GROUP_KIND.match(line).group(1))
        elif facts["steps"] and line.strip() and not GROUP_KIND.match(line):
            facts["steps"][-1] += "\n" + line
    return facts


def read_component_line(facts, component, line):
    for name, pattern in FIELD.items():
        found = pattern.search(line)
        if not found:
            continue
        if name == "width":
            facts["width"] = max(facts["width"], int(found.group(1)))
        elif name == "type":
            facts["strategy"].setdefault(component, found.group(1))
        else:
            facts[name][component] = found.group(1)


def read_case(path):
    text = path.read_text()
    lines = text.split("\n")
    description, header, problems = parse_header(lines[0], path.stem)
    reason = lines[1][len("# reason:"):].strip() if len(lines) > 1 and lines[1].startswith("# reason:") else ""
    return classify(path, description, header, reason, body_facts(text), problems)


# ── Classification: one function per axis ──────────────────────────────────
def classify(path, description, header, reason, body, problems):
    components = tuple(c for c, key in zip(COMPONENTS, ("Ne", "Nd", "Nr")) if header.get(key, 0))
    pods = "gang" if str(header.get("topo", "")).endswith("-mn") else "single"
    shape = "+".join(components) or "engine"
    size = header.get("Ne", 0)
    mode = header.get("mode", "rollout")
    strategy = body["strategy"].get("engine", "unset")
    verbs = {str(header.get(k, "")) for k in ("verb", "verb2", "prearm", "release")} - {""}
    operation, detail, second = classify_operation(header, mode, size)
    faults = classify_faults(header, mode, pods, strategy, verbs)
    canary_steps = classify_canary_steps(header, body["steps"], verbs) if mode == "canary" or "canary" in verbs_group(body) else []
    parked, issue = classify_parked(header)
    return Case(
        id=path.stem, folder=path.parent.name, description=description, header=header, reason=reason,
        components=components, shape=shape, pods=pods, gang_width=body["width"], size=size,
        size_class=str(size) if size in (1, 2) else "many", shape_family=f"{shape} {pods}",
        mode=mode, operation=operation, operation_detail=detail, second_operation=second,
        faults=faults,
        fault_lifted=fault_lifted(header, verbs),
        group=classify_group(body["groups"]),
        strategy=strategy,
        budget=classify_budget(strategy, body["surge"].get("engine"), body["unavailable"].get("engine")),
        restart_policy=body["restart"].get("engine", "unset"),
        timing=classify_timing(header, mode, operation, verbs, canary_steps),
        overlap=classify_overlap(header, mode, operation, second, faults, verbs),
        canary_steps=canary_steps,
        canary_cells=str(header.get("cell", "")).split(",") if header.get("cell") else [],
        parked=parked, issue=issue,
        outcome=classify_outcome(header),
        invariants_off="inv_off" in header,
        problems=problems,
    )


def verbs_group(body):
    return {kind for group in body["groups"] for kind in group}


def scale_direction(current, targets):
    """`scale up` when any target exceeds its current count, else `scale down`."""
    if any(target > now for now, target in zip(current, targets) if target is not None):
        return "scale up"
    return "scale down"


def classify_operation(h, mode, size):
    """(operation, detail, second operation) from the mode and its knobs."""
    during = str(h.get("during", ""))
    if h.get("expect") == "reject":
        return "admission reject", str(h.get("reason", "")), ""
    if mode == "kill":
        pause = {"true": "pause", "freeze": "freeze"}.get(str(h.get("pause", "")))
        kill = f"kill {h['kill']}" if h.get("kill") else "kill a pod"
        return (pause or "steady state"), kill + (", then the lift" if pause else ""), ""
    if mode == "scaledown":
        direction = scale_direction((h.get("Ne", 0), h.get("Nd", 0)), (h.get("to_e"), h.get("to_d")))
        return direction, f"engine {h.get('Ne')}->{h.get('to_e', h.get('Ne'))}", ""
    if mode == "migrate":
        detail = f"mig={h.get('mig', 'Auto')} inst={h.get('inst', 0)}" + (" dup" if h.get("dup") else "") + (" hint" if h.get("hint") else "")
        return "migrate", detail, "push" if during == "rollout" else ""
    if mode == "recreate":
        return "delete", "delete and re-create the service", {"rollout": "push", "migrate": "migrate"}.get(during, "")
    if mode == "compremove":
        return "remove component", str(h.get("remove", "")) + (", re-added" if h.get("readd") else ""), ""
    if mode == "runtimerev":
        detail = f"{h.get('edit', 'image')} edit, expect {h.get('expect', 'roll')}"
        return "runtime edit", detail, "push" if during == "rollout" else ""
    if mode == "pairing":
        return "push", f"pairing protocol -> {h.get('proto')}", pairing_second_operation(h)
    if mode == "partition":
        return "push", f"partition={h.get('partition')}", ""
    if mode == "rebump":
        return "push", "second revision mid-roll", ""
    if mode == "canary":
        return "canary", canary_gate(h), canary_second_operation(h)
    edit = str(h.get("edit", ""))
    if edit.startswith(("strategy:", "restartPolicy:")):
        return "strategy or policy edit", f"{edit} at {during}", "push"
    if during == "idle":
        return "steady state", "persistent fault at idle", ""
    return "push", "env edit" if edit == "env" else "annotation bump", ""


def pairing_second_operation(h):
    if h.get("pause"):
        return {"true": "pause", "freeze": "freeze"}[str(h["pause"])]
    if h.get("scale_e") or h.get("scale_d"):
        return scale_direction((h.get("Ne", 0), h.get("Nd", 0)), (h.get("scale_e"), h.get("scale_d")))
    return ""


def canary_second_operation(h):
    """The operator action a canary case lands on its ladder, when that action
    is itself an operation of the ledger (the ladder's own verbs are step kinds)."""
    verbs = [str(h.get(k, "")) for k in ("verb", "verb2", "prearm")]
    for verb in verbs:
        if verb == "scale":
            return scale_direction((h.get("Ne", 0), h.get("Nd", 0)), (h.get("scale_e"), h.get("scale_d")))
        if verb == "pause":
            return "pause"
        if verb == "pause-freeze":
            return "freeze"
        if verb in ("rebump", "rebump-fix", "pairing"):
            return "push"
    return ""


def canary_gate(h):
    gate = "analysis" if h.get("analysis") else str(h.get("canary", "auto"))
    extras = [f"{k}={h[k]}" for k in ("verb", "verb2", "prearm", "proto", "proto2", "bump", "unit") if k in h]
    return " ".join([gate] + extras)


def pod_delete_kind(pods):
    return "gang member lost" if pods == "gang" else "pod deleted by hand"


def podkill_victim(h, phase, strategy, pods):
    """(component, side) of a podkill, following run.py's component-blind
    victim rule: mid-rotation under a replacement-first strategy the newest
    new-revision pod dies, otherwise a settled pod of the running revision
    (the decoder's on a prefill/decode service), unless the header aims it."""
    victim = str(h.get("victim", ""))
    side = {"stable": "running", "canary": "new"}.get(victim, "")
    topo = str(h.get("topo", ""))
    newest = phase == "rotating" and strategy in ("SurgeThenDrain", "RecreatePod") and pods == "single"
    if not side:
        side = "new" if newest else "running"
    component = str(h.get("victimcomp", ""))
    if not component:
        component = "decoder" if topo.startswith("pd") and not newest else "engine"
    return component, side


def classify_faults(h, mode, pods, strategy, verbs):
    """Every fault the case carries as (kind, form, side, component), the
    `fault=` header first; a canary's unpullable image, a stuck surge pod, a
    recovery guard's bad runtime image and an analysis outcome are the other
    ways a case breaks a revision."""
    faults = []
    kind, _, param = str(h.get("fault", "")).partition(":")
    lifted = fault_lifted(h, verbs)
    if mode == "kill":
        faults.append((pod_delete_kind(pods), "one-shot", "running", "engine"))
    if h.get("pkill"):
        faults.append((pod_delete_kind(pods), "one-shot", "running", "decoder"))
    if kind == "podkill":
        component, side = podkill_victim(h, param.partition("@")[0], strategy, pods)
        faults.append((pod_delete_kind(pods), "one-shot", side, component))
    elif kind == "crashloop":
        component = str(h.get("victimcomp", "engine"))
        crash = "crash at start" if h.get("form") == "start" else "crash after Ready"
        faults.append(("sibling broken" if component != "engine" else crash, "persistent", "running", component))
    elif kind == "imagepull":
        component = str(h.get("victimcomp", "engine"))
        faults.append(("sibling broken" if component != "engine" else "image pull", "persistent", "running", component))
    elif kind == "configkey":
        faults.append(("config missing", "one-shot" if lifted else "persistent", "running", str(h.get("victimcomp", "engine"))))
    elif kind == "node":
        faults.append(("node", "persistent" if h.get("lift") == "after-recover" else "one-shot", "-", str(h.get("victimcomp", "engine"))))
    elif kind == "cordon":
        faults.append(("node", "persistent", "-", "cordoned source node"))
    elif kind == "quota":
        faults.append(("quota", "one-shot" if lifted else "persistent", "-", f"headroom {param}"))
    elif kind == "ctrlrestart":
        faults.append(("controller restart", "one-shot", "-", "manager"))
    if h.get("img"):
        component = str(h.get("imgcomp") or h.get("bump", "engine"))
        sibling = component in ("decoder", "router")
        faults.append(("sibling broken" if sibling else "image pull", "persistent", "new", component))
    if h.get("break") == "stuck":
        faults.append(("readiness never passes", "persistent", "new", "engine"))
    if mode == "runtimerev" and h.get("expect") == "recover" and h.get("break") != "stuck":
        faults.append(("image pull", "persistent", "new", "runtime image"))
    if h.get("analysis") == "breach" or h.get("health") == "breach":
        faults.append(("metric breach", "one-shot" if h.get("analysis") == "pass" else "persistent", "new", "canary metric"))
    if h.get("analysis") == "inconclusive":
        faults.append(("analysis source unusable", "persistent", "-", "prometheus"))
    return faults


def fault_lifted(h, verbs):
    """The harness takes the cause away again: a lift phase, a quota lift verb,
    or a metric breach that decays while the analysis still promotes."""
    return (h.get("lift") is not None or "quotalift" in verbs
            or (h.get("health") == "breach" and h.get("analysis") == "pass"))


def classify_group(groups):
    kinds = [set(g) for g in groups]
    if any("canary" in g for g in kinds):
        return "canary"
    if len(kinds) >= 2 and all("blueGreen" in g for g in kinds):
        return "Sequential"
    if len(kinds) == 1:
        if "blueGreen" in kinds[0]:
            return "blueGreen"
        if "maintainRatio" in kinds[0]:
            return "rollingUpdate+maintainRatio"
        if "rollingUpdate" in kinds[0]:
            return "rollingUpdate"
    return "none" if not kinds else "mixed"


def classify_budget(strategy, surge, unavailable):
    """The budget the engine rolls under: maxSurge for SurgeThenDrain, else
    maxUnavailable; classed the way section 2 crosses them."""
    value = surge if strategy == "SurgeThenDrain" else unavailable
    if value is None:
        return "unset"
    if value.endswith("%"):
        return "percent"
    return value if value in ("0", "1") else "2+"


def hook_timing(hook, h, canary_steps):
    if hook == "canary-serving":
        if h.get("analysis"):
            return "canary analysis"
        return "canary soak" if "timed soak" in canary_steps else "canary step"
    return {
        "settling": "settling", "rotating": "rotating", "migrating": "migrating",
        "rollout": "rotating", "migrate": "migrating", "idle": "steady state",
        "canary-staging": "canary staging", "canary-prehold": "canary pre-hold",
        "canary-draining": "canary draining", "canary-rollingback": "canary rolling back",
        "canary-rolledback": "canary rolled back", "canary-done": "canary done",
        "canary-failed": "canary failed",
    }.get(hook, "-")


def classify_timing(h, mode, operation, verbs, canary_steps):
    """When the case's event (its fault or its second action) lands. A fault
    phase wins over the verb hooks; the verb under test (during2) wins over
    the hook that set it up (during)."""
    if mode == "kill":
        return "paused" if h.get("pause") else "steady state"
    if h.get("prearm"):
        return "steady state"
    fault_phase = str(h.get("fault", "")).partition(":")[2].partition("@")[0]
    if fault_phase not in FAULT_PHASES | CANARY_HOOKS | {"idle"}:
        fault_phase = ""
    hook = fault_phase or str(h.get("during2") or h.get("during", "")).partition("@")[0]
    if hook:
        return hook_timing(hook, h, canary_steps)
    mid_transition = any(h.get(k) for k in ("pause", "scale_e", "scale_d", "proto2", "pkill"))
    if mode == "rebump" or (mode == "pairing" and mid_transition):
        return "rotating"
    if mode == "canary" and h.get("rollback") and not h.get("analysis"):
        return hook_timing("canary-serving", h, canary_steps)
    if mode == "canary" and h.get("img"):
        return "canary staging"
    if mode == "canary" and h.get("analysis"):
        return "canary analysis"
    if str(h.get("fault", "")).startswith(("crashloop:", "imagepull:")):
        return "settling"
    return "-"


def classify_overlap(h, mode, operation, second, faults, verbs):
    overlap = []
    if faults and operation not in ("steady state", "admission reject"):
        overlap.append("operation+fault")
    if second:
        overlap.append("operation+operation")
    if mode == "rebump" or verbs & {"rebump", "rebump-fix"} or h.get("proto2"):
        overlap.append("two revisions in a row")
    return overlap


def classify_canary_steps(h, step_chunks, verbs):
    kinds = set()
    for chunk in step_chunks:
        traffic = re.search(r"traffic:\s*([0-9]+)", chunk)
        capacity = re.search(r"capacity:\s*\"?([0-9]+)", chunk)
        if traffic and capacity and int(traffic.group(1)) == 0 and int(capacity.group(1)) > 0:
            kinds.add("capacity ahead of traffic")
        if traffic and int(traffic.group(1)) > 0:
            kinds.add("traffic step")
        if re.search(r"pause:\s*\{\s*\}", chunk):
            kinds.add("manual gate")
        if re.search(r"pause:\s*\{\s*duration", chunk):
            kinds.add("timed soak")
        if "analysis:" in chunk:
            kinds.add("analysis gate")
    if verbs & {"promote", "promote-stale"} or h.get("canary") == "manual":
        kinds.add("promote")
    if h.get("terminal") == "Failed" or (h.get("analysis") == "inconclusive" and h.get("oninconclusive") == "hold"):
        kinds.add("abort")
    if (h.get("rollback") or "rollback" in verbs or h.get("analysis") == "breach"
            or h.get("oninconclusive") == "rollback"):
        kinds.add("rollback mid-ladder")
    if verbs & {"rebump", "rebump-fix", "pairing"} or h.get("proto2"):
        kinds.add("re-target mid-ladder")
    if "resume" in verbs:
        kinds.add("resume")
    if verbs & {"repin", "planedit", "planedit-repin", "groupremove", "runlost", "timeout-set",
                "timeout-clear", "planinvalid"}:
        kinds.add("plan or run edit")
    return [k for k in CANARY_STEP_KINDS if k in kinds]


def classify_parked(h):
    if h.get("notimpl"):
        return "parked", h.get("issue")
    if h.get("expect") == "parked":
        return "parked", h.get("issue")
    if h.get("infeasible"):
        return "infeasible", None
    return "", None


def classify_outcome(h):
    parts = [f"{k}={h[k]}" for k in ("expect", "terminal", "nophase") if k in h]
    if h.get("blocked"):
        parts.append("blocked")
    return " ".join(parts)


# ── The corpus ──────────────────────────────────────────────────────────────
def load_cases(root=ISVCS):
    return [read_case(path) for path in sorted(root.glob("**/*.yaml"), key=lambda p: (p.parent.name, p.stem))]


def fault_row_label(row):
    kind, form, side = row
    if kind == "none":
        return "none"
    return kind + f", {form}" + (f", {side}" if side != "-" else "")


def all_fault_rows():
    rows = [NO_FAULT]
    rows += [(k, f, s) for k in REVISION_BOUND_FAULTS for f in FAULT_FORMS for s in FAULT_SIDES]
    rows += [(k, f, "-") for k in UNBOUND_FAULTS for f in FAULT_FORMS]
    return rows


def matrix_cells(cases):
    """{(shape family, operation, fault row): [cases]} over every case."""
    cells = defaultdict(list)
    for case in cases:
        for row in case.fault_rows():
            cells[(case.shape_family, case.operation, row)].append(case)
    return cells


def excluded(family, operation, row):
    return any(predicate(family, operation, row) for _reason, predicate in EXCLUDED_CELLS)


def product_cells():
    """Every cell of the ledger's product that is not removed with a reason."""
    return [(family, operation, row) for family in SHAPE_FAMILIES for operation in OPERATIONS
            for row in all_fault_rows() if not excluded(family, operation, row)]


def empty_cells(cells):
    return [key for key in product_cells() if key not in cells]


def story_cells(cases, cells):
    """[(number, text, proving cases, cells they occupy)] in story order."""
    out = []
    for number, text, predicate in STORIES:
        proving = [c for c in cases if predicate(c)]
        occupied = sorted({(c.shape_family, c.operation, row) for c in proving for row in c.fault_rows()})
        out.append((number, text, proving, occupied))
    return out


def parked_cells(cells):
    """Cells where every case is parked or infeasible, with the issues."""
    out = []
    for key, members in sorted(cells.items()):
        if members and all(c.parked for c in members):
            issues = sorted({c.issue for c in members if c.issue})
            out.append((key, [c.id for c in members], issues, sorted({c.parked for c in members})))
    return out


# ── Markdown rendering ──────────────────────────────────────────────────────
def table(headers, rows):
    out = ["| " + " | ".join(str(h) for h in headers) + " |", "|" + "---|" * len(headers)]
    out += ["| " + " | ".join(str(v) for v in row) + " |" for row in rows]
    return out + [""]


def crossing(cases, row_axis, row_values, col_axis, col_values, label=""):
    """A counts table of one axis against another, every value listed so the
    zeros are visible."""
    counts = Counter((getattr(c, row_axis), getattr(c, col_axis)) for c in cases)
    rows = [[value] + [counts.get((value, col), 0) or "." for col in col_values] for value in row_values]
    return table([label or row_axis] + list(col_values), rows)


def fault_crossing(cases, col_axis, col_values, label):
    """Fault rows (kind, form, side) against another axis."""
    counts = Counter()
    for c in cases:
        for row in c.fault_rows():
            counts[(row, getattr(c, col_axis))] += 1
    rows = [[fault_row_label(row)] + [counts.get((row, col), 0) or "." for col in col_values]
            for row in all_fault_rows() if any(counts.get((row, col)) for col in col_values)]
    return table([label] + list(col_values), rows)


def cell_text(members):
    parked = sum(1 for c in members if c.parked)
    return f"{len(members)}" + (f" ({parked} parked)" if parked else "")


def render_summary(cases):
    out = ["## Summary", ""]
    parked = [c for c in cases if c.parked == "parked"]
    infeasible = [c for c in cases if c.parked == "infeasible"]
    outside = [c for c in cases if c.shape_family not in SHAPE_FAMILIES or c.operation in OTHER_OPERATIONS]
    out.append(f"- cases: {len(cases)} in {len({c.folder for c in cases})} folders; "
               f"{len(parked)} parked on an issue, {len(infeasible)} infeasible by design")
    cells = matrix_cells(cases)
    product = product_cells()
    raw = len(SHAPE_FAMILIES) * len(OPERATIONS) * len(all_fault_rows())
    filled = sum(1 for key in product if key in cells)
    out.append(f"- ledger product: {len(SHAPE_FAMILIES)} shape families x {len(OPERATIONS)} operations x "
               f"{len(all_fault_rows())} fault rows = {raw} cells, {raw - len(product)} removed with a reason; "
               f"{filled} driven, {len(product) - filled} empty")
    out.append(f"- cases outside the product (engine+decoder shapes, {', '.join(OTHER_OPERATIONS)}): {len(outside)}")
    stories = story_cells(cases, cells)
    without = [str(n) for n, _t, proving, _c in stories if not proving]
    out.append("- stories with no cell: " + (", ".join(without) if without else "none"))
    problems = [(c.id, p) for c in cases for p in c.problems]
    out.append(f"- header problems: {len(problems)}")
    out.append("")
    folders = Counter(c.folder for c in cases)
    out += table(["folder", "cases"], [[f, n] for f, n in sorted(folders.items())])
    return out


def render_stories(cases, cells):
    out = ["## Operator stories", "",
           "Each story lists the cases that prove it and the cells (shape family / operation / fault row) "
           "they occupy; a story with no cell is a hole the marathon fills first.", ""]
    rows = []
    for number, text, proving, occupied in story_cells(cases, cells):
        shown = "; ".join(f"{f} / {o} / {fault_row_label(r)}" for f, o, r in occupied[:8])
        if len(occupied) > 8:
            shown += f"; +{len(occupied) - 8} more"
        rows.append([number, text, len(proving), len(occupied), shown or "**no cell**"])
    return out + table(["#", "story", "cases", "cells", "cells (first eight)"], rows)


def render_matrix(cases, cells):
    out = ["## The matrix: shape family x operation x fault", "",
           "Rows are the faults a shape family's cases carry (kind, form, side); columns are the "
           "operations. A case with two faults counts in each row. The `rollback` column is a rollback "
           "of a plain rollout; a canary's rollback is a step kind of the `canary` column. Rows with no "
           "case under a family are listed under the holes above.", ""]
    families = SHAPE_FAMILIES + sorted({c.shape_family for c in cases} - set(SHAPE_FAMILIES))
    for family in families:
        members = [c for c in cases if c.shape_family == family]
        note = "" if family in SHAPE_FAMILIES else " (outside the product)"
        out += [f"<details><summary>{family}{note}: {len(members)} cases</summary>", ""]
        columns = OPERATIONS + [o for o in OTHER_OPERATIONS if any(c.operation == o for c in members)]
        rows = []
        for row in all_fault_rows():
            line = [cell_text(cells[(family, op, row)]) if cells.get((family, op, row)) else "." for op in columns]
            if any(v != "." for v in line):
                rows.append([fault_row_label(row)] + line)
        out += table(["fault \\ operation"] + columns, rows)
        widths = Counter(c.gang_width for c in members if c.pods == "gang")
        sizes = Counter(c.size_class for c in members)
        out.append("sizes: " + ", ".join(f"{k}: {sizes[k]}" for k in SIZE_CLASSES if sizes[k])
                   + ("; gang widths: " + ", ".join(f"W{w}: {n}" for w, n in sorted(widths.items())) if widths else ""))
        out += ["", "</details>", ""]
    return out


def render_holes(cells):
    """The empty cells of the product, grouped so the biggest holes come first:
    the axis values no case touches, then counts per fault row, operation and
    shape family, then the cells by name."""
    product = product_cells()
    empty = empty_cells(cells)
    out = ["## Empty cells, biggest holes first", "",
           f"{len(empty)} of the product's {len(product)} cells have no case.", ""]
    per_fault, fault_total = axis_holes(empty, product, lambda key: key[2])
    per_op, op_total = axis_holes(empty, product, lambda key: key[1])
    per_family, family_total = axis_holes(empty, product, lambda key: key[0])
    nowhere_rows = [row for row in all_fault_rows() if per_fault.get(row) == fault_total.get(row)]
    out += ["### No case anywhere", ""]
    out.append(f"- fault rows ({len(nowhere_rows)}): " + "; ".join(fault_row_label(r) for r in nowhere_rows))
    out.append("- operations: " + (", ".join(o for o in OPERATIONS if per_op.get(o) == op_total.get(o)) or "none"))
    out.append("- shape families: " + (", ".join(f for f in SHAPE_FAMILIES if per_family.get(f) == family_total.get(f)) or "none"))
    out.append("")
    out += ["### By fault row (rows with some coverage)", ""]
    out += table(["fault row", "empty", "of"],
                 [[fault_row_label(r), n, fault_total[r]] for r, n in per_fault.most_common() if r not in nowhere_rows])
    out += ["### By operation", ""]
    out += table(["operation", "empty", "of"], [[o, n, op_total[o]] for o, n in per_op.most_common()])
    out += ["### By shape family", ""]
    out += table(["shape family", "empty", "of"], [[f, n, family_total[f]] for f, n in per_family.most_common()])
    out += ["### By name", "",
            "For each fault row with some coverage: the operations no shape family drives, then the "
            "operations some families drive with the families that do not.", ""]
    for row, _n in per_fault.most_common():
        if row in nowhere_rows:
            continue
        out.append(f"- **{fault_row_label(row)}**: " + name_holes(row, empty, product))
    out += ["", "### Removed from the product", ""]
    removed = Counter()
    for family in SHAPE_FAMILIES:
        for operation in OPERATIONS:
            for row in all_fault_rows():
                for reason, predicate in EXCLUDED_CELLS:
                    if predicate(family, operation, row):
                        removed[reason] += 1
                        break
    out += table(["reason", "cells"], [[reason, removed.get(reason, 0)] for reason, _p in EXCLUDED_CELLS])
    return out


def axis_holes(empty, product, pick):
    """(empty cells per axis value, product cells per axis value)."""
    return Counter(pick(key) for key in empty), Counter(pick(key) for key in product)


def name_holes(row, empty, product):
    """One fault row's holes in words: the operations empty under every family
    the row applies to, then the partially driven ones with their missing families."""
    missing = defaultdict(list)
    for family, operation, r in empty:
        if r == row:
            missing[operation].append(family)
    applicable = defaultdict(set)
    for family, operation, r in product:
        if r == row:
            applicable[operation].add(family)
    nowhere = [op for op in OPERATIONS if applicable.get(op) and len(missing.get(op, [])) == len(applicable[op])]
    partial = [(op, fams) for op, fams in missing.items() if op not in nowhere]
    parts = []
    if nowhere:
        parts.append("never under " + ", ".join(nowhere))
    parts += [f"{op} missing {', '.join(short_family(f) for f in fams)}" for op, fams in sorted(partial)]
    return "; ".join(parts)


def short_family(family):
    return family.replace("engine+decoder+router", "PD").replace("engine+router", "E+R")


def render_crossings(cases):
    out = ["## Policy crossings", ""]
    out += ["### Operation x group policy", ""] + crossing(cases, "operation", OPERATIONS + OTHER_OPERATIONS, "group", GROUPS, "operation")
    out += ["### Operation x update strategy", ""] + crossing(cases, "operation", OPERATIONS + OTHER_OPERATIONS, "strategy", STRATEGIES, "operation")
    out += ["### Operation x budget", ""] + crossing(cases, "operation", OPERATIONS + OTHER_OPERATIONS, "budget", BUDGETS, "operation")
    out += ["### Operation x restart policy", ""] + crossing(cases, "operation", OPERATIONS + OTHER_OPERATIONS, "restart_policy", RESTART_POLICIES, "operation")
    out += ["### Fault x update strategy", ""] + fault_crossing(cases, "strategy", STRATEGIES, "fault")
    out += ["### Fault x restart policy", ""] + fault_crossing(cases, "restart_policy", RESTART_POLICIES, "fault")
    out += ["### Fault x group policy", ""] + fault_crossing(cases, "group", GROUPS, "fault")
    out += ["### Timing x operation", ""] + crossing(cases, "timing", TIMINGS, "operation", OPERATIONS, "timing")
    out += ["### Size class x operation", ""] + crossing(cases, "size_class", SIZE_CLASSES, "operation", OPERATIONS, "Instances")
    overlaps = Counter(kind for c in cases for kind in c.overlap)
    out += ["### Overlap", ""] + table(["overlap", "cases"], [[k, overlaps.get(k, 0)] for k in OVERLAPS])
    seconds = Counter((c.operation, c.second_operation) for c in cases if c.second_operation)
    out += ["### Operation x second operation", ""] + table(
        ["operation", "second operation", "cases"], [[o, s, n] for (o, s), n in sorted(seconds.items())])
    return out


def render_canary(cases):
    canaries = [c for c in cases if c.canary_steps]
    out = ["## Canary ladders", "", f"{len(canaries)} cases drive a canary ladder.", ""]
    counts = Counter((kind, c.shape_family) for c in canaries for kind in c.canary_steps)
    families = sorted({c.shape_family for c in canaries})
    out += ["### Step kind x shape family", ""]
    out += table(["step kind"] + families,
                 [[kind] + [counts.get((kind, f), 0) or "." for f in families] for kind in CANARY_STEP_KINDS])
    by_fault = Counter()
    for c in canaries:
        for kind in c.canary_steps:
            for row in c.fault_rows():
                by_fault[(kind, row)] += 1
    rows_present = [r for r in all_fault_rows() if any(by_fault.get((k, r)) for k in CANARY_STEP_KINDS)]
    out += ["### Step kind x fault row", ""]
    out += table(["step kind"] + [fault_row_label(r) for r in rows_present],
                 [[kind] + [by_fault.get((kind, r), 0) or "." for r in rows_present] for kind in CANARY_STEP_KINDS])
    grid = defaultdict(set)
    for c in canaries:
        for cell in c.canary_cells:
            parts = cell.split("/")
            if len(parts) == 3:
                grid[parts[1]].add(parts[2])
    out += ["### State grid cells named by `cell=`", ""]
    out += table(["state", "events"], [[state, len(events)] for state, events in sorted(grid.items())])
    return out


def render_parked(cases, cells):
    out = ["## Parked", ""]
    parked = [c for c in cases if c.parked]
    if not parked:
        return out + ["No case is parked or infeasible.", ""]
    out += table(["case", "kind", "issue", "cell", "reason"],
                 [[c.id, c.parked, f"#{c.issue}" if c.issue else "-",
                   f"{c.shape_family} / {c.operation} / {fault_row_label(c.fault_rows()[0])}",
                   (c.reason or c.description)[:120]] for c in parked])
    only_parked = parked_cells(cells)
    out += ["### Cells with only parked cases", ""]
    if only_parked:
        out += table(["cell", "cases", "issues", "kind"],
                     [[f"{f} / {o} / {fault_row_label(r)}", ", ".join(ids),
                       ", ".join(f"#{i}" for i in issues) or "-", ", ".join(kinds)]
                      for (f, o, r), ids, issues, kinds in only_parked])
    else:
        out.append("None.")
    return out + [""]


def render_cases(cases):
    out = ["## Cases", ""]
    rows = []
    for c in cases:
        rows.append([c.id, c.folder, c.shape_family, c.gang_width or "-", c.size, c.operation,
                     c.second_operation or "-", fault_row_label(c.fault_rows()[0]) if c.faults else "none",
                     c.faults[0][3] if c.faults else "-", c.group, c.strategy, c.budget, c.restart_policy,
                     c.timing, ", ".join(c.overlap) or "-", ", ".join(c.canary_steps) or "-",
                     (c.parked + (f" #{c.issue}" if c.issue else "")) or "-", c.outcome or "-"])
    return out + table(["case", "folder", "shape", "W", "N", "operation", "second", "fault", "on",
                        "group", "strategy", "budget", "restart", "timing", "overlap",
                        "canary steps", "parked", "outcome"], rows)


def render_markdown(cases, with_cases=False):
    cells = matrix_cells(cases)
    out = ["# OMENative perm corpus: coverage ledger", ""]
    out += render_summary(cases)
    problems = [(c.id, p) for c in cases for p in c.problems]
    if problems:
        out += ["## Header problems", ""] + table(["case", "problem"], problems)
    out += render_stories(cases, cells)
    out += render_holes(cells)
    out += render_matrix(cases, cells)
    out += render_crossings(cases)
    out += render_canary(cases)
    out += render_parked(cases, cells)
    out += ["## Not derivable from the headers", ""] + [f"- {line}" for line in NOT_DERIVABLE] + [""]
    if with_cases:
        out += render_cases(cases)
    return "\n".join(out)


def render_json(cases):
    cells = matrix_cells(cases)
    return json.dumps({
        "cases": [case_json(c) for c in cases],
        "cells": [{"shape_family": f, "operation": o, "fault": list(r), "cases": [c.id for c in members],
                   "parked": [c.id for c in members if c.parked]}
                  for (f, o, r), members in sorted(cells.items())],
        "empty_cells": [{"shape_family": f, "operation": o, "fault": list(r)} for f, o, r in empty_cells(cells)],
        "stories": [{"number": n, "story": t, "cases": [c.id for c in proving],
                     "cells": [{"shape_family": f, "operation": o, "fault": list(r)} for f, o, r in occupied]}
                    for n, t, proving, occupied in story_cells(cases, cells)],
        "parked_cells": [{"shape_family": f, "operation": o, "fault": list(r), "cases": ids, "issues": issues}
                         for (f, o, r), ids, issues, _k in parked_cells(cells)],
        "problems": [{"case": c.id, "problem": p} for c in cases for p in c.problems],
        "not_derivable": NOT_DERIVABLE,
    }, indent=1, sort_keys=True)


def case_json(case):
    out = asdict(case)
    out["faults"] = [{"kind": k, "form": f, "side": s, "component": c} for k, f, s, c in case.faults]
    out["fault_rows"] = [list(r) for r in case.fault_rows()]
    return out


# ── --check ─────────────────────────────────────────────────────────────────
def check(cases):
    """The problems that fail the check: header keys run.py knows and this
    parser does not, header values no vocabulary holds, and stories no case
    proves. Prints them; returns the exit code."""
    problems = [f"{c.id}: {p}" for c in cases for p in c.problems]
    problems += [f"{c.id}: unknown axis value {axis}={value}" for c in cases for axis, value in unknown_axes(c)]
    try:
        import run
        for key in sorted(set(run.HDR_VALIDATORS) - HEADER_KEYS):
            problems.append(f"run.py validates header key {key}= that coverage.py does not classify")
    except ImportError:
        pass
    for number, text, proving, _cells in story_cells(cases, matrix_cells(cases)):
        if not proving:
            problems.append(f"story {number} has no case: {text}")
    for problem in problems:
        print(problem, file=sys.stderr)
    if problems:
        print(f"\ncoverage.py --check: FAIL — {len(problems)} problem(s)", file=sys.stderr)
        return 1
    print(f"coverage.py --check: OK — {len(cases)} cases, every header value classified, every story has a cell")
    return 0


def unknown_axes(case):
    """Axis values outside the vocabularies at the top: the classification
    produced a value the matrix has no row or column for."""
    checks = [("operation", case.operation, OPERATIONS + OTHER_OPERATIONS),
              ("group", case.group, GROUPS), ("strategy", case.strategy, STRATEGIES),
              ("budget", case.budget, BUDGETS), ("restart_policy", case.restart_policy, RESTART_POLICIES),
              ("timing", case.timing, TIMINGS)]
    checks += [("fault", kind, REVISION_BOUND_FAULTS + UNBOUND_FAULTS) for kind, _f, _s, _c in case.faults]
    return [(axis, value) for axis, value, known in checks if value not in known]


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--json", action="store_true", help="emit the ledger as JSON")
    parser.add_argument("--cases", action="store_true", help="append the per-case table to the Markdown")
    parser.add_argument("--check", action="store_true",
                        help="nonzero when a story has no cell or a header value is unknown")
    parser.add_argument("root", nargs="?", default=str(ISVCS), help="the case tree (default: isvcs/)")
    args = parser.parse_args(argv)
    cases = load_cases(Path(args.root))
    if not cases:
        print(f"no case YAMLs under {args.root}", file=sys.stderr)
        return 1
    if args.check:
        return check(cases)
    print(render_json(cases) if args.json else render_markdown(cases, with_cases=args.cases))
    return 0


if __name__ == "__main__":
    sys.exit(main())
