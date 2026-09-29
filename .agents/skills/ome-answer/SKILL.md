---
name: ome-answer
description: Answer an operator's question about a live OME workload from the OME bot - "why is my deployment down", "why is my rollout stuck", "is traffic draining", "which cluster serves X" - from read-only kubectl-ome evidence gathered through the ome-bridge helper, and reply with a verdict, the evidence, the next action and the scope searched. Never mutates anything. Load after ome-bridge.
---

# ome-answer — triage from evidence, reply with a verdict

Read `ome-bridge` first for the helper, the verbs, the fan-out rule and how
to pick clusters. Every command below is
`$B command "<verb and args>"` with
`B="python3 <ome-bridge skill dir>/scripts/ome_bridge.py"`. Everything here
is read-only; if the fix needs a guarded verb, name it and stop.

## 1. Locate the object

```sh
$B clusters                                              # names, roles, reachability
$B command "get isvc -A -o wide"                         # fans out to every cluster
$B command "placement status <name> -n <ns> --cluster <hub>"
$B command "placement endpoint <name> -n <ns> --cluster <hub>"
```

`get isvc -A` finds every cluster that carries the service. On the hub,
`placement status` and `placement endpoint` say where it is placed and which
origins serve it; then go to those members. No match anywhere: say so, list
where you looked, ask for the namespace.

## 2. Read the state

```sh
$B command "status <name> -n <ns> -o wide"               # fans out; or add --cluster <member>
$B command "rollout status <name> -n <ns> --cluster <member>"
$B command "instance list <name> -n <ns> --cluster <member>"
```

Conditions and the rollout phase decide which branch below applies.

## 3. Branch on the symptom

Service verbs take `-n <ns>`. Each runs on one cluster (`--cluster <name>`):
the hub for placement, traffic and quota budgets, the member for everything
else.

| Symptom | Read next | Usual causes |
|---|---|---|
| not Ready, replicas below desired | `instance status <name> <idx> --component engine`, `quota status`, `quota tree` (cluster-scoped), `admin doctor --isvc <name>` | quota or gang admission pending, image pull failure, model or adapter still downloading, node pressure, a Held revision |
| rollout stuck at a step | `rollout explain`, `rollout history`, `autoscale status` | a manual or analysis gate waiting, RatioBalanced pacing waiting on capacity, paused or frozen, canary analysis failing, repin needed after drift |
| traffic not moving | `traffic status`, `traffic explain`, `placement explain`, `placement endpoint` | drain in progress, weights held at zero while a home is failing, revision Service not converged, a selector no cluster matches |
| instances flapping or Failed | `instance status`, `logs <name> --tail 200`, `runtime effective` | runtime crash loop, wrong accelerator shape, probe timing, recent runtime change |
| "it worked yesterday" | `rollout history`, `runtime history`, `migration history` | a promotion, a runtime sync, a migration; name the event and its time |

Confirm one cause with direct evidence before naming it. Two candidate
causes with evidence for each is a fine answer; a guess is not. Stop
collecting once the verdict rests on two independent pieces of evidence.

A helper exit 3 still carries the CLI's output: kubectl-ome exit 2 is an
unmet assertion (a quota violation, an invalid rollout), which is often the
finding itself. The `admin doctor` BenchmarkJob line is expected and not a
finding (see `ome-bridge`).

## 4. Reply

Slack mrkdwn, under 3,500 characters:

```
*Verdict* — one sentence: what is wrong and since when.
*Evidence* — 2 to 5 lines quoted from the CLI, each prefixed by cluster/namespace.
*Next action* — the exact command or change; if it is a guarded verb, say
"needs the oncall" and stop.
*Scope* — clusters searched; clusters unreachable or timed out.
read-only, nothing changed
```

## Rules

- Never run or ask for a guarded verb (`rollout pause|resume|promote|rollback|repin`,
  `traffic drain|undrain`, `migration start`, `instance release-held`,
  `runtime sync`, `scale`), and never pass `--yes` or `--dry-run`. They are
  not in the bridge's grammar anyway; a `not_a_verb` refusal for one is the
  expected answer, not something to work around.
- Time-box: if the fleet lookup exceeds two minutes, answer with what you
  have and name the clusters that did not respond.
- Prefer the hub's view for placement and the member's view for pods.
- `Unavailable`, `Unreadable` or `forbidden` mean the bridge could not reach
  or read something; say so, and do not describe that as the workload being
  down.
