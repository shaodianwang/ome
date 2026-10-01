---
name: ome-answer
description: Answer an operator's question about a live OME workload from the OME bot - "why is my deployment down", "why is my rollout stuck", "is traffic draining", "which cluster serves X" - from read-only kubectl-ome evidence gathered through the ome-bridge helper, and reply with a verdict, the evidence, the next action and the scope searched. Never mutates anything. Load after ome-bridge.
---

# ome-answer — triage from evidence, reply with a verdict

Read `ome-bridge` first for the helper, the verbs, the fan-out rule and how
to pick clusters, and `ome-docs/references/ome-primer.md` once for the object
model, the status vocabulary and the failure signatures. Every command below is
`$B command "<verb and args>"` with
`B="python3 <ome-bridge skill dir>/scripts/ome_bridge.py"`. Everything here
is read-only; if the fix needs a guarded verb, name it and stop. An error-rate
or fatal-rate alert on a serving model belongs to the human oncall's
`serving-incident` runbook in this repository, which the bot does not carry:
say so, then answer the workload question from here. A
question that spans clusters, names a cluster `clusters` does not list, or
asks about incidents, nodes or capacity also needs `ome-fleet`: the registry
is a subset of the fleet, and the reply must say which clusters it covers.

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

Write for a person who did not run the commands. Slack mrkdwn, under 3,500
characters, no headers, no tables, no section labels such as Verdict or
Evidence:

- First line: the answer in one plain sentence with the key fact in bold,
  for example "*Two deployments are stuck* in `def-sandbox`, one for 7 days
  and one for 14."
- *What I checked*: two to five bullets, one fact each, written as sentences
  that name the service, the cluster and the number. Say what a condition
  means instead of naming it: "its router pods never became ready", not
  `ComponentNotReady`. Quote a CLI line only when the exact words matter,
  keep it under 100 characters, and explain it in the same bullet.
- *What to do*: one or two bullets naming who does what. Leave it out when
  nothing needs doing. A guarded verb reads "the oncall would need to ...";
  never present it as something the asker should run.
- Last line: `Looked at <clusters>; could not see <clusters or sources>.`
  followed by `_read-only, nothing changed_`.

The structured result still carries the raw quoted lines in `evidence`; the
posted reply is for the reader.

## Rules

- Never run or ask for a guarded verb (`rollout pause|resume|promote|rollback|repin`,
  `traffic drain|undrain`, `migration start`, `instance release-held`,
  `runtime sync`, `scale`), and never pass `--yes` or `--dry-run`. They are
  not in the bridge's grammar anyway; a `not_a_verb` refusal for one is the
  expected answer, not something to work around.
- Time-box: if the fleet lookup exceeds two minutes, answer with what you
  have and name the clusters that did not respond.
- Fan out with `-o wide`, never `-o json`: a reply is capped at about
  12,000 characters and a fleet-wide JSON dump arrives truncated and will
  not parse. `-o json` is for one object on one cluster (`--cluster` and a
  name), when you need a field the table does not show.
- Prefer the hub's view for placement and the member's view for pods.
- `Unavailable`, `Unreadable` or `forbidden` mean the bridge could not reach
  or read something; say so, and do not describe that as the workload being
  down.
