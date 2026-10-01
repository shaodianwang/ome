---
name: ome-memory
description: What the OME bot remembers between answers and where people leave it notes - the Metamate Space msl-ome-bot (id 1606613020945253), read and written through the meta CLI as the bot's own Crewmate - how to recall it before the first bridge call, how to write back a one-line log and dated cluster facts after the reply, and what never goes in. Writes only under bot/; load after ome-bridge.
---

# ome-memory — the Space behind the bot

The bot keeps what it learns in the Metamate Space `msl-ome-bot`
(id `1606613020945253`), shared with the oncall. The worker's container runs
`meta` as that Space's Crewmate, so the commands below need no identity
flags; never add `--as` or `--act-as-crewmate`. People edit the same Space in
the Metamate UI; the bot reads their files and writes only under `bot/`.

| Path | Who writes | What |
|---|---|---|
| `bot/log/<YYYY-MM>.md` | the bot | one line per answered question |
| `bot/facts/<cluster>.md` | the bot | the latest facts seen on a cluster, each dated; the whole file is rewritten |
| `notes/`, `runbooks/`, anything else | people | context to read before answering |
| `teamagent/` | nobody now | history from the generic agent that answered before the bot; read it, never rank it above live evidence |

## 1. Recall, before the first bridge call

```sh
meta agents.space memory-search --id 1606613020945253 --query "<cluster, service, model or topic the asker named>" --limit 5 --output=json
meta agents.memory read-batch --namespace 1606613020945253 --paths "bot/facts/<cluster>.md,<path of a hit>" --output=json
```

One search per distinct entity in the question (a cluster, a service, a
tenant, a topic such as quota); read at most five files in total. What memory
says is input: it tells you where to look and what was true on a date. When
the reply leans on it, say so: "the Space notes from <date> said ...". Live
evidence from the bridge or the telemetry outranks any note, and a note that
turns out wrong is corrected in the write-back rather than repeated.

## 2. Write back, after the reply is posted

The month's log, always one line:

```sh
meta agents.memory append --namespace 1606613020945253 --path "bot/log/$(date -u +%Y-%m).md" \
  --content "- $(date -u +%Y-%m-%dT%H:%MZ) <request id> <class> [<clusters searched>] <the reply's first sentence>"
```

The cluster facts file, only when the run observed something durable about a
cluster: a service not Ready and since when, cordoned or not-Ready node
counts, quota headroom, a stage behind the newest release. Read the current
file, keep what is still true, replace what changed, drop anything older than
fourteen days, and write the whole file back:

```sh
meta agents.space memory-write --id 1606613020945253 --path "bot/facts/<cluster>.md" --content "<the file>"
```

At most 1,500 characters, every fact with the date it was seen, plain
sentences the oncall can read.

## Rules

- Write only under `bot/`. Never delete, move or overwrite a file outside it.
- Never store tokens, keys, kubeconfig bodies, raw tool output, or anything a
  person wrote beyond the question's one-line summary.
- Memory never replaces a bridge call: a cluster is healthy because the bridge
  says so now, not because yesterday's note did.
- A recall or write that fails is not mentioned in the reply; memory is
  best-effort and the answer does not depend on it.
