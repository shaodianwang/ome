---
name: serving-incident
description: Triage a live serving alert on an OME-served model - SR fatal rate, 5xx, timeouts, "is model X broken right now" - by probing every hop at once (API proxy, its envoy sidecar, the multi-cluster TrafficMap, and per cluster the ingress gateway, router and engine or prefill/decode) and lining them up on one time axis. Read-only; posts findings to the alert.
---

# serving-incident — every hop at once, one time axis

Never walk the path top-down or bottom-up. Probe all hops in parallel; the
first hop whose row turns red, with clean hops below it, owns the failure.

| Hop | Source | Keeps |
|---|---|---|
| proxy | Scuba `llm_service_responses_api`, one row per request | days |
| sidecar | Scuba `envoy_access_logs`, sampled (~1:400) | days |
| traffic map | hub `trafficmap/<isvc>` (multi-cluster only) | live |
| gateway, router | in-cluster `ome-prometheus` | 1h |
| engine | pods labelled `ome.io/inferenceservice=<isvc>` | live |

## 1. Grid first (under a minute)

```sh
python3 <skill dir>/scripts/triage.py --isvc <isvc> --namespace <ns> \
  --clusters <c1>,<c2>,... [--hub <hub>] [--proxy-tier <tier>] \
  [--since 90 --bucket 10] [--model <proxy model name>]
```

- `--proxy-tier` is the alert's ODS entity without `smc.`. It is not
  derivable from the ISVC name.
- `--clusters` is every member cluster that may serve the model. Kubeconfigs
  are `~/.kube/<cluster>.conf` (see `msl-access-bootstrap`).
- Cells are error %, `·` means no data, and `Nr` means N container restarts.
- Exit 1 means a hop printed `UNREADABLE`. That hop is unknown, not healthy:
  fix access or probe it by hand.

How to read the grid:
- **Proxy red, the rest clean.** The request reached the engine and the
  failure is in the response or in one pod. The notes rank exception type ×
  checkpoint and callers. One checkpoint or one caller means a model-output
  problem, not infra.
- **`hot engine` lines** map the proxy's `decode_host` to a cluster and pod.
  A single pod failing most of its requests is a bad pod.
- **Gateway or router red.** Check whether it is one cluster or all of
  them, then compare with the engine row (restarts, not-ready) and with the
  TrafficMap weights.
- **Sidecar flags** such as `UF`, `UH`, `URX` or `UT` mean the proxy could
  not reach the gateway. `DC` is only the client hanging up.

## 2. Drill only into the red hop

`references/hops.md` has the per-hop queries and keys, how to find ODS keys,
how to read gateway or router history older than 1h, and how to find what
changed near onset.

## 3. Report on the alert

Check `meta metacli.command annotations --command "monitoring.alert comment" -o json`,
then post with `--dry-run` first:

```sh
meta monitoring.alert comment --alert-id='<id from the alert URL>' --comment="$(cat note.txt)"
```

Write the note as WHAT FAILS (rate, window, the exception) / WHERE (hop,
checkpoint, caller, pod) / RULED OUT (the clean hops) / CHANGE AFTER ONSET /
ASKS (owner and action). Re-run the grid to answer "is it recovering".

## Rules

- Read-only. Never restart, scale, drain, roll back or delete. Name the
  action and its owner.
- Run every meta command's annotation check first. Anything not
  `AUTONOMOUS` needs the user's confirmation.
- Never select `exception_message` from the proxy log. It is ACL-restricted,
  and the other columns name the failure.
