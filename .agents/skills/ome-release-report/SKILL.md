---
name: ome-release-report
description: Produce the OME release report - what each Kargo stage has promoted, what version each cluster actually runs, and what waits in the warehouse - from the ome-bridge release endpoint, on request or on a schedule, and post it to the working-group channel. Read-only.
---

# ome-release-report

A release is a GitOps promotion: CI publishes charts and images from `main`
(chart version `1.0.0-<UTC timestamp>.<short SHA>`, see `docs/RELEASE.md`),
the Kargo project on the management cluster discovers the release marker in
its Warehouse, and its Stages promote the versions through the
release-candidate plane, the production plane and the workload clusters by
writing the pins into the GitOps repository.

Every read goes through the bridge: `$B ...` with
`B="python3 <ome-bridge skill dir>/scripts/ome_bridge.py"`. Read `ome-bridge`
for the environment and exit codes.

## 1. Read

```sh
$B release          # every Stage (freight, chart versions, last promotion, phase, health), newest Freight, running manager version per cluster
$B --json release   # the same as structured data when you build the table yourself
$B clusters         # reachability and reported OME version per cluster
```

`release` answering `not_configured` (exit 2) means the bridge has no
release source; the report then says so and lists only the cluster versions
from `clusters`.

## 2. Interpret

- A stage whose current freight is older than the newest freight is a
  pending promotion; say which and how old.
- The running manager image tag is the short SHA. A cluster whose running
  SHA differs from its stage's freight has a sync that has not landed.
- A stage with a failing phase or unhealthy status is stuck; quote its
  reason.
- An unreachable cluster is reported as unreachable, not as stale.

## 3. Post

Table first, then what is waiting, then anything stuck. Under 3,000
characters; post with the `slack-post` skill.

```
*OME release* <date>
```
```
STAGE       PROMOTED (ome-resources)        WHEN         RUNNING ON CLUSTER
<stage>     1.0.0-<ts>.<sha>                 <age>        <sha>
...
```
```
*Waiting*: <freight> since <age>, not yet on <stage> (<reason from the stage>)
*Stuck*: <stage> <phase>: <reason>
```

## Rules

- Read-only: never promote, never touch Stage or Freight objects; the bridge
  has no verb for either.
- Every version in the report comes from the `release` or `clusters` output;
  never infer a version from a commit message.
- Missing access to a cluster or a missing release source is reported as
  such, not as a stale release.
- The list of `main` commits not yet promoted needs GitHub, which the bridge
  does not have; leave it out rather than guess.
