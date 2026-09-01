# kubectl-qt — the AcceleratorQuota tree

`kubectl qt` ("quota tree") renders every `AcceleratorQuota` in a cluster as the
hierarchy they describe.

An AcceleratorQuota is one node of a single-rooted tree. A **Cohort** groups
other nodes; a **ClusterQueue** is a leaf that binds serving namespaces and
carries the accelerator budget. The edges live in `spec.parentRef` on each node,
so `kubectl get aq` shows you a flat list and the shape stays invisible until
the whole set is assembled. That assembly is what this plugin does.

```console
$ kubectl qt
nvidia.com/gpu on flavor a100

NAME                ROLE          NOMINAL  ADMITTED  READY  AGE
root                Cohort        512      288              2d2h
├─ org-product      Cohort        256      128              3h
│  └─ team-serving  ClusterQueue  256      128       False  3h
└─ org-research     Cohort        256      160              3h
   ├─ team-nlp      ClusterQueue  128      96               3h
   └─ team-vision   ClusterQueue  128      64               3h
```

Where the tree is authored, each leaf's fleet-wide budget is divided into a
share per workload cluster, and those shares hang off the leaf as a tier of
`WorkloadCluster` rows. See [The per-cluster split](#the-per-cluster-split).

`kubectl get aq` names its columns the same way and in the same order, so a node
reads alike either way. It carries a subset: no tree, no more than a node's
first `(resource, flavor)` pair, no reason for a broken edge, and no observed
capacity — `ALLOCATABLE` and `PEAK` are the root's alone, which a flat list has
nowhere to put but a tree does.

## Install

```sh
make kubectl-qt            # builds bin/kubectl-qt
make install-kubectl-qt    # and copies it into $(go env GOPATH)/bin
```

kubectl discovers plugins by scanning `PATH` for `kubectl-*`, so anywhere on
`PATH` works:

```sh
sudo install -m 0755 bin/kubectl-qt /usr/local/bin/kubectl-qt
```

Verify:

```console
$ kubectl plugin list
The following compatible plugins are available:
/usr/local/bin/kubectl-qt

$ kubectl qt --version
kubectl-qt version v0.1.0 (a1b2c3d)
```

### Using krew?

Nothing extra to do. kubectl resolves `kubectl qt` by scanning `PATH` for a
`kubectl-qt` binary and exec'ing it — there is no manifest to register and no
plugin manager involved. [krew](https://krew.sigs.k8s.io/) is one way to get a
`kubectl-*` onto `PATH`, not something a plugin has to be built for, so if you
have krew set up in the default location its plugins and this one coexist and
`kubectl qt` works out of the box:

```console
$ kubectl plugin list
The following compatible plugins are available:
/home/you/go/bin/kubectl-qt
/home/you/.krew/bin/kubectl-tree
```

You can also run the binary directly as `kubectl-qt`; the `kubectl qt` form just
asks kubectl to exec it for you.

## Reading the output

| Column | Meaning |
|---|---|
| `NAME` | The node's `metadata.name`, indented by its position in the tree — or, on a `WorkloadCluster` row, the cluster's name. |
| `ROLE` | `Cohort` (groups other nodes), `ClusterQueue` (a leaf carrying the budget), or `WorkloadCluster` (one cluster's share of the row above). |
| `NOMINAL` | The allowance for this (resource, flavor) pair. |
| `ADMITTED` | What is currently admitted against the materialized queues, including anything borrowed. |
| `READY` | Blank when the node is Ready. A value here means it is not. |
| `AGE` | How long the node has existed, measured once per render so no two rows disagree. |

Four conventions are worth knowing:

- **`READY` is blank when healthy.** The table exists to surface problems, so a
  healthy fleet renders an empty column and an unhealthy node is the only thing
  in it. `-o wide` spells the condition out either way.
- **A trailing `*` on `NOMINAL` means the number came from `spec`**, because
  `status.budgets` holds no entry for that (resource, flavor) pair. On a
  management plane spec is the fleet-wide total while status is the share
  resolved for that plane, so the two legitimately differ — the mark keeps a
  fleet number from being read as this plane's resolved share. See
  [below](#why-is-every-budget-marked-) for what an all-`*` tree means.
- **A trailing `!` on `NOMINAL` means the split does not add up**: the
  per-cluster shares total something other than the allowance they divide. See
  [below](#a-split-that-does-not-add-up).
- **`-` means no value**, distinct from a blank. A Cohort carrying no budget and
  a leaf budgeted zero are different states.

### Why is every budget marked `*`?

Because nothing has written `status.budgets` on those nodes:

```console
$ kubectl qt
google.com/tpu on flavor tpu7x

NAME               ROLE          NOMINAL  ADMITTED  READY  AGE
root               Cohort        -        -                12m
├─ sim-tenants     Cohort        64*      -                12m
│  ├─ sim-batch    ClusterQueue  16*      -                12m
│  └─ sim-serving  ClusterQueue  48*      -                12m
└─ solo            ClusterQueue  16*      -                12m
```

This tree is **healthy**, not broken. A blank `READY` means every node is
`Ready=True`, so the quota controller is running and has accepted the tree —
it validated the edges, computed each `status.path`, and found no violations.

What it has not done is publish resolved budgets. `status.budgets` is written by
whatever resolves budgets on the plane being read, and where nothing does, the
controller writes only `observedGeneration`, `path`, `parent`, `conditions`,
and — on the root, when capacity derivation is enabled — `status.capacity`.
`NOMINAL` then falls back to spec and `ADMITTED` has no source at all. This is a
steady state, not a pending one: the `*` will not clear by waiting.

A workload cluster materializing into Kueue does resolve them, and there the
same tree reports a resolved `NOMINAL` with no mark and a real `ADMITTED`.

`root` showing `-` rather than a number is also normal. Budgets on a Cohort are
optional, and a workload-cluster root is typically authored without one — it
reports that cluster's derived capacity in status instead, which `-o wide`
shows as `ALLOCATABLE` and `PEAK`.

So read an all-`*` tree as *"this is the shape and the requested budget"*, and
`ADMITTED` as unavailable rather than zero.

## The per-cluster split

A management plane authors one fleet-wide number per leaf and divides it into a
share per workload cluster. The tree alone shows the same total on every tier,
which is the least interesting thing a management plane knows — so each leaf's
shares are drawn as a tier of `WorkloadCluster` rows below it:

```console
$ kubectl qt
google.com/tpu on flavor tpu7x

NAME               ROLE             NOMINAL  ADMITTED  READY  AGE
root               Cohort           192      0                3h
└─ tenants         Cohort           192      0                3h
   └─ serving      ClusterQueue     192      24               3h
      ├─ member-a  WorkloadCluster  128      16
      └─ member-b  WorkloadCluster  64       8
```

A `WorkloadCluster` row's `NOMINAL` is the share the management plane assigned
that cluster, and its `ADMITTED` is what the cluster reports holding against it,
read back off the projection. The columns that describe a node rather than a
share — `READY`, `AGE`, `TIER`, `NAMESPACES`, `PATH` — are blank, because a
cluster holds no position in the quota tree.

Three things follow from where the numbers come from:

- **Only leaves fan out.** A Cohort is apportioned nothing: it travels to every
  member as pure topology, and a fleet total on a grouping tier is a number no
  single cluster could hold. So a Cohort grows no tier.
- **A workload cluster shows none of this.** The copy projected there carries
  one already-resolved share and no trace of the split that produced it, so
  `status.budgets[].perCluster` is empty and the output is the tree alone.
- **The tier grows rows, not columns.** A fleet of twenty members makes the
  output twenty rows longer rather than pushing the table off the screen.

`--split=false` collapses the tier back to the plain tree. The `!` mark and its
footer survive that, because the gap it reports is there either way.

`-o wide` adds a `POLICY` column naming how the split was derived — `Explicit`
if an admin wrote the shares, `Proportional` if they were computed against what
each cluster reported holding. It shows `-` where neither the budget nor the
node declares one, which means an operator-level default applied: that value
lives in the manager's configuration, on no object this plugin reads, so naming
one would report a policy the cluster may not be using.

### A split that does not add up

```console
$ kubectl qt
nvidia.com/gpu on flavor a100

NAME            ROLE             NOMINAL  ADMITTED  READY  AGE
root            Cohort           120      30               3h
└─ team         ClusterQueue     120!     30               3h
   └─ member-a  WorkloadCluster  90       30

1 budget split does not add up:
  ! team: nvidia.com/gpu on a100 is 120 but its per-cluster shares total 90
```

The 30 accelerators between 120 and 90 are authorized and handed to nobody: a
member the management plane could not place a share on this pass. It is not an
invariant violation and nothing else in the view reports it — the node keeps
reporting the total the admin wrote, which is correct and, on its own,
misleading. A total *above* the allowance is marked the same way, and means the
opposite: capacity handed out that nobody authorized.

Usually transient. The management plane holds the whole split rather than
re-dividing it when a member drops out, so the ordinary failure mode is the tier
vanishing entirely, not shrinking; a *partial* split means one member fell out
between the read and the write. If it persists, `status.clusters[].message` on
the node says which member and why.

### Color

Color is on by default in a terminal (`--color=auto`) and off when the output is
piped, so `kubectl qt | grep` and `kubectl qt -o json | jq` stay clean. Force it
either way with `-c always` / `-c never`; `auto` also respects `NO_COLOR` and
`TERM=dumb`.

Every mark also survives without color — nothing is encoded in color alone:

| | Meaning |
|---|---|
| **bold** name | A live node. |
| <code>red</code> name | This node broke an invariant. It is the cause. |
| plain name | Frozen by an ancestor's violation — a consequence, not a cause. Also a `WorkloadCluster` row, which is a cluster rather than a node. |
| dim row | Kept only as the path to a filter match, not a match itself. |
| dim branch art, dim `-` | Structure and absent values, receding behind the numbers. |
| <code>yellow</code> `512*` | Read from `spec`; not yet resolved by the controller. |
| <code>red</code> `120!` | Divided into per-cluster shares that do not add up to it. |
| green / red condition | Judged by meaning, not literal value — `Degraded=True` is red where `Ready=True` is green. |

The one rule worth stating: **a violation on the root freezes every node beneath
it**, so coloring the frozen set would bury the single node at fault under its
own blast radius. Only causes are colored. Likewise a filter never dims away a
node that broke something, even when that node is only on screen as context.

### The header line

With one `(resource, flavor)` pair across the whole tree, the pair is named once
above the table. With more than one, `RESOURCE` and `FLAVOR` become columns and
a node carrying several budgets spans several rows — summing across flavors
would add up two different kinds of accelerator, so the plugin never does.

### Nodes with no position in the tree

A node whose `parentRef` does not resolve, or that sits in a cycle, is not
reachable from the root. It is still in the cluster, and it is usually the node
you are looking for, so it is rendered in its own section rather than dropped:

```console
$ kubectl qt
...
NODES WITH NO POSITION IN THE TREE
└─ stranded   ClusterQueue  nvidia.com/gpu  a100  4*  -  -

3 invariants violated:
  ! stranded: parentRef "deleted-cohort" does not resolve to an existing AcceleratorQuota
    reason: ParentMissing
  ! loop-a: parentRef forms a cycle: loop-a -> loop-b -> loop-a
    reason: ParentCycle
  ! org-a: budget nvidia.com/gpu on a100 is 80 but its children total 120
    reason: ContainmentViolated
```

Violations come from the same `pkg/quota/tree` package the admission webhook and
the quota controller use, so the plugin cannot disagree with them about what is
broken. A node with a red name broke an invariant itself; a yellow one is frozen
by an ancestor that did.

A filter never hides a fault. Violations outside the current view are counted
rather than spelled out:

```
1 invariant violated:
  ! org-a: budget nvidia.com/gpu on a100 is 80 but its children total 120
    reason: ContainmentViolated
2 further invariants violated outside this view; re-run without filters to see them
```

## Common tasks

```sh
# Every column: split policy, capacity, borrowed, ceiling, namespaces, tier,
# path, all conditions.
kubectl qt -o wide

# The tree alone, without each budget's per-cluster split.
kubectl qt --split=false

# How one tenant's budget is spread across the fleet.
kubectl qt --node team-nlp

# One subtree, re-rooted. Works on an orphan too.
kubectl qt --node org-research

# Which leaf charges a serving namespace — and where it sits.
kubectl qt --for-namespace nlp-prod

# Only the budget-carrying leaves, each with the path that leads to it.
kubectl qt --leaves-only

# One accelerator type across the fleet.
kubectl qt --resource nvidia.com/gpu --flavor a100

# Flag nodes further than three edges from the root.
kubectl qt --max-depth 3

# Machine-readable. Keeps orphans and violations, not just .root.
kubectl qt -o json | jq '.violations[] | {node, reason}'
```

`--for-namespace` and `--leaves-only` prune rather than flatten: a matched leaf
keeps its ancestors so you can see where it sits. Those ancestors are context,
not answers — they are dimmed when color is on, and the `ROLE` column separates
them either way, since only a `ClusterQueue` can match. A branch containing no
match is dropped entirely, so on a tree where every Cohort leads to some leaf,
`--leaves-only` legitimately prunes nothing.

`--resource` and `--flavor` filter budget *rows* inside a node, never which
nodes appear — a Cohort carrying no budget for the selected pair is still the
parent of the leaves that do.

### Flags

| Flag | Default | Purpose |
|---|---|---|
| `-o, --output` | `tree` | `tree`, `wide`, `json`, or `yaml`. |
| `--split` | `true` | Draw each budget's per-cluster shares as a tier below the node. `--split=false` for the tree alone. |
| `--node` | | Re-root the render at one node. |
| `--leaves-only` | `false` | Keep only budget-carrying leaves, with their paths. |
| `--for-namespace` | | Keep the leaves binding this serving namespace. |
| `--resource` | | Restrict budget rows to one resource, e.g. `nvidia.com/gpu`. |
| `--flavor` | | Restrict budget rows to one Kueue ResourceFlavor. |
| `--max-depth` | `0` | Report nodes deeper than this many edges. `0` disables the check. |
| `-c, --color` | `auto` | `auto`, `always`, `never`. `auto` honors `NO_COLOR` and `TERM`. |
| `--ascii` | `false` | Draw branches with ASCII instead of box-drawing characters. |

Plus the standard kubectl connection flags — `--kubeconfig`, `--context`,
`--cluster`, `--user`, `--server`, `--request-timeout`, and the auth and TLS
overrides. The `oidc`, `gcp`, and `azure` client-auth providers are compiled in,
so the plugin authenticates against whatever context your kubeconfig already
uses. Apiserver deprecation warnings are suppressed, since one landing mid-table
would break the alignment the tree depends on.

> [!NOTE]
> There is no `-n/--namespace`. AcceleratorQuota is cluster-scoped, so the flag
> would be accepted and do nothing. Use `--for-namespace` to ask which leaf
> charges a given serving namespace.

`--max-depth` defaults to off on purpose. The depth limit is operator
configuration the plugin cannot see from outside the cluster, so guessing one
would report a `DepthExceeded` the controller never raised.

## JSON and YAML

The machine formats emit the same view the tree renders, nested rather than
flattened, so a script and an operator read one structure:

```jsonc
{
  "root": {
    "name": "root",
    "role": "Cohort",
    "path": "/root",
    "depth": 0,
    "budgets": [
      { "resourceName": "nvidia.com/gpu", "resourceFlavor": "a100",
        "nominal": "512", "admitted": "288", "borrowed": "0" }
    ],
    "capacity": [
      { "resourceName": "nvidia.com/gpu", "resourceFlavor": "a100",
        "allocatable": "520", "highWaterMark": "544" }
    ],
    "conditions": { "Ready": "True" },
    "children": [
      { "name": "team-nlp",
        "role": "ClusterQueue",
        "budgets": [
          { "resourceName": "nvidia.com/gpu", "resourceFlavor": "a100",
            "policy": "Proportional",
            "nominal": "128", "admitted": "96", "borrowed": "0",
            "perCluster": [
              { "cluster": "member-a", "nominal": "96", "admitted": "72", "borrowed": "0" },
              { "cluster": "member-b", "nominal": "32", "admitted": "24", "borrowed": "0" }
            ] }
        ] }
    ]
  },
  "orphans":    [ /* nodes with no position in the tree */ ],
  "violations": [ { "node": "...", "reason": "...", "message": "..." } ]
}
```

Quantities marshal as strings, the same way they appear on the CR. `orphans` and
`violations` are always present when non-empty — a consumer reading only `.root`
would otherwise conclude a broken tree was healthy. An empty cluster emits `{}`,
never `null`, so "no quotas exist" stays distinguishable from a failed request.

`policy` and `perCluster` appear on leaves only, and are emitted whatever
`--split` and `-o wide` were set to: those flags budget terminal width, which a
machine format does not have. `perCluster` is absent rather than empty on a
workload cluster, which resolves no split.

## RBAC

Read-only, cluster-scoped:

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: acceleratorquota-viewer
rules:
  - apiGroups: ["ome.io"]
    resources: ["acceleratorquotas"]
    verbs: ["get", "list"]
```

## Releasing

The plugin is a pure-Go binary with no cgo, so it cross-compiles without a
toolchain per target:

```sh
for platform in linux/amd64 linux/arm64 darwin/amd64 darwin/arm64 windows/amd64; do
  GOOS=${platform%/*} GOARCH=${platform#*/} CGO_ENABLED=0 \
    go build -ldflags "-s -w" -o "dist/${platform%/*}_${platform#*/}/kubectl-qt" ./cmd/kubectl-qt
done
```

Archive each directory and publish the archives wherever releases go, with
checksums:

```sh
sha256sum dist/*.tar.gz
```

Consumers unpack the binary onto `PATH`; there is nothing to register.

## Development

```sh
make kubectl-qt                        # build
make install-kubectl-qt                # and put it on PATH to try it
go test ./pkg/kubectl/quotatree/...    # unit tests, no cluster needed
```

The command body lives in `pkg/kubectl/quotatree` rather than in `main`, so the
rendering is testable against fixtures. `cmd/kubectl-qt` is a thin entry point.

Tree assembly is **not** implemented here. `pkg/quota/tree` is the authority on
what the edges mean — which node is the root, which parents dangle, which
budgets overrun their parent — and this package is a view over its output.
Duplicating any of it would let the plugin disagree with the controller, which
is the one thing a diagnostic tool must never do.

There is also an end-to-end test that runs the real clientset against an
ephemeral apiserver. It skips unless envtest assets are present:

```sh
export KUBEBUILDER_ASSETS="$(bin/setup-envtest use "$(cat .envtest-k8s-version)" -p path)"
go test ./pkg/kubectl/quotatree/ -run TestAgainstLiveAPIServer -v
```
