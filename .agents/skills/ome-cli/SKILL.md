---
name: ome-cli
description: How a person on a devserver runs kubectl-ome against the clusters where OME is installed - building the binary, one kubeconfig per fleet cluster through msl-access-bootstrap, the read-only verbs, the rules for guarded verbs, and how to read the output. Load before running kubectl-ome by hand. The OME bot does not use this skill; it reaches the fleet through ome-bridge.
---

# ome-cli — kubectl-ome from a devserver

kubectl-ome carries all OME domain logic; you choose commands and read their
output. It is a standalone binary: `kubectl-ome get isvc -A` with a
kubeconfig works without kubectl on PATH, and `kubectl ome ...` works where
kubectl is present. The reference for every verb is the upstream README at
`cmd/kubectl-ome/README.md` and the task pages under
`site/content/en/docs/tasks/` of the upstream repository.

## The binary

No release tag carries the CLI yet; build it from upstream `main`:

```sh
git clone https://github.com/ome-projects/ome && cd ome && make kubectl-ome   # bin/kubectl-ome
```

## Kubeconfigs

Run the `msl-access-bootstrap` skill: it writes one kubeconfig per cluster
with the `cloud` CLI (devservers only) and lists every target in
`references/clusters.md`. Select a file per command, never with
`kubectl config use-context`:

```sh
kubectl-ome --kubeconfig "$HOME/.kube/<cluster>.conf" get isvc -A
```

| Cluster | Provider | Role | `cloud` target |
|---|---|---|---|
| `msl-mks-ome-lco-1-prod` | MKS | multicluster hub, owns placement | `mks/msl-mks-ome-lco-1-prod` |
| `msl-gcp-usc1-1-tpu` … `msl-gcp-usc1-4-tpu` | GKE | hub members, production serving | `msl-gcp-1/msl-gcp-usc1-N-tpu` |
| `msl-gcp-usc1-rc-1-tpu` | GKE | release candidate | `msl-gcp-rc/msl-gcp-usc1-rc-1-tpu` |
| `msl-gcp-usc1-ext-1-tpu` | GKE | standalone | `msl-gcp-ext-1/msl-gcp-usc1-ext-1-tpu` |
| `msl-cw-use2-1`, `msl-cw-use2-2`, `msl-cw-use2-dev-2` | CoreWeave | standalone | `cw/<cluster>` |

Hub first for anything about placement or "where does service X run";
members for pods, instances and logs. A `forbidden` on a read means your
identity lacks the cluster's read action; request it, never work around it.

## Read verbs

`get`, `status`, `logs`, `version`,
`rollout status|explain|history|validate`,
`traffic status|explain`, `placement status|explain|endpoint`,
`quota status|tree|validate`,
`instance list|status|retry-blocks`, `migration status|history`,
`runtime explain|effective|tree|history`, `autoscale status|explain`,
`accelerator explain`, `admin doctor|recommendations`, `cluster status`.

`get` and `status` without a target cluster mean "look everywhere": run them
on every cluster in the table and merge.

## Guarded verbs

`rollout pause|resume|promote|rollback|repin`, `traffic drain|undrain`,
`migration start`, `instance release-held`, `runtime sync`, `scale`.

- They need the operator role on the cluster and are the oncall's call.
- `--dry-run=server` first; read the plan, then run with `--yes`. The CLI's
  own safety reads still apply and may refuse stale or conflicting state;
  report that refusal, do not retry around it.
- Pass the reason: `--reason "<who asked and why>"` where the verb accepts it.
- Never override identity or endpoint beyond the fleet kubeconfig and its
  context: no `--as`, `--token`, `--server`, `--insecure-skip-tls-verify`.

## Output

- `-o json` when you will parse; `-o wide` when a person will read it (some
  verbs have no `wide`; the README lists them).
- Quote the CLI output you relied on, trimmed to what supports the answer.
  Never paste tokens, kubeconfig bodies or bearer headers.
- `Unavailable` or `Unreadable` mean the CLI could not reach or read a
  source; that is a transport or permission fact, not a finding about the
  workload. Say which.
- `admin doctor` probes a fixed API list that includes the BenchmarkJob API,
  which these clusters do not install; that line is expected.
