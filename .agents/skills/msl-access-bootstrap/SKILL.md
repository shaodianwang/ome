---
name: msl-access-bootstrap
description: Re-establish cluster, registry and chart access at the start of a session for OME work - kubeconfigs for the GKE tpu7x clusters and the CoreWeave/MKS dev clusters via the `cloud` CLI, ECR login for image pulls and `crane`, and Helm OCI login for the ome-charts registry. Use when kubectl, helm, crane or docker fail with auth or permission errors, when starting on a fresh devserver, or when the user says "get access", "regenerate the kubeconfig", "ECR login", "log in to the chart registry", "can't reach the TPU cluster".
---

# Access bootstrap for OME clusters and registries

One pass that materialises credentials you already hold: a kubeconfig per
cluster, a short-lived ECR token for image pulls and `crane`, and a Helm OCI
login for the chart registry. It uses your existing identity through the
`cloud` CLI and never grants anything - a `forbidden` means a missing
entitlement (see Troubleshooting); request it, do not work around it.

| Reference | Load when |
|---|---|
| `references/clusters.md` | you need a cluster's `cloud` target, kubeconfig path, context name, namespaces, or which OME install runs there |
| `references/registries.md` | you need the ECR accounts, the chart repository layout, or how CI performs the same logins (`.buildkite/_ecr_login.sh`) |

## Prerequisites

- **A devserver.** `cloud k8s generate-kubeconfig` does not issue kubeconfigs
  to laptops; generate there and copy the file over if you must work locally.
- `cloud`, `kubectl`, `helm`, `crane` (or `skopeo`), `jq`, and `docker` or
  `podman` on `PATH`. `make helm` and `make yq` install pinned copies under
  `bin/`; `crane` is a single static binary you install yourself. The
  `kind-ci` image (`dockerfiles/kind-ci.Dockerfile`) already carries all of them.
- Entitlements, held before you start: the per-cluster `K8S_CLUSTER` admin
  action for the namespace you deploy into, and the
  `SSOContainerRegistryReadWrite` role for ECR push (pull-only needs no role).

## Kubeconfig hygiene (binding)

Every kubeconfig this skill writes is a **real-cluster** file. Select it per
command - `kubectl --kubeconfig "$KC" --context "$CTX" ...` - and never run
`kubectl config use-context` / `set-context` against it. Never export
`KUBECONFIG` to one of these files in a shell that may run `kind`, `kwokctl`,
`make test-kind*` or `hack/kwok-sim/fleet-up.sh`: those merge a `kind-*`
context into whatever `KUBECONFIG` names and flip `current-context` to it.
Give ephemeral clusters their own temp file (`AGENTS.md`, Kubeconfig Hygiene).

## Contract (edit, then run the steps in one shell)

```sh
set -euo pipefail
# --- cluster (pick a row from references/clusters.md) ---
export CLOUD_CLUSTER=msl-gcp-1/msl-gcp-usc1-1-tpu          # `cloud k8s generate-kubeconfig` target
export KC="$HOME/.kube/msl-gcp-usc1-1.conf"                 # one file per cluster; never ~/.kube/config
export CTX=msl-gcp-1/us-central1/msl-gcp-usc1-1-tpu         # read it back with `kubectl config get-contexts`
export NS=sandbox                                          # namespace you hold the admin action for
# --- registries (docs/RELEASE.md "Registries") ---
export ECR_REGION=us-east-2
export DEV_ECR_ACCOUNT=588845226011                        # builds push here first
export PROD_ECR_ACCOUNT=168653207203                       # clusters and charts pull from here
export ECR_ROLE=SSOContainerRegistryReadWrite              # omit --role below for pull-only
export PROD_ECR="$PROD_ECR_ACCOUNT.dkr.ecr.$ECR_REGION.amazonaws.com"
export DEV_ECR="$DEV_ECR_ACCOUNT.dkr.ecr.$ECR_REGION.amazonaws.com"
export CHART_VERSION=1.0.0-<UTCtimestamp>.<shortSHA>            # a green main build; docs/RELEASE.md
```

## Step 1 - kubeconfig per cluster, then verify

```sh
mkdir -p "$(dirname "$KC")"
cloud k8s generate-kubeconfig "$CLOUD_CLUSTER" --path "$KC"
kubectl --kubeconfig "$KC" config get-contexts -o name          # confirm $CTX is the printed name
kubectl --kubeconfig "$KC" --context "$CTX" auth whoami -o json \
  | jq -r '.status.userInfo | .username, (.groups | join(","))'
# authorisation gates - each MUST print "yes" before deploying anything:
kubectl --kubeconfig "$KC" --context "$CTX" -n "$NS" auth can-i create pods
kubectl --kubeconfig "$KC" --context "$CTX" -n "$NS" auth can-i create inferenceservices.ome.io
# OME is installed if its API group answers:
kubectl --kubeconfig "$KC" --context "$CTX" api-resources --api-group=ome.io -o name
```

Repeat with another row of the contract for each cluster you need in the
session (the TPU clusters, a CoreWeave dev cluster for the OMENative harness,
an MKS cluster for a devbox). Regenerating over an existing file is safe. To
discover a target: `cloud k8s list cw` and `cloud k8s list mks` list the
CoreWeave and MKS clusters; the two GKE targets are fixed and listed in
`references/clusters.md`.

## Step 2 - ECR login (pulls, `crane`, pushes)

`cloud ecr get-credentials` prints a short-lived token; pipe it into whichever
client you use. Prod ECR is what the charts and clusters reference; dev ECR is
where CI builds land first (`docs/RELEASE.md`).

```sh
ecr_token() {  # $1 = account
  cloud ecr get-credentials --account "$1" --region "$ECR_REGION" --role "$ECR_ROLE"
}
ecr_token "$PROD_ECR_ACCOUNT" | crane auth login --username AWS --password-stdin "$PROD_ECR"
ecr_token "$PROD_ECR_ACCOUNT" | docker login --username AWS --password-stdin "$PROD_ECR"   # or podman
# read check without pulling an image (the manager image every install runs):
crane ls "$PROD_ECR/msl_infra/ome-manager" | tail -3
```

Do the same with `$DEV_ECR_ACCOUNT` / `$DEV_ECR` when you need a PR build's
image. Tokens expire in hours: a `401`/`403` from a pull or push that worked
earlier means re-run this step, not a lost grant.

## Step 3 - chart registry (Helm OCI)

Charts are published to `oci://<ecr>/msl_infra/ome-charts/<chart>` on both
registries by `.buildkite/push_chart.sh`; the same ECR token logs Helm in.

```sh
ecr_token "$PROD_ECR_ACCOUNT" | helm registry login --username AWS --password-stdin "$PROD_ECR"
tmp="$(mktemp -d "${TMPDIR:-/tmp}/ome-charts.XXXXXX")"
helm pull "oci://$PROD_ECR/msl_infra/ome-charts/ome-resources" --version "$CHART_VERSION" -d "$tmp"
helm show chart "$tmp"/ome-resources-*.tgz | head            # version = 1.0.0-<UTCtimestamp>.<shortSHA>
```

`CHART_VERSION` comes from a green `main` build (`docs/RELEASE.md`, "Finding
the short-SHA"). A local install from the checkout does not need this login:
`make deploy-helm` installs `charts/ome-crd` and `charts/ome-resources` from
the tree.

## Step 4 - buckets on the GKE clusters

Model weights on the TPU clusters reach pods through the gcsfuse CSI driver and
GKE Workload Identity, not through a devserver credential. The runtime's pod
spec names a per-cluster credential ConfigMap; confirm the namespace has one
before deploying a runtime that mounts a bucket:

```sh
kubectl --kubeconfig "$KC" --context "$CTX" -n "$NS" get configmap | grep -i gcs
```

Read-only is the default and correct for serving; a writer ConfigMap exists only
where a shared cache is seeded. There is nothing to log in to here.

## Step 5 - one-shot verification

```sh
echo "== identity =="; kubectl --kubeconfig "$KC" --context "$CTX" auth whoami -o json | jq -r .status.userInfo.username
echo "== cluster  =="; kubectl --kubeconfig "$KC" --context "$CTX" -n "$NS" auth can-i create inferenceservices.ome.io
echo "== ome      =="; kubectl --kubeconfig "$KC" --context "$CTX" -n ome get deploy ome-controller-manager -o jsonpath='{.status.readyReplicas}/{.status.replicas}{"\n"}'
echo "== ecr      =="; ecr_token "$PROD_ECR_ACCOUNT" >/dev/null && echo token OK
echo "== charts   =="; helm registry login --help >/dev/null && crane ls "$PROD_ECR/msl_infra/ome-charts/ome-resources" | tail -1
```

## Troubleshooting

- **`can-i ... = no` / `forbidden`** - the namespace admin action is missing
  for that cluster. Check your groups with `auth whoami`; request the grant.
  Never patch RBAC from a devserver.
- **`cloud: command not found`** - not on a devserver, or the tool is not on
  `PATH`; nothing here works without it.
- **context name not what you expected** - read it back with
  `kubectl --kubeconfig "$KC" config get-contexts -o name` and fix `CTX`; do
  not rename or switch contexts in the file.
- **a `kind-*` context appeared in a real kubeconfig** - a kind run inherited
  it. Delete the stale entries from that file only
  (`kubectl --kubeconfig "$KC" config delete-context kind-<name>` is scoped to
  `$KC`) and give the next kind run a temp file.
- **`crane ls` 401 after a successful login** - the token belonged to the
  other account; log in to the registry whose hostname you are listing.
- **image pull denied inside the cluster** - not a devserver problem. The
  install overlay has `global.imagePullSecrets` for a namespace that cannot pull
  prod ECR with node credentials (`charts/ome-resources/values-usc1-1-tpu.yaml`).

## Not covered

- Working from a laptop without a devserver (needs an SSH tunnel and a
  `proxy-url` in the kubeconfig; not reproducible from repo files).
- Registering clusters with the GitOps controller, or `gcloud`/`gsutil` bucket
  access from a devserver - neither is part of an OME deploy loop.
- Buildkite tokens: see the `fix-ci` skill.
