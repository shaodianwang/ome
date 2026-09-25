# Registries: images and charts

Source of truth: `docs/RELEASE.md` ("Registries") and the CI scripts named
below. Region is `us-east-2` for both accounts.

| Registry | Account | Role in the flow |
|---|---|---|
| dev ECR `588845226011.dkr.ecr.us-east-2.amazonaws.com` | `588845226011` | every `main` build pushes images and charts here first |
| prod ECR `168653207203.dkr.ecr.us-east-2.amazonaws.com` | `168653207203` | the `mirror-<name>` steps `crane copy` dev to prod; charts self-pin `global.hub` to this registry; clusters pull from it; Docker Hub is mirrored under `pt_dockerio/<namespace>/<repository>` |

Repositories you will touch:

| Path | Content |
|---|---|
| `msl_infra/ome-manager`, `msl_infra/ome-agent`, `msl_infra/ome-scheduler`, `msl_infra/ome-quota-manager` | operator images, tagged with the 8-character short SHA of the `main` commit |
| `msl_infra/kind-ci` | the offline toolchain image the KIND CI lane and the devbox skills run |
| `msl_infra/canary-metrics` | eval-harness metrics image |
| `msl_infra/ome-charts/<chart>` (OCI) | Helm charts, version `1.0.0-<UTCtimestamp>.<shortSHA>` |
| `pt_dockerio/library/docker:28-dind`, `pt_dockerio/library/busybox`, `pt_dockerio/vllm/vllm-tpu` | Docker Hub mirrors used by CI, the devbox sidecar and the TPU fixtures |

## Logging in from a devserver

`cloud ecr get-credentials --account <acct> --region us-east-2 [--role
SSOContainerRegistryReadWrite]` prints a short-lived password for user `AWS`.
Pipe it, never paste it:

```sh
tok() { cloud ecr get-credentials --account "$1" --region us-east-2 --role SSOContainerRegistryReadWrite; }
tok 168653207203 | crane auth login --username AWS --password-stdin 168653207203.dkr.ecr.us-east-2.amazonaws.com
tok 168653207203 | docker login     --username AWS --password-stdin 168653207203.dkr.ecr.us-east-2.amazonaws.com
tok 168653207203 | helm registry login --username AWS --password-stdin 168653207203.dkr.ecr.us-east-2.amazonaws.com
```

Pull-only access needs no `--role`. The token lasts hours; a `401` after a
working session means re-run the login. `skopeo login --authfile <tmp>` is
the equivalent for `skopeo inspect --raw docker://<image>` digest lookups.

## What CI does instead

`.buildkite/_ecr_login.sh` provides `ecr_login_for <dev|prod> <login-cmd>`
with three tiers: the in-image `cloud_oss` credential helper (dev uses the
`--dev` shortcut; prod is reached cross-account with
`--account 652086758864 --role SSOContainerRegistryReadWrite`), then the
pipeline secrets `ECR_AUTH_TOKEN` / `PROD_ECR_AUTH_TOKEN`, then
`aws ecr get-login-password`. `push_chart.sh` and `push_release_marker.sh`
use it for `helm registry login` + `helm push`. From a devserver the human
`cloud ecr get-credentials` path above is what works; if a direct prod-ECR
login with your identity is refused, the cross-account form CI uses is the
next thing to try.

## Verifying without pulling

```sh
crane ls 168653207203.dkr.ecr.us-east-2.amazonaws.com/msl_infra/ome-manager | grep <shortSHA>
crane digest 168653207203.dkr.ecr.us-east-2.amazonaws.com/msl_infra/kind-ci:latest
helm show chart oci://168653207203.dkr.ecr.us-east-2.amazonaws.com/msl_infra/ome-charts/ome-resources --version <v>
```

`crane` is not a Makefile tool target; it is on the `kind-ci` image and is a
single static binary to install on a devserver. `helm` and `yq` are
(`make helm`, `make yq`, installed under `bin/`).

## Charts from the tree instead

`make deploy-helm` installs `charts/ome-crd` and `charts/ome-resources` from
the checkout with `helm install` (no registry login) against whatever
`KUBECONFIG` names - so set it to the intended real-cluster file for that one
command, and never to a file a `kind` run may later inherit:

```sh
KUBECONFIG="$KC" make deploy-helm
```

Per-cluster overlays (`charts/ome-resources/values-usc1-*.yaml`) are what the
GitOps deployment feeds in; a hand install with a different values file will
drift from it.
