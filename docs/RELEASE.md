# Releasing OME

How an OME version goes from a merged commit to a running deployment: CI builds
and publishes versioned images + Helm charts on every `main` commit, and a
release is a GitOps PR that pins a chosen (green) version.

> Scope: this guide documents the **CI artifact side** (images, charts, tags,
> registries) from what the pipeline actually does, plus the **shape** of the
> GitOps promotion step. The exact argo-gitops repo path, Argo CD app name, and
> pin field live outside this repo — the placeholders marked `<…>` below must be
> filled in from the internal deployment runbook. If you know them, replace the
> placeholders in this file in a follow-up PR.

## TL;DR

1. Pick a **green `main` build** in Buildkite (`buildkite.com/msl/ome`) — all of
   its `build-*` / `mirror-*` / `push-chart-*` steps passed.
2. Its version is the **8-char commit short-SHA** (e.g. `a1b2c3d4`). That short-SHA
   is the image tag; the matching chart version is `1.0.0-<UTCtimestamp>.<shortSHA>`.
3. Open a PR in the **argo-gitops** repo bumping the OME pin to that version.
4. Merge; Argo CD syncs the app to the new version.

Only `main` commits are promotable — PR/branch builds do **not** publish (see below).

## What CI produces

Every commit **on `main`** produces, all tagged with the same 8-char short-SHA
(`BUILDKITE_COMMIT` truncated to 8 chars):

### Images

Built by the `build-<name>` steps and mirrored to prod by `mirror-<name>`:

| Image | Dockerfile | ECR repo |
|---|---|---|
| `ome-manager` | `dockerfiles/manager.Dockerfile` | `msl_infra/ome-manager` |
| `ome-agent` | `dockerfiles/ome-agent.Dockerfile` | `msl_infra/ome-agent` |
| `muse-server` | `dockerfiles/muse-server.Dockerfile` | `msl_infra/muse-server` |
| `ome-scheduler` | `dockerfiles/ome-scheduler.Dockerfile` | `msl_infra/ome-scheduler` |
| `canary-metrics` | `dockerfiles/canary-metrics.Dockerfile` | `msl_infra/canary-metrics` |

- **Tags:** each image is pushed as `latest` **and** `<shortSHA>`
  (`tags: ["latest", "{COMMIT[:8]}"]`). Pin the `<shortSHA>` for a release —
  never `latest`.
- **Multi-arch:** `linux/amd64,linux/arm64`.
- `model-agent` is **not** built here; it comes from an upstream-published image.
- `kind-ci` is CI-only tooling, not a runtime image.

### Charts

Packaged and pushed by `.buildkite/push_chart.sh` (`push-chart-<name>` steps):
`ome-crd`, `ome-resources`, `ome-isvc`, `muse`, `ome-scheduler`.

- **Chart version:** `1.0.0-<UTCtimestamp>.<shortSHA>` (stamped into `Chart.yaml`).
- **Self-pinning:** at package time the `ome-resources`, `ome-scheduler`, and
  `muse` charts stamp their `values.yaml` so `global.hub` points at the **prod
  ECR** and the image `tag`/`version` is that same `<shortSHA>`. A published
  chart therefore already references its own matching images — GitOps needs no
  separate image override if it pins the chart version.
- Pushed to `oci://<ecr>/msl_infra/ome-charts` (both dev and prod ECR).

### Registries

| | AWS account | Registry |
|---|---|---|
| Dev ECR (build pushes here first) | `588845226011` | `588845226011.dkr.ecr.us-east-2.amazonaws.com` |
| Prod ECR (consumers pull here) | `168653207203` | `168653207203.dkr.ecr.us-east-2.amazonaws.com` |

Prod ECR is the cluster-foundation-managed registry; the `cf-replace-image-registry`
admission policy fast-paths images already on it, which is why charts default
their hub there. The `mirror-<name>` steps `crane copy` dev→prod (no rebuild).

Region: `us-east-2`.

## Why only green `main` builds are promotable

The pipeline enforces this structurally — you don't have to police it by hand:

- **Publish is `main`-only.** Image steps set `push-branches: "main"`; the mirror
  and chart-push steps skip on non-`main` branches (`--only-main` / a
  `BUILDKITE_BRANCH=main` guard in `push_chart.sh`). PR builds compile and
  validate but publish nothing.
- **Tests gate the build.** Each `build-<name>` step is `depends_on: "test"`, and
  each `push-chart-<name>` depends on its image steps. A red unit-test step means
  no image and no chart for that commit.

So a "good version" = the short-SHA of a `main` build whose `build-*`, `mirror-*`,
and `push-chart-*` steps are all green. (There is **no** in-repo canary or
stage→prod gate beyond this — any such gate lives in the GitOps/Argo layer.)

### Finding the short-SHA

```sh
# From a local checkout, for a chosen main commit:
git rev-parse --short=8 <commit>

# Confirm the image exists in prod ECR:
crane ls 168653207203.dkr.ecr.us-east-2.amazonaws.com/msl_infra/ome-manager | grep <shortSHA>
```

## Promoting via argo-gitops

A release is a PR in the argo-gitops repo that moves the OME pin to the chosen
version, followed by an Argo CD sync.

> **Fill in from the internal runbook** (unavailable at authoring time):
> - argo-gitops repo: `<repo / URL>`
> - path to the OME app manifest / values: `<path>`
> - Argo CD app name(s): `<app-name>` (and any per-environment apps)
> - the exact field to bump: the chart `targetRevision: "1.0.0-<ts>.<shortSHA>"`
>   **or** an image `tag: "<shortSHA>"` override
> - environment promotion order (e.g. staging → prod) and approvers

Typical shape once the specifics are known:

1. Branch the argo-gitops repo off its default branch.
2. Bump the OME pin to the chosen version:
   - if the app references the published chart, set
     `targetRevision: 1.0.0-<ts>.<shortSHA>`;
   - if it overrides an image tag directly, set `tag: <shortSHA>` (and ensure the
     hub is the prod ECR `168653207203.dkr.ecr.us-east-2.amazonaws.com/msl_infra`).
3. Open the PR; get the required review.
4. Merge. Argo CD reconciles the app to the new revision (auto-sync, or a manual
   sync if the app is not auto-synced).

## Verifying a release

```sh
# Rollout landed on the new image:
kubectl -n <ns> get deploy ome-controller-manager \
  -o jsonpath='{.spec.template.spec.containers[*].image}{"\n"}'

# Controller healthy:
kubectl -n <ns> rollout status deploy/ome-controller-manager
kubectl -n <ns> get pods -l control-plane=ome-controller-manager
```

Argo CD app should report `Synced` + `Healthy` on the new revision.

## Rolling back

Re-point the same argo-gitops pin to the **previous** green short-SHA / chart
version and merge. Because images are immutable per-SHA in ECR, a rollback is
just pinning the older tag — no rebuild.

## Reference

- Pipeline generator: `.buildkite/generate_pipeline.py` (image list, tags,
  `push-branches`, `depends_on`).
- Chart packaging + publish: `.buildkite/push_chart.sh`.
- Image mirror (dev→prod): `.buildkite/mirror_image.sh`.
- Buildkite org `msl`; pipeline `buildkite.com/msl/ome`; CI infra owned by
  `msl_dev_infra`.
</content>
</invoke>
