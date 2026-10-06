# Disabled GitHub Actions Workflows

The workflow files in this directory are **disabled** in this fork.

GitHub Actions only discovers workflow files under `.github/workflows/`. By
keeping them here under `.github/workflows-disabled/` instead, none of these
workflows will run on pushes, pull requests, schedules, or any other event in
this repository, while the files remain intact for reference.

## Why are they disabled?

This is a private fork of [moirai-internal/ome](https://github.com/moirai-internal/ome).
CI for this fork runs via **BuildKite** — see the pipeline definitions under
[`.buildkite/`](../../.buildkite/) at the repo root.

These GitHub Actions workflows reference resources that do not apply to
this fork, including:

- Container registries under `ghcr.io/moirai-internal/...`
- Secrets and OIDC trust relationships scoped to another organization
- Release/publishing paths (PyPI, GHCR, pages) tied to other identities
- Label and dependabot targets pointing at other repositories

Running these as-is would either fail noisily or, worse, attempt to publish
artifacts under the wrong identity.

## Re-enabling a workflow

If you want to re-enable a specific workflow in this fork:

1. Move the file back into `.github/workflows/`:
   ```sh
   git mv .github/workflows-disabled/<file>.yml .github/workflows/<file>.yml
   ```
2. Audit the file end-to-end and update everything that assumes upstream infra:
   - Registry paths (`ghcr.io/moirai-internal/...` → mslsrc-owned destination)
   - Action permissions and OIDC `id-token` audiences
   - `secrets.*` references — make sure each secret exists in this repo
     (Settings → Secrets and variables → Actions)
   - Repo / org references in `repository_dispatch`, label sync, dependabot
     sync, etc.
   - Branch and tag triggers (`on.push.branches`, `on.pull_request.branches`)
3. Confirm the workflow does not duplicate or conflict with the corresponding
   BuildKite pipeline.
4. Open a PR so the change can be reviewed before it starts running.

## Re-enabling everything at once

Don't. Re-enable workflows individually after auditing each one — bulk
re-enabling will almost certainly break, leak, or double-publish something.
