# Daily RC promotion

Each day, Buildkite reads the OME chart and image versions currently deployed
in RC and tests that exact release. It deploys a CPU InferenceService and two
two-node `2x2x2` TPU InferenceServices concurrently: one engine gang uses the
default scheduler and the other uses `ome-scheduler`. Both routers remain on
the default scheduler.

If all three smoke tests pass and the RC release does not change during the
run, the pipeline opens a reviewable `argo-gitops` promotion PR, such as
[argo-gitops#1064](https://github.com/mslsrc/argo-gitops/pull/1064), which pins
the tested versions in production. The PR includes the OME commits since the
currently pinned production controller release. The pipeline does not merge
the PR.

## Kargo-triggered smoke run

Kargo can reuse the three smoke-test jobs without running registry discovery or
opening a GitOps PR. Trigger the daily pipeline at the exact OME commit and set
`KARGO_BUILD=true`; Buildkite supplies that revision as `BUILDKITE_COMMIT`.
The generated pipeline validates the commit, exports its first eight characters
as `KARGO_BUILD_COMMIT`, and starts all three smoke tests immediately.

The smoke tests still verify that RC is running the requested `ome-manager`,
`ome-agent`, and `ome-scheduler` images. They also require the deployed
`ome-resources` and `ome-scheduler` chart versions to embed the same revision.
This mode is test-only: Kargo owns promotion, so the Buildkite GitOps PR step is
not emitted.
