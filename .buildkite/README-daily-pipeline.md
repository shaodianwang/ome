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
