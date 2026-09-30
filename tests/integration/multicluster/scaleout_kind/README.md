# Multicluster KIND tests

The default suite runs two single-node members and a control-plane cluster.
The physical-fit shard uses the OME gang scheduler and two workers per member,
under separate cluster names ending in `-scheduler`.

Both members report four synthetic accelerator units. Each replica needs a
leader and worker requesting two units each. One member initially has its
workers in different topology domains. The test checks that Kueue admits both
gangs, placement assigns one replica per member, and the fragmented member
cannot bind even part of its gang. Joining its domains must restore readiness
without changing hardware totals or the placement plan.

This exercises real scheduling and kubelets with synthetic Node allocatable
resources; it does not exercise a GPU device plugin or model execution.

Run the physical-fit shard locally:

```sh
make dep-crds
make kind
(
  export KUBECONFIG="$(mktemp -t ome-gang-fit-XXXXXX.kubeconfig)"
  trap 'rm -f "$KUBECONFIG"' EXIT
  export PATH="$PWD/bin:$PATH"
  export KIND_NODE_IMAGE="$(cut -d@ -f1 .kind-node-image)"
  export KIND_KEEP_CLUSTER=true
  export OME_TEST_SCHEDULER=ome-scheduler
  export OME_TEST_SCHEDULER_WORKERS=2
  go test ./tests/integration/multicluster/scaleout_kind -count=1 -v -timeout=30m \
    -ginkgo.focus-file='/scheduling_kind_test.go$'
)
```

The local helper builds `ome-scheduler:kind-local` when the tag is absent.
Remove that cached image after editing scheduler source, or set
`OME_TEST_SCHEDULER_IMAGE` to a fresh local tag. Use `KIND_RECREATE=true` when
changing the worker count or node image of retained test clusters.

CI runs the physical-fit shard on PRs and `main`. Its bundle contains a static
scheduler binary built from the tested commit, with a verified checksum. The
runner packages it into a scratch image without network access. The ordinary
placement and quota shards keep their default scheduler and single-node members.
