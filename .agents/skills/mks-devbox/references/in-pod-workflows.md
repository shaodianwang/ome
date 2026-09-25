# Working inside the devbox

`k` is `kubectl --kubeconfig "$KC" --context "$CTX"`; `$DEVBOX`, `$NS` come
from the contract in `SKILL.md`. Inside the pod the tree lives at
`/work/ome` (the `work` emptyDir).

## Getting code in and out

```sh
# devserver -> pod (re-run to sync edits; build outputs excluded)
REPO=/path/to/your/ome/checkout
tar -C "$REPO" --exclude=./bin --exclude=./pkg/xet/target --exclude=./muse/target -cf - . \
  | k -n "$NS" exec -i "$DEVBOX" -c dev -- sh -c 'mkdir -p /work/ome && tar -xf - -C /work/ome'

# pod -> devserver (a patch, logs, an artifact)
k -n "$NS" exec "$DEVBOX" -c dev -- sh -c 'cd /work/ome && git diff' > "$RUN_DIR/pod.diff"
k -n "$NS" cp "$NS/$DEVBOX:/work/ome/artifact.tgz" "$RUN_DIR/artifact.tgz" -c dev
```

Cloning inside the pod works through the proxy with your own credentials
(an SSH agent forwarded over `kubectl exec` is not available; use a token you
type once, never one written into the pod spec or an image).

## Build and unit tests

The `kind-ci` image bakes the Go 1.26 and Rust toolchains, a pre-populated Go
module cache and cargo cache, so these run without egress:

```sh
cd /work/ome
make ome-manager            # builds pkg/xet (Rust) first
make ome-agent
make test-no-xet            # unit suite minus the xet-linked packages
make test                   # full unit suite (xet-linked packages need LD_LIBRARY_PATH; the Makefile sets it)
make fmt vet ci-lint        # the pre-push gates from AGENTS.md
```

## envtest suites

```sh
export KUBEBUILDER_ASSETS="$(bin/setup-envtest use "$(cat .envtest-k8s-version)" -p path)"
make integration-test
```

`setup-envtest` downloads the control-plane binaries on first use, which needs
the proxy. If the download is blocked in your namespace, run the envtest
suites on the devserver instead; they need no Docker.

## KIND inside the pod (needs `--dind`)

The sidecar exposes Docker at `DOCKER_HOST=tcp://localhost:2375` (already in
the dev container's environment). The sequence is the CI script's
(`.buildkite/run_kind_tests_gke.sh`), minus the test-bundle handling:

```sh
cd /work/ome
until docker info >/dev/null 2>&1; do sleep 2; done          # the sidecar starts in parallel
for t in kindest-node fake-serving-build fake-serving-base busybox kueue; do
  [ -f "/images/$t.tar" ] && docker load -i "/images/$t.tar"   # baked tarballs: no registry pulls
done
docker run --rm --privileged busybox sh -c \
  'sysctl -w fs.inotify.max_user_watches=1048576 fs.inotify.max_user_instances=8192' || true
export KIND_NODE_IMAGE="$(tr -d '[:space:]' < .kind-node-image | cut -d@ -f1)"   # tag-only: a digest ref would try a registry
export DOCKER_BUILDKIT=0                                     # legacy builder uses the loaded golang/distroless bases
export KUBECONFIG="$(mktemp -t ome-kind-XXXXXX.kubeconfig)"  # KIND writes here and nowhere else
make test-kind                                               # or: GINKGO_FOCUS='regex' make test-kind
make test-kind-raw                                           # RawDeployment suite
KIND_RECREATE=true make test-kind                            # after suspected dirty state
make test-kind-clean; rm -f "$KUBECONFIG"
```

`KIND_KEEP_CLUSTER=true` is the default, so re-runs reuse the cluster. Each
suite runs a three-node KIND cluster; the sidecar defaults (`--dind-cpu 2`,
`--dind-memory 8Gi`, `--dind-storage 40Gi`) match what one CI job gets - raise
them for parallel suites.

## Running the manager from source against an in-pod cluster

Use the perm harness bring-up target, which creates the KIND cluster, installs
the CRDs and fake-serving images and then expects an external manager:

```sh
export KUBECONFIG="$(mktemp -t ome-kind-XXXXXX.kubeconfig)"
make test-kind-perm-up            # cluster + CRDs + images, no specs
make run-ome-manager              # runs cmd/manager against $KUBECONFIG (foreground)
# second shell:  make test-kind-perm
```

`make run-ome-manager` honours `KUBECONFIG`, which is why it must be the temp
file: never copy a real-cluster kubeconfig into the pod to point the manager
at a shared cluster. For local-manager debugging against a cluster that already
runs OME, `make delete-webhooks` removes the webhook configurations that would
otherwise route admission to the in-cluster manager (`AGENTS.md`, Deployment).

## Leaving

```sh
make test-kind-clean; rm -f "$KUBECONFIG"       # inside the pod, if KIND ran
exit
k -n "$NS" delete pod "$DEVBOX" --wait=true     # from the devserver; the emptyDirs go with it
```
