---
name: tpu-serving-setup
description: Bring up, inspect, smoke-test and remove an OME-managed serving deployment on the GKE tpu7x clusters using OME's own objects (ClusterBaseModel, ClusterServingRuntime/ServingRuntime, InferenceService), from a devserver with an explicit kubeconfig. Use when the user says "deploy on the TPU cluster", "bring up an InferenceService on tpu7x", "TPU smoke test", "why is my TPU ISVC not Ready", "tear down the TPU serving", or wants to prove the TPU gang path works before a larger rollout.
---

# TPU serving on GKE tpu7x with OME

A serving deployment here is one runtime object plus one `InferenceService`.
OME renders the pods (leader + worker for a multi-host slice), the per-ISVC
`Service`, and - where the install has Gateway API on - the `HTTPRoute`s. You
create and delete the OME objects; you never create pods, Services, PodGroups
or routes by hand, and never a raw LeaderWorkerSet.

The proven, in-tree pair for real silicon is
`hack/quota-mc-demo/fixtures/40-runtime-tpu-jax.yaml` +
`41-isvc-tpu-jax.yaml`: JAX on a 2x2x2 slice (1 leader + 1 worker = 2 pods =
8 chips) behind an OpenAI-shaped liveness API on port 8080. It serves no
weights; it proves scheduling, the TPU mesh, probes, Service and teardown. The
design notes in `hack/quota-mc-demo/README.md` ("Optional - a runtime that
actually uses the TPU", "Design notes") are the authority on why each field is
there.

| Reference | Load when |
|---|---|
| `references/fixtures.md` | you are editing the fixtures (names, namespace, image mirror, chips per pod, single-pod variant, adding a ClusterBaseModel) |
| `references/inspect.md` | the ISVC is not Ready and you need the read-order: ISVC conditions, InferenceReplicas, pods, events, leader log, node/slice state |
| `references/troubleshooting.md` | you have a symptom (Pending, JAX init hang, `global=8`, ImagePullBackOff, pods ignoring delete, scheduling gates) |

## Prerequisites

- Cluster access from the `msl-access-bootstrap` skill: `$KC`/`$CTX` for the
  target cluster and `auth can-i create inferenceservices.ome.io` = `yes` in
  your namespace.
- OME installed there (`kubectl ... api-resources --api-group=ome.io`) and the
  gang scheduler the fixtures name (`schedulerName: ome-scheduler`) running:
  `kubectl --kubeconfig "$KC" --context "$CTX" get deploy -A | grep -i ome-scheduler`.
- `jq`, `curl`. Scratch under `~/.cache/ome-agent.noindex/scratch/tpu-serving/`,
  never under the repository.

## Choose the cluster and namespace

| Cluster | Context (`$CTX`) | Namespace | Notes |
|---|---|---|---|
| `msl-gcp-usc1-1-tpu` | `msl-gcp-1/us-central1/msl-gcp-usc1-1-tpu` | `sandbox` (also `staging`, `prod`) | OME installed from `charts/ome-resources/values-usc1-1-tpu.yaml`; Gateway API on, per-ISVC subdomains, model agent **off** |
| `msl-gcp-usc1-rc-1-tpu` | `msl-gcp-rc/us-central1/msl-gcp-usc1-rc-1-tpu` | `def-sandbox` | shared with CI validation; capacity is usually booked |

Use a namespace you hold the admin action for, and a name that is yours and
DNS-safe (the ISVC name becomes the Service name and part of the hostname):

```sh
set -euo pipefail
export KC="$HOME/.kube/msl-gcp-usc1-1.conf"
export CTX=msl-gcp-1/us-central1/msl-gcp-usc1-1-tpu
export NS=sandbox
export NAME="$(id -un | tr '[:upper:]' '[:lower:]' | tr -c 'a-z0-9\n' '-')-tpu-jax"
export RUN_DIR="$HOME/.cache/ome-agent.noindex/scratch/tpu-serving/$NAME"
k() { kubectl --kubeconfig "$KC" --context "$CTX" "$@"; }
mkdir -p "$RUN_DIR"
k -n "$NS" auth can-i create inferenceservices.ome.io
```

## Check capacity before creating anything

The fixtures pin `cloud.google.com/gke-tpu-topology: 2x2x2`. Count schedulable
nodes of that shape (from the comment block in
`hack/quota-mc-demo/fixtures/01-runtime.yaml`); two free hosts in one slice
are needed per replica:

```sh
k get nodes -l cloud.google.com/gke-tpu-topology=2x2x2 -o json \
  | jq '[.items[] | select((.spec.unschedulable // false) == false)] | length'
k get nodes -l cloud.google.com/gke-tpu-accelerator=tpu7x \
  -L cloud.google.com/gke-tpu-topology --no-headers | awk '{print $NF}' | sort | uniq -c
```

A Ready count is not a free count (the scheduler can also claim an unbound
slice); a gang that stays `Pending` with `Insufficient google.com/tpu` in its
events is the real signal. Do not fall back to the default scheduler.

## Render the objects (edit the fixtures, do not write new ones)

Only three edits are needed for your own copy: the runtime name, the ISVC name
plus its `spec.runtime.name`, and the namespace. Everything else in the pair
(node selector, toleration, `schedulerName`, `/dev/shm`, probe, chips per pod,
`topologyKey`, slice annotation) is load-bearing - see `references/fixtures.md`
before touching it.

```sh
F=hack/quota-mc-demo/fixtures
sed -e "s/^  name: mc-demo-tpu-jax$/  name: $NAME/" \
    -e "s/^  namespace: ome-mc-test$/  namespace: $NS/" \
    "$F/40-runtime-tpu-jax.yaml" > "$RUN_DIR/runtime.yaml"
sed -e "s/^  name: mc-demo-jax$/  name: $NAME/" \
    -e "s/^  namespace: ome-mc-test$/  namespace: $NS/" \
    -e "s/^    name: mc-demo-tpu-jax$/    name: $NAME/" \
    "$F/41-isvc-tpu-jax.yaml" > "$RUN_DIR/isvc.yaml"
grep -nE '^  (name|namespace):|^    name:' "$RUN_DIR"/runtime.yaml "$RUN_DIR"/isvc.yaml
```

If the cluster pulls only through the ECR mirror, rewrite the Docker Hub image
on the way in - the CI tree keeps the mirror paths:

```sh
source .buildkite/eval_image_mirror.sh          # AWS_REGION selects the replica
mirror_images < "$RUN_DIR/runtime.yaml" > "$RUN_DIR/runtime.mirrored.yaml"
mv "$RUN_DIR/runtime.mirrored.yaml" "$RUN_DIR/runtime.yaml"
```

Preflight with a server-side dry run (admission webhooks run, nothing is
created), then create the passive runtime first and the ISVC last - the ISVC is
what starts the TPU allocation. Use `create`, not `apply`, so an existing
object with the same name is an error rather than a silent mutation:

```sh
k -n "$NS" create --dry-run=server -f "$RUN_DIR/runtime.yaml" -f "$RUN_DIR/isvc.yaml" -o name
k -n "$NS" create -f "$RUN_DIR/runtime.yaml" -o json > "$RUN_DIR/runtime.created.json"
k -n "$NS" create -f "$RUN_DIR/isvc.yaml"    -o json > "$RUN_DIR/isvc.created.json"
jq -r '.metadata | "\(.name) \(.uid)"' "$RUN_DIR"/*.created.json     # keep: teardown re-checks these
```

## Watch it come up

```sh
k -n "$NS" get isvc "$NAME" -o wide                       # URL, READY, RUNTIME, PLACEMENT columns
k -n "$NS" get inferencereplicas -o wide | grep -E "NAME|^$NAME"
k -n "$NS" get pods -l "ome.io/inferenceservice=$NAME" -o wide -w
```

Expect two pods on two different nodes that share one
`cloud.google.com/gke-tpu-partition-2x2x2-id` value. The leader's readiness
probe gates on the TPU coming up, so `Ready` means the mesh formed:

```sh
k -n "$NS" wait --for=condition=Ready "isvc/$NAME" --timeout=20m
k -n "$NS" wait --for=condition=Ready pod -l "ome.io/inferenceservice=$NAME" --timeout=20m
k -n "$NS" logs -l "ome.io/inferenceservice=$NAME" -c ome-container --prefix --tail=20 | grep -E 'JAX .*global='
```

`global=16 local=8` is the healthy reading for a 2x2x2 gang (cores, not chips:
two cores per chip, 8 chips, 2 hosts x 4). `global=8` means a host did not
join. `scripts/isvc-status.sh "$KC" "$CTX" "$NS" "$NAME"` prints the whole
read-order from `references/inspect.md` on one screen.

## Smoke test through a port-forward

The ISVC's Service carries the ISVC name. Never measure performance through a
port-forward; it is a correctness probe only.

```sh
k -n "$NS" port-forward "svc/$NAME" 8080:8080 >"$RUN_DIR/port-forward.log" 2>&1 &
PF=$!; sleep 3
curl -s localhost:8080/health | jq .
curl -s localhost:8080/v1/models | jq -r '.data[].id'
curl -s localhost:8080/v1/chat/completions -H 'content-type: application/json' \
  -d '{"model":"tpu-jax-echo","messages":[{"role":"user","content":"hi"}]}' \
  | jq -r '.choices[0].message.content'
kill "$PF"
```

`status.url` on the ISVC is the routed hostname when the install has Gateway
API enabled (`k -n "$NS" get isvc "$NAME" -o jsonpath='{.status.url}'`). OME
owns those `HTTPRoute`s; if the route is wrong, fix the ISVC or the install
values, never the route object.

## Tear down

Delete the ISVC first and wait for its pods to release the chips; then the
runtime. Re-read each object by exact name before deleting - never a broad
label selector, never `--all` in a shared namespace, never a GitOps-managed
object (those come back on the next sync and must be removed from git).

```sh
k -n "$NS" get "isvc/$NAME" -o jsonpath='{.metadata.uid}{"\n"}'     # compare with isvc.created.json
k -n "$NS" delete "isvc/$NAME" --wait=true --timeout=10m
until [ -z "$(k -n "$NS" get pods -l "ome.io/inferenceservice=$NAME" -o name)" ]; do sleep 5; done
k -n "$NS" delete "servingruntime/$NAME"
k -n "$NS" get "isvc/$NAME" "servingruntime/$NAME" "svc/$NAME" --ignore-not-found   # all three gone
k -n "$NS" get pods -l "ome.io/inferenceservice=$NAME"                                # "No resources found"
```

Pods that stay `Terminating` for the whole grace period are holding chips; the
fixture containers trap `TERM` precisely so this does not happen - if you
replaced the command, see `references/troubleshooting.md`.

## Rules that keep this safe

- Explicit `--kubeconfig`/`--context` on every command; never `use-context`.
- One owner per object: create with `create`, delete by exact name, keep the
  recorded UIDs in `$RUN_DIR`.
- Do not patch CRDs, restart the controller or scheduler, or edit
  GitOps-managed runtimes/ISVCs from a devserver; open a PR instead.
- Do not switch the ISVC to `spec.model` on these clusters unless a model agent
  labels nodes there (`references/fixtures.md`, "ClusterBaseModel").

## Not covered

- Serving real weights on tpu7x (production TPU image, gcsfuse-mounted
  checkpoint bucket, cache tarball, `tensor-parallel-size`): no in-tree
  sample; those runtimes live in the GitOps repository and are deployed by it.
- Multi-cluster placement and quota (`spec.placement`, `AcceleratorQuota`,
  Kueue): `hack/quota-mc-demo/README.md` is the runbook.
- Autoscaling, canary rollouts and PD-disaggregated topologies on TPU.
