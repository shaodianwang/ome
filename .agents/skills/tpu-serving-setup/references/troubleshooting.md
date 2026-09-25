# Troubleshooting by symptom

`k` is `kubectl --kubeconfig "$KC" --context "$CTX"`. Evidence goes to
`$RUN_DIR`; nothing here patches cluster services from a devserver.

## `create` is rejected by the webhook

The validating webhook runs on `create --dry-run=server` too, so you see this
before anything exists. Common causes: the runtime named in
`spec.runtime.name` does not exist in that namespace (namespaced
`ServingRuntime`) or at cluster scope (`ClusterServingRuntime`); a name that is
not a DNS label; `runtime.kind` not matching the object you created. Fix the
rendered file, re-run the dry run.

## ISVC exists, no InferenceReplicas, no pods

The object did not resolve to OMENative, or the controller is not watching the
namespace. Check `{.spec.deploymentMode}` (the field, not the annotation) and
the controller log for the name
(`k -n ome logs deploy/ome-controller-manager --tail=500 | grep "$NAME"`).

## Pods `Pending`

Read the pod events first (`references/inspect.md`, step 4):

- `Insufficient google.com/tpu` / no node matches - the pinned topology has
  no free slice. Count schedulable `2x2x2` nodes (SKILL, "Check capacity");
  a Ready node is not a free node. Wait, or choose the other cluster; do not
  change the scheduler to the default one.
- untolerated taint - the toleration for `google.com/tpu` was dropped.
- `PodGroup`/gang message from `ome-scheduler` - one of the two pods cannot be
  placed in the same slice; the gang waits as a unit. Same remedy as capacity.
- pod carries a **scheduling gate** (`GATES` column) - the namespace is Kueue
  managed and the pod names a queue without admitted quota. The TPU pair names
  no queue; if you added `ome.io/local-queue`, that leaf must have budget
  (`kubectl get aq`, `hack/quota-mc-demo/README.md`).
- `node affinity` mentioning `models.ome.io/clusterbasemodel.<name>` - the
  ISVC references a model on a cluster with no model agent
  (`references/fixtures.md`, "ClusterBaseModel"). Use the lean runtime-pinned
  ISVC.

## Leader stuck `Running` but never `Ready`; log has no JAX line

JAX is waiting for a peer. Compare the two pods' node partition ids; if they
differ, the gang split across slices - `engine.topologyKey` is missing or
wrong on the ISVC, or a role's `nodeSelector` was changed. Delete the ISVC and
recreate from a corrected render; the pods will not heal in place.

## Log shows `global=8` (or any value below 16) on a 2x2x2 gang

One host did not join the mesh: the worker crashed at init (check its log for
`TPU init FAILED`), or the two pods run different images. Both roles must run
the same image; the worker must stay up for the leader's mesh to survive.

## `ImagePullBackOff` / `ErrImagePull`

The cluster pulls only through the ECR mirror and the manifest still names
`docker.io`. Re-render through `mirror_images` (`.buildkite/eval_image_mirror.sh`)
and recreate. If the mirrored path also fails, the tag is not mirrored yet;
pick a tag that is (`crane ls <prod-ecr>/pt_dockerio/vllm/vllm-tpu | tail`).

## Pods ignore `delete` and stay `Terminating` for minutes

The container's PID 1 does not handle SIGTERM (a bare `sleep`, or a server
without a handler). The fixtures keep a shell with a `trap` as PID 1 for
exactly this; if you replaced `command`/`args`, restore that pattern. The pods
will go away when the grace period expires; do not `--force` them on a shared
cluster - a force-deleted pod's chips are released only when the kubelet
actually stops the container.

## Smoke test fails but the pod is Ready

- port-forward died: check `$RUN_DIR/port-forward.log`, restart it; the
  Service name equals the ISVC name.
- `404` for `/v1/chat/completions` - wrong port or a different runtime; the
  JAX fixture serves on 8080.
- `status.url` does not answer - the route belongs to the install's gateway
  configuration (`charts/ome-resources/values-usc1-1-tpu.yaml`,
  `ingressGateway.*`). Verify with the port-forward first; if only the route is
  wrong, that is an install/GitOps change, not something to patch on the
  `HTTPRoute`.

## The ISVC shows as OutOfSync or drifts after you edit it

It is GitOps-managed. Hand edits are reverted on the next sync and
`kubectl delete` of a managed object is undone the same way. Remove or change
it in the GitOps source instead; use your own names for anything you create by
hand.

## `forbidden`

Missing namespace admin action for that cluster. `msl-access-bootstrap`
(Step 1) shows how to verify and what to request. Never patch RBAC.
