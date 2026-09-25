# What `scripts/render_devbox_pod.py` emits

A single `Pod` (JSON on stdout; `kubectl` accepts JSON wherever it accepts
YAML). The shape is the GKE KIND CI job's pod-spec patch
(`.buildkite/README-gke-kind.md`) reduced to one user: a toolchain container
that idles, and an optional privileged Docker daemon beside it.

```
Pod  <name>  ns <namespace>   labels app=devbox user=<owner> [+ --label]   [annotations from --annotation]
  restartPolicy Never, terminationGracePeriodSeconds 30 (--grace-period)
  [serviceAccountName --service-account]  [nodeSelector --node-selector]  [tolerations --toleration]  [imagePullSecrets --image-pull-secret]
  volumes: work (emptyDir)  [+ docker-graph (emptyDir) with --dind]
  containers:
    dev   image --image            command: sh -c "trap 'exit 0' TERM INT; while :; do sleep 3600 & wait $!; done"
          workingDir --workdir (/work), stdin+tty
          env: HTTP(S)_PROXY/NO_PROXY (with --proxy), DOCKER_HOST=tcp://localhost:2375 (with --dind), --env K=V
          resources: requests=limits cpu/memory/ephemeral-storage (--cpu --memory --ephemeral-storage) [+ <gpu-resource>: N]
          mounts: work at --workdir
    dind  (only with --dind) image --dind-image (docker:28-dind from the prod-ECR Docker Hub mirror)
          securityContext.privileged=true, DOCKER_TLS_CERTDIR="" (plain tcp on 2375 inside the pod netns)
          resources requests=limits (--dind-cpu --dind-memory --dind-storage)
          mounts: docker-graph at /var/lib/docker, work at --workdir
```

Why these choices:

- **Idle command with a trap.** A bare `sleep` as PID 1 ignores SIGTERM, so
  `kubectl delete` would wait the full grace period. The shell-with-trap form
  exits at once (the same pattern the repository's TPU fixtures use).
- **Requests equal limits.** Guaranteed QoS keeps the devbox from being the
  first thing evicted on a busy node; lower the numbers rather than dropping
  the limits.
- **`work` is an emptyDir.** It disappears with the pod. Anything you want to
  keep goes back to the devserver (`kubectl cp`/`tar` the other way) or into a
  branch you push from inside the pod.
- **Docker over TCP without TLS.** Only reachable inside the pod's network
  namespace; nothing is exposed on the node.

## CKS specifics

- **Kubeconfig** comes from `cloud k8s generate-kubeconfig cw/<cluster>
  --path <file>`; list candidates with `cloud k8s list cw` and verify you can
  read nodes and pods before creating anything.
- **Egress** for `go mod download`, `git` and `pip` goes through the
  in-cluster proxy `http://mtls-proxy.mtls-proxy.svc.cluster.local:8080`
  (`--proxy`). Use the fully qualified name: the short `...svc` form resolves
  for glibc clients but not for resolvers that ignore the search list (Rust
  tooling such as `uv` or cargo). The MKS `fwdproxy` name does not resolve on
  CKS. The renderer sets both upper- and lower-case proxy variables and a
  `NO_PROXY` covering `localhost`, `10.0.0.0/8`, `.svc` and
  `.svc.cluster.local` (override with `--no-proxy`).
- **GPUs.** `--gpus N` sets `nvidia.com/gpu: N` (change the resource name
  with `--gpu-resource`). GPU node pools may be tainted; pass the matching
  `--toleration key[=value]:Effect` (read it from `kubectl describe node`).
  Nodes carry an NVLink domain label (`ds.coreweave.com/nvlink.domain`) that
  multi-node jobs group on; a single devbox does not need it.
- **Privileged sidecar.** `--dind` requires the namespace to allow privileged
  pods (`pod-security.kubernetes.io/enforce` not `restricted`/`baseline`).
  Where it is refused, run without `--dind`; unit and envtest suites still
  work, KIND does not.
- **Image pull.** These clusters pull through the prod-ECR mirror
  (`.buildkite/eval_image_mirror.sh` documents the `pt_dockerio/` layout), and
  `kind-ci` is on the same registry
  (`168653207203.dkr.ecr.us-east-2.amazonaws.com/msl_infra/kind-ci:latest`).
  If a pull is refused, `--image-pull-secret <name>` with a secret the
  namespace already has.
- **No pod-identity annotations.** Unlike MKS there is no annotation-driven
  credential injection to copy; credentials for registries inside the pod are
  a `docker login`/`crane auth login` you perform interactively with a
  short-lived token.

## Flag reference

| Flag | Field |
|---|---|
| `--name`, `--namespace` | `metadata.name` (DNS label, <= 63), `metadata.namespace` |
| `--image` | `containers[dev].image` |
| `--owner` | `metadata.labels.user` (default: current login) |
| `--label K=V`, `--annotation K=V` | merged into `metadata.labels` / `metadata.annotations` |
| `--cpu`, `--memory`, `--ephemeral-storage` | dev requests and limits |
| `--gpus N`, `--gpu-resource` | dev `resources.limits[<gpu-resource>]` |
| `--dind`, `--dind-image`, `--dind-cpu`, `--dind-memory`, `--dind-storage` | the sidecar |
| `--proxy URL`, `--no-proxy LIST` | dev env |
| `--env K=V` | extra dev env (e.g. `DOCKER_BUILDKIT=0` to use loaded base images without pulls) |
| `--node-selector K=V` | `spec.nodeSelector` |
| `--toleration KEY[=VALUE]:EFFECT` | `spec.tolerations` (`Exists` without a value, `Equal` with one) |
| `--image-pull-secret NAME` | `spec.imagePullSecrets` |
| `--service-account NAME` | `spec.serviceAccountName` |
| `--workdir PATH` | mount point of the `work` volume and dev `workingDir` |
| `--grace-period SECONDS` | `spec.terminationGracePeriodSeconds` |
