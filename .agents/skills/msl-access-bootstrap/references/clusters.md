# Clusters OME work touches

`cloud k8s generate-kubeconfig <target> --path <file>` mints a kubeconfig for
one cluster under your own identity. Keep one file per cluster under
`$HOME/.kube/`, never merge them into `~/.kube/config`, and read the context
name back from the file rather than assuming it:

```sh
kubectl --kubeconfig "$KC" config get-contexts -o name
```

## GKE tpu7x clusters

| Cluster | `cloud` target | Kubeconfig (convention) | Context | Namespaces | OME |
|---|---|---|---|---|---|
| `msl-gcp-usc1-1-tpu` | `msl-gcp-1/msl-gcp-usc1-1-tpu` | `$HOME/.kube/msl-gcp-usc1-1.conf` | `msl-gcp-1/us-central1/msl-gcp-usc1-1-tpu` | `sandbox`, `staging`, `prod` (the overlay's `namespaceIngressGateways` keys) | installed from `charts/ome-resources/values-usc1-1-tpu.yaml`: prod-ECR hub, Gateway API on with per-ISVC subdomains, model agent off, bundled Prometheus |
| `msl-gcp-usc1-rc-1-tpu` | `msl-gcp-rc/msl-gcp-usc1-rc-1-tpu` | `$HOME/.kube/msl-gcp-usc1-rc-1.conf` | `msl-gcp-rc/us-central1/msl-gcp-usc1-rc-1-tpu` | `def-sandbox` | also hosts the CI agent pool for the GKE KIND lane (`.buildkite/README-gke-kind.md`); pods there have no public egress |

Both are `tpu7x` (four chips per host; a `2x2x2` slice is two hosts, eight
chips) with the `google.com/tpu` taint on TPU nodes. The
`tpu-serving-setup` skill covers deploying onto them.

Access is a per-cluster `K8S_CLUSTER` entitlement carrying an `admin` action
for the namespace; `kubectl auth can-i create pods -n <ns>` answering `no`
means the grant is missing, not that the kubeconfig is wrong.

## CoreWeave (CKS) dev clusters

Discover with `cloud k8s list cw`; the target is `cw/<cluster>` and the
context name is whatever the generated file says. The OMENative real-cluster
harness (`hack/omenative-testing/perm/run.py`) targets whichever of these
`OME_TEST_KUBECONFIG` names, with `OME_TEST_NS` defaulting to `sandbox`; the
`cks-devbox` skill runs a toolchain pod next to it. These clusters pull images
through the prod-ECR mirror (`.buildkite/eval_image_mirror.sh`).

## MKS clusters

Target `mks/<cluster>` (`cloud k8s list mks`), for example
`mks/prod-msl-capi-lco-4` for engineering dev with the `playground`
namespace; the `mks-devbox` skill has the list. Kubeconfig generation for MKS
works only from a devserver.

## What must never happen to these files

- `kubectl config use-context|set-context|set-cluster|delete-context` without
  `--kubeconfig` scoping it to the intended file.
- `kind`, `kwokctl`, `make test-kind*`, `hack/kwok-sim/fleet-up.sh` running
  while `KUBECONFIG` points at one of them: they merge a `kind-*` context in
  and flip `current-context`. Export a temp file first
  (`AGENTS.md`, Kubeconfig Hygiene).
- copying one into a pod or a pipeline. Pods get service accounts; CI gets
  its own credentials.
