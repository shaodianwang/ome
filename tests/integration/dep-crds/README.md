# dep-crds — CRD YAMLs vendored on demand from go.mod modules

This directory holds the CRDs that envtest installs when a test opts
into the corresponding `framework.OptionalCRDBundle`. The YAMLs
themselves are **not committed** — they're regenerated from go.mod by
`make dep-crds`, which `make integration-test` runs as a prerequisite.

The single source of truth for CRD versions is `go.mod`. Checking the
YAMLs in would create a drift surface and bloat the repo (Knative +
LWS + KEDA + Ray + Istio together is ~30 files) without buying anything
that the go.mod pin doesn't already give us.

## Layout (after `make dep-crds`)

```
dep-crds/
├── knative-serving/    # knative.dev/serving CRDs (Service, Route, …)
├── lws/                # leaderworkerset.x-k8s.io CRDs (TODO)
├── keda/               # keda.sh CRDs (TODO)
├── kuberay/            # ray.io CRDs (TODO)
├── istio/              # networking.istio.io CRDs (TODO)
└── README.md
```

`tests/integration/dep-crds/*/` is gitignored; only this README is
tracked.

## Bumping a vendored dep

```sh
go get -u knative.dev/serving@v0.40.0
go mod tidy
make integration-test    # re-vendors via dep-crds prerequisite, runs the suite
```

If a CRD shape change broke an integration test, the failure surfaces
immediately. No second commit needed for the YAMLs themselves.

## Sourcing a new bundle

When adding a bundle (e.g. KEDA), three pieces are needed:

1. The dep is in `go.mod` so `go list -m` can locate the module dir.
2. The module ships CRD YAMLs in a known sub-directory (e.g.
   `config/crd/bases/`). Locate via:
   ```sh
   find $(go list -m -mod=readonly -f '{{.Dir}}' keda.sh) -name '*.yaml' | grep -i 'CustomResourceDefinition'
   ```
3. A `<bundle>-crds` Make target that does
   `cp -f <module>/.../*.yaml $(DEP_CRDS_DIR)/<bundle>/`.
   Add the new target to the `dep-crds:` aggregator.
4. An `OptionalCRDBundle` constant in
   `tests/integration/framework/crd_bundles.go` with the right
   AddToScheme contributors.

If the dep is **not** in go.mod (e.g. only the operator binary is
distributed, no Go module), commit the YAMLs by hand under
`dep-crds/<bundle>/` (you'll need to remove the gitignore for that
specific path) with a `SOURCE` file noting the upstream URL + git SHA.
