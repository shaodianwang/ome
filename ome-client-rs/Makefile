# OME Rust client codegen.
#
# Regenerate the type modules from the OME CRD YAML in ../config/crd/full/ (which
# `make manifests` at the repo root produces from the Go API markers). The Go
# client (pkg/client) and this Rust client both derive from the same CRDs.
# The placement target checks the source, member and routing protocol together.
KOPIUM  ?= kopium
KOPIUM_VERSION ?= 0.24.0
PYTHON  ?= python3
RUSTFMT ?= rustfmt
CRD_DIR := ../config/crd/full
CRDS    ?= $(wildcard $(CRD_DIR)/ome.io_*.yaml)
PLACEMENT_KINDS := inferenceservices inferencereplicas trafficmaps workloadclusters
SRC     := src
SLIM    := hack/slim_crd.py

# Kubernetes version of the envtest control-plane assets, read from the
# repo-root single source of truth shared with the Go suites.
ENVTEST_K8S_VERSION ?= $(strip $(shell cat ../.envtest-k8s-version))

.PHONY: rust-client rust-placement-client ci-placement kopium-install check build rust-test rust-integration-test
kopium-install:
	cargo install kopium --version $(KOPIUM_VERSION) --locked

rust-client: ## regenerate src/<crd>.rs from the OME CRD YAML
	@set -eu; tmp=$$(mktemp -d); trap 'rm -rf "$$tmp"' EXIT; \
	for crd in $(CRDS); do \
	  mod=$$(basename $$crd .yaml); mod=$${mod#ome.io_}; \
	  echo "kopium $$mod"; \
	  $(PYTHON) $(SLIM) "$$crd" > "$$tmp/ome.io_$$mod.yaml"; \
	  $(KOPIUM) -f "$$tmp/ome.io_$$mod.yaml" --schema=disabled -d > "$$tmp/$$mod.rs"; \
	  $(RUSTFMT) --edition 2021 "$$tmp/$$mod.rs"; \
	  sed -i.bak "s| -f [^ ]* --schema=disabled| -f config/crd/full/ome.io_$$mod.yaml --schema=disabled|" "$$tmp/$$mod.rs"; \
	  mv "$$tmp/$$mod.rs" "$(SRC)/$$mod.rs"; \
	done
	@echo "regenerated: pod-template fields opaqued (hack/slim_crd.py), rustfmt'd, kopium"
	@echo "comment normalized to the canonical CRD path (deterministic for the drift gate)."
	@echo "if a CRD was added/removed, update src/lib.rs (the mod list)."

rust-placement-client: ## regenerate the placement protocol's typed clients
	$(MAKE) rust-client CRDS="$(foreach kind,$(PLACEMENT_KINDS),$(CRD_DIR)/ome.io_$(kind).yaml)"

ci-placement: ## require current placement types and lossless protocol round trips
	$(MAKE) rust-placement-client
	git diff --exit-code -- $(foreach kind,$(PLACEMENT_KINDS),$(SRC)/$(kind).rs)
	$(MAKE) rust-test

check:
	cargo check --all-features

build:
	cargo build --all-features

# Integration tests boot etcd + kube-apiserver from the controller-runtime
# envtest assets (no Docker) — the same binaries OME's Go suite uses — install
# the OME CRDs, and exercise the clientset/informer/lister against them.
rust-integration-test: ## envtest-backed clientset/informer/lister round-trips
	KUBEBUILDER_ASSETS="$$(cd .. && bin/setup-envtest use $(ENVTEST_K8S_VERSION) -p path)" \
	  cargo test --features runtime --test integration -- --nocapture

rust-test: ## crate unit tests (no apiserver needed)
	cargo test --features runtime --lib --test placement
