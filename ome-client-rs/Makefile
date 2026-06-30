# OME Rust client codegen.
#
# Regenerate the type modules from the OME CRD YAML in ../config/crd/full/ (which
# `make manifests` at the repo root produces from the Go API markers). The Go
# client (pkg/client) and this Rust client both derive from the same CRDs, so
# this belongs beside that codegen — wire it into the root `make generate` and a
# CI drift check (regenerate + `git diff --exit-code`) the way dep-crds-drift and
# the generated Python SDK already work.
KOPIUM  ?= kopium
CRD_DIR := ../config/crd/full
SRC     := src
SLIM    := hack/slim_crd.py

.PHONY: rust-client check build rust-test rust-integration-test
rust-client: ## regenerate src/<crd>.rs from the OME CRD YAML
	@tmp=$$(mktemp -d); \
	for crd in $(CRD_DIR)/ome.io_*.yaml; do \
	  mod=$$(basename $$crd .yaml); mod=$${mod#ome.io_}; \
	  echo "kopium $$mod"; \
	  python3 $(SLIM) $$crd > $$tmp/ome.io_$$mod.yaml; \
	  $(KOPIUM) -f $$tmp/ome.io_$$mod.yaml --schema=disabled -d > $(SRC)/$$mod.rs; \
	  rustfmt --edition 2021 $(SRC)/$$mod.rs; \
	  sed -i.bak "s| -f [^ ]* --schema=disabled| -f config/crd/full/ome.io_$$mod.yaml --schema=disabled|" $(SRC)/$$mod.rs; \
	  rm -f $(SRC)/$$mod.rs.bak; \
	done; \
	rm -rf $$tmp
	@echo "regenerated: pod-template fields opaqued (hack/slim_crd.py), rustfmt'd, kopium"
	@echo "comment normalized to the canonical CRD path (deterministic for the drift gate)."
	@echo "if a CRD was added/removed, update src/lib.rs (the mod list)."

check:
	cargo check --all-features

build:
	cargo build --all-features

# Integration tests boot etcd + kube-apiserver from the controller-runtime
# envtest assets (no Docker) — the same binaries OME's Go suite uses — install
# the OME CRDs, and exercise the clientset/informer/lister against them.
rust-integration-test: ## envtest-backed clientset/informer/lister round-trips
	KUBEBUILDER_ASSETS="$$(cd .. && bin/setup-envtest use 1.30 -p path)" \
	  cargo test --features runtime --test integration -- --nocapture

rust-test: ## crate unit tests (no apiserver needed)
	cargo test --features runtime --lib
