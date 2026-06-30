# MUSE build helpers.
#
# Codegen for the OME client types is NOT here — it lives in ../ome-client-rs
# (`make rust-client` there), because that crate is a repo-wide artifact, not a
# MUSE internal.
.PHONY: build run check fmt clippy
build:
	cargo build

run: ## run the read-plane server against your current kubecontext
	cargo run -p muse-server --bin muse

check:
	cargo check

fmt:
	cargo fmt

clippy:
	cargo clippy --all-targets -- -D warnings
