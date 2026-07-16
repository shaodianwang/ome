# MUSE build helpers.
#
# Codegen for the OME client types is NOT here — it lives in ../ome-client-rs
# (`make rust-client` there), because that crate is a repo-wide artifact, not a
# MUSE internal.
.PHONY: build run check fmt fmt-check clippy test ci ci-rust ci-web
build:
	cargo build

run: ## run the read-plane server against your current kubecontext
	set -a; . ./config/dev.env; set +a; cargo run -p muse-server --bin muse

check:
	cargo check

fmt:
	cargo fmt --all

fmt-check:
	cargo fmt --all -- --check

clippy:
	cargo clippy --all-targets -- -D warnings

test:
	cargo test

# One clean-checkout contract for local development. Buildkite runs the Rust
# group directly and the web group in the muse-server image's Node build stage.
ci: ci-rust ci-web

ci-rust:
	cargo fmt --all -- --check
	cargo clippy --all-targets -- -D warnings
	cargo test

ci-web:
	npm --prefix web install --no-package-lock
	npm --prefix web run check
	npm --prefix web run build
