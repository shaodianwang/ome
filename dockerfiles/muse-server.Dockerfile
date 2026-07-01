# muse-server — the MUSE read-plane API (Rust). Build context is the repo root
# (`build-context: "."`) because the crate depends on ../ome-client-rs by path.
#
# Configurable runtime base, mirroring manager.Dockerfile. Override with
# --build-arg BASE_IMAGE=ubuntu:24.04.
ARG BASE_IMAGE=oraclelinux:10-slim

FROM rust:1-bookworm AS builder
# rustls' crypto backend (ring / aws-lc-rs) needs a C toolchain, cmake and perl.
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    cmake \
    perl \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /workspace
# Preserve the path layout muse-server expects: its Cargo.toml points at
# `../../../ome-client-rs`, i.e. <root>/ome-client-rs relative to the crate.
COPY ome-client-rs/ ome-client-rs/
COPY muse/ muse/

WORKDIR /workspace/muse
# Cache the cargo registry/git and the target dir across builds; copy the binary
# out within the same layer, before the cache mount is released.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/workspace/muse/target \
    cargo build --release -p muse-server --bin muse \
    && cp target/release/muse /workspace/muse

ARG BASE_IMAGE
FROM ${BASE_IMAGE}
# glibc + libgcc for the dynamically-linked binary; ca-certificates for TLS to
# the apiservers it watches.
RUN if [ -f /usr/bin/microdnf ]; then \
        microdnf update -y && \
        microdnf install -y glibc libgcc ca-certificates && \
        microdnf clean all; \
    elif [ -f /usr/bin/apt-get ]; then \
        apt-get update && \
        apt-get install -y ca-certificates libc6 libgcc-s1 && \
        apt-get clean && rm -rf /var/lib/apt/lists/*; \
    fi
WORKDIR /
COPY --from=builder /workspace/muse /muse
USER 65532:65532
# API (8080) and Prometheus metrics (9090); actual ports are config-driven.
EXPOSE 8080 9090
ENTRYPOINT ["/muse"]
