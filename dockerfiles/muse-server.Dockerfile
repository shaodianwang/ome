# muse-server — the MUSE console: a Rust read-plane API that also serves the built
# web UI (SPA) at `/`, with the API under `/v1`. One image, no nginx: a gateway
# terminates TLS and routes to it. Build context is the repo root
# (`build-context: "."`) because the crate depends on ../ome-client-rs by path.
#
# Configurable runtime base (mirrors manager.Dockerfile); override with
# --build-arg BASE_IMAGE=ubuntu:24.04.
ARG BASE_IMAGE=oraclelinux:10-slim

# 1) Build the web console. Runs on the native builder arch (BUILDPLATFORM) since
#    the output is arch-independent JS. MUSE_WEB_BASE is the sub-path the console
#    is served under behind the gateway (default /muse/); override to re-route.
FROM --platform=$BUILDPLATFORM node:22-bookworm-slim AS web
ARG MUSE_WEB_BASE=/muse/
ENV MUSE_WEB_BASE=${MUSE_WEB_BASE}
WORKDIR /web
COPY muse/web/package.json ./
RUN npm install
COPY muse/web/ ./
RUN npm run build

# 2) Build the Rust server (per target arch).
FROM rust:1-bookworm AS builder
# rustls' crypto backend (ring / aws-lc-rs) needs a C toolchain, cmake and perl.
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    cmake \
    perl \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /workspace
# Preserve the path layout muse-server expects (`../../../ome-client-rs`).
COPY ome-client-rs/ ome-client-rs/
COPY muse/ muse/
WORKDIR /workspace/muse
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/workspace/muse/target \
    cargo build --release -p muse-server --bin muse \
    && cp target/release/muse /workspace/muse

# 3) Runtime.
ARG BASE_IMAGE
FROM ${BASE_IMAGE}
# glibc + libgcc for the dynamically-linked binary; ca-certificates for TLS to the
# apiservers it watches.
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
COPY --from=web /web/dist /web
# Serve the baked-in console; set MUSE_WEB_DIR="" for an API-only server.
ENV MUSE_WEB_DIR=/web
USER 65532:65532
# API (8080) and Prometheus metrics (9090); actual ports are config-driven.
EXPOSE 8080 9090
ENTRYPOINT ["/muse"]
