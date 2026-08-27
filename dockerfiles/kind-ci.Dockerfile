# kind-ci: the offline workload image for the GKE-backed KIND CI step
# (integration-test-kind-gke) and the kind validation Job.
#
# The MSL GCP RC cluster has NO public egress from pods — it can't reach Docker
# Hub, dl.k8s.io, proxy.golang.org, crates.io, or gcr.io at run time. So every
# tool, module, crate, and base image the KIND suite needs is baked in HERE and
# pulled from prod ECR (which the cluster CAN pull). Built on the TW build queue
# (which HAS egress) and mirrored to prod ECR by generate_pipeline.py.
#
# Baked in:
#   - Go 1.25 toolchain + a pre-populated Go module cache (go test runs offline),
#   - Rust toolchain + a pre-fetched cargo cache for pkg/xet (make xet-build
#     runs offline; the KIND suite transitively links the xet staticlib),
#   - docker CLI, kind, kubectl, Helm, Python, crane, jq + build deps,
#   - docker-loadable image tarballs under /images: kindest/node (the kind node),
#     golang:1.25 and gcr.io/distroless/static:nonroot (the two FROM stages of
#     tests/integration/kindutil/fake-serving/Dockerfile, which the suite
#     `docker build`s at run time), and busybox (validation Job probe),
#   - cert-manager's pinned manifest and every image referenced by it, plus the
#     nginx and Prometheus images used by the OMENative eval fixtures.
#
# Built by generate_pipeline.py via the cf-docker-image plugin with build-context
# "." (repo root) — so the COPY steps below see the repo. TARGETARCH is set by
# buildx per platform.
#
ARG GO_BUILDER_IMAGE=golang:1.25
FROM ${GO_BUILDER_IMAGE}

ARG TARGETARCH
ARG DOCKER_VERSION=28.0.1
# Fallbacks only. package_kind_test_image.sh appends the kind binary and the
# node tarball the run actually uses, read from hack/internal/tools/go.mod and
# .kind-node-image on the branch under test — this image is a main-only
# artifact, so a bump baked only here would never reach the branch that needs
# it. Keep these tracking those two files anyway, for anyone who runs the image
# standalone (the validation Job does).
ARG KIND_VERSION=v0.32.0
ARG KIND_NODE_TAG=v1.35.5
ARG CRANE_VERSION=v0.20.2
ARG HELM_VERSION=v3.20.2
ARG FAKE_SERVING_BUILD_IMAGE=golang:1.25
ARG FAKE_SERVING_BASE_IMAGE=gcr.io/distroless/static:nonroot
ARG CERT_MANAGER_VERSION=v1.16.2
ARG EVAL_NGINX_IMAGE=nginxinc/nginx-unprivileged:1.27-alpine
ARG EVAL_PROMETHEUS_IMAGE=prom/prometheus:v3.0.1

# System build deps the xet staticlib + CRD tooling need (mirror run_tests.sh).
RUN apt-get update && apt-get install -y --no-install-recommends \
      jq libssl-dev pkg-config build-essential curl ca-certificates git python3 \
    && rm -rf /var/lib/apt/lists/*

# docker CLI (talks to the dind sidecar at run time), kind, kubectl, Helm, crane.
RUN set -eux; \
    case "${TARGETARCH}" in \
      amd64) CRANE_ARCH=x86_64; DOCKER_ARCH=x86_64 ;; \
      arm64) CRANE_ARCH=arm64;  DOCKER_ARCH=aarch64 ;; \
      *) echo "unsupported TARGETARCH=${TARGETARCH}" >&2; exit 1 ;; \
    esac; \
    curl -fsSL "https://download.docker.com/linux/static/stable/${DOCKER_ARCH}/docker-${DOCKER_VERSION}.tgz" \
      | tar -xz -C /usr/local/bin --strip-components=1 docker/docker; \
    curl -fsSLo /usr/local/bin/kind "https://kind.sigs.k8s.io/dl/${KIND_VERSION}/kind-linux-${TARGETARCH}"; \
    chmod +x /usr/local/bin/kind; \
    KVER="$(curl -fsSL https://dl.k8s.io/release/stable.txt)"; \
    curl -fsSLo /usr/local/bin/kubectl "https://dl.k8s.io/release/${KVER}/bin/linux/${TARGETARCH}/kubectl"; \
    chmod +x /usr/local/bin/kubectl; \
    curl -fsSL "https://get.helm.sh/helm-${HELM_VERSION}-linux-${TARGETARCH}.tar.gz" \
      | tar -xz -C /usr/local/bin --strip-components=1 "linux-${TARGETARCH}/helm"; \
    curl -fsSL "https://github.com/google/go-containerregistry/releases/download/${CRANE_VERSION}/go-containerregistry_Linux_${CRANE_ARCH}.tar.gz" \
      | tar -xz -C /usr/local/bin crane

# Rust toolchain for pkg/xet.
ENV RUSTUP_HOME=/usr/local/rustup CARGO_HOME=/usr/local/cargo PATH=/usr/local/cargo/bin:$PATH
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
      | sh -s -- -y --default-toolchain stable --profile minimal

# Bake the Go module cache. Copy just the module manifests first so this layer
# caches unless deps change, then download every module the build graph needs.
ENV GOMODCACHE=/go/pkg/mod
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download all

# Bake the cargo cache for the xet staticlib (crates.io is unreachable at run
# time). Cargo.lock is gitignored, so copy the whole crate dir and let cargo
# fetch resolve + populate CARGO_HOME (index + crates) for offline builds.
COPY pkg/xet/ pkg/xet/
RUN cd pkg/xet && cargo fetch

# Bake the images kind + the suite need as docker-loadable tarballs (crane is
# daemonless; --platform pins to this build leg's arch).
RUN set -eux; mkdir -p /images; \
    crane pull --platform "linux/${TARGETARCH}" "kindest/node:${KIND_NODE_TAG}"  /images/kindest-node.tar; \
    crane pull --platform "linux/${TARGETARCH}" "${FAKE_SERVING_BUILD_IMAGE}"    /images/fake-serving-build.tar; \
    crane pull --platform "linux/${TARGETARCH}" "${FAKE_SERVING_BASE_IMAGE}"     /images/fake-serving-base.tar; \
    crane pull --platform "linux/${TARGETARCH}" docker.io/library/busybox:latest /images/busybox.tar

# The deployed eval needs cert-manager for webhook certificates, nginx for its
# CPU-only runtime fixtures, and Prometheus for metric-analysis cases. Derive
# the cert-manager image set from the manifest so a version change cannot leave
# an unbaked image behind.
RUN set -eux; \
    mkdir -p /eval; \
    curl -fsSLo /eval/cert-manager.yaml \
      "https://github.com/cert-manager/cert-manager/releases/download/${CERT_MANAGER_VERSION}/cert-manager.yaml"; \
    grep -Fq "app.kubernetes.io/version: \"${CERT_MANAGER_VERSION}\"" /eval/cert-manager.yaml; \
    grep -oE '^[[:space:]]+image: "?[^"[:space:]]+' /eval/cert-manager.yaml \
      | awk '{print $2}' | tr -d '"' | sort -u > /eval/cert-manager-images.txt; \
    test -s /eval/cert-manager-images.txt; \
    i=0; \
    while read -r image; do \
      crane pull --platform "linux/${TARGETARCH}" "$image" "/images/cert-manager-${i}.tar"; \
      i=$((i + 1)); \
    done < /eval/cert-manager-images.txt; \
    test "$i" -eq "$(wc -l < /eval/cert-manager-images.txt)"; \
    crane pull --platform "linux/${TARGETARCH}" "${EVAL_NGINX_IMAGE}" /images/eval-nginx.tar; \
    crane pull --platform "linux/${TARGETARCH}" "${EVAL_PROMETHEUS_IMAGE}" /images/eval-prometheus.tar

# OME discovers optional PodGroup support once at manager startup. Bake the CRD
# from the pinned scheduler-plugins module so deployed gang cases exercise it.
RUN set -eux; \
    scheduler_plugins_dir="$(go list -m -mod=readonly -f '{{.Dir}}' sigs.k8s.io/scheduler-plugins)"; \
    cp "${scheduler_plugins_dir}/config/crd/bases/scheduling.x-k8s.io_podgroups.yaml" \
      /eval/podgroup-crd.yaml

# Fail the build before publication if a tool or offline archive is incomplete.
RUN set -eux; \
    go version; \
    kind --version; \
    kubectl version --client=true -o yaml >/dev/null; \
    test "$(helm version --template '{{.Version}}')" = "${HELM_VERSION}"; \
    python3 --version; \
    cargo --version; \
    docker --version; \
    for archive in /images/kindest-node.tar /images/fake-serving-build.tar \
      /images/fake-serving-base.tar /images/busybox.tar /images/eval-nginx.tar \
      /images/eval-prometheus.tar; do \
      test -s "$archive"; \
      tar -tf "$archive" >/dev/null; \
    done; \
    test -s /eval/podgroup-crd.yaml; \
    i=0; \
    while read -r _; do \
      archive="/images/cert-manager-${i}.tar"; \
      test -s "$archive"; \
      tar -tf "$archive" >/dev/null; \
      i=$((i + 1)); \
    done < /eval/cert-manager-images.txt; \
    test "$i" -gt 0; \
    test "$i" -eq "$(wc -l < /eval/cert-manager-images.txt)"

WORKDIR /
