# ome-quota-manager: the OME fleet-quota control plane (OEP-0024). A separate
# binary from ome-manager so the Kueue write grant it will need is scoped to its
# own ServiceAccount rather than the one that also runs the InferenceService
# reconcilers and the fail-closed pod mutating webhook.
#
# Unlike ome-scheduler this shares the ROOT Go module — nothing it imports
# conflicts with the root dependency graph, so a second module would only add a
# replace directive back here — and therefore follows the root module's layout:
# cmd/ome-quota-manager, pkg/controller/v1beta1/acceleratorquota, pkg/quota.
# Links no CGO/xet, so it cross-compiles to a static binary on scratch. Build
# context is the repo root (matches the other dockerfiles/).
ARG GO_BUILDER_IMAGE=golang:1.25
FROM ${GO_BUILDER_IMAGE} AS builder
WORKDIR /workspace

# Module files first so the dependency layer caches independently of source.
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

# Only the trees this binary compiles. Copying all of cmd/ would rebuild this
# layer on any unrelated command's change; copying the whole repo, on any change
# at all.
COPY pkg/ ./pkg/
COPY cmd/ome-quota-manager/ ./cmd/ome-quota-manager/

ARG TARGETOS
ARG TARGETARCH

# Version info, stamped into pkg/version the same way manager.Dockerfile does it,
# so `--version` and the version metric agree across OME's images.
ARG VERSION
ARG GIT_TAG
ARG GIT_COMMIT

RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg/mod \
    CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH} \
    go build -trimpath \
    -ldflags "-s -w -X github.com/ome-projects/ome/pkg/version.GitVersion=${GIT_TAG} -X github.com/ome-projects/ome/pkg/version.GitCommit=${GIT_COMMIT}" \
    -o /ome-quota-manager ./cmd/ome-quota-manager

FROM scratch
COPY --from=builder /ome-quota-manager /ome-quota-manager
USER 65532:65532
ENTRYPOINT ["/ome-quota-manager"]
