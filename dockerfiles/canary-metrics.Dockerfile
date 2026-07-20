# Tiny CPU-only metrics workload for the OMENative canary-analysis e2e tests
# (hack/omenative-testing). Stdlib-only Go (no module deps) -> static binary on
# scratch. Build context is the repo root (matches the other dockerfiles/).
ARG GO_BUILDER_IMAGE=golang:1.25
FROM ${GO_BUILDER_IMAGE} AS build
WORKDIR /src
COPY hack/omenative-testing/metrics-server/ .
ARG TARGETOS
ARG TARGETARCH
RUN CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH:-amd64} \
    go build -trimpath -ldflags="-s -w" -o /canary-metrics .

FROM scratch
COPY --from=build /canary-metrics /canary-metrics
USER 65532:65532
EXPOSE 8080
ENTRYPOINT ["/canary-metrics"]
