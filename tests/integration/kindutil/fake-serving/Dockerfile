# Multi-stage so the final image is just the static binary.
FROM golang:1.25 AS build
WORKDIR /src
COPY main.go .
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o /fake-serving main.go

FROM gcr.io/distroless/static:nonroot
COPY --from=build /fake-serving /fake-serving
EXPOSE 8080
# VERSION is the in-place-update content-bump knob. Distinct values
# produce distinct image digests (via the LABEL below), so kubelet's
# ContainerStatus.Image flips on bump even though the binary is
# identical. Without this knob two tags pointing at the same content
# resolve to the same sha256 and ops.podRuntimeImagesMatch never
# detects the roll. Default `dev` for ad-hoc local docker-builds.
ARG VERSION=dev
LABEL ome.io/fake-serving-version=$VERSION
ENTRYPOINT ["/fake-serving"]
