# Package a host-compiled binary without running networked build steps.
ARG BASE_IMAGE
FROM ${BASE_IMAGE}

ARG RUN_USER=65532:65532

WORKDIR /
COPY . /
USER ${RUN_USER}
ENTRYPOINT ["/entrypoint"]
