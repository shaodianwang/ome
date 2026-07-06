# ome-scheduler

The OME accelerator scheduler — the upstream kube-scheduler built as a library
with the OME placement plugin (`OMEGangPack`) registered next to the built-in
plugins. See [OEP-0022](../oeps/0022-ome-scheduler/README.md).

## Why a separate Go module

Building the Kubernetes scheduler as a library requires importing
`k8s.io/kubernetes`, which needs the staging `replace` block in `go.mod` (pinning
every `k8s.io/*` to the fleet's Kubernetes version — currently 1.33). That block,
and the large transitive graph it pulls in, would poison the root
`github.com/ome-projects/ome` module (which pins k8s via controller-runtime on a
different cadence). So the scheduler is its **own module**, pinned and bumped
deliberately — the same structure scheduler-plugins / Volcano / KAI use.

## Layout

```
scheduler/
├── go.mod                        # own module; k8s 1.33 staging replace block
├── cmd/ome-scheduler/main.go     # entry point: kube-scheduler + our plugin
└── pkg/plugins/gangpack/         # OMEGangPack placement plugin (Phase A)
```

## Build & test

```sh
cd scheduler
go build ./...                                     # compile-check every package (no binary emitted)
go build -o bin/ome-scheduler ./cmd/ome-scheduler  # produce the scheduler binary
go test ./...
```

Note: `go build ./...` only *checks* that everything compiles — for a multi-package
build Go discards the executable. Use the explicit `-o` form (or `go install
./cmd/ome-scheduler`) to get the binary.

## Status

**Phase A walking skeleton.** The plugin registers and passes every node through
`Filter` as a no-op — the point of the skeleton is to prove the
kube-scheduler-as-library build and plugin registration end to end. Domain
choice, pinning, the gang gate (Permit), and best-fit packing land on top of this
scaffold; see OEP-0022 § Placement.

## Version policy

Pinned to the fleet's Kubernetes minor version (currently 1.33 →
`k8s.io/kubernetes v1.33.5`). The plugin framework API changes between k8s
minors, so version bumps are deliberate, reviewed changes: regenerate the
`replace` block from the matching `sigs.k8s.io/scheduler-plugins` release. See
OEP-0022 § Graduation Criteria.
