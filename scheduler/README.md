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
├── pkg/topology/                 # pure best-fit domain accounting (no k8s deps)
├── pkg/placement/                # per-gang domain pins
├── pkg/plugins/gangpack/         # OMEGangPack placement plugin (Phase A)
└── test/integration/             # envtest + real in-process scheduler
```

## Build & test

```sh
cd scheduler
go build ./...                                     # compile-check every package (no binary emitted)
go build -o bin/ome-scheduler ./cmd/ome-scheduler  # produce the scheduler binary
go test ./...                                       # unit tests
```

Note: `go build ./...` only *checks* that everything compiles — for a multi-package
build Go discards the executable. Use the explicit `-o` form (or `go install
./cmd/ome-scheduler`) to get the binary.

### Integration tests

`test/integration/` runs the plugin inside a **real in-process kube-scheduler**
against an [envtest](https://book.kubebuilder.io/reference/envtest) API server —
the authoritative proof that the choose → pin → enforce → gate → bind loop
actually schedules a gang, covering the informer/`New`/snapshot glue the unit
tests fake. The scheduler-plugins `PodGroup` CRD is sourced from the go.mod
module at test time (not vendored — go.mod is the source of truth, matching OME's
dep-crds policy).

```sh
export KUBEBUILDER_ASSETS="$(../bin/setup-envtest use 1.33 -p path)"   # envtest binaries
go test ./test/integration/... -v
```

## Status

**Phase A.** The placement loop is live end to end: `PreFilter` best-fits and
pins the gang's domain, `Filter` enforces it, `Permit` gates the gang until all
members are present, and `Unreserve` unwinds a half-formed gang. Gang membership
+ size come from the standard scheduler-plugins `PodGroup`; the domain label is
declared per-workload via the PodGroup's `ome.io/topology-key` annotation; and
node free-ness is inferred from the gang pod's own resource requests — the
scheduler bakes in no workload conventions or fabric values. Still ahead: a
per-gang capacity claim in `Reserve` (so two gangs can't race into one domain),
replica spreading, and metrics; see OEP-0022 § Placement.

## Version policy

Pinned to the fleet's Kubernetes minor version (currently 1.33 →
`k8s.io/kubernetes v1.33.5`). The plugin framework API changes between k8s
minors, so version bumps are deliberate, reviewed changes: regenerate the
`replace` block from the matching `sigs.k8s.io/scheduler-plugins` release. See
OEP-0022 § Graduation Criteria.
