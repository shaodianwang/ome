package types

import (
	"time"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
)

// ComponentPlan is the desired Component → Instance → Runner → Pod shape
// computed each reconcile from the workload's desired + observed state.
// Not persisted; recomputed every iteration. The policy fields hold the
// effective values after defaults are applied.
//
// Lives here in `workload/types/` so `workload/ops/` and the new
// `workload.Reconcile` entry point can both consume it without closing
// an import cycle (workload → workload/ops → workload). The builder
// for this type (BuildPlan) stays in the parent `workload` package
// because it pulls in `v1beta1` lifecycle-defaulting logic and isn't a
// pure data structure.
type ComponentPlan struct {
	// Component identifies which of router / engine / decoder this plan
	// describes. Typed as v1beta1.ComponentType because downstream call
	// sites in pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/
	// pass plan.Component on to query/, status/, and per-op helpers that
	// still take v1beta1.ComponentType.
	Component v1beta1.ComponentType

	// Replicas is the desired number of Instances for this Component
	// (= MinReplicas, or 1 when MinReplicas is unset).
	Replicas int32

	// Instances enumerates the desired Instances, one entry per index
	// 0..Replicas-1. Sparse indices (resulting from surge migration)
	// are introduced in a follow-up PR.
	Instances []InstancePlan

	// RestartPolicy is the effective Instance-group restart policy.
	RestartPolicy v1beta1.InstanceRestartPolicy

	// UpdateStrategy is the effective rollout policy across Instances.
	UpdateStrategy v1beta1.UpdateStrategy

	// ReadyPolicy is the effective Instance-level readiness aggregation.
	ReadyPolicy v1beta1.InstanceReadyPolicy

	// InstanceReadyTimeout is the wait ceiling on a newly-created
	// Instance becoming Ready.
	InstanceReadyTimeout time.Duration

	// MigrationMode is the effective migration disposition (auto / surge /
	// never).
	MigrationMode v1beta1.MigrationPolicyMode
}

// ComponentTypeFromV1beta1 converts a v1beta1.ComponentType to a
// workload.ComponentType through an explicit switch — the same shape
// internalsource.ComponentTypeToWorkload uses at the omenative adapter
// boundary, but available in-package so workload subpackages can reach
// for it without closing the internalsource → workload import cycle.
// Unknown values map to the empty string so callers tolerate
// API-version skew without panicking; the bare T(value) cast workload
// subpackages previously used silently accepted any v1beta1 spelling,
// including drift between the two enums.
//
// Lives in types/plan.go (not types.go) because this file already
// carries the residual v1beta1 dependency for ComponentPlan.Component;
// keeping the converter alongside the field that motivates it avoids
// adding a new v1beta1 import to types.go.
func ComponentTypeFromV1beta1(v v1beta1.ComponentType) ComponentType {
	switch v {
	case v1beta1.RouterComponent:
		return ComponentRouter
	case v1beta1.EngineComponent:
		return ComponentEngine
	case v1beta1.DecoderComponent:
		return ComponentDecoder
	default:
		return ComponentType("")
	}
}

// InstancePlan is the desired state for one Instance — the atomic unit of
// gang scheduling, restart, and migration.
type InstancePlan struct {
	// Index is the stable Instance ordinal. Sparse after surge migration:
	// when migrating index 0 with a surge at index 2, the post-migration
	// plan can hold a single Instance at index 2.
	Index int32

	// Incarnation is the monotonic lifecycle token for this (Component,
	// Index) pair. Initial create stamps 1; full recreate / restart-on-loss
	// bumps it; in-place update does not. It distinguishes old pod
	// materializations from current ones when stable names get reused
	// after a recreate.
	Incarnation int64

	// Runners enumerates the Runners that constitute this Instance.
	// Single-pod Instances have one Runner named "default" of size 1.
	// Multi-pod Instances (leader + worker) are introduced in a follow-up.
	Runners []RunnerPlan

	// MigrationOverlay carries placement hints from an in-flight surge
	// migration. Render reads it to inject hard anti-affinity from the
	// source node and preferred affinity to hint target nodes. Nil for
	// non-migration paths (steady-state, restart, in-place / recreate
	// update).
	MigrationOverlay *MigrationOverlay
}

// MigrationOverlay records the per-Instance placement constraints a
// surge migration carries onto the rendered pod. Materialized as
// pod affinity in Render and intentionally absent from the canonical
// revision payload — these are transient operation overlays, not
// steady-state template fields.
type MigrationOverlay struct {
	// FromNode is the node the migration is moving the Instance away
	// from. Surfaces as RequiredDuringScheduling anti-affinity on
	// kubernetes.io/hostname.
	FromNode string

	// HintTargetNodes are the caller-supplied preferred destinations.
	// Surface as PreferredDuringScheduling affinity; the scheduler is
	// free to pick another node when these are unavailable.
	HintTargetNodes []string
}

// RunnerPlan is the desired state for one Runner within an Instance.
type RunnerPlan struct {
	// Name is "leader", "worker", or "default" (single-pod).
	Name string

	// Size is the number of pods of this Runner within the Instance.
	Size int32
}

// TotalPods returns the total desired pod count across all Runners in
// this Instance — 1 for single-pod, 1+N for leader+worker. Defined as
// a method so callers can reach the sum without importing workload/query
// (which would close an import cycle once workload/ itself imports
// query/).
func (i InstancePlan) TotalPods() int32 {
	var n int32
	for _, r := range i.Runners {
		n += r.Size
	}
	return n
}

// AllocateSurgeIndex returns a fresh non-negative index for a surge
// replica — the lowest int32 not currently present in any of the
// InstanceStatuses. The migration op uses this to pick the slot for
// its +1 surge pod without colliding with steady-state indices or
// other in-flight surges.
//
// Lives in workload/types/ alongside the type it operates on so
// workload/ops/migrate.go can reach it without importing the parent
// workload package (which would close the workload→workload/ops cycle
// the types-split avoids). The parent workload.BuildPlan has its own
// equivalent inline helpers (instancePlanIndices) and does not call
// this function — both code paths use the same pigeonhole-bounded
// lowest-unused-int32 algorithm.
func AllocateSurgeIndex(instances []InstanceStatus) int32 {
	used := make(map[int32]struct{}, len(instances))
	for _, s := range instances {
		used[s.Index] = struct{}{}
	}
	// The loop is bounded by len(used)+1 — for any set of size N at
	// least one of {0,1,...,N} must be unused, so we never scan past
	// that. The bound prevents a pathological infinite loop if `used`
	// were ever to contain every int32 value.
	for i := int32(0); i <= int32(len(used)); i++ {
		if _, taken := used[i]; !taken {
			return i
		}
	}
	// Unreachable given the pigeonhole argument above; the explicit
	// fallback keeps the function total.
	return int32(len(used))
}
