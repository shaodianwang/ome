package types

import (
	"time"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
)

// ComponentPlan is the desired Component → Instance → Runner → Pod
// shape computed each reconcile from desired + observed state. Not
// persisted; policy fields hold the effective values after defaults.
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

// ComponentTypeFromV1beta1 converts via an explicit switch. Unknown
// values map to "" so callers tolerate API-version skew without
// panicking — a bare T(value) cast would silently accept any drift
// between the two enums.
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

// InstancePlan is the desired state for one Instance — the atomic
// unit of gang scheduling, restart, and migration.
type InstancePlan struct {
	// Index is the stable Instance ordinal. Sparse after surge
	// migration.
	Index int32

	// Incarnation is the monotonic lifecycle token for this
	// (Component, Index) pair. Initial create stamps 1; full recreate
	// / restart-on-loss bumps it; in-place update does not. Distinguishes
	// old pod materializations from current ones when stable names get
	// reused after a recreate.
	Incarnation int64

	// Runners enumerates the Runners that constitute this Instance.
	// Single-pod has one "default" Runner of size 1; multi-pod has
	// leader + worker.
	Runners []RunnerPlan

	// MigrationOverlay carries placement hints from an in-flight surge
	// migration. Render injects hard anti-affinity from the source node
	// and preferred affinity to hint target nodes. Nil for non-migration
	// paths.
	MigrationOverlay *MigrationOverlay
}

// MigrationOverlay records the per-Instance placement constraints a
// surge migration carries onto the rendered pod. Materialized as pod
// affinity in Render and intentionally absent from the canonical
// revision payload — transient operation overlay, not a steady-state
// template field.
type MigrationOverlay struct {
	// FromNode surfaces as RequiredDuringScheduling anti-affinity on
	// kubernetes.io/hostname.
	FromNode string
	// HintTargetNodes surface as PreferredDuringScheduling affinity;
	// the scheduler is free to pick another node when these are
	// unavailable.
	HintTargetNodes []string
}

// RunnerPlan is the desired state for one Runner within an Instance.
type RunnerPlan struct {
	// Name is "leader", "worker", or "default" (single-pod).
	Name string
	// Size is the number of pods of this Runner within the Instance.
	Size int32
}

// TotalPods returns the total desired pod count across all Runners —
// 1 for single-pod, 1+N for leader+worker.
func (i InstancePlan) TotalPods() int32 {
	var n int32
	for _, r := range i.Runners {
		n += r.Size
	}
	return n
}

// AllocateSurgeIndex returns the lowest int32 not present in any of
// the InstanceStatuses — the slot the migration op picks for its +1
// surge pod without colliding with steady-state indices or other
// in-flight surges.
func AllocateSurgeIndex(instances []InstanceStatus) int32 {
	used := make(map[int32]struct{}, len(instances))
	for _, s := range instances {
		used[s.Index] = struct{}{}
	}
	// Pigeonhole-bounded: for any set of size N at least one of
	// {0,..,N} must be unused.
	for i := int32(0); i <= int32(len(used)); i++ {
		if _, taken := used[i]; !taken {
			return i
		}
	}
	return int32(len(used)) // unreachable; keep function total.
}
