package omenative

import (
	"time"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
)

// ComponentPlan is the desired Component → Instance → Runner → Pod shape
// computed each reconcile from the InferenceService spec, ServingRuntime,
// and BaseModel. Not persisted; recomputed every iteration. The policy
// fields hold the effective values after defaults are applied.
type ComponentPlan struct {
	// Component identifies which of router / engine / decoder this plan
	// describes.
	Component v1beta1.ComponentType

	// Replicas is the desired number of Instances for this Component
	// (= MinReplicas, or 1 when MinReplicas is unset).
	Replicas int32

	// Instances enumerates the desired Instances, one entry per index
	// 0..Replicas-1. Sparse indices (resulting from surge migration)
	// are introduced in a follow-up PR.
	Instances []InstancePlan

	// RestartPolicy is the effective Instance-group restart policy.
	RestartPolicy v1beta1.OMENativeRestartPolicy

	// UpdateStrategy is the effective rollout policy across Instances.
	UpdateStrategy v1beta1.OMENativeUpdateStrategy

	// ReadyPolicy is the effective Instance-level readiness aggregation.
	ReadyPolicy v1beta1.OMENativeReadyPolicy

	// InstanceReadyTimeout is OMENative's wait ceiling on a newly-created
	// Instance becoming Ready.
	InstanceReadyTimeout time.Duration

	// MigrationMode is the effective migration disposition (auto / surge /
	// never).
	MigrationMode v1beta1.OMENativeMigrationPolicyMode
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

// OperationStep is the fine-grained resume point stored in
// InstanceStatus.Operation.Step (Drain, DeletePods, WaitZero, Recreate,
// WaitReady, ...). The value space is per-operation and filled in by the
// operation that introduces each step.
type OperationStep string
