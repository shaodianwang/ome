package core

import (
	"errors"
	"fmt"
	"sort"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

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

// ErrMultiPodNotYetSupported is returned by BuildPlan when the Component
// declares Leader or Worker (multi-pod Instances). The single-pod runner
// layout is what this PR implements; multi-pod (leader + worker per
// Instance) is deferred to a follow-up.
var ErrMultiPodNotYetSupported = errors.New("OMENative multi-pod Instances not yet supported")

// BuildPlan computes the desired ComponentPlan from a ReconcileParams.
//
// The function is pure: it inspects p (ISVC + ComponentExt + MultiPod
// flag + replica counts) and produces the plan the rest of the reconciler
// will drive toward. No I/O, no controller-runtime calls.
//
// Scope: single-pod Instances only. If p.MultiPod is true, returns
// ErrMultiPodNotYetSupported without partially populating ComponentPlan.
//
// Defaulting: the mutating webhook fills in `omenative.*` for OMENative-
// annotated Components, so in production the fields are non-nil. For
// robustness against pre-defaulter objects or test fixtures, BuildPlan
// also applies the same defaults inline rather than relying solely on
// the webhook.
func BuildPlan(p ReconcileParams) (ComponentPlan, error) {
	if p.ComponentExt == nil {
		return ComponentPlan{}, fmt.Errorf("nil ComponentExtensionSpec for component=%s", p.Component)
	}
	if p.MultiPod {
		return ComponentPlan{}, ErrMultiPodNotYetSupported
	}

	replicas := int32(1)
	if p.ComponentExt.MinReplicas != nil && *p.ComponentExt.MinReplicas > 0 {
		replicas = int32(*p.ComponentExt.MinReplicas)
	}

	// Compute the plan's Instance index set. Existing InstanceStatus
	// entries — including any migration-surge index above the replica
	// count — are preserved so the rest of Loop A keeps driving them.
	// The plan grows beyond replicas during the surge phase of a
	// migration; scale-down logic (extraInstanceIndices) is responsible
	// for picking the right deletion target after that surge resolves.
	indices := instancePlanIndices(p.ISVC, p.Component, replicas)
	instances := make([]InstancePlan, len(indices))
	for i, idx := range indices {
		instances[i] = InstancePlan{
			Index:       idx,
			Incarnation: incarnationForIndex(p.ISVC, p.Component, idx),
			Runners: []RunnerPlan{
				{Name: "default", Size: 1},
			},
		}
	}

	omenative := p.ComponentExt.OMENative
	if omenative == nil {
		omenative = &v1beta1.OMENativeComponentSpec{}
	}

	return ComponentPlan{
		Component:            p.Component,
		Replicas:             replicas,
		Instances:            instances,
		RestartPolicy:        restartPolicyOrDefault(omenative.RestartPolicy, p.MultiPod),
		UpdateStrategy:       updateStrategyWithDefaults(omenative.UpdateStrategy),
		ReadyPolicy:          readyPolicyOrDefault(omenative.ReadyPolicy, p.MultiPod),
		InstanceReadyTimeout: instanceReadyTimeoutOrDefault(omenative.InstanceReadyTimeout),
		MigrationMode:        migrationModeOrDefault(omenative.MigrationPolicy),
	}, nil
}

func restartPolicyOrDefault(p *v1beta1.OMENativeRestartPolicy, multiPod bool) v1beta1.OMENativeRestartPolicy {
	if p != nil {
		return *p
	}
	if multiPod {
		return v1beta1.OMENativeRestartPolicyRecreateInstance
	}
	return v1beta1.OMENativeRestartPolicyNone
}

func readyPolicyOrDefault(p *v1beta1.OMENativeReadyPolicy, multiPod bool) v1beta1.OMENativeReadyPolicy {
	if p != nil {
		return *p
	}
	if multiPod {
		return v1beta1.OMENativeReadyPolicyAllPodReady
	}
	return v1beta1.OMENativeReadyPolicyNone
}

func updateStrategyWithDefaults(s *v1beta1.OMENativeUpdateStrategy) v1beta1.OMENativeUpdateStrategy {
	if s == nil {
		s = &v1beta1.OMENativeUpdateStrategy{}
	}
	out := *s
	if out.Type == "" {
		// Default to SurgeThenDrain. It currently delegates to recreate
		// (no zero-downtime surge yet), but recreates throttle to the
		// MaxUnavailable budget at the dispatcher gate — in-place at
		// scale could mass-drain the fleet on a single wake-up. Opt
		// back into in-place explicitly via
		// spec.<component>.omeNative.updateStrategy.type if you have a
		// reason (pod-UID preservation, tight capacity).
		out.Type = v1beta1.OMENativeUpdateStrategySurgeThenDrain
	}
	if out.InPlaceUpdateStrategy == nil {
		out.InPlaceUpdateStrategy = &v1beta1.OMENativeInPlaceUpdateStrategy{}
	}
	if out.InPlaceUpdateStrategy.GracePeriodSeconds == nil {
		grace := int32(30)
		out.InPlaceUpdateStrategy.GracePeriodSeconds = &grace
	}
	if out.InPlaceUpdateStrategy.MarkNotReadyDuringLifecycle == nil {
		mark := true
		out.InPlaceUpdateStrategy.MarkNotReadyDuringLifecycle = &mark
	}
	return out
}

func instanceReadyTimeoutOrDefault(d *metav1.Duration) time.Duration {
	if d == nil {
		return 30 * time.Minute
	}
	return d.Duration
}

func migrationModeOrDefault(p *v1beta1.OMENativeMigrationPolicy) v1beta1.OMENativeMigrationPolicyMode {
	if p == nil || p.Mode == "" {
		return v1beta1.OMENativeMigrationPolicyModeAuto
	}
	return p.Mode
}

// instancePlanIndices computes the per-Instance index set the plan
// should drive: all migration-in-flight indices (unbounded by replicas;
// they're the transient surge pair), then the oldest existing
// non-migration indices up to the replica cap, then newly allocated
// indices at the lowest unused position to round out scale-up.
//
// The replica cap counts only non-migration indices. Counting the
// surge against it would drop a healthy non-migrating sibling out of
// the plan and into scale-down.
func instancePlanIndices(isvc *v1beta1.InferenceService, component v1beta1.ComponentType, replicas int32) []int32 {
	used := existingInstanceIndices(isvc, component)
	migrationProtected := migrationInFlightIndices(isvc, component)
	migrationSource := migrationSourceIndices(isvc, component)

	sorted := make([]int32, 0, len(used))
	for idx := range used {
		sorted = append(sorted, idx)
	}
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	indices := make([]int32, 0, len(sorted))
	picked := map[int32]struct{}{}

	// Pass 1: pin migration-in-flight indices (no cap on count; both
	// source AND surge get pinned). Source-side entries count toward
	// the steady replica budget — the source IS the user-facing replica
	// being relocated. Surge-side entries are a transient +1 over
	// MinReplicas and do NOT count. Without the source accounting,
	// Pass 3 over-allocates a brand-new index during the surge window
	// (e.g. replicas=1 + source@0 Migrating + surge@1 Creating →
	// Pass 3 invents Index=2), and on the very next reconcile after
	// Migrate completes the post-Migrate Create pass materializes a
	// phantom pod at that index.
	steadyCount := int32(0)
	for _, idx := range sorted {
		if _, protected := migrationProtected[idx]; !protected {
			continue
		}
		indices = append(indices, idx)
		picked[idx] = struct{}{}
		if _, isSource := migrationSource[idx]; isSource {
			steadyCount++
		}
	}

	// Pass 2: fill the steady replica budget, oldest first.
	for _, idx := range sorted {
		if _, already := picked[idx]; already {
			continue
		}
		if steadyCount >= replicas {
			break
		}
		indices = append(indices, idx)
		picked[idx] = struct{}{}
		steadyCount++
	}

	// Pass 3: allocate new slots for scale-up.
	taken := make(map[int32]struct{}, len(used)+len(indices))
	for k, v := range used {
		taken[k] = v
	}
	for _, idx := range indices {
		taken[idx] = struct{}{}
	}
	for steadyCount < replicas {
		next := lowestUnusedIndex(taken)
		indices = append(indices, next)
		taken[next] = struct{}{}
		steadyCount++
	}

	sort.Slice(indices, func(i, j int) bool { return indices[i] < indices[j] })
	return indices
}

// migrationInFlightIndices returns the indices of InstanceStatuses
// currently participating in a migration — either Phase=Migrating
// (the source) or Operation.Type=Migrate (the surge). These indices
// stay in the plan past scale-down boundaries because deleting them
// mid-migration would abandon a partially-rolled-out surge.
func migrationInFlightIndices(isvc *v1beta1.InferenceService, component v1beta1.ComponentType) map[int32]struct{} {
	out := map[int32]struct{}{}
	if isvc == nil {
		return out
	}
	cs, ok := isvc.Status.Components[component]
	if !ok || cs.OMENative == nil {
		return out
	}
	for _, s := range cs.OMENative.InstanceStatuses {
		if s.Phase == v1beta1.OMENativeInstanceMigrating {
			out[s.Index] = struct{}{}
			continue
		}
		if s.Operation != nil && s.Operation.Type == v1beta1.InstanceOperationMigrate {
			out[s.Index] = struct{}{}
		}
	}
	return out
}

// migrationSourceIndices returns the indices of source-side migration
// participants — InstanceStatuses with Phase=Migrating. Surge-side
// participants (Phase=Creating + Operation.Migrate) are not included
// because they are the +1 over MinReplicas, not the user-facing
// replica being relocated.
func migrationSourceIndices(isvc *v1beta1.InferenceService, component v1beta1.ComponentType) map[int32]struct{} {
	out := map[int32]struct{}{}
	if isvc == nil {
		return out
	}
	cs, ok := isvc.Status.Components[component]
	if !ok || cs.OMENative == nil {
		return out
	}
	for _, s := range cs.OMENative.InstanceStatuses {
		if s.Phase == v1beta1.OMENativeInstanceMigrating {
			out[s.Index] = struct{}{}
		}
	}
	return out
}

// existingInstanceIndices returns the set of indices the Component's
// InstanceStatus currently records.
func existingInstanceIndices(isvc *v1beta1.InferenceService, component v1beta1.ComponentType) map[int32]struct{} {
	out := map[int32]struct{}{}
	if isvc == nil {
		return out
	}
	cs, ok := isvc.Status.Components[component]
	if !ok || cs.OMENative == nil {
		return out
	}
	for _, s := range cs.OMENative.InstanceStatuses {
		out[s.Index] = struct{}{}
	}
	return out
}

// AllocateSurgeIndex returns a fresh non-negative index for a surge
// replica — the lowest int32 not currently present in any of the
// Component's InstanceStatuses. The migration op uses this to pick
// the slot for its +1 surge pod without colliding with steady-state
// indices or other in-flight surges.
func AllocateSurgeIndex(isvc *v1beta1.InferenceService, component v1beta1.ComponentType) int32 {
	return lowestUnusedIndex(existingInstanceIndices(isvc, component))
}

// lowestUnusedIndex returns the smallest non-negative int32 not
// present in used. The loop is bounded by len(used)+1 — for any set
// of size N, at least one of {0,1,...,N} must be unused, so we never
// scan past that. The bound prevents a pathological infinite loop if
// `used` were ever to contain every int32 value.
func lowestUnusedIndex(used map[int32]struct{}) int32 {
	for i := int32(0); i <= int32(len(used)); i++ {
		if _, taken := used[i]; !taken {
			return i
		}
	}
	// Unreachable given the pigeonhole argument above; the explicit
	// fallback keeps the function total.
	return int32(len(used))
}

// incarnationForIndex returns the current Incarnation for the (component, idx)
// pair. If the ISVC has no InstanceStatus for that pair yet — first reconcile,
// or after a delete — incarnation starts at 1. Otherwise the existing value
// is preserved. This function only reads status; the writers are the
// patchInstanceStatus* helpers that own each lifecycle transition.
func incarnationForIndex(isvc *v1beta1.InferenceService, component v1beta1.ComponentType, idx int32) int64 {
	if isvc == nil {
		return 1
	}
	cs, ok := isvc.Status.Components[component]
	if !ok || cs.OMENative == nil {
		return 1
	}
	for _, s := range cs.OMENative.InstanceStatuses {
		if s.Index == idx && s.Incarnation > 0 {
			return s.Incarnation
		}
	}
	return 1
}
