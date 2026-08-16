package workload

import (
	"sort"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// BuildPlan computes the desired ComponentPlan from the projected
// (WorkloadDesiredSpec, WorkloadObservedState) pair. Pure: no I/O,
// no controller-runtime calls.
//
// Runner layout:
//   - Single-pod (desired.MultiPod=false): one "default" Runner of
//     size 1. Used by Router and Engine/Decoder without Leader+Worker.
//   - Multi-pod (desired.MultiPod=true): one "leader" Runner of size 1
//     plus one "worker" Runner of the user-set Worker.Size.
//
// The mutating webhook fills in Lifecycle fields in production;
// BuildPlan re-applies defaults inline for robustness against
// pre-defaulter objects and test fixtures.
func BuildPlan(component ComponentType, desired WorkloadDesiredSpec, observed WorkloadObservedState) (ComponentPlan, error) {
	replicas := desired.Replicas
	if replicas <= 0 {
		replicas = 1
	}

	workerSize := workerSizeFromRunners(desired.Runners)

	// Compute the plan's Instance index set. Existing InstanceStatus
	// entries — including any migration-surge index above the replica
	// count — are preserved so the reconcile loop keeps driving them.
	// The plan grows beyond replicas during the surge phase of a
	// migration; scale-down logic is responsible for picking the right
	// deletion target after that surge resolves.
	indices := instancePlanIndices(observed.InstanceStatuses, replicas)
	instances := make([]InstancePlan, len(indices))
	runners := runnersForInstance(desired.MultiPod, workerSize)
	for i, idx := range indices {
		instances[i] = InstancePlan{
			Index:       idx,
			Incarnation: incarnationForIndex(observed.InstanceStatuses, idx),
			Runners:     append([]RunnerPlan(nil), runners...),
			// Relocation-directive memory: the adapter projects the
			// per-instance node-exclusion list from the audit ledger;
			// Render turns it into a required NotIn hostname term so
			// the rebuild lands off the recorded suspect node(s).
			ExcludedNodes: append([]string(nil), observed.ExcludedNodesByInstance[idx]...),
		}
	}

	lifecycle := desired.Lifecycle

	return ComponentPlan{
		Component:            component,
		Replicas:             replicas,
		Instances:            instances,
		RestartPolicy:        restartPolicyOrDefault(lifecycle.RestartPolicy, desired.MultiPod),
		UpdateStrategy:       updateStrategyWithDefaults(lifecycle.UpdateStrategy),
		ReadyPolicy:          readyPolicyOrDefault(lifecycle.ReadyPolicy, desired.MultiPod),
		InstanceReadyTimeout: InstanceReadyTimeoutOrDefault(lifecycle.InstanceReadyTimeout),
		MigrationMode:        MigrationModeOrDefault(lifecycle.MigrationPolicy),
		Paused:               desired.Paused,
		TopologyKey:          desired.TopologyKey,
	}, nil
}

// workerSizeFromRunners returns the "worker" Runner size or 0 when no
// worker Runner is present (single-pod Component).
func workerSizeFromRunners(runners []Runner) int32 {
	for _, r := range runners {
		if r.Name == "worker" {
			return r.Size
		}
	}
	return 0
}

// runnersForInstance returns the Runner layout for one Instance based
// on the Component's multi-pod flag and worker size. The "worker"
// runner is emitted even when workerSize=0 so the layout shape is
// consistent (downstream code keys on Runner.Name). Admission rejects
// Worker.Size <= 0 at the webhook so workerSize=0 multi-pod requests
// never reach this function in production.
func runnersForInstance(multiPod bool, workerSize int32) []RunnerPlan {
	if !multiPod {
		return []RunnerPlan{{Name: "default", Size: 1}}
	}
	return []RunnerPlan{
		{Name: "leader", Size: 1},
		{Name: "worker", Size: workerSize},
	}
}

func restartPolicyOrDefault(p *RestartPolicy, multiPod bool) RestartPolicy {
	if p != nil {
		return *p
	}
	if multiPod {
		return RestartPolicyRecreateInstance
	}
	return RestartPolicyNone
}

func readyPolicyOrDefault(p *InstanceReadyPolicy, multiPod bool) InstanceReadyPolicy {
	if p != nil {
		return *p
	}
	if multiPod {
		return InstanceReadyPolicyAllPodReady
	}
	return InstanceReadyPolicyNone
}

func updateStrategyWithDefaults(s *UpdateStrategy) UpdateStrategy {
	if s == nil {
		s = &UpdateStrategy{}
	}
	out := *s
	if out.Type == "" {
		// SurgeThenDrain default: recreates throttle to MaxUnavailable
		// at the dispatcher gate, whereas in-place at scale could mass-
		// drain the fleet on a single wake-up. Opt into in-place
		// explicitly via spec.<component>.omeNative.updateStrategy.type.
		out.Type = UpdateStrategySurgeThenDrain
	}
	if out.InPlaceUpdateStrategy == nil {
		out.InPlaceUpdateStrategy = &InPlaceUpdateStrategy{}
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

// InstanceReadyTimeoutOrDefault resolves the effective per-Component
// InstanceReadyTimeout: the configured lifecycle value, or the 30m
// default when unset. Exported so adapters stamping deadlines outside
// BuildPlan (e.g. migration accept) resolve the identical value the
// per-op writers read from ComponentPlan.InstanceReadyTimeout.
func InstanceReadyTimeoutOrDefault(d *metav1.Duration) time.Duration {
	if d == nil {
		return 30 * time.Minute
	}
	return d.Duration
}

// MigrationModeOrDefault resolves the effective per-Component
// migration mode: the configured MigrationPolicy.Mode, or Auto when
// unset. Exported so adapters gating outside BuildPlan (e.g. migration
// accept rejecting under Never) resolve the identical value the
// dispatcher reads from ComponentPlan.MigrationMode.
func MigrationModeOrDefault(p *MigrationPolicy) MigrationMode {
	if p == nil || p.Mode == "" {
		return MigrationModeAuto
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
func instancePlanIndices(instances []InstanceStatus, replicas int32) []int32 {
	used := existingInstanceIndices(instances)
	// "Protected" / "source" sets cover BOTH migration surge pairs and
	// multi-pod (gang) update-surge pairs — the two cases that transiently
	// hold an extra index over the replica cap. Union the gang-surge sets
	// in so the Pass 1/2/3 algorithm below treats them identically (pin
	// the pair; count only the source toward the steady budget).
	migrationProtected := migrationInFlightIndices(instances)
	for idx := range updateSurgeInFlightIndices(instances) {
		migrationProtected[idx] = struct{}{}
	}
	migrationSource := migrationSourceIndices(instances)
	for idx := range updateSurgeSourceIndices(instances) {
		migrationSource[idx] = struct{}{}
	}

	// Ready instances are stable replicas, preferred to keep.
	readyByIndex := map[int32]bool{}
	for _, s := range instances {
		if s.Phase == InstancePhaseReady {
			readyByIndex[s.Index] = true
		}
	}

	// Release a gang-surge SOURCE whose replacement (the instance at its
	// SurgeIndex) is already Ready: the live replacement now holds the slot,
	// so pinning/counting the stale source as a steady replica would displace
	// an already-rolled instance into scale-down — the multi-node double-roll
	// that troughs capacity and skews RatioBalanced. Released, the superseded
	// source falls out of the steady budget and becomes the scale-down target.
	// AllocateSurgeIndex picks a fresh index, so a Ready instance at SurgeIndex
	// is unambiguously this source's replacement.
	for _, s := range instances {
		if s.Operation == nil || s.Operation.SurgeIndex == nil {
			continue
		}
		if readyByIndex[*s.Operation.SurgeIndex] {
			delete(migrationProtected, s.Index)
			delete(migrationSource, s.Index)
		}
	}

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

	// Pass 2: fill the steady replica budget. Prefer Ready (stable) instances
	// over not-yet-Ready ones (e.g. a stale surge source mid-drain) so an
	// already-rolled instance is never dropped to keep one being phased out;
	// within each readiness tier, oldest (lowest index) first.
	fill := func(wantReady bool) {
		for _, idx := range sorted {
			if steadyCount >= replicas {
				return
			}
			if _, already := picked[idx]; already {
				continue
			}
			if readyByIndex[idx] != wantReady {
				continue
			}
			indices = append(indices, idx)
			picked[idx] = struct{}{}
			steadyCount++
		}
	}
	fill(true)
	fill(false)

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
func migrationInFlightIndices(instances []InstanceStatus) map[int32]struct{} {
	out := map[int32]struct{}{}
	for _, s := range instances {
		if s.Phase == InstancePhaseMigrating {
			out[s.Index] = struct{}{}
			continue
		}
		if s.Operation != nil && s.Operation.Type == InstanceOperationMigrate {
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
func migrationSourceIndices(instances []InstanceStatus) map[int32]struct{} {
	out := map[int32]struct{}{}
	for _, s := range instances {
		if s.Phase == InstancePhaseMigrating {
			out[s.Index] = struct{}{}
		}
	}
	return out
}

// updateSurgeInFlightIndices returns the indices participating in a
// multi-pod (gang) SurgeThenDrain update — both the source (Op.Type=
// Update with a SurgeIndex set) and the surge target (Op.Step=
// updateStepGangSurgeTarget). Like migration surge pairs, these stay in
// the plan past the replica cap so the replacement gang isn't scale-down-
// deleted mid-surge and EnsurePodGroups keeps its PodGroup alive.
//
// The gang source carries Op.Step=Surge so CurrentSurgeInFlight counts
// it against MaxSurge; the SurgeIndex pointer is what distinguishes a
// gang surge (new index) from a single-pod surge (ActiveOrdinal toggle,
// SurgeIndex nil).
//
// Marker-liveness invariant: a GangSurgeTarget marker is pinned only
// while some source instance carries a SurgeIndex operation referencing
// it. The marker's lifecycle is owned by its source's gangSurgeUpdate;
// once no source op points at it (the source promoted, was reset, or
// was re-adopted after a corrective edit) the marker is an ORPHAN —
// pinning it would leak a dead replacement gang forever. Unpinned, it
// falls out of the plan as a scale-down extra and the scale-down batch
// pipeline reaps its pods and clears the slot.
func updateSurgeInFlightIndices(instances []InstanceStatus) map[int32]struct{} {
	out := map[int32]struct{}{}
	referenced := map[int32]struct{}{}
	for _, s := range instances {
		if s.Operation == nil || s.Operation.Type != InstanceOperationUpdate {
			continue
		}
		if s.Operation.SurgeIndex != nil { // source
			out[s.Index] = struct{}{}
			referenced[*s.Operation.SurgeIndex] = struct{}{}
		}
	}
	for _, s := range instances {
		if s.Operation == nil || s.Operation.Type != InstanceOperationUpdate {
			continue
		}
		if s.Operation.Step == UpdateStepGangSurgeTarget || s.Operation.Step == UpdateStepGangSurgeTargetCleanup { // target (marker)
			if _, live := referenced[s.Index]; live {
				out[s.Index] = struct{}{}
			}
		}
	}
	return out
}

// updateSurgeSourceIndices returns the source-side indices of an
// in-flight gang surge (Op.Type=Update with SurgeIndex set). These count
// toward the steady replica budget — the source IS the user-facing
// replica being rolled; the surge target is the transient +1.
func updateSurgeSourceIndices(instances []InstanceStatus) map[int32]struct{} {
	out := map[int32]struct{}{}
	for _, s := range instances {
		if s.Operation != nil && s.Operation.Type == InstanceOperationUpdate && s.Operation.SurgeIndex != nil {
			out[s.Index] = struct{}{}
		}
	}
	return out
}

// existingInstanceIndices returns the set of recorded indices.
func existingInstanceIndices(instances []InstanceStatus) map[int32]struct{} {
	out := map[int32]struct{}{}
	for _, s := range instances {
		out[s.Index] = struct{}{}
	}
	return out
}

// lowestUnusedIndex returns the smallest non-negative int32 not
// present in used. Pigeonhole-bounded: for any set of size N at least
// one of {0,..,N} must be unused.
func lowestUnusedIndex(used map[int32]struct{}) int32 {
	for i := int32(0); i <= int32(len(used)); i++ {
		if _, taken := used[i]; !taken {
			return i
		}
	}
	return int32(len(used)) // unreachable; keep function total.
}

// incarnationForIndex returns the current Incarnation for idx, or 1
// when no matching entry exists yet (first reconcile or after delete).
func incarnationForIndex(instances []InstanceStatus, idx int32) int64 {
	for _, s := range instances {
		if s.Index == idx && s.Incarnation > 0 {
			return s.Incarnation
		}
	}
	return 1
}
