package inferencereplica

import (
	"testing"

	"github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload"
)

// mkSequentialParent returns an ISVC named "llama" (the parent every
// baselineIR points at) declaring one Sequential coordination group
// [decoder, engine] with Order decoder→engine. Generation is 2; callers
// stamp per-Component Status to model in-flight vs converged via
// ObservedGeneration skew (gen < 2 ⇒ in flight, the moment-of-bump
// signal observeSequentialComponentsForGate keys on).
func mkSequentialParent() *v1beta1.InferenceService {
	return &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "llama", Namespace: "default", Generation: 2},
		Spec: v1beta1.InferenceServiceSpec{
			Rollout: &v1beta1.RolloutSpec{Coordination: &v1beta1.RolloutCoordinationSpec{
				Groups: []v1beta1.RolloutCoordinationGroup{{
					Components: []v1beta1.ComponentType{v1beta1.DecoderComponent, v1beta1.EngineComponent},
					Policy:     v1beta1.CoordinationPolicySequential,
					Order:      []v1beta1.ComponentType{v1beta1.DecoderComponent, v1beta1.EngineComponent},
				}},
			}},
		},
	}
}

func setObservedGen(isvc *v1beta1.InferenceService, c v1beta1.ComponentType, gen int64) {
	if isvc.Status.Components == nil {
		isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{}
	}
	cs := isvc.Status.Components[c]
	if cs.Lifecycle == nil {
		cs.Lifecycle = &v1beta1.LifecycleStatus{}
	}
	cs.Lifecycle.ObservedGeneration = gen
	isvc.Status.Components[c] = cs
}

// TestBuildReconcileInput_WiresSequentialGate is the regression guard for
// the IR-managed coordination bug. The IR controller used to leave
// ReconcileInput.UpdateGate nil, so the default (IR-managed) path skipped
// CheckSequential entirely and both Components of a Sequential group
// rolled concurrently (the playground caught this as "Sequential overlap:
// engine started before decoder finished"). The gate must now be wired
// AND deny the engine while the decoder (first in Order) is in flight.
func TestBuildReconcileInput_WiresSequentialGate(t *testing.T) {
	g := gomega.NewWithT(t)
	r, _ := newReconciler(t)
	ir := baselineIR("llama-engine", "default", 1) // Component=engine, ParentRef=llama

	parent := mkSequentialParent()
	// Moment-of-bump: both Components lag isvc.Generation=2 ⇒ both in
	// flight. The active selector picks decoder (first in Order); the
	// engine must wait.
	setObservedGen(parent, v1beta1.DecoderComponent, 1)
	setObservedGen(parent, v1beta1.EngineComponent, 1)

	input := r.buildReconcileInput(ir, parent)
	g.Expect(input.UpdateGate).NotTo(gomega.BeNil(),
		"UpdateGate must be wired on the IR path when the parent declares coordination")

	allowed, reason := input.UpdateGate(workload.UpdateStrategySurgeThenDrain, 0, 0)
	g.Expect(allowed).To(gomega.BeFalse(),
		"engine must be denied while decoder (first in Sequential Order) is in flight")
	g.Expect(reason).To(gomega.ContainSubstring("Sequential waiting on decoder"))
}

// TestBuildReconcileInput_SequentialReleasesActiveComponent proves the
// gate is not a blanket block: once the decoder has converged, the engine
// becomes the active Sequential Component and is allowed to roll.
func TestBuildReconcileInput_SequentialReleasesActiveComponent(t *testing.T) {
	g := gomega.NewWithT(t)
	r, _ := newReconciler(t)
	ir := baselineIR("llama-engine", "default", 1)

	parent := mkSequentialParent()
	setObservedGen(parent, v1beta1.DecoderComponent, 2) // caught up ⇒ not in flight
	setObservedGen(parent, v1beta1.EngineComponent, 1)  // still lagging ⇒ engine's turn

	input := r.buildReconcileInput(ir, parent)
	g.Expect(input.UpdateGate).NotTo(gomega.BeNil())

	allowed, _ := input.UpdateGate(workload.UpdateStrategySurgeThenDrain, 0, 0)
	g.Expect(allowed).To(gomega.BeTrue(),
		"engine is the active Sequential Component once decoder converged; it must be allowed")
}

// TestBuildReconcileInput_NilParentLeavesGateNil pins that without a
// resolvable parent there is no RolloutCoordination block to enforce, so
// the gate stays nil and the dispatcher's "always allowed" fallback
// applies (matching the documented EventTarget fallback behavior).
func TestBuildReconcileInput_NilParentLeavesGateNil(t *testing.T) {
	g := gomega.NewWithT(t)
	r, _ := newReconciler(t)
	ir := baselineIR("llama-engine", "default", 1)

	input := r.buildReconcileInput(ir, nil)
	g.Expect(input.UpdateGate).To(gomega.BeNil(),
		"no parent ⇒ no coordination to enforce ⇒ gate stays nil")
}

// TestBuildReconcileInput_WiresMigrationWhenParentSet pins Layer 2: with
// a resolvable parent, the IR-managed path wires DetectMigrationRequest
// (so explicit + ProgressDeadlineSeconds auto migration fire on the
// production-default path) and points the migration audit ledger at the
// parent ISVC via LedgerOwner — so the ledger Migrate reads/writes is the
// same ISVC-owned ledger detection uses, while the IR still owns the pods.
// Nil parent leaves both nil (migration dormant), matching the gate/
// EventTarget fallback.
func TestBuildReconcileInput_WiresMigrationWhenParentSet(t *testing.T) {
	g := gomega.NewWithT(t)
	r, _ := newReconciler(t)
	ir := baselineIR("llama-engine", "default", 1)

	parent := mkSequentialParent() // any non-nil ISVC named "llama"
	input := r.buildReconcileInput(ir, parent)

	g.Expect(input.DetectMigrationRequest).NotTo(gomega.BeNil(),
		"DetectMigrationRequest must be wired on the IR path when the parent is resolvable")
	g.Expect(input.LedgerOwner).NotTo(gomega.BeNil(),
		"the migration ledger must be owned by the parent ISVC, not the IR")
	g.Expect(input.LedgerOwner.GetName()).To(gomega.Equal(parent.Name))
	g.Expect(input.LedgerOwnerGVK).To(gomega.Equal(isvcGVK))

	// Nil parent ⇒ migration dormant.
	nilInput := r.buildReconcileInput(ir, nil)
	g.Expect(nilInput.DetectMigrationRequest).To(gomega.BeNil(),
		"no parent ⇒ migration has no ISVC to read ⇒ DetectMigrationRequest stays nil")
	g.Expect(nilInput.LedgerOwner).To(gomega.BeNil())
}

// TestBuildReconcileInput_ThreadsGangSchedulingAvailable pins the IR-path
// multi-node gang fix: buildReconcileInput must thread the controller's
// GangSchedulingAvailable flag into DesiredSpec, because EnsurePodGroups gates
// per-Instance PodGroup creation on it. Without this, IR-managed multi-node
// pods render the gang reference but no PodGroup object is ever created, so the
// gang stays Pending forever ("PodGroup not found") — the bug this fixes.
func TestBuildReconcileInput_ThreadsGangSchedulingAvailable(t *testing.T) {
	g := gomega.NewWithT(t)
	r, _ := newReconciler(t)
	ir := baselineIR("llama-engine", "default", 1)

	r.GangSchedulingAvailable = true
	g.Expect(r.buildReconcileInput(ir, nil).DesiredSpec.GangSchedulingAvailable).To(gomega.BeTrue(),
		"DesiredSpec.GangSchedulingAvailable must follow the controller flag (true) so EnsurePodGroups runs")

	r.GangSchedulingAvailable = false
	g.Expect(r.buildReconcileInput(ir, nil).DesiredSpec.GangSchedulingAvailable).To(gomega.BeFalse(),
		"flag false ⇒ DesiredSpec false ⇒ EnsurePodGroups skips (CRD absent / degradation surface)")
}

// TestBuildReconcileInput_GateUsesFreshIRStatus pins the IR-path
// gate-staleness fix (p44/p51): the gate must read the GATED Component's
// counts from the IR's OWN fresh status, not the parent ISVC's lagged
// projection. Here the IR's fresh status shows the engine already one pod
// down (serving 3/4); a RatioBalanced drain (RecreatePod → -1) must be
// DENIED — the tiebreaker bounds in-flight to one pod — even though the
// parent's projected status still reports a full 4/4 (which, if the gate
// read it, would let the tiebreaker fire again and over-drain).
func TestBuildReconcileInput_GateUsesFreshIRStatus(t *testing.T) {
	g := gomega.NewWithT(t)
	r, _ := newReconciler(t)
	ir := baselineIR("llama-engine", "default", 4) // engine, parent llama
	ir.Status.Replicas = 4
	ir.Status.ServingReplicas = 3 // FRESH: one engine pod already out of rotation

	tol := int32(25)
	parent := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "llama", Namespace: "default", Generation: 1},
		Spec: v1beta1.InferenceServiceSpec{
			Rollout: &v1beta1.RolloutSpec{Coordination: &v1beta1.RolloutCoordinationSpec{
				Groups: []v1beta1.RolloutCoordinationGroup{{
					Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
					Policy:     v1beta1.CoordinationPolicyRollingUpdate,
					Pacing: &v1beta1.CoordinationPacing{
						Type:                  v1beta1.CoordinationPacingRatioBalanced,
						RatioTolerancePercent: &tol,
					},
				}},
			}},
		},
		Status: v1beta1.InferenceServiceStatus{
			RolloutCoordination: &v1beta1.RolloutCoordinationStatus{
				Groups: []v1beta1.RolloutCoordinationGroupStatus{{
					Name:       "0",
					Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
					Policy:     v1beta1.CoordinationPolicyRollingUpdate,
					ObservedRatio: &v1beta1.RolloutCoordinationRatio{
						Original: map[v1beta1.ComponentType]int32{
							v1beta1.EngineComponent:  4,
							v1beta1.DecoderComponent: 4,
						},
					},
				}},
			},
			Components: map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
				// STALE projection: still reports engine fully serving (4/4).
				v1beta1.EngineComponent:  {Lifecycle: &v1beta1.LifecycleStatus{Replicas: 4, ServingReplicas: 4}},
				v1beta1.DecoderComponent: {Lifecycle: &v1beta1.LifecycleStatus{Replicas: 4, ServingReplicas: 4}},
			},
		},
	}

	input := r.buildReconcileInput(ir, parent)
	g.Expect(input.UpdateGate).NotTo(gomega.BeNil())

	allowed, reason := input.UpdateGate(workload.UpdateStrategyRecreatePod, 0, 0)
	g.Expect(allowed).To(gomega.BeFalse(),
		"with the IR's fresh status (engine serving 3/4 = one pod already out), the "+
			"RatioBalanced tiebreaker must refuse a second drain; got allowed — the gate "+
			"read the stale parent projection (4/4) instead of the IR's fresh status: "+reason)
}
