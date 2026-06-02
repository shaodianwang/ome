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
			RolloutCoordination: &v1beta1.RolloutCoordinationSpec{
				Groups: []v1beta1.RolloutCoordinationGroup{{
					Components: []v1beta1.ComponentType{v1beta1.DecoderComponent, v1beta1.EngineComponent},
					Policy:     v1beta1.CoordinationPolicySequential,
					Order:      []v1beta1.ComponentType{v1beta1.DecoderComponent, v1beta1.EngineComponent},
				}},
			},
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
