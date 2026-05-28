package ops

import (
	"context"
	"testing"

	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/core"
)

// Tests for the v1beta1-typed helpers in legacy_create_helpers.go that
// the omenative dispatcher (dispatch.go) and the peer ops still use.
// These tests originally lived in delete_test.go before the per-Instance
// Delete state machine moved to workload/ops (part 2 slice 4c of N).
// Kept in this package because they exercise dispatch.go +
// extraInstanceIndices, which remain in the omenative/ops package.

func TestExtraInstanceIndices(t *testing.T) {
	isvc := &v1beta1.InferenceService{
		Status: v1beta1.InferenceServiceStatus{
			Components: map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
				v1beta1.EngineComponent: {
					OMENative: &v1beta1.OMENativeComponentStatus{
						InstanceStatuses: []v1beta1.OMENativeInstanceStatus{
							{Index: 0}, {Index: 1}, {Index: 2},
						},
					},
				},
			},
		},
	}
	// Plan covers only index 0; status carries 0, 1, 2 — so 1 and 2 are extras.
	planWithOnlyZero := core.ComponentPlan{
		Component: v1beta1.EngineComponent,
		Instances: []core.InstancePlan{{Index: 0}},
	}
	got := extraInstanceIndices(isvc, v1beta1.EngineComponent, planWithOnlyZero)
	if len(got) != 2 {
		t.Fatalf("len: got %d want 2 (indices 1 and 2)", len(got))
	}
	hit := map[int32]bool{}
	for _, i := range got {
		hit[i] = true
	}
	if !hit[1] || !hit[2] {
		t.Errorf("got %v want {1,2}", got)
	}

	// No extras when plan covers every status index.
	planWithAll := core.ComponentPlan{
		Component: v1beta1.EngineComponent,
		Instances: []core.InstancePlan{{Index: 0}, {Index: 1}, {Index: 2}},
	}
	if extras := extraInstanceIndices(isvc, v1beta1.EngineComponent, planWithAll); len(extras) != 0 {
		t.Errorf("expected zero extras when plan covers all status indices, got %v", extras)
	}

	// Migration-in-flight: source at index 0 (Phase=Migrating), surge
	// at index 3 (Operation=Migrate). Neither should appear in extras
	// even when the plan doesn't cover them.
	migIsvc := &v1beta1.InferenceService{
		Status: v1beta1.InferenceServiceStatus{
			Components: map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
				v1beta1.EngineComponent: {
					OMENative: &v1beta1.OMENativeComponentStatus{
						InstanceStatuses: []v1beta1.OMENativeInstanceStatus{
							{Index: 0, Phase: v1beta1.OMENativeInstanceMigrating},
							{Index: 3, Phase: v1beta1.OMENativeInstanceCreating, Operation: &v1beta1.InstanceOperation{
								Type: v1beta1.InstanceOperationMigrate,
							}},
						},
					},
				},
			},
		},
	}
	planEmpty := core.ComponentPlan{Component: v1beta1.EngineComponent}
	if extras := extraInstanceIndices(migIsvc, v1beta1.EngineComponent, planEmpty); len(extras) != 0 {
		t.Errorf("migration-in-flight indices must not be extras; got %v", extras)
	}

	// Scoped to component: a Decoder status entry doesn't bleed into Engine extras.
	isvc.Status.Components[v1beta1.DecoderComponent] = v1beta1.ComponentStatusSpec{
		OMENative: &v1beta1.OMENativeComponentStatus{
			InstanceStatuses: []v1beta1.OMENativeInstanceStatus{{Index: 7}},
		},
	}
	got = extraInstanceIndices(isvc, v1beta1.EngineComponent, core.ComponentPlan{Component: v1beta1.EngineComponent})
	for _, i := range got {
		if i == 7 {
			t.Errorf("decoder index 7 leaked into engine extras: %v", got)
		}
	}
}

func TestReconcileComponentOps_ScaleDownDrivesDelete(t *testing.T) {
	// MinReplicas=1, but status carries InstanceStatus for index 1
	// (left over from a previous replicas=2 reconcile). Dispatch
	// should drive Delete for index 1 before running Create for index 0.
	// Tests ReconcileComponentOps (the post-plan dispatcher) rather
	// than the top-level ReconcileComponent — that lives in omenative
	// and would close a cycle if pulled in here.
	resetExpectations(t)
	isvc := minimalISVC("llama-70b", "prod", 1)
	isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
		v1beta1.EngineComponent: {
			OMENative: &v1beta1.OMENativeComponentStatus{
				InstanceStatuses: []v1beta1.OMENativeInstanceStatus{
					{Index: 0, Phase: v1beta1.OMENativeInstanceReady},
					{Index: 1, Phase: v1beta1.OMENativeInstanceReady},
				},
			},
		},
	}
	c := newFakeClientWithDiscovery(t, isvc)
	p := minimalParams(t, isvc, c)
	plan, err := core.BuildPlan(p)
	if err != nil {
		t.Fatalf("BuildPlan: %v", err)
	}

	result, err := ReconcileComponentOps(context.Background(), p, plan, nil)
	if err != nil {
		t.Fatalf("ReconcileComponentOps: %v", err)
	}
	// Index 1 had no live pods, so Delete returns done=true on first
	// pass and the dispatcher proceeds to Create. Create on an empty
	// cluster produces pods + a requeue.
	if result.RequeueAfter == 0 {
		t.Errorf("expected requeue from Create after scale-down completed, got %+v", result)
	}

	// Status for index 1 should be gone.
	fresh := &v1beta1.InferenceService{}
	_ = c.Get(context.Background(), client.ObjectKeyFromObject(isvc), fresh)
	cs := fresh.Status.Components[v1beta1.EngineComponent]
	for _, s := range cs.OMENative.InstanceStatuses {
		if s.Index == 1 {
			t.Errorf("InstanceStatus for index 1 should have been removed, got %+v", s)
		}
	}
}
