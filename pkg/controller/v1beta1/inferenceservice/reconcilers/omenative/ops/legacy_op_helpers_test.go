package ops

import (
	"context"
	"testing"

	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/core"
)

// Tests for the omenative dispatcher (dispatch.go) entry point
// ReconcileComponentOps. The original delete-state-machine test moved
// to workload/ops (part 2 slice 4c of N); this case stays here because
// it exercises the omenative-adapter dispatcher shim end-to-end against
// a v1beta1 ISVC fixture.

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
