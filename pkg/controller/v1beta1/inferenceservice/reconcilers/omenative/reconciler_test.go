package omenative

import (
	"context"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/revision"
)

// TestReconcileComponent_NilClientRejected pins that the entry function
// short-circuits when Client is missing.
func TestReconcileComponent_NilClientRejected(t *testing.T) {
	_, err := ReconcileComponent(context.Background(), ReconcileParams{
		Component:    v1beta1.EngineComponent,
		ComponentExt: &v1beta1.ComponentExtensionSpec{},
	})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "nil client") {
		t.Fatalf("unexpected error: %v", err)
	}
}

// TestReconcileComponent_MultiPodDispatchesToCreate pins the multi-pod
// path: with a leader+worker plan, ReconcileComponent must reach Create
// and request a requeue (multi-pass) rather than failing with the
// pre-multi-node ErrMultiPodNotYetSupported short-circuit.
func TestReconcileComponent_MultiPodDispatchesToCreate(t *testing.T) {
	resetExpectations(t)
	isvc := minimalISVC("llama-70b", "prod", 1)
	c := newFakeClientWithDiscovery(t, isvc)
	p := minimalParams(t, isvc, c)
	// Multi-pod is leader + worker with WorkerSize > 0. The Engine spec
	// drives this at the dispatch site (components/engine.go); here we
	// just set MultiPod / WorkerSize / WorkerPodSpec directly because the
	// reconciler is the unit under test.
	p.MultiPod = true
	p.WorkerSize = 1
	p.WorkerPodSpec = &corev1.PodSpec{
		Containers: []corev1.Container{{Name: "worker", Image: "llama-worker:v1"}},
	}

	result, err := ReconcileComponent(context.Background(), p)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.RequeueAfter == 0 {
		t.Errorf("expected Requeue (Create is multi-pass), got %+v", result)
	}
}

// TestReconcileComponent_DispatchesToCreate pins the wiring: with a valid
// single-pod plan and a fake client, ReconcileComponent reaches Create
// and the empty cluster gets the desired pod.
func TestReconcileComponent_DispatchesToCreate(t *testing.T) {
	resetExpectations(t)
	isvc := minimalISVC("llama-70b", "prod", 1)
	c := newFakeClientWithDiscovery(t, isvc)
	result, err := ReconcileComponent(context.Background(), minimalParams(t, isvc, c))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.RequeueAfter == 0 {
		t.Errorf("expected Requeue (Create is multi-pass), got %+v", result)
	}
}

// TestReconcileComponent_RevisionCollisionBumpsAndRetries exercises the
// collision-retry wrapper in ensureRevisionWithCollisionRetry: a
// pre-existing CR at the deterministic hash name with mismatching Data
// must trigger a CollisionCount bump and a successful salted retry. The
// test lives here because the wrapper is defined in reconciler.go and
// runs outside the per-Component dispatch in ops/.
func TestReconcileComponent_RevisionCollisionBumpsAndRetries(t *testing.T) {
	resetExpectations(t)
	isvc := isvcReadyAtIncarnation("llama-70b", "prod", 1)
	targetSpec := targetSpecImage("llama:v2")
	hash, _, _ := revision.Hash(targetSpec, nil, nil)
	conflictingName := revision.Name(isvc.Name, v1beta1.EngineComponent, hash)
	conflicting := &appsv1.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{
			Name:      conflictingName,
			Namespace: isvc.Namespace,
			Labels:    revision.Labels(isvc.Name, v1beta1.EngineComponent),
		},
		Data:     runtime.RawExtension{Raw: []byte(`{"podSpec":{"containers":[{"name":"other","image":"other:v1"}]},"podMeta":null}`)},
		Revision: 1,
	}
	c := newFakeClientWithDiscovery(t, isvc, conflicting)

	p := minimalParams(t, isvc, c)
	p.PodSpec = targetSpec

	if _, err := ReconcileComponent(context.Background(), p); err != nil {
		t.Fatalf("ReconcileComponent: %v", err)
	}

	// Status should now show CollisionCount=1.
	fresh := &v1beta1.InferenceService{}
	_ = c.Get(context.Background(), client.ObjectKeyFromObject(isvc), fresh)
	cs := fresh.Status.Components[v1beta1.EngineComponent]
	if cs.OMENative == nil || cs.OMENative.CollisionCount == nil || *cs.OMENative.CollisionCount != 1 {
		t.Fatalf("CollisionCount: got %+v, want 1", cs.OMENative.CollisionCount)
	}

	// A new CR with a salt-shifted hash should now exist alongside the
	// conflicting one.
	list := &appsv1.ControllerRevisionList{}
	_ = c.List(context.Background(), list, client.InNamespace("prod"))
	if len(list.Items) != 2 {
		t.Errorf("CR count after retry: got %d want 2 (conflicting + salt-shifted)", len(list.Items))
	}
	saltedHash, _, _ := revision.Hash(targetSpec, nil, cs.OMENative.CollisionCount)
	saltedName := revision.Name(isvc.Name, v1beta1.EngineComponent, saltedHash)
	found := false
	names := make([]string, 0, len(list.Items))
	for _, cr := range list.Items {
		names = append(names, cr.Name)
		if cr.Name == saltedName {
			found = true
		}
	}
	if !found {
		t.Errorf("salt-shifted CR %s not present; got %v", saltedName, names)
	}
}
