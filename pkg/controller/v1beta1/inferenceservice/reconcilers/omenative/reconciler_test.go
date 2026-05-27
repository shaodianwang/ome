package omenative

import (
	"context"
	"strings"
	"testing"
	"time"

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

// TestReconcileComponent_SequentialGateDenialReturnsRequeue pins the
// post-PR-#240 follow-up contract: when CheckSequentialGate denies the
// per-Instance Update (because another Component is the active
// Sequential element, or the soak window between Components is still
// open), the OMENative reconciler MUST surface a non-zero RequeueAfter
// so the parent component reconciler can re-fire the loop in a few
// seconds — without it, the rollout sits idle until an unrelated
// watch event happens to wake controller-runtime, which on the dev
// cluster has been observed to take 60-90s post-decoder convergence.
//
// Set-up mirrors the moment-of-bump shape PR #240 already exercises in
// coordination/sequential_gate_test.go: isvc.Generation advanced one
// step (operator just bumped both Components), both Components carry
// stale ObservedGeneration. activeSequentialComponent picks decoder
// (first in Order), engine is denied, and the dispatcher returns
// ratioGateRequeueInterval.
func TestReconcileComponent_SequentialGateDenialReturnsRequeue(t *testing.T) {
	resetExpectations(t)
	isvc := isvcReadyAtIncarnation("v-seq", "prod", 1)
	// Add the decoder spec so collectOMENativeComponents sees both.
	// minimalISVC only sets Engine; Sequential gating needs Decoder too.
	mr := 1
	isvc.Spec.Decoder = &v1beta1.DecoderSpec{
		ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
			MinReplicas: &mr,
		},
	}
	// Sequential group [decoder, engine] with a 30s soak — the soak
	// window doesn't fire here because the gate routes off the active
	// selector (decoder is in flight, engine is non-active).
	soak := metav1.Duration{Duration: 30 * time.Second}
	isvc.Spec.RolloutCoordination = &v1beta1.RolloutCoordinationSpec{
		Groups: []v1beta1.RolloutCoordinationGroup{{
			Components: []v1beta1.ComponentType{v1beta1.DecoderComponent, v1beta1.EngineComponent},
			Policy:     v1beta1.CoordinationPolicySequential,
			Order:      []v1beta1.ComponentType{v1beta1.DecoderComponent, v1beta1.EngineComponent},
			Soak:       &soak,
		}},
	}
	// Operator just bumped both Components — isvc.Generation is N+1 but
	// neither Component's status has caught up yet.
	isvc.Generation = 2
	if isvc.Status.Components == nil {
		isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{}
	}
	engStatus := isvc.Status.Components[v1beta1.EngineComponent]
	engStatus.OMENative.ObservedGeneration = 1 // stale → generationSkew=true
	// RunningRevision on the existing Instance is the OLD canonical
	// revision name. detectUpdateTrigger uses it to decide whether the
	// pod needs to be re-rolled; setting it to a known stale name forces
	// needs=true.
	engStatus.OMENative.InstanceStatuses[0].RunningRevision = "v-seq-engine-oldhash"
	isvc.Status.Components[v1beta1.EngineComponent] = engStatus
	isvc.Status.Components[v1beta1.DecoderComponent] = v1beta1.ComponentStatusSpec{
		OMENative: &v1beta1.OMENativeComponentStatus{
			ObservedGeneration: 1, // stale → generationSkew=true → decoder.RolloutInFlight=true
			Replicas:           1,
		},
	}

	c := newFakeClientWithDiscovery(t, isvc)
	p := minimalParams(t, isvc, c)
	// Brand-new target spec (different image) — the per-Instance
	// dispatcher sees needs=true and consults CheckSequentialGate.
	p.PodSpec = targetSpecImage("v-seq-engine:NEW")

	result, err := ReconcileComponent(context.Background(), p)
	if err != nil {
		t.Fatalf("ReconcileComponent: %v", err)
	}
	// Engine is NOT the active Sequential Component (decoder is, by
	// generation-skew signal). The dispatcher must have hit the gate
	// denial branch and returned ratioGateRequeueInterval — which the
	// parent components/{engine,decoder,router}.go reconciler now
	// preserves end-to-end.
	if result.RequeueAfter == 0 && !result.Requeue {
		t.Errorf("expected non-zero requeue from Sequential gate denial, got %+v", result)
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
