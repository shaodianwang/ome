package inferencereplica

import (
	"context"
	"reflect"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/podreadiness"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	workloadtypes "github.com/ome-projects/ome/pkg/controller/v1beta1/workload/types"
)

func requestFor(namespace, name string) reconcile.Request {
	return reconcile.Request{NamespacedName: types.NamespacedName{Namespace: namespace, Name: name}}
}

// TestPeerReplicasOf_EnqueuesTheServingPeersOfTheParent pins the mapping a
// peer's transition takes back to the replicas that pair on it: every
// serving peer of the same parent, never the replica itself, and nothing
// for a replica outside a rollout group.
func TestPeerReplicasOf_EnqueuesTheServingPeersOfTheParent(t *testing.T) {
	ctx := context.Background()
	engine := projectedIR(v1beta1.EngineComponent, "7", "e1", "e1")
	decoder := projectedIR(v1beta1.DecoderComponent, "7", "d1", "d1")

	t.Run("engine and decoder wake each other", func(t *testing.T) {
		r, _ := newReconciler(t, engine, decoder, pdParent())
		if got, want := r.peerReplicasOf(ctx, decoder), []reconcile.Request{requestFor("default", "llama-engine")}; !reflect.DeepEqual(got, want) {
			t.Errorf("decoder event maps to %v, want %v", got, want)
		}
		if got, want := r.peerReplicasOf(ctx, engine), []reconcile.Request{requestFor("default", "llama-decoder")}; !reflect.DeepEqual(got, want) {
			t.Errorf("engine event maps to %v, want %v", got, want)
		}
	})

	t.Run("a declared router is a peer too", func(t *testing.T) {
		parent := pdParent()
		parent.Spec.Router = &v1beta1.RouterSpec{}
		r, _ := newReconciler(t, engine, decoder, parent)
		want := []reconcile.Request{requestFor("default", "llama-decoder"), requestFor("default", "llama-router")}
		if got := r.peerReplicasOf(ctx, engine); !reflect.DeepEqual(got, want) {
			t.Errorf("engine event maps to %v, want %v", got, want)
		}
	})

	t.Run("no rollout group declares no peers", func(t *testing.T) {
		parent := pdParent()
		parent.Spec.Rollout = nil
		r, _ := newReconciler(t, engine, decoder, parent)
		if got := r.peerReplicasOf(ctx, engine); got != nil {
			t.Errorf("a parent without rollout groups maps to %v, want nothing", got)
		}
	})

	t.Run("a standalone replica and a missing parent map to nothing", func(t *testing.T) {
		standalone := projectedIR(v1beta1.EngineComponent, "7", "e1", "e1")
		standalone.Name = "solo-engine"
		standalone.Spec.ParentRef = nil
		r, _ := newReconciler(t, standalone, engine)
		if got := r.peerReplicasOf(ctx, standalone); got != nil {
			t.Errorf("standalone replica maps to %v, want nothing", got)
		}
		if got := r.peerReplicasOf(ctx, engine); got != nil {
			t.Errorf("replica with an unresolvable parent maps to %v, want nothing", got)
		}
		if got := r.peerReplicasOf(ctx, &corev1.Pod{}); got != nil {
			t.Errorf("non-replica object maps to %v, want nothing", got)
		}
	})
}

// TestPeerRevisionPredicate_AdmitsOnlyPeerRevisionInputs pins which peer
// transitions wake a sibling: the ones its peer-revision resolution reads.
// Counter churn on a busy peer must not.
func TestPeerRevisionPredicate_AdmitsOnlyPeerRevisionInputs(t *testing.T) {
	base := projectedIR(v1beta1.DecoderComponent, "7", "d1", "d2")
	base.Status.ReadyReplicas = 1
	p := peerRevisionPredicate()

	cases := []struct {
		name   string
		mutate func(ir *v1beta1.InferenceReplica)
		want   bool
	}{
		{"unchanged", func(*v1beta1.InferenceReplica) {}, false},
		{"counter churn", func(ir *v1beta1.InferenceReplica) { ir.Status.ReadyReplicas = 2; ir.Status.ServingReplicas = 2 }, false},
		{"status observes the generation", func(ir *v1beta1.InferenceReplica) { ir.Status.ObservedGeneration++ }, true},
		{"generation bumps", func(ir *v1beta1.InferenceReplica) { ir.Generation++ }, true},
		{"projection reflects a new parent generation", func(ir *v1beta1.InferenceReplica) {
			ir.Annotations[constants.InferenceReplicaParentGenerationAnnotationKey] = "8"
		}, true},
		{"update revision moves", func(ir *v1beta1.InferenceReplica) { ir.Status.UpdateRevision = ir.Name + "-d3" }, true},
		{"current revision promotes", func(ir *v1beta1.InferenceReplica) { ir.Status.CurrentRevision = ir.Status.UpdateRevision }, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			updated := base.DeepCopy()
			tc.mutate(updated)
			if got := p.Update(event.UpdateEvent{ObjectOld: base, ObjectNew: updated}); got != tc.want {
				t.Errorf("Update admitted=%v, want %v", got, tc.want)
			}
		})
	}
	if !p.Create(event.CreateEvent{Object: base}) || !p.Delete(event.DeleteEvent{Object: base}) {
		t.Error("a peer appearing or vanishing must wake its siblings")
	}
}

// TestReconcile_PeerStatusCatchUpReleasesAHeldReplacement replays the
// stall the peer watch exists to prevent. A Component mid-surge holds its
// pass while a serving peer's status lags the peer's generation, and the
// replacement it already has ContainersReady stays out of rotation for as
// long as nothing reconciles it again. The peer's catch-up is the event
// that must do so: it maps back to the held replica, whose next pass admits
// the replacement to serving.
func TestReconcile_PeerStatusCatchUpReleasesAHeldReplacement(t *testing.T) {
	ctx := context.Background()
	// Control: with a fresh peer the same pass admits the replacement at
	// once, so a replacement left unadmitted below is the peer hold's doing.
	{
		engine, source, replacement := midSurgeEngine(t)
		decoder := projectedIR(v1beta1.DecoderComponent, "7", "d1", "d1")
		r, c := newReconciler(t, engine, decoder, pdParent(), source, replacement)
		if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(engine)}); err != nil {
			t.Fatalf("fresh-peer reconcile: %v", err)
		}
		if !podIsServing(t, c, replacement) {
			t.Fatal("with a fresh peer the first pass must admit the ContainersReady replacement")
		}
	}

	engine, source, replacement := midSurgeEngine(t)
	// The decoder's spec moved and its reconciler has not observed it yet.
	decoder := projectedIR(v1beta1.DecoderComponent, "7", "d1", "d1")
	decoder.Generation = decoder.Status.ObservedGeneration + 1

	r, c := newReconciler(t, engine, decoder, pdParent(), source, replacement)
	engineReq := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(engine)}

	if _, err := r.Reconcile(ctx, engineReq); err != nil {
		t.Fatalf("held reconcile: %v", err)
	}
	if podIsServing(t, c, replacement) {
		t.Fatal("the pass must hold while the peer lags; the replacement was admitted against an unresolved pairing")
	}

	// The decoder's reconciler catches up. That transition, and nothing
	// else, has to bring the engine back.
	fresh := &v1beta1.InferenceReplica{}
	if err := c.Get(ctx, client.ObjectKeyFromObject(decoder), fresh); err != nil {
		t.Fatalf("get decoder: %v", err)
	}
	fresh.Status.ObservedGeneration = fresh.Generation
	if err := c.Status().Update(ctx, fresh); err != nil {
		t.Fatalf("decoder status catch-up: %v", err)
	}
	woken := r.peerReplicasOf(ctx, fresh)
	if len(woken) != 1 || woken[0] != engineReq {
		t.Fatalf("the peer's catch-up maps to %v, want the held engine %v", woken, engineReq)
	}

	if _, err := r.Reconcile(ctx, engineReq); err != nil {
		t.Fatalf("released reconcile: %v", err)
	}
	if !podIsServing(t, c, replacement) {
		t.Fatal("once the peer is fresh the pass must admit the ContainersReady replacement to serving")
	}
}

// midSurgeEngine is a projected engine replica one pass into a
// SurgeThenDrain update of its only Instance: the source pod serves at
// ordinal 0 on the previous revision and the replacement at ordinal 1 is
// ContainersReady on the target, waiting to be admitted to serving.
func midSurgeEngine(t *testing.T) (*v1beta1.InferenceReplica, *corev1.Pod, *corev1.Pod) {
	t.Helper()
	engine := baselineIR("llama-engine", "default", 1)
	engine.Annotations[constants.InferenceReplicaParentGenerationAnnotationKey] = "7"
	target := targetRevisionNameFor(t, engine)
	engine.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{{
		Index: 0, Incarnation: 1, Phase: v1beta1.OMENativeInstanceUpdating,
		RunningRevision: "llama-engine-a1b2c3d4", TargetRevision: target,
		Operation: &v1beta1.InstanceOperation{
			ID: "update-0", Type: v1beta1.InstanceOperationUpdate,
			Step: workloadtypes.UpdateStepSurge, TargetRevision: target,
		},
	}}
	source := podForIR(engine, 0, string(v1beta1.RunnerNameDefault), 0, true, true)
	source.Labels[query.LabelRevisionHash] = "a1b2c3d4"
	replacement := podForIR(engine, 0, string(v1beta1.RunnerNameDefault), 1, true, false)
	replacement.Labels[query.LabelRevisionHash] = query.RevisionHashFromControllerRevisionName(target)
	return engine, source, replacement
}

func podIsServing(t *testing.T, c client.Client, pod *corev1.Pod) bool {
	t.Helper()
	fresh := &corev1.Pod{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(pod), fresh); err != nil {
		t.Fatalf("get pod %s: %v", pod.Name, err)
	}
	return podreadiness.IsServing(fresh)
}
