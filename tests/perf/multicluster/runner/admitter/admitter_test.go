package admitter

import (
	"context"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/placement/protocol"
	"github.com/ome-projects/ome/tests/integration/util"
)

func derivedFixture(t *testing.T, floor int32) *v1beta1.InferenceService {
	t.Helper()
	policy := &v1beta1.PlacementExecutionPolicy{PlanID: "plan-a", Revision: 1, SourceUID: "source-a", ClusterUID: "cluster-a", ReplicaFloors: []v1beta1.PlacementComponentFloor{{Component: v1beta1.EngineComponent, Replicas: floor}}}
	raw, err := protocol.Encode(policy)
	if err != nil {
		t.Fatal(err)
	}
	return &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "service-a", UID: "derived-a", Generation: 1,
		Labels:      map[string]string{constants.PlacementOrigin: "source-a", constants.PlacementControlPlane: "control-a"},
		Annotations: map[string]string{constants.PlacementOriginUID: "source-a", constants.PlacementExecution: raw}}, Spec: v1beta1.InferenceServiceSpec{Engine: &v1beta1.EngineSpec{Runner: &v1beta1.RunnerSpec{Container: corev1.Container{Name: constants.MainContainerName, Image: "example/model-server:test"}}}}}
}

func TestAdmitterFollowsExecutionPolicy(t *testing.T) {
	for _, tt := range []struct {
		name    string
		floor   int32
		refresh bool
	}{
		{name: "one replica", floor: 1},
		{name: "complete floor", floor: 3},
		{name: "new plan revision", floor: 2, refresh: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			derived := derivedFixture(t, tt.floor)
			scheme := runtime.NewScheme()
			if err := v1beta1.AddToScheme(scheme); err != nil {
				t.Fatal(err)
			}
			if err := corev1.AddToScheme(scheme); err != nil {
				t.Fatal(err)
			}
			statusWrites := 0
			c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(derived).
				WithStatusSubresource(&v1beta1.InferenceService{}, &v1beta1.InferenceReplica{}).
				WithInterceptorFuncs(interceptor.Funcs{Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
					obj.SetUID(types.UID(obj.GetName()))
					obj.SetGeneration(1)
					return c.Create(ctx, obj, opts...)
				}, SubResourceUpdate: func(ctx context.Context, c client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
					statusWrites++
					return c.SubResource(sub).Update(ctx, obj, opts...)
				}}).Build()
			now := time.Unix(100, 0)
			r := New(c, time.Second, 1, func() time.Time { return now }, nil)
			req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(derived)}
			got, err := r.Reconcile(t.Context(), req)
			if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(time.Second, got.RequeueAfter); diff != "" {
				t.Error(diff)
			}
			if diff := cmp.Diff(0, statusWrites); diff != "" {
				t.Error(diff)
			}
			now = now.Add(time.Second)
			if _, err := r.Reconcile(t.Context(), req); err != nil {
				t.Fatal(err)
			}
			if tt.refresh {
				if err := c.Get(t.Context(), req.NamespacedName, derived); err != nil {
					t.Fatal(err)
				}
				policy, err := protocol.FromDerived(derived)
				if err != nil {
					t.Fatal(err)
				}
				policy.Revision++
				policy.PlanID = "plan-b"
				raw, err := protocol.Encode(policy)
				if err != nil {
					t.Fatal(err)
				}
				derived.Annotations[constants.PlacementExecution] = raw
				if err := c.Update(t.Context(), derived); err != nil {
					t.Fatal(err)
				}
				if _, err := r.Reconcile(t.Context(), req); err != nil {
					t.Fatal(err)
				}
			}
			ir := &v1beta1.InferenceReplica{}
			if err := c.Get(t.Context(), client.ObjectKey{Namespace: derived.Namespace, Name: componentIRName(derived.Name, v1beta1.EngineComponent)}, ir); err != nil {
				t.Fatal(err)
			}
			policy, err := protocol.FromDerived(derived)
			if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(policy, ir.Spec.PlacementExecution); diff != "" {
				t.Errorf("acknowledged policy (-want +got):\n%s", diff)
			}
			if diff := cmp.Diff(ptr.To(tt.floor), ir.Spec.Replicas); diff != "" {
				t.Error(diff)
			}
			if !metav1.IsControlledBy(ir, derived) {
				t.Fatal("component has wrong owner")
			}
			if diff := cmp.Diff(map[string]string{constants.InferenceServicePodLabelKey: derived.Name, constants.PlacementOrigin: "source-a", constants.PlacementControlPlane: "control-a"}, ir.Labels); diff != "" {
				t.Error(diff)
			}
			rows, err := util.DecodedInstanceStatuses(ir)
			if err != nil {
				t.Fatal(err)
			}
			wantRows := make([]v1beta1.OMENativeInstanceStatus, int(tt.floor))
			for i := range wantRows {
				wantRows[i] = v1beta1.OMENativeInstanceStatus{Index: int32(i), Phase: v1beta1.OMENativeInstancePending, Admitted: true, RunningRevision: ir.Name + "-fixture", PodCount: 1}
			}
			if diff := cmp.Diff(wantRows, rows); diff != "" {
				t.Errorf("admitted rows (-want +got):\n%s", diff)
			}
			pods := &corev1.PodList{}
			if err := c.List(t.Context(), pods, client.InNamespace(ir.Namespace)); err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(int(tt.floor), len(pods.Items)); diff != "" {
				t.Fatalf("physical instances (-want +got):\n%s", diff)
			}
			for _, pod := range pods.Items {
				if !metav1.IsControlledBy(&pod, ir) {
					t.Fatal("Pod is not owned by its component")
				}
			}
			before := statusWrites
			if _, err := r.Reconcile(t.Context(), req); err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(before, statusWrites); diff != "" {
				t.Errorf("steady state writes: %s", diff)
			}
			if err := c.Get(t.Context(), req.NamespacedName, derived); err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(int64(0), derived.Status.ObservedGeneration); diff != "" {
				t.Errorf("member summary changed during admission: %s", diff)
			}
		})
	}
}

func TestAdmitterRejectsMissingAuthority(t *testing.T) {
	for _, tt := range []struct {
		name   string
		mutate func(*v1beta1.InferenceService)
	}{
		{name: "ordinary local service", mutate: func(s *v1beta1.InferenceService) {
			delete(s.Annotations, constants.PlacementOriginUID)
			delete(s.Labels, constants.PlacementOrigin)
		}},
		{name: "missing member identity", mutate: func(s *v1beta1.InferenceService) { s.UID = "" }},
		{name: "malformed policy", mutate: func(s *v1beta1.InferenceService) { s.Annotations[constants.PlacementExecution] = "{" }},
		{name: "missing component", mutate: func(s *v1beta1.InferenceService) { s.Spec.Engine = nil }},
		{name: "unresolved runner", mutate: func(s *v1beta1.InferenceService) { s.Spec.Engine.Runner = nil }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s := derivedFixture(t, 1)
			tt.mutate(s)
			if _, err := admissionIR(s, v1beta1.EngineComponent); err == nil {
				t.Fatal("accepted unresolved admission authority")
			}
		})
	}
}

func TestAdmitterPreservesForeignComponent(t *testing.T) {
	for _, tt := range []struct {
		name     string
		ownerUID types.UID
	}{
		{name: "different member service", ownerUID: "other-service"},
		{name: "unowned component"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			derived := derivedFixture(t, 1)
			foreign, err := admissionIR(derived, v1beta1.EngineComponent)
			if err != nil {
				t.Fatal(err)
			}
			foreign.OwnerReferences[0].UID = tt.ownerUID
			if tt.ownerUID == "" {
				foreign.OwnerReferences = nil
			}
			scheme := runtime.NewScheme()
			if err := v1beta1.AddToScheme(scheme); err != nil {
				t.Fatal(err)
			}
			c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(foreign).Build()
			before := &v1beta1.InferenceReplica{}
			if err := c.Get(t.Context(), client.ObjectKeyFromObject(foreign), before); err != nil {
				t.Fatal(err)
			}
			r := New(c, 0, 1, time.Now, nil)
			if err := r.markAdmitted(t.Context(), derived); err == nil {
				t.Fatal("foreign component accepted")
			}
			after := &v1beta1.InferenceReplica{}
			if err := c.Get(t.Context(), client.ObjectKeyFromObject(foreign), after); err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(before, after); diff != "" {
				t.Errorf("foreign component changed (-want +got):\n%s", diff)
			}
		})
	}
}
