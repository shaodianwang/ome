package omenative

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
)

// conflictOnce returns a SubResourceUpdate interceptor that injects a
// 409 Conflict on the first call, then forwards subsequent calls to
// the underlying client. Used by every C2 retry test to confirm the
// retry.RetryOnConflict wrapper actually lands the write on attempt
// #2.
func conflictOnce(counter *int64) interceptor.Funcs {
	return interceptor.Funcs{
		SubResourceUpdate: func(ctx context.Context, c client.Client, subResourceName string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
			if atomic.AddInt64(counter, 1) == 1 {
				return apierrors.NewConflict(
					schema.GroupResource{Group: "ome.io", Resource: "inferenceservices"},
					obj.GetName(),
					errors.New("simulated concurrent write"),
				)
			}
			return c.Status().Update(ctx, obj, opts...)
		},
	}
}

func newStatusRetryTestParams(t *testing.T, isvc *v1beta1.InferenceService, counter *int64) ReconcileParams {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add corev1: %v", err)
	}
	if err := appsv1.AddToScheme(scheme); err != nil {
		t.Fatalf("add appsv1: %v", err)
	}
	if err := discoveryv1.AddToScheme(scheme); err != nil {
		t.Fatalf("add discoveryv1: %v", err)
	}
	if err := v1beta1.AddToScheme(scheme); err != nil {
		t.Fatalf("add v1beta1: %v", err)
	}
	base := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(isvc).
		WithStatusSubresource(&v1beta1.InferenceService{}).
		Build()
	c := interceptor.NewClient(base, conflictOnce(counter))
	return ReconcileParams{
		ISVC:      isvc,
		Component: v1beta1.EngineComponent,
		Client:    c,
	}
}

func freshISVC(ns, name string) *v1beta1.InferenceService {
	return &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: ns,
			UID:       "isvc-uid",
		},
	}
}

func TestMutateInstanceStatus_RetriesOnConflict(t *testing.T) {
	var calls int64
	isvc := freshISVC("ns", "isvc")
	p := newStatusRetryTestParams(t, isvc, &calls)

	mutateCalls := 0
	err := mutateInstanceStatus(context.Background(), p, v1beta1.EngineComponent, 0,
		func(s *v1beta1.OMENativeInstanceStatus) bool {
			mutateCalls++
			s.Phase = v1beta1.OMENativeInstanceCreating
			return true
		})
	if err != nil {
		t.Fatalf("mutateInstanceStatus returned error after 409+retry: %v", err)
	}
	if calls != 2 {
		t.Errorf("expected 2 Update attempts (1 conflict + 1 success), got %d", calls)
	}
	if mutateCalls != 2 {
		t.Errorf("mutate callback should run once per attempt; got %d", mutateCalls)
	}

	// Verify the write actually landed.
	got := &v1beta1.InferenceService{}
	if err := p.Client.Get(context.Background(), client.ObjectKeyFromObject(isvc), got); err != nil {
		t.Fatalf("get after retry: %v", err)
	}
	cs := got.Status.Components[v1beta1.EngineComponent]
	if cs.OMENative == nil || len(cs.OMENative.InstanceStatuses) != 1 {
		t.Fatalf("InstanceStatus not landed: %+v", cs)
	}
	if cs.OMENative.InstanceStatuses[0].Phase != v1beta1.OMENativeInstanceCreating {
		t.Errorf("wrong Phase: got %s", cs.OMENative.InstanceStatuses[0].Phase)
	}
}

func TestRemoveInstanceStatus_RetriesOnConflict(t *testing.T) {
	var calls int64
	isvc := freshISVC("ns", "isvc")
	isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
		v1beta1.EngineComponent: {
			OMENative: &v1beta1.OMENativeComponentStatus{
				InstanceStatuses: []v1beta1.OMENativeInstanceStatus{
					{Index: 0, Phase: v1beta1.OMENativeInstanceDeleting},
				},
			},
		},
	}
	p := newStatusRetryTestParams(t, isvc, &calls)

	hadEntry, err := removeInstanceStatus(context.Background(), p, v1beta1.EngineComponent, 0)
	if err != nil {
		t.Fatalf("removeInstanceStatus returned error after 409+retry: %v", err)
	}
	if !hadEntry {
		t.Errorf("expected hadEntry=true on successful removal")
	}
	if calls != 2 {
		t.Errorf("expected 2 Update attempts, got %d", calls)
	}

	got := &v1beta1.InferenceService{}
	if err := p.Client.Get(context.Background(), client.ObjectKeyFromObject(isvc), got); err != nil {
		t.Fatalf("get after retry: %v", err)
	}
	if len(got.Status.Components[v1beta1.EngineComponent].OMENative.InstanceStatuses) != 0 {
		t.Errorf("InstanceStatus not removed after retry")
	}
}

func TestBumpCollisionCount_RetriesOnConflict(t *testing.T) {
	var calls int64
	isvc := freshISVC("ns", "isvc")
	p := newStatusRetryTestParams(t, isvc, &calls)

	got, err := bumpCollisionCount(context.Background(), p, v1beta1.EngineComponent)
	if err != nil {
		t.Fatalf("bumpCollisionCount returned error after 409+retry: %v", err)
	}
	if got == nil || *got != 1 {
		t.Errorf("expected CollisionCount=1, got %v", got)
	}
	if calls != 2 {
		t.Errorf("expected 2 Update attempts, got %d", calls)
	}

	// Verify the apiserver landed the bump.
	served := &v1beta1.InferenceService{}
	if err := p.Client.Get(context.Background(), client.ObjectKeyFromObject(isvc), served); err != nil {
		t.Fatalf("get after retry: %v", err)
	}
	cs := served.Status.Components[v1beta1.EngineComponent]
	if cs.OMENative == nil || cs.OMENative.CollisionCount == nil || *cs.OMENative.CollisionCount != 1 {
		t.Errorf("CollisionCount not persisted: %+v", cs)
	}

	// Verify the cached ISVC was also mirrored (the function does this
	// outside the retry, only on successful commit).
	if p.ISVC.Status.Components[v1beta1.EngineComponent].OMENative == nil ||
		p.ISVC.Status.Components[v1beta1.EngineComponent].OMENative.CollisionCount == nil {
		t.Errorf("cached ISVC not mirrored")
	}
}

func TestAggregateAndWriteStatus_RetriesOnConflict(t *testing.T) {
	var calls int64
	isvc := freshISVC("ns", "isvc")
	isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
		v1beta1.EngineComponent: {
			OMENative: &v1beta1.OMENativeComponentStatus{
				InstanceStatuses: []v1beta1.OMENativeInstanceStatus{
					{Index: 0, Phase: v1beta1.OMENativeInstanceReady},
				},
			},
		},
	}
	p := newStatusRetryTestParams(t, isvc, &calls)
	plan := ComponentPlan{
		Component: v1beta1.EngineComponent,
		Replicas:  1,
		Instances: []InstancePlan{{Index: 0, Incarnation: 1}},
	}

	if err := AggregateAndWriteStatus(context.Background(), p, plan, nil); err != nil {
		t.Fatalf("AggregateAndWriteStatus returned error after 409+retry: %v", err)
	}
	if calls != 2 {
		t.Errorf("expected 2 Update attempts, got %d", calls)
	}

	got := &v1beta1.InferenceService{}
	if err := p.Client.Get(context.Background(), client.ObjectKeyFromObject(isvc), got); err != nil {
		t.Fatalf("get after retry: %v", err)
	}
	cs := got.Status.Components[v1beta1.EngineComponent]
	if cs.OMENative.Replicas != 1 {
		t.Errorf("Replicas not aggregated: got %d", cs.OMENative.Replicas)
	}
	if cs.OMENative.LabelSelector == "" {
		t.Errorf("LabelSelector not set")
	}
}

func TestMutateInstanceStatus_NoWriteWhenMutateReturnsFalse(t *testing.T) {
	// Sanity: if the mutate callback returns false, no Update is
	// issued, no conflict, no retry, and the test interceptor's
	// counter stays at 0.
	var calls int64
	isvc := freshISVC("ns", "isvc")
	p := newStatusRetryTestParams(t, isvc, &calls)

	err := mutateInstanceStatus(context.Background(), p, v1beta1.EngineComponent, 0,
		func(s *v1beta1.OMENativeInstanceStatus) bool { return false })
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if calls != 0 {
		t.Errorf("expected 0 Update attempts when mutate returns false, got %d", calls)
	}
}
