package irprojector

import (
	"context"
	"testing"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestComponentIRStatus_ReturnsAuthoritativeStatus(t *testing.T) {
	ir := &v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: InferenceReplicaName("svc", v1beta1.EngineComponent)},
		Status: v1beta1.InferenceReplicaStatus{
			Replicas: 3, ReadyReplicas: 2,
			InstanceStatuses: []v1beta1.OMENativeInstanceStatus{{Index: 0, Phase: v1beta1.OMENativeInstanceReady}},
		},
	}
	c := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(ir).Build()

	got, err := ComponentIRStatus(context.Background(), c, "ns", "svc", v1beta1.EngineComponent)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if got == nil || got.Replicas != 3 || len(got.InstanceStatuses) != 1 {
		t.Fatalf("want authoritative IR status (3 replicas, 1 instance); got %+v", got)
	}
}

func TestComponentIRStatus_MissingIRReturnsNil(t *testing.T) {
	c := fake.NewClientBuilder().WithScheme(testScheme(t)).Build()
	got, err := ComponentIRStatus(context.Background(), c, "ns", "svc", v1beta1.EngineComponent)
	if err != nil {
		t.Fatalf("missing IR must not error; got %v", err)
	}
	if got != nil {
		t.Fatalf("missing IR must return nil status; got %+v", got)
	}
}

func TestComponentIRStatus_NilReaderReturnsNil(t *testing.T) {
	// A nil reader must degrade to "no authoritative status" (nil, nil)
	// rather than panicking a reconcile.
	got, err := ComponentIRStatus(context.Background(), nil, "ns", "svc", v1beta1.EngineComponent)
	if err != nil || got != nil {
		t.Fatalf("nil reader must return (nil, nil); got (%+v, %v)", got, err)
	}
}
