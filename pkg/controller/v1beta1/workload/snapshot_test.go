package workload_test

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload"
)

// countingReader wraps a client.Reader and counts List calls, so tests can
// assert the snapshot Lists each source at most once (memoization).
type countingReader struct {
	client.Client
	lists int
}

func (c *countingReader) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	c.lists++
	return c.Client.List(ctx, list, opts...)
}

func snapshotScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := corev1.AddToScheme(s); err != nil {
		t.Fatalf("add corev1: %v", err)
	}
	return s
}

// TestObservedSnapshot_LivePodsMemoized: the live source is Listed at most
// once per snapshot no matter how many passes call LivePods — the property
// that keeps the read count identical to the per-pass helpers (and off the
// apiserver hot path on large clusters).
func TestObservedSnapshot_LivePodsMemoized(t *testing.T) {
	fc := fake.NewClientBuilder().WithScheme(snapshotScheme(t)).Build()
	cr := &countingReader{Client: fc}
	deps := workload.Deps{Client: fc, APIReader: cr}
	input := workload.ReconcileInput{Key: workload.Key{Namespace: "ns", Component: workload.ComponentEngine, OwnerName: "own"}}

	snap := workload.NewObservedSnapshot(deps, input, workload.ComponentEngine, nil)
	for i := 0; i < 3; i++ {
		if _, err := snap.LivePods(context.Background()); err != nil {
			t.Fatalf("LivePods: %v", err)
		}
	}
	if cr.lists != 1 {
		t.Errorf("live source Listed %d times across 3 LivePods calls, want 1 (memoized)", cr.lists)
	}

	// A fresh snapshot re-reads (memoization is per-reconcile, not global).
	snap2 := workload.NewObservedSnapshot(deps, input, workload.ComponentEngine, nil)
	if _, err := snap2.LivePods(context.Background()); err != nil {
		t.Fatalf("LivePods (snap2): %v", err)
	}
	if cr.lists != 2 {
		t.Errorf("fresh snapshot must re-List: got %d want 2", cr.lists)
	}
}

// TestObservedSnapshot_EvidenceDeadline: EvidenceFor reports DeadlinePassed
// for a transient-phase instance whose Operation.Deadline is in the past,
// and not otherwise. Evidence only — no writes. (No stuck pod: the empty
// client yields no pods, so StuckPod stays nil.)
func TestObservedSnapshot_EvidenceDeadline(t *testing.T) {
	fc := fake.NewClientBuilder().WithScheme(snapshotScheme(t)).Build()
	deps := workload.Deps{Client: fc, APIReader: fc}
	input := workload.ReconcileInput{Key: workload.Key{Namespace: "ns", Component: workload.ComponentEngine, OwnerName: "own"}}

	now := time.Date(2026, 7, 29, 12, 0, 0, 0, time.UTC)
	insts := []workload.InstanceStatus{
		{ // deadline in the past → DeadlinePassed
			Index: 0, Phase: workload.InstancePhaseUpdating,
			Operation: &workload.InstanceOperation{Deadline: metav1.NewTime(now.Add(-time.Minute))},
		},
		{ // deadline in the future → not passed
			Index: 1, Phase: workload.InstancePhaseUpdating,
			Operation: &workload.InstanceOperation{Deadline: metav1.NewTime(now.Add(time.Minute))},
		},
	}
	snap := workload.NewObservedSnapshot(deps, input, workload.ComponentEngine, insts)

	if ev := snap.EvidenceFor(context.Background(), 0, now, 30*time.Second); !ev.DeadlinePassed || ev.StuckPod != nil {
		t.Errorf("instance 0: got DeadlinePassed=%v StuckPod=%v, want true/nil", ev.DeadlinePassed, ev.StuckPod)
	}
	if ev := snap.EvidenceFor(context.Background(), 1, now, 30*time.Second); ev.DeadlinePassed {
		t.Errorf("instance 1: future deadline must not be passed")
	}
}
