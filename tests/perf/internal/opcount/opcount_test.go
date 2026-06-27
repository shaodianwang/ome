package opcount

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	fakeclient "sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestClientCountsByVerbAndKind(t *testing.T) {
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "n"}}
	base := fakeclient.NewClientBuilder().WithObjects(pod).Build()

	counts := NewCounts()
	c := NewClient(base, counts)
	ctx := context.Background()

	if err := c.Get(ctx, client.ObjectKeyFromObject(pod), &corev1.Pod{}); err != nil {
		t.Fatalf("get: %v", err)
	}
	if err := c.List(ctx, &corev1.PodList{}); err != nil {
		t.Fatalf("list: %v", err)
	}
	if err := c.Create(ctx, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "p2", Namespace: "n"}}); err != nil {
		t.Fatalf("create: %v", err)
	}
	got := &corev1.Pod{}
	if err := c.Get(ctx, client.ObjectKeyFromObject(pod), got); err != nil {
		t.Fatalf("get2: %v", err)
	}
	got.Status.Phase = corev1.PodRunning
	if err := c.Status().Update(ctx, got); err != nil {
		t.Fatalf("status update: %v", err)
	}

	snap := counts.Snapshot()
	if snap["get"]["Pod"] != 2 {
		t.Errorf("get/Pod = %d, want 2", snap["get"]["Pod"])
	}
	if snap["list"]["Pod"] != 1 { // PodList -> Pod
		t.Errorf("list/Pod = %d, want 1", snap["list"]["Pod"])
	}
	if snap["create"]["Pod"] != 1 {
		t.Errorf("create/Pod = %d, want 1", snap["create"]["Pod"])
	}
	if snap["status-update"]["Pod"] != 1 {
		t.Errorf("status-update/Pod = %d, want 1", snap["status-update"]["Pod"])
	}
	if counts.Total() != 5 {
		t.Errorf("Total = %d, want 5", counts.Total())
	}
	if counts.TotalByVerb("get") != 2 {
		t.Errorf("TotalByVerb(get) = %d, want 2", counts.TotalByVerb("get"))
	}
}

func TestReaderCountsLiveReads(t *testing.T) {
	base := fakeclient.NewClientBuilder().Build()
	counts := NewCounts()
	r := NewReader(base, counts)
	ctx := context.Background()

	_ = r.List(ctx, &corev1.PodList{})
	_ = r.Get(ctx, client.ObjectKey{Name: "x", Namespace: "n"}, &corev1.Pod{})

	snap := counts.Snapshot()
	if snap["live-list"]["Pod"] != 1 {
		t.Errorf("live-list/Pod = %d, want 1", snap["live-list"]["Pod"])
	}
	if snap["live-get"]["Pod"] != 1 {
		t.Errorf("live-get/Pod = %d, want 1", snap["live-get"]["Pod"])
	}
}
