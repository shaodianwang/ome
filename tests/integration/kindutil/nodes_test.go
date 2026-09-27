package kindutil

import (
	"context"
	"slices"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

const otherTaintKey = "example.com/other"

// hourAgo is a whole-second instant so it survives the fake client's JSON
// round trip unchanged.
func hourAgo() metav1.Time {
	return metav1.NewTime(time.Now().Add(-time.Hour).Truncate(time.Second))
}

// fixtureNode carries one condition and one taint the helpers must leave
// alone: Ready, and a taint unrelated to anything the tests add or remove.
func fixtureNode() *corev1.Node {
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "w0"},
		Spec: corev1.NodeSpec{Taints: []corev1.Taint{
			{Key: otherTaintKey, Value: "1", Effect: corev1.TaintEffectNoSchedule},
		}},
		Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{{
			Type: corev1.NodeReady, Status: corev1.ConditionTrue,
			LastTransitionTime: hourAgo(), LastHeartbeatTime: hourAgo(),
		}}},
	}
}

func clientFor(node *corev1.Node) client.Client {
	return fake.NewClientBuilder().WithScheme(scheme.Scheme).
		WithObjects(node).WithStatusSubresource(node).Build()
}

func nodeClient(t *testing.T) client.Client {
	t.Helper()
	return clientFor(fixtureNode())
}

func getNode(t *testing.T, c client.Client) *corev1.Node {
	t.Helper()
	n := &corev1.Node{}
	if err := c.Get(context.Background(), client.ObjectKey{Name: "w0"}, n); err != nil {
		t.Fatal(err)
	}
	return n
}

func findCondition(node *corev1.Node, condType corev1.NodeConditionType) (corev1.NodeCondition, bool) {
	for _, cond := range node.Status.Conditions {
		if cond.Type == condType {
			return cond, true
		}
	}
	return corev1.NodeCondition{}, false
}

func mustCondition(t *testing.T, c client.Client, condType corev1.NodeConditionType) corev1.NodeCondition {
	t.Helper()
	cond, found := findCondition(getNode(t, c), condType)
	if !found {
		t.Fatalf("condition %s missing", condType)
	}
	return cond
}

func taintKeys(node *corev1.Node) []string {
	keys := make([]string, 0, len(node.Spec.Taints))
	for _, t := range node.Spec.Taints {
		keys = append(keys, t.Key)
	}
	return keys
}

func TestSetNodeExtendedResource(t *testing.T) {
	c := nodeClient(t)
	if err := SetNodeExtendedResource(context.Background(), c, "w0", "nvidia.com/gpu", 8); err != nil {
		t.Fatal(err)
	}
	n := getNode(t, c)
	want := resource.MustParse("8")
	if got := n.Status.Capacity["nvidia.com/gpu"]; got.Cmp(want) != 0 {
		t.Fatalf("capacity = %s", got.String())
	}
	if got := n.Status.Allocatable["nvidia.com/gpu"]; got.Cmp(want) != 0 {
		t.Fatalf("allocatable = %s", got.String())
	}
}

func TestSetAndClearNodeCondition(t *testing.T) {
	c := nodeClient(t)
	ctx := context.Background()
	if err := SetNodeCondition(ctx, c, "w0", "GpuUnhealthy", corev1.ConditionTrue, "XidError", "fault"); err != nil {
		t.Fatal(err)
	}
	if err := SetNodeCondition(ctx, c, "w0", "GpuUnhealthy", corev1.ConditionTrue, "XidError", "fault again"); err != nil {
		t.Fatal(err)
	}
	n := getNode(t, c)
	var seen int
	for _, cond := range n.Status.Conditions {
		if cond.Type == "GpuUnhealthy" {
			seen++
			if cond.Status != corev1.ConditionTrue || cond.Message != "fault again" {
				t.Fatalf("condition not upserted: %+v", cond)
			}
		}
	}
	if seen != 1 {
		t.Fatalf("condition upsert must keep one entry, got %d", seen)
	}
	if err := ClearNodeCondition(ctx, c, "w0", "GpuUnhealthy"); err != nil {
		t.Fatal(err)
	}
	n = getNode(t, c)
	if _, found := findCondition(n, "GpuUnhealthy"); found {
		t.Fatal("condition must be removed")
	}
	if ready, found := findCondition(n, corev1.NodeReady); !found || ready.Status != corev1.ConditionTrue {
		t.Fatalf("Ready must survive clearing another condition, conditions = %+v", n.Status.Conditions)
	}
}

func TestSetNodeConditionKeepsTransitionTimeOnSameStatus(t *testing.T) {
	node := fixtureNode()
	node.Status.Conditions = append(node.Status.Conditions, corev1.NodeCondition{
		Type: "GpuUnhealthy", Status: corev1.ConditionTrue, Reason: "XidError", Message: "fault",
		LastTransitionTime: hourAgo(), LastHeartbeatTime: hourAgo(),
	})
	c := clientFor(node)
	ctx := context.Background()
	before := mustCondition(t, c, "GpuUnhealthy")

	if err := SetNodeCondition(ctx, c, "w0", "GpuUnhealthy", corev1.ConditionTrue, "XidError", "fault again"); err != nil {
		t.Fatal(err)
	}
	same := mustCondition(t, c, "GpuUnhealthy")
	if !same.LastTransitionTime.Equal(&before.LastTransitionTime) {
		t.Fatalf("same status must keep LastTransitionTime: before %s, after %s", before.LastTransitionTime, same.LastTransitionTime)
	}
	if !same.LastHeartbeatTime.After(before.LastHeartbeatTime.Time) {
		t.Fatalf("same status must move LastHeartbeatTime: before %s, after %s", before.LastHeartbeatTime, same.LastHeartbeatTime)
	}
	if same.Message != "fault again" {
		t.Fatalf("message not updated: %+v", same)
	}

	if err := SetNodeCondition(ctx, c, "w0", "GpuUnhealthy", corev1.ConditionFalse, "Recovered", "ok"); err != nil {
		t.Fatal(err)
	}
	flipped := mustCondition(t, c, "GpuUnhealthy")
	if flipped.Status != corev1.ConditionFalse {
		t.Fatalf("status not updated: %+v", flipped)
	}
	if flipped.LastTransitionTime.Equal(&before.LastTransitionTime) {
		t.Fatalf("status change must stamp a new LastTransitionTime, still %s", flipped.LastTransitionTime)
	}
}

func TestTaintNodeIdempotent(t *testing.T) {
	c := nodeClient(t)
	ctx := context.Background()
	taint := corev1.Taint{Key: "maintenance.example.com/patching", Value: "true", Effect: corev1.TaintEffectNoSchedule}
	if err := AddNodeTaint(ctx, c, "w0", taint); err != nil {
		t.Fatal(err)
	}
	if err := AddNodeTaint(ctx, c, "w0", taint); err != nil {
		t.Fatal(err)
	}
	if got, want := taintKeys(getNode(t, c)), []string{otherTaintKey, taint.Key}; !slices.Equal(got, want) {
		t.Fatalf("taints = %v, want %v", got, want)
	}
	if err := RemoveNodeTaint(ctx, c, "w0", taint.Key); err != nil {
		t.Fatal(err)
	}
	if got, want := taintKeys(getNode(t, c)), []string{otherTaintKey}; !slices.Equal(got, want) {
		t.Fatalf("taints = %v, want %v", got, want)
	}
}

func TestRemoveNodeLabel(t *testing.T) {
	node := fixtureNode()
	node.Labels = map[string]string{"maintenance.example.com/state": "patching", "kubernetes.io/hostname": "w0"}
	c := clientFor(node)
	ctx := context.Background()
	if err := RemoveNodeLabel(ctx, c, "w0", "maintenance.example.com/state"); err != nil {
		t.Fatal(err)
	}
	got := getNode(t, c).Labels
	if _, present := got["maintenance.example.com/state"]; present {
		t.Fatalf("label must be removed, labels = %v", got)
	}
	if got["kubernetes.io/hostname"] != "w0" {
		t.Fatalf("other labels must be kept, labels = %v", got)
	}
	if err := RemoveNodeLabel(ctx, c, "w0", "maintenance.example.com/state"); err != nil {
		t.Fatalf("removing an absent label must not error: %v", err)
	}
}

func TestSetNodeUnschedulable(t *testing.T) {
	c := nodeClient(t)
	ctx := context.Background()
	if err := SetNodeUnschedulable(ctx, c, "w0", true); err != nil {
		t.Fatal(err)
	}
	if !getNode(t, c).Spec.Unschedulable {
		t.Fatal("node must be cordoned")
	}
	if err := SetNodeUnschedulable(ctx, c, "w0", false); err != nil {
		t.Fatal(err)
	}
	if getNode(t, c).Spec.Unschedulable {
		t.Fatal("node must be uncordoned")
	}
}
