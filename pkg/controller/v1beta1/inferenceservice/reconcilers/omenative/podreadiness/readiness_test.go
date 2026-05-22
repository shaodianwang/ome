package podreadiness

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func newReadinessTestClient(t *testing.T, objs ...client.Object) client.Client {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add corev1: %v", err)
	}
	return fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(objs...).
		WithStatusSubresource(&corev1.Pod{}).
		Build()
}

func newReadinessTestPod(name string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "default",
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "main", Image: "x"}},
			ReadinessGates: []corev1.PodReadinessGate{{
				ConditionType: ConditionType,
			}},
		},
	}
}

func TestFreshPod_RemoveNotReadyKey_WritesStatusTrue(t *testing.T) {
	// Pod has no condition. Removing a key (which isn't there) creates
	// the condition with Status=True.
	pod := newReadinessTestPod("p")
	c := newReadinessTestClient(t, pod)

	if err := RemoveNotReadyKey(context.Background(), c, pod, Message{UserAgent: "Lifecycle", Key: "InstanceReady"}); err != nil {
		t.Fatalf("RemoveNotReadyKey: %v", err)
	}

	got := &corev1.Pod{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(pod), got); err != nil {
		t.Fatalf("get: %v", err)
	}
	cond := findCondition(got, ConditionType)
	if cond == nil {
		t.Fatalf("condition not written")
	}
	if cond.Status != corev1.ConditionTrue {
		t.Errorf("expected Status=True, got %s", cond.Status)
	}
	if cond.Message != "" {
		t.Errorf("expected empty message, got %q", cond.Message)
	}
}

func TestAddNotReadyKey_SetsStatusFalse(t *testing.T) {
	pod := newReadinessTestPod("p")
	c := newReadinessTestClient(t, pod)

	if err := AddNotReadyKey(context.Background(), c, pod, Message{UserAgent: "Delete-drain", Key: "0"}); err != nil {
		t.Fatalf("AddNotReadyKey: %v", err)
	}

	got := &corev1.Pod{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(pod), got); err != nil {
		t.Fatalf("get: %v", err)
	}
	cond := findCondition(got, ConditionType)
	if cond == nil || cond.Status != corev1.ConditionFalse {
		t.Fatalf("expected Status=False, got %+v", cond)
	}
	list := parseList(cond.Message)
	if len(list) != 1 || list[0].UserAgent != "Delete-drain" || list[0].Key != "0" {
		t.Errorf("unexpected message list: %v", list)
	}
}

func TestMultiWriter_TwoKeysHoldNotReady(t *testing.T) {
	pod := newReadinessTestPod("p")
	c := newReadinessTestClient(t, pod)

	if err := AddNotReadyKey(context.Background(), c, pod, Message{UserAgent: "A", Key: "1"}); err != nil {
		t.Fatalf("Add A: %v", err)
	}
	got := &corev1.Pod{}
	_ = c.Get(context.Background(), client.ObjectKeyFromObject(pod), got)
	if err := AddNotReadyKey(context.Background(), c, got, Message{UserAgent: "B", Key: "2"}); err != nil {
		t.Fatalf("Add B: %v", err)
	}

	// Removing only A — pod must stay NotReady because B still holds it.
	_ = c.Get(context.Background(), client.ObjectKeyFromObject(pod), got)
	if err := RemoveNotReadyKey(context.Background(), c, got, Message{UserAgent: "A", Key: "1"}); err != nil {
		t.Fatalf("Remove A: %v", err)
	}
	_ = c.Get(context.Background(), client.ObjectKeyFromObject(pod), got)
	cond := findCondition(got, ConditionType)
	if cond.Status != corev1.ConditionFalse {
		t.Errorf("expected Status=False after removing one of two keys, got %s", cond.Status)
	}
	list := parseList(cond.Message)
	if len(list) != 1 || list[0].UserAgent != "B" {
		t.Errorf("expected only B remaining, got %v", list)
	}

	// Now remove B — pod must flip Ready.
	_ = c.Get(context.Background(), client.ObjectKeyFromObject(pod), got)
	if err := RemoveNotReadyKey(context.Background(), c, got, Message{UserAgent: "B", Key: "2"}); err != nil {
		t.Fatalf("Remove B: %v", err)
	}
	_ = c.Get(context.Background(), client.ObjectKeyFromObject(pod), got)
	cond = findCondition(got, ConditionType)
	if cond.Status != corev1.ConditionTrue {
		t.Errorf("expected Status=True after removing last key, got %s", cond.Status)
	}
}

func TestAddNotReadyKey_Idempotent(t *testing.T) {
	pod := newReadinessTestPod("p")
	c := newReadinessTestClient(t, pod)

	if err := AddNotReadyKey(context.Background(), c, pod, Message{UserAgent: "A", Key: "1"}); err != nil {
		t.Fatalf("first Add: %v", err)
	}
	first := &corev1.Pod{}
	_ = c.Get(context.Background(), client.ObjectKeyFromObject(pod), first)
	firstRV := first.ResourceVersion

	// Second Add of the same key must be a no-op (no patch issued).
	if err := AddNotReadyKey(context.Background(), c, first, Message{UserAgent: "A", Key: "1"}); err != nil {
		t.Fatalf("second Add: %v", err)
	}
	second := &corev1.Pod{}
	_ = c.Get(context.Background(), client.ObjectKeyFromObject(pod), second)
	if second.ResourceVersion != firstRV {
		t.Errorf("ResourceVersion bumped on idempotent Add: %s -> %s", firstRV, second.ResourceVersion)
	}
}

func TestRemoveNotReadyKey_IdempotentOnUnknownKey(t *testing.T) {
	pod := newReadinessTestPod("p")
	c := newReadinessTestClient(t, pod)

	// Seed Status=True via a remove that creates the condition.
	if err := RemoveNotReadyKey(context.Background(), c, pod, Message{UserAgent: "A", Key: "1"}); err != nil {
		t.Fatalf("seed Remove: %v", err)
	}
	first := &corev1.Pod{}
	_ = c.Get(context.Background(), client.ObjectKeyFromObject(pod), first)
	firstRV := first.ResourceVersion

	// Second Remove of the same key must be a no-op — condition already
	// True, key isn't in the list.
	if err := RemoveNotReadyKey(context.Background(), c, first, Message{UserAgent: "A", Key: "1"}); err != nil {
		t.Fatalf("second Remove: %v", err)
	}
	second := &corev1.Pod{}
	_ = c.Get(context.Background(), client.ObjectKeyFromObject(pod), second)
	if second.ResourceVersion != firstRV {
		t.Errorf("ResourceVersion bumped on idempotent Remove: %s -> %s", firstRV, second.ResourceVersion)
	}
}

func TestContainsNotReadyKey(t *testing.T) {
	pod := newReadinessTestPod("p")
	c := newReadinessTestClient(t, pod)

	if got := ContainsNotReadyKey(pod, Message{UserAgent: "A", Key: "1"}); got {
		t.Errorf("fresh pod has no keys; ContainsNotReadyKey should be false")
	}

	if err := AddNotReadyKey(context.Background(), c, pod, Message{UserAgent: "A", Key: "1"}); err != nil {
		t.Fatalf("Add: %v", err)
	}
	got := &corev1.Pod{}
	_ = c.Get(context.Background(), client.ObjectKeyFromObject(pod), got)
	if !ContainsNotReadyKey(got, Message{UserAgent: "A", Key: "1"}) {
		t.Errorf("expected ContainsNotReadyKey=true after Add")
	}
	if ContainsNotReadyKey(got, Message{UserAgent: "B", Key: "2"}) {
		t.Errorf("expected ContainsNotReadyKey=false for unrelated key")
	}
}

func TestIsServing(t *testing.T) {
	if IsServing(nil) {
		t.Errorf("nil pod should not be serving")
	}
	pod := newReadinessTestPod("p")
	if IsServing(pod) {
		t.Errorf("fresh pod should not be serving (no condition)")
	}
	pod.Status.Conditions = []corev1.PodCondition{{
		Type:   ConditionType,
		Status: corev1.ConditionFalse,
	}}
	if IsServing(pod) {
		t.Errorf("Status=False should not be serving")
	}
	pod.Status.Conditions[0].Status = corev1.ConditionTrue
	if !IsServing(pod) {
		t.Errorf("Status=True should be serving")
	}
}

func TestAddRemove_PreservesKubeletConditions(t *testing.T) {
	// Critical STATUS-2 property: our patches must not clobber
	// kubelet-managed conditions like ContainersReady. The strategic-
	// merge patch with patch-merge-key=type leaves other condition
	// slots alone.
	pod := newReadinessTestPod("p")
	pod.Status.Conditions = []corev1.PodCondition{
		{Type: corev1.PodScheduled, Status: corev1.ConditionTrue},
		{Type: corev1.ContainersReady, Status: corev1.ConditionTrue},
	}
	c := newReadinessTestClient(t, pod)

	if err := AddNotReadyKey(context.Background(), c, pod, Message{UserAgent: "Delete-drain", Key: "0"}); err != nil {
		t.Fatalf("Add: %v", err)
	}
	got := &corev1.Pod{}
	_ = c.Get(context.Background(), client.ObjectKeyFromObject(pod), got)

	if findCondition(got, corev1.PodScheduled) == nil {
		t.Errorf("kubelet's PodScheduled condition was clobbered")
	}
	if findCondition(got, corev1.ContainersReady) == nil {
		t.Errorf("kubelet's ContainersReady condition was clobbered")
	}
	if findCondition(got, ConditionType) == nil {
		t.Errorf("our condition was not written")
	}
}

func TestAddNotReadyKey_NilPodRejected(t *testing.T) {
	c := newReadinessTestClient(t)
	if err := AddNotReadyKey(context.Background(), c, nil, Message{UserAgent: "x", Key: "y"}); err == nil {
		t.Fatalf("expected error on nil pod")
	}
}

func TestRemoveNotReadyKey_NilPodRejected(t *testing.T) {
	c := newReadinessTestClient(t)
	if err := RemoveNotReadyKey(context.Background(), c, nil, Message{UserAgent: "x", Key: "y"}); err == nil {
		t.Fatalf("expected error on nil pod")
	}
}

func TestMessageListSerialization_Deterministic(t *testing.T) {
	// Two writers adding in opposite orders should produce the same
	// serialized list, so operators inspecting the pod see stable
	// output between reconciles when nothing changed.
	pod := newReadinessTestPod("p1")
	c := newReadinessTestClient(t, pod)
	if err := AddNotReadyKey(context.Background(), c, pod, Message{UserAgent: "B", Key: "2"}); err != nil {
		t.Fatalf("Add B: %v", err)
	}
	p1 := &corev1.Pod{}
	_ = c.Get(context.Background(), client.ObjectKeyFromObject(pod), p1)
	if err := AddNotReadyKey(context.Background(), c, p1, Message{UserAgent: "A", Key: "1"}); err != nil {
		t.Fatalf("Add A: %v", err)
	}
	_ = c.Get(context.Background(), client.ObjectKeyFromObject(pod), p1)
	cond1 := findCondition(p1, ConditionType)

	pod2 := newReadinessTestPod("p2")
	c2 := newReadinessTestClient(t, pod2)
	if err := AddNotReadyKey(context.Background(), c2, pod2, Message{UserAgent: "A", Key: "1"}); err != nil {
		t.Fatalf("Add A: %v", err)
	}
	p2 := &corev1.Pod{}
	_ = c2.Get(context.Background(), client.ObjectKeyFromObject(pod2), p2)
	if err := AddNotReadyKey(context.Background(), c2, p2, Message{UserAgent: "B", Key: "2"}); err != nil {
		t.Fatalf("Add B: %v", err)
	}
	_ = c2.Get(context.Background(), client.ObjectKeyFromObject(pod2), p2)
	cond2 := findCondition(p2, ConditionType)

	if cond1.Message != cond2.Message {
		t.Errorf("Message ordering not stable:\n  p1: %s\n  p2: %s", cond1.Message, cond2.Message)
	}
}
