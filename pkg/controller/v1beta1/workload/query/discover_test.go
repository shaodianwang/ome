package query

import (
	"context"
	"fmt"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
)

func newDiscoverTestClient(t *testing.T, objs ...client.Object) client.Client {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add corev1: %v", err)
	}
	if err := v1beta1.AddToScheme(scheme); err != nil {
		t.Fatalf("add v1beta1: %v", err)
	}
	return fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()
}

// testPodLabels reproduces the label set Render stamps on every
// OMENative pod. Duplicated here (rather than importing render.go's
// podLabels) because that would create a cycle: render.go imports
// query/.
func testPodLabels(isvc string, component v1beta1.ComponentType, idx int32, runner string, incarnation int64) map[string]string {
	return map[string]string{
		constants.InferenceServicePodLabelKey: isvc,
		constants.OMEComponentLabel:           string(component),
		LabelInstanceIdx:                      fmt.Sprintf("%d", idx),
		LabelInstanceIncarnation:              fmt.Sprintf("%d", incarnation),
		LabelRunner:                           runner,
		LabelManagedBy:                        ManagedByOMENative,
	}
}

func newDiscoverPod(name string, instanceIdx int32, incarnation int64, withDeletionTimestamp bool) *corev1.Pod {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "ns",
			Labels:    testPodLabels("isvc", v1beta1.EngineComponent, instanceIdx, "default", incarnation),
		},
	}
	if withDeletionTimestamp {
		t := metav1.Now()
		pod.DeletionTimestamp = &t
		// fake client requires finalizers on objects with DeletionTimestamp
		pod.Finalizers = []string{"keep"}
	}
	return pod
}

func TestLiveListPodsForInstance_FiltersByIndex(t *testing.T) {
	pod0 := newDiscoverPod("p-0", 0, 1, false)
	pod1 := newDiscoverPod("p-1", 1, 1, false)
	c := newDiscoverTestClient(t, pod0, pod1)

	got, err := LiveListPodsForInstance(context.Background(), c, "ns", "isvc", v1beta1.EngineComponent, 0)
	if err != nil {
		t.Fatalf("LiveListPodsForInstance: %v", err)
	}
	if len(got) != 1 || got[0].Name != "p-0" {
		t.Errorf("expected only p-0; got %v", podNames(got))
	}
}

func TestLiveOldPodsClearedForRecreate_NoPods(t *testing.T) {
	c := newDiscoverTestClient(t)
	clear, err := LiveOldPodsClearedForRecreate(context.Background(), c, "ns", "isvc", v1beta1.EngineComponent, 0, 2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !clear {
		t.Errorf("expected clear=true with no pods, got false")
	}
}

func TestLiveOldPodsClearedForRecreate_OnlyNewIncarnation(t *testing.T) {
	// Phase B reconcile after the controller previously created the
	// new-incarnation pods but crashed before Ready. Old pods are
	// long gone, new pods exist. Should be clear.
	c := newDiscoverTestClient(t,
		newDiscoverPod("p-new-0", 0, 2, false),
		newDiscoverPod("p-new-1", 0, 2, false),
	)
	clear, err := LiveOldPodsClearedForRecreate(context.Background(), c, "ns", "isvc", v1beta1.EngineComponent, 0, 2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !clear {
		t.Errorf("expected clear=true when only new-incarnation pods present, got false")
	}
}

func TestLiveOldPodsClearedForRecreate_OldStillRunning(t *testing.T) {
	// Phase B reconcile fired but Phase A deletes haven't propagated
	// — old pod still present, no DeletionTimestamp. Must NOT proceed.
	c := newDiscoverTestClient(t,
		newDiscoverPod("p-old", 0, 1, false),
	)
	clear, err := LiveOldPodsClearedForRecreate(context.Background(), c, "ns", "isvc", v1beta1.EngineComponent, 0, 2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if clear {
		t.Errorf("expected clear=false when old pod still running, got true")
	}
}

func TestLiveOldPodsClearedForRecreate_OldTerminatingTolerated(t *testing.T) {
	// Old pod has DeletionTimestamp (foreground propagation in
	// flight) — Phase B can proceed because the stable name will
	// land on a different pod once GC completes.
	c := newDiscoverTestClient(t,
		newDiscoverPod("p-old", 0, 1, true),
	)
	clear, err := LiveOldPodsClearedForRecreate(context.Background(), c, "ns", "isvc", v1beta1.EngineComponent, 0, 2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !clear {
		t.Errorf("expected clear=true for terminating old pod, got false")
	}
}

func TestLiveOldPodsClearedForRecreate_MissingLabelDoesNotBlockPhaseB(t *testing.T) {
	// Unknown (label-missing) pods are not OLD —
	// LiveOldPodsClearedForRecreate only gates on OLD. In production an
	// orphan short-circuits Restart via FoundOrphan before this helper
	// runs; this test pins the helper's narrower contract: "old pods,
	// are they all terminating?".
	pod := newDiscoverPod("p-orphan", 0, 0, false)
	delete(pod.Labels, LabelInstanceIncarnation)
	c := newDiscoverTestClient(t, pod)

	clear, err := LiveOldPodsClearedForRecreate(context.Background(), c, "ns", "isvc", v1beta1.EngineComponent, 0, 2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !clear {
		t.Errorf("expected clear=true: unknown bucket pods don't count as old, so the old set is empty")
	}
}

func TestAllTerminating(t *testing.T) {
	if !AllTerminating(nil) {
		t.Errorf("nil slice should be trivially terminating")
	}
	if !AllTerminating([]*corev1.Pod{}) {
		t.Errorf("empty slice should be trivially terminating")
	}
	terminating := newDiscoverPod("t", 0, 1, true)
	if !AllTerminating([]*corev1.Pod{terminating}) {
		t.Errorf("pod with DeletionTimestamp should be terminating")
	}
	running := newDiscoverPod("r", 0, 1, false)
	if AllTerminating([]*corev1.Pod{terminating, running}) {
		t.Errorf("mixed slice should NOT be all-terminating")
	}
}

func podNames(pods []*corev1.Pod) []string {
	out := make([]string, 0, len(pods))
	for _, p := range pods {
		out = append(out, p.Name)
	}
	return out
}
