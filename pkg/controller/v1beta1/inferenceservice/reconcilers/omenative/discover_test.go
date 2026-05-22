package omenative

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
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

func newDiscoverPod(name string, instanceIdx int32, incarnation int64, withDeletionTimestamp bool) *corev1.Pod {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "ns",
			Labels:    podLabels("isvc", v1beta1.EngineComponent, instanceIdx, "default", incarnation),
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

func newDiscoverParams(t *testing.T, objs ...client.Object) ReconcileParams {
	t.Helper()
	c := newDiscoverTestClient(t, objs...)
	return ReconcileParams{
		ISVC: &v1beta1.InferenceService{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "isvc",
				Namespace: "ns",
				UID:       types.UID("isvc-uid"),
			},
		},
		Component: v1beta1.EngineComponent,
		Client:    c,
		APIReader: c, // fake is both — exercise the reader() path
	}
}

func TestLiveListPodsForInstance_FiltersByIndex(t *testing.T) {
	pod0 := newDiscoverPod("p-0", 0, 1, false)
	pod1 := newDiscoverPod("p-1", 1, 1, false)
	p := newDiscoverParams(t, pod0, pod1)

	got, err := liveListPodsForInstance(context.Background(), p, v1beta1.EngineComponent, 0)
	if err != nil {
		t.Fatalf("liveListPodsForInstance: %v", err)
	}
	if len(got) != 1 || got[0].Name != "p-0" {
		t.Errorf("expected only p-0; got %v", podNames(got))
	}
}

func TestLiveOldPodsClearedForRecreate_NoPods(t *testing.T) {
	p := newDiscoverParams(t)
	clear, err := liveOldPodsClearedForRecreate(context.Background(), p, v1beta1.EngineComponent, 0, 2)
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
	p := newDiscoverParams(t,
		newDiscoverPod("p-new-0", 0, 2, false),
		newDiscoverPod("p-new-1", 0, 2, false),
	)
	clear, err := liveOldPodsClearedForRecreate(context.Background(), p, v1beta1.EngineComponent, 0, 2)
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
	p := newDiscoverParams(t,
		newDiscoverPod("p-old", 0, 1, false),
	)
	clear, err := liveOldPodsClearedForRecreate(context.Background(), p, v1beta1.EngineComponent, 0, 2)
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
	p := newDiscoverParams(t,
		newDiscoverPod("p-old", 0, 1, true),
	)
	clear, err := liveOldPodsClearedForRecreate(context.Background(), p, v1beta1.EngineComponent, 0, 2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !clear {
		t.Errorf("expected clear=true for terminating old pod, got false")
	}
}

func TestLiveOldPodsClearedForRecreate_MissingLabelDoesNotBlockPhaseB(t *testing.T) {
	// OPS-3: label-missing pods are now classified as `unknown`, not
	// `old`. liveOldPodsClearedForRecreate gates only on OLD pods, so
	// an unknown pod doesn't block Phase B from this helper's
	// perspective. The caller (Restart) routes orphan pods through
	// the FoundOrphan event path BEFORE reaching this helper, so in
	// production an unknown pod would short-circuit Restart entirely
	// before liveOldPodsClearedForRecreate runs. This test pins the
	// helper's narrower contract: "old pods, are they all terminating?"
	pod := newDiscoverPod("p-orphan", 0, 0, false)
	delete(pod.Labels, labelInstanceIncarnation)
	p := newDiscoverParams(t, pod)

	clear, err := liveOldPodsClearedForRecreate(context.Background(), p, v1beta1.EngineComponent, 0, 2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !clear {
		t.Errorf("expected clear=true: unknown bucket pods don't count as old, so the old set is empty")
	}
}

func TestAllTerminating(t *testing.T) {
	if !allTerminating(nil) {
		t.Errorf("nil slice should be trivially terminating")
	}
	if !allTerminating([]*corev1.Pod{}) {
		t.Errorf("empty slice should be trivially terminating")
	}
	terminating := newDiscoverPod("t", 0, 1, true)
	if !allTerminating([]*corev1.Pod{terminating}) {
		t.Errorf("pod with DeletionTimestamp should be terminating")
	}
	running := newDiscoverPod("r", 0, 1, false)
	if allTerminating([]*corev1.Pod{terminating, running}) {
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

// Sanity: confirm we don't accidentally clobber constants from this
// test by referencing them.
var _ = constants.InferenceServicePodLabelKey
