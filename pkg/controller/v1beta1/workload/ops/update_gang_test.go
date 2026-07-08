package ops

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	workload "github.com/ome-projects/ome/pkg/controller/v1beta1/workload/types"
)

// gangSurgePod fabricates a surge-gang pod at the given instance index +
// runner ordinal with the OMENative selector labels LiveListPodsForInstance
// filters on.
func gangSurgePod(isvc, ns string, instIdx int32, runner string, revHash string) *corev1.Pod {
	labels := legacyTestPodLabels(isvc, workload.ComponentEngine, instIdx, runner, 1, 0)
	labels[query.LabelRevisionHash] = revHash
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      query.PodName(isvc, workload.ComponentEngine, instIdx, runner, 0),
			Namespace: ns,
			Labels:    labels,
		},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: "bad:tag"}}},
	}
}

// gangAbandonInput builds a closure-backed ReconcileInput that records
// RemoveInstance calls and mutates an in-memory source InstanceStatus so the
// abandon path's reset is observable.
func gangAbandonInput(isvc, ns string, src *workload.InstanceStatus, removed *[]int32) workload.ReconcileInput {
	return workload.ReconcileInput{
		Key: workload.Key{
			Namespace: ns,
			OwnerName: isvc,
			Component: workload.ComponentEngine,
			SelectorLabels: map[string]string{
				constants.InferenceServicePodLabelKey: isvc,
				constants.OMEComponentLabel:           string(workload.ComponentEngine),
				query.LabelManagedBy:                  query.ManagedByOMENative,
			},
		},
		MutateInstance: func(_ context.Context, idx int32, mutate func(*workload.InstanceStatus) bool) error {
			if idx == src.Index {
				mutate(src)
			}
			return nil
		},
		RemoveInstance: func(_ context.Context, idx int32) (bool, error) {
			*removed = append(*removed, idx)
			return true, nil
		},
	}
}

// TestAbandonFailedGangSurge_DeletesStalePodsFirst pins the first phase:
// while the wedged surge gang's pods still exist, abandon deletes them and
// does NOT yet drop the marker or reset the source.
func TestAbandonFailedGangSurge_DeletesStalePodsFirst(t *testing.T) {
	legacyResetExpectations(t)
	const isvc, ns = "p414", "omenative-eval"
	leader := gangSurgePod(isvc, ns, 2, "leader", "badrev")
	worker := gangSurgePod(isvc, ns, 2, "worker", "badrev")
	c := legacyNewFakeClient(t, leader, worker)

	src := &workload.InstanceStatus{
		Index:           0,
		Phase:           workload.InstancePhaseFailed,
		RunningRevision: "p414-engine-goodrev",
	}
	var removed []int32
	input := gangAbandonInput(isvc, ns, src, &removed)
	plan := legacyMultiPodComponentPlan(workload.UpdateStrategySurgeThenDrain)

	done, err := abandonFailedGangSurge(context.Background(), legacyTestDeps(c), input, plan, 0, 2, src.RunningRevision)
	if err != nil {
		t.Fatalf("abandonFailedGangSurge: %v", err)
	}
	if done {
		t.Errorf("done: got true want false (abandon always requeues)")
	}
	// Both surge pods deleted.
	remaining, err := query.LiveListPodsForInstance(context.Background(), c, ns, isvc, workload.ComponentEngine, 2)
	if err != nil {
		t.Fatalf("list surge pods: %v", err)
	}
	if len(remaining) != 0 {
		t.Errorf("surge pods: got %d want 0 (all deleted)", len(remaining))
	}
	// Marker not yet dropped, source not yet reset — that's the next pass.
	if len(removed) != 0 {
		t.Errorf("RemoveInstance calls: got %v want none while pods still present", removed)
	}
	if src.Phase != workload.InstancePhaseFailed {
		t.Errorf("source Phase: got %q want still Failed (reset happens after pods gone)", src.Phase)
	}
}

// TestAbandonFailedGangSurge_ResetsSourceAfterPodsGone pins the second
// phase: once the surge gang's pods are gone, abandon drops the surge
// marker and resets the source to Ready on its running revision with the
// failed Operation cleared — so the next reconcile fires a fresh surge.
func TestAbandonFailedGangSurge_ResetsSourceAfterPodsGone(t *testing.T) {
	legacyResetExpectations(t)
	const isvc, ns = "p414", "omenative-eval"
	c := legacyNewFakeClient(t) // no surge pods — already deleted

	surgeIdx := int32(2)
	src := &workload.InstanceStatus{
		Index:           0,
		Phase:           workload.InstancePhaseFailed,
		RunningRevision: "p414-engine-goodrev",
		TargetRevision:  "p414-engine-badrev",
		Operation: &workload.InstanceOperation{
			Type:       workload.InstanceOperationUpdate,
			Step:       updateStepSurge,
			SurgeIndex: &surgeIdx,
		},
	}
	var removed []int32
	input := gangAbandonInput(isvc, ns, src, &removed)
	plan := legacyMultiPodComponentPlan(workload.UpdateStrategySurgeThenDrain)

	done, err := abandonFailedGangSurge(context.Background(), legacyTestDeps(c), input, plan, 0, surgeIdx, src.RunningRevision)
	if err != nil {
		t.Fatalf("abandonFailedGangSurge: %v", err)
	}
	if done {
		t.Errorf("done: got true want false (abandon always requeues)")
	}
	if len(removed) != 1 || removed[0] != surgeIdx {
		t.Errorf("RemoveInstance calls: got %v want [%d]", removed, surgeIdx)
	}
	if src.Phase != workload.InstancePhaseReady {
		t.Errorf("source Phase: got %q want Ready", src.Phase)
	}
	if src.Operation != nil {
		t.Errorf("source Operation: got %+v want nil (failed surge cleared)", src.Operation)
	}
	if src.RunningRevision != "p414-engine-goodrev" {
		t.Errorf("source RunningRevision: got %q want p414-engine-goodrev (unchanged)", src.RunningRevision)
	}
	if src.TargetRevision != "" {
		t.Errorf("source TargetRevision: got %q want cleared", src.TargetRevision)
	}
}
