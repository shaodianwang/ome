package ops

import (
	"fmt"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/core"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
)

// Shared test fixtures used by every *_test.go in the omenative/ops
// package. Originally lived in create_test.go before the per-Instance
// Create state machine moved to pkg/controller/v1beta1/workload/ops
// (part 2 slice 4b of N). Lifted here so the peer test files (restart,
// delete, migrate, surge, timeouts, update) keep compiling after the
// move; they shrink as each peer migrates to the workload package and
// brings its own test scaffolding with it.

// minimalISVC builds an ISVC with the minimum metadata Create() needs.
func minimalISVC(name, ns string, replicas int) *v1beta1.InferenceService {
	mr := replicas
	return &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: ns,
			UID:       types.UID(name + "-uid"),
		},
		Spec: v1beta1.InferenceServiceSpec{
			Engine: &v1beta1.EngineSpec{
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
					MinReplicas: &mr,
				},
			},
		},
	}
}

// testRevisionHash is the synthetic revision hash stamped on test
// pods (matches what production Render emits via ome.io/revision-hash).
// Per-revision Service fixtures in drain tests name themselves after
// this constant via coordination.PerRevisionServiceName.
const testRevisionHash = "testrev1"

// testPodLabels reproduces the label set workload/ops.Render stamps on
// every emitted pod. Duplicated here (rather than re-exported from
// workload/ops) because workload/ops keeps the helper private; tests
// in this package fabricate pods directly without going through Render.
func testPodLabels(isvc string, component v1beta1.ComponentType, instanceIdx int32, runner string, incarnation int64, ordinal int32) map[string]string {
	return map[string]string{
		constants.InferenceServicePodLabelKey: isvc,
		constants.OMEComponentLabel:           string(component),
		query.LabelInstanceIdx:                fmt.Sprintf("%d", instanceIdx),
		query.LabelInstanceIncarnation:        fmt.Sprintf("%d", incarnation),
		query.LabelRunner:                     runner,
		query.LabelManagedBy:                  query.ManagedByOMENative,
		query.LabelPodOrdinal:                 fmt.Sprintf("%d", ordinal),
	}
}

// minimalParams builds a core.ReconcileParams in the single-pod / engine shape.
func minimalParams(t *testing.T, isvc *v1beta1.InferenceService, c client.Client) core.ReconcileParams {
	t.Helper()
	return core.ReconcileParams{
		ISVC:         isvc,
		PodSpec:      &corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: "test:v1"}}},
		Component:    v1beta1.EngineComponent,
		ComponentExt: &isvc.Spec.Engine.ComponentExtensionSpec,
		Client:       c,
	}
}

// resetExpectations clears the process-level expectations singletons so
// per-test create attempts aren't blocked by entries from a previous
// test. Reseats BOTH core.DefaultExpectations and workload.DefaultExpectations
// because the dispatch shim's `deps.ExpectationsCache()` fallback path
// reads workload's copy when params.Expectations is nil (every test
// fixture in this package leaves it nil).
func resetExpectations(t *testing.T) {
	t.Helper()
	core.DefaultExpectations = core.NewExpectations()
	workload.DefaultExpectations = workload.NewExpectations()
}

// podForInstance fabricates a pod matching what Render() would produce
// for the given (ISVC, instance) pair, with knobs for runtime-ready /
// serving state.
//
// The `ready` knob synthesizes ContainersReady=True, not PodReady=True,
// because the controller gates on ContainersReady and PodReady requires
// every readiness gate (including ome.io/serving) to be True — which is
// exactly what the controller is about to write. Synthesizing PodReady
// in tests would mask the real ordering.
func podForInstance(isvc *v1beta1.InferenceService, instanceIdx int32, ready, serving bool) *corev1.Pod {
	labels := testPodLabels(isvc.Name, v1beta1.EngineComponent, instanceIdx, "default", 1, 0)
	// Production Render stamps ome.io/revision-hash so per-revision
	// Service selectors match. Test pods must carry it too — the drain
	// path computes the per-revision routed Service from this label.
	labels[query.LabelRevisionHash] = testRevisionHash
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      query.PodName(isvc.Name, v1beta1.EngineComponent, instanceIdx, "default", 0),
			Namespace: isvc.Namespace,
			Labels:    labels,
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "main", Image: "test:v1"}},
		},
	}
	now := metav1.NewTime(time.Now())
	if ready {
		pod.Status.Conditions = append(pod.Status.Conditions, corev1.PodCondition{
			Type:               corev1.ContainersReady,
			Status:             corev1.ConditionTrue,
			LastTransitionTime: now,
		})
	}
	if serving {
		pod.Status.Conditions = append(pod.Status.Conditions, corev1.PodCondition{
			Type:               query.ServingConditionType,
			Status:             corev1.ConditionTrue,
			LastTransitionTime: now,
		})
	}
	return pod
}
