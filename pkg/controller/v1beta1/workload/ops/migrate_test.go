// Internal-package tests for workload.Migrate's private helpers
// (validateFromNode, etc.) that aren't reachable from outside the
// workload/ops package. The public-API end-to-end TestMigrate_* tests
// live in the omenative adapter package (omenative/ops/migrate_test.go)
// because they depend on the omenative-side test harness (audit ledger
// seeding via real ISVC ConfigMap round-trips, multi-pass status
// counter refresh, per-revision EndpointSlice synthesis) coupled to
// pkg/apis/ome/v1beta1 fixtures.
package ops

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/revision"
	workload "github.com/ome-projects/ome/pkg/controller/v1beta1/workload/types"
)

// vfnFixture builds the input + deps + plan a validateFromNode test
// reads — a single-pod engine workload selecting pods labelled with
// the canonical ISVC pod-label-key seed.
func vfnFixture(t *testing.T, pods ...*corev1.Pod) (workload.Deps, workload.ReconcileInput, workload.ComponentPlan) {
	t.Helper()
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = v1beta1.AddToScheme(scheme)
	objs := make([]client.Object, 0, len(pods))
	for _, p := range pods {
		objs = append(objs, p)
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()
	input := workload.ReconcileInput{
		Key: workload.Key{
			Namespace: "prod",
			Component: workload.ComponentEngine,
			OwnerName: "llama-70b",
			SelectorLabels: map[string]string{
				constants.InferenceServicePodLabelKey: "llama-70b",
				constants.OMEComponentLabel:           string(workload.ComponentEngine),
				query.LabelManagedBy:                  query.ManagedByOMENative,
			},
		},
	}
	plan := workload.ComponentPlan{Component: workload.ComponentEngine}
	return workload.Deps{Client: c}, input, plan
}

// vfnPod fabricates a pod under the validateFromNode selector with the
// given NodeName. Always Instance idx=0 — every validateFromNode test
// drives source index 0.
func vfnPod(nodeName string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "llama-70b-engine-0-default-0",
			Namespace: "prod",
			Labels: map[string]string{
				constants.InferenceServicePodLabelKey: "llama-70b",
				constants.OMEComponentLabel:           string(workload.ComponentEngine),
				query.LabelInstanceIdx:                "0",
				query.LabelInstanceIncarnation:        "1",
				query.LabelRunner:                     "default",
				query.LabelManagedBy:                  query.ManagedByOMENative,
				query.LabelPodOrdinal:                 "0",
			},
		},
		Spec: corev1.PodSpec{
			NodeName:   nodeName,
			Containers: []corev1.Container{{Name: "main", Image: "test:v1"}},
		},
	}
}

func TestValidateFromNode_MatchOK(t *testing.T) {
	deps, input, plan := vfnFixture(t, vfnPod("node5"))
	mismatch, defer_, err := validateFromNode(context.Background(), deps, input, plan, 0, "node5")
	if err != nil {
		t.Fatalf("validateFromNode: %v", err)
	}
	if defer_ || mismatch != "" {
		t.Errorf("expected pass; got mismatch=%q defer=%v", mismatch, defer_)
	}
}

func TestValidateFromNode_MismatchRejected(t *testing.T) {
	deps, input, plan := vfnFixture(t, vfnPod("node7"))
	mismatch, defer_, err := validateFromNode(context.Background(), deps, input, plan, 0, "node5")
	if err != nil {
		t.Fatalf("validateFromNode: %v", err)
	}
	if defer_ {
		t.Errorf("scheduled pod on different node must be a rejection, not a defer")
	}
	if mismatch == "" {
		t.Errorf("expected mismatch reason")
	}
}

func TestValidateFromNode_UnscheduledDefers(t *testing.T) {
	// Pod exists but Spec.NodeName is empty. Defer rather than reject —
	// the next reconcile re-polls.
	deps, input, plan := vfnFixture(t, vfnPod(""))
	mismatch, defer_, err := validateFromNode(context.Background(), deps, input, plan, 0, "node5")
	if err != nil {
		t.Fatalf("validateFromNode: %v", err)
	}
	if !defer_ {
		t.Errorf("unscheduled pod must defer; got mismatch=%q defer=%v", mismatch, defer_)
	}
	if mismatch != "" {
		t.Errorf("defer must not carry a rejection reason; got %q", mismatch)
	}
}

func TestValidateFromNode_NoLivePodsRejects(t *testing.T) {
	// Source instance has no pods at all — a rejection (the migration
	// requester can't legitimately request a migration of a
	// non-existent instance).
	deps, input, plan := vfnFixture(t)
	mismatch, defer_, err := validateFromNode(context.Background(), deps, input, plan, 0, "node5")
	if err != nil {
		t.Fatalf("validateFromNode: %v", err)
	}
	if defer_ {
		t.Errorf("no live pods must reject, not defer")
	}
	if mismatch == "" {
		t.Errorf("expected mismatch reason for no-live-pods")
	}
}

// TestSurgeRevisionAndSpec_ReturnsGangWorkerSpec pins that the migrate
// surge resolver returns the source revision's WORKER template (not just
// the leader) so a gang migration renders its worker pods from the right
// template. Before gang migrate support this returned only the leader
// spec; the worker spec is nil for single-pod revisions.
func TestSurgeRevisionAndSpec_ReturnsGangWorkerSpec(t *testing.T) {
	legacyResetExpectations(t)
	isvc := legacyISVCReadyAtIncarnation("llama-70b", "prod", 1)
	c := legacyNewFakeClient(t, isvc)

	leader := &corev1.PodSpec{Containers: []corev1.Container{{Name: "leader", Image: "llama:v1"}}}
	worker := &corev1.PodSpec{Containers: []corev1.Container{{Name: "worker", Image: "llama:v1"}}}
	cr, _, err := revision.EnsureControllerRevisionWithWorker(
		context.Background(), c, c, isvc,
		v1beta1.SchemeGroupVersion.WithKind("InferenceService"),
		legacyEngineRevisionKey(isvc),
		leader, worker, nil, nil, isvc.UID,
	)
	if err != nil {
		t.Fatalf("EnsureControllerRevisionWithWorker: %v", err)
	}
	// Point instance 0's running revision at the gang CR (mutates through
	// the shared Lifecycle pointer + slice backing array).
	isvc.Status.Components[v1beta1.EngineComponent].Lifecycle.InstanceStatuses[0].RunningRevision = cr.Name

	input := legacyTestInput(isvc, c, workload.ComponentEngine)
	gotCR, gotLeader, gotWorker, err := surgeRevisionAndSpec(context.Background(), legacyTestDeps(c), input, 0)
	if err != nil {
		t.Fatalf("surgeRevisionAndSpec: %v", err)
	}
	if gotCR == nil || gotLeader == nil {
		t.Fatalf("expected CR + leader spec, got cr=%v leader=%v", gotCR, gotLeader)
	}
	if gotWorker == nil {
		t.Fatalf("expected a worker spec for a gang revision, got nil")
	}
	if len(gotWorker.Containers) == 0 || gotWorker.Containers[0].Name != "worker" {
		t.Errorf("worker spec mismatch: %+v", gotWorker)
	}
}
