package ops

import (
	"context"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	schedulingv1alpha1 "sigs.k8s.io/scheduler-plugins/apis/scheduling/v1alpha1"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/gang"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	workload "github.com/ome-projects/ome/pkg/controller/v1beta1/workload/types"
)

// gangSchedClient is legacyNewFakeClient plus the scheduler-plugins
// scheduling.x-k8s.io scheme so the test can assert PodGroup creation.
func gangSchedClient(t *testing.T, initObjs ...client.Object) client.Client {
	t.Helper()
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{
		corev1.AddToScheme,
		v1beta1.AddToScheme,
		discoveryv1.AddToScheme,
		appsv1.AddToScheme,
		schedulingv1alpha1.AddToScheme,
	} {
		if err := add(scheme); err != nil {
			t.Fatalf("add scheme: %v", err)
		}
	}
	return fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&v1beta1.InferenceService{}).
		WithObjects(initObjs...).
		Build()
}

// TestGangSurgeUpdate_EnsuresPodGroupBeforeSurgePods pins the canary
// gang-rollout fix: gangSurgeUpdate must announce the surge index's
// PodGroup to the gang scheduler BEFORE creating that gang's pods.
//
// Found by real-cluster multi-node canary testing (p304: engine-mn,
// minReplicas=4, canary capacity 50%). The canary surges a new gang at a
// fresh instance index; on the mkube-allocator (coscheduler-style) cluster
// the surge pods were rejected with "0/1 nodes are available: 1 PodGroup
// not found" and never scheduled, so the canary capacity gate never opened
// and the rollout stalled at phase=Pending.
//
// Root cause: the surge gang's PodGroup is created by the top-level
// EnsurePodGroups pass, which keys off plan.Instances — and the surge index
// only lands in the plan once its GangSurgeTarget status round-trips into
// ObservedState. In the window before that round-trip, gangSurgeUpdate
// creates the surge pods carrying the pod-group label while no PodGroup
// exists. The fix ensures the PodGroup inline before the create.
func TestGangSurgeUpdate_EnsuresPodGroupBeforeSurgePods(t *testing.T) {
	legacyResetExpectations(t)
	isvc := surgeISVCReady("p304", "prod", 1)
	plan := gangSurgePlan()

	v1Name := "p304-engine-rev-v1hash"
	v2Name := "p304-engine-rev-v2hash"

	// In-flight gang surge, BEFORE the surge pods are created: the source
	// (idx=0) carries Op{Surge, SurgeIndex=1}; idx=1 carries the
	// GangSurgeTarget marker.
	isvc.Status.Components = gangSurgeInFlightStatus(v1Name, v2Name)

	c := gangSchedClient(t, isvc)
	makeCR(t, c, isvc, v2Name)
	// Source gang still alive at idx=0 (capacity holds during the surge).
	for _, runner := range []string{"leader", "worker"} {
		hash := query.RevisionHashFromControllerRevisionName(v2Name)
		if err := c.Create(context.Background(), gangPodAt(isvc, 0, runner, hash, true, true)); err != nil {
			t.Fatalf("seed source pod (%s): %v", runner, err)
		}
	}

	input := gangInputWithRemove(isvc, c)
	// The cluster has the PodGroup CRD — same flag the IR reconciler threads.
	input.DesiredSpec.GangSchedulingAvailable = true

	v2 := &appsv1.ControllerRevision{}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: "prod", Name: v2Name}, v2); err != nil {
		t.Fatalf("get v2 CR: %v", err)
	}

	// Wire the inline surge-PodGroup ensure exactly as the IR reconciler
	// does (deps.EnsureGangPodGroup = gang.EnsureSurgePodGroup).
	deps := legacyTestDeps(c)
	deps.EnsureGangPodGroup = gang.EnsureSurgePodGroup(c)

	// Create-surge-gang pass: this is where the surge gang's pods are
	// created at idx=1.
	if _, err := surgeUpdate(context.Background(), deps, input, plan, plan.Instances[0], v2, nil); err != nil {
		t.Fatalf("gang surge create pass: %v", err)
	}

	// The surge pods exist...
	surgePods, err := query.LiveListPodsForInstance(context.Background(), c, "prod", "p304", workload.ComponentEngine, 1)
	if err != nil {
		t.Fatalf("list surge gang pods: %v", err)
	}
	if len(surgePods) == 0 {
		t.Fatalf("expected surge gang pods at idx=1; found none")
	}

	// ...and the PodGroup they reference MUST exist (else the gang scheduler
	// rejects them with "PodGroup not found").
	wantPG := query.PodGroupName("p304", workload.ComponentEngine, 1)
	pg := &schedulingv1alpha1.PodGroup{}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: "prod", Name: wantPG}, pg); err != nil {
		t.Fatalf("surge gang PodGroup %q missing when its pods exist: %v", wantPG, err)
	}
	// MinMember must cover the whole gang (leader + worker = 2).
	if pg.Spec.MinMember != 2 {
		t.Errorf("PodGroup %q MinMember=%d, want 2 (leader+worker)", wantPG, pg.Spec.MinMember)
	}
	// Every surge pod must carry the pod-group label naming that PodGroup.
	for _, pod := range surgePods {
		if got := pod.Labels[query.LabelPodGroup]; got != wantPG {
			t.Errorf("surge pod %s pod-group label=%q, want %q", pod.Name, got, wantPG)
		}
	}
}
