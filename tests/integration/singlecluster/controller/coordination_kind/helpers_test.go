package coordination_kind

import (
	"fmt"

	"github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/query"
	"github.com/ome-projects/ome/tests/integration/kindutil"
	"github.com/ome-projects/ome/tests/integration/util"
)

// =========================================================================
// ISVC + namespace fixtures
// =========================================================================

// applyFixture creates the CBM, CSR, and ISVC in order and returns the
// applied ISVC for the spec to assert against. Caller chains
// ginkgo.DeferCleanup to tear them down.
//
// Waits for the CBM + CSR to be visible in the controller manager's
// CACHED reader BEFORE creating the ISVC. The framework's k8sClient is
// a direct (uncached) apiserver client — Get returns the moment
// apiserver acks the Create. But the controller's reconciler reads
// through its informer cache, which lags behind. Without the cached
// wait the ISVC reconciler can fire with a stale cache, surface
// "ClusterBaseModel not found", and requeue with controller-runtime's
// exponential backoff (5ms → 16ms → ... → up to 1000s). By the time
// the cache catches up, the test's Eventually has already timed out.
//
// suiteFwk.Manager().GetClient() returns the cached reader the
// reconciler actually consults, so waiting on it is the deterministic
// way to detect cache convergence.
func applyFixture(g gomega.Gomega, f *coordFixture) {
	g.ExpectWithOffset(1, k8sClient.Create(suiteCtx, f.CBM)).To(gomega.Succeed(), "create CBM")
	g.ExpectWithOffset(1, k8sClient.Create(suiteCtx, f.CSR)).To(gomega.Succeed(), "create CSR")

	// Label every node with the model-Ready key OME's pod-injection
	// expects in its NodeSelector. In production this label is set by
	// the model-agent after the per-node weight download completes.
	// Under KIND we run no model-agent — the fake-serving container
	// doesn't load weights — so we stamp it directly so the scheduler
	// can place the ISVC pods. Without this, pods stay Pending with
	// "didn't match Pod's node affinity/selector."
	g.ExpectWithOffset(1, kindutil.LabelClusterBaseModelReady(suiteCtx, k8sClient, f.CBM.Name)).
		To(gomega.Succeed(), "label nodes for CBM")

	cachedClient := kindutil.Suite().Manager.GetClient()
	gomega.EventuallyWithOffset(1, func(gg gomega.Gomega) {
		got := &v1beta1.ClusterBaseModel{}
		gg.Expect(cachedClient.Get(suiteCtx, client.ObjectKey{Name: f.CBM.Name}, got)).To(gomega.Succeed())
		got2 := &v1beta1.ClusterServingRuntime{}
		gg.Expect(cachedClient.Get(suiteCtx, client.ObjectKey{Name: f.CSR.Name}, got2)).To(gomega.Succeed())
	}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(gomega.Succeed())
	g.ExpectWithOffset(1, k8sClient.Create(suiteCtx, f.ISVC)).To(gomega.Succeed(), "create ISVC")
}

// =========================================================================
// Per-revision Service assertions
// =========================================================================

// listPerRevisionServices returns every Service in namespace tagged
// with the per-revision selector key `ome.io/revision-hash`.
func listPerRevisionServices(g gomega.Gomega, namespace string) []corev1.Service {
	svcs := &corev1.ServiceList{}
	g.ExpectWithOffset(1, k8sClient.List(suiteCtx, svcs,
		client.InNamespace(namespace),
	)).To(gomega.Succeed())
	out := make([]corev1.Service, 0, len(svcs.Items))
	for _, s := range svcs.Items {
		if _, ok := s.Spec.Selector[query.LabelRevisionHash]; ok {
			out = append(out, s)
		}
	}
	return out
}

// =========================================================================
// Coordination status assertions
// =========================================================================

// freshISVC re-reads the ISVC named (namespace, name) from the cache.
// Used inside Eventually blocks where the spec needs the latest status.
func freshISVC(g gomega.Gomega, namespace, name string) *v1beta1.InferenceService {
	out := &v1beta1.InferenceService{}
	g.ExpectWithOffset(1, k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: namespace, Name: name}, out)).To(gomega.Succeed())
	return out
}

// trafficForComponent returns the per-Component Traffic[] from the
// ISVC's component status, or nil when the entry is missing.
func trafficForComponent(isvc *v1beta1.InferenceService, component v1beta1.ComponentType) []v1beta1.ComponentTrafficTarget {
	if isvc.Status.Components == nil {
		return nil
	}
	cs, ok := isvc.Status.Components[component]
	if !ok {
		return nil
	}
	return cs.Traffic
}

// =========================================================================
// Pod + ControllerRevision assertions
// =========================================================================

// listOMENativePods returns every OMENative-owned pod in namespace.
// Selector keys mirror what the production code uses (managed-by +
// inferenceservice).
func listOMENativePods(g gomega.Gomega, namespace, isvcName string) []corev1.Pod {
	pods := &corev1.PodList{}
	g.ExpectWithOffset(1, k8sClient.List(suiteCtx, pods,
		client.InNamespace(namespace),
		client.MatchingLabels{
			util.LabelManagedByKey:                util.ManagedByOMENativeValue,
			constants.InferenceServicePodLabelKey: isvcName,
		},
	)).To(gomega.Succeed())
	return pods.Items
}

// listOMENativeCRs returns every ControllerRevision in namespace tagged
// with the OMENative managed-by label. Used by tests that assert on
// revision history alongside per-revision Service materialization.
func listOMENativeCRs(g gomega.Gomega, namespace string) []appsv1.ControllerRevision {
	crs := &appsv1.ControllerRevisionList{}
	g.ExpectWithOffset(1, k8sClient.List(suiteCtx, crs,
		client.InNamespace(namespace),
		client.MatchingLabels{util.LabelManagedByKey: util.ManagedByOMENativeValue},
	)).To(gomega.Succeed())
	return crs.Items
}

// =========================================================================
// Debug formatters
// =========================================================================

// describeTraffic renders a Traffic[] slice in compact form for
// assertion-failure messages.
func describeTraffic(t []v1beta1.ComponentTrafficTarget) string {
	if len(t) == 0 {
		return "(none)"
	}
	out := ""
	for _, e := range t {
		latest := ""
		if e.LatestRevision {
			latest = " (latest)"
		}
		out += fmt.Sprintf("\n  %s = %d%%%s", e.RevisionName, e.Percent, latest)
	}
	return out
}
