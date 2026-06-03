package omenative_coordination_kind

import (
	"fmt"

	"github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	"github.com/ome-projects/ome/tests/integration/kindfix"
	"github.com/ome-projects/ome/tests/integration/kindutil"
	"github.com/ome-projects/ome/tests/integration/util"
)

// =========================================================================
// ISVC + namespace fixtures
// =========================================================================

// applyFixture creates the CBM, CSR, and ISVC in order. It waits for the
// in-process manager's cached reader to observe the CBM + CSR before creating
// the ISVC, so the first reconcile doesn't miss the model + runtime (see
// kindfix.ApplyModelRuntime). Caller chains ginkgo.DeferCleanup to tear down.
func applyFixture(g gomega.Gomega, f *coordFixture) {
	kindfix.ApplyModelRuntime(suiteCtx, g, k8sClient, f.CBM, f.CSR, kindfix.ApplyModelRuntimeOpts{
		CachedReader: kindutil.Suite().Manager.GetClient(),
	})
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
