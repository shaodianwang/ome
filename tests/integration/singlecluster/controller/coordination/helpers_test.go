package coordination

import (
	"fmt"
	"time"

	"github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/coordination"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/query"
	"github.com/ome-projects/ome/tests/integration/util"
)

// Polling cadence + timeout knobs shared across the suite. Tuned to
// give the kubelet + EndpointSlice simulators (poll at 100ms) enough
// ticks to converge a 2-Component coordination scenario.
//
// Several specs across the upcoming commit plan reference these
// constants directly (state-machine timing thresholds, drain wait
// budgets) rather than the wider util.* values; keep them defined here
// even though the producer commits don't reference them yet.
//
//nolint:unused // referenced by upcoming state-machine + pacing specs
const (
	pollInterval = 200 * time.Millisecond
	shortTimeout = 10 * time.Second
	medTimeout   = 60 * time.Second
	longTimeout  = 180 * time.Second
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
	cachedClient := suiteFwk.Manager().GetClient()
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
// with the per-revision selector key `ome.io/revision-hash`. Used by
// SVC-* and CLEAN-* tests to assert which Services coordination has
// materialized.
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

// perRevisionServiceFor returns the routing Service for
// (isvc, component, revisionHash), or fails the assertion via g if
// absent. Tests reading specific fields off a Service use this rather
// than digging through the full list.
//
//nolint:unused // referenced by upcoming state-machine specs (A-*, PM-*, S-*)
func perRevisionServiceFor(g gomega.Gomega, namespace, isvc string, component v1beta1.ComponentType, revHash string) corev1.Service {
	svc := &corev1.Service{}
	name := coordination.PerRevisionServiceName(isvc, component, revHash)
	g.ExpectWithOffset(1, k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: namespace, Name: name}, svc)).To(gomega.Succeed())
	return *svc
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

// groupStatusByName returns the named RolloutCoordinationGroupStatus
// from the ISVC's coordination status, or nil when absent. The name is
// the stringified group index ("0", "1", ...) the coordination layer
// derives from the spec.
//
//nolint:unused // referenced by upcoming state-machine specs (A-*, PM-*, S-*, I-*, R-*)
func groupStatusByName(isvc *v1beta1.InferenceService, name string) *v1beta1.RolloutCoordinationGroupStatus {
	if isvc.Status.RolloutCoordination == nil {
		return nil
	}
	for i := range isvc.Status.RolloutCoordination.Groups {
		g := &isvc.Status.RolloutCoordination.Groups[i]
		if g.Name == name {
			return g
		}
	}
	return nil
}

// trafficForComponent returns the per-Component Traffic[] from the
// ISVC's component status, or nil when the entry is missing. Used by
// T-* assertions that check the producer-side write.
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
//
//nolint:unused // referenced by upcoming peer-discovery (D-*) + state-machine specs
func listOMENativePods(g gomega.Gomega, namespace, isvcName string) []corev1.Pod {
	pods := &corev1.PodList{}
	g.ExpectWithOffset(1, k8sClient.List(suiteCtx, pods,
		client.InNamespace(namespace),
		client.MatchingLabels{
			util.LabelManagedByKey: util.ManagedByOMENativeValue,
			"inferenceservice":     isvcName,
		},
	)).To(gomega.Succeed())
	return pods.Items
}

// listOMENativeCRs returns every ControllerRevision in namespace tagged
// with the OMENative managed-by label. Used by tests that assert on
// revision history alongside per-revision Service materialization.
//
//nolint:unused // referenced by upcoming cleanup (CLEAN-*) + lifecycle specs
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

// describeServices renders a per-revision Service list as a short
// human-readable summary for assertion failure messages.
func describeServices(svcs []corev1.Service) string {
	if len(svcs) == 0 {
		return "(none)"
	}
	out := ""
	for _, s := range svcs {
		out += fmt.Sprintf("\n  %s sel=%v", s.Name, s.Spec.Selector)
	}
	return out
}

// describeTraffic renders a Traffic[] slice in compact form.
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
