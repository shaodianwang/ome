package inferenceservice

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/traffic"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/traffic/translators/envoygateway"
	v1beta1testing "github.com/ome-projects/ome/pkg/utils/testing/v1beta1"

	"github.com/ome-projects/ome/tests/integration/util"
)

// OEP-0011 traffic-management integration specs.
//
// These exercise the traffic.Reconciler directly against a real
// envtest apiserver with the Envoy Gateway BackendTrafficPolicy CRD
// loaded (via the EnvoyGateway optional bundle in suite_test.go). The
// translator is envoygateway.New() — same code path that runs on a
// production EG cluster.
//
// We bypass the full InferenceServiceReconciler chain (which needs a
// BaseModel + ServingRuntime to reach the traffic step) and drive
// traffic.Reconciler.Reconcile directly with a hand-built ISVC. This
// keeps the test focused on the translator -> apply -> status loop
// without re-testing model/runtime resolution.
//
// What is NOT tested here:
//   - Gateway-controller acceptance (no EG control plane in envtest).
//     ObserveAcceptance returns Pending; the condition stays at
//     Unknown/Pending. Acceptance mapping is unit-tested in
//     translators/envoygateway.

var btpGVK = schema.GroupVersionKind{
	Group:   "gateway.envoyproxy.io",
	Version: "v1alpha1",
	Kind:    "BackendTrafficPolicy",
}

func newBTP() *unstructured.Unstructured {
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(btpGVK)
	return u
}

var _ = ginkgo.Describe("OEP-0011 traffic reconciler", ginkgo.Ordered, func() {
	var (
		ns *corev1.Namespace
		r  *traffic.Reconciler
	)

	ginkgo.BeforeAll(func() {
		// One reconciler per Describe — translator selection is
		// process-level in production.
		r = traffic.NewReconciler(k8sClient, k8sClient.Scheme(), envoygateway.New())
	})

	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "traffic-reco-")
		Expect(err).NotTo(HaveOccurred())
	})

	ginkgo.AfterEach(func() {
		// Clean up any BTPs left in the namespace.
		btps := &unstructured.UnstructuredList{}
		btps.SetGroupVersionKind(btpGVK)
		_ = k8sClient.List(suiteCtx, btps, client.InNamespace(ns.Name))
		for i := range btps.Items {
			_ = k8sClient.Delete(suiteCtx, &btps.Items[i])
		}
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	// isvcWith builds a minimal ISVC. The traffic.Reconciler only reads
	// spec.traffic / annotations / metadata so the spec doesn't need a
	// real model+runtime here — we never invoke the full ISVC reconciler.
	isvcWith := func(name string, mutate func(*v1beta1.InferenceService)) *v1beta1.InferenceService {
		w := v1beta1testing.MakeInferenceService(name, ns.Name)
		isvc := w.Obj()
		// Persist so owner references resolve.
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())
		Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(isvc), isvc)).To(Succeed())
		if mutate != nil {
			mutate(isvc)
		}
		return isvc
	}

	ginkgo.It("emits a BTP with the right shape when spec.traffic.algorithm + ConsistentHash declared", func() {
		alg := v1beta1.LoadBalancingTypeConsistentHash
		isvc := isvcWith("isvc-lb", func(i *v1beta1.InferenceService) {
			i.Spec.Traffic = &v1beta1.TrafficSpec{
				Algorithm: &alg,
				ConsistentHash: &v1beta1.ConsistentHashSpec{
					Type:    v1beta1.HashTypeHeader,
					Headers: []v1beta1.HashHeader{{Name: "X-Session-ID"}},
				},
			}
		})

		status, err := r.Reconcile(suiteCtx, isvc, []string{"isvc-lb", "isvc-lb-engine"})
		Expect(err).NotTo(HaveOccurred())
		Expect(status).NotTo(BeNil())

		got := newBTP()
		Expect(k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: ns.Name, Name: "isvc-lb"}, got)).To(Succeed())

		alg2, _, _ := unstructured.NestedString(got.Object, "spec", "loadBalancer", "type")
		Expect(alg2).To(Equal("ConsistentHash"))
		hdr, _, _ := unstructured.NestedString(got.Object, "spec", "loadBalancer", "consistentHash", "header", "name")
		Expect(hdr).To(Equal("X-Session-ID"))

		// Owner reference points back to the ISVC.
		Expect(got.GetOwnerReferences()).To(HaveLen(1))
		Expect(got.GetOwnerReferences()[0].Name).To(Equal("isvc-lb"))

		// Status reflects emission with Pending acceptance.
		Expect(status.Algorithm).To(Equal("ConsistentHash"))
		Expect(status.BackendPolicyResource).NotTo(BeNil())
		Expect(status.BackendPolicyResource.Name).To(Equal("isvc-lb"))
		Expect(status.Conditions).To(HaveLen(1))
		Expect(status.Conditions[0].Type).To(Equal(v1beta1.TrafficConditionBackendPolicyReady))
		Expect(status.Conditions[0].Status).To(Equal(metav1.ConditionUnknown))
		Expect(status.Conditions[0].Reason).To(Equal(v1beta1.TrafficReasonPending))
	})

	ginkgo.It("updates the BTP in place when intent changes", func() {
		alg := v1beta1.LoadBalancingTypeRoundRobin
		isvc := isvcWith("isvc-update", func(i *v1beta1.InferenceService) {
			i.Spec.Traffic = &v1beta1.TrafficSpec{Algorithm: &alg}
		})

		_, err := r.Reconcile(suiteCtx, isvc, []string{"isvc-update"})
		Expect(err).NotTo(HaveOccurred())

		got := newBTP()
		Expect(k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: ns.Name, Name: "isvc-update"}, got)).To(Succeed())
		v, _, _ := unstructured.NestedString(got.Object, "spec", "loadBalancer", "type")
		Expect(v).To(Equal("RoundRobin"))

		// Flip to LeastRequest; reconcile again.
		newAlg := v1beta1.LoadBalancingTypeLeastRequest
		isvc.Spec.Traffic = &v1beta1.TrafficSpec{Algorithm: &newAlg}
		_, err = r.Reconcile(suiteCtx, isvc, []string{"isvc-update"})
		Expect(err).NotTo(HaveOccurred())

		got2 := newBTP()
		Expect(k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: ns.Name, Name: "isvc-update"}, got2)).To(Succeed())
		v2, _, _ := unstructured.NestedString(got2.Object, "spec", "loadBalancer", "type")
		Expect(v2).To(Equal("LeastRequest"))

		// Same BTP (not recreated): UID preserved.
		Expect(got2.GetUID()).To(Equal(got.GetUID()))
	})

	ginkgo.It("deletes the BTP when intent is removed (F2 cleanup)", func() {
		alg := v1beta1.LoadBalancingTypeRoundRobin
		isvc := isvcWith("isvc-cleanup", func(i *v1beta1.InferenceService) {
			i.Spec.Traffic = &v1beta1.TrafficSpec{Algorithm: &alg}
		})

		// Emit BTP.
		status, err := r.Reconcile(suiteCtx, isvc, []string{"isvc-cleanup"})
		Expect(err).NotTo(HaveOccurred())
		Expect(status.BackendPolicyResource).NotTo(BeNil())

		// Persist the status into the ISVC so F2's gate sees it. The
		// full ISVC controller is registered in this suite and reconciles
		// the test fixture too (it requeues on "model reference is
		// required"), so the resourceVersion can advance between our
		// Get-implicit-in-isvc and our Status().Update. Refresh-and-retry
		// under Eventually to ride out that race.
		Eventually(func(g Gomega) {
			fresh := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(isvc), fresh)).To(Succeed())
			fresh.Status.Traffic = status
			g.Expect(k8sClient.Status().Update(suiteCtx, fresh)).To(Succeed())
			isvc.ResourceVersion = fresh.ResourceVersion
			isvc.Status.Traffic = status
		}).WithTimeout(util.ShortTimeout).WithPolling(util.PollInterval).Should(Succeed())

		// Strip intent.
		isvc.Spec.Traffic = nil

		out, err := r.Reconcile(suiteCtx, isvc, nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(out).To(BeNil(), "no status when no intent")

		// BTP must be gone.
		got := newBTP()
		err = k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: ns.Name, Name: "isvc-cleanup"}, got)
		Expect(apierrors.IsNotFound(err)).To(BeTrue(), "expected BTP to be deleted; got err=%v", err)
	})

	ginkgo.It("surfaces UnsupportedField when operator sets an Istio-only passthrough on EG", func() {
		alg := v1beta1.LoadBalancingTypeRoundRobin
		isvc := isvcWith("isvc-unsupported", func(i *v1beta1.InferenceService) {
			i.Spec.Traffic = &v1beta1.TrafficSpec{Algorithm: &alg}
			i.Annotations = map[string]string{
				"ome.io/dr.trafficPolicy.connectionPool.tcp.tcpKeepalive.time": "30s",
			}
		})

		status, err := r.Reconcile(suiteCtx, isvc, []string{"isvc-unsupported"})
		Expect(err).NotTo(HaveOccurred())
		Expect(status.Conditions).To(HaveLen(2))

		var unsupported *metav1.Condition
		for i := range status.Conditions {
			if status.Conditions[i].Type == v1beta1.TrafficConditionBackendPolicyUnsupportedFields {
				unsupported = &status.Conditions[i]
			}
		}
		Expect(unsupported).NotTo(BeNil(), "expected BackendPolicyUnsupportedFields condition")
		Expect(unsupported.Status).To(Equal(metav1.ConditionTrue))
		Expect(unsupported.Reason).To(Equal(v1beta1.TrafficReasonUnsupportedField))
		Expect(unsupported.Message).To(ContainSubstring("ome.io/dr.trafficPolicy.connectionPool.tcp.tcpKeepalive.time"))
	})

	ginkgo.It("skips the BTP entirely when no intent is declared", func() {
		isvc := isvcWith("isvc-no-intent", nil)

		status, err := r.Reconcile(suiteCtx, isvc, []string{"isvc-no-intent"})
		Expect(err).NotTo(HaveOccurred())
		Expect(status).To(BeNil())

		got := newBTP()
		err = k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: ns.Name, Name: "isvc-no-intent"}, got)
		Expect(apierrors.IsNotFound(err)).To(BeTrue())
	})
})
