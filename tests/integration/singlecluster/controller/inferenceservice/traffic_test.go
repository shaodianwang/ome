package inferenceservice

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	knapis "knative.dev/pkg/apis"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/traffic"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/traffic/translators/envoygateway"
	v1beta1testing "github.com/ome-projects/ome/pkg/utils/testing/v1beta1"

	"github.com/ome-projects/ome/tests/integration/util"
)

// Traffic-management integration specs.
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

var _ = ginkgo.Describe("traffic reconciler", ginkgo.Ordered, func() {
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

	ginkgo.It("A1: BTP targetRefs lists every OME-managed HTTPRoute (engine-only ISVC)", func() {
		alg := v1beta1.LoadBalancingTypeLeastRequest
		isvc := isvcWith("isvc-targets", func(i *v1beta1.InferenceService) {
			i.Spec.Traffic = &v1beta1.TrafficSpec{Algorithm: &alg}
		})
		// Engine-only ISVC: expect <isvc> (top-level) + <isvc>-engine.
		routes := []string{"isvc-targets", "isvc-targets-engine"}

		_, err := r.Reconcile(suiteCtx, isvc, routes)
		Expect(err).NotTo(HaveOccurred())

		got := newBTP()
		Expect(k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: ns.Name, Name: "isvc-targets"}, got)).To(Succeed())

		targetRefs, found, err := unstructured.NestedSlice(got.Object, "spec", "targetRefs")
		Expect(err).NotTo(HaveOccurred())
		Expect(found).To(BeTrue(), "spec.targetRefs missing on emitted BTP")
		Expect(targetRefs).To(HaveLen(2), "expected 2 targetRefs (top-level + engine) for engine-only ISVC")

		names := make([]string, 0, len(targetRefs))
		for _, ref := range targetRefs {
			refMap, ok := ref.(map[string]interface{})
			Expect(ok).To(BeTrue())
			Expect(refMap["group"]).To(Equal("gateway.networking.k8s.io"))
			Expect(refMap["kind"]).To(Equal("HTTPRoute"))
			names = append(names, refMap["name"].(string))
		}
		Expect(names).To(ConsistOf("isvc-targets", "isvc-targets-engine"))
	})

	ginkgo.It("A3: single circuit-breaker annotation → BTP circuitBreaker, no loadBalancer block", func() {
		isvc := isvcWith("isvc-cb-perep", func(i *v1beta1.InferenceService) {
			i.Annotations = map[string]string{
				"ome.io/circuit-breaker-per-endpoint-max-connections": "1",
			}
		})

		_, err := r.Reconcile(suiteCtx, isvc, []string{"isvc-cb-perep"})
		Expect(err).NotTo(HaveOccurred())

		got := newBTP()
		Expect(k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: ns.Name, Name: "isvc-cb-perep"}, got)).To(Succeed())

		// Annotation maps to perEndpoint.maxConnections.
		perEpMaxConn, found, err := unstructured.NestedInt64(got.Object, "spec", "circuitBreaker", "perEndpoint", "maxConnections")
		Expect(err).NotTo(HaveOccurred())
		Expect(found).To(BeTrue(), "circuitBreaker.perEndpoint.maxConnections missing")
		Expect(perEpMaxConn).To(Equal(int64(1)))

		// Annotation-only intent (no spec.traffic.algorithm) → no loadBalancer block.
		_, foundLB, _ := unstructured.NestedMap(got.Object, "spec", "loadBalancer")
		Expect(foundLB).To(BeFalse(), "annotation-only intent must not emit a loadBalancer block")
	})

	ginkgo.It("A2: endpointOverride.type=Header → BTP loadBalancer.endpointOverride.extractFrom", func() {
		alg := v1beta1.LoadBalancingTypeRoundRobin
		isvc := isvcWith("isvc-endpoint-override", func(i *v1beta1.InferenceService) {
			i.Spec.Traffic = &v1beta1.TrafficSpec{
				Algorithm: &alg,
				EndpointOverride: &v1beta1.EndpointOverrideSpec{
					Type:    v1beta1.EndpointOverrideTypeHeader,
					Headers: []v1beta1.HashHeader{{Name: "X-Endpoint-HostPort"}},
				},
			}
		})

		_, err := r.Reconcile(suiteCtx, isvc, []string{"isvc-endpoint-override"})
		Expect(err).NotTo(HaveOccurred())

		got := newBTP()
		Expect(k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: ns.Name, Name: "isvc-endpoint-override"}, got)).To(Succeed())

		extractFrom, found, err := unstructured.NestedSlice(got.Object, "spec", "loadBalancer", "endpointOverride", "extractFrom")
		Expect(err).NotTo(HaveOccurred())
		Expect(found).To(BeTrue(), "endpointOverride.extractFrom missing on emitted BTP")
		Expect(extractFrom).To(HaveLen(1))
		first, ok := extractFrom[0].(map[string]interface{})
		Expect(ok).To(BeTrue())
		Expect(first["header"]).To(Equal("X-Endpoint-HostPort"))
	})

	ginkgo.It("A4: ome.io/btp.* pass-through stitched into BTP + condition message lists key", func() {
		isvc := isvcWith("isvc-passthrough", func(i *v1beta1.InferenceService) {
			i.Annotations = map[string]string{
				"ome.io/btp.loadBalancer.slowStart.window": "30s",
			}
		})

		status, err := r.Reconcile(suiteCtx, isvc, []string{"isvc-passthrough"})
		Expect(err).NotTo(HaveOccurred())
		Expect(status).NotTo(BeNil())

		got := newBTP()
		Expect(k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: ns.Name, Name: "isvc-passthrough"}, got)).To(Succeed())

		// The pass-through key was stitched verbatim into the BTP at
		// the same dotted path.
		window, found, err := unstructured.NestedString(got.Object, "spec", "loadBalancer", "slowStart", "window")
		Expect(err).NotTo(HaveOccurred())
		Expect(found).To(BeTrue(), "spec.loadBalancer.slowStart.window missing on emitted BTP")
		Expect(window).To(Equal("30s"))

		// TrafficStatus condition message lists the pass-through key so
		// operators can audit what was applied.
		Expect(status.Conditions).NotTo(BeEmpty())
		var ready *metav1.Condition
		for i := range status.Conditions {
			if status.Conditions[i].Type == v1beta1.TrafficConditionBackendPolicyReady {
				ready = &status.Conditions[i]
			}
		}
		Expect(ready).NotTo(BeNil())
		Expect(ready.Message).To(ContainSubstring("loadBalancer.slowStart.window"),
			"condition message must list the pass-through key applied")
	})

	ginkgo.It("A5: ISVC owner-reference on BTP is GC-correct (controller=true, BlockOwnerDeletion, UID match)", func() {
		// envtest doesn't run the kube-controller-manager so we can't
		// observe an actual cascade delete; instead we assert the
		// OwnerReference fields that Kubernetes' GC controller relies on
		// (Controller=true + BlockOwnerDeletion=true + UID match), so
		// that a real cluster would GC the BTP on ISVC delete.
		alg := v1beta1.LoadBalancingTypeRoundRobin
		isvc := isvcWith("isvc-gc-contract", func(i *v1beta1.InferenceService) {
			i.Spec.Traffic = &v1beta1.TrafficSpec{Algorithm: &alg}
		})

		_, err := r.Reconcile(suiteCtx, isvc, []string{"isvc-gc-contract"})
		Expect(err).NotTo(HaveOccurred())

		got := newBTP()
		Expect(k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: ns.Name, Name: "isvc-gc-contract"}, got)).To(Succeed())

		refs := got.GetOwnerReferences()
		Expect(refs).To(HaveLen(1), "BTP must carry exactly one owner reference")
		ref := refs[0]
		Expect(ref.APIVersion).To(Equal("ome.io/v1beta1"))
		Expect(ref.Kind).To(Equal("InferenceService"))
		Expect(ref.Name).To(Equal("isvc-gc-contract"))
		Expect(ref.UID).To(Equal(isvc.UID), "owner-ref UID must match the ISVC's UID")
		Expect(ref.Controller).NotTo(BeNil())
		Expect(*ref.Controller).To(BeTrue(), "Controller=true required so GC follows the ref")
		Expect(ref.BlockOwnerDeletion).NotTo(BeNil())
		Expect(*ref.BlockOwnerDeletion).To(BeTrue(), "BlockOwnerDeletion=true so Foreground delete waits")
	})

	ginkgo.It("B1: algorithm=RoundRobin → BTP loadBalancer.type=RoundRobin, no consistentHash", func() {
		alg := v1beta1.LoadBalancingTypeRoundRobin
		isvc := isvcWith("isvc-b1", func(i *v1beta1.InferenceService) {
			i.Spec.Traffic = &v1beta1.TrafficSpec{Algorithm: &alg}
		})
		_, err := r.Reconcile(suiteCtx, isvc, []string{"isvc-b1"})
		Expect(err).NotTo(HaveOccurred())

		got := newBTP()
		Expect(k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: ns.Name, Name: "isvc-b1"}, got)).To(Succeed())

		typ, _, _ := unstructured.NestedString(got.Object, "spec", "loadBalancer", "type")
		Expect(typ).To(Equal("RoundRobin"))
		_, foundCH, _ := unstructured.NestedMap(got.Object, "spec", "loadBalancer", "consistentHash")
		Expect(foundCH).To(BeFalse(), "RoundRobin must NOT emit a consistentHash block")
	})

	ginkgo.It("B2: algorithm=LeastRequest → BTP loadBalancer.type=LeastRequest", func() {
		alg := v1beta1.LoadBalancingTypeLeastRequest
		isvc := isvcWith("isvc-b2", func(i *v1beta1.InferenceService) {
			i.Spec.Traffic = &v1beta1.TrafficSpec{Algorithm: &alg}
		})
		_, err := r.Reconcile(suiteCtx, isvc, []string{"isvc-b2"})
		Expect(err).NotTo(HaveOccurred())

		got := newBTP()
		Expect(k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: ns.Name, Name: "isvc-b2"}, got)).To(Succeed())
		typ, _, _ := unstructured.NestedString(got.Object, "spec", "loadBalancer", "type")
		Expect(typ).To(Equal("LeastRequest"))
	})

	ginkgo.It("B3: algorithm=Random → BTP loadBalancer.type=Random", func() {
		alg := v1beta1.LoadBalancingTypeRandom
		isvc := isvcWith("isvc-b3", func(i *v1beta1.InferenceService) {
			i.Spec.Traffic = &v1beta1.TrafficSpec{Algorithm: &alg}
		})
		_, err := r.Reconcile(suiteCtx, isvc, []string{"isvc-b3"})
		Expect(err).NotTo(HaveOccurred())

		got := newBTP()
		Expect(k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: ns.Name, Name: "isvc-b3"}, got)).To(Succeed())
		typ, _, _ := unstructured.NestedString(got.Object, "spec", "loadBalancer", "type")
		Expect(typ).To(Equal("Random"))
	})

	ginkgo.It("B4: ConsistentHash type=Cookie + TTL → BTP consistentHash.cookie.{name,ttl}", func() {
		alg := v1beta1.LoadBalancingTypeConsistentHash
		ttl := int64(3600)
		isvc := isvcWith("isvc-b4", func(i *v1beta1.InferenceService) {
			i.Spec.Traffic = &v1beta1.TrafficSpec{
				Algorithm: &alg,
				ConsistentHash: &v1beta1.ConsistentHashSpec{
					Type:   v1beta1.HashTypeCookie,
					Cookie: &v1beta1.HashCookie{Name: "session", TTLSeconds: &ttl},
				},
			}
		})
		_, err := r.Reconcile(suiteCtx, isvc, []string{"isvc-b4"})
		Expect(err).NotTo(HaveOccurred())

		got := newBTP()
		Expect(k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: ns.Name, Name: "isvc-b4"}, got)).To(Succeed())
		name, _, _ := unstructured.NestedString(got.Object, "spec", "loadBalancer", "consistentHash", "cookie", "name")
		Expect(name).To(Equal("session"))
		ttlOut, _, _ := unstructured.NestedString(got.Object, "spec", "loadBalancer", "consistentHash", "cookie", "ttl")
		Expect(ttlOut).To(Equal("3600s"))
		// No header / sourceIP blocks should sneak in.
		_, foundHeader, _ := unstructured.NestedMap(got.Object, "spec", "loadBalancer", "consistentHash", "header")
		Expect(foundHeader).To(BeFalse())
	})

	ginkgo.It("B5: ConsistentHash type=SourceIP → BTP consistentHash.type=SourceIP, no header/cookie", func() {
		alg := v1beta1.LoadBalancingTypeConsistentHash
		isvc := isvcWith("isvc-b5", func(i *v1beta1.InferenceService) {
			i.Spec.Traffic = &v1beta1.TrafficSpec{
				Algorithm:      &alg,
				ConsistentHash: &v1beta1.ConsistentHashSpec{Type: v1beta1.HashTypeSourceIP},
			}
		})
		_, err := r.Reconcile(suiteCtx, isvc, []string{"isvc-b5"})
		Expect(err).NotTo(HaveOccurred())

		got := newBTP()
		Expect(k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: ns.Name, Name: "isvc-b5"}, got)).To(Succeed())
		hashType, _, _ := unstructured.NestedString(got.Object, "spec", "loadBalancer", "consistentHash", "type")
		Expect(hashType).To(Equal("SourceIP"))
		// EG infers source-IP hashing from type alone; no header/cookie sub-blocks expected.
		_, foundHeader, _ := unstructured.NestedMap(got.Object, "spec", "loadBalancer", "consistentHash", "header")
		Expect(foundHeader).To(BeFalse())
		_, foundCookie, _ := unstructured.NestedMap(got.Object, "spec", "loadBalancer", "consistentHash", "cookie")
		Expect(foundCookie).To(BeFalse())
	})

	ginkgo.It("B6: ConsistentHash with multiple headers → BTP consistentHash.headers[] (plural EG v1.7+ form)", func() {
		// EG v1.7+ supports multi-header consistent hashing via the
		// plural form: spec.loadBalancer.consistentHash.type="Headers"
		// + headers[] list. OME's API exposes the singular "Header"
		// enum value; the translator promotes to the plural EG form
		// when the operator declares 2+ headers.
		alg := v1beta1.LoadBalancingTypeConsistentHash
		isvc := isvcWith("isvc-b6", func(i *v1beta1.InferenceService) {
			i.Spec.Traffic = &v1beta1.TrafficSpec{
				Algorithm: &alg,
				ConsistentHash: &v1beta1.ConsistentHashSpec{
					Type: v1beta1.HashTypeHeader,
					Headers: []v1beta1.HashHeader{
						{Name: "X-User"},
						{Name: "X-Session"},
					},
				},
			}
		})
		_, err := r.Reconcile(suiteCtx, isvc, []string{"isvc-b6"})
		Expect(err).NotTo(HaveOccurred())

		got := newBTP()
		Expect(k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: ns.Name, Name: "isvc-b6"}, got)).To(Succeed())

		// Type promoted to plural "Headers".
		typ, _, _ := unstructured.NestedString(got.Object, "spec", "loadBalancer", "consistentHash", "type")
		Expect(typ).To(Equal("Headers"), "translator must promote type to Headers when multiple headers are declared")

		// Plural headers[] populated with both entries, in order.
		headers, found, err := unstructured.NestedSlice(got.Object, "spec", "loadBalancer", "consistentHash", "headers")
		Expect(err).NotTo(HaveOccurred())
		Expect(found).To(BeTrue(), "consistentHash.headers (plural list) must be set")
		Expect(headers).To(HaveLen(2))
		names := make([]string, 0, len(headers))
		for _, h := range headers {
			m, ok := h.(map[string]interface{})
			Expect(ok).To(BeTrue())
			names = append(names, m["name"].(string))
		}
		Expect(names).To(Equal([]string{"X-User", "X-Session"}), "header order must be preserved")

		// Singular header field must NOT also be set (EG CEL: type/field XOR).
		_, foundSingular, _ := unstructured.NestedMap(got.Object, "spec", "loadBalancer", "consistentHash", "header")
		Expect(foundSingular).To(BeFalse(), "type=Headers must not also set the singular header object")
	})

	ginkgo.It("B7: annotation-only intent (no spec.traffic) → no loadBalancer block, status.algorithm=Default", func() {
		isvc := isvcWith("isvc-b7", func(i *v1beta1.InferenceService) {
			i.Annotations = map[string]string{
				"ome.io/circuit-breaker-max-connections": "512",
			}
		})

		status, err := r.Reconcile(suiteCtx, isvc, []string{"isvc-b7"})
		Expect(err).NotTo(HaveOccurred())
		Expect(status).NotTo(BeNil())

		got := newBTP()
		Expect(k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: ns.Name, Name: "isvc-b7"}, got)).To(Succeed())

		// Circuit breaker field present.
		maxConn, found, err := unstructured.NestedInt64(got.Object, "spec", "circuitBreaker", "maxConnections")
		Expect(err).NotTo(HaveOccurred())
		Expect(found).To(BeTrue())
		Expect(maxConn).To(Equal(int64(512)))
		// No loadBalancer block since no algorithm declared.
		_, foundLB, _ := unstructured.NestedMap(got.Object, "spec", "loadBalancer")
		Expect(foundLB).To(BeFalse())
		// Status carries the "Default" sentinel so operators can distinguish
		// "OME defaulted" from any concrete algorithm name.
		Expect(status.Algorithm).To(Equal("Default"))
	})

	// cbField sets one circuit-breaker annotation, reconciles, and
	// asserts the BTP integer field at the given nested path.
	cbField := func(name, annotation, valueStr string, want int64, path ...string) {
		isvc := isvcWith(name, func(i *v1beta1.InferenceService) {
			i.Annotations = map[string]string{annotation: valueStr}
		})
		_, err := r.Reconcile(suiteCtx, isvc, []string{name})
		Expect(err).NotTo(HaveOccurred())

		got := newBTP()
		Expect(k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: ns.Name, Name: name}, got)).To(Succeed())
		v, found, err := unstructured.NestedInt64(got.Object, append([]string{"spec", "circuitBreaker"}, path...)...)
		Expect(err).NotTo(HaveOccurred())
		Expect(found).To(BeTrue(), "field %v missing on BTP", path)
		Expect(v).To(Equal(want))
	}

	ginkgo.It("C1: ome.io/circuit-breaker-max-connections → BTP circuitBreaker.maxConnections", func() {
		cbField("isvc-c1", "ome.io/circuit-breaker-max-connections", "1024", 1024, "maxConnections")
	})

	ginkgo.It("C2: ome.io/circuit-breaker-max-parallel-requests → BTP circuitBreaker.maxParallelRequests", func() {
		cbField("isvc-c2", "ome.io/circuit-breaker-max-parallel-requests", "256", 256, "maxParallelRequests")
	})

	ginkgo.It("C3: ome.io/circuit-breaker-max-pending-requests → BTP circuitBreaker.maxPendingRequests", func() {
		cbField("isvc-c3", "ome.io/circuit-breaker-max-pending-requests", "128", 128, "maxPendingRequests")
	})

	ginkgo.It("C4: ome.io/circuit-breaker-max-parallel-retries → BTP circuitBreaker.maxParallelRetries", func() {
		cbField("isvc-c4", "ome.io/circuit-breaker-max-parallel-retries", "8", 8, "maxParallelRetries")
	})

	ginkgo.It("C5: per-endpoint CB annotation + ConsistentHash → BTP carries both blocks (recommended composition)", func() {
		// The perEndpoint CB knob is the
		// recommended pairing with ConsistentHash so a single hot
		// session can't open many connections to one pod and starve
		// other sessions sharing the same hash bucket.
		alg := v1beta1.LoadBalancingTypeConsistentHash
		isvc := isvcWith("isvc-c5", func(i *v1beta1.InferenceService) {
			i.Spec.Traffic = &v1beta1.TrafficSpec{
				Algorithm: &alg,
				ConsistentHash: &v1beta1.ConsistentHashSpec{
					Type:    v1beta1.HashTypeHeader,
					Headers: []v1beta1.HashHeader{{Name: "X-Session-ID"}},
				},
			}
			i.Annotations = map[string]string{
				"ome.io/circuit-breaker-per-endpoint-max-connections": "1",
			}
		})

		_, err := r.Reconcile(suiteCtx, isvc, []string{"isvc-c5"})
		Expect(err).NotTo(HaveOccurred())

		got := newBTP()
		Expect(k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: ns.Name, Name: "isvc-c5"}, got)).To(Succeed())

		// ConsistentHash block is intact.
		typ, _, _ := unstructured.NestedString(got.Object, "spec", "loadBalancer", "type")
		Expect(typ).To(Equal("ConsistentHash"))
		hdr, _, _ := unstructured.NestedString(got.Object, "spec", "loadBalancer", "consistentHash", "header", "name")
		Expect(hdr).To(Equal("X-Session-ID"))
		// perEndpoint.maxConnections is present from the annotation.
		v, found, _ := unstructured.NestedInt64(got.Object, "spec", "circuitBreaker", "perEndpoint", "maxConnections")
		Expect(found).To(BeTrue())
		Expect(v).To(Equal(int64(1)))
	})

	ginkgo.It("C6: retry-attempts + retry-on (5xx, reset) → BTP retry.numRetries + retry.retryOn.triggers", func() {
		isvc := isvcWith("isvc-c6", func(i *v1beta1.InferenceService) {
			i.Annotations = map[string]string{
				"ome.io/retry-attempts": "3",
				"ome.io/retry-on":       "5xx,reset",
			}
		})
		_, err := r.Reconcile(suiteCtx, isvc, []string{"isvc-c6"})
		Expect(err).NotTo(HaveOccurred())

		got := newBTP()
		Expect(k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: ns.Name, Name: "isvc-c6"}, got)).To(Succeed())

		n, _, _ := unstructured.NestedInt64(got.Object, "spec", "retry", "numRetries")
		Expect(n).To(Equal(int64(3)))
		triggers, found, _ := unstructured.NestedSlice(got.Object, "spec", "retry", "retryOn", "triggers")
		Expect(found).To(BeTrue())
		got2 := make([]string, 0, len(triggers))
		for _, t := range triggers {
			got2 = append(got2, t.(string))
		}
		Expect(got2).To(ConsistOf("5xx", "reset"))
	})

	ginkgo.It("C7: retry-per-try-timeout → BTP retry.perRetry.timeout", func() {
		isvc := isvcWith("isvc-c7", func(i *v1beta1.InferenceService) {
			i.Annotations = map[string]string{
				"ome.io/retry-attempts":        "3",
				"ome.io/retry-on":              "5xx",
				"ome.io/retry-per-try-timeout": "2s",
			}
		})
		_, err := r.Reconcile(suiteCtx, isvc, []string{"isvc-c7"})
		Expect(err).NotTo(HaveOccurred())

		got := newBTP()
		Expect(k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: ns.Name, Name: "isvc-c7"}, got)).To(Succeed())
		v, _, _ := unstructured.NestedString(got.Object, "spec", "retry", "perRetry", "timeout")
		Expect(v).To(Equal("2s"))
	})

	ginkgo.It("C8: timeout-idle → BTP timeout.http.connectionIdleTimeout", func() {
		isvc := isvcWith("isvc-c8", func(i *v1beta1.InferenceService) {
			i.Annotations = map[string]string{"ome.io/timeout-idle": "2m0s"}
		})
		_, err := r.Reconcile(suiteCtx, isvc, []string{"isvc-c8"})
		Expect(err).NotTo(HaveOccurred())

		got := newBTP()
		Expect(k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: ns.Name, Name: "isvc-c8"}, got)).To(Succeed())
		v, _, _ := unstructured.NestedString(got.Object, "spec", "timeout", "http", "connectionIdleTimeout")
		Expect(v).To(Equal("2m0s"))
	})

	ginkgo.It("C9: timeout-max-connection-duration → BTP timeout.http.maxConnectionDuration", func() {
		isvc := isvcWith("isvc-c9", func(i *v1beta1.InferenceService) {
			i.Annotations = map[string]string{"ome.io/timeout-max-connection-duration": "5m0s"}
		})
		_, err := r.Reconcile(suiteCtx, isvc, []string{"isvc-c9"})
		Expect(err).NotTo(HaveOccurred())

		got := newBTP()
		Expect(k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: ns.Name, Name: "isvc-c9"}, got)).To(Succeed())
		v, _, _ := unstructured.NestedString(got.Object, "spec", "timeout", "http", "maxConnectionDuration")
		Expect(v).To(Equal("5m0s"))
	})

	ginkgo.It("C10: timeout-tcp-connect → BTP timeout.tcp.connectTimeout", func() {
		isvc := isvcWith("isvc-c10", func(i *v1beta1.InferenceService) {
			i.Annotations = map[string]string{"ome.io/timeout-tcp-connect": "5s"}
		})
		_, err := r.Reconcile(suiteCtx, isvc, []string{"isvc-c10"})
		Expect(err).NotTo(HaveOccurred())

		got := newBTP()
		Expect(k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: ns.Name, Name: "isvc-c10"}, got)).To(Succeed())
		v, _, _ := unstructured.NestedString(got.Object, "spec", "timeout", "tcp", "connectTimeout")
		Expect(v).To(Equal("5s"))
	})

	ginkgo.It("C11: two ome.io/btp.* pass-through keys at once → both stitched into sibling fields, both in condition message", func() {
		// Two pass-throughs targeting sibling fields under the same
		// parent map. Verifies the translator's unstructured stitch
		// builds (not clobbers) the intermediate slowStart map so both
		// children persist after the second SetNestedField call.
		// Value "30s" parses as string; "1.5" parses as float64
		// (parseScalar best-effort coerces); both shapes must round-
		// trip through unstructured.SetNestedField unchanged.
		isvc := isvcWith("isvc-c11", func(i *v1beta1.InferenceService) {
			i.Annotations = map[string]string{
				"ome.io/btp.loadBalancer.slowStart.window":     "30s",
				"ome.io/btp.loadBalancer.slowStart.aggression": "1.5",
			}
		})

		status, err := r.Reconcile(suiteCtx, isvc, []string{"isvc-c11"})
		Expect(err).NotTo(HaveOccurred())

		got := newBTP()
		Expect(k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: ns.Name, Name: "isvc-c11"}, got)).To(Succeed())

		win, foundWin, _ := unstructured.NestedString(got.Object, "spec", "loadBalancer", "slowStart", "window")
		Expect(foundWin).To(BeTrue(), "window field missing")
		Expect(win).To(Equal("30s"))

		// aggression came in as "1.5"; parseScalar coerces to float64.
		// Use the generic NestedFieldNoCopy accessor since NestedString
		// would return found=false on a non-string value.
		aggVal, foundAgg, err := unstructured.NestedFieldNoCopy(got.Object, "spec", "loadBalancer", "slowStart", "aggression")
		Expect(err).NotTo(HaveOccurred())
		Expect(foundAgg).To(BeTrue(), "aggression field missing — multi-pass-through under same parent dropped a sibling")
		Expect(aggVal).To(BeAssignableToTypeOf(float64(0)), "aggression must round-trip as the parseScalar-inferred type")
		Expect(aggVal.(float64)).To(Equal(1.5))

		// Condition message lists BOTH keys so operators can audit.
		var ready *metav1.Condition
		for i := range status.Conditions {
			if status.Conditions[i].Type == v1beta1.TrafficConditionBackendPolicyReady {
				ready = &status.Conditions[i]
			}
		}
		Expect(ready).NotTo(BeNil())
		Expect(ready.Message).To(ContainSubstring("loadBalancer.slowStart.window"))
		Expect(ready.Message).To(ContainSubstring("loadBalancer.slowStart.aggression"))
	})

	ginkgo.It("C12: kitchen-sink — typed algorithm + every documented annotation → BTP has every field, no collisions", func() {
		// Catches field-collision bugs in the unstructured-stitch
		// translator: if two annotations write overlapping nested
		// paths one would clobber the other.
		alg := v1beta1.LoadBalancingTypeRoundRobin
		isvc := isvcWith("isvc-c12", func(i *v1beta1.InferenceService) {
			i.Spec.Traffic = &v1beta1.TrafficSpec{Algorithm: &alg}
			i.Annotations = map[string]string{
				"ome.io/circuit-breaker-max-connections":              "1024",
				"ome.io/circuit-breaker-max-parallel-requests":        "256",
				"ome.io/circuit-breaker-max-pending-requests":         "128",
				"ome.io/circuit-breaker-max-parallel-retries":         "8",
				"ome.io/circuit-breaker-per-endpoint-max-connections": "2",
				"ome.io/retry-attempts":                               "3",
				"ome.io/retry-on":                                     "5xx,reset",
				"ome.io/retry-per-try-timeout":                        "2s",
				"ome.io/timeout-idle":                                 "2m0s",
				"ome.io/timeout-max-connection-duration":              "5m0s",
				"ome.io/timeout-tcp-connect":                          "5s",
			}
		})

		_, err := r.Reconcile(suiteCtx, isvc, []string{"isvc-c12"})
		Expect(err).NotTo(HaveOccurred())

		got := newBTP()
		Expect(k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: ns.Name, Name: "isvc-c12"}, got)).To(Succeed())

		// Typed algorithm.
		typ, _, _ := unstructured.NestedString(got.Object, "spec", "loadBalancer", "type")
		Expect(typ).To(Equal("RoundRobin"))

		// Every circuit-breaker field present.
		assertCBInt := func(want int64, path ...string) {
			v, found, _ := unstructured.NestedInt64(got.Object, append([]string{"spec", "circuitBreaker"}, path...)...)
			Expect(found).To(BeTrue(), "circuitBreaker.%v missing", path)
			Expect(v).To(Equal(want), "circuitBreaker.%v value", path)
		}
		assertCBInt(1024, "maxConnections")
		assertCBInt(256, "maxParallelRequests")
		assertCBInt(128, "maxPendingRequests")
		assertCBInt(8, "maxParallelRetries")
		assertCBInt(2, "perEndpoint", "maxConnections")

		// Retry fields.
		n, _, _ := unstructured.NestedInt64(got.Object, "spec", "retry", "numRetries")
		Expect(n).To(Equal(int64(3)))
		perRetry, _, _ := unstructured.NestedString(got.Object, "spec", "retry", "perRetry", "timeout")
		Expect(perRetry).To(Equal("2s"))
		triggers, _, _ := unstructured.NestedSlice(got.Object, "spec", "retry", "retryOn", "triggers")
		Expect(triggers).To(HaveLen(2))

		// Timeout fields.
		idle, _, _ := unstructured.NestedString(got.Object, "spec", "timeout", "http", "connectionIdleTimeout")
		Expect(idle).To(Equal("2m0s"))
		maxConn, _, _ := unstructured.NestedString(got.Object, "spec", "timeout", "http", "maxConnectionDuration")
		Expect(maxConn).To(Equal("5m0s"))
		tcp, _, _ := unstructured.NestedString(got.Object, "spec", "timeout", "tcp", "connectTimeout")
		Expect(tcp).To(Equal("5s"))
	})

	// assertTargetRefNames verifies the BTP's spec.targetRefs list has
	// exactly the given HTTPRoute names (each ref is also expected to
	// be a gateway.networking.k8s.io HTTPRoute kind).
	assertTargetRefNames := func(btp *unstructured.Unstructured, want ...string) {
		targetRefs, found, err := unstructured.NestedSlice(btp.Object, "spec", "targetRefs")
		Expect(err).NotTo(HaveOccurred())
		Expect(found).To(BeTrue(), "spec.targetRefs missing")
		Expect(targetRefs).To(HaveLen(len(want)))
		got := make([]string, 0, len(targetRefs))
		for _, ref := range targetRefs {
			refMap, ok := ref.(map[string]interface{})
			Expect(ok).To(BeTrue())
			Expect(refMap["group"]).To(Equal("gateway.networking.k8s.io"))
			Expect(refMap["kind"]).To(Equal("HTTPRoute"))
			got = append(got, refMap["name"].(string))
		}
		Expect(got).To(ConsistOf(want))
	}

	ginkgo.It("D2: Engine + Router ISVC → BTP targetRefs contains top-level + engine + router", func() {
		alg := v1beta1.LoadBalancingTypeRoundRobin
		isvc := isvcWith("isvc-d2", func(i *v1beta1.InferenceService) {
			i.Spec.Engine = &v1beta1.EngineSpec{}
			i.Spec.Router = &v1beta1.RouterSpec{}
			i.Spec.Traffic = &v1beta1.TrafficSpec{Algorithm: &alg}
		})
		routes := []string{"isvc-d2", "isvc-d2-engine", "isvc-d2-router"}

		_, err := r.Reconcile(suiteCtx, isvc, routes)
		Expect(err).NotTo(HaveOccurred())

		got := newBTP()
		Expect(k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: ns.Name, Name: "isvc-d2"}, got)).To(Succeed())
		assertTargetRefNames(got, "isvc-d2", "isvc-d2-engine", "isvc-d2-router")
	})

	ginkgo.It("D3: Engine + Decoder (PD) ISVC → BTP targetRefs contains top-level + engine + decoder", func() {
		alg := v1beta1.LoadBalancingTypeRoundRobin
		isvc := isvcWith("isvc-d3", func(i *v1beta1.InferenceService) {
			i.Spec.Engine = &v1beta1.EngineSpec{}
			i.Spec.Decoder = &v1beta1.DecoderSpec{}
			i.Spec.Traffic = &v1beta1.TrafficSpec{Algorithm: &alg}
		})
		routes := []string{"isvc-d3", "isvc-d3-engine", "isvc-d3-decoder"}

		_, err := r.Reconcile(suiteCtx, isvc, routes)
		Expect(err).NotTo(HaveOccurred())

		got := newBTP()
		Expect(k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: ns.Name, Name: "isvc-d3"}, got)).To(Succeed())
		assertTargetRefNames(got, "isvc-d3", "isvc-d3-engine", "isvc-d3-decoder")
	})

	ginkgo.It("D4: Engine + Router + Decoder ISVC → BTP targetRefs contains all four route names", func() {
		alg := v1beta1.LoadBalancingTypeRoundRobin
		isvc := isvcWith("isvc-d4", func(i *v1beta1.InferenceService) {
			i.Spec.Engine = &v1beta1.EngineSpec{}
			i.Spec.Router = &v1beta1.RouterSpec{}
			i.Spec.Decoder = &v1beta1.DecoderSpec{}
			i.Spec.Traffic = &v1beta1.TrafficSpec{Algorithm: &alg}
		})
		routes := []string{"isvc-d4", "isvc-d4-engine", "isvc-d4-router", "isvc-d4-decoder"}

		_, err := r.Reconcile(suiteCtx, isvc, routes)
		Expect(err).NotTo(HaveOccurred())

		got := newBTP()
		Expect(k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: ns.Name, Name: "isvc-d4"}, got)).To(Succeed())
		assertTargetRefNames(got, "isvc-d4", "isvc-d4-engine", "isvc-d4-router", "isvc-d4-decoder")
	})

	ginkgo.It("D5: ComputeTargetHTTPRoutes builds the right set per Component flags (C1 regression)", func() {
		// ComputeTargetHTTPRoutes is the helper the full ISVC
		// reconciler uses to determine which HTTPRoute names to pass
		// the traffic reconciler. Pre-OEP C1, engine and top-level
		// collided (both isvc.Name); the helper must now produce
		// "<isvc>-engine" distinct from "<isvc>" (top-level).
		isvc := isvcWith("isvc-d5", nil)

		// Engine-only.
		got := traffic.ComputeTargetHTTPRoutes(isvc, false, false)
		Expect(got).To(ConsistOf("isvc-d5", "isvc-d5-engine"))

		// With decoder.
		got = traffic.ComputeTargetHTTPRoutes(isvc, true, false)
		Expect(got).To(ConsistOf("isvc-d5", "isvc-d5-engine", "isvc-d5-decoder"))

		// With router.
		got = traffic.ComputeTargetHTTPRoutes(isvc, false, true)
		Expect(got).To(ConsistOf("isvc-d5", "isvc-d5-engine", "isvc-d5-router"))

		// All three.
		got = traffic.ComputeTargetHTTPRoutes(isvc, true, true)
		Expect(got).To(ConsistOf("isvc-d5", "isvc-d5-engine", "isvc-d5-router", "isvc-d5-decoder"))
	})

	ginkgo.It("F2: fake gateway Accepted=True on BTP → next reconcile flips BackendPolicyReady to True/AcceptedByGateway", func() {
		alg := v1beta1.LoadBalancingTypeRoundRobin
		isvc := isvcWith("isvc-f2", func(i *v1beta1.InferenceService) {
			i.Spec.Traffic = &v1beta1.TrafficSpec{Algorithm: &alg}
		})

		// First reconcile: BTP emitted, no gateway ack yet → Pending.
		status, err := r.Reconcile(suiteCtx, isvc, []string{"isvc-f2"})
		Expect(err).NotTo(HaveOccurred())
		Expect(status.Conditions[0].Status).To(Equal(metav1.ConditionUnknown))
		Expect(status.Conditions[0].Reason).To(Equal(v1beta1.TrafficReasonPending))

		// Simulate the EG controller stamping Accepted=True on the BTP.
		got := newBTP()
		Expect(k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: ns.Name, Name: "isvc-f2"}, got)).To(Succeed())
		// Conditions live under status.ancestors[0].conditions in real EG BTP
		// shape; but the translator's ObserveAcceptance reads top-level
		// status.conditions for backward-compat. Set both to be safe.
		acceptedCond := map[string]interface{}{
			"type":               "Accepted",
			"status":             "True",
			"reason":             "Accepted",
			"message":            "Policy accepted by gateway",
			"lastTransitionTime": metav1.Now().Format("2006-01-02T15:04:05Z"),
		}
		Expect(unstructured.SetNestedSlice(got.Object, []interface{}{acceptedCond}, "status", "conditions")).To(Succeed())
		Expect(k8sClient.Status().Update(suiteCtx, got)).To(Succeed())

		// Second reconcile: should observe acceptance.
		status2, err := r.Reconcile(suiteCtx, isvc, []string{"isvc-f2"})
		Expect(err).NotTo(HaveOccurred())
		var ready *metav1.Condition
		for i := range status2.Conditions {
			if status2.Conditions[i].Type == v1beta1.TrafficConditionBackendPolicyReady {
				ready = &status2.Conditions[i]
			}
		}
		Expect(ready).NotTo(BeNil())
		Expect(ready.Status).To(Equal(metav1.ConditionTrue), "BackendPolicyReady must flip to True after gateway Accepted=True")
		Expect(ready.Reason).To(Equal(v1beta1.TrafficReasonAcceptedByGateway))
	})

	ginkgo.It("F3: pre-existing same-name BTP with NO owner-ref → ConflictingPolicy, hand-authored preserved", func() {
		alg := v1beta1.LoadBalancingTypeRoundRobin
		isvc := isvcWith("isvc-f3", func(i *v1beta1.InferenceService) {
			i.Spec.Traffic = &v1beta1.TrafficSpec{Algorithm: &alg}
		})

		// Pre-existing hand-authored BTP with the same name OME would
		// use, no owner reference.
		handAuthored := newBTP()
		handAuthored.SetName("isvc-f3")
		handAuthored.SetNamespace(ns.Name)
		handAuthored.SetLabels(map[string]string{"hand-authored": "true"})
		// Distinctive spec so we can verify it isn't overwritten.
		Expect(unstructured.SetNestedField(handAuthored.Object, "LeastRequest", "spec", "loadBalancer", "type")).To(Succeed())
		Expect(k8sClient.Create(suiteCtx, handAuthored)).To(Succeed())

		status, err := r.Reconcile(suiteCtx, isvc, []string{"isvc-f3"})
		Expect(err).To(HaveOccurred(), "Reconcile must surface the conflict error")
		Expect(err.Error()).To(ContainSubstring("backend policy with the OME-managed name exists"))
		Expect(status).NotTo(BeNil())
		Expect(status.Conditions).NotTo(BeEmpty())
		var ready *metav1.Condition
		for i := range status.Conditions {
			if status.Conditions[i].Type == v1beta1.TrafficConditionBackendPolicyReady {
				ready = &status.Conditions[i]
			}
		}
		Expect(ready).NotTo(BeNil())
		Expect(ready.Status).To(Equal(metav1.ConditionFalse))
		Expect(ready.Reason).To(Equal(v1beta1.TrafficReasonConflictingPolicy))

		// Hand-authored BTP must NOT have been overwritten.
		preserved := newBTP()
		Expect(k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: ns.Name, Name: "isvc-f3"}, preserved)).To(Succeed())
		Expect(preserved.GetLabels()).To(HaveKeyWithValue("hand-authored", "true"))
		typ, _, _ := unstructured.NestedString(preserved.Object, "spec", "loadBalancer", "type")
		Expect(typ).To(Equal("LeastRequest"), "hand-authored BTP spec must be untouched")
	})

	ginkgo.It("F4: pre-existing BTP with controller-ref to a DIFFERENT ISVC → ConflictingPolicy, original preserved", func() {
		alg := v1beta1.LoadBalancingTypeRoundRobin
		// "Other" ISVC owns the pre-existing BTP.
		other := isvcWith("isvc-f4-other", nil)
		// Subject ISVC of this test.
		isvc := isvcWith("isvc-f4", func(i *v1beta1.InferenceService) {
			i.Spec.Traffic = &v1beta1.TrafficSpec{Algorithm: &alg}
		})

		// Pre-existing BTP with the OME-managed name "isvc-f4" but
		// owned by "isvc-f4-other".
		preexisting := newBTP()
		preexisting.SetName("isvc-f4")
		preexisting.SetNamespace(ns.Name)
		preexisting.SetOwnerReferences([]metav1.OwnerReference{{
			APIVersion: "ome.io/v1beta1",
			Kind:       "InferenceService",
			Name:       other.Name,
			UID:        other.UID,
			Controller: ptrToBool(true),
		}})
		Expect(unstructured.SetNestedField(preexisting.Object, "LeastRequest", "spec", "loadBalancer", "type")).To(Succeed())
		Expect(k8sClient.Create(suiteCtx, preexisting)).To(Succeed())

		_, err := r.Reconcile(suiteCtx, isvc, []string{"isvc-f4"})
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("backend policy with the OME-managed name exists"))

		// Pre-existing BTP still owned by the other ISVC.
		preserved := newBTP()
		Expect(k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: ns.Name, Name: "isvc-f4"}, preserved)).To(Succeed())
		Expect(preserved.GetOwnerReferences()).To(HaveLen(1))
		Expect(preserved.GetOwnerReferences()[0].Name).To(Equal(other.Name))
	})

	ginkgo.It("F5: delete the conflicting hand-authored BTP → next reconcile takes ownership, status flips to Pending", func() {
		alg := v1beta1.LoadBalancingTypeRoundRobin
		isvc := isvcWith("isvc-f5", func(i *v1beta1.InferenceService) {
			i.Spec.Traffic = &v1beta1.TrafficSpec{Algorithm: &alg}
		})

		// Pre-existing hand-authored BTP triggers conflict.
		handAuthored := newBTP()
		handAuthored.SetName("isvc-f5")
		handAuthored.SetNamespace(ns.Name)
		Expect(unstructured.SetNestedField(handAuthored.Object, "Random", "spec", "loadBalancer", "type")).To(Succeed())
		Expect(k8sClient.Create(suiteCtx, handAuthored)).To(Succeed())

		_, err := r.Reconcile(suiteCtx, isvc, []string{"isvc-f5"})
		Expect(err).To(HaveOccurred())

		// Operator deletes the conflicting BTP.
		Expect(k8sClient.Delete(suiteCtx, handAuthored)).To(Succeed())
		// Eventually the apiserver settles the delete.
		Eventually(func() bool {
			tmp := newBTP()
			err := k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: ns.Name, Name: "isvc-f5"}, tmp)
			return apierrors.IsNotFound(err)
		}).WithTimeout(util.ShortTimeout).WithPolling(util.PollInterval).Should(BeTrue())

		// Next reconcile takes ownership: creates a fresh OME-owned BTP.
		status, err := r.Reconcile(suiteCtx, isvc, []string{"isvc-f5"})
		Expect(err).NotTo(HaveOccurred())
		Expect(status.Conditions[0].Reason).To(Equal(v1beta1.TrafficReasonPending),
			"after the conflict clears, status should flip back to Pending (gateway hasn't ack'd yet)")

		// New BTP has OME's controller-ref.
		now := newBTP()
		Expect(k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: ns.Name, Name: "isvc-f5"}, now)).To(Succeed())
		Expect(now.GetOwnerReferences()).To(HaveLen(1))
		Expect(now.GetOwnerReferences()[0].Name).To(Equal("isvc-f5"))
		Expect(now.GetOwnerReferences()[0].UID).To(Equal(isvc.UID))
		typ, _, _ := unstructured.NestedString(now.Object, "spec", "loadBalancer", "type")
		Expect(typ).To(Equal("RoundRobin"), "new BTP carries OME's translated spec, not the hand-authored one")
	})

	ginkgo.It("F6: generation bump → ObservedGeneration in condition matches the new generation", func() {
		alg := v1beta1.LoadBalancingTypeRoundRobin
		isvc := isvcWith("isvc-f6", func(i *v1beta1.InferenceService) {
			i.Spec.Traffic = &v1beta1.TrafficSpec{Algorithm: &alg}
		})

		// Reconcile against the initial generation.
		status, err := r.Reconcile(suiteCtx, isvc, []string{"isvc-f6"})
		Expect(err).NotTo(HaveOccurred())
		gen1 := isvc.Generation
		Expect(status.Conditions[0].ObservedGeneration).To(Equal(gen1),
			"first reconcile must stamp the initial generation")

		// Bump spec to force the apiserver to advance generation.
		Eventually(func(g Gomega) {
			fresh := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(isvc), fresh)).To(Succeed())
			newAlg := v1beta1.LoadBalancingTypeLeastRequest
			fresh.Spec.Traffic = &v1beta1.TrafficSpec{Algorithm: &newAlg}
			g.Expect(k8sClient.Update(suiteCtx, fresh)).To(Succeed())
			isvc.ResourceVersion = fresh.ResourceVersion
			isvc.Generation = fresh.Generation
			isvc.Spec = fresh.Spec
		}).WithTimeout(util.ShortTimeout).WithPolling(util.PollInterval).Should(Succeed())

		Expect(isvc.Generation).To(BeNumerically(">", gen1), "spec change must bump generation")

		// Reconcile again; ObservedGeneration must advance.
		status2, err := r.Reconcile(suiteCtx, isvc, []string{"isvc-f6"})
		Expect(err).NotTo(HaveOccurred())
		Expect(status2.Conditions[0].ObservedGeneration).To(Equal(isvc.Generation),
			"second reconcile's condition must record the new generation")
	})

	ginkgo.It("H2: ConsistentHash + canary + ome.io/btp.* pass-through → all three layers compose", func() {
		alg := v1beta1.LoadBalancingTypeConsistentHash
		isvc := isvcWith("isvc-h2", func(i *v1beta1.InferenceService) {
			i.Spec.Engine = &v1beta1.EngineSpec{}
			i.Spec.Traffic = &v1beta1.TrafficSpec{
				Algorithm: &alg,
				ConsistentHash: &v1beta1.ConsistentHashSpec{
					Type:    v1beta1.HashTypeHeader,
					Headers: []v1beta1.HashHeader{{Name: "X-Session-ID"}},
				},
			}
			i.Annotations = map[string]string{
				"ome.io/btp.loadBalancer.slowStart.window": "30s",
			}
		})
		isvc.Status.SetCondition(v1beta1.EngineReady, &knapis.Condition{Type: v1beta1.EngineReady, Status: corev1.ConditionTrue})

		status, err := r.Reconcile(suiteCtx, isvc, []string{"isvc-h2", "isvc-h2-engine"})
		Expect(err).NotTo(HaveOccurred())
		btp := newBTP()
		Expect(k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: ns.Name, Name: "isvc-h2"}, btp)).To(Succeed())

		// All three layers present in the BTP:
		// (1) typed algorithm + consistentHash
		alg2, _, _ := unstructured.NestedString(btp.Object, "spec", "loadBalancer", "type")
		Expect(alg2).To(Equal("ConsistentHash"))
		hdr, _, _ := unstructured.NestedString(btp.Object, "spec", "loadBalancer", "consistentHash", "header", "name")
		Expect(hdr).To(Equal("X-Session-ID"))
		// (2) pass-through stitched verbatim
		window, _, _ := unstructured.NestedString(btp.Object, "spec", "loadBalancer", "slowStart", "window")
		Expect(window).To(Equal("30s"))
		// (3) condition message lists the pass-through
		var ready *metav1.Condition
		for i := range status.Conditions {
			if status.Conditions[i].Type == v1beta1.TrafficConditionBackendPolicyReady {
				ready = &status.Conditions[i]
			}
		}
		Expect(ready).NotTo(BeNil())
		Expect(ready.Message).To(ContainSubstring("loadBalancer.slowStart.window"))
		// Layer (4) weighted HTTPRoute backendRefs was originally written
		// against ComponentStatusSpec.Traffic []knservingv1.TrafficTarget;
		// removed during Serverless cleanup and to be re-introduced once
		// a non-Knative per-revision producer exists.
	})

	ginkgo.It("I1: empty spec.traffic struct (all fields nil) → HasIntent=false, no BTP emitted", func() {
		// Operator declared the struct but didn't fill anything in.
		// HasIntent must treat this as "no intent" (rather than
		// emitting a BTP with default LB).
		isvc := isvcWith("isvc-i1", func(i *v1beta1.InferenceService) {
			i.Spec.Traffic = &v1beta1.TrafficSpec{} // all fields nil
		})

		status, err := r.Reconcile(suiteCtx, isvc, []string{"isvc-i1"})
		Expect(err).NotTo(HaveOccurred())
		Expect(status).To(BeNil(), "empty TrafficSpec must not emit a BTP or surface a status")

		miss := newBTP()
		Expect(apierrors.IsNotFound(k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: ns.Name, Name: "isvc-i1"}, miss))).To(BeTrue())
	})

	ginkgo.It("I2: two ISVCs in the same namespace with traffic intent → two distinct BTPs, no cross-talk", func() {
		alg := v1beta1.LoadBalancingTypeRoundRobin
		a := isvcWith("isvc-i2-a", func(i *v1beta1.InferenceService) {
			i.Spec.Traffic = &v1beta1.TrafficSpec{Algorithm: &alg}
		})
		b := isvcWith("isvc-i2-b", func(i *v1beta1.InferenceService) {
			alg2 := v1beta1.LoadBalancingTypeLeastRequest
			i.Spec.Traffic = &v1beta1.TrafficSpec{Algorithm: &alg2}
		})

		_, errA := r.Reconcile(suiteCtx, a, []string{"isvc-i2-a"})
		_, errB := r.Reconcile(suiteCtx, b, []string{"isvc-i2-b"})
		Expect(errA).NotTo(HaveOccurred())
		Expect(errB).NotTo(HaveOccurred())

		gotA := newBTP()
		Expect(k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: ns.Name, Name: "isvc-i2-a"}, gotA)).To(Succeed())
		typA, _, _ := unstructured.NestedString(gotA.Object, "spec", "loadBalancer", "type")
		Expect(typA).To(Equal("RoundRobin"))
		// Owned by A.
		Expect(gotA.GetOwnerReferences()).To(HaveLen(1))
		Expect(gotA.GetOwnerReferences()[0].UID).To(Equal(a.UID))

		gotB := newBTP()
		Expect(k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: ns.Name, Name: "isvc-i2-b"}, gotB)).To(Succeed())
		typB, _, _ := unstructured.NestedString(gotB.Object, "spec", "loadBalancer", "type")
		Expect(typB).To(Equal("LeastRequest"))
		Expect(gotB.GetOwnerReferences()[0].UID).To(Equal(b.UID))

		// Distinct objects.
		Expect(gotA.GetUID()).NotTo(Equal(gotB.GetUID()))
	})

	ginkgo.It("I3: two ISVCs with the SAME name in different namespaces → namespace-scoped BTPs, no collision", func() {
		// Create a second namespace alongside the per-test one and
		// place an identically-named ISVC + BTP there.
		ns2, err := util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "traffic-reco-")
		Expect(err).NotTo(HaveOccurred())
		defer func() { _ = util.DeleteNamespace(suiteCtx, k8sClient, ns2.Name) }()

		alg := v1beta1.LoadBalancingTypeRoundRobin

		// ISVC #1 in `ns`.
		a := isvcWith("isvc-i3", func(i *v1beta1.InferenceService) {
			i.Spec.Traffic = &v1beta1.TrafficSpec{Algorithm: &alg}
		})

		// ISVC #2 in `ns2` (same name, different namespace).
		w := v1beta1testing.MakeInferenceService("isvc-i3", ns2.Name)
		b := w.Obj()
		Expect(k8sClient.Create(suiteCtx, b)).To(Succeed())
		Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(b), b)).To(Succeed())
		alg2 := v1beta1.LoadBalancingTypeLeastRequest
		b.Spec.Traffic = &v1beta1.TrafficSpec{Algorithm: &alg2}

		_, errA := r.Reconcile(suiteCtx, a, []string{"isvc-i3"})
		_, errB := r.Reconcile(suiteCtx, b, []string{"isvc-i3"})
		Expect(errA).NotTo(HaveOccurred())
		Expect(errB).NotTo(HaveOccurred())

		// Each BTP scoped to its own namespace.
		gotA := newBTP()
		Expect(k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: ns.Name, Name: "isvc-i3"}, gotA)).To(Succeed())
		typA, _, _ := unstructured.NestedString(gotA.Object, "spec", "loadBalancer", "type")
		Expect(typA).To(Equal("RoundRobin"))
		Expect(gotA.GetOwnerReferences()[0].UID).To(Equal(a.UID))

		gotB := newBTP()
		Expect(k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: ns2.Name, Name: "isvc-i3"}, gotB)).To(Succeed())
		typB, _, _ := unstructured.NestedString(gotB.Object, "spec", "loadBalancer", "type")
		Expect(typB).To(Equal("LeastRequest"))
		Expect(gotB.GetOwnerReferences()[0].UID).To(Equal(b.UID))

		// Two distinct objects.
		Expect(gotA.GetUID()).NotTo(Equal(gotB.GetUID()))
	})

	ginkgo.It("I4: bypass-admission case — ConsistentHash without consistentHash block → TranslationFailed in status", func() {
		// Admission would reject this (MissingConsistentHashSpec) but
		// we bypass admission by constructing the ISVC spec directly
		// in the test fixture. The translator must reject — silently
		// emitting a malformed BTP would be worse than failing fast.
		alg := v1beta1.LoadBalancingTypeConsistentHash
		isvc := isvcWith("isvc-i4", func(i *v1beta1.InferenceService) {
			// Algorithm=ConsistentHash but no consistentHash block.
			i.Spec.Traffic = &v1beta1.TrafficSpec{Algorithm: &alg}
		})

		status, err := r.Reconcile(suiteCtx, isvc, []string{"isvc-i4"})
		Expect(err).To(HaveOccurred(), "Reconcile must surface the translator error")
		Expect(status).NotTo(BeNil())

		var ready *metav1.Condition
		for i := range status.Conditions {
			if status.Conditions[i].Type == v1beta1.TrafficConditionBackendPolicyReady {
				ready = &status.Conditions[i]
			}
		}
		Expect(ready).NotTo(BeNil())
		Expect(ready.Status).To(Equal(metav1.ConditionFalse))
		Expect(ready.Reason).To(Equal(v1beta1.TrafficReasonTranslationFailed))

		// No BTP should be created when translation fails.
		miss := newBTP()
		Expect(apierrors.IsNotFound(k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: ns.Name, Name: "isvc-i4"}, miss))).To(BeTrue())
	})
})

// ptrToBool is a tiny helper for owner-ref construction.
func ptrToBool(v bool) *bool { return &v }
