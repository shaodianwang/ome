package omenative_coordination_kind

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/v1beta1convert"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	"github.com/ome-projects/ome/tests/integration/util"
)

// A multi-pod stable Service routes to the leader while its headless peer
// Service keeps every gang member discoverable.
var _ = ginkgo.Describe("Multi-pod stable Service routing", ginkgo.Ordered, func() {
	var ns *corev1.Namespace
	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "leader-routing-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("routes only to the leader while headless discovery includes workers", func() {
		const isvcName = "leader-routing"

		// 3 engine pods (1 leader + 2 workers). The KIND suite has 2
		// worker nodes (plus the control-plane node which carries
		// the standard NoSchedule taint); the engine gang of 3 lands
		// on the two worker nodes per default soft affinity. Decoder
		// is left at single-pod default for spec brevity.
		fix := newCoordFixture(ns.Name,
			WithISVCName(isvcName),
			WithEngineImage("fake-serving:v1"),
			WithEngineWorkerSize(2),
			WithoutDecoder())
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		// Wait until all 3 engine pods are observed by the controller.
		// Steady state is the point where the EndpointSlice mirror has
		// caught up — without this wait the slice could legitimately
		// be empty or partial on a fast machine.
		Eventually(func(g Gomega) {
			pods := listOMENativePods(g, ns.Name, isvcName)
			engPods := 0
			for _, p := range pods {
				if p.Labels[constants.OMEComponentLabel] == string(v1beta1.EngineComponent) {
					engPods++
				}
			}
			g.Expect(engPods).To(Equal(3),
				"expected 3 engine pods (1 leader + 2 workers); got %d", engPods)
			c := freshISVC(g, ns.Name, isvcName)
			engCS := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(engCS.Lifecycle).NotTo(BeNil())
			// All 3 pods reach Ready (= 1 Instance with 3 pods of
			// 3 ready). OMENative reports ServingReplicas as the
			// count of fully-ready INSTANCES, not pods.
			g.Expect(engCS.Lifecycle.ServingReplicas).To(Equal(int32(1)),
				"expected 1 multi-pod engine Instance ready; got %d", engCS.Lifecycle.ServingReplicas)
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Per-Component STABLE Service: <isvc>-<comp>.
		stableSvcName := query.StableServiceName(isvcName,
			v1beta1convert.ComponentTypeToWorkload(v1beta1.EngineComponent))
		stable := &corev1.Service{}
		Expect(k8sClient.Get(suiteCtx,
			client.ObjectKey{Namespace: ns.Name, Name: stableSvcName},
			stable)).To(Succeed(), "stable Service %s should exist", stableSvcName)
		// The stable Service selects the leader pod for this raw
		// multi-pod shape.
		Expect(stable.Spec.Selector).To(HaveKeyWithValue(query.LabelPodOrdinal, "0"),
			"multi-pod stable Service %s must carry %s=0 selector; got %v",
			stableSvcName, query.LabelPodOrdinal, stable.Spec.Selector)

		// Per-Component HEADLESS Service: <isvc>-<comp>-headless.
		headlessSvcName := query.HeadlessServiceName(isvcName,
			v1beta1convert.ComponentTypeToWorkload(v1beta1.EngineComponent))
		headless := &corev1.Service{}
		Expect(k8sClient.Get(suiteCtx,
			client.ObjectKey{Namespace: ns.Name, Name: headlessSvcName},
			headless)).To(Succeed(), "headless Service %s should exist", headlessSvcName)
		// The headless Service remains broad so workers are available for
		// peer discovery.
		Expect(headless.Spec.Selector).NotTo(HaveKey(query.LabelPodOrdinal),
			"headless Service %s selector must NOT carry %s; got %v",
			headlessSvcName, query.LabelPodOrdinal, headless.Spec.Selector)

		// EndpointSlice for the stable Service has exactly 1 endpoint —
		// the leader pod (ordinal=0). kube-controller-manager builds
		// EndpointSlices from the Service selector + matching pod IPs.
		Eventually(func(g Gomega) {
			slices := &discoveryv1.EndpointSliceList{}
			g.Expect(k8sClient.List(suiteCtx, slices,
				client.InNamespace(ns.Name),
				client.MatchingLabels{discoveryv1.LabelServiceName: stableSvcName},
			)).To(Succeed())
			g.Expect(slices.Items).NotTo(BeEmpty(),
				"EndpointSlice for stable Service %s should be populated", stableSvcName)
			total := 0
			for _, sl := range slices.Items {
				total += len(sl.Endpoints)
			}
			g.Expect(total).To(Equal(1),
				"stable Service %s EndpointSlice must publish exactly 1 endpoint (rank-0 leader only); got %d",
				stableSvcName, total)
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// EndpointSlice for the headless Service has all 3 endpoints —
		// the leader and both workers must be addressable for peer DNS.
		Eventually(func(g Gomega) {
			slices := &discoveryv1.EndpointSliceList{}
			g.Expect(k8sClient.List(suiteCtx, slices,
				client.InNamespace(ns.Name),
				client.MatchingLabels{discoveryv1.LabelServiceName: headlessSvcName},
			)).To(Succeed())
			g.Expect(slices.Items).NotTo(BeEmpty(),
				"EndpointSlice for headless Service %s should be populated", headlessSvcName)
			total := 0
			for _, sl := range slices.Items {
				total += len(sl.Endpoints)
			}
			g.Expect(total).To(Equal(3),
				"headless Service %s EndpointSlice must publish all 3 gang members (1 leader + 2 workers); got %d",
				headlessSvcName, total)
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})
})
