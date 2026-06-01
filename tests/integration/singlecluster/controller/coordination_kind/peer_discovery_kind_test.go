package coordination_kind

import (
	"fmt"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/v1beta1convert"
	"github.com/ome-projects/ome/tests/integration/util"
)

// PEER-DNS — per-pod DNS subdomain pin.
//
// Asserts the controller plus kube-controller-manager produce the
// data plane state that lets `<pod>.<isvc>-<comp>-headless.<ns>.svc.cluster.local`
// resolve in a real cluster. The pure-function naming math is covered
// by pure-Go unit tests in pkg/.../coordination/discovery_test.go.
//
// This spec pins:
//
//	a. headless Service `<isvc>-<comp>-headless` exists with
//	   ClusterIP=None and PublishNotReadyAddresses=true.
//	b. every OMENative pod has Spec.Subdomain set to that headless
//	   Service name (production code in ops/render.go).
//	c. the EndpointSlice for that headless Service carries each pod's
//	   hostname — which is what kube-DNS reads when answering the
//	   per-pod A record query.
//
// Asserting on the EndpointSlice is a stronger pin than spawning a
// debug pod + nslookup: it removes the network-from-pod variability
// while still proving the data plane has everything DNS needs.
var _ = ginkgo.Describe("PEER-DNS — per-pod headless DNS plumbing", ginkgo.Ordered, func() {
	var ns *corev1.Namespace
	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "peer-dns-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("PEER-DNS headless Service + pod subdomain + EndpointSlice hostname all line up", func() {
		// 2 engine replicas so the EndpointSlice has > 1 endpoint —
		// proves the per-pod hostname is per-replica, not a constant
		// the controller stamps once.
		fix := newCoordFixture(ns.Name, WithISVCName("peer-dns"),
			WithEngineReplicas(2),
			WithEngineImage("fake-serving:v1"))
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		// Wait for engine + decoder to converge so both have pods +
		// EndpointSlices in steady state.
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "peer-dns")
			engCS := c.Status.Components[v1beta1.EngineComponent]
			decCS := c.Status.Components[v1beta1.DecoderComponent]
			g.Expect(engCS.Lifecycle).NotTo(BeNil())
			g.Expect(decCS.Lifecycle).NotTo(BeNil())
			g.Expect(engCS.Lifecycle.ServingReplicas).To(Equal(int32(2)))
			g.Expect(decCS.Lifecycle.ServingReplicas).To(Equal(int32(1)))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// (a) headless Service exists per Component with ClusterIP=None
		// + PublishNotReadyAddresses=true. Production code:
		// reconcilers/omenative/services.go.
		for _, comp := range []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent} {
			svcName := query.HeadlessServiceName("peer-dns", v1beta1convert.ComponentTypeToWorkload(comp))
			svc := &corev1.Service{}
			Expect(k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: ns.Name, Name: svcName}, svc)).
				To(Succeed(), "headless Service %s should exist", svcName)
			Expect(svc.Spec.ClusterIP).To(Equal(corev1.ClusterIPNone),
				"headless Service %s must have ClusterIP=None", svcName)
			Expect(svc.Spec.PublishNotReadyAddresses).To(BeTrue(),
				"headless Service %s must publish not-ready addresses (peer gang-init contract)", svcName)
		}

		// (b) every pod has Spec.Subdomain set to the per-Component
		// headless Service name. Production code: ops/render.go.
		pods := listOMENativePods(Default, ns.Name, "peer-dns")
		Expect(pods).To(HaveLen(3),
			"expected 2 engine + 1 decoder pod, got %d", len(pods))
		for _, p := range pods {
			comp := v1beta1.ComponentType(p.Labels["component"])
			wantSubdomain := query.HeadlessServiceName("peer-dns", v1beta1convert.ComponentTypeToWorkload(comp))
			Expect(p.Spec.Subdomain).To(Equal(wantSubdomain),
				"pod %s must have Subdomain=%s for DNS resolution; got %q",
				p.Name, wantSubdomain, p.Spec.Subdomain)
			Expect(p.Spec.Hostname).NotTo(BeEmpty(),
				"pod %s must have Hostname set so EndpointSlice publishes a per-pod DNS A record",
				p.Name)
		}

		// (c) EndpointSlice for the engine headless Service publishes
		// the per-pod hostnames. kube-controller-manager mirrors each
		// pod's `hostname` field into the EndpointSlice's
		// Endpoints[].Hostname; this is what kube-DNS reads when
		// answering `<pod>.<svc>.<ns>.svc.cluster.local` queries.
		engSvc := query.HeadlessServiceName("peer-dns", v1beta1convert.ComponentTypeToWorkload(v1beta1.EngineComponent))
		Eventually(func(g Gomega) {
			slices := &discoveryv1.EndpointSliceList{}
			g.Expect(k8sClient.List(suiteCtx, slices,
				client.InNamespace(ns.Name),
				client.MatchingLabels{discoveryv1.LabelServiceName: engSvc},
			)).To(Succeed())
			g.Expect(slices.Items).NotTo(BeEmpty(),
				"EndpointSlice for headless Service %s should be populated by kube-controller-manager", engSvc)

			// Collect per-pod hostnames out of every slice. With
			// PublishNotReadyAddresses=true the slice carries entries
			// regardless of pod readiness, so this assertion holds
			// throughout the rollout.
			gotHostnames := map[string]bool{}
			for _, sl := range slices.Items {
				for _, e := range sl.Endpoints {
					g.Expect(e.Hostname).NotTo(BeNil(),
						"EndpointSlice %s must carry per-pod hostnames for DNS resolution", sl.Name)
					gotHostnames[*e.Hostname] = true
				}
			}

			snap, err := util.SnapshotPods(suiteCtx, k8sClient, ns.Name, "peer-dns")
			g.Expect(err).NotTo(HaveOccurred())
			for _, p := range snap.ByComponent[v1beta1.EngineComponent] {
				g.Expect(gotHostnames).To(HaveKey(p.Spec.Hostname),
					"engine pod %s (hostname=%s) missing from EndpointSlice — DNS A record won't resolve; slice hostnames=%v",
					p.Name, p.Spec.Hostname, dumpSet(gotHostnames))
			}
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})
})

// dumpSet renders a hostname set for assertion-failure debug output.
func dumpSet(s map[string]bool) []string {
	out := make([]string, 0, len(s))
	for k := range s {
		out = append(out, fmt.Sprintf("%q", k))
	}
	return out
}
