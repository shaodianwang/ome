package omenative_coordination_kind

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/podreadiness"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	"github.com/ome-projects/ome/tests/integration/util"
)

// Replica rebound while a committed victim is still DRAINING — before
// any pod delete has been issued. The routed-endpoint gate defers the
// pod delete while any EndpointSlice of the revision Service still
// publishes a Ready endpoint for the victim, and a desired-replica
// rebound during that window must not cancel the committed deletion:
// the victim stays delete-owned and out of service, no concurrent
// replacement is created at its index, and once the endpoint clears the
// controller finishes the delete and only then recreates the capacity.
//
// A foreign EndpointSlice carrying a Ready endpoint for the victim
// holds the drain gate open; the real slice goes NotReady as soon as
// the serving hold lands, so the synthetic one is the only thing
// keeping the pod "routable".
//
// Invariants asserted:
//   - the victim receives the serving hold but no delete while routed;
//   - a rebound mid-drain neither cancels the Delete ownership nor
//     returns the victim to service nor creates a same-index twin;
//   - clearing the endpoint completes the delete, and the rebound is
//     satisfied by a fresh replacement pod.
var _ = ginkgo.Describe("OMENative scale transitions — rebound while draining (KIND)", ginkgo.Ordered, func() {
	var ns *corev1.Namespace

	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "scale-tr-drain-")
		Expect(err).NotTo(HaveOccurred())
	})

	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("finishes a drain-held deletion after a rebound, then recreates the capacity", func() {
		const isvcName = "scale-tr-drain"
		fix := newCoordFixture(ns.Name, WithISVCName(isvcName),
			WithoutDecoder(),
			WithEngineReplicas(2),
			WithEngineImage("fake-serving:v1"))
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		ginkgo.By("waiting for two Ready singleton Instances")
		Eventually(func(g Gomega) {
			statuses := instanceStatuses(g, ns.Name, isvcName, v1beta1.EngineComponent)
			g.Expect(statuses).To(HaveLen(2))
			for _, status := range statuses {
				g.Expect(status.Phase).To(Equal(v1beta1.OMENativeInstanceReady))
			}
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		pod0 := aliveEnginePodForInstance(Default, ns.Name, isvcName, 0)
		victim := aliveEnginePodForInstance(Default, ns.Name, isvcName, 1)
		victimUID := victim.UID
		revisionHash := victim.Labels[query.LabelRevisionHash]
		Expect(revisionHash).NotTo(BeEmpty(), "engine pods must carry the revision-hash label")
		Expect(victim.Status.PodIP).NotTo(BeEmpty(), "a Ready pod must have an IP")
		routedService := query.PerRevisionServiceName(isvcName, workload.ComponentEngine, revisionHash)

		ginkgo.By("publishing a synthetic Ready endpoint that keeps the victim routable")
		ready := true
		hold := &discoveryv1.EndpointSlice{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "scale-tr-drain-hold",
				Namespace: ns.Name,
				Labels: map[string]string{
					discoveryv1.LabelServiceName: routedService,
					discoveryv1.LabelManagedBy:   "scale-tr-test.example.com",
				},
			},
			AddressType: discoveryv1.AddressTypeIPv4,
			Endpoints: []discoveryv1.Endpoint{{
				Addresses:  []string{victim.Status.PodIP},
				Conditions: discoveryv1.EndpointConditions{Ready: &ready},
				TargetRef: &corev1.ObjectReference{
					Kind:      "Pod",
					Namespace: ns.Name,
					Name:      victim.Name,
					UID:       victim.UID,
				},
			}},
		}
		Expect(k8sClient.Create(suiteCtx, hold)).To(Succeed())
		ginkgo.DeferCleanup(func() {
			_ = k8sClient.Delete(suiteCtx, hold)
		})

		ginkgo.By("reducing desired replicas from two to one")
		Eventually(func(g Gomega) {
			current := freshISVC(g, ns.Name, isvcName)
			one := 1
			current.Spec.Engine.MinReplicas = &one
			g.Expect(k8sClient.Update(suiteCtx, current)).To(Succeed())
		}).WithTimeout(kindShortTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("observing the serving hold land while the routed endpoint defers the delete")
		Eventually(func(g Gomega) {
			g.Expect(deleteOwnedInstanceIndices(instanceStatuses(g, ns.Name, isvcName, v1beta1.EngineComponent))).
				To(Equal([]int32{1}))
			fresh := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(victim), fresh)).To(Succeed())
			g.Expect(fresh.UID).To(Equal(victimUID))
			g.Expect(podreadiness.IsServing(fresh)).To(BeFalse(), "the serving hold must land before any delete")
			g.Expect(fresh.DeletionTimestamp).To(BeNil(), "no pod delete may be issued while an endpoint still routes to it")
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("restoring desired replicas while the victim is still draining")
		Eventually(func(g Gomega) {
			current := freshISVC(g, ns.Name, isvcName)
			two := 2
			current.Spec.Engine.MinReplicas = &two
			g.Expect(k8sClient.Update(suiteCtx, current)).To(Succeed())
		}).WithTimeout(kindShortTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("asserting the rebound cancels nothing while the drain gate holds")
		Consistently(func(g Gomega) {
			g.Expect(deleteOwnedInstanceIndices(instanceStatuses(g, ns.Name, isvcName, v1beta1.EngineComponent))).
				To(Equal([]int32{1}), "a rebound must not cancel committed Delete ownership")

			pods := boundedScaleDownPodsForInstance(g, ns.Name, isvcName, 1)
			g.Expect(pods).To(HaveLen(1), "no replacement may be created at a delete-owned index")
			g.Expect(pods[0].UID).To(Equal(victimUID))
			g.Expect(pods[0].DeletionTimestamp).To(BeNil(), "the routed endpoint must keep deferring the delete")
			g.Expect(podreadiness.IsServing(&pods[0])).To(BeFalse(), "a rebound must not return the victim to service")

			survivor := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(pod0), survivor)).To(Succeed())
			g.Expect(survivor.UID).To(Equal(pod0.UID))
			g.Expect(survivor.DeletionTimestamp).To(BeNil())

			g.Expect(listOMENativePods(g, ns.Name, isvcName)).To(HaveLen(2),
				"Create must not advance at any index while delete work is incomplete")
		}).WithTimeout(kindShortTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("clearing the synthetic endpoint and expecting finish-then-recreate")
		Expect(k8sClient.Delete(suiteCtx, hold)).To(Succeed())
		Eventually(func(g Gomega) {
			statuses := instanceStatuses(g, ns.Name, isvcName, v1beta1.EngineComponent)
			g.Expect(statuses).To(HaveLen(2))
			for _, status := range statuses {
				g.Expect(status.Phase).To(Equal(v1beta1.OMENativeInstanceReady))
			}
			replacement := aliveEnginePodForInstance(g, ns.Name, isvcName, 1)
			g.Expect(replacement.UID).NotTo(Equal(victimUID), "the committed victim must finish before the index is recreated")
			survivor := aliveEnginePodForInstance(g, ns.Name, isvcName, 0)
			g.Expect(survivor.UID).To(Equal(pod0.UID), "the retained instance must survive the whole cycle")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})
})
