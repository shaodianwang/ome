package omenative_coordination_kind

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/podreadiness"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	"github.com/ome-projects/ome/tests/integration/util"
)

// A gang rebuilt into the room it vacated on one node binds.
//
// The pool is packed: every worker but one is cordoned, so the engine's
// single two-pod Instance (a leader and one worker) runs with both members
// on the one feasible node, and the only room for a rebuild is what its own
// pods vacate. The operator pushes a revision under RecreatePod; the gang is
// torn down and rebuilt as a unit, and both new pods bind to the same node
// and the Instance serves the new revision. Room on a node is counted in
// members, so a node with room for two is room for the gang, not for one
// member; the scheduler twins must agree.
//
// Cells: instance/Update/Recreate/gang/packed-pool
var _ = ginkgo.Describe("behavior — a gang rebuilt into the room it vacated on one node binds (KIND)", ginkgo.Ordered, func() {
	var ns *corev1.Namespace
	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "packed-gang-room-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("the rebuilt gang lands both members on the one node with room and serves", func() {
		const (
			isvcName = "packed-gang-room"
			index    = "0"
			newImage = "fake-serving:v2"
		)

		workers := schedulableWorkerNodes(Default)
		Expect(len(workers)).To(BeNumerically(">=", 2),
			"a packed pool needs a second worker to cordon; got %v", workers)
		onlyNode, spareNodes := workers[0], workers[1:]

		// Every other worker is cordoned for the whole story, so the gang's
		// only room, first and after the rebuild, is on one node. cordonNode's
		// cleanup uncordons on any exit.
		for _, node := range spareNodes {
			cordonNode(node)
		}

		fix := newCoordFixture(ns.Name, WithISVCName(isvcName),
			WithoutDecoder(),
			WithEngineWorkerSize(1),
			WithEngineUpdateStrategy(v1beta1.UpdateStrategyRecreatePod),
			WithEngineImage("fake-serving:v1"))
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		ginkgo.By("waiting for the gang to serve with both members on the only feasible node")
		var firstTarget string
		firstPods := map[types.UID]bool{}
		Eventually(func(g Gomega) {
			insts := instanceStatuses(g, ns.Name, isvcName, v1beta1.EngineComponent)
			g.Expect(insts).To(HaveLen(1))
			g.Expect(insts[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
			pods := enginePodsOfInstance(g, ns.Name, isvcName, index)
			g.Expect(pods).To(HaveLen(2), "a leader and one worker")
			for _, pod := range pods {
				g.Expect(pod.Spec.NodeName).To(Equal(onlyNode), "every spare is cordoned; pod %s", pod.Name)
				g.Expect(podreadiness.IsContainersReady(&pod)).To(BeTrue(), "pod %s", pod.Name)
				firstPods[pod.UID] = true
			}
			firstTarget = engineTargetRevision(g, ns.Name, isvcName)
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("pushing a revision under RecreatePod, which tears the gang down and rebuilds it")
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, isvcName)
			cur.Spec.Engine.Leader.PodSpec.Containers[0].Image = newImage
			cur.Spec.Engine.Worker.PodSpec.Containers[0].Image = newImage
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())
		awaitEngineTargetMoved(ns.Name, isvcName, firstTarget)

		ginkgo.By("waiting for the rebuilt gang to bind both members into the room it vacated and serve")
		Eventually(func(g Gomega) {
			target := engineTargetRevision(g, ns.Name, isvcName)
			g.Expect(target).NotTo(Equal(firstTarget))
			insts := instanceStatuses(g, ns.Name, isvcName, v1beta1.EngineComponent)
			g.Expect(insts).To(HaveLen(1))
			g.Expect(insts[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
			pods := enginePodsOfInstance(g, ns.Name, isvcName, index)
			g.Expect(pods).To(HaveLen(2), "a leader and one worker")
			for _, pod := range pods {
				g.Expect(firstPods).NotTo(HaveKey(pod.UID), "RecreatePod rebuilds the gang; pod %s must be new", pod.Name)
				g.Expect(pod.Labels[query.LabelRevisionHash]).To(Equal(query.RevisionHashFromControllerRevisionName(target)),
					"pod %s must run the pushed revision", pod.Name)
				g.Expect(pod.Spec.NodeName).To(Equal(onlyNode),
					"the only room is what the gang vacated on %s; pod %s", onlyNode, pod.Name)
				g.Expect(podreadiness.IsContainersReady(&pod)).To(BeTrue(), "pod %s", pod.Name)
			}
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})
})

// enginePodsOfInstance returns the live engine pods of an Instance index.
func enginePodsOfInstance(g Gomega, namespace, isvcName, index string) []corev1.Pod {
	pods := &corev1.PodList{}
	g.Expect(k8sClient.List(suiteCtx, pods,
		client.InNamespace(namespace),
		client.MatchingLabels{
			util.LabelManagedByKey:                util.ManagedByOMENativeValue,
			constants.InferenceServicePodLabelKey: isvcName,
			constants.OMEComponentLabel:           string(v1beta1.EngineComponent),
			query.LabelInstanceIdx:                index,
		})).To(Succeed())
	live := make([]corev1.Pod, 0, len(pods.Items))
	for _, pod := range pods.Items {
		if pod.DeletionTimestamp == nil {
			live = append(live, pod)
		}
	}
	return live
}
