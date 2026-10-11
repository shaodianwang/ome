package omenative_coordination_kind

import (
	"strings"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/podreadiness"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	"github.com/ome-projects/ome/tests/integration/util"
)

// A gang on a node whose kubelet stops is freed as one set and rebuilt
// elsewhere.
//
// One engine Instance of a leader and one worker runs under
// RecreateInstanceOnPodRestart with the force-delete policy on. Both pods
// are placed on one worker node (every other worker is cordoned while the
// gang starts), then that node's container is paused: its kubelet stops
// reporting, the node turns NotReady and after the policy's threshold the
// unreachable evidence is proven. OME must then free both pods by force in
// the same sweep, one audit row and one event each, never issue a graceful
// delete the dead kubelet cannot carry out, and rebuild the whole gang on
// the other node while the dead one is still down.

// Cells: instance/Ready/pod.nodeLost/before, instance/Restart/Drain/pod.nodeLost
var _ = ginkgo.Describe("OMENative force-delete frees a whole gang on a dead node (KIND)", ginkgo.Ordered, func() {
	var ns *corev1.Namespace

	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "omenative-fd-gang-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("force-deletes both members of the gang at once and rebuilds the gang on another node", func() {
		const isvcName = "fd-gang"
		const gangSize = 2
		configureLifecycleConfig(forceDeleteLifecycleJSON)

		workers := schedulableWorkerNodes(Default)
		Expect(len(workers)).To(BeNumerically(">=", 2),
			"the story needs a worker to lose and a worker to rebuild on; got %v", workers)
		gangNode := workers[0]

		// Every other worker is cordoned while the gang starts, so both
		// members land on gangNode; cordonNode's cleanup uncordons on any
		// exit, and the cordons are lifted by hand before the node dies so
		// the rebuild has somewhere to land.
		ginkgo.By("cordoning every worker but " + gangNode + " so the whole gang lands on it")
		var cordoned []string
		for _, w := range workers[1:] {
			cordonNode(w)
			cordoned = append(cordoned, w)
		}

		restartPolicy := v1beta1.InstanceRestartPolicyRecreateInstance
		fix := newCoordFixture(ns.Name, WithISVCName(isvcName),
			WithoutDecoder(),
			WithEngineReplicas(1),
			WithEngineWorkerSize(gangSize-1),
			WithEngineImage("fake-serving:v1"))
		fix.ISVC.Spec.Engine.Lifecycle = &v1beta1.LifecycleSpec{RestartPolicy: &restartPolicy}
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		ginkgo.By("waiting for the gang to serve with both members on " + gangNode)
		original := map[types.UID]string{}
		Eventually(func(g Gomega) {
			insts := instanceStatuses(g, ns.Name, isvcName, v1beta1.EngineComponent)
			g.Expect(insts).To(HaveLen(1))
			g.Expect(insts[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
			g.Expect(insts[0].Operation).To(BeNil())
			members := liveEnginePodsOfInstance(g, ns.Name, isvcName, 0)
			g.Expect(members).To(HaveLen(gangSize), "the gang must be whole before the node dies")
			original = map[types.UID]string{}
			for _, pod := range members {
				g.Expect(pod.Spec.NodeName).To(Equal(gangNode), "member %s must sit on the node that will die", pod.Name)
				g.Expect(podIsReady(&pod)).To(BeTrue())
				g.Expect(podreadiness.IsServing(&pod)).To(BeTrue())
				expectBoundBySuiteScheduler(g, &pod)
				original[pod.UID] = pod.Name
			}
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("uncordoning the other workers so the rebuild has a live node")
		for _, w := range cordoned {
			setNodeUnschedulable(w, false)
		}

		// From the node's death to the rebuild, no original member may ever
		// carry a deletion timestamp: a graceful delete on a dead kubelet is
		// the drain that strands the gang. The sweep removes a pod by force,
		// which takes the object away in one step.
		stop := util.StartSampler(suiteCtx, kindPollInterval, func(g Gomega) {
			for _, pod := range listOMENativePods(g, ns.Name, isvcName) {
				if _, old := original[pod.UID]; !old {
					continue
				}
				g.Expect(pod.DeletionTimestamp).To(BeNil(),
					"member %s on the dead node %s was deleted gracefully; the dead kubelet can never finish it", pod.Name, gangNode)
			}
		})
		defer stop()

		ginkgo.By("pausing node " + gangNode + " under the whole gang")
		pauseKindNode(gangNode)

		// Both members leave the API despite the paused kubelet: only the
		// escalation's grace-0 delete can do that, and it must do it for the
		// whole set once the node has been unreachable for the threshold.
		ginkgo.By("expecting both members force-deleted once the node is provably dead")
		Eventually(func(g Gomega) {
			for uid, name := range original {
				cur := &corev1.Pod{}
				err := k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: ns.Name, Name: name}, cur)
				if err != nil {
					g.Expect(client.IgnoreNotFound(err)).To(Succeed())
					continue
				}
				g.Expect(cur.UID).NotTo(Equal(uid), "member %s must disappear despite the paused kubelet", name)
			}
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("every member has its own PodForceDeleted warning and audit row")
		Eventually(func(g Gomega) {
			events := eventsWithReason(g, ns.Name, "PodForceDeleted")
			for _, name := range original {
				named := false
				for _, e := range events {
					g.Expect(e.Type).To(Equal(corev1.EventTypeWarning))
					if containsAll(e.Message, name, gangNode) {
						named = true
					}
				}
				g.Expect(named).To(BeTrue(), "PodForceDeleted must name member %s and node %s", name, gangNode)
			}
			rows := map[string]auditEntryShape{}
			for _, e := range loadLedger(g, ns.Name, isvcName).Entries {
				if e.Reason == "ForceDelete" {
					rows[e.RequestUUID] = e
				}
			}
			for uid, name := range original {
				row, ok := rows[string(uid)]
				g.Expect(ok).To(BeTrue(), "the audit ledger must carry a ForceDelete row for member %s (%s)", name, uid)
				g.Expect(row.Outcome).To(Equal("force-delete-unreachable"))
				g.Expect(row.FromNode).To(Equal(gangNode))
			}
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("the gang is rebuilt whole on another node while " + gangNode + " is still down")
		Eventually(func(g Gomega) {
			node := &corev1.Node{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{Name: gangNode}, node)).To(Succeed())
			g.Expect(nodeIsReady(node)).To(BeFalse(), "the rebuild must not wait for the dead node to return")

			insts := instanceStatuses(g, ns.Name, isvcName, v1beta1.EngineComponent)
			g.Expect(insts).To(HaveLen(1), "the gang keeps its one index; no extra Instance may appear")
			g.Expect(insts[0].Index).To(Equal(int32(0)))
			g.Expect(insts[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
			g.Expect(insts[0].Operation).To(BeNil(), "the rebuild must end with no attempt left open")

			members := liveEnginePodsOfInstance(g, ns.Name, isvcName, 0)
			g.Expect(members).To(HaveLen(gangSize), "the gang must be rebuilt whole")
			runners := map[string]int{}
			for _, pod := range members {
				_, old := original[pod.UID]
				g.Expect(old).To(BeFalse(), "member %s is an original pod; the rebuild must create new ones", pod.Name)
				g.Expect(pod.Spec.NodeName).NotTo(BeEmpty(), "member %s must be bound", pod.Name)
				g.Expect(pod.Spec.NodeName).NotTo(Equal(gangNode), "member %s must be rebuilt away from the dead node", pod.Name)
				g.Expect(podIsReady(&pod)).To(BeTrue(), "member %s must be Ready", pod.Name)
				g.Expect(podreadiness.IsServing(&pod)).To(BeTrue(), "member %s must be in rotation", pod.Name)
				runners[pod.Labels[query.LabelRunner]]++
			}
			g.Expect(runners).To(Equal(map[string]int{"leader": 1, "worker": gangSize - 1}), "the rebuilt gang must have one leader and its workers")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		samples := stop()
		Expect(samples).To(BeNumerically(">", 0), "the no-graceful-delete invariant must have been sampled while the node was down")
		Expect(listOriginalsLeft(ns.Name, isvcName, original)).To(BeEmpty(),
			"no original member may remain in the API once the gang is rebuilt")
	})
})

// liveEnginePodsOfInstance lists the engine pods of the Instance at idx
// that are not on their way out.
func liveEnginePodsOfInstance(g Gomega, namespace, isvcName string, idx int32) []corev1.Pod {
	var out []corev1.Pod
	for _, pod := range listOMENativePods(g, namespace, isvcName) {
		if pod.DeletionTimestamp != nil {
			continue
		}
		if i, ok := query.InstanceIdxFromLabels(&pod); ok && i == idx {
			out = append(out, pod)
		}
	}
	return out
}

// listOriginalsLeft names the pods of original that the API still lists.
func listOriginalsLeft(namespace, isvcName string, original map[types.UID]string) []string {
	var left []string
	for _, pod := range listOMENativePods(Default, namespace, isvcName) {
		if name, old := original[pod.UID]; old {
			left = append(left, name)
		}
	}
	return left
}

// containsAll reports whether s contains every one of parts.
func containsAll(s string, parts ...string) bool {
	for _, part := range parts {
		if !strings.Contains(s, part) {
			return false
		}
	}
	return true
}
