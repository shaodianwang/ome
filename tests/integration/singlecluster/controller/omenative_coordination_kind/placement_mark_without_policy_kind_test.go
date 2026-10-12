package omenative_coordination_kind

import (
	"github.com/google/go-cmp/cmp"
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/placement/protocol"
	"github.com/ome-projects/ome/tests/integration/util"
)

// A service marked for placement keeps reconciling until its policy arrives.
//
// A local engine-only service runs one pod. The placement hub stamps its
// mark on the service first, the ome.io/placement-origin-uid annotation,
// and writes the execution policy later, when it applies its plan. In
// between, the operator raises the engine's minimum replicas from 1 to 2.
// The second pod must appear and serve: without a policy the service is
// still the local service it was, and a service that stops reconciling on
// the mark alone freezes every change until the hub catches up. The status
// says the policy is pending: the PlacementReplicaFloorsReady condition
// reads Unknown with reason ExecutionPolicyPending, and one
// PlacementExecutionPending warning is recorded when the condition first
// appears, not one per pass. The projected InferenceReplica carries no
// placement policy. Once the hub writes a policy whose engine floor is the
// two replicas, the condition turns True with reason ReplicaFloorsAccepted
// and the InferenceReplica carries the policy.
const markOnlyISVC = "placement-marked"

var _ = ginkgo.Describe("behavior — a service marked for placement keeps reconciling until its policy arrives (KIND)", ginkgo.Ordered, func() {
	var ns *corev1.Namespace
	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "placement-mark-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("lands a replica change under the mark alone, says the policy is pending once, and accepts the policy when it arrives", func() {
		fix := newCoordFixture(ns.Name, WithISVCName(markOnlyISVC),
			WithoutDecoder(),
			WithEngineImage("fake-serving:v1"))
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		// pendingWarnings lists the PlacementExecutionPending events on the
		// service.
		pendingWarnings := func(g Gomega) []corev1.Event {
			var out []corev1.Event
			for _, e := range eventsWithReason(g, ns.Name, "PlacementExecutionPending") {
				if e.InvolvedObject.Kind == "InferenceService" && e.InvolvedObject.Name == markOnlyISVC {
					out = append(out, e)
				}
			}
			return out
		}
		expectReadyPods := func(g Gomega, n int) {
			pods := listOMENativePods(g, ns.Name, markOnlyISVC)
			g.Expect(pods).To(HaveLen(n))
			for i := range pods {
				g.Expect(podIsReady(&pods[i])).To(BeTrue(), "pod %s must be Ready", pods[i].Name)
			}
		}

		ginkgo.By("waiting for the local service to serve on one pod")
		Eventually(func(g Gomega) {
			ir := freshInferenceReplica(g, ns.Name, markOnlyISVC, v1beta1.EngineComponent)
			g.Expect(ir.Status.ServingReplicas).To(Equal(int32(1)))
			g.Expect(ir.Spec.PlacementExecution).To(BeNil())
			expectReadyPods(g, 1)
			g.Expect(freshISVC(g, ns.Name, markOnlyISVC).Status.GetCondition(v1beta1.PlacementReplicaFloorsReady)).To(BeNil(),
				"a local service carries no placement condition")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		const sourceUID = types.UID("source-a")
		ginkgo.By("stamping the placement mark and raising the engine to two replicas in one write")
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, markOnlyISVC)
			if cur.Annotations == nil {
				cur.Annotations = map[string]string{}
			}
			cur.Annotations[constants.PlacementOriginUID] = string(sourceUID)
			cur.Spec.Engine.MinReplicas = ptr.To(2)
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("expecting the second pod to serve and the status to say the policy is pending")
		Eventually(func(g Gomega) {
			ir := freshInferenceReplica(g, ns.Name, markOnlyISVC, v1beta1.EngineComponent)
			g.Expect(ir.Spec.Replicas).To(HaveValue(Equal(int32(2))), "the replica change must land under the mark alone")
			g.Expect(ir.Spec.PlacementExecution).To(BeNil(), "no policy was written, so none may be projected")
			g.Expect(ir.Status.ServingReplicas).To(Equal(int32(2)))
			expectReadyPods(g, 2)
			cond := freshISVC(g, ns.Name, markOnlyISVC).Status.GetCondition(v1beta1.PlacementReplicaFloorsReady)
			g.Expect(cond).NotTo(BeNil(), "the status must say the policy is pending")
			g.Expect(cond.Status).To(Equal(corev1.ConditionUnknown))
			g.Expect(cond.Reason).To(Equal("ExecutionPolicyPending"))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("recording the pending policy once, not on every pass")
		Consistently(func(g Gomega) {
			warnings := pendingWarnings(g)
			g.Expect(warnings).To(HaveLen(1), "one PlacementExecutionPending warning, recorded when the condition first appeared")
			g.Expect(warnings[0].Type).To(Equal(corev1.EventTypeWarning))
			g.Expect(warnings[0].Count).To(BeNumerically("<=", 1), "the warning must not be repeated on every pass")
			cond := freshISVC(g, ns.Name, markOnlyISVC).Status.GetCondition(v1beta1.PlacementReplicaFloorsReady)
			g.Expect(cond).NotTo(BeNil())
			g.Expect(cond.Status).To(Equal(corev1.ConditionUnknown))
			g.Expect(cond.Reason).To(Equal("ExecutionPolicyPending"))
		}).WithTimeout(2 * kindSteadyStateWindow).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("writing the execution policy with the two-replica floor")
		policy := &v1beta1.PlacementExecutionPolicy{
			PlanID: "plan-a", Revision: 1, SourceUID: sourceUID, ClusterUID: "cluster-a",
			ReplicaFloors: []v1beta1.PlacementComponentFloor{{Component: v1beta1.EngineComponent, Replicas: 2}},
		}
		raw, err := protocol.Encode(policy)
		Expect(err).NotTo(HaveOccurred())
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, markOnlyISVC)
			cur.Annotations[constants.PlacementExecution] = raw
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("expecting the floors accepted and the policy projected")
		Eventually(func(g Gomega) {
			cond := freshISVC(g, ns.Name, markOnlyISVC).Status.GetCondition(v1beta1.PlacementReplicaFloorsReady)
			g.Expect(cond).NotTo(BeNil())
			g.Expect(cond.Status).To(Equal(corev1.ConditionTrue))
			g.Expect(cond.Reason).To(Equal("ReplicaFloorsAccepted"))
			ir := freshInferenceReplica(g, ns.Name, markOnlyISVC, v1beta1.EngineComponent)
			g.Expect(cmp.Diff(policy, ir.Spec.PlacementExecution)).To(BeEmpty(), "the InferenceReplica must carry the policy")
			g.Expect(ir.Status.ServingReplicas).To(Equal(int32(2)))
			expectReadyPods(g, 2)
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})
})
