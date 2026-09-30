package scaleout_kind

import (
	"slices"

	"github.com/google/go-cmp/cmp"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	kueuev1beta2 "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	schedulingv1alpha1 "sigs.k8s.io/scheduler-plugins/apis/scheduling/v1alpha1"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	"github.com/ome-projects/ome/tests/integration/kindfix"
)

var _ = ginkgo.Describe("Multicluster gang physical fit (KIND)", func() {
	ginkgo.It("keeps equal nominal floors while waiting for a whole topology domain", func() {
		if kindfix.SchedulerName() == "" {
			ginkgo.Skip("requires OME_TEST_SCHEDULER and two workers per member")
		}
		env := setupSpecEnv(withKueueManagedNamespaces(), withEngineCPU("500m"))
		label := uniqueName("capacity")
		var memberNodes [][]corev1.Node
		for i, member := range env.members {
			nodes := &corev1.NodeList{}
			gomega.Expect(member.c.client.List(member.c.ctx, nodes)).To(gomega.Succeed())
			workers := slices.DeleteFunc(nodes.Items, func(node corev1.Node) bool {
				_, controlPlane := node.Labels["node-role.kubernetes.io/control-plane"]
				return controlPlane
			})
			gomega.Expect(workers).To(gomega.HaveLen(2))
			memberNodes = append(memberNodes, workers)
			setupCapacityHardwareNodes(member.c, label, 2, workers)
			for j, node := range workers {
				before := node.Labels[corev1.LabelTopologyZone]
				domain := "domain-a"
				if i == 0 && j == 1 {
					domain = "domain-b"
				}
				setGangDomain(member.c, node.Name, domain)
				ginkgo.DeferCleanup(func() { setGangDomain(member.c, node.Name, before) })
			}
			env.grantQuota(member, env.cpuForReplicas(2))
			queue := &kueuev1beta2.ClusterQueue{ObjectMeta: metav1.ObjectMeta{Name: env.queueName + "-cq"}}
			ginkgo.DeferCleanup(func() {
				deleteIfExists(member.c, &kueuev1beta2.LocalQueue{ObjectMeta: metav1.ObjectMeta{Namespace: env.isvcNS, Name: env.queueName}})
				deleteIfExists(member.c, queue)
				gomega.Eventually(func() bool {
					return apierrors.IsNotFound(member.c.client.Get(member.c.ctx, client.ObjectKeyFromObject(queue), &kueuev1beta2.ClusterQueue{}))
				}).WithTimeout(longTimeout).WithPolling(pollInterval).Should(gomega.BeTrue(), "release the queue's ResourceFlavor reference")
			})
			gomega.Expect(member.c.client.Get(member.c.ctx, client.ObjectKey{Name: env.queueName + "-cq"}, queue)).To(gomega.Succeed())
			before := queue.DeepCopy()
			queue.Spec.ResourceGroups = append(queue.Spec.ResourceGroups, kueuev1beta2.ResourceGroup{
				CoveredResources: []corev1.ResourceName{syntheticAccelerator},
				Flavors: []kueuev1beta2.FlavorQuotas{{Name: kueuev1beta2.ResourceFlavorReference(label), Resources: []kueuev1beta2.ResourceQuota{
					{Name: syntheticAccelerator, NominalQuota: resource.MustParse("4")},
				}}},
			})
			gomega.Expect(member.c.client.Patch(member.c.ctx, queue, client.MergeFrom(before))).To(gomega.Succeed())
		}
		collect := capacityCollector(env)
		collect()
		model, runtime := env.seedModelRuntimeOnMembers()
		source := runnableSourceISVC(env.isvcNS, env.isvcName, model, runtime, v1beta1.PlacementModeSplitByCapacity, 1)
		source.Spec.Placement.Split = &v1beta1.SplitSpec{Replicas: ptr.To[int32](2)}
		env.applyEngineResources(source)
		source.Spec.Engine.NodeSelector = map[string]string{syntheticAccelerator: label}
		source.Spec.Engine.Containers[0].Resources.Requests[syntheticAccelerator] = resource.MustParse("2")
		source.Spec.Engine.Containers[0].Resources.Limits = corev1.ResourceList{syntheticAccelerator: resource.MustParse("2")}
		source.Spec.Engine.Leader = &v1beta1.LeaderSpec{PodSpec: *source.Spec.Engine.PodSpec.DeepCopy()}
		source.Spec.Engine.Worker = &v1beta1.WorkerSpec{Size: ptr.To(1), PodSpec: *source.Spec.Engine.PodSpec.DeepCopy()}
		env.routeToSpecQueue(source)
		mustCreate(controlCluster, source)
		ginkgo.DeferCleanup(func() {
			deleteIfExists(controlCluster, source)
			for _, member := range env.members {
				gomega.Eventually(func() bool {
					return derivedSweptOn(member.c, env.isvcNS, env.isvcName)
				}).WithTimeout(longTimeout).WithPolling(pollInterval).Should(gomega.BeTrue())
			}
		})

		assertPlan := func(g gomega.Gomega, wantReady []int32) {
			g.Expect(controlCluster.client.Get(controlCluster.ctx, client.ObjectKeyFromObject(source), source)).To(gomega.Succeed())
			g.Expect(source.Status.Placement).NotTo(gomega.BeNil())
			g.Expect(source.Status.Placement.Plan).NotTo(gomega.BeNil())
			g.Expect(cmp.Diff(int64(2), source.Status.Placement.Plan.AssignedReplicas)).To(gomega.BeEmpty())
			for i, member := range env.members {
				candidate := candidateFor(source.Status.Placement, member.wcName)
				g.Expect(candidate).NotTo(gomega.BeNil())
				g.Expect(candidate.Allocation).NotTo(gomega.BeNil())
				g.Expect(candidate.Allocation.Capacity).NotTo(gomega.BeNil())
				g.Expect(candidate.ObservationKnown).To(gomega.BeTrue())
				g.Expect(cmp.Diff(source.Status.Placement.Plan.ID, candidate.AppliedPlanID)).To(gomega.BeEmpty())
				g.Expect(cmp.Diff(int32(1), candidate.Allocation.DesiredReplicas)).To(gomega.BeEmpty())
				g.Expect(cmp.Diff(int32(1), candidate.Allocation.CurrentReplicas)).To(gomega.BeEmpty())
				g.Expect(cmp.Diff(int64(1), candidate.Allocation.Capacity.Replicas)).To(gomega.BeEmpty())
				g.Expect(candidate.Allocation.Capacity.Pools).To(gomega.HaveLen(1))
				pool := candidate.Allocation.Capacity.Pools[0]
				g.Expect(cmp.Diff(int64(4), pool.Allocatable)).To(gomega.BeEmpty())
				g.Expect(cmp.Diff(int64(4), pool.Demand)).To(gomega.BeEmpty())
				g.Expect(cmp.Diff(int32(1), candidate.AdmittedReplicas)).To(gomega.BeEmpty())
				g.Expect(cmp.Diff(wantReady[i], candidate.ReadyReplicas)).To(gomega.BeEmpty())
			}
		}
		assertGang := func(g gomega.Gomega, member memberReg, ready bool) {
			pods := enginePodsOn(g, member.c, env.isvcNS, env.isvcName)
			g.Expect(pods).To(gomega.HaveLen(2))
			groups, nodes := map[string]bool{}, map[string]bool{}
			for _, pod := range pods {
				g.Expect(cmp.Diff(kindfix.SchedulerName(), pod.Spec.SchedulerName)).To(gomega.BeEmpty())
				g.Expect(pod.Spec.SchedulingGates).To(gomega.BeEmpty(), "Kueue must admit both members")
				g.Expect(pod.DeletionTimestamp.IsZero()).To(gomega.BeTrue())
				g.Expect(pod.Labels[query.LabelPodGroup]).NotTo(gomega.BeEmpty())
				groups[pod.Labels[query.LabelPodGroup]] = true
				if ready {
					g.Expect(cmp.Diff(corev1.PodRunning, pod.Status.Phase)).To(gomega.BeEmpty())
					g.Expect(pod.Spec.NodeName).NotTo(gomega.BeEmpty())
					nodes[pod.Spec.NodeName] = true
					node := &corev1.Node{}
					g.Expect(member.c.client.Get(member.c.ctx, client.ObjectKey{Name: pod.Spec.NodeName}, node)).To(gomega.Succeed())
					g.Expect(cmp.Diff("domain-a", node.Labels[corev1.LabelTopologyZone])).To(gomega.BeEmpty())
				} else {
					g.Expect(pod.Spec.NodeName).To(gomega.BeEmpty(), "a partial gang must not bind")
					g.Expect(cmp.Diff(corev1.PodPending, pod.Status.Phase)).To(gomega.BeEmpty())
					condition := slices.IndexFunc(pod.Status.Conditions, func(c corev1.PodCondition) bool { return c.Type == corev1.PodScheduled })
					g.Expect(condition).To(gomega.BeNumerically(">=", 0))
					g.Expect(cmp.Diff(corev1.ConditionFalse, pod.Status.Conditions[condition].Status)).To(gomega.BeEmpty())
					g.Expect(pod.Status.Conditions[condition].Message).To(gomega.ContainSubstring("no domain has room for gang"))
				}
			}
			g.Expect(groups).To(gomega.HaveLen(1))
			for name := range groups {
				group := &schedulingv1alpha1.PodGroup{}
				g.Expect(member.c.client.Get(member.c.ctx, client.ObjectKey{Namespace: env.isvcNS, Name: name}, group)).To(gomega.Succeed())
				g.Expect(cmp.Diff(int32(2), group.Spec.MinMember)).To(gomega.BeEmpty())
				g.Expect(metav1.GetControllerOf(group)).NotTo(gomega.BeNil())
			}
			if ready {
				g.Expect(nodes).To(gomega.HaveLen(2), "a gang needs distinct worker nodes")
			}
		}
		blocked := func(g gomega.Gomega) {
			assertGang(g, env.members[0], false)
			assertGang(g, env.members[1], true)
			assertPlan(g, []int32{0, 1})
		}
		ginkgo.By("observing quota admission without physical fit on one member")
		gomega.Eventually(blocked).WithTimeout(longTimeout).WithPolling(pollInterval).Should(gomega.Succeed())
		planID := source.Status.Placement.Plan.ID
		gomega.Consistently(blocked).WithTimeout(kindfix.ShortTimeout).WithPolling(pollInterval).Should(gomega.Succeed())

		ginkgo.By("joining the topology domains without changing hardware or floors")
		for _, node := range memberNodes[0] {
			setGangDomain(env.members[0].c, node.Name, "domain-a")
		}
		collect()
		gomega.Eventually(func(g gomega.Gomega) {
			for _, member := range env.members {
				assertGang(g, member, true)
			}
			assertPlan(g, []int32{1, 1})
			g.Expect(cmp.Diff(planID, source.Status.Placement.Plan.ID)).To(gomega.BeEmpty())
		}).WithTimeout(longTimeout).WithPolling(pollInterval).Should(gomega.Succeed())
	})
})

func setGangDomain(member cluster, name, domain string) {
	gomega.Eventually(func(g gomega.Gomega) {
		node := &corev1.Node{}
		g.Expect(member.client.Get(member.ctx, client.ObjectKey{Name: name}, node)).To(gomega.Succeed())
		before := node.DeepCopy()
		node.Labels[corev1.LabelTopologyZone] = domain
		g.Expect(member.client.Patch(member.ctx, node, client.MergeFrom(before))).To(gomega.Succeed())
	}).WithTimeout(mediumTimeout).WithPolling(pollInterval).Should(gomega.Succeed())
}
