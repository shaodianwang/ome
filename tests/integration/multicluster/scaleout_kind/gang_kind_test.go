package scaleout_kind

import (
	"slices"
	"strconv"

	"github.com/google/go-cmp/cmp"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
	"k8s.io/utils/ptr"
	"knative.dev/pkg/apis"
	"sigs.k8s.io/controller-runtime/pkg/client"
	schedulingv1alpha1 "sigs.k8s.io/scheduler-plugins/apis/scheduling/v1alpha1"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	"github.com/ome-projects/ome/tests/integration/kindfix"
	"github.com/ome-projects/ome/tests/integration/util"
)

var _ = ginkgo.Describe("Multicluster worker gangs (KIND)", func() {
	ginkgo.DescribeTable("counts complete admitted gangs as whole replicas",
		func(mode v1beta1.PlacementMode) {
			env := setupSpecEnv(withKueueManagedNamespaces(), withEngineCPU("500m"))
			model, runtime := env.seedModelRuntimeOnMembers()
			const width = 2
			env.grantQuota(env.members[0], env.cpuForReplicas(width-1))
			env.grantQuota(env.members[1], env.cpuForReplicas(width))

			source := runnableSourceISVC(env.isvcNS, env.isvcName, model, runtime, mode, 1)
			if mode == v1beta1.PlacementModeSplit {
				source.Spec.Placement.Split = &v1beta1.SplitSpec{Replicas: ptr.To[int32](2)}
			}
			env.applyEngineResources(source)
			source.Spec.Engine.Leader = &v1beta1.LeaderSpec{PodSpec: *source.Spec.Engine.PodSpec.DeepCopy()}
			source.Spec.Engine.Worker = &v1beta1.WorkerSpec{Size: ptr.To(width - 1), PodSpec: *source.Spec.Engine.PodSpec.DeepCopy()}
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

			assertMemberGang := func(g gomega.Gomega, member memberReg, ready bool) {
				component := &v1beta1.InferenceReplica{}
				g.Expect(member.c.client.Get(member.c.ctx, client.ObjectKey{Namespace: env.isvcNS, Name: env.isvcName + "-engine"}, component)).To(gomega.Succeed())
				g.Expect(cmp.Diff(ptr.To[int32](1), component.Spec.Replicas)).To(gomega.BeEmpty())
				g.Expect(component.Spec.PlacementExecution).NotTo(gomega.BeNil())
				g.Expect(cmp.Diff(source.UID, component.Spec.PlacementExecution.SourceUID)).To(gomega.BeEmpty())
				pods := enginePodsOn(g, member.c, env.isvcNS, env.isvcName)
				g.Expect(cmp.Diff(width, len(pods))).To(gomega.BeEmpty())
				runners := []string{}
				groups := map[string]bool{}
				gated := false
				for _, pod := range pods {
					owner := metav1.GetControllerOf(&pod)
					g.Expect(owner).NotTo(gomega.BeNil())
					g.Expect(cmp.Diff(component.UID, owner.UID)).To(gomega.BeEmpty())
					g.Expect(cmp.Diff("0", pod.Labels[query.LabelInstanceIdx])).To(gomega.BeEmpty())
					g.Expect(pod.Labels[query.LabelPodGroup]).NotTo(gomega.BeEmpty())
					groups[pod.Labels[query.LabelPodGroup]] = true
					g.Expect(cmp.Diff("0", pod.Labels[query.LabelPodOrdinal])).To(gomega.BeEmpty())
					runners = append(runners, pod.Labels[query.LabelRunner])
					gated = gated || len(pod.Spec.SchedulingGates) > 0
					if ready {
						g.Expect(pod.DeletionTimestamp.IsZero()).To(gomega.BeTrue())
						g.Expect(cmp.Diff(corev1.PodRunning, pod.Status.Phase)).To(gomega.BeEmpty())
						g.Expect(pod.Spec.SchedulingGates).To(gomega.BeEmpty())
						g.Expect(slices.ContainsFunc(pod.Status.Conditions, func(c corev1.PodCondition) bool {
							return c.Type == corev1.PodReady && c.Status == corev1.ConditionTrue
						})).To(gomega.BeTrue())
					}
				}
				slices.Sort(runners)
				g.Expect(cmp.Diff([]string{"leader", "worker"}, runners)).To(gomega.BeEmpty())
				g.Expect(cmp.Diff(1, len(groups))).To(gomega.BeEmpty())
				g.Expect(cmp.Diff(!ready, gated)).To(gomega.BeEmpty())
				for name := range groups {
					group := &schedulingv1alpha1.PodGroup{}
					g.Expect(member.c.client.Get(member.c.ctx, client.ObjectKey{Namespace: env.isvcNS, Name: name}, group)).To(gomega.Succeed())
					g.Expect(cmp.Diff(int32(width), group.Spec.MinMember)).To(gomega.BeEmpty())
					owner := metav1.GetControllerOf(group)
					g.Expect(owner).NotTo(gomega.BeNil())
					g.Expect(cmp.Diff(component.UID, owner.UID)).To(gomega.BeEmpty())
				}
			}
			assertPlacement := func(g gomega.Gomega, want []int32) {
				g.Expect(controlCluster.client.Get(controlCluster.ctx, client.ObjectKeyFromObject(source), source)).To(gomega.Succeed())
				g.Expect(source.Status.Placement).NotTo(gomega.BeNil())
				g.Expect(source.Status.Placement.Plan).NotTo(gomega.BeNil())
				g.Expect(cmp.Diff(int64(2), source.Status.Placement.Plan.AssignedReplicas)).To(gomega.BeEmpty())
				gotAdmitted, gotReady := []int32{}, []int32{}
				for _, member := range env.members {
					candidate := candidateFor(source.Status.Placement, member.wcName)
					g.Expect(candidate).NotTo(gomega.BeNil())
					g.Expect(candidate.Allocation).NotTo(gomega.BeNil())
					g.Expect(candidate.ObservationKnown).To(gomega.BeTrue())
					g.Expect(cmp.Diff(int32(1), candidate.Allocation.CurrentReplicas)).To(gomega.BeEmpty())
					g.Expect(cmp.Diff(int32(1), candidate.Allocation.DesiredReplicas)).To(gomega.BeEmpty())
					gotAdmitted = append(gotAdmitted, candidate.AdmittedReplicas)
					gotReady = append(gotReady, candidate.ReadyReplicas)
				}
				g.Expect(cmp.Diff(want, gotAdmitted)).To(gomega.BeEmpty())
				g.Expect(cmp.Diff(want, gotReady)).To(gomega.BeEmpty())
			}

			ginkgo.By("withholding replica credit while one member cannot admit every gang pod")
			gomega.Eventually(func(g gomega.Gomega) {
				assertMemberGang(g, env.members[0], false)
				assertMemberGang(g, env.members[1], true)
				assertPlacement(g, []int32{0, 1})
				condition := source.Status.GetCondition(v1beta1.PlacementSatisfied)
				g.Expect(condition).NotTo(gomega.BeNil())
				g.Expect(cmp.Diff(corev1.ConditionFalse, condition.Status)).To(gomega.BeEmpty())
				g.Expect(cmp.Diff("AllocationShortfall", condition.Reason)).To(gomega.BeEmpty())
			}).WithTimeout(longTimeout).WithPolling(pollInterval).Should(gomega.Succeed())

			ginkgo.By("admitting the complete remaining gang without changing the assigned floors")
			env.grantQuota(env.members[0], env.cpuForReplicas(width))
			gomega.Eventually(func(g gomega.Gomega) {
				for _, member := range env.members {
					assertMemberGang(g, member, true)
				}
				assertPlacement(g, []int32{1, 1})
				condition := source.Status.GetCondition(v1beta1.PlacementSatisfied)
				g.Expect(condition).NotTo(gomega.BeNil())
				g.Expect(cmp.Diff(corev1.ConditionTrue, condition.Status)).To(gomega.BeEmpty())
			}).WithTimeout(longTimeout).WithPolling(pollInterval).Should(gomega.Succeed())

			ginkgo.By("retaining the accepted plan while member serving status changes")
			accepted := source.Status.Placement.Plan.DeepCopy()
			heartbeat := 0
			gomega.Consistently(func(g gomega.Gomega) {
				heartbeat++
				for _, member := range env.members {
					g.Expect(retry.RetryOnConflict(retry.DefaultRetry, func() error {
						service := &v1beta1.InferenceService{}
						if err := member.c.client.Get(member.c.ctx, client.ObjectKeyFromObject(source), service); err != nil {
							return err
						}
						before := service.DeepCopy()
						condition := apis.Condition{Type: "MemberHeartbeat", Status: corev1.ConditionTrue, Reason: "Observed", Message: strconv.Itoa(heartbeat)}
						index := slices.IndexFunc(service.Status.Conditions, func(c apis.Condition) bool { return c.Type == condition.Type })
						if index < 0 {
							service.Status.Conditions = append(service.Status.Conditions, condition)
						} else {
							service.Status.Conditions[index] = condition
						}
						return member.c.client.Status().Patch(member.c.ctx, service, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{}))
					})).To(gomega.Succeed())
				}
				g.Expect(controlCluster.client.Get(controlCluster.ctx, client.ObjectKeyFromObject(source), source)).To(gomega.Succeed())
				g.Expect(cmp.Diff(accepted, source.Status.Placement.Plan)).To(gomega.BeEmpty())
			}).WithTimeout(kindfix.ShortTimeout).WithPolling(pollInterval).Should(gomega.Succeed())
			gomega.Eventually(func(g gomega.Gomega) {
				assertPlacement(g, []int32{1, 1})
				g.Expect(cmp.Diff(accepted, source.Status.Placement.Plan)).To(gomega.BeEmpty())
				for _, member := range env.members {
					candidate := candidateFor(source.Status.Placement, member.wcName)
					g.Expect(cmp.Diff(accepted.ID, candidate.AppliedPlanID)).To(gomega.BeEmpty())
				}
			}).WithTimeout(longTimeout).WithPolling(pollInterval).Should(gomega.Succeed())
		},
		ginkgo.Entry("static split", v1beta1.PlacementModeSplit),
		ginkgo.Entry("full policy on every member", v1beta1.PlacementModeAll),
	)

	ginkgo.It("keeps the Single race open until every gang member exists and is admitted", func() {
		env := setupSpecEnv(withKueueManagedNamespaces(), withEngineCPU("500m"))
		model, runtime := env.seedModelRuntimeOnMembers()
		env.grantQuota(env.members[0], env.cpuForReplicas(2))
		env.grantQuota(env.members[1], env.cpuForReplicas(0))
		podQuota := &corev1.ResourceQuota{
			ObjectMeta: metav1.ObjectMeta{Namespace: env.isvcNS, Name: "pod-count"},
			Spec:       corev1.ResourceQuotaSpec{Hard: corev1.ResourceList{corev1.ResourcePods: resource.MustParse("1")}},
		}
		mustCreate(env.members[0].c, podQuota)
		source := runnableSourceISVC(env.isvcNS, env.isvcName, model, runtime, v1beta1.PlacementModeSingle, 1)
		env.applyEngineResources(source)
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

		ginkgo.By("admitting one Pod while the namespace quota prevents its worker from existing")
		gomega.Eventually(func(g gomega.Gomega) {
			pods := enginePodsOn(g, env.members[0].c, env.isvcNS, env.isvcName)
			g.Expect(cmp.Diff(1, len(pods))).To(gomega.BeEmpty())
			g.Expect(pods[0].Spec.SchedulingGates).To(gomega.BeEmpty())
			rows, err := util.GetInstanceStatuses(env.members[0].c.ctx, env.members[0].c.client, client.ObjectKey{Namespace: env.isvcNS, Name: env.isvcName + "-engine"})
			g.Expect(err).NotTo(gomega.HaveOccurred())
			g.Expect(rows).To(gomega.HaveLen(1))
			g.Expect(rows[0].Admitted).To(gomega.BeTrue())
			g.Expect(cmp.Diff(int32(1), rows[0].PodCount)).To(gomega.BeEmpty())
		}).WithTimeout(longTimeout).WithPolling(pollInterval).Should(gomega.Succeed())

		ginkgo.By("withholding winner selection and preserving the other candidate")
		gomega.Consistently(func(g gomega.Gomega) {
			g.Expect(controlCluster.client.Get(controlCluster.ctx, client.ObjectKeyFromObject(source), source)).To(gomega.Succeed())
			g.Expect(source.Status.Placement).NotTo(gomega.BeNil())
			g.Expect(source.Status.Placement.Plan).NotTo(gomega.BeNil())
			g.Expect(source.Status.Placement.Plan.Winner).To(gomega.BeEmpty())
			for _, member := range env.members {
				derived := &v1beta1.InferenceService{}
				g.Expect(member.c.client.Get(member.c.ctx, client.ObjectKeyFromObject(source), derived)).To(gomega.Succeed())
				g.Expect(derived.DeletionTimestamp.IsZero()).To(gomega.BeTrue())
			}
		}).WithTimeout(kindfix.ShortTimeout).WithPolling(pollInterval).Should(gomega.Succeed())

		ginkgo.By("allowing the complete gang to win and cleaning up its competitor")
		gomega.Expect(env.members[0].c.client.Get(env.members[0].c.ctx, client.ObjectKeyFromObject(podQuota), podQuota)).To(gomega.Succeed())
		podQuota.Spec.Hard[corev1.ResourcePods] = resource.MustParse("2")
		gomega.Expect(env.members[0].c.client.Update(env.members[0].c.ctx, podQuota)).To(gomega.Succeed())
		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(controlCluster.client.Get(controlCluster.ctx, client.ObjectKeyFromObject(source), source)).To(gomega.Succeed())
			g.Expect(cmp.Diff(env.members[0].wcName, source.Status.Placement.Plan.Winner)).To(gomega.BeEmpty())
			candidate := candidateFor(source.Status.Placement, env.members[0].wcName)
			g.Expect(candidate).NotTo(gomega.BeNil())
			g.Expect(cmp.Diff(int32(1), candidate.AdmittedReplicas)).To(gomega.BeEmpty())
			g.Expect(cmp.Diff(int32(1), candidate.ReadyReplicas)).To(gomega.BeEmpty())
			pods := enginePodsOn(g, env.members[0].c, env.isvcNS, env.isvcName)
			g.Expect(pods).To(gomega.HaveLen(2))
			for _, pod := range pods {
				g.Expect(pod.DeletionTimestamp.IsZero()).To(gomega.BeTrue())
				g.Expect(pod.Spec.SchedulingGates).To(gomega.BeEmpty())
				g.Expect(cmp.Diff(corev1.PodRunning, pod.Status.Phase)).To(gomega.BeEmpty())
			}
			g.Expect(derivedSweptOn(env.members[1].c, env.isvcNS, env.isvcName)).To(gomega.BeTrue())
		}).WithTimeout(longTimeout).WithPolling(pollInterval).Should(gomega.Succeed())
	})
})
