package scaleout_kind

import (
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"knative.dev/pkg/apis"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/placement/protocol"
)

// Member quota controls admission while the source retains its exact shares.
var _ = ginkgo.Describe("Split under real quota (KIND)", func() {
	var env *specEnv
	ginkgo.BeforeEach(func() {
		env = setupSpecEnv(withKueueManagedNamespaces(), withEngineCPU("500m"))
	})

	ginkgo.DescribeTable("retains exact allocation through real admission",
		func(desired, ceiling int, quotas, wantRunning, wantFloors []int) {
			model, runtime := env.seedModelRuntimeOnMembers()
			first := env.preferredMember()
			members := []memberReg{first, env.other(first)}
			for i, m := range members {
				env.grantQuota(m, env.cpuForReplicas(quotas[i]))
			}
			env.createSplitSourceWith(model, runtime, desired, &v1beta1.SplitSpec{MaxReplicasPerCluster: int32(ceiling)})

			assertAllocation := func(g gomega.Gomega) {
				running, floors := []int{}, []int{}
				for _, m := range members {
					running = append(running, runningEnginePodsOn(g, m.c, env.isvcNS, env.isvcName))
					floor := 0
					if derived, found := getDerivedOn(m.c, env.isvcNS, env.isvcName); found {
						g.Expect(derived.Spec.Engine.MinReplicas).NotTo(gomega.BeNil())
						floor = *derived.Spec.Engine.MinReplicas
						policy, err := protocol.FromDerived(derived)
						g.Expect(err).NotTo(gomega.HaveOccurred())
						g.Expect(policy).NotTo(gomega.BeNil())
						g.Expect(policy.PauseSurge).To(gomega.BeFalse(), "settled allocation must release local rollout and autoscaling")
					}
					floors = append(floors, floor)
				}
				g.Expect(cmp.Diff(wantFloors, floors)).To(gomega.BeEmpty())
				g.Expect(cmp.Diff(wantRunning, running)).To(gomega.BeEmpty())
				ps := placementStatusOf(env.isvcNS, env.isvcName)
				g.Expect(ps).NotTo(gomega.BeNil())
				g.Expect(ps.Plan).NotTo(gomega.BeNil())
				g.Expect(ps.Plan.PauseSurge).To(gomega.BeFalse())
			}
			gomega.Eventually(assertAllocation).WithTimeout(longTimeout).WithPolling(pollInterval).Should(gomega.Succeed())
			gomega.Consistently(assertAllocation).WithTimeout(10 * time.Second).WithPolling(time.Second).Should(gomega.Succeed())
		},
		ginkgo.Entry("even shares when each home can admit its floor", 6, 0, []int{3, 3}, []int{3, 3}, []int{3, 3}),
		ginkgo.Entry("odd total stays exact despite surplus quota", 3, 0, []int{4, 4}, []int{2, 1}, []int{2, 1}),
		ginkgo.Entry("zero share leaves no member workload", 1, 0, []int{2, 2}, []int{1, 0}, []int{1, 0}),
		ginkgo.Entry("local ceiling permits a matching allocation", 4, 2, []int{4, 4}, []int{2, 2}, []int{2, 2}),
		ginkgo.Entry("partial admission retains pending replica floors", 6, 0, []int{1, 1}, []int{1, 1}, []int{3, 3}),
		ginkgo.Entry("one constrained home cannot spill onto spare quota", 3, 0, []int{1, 3}, []int{1, 1}, []int{2, 1}),
	)

	ginkgo.It("rejects the whole allocation when an exact share exceeds its ceiling", func() {
		model, runtime := env.seedModelRuntimeOnMembers()
		for _, m := range env.members {
			env.grantQuota(m, env.cpuForReplicas(5))
		}
		env.createSplitSourceWith(model, runtime, 5, &v1beta1.SplitSpec{MaxReplicasPerCluster: 2})
		gomega.Eventually(func(g gomega.Gomega) {
			source := &v1beta1.InferenceService{}
			g.Expect(controlCluster.client.Get(controlCluster.ctx, client.ObjectKey{Namespace: env.isvcNS, Name: env.isvcName}, source)).To(gomega.Succeed())
			condition := source.Status.GetCondition(apis.ConditionType(v1beta1.PlacementConverged))
			g.Expect(condition).NotTo(gomega.BeNil())
			g.Expect(condition.Reason).To(gomega.Equal("AllocationUnresolved"))
			g.Expect(source.Status.Placement.Plan).To(gomega.BeNil())
			for _, m := range env.members {
				_, found := getDerivedOn(m.c, env.isvcNS, env.isvcName)
				g.Expect(found).To(gomega.BeFalse())
			}
		}).WithTimeout(longTimeout).WithPolling(pollInterval).Should(gomega.Succeed())
	})
})
