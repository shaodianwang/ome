package multicluster

import (
	"github.com/google/go-cmp/cmp"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/utils/ptr"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	ometesting "github.com/ome-projects/ome/pkg/utils/testing/v1beta1"
)

// Static Split assigns exact shares before observing member admission.
var _ = ginkgo.Describe("Multi-cluster placement split mode", func() {
	const (
		// acceleratorLabel/Value is the capability both members advertise and the
		// source ISVC requires (a fixed test value; production resolves it from
		// spec.model + runtime).
		acceleratorLabel = "gpu"
		acceleratorValue = "test-accel"
	)

	var (
		secretNS string
		isvcNS   string

		wc1Name, wc2Name string
		secret1, secret2 string
		isvcName         string
	)

	matchingLabels := func() map[string]string { return map[string]string{acceleratorLabel: acceleratorValue} }
	requirement := func() string { return acceleratorLabel + "=" + acceleratorValue }

	ginkgo.BeforeEach(func() {
		skipIfRealKueue()
		secretNS = uniqueName("multiclu-sp-secrets")
		isvcNS = uniqueName("multiclu-sp-workload")
		createNamespace(controlCluster, secretNS)
		createNamespace(controlCluster, isvcNS)
		createNamespace(member1Cluster, isvcNS)
		createNamespace(member2Cluster, isvcNS)

		secret1 = uniqueName("multiclu-sp-kc-m1")
		secret2 = uniqueName("multiclu-sp-kc-m2")
		wc1Name = uniqueName("multiclu-sp-a")
		wc2Name = uniqueName("multiclu-sp-b")
		isvcName = uniqueName("multiclu-sp-svc")

		mustCreate(controlCluster, kubeconfigSecretFor(member1Cluster, secretNS, secret1))
		mustCreate(controlCluster, kubeconfigSecretFor(member2Cluster, secretNS, secret2))

		ginkgo.By("creating the WorkloadClusters and waiting for Ready=True")
		for _, wc := range []struct{ name, secret string }{{wc1Name, secret1}, {wc2Name, secret2}} {
			ometesting.CreateAndWaitReady(controlCluster.ctx, controlCluster.client,
				WorkloadClusterFor(wc.name, secretNS, wc.secret, matchingLabels()),
				"WorkloadCluster Ready=True",
				func(o *v1beta1.WorkloadCluster) bool {
					return apimeta.IsStatusConditionTrue(o.Status.Conditions, v1beta1.WorkloadClusterReady)
				})
		}
	})

	ginkgo.AfterEach(func() {
		deleteIfExists(controlCluster, splitSourceISVC(isvcNS, isvcName, requirement(), nil))
		deleteIfExists(controlCluster, WorkloadClusterFor(wc1Name, secretNS, secret1, nil))
		deleteIfExists(controlCluster, WorkloadClusterFor(wc2Name, secretNS, secret2, nil))
	})

	// derivedEngineMin reads the derived ISVC's pinned engine floor on a member,
	// returning -1 while the derived (or its pinned floor) is absent so an
	// Eventually can converge on the expected share.
	derivedEngineMin := func(member cluster) int {
		derived, ok := getDerivedOn(member, isvcNS, isvcName)
		if !ok || derived.Spec.Engine == nil || derived.Spec.Engine.MinReplicas == nil {
			return -1
		}
		return *derived.Spec.Engine.MinReplicas
	}

	ginkgo.DescribeTable("assigns exact initial floors",
		func(replicas int32, weighted bool, want []int) {
			source := splitSourceISVC(isvcNS, isvcName, requirement(), &v1beta1.SplitSpec{Replicas: ptr.To(replicas)})
			if weighted {
				source.Spec.Placement.ClusterAffinity = []v1beta1.ClusterAffinityTerm{
					{Weight: ptr.To[int32](3), MatchFields: []v1beta1.ClusterSelectorRequirement{{Key: "metadata.name", Operator: corev1.NodeSelectorOpIn, Values: []string{wc1Name}}}},
					{Weight: ptr.To[int32](1), MatchFields: []v1beta1.ClusterSelectorRequirement{{Key: "metadata.name", Operator: corev1.NodeSelectorOpIn, Values: []string{wc2Name}}}},
				}
			}
			mustCreate(controlCluster, source)
			gomega.Eventually(func() string {
				return cmp.Diff(want, []int{derivedEngineMin(member1Cluster), derivedEngineMin(member2Cluster)})
			}, eventuallyTimeout, eventuallyPoll).Should(gomega.BeEmpty())
			gomega.Consistently(func() string {
				return cmp.Diff(want, []int{derivedEngineMin(member1Cluster), derivedEngineMin(member2Cluster)})
			}, "1.5s", eventuallyPoll).Should(gomega.BeEmpty())
		},
		ginkgo.Entry("divisible floor", int32(4), false, []int{2, 2}),
		ginkgo.Entry("remainder uses deterministic name order", int32(3), false, []int{2, 1}),
		ginkgo.Entry("zero share creates no member service", int32(1), false, []int{1, -1}),
		ginkgo.Entry("explicit weights allocate exact ratios", int32(4), true, []int{3, 1}),
	)

	ginkgo.It("retains a pending share despite surplus admission on another home", func() {
		mustCreate(controlCluster, splitSourceISVC(isvcNS, isvcName, requirement(),
			&v1beta1.SplitSpec{Replicas: ptr.To[int32](3)}))
		gomega.Eventually(func() string {
			return cmp.Diff([]int{2, 1}, []int{derivedEngineMin(member1Cluster), derivedEngineMin(member2Cluster)})
		}, eventuallyTimeout, eventuallyPoll).Should(gomega.BeEmpty())

		upsertPlannedEngineStatusOn(member1Cluster, isvcNS, isvcName, admittedEngineInstances(3))
		upsertPlannedEngineStatusOn(member2Cluster, isvcNS, isvcName, nil)
		gomega.Eventually(func(g gomega.Gomega) {
			ps := placementStatusOf(isvcNS, isvcName)
			g.Expect(ps).NotTo(gomega.BeNil())
			g.Expect(ps.Plan).NotTo(gomega.BeNil())
			admitted, desired := map[string]int32{}, map[string]int32{}
			for _, c := range ps.Candidates {
				g.Expect(c.Allocation).NotTo(gomega.BeNil())
				admitted[c.Cluster], desired[c.Cluster] = c.AdmittedReplicas, c.Allocation.DesiredReplicas
			}
			g.Expect(cmp.Diff(map[string]int32{wc1Name: 3, wc2Name: 0}, admitted)).To(gomega.BeEmpty())
			g.Expect(cmp.Diff(map[string]int32{wc1Name: 2, wc2Name: 1}, desired)).To(gomega.BeEmpty())
		}, eventuallyTimeout, eventuallyPoll).Should(gomega.Succeed())
		gomega.Consistently(func() string {
			return cmp.Diff([]int{2, 1}, []int{derivedEngineMin(member1Cluster), derivedEngineMin(member2Cluster)})
		}, "1.5s", eventuallyPoll).Should(gomega.BeEmpty())
	})
})

// splitSourceISVC declares a fleet floor with one Engine per replica unit.
func splitSourceISVC(namespace, name, requirements string, split *v1beta1.SplitSpec) *v1beta1.InferenceService {
	source := sourceISVC(namespace, name, requirements)
	source.Spec.Placement.Mode = v1beta1.PlacementModeSplit
	source.Spec.Placement.Split = split
	return source
}
