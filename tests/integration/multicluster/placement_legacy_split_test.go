package multicluster

import (
	"github.com/google/go-cmp/cmp"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	ometesting "github.com/ome-projects/ome/pkg/utils/testing/v1beta1"
)

// Legacy Packed placement fills by admission: every candidate is asked for the
// floor, the first home to admit keeps it, and a later replica update tops up
// the home already serving rather than re-racing the floor.
var _ = ginkgo.Describe("Multi-cluster legacy Packed placement", func() {
	const (
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
		secretNS = uniqueName("multiclu-lp-secrets")
		isvcNS = uniqueName("multiclu-lp-workload")
		createNamespace(controlCluster, secretNS)
		createNamespace(controlCluster, isvcNS)
		createNamespace(member1Cluster, isvcNS)
		createNamespace(member2Cluster, isvcNS)

		secret1 = uniqueName("multiclu-lp-kc-m1")
		secret2 = uniqueName("multiclu-lp-kc-m2")
		// The member names sort in this order, so the serving home of this
		// story is the candidate that sorts last.
		wc1Name = uniqueName("multiclu-lp-a")
		wc2Name = uniqueName("multiclu-lp-b")
		isvcName = uniqueName("multiclu-lp-svc")

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
		deleteIfExists(controlCluster, legacyPackedSourceISVC(isvcNS, isvcName, requirement(), 1))
		deleteIfExists(controlCluster, WorkloadClusterFor(wc1Name, secretNS, secret1, nil))
		deleteIfExists(controlCluster, WorkloadClusterFor(wc2Name, secretNS, secret2, nil))
	})

	// floorOn reads a member's derived engine floor, or -1 while the member
	// holds no derived copy.
	floorOn := func(member cluster) int {
		derived, ok := getDerivedOn(member, isvcNS, isvcName)
		if !ok || derived.Spec.Engine == nil || derived.Spec.Engine.MinReplicas == nil {
			return -1
		}
		return *derived.Spec.Engine.MinReplicas
	}
	floors := func() []int { return []int{floorOn(member1Cluster), floorOn(member2Cluster)} }
	uidOn := func(member cluster) types.UID {
		derived, ok := getDerivedOn(member, isvcNS, isvcName)
		if !ok {
			return ""
		}
		return derived.UID
	}
	admittedByHome := func() map[string]int32 {
		out := map[string]int32{}
		ps := placementStatusOf(isvcNS, isvcName)
		if ps == nil {
			return out
		}
		for _, c := range ps.Candidates {
			out[c.Cluster] = c.AdmittedReplicas
		}
		return out
	}

	ginkgo.It("tops up the serving home on a replica update", func() {
		source := legacyPackedSourceISVC(isvcNS, isvcName, requirement(), 3)
		mustCreate(controlCluster, source)

		ginkgo.By("asking every candidate for the whole floor")
		gomega.Eventually(func() string {
			return cmp.Diff([]int{3, 3}, floors())
		}, eventuallyTimeout, eventuallyPoll).Should(gomega.BeEmpty())

		ginkgo.By("keeping the home that admits first and sweeping the other")
		upsertPlannedEngineStatusOn(member2Cluster, isvcNS, isvcName, admittedEngineInstances(3))
		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(cmp.Diff([]int{-1, 3}, floors())).To(gomega.BeEmpty())
			g.Expect(cmp.Diff(map[string]int32{wc2Name: 3}, admittedByHome())).To(gomega.BeEmpty())
		}, eventuallyTimeout, eventuallyPoll).Should(gomega.Succeed())
		servingUID := uidOn(member2Cluster)
		gomega.Expect(servingUID).NotTo(gomega.BeEmpty())

		ginkgo.By("raising the floor from three to five")
		gomega.Expect(retry.RetryOnConflict(retry.DefaultRetry, func() error {
			current := &v1beta1.InferenceService{}
			if err := controlCluster.client.Get(controlCluster.ctx, client.ObjectKeyFromObject(source), current); err != nil {
				return err
			}
			current.Spec.Placement.Split.Replicas = ptr.To[int32](5)
			return controlCluster.client.Update(controlCluster.ctx, current)
		})).To(gomega.Succeed())
		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(cmp.Diff([]int{2, 5}, floors())).To(gomega.BeEmpty(), "the serving home is topped up; the empty candidate gets the deficit")
			g.Expect(uidOn(member2Cluster)).To(gomega.Equal(servingUID), "the serving home keeps its derived copy")
		}, eventuallyTimeout, eventuallyPoll).Should(gomega.Succeed())

		ginkgo.By("admitting the deficit on the empty candidate before the serving home scales")
		upsertPlannedEngineStatusOn(member1Cluster, isvcNS, isvcName, admittedEngineInstances(2))
		servingRetained := func(g gomega.Gomega) {
			current := floors()
			g.Expect(uidOn(member2Cluster)).To(gomega.Equal(servingUID), "the serving home keeps its derived copy")
			g.Expect(current[1]).To(gomega.BeNumerically(">=", 3), "the serving home keeps its serving share")
			g.Expect(current[0] + current[1]).To(gomega.Equal(5))
		}
		gomega.Eventually(servingRetained, eventuallyTimeout, eventuallyPoll).Should(gomega.Succeed())
		gomega.Consistently(servingRetained, "1.5s", eventuallyPoll).Should(gomega.Succeed())
	})
})

// legacyPackedSourceISVC declares legacy Packed placement by label requirement,
// with a fleet floor of `replicas` and no cluster-affinity policy.
func legacyPackedSourceISVC(namespace, name, requirements string, replicas int32) *v1beta1.InferenceService {
	source := sourceISVC(namespace, name, requirements)
	source.Spec.Placement = &v1beta1.PlacementSpec{
		Mode:         v1beta1.PlacementModeSplit,
		Requirements: requirements,
		Split:        &v1beta1.SplitSpec{Replicas: ptr.To(replicas)},
	}
	return source
}
