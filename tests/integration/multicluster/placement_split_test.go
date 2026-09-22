package multicluster

import (
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	ometesting "github.com/ome-projects/ome/pkg/utils/testing/v1beta1"
)

// These specs (S12-S14) drive PlacementModeSplit end-to-end across the three
// envtest apiservers on the sim path: the fleet-wide floor is apportioned onto
// the candidates (FILL over-requests the deficit; Kueue on each member admits
// what fits), the admitted counts reported by each home's InferenceReplica are
// what the control plane sums against the floor, TRIM pins each kept home's
// request to its admitted share, an unneeded home is swept, and Balanced spread
// requests an even share everywhere. The sources declare their candidacy via
// the structured spec.placement block (requirements + mode).
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

	// candidateByCluster returns the placement-status candidate entry for a
	// cluster, or nil when absent.
	candidateByCluster := func(ps *v1beta1.PlacementStatus, name string) *v1beta1.CandidatePlacement {
		if ps == nil {
			return nil
		}
		for i := range ps.Candidates {
			if ps.Candidates[i].Cluster == name {
				return &ps.Candidates[i]
			}
		}
		return nil
	}

	ginkgo.It("S12 packed floor: sums admitted replicas across homes and trims each request to its admitted share", func() {
		ginkgo.By("creating a Split source with a fleet-wide floor of 3")
		mustCreate(controlCluster, splitSourceISVC(isvcNS, isvcName, requirement(),
			&v1beta1.SplitSpec{Replicas: ptr.To[int32](3)}))

		// While nothing is admitted, FILL over-requests the whole deficit on every
		// home (each member's Kueue is expected to admit what fits).
		ginkgo.By("asserting the fan-out over-requests the full floor on both homes")
		gomega.Eventually(func() int { return derivedEngineMin(member1Cluster) },
			eventuallyTimeout, eventuallyPoll).Should(gomega.Equal(3))
		gomega.Eventually(func() int { return derivedEngineMin(member2Cluster) },
			eventuallyTimeout, eventuallyPoll).Should(gomega.Equal(3))

		ginkgo.By("admitting 2 replicas on member1 and 1 on member2")
		upsertEngineIRStatusOn(member1Cluster, isvcNS, isvcName, admittedEngineInstances(2))
		upsertEngineIRStatusOn(member2Cluster, isvcNS, isvcName, admittedEngineInstances(1))

		// The floor is met from the ADMITTED counts (2+1), not readiness: no
		// instance reports ready, yet the placement must be Placed with both homes
		// recorded and no single winner cluster.
		ginkgo.By("asserting the placement is Placed from admitted counts, with per-home accounting")
		gomega.Eventually(func(g gomega.Gomega) {
			ps := placementStatusOf(isvcNS, isvcName)
			g.Expect(ps).NotTo(gomega.BeNil())
			g.Expect(ps.Phase).To(gomega.Equal(v1beta1.PlacementPhasePlaced))
			g.Expect(ps.Cluster).To(gomega.BeEmpty(), "Split has no single winner cluster")
			g.Expect(ps.Candidates).To(gomega.HaveLen(2))
			c1 := candidateByCluster(ps, wc1Name)
			g.Expect(c1).NotTo(gomega.BeNil())
			g.Expect(c1.Phase).To(gomega.Equal(v1beta1.CandidatePhaseAdmitted))
			g.Expect(c1.AdmittedReplicas).To(gomega.Equal(int32(2)))
			c2 := candidateByCluster(ps, wc2Name)
			g.Expect(c2).NotTo(gomega.BeNil())
			g.Expect(c2.Phase).To(gomega.Equal(v1beta1.CandidatePhaseAdmitted))
			g.Expect(c2.AdmittedReplicas).To(gomega.Equal(int32(1)))
		}, eventuallyTimeout, eventuallyPoll).Should(gomega.Succeed())

		// Once the admitted sum covers the floor, TRIM pins each home's request to
		// its admitted share so no gated remainder can over-admit later.
		ginkgo.By("asserting each home's request converges to its admitted share")
		gomega.Eventually(func() int { return derivedEngineMin(member1Cluster) },
			eventuallyTimeout, eventuallyPoll).Should(gomega.Equal(2))
		gomega.Eventually(func() int { return derivedEngineMin(member2Cluster) },
			eventuallyTimeout, eventuallyPoll).Should(gomega.Equal(1))
	})

	ginkgo.It("S13 packed trim: sweeps the home that is not needed once one home covers the floor", func() {
		ginkgo.By("creating a Split source with a fleet-wide floor of 2")
		mustCreate(controlCluster, splitSourceISVC(isvcNS, isvcName, requirement(),
			&v1beta1.SplitSpec{Replicas: ptr.To[int32](2)}))

		ginkgo.By("asserting the fan-out reaches both homes")
		gomega.Eventually(func() int { return derivedEngineMin(member1Cluster) },
			eventuallyTimeout, eventuallyPoll).Should(gomega.Equal(2))
		gomega.Eventually(func() int { return derivedEngineMin(member2Cluster) },
			eventuallyTimeout, eventuallyPoll).Should(gomega.Equal(2))

		ginkgo.By("admitting the whole floor on member1 only")
		upsertEngineIRStatusOn(member1Cluster, isvcNS, isvcName, admittedEngineInstances(2))

		ginkgo.By("asserting the placement is Placed on member1 alone and member2's derived is swept")
		gomega.Eventually(func(g gomega.Gomega) {
			ps := placementStatusOf(isvcNS, isvcName)
			g.Expect(ps).NotTo(gomega.BeNil())
			g.Expect(ps.Phase).To(gomega.Equal(v1beta1.PlacementPhasePlaced))
			g.Expect(ps.Candidates).To(gomega.HaveLen(1))
			g.Expect(ps.Candidates[0].Cluster).To(gomega.Equal(wc1Name))
			g.Expect(ps.Candidates[0].AdmittedReplicas).To(gomega.Equal(int32(2)))
		}, eventuallyTimeout, eventuallyPoll).Should(gomega.Succeed())
		gomega.Eventually(func() error {
			return member2Cluster.client.Get(member2Cluster.ctx,
				types.NamespacedName{Namespace: isvcNS, Name: isvcName}, &v1beta1.InferenceService{})
		}, eventuallyTimeout, eventuallyPoll).Should(ometesting.BeNotFoundError(),
			"a home the packed apportionment does not need must be swept")

		// The floor stays covered by member1, so the swept home must not be
		// re-fanned-out and the kept home's request must stay at the floor.
		ginkgo.By("asserting the swept home stays swept and the kept home stays pinned")
		gomega.Consistently(func(g gomega.Gomega) {
			_, ok := getDerivedOn(member2Cluster, isvcNS, isvcName)
			g.Expect(ok).To(gomega.BeFalse())
			g.Expect(derivedEngineMin(member1Cluster)).To(gomega.Equal(2))
		}, "1.5s", eventuallyPoll).Should(gomega.Succeed())
	})

	ginkgo.It("S14 balanced spread: requests an even share on every home while the race is still gated", func() {
		ginkgo.By("creating a Split source with a floor of 4 and Spread enabled")
		mustCreate(controlCluster, splitSourceISVC(isvcNS, isvcName, requirement(),
			&v1beta1.SplitSpec{Replicas: ptr.To[int32](4), Spread: true}))

		ginkgo.By("asserting each home is asked for the even ceil share")
		gomega.Eventually(func() int { return derivedEngineMin(member1Cluster) },
			eventuallyTimeout, eventuallyPoll).Should(gomega.Equal(2))
		gomega.Eventually(func() int { return derivedEngineMin(member2Cluster) },
			eventuallyTimeout, eventuallyPoll).Should(gomega.Equal(2))

		ginkgo.By("asserting the placement races (both homes placed, none admitted)")
		gomega.Eventually(func(g gomega.Gomega) {
			ps := placementStatusOf(isvcNS, isvcName)
			g.Expect(ps).NotTo(gomega.BeNil())
			g.Expect(ps.Phase).To(gomega.Equal(v1beta1.PlacementPhaseAdmitting))
			g.Expect(ps.Candidates).To(gomega.HaveLen(2))
			for _, c := range ps.Candidates {
				g.Expect(c.Phase).To(gomega.Equal(v1beta1.CandidatePhaseAdmitting))
			}
		}, eventuallyTimeout, eventuallyPoll).Should(gomega.Succeed())

		ginkgo.By("asserting no home is declared Placed while everything stays gated")
		gomega.Consistently(func(g gomega.Gomega) {
			ps := placementStatusOf(isvcNS, isvcName)
			g.Expect(ps).NotTo(gomega.BeNil())
			g.Expect(ps.Phase).NotTo(gomega.Equal(v1beta1.PlacementPhasePlaced))
			g.Expect(derivedEngineMin(member1Cluster)).To(gomega.Equal(2))
			g.Expect(derivedEngineMin(member2Cluster)).To(gomega.Equal(2))
		}, "1.5s", eventuallyPoll).Should(gomega.Succeed())
	})
})

// splitSourceISVC builds a control-plane source InferenceService that declares
// its candidacy through the structured spec.placement block: Split mode, the
// capability requirements selector, and the given Split tuning (nil keeps the
// Split defaults). A single Engine component is the replica-scaled unit.
func splitSourceISVC(namespace, name, requirements string, split *v1beta1.SplitSpec) *v1beta1.InferenceService {
	return &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec: v1beta1.InferenceServiceSpec{
			Placement: &v1beta1.PlacementSpec{
				Mode:         v1beta1.PlacementModeSplit,
				Requirements: requirements,
				Split:        split,
			},
			Engine: &v1beta1.EngineSpec{
				Runner: &v1beta1.RunnerSpec{
					Container: corev1.Container{Name: "ome-container", Image: "example/model-server:test"},
				},
			},
		},
	}
}
