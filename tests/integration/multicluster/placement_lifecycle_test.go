package multicluster

import (
	"github.com/google/go-cmp/cmp"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/placement"
	ometesting "github.com/ome-projects/ome/pkg/utils/testing/v1beta1"
)

// These specs (S8-S11) drive the single-mode placement lifecycle beyond the
// happy race, end-to-end across the three envtest apiservers on the sim path:
// the winner-lost grace window (report loss, then re-race), admission loss
// (report Admitting, re-place onto the surviving member, sweep the former winner), terminal
// failure (surface Failed, never re-place into a hot loop), and the origin
// guard that keeps the fan-out and the loser sweep away from a user-owned
// same-named InferenceService on a candidate cluster.
var _ = ginkgo.Describe("Multi-cluster placement lifecycle", func() {
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
		secretNS = uniqueName("multiclu-secrets")
		isvcNS = uniqueName("multiclu-workload")
		createNamespace(controlCluster, secretNS)
		createNamespace(controlCluster, isvcNS)
		createNamespace(member1Cluster, isvcNS)
		createNamespace(member2Cluster, isvcNS)

		secret1 = uniqueName("multiclu-kc-m1")
		secret2 = uniqueName("multiclu-kc-m2")
		wc1Name = uniqueName("multiclu-a")
		wc2Name = uniqueName("multiclu-b")
		isvcName = uniqueName("multiclu-svc")

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
		deleteIfExists(controlCluster, sourceISVC(isvcNS, isvcName, requirement()))
		deleteIfExists(controlCluster, WorkloadClusterFor(wc1Name, secretNS, secret1, nil))
		deleteIfExists(controlCluster, WorkloadClusterFor(wc2Name, secretNS, secret2, nil))
	})

	// placeAndWinOn creates the source ISVC, admits it on the given member, and
	// waits until that member is recorded as the Placed winner.
	placeAndWinOn := func(winner cluster, winnerName string) {
		mustCreate(controlCluster, sourceISVC(isvcNS, isvcName, requirement()))
		gomega.Eventually(func() bool {
			_, ok := getDerivedOn(winner, isvcNS, isvcName)
			return ok
		}, eventuallyTimeout, eventuallyPoll).Should(gomega.BeTrue())
		admitDerivedOn(winner, isvcNS, isvcName)
		gomega.Eventually(func(g gomega.Gomega) {
			ps := placementStatusOf(isvcNS, isvcName)
			g.Expect(ps).NotTo(gomega.BeNil())
			g.Expect(ps.Phase).To(gomega.Equal(v1beta1.PlacementPhasePlaced))
			g.Expect(ps.Cluster).To(gomega.Equal(winnerName))
		}, eventuallyTimeout, eventuallyPoll).Should(gomega.Succeed())
	}

	ginkgo.It("S8 winner-lost grace: reports loss within the window, re-races only after it elapses", func() {
		placeAndWinOn(member1Cluster, wc1Name)

		ginkgo.By("deleting the winner's derived out from under the control plane")
		derived, ok := getDerivedOn(member1Cluster, isvcNS, isvcName)
		gomega.Expect(ok).To(gomega.BeTrue())
		gomega.Expect(member1Cluster.client.Delete(member1Cluster.ctx, derived)).To(gomega.Succeed())

		ginkgo.By("asserting the observed absence retains only the winner identity")
		gomega.Eventually(func(g gomega.Gomega) {
			ps := placementStatusOf(isvcNS, isvcName)
			g.Expect(ps).NotTo(gomega.BeNil())
			g.Expect(ps.Phase).To(gomega.Equal(v1beta1.PlacementPhasePending))
			g.Expect(ps.Cluster).To(gomega.Equal(wc1Name))
			g.Expect(ps.Plan).NotTo(gomega.BeNil())
			g.Expect(cmp.Diff(wc1Name, ps.Plan.Winner)).To(gomega.BeEmpty())
			for _, candidate := range ps.Candidates {
				g.Expect(candidate.Allocation).NotTo(gomega.BeNil())
				g.Expect(candidate.Endpoint).To(gomega.BeNil())
				g.Expect(cmp.Diff(int32(0), candidate.ReadyReplicas)).To(gomega.BeEmpty())
				g.Expect(cmp.Diff(int32(0), candidate.AdmittedReplicas)).To(gomega.BeEmpty())
			}
		}, eventuallyTimeout, eventuallyPoll).Should(gomega.Succeed())

		ginkgo.By("collecting the old member inventory before admitting a fresh derived identity")
		gomega.Expect(member1Cluster.client.DeleteAllOf(member1Cluster.ctx, &corev1.Pod{}, client.InNamespace(isvcNS),
			client.MatchingLabels{constants.InferenceServicePodLabelKey: isvcName}, client.GracePeriodSeconds(0))).To(gomega.Succeed())
		gomega.Expect(member1Cluster.client.Delete(member1Cluster.ctx, &v1beta1.InferenceReplica{ObjectMeta: metav1.ObjectMeta{Namespace: isvcNS, Name: isvcName + "-engine"}})).To(gomega.Succeed())
		gomega.Eventually(func() bool {
			_, exists := getDerivedOn(member1Cluster, isvcNS, isvcName)
			return exists
		}, eventuallyTimeout, eventuallyPoll).Should(gomega.BeTrue())
		admitDerivedOn(member1Cluster, isvcNS, isvcName)
		ginkgo.By("asserting the re-race resolves back to the admitted member after the window")
		gomega.Eventually(func(g gomega.Gomega) {
			_, ok := getDerivedOn(member1Cluster, isvcNS, isvcName)
			g.Expect(ok).To(gomega.BeTrue(), "the derived must be re-fanned-out after the grace window")
			ps := placementStatusOf(isvcNS, isvcName)
			g.Expect(ps).NotTo(gomega.BeNil())
			g.Expect(ps.Phase).To(gomega.Equal(v1beta1.PlacementPhasePlaced))
			g.Expect(ps.Cluster).To(gomega.Equal(wc1Name))
		}, eventuallyTimeout, eventuallyPoll).Should(gomega.Succeed())
		gomega.Eventually(func() error {
			return member2Cluster.client.Get(member2Cluster.ctx,
				types.NamespacedName{Namespace: isvcNS, Name: isvcName}, &v1beta1.InferenceService{})
		}, eventuallyTimeout, eventuallyPoll).Should(ometesting.BeNotFoundError(),
			"the loser copy from the re-race fan-out must be swept once the winner re-admits")
	})

	ginkgo.It("S9 lost admission: reports Admitting during grace and re-places onto the surviving member", func() {
		placeAndWinOn(member1Cluster, wc1Name)

		// Every instance loses its Kueue admission (the preemption shape): the
		// derived object still exists, so only the admission signal is gone.
		ginkgo.By("revoking the winner's admission (instances present but gated)")
		upsertEngineIRStatusOn(member1Cluster, isvcNS, isvcName, []v1beta1.OMENativeInstanceStatus{
			{Index: 0, Phase: v1beta1.OMENativeInstancePending, Admitted: false},
		})

		ginkgo.By("asserting the observed admission loss retains only the winner identity")
		gomega.Eventually(func(g gomega.Gomega) {
			ps := placementStatusOf(isvcNS, isvcName)
			g.Expect(ps).NotTo(gomega.BeNil())
			g.Expect(ps.Phase).To(gomega.Equal(v1beta1.PlacementPhaseAdmitting))
			g.Expect(ps.Cluster).To(gomega.Equal(wc1Name))
			g.Expect(ps.Plan).NotTo(gomega.BeNil())
			g.Expect(cmp.Diff(wc1Name, ps.Plan.Winner)).To(gomega.BeEmpty())
			for _, candidate := range ps.Candidates {
				g.Expect(candidate.Allocation).NotTo(gomega.BeNil())
				g.Expect(candidate.Endpoint).To(gomega.BeNil())
				g.Expect(cmp.Diff(int32(0), candidate.ReadyReplicas)).To(gomega.BeEmpty())
				g.Expect(cmp.Diff(int32(0), candidate.AdmittedReplicas)).To(gomega.BeEmpty())
			}
		}, eventuallyTimeout, eventuallyPoll).Should(gomega.Succeed())

		ginkgo.By("admitting the surviving member so it wins the re-race")
		gomega.Eventually(func() bool {
			_, ok := getDerivedOn(member2Cluster, isvcNS, isvcName)
			return ok
		}, eventuallyTimeout, eventuallyPoll).Should(gomega.BeTrue(),
			"the re-race must fan the derived out to the surviving candidate")
		admitDerivedOn(member2Cluster, isvcNS, isvcName)

		gomega.Eventually(func(g gomega.Gomega) {
			ps := placementStatusOf(isvcNS, isvcName)
			g.Expect(ps).NotTo(gomega.BeNil())
			g.Expect(ps.Phase).To(gomega.Equal(v1beta1.PlacementPhasePlaced))
			g.Expect(ps.Cluster).To(gomega.Equal(wc2Name))
		}, eventuallyTimeout, eventuallyPoll).Should(gomega.Succeed())

		ginkgo.By("asserting the former winner's derived is swept as the new loser")
		gomega.Eventually(func() error {
			return member1Cluster.client.Get(member1Cluster.ctx,
				types.NamespacedName{Namespace: isvcNS, Name: isvcName}, &v1beta1.InferenceService{})
		}, eventuallyTimeout, eventuallyPoll).Should(ometesting.BeNotFoundError())
		gomega.Consistently(func() bool {
			_, ok := getDerivedOn(member2Cluster, isvcNS, isvcName)
			return ok
		}, 2*eventuallyPoll, eventuallyPoll).Should(gomega.BeTrue())
	})

	ginkgo.It("S10 terminal failure: surfaces Failed on the winner and never re-places into a fail loop", func() {
		placeAndWinOn(member1Cluster, wc1Name)

		ginkgo.By("escalating the winner's instance to a terminal Failed phase")
		upsertEngineIRStatusOn(member1Cluster, isvcNS, isvcName, []v1beta1.OMENativeInstanceStatus{
			{Index: 0, Phase: v1beta1.OMENativeInstanceFailed, Admitted: false},
		})

		ginkgo.By("asserting the placement surfaces Failed while still naming the failed winner")
		gomega.Eventually(func(g gomega.Gomega) {
			ps := placementStatusOf(isvcNS, isvcName)
			g.Expect(ps).NotTo(gomega.BeNil())
			g.Expect(ps.Phase).To(gomega.Equal(v1beta1.PlacementPhaseFailed))
			g.Expect(ps.Cluster).To(gomega.Equal(wc1Name))
		}, eventuallyTimeout, eventuallyPoll).Should(gomega.Succeed())

		// A terminal failure needs human/spec action: the placer must neither tear
		// the failed derived down nor fan out to another candidate.
		ginkgo.By("asserting Failed is stable: the derived stays put and no re-place happens")
		gomega.Consistently(func(g gomega.Gomega) {
			ps := placementStatusOf(isvcNS, isvcName)
			g.Expect(ps).NotTo(gomega.BeNil())
			g.Expect(ps.Phase).To(gomega.Equal(v1beta1.PlacementPhaseFailed))
			_, ok1 := getDerivedOn(member1Cluster, isvcNS, isvcName)
			g.Expect(ok1).To(gomega.BeTrue(), "the failed winner's derived must not be swept")
			_, ok2 := getDerivedOn(member2Cluster, isvcNS, isvcName)
			g.Expect(ok2).To(gomega.BeFalse(), "a terminally-failed placement must not re-fan-out")
		}, "2.5s", eventuallyPoll).Should(gomega.Succeed())
	})

	ginkgo.It("S11 origin guard: a user-owned same-named ISVC on a candidate is never overwritten or swept", func() {
		const (
			userAnnotationKey   = "example.com/owner"
			userAnnotationValue = "member-user"
			userImage           = "example/user-owned:v1"
		)

		ginkgo.By("pre-creating a user-owned ISVC with the source's name on member2")
		userOwned := &v1beta1.InferenceService{
			ObjectMeta: metav1.ObjectMeta{
				Name:        isvcName,
				Namespace:   isvcNS,
				Annotations: map[string]string{userAnnotationKey: userAnnotationValue},
			},
			Spec: v1beta1.InferenceServiceSpec{
				Engine: &v1beta1.EngineSpec{
					Runner: &v1beta1.RunnerSpec{
						Container: corev1.Container{Name: "ome-container", Image: userImage},
					},
				},
			},
		}
		mustCreate(member2Cluster, userOwned)
		ginkgo.DeferCleanup(func() { deleteIfExists(member2Cluster, userOwned) })

		// The fan-out to member2 must fail closed (the name is taken by a
		// non-derived object), while the race still resolves on member1.
		placeAndWinOn(member1Cluster, wc1Name)

		// The window spans several Placed passes (each re-runs the loser sweep)
		// and multiple GC sweeps, so every deletion/overwrite path gets a chance
		// to misfire before we declare the object untouched.
		ginkgo.By("asserting the user-owned ISVC survives the race, the loser sweep, and the GC untouched")
		gomega.Consistently(func(g gomega.Gomega) {
			got := &v1beta1.InferenceService{}
			g.Expect(member2Cluster.client.Get(member2Cluster.ctx,
				types.NamespacedName{Namespace: isvcNS, Name: isvcName}, got)).To(gomega.Succeed(),
				"the user-owned ISVC must never be deleted by the placer or the GC")
			g.Expect(got.Labels).NotTo(gomega.HaveKey(placement.PlacementOriginLabel),
				"origin markers must never be stamped onto a user-owned object")
			g.Expect(got.Labels).NotTo(gomega.HaveKey(placement.PlacementControlPlaneLabel))
			g.Expect(got.Annotations).To(gomega.HaveKeyWithValue(userAnnotationKey, userAnnotationValue))
			g.Expect(got.Spec.Engine).NotTo(gomega.BeNil())
			g.Expect(got.Spec.Engine.Runner.Container.Image).To(gomega.Equal(userImage),
				"the user-owned spec must not be overwritten by the fan-out apply")
		}, "3s", eventuallyPoll).Should(gomega.Succeed())
	})
})

// upsertEngineIRStatusOn projects the member's policy and materializes the
// instance inventory before publishing simulated status.
func upsertEngineIRStatusOn(member cluster, namespace, name string, instances []v1beta1.OMENativeInstanceStatus) {
	upsertPlannedEngineStatusOn(member, namespace, name, instances)
}

// admittedEngineInstances builds n admitted, Ready engine instance statuses —
// the per-home admitted-replica shape Split mode accounts against its floor.
func admittedEngineInstances(n int) []v1beta1.OMENativeInstanceStatus {
	out := make([]v1beta1.OMENativeInstanceStatus, n)
	for i := range out {
		out[i] = v1beta1.OMENativeInstanceStatus{
			Index:    int32(i),
			Phase:    v1beta1.OMENativeInstanceReady,
			Admitted: true,
		}
	}
	return out
}
