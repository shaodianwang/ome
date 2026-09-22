package multicluster

import (
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/placement"
	ometesting "github.com/ome-projects/ome/pkg/utils/testing/v1beta1"
	"github.com/ome-projects/ome/tests/integration/util"
)

// These specs (S8-S11) drive the single-mode placement lifecycle beyond the
// happy race, end-to-end across the three envtest apiservers on the sim path:
// the winner-lost grace window (hold, then re-race), admission loss (demote to
// Admitting, re-place onto the surviving member, sweep the former winner), terminal
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

	ginkgo.It("S8 winner-lost grace: holds the placement within the window, re-races only after it elapses", func() {
		placeAndWinOn(member1Cluster, wc1Name)

		ginkgo.By("deleting the winner's derived out from under the control plane")
		derived, ok := getDerivedOn(member1Cluster, isvcNS, isvcName)
		gomega.Expect(ok).To(gomega.BeTrue())
		gomega.Expect(member1Cluster.client.Delete(member1Cluster.ctx, derived)).To(gomega.Succeed())

		// The grace clock arms when the placer first observes the derived absent
		// on the connected winner, which is at or after the delete — so a window
		// strictly shorter than winnerLostGrace, measured from the delete, must
		// see the placement held and no re-fan-out anywhere.
		ginkgo.By("asserting the placement holds and nothing is re-fanned-out within the grace window")
		gomega.Consistently(func(g gomega.Gomega) {
			ps := placementStatusOf(isvcNS, isvcName)
			g.Expect(ps).NotTo(gomega.BeNil())
			g.Expect(ps.Phase).To(gomega.Equal(v1beta1.PlacementPhasePlaced))
			g.Expect(ps.Cluster).To(gomega.Equal(wc1Name))
			_, ok1 := getDerivedOn(member1Cluster, isvcNS, isvcName)
			g.Expect(ok1).To(gomega.BeFalse(), "the placer must not re-apply the derived within the grace window")
			_, ok2 := getDerivedOn(member2Cluster, isvcNS, isvcName)
			g.Expect(ok2).To(gomega.BeFalse(), "the placer must not fan out to another candidate within the grace window")
		}, "1s", eventuallyPoll).Should(gomega.Succeed())

		// After the grace window the placer re-races: the derived reappears on the
		// candidates, member1's still-admitted engine wins again, and the loser
		// copy on member2 is swept.
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

	ginkgo.It("S9 lost admission: demotes to Admitting after the grace window and re-places onto the surviving member", func() {
		placeAndWinOn(member1Cluster, wc1Name)

		// Every instance loses its Kueue admission (the preemption shape): the
		// derived object still exists, so only the admission signal is gone.
		ginkgo.By("revoking the winner's admission (instances present but gated)")
		upsertEngineIRStatusOn(member1Cluster, isvcNS, isvcName, []v1beta1.OMENativeInstanceStatus{
			{Index: 0, Phase: v1beta1.OMENativeInstancePending, Admitted: false},
		})

		ginkgo.By("asserting the placement rides out the dip within the grace window")
		gomega.Consistently(func(g gomega.Gomega) {
			ps := placementStatusOf(isvcNS, isvcName)
			g.Expect(ps).NotTo(gomega.BeNil())
			g.Expect(ps.Phase).To(gomega.Equal(v1beta1.PlacementPhasePlaced))
			g.Expect(ps.Cluster).To(gomega.Equal(wc1Name))
		}, "1s", eventuallyPoll).Should(gomega.Succeed())

		ginkgo.By("asserting the placement demotes to Admitting once the window elapses")
		gomega.Eventually(func() v1beta1.PlacementPhase {
			ps := placementStatusOf(isvcNS, isvcName)
			if ps == nil {
				return ""
			}
			return ps.Phase
		}, eventuallyTimeout, eventuallyPoll).Should(gomega.Equal(v1beta1.PlacementPhaseAdmitting))

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

// upsertEngineIRStatusOn creates (if needed) the engine-component
// InferenceReplica for a derived ISVC on a member and sets its InstanceStatuses
// wholesale — the authoritative per-instance admission/phase signal the
// control-plane placer reads. The sim path has no member reconciler, so the
// suite plays the worker's role here (a generalization of admitDerivedOn for
// arbitrary instance sets).
func upsertEngineIRStatusOn(member cluster, namespace, name string, instances []v1beta1.OMENativeInstanceStatus) {
	irName := name + "-" + string(v1beta1.EngineComponent)
	gomega.EventuallyWithOffset(1, func(g gomega.Gomega) {
		ir := &v1beta1.InferenceReplica{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: irName},
			// Minimal valid spec so the Create passes CRD schema validation on
			// bare members (the sim path has no manager to pre-create the IR).
			Spec: v1beta1.InferenceReplicaSpec{
				ParentRef: v1beta1.ParentReference{Name: name},
				Component: v1beta1.EngineComponent,
				Runners: []v1beta1.Runner{{
					Name: v1beta1.RunnerNameDefault,
					Size: 1,
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: "sim:latest"}}},
					},
				}},
			},
		}
		g.Expect(client.IgnoreAlreadyExists(member.client.Create(member.ctx, ir))).To(gomega.Succeed())
		g.Expect(member.client.Get(member.ctx, types.NamespacedName{Namespace: namespace, Name: irName}, ir)).To(gomega.Succeed())
		g.Expect(util.WriteDenseInstanceStatuses(member.ctx, member.client, ir, instances)).To(gomega.Succeed())
	}, eventuallyTimeout, eventuallyPoll).Should(gomega.Succeed())
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
