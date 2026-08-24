package scaleout_kind

import (
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/types"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/tests/integration/multicluster"
)

// Cross-cluster ownership: which objects on a member the control plane is
// allowed to overwrite or delete. These are the guards against the worst
// failure mode in the design — deleting someone else's workload by identity —
// and on a real cluster the stakes are visible: the object under threat has
// pods a kubelet is running.
var _ = ginkgo.Describe("Multi-cluster ownership (KIND)", func() {
	var env *specEnv

	ginkgo.BeforeEach(func() {
		env = setupSpecEnv()
	})

	ginkgo.It("never overwrites or deletes a member-owned ISVC that shares the placed name", func() {
		model, runtime := env.seedModelRuntimeOnMembers()

		// A user creates their own ISVC directly on a member, at exactly the
		// name/namespace the placer is about to derive to. It carries no origin
		// markers, so it is not ours: the placer must neither adopt it (overwrite
		// its spec) nor sweep it (delete it as a loser).
		squatter := env.members[1]
		ginkgo.By("creating a member-owned ISVC on " + squatter.wcName + " at the name the placer will use")
		own := env.createMemberOwnedISVC(squatter, model, runtime)

		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(runningEnginePodsOn(g, squatter.c, env.isvcNS, env.isvcName)).
				To(gomega.BeNumerically(">=", 1))
		}).WithTimeout(longTimeout).WithPolling(pollInterval).Should(gomega.Succeed(),
			"the member-owned workload should be running before the placer arrives")

		podNames := func() []string {
			var out []string
			for _, p := range enginePodsOn(gomega.Default, squatter.c, env.isvcNS, env.isvcName) {
				out = append(out, p.Name)
			}
			return out
		}
		before := podNames()

		ginkgo.By("placing the source, whose derived would collide with the member-owned ISVC")
		env.createSource(model, runtime, v1beta1.PlacementModeSingle, 1)

		// The collision makes the fan-out to this member fail (tolerated per
		// cluster), so the other member is the only viable home.
		ginkgo.By("asserting the placement lands on the OTHER member")
		winner := env.awaitWinner()
		gomega.Expect(winner.wcName).To(gomega.Equal(env.other(squatter).wcName),
			"the placer must not take over the member-owned name")

		ginkgo.By("asserting the member-owned ISVC survives, unadopted and unswept")
		gomega.Consistently(func(g gomega.Gomega) {
			cur := &v1beta1.InferenceService{}
			g.Expect(squatter.c.client.Get(squatter.c.ctx,
				types.NamespacedName{Namespace: env.isvcNS, Name: env.isvcName}, cur)).To(gomega.Succeed(),
				"the member-owned ISVC must not be deleted")
			g.Expect(cur.DeletionTimestamp).To(gomega.BeNil(), "it must not even be marked for deletion")
			g.Expect(multicluster.OriginUID(cur)).To(gomega.BeEmpty(),
				"the placer must not stamp its origin markers onto a foreign object")
			g.Expect(cur.UID).To(gomega.Equal(own.UID), "it must be the same object, not a replacement")
		}).WithTimeout(10 * time.Second).WithPolling(time.Second).Should(gomega.Succeed())

		ginkgo.By("asserting its pods were never disturbed")
		gomega.Expect(podNames()).To(gomega.ConsistOf(before),
			"the member-owned workload's pods must be untouched")
	})

	ginkgo.It("reaps its own orphaned derived and its pods, leaving another control plane's alone", func() {
		model, runtime := env.seedModelRuntimeOnMembers()

		// The sweep treats an empty live-ISVC list as suspect and skips, so a
		// surviving unrelated source must exist for the GC to run at all.
		keeper := env.createKeeperISVC()
		ginkgo.DeferCleanup(func() { deleteIfExists(controlCluster, keeper) })

		host := env.members[0]
		orphanName := uniqueName("orphan")
		foreignName := uniqueName("foreign")

		ginkgo.By("planting an orphan of THIS control plane and a derived of another one")
		env.createDerivedLookalike(host, orphanName, model, runtime, controlPlaneID, uniqueName("dead-uid"))
		env.createDerivedLookalike(host, foreignName, model, runtime, "some-other-control-plane", uniqueName("dead-uid"))

		ginkgo.By("waiting for both to be running, so the reap has real pods to remove")
		for _, name := range []string{orphanName, foreignName} {
			gomega.Eventually(func(g gomega.Gomega) {
				g.Expect(runningEnginePodsOn(g, host.c, env.isvcNS, name)).
					To(gomega.BeNumerically(">=", 1))
			}).WithTimeout(longTimeout).WithPolling(pollInterval).Should(gomega.Succeed(),
				"%s should be running before the sweep", name)
		}

		ginkgo.By("asserting the GC reaps our orphan and its pods")
		gomega.Eventually(func() bool {
			_, ok := getDerivedOn(host.c, env.isvcNS, orphanName)
			return ok
		}).WithTimeout(gcInterval+longTimeout).WithPolling(pollInterval).Should(gomega.BeFalse(),
			"an orphan stamped with our control-plane ID should be reaped")
		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(enginePodsOn(g, host.c, env.isvcNS, orphanName)).To(gomega.BeEmpty())
		}).WithTimeout(longTimeout).WithPolling(pollInterval).Should(gomega.Succeed(),
			"reaping the orphan must take its pods with it")

		ginkgo.By("asserting the other control plane's derived is left completely alone")
		gomega.Consistently(func(g gomega.Gomega) {
			_, ok := getDerivedOn(host.c, env.isvcNS, foreignName)
			g.Expect(ok).To(gomega.BeTrue(), "another control plane's derived must never be reaped")
			g.Expect(runningEnginePodsOn(g, host.c, env.isvcNS, foreignName)).To(gomega.BeNumerically(">=", 1),
				"its pods must keep running")
		}).WithTimeout(10 * time.Second).WithPolling(time.Second).Should(gomega.Succeed())
	})
})
