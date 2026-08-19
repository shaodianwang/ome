package omenative_coordination_kind

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/coordination"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	"github.com/ome-projects/ome/tests/integration/util"
)

// Revision identifier contract on Status.Components.<c> against real
// pod truth:
//
//   - LatestReadyRevision and LatestRolledoutRevision carry the
//     per-revision routing Service name (`<isvc>-<component>-rev-<hash>`),
//     and that Service actually exists with the matching revision-hash
//     selector — the HTTPRoute consumer dereferences these names
//     directly, so a name that is not a live Service is a black hole.
//   - Both fields advance across a completed BlueGreen rollout, and the
//     retired revision's routing + headless Service pair is GC'd once no
//     pod carries its hash.
//   - A replica-count change is NOT a revision change: hash, Services,
//     and the Latest fields all hold steady across a scale-up.
//
// SVC-1/SVC-2 cover Service shape and drift recreation for a single
// revision; TRAFFIC-1/2 cover the Traffic[] entries themselves. This
// file pins the revision *identifier* fields and cross-rollout Service
// lifecycle neither of those observes.
var _ = ginkgo.Describe("BLUEGREEN-REV — revision status fields + Service lifecycle", ginkgo.Ordered, func() {
	var ns *corev1.Namespace
	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "bluegree-rev-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	// alivePodHashes returns the revision-hash label of every
	// non-terminating pod of the ISVC, as a set.
	alivePodHashes := func(g Gomega, namespace, isvcName string) map[string]int {
		out := map[string]int{}
		for _, p := range listOMENativePods(g, namespace, isvcName) {
			if p.DeletionTimestamp != nil {
				continue
			}
			out[p.Labels[query.LabelRevisionHash]]++
		}
		return out
	}

	ginkgo.It("Latest revision fields advance across a BlueGreen rollout and the old Service pair is GC'd", func() {
		fix := newCoordFixture(ns.Name, WithISVCName("bluegree-rev"), WithoutDecoder(),
			WithEngineReplicas(2),
			WithBlueGreenGroup(v1beta1.EngineComponent),
			WithEngineImage("fake-serving:v1"))
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		// Steady state on v1: LatestReady == LatestRolledout == the
		// single Traffic entry's RevisionName, and the name is derived
		// from the live pods' revision-hash.
		var v1Name, v1Hash string
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "bluegree-rev")
			g.Expect(c.Status.RolloutCoordination).NotTo(BeNil())
			g.Expect(c.Status.RolloutCoordination.Groups[0].Phase).
				To(Equal(v1beta1.CoordinationPhaseIdle))
			engCS := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(engCS.Lifecycle).NotTo(BeNil())
			g.Expect(engCS.Lifecycle.ServingReplicas).To(Equal(int32(2)))

			hashes := alivePodHashes(g, ns.Name, "bluegree-rev")
			g.Expect(hashes).To(HaveLen(1), "steady state must have exactly one live revision hash")
			for h := range hashes {
				v1Hash = h
			}
			g.Expect(v1Hash).NotTo(BeEmpty())

			traffic := trafficForComponent(c, v1beta1.EngineComponent)
			g.Expect(traffic).To(HaveLen(1))
			g.Expect(traffic[0].Percent).To(Equal(int32(100)))

			wantName := coordination.PerRevisionServiceName("bluegree-rev", v1beta1.EngineComponent, v1Hash)
			g.Expect(engCS.LatestReadyRevision).To(Equal(wantName),
				"LatestReadyRevision must be the per-revision routing Service name for the live hash")
			g.Expect(engCS.LatestRolledoutRevision).To(Equal(wantName),
				"LatestRolledoutRevision must equal LatestReadyRevision once the rollout is complete")
			g.Expect(traffic[0].RevisionName).To(Equal(wantName),
				"the single Traffic entry must reference the same per-revision Service name")
			v1Name = wantName
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// The names in status must dereference to real Services whose
		// selector pins the same revision hash.
		routed := &corev1.Service{}
		Expect(k8sClient.Get(suiteCtx, namespacedName(ns.Name, v1Name), routed)).To(Succeed(),
			"the Service named by LatestRolledoutRevision must exist")
		Expect(routed.Spec.Selector).To(HaveKeyWithValue(query.LabelRevisionHash, v1Hash),
			"routing Service selector must pin the revision hash embedded in its name")
		headless := &corev1.Service{}
		v1HeadlessName := coordination.PerRevisionHeadlessServiceName("bluegree-rev", v1beta1.EngineComponent, v1Hash)
		Expect(k8sClient.Get(suiteCtx, namespacedName(ns.Name, v1HeadlessName), headless)).To(Succeed(),
			"the headless twin of the rolled-out revision must exist")

		// Bump v1 → v2 and let the BlueGreen rollout complete.
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, "bluegree-rev")
			cur.Spec.Engine.PodSpec.Containers[0].Image = "fake-serving:v2"
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		var v2Name, v2Hash string
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "bluegree-rev")
			g.Expect(c.Status.RolloutCoordination.Groups[0].Phase).
				To(Equal(v1beta1.CoordinationPhaseIdle))
			engCS := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(engCS.Lifecycle.ServingReplicas).To(Equal(int32(2)))

			hashes := alivePodHashes(g, ns.Name, "bluegree-rev")
			g.Expect(hashes).To(HaveLen(1), "post-rollout there must be exactly one live revision hash")
			for h := range hashes {
				v2Hash = h
			}
			g.Expect(v2Hash).NotTo(Equal(v1Hash), "the image bump must have produced a new revision hash")

			wantName := coordination.PerRevisionServiceName("bluegree-rev", v1beta1.EngineComponent, v2Hash)
			g.Expect(engCS.LatestRolledoutRevision).To(Equal(wantName),
				"LatestRolledoutRevision must advance to the new revision's Service name")
			g.Expect(engCS.LatestReadyRevision).To(Equal(wantName),
				"LatestReadyRevision must equal LatestRolledoutRevision at completion")
			traffic := trafficForComponent(c, v1beta1.EngineComponent)
			g.Expect(traffic).To(HaveLen(1))
			g.Expect(traffic[0].RevisionName).To(Equal(wantName))
			g.Expect(traffic[0].Percent).To(Equal(int32(100)))
			v2Name = wantName
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// The new pair exists; the retired v1 pair is GC'd once its last
		// pod is gone. Converge on the exact surviving name set so a
		// stale extra Service pair (missed GC) fails loudly.
		v2HeadlessName := coordination.PerRevisionHeadlessServiceName("bluegree-rev", v1beta1.EngineComponent, v2Hash)
		Eventually(func(g Gomega) {
			names := map[string]struct{}{}
			for _, s := range listPerRevisionServices(g, ns.Name) {
				names[s.Name] = struct{}{}
			}
			g.Expect(names).To(HaveKey(v2Name), "new routing Service must exist")
			g.Expect(names).To(HaveKey(v2HeadlessName), "new headless Service must exist")
			g.Expect(names).NotTo(HaveKey(v1Name),
				"retired revision's routing Service must be GC'd after its pods drain")
			g.Expect(names).NotTo(HaveKey(v1HeadlessName),
				"retired revision's headless Service must be GC'd after its pods drain")
			g.Expect(names).To(HaveLen(2),
				"exactly the current revision's Service pair must remain; got %v", names)
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})

	ginkgo.It("a scale-up is not a revision change: hash, Services, and Latest fields hold steady", func() {
		fix := newCoordFixture(ns.Name, WithISVCName("bluegree-scale"), WithoutDecoder(),
			WithEngineReplicas(1),
			WithBlueGreenGroup(v1beta1.EngineComponent),
			WithEngineImage("fake-serving:v1"))
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		var revHash, rolledoutName string
		var crCount int
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "bluegree-scale")
			g.Expect(c.Status.RolloutCoordination).NotTo(BeNil())
			g.Expect(c.Status.RolloutCoordination.Groups[0].Phase).
				To(Equal(v1beta1.CoordinationPhaseIdle))
			engCS := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(engCS.Lifecycle).NotTo(BeNil())
			g.Expect(engCS.Lifecycle.ServingReplicas).To(Equal(int32(1)))

			hashes := alivePodHashes(g, ns.Name, "bluegree-scale")
			g.Expect(hashes).To(HaveLen(1))
			for h := range hashes {
				revHash = h
			}
			g.Expect(revHash).NotTo(BeEmpty())
			rolledoutName = engCS.LatestRolledoutRevision
			g.Expect(rolledoutName).To(Equal(
				coordination.PerRevisionServiceName("bluegree-scale", v1beta1.EngineComponent, revHash)))
			g.Expect(listPerRevisionServices(g, ns.Name)).To(HaveLen(2),
				"steady state must carry exactly one routing + headless pair")
			crCount = len(listOMENativeCRs(g, ns.Name))
			g.Expect(crCount).To(BeNumerically(">=", 1))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Replica-only change: MinReplicas 1 → 2, template untouched.
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, "bluegree-scale")
			two := 2
			cur.Spec.Engine.MinReplicas = &two
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "bluegree-scale")
			engCS := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(engCS.Lifecycle.ServingReplicas).To(Equal(int32(2)),
				"scale-up must converge to 2 serving replicas")

			// Both pods carry the ORIGINAL revision hash — replicas are
			// not part of the revision payload.
			hashes := alivePodHashes(g, ns.Name, "bluegree-scale")
			g.Expect(hashes).To(Equal(map[string]int{revHash: 2}),
				"every pod after the scale-up must carry the pre-scale revision hash")

			g.Expect(engCS.LatestRolledoutRevision).To(Equal(rolledoutName),
				"LatestRolledoutRevision must not move on a replica-only change")
			g.Expect(engCS.LatestReadyRevision).To(Equal(rolledoutName),
				"LatestReadyRevision must not move on a replica-only change")
			traffic := trafficForComponent(c, v1beta1.EngineComponent)
			g.Expect(traffic).To(HaveLen(1))
			g.Expect(traffic[0].RevisionName).To(Equal(rolledoutName))
			g.Expect(traffic[0].Percent).To(Equal(int32(100)))

			g.Expect(listPerRevisionServices(g, ns.Name)).To(HaveLen(2),
				"no new per-revision Service pair may appear on a replica-only change")
			g.Expect(listOMENativeCRs(g, ns.Name)).To(HaveLen(crCount),
				"no new ControllerRevision may be minted on a replica-only change")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})
})
