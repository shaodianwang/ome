package coordination

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/coordination"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/query"
	"github.com/ome-projects/ome/tests/integration/util"
)

// Status.Components.<c>.Traffic[] producer specs for OEP-0007.1.
// coordination.Reconcile observes per-revision pod counts and writes
// `Status.Components.<c>.Traffic[]` plus the matching
// `LatestReadyRevision` field. The HTTPRoute weighted-backendRef
// consumer (OEP-0011 owned) reads this slice to populate route
// backends; the producer side is OEP-0007.1's responsibility.
//
// All Service-emitting / pod-count-observing T-* tests share the same
// G1 blocker as the SVC-* family: coordination.Reconcile only runs
// after every per-Component reconciler returns without requeue, but
// during a rollout Components requeue every pass. So even though the
// producer is opt-out (no rolloutCoordination block required), the
// Traffic[] writes don't actually happen end-to-end yet.
//
// The pure-function T-3 / T-7 / T-8 / T-16 / T-18 / T-19 / T-20 cases
// exercise coordination.SortedWeights / NormalizeWeights /
// ComputeWeightsFromPods / TrafficDiffersMeaningfully directly — they
// don't go through the controller and aren't blocked by G1.

var _ = ginkgo.Describe("Coordination Status.Components.<c>.Traffic[] producer (OEP-0007.1)", ginkgo.Ordered, func() {
	var ns *corev1.Namespace
	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "coord-traffic-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	// ----------------------------------------------------------------
	// Producer end-to-end tests (G1-blocked PIts)
	// ----------------------------------------------------------------

	ginkgo.PIt("G1: T-1 all pods on one revision → Traffic = [{rev, 100%, LatestRevision=true}]", func() {
		fix := newCoordFixture(ns.Name, WithISVCName("t1"), WithoutDecoder())
		applyFixture(Default, fix)
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.ISVC) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CBM) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CSR) })

		Eventually(func(g Gomega) {
			isvc := freshISVC(g, ns.Name, "t1")
			traffic := trafficForComponent(isvc, v1beta1.EngineComponent)
			g.Expect(traffic).To(HaveLen(1), "expected single Traffic entry; got %s", describeTraffic(traffic))
			g.Expect(traffic[0].Percent).To(Equal(int32(100)))
			g.Expect(traffic[0].LatestRevision).To(BeTrue())
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.PIt("G1: T-2 50/50 pod split → two entries summing to 100", func() {
		// Drives a rolling image change to land both old + new revisions
		// briefly; producer should show both at ~50%.
		fix := newCoordFixture(ns.Name, WithISVCName("t2"), WithoutDecoder(),
			WithEngineImage("ghcr.io/test/runtime:v1"))
		applyFixture(Default, fix)
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.ISVC) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CBM) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CSR) })

		// Bump image to v2.
		fresh := &v1beta1.InferenceService{}
		Eventually(func(g Gomega) {
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: ns.Name, Name: "t2"}, fresh)).To(Succeed())
		}).Should(Succeed())
		fresh.Spec.Engine.PodSpec.Containers[0].Image = "ghcr.io/test/runtime:v2"
		Expect(k8sClient.Update(suiteCtx, fresh)).To(Succeed())

		// Window where both versions coexist in Traffic[].
		Eventually(func(g Gomega) {
			isvc := freshISVC(g, ns.Name, "t2")
			traffic := trafficForComponent(isvc, v1beta1.EngineComponent)
			g.Expect(traffic).To(HaveLen(2), "expected 2 Traffic entries mid-rollout; got %s", describeTraffic(traffic))
			sum := int32(0)
			for _, t := range traffic {
				sum += t.Percent
			}
			g.Expect(sum).To(Equal(int32(100)), "weights must sum to 100")
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.PIt("G1: T-4 LatestRevision tag tracks the target hash", func() {
		fix := newCoordFixture(ns.Name, WithISVCName("t4"), WithoutDecoder())
		applyFixture(Default, fix)
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.ISVC) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CBM) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CSR) })

		Eventually(func(g Gomega) {
			isvc := freshISVC(g, ns.Name, "t4")
			traffic := trafficForComponent(isvc, v1beta1.EngineComponent)
			g.Expect(traffic).NotTo(BeEmpty())
			anyLatest := false
			for _, t := range traffic {
				if t.LatestRevision {
					anyLatest = true
				}
			}
			g.Expect(anyLatest).To(BeTrue(), "at least one Traffic entry must carry LatestRevision=true")
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.PIt("G1: T-5 LatestReadyRevision = <isvc>-<c>-rev-<hash>", func() {
		fix := newCoordFixture(ns.Name, WithISVCName("t5"), WithoutDecoder())
		applyFixture(Default, fix)
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.ISVC) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CBM) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CSR) })

		Eventually(func(g Gomega) {
			isvc := freshISVC(g, ns.Name, "t5")
			cs, ok := isvc.Status.Components[v1beta1.EngineComponent]
			g.Expect(ok).To(BeTrue())
			g.Expect(cs.LatestReadyRevision).To(MatchRegexp(`^t5-engine-rev-[a-z0-9]+$`),
				"LatestReadyRevision should be a per-revision Service name; got %q",
				cs.LatestReadyRevision)
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.PIt("G1: T-6 empty pod set → Traffic = nil", func() {
		// Create an ISVC with replicas=0 (or wait before pods exist).
		// Easiest: assert on first reconcile-cycle window before pods reach
		// Ready.
		fix := newCoordFixture(ns.Name, WithISVCName("t6"), WithoutDecoder())
		applyFixture(Default, fix)
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.ISVC) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CBM) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CSR) })

		Eventually(func(g Gomega) {
			isvc := freshISVC(g, ns.Name, "t6")
			cs, ok := isvc.Status.Components[v1beta1.EngineComponent]
			g.Expect(ok).To(BeTrue())
			// Either nil or empty — both indicate no live pods bucketed.
			g.Expect(cs.Traffic).To(BeEmpty())
		}).WithTimeout(util.ShortTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.PIt("G1: T-9 pods missing revision-hash label excluded from weights", func() {
		fix := newCoordFixture(ns.Name, WithISVCName("t9"), WithoutDecoder())
		applyFixture(Default, fix)
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.ISVC) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CBM) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CSR) })

		// Pods with revision-hash should be in Traffic. Pods without (manual
		// orphan) shouldn't shift weights. This requires producer side to
		// be running first; G1 unblocks.
		Eventually(func(g Gomega) {
			isvc := freshISVC(g, ns.Name, "t9")
			traffic := trafficForComponent(isvc, v1beta1.EngineComponent)
			g.Expect(traffic).NotTo(BeEmpty())
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.PIt("G1: T-10 multi-Component ISVC: each Component has independent Traffic[]", func() {
		fix := newCoordFixture(ns.Name, WithISVCName("t10")) // engine + decoder default
		applyFixture(Default, fix)
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.ISVC) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CBM) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CSR) })

		Eventually(func(g Gomega) {
			isvc := freshISVC(g, ns.Name, "t10")
			eng := trafficForComponent(isvc, v1beta1.EngineComponent)
			dec := trafficForComponent(isvc, v1beta1.DecoderComponent)
			g.Expect(eng).NotTo(BeEmpty(), "engine traffic empty: %s", describeTraffic(eng))
			g.Expect(dec).NotTo(BeEmpty(), "decoder traffic empty: %s", describeTraffic(dec))
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.PIt("G1: T-11 Traffic updates DURING a rollout, not just after convergence", func() {
		// The canonical G1 test. With requeue early-return blocking Step 6a,
		// Traffic[] never updates while the rollout is in flight.
		fix := newCoordFixture(ns.Name, WithISVCName("t11"), WithoutDecoder(),
			WithEngineImage("ghcr.io/test/runtime:v1"))
		applyFixture(Default, fix)
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.ISVC) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CBM) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CSR) })

		// Capture v1 hash from Traffic[].
		var v1Hash string
		Eventually(func(g Gomega) {
			isvc := freshISVC(g, ns.Name, "t11")
			traffic := trafficForComponent(isvc, v1beta1.EngineComponent)
			g.Expect(traffic).To(HaveLen(1))
			v1Hash = revisionHashFromServiceName(traffic[0].RevisionName)
			g.Expect(v1Hash).NotTo(BeEmpty())
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		// Bump to v2.
		fresh := &v1beta1.InferenceService{}
		Expect(k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: ns.Name, Name: "t11"}, fresh)).To(Succeed())
		fresh.Spec.Engine.PodSpec.Containers[0].Image = "ghcr.io/test/runtime:v2"
		Expect(k8sClient.Update(suiteCtx, fresh)).To(Succeed())

		// During the rollout (Component requeues every cycle), Traffic[]
		// MUST reflect the v2 hash showing up. The G1 bug freezes Traffic[]
		// at v1-only until the Component stops requeueing.
		Eventually(func(g Gomega) {
			isvc := freshISVC(g, ns.Name, "t11")
			traffic := trafficForComponent(isvc, v1beta1.EngineComponent)
			hashes := map[string]bool{}
			for _, t := range traffic {
				hashes[revisionHashFromServiceName(t.RevisionName)] = true
			}
			g.Expect(len(hashes)).To(BeNumerically(">=", 2),
				"Traffic[] must include both v1 (%s) and v2 hashes mid-rollout; got %s",
				v1Hash, describeTraffic(traffic))
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.PIt("G1: T-12 RevisionName matches the per-revision Service name exactly", func() {
		fix := newCoordFixture(ns.Name, WithISVCName("t12"), WithoutDecoder())
		applyFixture(Default, fix)
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.ISVC) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CBM) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CSR) })

		Eventually(func(g Gomega) {
			isvc := freshISVC(g, ns.Name, "t12")
			traffic := trafficForComponent(isvc, v1beta1.EngineComponent)
			g.Expect(traffic).NotTo(BeEmpty())
			// RevisionName format: <isvc>-<component>-rev-<hash>
			for _, t := range traffic {
				g.Expect(t.RevisionName).To(MatchRegexp(`^t12-engine-rev-[a-z0-9]+$`),
					"RevisionName must equal PerRevisionServiceName format; got %q",
					t.RevisionName)
				// The Service should also exist with that name.
				svc := &corev1.Service{}
				g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: ns.Name, Name: t.RevisionName}, svc)).
					To(Succeed(), "Service %q must exist for the consumer to use as backendRef", t.RevisionName)
			}
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	// ----------------------------------------------------------------
	// Pure-function producer tests (NOT blocked by G1)
	// ----------------------------------------------------------------

	ginkgo.It("T-3 SortedWeights sorts by descending Percent, tie-break by hash", func() {
		weights := []coordination.RevisionWeight{
			{RevisionHash: "ccc", Percent: 30},
			{RevisionHash: "bbb", Percent: 50},
			{RevisionHash: "aaa", Percent: 50},
			{RevisionHash: "ddd", Percent: 20},
		}
		sorted := coordination.SortedWeights(weights)
		Expect(sorted).To(HaveLen(4))
		Expect(sorted[0].Percent).To(Equal(int32(50)))
		Expect(sorted[1].Percent).To(Equal(int32(50)))
		Expect(sorted[0].RevisionHash).To(Equal("aaa"), "tied weights sort by hash asc")
		Expect(sorted[1].RevisionHash).To(Equal("bbb"))
		Expect(sorted[2].Percent).To(Equal(int32(30)))
		Expect(sorted[3].Percent).To(Equal(int32(20)))
	})

	ginkgo.It("T-7 TrafficDiffersMeaningfully no-op when traffic unchanged", func() {
		a := []v1beta1.ComponentTrafficTarget{
			{RevisionName: "x", Percent: 100, LatestRevision: true, Tag: "foo"},
		}
		b := []v1beta1.ComponentTrafficTarget{
			{RevisionName: "x", Percent: 100, LatestRevision: false, Tag: "bar"}, // cosmetic-only differs
		}
		Expect(coordination.TrafficDiffersMeaningfully(a, b)).To(BeFalse(),
			"cosmetic Tag/LatestRevision changes shouldn't trigger a write")

		// Real percent change DOES count as meaningful.
		c := []v1beta1.ComponentTrafficTarget{
			{RevisionName: "x", Percent: 50, LatestRevision: false},
		}
		Expect(coordination.TrafficDiffersMeaningfully(a, c)).To(BeTrue())

		// Different set of RevisionNames is meaningful.
		d := []v1beta1.ComponentTrafficTarget{
			{RevisionName: "y", Percent: 100, LatestRevision: false},
		}
		Expect(coordination.TrafficDiffersMeaningfully(a, d)).To(BeTrue())
	})

	ginkgo.It("T-8 NormalizeWeights pushes 3-way 33/33/33 to sum=100", func() {
		// Three weights of 33% each = 99% total. NormalizeWeights bumps
		// the largest entry by the residual (1 in this case).
		in := []coordination.RevisionWeight{
			{RevisionHash: "a", Percent: 33},
			{RevisionHash: "b", Percent: 33},
			{RevisionHash: "c", Percent: 33},
		}
		out := coordination.NormalizeWeights(in)
		Expect(out).To(HaveLen(3))
		sum := int32(0)
		for _, w := range out {
			sum += w.Percent
		}
		Expect(sum).To(Equal(int32(100)))
	})

	ginkgo.It("T-16 ComputeWeightsFromPods tie-broken by hash on equal Percent", func() {
		perRev := map[string]int32{
			"aaa": 1,
			"bbb": 1,
		}
		weights := coordination.ComputeWeightsFromPods(perRev, 2, "aaa")
		Expect(weights).To(HaveLen(2))
		// SortedWeights ties → hash asc → aaa first.
		Expect(weights[0].RevisionHash).To(Equal("aaa"))
		Expect(weights[0].LatestRevision).To(BeTrue())
		Expect(weights[1].RevisionHash).To(Equal("bbb"))
		Expect(weights[1].LatestRevision).To(BeFalse())
	})

	ginkgo.It("T-17 ComputeWeightsFromPods excludes zero-count revisions", func() {
		perRev := map[string]int32{
			"aaa": 0, // race during scale-down
			"bbb": 2,
		}
		weights := coordination.ComputeWeightsFromPods(perRev, 2, "bbb")
		Expect(weights).To(HaveLen(1))
		Expect(weights[0].RevisionHash).To(Equal("bbb"))
		Expect(weights[0].Percent).To(Equal(int32(100)))
	})

	ginkgo.It("T-18 ComputeWeightsFromPods single pod → 100% on its hash", func() {
		perRev := map[string]int32{"abc": 1}
		weights := coordination.ComputeWeightsFromPods(perRev, 1, "abc")
		Expect(weights).To(HaveLen(1))
		Expect(weights[0].Percent).To(Equal(int32(100)))
	})

	ginkgo.It("T-19 ComputeWeightsFromPods 7-pod uneven set normalizes to sum=100", func() {
		// 7 distinct hashes, 1 pod each → 14% × 7 = 98. NormalizeWeights
		// gives the residual to the entry that wins the SortedWeights
		// tie-break.
		perRev := map[string]int32{
			"a": 1, "b": 1, "c": 1, "d": 1, "e": 1, "f": 1, "g": 1,
		}
		weights := coordination.ComputeWeightsFromPods(perRev, 7, "a")
		Expect(weights).To(HaveLen(7))
		sum := int32(0)
		for _, w := range weights {
			sum += w.Percent
		}
		Expect(sum).To(Equal(int32(100)))
	})

	ginkgo.It("T-20 TrafficDiffersMeaningfully comparison covers length-mismatch", func() {
		a := []v1beta1.ComponentTrafficTarget{{RevisionName: "x", Percent: 100}}
		b := []v1beta1.ComponentTrafficTarget{
			{RevisionName: "x", Percent: 50},
			{RevisionName: "y", Percent: 50},
		}
		Expect(coordination.TrafficDiffersMeaningfully(a, b)).To(BeTrue(),
			"different-length slices must be flagged as meaningful")
	})
})

// revisionHashFromServiceName extracts the hash suffix from a
// per-revision Service name `<isvc>-<c>-rev-<hash>` — used by the
// producer-side tests that compare RevisionName entries against hashes
// observed on pods. Mirrors query.RevisionHashFromControllerRevisionName
// but for the Service form.
func revisionHashFromServiceName(name string) string {
	// Format: <anything>-rev-<hash>. Walk forward looking for "-rev-".
	for i := 0; i+len("-rev-") <= len(name); i++ {
		if name[i:i+len("-rev-")] == "-rev-" {
			return name[i+len("-rev-"):]
		}
	}
	return query.RevisionHashFromControllerRevisionName(name)
}
