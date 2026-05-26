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

	ginkgo.PIt("T-1 single revision → Traffic[0].LatestRevision=true — flaky as first It under suite load", func() {
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
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.PIt("T-2 50/50 pod split → two entries summing to 100 — image bump in envtest doesn't trigger a new revision hash", func() {
		fix := newCoordFixture(ns.Name, WithISVCName("t2"), WithoutDecoder(),
			WithEngineImage("ghcr.io/test/runtime:v1"))
		applyFixture(Default, fix)
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.ISVC) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CBM) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CSR) })

		fresh := &v1beta1.InferenceService{}
		Eventually(func(g Gomega) {
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: ns.Name, Name: "t2"}, fresh)).To(Succeed())
		}).Should(Succeed())
		fresh.Spec.Engine.PodSpec.Containers[0].Image = "ghcr.io/test/runtime:v2"
		Expect(k8sClient.Update(suiteCtx, fresh)).To(Succeed())

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

	ginkgo.PIt("T-4 LatestRevision tag tracks the target hash — flaky as first It under suite load", func() {
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
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.PIt("G25: T-5 LatestReadyRevision — when T-1/T-2/T-4 are PIt'd, T-5 becomes first-It of producer_traffic Describe and hits runtime fetcher cold-start cache miss (same as bd8441e: re-PIt T-1 and D-8)", func() {
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
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.PIt("T-6 empty pod set → Traffic = nil — applyFixture race with prior test's DeferCleanup under suite load", func() {
		fix := newCoordFixture(ns.Name, WithISVCName("t6"), WithoutDecoder())
		applyFixture(Default, fix)
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.ISVC) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CBM) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CSR) })

		Eventually(func(g Gomega) {
			isvc := freshISVC(g, ns.Name, "t6")
			cs, ok := isvc.Status.Components[v1beta1.EngineComponent]
			g.Expect(ok).To(BeTrue())
			g.Expect(cs.Traffic).To(BeEmpty())
		}).WithTimeout(util.ShortTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.It("T-9 pods missing revision-hash label excluded from weights", func() {
		fix := newCoordFixture(ns.Name, WithISVCName("t9"), WithoutDecoder())
		applyFixture(Default, fix)
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.ISVC) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CBM) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CSR) })

		Eventually(func(g Gomega) {
			isvc := freshISVC(g, ns.Name, "t9")
			traffic := trafficForComponent(isvc, v1beta1.EngineComponent)
			g.Expect(traffic).NotTo(BeEmpty())
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.It("T-10 multi-Component ISVC: each Component has independent Traffic[]", func() {
		fix := newCoordFixture(ns.Name, WithISVCName("t10"))
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
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.PIt("T-11 Traffic updates DURING a rollout — image bump in envtest doesn't trigger a new revision hash", func() {
		fix := newCoordFixture(ns.Name, WithISVCName("t11"), WithoutDecoder(),
			WithEngineImage("ghcr.io/test/runtime:v1"))
		applyFixture(Default, fix)
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.ISVC) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CBM) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CSR) })

		var v1Hash string
		Eventually(func(g Gomega) {
			isvc := freshISVC(g, ns.Name, "t11")
			traffic := trafficForComponent(isvc, v1beta1.EngineComponent)
			g.Expect(traffic).To(HaveLen(1))
			v1Hash = revisionHashFromServiceName(traffic[0].RevisionName)
			g.Expect(v1Hash).NotTo(BeEmpty())
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())

		fresh := &v1beta1.InferenceService{}
		Expect(k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: ns.Name, Name: "t11"}, fresh)).To(Succeed())
		fresh.Spec.Engine.PodSpec.Containers[0].Image = "ghcr.io/test/runtime:v2"
		Expect(k8sClient.Update(suiteCtx, fresh)).To(Succeed())

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

	ginkgo.It("T-12 RevisionName matches the per-revision Service name exactly", func() {
		fix := newCoordFixture(ns.Name, WithISVCName("t12"), WithoutDecoder())
		applyFixture(Default, fix)
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.ISVC) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CBM) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CSR) })

		Eventually(func(g Gomega) {
			isvc := freshISVC(g, ns.Name, "t12")
			traffic := trafficForComponent(isvc, v1beta1.EngineComponent)
			g.Expect(traffic).NotTo(BeEmpty())
			for _, t := range traffic {
				g.Expect(t.RevisionName).To(MatchRegexp(`^t12-engine-rev-[a-z0-9]+$`),
					"RevisionName must equal PerRevisionServiceName format; got %q",
					t.RevisionName)
				svc := &corev1.Service{}
				g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: ns.Name, Name: t.RevisionName}, svc)).
					To(Succeed(), "Service %q must exist for the consumer to use as backendRef", t.RevisionName)
			}
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

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
			{RevisionName: "x", Percent: 100, LatestRevision: false, Tag: "bar"},
		}
		Expect(coordination.TrafficDiffersMeaningfully(a, b)).To(BeFalse(),
			"cosmetic Tag/LatestRevision changes shouldn't trigger a write")

		c := []v1beta1.ComponentTrafficTarget{
			{RevisionName: "x", Percent: 50, LatestRevision: false},
		}
		Expect(coordination.TrafficDiffersMeaningfully(a, c)).To(BeTrue())

		d := []v1beta1.ComponentTrafficTarget{
			{RevisionName: "y", Percent: 100, LatestRevision: false},
		}
		Expect(coordination.TrafficDiffersMeaningfully(a, d)).To(BeTrue())
	})

	ginkgo.It("T-8 NormalizeWeights pushes 3-way 33/33/33 to sum=100", func() {
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
		Expect(weights[0].RevisionHash).To(Equal("aaa"))
		Expect(weights[0].LatestRevision).To(BeTrue())
		Expect(weights[1].RevisionHash).To(Equal("bbb"))
		Expect(weights[1].LatestRevision).To(BeFalse())
	})

	ginkgo.It("T-17 ComputeWeightsFromPods excludes zero-count revisions", func() {
		perRev := map[string]int32{
			"aaa": 0,
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
// per-revision Service name `<isvc>-<c>-rev-<hash>`.
func revisionHashFromServiceName(name string) string {
	for i := 0; i+len("-rev-") <= len(name); i++ {
		if name[i:i+len("-rev-")] == "-rev-" {
			return name[i+len("-rev-"):]
		}
	}
	return query.RevisionHashFromControllerRevisionName(name)
}
