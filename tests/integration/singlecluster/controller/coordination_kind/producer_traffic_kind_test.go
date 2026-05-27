package coordination_kind

import (
	"sync"
	"time"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/tests/integration/util"
)

// Status.Components.<c>.Traffic[] population.
//
// Pure-function tests (SortedWeights, TrafficDiffersMeaningfully,
// NormalizeWeights, ComputeWeightsFromPods) are covered by
// pkg/.../coordination/traffic_test.go. This file pins what the
// producer actually writes to Status during steady state and
// mid-rollout:
//
//   - TRAFFIC-1 steady: single revision → one entry, Percent=100,
//     LatestRevision=true, RevisionName matches the per-revision
//     Service name format.
//   - TRAFFIC-2 mid-bump: image v1 → v2 produces a window where
//     Traffic[] carries TWO entries (v1 dropping, v2 rising). KIND's
//     fake-serving v1/v2 images have distinct content hashes so the
//     revision rolls cleanly and the two-entry window is observable.
var _ = ginkgo.Describe("TRAFFIC — Status.Components.<c>.Traffic[] producer", ginkgo.Ordered, func() {
	var ns *corev1.Namespace
	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "traffic-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("TRAFFIC-1 single revision converges to one Traffic entry at 100% with LatestRevision=true", func() {
		fix := newCoordFixture(ns.Name, WithISVCName("traffic1"), WithoutDecoder(),
			WithEngineImage("fake-serving:v1"))
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "traffic1")
			engCS := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(engCS.OMENative).NotTo(BeNil())
			g.Expect(engCS.OMENative.ServingReplicas).To(Equal(int32(1)))

			traffic := trafficForComponent(c, v1beta1.EngineComponent)
			g.Expect(traffic).To(HaveLen(1),
				"single-revision steady state must have exactly one Traffic entry; got %d", len(traffic))
			g.Expect(traffic[0].Percent).To(Equal(int32(100)),
				"single entry must carry 100% of traffic")
			g.Expect(traffic[0].LatestRevision).To(BeTrue(),
				"single entry must be tagged LatestRevision=true")
			// RevisionName equals the per-revision Service name; the
			// consumer (HTTPRoute weighted-backendRef) uses it directly.
			g.Expect(traffic[0].RevisionName).To(MatchRegexp(`^traffic1-engine-rev-[a-z0-9]+$`),
				"RevisionName must equal the per-revision routed Service name; got %q", traffic[0].RevisionName)
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})

	ginkgo.It("TRAFFIC-2 image bump v1→v2: Traffic[] enters a 2-entry window before converging back to 1", func() {
		// 2 engine replicas. No coord group declared — single-Component
		// data-plane Traffic[] still emits because the producer runs
		// for every OMENative Component regardless of group declaration
		// (the per-group state machine is the opt-in part). Default
		// SurgeThenDrain on a 2-replica spec goes through a mid-bump
		// window where one v1 pod is still serving while the v2 pod
		// is coming up — the producer must emit a 2-entry Traffic[]
		// during that window.
		//
		// A single-Component RollingUpdate group would fail
		// ValidateGroupShape (requires >= 2 Components); using no group
		// avoids that and still exercises the multi-revision producer.
		fix := newCoordFixture(ns.Name, WithISVCName("traffic2"), WithoutDecoder(),
			WithEngineReplicas(2),
			WithEngineImage("fake-serving:v1"))
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		// Steady state on v1: one entry.
		var v1RevisionName string
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "traffic2")
			engCS := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(engCS.OMENative).NotTo(BeNil())
			g.Expect(engCS.OMENative.ServingReplicas).To(Equal(int32(2)))
			traffic := trafficForComponent(c, v1beta1.EngineComponent)
			g.Expect(traffic).To(HaveLen(1),
				"pre-bump Traffic[] must have exactly one entry; got %d", len(traffic))
			v1RevisionName = traffic[0].RevisionName
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Sampler captures the maximum Traffic length seen during the
		// rollout. Single sample at the converged moment would miss
		// the mid-bump window; the sampler runs on every tick (100ms)
		// from bump → re-converge, so any 2-entry window is captured.
		//
		// Mutex-guarded because samplers run in a background goroutine.
		var (
			mu              sync.Mutex
			maxTrafficLen   int
			sawV2InMidBump  bool
			sawV1InMidBump  bool
			lastSeenEntries []v1beta1.ComponentTrafficTarget
		)
		stop := util.StartSampler(suiteCtx, 100*time.Millisecond, func(g Gomega) {
			c := &v1beta1.InferenceService{}
			err := k8sClient.Get(suiteCtx, namespacedName(ns.Name, "traffic2"), c)
			if err != nil {
				return // ISVC race during DeferCleanup; sampler ignores transient
			}
			traffic := trafficForComponent(c, v1beta1.EngineComponent)
			mu.Lock()
			defer mu.Unlock()
			if len(traffic) > maxTrafficLen {
				maxTrafficLen = len(traffic)
				lastSeenEntries = append([]v1beta1.ComponentTrafficTarget(nil), traffic...)
			}
			for _, t := range traffic {
				if t.RevisionName == v1RevisionName {
					sawV1InMidBump = true
				} else if t.RevisionName != "" {
					sawV2InMidBump = true
				}
			}
		})
		defer stop()

		// Bump the engine image to v2.
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, "traffic2")
			cur.Spec.Engine.PodSpec.Containers[0].Image = "fake-serving:v2"
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Convergence on v2: a single Traffic entry, RevisionName
		// distinct from v1's.
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "traffic2")
			engCS := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(engCS.OMENative).NotTo(BeNil())
			g.Expect(engCS.OMENative.ServingReplicas).To(Equal(int32(2)))

			traffic := trafficForComponent(c, v1beta1.EngineComponent)
			g.Expect(traffic).To(HaveLen(1),
				"post-bump Traffic[] must converge back to one entry; got %d entries: %s",
				len(traffic), describeTraffic(traffic))
			g.Expect(traffic[0].RevisionName).NotTo(Equal(v1RevisionName),
				"post-bump entry must be the v2 revision (RevisionName=%q matches v1)", traffic[0].RevisionName)
			g.Expect(traffic[0].Percent).To(Equal(int32(100)))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
		stop()

		// Sampler verdict: at least one mid-bump tick observed >=2
		// entries, with both v1 + v2 RevisionName present. The exact
		// percentage split is policy-dependent (the apportionment depends
		// on pod counts on each revision), so we don't pin the numbers —
		// only the structural "two revisions visible at once" claim.
		mu.Lock()
		defer mu.Unlock()
		Expect(maxTrafficLen).To(BeNumerically(">=", 2),
			"sampler never observed a 2-entry Traffic[] mid-bump (max seen=%d); last sample=%s; v1RevName=%s",
			maxTrafficLen, describeTraffic(lastSeenEntries), v1RevisionName)
		Expect(sawV1InMidBump).To(BeTrue(),
			"sampler never saw the v1 RevisionName during mid-bump")
		Expect(sawV2InMidBump).To(BeTrue(),
			"sampler never saw a v2 RevisionName during mid-bump")
		// Mid-bump weights still sum to 100% — the producer normalizes
		// even during the rolling transition.
		sumPercent := int32(0)
		for _, t := range lastSeenEntries {
			sumPercent += t.Percent
		}
		Expect(sumPercent).To(Equal(int32(100)),
			"mid-bump weights must sum to 100; got %d (entries=%s)", sumPercent, describeTraffic(lastSeenEntries))
	})
})

// namespacedName is a small adapter so the sampler can read by key
// without importing types.NamespacedName everywhere.
func namespacedName(ns, name string) client.ObjectKey {
	return client.ObjectKey{Namespace: ns, Name: name}
}
