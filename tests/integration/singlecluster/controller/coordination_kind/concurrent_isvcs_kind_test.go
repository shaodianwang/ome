package coordination_kind

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/tests/integration/util"
)

// KCG-5 — three independent ISVCs in the same namespace all reach Ready,
// and their per-revision Service selectors don't cross-match across
// ISVCs.
//
// Gap-list item #12. The reconciler's work queue handles per-ISVC
// concurrency, and `watches` already pins that pod events from a
// different ISVC don't enqueue this one. What's NOT pinned: a
// per-revision Service emitted by one ISVC must NOT select pods owned
// by a different ISVC, even when both ISVCs share the same model + CSR
// + namespace (which is realistic for an A/B test or two prod versions
// running side by side). A regression in the per-revision Service
// selector key set would silently route traffic across ISVCs — exactly
// the class of bug a multi-ISVC same-NS test catches but a single-ISVC
// test cannot.
var _ = ginkgo.Describe("behavior — concurrent ISVCs in same namespace", ginkgo.Ordered, func() {
	var ns *corev1.Namespace
	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "concurrent-isvc-kcg5-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("KCG-5 three ISVCs same NS reach Ready; per-revision Services are isolated by ome.io/inferenceservice", func() {
		// Each ISVC gets its own (CBM, CSR, ISVC) triplet so the
		// per-ISVC reconciler streams are fully independent. The
		// fixture's default behaviour (CBM/CSR named after the ISVC) is
		// what we want here.
		names := []string{"concurrent-a", "concurrent-b", "concurrent-c"}
		for _, n := range names {
			fix := newCoordFixture(ns.Name,
				WithISVCName(n),
				WithoutDecoder(),
				WithEngineImage("fake-serving:v1"))
			applyFixture(Default, fix)
			deferFixtureCleanup(fix)
		}

		ginkgo.By("waiting for all three ISVCs to converge to Ready independently")
		for _, n := range names {
			isvcName := n
			Eventually(func(g Gomega) {
				c := freshISVC(g, ns.Name, isvcName)
				cs := c.Status.Components[v1beta1.EngineComponent]
				g.Expect(cs.Lifecycle).NotTo(BeNil(), "ISVC %s missing OMENative status", isvcName)
				g.Expect(cs.Lifecycle.InstanceStatuses).To(HaveLen(1),
					"ISVC %s must produce exactly one engine Instance", isvcName)
				g.Expect(cs.Lifecycle.InstanceStatuses[0].Phase).
					To(Equal(v1beta1.OMENativeInstanceReady),
						"ISVC %s engine Instance must reach Ready", isvcName)
			}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
		}

		ginkgo.By("asserting per-revision Service selectors are ISVC-scoped (no cross-ISVC selection)")
		// Every per-revision Service in this namespace has an
		// ome.io/revision-hash selector key. For the isolation contract
		// we additionally require that EVERY per-revision Service also
		// carries the ome.io/inferenceservice selector key, and that key
		// matches exactly ONE of our three ISVC names.
		svcs := listPerRevisionServices(Default, ns.Name)
		Expect(len(svcs)).To(BeNumerically(">=", len(names)),
			"KCG-5: expected at least one per-revision Service per ISVC; got %d Services for %d ISVCs",
			len(svcs), len(names))

		perISVCSvcCount := map[string]int{}
		for _, svc := range svcs {
			isvcLabel, hasISVC := svc.Spec.Selector[constants.InferenceServicePodLabelKey]
			Expect(hasISVC).To(BeTrue(),
				"KCG-5: per-revision Service %s missing %s selector key — would cross-match ISVCs",
				svc.Name, constants.InferenceServicePodLabelKey)
			perISVCSvcCount[isvcLabel]++
		}
		for _, n := range names {
			Expect(perISVCSvcCount[n]).To(BeNumerically(">=", 1),
				"KCG-5: ISVC %s must own at least one per-revision Service; perISVCSvcCount=%v",
				n, perISVCSvcCount)
		}

		ginkgo.By("asserting pods carry the matching ome.io/inferenceservice label so the per-rev selector lands on the right pod set")
		// Cross-check the other half: each ISVC's pods must carry the
		// ome.io/inferenceservice label set to that ISVC's name and
		// NOTHING ELSE — otherwise a Service selector that's correct in
		// shape could still match the wrong pods. listOMENativePods
		// already filters on this label, so each per-ISVC call returns
		// only that ISVC's pods.
		for _, n := range names {
			pods := listOMENativePods(Default, ns.Name, n)
			Expect(pods).NotTo(BeEmpty(),
				"KCG-5: ISVC %s must have at least one OMENative-managed pod", n)
			for _, pod := range pods {
				Expect(pod.Labels[constants.InferenceServicePodLabelKey]).To(Equal(n),
					"KCG-5: pod %s/%s carries %s=%q, expected %q",
					pod.Namespace, pod.Name,
					constants.InferenceServicePodLabelKey,
					pod.Labels[constants.InferenceServicePodLabelKey], n)
			}
		}
	})
})
