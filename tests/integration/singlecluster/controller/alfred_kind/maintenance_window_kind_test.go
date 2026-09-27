package alfred_kind

import (
	"time"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/tests/integration/util"
)

// closedWindow is a weekly window alfred is outside of for the whole spec: it
// spans the entire UTC weekday two days from now, so a spec that crosses UTC
// midnight is still a full day short of it. Windows are weekly, in UTC, with
// three-letter English day names.
func closedWindow() configMutator {
	day := time.Now().UTC().Add(48 * time.Hour).Weekday().String()[:3]
	return setConfig("maintenanceWindows", []any{
		map[string]any{"days": []any{day}, "start": "00:00", "end": "23:59"},
	})
}

var _ = ginkgo.Describe("alfred maintenance windows", func() {
	var ns *corev1.Namespace

	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "alfred-window-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	// Only the defragmentation policy reads the windows: outside every window
	// it drops its executable findings before they reach arbitration, so no
	// verdict is reported for them, while advisories and emergencies pass.
	// Node health never consults the windows. The spec first shows, in
	// recommend-only mode (where nothing can be dispatched and an executable
	// finding is reported as an advisory), that the layout yields an
	// executable-shaped defragmentation finding for each Instance; then it
	// closes the window in execute mode, where the findings are gone and
	// nothing is dispatched; then it evacuates one of the two workers for
	// health inside that same window.
	ginkgo.It("gate defragmentation but not node-health evacuation", func() {
		a, b := workers[0], workers[1]
		patchAlfredConfig(
			setConfig("mode", "recommend-only"),
			setConfig("policies.defragmentation.enabled", true),
			setConfig("emergencyPendingAgeMinutes", nonEmergencyPendingAgeMinutes),
		)
		left, right, pending := fragmentedPair(ns.Name)
		leftUID := singleLivePod(Default, left, a).UID
		rightUID := singleLivePod(Default, right, b).UID

		// The hint is the other worker alone: the source is excluded and the
		// cordoned worker is no target. The pending pod is far younger than
		// the configured emergency age, so the finding is not an emergency.
		ginkgo.By("with no window configured, each Instance is an executable-shaped defragmentation finding")
		finding := func(from, target string) OmegaMatcher {
			return SatisfyAll(
				HaveField("Policy", defragPolicy),
				HaveField("Reason", fragmentationReason),
				HaveField("Instance", int32(0)),
				HaveField("FromNode", from),
				HaveField("HintTargets", ConsistOf(target)),
				HaveField("Outcome", advisoryOutcome),
				HaveField("AdvisoryReason", recommendOnlyAdvisory),
				HaveField("Emergency", BeFalse()),
			)
		}
		Eventually(func(g Gomega) {
			cycle := recommendations(g)
			g.Expect(cycle.Mode).To(Equal("recommend-only"))
			g.Expect(recsFor(cycle, workloadID(left))).To(ContainElement(finding(a, b)),
				func() string { return evacuationState(ns.Name, left.ISVC.Name) })
			g.Expect(recsFor(cycle, workloadID(right))).To(ContainElement(finding(b, a)),
				func() string { return evacuationState(ns.Name, right.ISVC.Name) })
			for _, f := range []*alfredFixture{left, right} {
				var messages []string
				for _, e := range alfredEventsFor(g, ns.Name, f.ISVC.Name, fragmentationEvent) {
					messages = append(messages, e.Message)
				}
				g.Expect(messages).To(ContainElement(ContainSubstring(recommendOnlyAdvisory)), "events on %s", workloadID(f))
			}
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// The mode and the window change in one document, so no pass runs in
		// execute mode without the window. The findings vanish rather than
		// being withheld: the policy drops them before the engine can report
		// a verdict.
		ginkgo.By("inside the closed window the findings are gone and execute mode dispatches nothing")
		patchAlfredConfig(setConfig("mode", "execute"), closedWindow())
		Eventually(func(g Gomega) {
			cycle := recommendations(g)
			g.Expect(cycle.Mode).To(Equal("execute"))
			for _, f := range []*alfredFixture{left, right} {
				g.Expect(recsFor(cycle, workloadID(f))).To(BeEmpty(),
					func() string { return evacuationState(ns.Name, f.ISVC.Name) })
			}
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())
		// A transient observation gap can add an advisory; nothing executable
		// may appear, nothing may be dispatched, and the pending pod stays
		// unseated, which is what an untouched layout means.
		cycleBefore := recommendations(Default).Timestamp
		Consistently(func(g Gomega) {
			cycle := recommendations(g)
			for _, f := range []*alfredFixture{left, right} {
				recs := recsFor(cycle, workloadID(f))
				for _, r := range recs {
					g.Expect(r.Outcome).To(Equal(advisoryOutcome), "finding for %s: %s", workloadID(f), describeRecs(recs))
				}
			}
			leftLive := assertNoDispatch(g, left, a)
			g.Expect(leftLive).To(HaveLen(1))
			g.Expect(leftLive[0].UID).To(Equal(leftUID))
			rightLive := assertNoDispatch(g, right, b)
			g.Expect(rightLive).To(HaveLen(1))
			g.Expect(rightLive[0].UID).To(Equal(rightUID))
			g.Expect(freshPod(g, pending).Spec.NodeName).To(BeEmpty(), "the pending pod was seated")
		}).WithTimeout(quietPeriod).WithPolling(quietPollInterval).Should(Succeed())
		Expect(recommendations(Default).Timestamp).To(BeTemporally(">", cycleBefore), "no decision cycle during the quiet window")

		// The replacement can only land on the other worker: the request
		// excludes the source and the third worker is cordoned.
		ginkgo.By("node health still evacuates inside the closed window")
		waitNodeRecordGone(a)
		waitJournalSettled()
		entry := evacuateAndComplete(left, a, v1beta1.EngineComponent)
		Expect(entry.Phase).To(Equal("completed"))
		Expect(entry.Targets).To(ConsistOf(b), "predicted placement")
		req := parseMigrationRequest(Default, entry.Payload)
		Expect(req.Reason).To(Equal(nodeUnhealthyReason))
		Expect(req.HintTargetNodes).To(ConsistOf(b))
		waitPodsOffNode(ns.Name, left.ISVC.Name, a)
		Eventually(func(g Gomega) {
			live := 0
			for _, p := range omenativePods(g, ns.Name, left.ISVC.Name) {
				if p.DeletionTimestamp != nil {
					continue
				}
				live++
				g.Expect(p.UID).NotTo(Equal(leftUID), "pod %s is the original", p.Name)
				g.Expect(p.Spec.NodeName).To(Equal(b), "pod %s", p.Name)
				g.Expect(podIsReady(&p)).To(BeTrue(), "pod %s", p.Name)
			}
			g.Expect(live).To(Equal(1))
			g.Expect(migrationRequestAnnotations(g, isvcKey(left))).To(BeEmpty())
			rightLive := assertNoDispatch(g, right, b)
			g.Expect(rightLive).To(HaveLen(1), "the healthy worker's Instance was moved")
			g.Expect(rightLive[0].UID).To(Equal(rightUID), "the healthy worker's Instance was moved")
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})
})
