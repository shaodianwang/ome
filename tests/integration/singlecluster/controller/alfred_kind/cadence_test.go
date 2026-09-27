package alfred_kind

import (
	"fmt"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// decisionPassesMetric is the count of the leader's decision-loop duration
// histogram: one increment per completed decision pass, regular or early.
const decisionPassesMetric = "alfred_decision_loop_duration_seconds_count"

// cadenceMinPasses is the growth of decisionPassesMetric that proves the
// regular timer within cadenceWindow; a single early pass cannot reach it.
const cadenceMinPasses = 2

// waitDecisionCadence fails the suite unless the leader runs decision passes
// at the configured interval, which every spec relies on from its first step
// (cycle records advancing, the journal reconciled within seconds). A leader
// arms the configured interval as soon as the config is applied, so the very
// first spec must see cadenceMinPasses passes within cadenceWindow; the check
// is a fail-fast guard, and a miss names the leader, its pass counts and the
// Lease holder before and after the window.
func waitDecisionCadence() {
	leader := currentLeader()
	before := decisionPassesEventually(leader.pod)
	after, scrapeErr, proven := cadenceObserved(leader.pod, before)
	if proven {
		fmt.Fprintf(ginkgo.GinkgoWriter, "decision loop cadence proven: leader=%s passes=%v in %s\n",
			leader.pod, after-before, cadenceWindow)
		return
	}
	var holderNow string
	if err := quietly(func(g gomega.Gomega) { holderNow = leaseHolder(g) }); err != nil {
		holderNow = "unreadable: " + err.Error()
	}
	gomega.ExpectWithOffset(1, after-before).To(gomega.BeNumerically(">=", cadenceMinPasses),
		"decision loop cadence not proven: leader %s completed %v passes (%v -> %v) in %s at a %s interval; last scrape error %q; lease %s holder %q before the window, %q after",
		leader.pod, after-before, before, after, cadenceWindow, decisionInterval, scrapeErr, alfredLeaseName, leader.holder, holderNow)
}

// cadenceLeader is one observation of leadership: the pod reporting it and
// the Lease holder identity (<pod>_<uuid>) naming that pod.
type cadenceLeader struct {
	pod    string
	holder string
}

// currentLeader reads the leader pod and the Lease holder together, so a
// handoff in flight is waited out rather than acted on.
func currentLeader() cadenceLeader {
	var l cadenceLeader
	gomega.EventuallyWithOffset(1, func(g gomega.Gomega) {
		l.pod = leaderPod(g).Name
		l.holder = leaseHolder(g)
		g.Expect(l.holder).To(gomega.HavePrefix(l.pod+"_"), "lease %s holder %q does not name the leader pod %s", alfredLeaseName, l.holder, l.pod)
	}).WithTimeout(kindShortTimeout).WithPolling(kindPollInterval).Should(gomega.Succeed())
	return l
}

// decisionPasses is the number of decision passes pod has completed. The
// sample is registered at startup, so its absence is a metric rename.
func decisionPasses(g gomega.Gomega, pod string) float64 {
	v, ok := scrapePod(g, pod).value(decisionPassesMetric, nil)
	g.ExpectWithOffset(1, ok).To(gomega.BeTrue(), "%s does not expose %s", pod, decisionPassesMetric)
	return v
}

// decisionPassesEventually reads decisionPasses with retries, so one
// pod-proxy hiccup does not end BeforeSuite.
func decisionPassesEventually(pod string) float64 {
	var v float64
	gomega.EventuallyWithOffset(1, func(g gomega.Gomega) {
		v = decisionPasses(g, pod)
	}).WithTimeout(kindShortTimeout).WithPolling(kindPollInterval).Should(gomega.Succeed())
	return v
}

// cadenceObserved polls pod's pass count every cadencePoll for cadenceWindow
// and reports whether it grew by cadenceMinPasses over before; after is the
// last count read, before if no read succeeded, and scrapeErr is why the last
// read failed, "" when it succeeded. The window runs on a Gomega whose
// failures are silent, so its expiry is a result, not a suite failure.
func cadenceObserved(pod string, before float64) (after float64, scrapeErr string, ok bool) {
	after = before
	quiet := gomega.NewGomega(func(string, ...int) {})
	ok = quiet.Eventually(func(g gomega.Gomega) {
		var read float64
		err := quietly(func(gg gomega.Gomega) { read = decisionPasses(gg, pod) })
		scrapeErr = ""
		if err != nil {
			scrapeErr = err.Error()
		}
		g.Expect(err).NotTo(gomega.HaveOccurred())
		after = read
		g.Expect(after - before).To(gomega.BeNumerically(">=", cadenceMinPasses))
	}).WithTimeout(cadenceWindow).WithPolling(cadencePoll).Should(gomega.Succeed())
	return after, scrapeErr, ok
}

// leaseHolder is the holder identity on alfred's leader-election Lease, ""
// while it is unheld.
func leaseHolder(g gomega.Gomega) string {
	lease, err := clientset.CoordinationV1().Leases(alfredNamespace).Get(suiteCtx, alfredLeaseName, metav1.GetOptions{})
	g.ExpectWithOffset(1, err).NotTo(gomega.HaveOccurred(), "read lease %s", alfredLeaseName)
	if lease.Spec.HolderIdentity == nil {
		return ""
	}
	return *lease.Spec.HolderIdentity
}

// takeOverLeadership deletes the leader pod and blocks until an identity
// other than the one observed holds the Lease (identities are compared
// whole), the old pod is gone, and the Deployment is fully Ready again, so
// every pass observed afterwards is the new leader's.
func takeOverLeadership(l cadenceLeader) {
	err := clientset.CoreV1().Pods(alfredNamespace).Delete(suiteCtx, l.pod, metav1.DeleteOptions{})
	gomega.ExpectWithOffset(1, client.IgnoreNotFound(err)).NotTo(gomega.HaveOccurred(), "delete leader pod %s", l.pod)
	gomega.EventuallyWithOffset(1, func(g gomega.Gomega) {
		holder := leaseHolder(g)
		g.Expect(holder).NotTo(gomega.BeEmpty(), "lease %s is unheld", alfredLeaseName)
		g.Expect(holder).NotTo(gomega.Equal(l.holder), "lease %s is still held by %s", alfredLeaseName, l.holder)
		err := k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: alfredNamespace, Name: l.pod}, &corev1.Pod{})
		g.Expect(apierrors.IsNotFound(err)).To(gomega.BeTrue(), "the old leader pod %s is still present", l.pod)
	}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(gomega.Succeed())
	waitAlfredReady()
}
