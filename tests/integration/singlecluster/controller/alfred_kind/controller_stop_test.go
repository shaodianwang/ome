package alfred_kind

import (
	"fmt"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"

	"github.com/ome-projects/ome/tests/integration/kindutil"
)

// stopControllerForSpec stops the in-process controller manager and returns an
// idempotent restart. The restart is registered as cleanup before the stop, so
// a failure anywhere after it still hands the next spec a running controller.
// A failed spec also resets alfred's dispatch state before that restart: a
// request left unresolved with the controller away is a non-terminal journal
// entry, and one such entry withholds every later dispatch.
func stopControllerForSpec(fix *alfredFixture) (restart func()) {
	restarted := false
	restart = func() {
		if restarted {
			return
		}
		gomega.Expect(kindutil.Suite().StartManager()).To(gomega.Succeed(), "restart the controller manager")
		restarted = true
	}
	ginkgo.DeferCleanup(restart)
	ginkgo.DeferCleanup(func() {
		if ginkgo.CurrentSpecReport().Failed() {
			resetDispatchAfterFailure(fix)
		}
	})

	ginkgo.By("the controller manager is stopped")
	gomega.ExpectWithOffset(1, kindutil.Suite().Manager).NotTo(gomega.BeNil())
	gomega.ExpectWithOffset(1, kindutil.Suite().StopManager()).To(gomega.Succeed())
	gomega.ExpectWithOffset(1, kindutil.Suite().Manager).To(gomega.BeNil())
	return restart
}

// resetDispatchAfterFailure removes the fixture's pending requests from its
// mailbox, so the returning controller has nothing to consume, and overwrites
// the journal with the empty document. Alfred saves the journal with an
// optimistic update of the ConfigMap it loaded for the pass, so a save racing
// this write fails and the next pass reads the empty document; an entry that
// still lands is overwritten again within the wait.
func resetDispatchAfterFailure(fix *alfredFixture) {
	fmt.Fprintf(ginkgo.GinkgoWriter, "spec failed: clearing the mailbox of %s and resetting the dispatch journal\n", workloadID(fix))
	gomega.Eventually(func(g gomega.Gomega) {
		g.Expect(clearMigrationRequests(isvcKey(fix))).To(gomega.Succeed(), "clear the mailbox of %s", workloadID(fix))
	}).WithTimeout(kindShortTimeout).WithPolling(kindPollInterval).Should(gomega.Succeed())
	// The first write runs even when the journal is already empty: the empty
	// document also drops backoffUntil.
	setJournal(emptyJournal)
	gomega.Eventually(func(g gomega.Gomega) {
		entries := journal(g).Entries
		if len(entries) > 0 {
			writeJournal(g, emptyJournal)
		}
		g.Expect(entries).To(gomega.BeEmpty(), func() string { return "journal after the reset: " + describeJournal(entries) })
	}).WithTimeout(kindShortTimeout).WithPolling(kindPollInterval).Should(gomega.Succeed())
}
