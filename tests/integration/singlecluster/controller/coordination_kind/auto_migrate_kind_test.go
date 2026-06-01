package coordination_kind

import (
	"time"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/tests/integration/util"
)

// Auto-migration on stuck Instance — when a pod is stuck in
// Phase=Creating / Updating longer than
// MigrationPolicy.ProgressDeadlineSeconds, the controller self-triggers
// a migration to recover. After MaxAutoRetries failed attempts the
// Instance escalates to Phase=Failed with the
// ProgressDeadlineExceeded condition.
//
// The fixture uses a non-existent image so the surge pod can never
// become Ready — exercising the retry budget rapidly. With a 30s
// progress deadline + MaxAutoRetries=2, we expect: first auto-trigger
// at T+30s; second auto-trigger after the first surge fails timeout;
// retry cap reached → Phase=Failed + condition. Total wall-clock
// budget around 4-5 minutes (each surge takes ~InstanceReadyTimeout
// to fail).
var _ = ginkgo.Describe("OMENative auto-migration on stuck Instance (KIND)", ginkgo.Ordered, func() {
	var ns *corev1.Namespace
	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "omenative-auto-mig-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("stuck Instance triggers auto-migration; retry cap escalates to Phase=Failed", func() {
		// Build the fixture with a bad image so the first pod NEVER
		// becomes Ready. Configure ProgressDeadlineSeconds=30 and
		// MaxAutoRetries=2 so the test fits the suite budget.
		deadline := int32(30)
		maxRetries := int32(2)
		// Use a short InstanceReadyTimeout so the failed surge times
		// out quickly enough to chew through MaxAutoRetries within the
		// test wall-clock budget. 45s is comfortable above the 30s
		// progress deadline.
		shortReadyTimeout := metav1.Duration{Duration: 45 * time.Second}

		fix := newCoordFixture(ns.Name, WithISVCName("auto-mig-am1"),
			WithoutDecoder(),
			WithEngineImage("ghcr.io/ome-projects/this-image-does-not-exist:v0"))
		fix.ISVC.Spec.Engine.Lifecycle = &v1beta1.LifecycleSpec{
			InstanceReadyTimeout: &shortReadyTimeout,
			MigrationPolicy: &v1beta1.MigrationPolicy{
				Mode:                    v1beta1.MigrationPolicyModeAuto,
				ProgressDeadlineSeconds: &deadline,
				MaxAutoRetries:          &maxRetries,
			},
		}
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		ginkgo.By("waiting for the first pod to land and enter Phase=Creating (it will NEVER become Ready)")
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "auto-mig-am1")
			cs := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.Lifecycle).NotTo(BeNil())
			g.Expect(cs.Lifecycle.InstanceStatuses).NotTo(BeEmpty())
			// Phase will be Creating; pod will be ImagePullBackOff.
			g.Expect(cs.Lifecycle.InstanceStatuses[0].Phase).
				To(Equal(v1beta1.OMENativeInstanceCreating))
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("waiting for the controller to write the first synthetic AutoRecover audit ledger entry past the deadline")
		Eventually(func(g Gomega) {
			ledger := loadLedger(g, ns.Name, "auto-mig-am1")
			autoRecoverCount := 0
			for _, e := range ledger.Entries {
				if e.Reason == "AutoRecover" && e.Component == "engine" {
					autoRecoverCount++
				}
			}
			g.Expect(autoRecoverCount).To(BeNumerically(">=", 1),
				"expected at least one AutoRecover audit ledger entry past ProgressDeadlineSeconds=30s; got %+v", ledger.Entries)
		}).WithTimeout(2 * time.Minute).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("waiting for the AutoMigrationTriggered event on the ISVC")
		Eventually(func(g Gomega) {
			events := &corev1.EventList{}
			g.Expect(k8sClient.List(suiteCtx, events, client.InNamespace(ns.Name))).To(Succeed())
			gotEvent := false
			for _, e := range events.Items {
				if e.InvolvedObject.Name == "auto-mig-am1" && e.Reason == "AutoMigrationTriggered" {
					gotEvent = true
					break
				}
			}
			g.Expect(gotEvent).To(BeTrue(), "expected AutoMigrationTriggered event")
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("waiting for the retry cap to be reached and Phase=Failed + ProgressDeadlineExceeded condition stamped")
		// Worst-case budget: 2 retries × (InstanceReadyTimeout 45s +
		// surge create + drain + ledger write headroom) ≈ 4-5 minutes.
		// 8 minutes is a comfortable ceiling.
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "auto-mig-am1")
			cs := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.Lifecycle).NotTo(BeNil())
			g.Expect(cs.Lifecycle.InstanceStatuses).NotTo(BeEmpty())
			// Find the original source InstanceStatus — it's the one
			// stuck at the auto-migration trigger point. Surge indices
			// are different.
			foundFailedWithCondition := false
			for _, s := range cs.Lifecycle.InstanceStatuses {
				if s.Phase != v1beta1.OMENativeInstanceFailed {
					continue
				}
				for _, c := range s.Conditions {
					if c.Type == "ProgressDeadlineExceeded" && c.Status == metav1.ConditionTrue {
						foundFailedWithCondition = true
						break
					}
				}
				if foundFailedWithCondition {
					break
				}
			}
			g.Expect(foundFailedWithCondition).To(BeTrue(),
				"expected at least one InstanceStatus with Phase=Failed + ProgressDeadlineExceeded condition; got %+v", cs.Lifecycle.InstanceStatuses)
		}).WithTimeout(8 * time.Minute).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("expecting an AutoMigrationCapReached warning event")
		Eventually(func(g Gomega) {
			events := &corev1.EventList{}
			g.Expect(k8sClient.List(suiteCtx, events, client.InNamespace(ns.Name))).To(Succeed())
			gotCap := false
			for _, e := range events.Items {
				if e.InvolvedObject.Name == "auto-mig-am1" && e.Reason == "AutoMigrationCapReached" {
					gotCap = true
					break
				}
			}
			g.Expect(gotCap).To(BeTrue(), "expected AutoMigrationCapReached event after retry budget exhausted")
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})
})
