package v1beta1testing_test

import (
	"time"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	v1beta1testing "github.com/ome-projects/ome/pkg/utils/testing/v1beta1"

	"github.com/ome-projects/ome/tests/integration/util"
)

var _ = ginkgo.Describe("SetDefaultEventually", func() {
	ginkgo.It("installs the package default timeout/poll on Gomega", func() {
		// Save and restore so this spec doesn't leak global state into
		// the rest of the suite.
		origTimeout := v1beta1testing.DefaultEventuallyTimeout
		origPoll := v1beta1testing.DefaultEventuallyPollingInterval
		ginkgo.DeferCleanup(func() {
			v1beta1testing.DefaultEventuallyTimeout = origTimeout
			v1beta1testing.DefaultEventuallyPollingInterval = origPoll
			v1beta1testing.SetDefaultEventually()
		})

		v1beta1testing.DefaultEventuallyTimeout = 750 * time.Millisecond
		v1beta1testing.DefaultEventuallyPollingInterval = 50 * time.Millisecond
		v1beta1testing.SetDefaultEventually()

		// A bare Eventually with no .WithTimeout must now honor the
		// 750ms default: a never-true condition fails in well under the
		// 30s baseline, proving the default took effect.
		start := time.Now()
		ok := InterceptGomegaFailure(func() {
			Eventually(func() bool { return false }).Should(BeTrue())
		})
		Expect(ok).To(HaveOccurred(), "expected the bare Eventually to time out")
		Expect(time.Since(start)).To(BeNumerically("<", 5*time.Second),
			"bare Eventually did not pick up the shortened default timeout")
	})
})

var _ = ginkgo.Describe("CreateAndWait / CreateAndWaitReady", func() {

	ginkgo.It("CreateAndWait creates and blocks until the object is observable", func() {
		ns, err := util.CreateNamespaceFromPrefix(suiteCtx, suiteClient, "p2-caw-")
		Expect(err).NotTo(HaveOccurred())

		bm := v1beta1testing.MakeBaseModel("caw-model", ns.Name).
			StorageURI("pvc://my-pvc/models/caw").Obj()

		v1beta1testing.CreateAndWait(suiteCtx, suiteClient, bm)

		got := &v1beta1.BaseModel{}
		Expect(suiteClient.Get(suiteCtx, types.NamespacedName{Name: "caw-model", Namespace: ns.Name}, got)).To(Succeed())
	})

	ginkgo.It("CreateAndWaitReady blocks until the readiness predicate holds", func() {
		ns, err := util.CreateNamespaceFromPrefix(suiteCtx, suiteClient, "p2-cawr-")
		Expect(err).NotTo(HaveOccurred())

		bm := v1beta1testing.MakeBaseModel("cawr-model", ns.Name).
			StorageURI("pvc://my-pvc/models/cawr").Obj()

		// No controller runs in this suite, so drive the status to Ready
		// ourselves shortly after the object is created. CreateAndWaitReady
		// must keep polling until it observes State==Ready.
		go func() {
			defer ginkgo.GinkgoRecover()
			key := types.NamespacedName{Name: "cawr-model", Namespace: ns.Name}
			Eventually(func(g Gomega) {
				live := &v1beta1.BaseModel{}
				g.Expect(suiteClient.Get(suiteCtx, key, live)).To(Succeed())
				live.Status.State = v1beta1.LifeCycleStateReady
				g.Expect(suiteClient.Status().Update(suiteCtx, live)).To(Succeed())
			}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
		}()

		v1beta1testing.CreateAndWaitReadyWithTimeout(suiteCtx, suiteClient, bm,
			"BaseModel State==Ready",
			func(m *v1beta1.BaseModel) bool { return m.Status.State == v1beta1.LifeCycleStateReady },
			util.MediumTimeout, util.PollInterval)

		Expect(bm.Status.State).To(Equal(v1beta1.LifeCycleStateReady))
	})
})

var _ = ginkgo.Describe("Status setters round-trip through the status subresource", func() {

	ginkgo.It("BaseModel StatusState + StatusConditionTrue persist and satisfy the condition matcher", func() {
		ns, err := util.CreateNamespaceFromPrefix(suiteCtx, suiteClient, "p2-stat-")
		Expect(err).NotTo(HaveOccurred())

		// Build with the desired status, then create. Create persists
		// only spec and overwrites the local object with the server copy
		// (status cleared), so re-apply the seeded status onto the
		// created object before the status-subresource write.
		desired := v1beta1testing.MakeBaseModel("stat-model", ns.Name).
			StorageURI("pvc://my-pvc/models/stat").
			StatusState(v1beta1.LifeCycleStateReady).
			StatusConditionTrue("Ready", "AllGood").
			Obj()

		bm := desired.DeepCopy()
		Expect(suiteClient.Create(suiteCtx, bm)).To(Succeed())
		bm.Status = desired.Status
		Expect(suiteClient.Status().Update(suiteCtx, bm)).To(Succeed())

		got := &v1beta1.BaseModel{}
		Expect(suiteClient.Get(suiteCtx, types.NamespacedName{Name: "stat-model", Namespace: ns.Name}, got)).To(Succeed())
		Expect(got.Status.State).To(Equal(v1beta1.LifeCycleStateReady))
		Expect(got.Status.Conditions).To(v1beta1testing.HaveConditionStatusTrueAndReason("Ready", "AllGood"))
	})

	ginkgo.It("StatusCondition upserts (does not duplicate) a same-type condition", func() {
		bm := v1beta1testing.MakeBaseModel("dup", "any").
			StatusConditionTrue("Ready", "First").
			StatusCondition(metav1.Condition{Type: "Ready", Status: metav1.ConditionFalse, Reason: "Second"}).
			Obj()
		Expect(bm.Status.Conditions).To(HaveLen(1))
		Expect(bm.Status.Conditions).To(v1beta1testing.HaveConditionStatusFalseAndReason("Ready", "Second"))
	})
})
