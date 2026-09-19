package irmanaged_kind

import (
	"os"
	"time"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"knative.dev/pkg/apis"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/tests/integration/util"
)

// IR-managed OMENative rollout specs.
//
// On the IR-managed path a spec change must roll through promptly: the
// Instance leaves Phase=Updating, readyReplicas recovers, and the ISVC
// returns to Ready=True. That holds only while the InferenceReplica
// controller's pod event handler (inferencereplica/watches.go) feeds pod
// create/delete observations into the Expectations cache its workload
// dispatcher reads; otherwise the surge/recreate destructive-step gates
// (ExpectationsCache().Satisfied()) release only on the expectations TTL.
var _ = ginkgo.Describe("IR-managed OMENative rollout", ginkgo.Ordered, func() {
	var ns *corev1.Namespace

	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "irm-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	// IRM-1: the core regression guard. Single-replica engine, default
	// SurgeThenDrain. An env-var bump must surge to the alternate ordinal
	// and CONVERGE — readyReplicas back to 1, currentRevision catches up,
	// ISVC Ready True — rather than parking at readyReplicas=0 with the
	// Instance stuck Phase=Updating behind the expectations gate.
	ginkgo.It("IRM-1 SurgeThenDrain env-var bump converges (readyReplicas recovers, no Incarnation bump)", func() {
		name := "irm-surge-isvc"
		fix := newIRFixture(ns.Name, withISVCName(name), withEngineReplicas(1), withEngineImage("fake-serving:v1"))
		applyIRFixture(Default, fix)
		deferIRFixtureCleanup(fix)

		origRev := waitSteadyReady(ns.Name, name)
		bumpEngineEnv(ns.Name, name, "EXTRA_SETTING", "yes")

		ginkgo.By("expecting the surge rollout to converge (readyReplicas=1, new rev, Ready=True)")
		Eventually(func(g Gomega) {
			on := engineOMENative(freshISVC(g, ns.Name, name))
			g.Expect(on).NotTo(BeNil())
			g.Expect(on.UpdateRevision).NotTo(Equal(origRev), "should roll to a new revision")
			g.Expect(on.ReadyReplicas).To(Equal(int32(1)),
				"readyReplicas must recover to 1 after the surge (regression: stuck at 0)")
			g.Expect(on.CurrentRevision).To(Equal(on.UpdateRevision),
				"rollout must converge: currentRevision catches up to updateRevision")
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		expectISVCReady(ns.Name, name)

		ginkgo.By("asserting surge semantics on the IR: ActiveOrdinal 0->1, Incarnation unchanged")
		ir, ok := engineIR(ns.Name, name)
		Expect(ok).To(BeTrue(), "engine InferenceReplica must exist")
		rows := decodedInstanceStatuses(Default, ir)
		Expect(rows).To(HaveLen(1))
		Expect(rows[0].ActiveOrdinal).To(Equal(int32(1)),
			"surge must advance ActiveOrdinal to the alternate slot")
		Expect(rows[0].Incarnation).To(Equal(int64(1)),
			"surge must NOT bump Incarnation (that is the recreate counter)")
	})

	// IRM-2: the same gate on the RecreatePod path (its old-pod delete
	// also waits on the expectations cache). An env-var bump must recreate
	// the pod at the SAME ordinal (0) with a bumped Incarnation and
	// converge. Distinguishes the recreate semantics from surge (IRM-1).
	ginkgo.It("IRM-2 RecreatePod env-var bump converges (readyReplicas recovers, Incarnation bumps)", func() {
		name := "irm-recreate-isvc"
		fix := newIRFixture(ns.Name, withISVCName(name), withEngineReplicas(1),
			withEngineImage("fake-serving:v1"), withUpdateStrategy(v1beta1.UpdateStrategyRecreatePod))
		applyIRFixture(Default, fix)
		deferIRFixtureCleanup(fix)

		origRev := waitSteadyReady(ns.Name, name)
		bumpEngineEnv(ns.Name, name, "EXTRA_SETTING", "yes")

		ginkgo.By("expecting the recreate rollout to converge (readyReplicas=1, new rev, Ready=True)")
		Eventually(func(g Gomega) {
			on := engineOMENative(freshISVC(g, ns.Name, name))
			g.Expect(on).NotTo(BeNil())
			g.Expect(on.UpdateRevision).NotTo(Equal(origRev), "should roll to a new revision")
			g.Expect(on.ReadyReplicas).To(Equal(int32(1)),
				"readyReplicas must recover to 1 after the recreate (regression: stuck at 0)")
			g.Expect(on.CurrentRevision).To(Equal(on.UpdateRevision),
				"rollout must converge: currentRevision catches up to updateRevision")
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		expectISVCReady(ns.Name, name)

		ginkgo.By("asserting recreate semantics on the IR: ActiveOrdinal stays 0, Incarnation bumps")
		ir, ok := engineIR(ns.Name, name)
		Expect(ok).To(BeTrue(), "engine InferenceReplica must exist")
		rows := decodedInstanceStatuses(Default, ir)
		Expect(rows).To(HaveLen(1))
		Expect(rows[0].ActiveOrdinal).To(Equal(int32(0)),
			"recreate reuses ordinal 0 (no surge)")
		Expect(rows[0].Incarnation).To(BeNumerically(">=", int64(2)),
			"recreate must bump Incarnation")
	})

	// IRM-DIAG: env-gated (KIND_DIAG=1) full-snapshot dump for debugging a
	// wedged rollout. Not an assertion — always passes. Keeps the rich
	// per-tick diagnostic (pods + IR status + routed slices) one env var
	// away when a stalled rollout needs diagnosing.
	ginkgo.It("IRM-DIAG single-replica env-var bump: capture full snapshot", func() {
		if os.Getenv("KIND_DIAG") == "" {
			ginkgo.Skip("set KIND_DIAG=1 to run the diagnostic snapshot loop")
		}
		name := "irm-diag-isvc"
		fix := newIRFixture(ns.Name, withISVCName(name), withEngineReplicas(1), withEngineImage("fake-serving:v1"))
		applyIRFixture(Default, fix)
		deferIRFixtureCleanup(fix)

		waitSteadyReady(ns.Name, name)
		dumpDiag(0, ns.Name, name)
		bumpEngineEnv(ns.Name, name, "EXTRA_SETTING", "yes")

		deadline := time.Now().Add(3 * time.Minute)
		tick := 1
		for time.Now().Before(deadline) {
			dumpDiag(tick, ns.Name, name)
			if on := engineOMENative(freshISVC(Default, ns.Name, name)); on != nil &&
				on.ReadyReplicas >= 1 && on.CurrentRevision == on.UpdateRevision && on.UpdateRevision != "" {
				break
			}
			tick++
			time.Sleep(3 * time.Second)
		}
	})
})

// waitSteadyReady blocks until the engine reaches readyReplicas=1 on a
// converged revision and returns that revision name.
func waitSteadyReady(namespace, name string) string {
	var rev string
	Eventually(func(g Gomega) {
		on := engineOMENative(freshISVC(g, namespace, name))
		g.Expect(on).NotTo(BeNil(), "engine omenative status nil")
		g.Expect(on.ReadyReplicas).To(Equal(int32(1)), "steady-state readyReplicas")
		g.Expect(on.CurrentRevision).To(Equal(on.UpdateRevision), "steady-state converged")
		g.Expect(on.UpdateRevision).NotTo(BeEmpty())
		rev = on.UpdateRevision
	}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	return rev
}

// bumpEngineEnv appends an env var to the engine container — a non-image
// diff that triggers a rollout (surge or recreate per strategy).
func bumpEngineEnv(namespace, name, k, v string) {
	ginkgo.By("bumping engine container env " + k + "=" + v)
	Eventually(func(g Gomega) {
		cur := freshISVC(g, namespace, name)
		cur.Spec.Engine.PodSpec.Containers[0].Env = append(
			cur.Spec.Engine.PodSpec.Containers[0].Env,
			corev1.EnvVar{Name: k, Value: v},
		)
		g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
	}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())
}

// expectISVCReady asserts the ISVC top-level Ready condition is True.
func expectISVCReady(namespace, name string) {
	ginkgo.By("expecting ISVC top-level Ready=True")
	Eventually(func(g Gomega) {
		c := &v1beta1.InferenceService{}
		g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: namespace, Name: name}, c)).To(Succeed())
		cond := c.Status.GetCondition(apis.ConditionReady)
		g.Expect(cond).NotTo(BeNil(), "ISVC Ready condition missing")
		g.Expect(cond.Status).To(Equal(corev1.ConditionTrue),
			"ISVC Ready must be True after rollout (regression: stuck False); reason=%s msg=%s", cond.Reason, cond.Message)
	}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())
}
