package alfred_kind

import (
	"strconv"
	"strings"

	"github.com/google/uuid"
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	admissionv1 "k8s.io/api/admissionregistration/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/tests/integration/util"
)

// Messages of the guard's three validations, in evaluation order; a denial
// carries the message of the first one that fails.
const (
	guardDenyWorkloadFields = "Alfred may not change workload fields or non-annotation metadata"
	guardDenyAnnotationLoss = "Alfred may not remove or replace existing annotations"
	guardDenyRequestShape   = "Alfred may add only one bounded v1 migration request"
)

// patchISVCAs reads the ISVC as the suite's admin identity and sends the
// change mutate makes as a merge patch through c, so the update the guard
// evaluates carries exactly that change.
func patchISVCAs(c client.Client, key client.ObjectKey, mutate func(*v1beta1.InferenceService)) error {
	isvc := &v1beta1.InferenceService{}
	if err := k8sClient.Get(suiteCtx, key, isvc); err != nil {
		return err
	}
	if isvc.Annotations == nil {
		isvc.Annotations = map[string]string{}
	}
	if isvc.Labels == nil {
		isvc.Labels = map[string]string{}
	}
	base := isvc.DeepCopy()
	mutate(isvc)
	return c.Patch(suiteCtx, isvc, client.MergeFrom(base))
}

// validRequest is a minimal valid v1 migration request for engine Instance 0
// on src. It carries only what the controller's accept path consumes
// (schemaVersion, component, instance, from_node, requested_by), not the
// full shape alfred writes.
func validRequest(src string) string {
	return `{"schemaVersion":"v1","component":"engine","instance":0,"from_node":"` + src + `","requested_by":"alfred"}`
}

// waitGuardInstalled blocks until the policy and binding exist and the policy
// is type-checked at its current generation, which is what alfred's own guard
// check requires before it dispatches.
func waitGuardInstalled() {
	EventuallyWithOffset(1, func(g Gomega) {
		policy := &admissionv1.ValidatingAdmissionPolicy{}
		g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{Name: alfredGuardName}, policy)).To(Succeed())
		g.Expect(policy.Status.ObservedGeneration).To(Equal(policy.Generation), "policy status is stale")
		g.Expect(policy.Status.TypeChecking).NotTo(BeNil(), "policy is not type-checked yet")
		g.Expect(policy.Status.TypeChecking.ExpressionWarnings).To(BeEmpty())
		binding := &admissionv1.ValidatingAdmissionPolicyBinding{}
		g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{Name: alfredGuardName}, binding)).To(Succeed())
	}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())
}

// waitGuardEnforced blocks until the apiserver denies, for alfred's identity,
// a label change on the ISVC at key. Applying the policy does not wait for
// enforcement, so nothing that relies on a denial may run before this. Each
// probe carries a fresh value: a probe admitted before enforcement would
// otherwise make the next one a no-op the guard has nothing to deny.
func waitGuardEnforced(c client.Client, key client.ObjectKey) {
	attempt := 0
	EventuallyWithOffset(1, func(g Gomega) {
		attempt++
		value := strconv.Itoa(attempt)
		err := patchISVCAs(c, key, func(i *v1beta1.InferenceService) { i.Labels["alfred-kind/guard-probe"] = value })
		g.Expect(err).To(HaveOccurred(), "the guard is not enforced yet")
		g.Expect(err.Error()).To(ContainSubstring(guardDenyWorkloadFields))
	}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())
}

var _ = ginkgo.Describe("alfred admission guard and RBAC", func() {
	var ns *corev1.Namespace

	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "alfred-guard-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("withholds execution while the guard is missing and resumes when it is back", func() {
		src := workers[0]
		// The guard is cluster-scoped and shared by every spec, so it is put
		// back, and usable by alfred again, at spec end whatever happens in between.
		ginkgo.DeferCleanup(func() {
			applyManifest(guardManifest)
			waitGuardInstalled()
		})
		binding := &admissionv1.ValidatingAdmissionPolicyBinding{ObjectMeta: metav1.ObjectMeta{Name: alfredGuardName}}
		policy := &admissionv1.ValidatingAdmissionPolicy{ObjectMeta: metav1.ObjectMeta{Name: alfredGuardName}}
		Expect(k8sClient.Delete(suiteCtx, binding)).To(Succeed())
		Expect(k8sClient.Delete(suiteCtx, policy)).To(Succeed())

		fix := newAlfredFixture(ns.Name, withName("guard-off"))
		readyOn(fix, src, 1)
		markUnhealthy(src)

		// alfred reads the policy and binding back, uncached, on every
		// execution pass; without them an admitted candidate is withheld
		// before the dispatcher writes anything.
		ginkgo.By("the evacuation is admitted but execution is withheld")
		Eventually(func(g Gomega) {
			recs := recsFor(recommendations(g), workloadID(fix))
			g.Expect(recs).NotTo(BeEmpty())
			g.Expect(recs[0].Policy).To(Equal(nodeHealthPolicy))
			g.Expect(recs[0].FromNode).To(Equal(src))
			g.Expect(recs[0].Outcome).To(Equal("withheld"))
			g.Expect(recs[0].DispatchStatus).To(Equal("withheld"))
			g.Expect(recs[0].DispatchReason).To(Equal("GuardUnavailable"))
			events := alfredEventsFor(g, ns.Name, fix.ISVC.Name, "RecommendationWithheld")
			g.Expect(events).NotTo(BeEmpty())
			g.Expect(events[0].Message).To(ContainSubstring("GuardUnavailable"))
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("it stays withheld and nothing is dispatched")
		cycleBefore := recommendations(Default).Timestamp
		Consistently(func(g Gomega) {
			// A transient observation gap can add a component-wide advisory
			// beside the instance finding; only the executable rows matter here.
			withheld := 0
			for _, r := range recsFor(recommendations(g), workloadID(fix)) {
				if r.Outcome == advisoryOutcome {
					continue
				}
				g.Expect(r.DispatchStatus).To(Equal("withheld"), "guard-less dispatch for %s", workloadID(fix))
				g.Expect(r.DispatchReason).To(Equal("GuardUnavailable"))
				withheld++
			}
			g.Expect(withheld).NotTo(BeZero(), "no withheld recommendation for %s", workloadID(fix))
			g.Expect(assertNoDispatch(g, fix, src)).To(HaveLen(1), "%s pod left %s", workloadID(fix), src)
		}).WithTimeout(quietPeriod).WithPolling(quietPollInterval).Should(Succeed())
		Expect(recommendations(Default).Timestamp).To(BeTemporally(">", cycleBefore), "no decision cycle during the quiet window")

		ginkgo.By("reinstalling the guard lets the dispatch through")
		applyManifest(guardManifest)
		waitGuardInstalled()
		entry := waitJournalEntry(ns.Name, fix.ISVC.Name, dispatchTimeout, "submitted", "acknowledged", "completed")
		Expect(entry.FromNode).To(Equal(src))
		waitMigrationCompleted(ns.Name, fix.ISVC.Name, v1beta1.EngineComponent, entry.UUID)
		closed := waitJournalPhase(entry.UUID, "completed", kindMediumTimeout)
		Expect(closed.CompletedAt).NotTo(BeNil())
		waitPodsOffNode(ns.Name, fix.ISVC.Name, src)
		Eventually(func(g Gomega) {
			g.Expect(migrationRequestAnnotations(g, isvcKey(fix))).To(BeEmpty())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})

	ginkgo.It("lets alfred's identity add one request and nothing else", func() {
		src := workers[0]
		// Under MigrationPolicy Mode=Never the controller answers the one
		// admitted request at accept, so it has no side effect on the workload.
		fix := newAlfredFixture(ns.Name, withName("guard-matrix"), withMigrationMode(v1beta1.MigrationPolicyModeNever))
		readyOn(fix, src, 1)
		key := isvcKey(fix)
		Eventually(func(g Gomega) {
			ir := freshIR(g, ns.Name, fix.ISVC.Name, v1beta1.EngineComponent)
			g.Expect(ir.Spec.Lifecycle).NotTo(BeNil())
			g.Expect(ir.Spec.Lifecycle.MigrationPolicy).NotTo(BeNil())
			g.Expect(ir.Spec.Lifecycle.MigrationPolicy.Mode).To(Equal(v1beta1.MigrationPolicyModeNever))
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())
		annotateISVC(Default, key, map[string]string{"example.com/keep": "yes"})
		c, cs := asAlfred()
		waitGuardInstalled()
		waitGuardEnforced(c, key)
		// Captured only once the guard denies writes, so a probe admitted
		// before enforcement can never be mistaken for a rollout below.
		onSrc := livePodsOnNode(omenativePods(Default, ns.Name, fix.ISVC.Name), src)
		Expect(onSrc).To(HaveLen(1))
		oldUID := onSrc[0].UID

		ginkgo.By("one bounded v1 request is admitted")
		id := uuid.NewString()
		Expect(patchISVCAs(c, key, func(i *v1beta1.InferenceService) {
			i.Annotations[migrationAnnoPrefix+id] = validRequest(src)
		})).To(Succeed())

		ginkgo.By("every other write is denied by the rule it breaks")
		deny := func(what string, mutate func(*v1beta1.InferenceService), message string) {
			err := patchISVCAs(c, key, mutate)
			ExpectWithOffset(1, err).To(HaveOccurred(), what)
			ExpectWithOffset(1, err.Error()).To(ContainSubstring(message), what)
		}
		deny("spec change", func(i *v1beta1.InferenceService) {
			two := 2
			i.Spec.Engine.MinReplicas = &two
		}, guardDenyWorkloadFields)
		deny("label change", func(i *v1beta1.InferenceService) {
			i.Labels["alfred-kind/denied"] = "true"
		}, guardDenyWorkloadFields)
		deny("two requests at once", func(i *v1beta1.InferenceService) {
			i.Annotations[migrationAnnoPrefix+uuid.NewString()] = validRequest(src)
			i.Annotations[migrationAnnoPrefix+uuid.NewString()] = validRequest(src)
		}, guardDenyRequestShape)
		deny("removing an annotation", func(i *v1beta1.InferenceService) {
			delete(i.Annotations, "example.com/keep")
		}, guardDenyAnnotationLoss)
		deny("replacing an annotation", func(i *v1beta1.InferenceService) {
			i.Annotations["example.com/keep"] = "no"
		}, guardDenyAnnotationLoss)
		deny("oversize payload", func(i *v1beta1.InferenceService) {
			i.Annotations[migrationAnnoPrefix+uuid.NewString()] = `{"schemaVersion":"v1","reason":"` + strings.Repeat("x", 5000) + `"}`
		}, guardDenyRequestShape)
		deny("foreign annotation key", func(i *v1beta1.InferenceService) {
			i.Annotations["example.com/foo"] = "bar"
		}, guardDenyRequestShape)

		// A valid request under Mode=Never is consumed as a born-terminal
		// Failed entry and the mailbox is emptied; no surge is allocated.
		ginkgo.By("the controller rejects the admitted request at accept")
		waitControllerEvent(ns.Name, fix.ISVC.Name, "MigrationRequestRejected", kindMediumTimeout)
		Eventually(func(g Gomega) {
			m := findMigration(migrationEntries(g, ns.Name, fix.ISVC.Name, v1beta1.EngineComponent), id)
			g.Expect(m).NotTo(BeNil(), "migration %s not recorded on the IR yet", id)
			g.Expect(m.Phase).To(Equal(v1beta1.MigrationPhaseFailed))
			g.Expect(m.Trigger).To(Equal(v1beta1.MigrationTriggerManual))
			g.Expect(m.Message).To(ContainSubstring("Mode=Never"))
			g.Expect(m.SurgeInstance).To(BeNil())
			g.Expect(migrationRequestAnnotations(g, key)).To(BeEmpty())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())
		Consistently(func(g Gomega) {
			live := livePodsOnNode(omenativePods(g, ns.Name, fix.ISVC.Name), src)
			g.Expect(live).To(HaveLen(1))
			g.Expect(live[0].UID).To(Equal(oldUID))
		}).WithTimeout(kindShortTimeout).WithPolling(quietPollInterval).Should(Succeed())

		ginkgo.By("alfred holds no pod or node write")
		pods := omenativePods(Default, ns.Name, fix.ISVC.Name)
		Expect(pods).NotTo(BeEmpty())
		err := cs.CoreV1().Pods(ns.Name).Delete(suiteCtx, pods[0].Name, metav1.DeleteOptions{})
		Expect(apierrors.IsForbidden(err)).To(BeTrue(), "pod delete: %v", err)
		err = cs.PolicyV1().Evictions(ns.Name).Evict(suiteCtx, &policyv1.Eviction{
			ObjectMeta: metav1.ObjectMeta{Name: pods[0].Name, Namespace: ns.Name},
		})
		Expect(apierrors.IsForbidden(err)).To(BeTrue(), "eviction: %v", err)
		node := &corev1.Node{}
		Expect(k8sClient.Get(suiteCtx, client.ObjectKey{Name: src}, node)).To(Succeed())
		base := node.DeepCopy()
		node.Labels["alfred-kind/should-fail"] = "true"
		err = c.Patch(suiteCtx, node, client.MergeFrom(base))
		Expect(apierrors.IsForbidden(err)).To(BeTrue(), "node patch: %v", err)
	})
})
