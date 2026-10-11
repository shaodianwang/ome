package omenative_coordination_kind

import (
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	"github.com/ome-projects/ome/tests/integration/util"
)

// Fast force-delete policy for KIND specs. The numeric consts mirror
// forceDeleteLifecycleJSON — assertion deadlines are computed from them
// (plus the pod's own DeletionTimestamp), never from ad-hoc sleeps.
const (
	forceDeleteOverdueSlack  = 15 * time.Second
	forceDeleteNodeThreshold = 30 * time.Second
)

const forceDeleteLifecycleJSON = `{"forceDelete":{"overdueSlack":"15s","nodeUnreachableThreshold":"30s"}}`

// lifecycleNoForceDeleteJSON pins the control: lifecycle block present,
// forceDelete key absent — absent means the escalation does not exist.
const lifecycleNoForceDeleteJSON = `{}`

// forceDeleteAbandonedLifecycleJSON arms the escalation beside a short
// stuck-pod grace, so a replacement whose image never pulls parks its
// row at Failed inside the suite budget.
const forceDeleteAbandonedLifecycleJSON = `{"forceDelete":{"overdueSlack":"15s","nodeUnreachableThreshold":"30s"},"stuckPodGracePeriod":"20s"}`

// unpullableImage is a tag no registry serves: a pod on it parks in
// ErrImagePull / ImagePullBackOff and never reaches ContainersReady.
const unpullableImage = "ghcr.io/ome-projects/this-image-does-not-exist:v0"

// =========================================================================
// KIND node pause helpers (docker-level kubelet freeze)
// =========================================================================

// pauseKindNode freezes the docker container backing a KIND node (the
// container name equals the Node name), which stops the kubelet's
// heartbeats: the KCM marks the node Ready=Unknown after its
// node-monitor grace period (~40s) and adds the unreachable taint. A
// paused node poisons the shared KIND_KEEP_CLUSTER cluster for every
// later spec, so this registers a DeferCleanup that resumes the node
// and waits for it to rejoin — even when the spec body fails.
func pauseKindNode(node string) {
	out, err := exec.Command("docker", "pause", node).CombinedOutput()
	Expect(err).NotTo(HaveOccurred(), "docker pause %s: %s", node, string(out))
	ginkgo.DeferCleanup(func() { resumeKindNode(node) })
}

// resumeKindNode unpauses the node's container (idempotent — safe when
// the spec already resumed it mid-body) and blocks until the node
// reports Ready=True with the unreachable taint cleared, so the next
// spec inherits a healthy cluster. Fails loudly when the node cannot
// recover: a silent skip here would leak a dead node into every
// subsequent spec.
func resumeKindNode(node string) {
	out, err := exec.Command("docker", "inspect", "-f", "{{.State.Paused}}", node).CombinedOutput()
	Expect(err).NotTo(HaveOccurred(), "docker inspect %s: %s", node, string(out))
	if strings.TrimSpace(string(out)) == "true" {
		unpauseOut, unpauseErr := exec.Command("docker", "unpause", node).CombinedOutput()
		Expect(unpauseErr).NotTo(HaveOccurred(), "docker unpause %s: %s", node, string(unpauseOut))
	}
	Eventually(func(g Gomega) {
		n := &corev1.Node{}
		g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{Name: node}, n)).To(Succeed())
		g.Expect(nodeIsReady(n)).To(BeTrue(), "node %s must report Ready=True after unpause", node)
		for _, taint := range n.Spec.Taints {
			g.Expect(taint.Key).NotTo(Equal(corev1.TaintNodeUnreachable),
				"unreachable taint on %s must clear after unpause", node)
		}
	}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())
}

func nodeIsReady(n *corev1.Node) bool {
	for _, c := range n.Status.Conditions {
		if c.Type == corev1.NodeReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}

// expectNodeNotReadyAtLeast asserts node's NodeReady condition has been
// False/Unknown for at least d — the same evidence branch the
// escalation's nodeUnreachableThreshold measures, read from the
// condition's own LastTransitionTime.
func expectNodeNotReadyAtLeast(g Gomega, node string, d time.Duration) {
	n := &corev1.Node{}
	g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{Name: node}, n)).To(Succeed())
	for _, c := range n.Status.Conditions {
		if c.Type != corev1.NodeReady {
			continue
		}
		g.Expect(c.Status).NotTo(Equal(corev1.ConditionTrue),
			"node %s must not be Ready while its container is paused", node)
		g.Expect(time.Since(c.LastTransitionTime.Time)).To(BeNumerically(">=", d),
			"node %s NotReady age must reach the threshold", node)
		return
	}
	g.Expect(false).To(BeTrue(), "NodeReady condition missing on node %s", node)
}

// =========================================================================
// Spec-local assertion helpers
// =========================================================================

// eventsWithReason returns every Event in namespace with the given Reason.
func eventsWithReason(g Gomega, namespace, reason string) []corev1.Event {
	events := &corev1.EventList{}
	g.Expect(k8sClient.List(suiteCtx, events, client.InNamespace(namespace))).To(Succeed())
	var out []corev1.Event
	for i := range events.Items {
		if events.Items[i].Reason == reason {
			out = append(out, events.Items[i])
		}
	}
	return out
}

// aliveEnginePodForInstance returns the single non-Terminating engine
// pod carrying ome.io/instance-index == idx.
func aliveEnginePodForInstance(g Gomega, namespace, isvcName string, idx int32) *corev1.Pod {
	pods := listOMENativePods(g, namespace, isvcName)
	var match *corev1.Pod
	for i := range pods {
		p := &pods[i]
		if p.Labels[query.LabelInstanceIdx] != strconv.Itoa(int(idx)) || p.DeletionTimestamp != nil {
			continue
		}
		g.Expect(match).To(BeNil(), "expected exactly one alive pod for instance %d", idx)
		match = p
	}
	g.Expect(match).NotTo(BeNil(), "no alive pod found for instance %d", idx)
	return match
}

// wedgeScaleDownOnPausedNode drives the shared wedge both specs build
// on: 2 Ready engine instances, pause the node under instance 1's pod,
// then scale 2 → 1 so the workload Delete op issues a graceful delete
// the frozen kubelet can never acknowledge. Returns the wedged pod
// (DeletionTimestamp set, original UID). Pause cleanup is already
// registered by pauseKindNode.
//
// The graceful delete is only issued once the Delete op's drain gate
// passes, which itself waits for the KCM to mark the paused node's
// pods NotReady (node-monitor grace, ~40s) — kindMediumTimeout covers
// that built-in latency.
func wedgeScaleDownOnPausedNode(ns, isvcName string) *corev1.Pod {
	fix := newCoordFixture(ns, WithISVCName(isvcName),
		WithoutDecoder(),
		WithEngineReplicas(2))
	applyFixture(Default, fix)
	deferFixtureCleanup(fix)

	ginkgo.By("waiting for both engine instances to reach Ready")
	Eventually(func(g Gomega) {
		insts := instanceStatuses(g, ns, isvcName, v1beta1.EngineComponent)
		g.Expect(insts).To(HaveLen(2))
		for _, s := range insts {
			g.Expect(s.Phase).To(Equal(v1beta1.OMENativeInstanceReady))
		}
	}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

	victim := aliveEnginePodForInstance(Default, ns, isvcName, 1)
	Expect(victim.Spec.NodeName).NotTo(BeEmpty(), "victim pod must be scheduled")

	ginkgo.By("pausing node " + victim.Spec.NodeName + " so its kubelet can never ack a termination")
	pauseKindNode(victim.Spec.NodeName)

	ginkgo.By("scaling engine 2 -> 1 to trigger the Delete op on instance 1")
	Eventually(func(g Gomega) {
		cur := freshISVC(g, ns, isvcName)
		one := 1
		cur.Spec.Engine.MinReplicas = &one
		g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
	}).WithTimeout(kindShortTimeout).WithPolling(kindPollInterval).Should(Succeed())

	ginkgo.By("waiting for the graceful delete to strand the pod Terminating")
	wedged := &corev1.Pod{}
	Eventually(func(g Gomega) {
		g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: ns, Name: victim.Name}, wedged)).To(Succeed())
		g.Expect(wedged.UID).To(Equal(victim.UID), "the wedge must be the original pod, not a successor")
		g.Expect(wedged.DeletionTimestamp).NotTo(BeNil())
	}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())
	return wedged
}

// Stuck-Terminating force-delete escalation. A scale-down's graceful
// pod delete against a dead node wedges forever: the kubelet never
// acknowledges the termination, the Delete op skips Terminating pods
// every pass, and the whole Component stays blocked. With
// lifecycle.forceDelete configured, a pod that is (a) overdue past its
// OWN deletionTimestamp + overdueSlack, (b) on a node provably dead
// for at least nodeUnreachableThreshold, and (c) free of foreign
// finalizers is force-deleted (grace 0, UID-preconditioned), evented
// (PodForceDeleted), and ledgered. With the key absent, the escalation
// must not exist at all.
//
// Node death is real here: docker-pausing the KIND node container
// freezes its kubelet, so the KCM marks the node Ready=Unknown after
// the node-monitor grace period (~40s) and adds the unreachable taint
// — the exact evidence the escalation reads.
var _ = ginkgo.Describe("OMENative force-delete escalation (KIND)", ginkgo.Ordered, func() {
	var ns *corev1.Namespace

	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "omenative-fd-")
		Expect(err).NotTo(HaveOccurred())
	})

	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("force-deletes a pod stuck Terminating on a dead node, events + ledgers it, and the scale-down completes", func() {
		const isvcName = "fd-escalate"
		configureLifecycleConfig(forceDeleteLifecycleJSON)

		wedged := wedgeScaleDownOnPausedNode(ns.Name, isvcName)
		node := wedged.Spec.NodeName
		podKey := client.ObjectKey{Namespace: ns.Name, Name: wedged.Name}

		// Within-grace veto: nothing may remove the pod before its own
		// deletionTimestamp + overdueSlack (the deletionTimestamp already
		// includes the pod's terminationGracePeriodSeconds). With the
		// kubelet frozen, a force-delete is the ONLY thing that could
		// remove the object, so the pod persisting until the deadline
		// pins the veto. The 2s margin keeps the guard clear of the
		// first legal escalation pass.
		deadline := wedged.DeletionTimestamp.Add(forceDeleteOverdueSlack)
		if guard := time.Until(deadline) - 2*time.Second; guard > 0 {
			ginkgo.By("holding inside the pod's own grace window: no early force-delete")
			Consistently(func(g Gomega) {
				cur := &corev1.Pod{}
				g.Expect(k8sClient.Get(suiteCtx, podKey, cur)).To(Succeed())
				g.Expect(cur.UID).To(Equal(wedged.UID))
				g.Expect(eventsWithReason(g, ns.Name, "PodForceDeleted")).To(BeEmpty(),
					"no force-delete may fire inside the grace window")
			}, guard, time.Second).Should(Succeed())
		}

		// The wedged pod must disappear DESPITE the paused kubelet —
		// only the escalation's grace-0 delete can do that. The Delete op
		// evaluates the escalation before its expectations gate (the
		// never-acked graceful delete's expectation cannot defer it), so
		// latency is governed only by the policy thresholds plus the
		// KCM's node-monitor grace (~40s) maturing the evidence;
		// kindLongTimeout is generous headroom.
		ginkgo.By("expecting the escalation to force-delete the wedged pod once the node is provably dead")
		Eventually(func(g Gomega) {
			cur := &corev1.Pod{}
			err := k8sClient.Get(suiteCtx, podKey, cur)
			if err != nil {
				g.Expect(apierrors.IsNotFound(err)).To(BeTrue())
				return
			}
			g.Expect(cur.UID).NotTo(Equal(wedged.UID),
				"the wedged pod object must disappear despite the paused kubelet")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("PodForceDeleted Warning names the pod and its node")
		Eventually(func(g Gomega) {
			evts := eventsWithReason(g, ns.Name, "PodForceDeleted")
			g.Expect(evts).NotTo(BeEmpty())
			named := false
			for _, e := range evts {
				g.Expect(e.Type).To(Equal(corev1.EventTypeWarning))
				if strings.Contains(e.Message, wedged.Name) && strings.Contains(e.Message, node) {
					named = true
				}
			}
			g.Expect(named).To(BeTrue(),
				"PodForceDeleted must name pod %s and node %s", wedged.Name, node)
		}).WithTimeout(kindShortTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("audit ledger records the terminal ForceDelete row keyed by the pod UID")
		Eventually(func(g Gomega) {
			var rows []auditEntryShape
			for _, e := range loadLedger(g, ns.Name, isvcName).Entries {
				if e.Reason == "ForceDelete" {
					rows = append(rows, e)
				}
			}
			g.Expect(rows).To(HaveLen(1))
			g.Expect(rows[0].RequestUUID).To(Equal(string(wedged.UID)))
			g.Expect(rows[0].Outcome).To(Equal("force-delete-unreachable"))
			g.Expect(rows[0].FromNode).To(Equal(node))
		}).WithTimeout(kindShortTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("scale-down completes: instance 1 removed from the IR")
		Eventually(func(g Gomega) {
			g.Expect(instanceStatuses(g, ns.Name, isvcName, v1beta1.EngineComponent)).To(HaveLen(1))
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})

	// A drained pod no operation waits on. A roll to an image that never
	// pulls ends the attempt with the Instance still Ready on its serving
	// pod and the failure on its record; its node is paused; the image is
	// rolled back, so the wreckage sweep drains and deletes the wedged
	// replacement, which its frozen kubelet never finishes, and the
	// Instance is Ready on its serving pod with no operation left to
	// re-read the Terminating pod. The Component pass still
	// force-deletes it once the node has been unreachable for the
	// threshold and the pod is past its own deadline plus the slack, with
	// the event and the ledger row, and the serving pod is never touched.
	ginkgo.It("force-deletes a drained pod no operation waits on once its node is dead", func() {
		const isvcName = "fd-abandoned"
		configureLifecycleConfig(forceDeleteAbandonedLifecycleJSON)

		fix := newCoordFixture(ns.Name, WithISVCName(isvcName),
			WithoutDecoder(),
			WithEngineReplicas(1),
			WithEngineImage("fake-serving:v1"))
		// One pod of the Component per node: the replacement lands away
		// from the pod it would replace, so pausing its node freezes
		// nothing else of the Instance.
		fix.ISVC.Spec.Engine.PodSpec.Affinity = &corev1.Affinity{PodAntiAffinity: &corev1.PodAntiAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{{
				LabelSelector: &metav1.LabelSelector{MatchLabels: map[string]string{constants.InferenceServicePodLabelKey: isvcName}},
				TopologyKey:   corev1.LabelHostname,
			}},
		}}
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		ginkgo.By("waiting for the engine Instance to reach Ready")
		Eventually(func(g Gomega) {
			insts := instanceStatuses(g, ns.Name, isvcName, v1beta1.EngineComponent)
			g.Expect(insts).To(HaveLen(1))
			g.Expect(insts[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
		source := aliveEnginePodForInstance(Default, ns.Name, isvcName, 0)
		Expect(source.Spec.NodeName).NotTo(BeEmpty(), "serving pod must be scheduled")

		ginkgo.By("rolling to an image that never pulls, so the replacement wedges and the attempt fails")
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, isvcName)
			cur.Spec.Engine.PodSpec.Containers[0].Image = unpullableImage
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindShortTimeout).WithPolling(kindPollInterval).Should(Succeed())
		var wedged *corev1.Pod
		Eventually(func(g Gomega) {
			wedged = nil
			for _, p := range listOMENativePods(g, ns.Name, isvcName) {
				if p.UID != source.UID && p.Spec.Containers[0].Image == unpullableImage && p.Spec.NodeName != "" {
					pod := p
					wedged = &pod
				}
			}
			g.Expect(wedged).NotTo(BeNil(), "the replacement must be bound to a node")
			ir := freshInferenceReplica(g, ns.Name, isvcName, v1beta1.EngineComponent)
			badRev := ir.Status.UpdateRevision
			g.Expect(badRev).NotTo(Equal(ir.Status.CurrentRevision), "the roll must still be in flight")
			g.Expect(findRetryBlock(ir.Status.RetryBlocks, badRev)).NotTo(BeNil(),
				"the failed attempt must count on the unpullable revision's retry ladder")
			insts := decodedInstanceStatuses(g, ir)
			g.Expect(insts).To(HaveLen(1))
			g.Expect(insts[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady),
				"the Instance serves on its source and reads Ready; the replacement is the attempt that failed")
			g.Expect(insts[0].Operation).To(BeNil(), "the ended attempt leaves no operation on the row")
			g.Expect(insts[0].TargetRevision).To(Equal(badRev), "the revision the failed attempt was for stays on the row")
			g.Expect(insts[0].LastFailure).NotTo(BeNil(), "the failed attempt must stay on the record")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
		Expect(wedged.Spec.NodeName).NotTo(Equal(source.Spec.NodeName), "the replacement must sit on its own node")
		node := wedged.Spec.NodeName
		podKey := client.ObjectKey{Namespace: ns.Name, Name: wedged.Name}

		ginkgo.By("pausing node " + node + " under the wedged replacement")
		pauseKindNode(node)

		ginkgo.By("rolling back: the wreckage sweep drains and deletes the replacement, which its frozen kubelet never finishes")
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, isvcName)
			cur.Spec.Engine.PodSpec.Containers[0].Image = "fake-serving:v1"
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindShortTimeout).WithPolling(kindPollInterval).Should(Succeed())
		drained := &corev1.Pod{}
		Eventually(func(g Gomega) {
			g.Expect(k8sClient.Get(suiteCtx, podKey, drained)).To(Succeed())
			g.Expect(drained.UID).To(Equal(wedged.UID), "the drained pod must be the original replacement, not a successor")
			g.Expect(drained.DeletionTimestamp).NotTo(BeNil())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("the Instance is Ready on its serving pod with no operation while the drained pod is still in the API")
		Eventually(func(g Gomega) {
			insts := instanceStatuses(g, ns.Name, isvcName, v1beta1.EngineComponent)
			g.Expect(insts).To(HaveLen(1))
			g.Expect(insts[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
			g.Expect(insts[0].Operation).To(BeNil())
			cur := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, podKey, cur)).To(Succeed())
			g.Expect(cur.UID).To(Equal(wedged.UID))
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Within-grace veto: nothing may remove the pod before its own
		// deletionTimestamp + overdueSlack, exactly as for a wave's victim.
		deadline := drained.DeletionTimestamp.Add(forceDeleteOverdueSlack)
		if guard := time.Until(deadline) - 2*time.Second; guard > 0 {
			ginkgo.By("holding inside the pod's own grace window: no early force-delete")
			Consistently(func(g Gomega) {
				cur := &corev1.Pod{}
				g.Expect(k8sClient.Get(suiteCtx, podKey, cur)).To(Succeed())
				g.Expect(cur.UID).To(Equal(wedged.UID))
				g.Expect(eventsWithReason(g, ns.Name, "PodForceDeleted")).To(BeEmpty(),
					"no force-delete may fire inside the grace window")
			}, guard, time.Second).Should(Succeed())
		}

		ginkgo.By("expecting the Component pass to force-delete the drained pod once the node is provably dead")
		Eventually(func(g Gomega) {
			cur := &corev1.Pod{}
			err := k8sClient.Get(suiteCtx, podKey, cur)
			if err != nil {
				g.Expect(apierrors.IsNotFound(err)).To(BeTrue())
				return
			}
			g.Expect(cur.UID).NotTo(Equal(wedged.UID),
				"the drained pod object must disappear despite the paused kubelet")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("PodForceDeleted Warning names the pod and its node")
		Eventually(func(g Gomega) {
			evts := eventsWithReason(g, ns.Name, "PodForceDeleted")
			g.Expect(evts).NotTo(BeEmpty())
			named := false
			for _, e := range evts {
				g.Expect(e.Type).To(Equal(corev1.EventTypeWarning))
				if strings.Contains(e.Message, wedged.Name) && strings.Contains(e.Message, node) {
					named = true
				}
			}
			g.Expect(named).To(BeTrue(), "PodForceDeleted must name pod %s and node %s", wedged.Name, node)
		}).WithTimeout(kindShortTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("audit ledger records the terminal ForceDelete row keyed by the pod UID")
		Eventually(func(g Gomega) {
			var rows []auditEntryShape
			for _, e := range loadLedger(g, ns.Name, isvcName).Entries {
				if e.Reason == "ForceDelete" {
					rows = append(rows, e)
				}
			}
			g.Expect(rows).To(HaveLen(1))
			g.Expect(rows[0].RequestUUID).To(Equal(string(wedged.UID)))
			g.Expect(rows[0].Outcome).To(Equal("force-delete-unreachable"))
			g.Expect(rows[0].FromNode).To(Equal(node))
		}).WithTimeout(kindShortTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("the serving pod and its row are untouched")
		Expect(aliveEnginePodForInstance(Default, ns.Name, isvcName, 0).UID).To(Equal(source.UID),
			"the pod on the live node must be the one that served throughout")
		insts := instanceStatuses(Default, ns.Name, isvcName, v1beta1.EngineComponent)
		Expect(insts).To(HaveLen(1))
		Expect(insts[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
	})

	ginkgo.It("config absent: the identical wedge is left alone — no force-delete, no event; unpause releases it", func() {
		const isvcName = "fd-control"
		configureLifecycleConfig(lifecycleNoForceDeleteJSON)

		wedged := wedgeScaleDownOnPausedNode(ns.Name, isvcName)
		node := wedged.Spec.NodeName
		podKey := client.ObjectKey{Namespace: ns.Name, Name: wedged.Name}

		// Only start the hold once the evidence WOULD be actionable —
		// pod overdue past deletionTimestamp+slack AND node dead past
		// the threshold — so the Consistently below is a real negative
		// control, not a within-grace tautology.
		ginkgo.By("waiting until the evidence would be actionable were the escalation configured")
		Eventually(func(g Gomega) {
			expectNodeNotReadyAtLeast(g, node, forceDeleteNodeThreshold)
			g.Expect(time.Now().After(wedged.DeletionTimestamp.Add(forceDeleteOverdueSlack))).To(BeTrue(),
				"pod must be overdue past its own deletionTimestamp + slack")
		}).WithTimeout(kindMediumTimeout).WithPolling(time.Second).Should(Succeed())

		ginkgo.By("holding: with lifecycle.forceDelete absent the escalation does not exist")
		Consistently(func(g Gomega) {
			cur := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, podKey, cur)).To(Succeed(),
				"the Terminating pod must persist with config absent")
			g.Expect(cur.UID).To(Equal(wedged.UID))
			g.Expect(cur.DeletionTimestamp).NotTo(BeNil())
			g.Expect(eventsWithReason(g, ns.Name, "PodForceDeleted")).To(BeEmpty())
			g.Expect(eventsWithReason(g, ns.Name, "PodDeleteBlockedByFinalizer")).To(BeEmpty())
		}, 2*forceDeleteOverdueSlack, time.Second).Should(Succeed())

		ginkgo.By("unpausing the node lets the normal teardown finish")
		resumeKindNode(node)
		Eventually(func(g Gomega) {
			err := k8sClient.Get(suiteCtx, podKey, &corev1.Pod{})
			g.Expect(apierrors.IsNotFound(err)).To(BeTrue(),
				"the recovered kubelet must complete the graceful termination")
			g.Expect(instanceStatuses(g, ns.Name, isvcName, v1beta1.EngineComponent)).To(HaveLen(1))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		Expect(eventsWithReason(Default, ns.Name, "PodForceDeleted")).To(BeEmpty(),
			"no force-delete may ever have fired with config absent")
	})
})
