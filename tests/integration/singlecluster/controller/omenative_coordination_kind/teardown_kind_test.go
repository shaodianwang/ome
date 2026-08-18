package omenative_coordination_kind

import (
	"time"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferencereplica"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/irprojector"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/v1beta1convert"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/podreadiness"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	"github.com/ome-projects/ome/tests/integration/kindfix"
	"github.com/ome-projects/ome/tests/integration/util"
)

// Lifecycle seeds for the teardown specs. Deadlines are seeded short so
// the specs fit the suite budget; where a deadline is present it is
// sized so it must NOT fire — the reconciled teardown (or the
// force-delete escalation) has to finish first.
const (
	// TD-DRAIN: teardown deadline present but generous — completion must
	// come from the drain-then-delete pipeline, never the deadline.
	teardownDrainLifecycleJSON = `{"teardown":{"deadline":"3m"}}`
	// TD-ESCALATE: force-delete escalation active UNDER teardown; the 5m
	// teardown deadline is a backstop that must stay unfired.
	teardownForceDeleteLifecycleJSON = `{"forceDelete":{"overdueSlack":"15s","nodeUnreachableThreshold":"30s"},"teardown":{"deadline":"5m"}}`
	// TD-HANDOFF: deadline configured so the strict-hold TeardownBlocked
	// warning never fires — the spec asserts zero teardown events.
	teardownHandoffLifecycleJSON = `{"teardown":{"deadline":"5m"}}`
)

// irKeyFor returns the object key of the projected IR for one Component.
func irKeyFor(namespace, isvcName string, component v1beta1.ComponentType) client.ObjectKey {
	return client.ObjectKey{Namespace: namespace, Name: irprojector.InferenceReplicaName(isvcName, component)}
}

// podPublishedReady reports whether serviceName's EndpointSlices publish
// an endpoint targeting podName with Ready=true — i.e. kube-proxy still
// routes traffic to the pod.
func podPublishedReady(g Gomega, namespace, serviceName, podName string) bool {
	slices := &discoveryv1.EndpointSliceList{}
	g.Expect(k8sClient.List(suiteCtx, slices,
		client.InNamespace(namespace),
		client.MatchingLabels{discoveryv1.LabelServiceName: serviceName},
	)).To(Succeed())
	for _, sl := range slices.Items {
		for _, ep := range sl.Endpoints {
			if ep.TargetRef == nil || ep.TargetRef.Name != podName {
				continue
			}
			if ep.Conditions.Ready != nil && *ep.Conditions.Ready {
				return true
			}
		}
	}
	return false
}

// Reconciled IR teardown. An IR with a DeletionTimestamp reconciles as
// "desired instances = zero": every instance runs the ordinary Delete
// pipeline (drain -> graceful delete -> stuck-Terminating force-delete
// escalation) and the ome.io/ir-teardown finalizer lifts only when component
// Pods and owned PodGroups are absent. A Terminating IR without the finalizer
// is left entirely to background GC.
var _ = ginkgo.Describe("OMENative reconciled IR teardown (KIND)", ginkgo.Ordered, func() {
	var ns *corev1.Namespace

	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "omenative-td-")
		Expect(err).NotTo(HaveOccurred())
	})

	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	// TD-DRAIN — deleting the ISVC must not kill pods while they are
	// still routable: each pod is taken out of serving (readiness gate
	// flipped, endpoint no longer published Ready) BEFORE its object is
	// deleted, the IR holds Terminating with the finalizer for the whole
	// drain, and everything is gone at the end.
	ginkgo.It("TEARDOWN-DRAINS: ISVC delete drains pods out of serving before deleting them; IR holds Terminating until clean", func() {
		const isvcName = "td-drain"
		configureLifecycleConfig(teardownDrainLifecycleJSON)

		fix := newCoordFixture(ns.Name, WithISVCName(isvcName),
			WithoutDecoder(),
			WithEngineReplicas(2))
		applyFixture(Default, fix)
		// The spec deletes the ISVC itself; only the cluster-scoped
		// CBM/CSR need explicit cleanup.
		ginkgo.DeferCleanup(func() {
			_ = k8sClient.Delete(suiteCtx, fix.CBM)
			_ = k8sClient.Delete(suiteCtx, fix.CSR)
		})

		ginkgo.By("waiting for both engine instances to reach Ready")
		Eventually(func(g Gomega) {
			insts := instanceStatuses(g, ns.Name, isvcName, v1beta1.EngineComponent)
			g.Expect(insts).To(HaveLen(2))
			for _, s := range insts {
				g.Expect(s.Phase).To(Equal(v1beta1.OMENativeInstanceReady))
			}
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		pods := listOMENativePods(Default, ns.Name, isvcName)
		Expect(pods).To(HaveLen(2), "expected exactly 2 single-pod engine instances")
		revHash := pods[0].Labels[query.LabelRevisionHash]
		Expect(revHash).NotTo(BeEmpty(), "pods must carry the revision-hash label")
		tracked := map[string]types.UID{}
		for _, p := range pods {
			tracked[p.Name] = p.UID
		}
		component := v1beta1convert.ComponentTypeToWorkload(v1beta1.EngineComponent)
		routedSvc := query.PerRevisionServiceName(isvcName, component, revHash)

		ginkgo.By("baseline: both pods are published Ready by the routed per-revision Service")
		Eventually(func(g Gomega) {
			for name := range tracked {
				g.Expect(podPublishedReady(g, ns.Name, routedSvc, name)).To(BeTrue(),
					"pod %s must be a Ready endpoint of %s before the delete", name, routedSvc)
			}
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		irKey := irKeyFor(ns.Name, isvcName, v1beta1.EngineComponent)
		Expect(k8sClient.Delete(suiteCtx, fix.ISVC)).To(Succeed())

		// Ordering watch: each poll records, per pod, whether it was
		// observed OUT of serving while the pod object still existed. The
		// serving gate remains observable while a pod is Terminating; for
		// a live pod, removal from the routed Service also counts. A pod
		// that disappears without either observation violates the drain
		// contract. The IR must be seen Terminating WITH
		// the finalizer while pods remain.
		drainSeen := map[string]bool{}
		gone := map[string]bool{}
		irHoldSeen := false
		ginkgo.By("watching the drain-before-delete ordering while teardown runs")
		Eventually(func(g Gomega) {
			ir := &v1beta1.InferenceReplica{}
			if err := k8sClient.Get(suiteCtx, irKey, ir); err == nil && ir.DeletionTimestamp != nil {
				g.Expect(controllerutil.ContainsFinalizer(ir, inferencereplica.TeardownFinalizer)).To(BeTrue(),
					"a Terminating IR must hold the teardown finalizer while its pods drain")
				irHoldSeen = true
			}
			for name, uid := range tracked {
				if gone[name] {
					continue
				}
				pod := &corev1.Pod{}
				err := k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: ns.Name, Name: name}, pod)
				if apierrors.IsNotFound(err) || (err == nil && pod.UID != uid) {
					g.Expect(drainSeen[name]).To(BeTrue(),
						"pod %s was deleted without ever being observed out of serving — teardown must drain before it deletes", name)
					gone[name] = true
					continue
				}
				g.Expect(err).NotTo(HaveOccurred())
				if !podreadiness.IsServing(pod) {
					drainSeen[name] = true
				}
				if pod.DeletionTimestamp != nil {
					continue
				}
				if !podPublishedReady(g, ns.Name, routedSvc, name) {
					drainSeen[name] = true
				}
			}
			g.Expect(gone).To(HaveLen(len(tracked)), "every pod must be torn down; drained so far: %v", drainSeen)
			g.Expect(irHoldSeen).To(BeTrue(), "the IR was never observed Terminating with the finalizer")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("IR completes and vanishes once no pod remains")
		Eventually(func(g Gomega) {
			err := k8sClient.Get(suiteCtx, irKey, &v1beta1.InferenceReplica{})
			g.Expect(apierrors.IsNotFound(err)).To(BeTrue(), "IR should be fully gone; got err=%v", err)
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("namespace clean: no component pods, no per-revision Services, no headless Service")
		headlessName := query.HeadlessServiceName(isvcName, component)
		Eventually(func(g Gomega) {
			g.Expect(listOMENativePods(g, ns.Name, isvcName)).To(BeEmpty())
			g.Expect(listPerRevisionServices(g, ns.Name)).To(BeEmpty())
			err := k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: ns.Name, Name: headlessName}, &corev1.Service{})
			g.Expect(apierrors.IsNotFound(err)).To(BeTrue(), "headless Service should be deleted; got err=%v", err)
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		Expect(eventsWithReason(Default, ns.Name, inferencereplica.ReasonTeardownDeadlineExceeded)).To(BeEmpty(),
			"the drain pipeline, not the teardown deadline, must complete the teardown")
	})

	// TD-ESCALATE — a pod on a dead node wedges Terminating during
	// teardown; the force-delete escalation must clear it (the finalizer
	// keeps a controller responsible for the orphan), the IR must then
	// complete, and a same-name ISVC created immediately after must come
	// up cleanly — its stable-named pod is a fresh object, not a leftover
	// stuck behind an AlreadyExists collision.
	ginkgo.It("TEARDOWN-ESCALATES: a pod wedged on a dead node is force-deleted under teardown, and a same-name ISVC recreate succeeds", func() {
		const isvcName = "td-escalate"
		configureLifecycleConfig(teardownForceDeleteLifecycleJSON)

		workers := schedulableWorkerNodes(Default)
		Expect(len(workers)).To(BeNumerically(">=", 2), "spec needs 2 workers: one to pin onto, one to cordon")
		pinned := workers[0]
		for _, other := range workers[1:] {
			cordonNode(other)
		}

		fix := newCoordFixture(ns.Name, WithISVCName(isvcName), WithoutDecoder())
		applyFixture(Default, fix)
		ginkgo.DeferCleanup(func() {
			_ = k8sClient.Delete(suiteCtx, fix.CBM)
			_ = k8sClient.Delete(suiteCtx, fix.CSR)
		})

		ginkgo.By("waiting for the single engine instance to reach Ready on the pinned node")
		Eventually(func(g Gomega) {
			insts := instanceStatuses(g, ns.Name, isvcName, v1beta1.EngineComponent)
			g.Expect(insts).To(HaveLen(1))
			g.Expect(insts[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		victim := aliveEnginePodForInstance(Default, ns.Name, isvcName, 0)
		Expect(victim.Spec.NodeName).To(Equal(pinned),
			"cordoning every other worker must pin the pod onto %s", pinned)
		podKey := client.ObjectKey{Namespace: ns.Name, Name: victim.Name}
		irKey := irKeyFor(ns.Name, isvcName, v1beta1.EngineComponent)

		ginkgo.By("pausing node " + pinned + " so its kubelet can never ack a termination")
		pauseKindNode(pinned)

		ginkgo.By("deleting the ISVC while the pod's node is dead")
		Expect(k8sClient.Delete(suiteCtx, fix.ISVC)).To(Succeed())

		// The teardown Delete op drains, then issues a graceful delete
		// the frozen kubelet can never acknowledge: the pod wedges
		// Terminating while the IR holds Terminating with the finalizer
		// — a controller stays responsible for the orphan.
		ginkgo.By("waiting for the graceful delete to strand the pod Terminating under the finalizer hold")
		Eventually(func(g Gomega) {
			wedged := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, podKey, wedged)).To(Succeed())
			g.Expect(wedged.UID).To(Equal(victim.UID), "the wedge must be the original pod")
			g.Expect(wedged.DeletionTimestamp).NotTo(BeNil())
			ir := &v1beta1.InferenceReplica{}
			g.Expect(k8sClient.Get(suiteCtx, irKey, ir)).To(Succeed())
			g.Expect(ir.DeletionTimestamp).NotTo(BeNil())
			g.Expect(controllerutil.ContainsFinalizer(ir, inferencereplica.TeardownFinalizer)).To(BeTrue(),
				"the IR must hold the teardown finalizer while the pod wedges")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Only the escalation's grace-0 delete can remove the object
		// while the kubelet is paused.
		ginkgo.By("expecting the force-delete escalation to clear the wedge despite the paused kubelet")
		Eventually(func(g Gomega) {
			cur := &corev1.Pod{}
			err := k8sClient.Get(suiteCtx, podKey, cur)
			if err != nil {
				g.Expect(apierrors.IsNotFound(err)).To(BeTrue())
				return
			}
			g.Expect(cur.UID).NotTo(Equal(victim.UID),
				"the wedged pod object must disappear despite the paused kubelet")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("PodForceDeleted fires — the escalation ran under teardown")
		Eventually(func(g Gomega) {
			g.Expect(eventsWithReason(g, ns.Name, "PodForceDeleted")).NotTo(BeEmpty())
		}).WithTimeout(kindShortTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("teardown completes: the IR vanishes")
		Eventually(func(g Gomega) {
			err := k8sClient.Get(suiteCtx, irKey, &v1beta1.InferenceReplica{})
			g.Expect(apierrors.IsNotFound(err)).To(BeTrue(), "IR should be fully gone; got err=%v", err)
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		Expect(eventsWithReason(Default, ns.Name, inferencereplica.ReasonTeardownDeadlineExceeded)).To(BeEmpty(),
			"the escalation, not the teardown deadline, must clear the wedge")

		ginkgo.By("resuming the node and immediately recreating a same-name ISVC")
		resumeKindNode(pinned)
		// Same ISVC name derives the same CBM/CSR names, which still
		// exist — create only the ISVC.
		fix2 := newCoordFixture(ns.Name, WithISVCName(isvcName), WithoutDecoder())
		Expect(k8sClient.Create(suiteCtx, fix2.ISVC)).To(Succeed())
		ginkgo.DeferCleanup(func() {
			kindfix.CleanupModelRuntime(suiteCtx, k8sClient, nil, nil, fix2.ISVC)
		})

		// The acceptance: the recreate converges. Stable pod naming means
		// the new instance reuses the old pod name — it must be a FRESH
		// object (different UID), not a leftover blocking creation.
		ginkgo.By("the same-name recreate reaches Ready with a fresh pod")
		Eventually(func(g Gomega) {
			insts := instanceStatuses(g, ns.Name, isvcName, v1beta1.EngineComponent)
			g.Expect(insts).To(HaveLen(1))
			g.Expect(insts[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
			pod := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, podKey, pod)).To(Succeed())
			g.Expect(pod.UID).NotTo(Equal(victim.UID), "the recreated pod must be a fresh object")
			g.Expect(pod.DeletionTimestamp).To(BeNil())
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})

	// TD-HANDOFF — a Terminating IR WITHOUT the teardown finalizer
	// belongs to background GC: the controller must not re-add the
	// finalizer, must emit no teardown events, and once the IR is gone
	// the projector recreates a fresh one (the parent ISVC still
	// exists). A spec-owned hold finalizer keeps the Terminating state
	// observable; the strips are plain finalizer-only client updates
	// (the kind manager runs no webhooks; the IR validator's
	// finalizer-only exemption is pinned by the webhook envtest suite).
	ginkgo.It("TEARDOWN-HANDOFF: a Terminating IR without the finalizer is left to GC and the projector rebuilds a fresh IR", func() {
		const isvcName = "td-handoff"
		const holdFinalizer = "kind-test.ome.io/teardown-hold"
		configureLifecycleConfig(teardownHandoffLifecycleJSON)

		fix := newCoordFixture(ns.Name, WithISVCName(isvcName), WithoutDecoder())
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		ginkgo.By("waiting for the single engine instance to reach Ready")
		Eventually(func(g Gomega) {
			insts := instanceStatuses(g, ns.Name, isvcName, v1beta1.EngineComponent)
			g.Expect(insts).To(HaveLen(1))
			g.Expect(insts[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		irKey := irKeyFor(ns.Name, isvcName, v1beta1.EngineComponent)
		oldIR := freshInferenceReplica(Default, ns.Name, isvcName, v1beta1.EngineComponent)
		oldIRUID := oldIR.UID
		victim := aliveEnginePodForInstance(Default, ns.Name, isvcName, 0)

		// Swap the finalizer set to the spec's hold finalizer, then
		// delete the IR directly (the operator-delete path). The hold
		// keeps the object visible as Terminating; dropping the teardown
		// finalizer routes it to the GC hand-off branch.
		ginkgo.By("replacing the teardown finalizer with a hold finalizer, then deleting the IR")
		Eventually(func(g Gomega) {
			fresh := freshInferenceReplica(g, ns.Name, isvcName, v1beta1.EngineComponent)
			fresh.Finalizers = []string{holdFinalizer}
			g.Expect(k8sClient.Update(suiteCtx, fresh)).To(Succeed())
		}).WithTimeout(kindShortTimeout).WithPolling(kindPollInterval).Should(Succeed())
		Expect(k8sClient.Delete(suiteCtx, oldIR)).To(Succeed())

		// The controller re-adds the finalizer on every live reconcile,
		// so it may have won the strip-vs-delete race. Once Terminating
		// it never re-adds, so a post-delete strip is deterministic —
		// retry until the IR is observed Terminating with ONLY the hold.
		ginkgo.By("ensuring the Terminating IR carries only the hold finalizer")
		Eventually(func(g Gomega) {
			fresh := freshInferenceReplica(g, ns.Name, isvcName, v1beta1.EngineComponent)
			g.Expect(fresh.UID).To(Equal(oldIRUID))
			g.Expect(fresh.DeletionTimestamp).NotTo(BeNil())
			if controllerutil.ContainsFinalizer(fresh, inferencereplica.TeardownFinalizer) {
				controllerutil.RemoveFinalizer(fresh, inferencereplica.TeardownFinalizer)
				g.Expect(k8sClient.Update(suiteCtx, fresh)).To(Succeed())
			}
			g.Expect(fresh.Finalizers).To(Equal([]string{holdFinalizer}))
		}).WithTimeout(kindShortTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("hand-off window: the controller does not interfere")
		Consistently(func(g Gomega) {
			fresh := freshInferenceReplica(g, ns.Name, isvcName, v1beta1.EngineComponent)
			g.Expect(fresh.UID).To(Equal(oldIRUID))
			g.Expect(fresh.DeletionTimestamp).NotTo(BeNil())
			g.Expect(controllerutil.ContainsFinalizer(fresh, inferencereplica.TeardownFinalizer)).To(BeFalse(),
				"the controller must never re-add the teardown finalizer to a Terminating IR")
			g.Expect(eventsWithReason(g, ns.Name, inferencereplica.ReasonTeardownBlocked)).To(BeEmpty())
			g.Expect(eventsWithReason(g, ns.Name, inferencereplica.ReasonTeardownDeadlineExceeded)).To(BeEmpty())
		}, 10*time.Second, kindPollInterval).Should(Succeed())

		ginkgo.By("releasing the hold: the IR is removed without controller involvement")
		Eventually(func(g Gomega) {
			fresh := &v1beta1.InferenceReplica{}
			err := k8sClient.Get(suiteCtx, irKey, fresh)
			if apierrors.IsNotFound(err) {
				return
			}
			g.Expect(err).NotTo(HaveOccurred())
			if fresh.UID != oldIRUID {
				return
			}
			fresh.Finalizers = nil
			g.Expect(k8sClient.Update(suiteCtx, fresh)).To(Succeed())
		}).WithTimeout(kindShortTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// The parent ISVC still exists, so the projector must rebuild a
		// FRESH IR — operator IR deletes are drain-then-recreate.
		ginkgo.By("the projector recreates a fresh IR")
		Eventually(func(g Gomega) {
			fresh := freshInferenceReplica(g, ns.Name, isvcName, v1beta1.EngineComponent)
			g.Expect(fresh.UID).NotTo(Equal(oldIRUID), "a fresh IR must replace the GC'd one")
			g.Expect(fresh.DeletionTimestamp).To(BeNil())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Old pods die with the old IR via ownerRef GC; the fresh IR
		// rebuilds the stable-named pod as a new object and converges.
		ginkgo.By("the fresh IR converges: same pod name, new pod UID, Ready")
		Eventually(func(g Gomega) {
			insts := instanceStatuses(g, ns.Name, isvcName, v1beta1.EngineComponent)
			g.Expect(insts).To(HaveLen(1))
			g.Expect(insts[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
			pod := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: ns.Name, Name: victim.Name}, pod)).To(Succeed())
			g.Expect(pod.UID).NotTo(Equal(victim.UID), "the rebuilt pod must be a fresh object")
			g.Expect(pod.DeletionTimestamp).To(BeNil())
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})
})
