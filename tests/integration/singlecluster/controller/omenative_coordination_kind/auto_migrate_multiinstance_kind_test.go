package omenative_coordination_kind

import (
	"time"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/tests/integration/util"
)

// Relocation isolation within one Component's index space.
//
// Two engine Instances land on the same node; only Instance 0 is broken
// there (fake-serving FAIL_ON_NODE narrowed by FAIL_ON_POD to Instance
// 0's pod — a node-scoped fault with no kubelet waiting reason), so
// Instance 1 converges Ready as the healthy sibling. The deadline
// disposition must scope its relocation directive to the stuck Instance
// alone:
//
//   - directive isolation: exactly one AutoRecover ledger entry, for
//     SourceInstance=0 — the healthy sibling never accrues one;
//   - rebuild isolation: only Instance 0's pod is recreated (with the
//     required hostname NotIn exclusion, landing off the suspect node);
//     the sibling's pod keeps its UID for the whole window — no
//     fleet-wide churn from one stuck Instance;
//   - budget/exclusion reset: the directive is pruned once Instance 0
//     proves its new placement Ready.
var _ = ginkgo.Describe("OMENative auto-relocation multi-Instance isolation (KIND)", ginkgo.Ordered, func() {
	var ns *corev1.Namespace
	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "omenative-auto-mig-mi-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("relocates only the stuck Instance; the healthy sibling's pod keeps its UID", func() {
		const (
			isvcName = "auto-mig-mi1"
			replicas = 2
			stuckPod = isvcName + "-engine-0-default-0"
		)

		workers := schedulableWorkerNodes(Default)
		Expect(len(workers)).To(BeNumerically(">=", 2),
			"relocation needs a second schedulable worker; got %v", workers)
		failNode, spareNodes := workers[0], workers[1:]

		configureFastDisposition()

		// Cordon every other worker so BOTH pods deterministically land
		// on the failing node whatever the worker count or the scheduler
		// (only Instance 0's pod breaks there).
		for _, node := range spareNodes {
			cordonNode(node)
		}

		fix := newCoordFixture(ns.Name, WithISVCName(isvcName),
			WithoutDecoder(),
			WithEngineReplicas(replicas),
			WithEngineUpdateStrategy(v1beta1.UpdateStrategyRecreatePod),
			WithEngineImage("fake-serving:v1"))
		// Pin maxReplicas=minReplicas so no autoscaler drift changes the
		// index space mid-test.
		fix.ISVC.Spec.Engine.MaxReplicas = replicas
		withNodeScopedFailure(&fix.ISVC.Spec.Engine.PodSpec.Containers[0], failNode, stuckPod)
		fix.ISVC.Spec.Engine.Lifecycle.MigrationPolicy = &v1beta1.MigrationPolicy{
			Mode: v1beta1.MigrationPolicyModeAuto,
		}
		fix.ISVC.Spec.Engine.Lifecycle.InstanceReadyTimeout = &metav1.Duration{Duration: 45 * time.Second}
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		ginkgo.By("waiting for Instance 0 to park stuck and Instance 1 to converge Ready, both on the failing node")
		var stuckUID types.UID
		var siblingName string
		var siblingUID types.UID
		Eventually(func(g Gomega) {
			insts := instanceStatuses(g, ns.Name, isvcName, v1beta1.EngineComponent)
			g.Expect(insts).To(HaveLen(replicas))
			byIdx := map[int32]v1beta1.OMENativeInstanceStatus{}
			for _, s := range insts {
				byIdx[s.Index] = s
			}
			g.Expect(byIdx[0].Phase).To(Equal(v1beta1.OMENativeInstanceCreating),
				"Instance 0 must be stuck Creating; got %+v", insts)
			g.Expect(byIdx[1].Phase).To(Equal(v1beta1.OMENativeInstanceReady),
				"Instance 1 must converge Ready on the same node; got %+v", insts)

			pod := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: ns.Name, Name: stuckPod}, pod)).To(Succeed())
			g.Expect(pod.Spec.NodeName).To(Equal(failNode))
			stuckUID = pod.UID

			snap, err := util.SnapshotPods(suiteCtx, k8sClient, ns.Name, isvcName)
			g.Expect(err).NotTo(HaveOccurred())
			for _, p := range snap.ByComponent[v1beta1.EngineComponent] {
				if p.Name == stuckPod {
					continue
				}
				g.Expect(p.Spec.NodeName).To(Equal(failNode),
					"sibling pod must share the failing node (every spare is cordoned)")
				siblingName, siblingUID = p.Name, p.UID
			}
			g.Expect(siblingName).NotTo(BeEmpty(), "expected a healthy sibling pod")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Give the rebuild somewhere off-suspect to land BEFORE the
		// deadline disposition fires.
		for _, node := range spareNodes {
			setNodeUnschedulable(node, false)
		}

		// Standing isolation invariant, threaded through every poll
		// below: the healthy sibling is never torn down or recreated
		// while its neighbor relocates.
		observeSibling := func(g Gomega) {
			pod := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: ns.Name, Name: siblingName}, pod)).To(Succeed(),
				"healthy sibling pod %s must survive its neighbor's relocation", siblingName)
			g.Expect(pod.UID).To(Equal(siblingUID),
				"healthy sibling pod %s must keep its UID (a new UID means it was recreated)", siblingName)
			g.Expect(pod.DeletionTimestamp).To(BeNil(),
				"healthy sibling pod %s must not be terminating", siblingName)
		}

		ginkgo.By("waiting for exactly one AutoRecover directive, scoped to Instance 0")
		Eventually(func(g Gomega) {
			observeSibling(g)
			entries := autoRecoverEntries(loadLedger(g, ns.Name, isvcName), "engine")
			g.Expect(entries).To(HaveLen(1),
				"exactly one directive (the stuck Instance); got %+v", entries)
			e := entries[0]
			g.Expect(e.SourceInstance).To(Equal(int32(0)),
				"the directive must target the stuck Instance, never the healthy sibling")
			g.Expect(e.Phase).To(Equal("Completed"))
			g.Expect(e.Outcome).To(Equal("relocate-recreate"))
			g.Expect(e.FromNode).To(Equal(failNode))
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("waiting for Instance 0's pod to be recreated off the suspect node with the NotIn exclusion")
		Eventually(func(g Gomega) {
			observeSibling(g)
			pod := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: ns.Name, Name: stuckPod}, pod)).To(Succeed())
			g.Expect(pod.UID).NotTo(Equal(stuckUID), "relocation must recreate the stuck pod (new UID)")
			g.Expect(podExcludesHostname(pod, failNode)).To(BeTrue(),
				"recreated pod must carry a required kubernetes.io/hostname NotIn[%s] term; affinity=%+v",
				failNode, pod.Spec.Affinity)
			g.Expect(pod.Spec.NodeName).NotTo(BeEmpty(), "recreated pod must schedule")
			g.Expect(pod.Spec.NodeName).NotTo(Equal(failNode),
				"recreated pod must land off the suspect node")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("waiting for both Instances Ready")
		Eventually(func(g Gomega) {
			observeSibling(g)
			insts := instanceStatuses(g, ns.Name, isvcName, v1beta1.EngineComponent)
			g.Expect(insts).To(HaveLen(replicas))
			for _, s := range insts {
				g.Expect(s.Phase).To(Equal(v1beta1.OMENativeInstanceReady),
					"instance %d must be Ready; got %+v", s.Index, insts)
			}
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("holding the isolation invariant: sibling untouched, no directive ever lands for it")
		Consistently(func(g Gomega) {
			observeSibling(g)
			for _, e := range autoRecoverEntries(loadLedger(g, ns.Name, isvcName), "engine") {
				g.Expect(e.SourceInstance).To(Equal(int32(0)),
					"no AutoRecover directive may ever target the healthy sibling; got %+v", e)
			}
		}, 10*time.Second, 200*time.Millisecond).Should(Succeed())

		ginkgo.By("expecting the directive pruned once Instance 0 is Ready")
		Eventually(func(g Gomega) {
			entries := autoRecoverEntries(loadLedger(g, ns.Name, isvcName), "engine")
			g.Expect(entries).To(BeEmpty(),
				"Ready proves the placement; exclusion memory and budget must reset; got %+v", entries)
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})
})
