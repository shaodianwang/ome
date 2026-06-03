package omenative_coordination_kind

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/tests/integration/util"
)

// objectMeta is a tiny helper for the pod-delete fixtures below. The
// fixtures only need Name + Namespace to address the pod.
func objectMeta(ns, name string) metav1.ObjectMeta {
	return metav1.ObjectMeta{Namespace: ns, Name: name}
}

// foregroundDeletion is the propagation policy the partial-failure
// tests use. Block-on-pod-purge so the controller sees an empty pod
// set before the next reconcile.
func foregroundDeletion() *metav1.DeletionPropagation {
	p := metav1.DeletePropagationForeground
	return &p
}

// Partial-failure specs. To provoke a failure on real kubelet (which
// owns pod status), we delete the pod outright: the Restart trigger
// fires on "live pod count below desired" (see
// ops/restart.go::detectRestartTrigger), driving the controller-side
// recovery path.
//
// Disabling the Restart path (RestartPolicy=None) flips the contract:
// a deleted pod must NOT be brought back; siblings must be untouched.
// PF-3 pins that negative case.
var _ = ginkgo.Describe("partial failure recovery", ginkgo.Ordered, func() {
	var ns *corev1.Namespace
	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "pf-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("PF-1 deleting pod at instance 1 restarts only that Instance; siblings untouched", func() {
		fix := newCoordFixture(ns.Name, WithISVCName("pf1"),
			WithoutDecoder(),
			WithEngineReplicas(3),
			WithEngineImage("fake-serving:v1"))
		// Single-pod default is RestartPolicy=None — deleted pods get
		// silently recreated by the Create pass without bumping
		// Incarnation. Opt into RecreateInstance so the Restart path
		// fires + Incarnation bumps. PF-3 below pins the None
		// behavior as the negative.
		restartRecreate := v1beta1.InstanceRestartPolicyRecreateInstance
		fix.ISVC.Spec.Engine.Lifecycle = &v1beta1.LifecycleSpec{
			RestartPolicy: &restartRecreate,
		}
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		// Capture initial Incarnation per instance.
		initialInc := map[int32]int64{}
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "pf1")
			cs := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.Lifecycle).NotTo(BeNil())
			g.Expect(cs.Lifecycle.InstanceStatuses).To(HaveLen(3))
			for _, s := range cs.Lifecycle.InstanceStatuses {
				g.Expect(s.Phase).To(Equal(v1beta1.OMENativeInstanceReady),
					"instance %d must be Ready at baseline", s.Index)
				initialInc[s.Index] = s.Incarnation
			}
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Delete the pod for instance 1 (force, immediate). kubelet
		// + apiserver remove it; live pod count drops; detectRestartTrigger
		// fires only on instance 1.
		grace := int64(0)
		Expect(k8sClient.Delete(suiteCtx, &corev1.Pod{
			ObjectMeta: objectMeta(ns.Name, "pf1-engine-1-default-0"),
		}, &client.DeleteOptions{
			GracePeriodSeconds: &grace,
			PropagationPolicy:  foregroundDeletion(),
		})).To(SatisfyAny(Succeed(), MatchError(apierrors.IsNotFound, "not found is OK")))

		// Instance 1 Incarnation bumps; instances 0 + 2 are untouched;
		// all three converge back to Ready.
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "pf1")
			cs := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.Lifecycle).NotTo(BeNil())
			g.Expect(cs.Lifecycle.InstanceStatuses).To(HaveLen(3))
			byIdx := map[int32]int64{}
			phases := map[int32]v1beta1.OMENativeInstancePhase{}
			for _, s := range cs.Lifecycle.InstanceStatuses {
				byIdx[s.Index] = s.Incarnation
				phases[s.Index] = s.Phase
			}
			g.Expect(byIdx[1]).To(BeNumerically(">", initialInc[1]),
				"PF-1: instance 1 must have bumped Incarnation; got %d, was %d", byIdx[1], initialInc[1])
			g.Expect(byIdx[0]).To(Equal(initialInc[0]),
				"PF-1: instance 0 Incarnation must NOT change; Restart leaked to sibling")
			g.Expect(byIdx[2]).To(Equal(initialInc[2]),
				"PF-1: instance 2 Incarnation must NOT change; Restart leaked to sibling")
			g.Expect(phases[0]).To(Equal(v1beta1.OMENativeInstanceReady))
			g.Expect(phases[1]).To(Equal(v1beta1.OMENativeInstanceReady),
				"PF-1: failed instance 1 must converge back to Ready after Restart")
			g.Expect(phases[2]).To(Equal(v1beta1.OMENativeInstanceReady))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})

	ginkgo.It("PF-2 deleting all 3 pods simultaneously restarts each Instance independently", func() {
		fix := newCoordFixture(ns.Name, WithISVCName("pf2"),
			WithoutDecoder(),
			WithEngineReplicas(3),
			WithEngineImage("fake-serving:v1"))
		restartRecreate := v1beta1.InstanceRestartPolicyRecreateInstance
		fix.ISVC.Spec.Engine.Lifecycle = &v1beta1.LifecycleSpec{
			RestartPolicy: &restartRecreate,
		}
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		initialInc := map[int32]int64{}
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "pf2")
			cs := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.Lifecycle).NotTo(BeNil(), "engine OMENative status must populate before delete")
			g.Expect(cs.Lifecycle.InstanceStatuses).To(HaveLen(3))
			for _, s := range cs.Lifecycle.InstanceStatuses {
				g.Expect(s.Phase).To(Equal(v1beta1.OMENativeInstanceReady))
				initialInc[s.Index] = s.Incarnation
			}
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Delete all 3 pods. Each Instance's Restart cycle is keyed
		// independently in the per-Component expectations cache; they
		// must NOT serialize or deadlock.
		grace := int64(0)
		for _, name := range []string{"pf2-engine-0-default-0", "pf2-engine-1-default-0", "pf2-engine-2-default-0"} {
			_ = k8sClient.Delete(suiteCtx, &corev1.Pod{ObjectMeta: objectMeta(ns.Name, name)}, &client.DeleteOptions{
				GracePeriodSeconds: &grace, PropagationPolicy: foregroundDeletion(),
			})
		}

		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "pf2")
			cs := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.Lifecycle).NotTo(BeNil())
			g.Expect(cs.Lifecycle.InstanceStatuses).To(HaveLen(3))
			for _, s := range cs.Lifecycle.InstanceStatuses {
				g.Expect(s.Phase).To(Equal(v1beta1.OMENativeInstanceReady),
					"PF-2: instance %d must converge back to Ready after Restart", s.Index)
				g.Expect(s.Incarnation).To(BeNumerically(">", initialInc[s.Index]),
					"PF-2: instance %d Incarnation must bump", s.Index)
			}
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})

	ginkgo.It("PF-3 RestartPolicy=None recreates the missing pod via the Create pass without bumping Incarnation", func() {
		fix := newCoordFixture(ns.Name, WithISVCName("pf3"),
			WithoutDecoder(),
			WithEngineReplicas(3),
			WithEngineImage("fake-serving:v1"))
		// Default single-pod RestartPolicy is None — the Restart pass
		// is skipped, so a deleted pod is brought back by the Create
		// pass without an Incarnation bump. The contract this pins:
		// Restart and Create are distinct paths, and the per-Instance
		// lifecycle token (Incarnation) only advances when the Restart
		// path explicitly fires.
		restartNone := v1beta1.InstanceRestartPolicyNone
		fix.ISVC.Spec.Engine.Lifecycle = &v1beta1.LifecycleSpec{
			RestartPolicy: &restartNone,
		}
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		initialInc := map[int32]int64{}
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "pf3")
			cs := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.Lifecycle).NotTo(BeNil())
			g.Expect(cs.Lifecycle.InstanceStatuses).To(HaveLen(3))
			for _, s := range cs.Lifecycle.InstanceStatuses {
				g.Expect(s.Phase).To(Equal(v1beta1.OMENativeInstanceReady))
				initialInc[s.Index] = s.Incarnation
			}
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Delete pod for instance 1. With RestartPolicy=None the
		// Restart path stays inert; the Create pass alone brings the
		// pod back, without bumping Incarnation.
		grace := int64(0)
		_ = k8sClient.Delete(suiteCtx, &corev1.Pod{ObjectMeta: objectMeta(ns.Name, "pf3-engine-1-default-0")}, &client.DeleteOptions{
			GracePeriodSeconds: &grace, PropagationPolicy: foregroundDeletion(),
		})

		// Eventually the controller's Create pass brings the pod back
		// AND all 3 instances converge to Ready AND Incarnation never
		// bumped on any of them. The pod-Get is inside the Eventually
		// so the status-Ready assertion can't race a stale post-Ready
		// snapshot from before the controller observed the delete.
		Eventually(func(g Gomega) {
			pod := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Namespace: ns.Name, Name: "pf3-engine-1-default-0"}, pod)).To(Succeed(),
				"PF-3: Create pass should have brought the deleted pod back even under RestartPolicy=None")
			g.Expect(pod.Status.Phase).To(Equal(corev1.PodRunning),
				"PF-3: replacement pod must be Running")

			c := freshISVC(g, ns.Name, "pf3")
			cs := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.Lifecycle).NotTo(BeNil())
			g.Expect(cs.Lifecycle.InstanceStatuses).To(HaveLen(3))
			for _, s := range cs.Lifecycle.InstanceStatuses {
				g.Expect(s.Phase).To(Equal(v1beta1.OMENativeInstanceReady),
					"PF-3: instance %d must converge back to Ready via Create pass", s.Index)
				g.Expect(s.Incarnation).To(Equal(initialInc[s.Index]),
					"PF-3: instance %d Incarnation must NOT bump under RestartPolicy=None (got %d, was %d)", s.Index, s.Incarnation, initialInc[s.Index])
			}
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})
})
