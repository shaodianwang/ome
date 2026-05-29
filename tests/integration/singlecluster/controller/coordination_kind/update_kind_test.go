package coordination_kind

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/tests/integration/util"
)

// OMENative Update path specs ported from envtest update_test.go.
// chooseUpdateMode branches on:
//   - InPlaceIfPossible + image-only diff → in-place patch
//   - InPlaceOnly + ineligible (env-var) diff → reject + event
//   - RecreatePod → always recreate
//   - default fallback for non-image diffs → recreate
//
// Coverage already in sibling KIND specs:
//   - InPlaceIfPossible + image-only → behavior_l_inplace_kind L-1/2/3
//     (asserts UID preserved + serving budget + no surge)
//   - RecreatePod + image diff → behavior_b_recreatepod_kind B-1/2/3 +
//     recreate_pod_kind RP-1/2/3 (assert UID rotates + no surge)
//   - MinReplicas=2 in-place patch — covered indirectly by behavior_l
//     (3 engine replicas demonstrates the per-Instance dispatch loop)
//
// Ported below: the explicit InPlaceOnly + InPlaceUpdateNotPossible
// rejection path, and the env-var-falls-through-to-recreate path
// (which the in-place / recreate behavior tests don't directly assert
// since they exercise image-only diffs).
var _ = ginkgo.Describe("OMENative Update path (extras)", ginkgo.Ordered, func() {
	var ns *corev1.Namespace

	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "omenative-upd-")
		Expect(err).NotTo(HaveOccurred())
	})

	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	// InPlaceOnly strategy + env-var diff: must reject with
	// InPlaceUpdateNotPossible event, NOT touch the pod, NOT fall
	// through to recreate. A regression that downgrades to recreate
	// on InPlaceOnly would silently violate operator intent.
	ginkgo.It("InPlaceOnly strategy rejects an env-var diff (InPlaceUpdateNotPossible event, pod untouched)", func() {
		fix := newCoordFixture(ns.Name, WithISVCName("inplaceonly-isvc"),
			WithoutDecoder(),
			WithEngineImage("fake-serving:v1"))
		fix.ISVC.Spec.Engine.Lifecycle = &v1beta1.LifecycleSpec{
			UpdateStrategy: &v1beta1.UpdateStrategy{
				Type: v1beta1.UpdateStrategyInPlaceOnly,
			},
		}
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		ginkgo.By("waiting for the initial pod to be Ready")
		var origUID string
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "inplaceonly-isvc")
			cs := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.OMENative).NotTo(BeNil())
			g.Expect(cs.OMENative.InstanceStatuses).To(HaveLen(1))
			g.Expect(cs.OMENative.InstanceStatuses[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))

			pod := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
				Namespace: ns.Name, Name: "inplaceonly-isvc-engine-0-default-0",
			}, pod)).To(Succeed())
			origUID = string(pod.UID)
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("adding an env var (ineligible for in-place)")
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, "inplaceonly-isvc")
			cur.Spec.Engine.PodSpec.Containers[0].Env = append(
				cur.Spec.Engine.PodSpec.Containers[0].Env,
				corev1.EnvVar{Name: "SHOULD_BE_REJECTED", Value: "yes"},
			)
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("expecting an InPlaceUpdateNotPossible warning event")
		Eventually(func(g Gomega) {
			events := &corev1.EventList{}
			g.Expect(k8sClient.List(suiteCtx, events, client.InNamespace(ns.Name))).To(Succeed())
			matched := false
			for _, e := range events.Items {
				if e.Reason == "InPlaceUpdateNotPossible" && e.InvolvedObject.Name == "inplaceonly-isvc" {
					matched = true
					break
				}
			}
			g.Expect(matched).To(BeTrue(),
				"expected InPlaceUpdateNotPossible event; got %d events", len(events.Items))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("asserting the pod was NOT touched (UID unchanged, env var not applied)")
		Consistently(func(g Gomega) {
			pod := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
				Namespace: ns.Name, Name: "inplaceonly-isvc-engine-0-default-0",
			}, pod)).To(Succeed())
			g.Expect(string(pod.UID)).To(Equal(origUID),
				"InPlaceOnly rejection must not recreate the pod; UID changed")
			for _, e := range pod.Spec.Containers[0].Env {
				g.Expect(e.Name).NotTo(Equal("SHOULD_BE_REJECTED"),
					"env var must NOT be applied to the live pod under InPlaceOnly rejection")
			}
		}, "3s", kindPollInterval).Should(Succeed())
	})

	// env-var change (non-image diff) under the default strategy
	// (InPlaceIfPossible) must fall through to recreate — the pod UID
	// changes and Incarnation bumps. chooseUpdateMode regression that
	// routes env diffs through in-place would silently no-op.
	ginkgo.It("env-var change falls through to recreate (pod UID rotates, Incarnation bumps)", func() {
		fix := newCoordFixture(ns.Name, WithISVCName("recreate-env-isvc"),
			WithoutDecoder(),
			WithEngineImage("fake-serving:v1"))
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		ginkgo.By("waiting for the initial pod to be Ready at Incarnation=1")
		var origUID string
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "recreate-env-isvc")
			cs := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.OMENative).NotTo(BeNil())
			g.Expect(cs.OMENative.InstanceStatuses).To(HaveLen(1))
			g.Expect(cs.OMENative.InstanceStatuses[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
			g.Expect(cs.OMENative.InstanceStatuses[0].Incarnation).To(Equal(int64(1)))

			pod := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
				Namespace: ns.Name, Name: "recreate-env-isvc-engine-0-default-0",
			}, pod)).To(Succeed())
			origUID = string(pod.UID)
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("adding an env var to the engine container (non-image diff)")
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, "recreate-env-isvc")
			cur.Spec.Engine.PodSpec.Containers[0].Env = append(
				cur.Spec.Engine.PodSpec.Containers[0].Env,
				corev1.EnvVar{Name: "EXTRA_SETTING", Value: "yes"},
			)
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("expecting Incarnation to bump and a fresh pod (UID changed) with the new env var")
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "recreate-env-isvc")
			cs := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.OMENative).NotTo(BeNil())
			g.Expect(cs.OMENative.InstanceStatuses).To(HaveLen(1))
			g.Expect(cs.OMENative.InstanceStatuses[0].Incarnation).To(BeNumerically(">=", int64(2)),
				"recreate update must bump Incarnation; got %d", cs.OMENative.InstanceStatuses[0].Incarnation)

			pod := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
				Namespace: ns.Name, Name: "recreate-env-isvc-engine-0-default-0",
			}, pod)).To(Succeed())
			g.Expect(string(pod.UID)).NotTo(Equal(origUID),
				"recreate update must produce a new pod; UID stayed %s", origUID)
			foundEnv := false
			for _, e := range pod.Spec.Containers[0].Env {
				if e.Name == "EXTRA_SETTING" && e.Value == "yes" {
					foundEnv = true
					break
				}
			}
			g.Expect(foundEnv).To(BeTrue(),
				"new pod must reflect the new env var; got %+v", pod.Spec.Containers[0].Env)
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})
})
