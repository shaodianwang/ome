package coordination_kind

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/tests/integration/util"
)

// KCG-2 — RecreateISVCOnPodRestart cascades a pod failure across every
// OMENative-managed Component in the ISVC.
//
// Gap-list item #2. `events RestartTriggered fires when a pod is deleted
// under RecreateInstance policy` covers `RecreateInstanceOnPodRestart`,
// which only recreates the affected Instance. `RecreateISVCOnPodRestart`
// is a DIFFERENT policy: when ANY managed pod restarts, EVERY pod in
// EVERY OMENative-managed Component-Instance is recreated. This is the
// safety hatch for tightly-coupled PD topologies that share KV-cache or
// other cross-Component state.
//
// The cascade invariant is: kill the engine pod, both the engine pod
// AND the decoder pod end up with NEW UIDs after the controller
// reacts. With the per-Instance policy (RecreateInstanceOnPodRestart)
// the decoder would NEVER change UID — that's the contrast the test
// proves.
var _ = ginkgo.Describe("behavior — RecreateISVCOnPodRestart cross-Component cascade", ginkgo.Ordered, func() {
	var ns *corev1.Namespace
	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "recreate-isvc-kcg2-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	// PIt: PARKED — first KIND run exposed a real product bug: the
	// decoder pod UID does NOT rotate after the engine pod is killed,
	// even though both Components opt into `RecreateISVCOnPodRestart`.
	// The policy is in the API enum + has a docstring promising
	// cross-Component cascade, but the dispatcher never enqueues a
	// peer-Component restart — it behaves identically to
	// `RecreateInstanceOnPodRestart`. Un-park once the controller
	// implements the cascade (or the enum value is removed). See
	// .claude/specs/2026-05-30-kind-test-walk/
	//   recreate-isvc-on-pod-restart-no-cross-component-cascade.md
	ginkgo.PIt("KCG-2 RecreateISVCOnPodRestart on engine cascades to decoder pod (both UIDs rotate)", func() {
		fix := newCoordFixture(ns.Name, WithISVCName("recreate-isvc-kcg2"),
			WithEngineImage("fake-serving:v1"))
		// Both Components opt into the cascade policy. The policy lives
		// on each per-Component Lifecycle, so it must be set on BOTH for
		// the cascade contract to engage.
		restartCascade := v1beta1.InstanceRestartPolicyRecreateISVC
		fix.ISVC.Spec.Engine.Lifecycle = &v1beta1.LifecycleSpec{
			RestartPolicy: &restartCascade,
		}
		fix.ISVC.Spec.Decoder.Lifecycle = &v1beta1.LifecycleSpec{
			RestartPolicy: &restartCascade,
		}
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		ginkgo.By("waiting for both Components to reach Ready and capturing initial pod UIDs")
		var engineUIDv1, decoderUIDv1 string
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "recreate-isvc-kcg2")
			for _, comp := range []v1beta1.ComponentType{
				v1beta1.EngineComponent, v1beta1.DecoderComponent,
			} {
				cs := c.Status.Components[comp]
				g.Expect(cs.OMENative).NotTo(BeNil(), "Component %s missing OMENative status", comp)
				g.Expect(cs.OMENative.InstanceStatuses).To(HaveLen(1))
				g.Expect(cs.OMENative.InstanceStatuses[0].Phase).
					To(Equal(v1beta1.OMENativeInstanceReady),
						"Component %s must reach Ready before kill", comp)
			}

			ePod := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
				Namespace: ns.Name, Name: "recreate-isvc-kcg2-engine-0-default-0",
			}, ePod)).To(Succeed())
			engineUIDv1 = string(ePod.UID)
			g.Expect(engineUIDv1).NotTo(BeEmpty())

			dPod := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
				Namespace: ns.Name, Name: "recreate-isvc-kcg2-decoder-0-default-0",
			}, dPod)).To(Succeed())
			decoderUIDv1 = string(dPod.UID)
			g.Expect(decoderUIDv1).NotTo(BeEmpty())
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("force-deleting ONLY the engine pod (decoder pod is untouched by us)")
		grace := int64(0)
		Expect(k8sClient.Delete(suiteCtx, &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: ns.Name, Name: "recreate-isvc-kcg2-engine-0-default-0",
			},
		}, &client.DeleteOptions{GracePeriodSeconds: &grace})).To(Succeed())

		ginkgo.By("expecting the cascade: both engine AND decoder pods land with NEW UIDs")
		// Both pods must end up with UIDs different from the captured v1
		// values. If RecreateInstance (not RecreateISVC) were active, the
		// decoder UID would stay equal to decoderUIDv1 — that's the
		// failure mode the test pins against.
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "recreate-isvc-kcg2")
			for _, comp := range []v1beta1.ComponentType{
				v1beta1.EngineComponent, v1beta1.DecoderComponent,
			} {
				cs := c.Status.Components[comp]
				g.Expect(cs.OMENative).NotTo(BeNil())
				g.Expect(cs.OMENative.InstanceStatuses).To(HaveLen(1))
				g.Expect(cs.OMENative.InstanceStatuses[0].Phase).
					To(Equal(v1beta1.OMENativeInstanceReady),
						"Component %s must converge back to Ready after cascade", comp)
			}

			ePod := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
				Namespace: ns.Name, Name: "recreate-isvc-kcg2-engine-0-default-0",
			}, ePod)).To(Succeed())
			g.Expect(string(ePod.UID)).NotTo(Equal(engineUIDv1),
				"KCG-2: engine pod UID must rotate (the killed-and-recreated path)")

			dPod := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
				Namespace: ns.Name, Name: "recreate-isvc-kcg2-decoder-0-default-0",
			}, dPod)).To(Succeed())
			g.Expect(string(dPod.UID)).NotTo(Equal(decoderUIDv1),
				"KCG-2: decoder pod UID must ALSO rotate under RecreateISVCOnPodRestart "+
					"(cross-Component cascade is the distinguishing contract vs. RecreateInstanceOnPodRestart). "+
					"old=%s new=%s", decoderUIDv1, string(dPod.UID))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})
})
