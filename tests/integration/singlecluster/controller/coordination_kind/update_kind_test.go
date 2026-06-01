package coordination_kind

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/tests/integration/util"
)

// OMENative Update path specs ported from envtest update_test.go.
// chooseUpdateMode branches on:
//   - InPlaceIfPossible + image-only diff → in-place patch
//   - InPlaceIfPossible + non-image (env-var) diff → recreate
//   - InPlaceOnly + ineligible (env-var) diff → reject + event
//   - RecreatePod → always recreate
//   - SurgeThenDrain (the default) → surge for a single-pod Instance
//     (new pod at the alternate ordinal, drain, promote); multi-pod
//     gangs fall back to recreate
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
// rejection path, and the default-SurgeThenDrain non-image-diff path
// (a surge to the alternate ordinal — the in-place / recreate behavior
// tests exercise image-only diffs and don't cover an env-var diff on
// the default path).
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
			g.Expect(cs.Lifecycle).NotTo(BeNil())
			g.Expect(cs.Lifecycle.InstanceStatuses).To(HaveLen(1))
			g.Expect(cs.Lifecycle.InstanceStatuses[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))

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

	// env-var change (non-image diff) under the DEFAULT strategy
	// (SurgeThenDrain): chooseUpdateMode routes a non-image diff for a
	// single-pod Instance to updateModeSurge. Surge creates the
	// replacement pod at the ALTERNATE ordinal (1), drains + deletes the
	// old ordinal-0 pod, and advances ActiveOrdinal 0→1 — and by design
	// does NOT bump Incarnation (the ordinal slot, not the
	// instance-incarnation label, distinguishes old from new; see the
	// ActiveOrdinal vs Incarnation field docs on OMENativeInstanceStatus).
	// A regression that routed env diffs through in-place would silently
	// no-op; one that bumped Incarnation here would over-count the
	// recreate counter and cross the two orthogonal identity dimensions.
	ginkgo.It("env-var change under default SurgeThenDrain surges to the alternate ordinal (UID rotates, Incarnation stays)", func() {
		fix := newCoordFixture(ns.Name, WithISVCName("recreate-env-isvc"),
			WithoutDecoder(),
			WithEngineImage("fake-serving:v1"))
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		ginkgo.By("waiting for the initial pod to be Ready at ordinal 0, Incarnation=1")
		var origUID string
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "recreate-env-isvc")
			cs := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.Lifecycle).NotTo(BeNil())
			g.Expect(cs.Lifecycle.InstanceStatuses).To(HaveLen(1))
			g.Expect(cs.Lifecycle.InstanceStatuses[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
			g.Expect(cs.Lifecycle.InstanceStatuses[0].Incarnation).To(Equal(int64(1)))
			g.Expect(cs.Lifecycle.InstanceStatuses[0].ActiveOrdinal).To(Equal(int32(0)),
				"initial Instance must start at ordinal 0")

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

		ginkgo.By("expecting the surge: new pod at ordinal 1 carries the env var, ActiveOrdinal 0->1, Incarnation unchanged, old ordinal-0 pod gone")
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "recreate-env-isvc")
			cs := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.Lifecycle).NotTo(BeNil())
			g.Expect(cs.Lifecycle.InstanceStatuses).To(HaveLen(1))
			g.Expect(cs.Lifecycle.InstanceStatuses[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
			// Surge promotes to the alternate ordinal but keeps Incarnation:
			// the ordinal slot distinguishes old from new, not the label.
			g.Expect(cs.Lifecycle.InstanceStatuses[0].ActiveOrdinal).To(Equal(int32(1)),
				"surge must advance ActiveOrdinal to the alternate slot")
			g.Expect(cs.Lifecycle.InstanceStatuses[0].Incarnation).To(Equal(int64(1)),
				"surge must NOT bump Incarnation (that is the recreate counter); got %d", cs.Lifecycle.InstanceStatuses[0].Incarnation)

			// The new canonical pod lives at ordinal 1, has a fresh UID,
			// and carries the new env var.
			newPod := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
				Namespace: ns.Name, Name: "recreate-env-isvc-engine-0-default-1",
			}, newPod)).To(Succeed())
			g.Expect(string(newPod.UID)).NotTo(Equal(origUID),
				"surge must produce a new pod at the alternate ordinal")
			foundEnv := false
			for _, e := range newPod.Spec.Containers[0].Env {
				if e.Name == "EXTRA_SETTING" && e.Value == "yes" {
					foundEnv = true
					break
				}
			}
			g.Expect(foundEnv).To(BeTrue(),
				"surge pod must reflect the new env var; got %+v", newPod.Spec.Containers[0].Env)

			// The old ordinal-0 pod is drained and deleted — gone, or
			// terminating with its grace period still ticking.
			oldPod := &corev1.Pod{}
			err := k8sClient.Get(suiteCtx, client.ObjectKey{
				Namespace: ns.Name, Name: "recreate-env-isvc-engine-0-default-0",
			}, oldPod)
			if err == nil {
				g.Expect(oldPod.DeletionTimestamp).NotTo(BeNil(),
					"old ordinal-0 pod must be deleted/terminating after surge promote")
			} else {
				g.Expect(apierrors.IsNotFound(err)).To(BeTrue(),
					"old ordinal-0 pod lookup must be NotFound after surge; got %v", err)
			}
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})
})
