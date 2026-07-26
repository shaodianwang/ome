package omenative_coordination_kind

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/tests/integration/util"
)

// Lifecycle specs covering pod
// naming, scale, MinReplicas=0 behavior, and delete-then-recreate. The
// basic "ISVC creates pods + Service + InstanceStatus=Ready" path is
// covered by baseline_no_coord_kind_test (A-6) and lifecycle_basic_kind_test
// (LIFE-BASIC) so we don't duplicate it here.
//
// Missing-CBM / missing-CSR variants are ported in
// runtime_resolution_kind_test.go.
var _ = ginkgo.Describe("OMENative basic lifecycle (KIND)", ginkgo.Ordered, func() {
	var ns *corev1.Namespace

	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "omenative-lc-kind-")
		Expect(err).NotTo(HaveOccurred())
	})

	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	// Pod-naming pattern + index allocation under MinReplicas=3.
	// Catches regressions where BuildPlan emits the wrong indices or
	// the deterministic name shape <isvc>-<component>-<idx>-<runner>-<ordinal>
	// drifts.
	ginkgo.It("MinReplicas=3 creates pods at indices 0, 1, 2 with the expected name pattern", func() {
		fix := newCoordFixture(ns.Name, WithISVCName("mr3-isvc"),
			WithoutDecoder(),
			WithEngineReplicas(3),
			WithEngineImage("fake-serving:v1"))
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		expected := []string{
			"mr3-isvc-engine-0-default-0",
			"mr3-isvc-engine-1-default-0",
			"mr3-isvc-engine-2-default-0",
		}
		Eventually(func(g Gomega) {
			for _, name := range expected {
				pod := &corev1.Pod{}
				g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
					Namespace: ns.Name, Name: name,
				}, pod)).To(Succeed(),
					"missing pod %q — index allocation or naming pattern regressed", name)
			}
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})

	// MinReplicas=0 currently creates 1 pod — defaulter promotes 0 → 1
	// because the single-pod alpha doesn't support scale-to-zero.
	// Locked in here so a future fix (whether enforcing 0 or
	// documenting the 0→1 promotion) is deliberate.
	ginkgo.It("MinReplicas=0 currently creates 1 pod (defaulter promotes 0 → 1; OEP open question)", func() {
		fix := newCoordFixture(ns.Name, WithISVCName("mr0-isvc"),
			WithoutDecoder(),
			WithEngineReplicas(0),
			WithEngineImage("fake-serving:v1"))
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		Eventually(func(g Gomega) {
			pods := listOMENativePods(g, ns.Name, "mr0-isvc")
			g.Expect(pods).NotTo(BeEmpty(),
				"observed behavior: MinReplicas=0 promotes to 1 pod")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})

	// Stable-name reuse: a pre-existing pod with the deterministic
	// name shouldn't wedge the controller (controller must NOT adopt
	// a non-OMENative pod by relabeling it).
	ginkgo.It("pre-existing pod with the deterministic name doesn't wedge expectations", func() {
		fix := newCoordFixture(ns.Name, WithISVCName("ops7-isvc"),
			WithoutDecoder(),
			WithEngineImage("fake-serving:v1"))

		// Pre-create the CBM + CSR first so the controller can resolve
		// runtime when it tries to create the engine pod.
		Expect(k8sClient.Create(suiteCtx, fix.CBM)).To(Succeed())
		Expect(k8sClient.Create(suiteCtx, fix.CSR)).To(Succeed())
		ginkgo.DeferCleanup(func() {
			_ = k8sClient.Delete(suiteCtx, fix.CBM)
			_ = k8sClient.Delete(suiteCtx, fix.CSR)
		})

		// Pre-create a stranger pod with the SAME deterministic name
		// the controller will compute, but WITHOUT the OMENative
		// managed-by label so the controller's pod-listing query
		// won't match it. The controller will then attempt to Create
		// a pod with the same name and get AlreadyExists.
		stranger := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "ops7-isvc-engine-0-default-0",
				Namespace: ns.Name,
				Labels:    map[string]string{"placeholder": "not-OMENative"},
			},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{{Name: "ph", Image: "test:placeholder"}},
				// Skip image pull — placeholder; we just need the apiserver
				// object to exist.
				NodeSelector: map[string]string{"non-existent-key": "ignored"},
			},
		}
		Expect(k8sClient.Create(suiteCtx, stranger)).To(Succeed())
		ginkgo.DeferCleanup(func() {
			grace := int64(0)
			_ = k8sClient.Delete(suiteCtx, stranger, &client.DeleteOptions{GracePeriodSeconds: &grace})
		})

		Expect(k8sClient.Create(suiteCtx, fix.ISVC)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.ISVC) })

		// The contract: controller does NOT adopt the stranger pod
		// (it doesn't relabel it). Placeholder stays unmanaged.
		Consistently(func(g Gomega) {
			got := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(stranger), got)).To(Succeed())
			g.Expect(got.Labels[util.LabelManagedByKey]).NotTo(Equal(util.ManagedByOMENativeValue),
				"controller must not adopt a stranger pod by relabeling it; labels=%+v", got.Labels)
		}, "3s", kindPollInterval).Should(Succeed())
	})

	// Scale-up 1 → 3 must append new pods at indices 1, 2 without
	// touching pod 0. extraInstanceIndices over-selecting would
	// retire/recreate the existing pod 0 (changing its UID).
	ginkgo.It("scale up MinReplicas 1 → 3 appends pods at indices 1, 2 leaving pod 0 untouched", func() {
		fix := newCoordFixture(ns.Name, WithISVCName("scaleup-isvc"),
			WithoutDecoder(),
			WithEngineImage("fake-serving:v1"))
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		ginkgo.By("waiting for the initial single-pod instance to be Ready")
		var pod0UID string
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "scaleup-isvc")
			cs := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.Lifecycle).NotTo(BeNil())
			insts := instanceStatuses(g, ns.Name, "scaleup-isvc", v1beta1.EngineComponent)
			g.Expect(insts).To(HaveLen(1))
			g.Expect(insts[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))

			pod := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
				Namespace: ns.Name, Name: "scaleup-isvc-engine-0-default-0",
			}, pod)).To(Succeed())
			pod0UID = string(pod.UID)
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("bumping MinReplicas 1 → 3")
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, "scaleup-isvc")
			three := 3
			cur.Spec.Engine.MinReplicas = &three
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("expecting pods at indices 0, 1, 2 with pod 0's UID preserved")
		Eventually(func(g Gomega) {
			pod0 := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
				Namespace: ns.Name, Name: "scaleup-isvc-engine-0-default-0",
			}, pod0)).To(Succeed())
			g.Expect(string(pod0.UID)).To(Equal(pod0UID),
				"pod 0 must NOT be recreated on scale-up; UID changed from %s to %s", pod0UID, pod0.UID)

			pod1 := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
				Namespace: ns.Name, Name: "scaleup-isvc-engine-1-default-0",
			}, pod1)).To(Succeed())
			pod2 := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
				Namespace: ns.Name, Name: "scaleup-isvc-engine-2-default-0",
			}, pod2)).To(Succeed())
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})

	// Scale-up race: bump MinReplicas before pod 0 reaches Ready.
	// Tests the planner's handling of "pod 0 still Creating" when
	// computing additional indices.
	ginkgo.It("scale-up race: bump MinReplicas before pod 0 is Ready converges to all replicas", func() {
		fix := newCoordFixture(ns.Name, WithISVCName("race-isvc"),
			WithoutDecoder(),
			WithEngineImage("fake-serving:v1"))
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		ginkgo.By("immediately bumping MinReplicas 1 → 3 without waiting for Ready")
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, "race-isvc")
			three := 3
			cur.Spec.Engine.MinReplicas = &three
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("expecting all 3 InstanceStatuses to reach Ready with indices {0,1,2}")
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "race-isvc")
			cs := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.Lifecycle).NotTo(BeNil())
			insts := instanceStatuses(g, ns.Name, "race-isvc", v1beta1.EngineComponent)
			g.Expect(insts).To(HaveLen(3))
			seen := map[int32]bool{}
			for _, s := range insts {
				g.Expect(s.Phase).To(Equal(v1beta1.OMENativeInstanceReady),
					"all 3 instances must converge to Ready; got Index=%d Phase=%q", s.Index, s.Phase)
				seen[s.Index] = true
			}
			g.Expect(seen).To(HaveKey(int32(0)))
			g.Expect(seen).To(HaveKey(int32(1)))
			g.Expect(seen).To(HaveKey(int32(2)))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})

	// Scale-down 3 → 1: pods at indices 1, 2 disappear; pod 0 stays
	// Ready with its original UID. Catches over-selection bugs in
	// extraInstanceIndices that would kill a live serving pod.
	ginkgo.It("scale down MinReplicas 3 → 1 deletes pods 1+2, leaves pod 0 Ready", func() {
		fix := newCoordFixture(ns.Name, WithISVCName("scaledown-isvc"),
			WithoutDecoder(),
			WithEngineReplicas(3),
			WithEngineImage("fake-serving:v1"))
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		ginkgo.By("waiting for all three instances to be Ready")
		var pod0UID string
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "scaledown-isvc")
			cs := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.Lifecycle).NotTo(BeNil())
			insts := instanceStatuses(g, ns.Name, "scaledown-isvc", v1beta1.EngineComponent)
			g.Expect(insts).To(HaveLen(3))
			for _, s := range insts {
				g.Expect(s.Phase).To(Equal(v1beta1.OMENativeInstanceReady),
					"InstanceStatus[%d] should be Ready", s.Index)
			}
			pod0 := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
				Namespace: ns.Name, Name: "scaledown-isvc-engine-0-default-0",
			}, pod0)).To(Succeed())
			pod0UID = string(pod0.UID)
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("scaling down MinReplicas 3 → 1")
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, "scaledown-isvc")
			one := 1
			cur.Spec.Engine.MinReplicas = &one
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("expecting pods 1 and 2 to disappear and pod 0 to remain untouched")
		Eventually(func(g Gomega) {
			pod1 := &corev1.Pod{}
			err1 := k8sClient.Get(suiteCtx, client.ObjectKey{
				Namespace: ns.Name, Name: "scaledown-isvc-engine-1-default-0",
			}, pod1)
			g.Expect(apierrors.IsNotFound(err1)).To(BeTrue(),
				"pod 1 should be gone (NotFound) after scale-down; got err=%v", err1)
			pod2 := &corev1.Pod{}
			err2 := k8sClient.Get(suiteCtx, client.ObjectKey{
				Namespace: ns.Name, Name: "scaledown-isvc-engine-2-default-0",
			}, pod2)
			g.Expect(apierrors.IsNotFound(err2)).To(BeTrue(),
				"pod 2 should be gone (NotFound) after scale-down; got err=%v", err2)

			pod0 := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
				Namespace: ns.Name, Name: "scaledown-isvc-engine-0-default-0",
			}, pod0)).To(Succeed())
			g.Expect(string(pod0.UID)).To(Equal(pod0UID),
				"pod 0 must NOT be touched on scale-down; UID changed from %s to %s", pod0UID, pod0.UID)
			g.Expect(pod0.DeletionTimestamp).To(BeNil(),
				"pod 0 must NOT be marked for deletion")

			c := freshISVC(g, ns.Name, "scaledown-isvc")
			cs := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.Lifecycle).NotTo(BeNil())
			g.Expect(instanceStatuses(g, ns.Name, "scaledown-isvc", v1beta1.EngineComponent)).To(HaveLen(1),
				"InstanceStatuses should converge to exactly one entry after scale-down")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})

	// Delete + recreate same-name ISVC: new ISVC has a fresh UID and
	// must NOT inherit the old CR's identity. Two cases need handling:
	// (a) cascade-GC has reaped the old CR by the time the new ISVC
	//     reconciles → name lookup returns NotFound → Create runs. With
	//     a UID-aware hash, the new CR's name differs from the old.
	// (b) GC is still in flight when the new ISVC reconciles → name
	//     lookup finds the old CR with foreign owner UID → existing
	//     collision branch bumps CollisionCount → retry lands at a
	//     fresh name.
	// Both code paths are exercised in production; this spec pins (a),
	// the dominant outcome under normal cascade-GC timing. The
	// foreign-owner path is pinned at unit scope in
	// pkg/controller/v1beta1/workload/revision/revision_test.go:
	// TestEnsureControllerRevision_RejectsForeignOwnerAsCollision.
	ginkgo.It("delete + recreate same-name ISVC produces a fresh ControllerRevision (no foreign-owner adoption)", func() {
		fix1 := newCoordFixture(ns.Name, WithISVCName("recreate-isvc"),
			WithoutDecoder(),
			WithEngineImage("fake-serving:v1"))
		applyFixture(Default, fix1)
		// Don't deferFixtureCleanup here — we explicitly recreate
		// below; clean up CBM/CSR manually at the end.
		ginkgo.DeferCleanup(func() {
			_ = k8sClient.Delete(suiteCtx, fix1.CBM)
			_ = k8sClient.Delete(suiteCtx, fix1.CSR)
		})

		var originalCR, originalUID string
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "recreate-isvc")
			cs := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.Lifecycle).NotTo(BeNil())
			insts := instanceStatuses(g, ns.Name, "recreate-isvc", v1beta1.EngineComponent)
			g.Expect(insts).To(HaveLen(1))
			g.Expect(insts[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
			g.Expect(cs.Lifecycle.UpdateRevision).NotTo(BeEmpty())
			originalCR = cs.Lifecycle.UpdateRevision
			originalUID = string(c.UID)
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("delete the ISVC and wait for cascade-GC to clear pods")
		Expect(k8sClient.Delete(suiteCtx, fix1.ISVC)).To(Succeed())
		Eventually(func(g Gomega) {
			err := k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: ns.Name, Name: "recreate-isvc"}, &v1beta1.InferenceService{})
			g.Expect(apierrors.IsNotFound(err)).To(BeTrue(),
				"ISVC must vanish from apiserver before recreate; got err=%v", err)
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("recreate ISVC with the SAME name (new UID)")
		fix2 := newCoordFixture(ns.Name, WithISVCName("recreate-isvc"),
			WithCBMName(fix1.CBM.Name),
			WithCSRName(fix1.CSR.Name),
			WithoutDecoder(),
			WithEngineImage("fake-serving:v1"))
		Expect(k8sClient.Create(suiteCtx, fix2.ISVC)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix2.ISVC) })

		var newUID string
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "recreate-isvc")
			g.Expect(string(c.UID)).NotTo(Equal(originalUID),
				"recreated ISVC must have a new UID")
			newUID = string(c.UID)
			cs := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.Lifecycle).NotTo(BeNil())
			insts := instanceStatuses(g, ns.Name, "recreate-isvc", v1beta1.EngineComponent)
			g.Expect(insts).To(HaveLen(1))
			g.Expect(insts[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
			g.Expect(cs.Lifecycle.UpdateRevision).NotTo(BeEmpty())
			g.Expect(cs.Lifecycle.UpdateRevision).NotTo(Equal(originalCR),
				"new ISVC must NOT adopt the old CR (foreign UID); EnsureControllerRevision must bump CollisionCount. Old=%s New=%s",
				originalCR, cs.Lifecycle.UpdateRevision)
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("verify the new CR's controller OwnerReference points at the new ISVC's UID")
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "recreate-isvc")
			cs := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.Lifecycle).NotTo(BeNil())
			g.Expect(cs.Lifecycle.UpdateRevision).NotTo(BeEmpty())
			cr := &appsv1.ControllerRevision{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
				Namespace: ns.Name, Name: cs.Lifecycle.UpdateRevision,
			}, cr)).To(Succeed())
			var ownerUID string
			for _, ref := range cr.OwnerReferences {
				if ref.Controller != nil && *ref.Controller {
					ownerUID = string(ref.UID)
					break
				}
			}
			g.Expect(ownerUID).To(Equal(newUID),
				"new CR controller-owner UID must be the recreated ISVC's UID; got owner=%q, want %q",
				ownerUID, newUID)
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})
})
