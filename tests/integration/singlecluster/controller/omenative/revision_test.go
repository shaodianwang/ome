package omenative

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/tests/integration/util"
)

func toRawExt(b []byte) runtime.RawExtension { return runtime.RawExtension{Raw: b} }

// Revision lifecycle specs — pin OMENative's ControllerRevision
// bookkeeping against envtest.
var _ = ginkgo.Describe("OMENative ControllerRevision lifecycle", ginkgo.Ordered, func() {
	var ns *corev1.Namespace

	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "omenative-rev-")
		Expect(err).NotTo(HaveOccurred())
	})

	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	// Spec scenario #65. Bug it catches: a regression of nextRevisionNumber
	// that starts the count somewhere other than 1, OR a regression of
	// revision-label stamping that breaks the listOMENativeRevisions
	// owner-aware query (which would also break retention sweeps).
	ginkgo.It("first reconcile creates Revision=1 with controller-owned labels", func() {
		fx := newOMENativeFixture(ns.Name,
			WithCBMName("rev1-llama-7b"),
			WithCSRName("rev1-runtime"),
			WithISVCName("rev1-isvc"),
		)
		Expect(k8sClient.Create(suiteCtx, fx.CBM)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CBM) })
		Expect(k8sClient.Create(suiteCtx, fx.CSR)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CSR) })
		Expect(k8sClient.Create(suiteCtx, fx.ISVC)).To(Succeed())

		Eventually(func(g Gomega) {
			crs := &appsv1.ControllerRevisionList{}
			g.Expect(k8sClient.List(suiteCtx, crs,
				client.InNamespace(ns.Name),
				client.MatchingLabels{labelManagedByKey: managedByValue},
			)).To(Succeed())
			g.Expect(crs.Items).NotTo(BeEmpty(), "expected at least one OMENative CR")
			// The FIRST revision must be #1. Higher numbers indicate
			// either nextRevisionNumber regression or a collision +
			// retry path that wrote multiple CRs unexpectedly.
			minRev := int64(-1)
			for i := range crs.Items {
				r := crs.Items[i].Revision
				if minRev < 0 || r < minRev {
					minRev = r
				}
			}
			g.Expect(minRev).To(Equal(int64(1)), "first revision must be 1; got %d (full list: %+v)", minRev, crs.Items)
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	// Spec scenario #66. Bug it catches: regression of revision-name
	// determinism (same input → same hash → same name), or regression
	// of EnsureControllerRevision's reuse logic that produces a fresh
	// CR per reconcile instead of reusing the existing one.
	ginkgo.It("identical spec across reconciles produces exactly one CR (deterministic name + reuse)", func() {
		fx := newOMENativeFixture(ns.Name,
			WithCBMName("rev-stable-llama"),
			WithCSRName("rev-stable-runtime"),
			WithISVCName("rev-stable-isvc"),
		)
		Expect(k8sClient.Create(suiteCtx, fx.CBM)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CBM) })
		Expect(k8sClient.Create(suiteCtx, fx.CSR)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CSR) })
		Expect(k8sClient.Create(suiteCtx, fx.ISVC)).To(Succeed())

		// Bounce the ISVC's MinReplicas to trigger several reconciles
		// without changing the rendered PodSpec. nextRevisionNumber's
		// fast path should reuse the existing CR; the CR count should
		// not grow.
		Eventually(func(g Gomega) {
			crs := &appsv1.ControllerRevisionList{}
			g.Expect(k8sClient.List(suiteCtx, crs,
				client.InNamespace(ns.Name),
				client.MatchingLabels{labelManagedByKey: managedByValue},
			)).To(Succeed())
			g.Expect(crs.Items).NotTo(BeEmpty())
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		// Now poke the ISVC several times — bouncing MinReplicas
		// is a spec edit that does NOT change the rendered PodSpec
		// (replica count is metadata, not template).
		//
		// The controller writes the ISVC's Status concurrently, so
		// a Get-then-Update can race-fail with 409 Conflict. Retry
		// via Eventually on each edit until it lands.
		for i := 0; i < 3; i++ {
			mr := 1 + (i % 2)
			Eventually(func(g Gomega) {
				fresh := &v1beta1.InferenceService{}
				g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(fx.ISVC), fresh)).To(Succeed())
				fresh.Spec.Engine.ComponentExtensionSpec.MinReplicas = &mr
				g.Expect(k8sClient.Update(suiteCtx, fresh)).To(Succeed())
			}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
		}

		// Then assert via Consistently that the CR count stays at 1.
		// (Reuse means no new CRs even after multiple reconciles.)
		Consistently(func(g Gomega) {
			crs := &appsv1.ControllerRevisionList{}
			g.Expect(k8sClient.List(suiteCtx, crs,
				client.InNamespace(ns.Name),
				client.MatchingLabels{labelManagedByKey: managedByValue},
			)).To(Succeed())
			g.Expect(crs.Items).To(HaveLen(1), "spec edits that don't change PodSpec must NOT create new CRs; got %d", len(crs.Items))
		}, "3s", util.PollInterval).Should(Succeed())
	})

	// Foreign-owner collision: a CR with the right name but an
	// OwnerReference UID belonging to a previously-deleted-and-recreated
	// ISVC must not be reused. EnsureControllerRevision treats it as a
	// hash collision; the caller bumps CollisionCount and writes a CR
	// with a different name. Without this guard, OMENative would
	// silently adopt the stale CR and mix histories.
	ginkgo.It("CR with foreign OwnerReference UID triggers collision-bump", func() {
		fx := newOMENativeFixture(ns.Name,
			WithCBMName("rev-collision-llama"),
			WithCSRName("rev-collision-runtime"),
			WithISVCName("rev-collision-isvc"),
		)
		Expect(k8sClient.Create(suiteCtx, fx.CBM)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CBM) })
		Expect(k8sClient.Create(suiteCtx, fx.CSR)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CSR) })
		Expect(k8sClient.Create(suiteCtx, fx.ISVC)).To(Succeed())

		ginkgo.By("capturing the initial CR's name + Data + Revision")
		var origName string
		var origData []byte
		var origRev int64
		Eventually(func(g Gomega) {
			crs := &appsv1.ControllerRevisionList{}
			g.Expect(k8sClient.List(suiteCtx, crs,
				client.InNamespace(ns.Name),
				client.MatchingLabels{labelManagedByKey: managedByValue},
			)).To(Succeed())
			g.Expect(crs.Items).To(HaveLen(1))
			origName = crs.Items[0].Name
			origData = crs.Items[0].Data.Raw
			origRev = crs.Items[0].Revision
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("replacing the CR with one carrying a FOREIGN controller UID")
		// Delete the controller-managed CR. Without the OwnerRef back to
		// the ISVC, envtest's gc would skip it anyway — we delete to
		// give ourselves a clean re-create slot.
		Eventually(func(g Gomega) {
			toDelete := &appsv1.ControllerRevision{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
				Namespace: ns.Name, Name: origName,
			}, toDelete)).To(Succeed())
			g.Expect(k8sClient.Delete(suiteCtx, toDelete)).To(Succeed())
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		// Re-create with the same name and Data, but a foreign
		// (random-UUID) OwnerRef.
		foreignUID := types.UID("00000000-1111-2222-3333-444455556666")
		impostor := &appsv1.ControllerRevision{
			ObjectMeta: metav1.ObjectMeta{
				Name:      origName,
				Namespace: ns.Name,
				Labels: map[string]string{
					labelManagedByKey: managedByValue,
				},
				OwnerReferences: []metav1.OwnerReference{{
					APIVersion: "ome.io/v1beta1",
					Kind:       "InferenceService",
					Name:       "stale-isvc-of-the-same-name",
					UID:        foreignUID,
					Controller: ptr.To(true),
				}},
			},
			Data:     toRawExt(origData),
			Revision: origRev,
		}
		// Retry on transient errors (the controller may race to recreate
		// the CR under its own UID; if that lands first our Create gets
		// AlreadyExists — wait it out, then Delete+Create again).
		Eventually(func(g Gomega) {
			_ = k8sClient.Delete(suiteCtx, &appsv1.ControllerRevision{
				ObjectMeta: metav1.ObjectMeta{Name: origName, Namespace: ns.Name},
			})
			g.Expect(k8sClient.Create(suiteCtx, impostor.DeepCopy())).To(Succeed())
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("poking the ISVC to force a reconcile")
		Eventually(func(g Gomega) {
			fresh := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(fx.ISVC), fresh)).To(Succeed())
			one := 1
			fresh.Spec.Engine.ComponentExtensionSpec.MinReplicas = &one
			if fresh.Spec.Engine.ComponentExtensionSpec.Annotations == nil {
				fresh.Spec.Engine.ComponentExtensionSpec.Annotations = map[string]string{}
			}
			// Tweak an annotation to bump generation without changing the
			// hash inputs that revisionHash captures.
			fresh.Spec.Engine.ComponentExtensionSpec.Annotations["poke"] = "1"
			g.Expect(k8sClient.Update(suiteCtx, fresh)).To(Succeed())
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("expecting a SECOND CR with a different name (CollisionCount-salted)")
		Eventually(func(g Gomega) {
			crs := &appsv1.ControllerRevisionList{}
			g.Expect(k8sClient.List(suiteCtx, crs,
				client.InNamespace(ns.Name),
				client.MatchingLabels{labelManagedByKey: managedByValue},
			)).To(Succeed())
			// One impostor (foreign UID) + one fresh CR with controller UID.
			foundFresh := false
			for _, cr := range crs.Items {
				if cr.Name == origName {
					continue
				}
				for _, ref := range cr.OwnerReferences {
					if ref.Controller != nil && *ref.Controller && ref.UID != foreignUID {
						foundFresh = true
						break
					}
				}
			}
			g.Expect(foundFresh).To(BeTrue(),
				"expected a fresh controller-owned CR at a NEW name; got %+v",
				crs.Items)
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	// A real PodSpec change (image bump) MUST produce a new CR whose
	// .Revision is monotonically larger than the previous one. Guards
	// nextRevisionNumber's monotonicity and the EnsureControllerRevision
	// path that creates rather than reuses on a hash mismatch.
	ginkgo.It("image spec change produces a new CR with Revision > previous", func() {
		fx := newOMENativeFixture(ns.Name,
			WithCBMName("rev-bump-llama"),
			WithCSRName("rev-bump-runtime"),
			WithISVCName("rev-bump-isvc"),
			WithImage("ghcr.io/test/runtime:v1"),
		)
		Expect(k8sClient.Create(suiteCtx, fx.CBM)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CBM) })
		Expect(k8sClient.Create(suiteCtx, fx.CSR)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CSR) })
		Expect(k8sClient.Create(suiteCtx, fx.ISVC)).To(Succeed())

		ginkgo.By("capturing the initial CR (Revision=1)")
		var initialName string
		var initialRev int64
		Eventually(func(g Gomega) {
			crs := &appsv1.ControllerRevisionList{}
			g.Expect(k8sClient.List(suiteCtx, crs,
				client.InNamespace(ns.Name),
				client.MatchingLabels{labelManagedByKey: managedByValue},
			)).To(Succeed())
			g.Expect(crs.Items).To(HaveLen(1))
			initialName = crs.Items[0].Name
			initialRev = crs.Items[0].Revision
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("flipping the engine container image to v2")
		Eventually(func(g Gomega) {
			fresh := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(fx.ISVC), fresh)).To(Succeed())
			fresh.Spec.Engine.PodSpec.Containers[0].Image = "ghcr.io/test/runtime:v2"
			g.Expect(k8sClient.Update(suiteCtx, fresh)).To(Succeed())
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("expecting a new CR with a distinct name and Revision > initial")
		Eventually(func(g Gomega) {
			crs := &appsv1.ControllerRevisionList{}
			g.Expect(k8sClient.List(suiteCtx, crs,
				client.InNamespace(ns.Name),
				client.MatchingLabels{labelManagedByKey: managedByValue},
			)).To(Succeed())
			g.Expect(len(crs.Items)).To(BeNumerically(">=", 2),
				"image bump must produce a new CR; got %d total", len(crs.Items))
			foundNewer := false
			for _, cr := range crs.Items {
				if cr.Name != initialName && cr.Revision > initialRev {
					foundNewer = true
					break
				}
			}
			g.Expect(foundNewer).To(BeTrue(),
				"expected at least one CR with Name != %s AND Revision > %d; got %+v",
				initialName, initialRev, crs.Items)
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})
})
