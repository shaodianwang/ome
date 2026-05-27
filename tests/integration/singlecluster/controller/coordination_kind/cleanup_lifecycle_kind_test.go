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

// LIFE-DEL — ISVC delete → owned children (pods, per-revision Services,
// ControllerRevisions) reach DeletionTimestamp / get garbage-collected.
//
// KIND mirror of the envtest LIFE-1 spec. The envtest version was PIt'd
// because envtest doesn't run the kube-controller-manager garbage
// collector (only apiserver+etcd), so cascade-delete via OwnerReference
// only fires under a real cluster. KIND brings the GC in scope, so this
// spec asserts the actual cascade — not just the OwnerRef shape that
// SVC-4 already pins.
//
// PIts in the envtest sibling (LIFE-2..6) are dropped: cascade behavior
// is identical regardless of group policy / Sequential mid-flight / paused
// state — all that varies is which sub-resources exist at delete time.
// One spec covering the common cascade is the meaningful contract; the
// rest were envtest hedges against the GC absence.
var _ = ginkgo.Describe("LIFE-DEL — ISVC delete cascades owned children", ginkgo.Ordered, func() {
	var ns *corev1.Namespace
	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "life-del-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("LIFE-DEL deleting the ISVC cascades pods, per-revision Services, and ControllerRevisions", func() {
		fix := newCoordFixture(ns.Name, WithISVCName("life-del"),
			WithEngineImage("fake-serving:v1"))
		applyFixture(Default, fix)
		// Manually defer CBM/CSR cleanup — we delete the ISVC explicitly
		// below to drive the cascade, so deferFixtureCleanup's
		// delete-ISVC-then-wait sequence would race with our own delete.
		ginkgo.DeferCleanup(func() {
			_ = k8sClient.Delete(suiteCtx, fix.CBM)
			_ = k8sClient.Delete(suiteCtx, fix.CSR)
		})

		// Wait for the ISVC to converge and produce the full set of
		// owned children: pods, per-revision Services (routing + headless
		// per Component), ControllerRevisions.
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "life-del")
			engCS := c.Status.Components[v1beta1.EngineComponent]
			decCS := c.Status.Components[v1beta1.DecoderComponent]
			g.Expect(engCS.OMENative).NotTo(BeNil())
			g.Expect(decCS.OMENative).NotTo(BeNil())
			g.Expect(engCS.OMENative.ServingReplicas).To(Equal(int32(1)))
			g.Expect(decCS.OMENative.ServingReplicas).To(Equal(int32(1)))
			g.Expect(listOMENativePods(g, ns.Name, "life-del")).NotTo(BeEmpty(),
				"pods should exist pre-delete")
			g.Expect(listOMENativeCRs(g, ns.Name)).NotTo(BeEmpty(),
				"ControllerRevisions should exist pre-delete")
			g.Expect(listPerRevisionServices(g, ns.Name)).NotTo(BeEmpty(),
				"per-revision Services should exist pre-delete")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Capture the ISVC UID — kube-GC cascades by matching the
		// OwnerReference UID, so future-proofing the assertion against
		// a stale-name collision means binding to UID.
		preDelete := freshISVC(Default, ns.Name, "life-del")
		isvcUID := preDelete.UID

		// Delete the ISVC. Cascade is the default (PropagationPolicy
		// nil → background by k8s GC defaults; deletion of the ISVC
		// triggers the kube-controller-manager to walk OwnerReferences
		// and delete owned objects.)
		Expect(k8sClient.Delete(suiteCtx, fix.ISVC)).To(Succeed())

		// ISVC itself purged (finalizer drop + apiserver tombstone).
		Eventually(func(g Gomega) {
			fresh := &v1beta1.InferenceService{}
			err := k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: ns.Name, Name: "life-del"}, fresh)
			g.Expect(apierrors.IsNotFound(err)).To(BeTrue(),
				"ISVC should be fully deleted; got err=%v", err)
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Owned children should be GC'd (or at least marked for
		// deletion). Filter remaining items by isvcUID to ignore any
		// resources owned by an earlier ISVC of the same name (none
		// expected here — fresh namespace — but the check is
		// defensive).
		Eventually(func(g Gomega) {
			// Pods owned by life-del's UID.
			pods := &corev1.PodList{}
			g.Expect(k8sClient.List(suiteCtx, pods, client.InNamespace(ns.Name))).To(Succeed())
			for _, p := range pods.Items {
				for _, or := range p.OwnerReferences {
					g.Expect(or.UID).NotTo(Equal(isvcUID),
						"pod %s still owned by deleted ISVC %s after cascade", p.Name, isvcUID)
				}
			}

			// Per-revision Services owned by life-del's UID.
			svcs := listPerRevisionServices(g, ns.Name)
			for _, s := range svcs {
				for _, or := range s.OwnerReferences {
					g.Expect(or.UID).NotTo(Equal(isvcUID),
						"per-revision Service %s still owned by deleted ISVC %s after cascade", s.Name, isvcUID)
				}
			}

			// ControllerRevisions owned by life-del's UID.
			crs := listOMENativeCRs(g, ns.Name)
			for _, cr := range crs {
				for _, or := range cr.OwnerReferences {
					g.Expect(or.UID).NotTo(Equal(isvcUID),
						"ControllerRevision %s still owned by deleted ISVC %s after cascade", cr.Name, isvcUID)
				}
			}
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed(),
			"kube-GC should cascade-delete every child owned by the ISVC's UID")
	})
})
