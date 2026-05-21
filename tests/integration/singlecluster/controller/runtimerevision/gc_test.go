package runtimerevision

import (
	"fmt"
	"time"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	v1beta1testing "github.com/ome-projects/ome/pkg/utils/testing/v1beta1"

	"github.com/ome-projects/ome/tests/integration/util"
)

// Suite uses retention=2, grace=2s, resync=1s for fast observation.

var _ = ginkgo.Describe("RuntimeRevision GC controller (OEP-0014 Phase 3)", func() {

	mkRev := func(name, runtimeOf string, ageOffset time.Duration, omeOwned bool) *appsv1.ControllerRevision {
		labels := map[string]string{}
		annotations := map[string]string{}
		if omeOwned {
			labels[constants.RuntimeRevisionOfLabelKey] = runtimeOf
			annotations[constants.RuntimeRevisionCreatedByKey] = constants.RuntimeRevisionCreatedByOMEValue
		} else {
			labels["app"] = "not-ome"
		}
		// CreationTimestamp is set by the apiserver on Create — we
		// can't backdate. Instead, tests stagger Create calls and use
		// counts/labels to know the order.
		return &appsv1.ControllerRevision{
			ObjectMeta: metav1.ObjectMeta{
				Name:        name,
				Namespace:   constants.OMENamespace,
				Labels:      labels,
				Annotations: annotations,
			},
			Data:     runtime.RawExtension{Raw: []byte(fmt.Sprintf(`{"name":%q}`, name))},
			Revision: 1,
		}
	}

	getRev := func(name string) (*appsv1.ControllerRevision, error) {
		rev := &appsv1.ControllerRevision{}
		err := k8sClient.Get(suiteCtx, types.NamespacedName{Namespace: constants.OMENamespace, Name: name}, rev)
		return rev, err
	}

	// Helper: create N OME-owned revisions for a runtime, in order
	// (oldest first). metav1.Time round-trips at SECOND precision via
	// etcd, so sub-second sleeps would let multiple revisions share a
	// CreationTimestamp and make the sort tie-break non-deterministic.
	// 1.1s between creates guarantees the per-second tick advances.
	createOrdered := func(runtimeOf string, names []string) {
		for _, n := range names {
			Expect(k8sClient.Create(suiteCtx, mkRev(n, runtimeOf, 0, true))).To(Succeed())
			time.Sleep(1100 * time.Millisecond)
		}
	}

	ginkgo.AfterEach(func() {
		// Best-effort wipe of any leftover OME-owned revisions so
		// specs don't bleed into each other.
		var list appsv1.ControllerRevisionList
		_ = k8sClient.List(suiteCtx, &list, client.InNamespace(constants.OMENamespace))
		for i := range list.Items {
			if list.Items[i].Annotations[constants.RuntimeRevisionCreatedByKey] == constants.RuntimeRevisionCreatedByOMEValue ||
				list.Items[i].Labels["app"] == "not-ome" {
				_ = k8sClient.Delete(suiteCtx, &list.Items[i])
			}
		}
		// Also clean stray ISVCs from these specs.
		nss := &v1beta1.InferenceServiceList{}
		_ = k8sClient.List(suiteCtx, nss)
		for i := range nss.Items {
			_ = k8sClient.Delete(suiteCtx, &nss.Items[i])
		}
	})

	ginkgo.It("keeps the N most recent OME-owned revisions per runtime, marks the rest", func() {
		// retention=2; create 4 revisions for one runtime.
		createOrdered("rt-keep-n", []string{"cr-rt-keep-n-001", "cr-rt-keep-n-002", "cr-rt-keep-n-003", "cr-rt-keep-n-004"})

		ginkgo.By("two oldest get gc-eligible-since; two newest are untouched")
		Eventually(func(g Gomega) {
			r1, err := getRev("cr-rt-keep-n-001")
			g.Expect(err).NotTo(HaveOccurred())
			r2, err := getRev("cr-rt-keep-n-002")
			g.Expect(err).NotTo(HaveOccurred())
			r3, err := getRev("cr-rt-keep-n-003")
			g.Expect(err).NotTo(HaveOccurred())
			r4, err := getRev("cr-rt-keep-n-004")
			g.Expect(err).NotTo(HaveOccurred())

			g.Expect(r1.Annotations).To(HaveKey(constants.RuntimeRevisionGCEligibleSinceKey))
			g.Expect(r2.Annotations).To(HaveKey(constants.RuntimeRevisionGCEligibleSinceKey))
			g.Expect(r3.Annotations).NotTo(HaveKey(constants.RuntimeRevisionGCEligibleSinceKey))
			g.Expect(r4.Annotations).NotTo(HaveKey(constants.RuntimeRevisionGCEligibleSinceKey))
		}, 5*time.Second, 200*time.Millisecond).Should(Succeed())
	})

	ginkgo.It("deletes marked revisions after the grace period elapses", func() {
		createOrdered("rt-delete", []string{"cr-rt-delete-001", "cr-rt-delete-002", "cr-rt-delete-003"})

		ginkgo.By("oldest revision is gone within grace + resync window (~15s budget)")
		Eventually(func() bool {
			_, err := getRev("cr-rt-delete-001")
			return err != nil
		}, 20*time.Second, 500*time.Millisecond).Should(BeTrue())

		ginkgo.By("retained pair is still present")
		_, err := getRev("cr-rt-delete-002")
		Expect(err).NotTo(HaveOccurred())
		_, err = getRev("cr-rt-delete-003")
		Expect(err).NotTo(HaveOccurred())
	})

	ginkgo.It("never touches ControllerRevisions that aren't OME-owned", func() {
		// Create a foreign revision (no OME annotation).
		Expect(k8sClient.Create(suiteCtx, mkRev("sts-foreign-cr", "stateful-app", 0, false))).To(Succeed())

		// Sit longer than grace+resync — controller must NOT touch it.
		Consistently(func() bool {
			_, err := getRev("sts-foreign-cr")
			return err == nil
		}, 15*time.Second, 500*time.Millisecond).Should(BeTrue())
	})

	ginkgo.It("retains a revision currently referenced by an ISVC even if oldest", func() {
		ns, err := util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "gc-ref-")
		Expect(err).NotTo(HaveOccurred())
		ginkgo.DeferCleanup(func() { _ = util.DeleteNamespace(suiteCtx, k8sClient, ns.Name) })

		oldest := "cr-rt-ref-001"

		// Create the ISVC FIRST so its spec.runtime.revision puts the
		// (not-yet-existing) revision name into the GC controller's
		// referenced set from the very first reconcile. Otherwise the
		// GC would reconcile after rev creation but before the ISVC
		// existed, mark oldest, and only later clear once the ISVC
		// event arrived — racy in CI. The ISVC reconciler isn't running
		// in this suite, so admission/reconcile state on the ISVC
		// doesn't matter; we only need the spec.runtime.revision
		// field for the GC controller's reference set.
		isvc := &v1beta1.InferenceService{
			ObjectMeta: metav1.ObjectMeta{Name: "gc-pinner", Namespace: ns.Name},
			Spec: v1beta1.InferenceServiceSpec{
				Runtime: &v1beta1.ServingRuntimeRef{
					Name:     "rt-ref",
					AutoSync: ptrBool(false),
					Revision: &oldest,
				},
				Engine: &v1beta1.EngineSpec{},
			},
		}
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())

		// Now create the revisions. With retention=2 and 3 revisions,
		// the oldest would normally be marked — but the ISVC's pin
		// keeps it referenced.
		Expect(k8sClient.Create(suiteCtx, mkRev(oldest, "rt-ref", 0, true))).To(Succeed())
		time.Sleep(1100 * time.Millisecond)
		createOrdered("rt-ref", []string{"cr-rt-ref-002", "cr-rt-ref-003"})

		// Tolerate any transient mark from a race window where GC
		// observes a new revision before its cache reflects the ISVC's
		// reference set update: wait for convergence, then verify it
		// stays cleared across the grace + resync budget.
		Eventually(func(g Gomega) {
			r, err := getRev(oldest)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(r.Annotations).NotTo(HaveKey(constants.RuntimeRevisionGCEligibleSinceKey))
		}, 5*time.Second, 200*time.Millisecond).Should(Succeed())

		Consistently(func(g Gomega) {
			r, err := getRev(oldest)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(r.Annotations).NotTo(HaveKey(constants.RuntimeRevisionGCEligibleSinceKey),
				"referenced revision must NOT be marked for GC")
		}, 12*time.Second, 500*time.Millisecond).Should(Succeed())
	})

	ginkgo.It("retains every revision when each one is referenced by an ISVC (refs beat retention)", func() {
		// retention=2; create N=3 revisions for one runtime; create N
		// ISVCs each pinning a different revision. The reference set
		// should keep every revision marked Keep regardless of position
		// (the existing "retains referenced revision" spec only covers
		// the single-reference case).
		ns, err := util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "gc-all-refs-")
		Expect(err).NotTo(HaveOccurred())
		ginkgo.DeferCleanup(func() { _ = util.DeleteNamespace(suiteCtx, k8sClient, ns.Name) })

		names := []string{"cr-rt-allref-001", "cr-rt-allref-002", "cr-rt-allref-003"}

		// Create ISVCs first so the reference set is populated from the
		// first GC reconcile — same race-avoidance pattern as the
		// "retains referenced revision" spec above.
		for i, n := range names {
			pin := n
			isvc := &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("gc-all-pinner-%d", i), Namespace: ns.Name},
				Spec: v1beta1.InferenceServiceSpec{
					Runtime: &v1beta1.ServingRuntimeRef{
						Name:     "rt-allref",
						AutoSync: ptrBool(false),
						Revision: &pin,
					},
					Engine: &v1beta1.EngineSpec{},
				},
			}
			Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())
		}

		// Now create the 3 revisions. With retention=2 the oldest
		// would normally be marked — but every revision is pinned.
		Expect(k8sClient.Create(suiteCtx, mkRev(names[0], "rt-allref", 0, true))).To(Succeed())
		time.Sleep(1100 * time.Millisecond)
		createOrdered("rt-allref", names[1:])

		Eventually(func(g Gomega) {
			for _, n := range names {
				r, err := getRev(n)
				g.Expect(err).NotTo(HaveOccurred(), n)
				g.Expect(r.Annotations).NotTo(HaveKey(constants.RuntimeRevisionGCEligibleSinceKey), n)
			}
		}, 5*time.Second, 200*time.Millisecond).Should(Succeed())

		Consistently(func(g Gomega) {
			for _, n := range names {
				r, err := getRev(n)
				g.Expect(err).NotTo(HaveOccurred(), n)
				g.Expect(r.Annotations).NotTo(HaveKey(constants.RuntimeRevisionGCEligibleSinceKey),
					"all-referenced revision %s must NOT be marked", n)
			}
		}, 12*time.Second, 500*time.Millisecond).Should(Succeed())
	})

	ginkgo.It("marks a previously-pinned revision after its ISVC is deleted", func() {
		// retention=2; oldest is pinned so it stays a Keep. Once the
		// ISVC is deleted the reference set shrinks and the oldest
		// should fall out → controller observes it and adds the
		// gc-eligible-since annotation.
		ns, err := util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "gc-isvc-del-")
		Expect(err).NotTo(HaveOccurred())
		ginkgo.DeferCleanup(func() { _ = util.DeleteNamespace(suiteCtx, k8sClient, ns.Name) })

		oldest := "cr-rt-del-isvc-001"
		isvc := &v1beta1.InferenceService{
			ObjectMeta: metav1.ObjectMeta{Name: "gc-del-pinner", Namespace: ns.Name},
			Spec: v1beta1.InferenceServiceSpec{
				Runtime: &v1beta1.ServingRuntimeRef{
					Name:     "rt-del-isvc",
					AutoSync: ptrBool(false),
					Revision: &oldest,
				},
				Engine: &v1beta1.EngineSpec{},
			},
		}
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())

		Expect(k8sClient.Create(suiteCtx, mkRev(oldest, "rt-del-isvc", 0, true))).To(Succeed())
		time.Sleep(1100 * time.Millisecond)
		createOrdered("rt-del-isvc", []string{"cr-rt-del-isvc-002", "cr-rt-del-isvc-003"})

		// Sanity-check: while the ISVC exists the oldest must NOT be
		// marked. Same Eventually-then-Consistently pattern as the
		// other reference-keeps-it specs.
		Eventually(func(g Gomega) {
			r, err := getRev(oldest)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(r.Annotations).NotTo(HaveKey(constants.RuntimeRevisionGCEligibleSinceKey))
		}, 5*time.Second, 200*time.Millisecond).Should(Succeed())

		Expect(k8sClient.Delete(suiteCtx, isvc)).To(Succeed())

		// ISVC delete event → GC singleton enqueue → reconcile sees
		// the reference is gone, oldest is now over retention → Mark.
		Eventually(func(g Gomega) {
			r, err := getRev(oldest)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(r.Annotations).To(HaveKey(constants.RuntimeRevisionGCEligibleSinceKey),
				"after ISVC delete the oldest revision should fall out of refs and be marked")
		}, 8*time.Second, 200*time.Millisecond).Should(Succeed())
	})

	ginkgo.It("clears gc-eligible-since when a marked revision is referenced before grace expires", func() {
		// retention=2; create 3 revisions. Oldest will be marked.
		// Then create an ISVC pinning to it → annotation cleared.
		oldest := "cr-rt-clear-001"
		Expect(k8sClient.Create(suiteCtx, mkRev(oldest, "rt-clear", 0, true))).To(Succeed())
		time.Sleep(1100 * time.Millisecond)
		createOrdered("rt-clear", []string{"cr-rt-clear-002", "cr-rt-clear-003"})

		// Wait for oldest to be marked.
		Eventually(func(g Gomega) {
			r, err := getRev(oldest)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(r.Annotations).To(HaveKey(constants.RuntimeRevisionGCEligibleSinceKey))
		}, 5*time.Second, 200*time.Millisecond).Should(Succeed())

		// Pin an ISVC to it (need the CSR for the ISVC's runtime ref shape).
		cbm := v1beta1testing.MakeClusterBaseModel("gc-clear-cbm").
			StorageURI("oci://n/test/b/models/o/llama").
			ModelFormat("safetensors").
			ModelArchitecture("LlamaForCausalLM").
			ModelFramework("Transformers").Obj()
		Expect(k8sClient.Create(suiteCtx, cbm)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, cbm) })
		csr := v1beta1testing.MakeClusterServingRuntime("gc-clear-csr").
			SupportsModelFormat("safetensors", true, 10).
			WithLastFormatArchitecture("LlamaForCausalLM").Obj()
		Expect(k8sClient.Create(suiteCtx, csr)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, csr) })

		ns, err := util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "gc-clear-")
		Expect(err).NotTo(HaveOccurred())
		ginkgo.DeferCleanup(func() { _ = util.DeleteNamespace(suiteCtx, k8sClient, ns.Name) })

		isvc := &v1beta1.InferenceService{
			ObjectMeta: metav1.ObjectMeta{Name: "gc-clear-pinner", Namespace: ns.Name},
			Spec: v1beta1.InferenceServiceSpec{
				Runtime: &v1beta1.ServingRuntimeRef{
					Name:     "gc-clear-csr",
					AutoSync: ptrBool(false),
					Revision: &oldest,
				},
				Engine: &v1beta1.EngineSpec{},
			},
		}
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())

		// Annotation should be cleared on the next resync — race with
		// the delete clock, so we have at most grace-period seconds.
		Eventually(func(g Gomega) {
			r, err := getRev(oldest)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(r.Annotations).NotTo(HaveKey(constants.RuntimeRevisionGCEligibleSinceKey))
		}, 5*time.Second, 200*time.Millisecond).Should(Succeed())
	})
})

func ptrBool(b bool) *bool { return &b }
