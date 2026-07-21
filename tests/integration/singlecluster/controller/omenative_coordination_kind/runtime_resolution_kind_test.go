package omenative_coordination_kind

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/tests/integration/kindutil"
	"github.com/ome-projects/ome/tests/integration/util"
)

// Runtime + Model surface coverage for OMENative.
//
// RT-1 (CSR engineConfig.runner.image fills in when ISVC omits image),
// RT-2 (ISVC image wins over CSR), RT-3 (CSR + ISVC env merge) are
// pinned at the pkg level in:
//   - pkg/.../reconcilers/common/podspec_reconciler_test.go
//     TestPodSpecReconciler_ImagePrecedence (image precedence matrix)
//   - pkg/.../utils/merging_test.go (env-var union, override, etc.)
//
// Only the controller-level "what happens when the dependency isn't
// there at create time" cases need integration coverage; those are
// what this file ports.
var _ = ginkgo.Describe("OMENative runtime/model resolution (KIND)", ginkgo.Ordered, func() {
	var ns *corev1.Namespace

	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "omenative-rt-kind-")
		Expect(err).NotTo(HaveOccurred())
	})

	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	// RT-4 — ISVC created before CBM stalls without crashing; once the
	// CBM is created, the reconciler picks it up and converges to
	// Ready. Pins the reconciler's resilience to dependency ordering.
	ginkgo.It("RT-4: ISVC stalls until missing CBM is created, then converges", func() {
		fix := newCoordFixture(ns.Name, WithISVCName("rt4-isvc"),
			WithoutDecoder(),
			WithEngineImage("fake-serving:v1"))

		// CSR + ISVC only — leave CBM uncreated.
		Expect(k8sClient.Create(suiteCtx, fix.CSR)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CSR) })
		Expect(k8sClient.Create(suiteCtx, fix.ISVC)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.ISVC) })

		ginkgo.By("no engine pod should exist while CBM is missing")
		Consistently(func(g Gomega) {
			err := k8sClient.Get(suiteCtx, client.ObjectKey{
				Namespace: ns.Name, Name: "rt4-isvc-engine-0-default-0",
			}, &corev1.Pod{})
			g.Expect(err).To(HaveOccurred(),
				"pod must NOT exist until CBM is Ready; controller must stall (and not crash)")
			g.Expect(apierrors.IsNotFound(err)).To(BeTrue(),
				"missing CBM should produce NotFound, not some other error")
		}, "3s", kindPollInterval).Should(Succeed())

		ginkgo.By("creating the CBM unblocks the reconciler")
		Expect(k8sClient.Create(suiteCtx, fix.CBM)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CBM) })
		Expect(kindutil.LabelClusterBaseModelReady(suiteCtx, k8sClient, fix.CBM.Name)).
			To(Succeed(), "label nodes for CBM after late create")

		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "rt4-isvc")
			cs := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.Lifecycle).NotTo(BeNil())
			insts := instanceStatuses(g, ns.Name, "rt4-isvc", v1beta1.EngineComponent)
			g.Expect(insts).To(HaveLen(1))
			g.Expect(insts[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})

	// RT-5 — Missing CSR: no Service or CR is created. The ISVC
	// reconciler short-circuits before reaching DetermineDeploymentModes,
	// so OMENative dispatch never runs.
	ginkgo.It("RT-5: missing CSR leaves no Service or ControllerRevision behind", func() {
		fix := newCoordFixture(ns.Name, WithISVCName("rt5-isvc"),
			WithoutDecoder(),
			WithEngineImage("fake-serving:v1"))

		Expect(k8sClient.Create(suiteCtx, fix.CBM)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CBM) })
		// Skip CSR Create — leave it missing.
		Expect(k8sClient.Create(suiteCtx, fix.ISVC)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.ISVC) })

		ginkgo.By("no headless Service should be created while CSR is missing")
		Consistently(func(g Gomega) {
			err := k8sClient.Get(suiteCtx, client.ObjectKey{
				Namespace: ns.Name, Name: "rt5-isvc-engine-headless",
			}, &corev1.Service{})
			g.Expect(err).To(HaveOccurred(),
				"OMENative-owned headless Service must NOT exist until CSR is created")
			g.Expect(apierrors.IsNotFound(err)).To(BeTrue(),
				"missing CSR should produce NotFound, not some other error")
		}, "3s", kindPollInterval).Should(Succeed())

		ginkgo.By("no ControllerRevision should be created either")
		Consistently(func(g Gomega) {
			g.Expect(listOMENativeCRs(g, ns.Name)).To(BeEmpty(),
				"no CR should exist; OMENative dispatch never ran")
		}, "2s", kindPollInterval).Should(Succeed())
	})
})
