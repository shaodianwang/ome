package autoscalerpolicy

import (
	"sync"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/validation"
)

// warningRecorder captures admission warning headers so a spec can assert
// on the exact warnings a write produced.
type warningRecorder struct {
	mu       sync.Mutex
	warnings []string
}

func (w *warningRecorder) HandleWarningHeader(_ int, _ string, text string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.warnings = append(w.warnings, text)
}

func (w *warningRecorder) snapshot() []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]string(nil), w.warnings...)
}

var _ = ginkgo.Describe("InferenceService policy-ref feature gate", func() {

	// disabledNamespace routes writes through the second validator instance
	// (AutoscalerPolicyEnabled=false) via its namespaceSelector.
	ginkgo.It("rejects any ref end-to-end while the feature is disabled", func() {
		isvc := mkConsumer(uniqueName("consumer"), uniqueName("request-activity"))
		isvc.Namespace = disabledNamespace
		msg := denial(k8sClient.Create(suiteCtx, isvc))
		gomega.Expect(msg).To(gomega.ContainSubstring(validation.ReasonAutoscalerPolicyFeatureDisabled))

		// Control: the same shape without a ref is admitted — the disabled
		// validator gates the ref, not the namespace.
		noRef := mkConsumer(uniqueName("consumer"), "unused")
		noRef.Namespace = disabledNamespace
		noRef.Spec.Engine.AutoscalerPolicyRef = nil
		mustCreate(noRef)
	})

	// Inline + ref coexistence is the operator escape hatch: the inline
	// block outranks the ref at resolution, so admission must accept the
	// pair rather than force a two-step migration.
	ginkgo.It("admits a ref coexisting with an inline autoscaler block", func() {
		isvc := mkConsumer(uniqueName("consumer"), uniqueName("request-activity"))
		isvc.Spec.Engine.ComponentExtensionSpec.Autoscaler = &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerHPA}
		mustCreate(isvc)
	})

	ginkgo.It("surfaces Split placement with a ref and no per-cluster cap as an admission warning", func() {
		recorder := &warningRecorder{}
		cfg := rest.CopyConfig(suiteCfg)
		cfg.WarningHandler = recorder
		warned, err := client.New(cfg, client.Options{Scheme: k8sClient.Scheme()})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		isvc := mkConsumer(uniqueName("consumer"), uniqueName("request-activity"))
		isvc.Spec.Placement = &v1beta1.PlacementSpec{
			Policy: v1beta1.PlacementPolicyClusterAffinity,
			Mode:   v1beta1.PlacementModeSplit,
		}
		gomega.Expect(warned.Create(suiteCtx, isvc)).To(gomega.Succeed(),
			"the unbounded-ceiling combination warns, never rejects")
		ginkgo.DeferCleanup(func() {
			gomega.Expect(client.IgnoreNotFound(k8sClient.Delete(suiteCtx, isvc))).To(gomega.Succeed())
		})

		gomega.Expect(recorder.snapshot()).To(gomega.ContainElement(gomega.And(
			gomega.ContainSubstring("maxReplicasPerCluster"),
			gomega.ContainSubstring(v1beta1.PlacementPolicyPreflightReasonUnboundedSplitCeiling),
		)), "the webhook response must carry the unbounded-split-ceiling warning")
	})
})

var _ = ginkgo.Describe("AutoscalerPolicy delete admission in a terminating namespace", func() {

	// envtest has no namespace controller, so a deleted namespace parks in
	// Terminating — exactly the teardown window the in-use exemption
	// covers: teardown deletes objects in arbitrary order and must not
	// wedge on the reference check.
	ginkgo.It("admits an in-use deletion once the namespace is terminating", func() {
		nsName := uniqueName("aspolicy-terminating")
		gomega.Expect(k8sClient.Create(suiteCtx, &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{Name: nsName},
		})).To(gomega.Succeed())

		policy := mkPolicy(uniqueName("request-activity"), validKedaSpec())
		policy.Namespace = nsName
		consumer := mkConsumer(uniqueName("consumer"), policy.Name)
		consumer.Namespace = nsName
		mustCreate(policy, consumer)

		gomega.Expect(k8sClient.Delete(suiteCtx, &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{Name: nsName},
		})).To(gomega.Succeed())
		gomega.Eventually(func(g gomega.Gomega) {
			terminating := &corev1.Namespace{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: nsName}, terminating)).To(gomega.Succeed())
			g.Expect(terminating.DeletionTimestamp.IsZero()).To(gomega.BeFalse())
		}).Should(gomega.Succeed())

		gomega.Expect(k8sClient.Delete(suiteCtx, policy)).To(gomega.Succeed(),
			"the in-use check must not block deletes inside a terminating namespace")
	})
})
