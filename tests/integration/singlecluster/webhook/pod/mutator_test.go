package pod

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/ome-projects/ome/pkg/constants"
)

// ensureNamespace creates the "pod-webhook" namespace shared by
// every spec. Centralized so each Describe's BeforeEach is one
// line and there's no risk of namespace drift across files.
func ensureNamespace() {
	ginkgo.GinkgoHelper()
	_ = k8sClient.Create(suiteCtx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: "pod-webhook"},
	})
}

// makePod returns a minimal-but-valid Pod spec with a single
// container so apiserver admission accepts it. Callers chain Label
// or annotate via the returned object.
func makePod(name string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "pod-webhook"},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{
				Name:  "ome-container",
				Image: "ghcr.io/test/runtime:test",
			}},
			// Set a short termination grace so envtest doesn't hold
			// onto Terminating pods between specs.
			TerminationGracePeriodSeconds: ptrInt64(0),
		},
	}
}

func ptrInt64(v int64) *int64 { return &v }

// Pod mutator integration. The Mutator (mutator.go:31) chains
// several injectors at admission time; each is gated on either the
// InferenceServicePodLabelKey / TrainingJobPodLabelKey on the pod
// or specific ome.io annotations. These specs cover the gating
// contract end-to-end with the real apiserver dialing /mutate-pods.
var _ = ginkgo.Describe("Pod mutator webhook", func() {

	ginkgo.BeforeEach(ensureNamespace)

	ginkgo.It("pod WITHOUT inferenceservice label is passed through unchanged", func() {
		// needMutate (mutator.go:130) gates on
		// InferenceServicePodLabelKey or TrainingJobPodLabelKey.
		// Without either, the mutator returns a no-op response —
		// proves we don't accidentally mutate user/system pods.
		pod := makePod("plain-pod")
		Expect(k8sClient.Create(suiteCtx, pod)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, pod) })

		got := &corev1.Pod{}
		Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "plain-pod", Namespace: "pod-webhook"}, got)).To(Succeed())

		Expect(got.Spec.InitContainers).To(BeEmpty(),
			"unlabeled pods must not get init containers injected")
		Expect(got.Spec.Containers).To(HaveLen(1),
			"unlabeled pods must keep their original container set unmodified")
		Expect(got.Spec.Containers[0].Name).To(Equal("ome-container"))
	})

	ginkgo.It("pod WITH inferenceservice label but no inject-trigger annotations passes through (gating works at injector level)", func() {
		// The label triggers needMutate=true, the mutator runs all
		// of its chained injectors, but each one is internally gated
		// by its own annotation/config — without those, none of
		// them adds anything. Net: pod gets through unchanged.
		// Catches a regression where the mutator unconditionally
		// injects regardless of per-injector triggers.
		pod := makePod("labeled-no-trigger")
		pod.Labels = map[string]string{constants.InferenceServicePodLabelKey: "some-isvc"}
		Expect(k8sClient.Create(suiteCtx, pod)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, pod) })

		got := &corev1.Pod{}
		Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "labeled-no-trigger", Namespace: "pod-webhook"}, got)).To(Succeed())

		// Spec passes if the pod was admitted (Get succeeds) and
		// has no unexpected init containers from the injectors.
		// Some injectors might add metadata-only changes like
		// labels/annotations; we only assert on the container set
		// to avoid false positives from those.
		Expect(got.Spec.InitContainers).To(BeEmpty(),
			"no injector should add an init container without its specific trigger annotation")
	})
})
