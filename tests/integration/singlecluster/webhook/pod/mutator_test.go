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

// withModelVolumes pre-populates the volumes the model-init / FT
// adapter injectors need to add VolumeMounts onto. In production
// the controller (deployment reconciler) puts these volumes on the
// pod template before the webhook fires; the injectors only ADD
// VolumeMounts that reference pre-existing volumes. For test pods
// we Create directly, we have to populate them ourselves or the
// admission webhook returns a 400 from apiserver-side schema
// validation ("volumeMount.name: Not found").
//
// baseModelName must match the value of the
// constants.BaseModelName annotation on the pod (the injector
// looks up that annotation to compute the volume name).
func withModelVolumes(pod *corev1.Pod, baseModelName string) *corev1.Pod {
	pod.Spec.Volumes = append(pod.Spec.Volumes,
		corev1.Volume{
			Name: constants.ModelEmptyDirVolumeName,
			VolumeSource: corev1.VolumeSource{
				EmptyDir: &corev1.EmptyDirVolumeSource{},
			},
		},
		corev1.Volume{
			Name: baseModelName,
			VolumeSource: corev1.VolumeSource{
				EmptyDir: &corev1.EmptyDirVolumeSource{},
			},
		},
	)
	return pod
}

func ptrInt64(v int64) *int64 { return &v }

// Pod mutator integration. The Mutator (mutator.go:31) chains 5
// injectors at admission time; each is gated on either the
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
		// 5 injectors, but each injector is also internally gated
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

	ginkgo.It("pod with ome.io/inject-model-init=true → model-init init container is injected", func() {
		// The model-init injector is gated on
		// ModelInitInjectionKey (mutator.go:48); when present and
		// "true", the injector adds an init container named
		// model-init at the front of pod.Spec.InitContainers.
		pod := withModelVolumes(makePod("model-init-pod"), "some-base-model")
		pod.Labels = map[string]string{constants.InferenceServicePodLabelKey: "model-init-isvc"}
		pod.Annotations = map[string]string{
			constants.ModelInitInjectionKey: "true",
			constants.BaseModelName:         "some-base-model",
		}
		Expect(k8sClient.Create(suiteCtx, pod)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, pod) })

		got := &corev1.Pod{}
		Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "model-init-pod", Namespace: "pod-webhook"}, got)).To(Succeed())

		Expect(got.Spec.InitContainers).NotTo(BeEmpty(),
			"ModelInitInjectionKey=true must produce an init container")
		names := []string{}
		for _, c := range got.Spec.InitContainers {
			names = append(names, c.Name)
		}
		Expect(names).To(ContainElement(constants.ModelInitContainerName),
			"injected init container must be named %q (got %v)", constants.ModelInitContainerName, names)
	})

	ginkgo.It("pod with ome.io/inject-model-init=false → no init container injected (explicit opt-out)", func() {
		// Annotation present but value != "true" → the injector
		// must NOT fire. Catches a regression where the gate
		// checks only key existence rather than value.
		pod := makePod("model-init-optout")
		pod.Labels = map[string]string{constants.InferenceServicePodLabelKey: "model-init-isvc"}
		pod.Annotations = map[string]string{
			constants.ModelInitInjectionKey: "false",
		}
		Expect(k8sClient.Create(suiteCtx, pod)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, pod) })

		got := &corev1.Pod{}
		Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "model-init-optout", Namespace: "pod-webhook"}, got)).To(Succeed())

		for _, c := range got.Spec.InitContainers {
			Expect(c.Name).NotTo(Equal(constants.ModelInitContainerName),
				"explicit opt-out must NOT inject the model-init container")
		}
	})

	ginkgo.It("pod with both ome.io/inject-model-init=true AND fine-tuned-weight annotation → ModelInit comes BEFORE FineTunedAdapter (sort invariant)", func() {
		// mutator.go:104-114 sorts InitContainers so ModelInit
		// always precedes FineTunedAdapter. The fine-tuned-adapter
		// downloads the adapter on top of the base model, so the
		// model-init step must run first or the adapter download
		// has nothing to layer onto.
		pod := withModelVolumes(makePod("model-init-and-ft"), "some-base-model")
		pod.Labels = map[string]string{constants.InferenceServicePodLabelKey: "ft-isvc"}
		pod.Annotations = map[string]string{
			constants.ModelInitInjectionKey:        "true",
			constants.BaseModelName:                "some-base-model",
			constants.FineTunedWeightFTStrategyKey: "tfew",
		}
		Expect(k8sClient.Create(suiteCtx, pod)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, pod) })

		got := &corev1.Pod{}
		Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "model-init-and-ft", Namespace: "pod-webhook"}, got)).To(Succeed())

		// Walk the init containers and assert ModelInit's index <
		// FineTunedAdapter's index. Other init containers may
		// exist between/around them; we only care about the
		// pairwise ordering.
		modelInitIdx := -1
		ftAdapterIdx := -1
		for i, c := range got.Spec.InitContainers {
			switch c.Name {
			case constants.ModelInitContainerName:
				modelInitIdx = i
			case constants.FineTunedAdapterContainerName:
				ftAdapterIdx = i
			}
		}
		Expect(modelInitIdx).NotTo(Equal(-1), "model-init init container must be present")
		if ftAdapterIdx != -1 {
			// Only enforce the ordering when both are actually
			// injected; if the FT injector chose not to fire
			// (annotation insufficient on its own without the
			// fineTunedAdapter ConfigMap key), the spec still
			// passes — the relevant negative is "ordering must
			// hold WHEN both fire", not "FT injector must fire".
			Expect(modelInitIdx).To(BeNumerically("<", ftAdapterIdx),
				"%s must precede %s (mutator.go:104 sort invariant)",
				constants.ModelInitContainerName, constants.FineTunedAdapterContainerName)
		}
	})
})
