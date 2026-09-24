package inferenceservice_admission

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/utils/ptr"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	v1beta1testing "github.com/ome-projects/ome/pkg/utils/testing/v1beta1"
)

// ScalingPolicy admission through the full chain, including the
// update-time ratchet: only a write that sets or changes the policy is
// re-validated, so a stored object keeps accepting unrelated updates.
var _ = ginkgo.Describe("ISVC admission matrix: scaling policy", func() {

	ginkgo.BeforeEach(ensureNamespace)

	ginkgo.It("create with an unimplemented scalingPolicy mode is rejected", func() {
		isvc := baselineISVC("admissio-sp-proportional").Obj()
		isvc.Spec.ScalingPolicy = &v1beta1.ScalingPolicy{Mode: v1beta1.ScalingProportional}
		err := k8sClient.Create(suiteCtx, isvc)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("ScalingModeNotImplemented"),
			"a mode no controller applies must be rejected, never stored as a silent no-op")
		Expect(err.Error()).To(ContainSubstring("spec.scalingPolicy"))
	})

	ginkgo.It("stored Independent policy admits unrelated updates but rejects a switch to an unimplemented mode", func() {
		isvc := baselineISVC("admissio-sp-ratchet").Obj()
		isvc.Spec.ScalingPolicy = &v1beta1.ScalingPolicy{Mode: v1beta1.ScalingIndependent}
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, isvc) })

		ginkgo.By("an update that does not touch the policy is admitted")
		got := mustGet("admissio-sp-ratchet")
		if got.Labels == nil {
			got.Labels = map[string]string{}
		}
		got.Labels["admissio-sp"] = "unrelated"
		Expect(k8sClient.Update(suiteCtx, got)).To(Succeed())

		ginkgo.By("an update that switches the mode is re-validated and rejected")
		got = mustGet("admissio-sp-ratchet")
		got.Spec.ScalingPolicy = &v1beta1.ScalingPolicy{Mode: v1beta1.ScalingPinned}
		err := k8sClient.Update(suiteCtx, got)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("ScalingModeNotImplemented"))
	})
})

// ome.io/* annotation validation through the full chain. The
// revision-history-limit key is typed as an integer by the traffic
// annotation classifier; a malformed value must be rejected at
// admission instead of surfacing later as a controller parse failure.
var _ = ginkgo.Describe("ISVC admission matrix: revision-history-limit annotation", func() {

	ginkgo.BeforeEach(ensureNamespace)

	ginkgo.It("create with a non-integer revision-history-limit is rejected", func() {
		isvc := baselineISVC("admissio-rhl-bad").
			Annotation(constants.RevisionHistoryLimitAnnotation, "many").Obj()
		err := k8sClient.Create(suiteCtx, isvc)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("InvalidIntValue"))
		Expect(err.Error()).To(ContainSubstring(constants.RevisionHistoryLimitAnnotation))
	})

	ginkgo.It("create with a valid revision-history-limit is admitted and stored", func() {
		isvc := baselineISVC("admissio-rhl-ok").
			Annotation(constants.RevisionHistoryLimitAnnotation, "5").Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, isvc) })

		got := mustGet("admissio-rhl-ok")
		Expect(got.Annotations).To(HaveKeyWithValue(constants.RevisionHistoryLimitAnnotation, "5"))
	})
})

// Admission stores an OMENative InferenceService as authored: no lifecycle
// policy, rollout budget, worker size, or deployment-mode annotation is
// written. The controller resolves all of them after the runtime merge, so
// the validator only ever judges explicit values.
var _ = ginkgo.Describe("ISVC admission matrix: nothing is defaulted at admission", func() {

	ginkgo.BeforeEach(ensureNamespace)

	runner := func(image string) *v1beta1.RunnerSpec {
		return &v1beta1.RunnerSpec{Container: corev1.Container{Name: "ome-container", Image: image}}
	}
	multiPodEngine := func(size *int) *v1beta1.EngineSpec {
		return &v1beta1.EngineSpec{
			Leader: &v1beta1.LeaderSpec{Runner: runner("ghcr.io/example/runtime:v1")},
			Worker: &v1beta1.WorkerSpec{Size: size, Runner: runner("ghcr.io/example/runtime:v1")},
		}
	}
	// omeNativeISVC declares the mode through the typed spec field only, so
	// the stored annotations show exactly what admission wrote.
	omeNativeISVC := func(name string, engine *v1beta1.EngineSpec) *v1beta1testing.InferenceServiceWrapper {
		w := v1beta1testing.MakeInferenceService(name, "admissio-matrix").
			Runtime("admissio-runtime").
			AutoSync(false).
			Engine(engine)
		w.Spec.DeploymentMode = ptr.To(constants.OMENative)
		return w
	}

	ginkgo.It("multi-pod OMENative create without lifecycle stores no lifecycle and no annotation", func() {
		isvc := omeNativeISVC("admissio-mn-authored", multiPodEngine(ptr.To(2))).
			Router(admissionRouter()).
			Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, isvc) })

		got := mustGet("admissio-mn-authored")
		Expect(got.Spec.Engine.Lifecycle).To(BeNil(), "lifecycle policies resolve at reconcile time")
		Expect(got.Spec.Router.Lifecycle).To(BeNil())
		Expect(got.Spec.Engine.Worker.Size).To(HaveValue(Equal(2)))
		Expect(got.Annotations).NotTo(HaveKey(constants.DeploymentMode))
	})

	ginkgo.It("leader+worker pair without a size is admitted and stores the size unset", func() {
		isvc := omeNativeISVC("admissio-worker-size", multiPodEngine(nil)).Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, isvc) })

		got := mustGet("admissio-worker-size")
		Expect(got.Spec.Engine.Worker.Size).To(BeNil(),
			"the size resolves to the runtime's value or one worker at reconcile time")
		Expect(got.Spec.Engine.Lifecycle).To(BeNil())
	})

	ginkgo.It("readyPolicy None on a size-less leader+worker pair is rejected like any multi-pod pair", func() {
		engine := multiPodEngine(nil)
		engine.Lifecycle = &v1beta1.LifecycleSpec{ReadyPolicy: ptr.To(v1beta1.InstanceReadyPolicyNone)}
		isvc := omeNativeISVC("admissio-none-unsized", engine).Obj()
		err := k8sClient.Create(suiteCtx, isvc)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("is not allowed on a multi-pod OMENative component"))
	})
})
