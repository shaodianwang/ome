package inferenceservice_admission

import (
	"time"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

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

// OMENative lifecycle defaulting × validation. The defaulter stamps the
// exact readyPolicy the validator demands for multi-pod Components
// (AllPodReady), so an operator who omits the field must land on the
// valid default — this interplay only exists with both webhooks
// installed.
var _ = ginkgo.Describe("ISVC admission matrix: OMENative lifecycle defaults", func() {

	ginkgo.BeforeEach(ensureNamespace)

	runner := func(image string) *v1beta1.RunnerSpec {
		return &v1beta1.RunnerSpec{Container: corev1.Container{Name: "ome-container", Image: image}}
	}
	multiPodEngine := func(size int) *v1beta1.EngineSpec {
		return &v1beta1.EngineSpec{
			Leader: &v1beta1.LeaderSpec{Runner: runner("ghcr.io/example/runtime:v1")},
			Worker: &v1beta1.WorkerSpec{Size: &size, Runner: runner("ghcr.io/example/runtime:v1")},
		}
	}

	ginkgo.It("multi-pod OMENative create without lifecycle stores the full shape-dependent defaults", func() {
		isvc := v1beta1testing.MakeInferenceService("admissio-mn-defaults", "admissio-matrix").
			Runtime("admissio-runtime").
			AutoSync(false).
			DeploymentMode(constants.OMENative).
			Engine(multiPodEngine(2)).
			Router(admissionRouter()).
			Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, isvc) })

		got := mustGet("admissio-mn-defaults")

		ginkgo.By("engine (multi-pod) defaults")
		lc := got.Spec.Engine.Lifecycle
		Expect(lc).NotTo(BeNil())
		Expect(lc.ReadyPolicy).To(HaveValue(Equal(v1beta1.InstanceReadyPolicyAllPodReady)),
			"multi-pod readiness must default to the aggregation the validator requires")
		Expect(lc.RestartPolicy).To(HaveValue(Equal(v1beta1.InstanceRestartPolicyRecreateInstance)))
		Expect(lc.UpdateStrategy).NotTo(BeNil())
		Expect(lc.UpdateStrategy.Type).To(Equal(v1beta1.UpdateStrategySurgeThenDrain))
		Expect(lc.UpdateStrategy.RollingUpdate).NotTo(BeNil())
		Expect(lc.UpdateStrategy.RollingUpdate.MaxSurge).To(HaveValue(Equal(intstr.FromString("25%"))))
		Expect(lc.UpdateStrategy.RollingUpdate.MaxUnavailable).To(BeNil(),
			"SurgeThenDrain is gated on MaxSurge and never reads MaxUnavailable, so the defaulter leaves it unset")
		Expect(lc.InstanceReadyTimeout).NotTo(BeNil())
		Expect(lc.InstanceReadyTimeout.Duration).To(Equal(30 * time.Minute))
		Expect(lc.MigrationPolicy).NotTo(BeNil())
		Expect(lc.MigrationPolicy.Mode).To(Equal(v1beta1.MigrationPolicyModeAuto))

		ginkgo.By("router (always single-pod) defaults")
		rlc := got.Spec.Router.Lifecycle
		Expect(rlc).NotTo(BeNil())
		Expect(rlc.ReadyPolicy).To(HaveValue(Equal(v1beta1.InstanceReadyPolicyNone)))
		Expect(rlc.RestartPolicy).To(HaveValue(Equal(v1beta1.InstanceRestartPolicyNone)))
	})

	ginkgo.It("single-pod OMENative engine leaves shape-dependent policies unset for runtime resolution", func() {
		// A raw single-pod engine may still inherit a Leader/Worker shape
		// from its runtime, so the defaulter must NOT commit readyPolicy or
		// restartPolicy at admission — only the shape-independent defaults.
		engine := admissionEngine()
		engine.ComponentExtensionSpec.Annotations = map[string]string{
			constants.DeploymentMode: string(constants.OMENative),
		}
		isvc := v1beta1testing.MakeInferenceService("admissio-sp-unresolved", "admissio-matrix").
			Runtime("admissio-runtime").
			AutoSync(false).
			Engine(engine).
			Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, isvc) })

		got := mustGet("admissio-sp-unresolved")
		lc := got.Spec.Engine.Lifecycle
		Expect(lc).NotTo(BeNil())
		Expect(lc.ReadyPolicy).To(BeNil(),
			"readyPolicy is shape-dependent and must stay unset while the shape is unresolved")
		Expect(lc.RestartPolicy).To(BeNil(),
			"restartPolicy is shape-dependent and must stay unset while the shape is unresolved")
		Expect(lc.UpdateStrategy).NotTo(BeNil())
		Expect(lc.UpdateStrategy.Type).To(Equal(v1beta1.UpdateStrategySurgeThenDrain))
		Expect(lc.InstanceReadyTimeout).NotTo(BeNil())
		Expect(lc.InstanceReadyTimeout.Duration).To(Equal(30 * time.Minute))
		Expect(lc.MigrationPolicy).NotTo(BeNil())
		Expect(lc.MigrationPolicy.Mode).To(Equal(v1beta1.MigrationPolicyModeAuto))
	})

	ginkgo.It("leader+worker pair without an explicit size stores worker.size=1 and multi-pod defaults", func() {
		engine := &v1beta1.EngineSpec{
			Leader: &v1beta1.LeaderSpec{Runner: runner("ghcr.io/example/runtime:v1")},
			Worker: &v1beta1.WorkerSpec{Runner: runner("ghcr.io/example/runtime:v1")},
		}
		isvc := v1beta1testing.MakeInferenceService("admissio-worker-size", "admissio-matrix").
			Runtime("admissio-runtime").
			AutoSync(false).
			DeploymentMode(constants.OMENative).
			Engine(engine).
			Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, isvc) })

		got := mustGet("admissio-worker-size")
		Expect(got.Spec.Engine.Worker.Size).To(HaveValue(Equal(1)),
			"a declared Leader+Worker pair without an explicit size must default to 1")
		// Size=1 resolves the shape to multi-pod before the lifecycle
		// defaults run, so the multi-pod readiness default applies.
		Expect(got.Spec.Engine.Lifecycle).NotTo(BeNil())
		Expect(got.Spec.Engine.Lifecycle.ReadyPolicy).To(HaveValue(Equal(v1beta1.InstanceReadyPolicyAllPodReady)))
	})
})
