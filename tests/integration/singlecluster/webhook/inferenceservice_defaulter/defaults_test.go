package inferenceservice_defaulter

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	v1beta1testing "github.com/ome-projects/ome/pkg/utils/testing/v1beta1"
)

// ensureNamespace creates the "isvc-defaulter" namespace shared by
// every spec.
func ensureNamespace() {
	ginkgo.GinkgoHelper()
	_ = k8sClient.Create(suiteCtx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: "isvc-defaulter"},
	})
}

// engineWithImage returns an EngineSpec populated with one
// container so apiserver schema validation is satisfied. Defaulter
// behavior depends on which top-level fields are set
// (Engine/Decoder/Engine.Leader+Worker), not on container details.
func engineWithImage(image string) *v1beta1.EngineSpec {
	return &v1beta1.EngineSpec{
		PodSpec: v1beta1.PodSpec{
			Containers: []corev1.Container{{Name: "ome-container", Image: image}},
		},
	}
}

// InferenceServiceDefaulter (inference_service_defaults.go) sets
// the ome.io/deploymentMode annotation based on the shape of the
// ISVC's Engine/Decoder fields. These specs assert each branch of
// that decision tree end-to-end with the real apiserver dialing
// /mutate-ome-io-v1beta1-inferenceservice.
var _ = ginkgo.Describe("InferenceService defaulter webhook", func() {

	ginkgo.BeforeEach(ensureNamespace)

	ginkgo.It("Engine only + no annotation → defaultDeploymentMode from inferenceservice-config (RawDeployment)", func() {
		// SeedDefaultConfigMaps puts deploy.defaultDeploymentMode
		// =RawDeployment in the CM (util/seed.go). Engine without
		// Decoder and without the multi-node signal must inherit
		// that default.
		isvc := v1beta1testing.MakeInferenceService("def-engine-only", "isvc-defaulter").
			Model("any-model").
			Engine(engineWithImage("ghcr.io/test/runtime:test")).Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, isvc) })

		got := &v1beta1.InferenceService{}
		Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "def-engine-only", Namespace: "isvc-defaulter"}, got)).To(Succeed())
		Expect(got.Annotations).To(HaveKeyWithValue(constants.DeploymentMode, string(constants.RawDeployment)))
	})

	ginkgo.It("Engine + Decoder → DeploymentMode=PDDisaggregated", func() {
		// Both components present → defaulter sets PDDisaggregated
		// regardless of the configured default deployment mode.
		isvc := v1beta1testing.MakeInferenceService("def-engine-decoder", "isvc-defaulter").
			Model("any-model").
			Engine(engineWithImage("ghcr.io/test/runtime:test")).
			Decoder(&v1beta1.DecoderSpec{
				PodSpec: v1beta1.PodSpec{
					Containers: []corev1.Container{{Name: "ome-container", Image: "ghcr.io/test/decoder:test"}},
				},
			}).Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, isvc) })

		got := &v1beta1.InferenceService{}
		Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "def-engine-decoder", Namespace: "isvc-defaulter"}, got)).To(Succeed())
		Expect(got.Annotations).To(HaveKeyWithValue(constants.DeploymentMode, string(constants.PDDisaggregated)))
	})

	ginkgo.It("Engine.Leader + Worker.Size>0 → DeploymentMode=OMENative", func() {
		// Defaulter detects the multi-node signal: Leader+Worker
		// present AND Worker.Size > 0 → OMENative (native multi-node
		// serving). Without Worker.Size > 0 the branch falls through
		// to the default-deployment-mode path (covered by the next
		// spec).
		isvc := v1beta1testing.MakeInferenceService("def-multinode", "isvc-defaulter").
			Model("any-model").
			Engine(&v1beta1.EngineSpec{
				PodSpec: v1beta1.PodSpec{
					Containers: []corev1.Container{{Name: "ome-container", Image: "ghcr.io/test/runtime:test"}},
				},
				Leader: &v1beta1.LeaderSpec{},
				Worker: &v1beta1.WorkerSpec{
					Size: ptr.To(2),
				},
			}).Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, isvc) })

		got := &v1beta1.InferenceService{}
		Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "def-multinode", Namespace: "isvc-defaulter"}, got)).To(Succeed())
		Expect(got.Annotations).To(HaveKeyWithValue(constants.DeploymentMode, string(constants.OMENative)))
	})

	ginkgo.It("Engine.Leader + Worker.Size=0 → falls through to default mode (NOT OMENative)", func() {
		// Worker.Size=0 disables the multi-node branch — the
		// defaulter must treat it as if the multi-node signal isn't
		// present and apply the configured default. Catches a
		// regression where the gate checks Worker.Size != nil
		// instead of *Worker.Size > 0.
		isvc := v1beta1testing.MakeInferenceService("def-multinode-zero", "isvc-defaulter").
			Model("any-model").
			Engine(&v1beta1.EngineSpec{
				PodSpec: v1beta1.PodSpec{
					Containers: []corev1.Container{{Name: "ome-container", Image: "ghcr.io/test/runtime:test"}},
				},
				Leader: &v1beta1.LeaderSpec{},
				Worker: &v1beta1.WorkerSpec{
					Size: ptr.To(0),
				},
			}).Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, isvc) })

		got := &v1beta1.InferenceService{}
		Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "def-multinode-zero", Namespace: "isvc-defaulter"}, got)).To(Succeed())
		// Falls through to the default deployment mode rather than
		// being treated as multi-node.
		Expect(got.Annotations).To(HaveKeyWithValue(constants.DeploymentMode, string(constants.RawDeployment)))
	})

	ginkgo.It("OMENative + omitted rollingUpdate → MaxSurge and MaxUnavailable default to 25%", func() {
		// Rollout-budget lock: an OMENative Component that omits
		// lifecycle.updateStrategy.rollingUpdate must not resolve to the
		// uncapped BudgetNoLimit — that would let a single spec bump
		// drain every Instance at once. The defaulter fills
		// both budgets with 25%; assert the read-back after real admission.
		//
		// The lifecycle defaulter resolves OMENative from the per-Component
		// annotation (escape hatch) or spec.deploymentMode, NOT the ObjectMeta
		// mode annotation the multi-node heuristic writes — so set the
		// per-Component annotation to trigger the OMENative branch.
		isvc := v1beta1testing.MakeInferenceService("def-rollout-budgets", "isvc-defaulter").
			Model("any-model").
			Engine(&v1beta1.EngineSpec{
				PodSpec: v1beta1.PodSpec{
					Containers: []corev1.Container{{Name: "ome-container", Image: "ghcr.io/test/runtime:test"}},
				},
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
					Annotations: map[string]string{constants.DeploymentMode: string(constants.OMENative)},
				},
			}).Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, isvc) })

		got := &v1beta1.InferenceService{}
		Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "def-rollout-budgets", Namespace: "isvc-defaulter"}, got)).To(Succeed())
		Expect(got.Spec.Engine).NotTo(BeNil())
		Expect(got.Spec.Engine.Lifecycle).NotTo(BeNil())
		Expect(got.Spec.Engine.Lifecycle.UpdateStrategy).NotTo(BeNil())
		ru := got.Spec.Engine.Lifecycle.UpdateStrategy.RollingUpdate
		Expect(ru).NotTo(BeNil(), "omitted rollingUpdate must be defaulted, not left nil (would resolve to uncapped BudgetNoLimit)")
		want := intstr.FromString("25%")
		Expect(ru.MaxSurge).NotTo(BeNil())
		Expect(*ru.MaxSurge).To(Equal(want), "MaxSurge must default to 25%")
		Expect(ru.MaxUnavailable).NotTo(BeNil())
		Expect(*ru.MaxUnavailable).To(Equal(want), "MaxUnavailable must default to 25%")
	})

	ginkgo.It("engine minReplicas=5 with maxReplicas omitted → admitted with maxReplicas clamped to 5", func() {
		// Replica-clamp lock: filling an omitted maxReplicas below an
		// explicit minReplicas would manufacture a min>max conflict that
		// the validating webhook rejects on create and on every later
		// non-delete update. The defaulter must raise the filled max to
		// the authored min.
		engine := engineWithImage("ghcr.io/test/runtime:test")
		engine.MinReplicas = ptr.To(5)
		isvc := v1beta1testing.MakeInferenceService("def-clamp-min5", "isvc-defaulter").
			Model("any-model").
			Engine(engine).Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, isvc) })

		got := &v1beta1.InferenceService{}
		Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "def-clamp-min5", Namespace: "isvc-defaulter"}, got)).To(Succeed())
		Expect(got.Spec.Engine.MinReplicas).To(HaveValue(Equal(5)))
		Expect(got.Spec.Engine.MaxReplicas).To(Equal(5), "defaulted maxReplicas must be clamped up to minReplicas")
	})

	ginkgo.It("engine minReplicas=2 with maxReplicas omitted → stored maxReplicas is the configured default 3", func() {
		// The clamp only raises the filled max to min; a min at or below
		// the configured component default must still receive that default
		// (deploy.replicas in the seeded inferenceservice-config).
		engine := engineWithImage("ghcr.io/test/runtime:test")
		engine.MinReplicas = ptr.To(2)
		isvc := v1beta1testing.MakeInferenceService("def-clamp-min2", "isvc-defaulter").
			Model("any-model").
			Engine(engine).Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, isvc) })

		got := &v1beta1.InferenceService{}
		Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "def-clamp-min2", Namespace: "isvc-defaulter"}, got)).To(Succeed())
		Expect(got.Spec.Engine.MinReplicas).To(HaveValue(Equal(2)))
		Expect(got.Spec.Engine.MaxReplicas).To(Equal(3))
	})

	ginkgo.It("router minReplicas=5 with maxReplicas omitted → admitted with maxReplicas clamped to 5", func() {
		// Router has its own (lower) configured max default, so it needs
		// its own clamp lock: an authored router floor above that default
		// must not be capped below itself.
		isvc := v1beta1testing.MakeInferenceService("def-clamp-router", "isvc-defaulter").
			Model("any-model").
			Engine(engineWithImage("ghcr.io/test/runtime:test")).
			Router(&v1beta1.RouterSpec{
				PodSpec: v1beta1.PodSpec{
					Containers: []corev1.Container{{Name: "ome-container", Image: "ghcr.io/test/router:test"}},
				},
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
					MinReplicas: ptr.To(5),
				},
			}).Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, isvc) })

		got := &v1beta1.InferenceService{}
		Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "def-clamp-router", Namespace: "isvc-defaulter"}, got)).To(Succeed())
		Expect(got.Spec.Router.MinReplicas).To(HaveValue(Equal(5)))
		Expect(got.Spec.Router.MaxReplicas).To(Equal(5), "defaulted router maxReplicas must be clamped up to minReplicas")
	})

	ginkgo.It("update that re-defaults an unset maxReplicas clamps to the new minReplicas", func() {
		// Updates run through the defaulter again. Raising minReplicas
		// while clearing maxReplicas must re-default the max at or above
		// the new floor, keeping the stored object admissible.
		isvc := v1beta1testing.MakeInferenceService("def-clamp-update", "isvc-defaulter").
			Model("any-model").
			Engine(engineWithImage("ghcr.io/test/runtime:test")).Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, isvc) })

		got := &v1beta1.InferenceService{}
		Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "def-clamp-update", Namespace: "isvc-defaulter"}, got)).To(Succeed())
		Expect(got.Spec.Engine.MaxReplicas).To(Equal(3))

		got.Spec.Engine.MinReplicas = ptr.To(5)
		got.Spec.Engine.MaxReplicas = 0
		Expect(k8sClient.Update(suiteCtx, got)).To(Succeed())

		after := &v1beta1.InferenceService{}
		Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "def-clamp-update", Namespace: "isvc-defaulter"}, after)).To(Succeed())
		Expect(after.Spec.Engine.MinReplicas).To(HaveValue(Equal(5)))
		Expect(after.Spec.Engine.MaxReplicas).To(Equal(5), "re-defaulted maxReplicas must clamp to the updated minReplicas")
	})

	ginkgo.It("no deploy.replicas config → replica bounds are stored exactly as authored", func() {
		// Config-driven degrade lock: the replica defaults live only in
		// the inferenceservice-config ConfigMap. With the deploy.replicas
		// block absent the defaulter stamps nothing — no literals baked
		// into the binary — and the spec round-trips as authored.
		cm := &corev1.ConfigMap{}
		Expect(k8sClient.Get(suiteCtx, types.NamespacedName{
			Namespace: constants.OMENamespace, Name: constants.InferenceServiceConfigMapName,
		}, cm)).To(Succeed())
		originalDeploy := cm.Data["deploy"]
		cm.Data["deploy"] = `{"defaultDeploymentMode": "RawDeployment"}`
		Expect(k8sClient.Update(suiteCtx, cm)).To(Succeed())
		ginkgo.DeferCleanup(func() {
			restore := &corev1.ConfigMap{}
			Expect(k8sClient.Get(suiteCtx, types.NamespacedName{
				Namespace: constants.OMENamespace, Name: constants.InferenceServiceConfigMapName,
			}, restore)).To(Succeed())
			restore.Data["deploy"] = originalDeploy
			Expect(k8sClient.Update(suiteCtx, restore)).To(Succeed())
		})

		isvc := v1beta1testing.MakeInferenceService("def-no-replica-config", "isvc-defaulter").
			Model("any-model").
			Engine(engineWithImage("ghcr.io/test/runtime:test")).Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, isvc) })

		got := &v1beta1.InferenceService{}
		Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "def-no-replica-config", Namespace: "isvc-defaulter"}, got)).To(Succeed())
		Expect(got.Spec.Engine.MinReplicas).To(BeNil(), "unconfigured minReplicas default must not be stamped")
		Expect(got.Spec.Engine.MaxReplicas).To(BeZero(), "unconfigured maxReplicas default must not be stamped")
	})

	ginkgo.It("explicit maxReplicas below minReplicas passes through the defaulter untouched", func() {
		// The clamp applies only to a max the defaulter itself fills. An
		// authored min>max pair must reach the validating webhook as
		// written so it can be rejected (pinned in the validator suite);
		// this suite installs only the mutating webhook, so we assert
		// pass-through on the stored object.
		engine := engineWithImage("ghcr.io/test/runtime:test")
		engine.MinReplicas = ptr.To(5)
		engine.MaxReplicas = 2
		isvc := v1beta1testing.MakeInferenceService("def-clamp-explicit", "isvc-defaulter").
			Model("any-model").
			Engine(engine).Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, isvc) })

		got := &v1beta1.InferenceService{}
		Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "def-clamp-explicit", Namespace: "isvc-defaulter"}, got)).To(Succeed())
		Expect(got.Spec.Engine.MinReplicas).To(HaveValue(Equal(5)))
		Expect(got.Spec.Engine.MaxReplicas).To(Equal(2), "explicit maxReplicas must never be adjusted by the defaulter")
	})

	ginkgo.It("explicit DeploymentMode annotation is preserved (defaulter does not overwrite)", func() {
		// User opt-in to a specific mode must win over the
		// defaulter's heuristic. The defaulter checks for the
		// annotation's existence FIRST (modeExists at line 73)
		// and skips the rest of the dispatch tree if set.
		isvc := v1beta1testing.MakeInferenceService("def-explicit", "isvc-defaulter").
			Model("any-model").
			Engine(engineWithImage("ghcr.io/test/runtime:test")).
			Decoder(&v1beta1.DecoderSpec{
				PodSpec: v1beta1.PodSpec{
					Containers: []corev1.Container{{Name: "ome-container", Image: "ghcr.io/test/decoder:test"}},
				},
			}).
			// Engine + Decoder would normally → PDDisaggregated;
			// the explicit annotation must override that.
			Annotation(constants.DeploymentMode, string(constants.RawDeployment)).Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, isvc) })

		got := &v1beta1.InferenceService{}
		Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "def-explicit", Namespace: "isvc-defaulter"}, got)).To(Succeed())
		Expect(got.Annotations).To(HaveKeyWithValue(constants.DeploymentMode, string(constants.RawDeployment)),
			"explicit DeploymentMode annotation must NOT be overwritten by Engine+Decoder heuristic")
	})
})
