package inferenceservice_defaulter

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	v1beta1testing "github.com/ome-projects/ome/pkg/utils/testing/v1beta1"
)

// The lifecycle.minReadySeconds admission default is config-driven: the
// defaulter stamps deploy.minReadySeconds from inferenceservice-config onto
// every OMENative component that leaves the field unset, and stamps
// nothing when the key is absent.
var _ = ginkgo.Describe("InferenceService defaulter: lifecycle.minReadySeconds", func() {
	ginkgo.BeforeEach(ensureNamespace)

	configKey := types.NamespacedName{Namespace: constants.OMENamespace, Name: constants.InferenceServiceConfigMapName}

	// setDeployConfig replaces the deploy key for one spec and restores the
	// seeded value afterwards.
	setDeployConfig := func(deploy string) {
		cm := &corev1.ConfigMap{}
		Expect(k8sClient.Get(suiteCtx, configKey, cm)).To(Succeed())
		original := cm.Data["deploy"]
		cm.Data["deploy"] = deploy
		Expect(k8sClient.Update(suiteCtx, cm)).To(Succeed())
		ginkgo.DeferCleanup(func() {
			restore := &corev1.ConfigMap{}
			Expect(k8sClient.Get(suiteCtx, configKey, restore)).To(Succeed())
			restore.Data["deploy"] = original
			Expect(k8sClient.Update(suiteCtx, restore)).To(Succeed())
		})
	}

	omeNativeISVC := func(name string, lifecycle *v1beta1.LifecycleSpec) *v1beta1.InferenceService {
		engine := engineWithImage("ghcr.io/test/runtime:test")
		engine.Lifecycle = lifecycle
		return v1beta1testing.MakeInferenceService(name, "isvc-defaulter").
			Model("any-model").
			DeploymentMode(constants.OMENative).
			Engine(engine).
			Obj()
	}

	ginkgo.It("stamps deploy.minReadySeconds onto an OMENative engine that leaves it unset", func() {
		setDeployConfig(`{"defaultDeploymentMode": "RawDeployment", "minReadySeconds": 25}`)

		isvc := omeNativeISVC("def-minready-stamped", nil)
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, isvc) })

		got := &v1beta1.InferenceService{}
		Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "def-minready-stamped", Namespace: "isvc-defaulter"}, got)).To(Succeed())
		Expect(got.Spec.Engine.Lifecycle).NotTo(BeNil())
		Expect(got.Spec.Engine.Lifecycle.MinReadySeconds).To(HaveValue(Equal(int32(25))))
	})

	ginkgo.It("keeps an authored minReadySeconds over the configured default", func() {
		setDeployConfig(`{"defaultDeploymentMode": "RawDeployment", "minReadySeconds": 25}`)

		isvc := omeNativeISVC("def-minready-authored", &v1beta1.LifecycleSpec{MinReadySeconds: ptr.To(int32(5))})
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, isvc) })

		got := &v1beta1.InferenceService{}
		Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "def-minready-authored", Namespace: "isvc-defaulter"}, got)).To(Succeed())
		Expect(got.Spec.Engine.Lifecycle.MinReadySeconds).To(HaveValue(Equal(int32(5))))
	})

	ginkgo.It("stamps nothing when deploy.minReadySeconds is absent", func() {
		// The seeded config carries no minReadySeconds: the binary has no
		// built-in default, so the field round-trips unset.
		isvc := omeNativeISVC("def-minready-unset", nil)
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, isvc) })

		got := &v1beta1.InferenceService{}
		Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "def-minready-unset", Namespace: "isvc-defaulter"}, got)).To(Succeed())
		Expect(got.Spec.Engine.Lifecycle).NotTo(BeNil(), "OMENative lifecycle defaults still apply")
		Expect(got.Spec.Engine.Lifecycle.MinReadySeconds).To(BeNil(), "unconfigured minReadySeconds must not be stamped")
	})
})
