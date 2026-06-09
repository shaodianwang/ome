package inferenceservice_defaulter

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
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
		// present AND Worker.Size > 0, which resolves to OMENative
		// (native multi-node). Without Worker.Size > 0 the branch
		// falls through to the default-deployment-mode path (covered
		// by the next spec).
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

	ginkgo.It("Engine.Leader + Worker.Size=0 → falls through to default mode (NOT multi-node)", func() {
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
