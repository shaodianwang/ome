package inferenceservice

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/utils/ptr"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	v1beta1testing "github.com/ome-projects/ome/pkg/utils/testing/v1beta1"
)

// Admission specs for lifecycle.minReadySeconds: the CRD schema and the
// validating webhook both bound it at >= 0.
var _ = ginkgo.Describe("InferenceService lifecycle.minReadySeconds admission", func() {
	ginkgo.BeforeEach(func() {
		ensureNamespace()
	})

	minReadyISVC := func(name string, seconds int32) *v1beta1.InferenceService {
		return v1beta1testing.MakeInferenceService(name, "isvc-webhook").
			Runtime("mrs-runtime").
			AutoSync(false).
			DeploymentMode(constants.OMENative).
			Engine(&v1beta1.EngineSpec{
				PodSpec: v1beta1.PodSpec{Containers: []corev1.Container{{Name: "ome-container", Image: "ghcr.io/test/runtime:v1"}}},
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
					Lifecycle: &v1beta1.LifecycleSpec{MinReadySeconds: ptr.To(seconds)},
				},
			}).
			Obj()
	}

	ginkgo.It("rejects a negative engine minReadySeconds", func() {
		err := k8sClient.Create(suiteCtx, minReadyISVC("mrs-negative", -1))
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("minReadySeconds"))
	})

	ginkgo.It("accepts zero and positive minReadySeconds and stores them as authored", func() {
		for name, seconds := range map[string]int32{"mrs-zero": 0, "mrs-thirty": 30} {
			isvc := minReadyISVC(name, seconds)
			Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())
			ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, isvc) })
			Expect(isvc.Spec.Engine.Lifecycle.MinReadySeconds).To(HaveValue(Equal(seconds)))
		}
	})
})
