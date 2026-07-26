package servingruntime

import (
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
)

// ServingRuntime webhook validation for the
// per-Component Autoscaler block. ServingRuntime can declare
// autoscaler defaults inherited by ISVCs,
// so the same shape rules apply at admission time on the runtime
// side too.
var _ = ginkgo.Describe("ClusterServingRuntime webhook Autoscaler validation", func() {

	ginkgo.It("rejects engineConfig.autoscaler unknown class", func() {
		csr := &v1beta1.ClusterServingRuntime{
			ObjectMeta: metav1.ObjectMeta{GenerateName: "bad-class-"},
			Spec: v1beta1.ServingRuntimeSpec{
				SupportedModelFormats: []v1beta1.SupportedModelFormat{{
					ModelFormat:    &v1beta1.ModelFormat{Name: "safetensors"},
					ModelFramework: &v1beta1.ModelFrameworkSpec{Name: "transformers"},
				}},
				EngineConfig: &v1beta1.EngineSpec{
					ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
						Autoscaler: &v1beta1.ComponentAutoscaler{
							Class: v1beta1.AutoscalerClass("knative"),
						},
					},
				},
			},
		}
		err := k8sClient.Create(suiteCtx, csr)
		gomega.Expect(err).To(gomega.HaveOccurred())
		gomega.Expect(err.Error()).To(gomega.SatisfyAny(
			gomega.ContainSubstring("is not one of HPA|KEDA|External|None"),
			gomega.ContainSubstring("Unsupported value"),
		))
	})

	ginkgo.It("admits engineConfig.autoscaler with class=hpa default", func() {
		csr := &v1beta1.ClusterServingRuntime{
			ObjectMeta: metav1.ObjectMeta{GenerateName: "ok-hpa-"},
			Spec: v1beta1.ServingRuntimeSpec{
				SupportedModelFormats: []v1beta1.SupportedModelFormat{{
					ModelFormat:    &v1beta1.ModelFormat{Name: "safetensors"},
					ModelFramework: &v1beta1.ModelFrameworkSpec{Name: "transformers"},
				}},
				EngineConfig: &v1beta1.EngineSpec{
					ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
						Autoscaler: &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerHPA},
					},
				},
			},
		}
		gomega.Expect(k8sClient.Create(suiteCtx, csr)).To(gomega.Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, csr) })
	})
})
