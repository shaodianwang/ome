package runtimepreset

import (
	"strings"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/tests/integration/util"
)

const cleanupTimeout = 10 * time.Second

func deleteIfExists(obj client.Object) {
	_ = k8sClient.Delete(suiteCtx, obj)
	gomega.Eventually(func() bool {
		err := k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(obj), obj)
		return apierrors.IsNotFound(err)
	}, cleanupTimeout, 200*time.Millisecond).Should(gomega.BeTrue())
}

func minimalSRSpec() v1beta1.ServingRuntimeSpec {
	return v1beta1.ServingRuntimeSpec{
		SupportedModelFormats: []v1beta1.SupportedModelFormat{{
			ModelFormat:    &v1beta1.ModelFormat{Name: "safetensors"},
			ModelFramework: &v1beta1.ModelFrameworkSpec{Name: "transformers"},
		}},
	}
}

func mkSR(ns, name string, annotations map[string]string, spec v1beta1.ServingRuntimeSpec) *v1beta1.ServingRuntime {
	return &v1beta1.ServingRuntime{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns, Annotations: annotations},
		Spec:       spec,
	}
}

func mkCSR(name string, annotations map[string]string, spec v1beta1.ServingRuntimeSpec) *v1beta1.ClusterServingRuntime {
	return &v1beta1.ClusterServingRuntime{
		ObjectMeta: metav1.ObjectMeta{Name: name, Annotations: annotations},
		Spec:       spec,
	}
}

var _ = ginkgo.Describe("Runtime preset mutator (preset-only paths)", ginkgo.Ordered, func() {
	var ns string

	ginkgo.BeforeAll(func() {
		nsObj, err := util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "rt-preset-")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ns = nsObj.Name
		ginkgo.DeferCleanup(func() { _ = util.DeleteNamespace(suiteCtx, k8sClient, ns) })
	})

	ginkgo.It("happy: SR with no engine annotation is stored unchanged", func() {
		sr := mkSR(ns, "no-anno", nil, minimalSRSpec())
		gomega.Expect(k8sClient.Create(suiteCtx, sr)).To(gomega.Succeed())
		ginkgo.DeferCleanup(func() { deleteIfExists(sr) })

		got := &v1beta1.ServingRuntime{}
		gomega.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(sr), got)).To(gomega.Succeed())
		gomega.Expect(got.Spec.EngineConfig).To(gomega.BeNil())
		gomega.Expect(got.Spec.DecoderConfig).To(gomega.BeNil())
		gomega.Expect(got.Spec.RouterConfig).To(gomega.BeNil())
	})

	ginkgo.It("happy: SR with ome.io/engine=sglang-pd gets engine+decoder+router filled", func() {
		sr := mkSR(ns, "sg-vanilla", map[string]string{
			constants.RuntimeEngineAnnotationKey: "sglang-pd",
		}, minimalSRSpec())
		gomega.Expect(k8sClient.Create(suiteCtx, sr)).To(gomega.Succeed())
		ginkgo.DeferCleanup(func() { deleteIfExists(sr) })

		got := &v1beta1.ServingRuntime{}
		gomega.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(sr), got)).To(gomega.Succeed())

		gomega.Expect(got.Spec.EngineConfig).NotTo(gomega.BeNil())
		gomega.Expect(got.Spec.EngineConfig.Runner.Image).To(gomega.ContainSubstring("sglang"))
		gomega.Expect(got.Spec.EngineConfig.Runner.Command).To(gomega.ContainElement("prefill"))
		gomega.Expect(got.Spec.EngineConfig.Runner.Command).To(gomega.ContainElement("$(MODEL_PATH)"))

		gomega.Expect(got.Spec.DecoderConfig).NotTo(gomega.BeNil())
		gomega.Expect(got.Spec.DecoderConfig.Runner.Command).To(gomega.ContainElement("decode"))

		gomega.Expect(got.Spec.RouterConfig).NotTo(gomega.BeNil())
		gomega.Expect(got.Spec.RouterConfig.Runner.Image).To(gomega.ContainSubstring("router"))
	})

	ginkgo.It("happy: user-supplied engine image survives merge (runtime wins)", func() {
		sr := mkSR(ns, "sg-override", map[string]string{
			constants.RuntimeEngineAnnotationKey: "sglang-pd",
		}, v1beta1.ServingRuntimeSpec{
			SupportedModelFormats: minimalSRSpec().SupportedModelFormats,
			EngineConfig: &v1beta1.EngineSpec{
				Runner: &v1beta1.RunnerSpec{Container: corev1.Container{
					Name:  "ome-container",
					Image: "internal.example.com/sglang:fork",
				}},
			},
		})
		gomega.Expect(k8sClient.Create(suiteCtx, sr)).To(gomega.Succeed())
		ginkgo.DeferCleanup(func() { deleteIfExists(sr) })

		got := &v1beta1.ServingRuntime{}
		gomega.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(sr), got)).To(gomega.Succeed())

		gomega.Expect(got.Spec.EngineConfig.Runner.Image).To(gomega.Equal("internal.example.com/sglang:fork"))
		// Preset still filled command/env/ports + decoder + router.
		gomega.Expect(got.Spec.EngineConfig.Runner.Command).To(gomega.ContainElement("prefill"))
		gomega.Expect(got.Spec.DecoderConfig).NotTo(gomega.BeNil())
		gomega.Expect(got.Spec.RouterConfig).NotTo(gomega.BeNil())
	})

	ginkgo.It("happy: empty annotation value is treated as 'no annotation' (passthrough)", func() {
		sr := mkSR(ns, "sg-empty", map[string]string{
			constants.RuntimeEngineAnnotationKey: "",
		}, minimalSRSpec())
		gomega.Expect(k8sClient.Create(suiteCtx, sr)).To(gomega.Succeed())
		ginkgo.DeferCleanup(func() { deleteIfExists(sr) })

		got := &v1beta1.ServingRuntime{}
		gomega.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(sr), got)).To(gomega.Succeed())
		gomega.Expect(got.Spec.EngineConfig).To(gomega.BeNil())
	})

	ginkgo.It("unhappy: unknown engine name is rejected at admission", func() {
		sr := mkSR(ns, "sg-bad", map[string]string{
			constants.RuntimeEngineAnnotationKey: "vllm-pd",
		}, minimalSRSpec())
		err := k8sClient.Create(suiteCtx, sr)
		gomega.Expect(err).To(gomega.HaveOccurred())
		gomega.Expect(strings.ToLower(err.Error())).To(gomega.ContainSubstring("unknown"))
		gomega.Expect(err.Error()).To(gomega.ContainSubstring("sglang-pd"))
	})

	ginkgo.It("update F4: removing the annotation does NOT strip injected fields (sticky)", func() {
		sr := mkSR(ns, "sg-sticky", map[string]string{
			constants.RuntimeEngineAnnotationKey: "sglang-pd",
		}, minimalSRSpec())
		gomega.Expect(k8sClient.Create(suiteCtx, sr)).To(gomega.Succeed())
		ginkgo.DeferCleanup(func() { deleteIfExists(sr) })

		// Refetch to get the mutated state (image is filled).
		got := &v1beta1.ServingRuntime{}
		gomega.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(sr), got)).To(gomega.Succeed())
		gomega.Expect(got.Spec.EngineConfig).NotTo(gomega.BeNil())

		// Remove the annotation and update — webhook becomes a no-op.
		delete(got.Annotations, constants.RuntimeEngineAnnotationKey)
		gomega.Expect(k8sClient.Update(suiteCtx, got)).To(gomega.Succeed())

		after := &v1beta1.ServingRuntime{}
		gomega.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(sr), after)).To(gomega.Succeed())
		// Injected fields are persisted on the stored object; webhook
		// doesn't reach back to clean them up (admission-time only).
		gomega.Expect(after.Spec.EngineConfig).NotTo(gomega.BeNil())
		gomega.Expect(after.Spec.DecoderConfig).NotTo(gomega.BeNil())
		gomega.Expect(after.Spec.RouterConfig).NotTo(gomega.BeNil())
	})

	ginkgo.It("update F5: re-admission with annotation still set re-fills gaps", func() {
		sr := mkSR(ns, "sg-refill", map[string]string{
			constants.RuntimeEngineAnnotationKey: "sglang-pd",
		}, minimalSRSpec())
		gomega.Expect(k8sClient.Create(suiteCtx, sr)).To(gomega.Succeed())
		ginkgo.DeferCleanup(func() { deleteIfExists(sr) })

		got := &v1beta1.ServingRuntime{}
		gomega.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(sr), got)).To(gomega.Succeed())
		gomega.Expect(got.Spec.RouterConfig).NotTo(gomega.BeNil())

		// User nulls out router on an update. Preset re-injects it.
		got.Spec.RouterConfig = nil
		gomega.Expect(k8sClient.Update(suiteCtx, got)).To(gomega.Succeed())

		after := &v1beta1.ServingRuntime{}
		gomega.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(sr), after)).To(gomega.Succeed())
		gomega.Expect(after.Spec.RouterConfig).NotTo(gomega.BeNil(),
			"preset re-fills nil fields on each admission while annotation is set")
	})

	ginkgo.It("happy: ClusterServingRuntime path injects the same shape", func() {
		csr := mkCSR("csr-sg-vanilla", map[string]string{
			constants.RuntimeEngineAnnotationKey: "sglang-pd",
		}, minimalSRSpec())
		gomega.Expect(k8sClient.Create(suiteCtx, csr)).To(gomega.Succeed())
		ginkgo.DeferCleanup(func() { deleteIfExists(csr) })

		got := &v1beta1.ClusterServingRuntime{}
		gomega.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: csr.Name}, got)).To(gomega.Succeed())

		gomega.Expect(got.Spec.EngineConfig).NotTo(gomega.BeNil())
		gomega.Expect(got.Spec.DecoderConfig).NotTo(gomega.BeNil())
		gomega.Expect(got.Spec.RouterConfig).NotTo(gomega.BeNil())
	})

	ginkgo.It("unhappy: ClusterServingRuntime unknown engine is rejected", func() {
		csr := mkCSR("csr-sg-bad", map[string]string{
			constants.RuntimeEngineAnnotationKey: "tgi-pd",
		}, minimalSRSpec())
		err := k8sClient.Create(suiteCtx, csr)
		gomega.Expect(err).To(gomega.HaveOccurred())
		gomega.Expect(err.Error()).To(gomega.ContainSubstring("tgi-pd"))
	})
})
