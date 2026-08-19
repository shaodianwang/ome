package inferenceservice

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	v1beta1testing "github.com/ome-projects/ome/pkg/utils/testing/v1beta1"
)

// End-to-end admission coverage for the per-Component replica-bound and
// disruption-budget validators: each spec drives a Create or Update
// through the real apiserver → webhook → validator chain and asserts
// the admission decision plus the exact spec field path in the error.
//
// The replica-bound and budget checks run before model resolution, so
// rejection specs reference a model that never exists; acceptance specs
// provide a real ClusterBaseModel + ClusterServingRuntime pair.
var _ = ginkgo.Describe("InferenceService webhook replica-bound and disruption-budget validation", func() {

	ensureBoundsNamespace := func() {
		ginkgo.GinkgoHelper()
		_ = k8sClient.Create(suiteCtx, &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{Name: "raw-depl-bounds"},
		})
	}

	ginkgo.BeforeEach(ensureBoundsNamespace)

	engineWithBounds := func(mutate func(*v1beta1.ComponentExtensionSpec)) *v1beta1.EngineSpec {
		engine := &v1beta1.EngineSpec{
			PodSpec: v1beta1.PodSpec{
				Containers: []corev1.Container{{Name: constants.MainContainerName, Image: "example.com/engine:v1"}},
			},
		}
		mutate(&engine.ComponentExtensionSpec)
		return engine
	}

	ginkgo.It("rejects negative engine minReplicas with the exact field path", func() {
		minReplicas := -1
		isvc := v1beta1testing.MakeInferenceService("raw-depl-neg-min", "raw-depl-bounds").
			Model("raw-depl-absent-model").
			DeploymentMode(constants.RawDeployment).
			Engine(engineWithBounds(func(ext *v1beta1.ComponentExtensionSpec) {
				ext.MinReplicas = &minReplicas
			})).Obj()
		err := k8sClient.Create(suiteCtx, isvc)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("spec.engine.minReplicas must be >= 0"))
	})

	ginkgo.It("rejects explicit negative engine maxReplicas", func() {
		isvc := v1beta1testing.MakeInferenceService("raw-depl-neg-max", "raw-depl-bounds").
			Model("raw-depl-absent-model").
			DeploymentMode(constants.RawDeployment).
			Engine(engineWithBounds(func(ext *v1beta1.ComponentExtensionSpec) {
				ext.MaxReplicas = -2
			})).Obj()
		err := k8sClient.Create(suiteCtx, isvc)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("spec.engine.maxReplicas must be >= 1"))
	})

	ginkgo.It("rejects engine minReplicas greater than maxReplicas", func() {
		minReplicas := 5
		isvc := v1beta1testing.MakeInferenceService("raw-depl-min-gt-max", "raw-depl-bounds").
			Model("raw-depl-absent-model").
			DeploymentMode(constants.RawDeployment).
			Engine(engineWithBounds(func(ext *v1beta1.ComponentExtensionSpec) {
				ext.MinReplicas = &minReplicas
				ext.MaxReplicas = 2
			})).Obj()
		err := k8sClient.Create(suiteCtx, isvc)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("minReplicas (5) must be <= maxReplicas (2)"))
	})

	ginkgo.It("accepts minReplicas with maxReplicas omitted (unset max is not a bound violation)", func() {
		// MaxReplicas is a non-pointer int, so an omitted max arrives as 0.
		// Admission must treat that as "no upper bound declared", not as
		// max < min.
		cbm := v1beta1testing.MakeClusterBaseModel("raw-depl-bounds-cbm").
			StorageURI("oci://n/example/b/models/o/raw-depl-bounds").
			ModelFormat("safetensors").
			ModelArchitecture("LlamaForCausalLM").
			ModelFramework("Transformers").Obj()
		Expect(k8sClient.Create(suiteCtx, cbm)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, cbm) })

		csr := v1beta1testing.MakeClusterServingRuntime("raw-depl-bounds-csr").
			SupportsModelFormat("safetensors", true, 10).
			WithLastFormatArchitecture("LlamaForCausalLM").
			EngineRunner(corev1.Container{Name: constants.MainContainerName, Image: "example.com/engine:v1"}).Obj()
		Expect(k8sClient.Create(suiteCtx, csr)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, csr) })

		minReplicas := 3
		isvc := v1beta1testing.MakeInferenceService("raw-depl-min-only", "raw-depl-bounds").
			Model("raw-depl-bounds-cbm").
			Runtime("raw-depl-bounds-csr").
			DeploymentMode(constants.RawDeployment).
			Engine(engineWithBounds(func(ext *v1beta1.ComponentExtensionSpec) {
				ext.MinReplicas = &minReplicas
			})).Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, isvc) })
	})

	ginkgo.It("rejects an engine budget declaring both minAvailable and maxUnavailable", func() {
		minAvailable := intstr.FromInt(2)
		maxUnavailable := intstr.FromInt(1)
		isvc := v1beta1testing.MakeInferenceService("raw-depl-both-budget", "raw-depl-bounds").
			Model("raw-depl-absent-model").
			DeploymentMode(constants.RawDeployment).
			Engine(engineWithBounds(func(ext *v1beta1.ComponentExtensionSpec) {
				ext.MinAvailable = &minAvailable
				ext.MaxUnavailable = &maxUnavailable
			})).Obj()
		err := k8sClient.Create(suiteCtx, isvc)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("spec.engine.minAvailable and spec.engine.maxUnavailable cannot both be set"))
	})

	ginkgo.It("rejects an engine maxUnavailable percentage above 100%", func() {
		maxUnavailable := intstr.FromString("150%")
		isvc := v1beta1testing.MakeInferenceService("raw-depl-over-percent", "raw-depl-bounds").
			Model("raw-depl-absent-model").
			DeploymentMode(constants.RawDeployment).
			Engine(engineWithBounds(func(ext *v1beta1.ComponentExtensionSpec) {
				ext.MaxUnavailable = &maxUnavailable
			})).Obj()
		err := k8sClient.Create(suiteCtx, isvc)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("spec.engine.maxUnavailable must be a non-negative integer or a percentage from 0% to 100%"))
	})

	ginkgo.It("rejects a negative decoder minAvailable naming the decoder field path", func() {
		minAvailable := intstr.FromInt(-3)
		minReplicas := 1
		isvc := v1beta1testing.MakeInferenceService("raw-depl-decoder-budget", "raw-depl-bounds").
			Model("raw-depl-absent-model").
			DeploymentMode(constants.RawDeployment).
			Engine(engineWithBounds(func(ext *v1beta1.ComponentExtensionSpec) {
				ext.MinReplicas = &minReplicas
			})).
			Decoder(&v1beta1.DecoderSpec{
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
					MinAvailable: &minAvailable,
				},
				PodSpec: v1beta1.PodSpec{
					Containers: []corev1.Container{{Name: constants.MainContainerName, Image: "example.com/decoder:v1"}},
				},
			}).Obj()
		err := k8sClient.Create(suiteCtx, isvc)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("spec.decoder.minAvailable must be a non-negative integer or a percentage from 0% to 100%"))
	})

	ginkgo.It("accepts a valid engine budget and rejects an update that breaks the replica bounds", func() {
		cbm := v1beta1testing.MakeClusterBaseModel("raw-depl-update-cbm").
			StorageURI("oci://n/example/b/models/o/raw-depl-update").
			ModelFormat("safetensors").
			ModelArchitecture("LlamaForCausalLM").
			ModelFramework("Transformers").Obj()
		Expect(k8sClient.Create(suiteCtx, cbm)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, cbm) })

		csr := v1beta1testing.MakeClusterServingRuntime("raw-depl-update-csr").
			SupportsModelFormat("safetensors", true, 10).
			WithLastFormatArchitecture("LlamaForCausalLM").
			EngineRunner(corev1.Container{Name: constants.MainContainerName, Image: "example.com/engine:v1"}).Obj()
		Expect(k8sClient.Create(suiteCtx, csr)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, csr) })

		minReplicas := 1
		maxUnavailable := intstr.FromString("25%")
		isvc := v1beta1testing.MakeInferenceService("raw-depl-update-isvc", "raw-depl-bounds").
			Model("raw-depl-update-cbm").
			Runtime("raw-depl-update-csr").
			DeploymentMode(constants.RawDeployment).
			Engine(engineWithBounds(func(ext *v1beta1.ComponentExtensionSpec) {
				ext.MinReplicas = &minReplicas
				ext.MaxReplicas = 4
				ext.MaxUnavailable = &maxUnavailable
			})).Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, isvc) })

		fresh := &v1beta1.InferenceService{}
		Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(isvc), fresh)).To(Succeed())
		brokenMin := 7
		fresh.Spec.Engine.MinReplicas = &brokenMin
		err := k8sClient.Update(suiteCtx, fresh)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("minReplicas (7) must be <= maxReplicas (4)"))
	})
})
