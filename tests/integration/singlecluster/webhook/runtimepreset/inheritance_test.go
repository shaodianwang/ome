package runtimepreset

import (
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/runtimeinheritance"
	"github.com/ome-projects/ome/tests/integration/util"
)

// mkProfileCSR builds an infra profile (disabled, marked
// runtime-profile=true). Engine/decoder/router gaps left for the child
// to fill via the preset.
func mkProfileCSR(name string, spec v1beta1.ServingRuntimeSpec) *v1beta1.ClusterServingRuntime {
	out := spec.DeepCopy()
	out.Disabled = ptr.To(true)
	return &v1beta1.ClusterServingRuntime{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Annotations: map[string]string{constants.RuntimeProfileAnnotationKey: "true"},
		},
		Spec: *out,
	}
}

// infraOnlyProfileSpec returns a spec with only infra fields populated
// at the spec-root (nodeSelector, tolerations, annotations). The
// child's engine preset will provide engine/decoder/router.
func infraOnlyProfileSpec() v1beta1.ServingRuntimeSpec {
	return v1beta1.ServingRuntimeSpec{
		SupportedModelFormats: []v1beta1.SupportedModelFormat{{
			ModelFormat:    &v1beta1.ModelFormat{Name: "safetensors"},
			ModelFramework: &v1beta1.ModelFrameworkSpec{Name: "transformers"},
		}},
		ServingRuntimePodSpec: v1beta1.ServingRuntimePodSpec{
			NodeSelector: map[string]string{"kubernetes.io/arch": "arm64"},
			Tolerations: []corev1.Toleration{{
				Key:      "nvidia.com/gpu",
				Operator: corev1.TolerationOpExists,
				Effect:   corev1.TaintEffectNoSchedule,
			}},
			Annotations: map[string]string{
				"prometheus.io/scrape": "true",
				"prometheus.io/port":   "30000",
			},
		},
	}
}

var _ = ginkgo.Describe("Runtime preset + inheritance composition", ginkgo.Ordered, func() {
	var ns string

	ginkgo.BeforeAll(func() {
		nsObj, err := util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "rt-preset-inh-")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ns = nsObj.Name
		ginkgo.DeferCleanup(func() { _ = util.DeleteNamespace(suiteCtx, k8sClient, ns) })
	})

	ginkgo.It("CSR child with preset + CSR profile parent: resolver yields engine (preset) + infra (profile)", func() {
		profile := mkProfileCSR("infra-edge-arm64", infraOnlyProfileSpec())
		child := mkCSR("rt-csr-pd", map[string]string{
			constants.RuntimeEngineAnnotationKey:      "sglang-pd",
			constants.RuntimeInheritFromAnnotationKey: profile.Name,
		}, minimalSRSpec())
		gomega.Expect(k8sClient.Create(suiteCtx, profile)).To(gomega.Succeed())
		gomega.Expect(k8sClient.Create(suiteCtx, child)).To(gomega.Succeed())
		ginkgo.DeferCleanup(func() {
			deleteIfExists(child)
			deleteIfExists(profile)
		})

		merged, chain, err := runtimeinheritance.ResolveClusterRuntime(suiteCtx, k8sClient, child.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(chain).To(gomega.Equal([]string{"infra-edge-arm64", "rt-csr-pd"}))

		// From the preset (filled on child at admission, kept on resolve).
		gomega.Expect(merged.EngineConfig).NotTo(gomega.BeNil())
		gomega.Expect(merged.EngineConfig.Runner.Command).To(gomega.ContainElement("prefill"))
		gomega.Expect(merged.DecoderConfig).NotTo(gomega.BeNil())
		gomega.Expect(merged.RouterConfig).NotTo(gomega.BeNil())

		// From the profile (filled at resolve time).
		gomega.Expect(merged.NodeSelector).To(gomega.HaveKeyWithValue("kubernetes.io/arch", "arm64"))
		gomega.Expect(merged.Tolerations).NotTo(gomega.BeEmpty())
		gomega.Expect(merged.Annotations).To(gomega.HaveKeyWithValue("prometheus.io/scrape", "true"))
	})

	ginkgo.It("namespaced SR child with preset + CSR profile parent (mixed scope): resolver composes", func() {
		profile := mkProfileCSR("infra-mixed", infraOnlyProfileSpec())
		child := mkSR(ns, "rt-mixed", map[string]string{
			constants.RuntimeEngineAnnotationKey:      "sglang-pd",
			constants.RuntimeInheritFromAnnotationKey: profile.Name,
		}, minimalSRSpec())
		gomega.Expect(k8sClient.Create(suiteCtx, profile)).To(gomega.Succeed())
		gomega.Expect(k8sClient.Create(suiteCtx, child)).To(gomega.Succeed())
		ginkgo.DeferCleanup(func() {
			deleteIfExists(child)
			deleteIfExists(profile)
		})

		merged, chain, err := runtimeinheritance.ResolveNamespacedRuntime(suiteCtx, k8sClient, ns, child.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(chain).To(gomega.Equal([]string{"infra-mixed", "rt-mixed"}))

		gomega.Expect(merged.EngineConfig).NotTo(gomega.BeNil())
		gomega.Expect(merged.NodeSelector).To(gomega.HaveKey("kubernetes.io/arch"))
	})

	ginkgo.It("collision: child's preset-injected engine image wins over profile's engine image", func() {
		// Profile sets a custom engine image. Profiles are meant to
		// be infra-only — but if one accidentally sets engine fields,
		// the child's (preset-injected) value wins because child > parent
		// in fill-only-nil merge.
		profileSpec := infraOnlyProfileSpec()
		profileSpec.EngineConfig = &v1beta1.EngineSpec{
			Runner: &v1beta1.RunnerSpec{Container: corev1.Container{
				Name:  "ome-container",
				Image: "profile.example.com/sglang:profile-image",
			}},
		}
		profile := mkProfileCSR("infra-with-engine", profileSpec)
		child := mkCSR("rt-collision", map[string]string{
			constants.RuntimeEngineAnnotationKey:      "sglang-pd",
			constants.RuntimeInheritFromAnnotationKey: profile.Name,
		}, minimalSRSpec())
		gomega.Expect(k8sClient.Create(suiteCtx, profile)).To(gomega.Succeed())
		gomega.Expect(k8sClient.Create(suiteCtx, child)).To(gomega.Succeed())
		ginkgo.DeferCleanup(func() {
			deleteIfExists(child)
			deleteIfExists(profile)
		})

		merged, _, err := runtimeinheritance.ResolveClusterRuntime(suiteCtx, k8sClient, child.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(merged.EngineConfig.Runner.Image).To(gomega.ContainSubstring("lmsysorg/sglang"),
			"child's preset-injected image wins over the profile-set image")
	})

	ginkgo.It("collision: user-set engine image on child wins over BOTH preset and profile", func() {
		profileSpec := infraOnlyProfileSpec()
		profileSpec.EngineConfig = &v1beta1.EngineSpec{
			Runner: &v1beta1.RunnerSpec{Container: corev1.Container{
				Name:  "ome-container",
				Image: "profile.example.com/sglang:profile-image",
			}},
		}
		profile := mkProfileCSR("infra-with-engine-2", profileSpec)
		childSpec := minimalSRSpec()
		childSpec.EngineConfig = &v1beta1.EngineSpec{
			Runner: &v1beta1.RunnerSpec{Container: corev1.Container{
				Name:  "ome-container",
				Image: "user.example.com/sglang:user-pinned",
			}},
		}
		child := mkCSR("rt-user-pin", map[string]string{
			constants.RuntimeEngineAnnotationKey:      "sglang-pd",
			constants.RuntimeInheritFromAnnotationKey: profile.Name,
		}, childSpec)
		gomega.Expect(k8sClient.Create(suiteCtx, profile)).To(gomega.Succeed())
		gomega.Expect(k8sClient.Create(suiteCtx, child)).To(gomega.Succeed())
		ginkgo.DeferCleanup(func() {
			deleteIfExists(child)
			deleteIfExists(profile)
		})

		merged, _, err := runtimeinheritance.ResolveClusterRuntime(suiteCtx, k8sClient, child.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(merged.EngineConfig.Runner.Image).To(gomega.Equal("user.example.com/sglang:user-pinned"),
			"user-set field on child wins over preset and profile alike")
	})

	ginkgo.It("three-level: leaf with preset + mid + root profile resolves through full chain", func() {
		root := mkProfileCSR("inh-root", infraOnlyProfileSpec())
		// Mid is a non-profile pass-through that just declares the inherit
		// chain; could carry additional fields, but the test focuses on
		// chain traversal so we keep it empty.
		mid := &v1beta1.ClusterServingRuntime{
			ObjectMeta: metav1.ObjectMeta{
				Name: "inh-mid",
				Annotations: map[string]string{
					constants.RuntimeInheritFromAnnotationKey: root.Name,
				},
			},
			Spec: minimalSRSpec(),
		}
		leaf := mkCSR("inh-leaf", map[string]string{
			constants.RuntimeEngineAnnotationKey:      "sglang-pd",
			constants.RuntimeInheritFromAnnotationKey: mid.Name,
		}, minimalSRSpec())
		gomega.Expect(k8sClient.Create(suiteCtx, root)).To(gomega.Succeed())
		gomega.Expect(k8sClient.Create(suiteCtx, mid)).To(gomega.Succeed())
		gomega.Expect(k8sClient.Create(suiteCtx, leaf)).To(gomega.Succeed())
		ginkgo.DeferCleanup(func() {
			deleteIfExists(leaf)
			deleteIfExists(mid)
			deleteIfExists(root)
		})

		merged, chain, err := runtimeinheritance.ResolveClusterRuntime(suiteCtx, k8sClient, leaf.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(chain).To(gomega.Equal([]string{"inh-root", "inh-mid", "inh-leaf"}))
		// Preset (filled on leaf at admission) survives the chain walk.
		gomega.Expect(merged.EngineConfig).NotTo(gomega.BeNil())
		gomega.Expect(merged.EngineConfig.Runner.Command).To(gomega.ContainElement("prefill"))
		// Root's infra reaches the leaf.
		gomega.Expect(merged.NodeSelector).To(gomega.HaveKey("kubernetes.io/arch"))
	})

	ginkgo.It("preset on the PROFILE itself: webhook fills it; child inherits the merged engine spec", func() {
		// A profile carrying the engine annotation is unusual (profiles
		// are meant to be infra-only) but valid — the webhook runs on
		// every SR/CSR independently. The child then inherits the
		// profile's engine bits via the inheritance resolver.
		profileSpec := infraOnlyProfileSpec()
		profile := &v1beta1.ClusterServingRuntime{
			ObjectMeta: metav1.ObjectMeta{
				Name: "engine-profile",
				Annotations: map[string]string{
					constants.RuntimeProfileAnnotationKey: "true",
					constants.RuntimeEngineAnnotationKey:  "sglang-pd",
				},
			},
			Spec: *profileSpec.DeepCopy(),
		}
		profile.Spec.Disabled = ptr.To(true)
		child := mkCSR("rt-inherits-engine-profile", map[string]string{
			constants.RuntimeInheritFromAnnotationKey: profile.Name,
		}, minimalSRSpec())
		gomega.Expect(k8sClient.Create(suiteCtx, profile)).To(gomega.Succeed())
		gomega.Expect(k8sClient.Create(suiteCtx, child)).To(gomega.Succeed())
		ginkgo.DeferCleanup(func() {
			deleteIfExists(child)
			deleteIfExists(profile)
		})

		// The profile itself was mutated to carry the preset.
		gotProfile := &v1beta1.ClusterServingRuntime{}
		gomega.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(profile), gotProfile)).To(gomega.Succeed())
		gomega.Expect(gotProfile.Spec.EngineConfig).NotTo(gomega.BeNil())

		// The child has no engine of its own; the merged spec gets engine
		// from the profile via the resolver.
		merged, _, err := runtimeinheritance.ResolveClusterRuntime(suiteCtx, k8sClient, child.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(merged.EngineConfig).NotTo(gomega.BeNil())
		gomega.Expect(merged.EngineConfig.Runner.Command).To(gomega.ContainElement("prefill"))
		gomega.Expect(merged.DecoderConfig).NotTo(gomega.BeNil())
		gomega.Expect(merged.RouterConfig).NotTo(gomega.BeNil())
	})

	ginkgo.It("unhappy: child uses unknown engine value → child create denied even though inherit chain is valid", func() {
		profile := mkProfileCSR("infra-good", infraOnlyProfileSpec())
		gomega.Expect(k8sClient.Create(suiteCtx, profile)).To(gomega.Succeed())
		ginkgo.DeferCleanup(func() { deleteIfExists(profile) })

		bad := mkCSR("rt-bad-engine", map[string]string{
			constants.RuntimeEngineAnnotationKey:      "vllm-pd",
			constants.RuntimeInheritFromAnnotationKey: profile.Name,
		}, minimalSRSpec())
		err := k8sClient.Create(suiteCtx, bad)
		gomega.Expect(err).To(gomega.HaveOccurred())
		gomega.Expect(err.Error()).To(gomega.ContainSubstring("vllm-pd"))
	})
})
