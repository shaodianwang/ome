package pod

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
)

const injectNamespace = "pod-webh-inject"

// injectPod returns a minimal InferenceService-labeled pod in the
// injection-matrix namespace. The single container is named
// ome-container — the default injection target for every
// annotation-gated injector.
func injectPod(name string, annotations map[string]string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   injectNamespace,
			Labels:      map[string]string{constants.InferenceServicePodLabelKey: "pod-webh-isvc"},
			Annotations: annotations,
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{
				Name:  constants.MainContainerName,
				Image: "ghcr.io/test/runtime:test",
			}},
			TerminationGracePeriodSeconds: ptrInt64(0),
		},
	}
}

// Pod mutator injection matrix: each spec drives one annotation-gated
// injector end-to-end through the real apiserver, proving the JSON
// patch computed by the webhook survives admission (patch application,
// apiserver validation of injected volumes/mounts, defaulting).
var _ = ginkgo.Describe("Pod mutator injection matrix", ginkgo.Ordered, func() {

	ginkgo.BeforeAll(func() {
		_ = k8sClient.Create(suiteCtx, &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{Name: injectNamespace},
		})

		// The sidecar and adapter injectors validate their config blocks
		// at admission time; give the suite ConfigMap production-shaped
		// entries. Keys are additive — specs in other files gate on
		// annotations these pods don't set.
		cm := &corev1.ConfigMap{}
		Expect(k8sClient.Get(suiteCtx, types.NamespacedName{
			Name:      constants.InferenceServiceConfigMapName,
			Namespace: constants.OMENamespace,
		}, cm)).To(Succeed())
		cm.Data["servingSidecar"] = `{
  "image": "ghcr.io/test/serving-agent:test",
  "compartmentId": "compartment-1",
  "authType": "InstancePrincipal",
  "region": "region-1",
  "memoryRequest": "100Mi",
  "memoryLimit": "1Gi",
  "cpuRequest": "100m",
  "cpuLimit": "1"
}`
		cm.Data["fineTunedAdapter"] = `{
  "image": "ghcr.io/test/fine-tuned-adapter:test",
  "compartmentId": "compartment-1",
  "authType": "InstancePrincipal",
  "region": "region-1",
  "memoryRequest": "100Mi",
  "memoryLimit": "1Gi",
  "cpuRequest": "100m",
  "cpuLimit": "1"
}`
		Expect(k8sClient.Update(suiteCtx, cm)).To(Succeed())
	})

	createAndFetch := func(pod *corev1.Pod) *corev1.Pod {
		ginkgo.GinkgoHelper()
		Expect(k8sClient.Create(suiteCtx, pod)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, pod) })
		got := &corev1.Pod{}
		Expect(k8sClient.Get(suiteCtx, types.NamespacedName{
			Name: pod.Name, Namespace: injectNamespace,
		}, got)).To(Succeed())
		return got
	}

	ginkgo.It("applies the default RDMA profile: env, volumes and security context land on the admitted pod", func() {
		got := createAndFetch(injectPod("pod-webh-rdma", map[string]string{
			constants.RDMAAutoInjectAnnotationKey: "true",
		}))

		env := map[string]string{}
		for _, e := range got.Spec.Containers[0].Env {
			env[e.Name] = e.Value
		}
		Expect(env).To(HaveKeyWithValue("NCCL_IB_TC", "41"),
			"oci-roce profile env must be applied to ome-container")
		Expect(env).To(HaveKeyWithValue("NCCL_SOCKET_IFNAME", "eth0"))

		volumes := map[string]corev1.VolumeSource{}
		for _, v := range got.Spec.Volumes {
			volumes[v.Name] = v.VolumeSource
		}
		Expect(volumes).To(HaveKey("dshm"))
		Expect(volumes).To(HaveKey("devinf"))

		sc := got.Spec.Containers[0].SecurityContext
		Expect(sc).NotTo(BeNil())
		Expect(sc.Privileged).To(BeNil())
		Expect(sc.Capabilities.Add).To(ContainElement(corev1.Capability("IPC_LOCK")))
	})

	ginkgo.It("applies shm, probe and observability runtime profiles together", func() {
		got := createAndFetch(injectPod("pod-webh-profiles", map[string]string{
			constants.RuntimeShmProfileAnnotationKey:           "default",
			constants.RuntimeProbeProfileAnnotationKey:         "sglang-http",
			constants.RuntimeProbePortAnnotationKey:            "8081",
			constants.RuntimeObservabilityProfileAnnotationKey: "prometheus",
			constants.RuntimeObservabilityPortAnnotationKey:    "9090",
		}))

		// shm: memory-backed emptyDir mounted at /dev/shm.
		Expect(got.Spec.Volumes).To(ContainElement(SatisfyAll(
			HaveField("Name", "dshm"),
			HaveField("VolumeSource.EmptyDir.Medium", corev1.StorageMediumMemory),
		)))
		Expect(got.Spec.Containers[0].VolumeMounts).To(ContainElement(
			HaveField("MountPath", "/dev/shm")))

		// probes: all three injected with the annotated port.
		c := got.Spec.Containers[0]
		for _, probe := range []*corev1.Probe{c.ReadinessProbe, c.LivenessProbe, c.StartupProbe} {
			Expect(probe).NotTo(BeNil())
			Expect(probe.HTTPGet.Path).To(Equal("/health"))
			Expect(probe.HTTPGet.Port.IntValue()).To(Equal(8081))
		}

		// observability: prometheus scrape annotations with the
		// annotated port.
		Expect(got.Annotations).To(HaveKeyWithValue(constants.PrometheusScrapeAnnotationKey, "true"))
		Expect(got.Annotations).To(HaveKeyWithValue(constants.PrometheusPortAnnotationKey, "9090"))
		Expect(got.Annotations).To(HaveKeyWithValue(constants.PrometheusPathAnnotationKey, "/metrics"))
	})

	ginkgo.It("injects the serving sidecar with config-driven image and agent env", func() {
		pod := injectPod("pod-webh-sidecar", map[string]string{
			constants.ServingSidecarInjectionKey:   "true",
			constants.FineTunedWeightFTStrategyKey: "lora",
		})
		// The sidecar mounts the shared model emptyDir; the pod must
		// declare it for apiserver mount validation to pass.
		pod.Spec.Volumes = []corev1.Volume{{
			Name:         constants.ModelEmptyDirVolumeName,
			VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
		}}
		got := createAndFetch(pod)

		Expect(got.Spec.Containers).To(HaveLen(2))
		sidecar := got.Spec.Containers[1]
		Expect(sidecar.Name).To(Equal(constants.ServingSidecarContainerName))
		Expect(sidecar.Image).To(Equal("ghcr.io/test/serving-agent:test"))

		env := map[string]string{}
		for _, e := range sidecar.Env {
			env[e.Name] = e.Value
		}
		Expect(env).To(HaveKeyWithValue(constants.AgentAuthTypeEnvVarKey, "InstancePrincipal"))
		Expect(env).To(HaveKeyWithValue(constants.AgentCompartmentIDEnvVarKey, "compartment-1"))
		Expect(env).To(HaveKeyWithValue(constants.AgentUnzippedFineTunedWeightDirectory, "/opt/ml/lora"))
	})

	ginkgo.It("injects the fine-tuned adapter init container from a real FineTunedWeight", func() {
		uri := "oci://n/example-ns/b/example-bucket/o/adapters/pod-webh-adapter"
		ftw := &v1beta1.FineTunedWeight{
			ObjectMeta: metav1.ObjectMeta{Name: "pod-webh-weight"},
			Spec: v1beta1.FineTunedWeightSpec{
				BaseModelRef:    v1beta1.ObjectReference{Name: ptrString("pod-webh-base")},
				TrainingJobRef:  v1beta1.ObjectReference{Name: ptrString("pod-webh-training")},
				ModelType:       ptrString("Adapter"),
				HyperParameters: runtime.RawExtension{Raw: []byte(`{}`)},
				Storage:         &v1beta1.StorageSpec{StorageUri: &uri},
			},
		}
		Expect(k8sClient.Create(suiteCtx, ftw)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, ftw) })

		pod := injectPod("pod-webh-adapter", map[string]string{
			constants.FineTunedAdapterInjectionKey: "pod-webh-weight",
		})
		pod.Spec.Volumes = []corev1.Volume{{
			Name:         constants.ModelEmptyDirVolumeName,
			VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
		}}
		got := createAndFetch(pod)

		Expect(got.Spec.InitContainers).To(HaveLen(1))
		ic := got.Spec.InitContainers[0]
		Expect(ic.Name).To(Equal(constants.FineTunedAdapterContainerName))
		Expect(ic.Image).To(Equal("ghcr.io/test/fine-tuned-adapter:test"))

		env := map[string]string{}
		for _, e := range ic.Env {
			env[e.Name] = e.Value
		}
		Expect(env).To(HaveKeyWithValue(constants.AgentModelBucketNameEnvVarKey, "example-bucket"))
		Expect(env).To(HaveKeyWithValue(constants.AgentModelNamespaceEnvVarKey, "example-ns"))
		Expect(env).To(HaveKeyWithValue(constants.AgentModelObjectName, "adapters/pod-webh-adapter"))
	})

	ginkgo.It("rejects pod creation when an injector errors (failurePolicy=Fail surface)", func() {
		// Injector errors must fail the create loudly, never admit a
		// half-injected pod.
		cases := map[string]map[string]string{
			"pod-webh-bad-rdma": {
				constants.RDMAAutoInjectAnnotationKey: "true",
				constants.RDMAProfileAnnotationKey:    "no-such-profile",
			},
			"pod-webh-bad-shm": {
				constants.RuntimeShmProfileAnnotationKey: "no-such-profile",
			},
			"pod-webh-bad-port": {
				constants.RuntimeProbeProfileAnnotationKey: "sglang-http",
				constants.RuntimeProbePortAnnotationKey:    "not-a-number",
			},
		}
		expectSubstring := map[string]string{
			"pod-webh-bad-rdma": "unknown RDMA profile",
			"pod-webh-bad-shm":  "unknown shm profile",
			"pod-webh-bad-port": "invalid port",
		}
		for name, annos := range cases {
			err := k8sClient.Create(suiteCtx, injectPod(name, annos))
			Expect(err).To(HaveOccurred(), "pod %s must be denied", name)
			Expect(err.Error()).To(ContainSubstring(expectSubstring[name]))
			Expect(apierrors.IsNotFound(k8sClient.Get(suiteCtx, types.NamespacedName{
				Name: name, Namespace: injectNamespace,
			}, &corev1.Pod{}))).To(BeTrue(), "denied pod %s must not exist", name)
		}
	})

	ginkgo.It("leaves unlabeled pods untouched even when they carry injector annotations", func() {
		// The ISVC/TrainingJob label gate runs before any injector: a
		// non-OME pod with a broken injector annotation must still be
		// admitted unmodified.
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "pod-webh-unlabeled",
				Namespace: injectNamespace,
				Annotations: map[string]string{
					constants.RDMAAutoInjectAnnotationKey: "true",
					constants.RDMAProfileAnnotationKey:    "no-such-profile",
				},
			},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{{
					Name:  constants.MainContainerName,
					Image: "ghcr.io/test/runtime:test",
				}},
				TerminationGracePeriodSeconds: ptrInt64(0),
			},
		}
		Expect(k8sClient.Create(suiteCtx, pod)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, pod) })

		got := &corev1.Pod{}
		Expect(k8sClient.Get(suiteCtx, types.NamespacedName{
			Name: pod.Name, Namespace: injectNamespace,
		}, got)).To(Succeed())
		Expect(got.Spec.Containers[0].Env).To(BeEmpty())
		Expect(got.Spec.Volumes).To(BeEmpty())
	})
})

func ptrString(s string) *string { return &s }
