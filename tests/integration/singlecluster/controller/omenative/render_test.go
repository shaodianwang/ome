package omenative

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apiresource "k8s.io/apimachinery/pkg/api/resource"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/tests/integration/util"
)

func apiResource(s string) apiresource.Quantity { return apiresource.MustParse(s) }

// Pod rendering invariants — pin OMENative.Render's contract end-
// to-end. Pure pod-shape assertions; fire as soon as a pod exists,
// no readiness convergence required.
var _ = ginkgo.Describe("OMENative pod rendering", ginkgo.Ordered, func() {
	var ns *corev1.Namespace

	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "omenative-render-")
		Expect(err).NotTo(HaveOccurred())
	})

	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	// Catches a hasServingReadinessGate dedup regression. Two copies of
	// ome.io/serving would keep PodReady=False forever (kubelet ANDs both
	// gates; the controller only writes one). Standard fixture can't
	// inject a user-supplied gate, so we assert "exactly ONE gate" on
	// rendered pods; the unit test covers the duplicate-input case.
	ginkgo.It("rendered pod carries exactly one ome.io/serving readiness gate", func() {
		fx := newOMENativeFixture(ns.Name,
			WithCBMName("gate-llama"),
			WithCSRName("gate-runtime"),
			WithISVCName("gate-isvc"),
		)
		Expect(k8sClient.Create(suiteCtx, fx.CBM)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CBM) })
		Expect(k8sClient.Create(suiteCtx, fx.CSR)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CSR) })
		Expect(k8sClient.Create(suiteCtx, fx.ISVC)).To(Succeed())

		Eventually(func(g Gomega) {
			pods := &corev1.PodList{}
			g.Expect(k8sClient.List(suiteCtx, pods,
				client.InNamespace(ns.Name),
				client.MatchingLabels{util.LabelManagedByKey: util.ManagedByOMENativeValue},
			)).To(Succeed())
			g.Expect(pods.Items).NotTo(BeEmpty())
			for i := range pods.Items {
				p := &pods.Items[i]
				count := 0
				for _, gate := range p.Spec.ReadinessGates {
					if gate.ConditionType == "ome.io/serving" {
						count++
					}
				}
				g.Expect(count).To(Equal(1),
					"pod %s must carry exactly one ome.io/serving gate; got %d (gates=%+v)",
					p.Name, count, p.Spec.ReadinessGates)
			}
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	// Spec scenario #71. Bug it catches: injectOMENativeEnv regression
	// — missing OME_* vars on a container, wrong values, or user-
	// supplied collisions winning over OMENative-controlled values.
	ginkgo.It("OME_* env vars are injected on every container with expected values", func() {
		fx := newOMENativeFixture(ns.Name,
			WithCBMName("env-llama"),
			WithCSRName("env-runtime"),
			WithISVCName("env-isvc"),
		)
		Expect(k8sClient.Create(suiteCtx, fx.CBM)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CBM) })
		Expect(k8sClient.Create(suiteCtx, fx.CSR)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CSR) })
		Expect(k8sClient.Create(suiteCtx, fx.ISVC)).To(Succeed())

		// Required vars from buildOMENativeEnv: ISVC name, component,
		// component replicas, instance index, runner, runner size,
		// runner index, instance subdomain. Single-pod has no
		// OME_LEADER_ADDRESS.
		required := []string{
			"OME_INFERENCESERVICE_NAME",
			"OME_COMPONENT",
			"OME_COMPONENT_REPLICAS",
			"OME_INSTANCE_INDEX",
			"OME_RUNNER",
			"OME_RUNNER_SIZE",
			"OME_RUNNER_INDEX",
			"OME_INSTANCE_SUBDOMAIN",
		}

		Eventually(func(g Gomega) {
			pods := &corev1.PodList{}
			g.Expect(k8sClient.List(suiteCtx, pods,
				client.InNamespace(ns.Name),
				client.MatchingLabels{util.LabelManagedByKey: util.ManagedByOMENativeValue},
			)).To(Succeed())
			g.Expect(pods.Items).NotTo(BeEmpty())

			for i := range pods.Items {
				p := &pods.Items[i]
				g.Expect(p.Spec.Containers).NotTo(BeEmpty(), "pod %s has no containers", p.Name)
				for _, c := range p.Spec.Containers {
					byName := map[string]string{}
					for _, e := range c.Env {
						byName[e.Name] = e.Value
					}
					for _, want := range required {
						g.Expect(byName).To(HaveKey(want),
							"container %s on pod %s missing OME_* var %s; env=%+v",
							c.Name, p.Name, want, c.Env)
					}
					// Value-spot-checks tied to the fixture defaults.
					g.Expect(byName["OME_INFERENCESERVICE_NAME"]).To(Equal("env-isvc"))
					g.Expect(byName["OME_COMPONENT"]).To(Equal("engine"))
					g.Expect(byName["OME_INSTANCE_INDEX"]).To(Equal("0"))
				}
			}
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	// Variant of #71: an extra container in the engine PodSpec must
	// also receive the OME_* vars. Catches a regression where the env
	// injector only walks the first container or skips user-added
	// sidecars.
	ginkgo.It("extra user-declared container also receives OME_* env vars", func() {
		extra := corev1.Container{
			Name:  "sidecar",
			Image: "test:sidecar",
		}
		fx := newOMENativeFixture(ns.Name,
			WithCBMName("env-extra-llama"),
			WithCSRName("env-extra-runtime"),
			WithISVCName("env-extra-isvc"),
			WithExtraContainer(extra),
		)
		Expect(k8sClient.Create(suiteCtx, fx.CBM)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CBM) })
		Expect(k8sClient.Create(suiteCtx, fx.CSR)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CSR) })
		Expect(k8sClient.Create(suiteCtx, fx.ISVC)).To(Succeed())

		Eventually(func(g Gomega) {
			pods := &corev1.PodList{}
			g.Expect(k8sClient.List(suiteCtx, pods,
				client.InNamespace(ns.Name),
				client.MatchingLabels{util.LabelManagedByKey: util.ManagedByOMENativeValue},
			)).To(Succeed())
			g.Expect(pods.Items).NotTo(BeEmpty())

			for i := range pods.Items {
				p := &pods.Items[i]
				// The sidecar container may have been swapped in by
				// runtime resolution; assert by walking every container.
				foundSidecar := false
				for _, c := range p.Spec.Containers {
					if c.Name != "sidecar" {
						continue
					}
					foundSidecar = true
					byName := map[string]string{}
					for _, e := range c.Env {
						byName[e.Name] = e.Value
					}
					g.Expect(byName).To(HaveKey("OME_COMPONENT"),
						"sidecar container missing OME_COMPONENT; env=%+v", c.Env)
				}
				g.Expect(foundSidecar).To(BeTrue(),
					"expected to find a 'sidecar' container on pod %s; got %+v", p.Name, containerNames(p.Spec.Containers))
			}
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	// Rendered pod must carry Hostname == pod.Name and Subdomain ==
	// the per-Component headless Service name. Required for the
	// per-pod DNS records that the headless Service publishes — without
	// them, the dataplane can't reach individual pods by stable name.
	ginkgo.It("rendered pod has Hostname=name and Subdomain=component-headless-service", func() {
		fx := newOMENativeFixture(ns.Name,
			WithCBMName("dns-llama"),
			WithCSRName("dns-runtime"),
			WithISVCName("dns-isvc"),
		)
		Expect(k8sClient.Create(suiteCtx, fx.CBM)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CBM) })
		Expect(k8sClient.Create(suiteCtx, fx.CSR)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CSR) })
		Expect(k8sClient.Create(suiteCtx, fx.ISVC)).To(Succeed())

		Eventually(func(g Gomega) {
			pod := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
				Namespace: ns.Name, Name: "dns-isvc-engine-0-default-0",
			}, pod)).To(Succeed())
			g.Expect(pod.Spec.Hostname).To(Equal("dns-isvc-engine-0-default-0"),
				"Hostname should equal pod name; got %q", pod.Spec.Hostname)
			g.Expect(pod.Spec.Subdomain).To(Equal("dns-isvc-engine-headless"),
				"Subdomain should equal the per-Component headless service; got %q", pod.Spec.Subdomain)
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	// User-declared volumes + volumeMounts must propagate to the
	// rendered pod. Volumes/mounts are not fields Render owns; a
	// regression that dropped them would silently break model
	// mounting, config sidecars, secret injection, etc.
	ginkgo.It("user-declared volumes + volumeMounts propagate to rendered pod", func() {
		fx := newOMENativeFixture(ns.Name,
			WithCBMName("vol-llama"),
			WithCSRName("vol-runtime"),
			WithISVCName("vol-isvc"),
		)
		fx.ISVC.Spec.Engine.PodSpec.Volumes = []corev1.Volume{{
			Name: "scratch",
			VolumeSource: corev1.VolumeSource{
				EmptyDir: &corev1.EmptyDirVolumeSource{},
			},
		}}
		fx.ISVC.Spec.Engine.PodSpec.Containers[0].VolumeMounts = []corev1.VolumeMount{{
			Name: "scratch", MountPath: "/var/scratch",
		}}
		Expect(k8sClient.Create(suiteCtx, fx.CBM)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CBM) })
		Expect(k8sClient.Create(suiteCtx, fx.CSR)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CSR) })
		Expect(k8sClient.Create(suiteCtx, fx.ISVC)).To(Succeed())

		Eventually(func(g Gomega) {
			pod := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
				Namespace: ns.Name, Name: "vol-isvc-engine-0-default-0",
			}, pod)).To(Succeed())
			gotVolume := false
			for _, v := range pod.Spec.Volumes {
				if v.Name == "scratch" && v.EmptyDir != nil {
					gotVolume = true
					break
				}
			}
			g.Expect(gotVolume).To(BeTrue(),
				"pod %s missing user-declared 'scratch' volume; volumes=%+v",
				pod.Name, pod.Spec.Volumes)

			gotMount := false
			for _, c := range pod.Spec.Containers {
				if c.Name != "ome-container" {
					continue
				}
				for _, m := range c.VolumeMounts {
					if m.Name == "scratch" && m.MountPath == "/var/scratch" {
						gotMount = true
						break
					}
				}
			}
			g.Expect(gotMount).To(BeTrue(),
				"engine container missing 'scratch' VolumeMount at /var/scratch")
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	// User-supplied env vars on the engine container must survive
	// Render's OME_* injection. Injection MUST add the OME_* set
	// without dropping user-declared vars; user names that collide
	// with OME_* are documented as overridden (Render wins) — the
	// non-colliding ones should pass through unchanged.
	ginkgo.It("user-supplied env vars survive Render's OME_* injection", func() {
		fx := newOMENativeFixture(ns.Name,
			WithCBMName("env-user-llama"),
			WithCSRName("env-user-runtime"),
			WithISVCName("env-user-isvc"),
		)
		fx.ISVC.Spec.Engine.PodSpec.Containers[0].Env = []corev1.EnvVar{
			{Name: "MY_FLAG", Value: "true"},
			{Name: "LOG_LEVEL", Value: "debug"},
		}
		Expect(k8sClient.Create(suiteCtx, fx.CBM)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CBM) })
		Expect(k8sClient.Create(suiteCtx, fx.CSR)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CSR) })
		Expect(k8sClient.Create(suiteCtx, fx.ISVC)).To(Succeed())

		Eventually(func(g Gomega) {
			pod := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
				Namespace: ns.Name, Name: "env-user-isvc-engine-0-default-0",
			}, pod)).To(Succeed())
			var engineC *corev1.Container
			for i := range pod.Spec.Containers {
				if pod.Spec.Containers[i].Name == "ome-container" {
					engineC = &pod.Spec.Containers[i]
					break
				}
			}
			g.Expect(engineC).NotTo(BeNil())
			byName := map[string]string{}
			for _, e := range engineC.Env {
				byName[e.Name] = e.Value
			}
			g.Expect(byName).To(HaveKeyWithValue("MY_FLAG", "true"),
				"user-supplied env MY_FLAG must survive Render; got %+v", engineC.Env)
			g.Expect(byName).To(HaveKeyWithValue("LOG_LEVEL", "debug"),
				"user-supplied env LOG_LEVEL must survive Render; got %+v", engineC.Env)
			g.Expect(byName).To(HaveKey("OME_COMPONENT"),
				"OME_COMPONENT must still be injected alongside user env")
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	// Resource requests/limits set on the engine container must
	// propagate to the rendered pod. Render is supposed to be a
	// passthrough on container fields other than the ones it controls
	// (image, env, gates, hostname/subdomain). A regression that drops
	// resources would silently downsize every workload.
	ginkgo.It("engine container resource requests propagate to rendered pod", func() {
		fx := newOMENativeFixture(ns.Name,
			WithCBMName("res-llama"),
			WithCSRName("res-runtime"),
			WithISVCName("res-isvc"),
		)
		// Mutate the engine container before applying.
		fx.ISVC.Spec.Engine.PodSpec.Containers[0].Resources = corev1.ResourceRequirements{
			Requests: corev1.ResourceList{
				corev1.ResourceCPU:    apiResource("100m"),
				corev1.ResourceMemory: apiResource("256Mi"),
			},
			Limits: corev1.ResourceList{
				corev1.ResourceCPU:    apiResource("1"),
				corev1.ResourceMemory: apiResource("2Gi"),
			},
		}
		Expect(k8sClient.Create(suiteCtx, fx.CBM)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CBM) })
		Expect(k8sClient.Create(suiteCtx, fx.CSR)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CSR) })
		Expect(k8sClient.Create(suiteCtx, fx.ISVC)).To(Succeed())

		Eventually(func(g Gomega) {
			pod := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
				Namespace: ns.Name, Name: "res-isvc-engine-0-default-0",
			}, pod)).To(Succeed())
			var engineC *corev1.Container
			for i := range pod.Spec.Containers {
				if pod.Spec.Containers[i].Name == "ome-container" {
					engineC = &pod.Spec.Containers[i]
					break
				}
			}
			g.Expect(engineC).NotTo(BeNil(), "expected an 'ome-container' on pod %s", pod.Name)
			g.Expect(engineC.Resources.Requests).To(HaveKeyWithValue(corev1.ResourceCPU, apiResource("100m")))
			g.Expect(engineC.Resources.Requests).To(HaveKeyWithValue(corev1.ResourceMemory, apiResource("256Mi")))
			g.Expect(engineC.Resources.Limits).To(HaveKeyWithValue(corev1.ResourceCPU, apiResource("1")))
			g.Expect(engineC.Resources.Limits).To(HaveKeyWithValue(corev1.ResourceMemory, apiResource("2Gi")))
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	// Rendered pod must carry the OMENative-required labels (managed-by,
	// inferenceservice, component, instance-index, instance-incarnation,
	// runner). These labels back the selectors that listOMENativePods,
	// the headless Service, and EndpointSlice mapping all use.
	ginkgo.It("rendered pod carries the full OMENative label set", func() {
		fx := newOMENativeFixture(ns.Name,
			WithCBMName("labels-llama"),
			WithCSRName("labels-runtime"),
			WithISVCName("labels-isvc"),
		)
		Expect(k8sClient.Create(suiteCtx, fx.CBM)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CBM) })
		Expect(k8sClient.Create(suiteCtx, fx.CSR)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CSR) })
		Expect(k8sClient.Create(suiteCtx, fx.ISVC)).To(Succeed())

		Eventually(func(g Gomega) {
			pod := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
				Namespace: ns.Name, Name: "labels-isvc-engine-0-default-0",
			}, pod)).To(Succeed())
			g.Expect(pod.Labels).To(HaveKeyWithValue("ome.io/managed-by", "OMENative"))
			g.Expect(pod.Labels).To(HaveKeyWithValue("ome.io/inferenceservice", "labels-isvc"))
			g.Expect(pod.Labels).To(HaveKeyWithValue("component", "engine"))
			g.Expect(pod.Labels).To(HaveKeyWithValue("ome.io/instance-index", "0"))
			g.Expect(pod.Labels).To(HaveKeyWithValue("ome.io/instance-incarnation", "1"))
			g.Expect(pod.Labels).To(HaveKeyWithValue("ome.io/runner", "default"))
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})
})

func containerNames(cs []corev1.Container) []string {
	out := make([]string, 0, len(cs))
	for _, c := range cs {
		out = append(out, c.Name)
	}
	return out
}
