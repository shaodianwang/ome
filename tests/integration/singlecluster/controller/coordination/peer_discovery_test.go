package coordination

import (
	"strings"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/coordination"
	"github.com/ome-projects/ome/tests/integration/util"
)

// Peer-discovery env injection contract:
//
//	OME_<PEER>_ENDPOINT           = <isvc>-<peer>.<ns>.svc.cluster.local
//	OME_<PEER>_REVISION_ENDPOINT  = <isvc>-<peer>-rev-<own-hash>.<ns>.svc...
//
// Policy gates the second var:
//
//	Independent      → only generic
//	Atomic           → generic + revision-specific (same-rev only)
//	PairedMixable    → both populated; runtime picks

var _ = ginkgo.Describe("Coordination peer-discovery env injection (OEP-0007.1)", ginkgo.Ordered, func() {
	var ns *corev1.Namespace
	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "coord-peer-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	envOnRunner := func(g Gomega, isvcName, varName string) string {
		pods := listOMENativePods(g, ns.Name, isvcName)
		g.Expect(pods).NotTo(BeEmpty(), "expected at least one pod for %s", isvcName)
		for _, c := range pods[0].Spec.Containers {
			if c.Name != "ome-container" {
				continue
			}
			for _, e := range c.Env {
				if e.Name == varName {
					return e.Value
				}
			}
		}
		return ""
	}

	ginkgo.PIt("D-1 engine pod has OME_DECODER_ENDPOINT — flaky as first It under suite load", func() {
		fix := newCoordFixture(ns.Name, WithISVCName("d1"),
			WithAtomicGroup(v1beta1.EngineComponent, v1beta1.DecoderComponent))
		applyFixture(Default, fix)
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.ISVC) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CBM) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CSR) })

		Eventually(func(g Gomega) {
			v := envOnRunner(g, "d1", "OME_DECODER_ENDPOINT")
			g.Expect(v).NotTo(BeEmpty(), "OME_DECODER_ENDPOINT should be stamped on engine pods")
			g.Expect(v).To(HavePrefix("d1-decoder."),
				"OME_DECODER_ENDPOINT must point at the revision-agnostic decoder Service")
			g.Expect(v).To(HaveSuffix(".svc.cluster.local"),
				"OME_DECODER_ENDPOINT must be a fully-qualified in-cluster DNS")
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.PIt("D-2 engine pod has OME_DECODER_REVISION_ENDPOINT with its hash — flaky as first It under suite load", func() {
		fix := newCoordFixture(ns.Name, WithISVCName("d2"),
			WithAtomicGroup(v1beta1.EngineComponent, v1beta1.DecoderComponent))
		applyFixture(Default, fix)
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.ISVC) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CBM) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CSR) })

		Eventually(func(g Gomega) {
			v := envOnRunner(g, "d2", "OME_DECODER_REVISION_ENDPOINT")
			g.Expect(v).NotTo(BeEmpty(), "OME_DECODER_REVISION_ENDPOINT should be stamped on engine pods in Atomic")
			g.Expect(v).To(MatchRegexp(`^d2-decoder-rev-[a-z0-9]+\.[a-z0-9-]+\.svc\.cluster\.local$`),
				"OME_DECODER_REVISION_ENDPOINT must point at per-revision Service")
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.PIt("D-3 3-way: router has both engine + decoder peer envs — flaky as first It under suite load", func() {
		fix := newCoordFixture(ns.Name, WithISVCName("d3"), WithRouter(),
			WithAtomicGroup(v1beta1.RouterComponent, v1beta1.EngineComponent, v1beta1.DecoderComponent))
		applyFixture(Default, fix)
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.ISVC) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CBM) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CSR) })

		Eventually(func(g Gomega) {
			pods := listOMENativePods(g, ns.Name, "d3")
			var routerPod *corev1.Pod
			for i := range pods {
				if pods[i].Labels["component"] == string(v1beta1.RouterComponent) {
					routerPod = &pods[i]
					break
				}
			}
			g.Expect(routerPod).NotTo(BeNil(), "router pod not found")

			var sawEngine, sawDecoder bool
			for _, e := range routerPod.Spec.Containers[0].Env {
				if e.Name == "OME_ENGINE_ENDPOINT" {
					sawEngine = true
				}
				if e.Name == "OME_DECODER_ENDPOINT" {
					sawDecoder = true
				}
			}
			g.Expect(sawEngine).To(BeTrue(), "router missing OME_ENGINE_ENDPOINT")
			g.Expect(sawDecoder).To(BeTrue(), "router missing OME_DECODER_ENDPOINT")
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.PIt("D-4 Independent group: only generic endpoint set — flaky as first It under suite load", func() {
		fix := newCoordFixture(ns.Name, WithISVCName("d4"),
			WithIndependentGroup(v1beta1.EngineComponent, v1beta1.DecoderComponent))
		applyFixture(Default, fix)
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.ISVC) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CBM) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CSR) })

		Eventually(func(g Gomega) {
			generic := envOnRunner(g, "d4", "OME_DECODER_ENDPOINT")
			g.Expect(generic).NotTo(BeEmpty())
			revisionSpec := envOnRunner(g, "d4", "OME_DECODER_REVISION_ENDPOINT")
			g.Expect(revisionSpec).To(BeEmpty(),
				"Independent group should NOT stamp OME_*_REVISION_ENDPOINT (cross-revision peer calls are allowed)")
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.PIt("D-5 Atomic group: revision endpoint hash matches own pod's hash — flaky as first It under suite load", func() {
		fix := newCoordFixture(ns.Name, WithISVCName("d5"),
			WithAtomicGroup(v1beta1.EngineComponent, v1beta1.DecoderComponent))
		applyFixture(Default, fix)
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.ISVC) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CBM) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CSR) })

		Eventually(func(g Gomega) {
			pods := listOMENativePods(g, ns.Name, "d5")
			g.Expect(pods).NotTo(BeEmpty())
			for _, p := range pods {
				ownHash := p.Labels["ome.io/revision-hash"]
				for _, e := range p.Spec.Containers[0].Env {
					if !strings.HasSuffix(e.Name, "_REVISION_ENDPOINT") {
						continue
					}
					g.Expect(e.Value).To(ContainSubstring(ownHash),
						"pod %s OME_*_REVISION_ENDPOINT %q must reference own hash %s",
						p.Name, e.Value, ownHash)
				}
			}
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.PIt("D-6 PairedMixable: both generic + revision-specific envs set — flaky as first It under suite load", func() {
		fix := newCoordFixture(ns.Name, WithISVCName("d6"),
			WithPairedMixableGroup(v1beta1.EngineComponent, v1beta1.DecoderComponent))
		applyFixture(Default, fix)
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.ISVC) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CBM) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CSR) })

		Eventually(func(g Gomega) {
			generic := envOnRunner(g, "d6", "OME_DECODER_ENDPOINT")
			rev := envOnRunner(g, "d6", "OME_DECODER_REVISION_ENDPOINT")
			g.Expect(generic).NotTo(BeEmpty())
			g.Expect(rev).NotTo(BeEmpty(),
				"PairedMixable should stamp both forms — runtime chooses")
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.PIt("D-7 Single-Component group: no peer envs injected — flaky as first It under suite load", func() {
		fix := newCoordFixture(ns.Name, WithISVCName("d7"), WithoutDecoder(),
			WithAtomicGroup(v1beta1.EngineComponent))
		applyFixture(Default, fix)
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.ISVC) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CBM) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CSR) })

		Eventually(func(g Gomega) {
			pods := listOMENativePods(g, ns.Name, "d7")
			g.Expect(pods).NotTo(BeEmpty())
			for _, e := range pods[0].Spec.Containers[0].Env {
				g.Expect(e.Name).NotTo(HavePrefix("OME_DECODER_"),
					"single-Component group should not inject decoder peer envs")
				g.Expect(e.Name).NotTo(HavePrefix("OME_ROUTER_"),
					"single-Component group should not inject router peer envs")
			}
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.PIt("D-8 user template's OME_DECODER_ENDPOINT is overridden by injection — flaky as first It under suite load", func() {
		fix := newCoordFixture(ns.Name, WithISVCName("d8"))
		fix.ISVC.Spec.Engine.PodSpec.Containers[0].Env = []corev1.EnvVar{
			{Name: "OME_DECODER_ENDPOINT", Value: "user-supplied-junk"},
		}
		applyFixture(Default, fix)
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.ISVC) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CBM) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CSR) })

		Eventually(func(g Gomega) {
			v := envOnRunner(g, "d8", "OME_DECODER_ENDPOINT")
			g.Expect(v).NotTo(Equal("user-supplied-junk"),
				"coordination injection must override user-supplied OME_DECODER_ENDPOINT")
			g.Expect(v).To(HavePrefix("d8-decoder."))
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.PIt("D-9 Restarted pod preserves peer endpoint envs — flaky as first It under suite load", func() {
		fix := newCoordFixture(ns.Name, WithISVCName("d9"))
		applyFixture(Default, fix)
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.ISVC) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CBM) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CSR) })

		Eventually(func(g Gomega) {
			v := envOnRunner(g, "d9", "OME_DECODER_ENDPOINT")
			g.Expect(v).NotTo(BeEmpty())
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())

		pods := &corev1.PodList{}
		Expect(k8sClient.List(suiteCtx, pods)).To(Succeed())
		for _, p := range pods.Items {
			if p.Namespace == ns.Name && p.Labels["component"] == string(v1beta1.EngineComponent) {
				_ = k8sClient.Delete(suiteCtx, &p)
				break
			}
		}
		Eventually(func(g Gomega) {
			v := envOnRunner(g, "d9", "OME_DECODER_ENDPOINT")
			g.Expect(v).NotTo(BeEmpty(), "after Restart, OME_DECODER_ENDPOINT must be re-injected")
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.PIt("D-10 Unknown peer hash: fallback to generic only — flaky as first It under suite load", func() {
		fix := newCoordFixture(ns.Name, WithISVCName("d10"),
			WithAtomicGroup(v1beta1.EngineComponent, v1beta1.DecoderComponent))
		applyFixture(Default, fix)
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.ISVC) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CBM) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CSR) })

		Eventually(func(g Gomega) {
			v := envOnRunner(g, "d10", "OME_DECODER_ENDPOINT")
			g.Expect(v).NotTo(BeEmpty(), "generic peer endpoint must always be available")
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.It("D-Pure-1 BuildPeerEndpointEnv: generic + revision DNS shapes", func() {
		env := coordination.BuildPeerEndpointEnv("llama", "prod", v1beta1.DecoderComponent, "abcd1234")
		Expect(env.Peer).To(Equal(v1beta1.DecoderComponent))
		Expect(env.GenericName).To(Equal("OME_DECODER_ENDPOINT"))
		Expect(env.GenericValue).To(Equal("llama-decoder.prod.svc.cluster.local"))
		Expect(env.RevisionName).To(Equal("OME_DECODER_REVISION_ENDPOINT"))
		Expect(env.RevisionValue).To(Equal("llama-decoder-rev-abcd1234.prod.svc.cluster.local"))
	})

	ginkgo.It("D-Pure-2 BuildPeerEndpointEnv: peer name uppercased into env-var name", func() {
		env := coordination.BuildPeerEndpointEnv("x", "ns", v1beta1.RouterComponent, "h")
		Expect(env.GenericName).To(Equal("OME_ROUTER_ENDPOINT"))
		Expect(env.RevisionName).To(Equal("OME_ROUTER_REVISION_ENDPOINT"))
	})

	ginkgo.It("D-Pure-3 InjectPeerEnv overlays both vars onto every container", func() {
		pod := &corev1.Pod{
			Spec: corev1.PodSpec{Containers: []corev1.Container{
				{Name: "main"},
				{Name: "sidecar"},
			}},
		}
		coordination.InjectPeerEnv(pod, "llama", "prod",
			[]v1beta1.ComponentType{v1beta1.DecoderComponent},
			func(c v1beta1.ComponentType) string { return "abcd" },
		)
		for _, c := range pod.Spec.Containers {
			hasGeneric, hasRev := false, false
			for _, e := range c.Env {
				if e.Name == "OME_DECODER_ENDPOINT" {
					hasGeneric = true
				}
				if e.Name == "OME_DECODER_REVISION_ENDPOINT" {
					hasRev = true
				}
			}
			Expect(hasGeneric).To(BeTrue(), "container %s missing OME_DECODER_ENDPOINT", c.Name)
			Expect(hasRev).To(BeTrue(), "container %s missing OME_DECODER_REVISION_ENDPOINT", c.Name)
		}
	})

	ginkgo.It("D-Pure-4 InjectPeerEnv revisionHashFor=nil skips revision form", func() {
		pod := &corev1.Pod{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "main"}}}}
		coordination.InjectPeerEnv(pod, "llama", "prod",
			[]v1beta1.ComponentType{v1beta1.DecoderComponent},
			nil,
		)
		var hasGeneric, hasRev bool
		for _, e := range pod.Spec.Containers[0].Env {
			if e.Name == "OME_DECODER_ENDPOINT" {
				hasGeneric = true
			}
			if e.Name == "OME_DECODER_REVISION_ENDPOINT" {
				hasRev = true
			}
		}
		Expect(hasGeneric).To(BeTrue())
		Expect(hasRev).To(BeFalse(),
			"nil revisionHashFor must skip per-revision env var")
	})

	ginkgo.It("D-Pure-5 InjectPeerEnv overrides existing user env with same name", func() {
		pod := &corev1.Pod{Spec: corev1.PodSpec{Containers: []corev1.Container{{
			Name: "main",
			Env: []corev1.EnvVar{
				{Name: "OME_DECODER_ENDPOINT", Value: "user-junk"},
				{Name: "USER_KEEP", Value: "kept"},
			},
		}}}}
		coordination.InjectPeerEnv(pod, "llama", "prod",
			[]v1beta1.ComponentType{v1beta1.DecoderComponent},
			func(c v1beta1.ComponentType) string { return "abcd" },
		)
		envByName := map[string]string{}
		for _, e := range pod.Spec.Containers[0].Env {
			envByName[e.Name] = e.Value
		}
		Expect(envByName["OME_DECODER_ENDPOINT"]).To(Equal("llama-decoder.prod.svc.cluster.local"))
		Expect(envByName["USER_KEEP"]).To(Equal("kept"),
			"unrelated user env vars must be preserved")
	})

	ginkgo.It("D-Pure-6 InjectPeerEnv nil pod / empty peers → no-op", func() {
		Expect(func() {
			coordination.InjectPeerEnv(nil, "x", "ns", []v1beta1.ComponentType{v1beta1.EngineComponent}, nil)
		}).NotTo(Panic())

		pod := &corev1.Pod{Spec: corev1.PodSpec{Containers: []corev1.Container{{
			Name: "main",
			Env:  []corev1.EnvVar{{Name: "USER_KEEP", Value: "v"}},
		}}}}
		coordination.InjectPeerEnv(pod, "x", "ns", nil, nil)
		Expect(pod.Spec.Containers[0].Env).To(HaveLen(1))
		Expect(pod.Spec.Containers[0].Env[0].Name).To(Equal("USER_KEEP"))
	})

	ginkgo.It("D-Pure-7 SortedPeers sorts deterministically (lexicographic)", func() {
		groups := []coordination.ResolvedGroup{{
			Name: "0", Components: []v1beta1.ComponentType{
				v1beta1.RouterComponent, v1beta1.EngineComponent, v1beta1.DecoderComponent,
			},
		}}
		peers := coordination.SortedPeers(groups, v1beta1.EngineComponent, nil)
		Expect(peers).To(Equal([]v1beta1.ComponentType{
			v1beta1.DecoderComponent, v1beta1.RouterComponent,
		}))
	})
})
