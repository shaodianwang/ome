package inferenceservice

import (
	kedav1 "github.com/kedacore/keda/v2/apis/keda/v1alpha1"
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	isvcutils "github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/utils"
	v1beta1testing "github.com/ome-projects/ome/pkg/utils/testing/v1beta1"

	"github.com/ome-projects/ome/tests/integration/util"
)

// KEDA-driven autoscaling for InferenceService. The default
// shouldCreateScaledObject branch (keda_reconciler.go:259-263) treats
// both `autoscalerClass=keda` and the unset-annotation case as KEDA-
// eligible, so these specs explicitly set the annotation to make the
// intent visible at the test boundary.
//
// Until this file, the entire KEDA path had zero integration coverage
// — the existing autoscaler_class_test.go only pinned the
// `autoscalerClass=external` negative (NO HPA). Bugs in the KEDA
// triggers / scale bounds / lifecycle would have escaped CI.
var _ = ginkgo.Describe("InferenceService autoscalerClass=keda", ginkgo.Ordered, func() {
	var ns *corev1.Namespace

	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "isvc-keda-")
		Expect(err).NotTo(HaveOccurred())
	})

	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	// makeKEDAFixtures creates a minimal CBM + CSR baseline shared by
	// each KEDA spec. Each spec uses a unique baseModelName / runtimeName
	// derived from the prefix so the suite-Ordered Describe doesn't
	// reuse stale objects across specs.
	makeKEDAFixtures := func(prefix string) (*v1beta1.ClusterBaseModel, *v1beta1.ClusterServingRuntime) {
		cbm := v1beta1testing.MakeClusterBaseModel(prefix + "-llama").
			StorageURI("oci://n/test/b/models/o/" + prefix).
			ModelFormat("safetensors").
			ModelArchitecture("LlamaForCausalLM").
			ModelFramework("Transformers").Obj()
		Expect(k8sClient.Create(suiteCtx, cbm)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, cbm) })

		csr := v1beta1testing.MakeClusterServingRuntime(prefix+"-runtime").
			SupportsModelFormat("safetensors", true, 10).
			WithLastFormatArchitecture("LlamaForCausalLM").
			EngineRunner(corev1.Container{Name: "ome-container", Image: "ghcr.io/test/runtime:test"}).Obj()
		Expect(k8sClient.Create(suiteCtx, csr)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, csr) })

		return cbm, csr
	}

	ginkgo.It("autoscalerClass=keda + MinReplicas<MaxReplicas → ScaledObject created with prometheus trigger; no HPA", func() {
		// Happy-path pin. Setting the annotation routes through KEDA
		// instead of the HPA reconciler. Asserts:
		//  1. A ScaledObject is created (and named per
		//     isvcutils.GetScaledObjectName)
		//  2. Spec.ScaleTargetRef points at the engine Deployment
		//  3. MinReplicaCount / MaxReplicaCount mirror the ISVC's
		//     ComponentExtensionSpec
		//  4. The default trigger is `prometheus` (the only trigger
		//     type the reconciler emits)
		//  5. NO HPA exists in the namespace — confirms the autoscaler
		//     dispatch sent the path to KEDA, not HPA
		prefix := "kbase"
		_, _ = makeKEDAFixtures(prefix)

		minR, maxR := 2, 6
		isvc := v1beta1testing.MakeInferenceService(prefix+"-isvc", ns.Name).
			Model(prefix+"-llama").
			Runtime(prefix+"-runtime").
			DeploymentMode(constants.RawDeployment).
			Annotation(constants.AutoscalerClass, string(constants.AutoscalerClassKEDA)).
			Engine(&v1beta1.EngineSpec{
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
					MinReplicas: &minR,
					MaxReplicas: maxR,
				},
			}).Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())

		Eventually(func(g Gomega) {
			so := &kedav1.ScaledObject{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{
				Name:      isvcutils.GetScaledObjectName(isvc.Name + "-engine"),
				Namespace: ns.Name,
			}, so)).To(Succeed())

			// ScaleTargetRef must point at the engine Deployment
			// (same name as the ISVC component metadata).
			g.Expect(so.Spec.ScaleTargetRef).NotTo(BeNil())
			g.Expect(so.Spec.ScaleTargetRef.Kind).To(Equal("Deployment"))

			g.Expect(so.Spec.MinReplicaCount).NotTo(BeNil())
			g.Expect(*so.Spec.MinReplicaCount).To(Equal(int32(minR)))
			g.Expect(so.Spec.MaxReplicaCount).NotTo(BeNil())
			g.Expect(*so.Spec.MaxReplicaCount).To(Equal(int32(maxR)))

			g.Expect(so.Spec.Triggers).NotTo(BeEmpty(),
				"keda reconciler always emits at least one trigger")
			g.Expect(so.Spec.Triggers[0].Type).To(Equal("prometheus"),
				"only prometheus trigger is implemented today")
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		// Bracket: assert NO HPA exists. KEDA-driven scaling must not
		// create a parallel HPA, otherwise both autoscalers would
		// fight over the Deployment's replica count.
		Consistently(func(g Gomega) {
			hpas := &autoscalingv2.HorizontalPodAutoscalerList{}
			g.Expect(k8sClient.List(suiteCtx, hpas, client.InNamespace(ns.Name))).To(Succeed())
			g.Expect(hpas.Items).To(BeEmpty(),
				"autoscalerClass=keda must NOT also create an HPA (got %d)", len(hpas.Items))
		}, util.ShortTimeout, util.PollInterval).Should(Succeed())
	})

	ginkgo.It("KEDA scale bounds: MaxReplicas <= MinReplicas → MaxReplicaCount clamped to MinReplicaCount", func() {
		// keda_reconciler.go:96-101 — calculateMaxReplicas returns
		// minReplicas when MaxReplicas <= int(minReplicas). The
		// reconciler is defensive against operator typos (Min=3 Max=1
		// would otherwise produce a ScaledObject that scales DOWN to
		// 1 with a target of 3, an outright bug). Pins the clamp at
		// the integration layer. Catches a regression where the
		// reconciler propagates the user's invalid value verbatim.
		prefix := "kclamp"
		_, _ = makeKEDAFixtures(prefix)

		// Min=3, Max=1 (intentionally invalid): Max must clamp to 3.
		minR, maxR := 3, 1
		isvc := v1beta1testing.MakeInferenceService(prefix+"-isvc", ns.Name).
			Model(prefix+"-llama").
			Runtime(prefix+"-runtime").
			DeploymentMode(constants.RawDeployment).
			Annotation(constants.AutoscalerClass, string(constants.AutoscalerClassKEDA)).
			Engine(&v1beta1.EngineSpec{
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
					MinReplicas: &minR,
					MaxReplicas: maxR,
				},
			}).Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())

		Eventually(func(g Gomega) {
			so := &kedav1.ScaledObject{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{
				Name:      isvcutils.GetScaledObjectName(isvc.Name + "-engine"),
				Namespace: ns.Name,
			}, so)).To(Succeed())

			g.Expect(so.Spec.MinReplicaCount).NotTo(BeNil())
			g.Expect(*so.Spec.MinReplicaCount).To(Equal(int32(minR)))

			g.Expect(so.Spec.MaxReplicaCount).NotTo(BeNil())
			g.Expect(*so.Spec.MaxReplicaCount).To(Equal(int32(minR)),
				"MaxReplicaCount must clamp UP to MinReplicaCount when the user supplies Max<=Min (defensive against typo'd specs)")
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.It("KEDA prometheus annotation overrides → ScaledObject trigger metadata reflects annotation values", func() {
		// keda_reconciler.go:147-215 — each scaling parameter
		// (threshold, operator, prom serverAddress, prom query) is
		// first looked up in pod annotations, then KedaConfig, then a
		// hard-coded default. Pins that the per-ISVC annotation path
		// actually flows into the ScaledObject's trigger metadata.
		// Catches a regression where the annotation lookup gets
		// detached from the trigger build (e.g., reading from the
		// wrong map or losing the override during ScaledObject
		// reconciliation).
		prefix := "kanno"
		_, _ = makeKEDAFixtures(prefix)

		const (
			customThreshold = "42"
			customOperator  = "GreaterThan"
			customPromAddr  = "http://prom.custom.svc.cluster.local:9090"
			customPromQuery = "sum(rate(custom_metric_total[1m]))"
		)
		minR, maxR := 1, 5
		isvc := v1beta1testing.MakeInferenceService(prefix+"-isvc", ns.Name).
			Model(prefix+"-llama").
			Runtime(prefix+"-runtime").
			DeploymentMode(constants.RawDeployment).
			Annotation(constants.AutoscalerClass, string(constants.AutoscalerClassKEDA)).
			Annotation(constants.KedaScalingThreshold, customThreshold).
			Annotation(constants.KedaScalingOperator, customOperator).
			Annotation(constants.KedaPrometheusServerAddress, customPromAddr).
			Annotation(constants.KedaPrometheusQuery, customPromQuery).
			Engine(&v1beta1.EngineSpec{
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
					MinReplicas: &minR,
					MaxReplicas: maxR,
				},
			}).Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())

		Eventually(func(g Gomega) {
			so := &kedav1.ScaledObject{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{
				Name:      isvcutils.GetScaledObjectName(isvc.Name + "-engine"),
				Namespace: ns.Name,
			}, so)).To(Succeed())

			g.Expect(so.Spec.Triggers).To(HaveLen(1))
			md := so.Spec.Triggers[0].Metadata
			g.Expect(md).NotTo(BeNil())
			g.Expect(md["threshold"]).To(Equal(customThreshold),
				"ome.io/threshold annotation must override the default '10'")
			g.Expect(md["operator"]).To(Equal(customOperator),
				"ome.io/operator annotation must override the default 'LessThanOrEqual'")
			g.Expect(md["serverAddress"]).To(Equal(customPromAddr),
				"ome.io/prometheus.serverAddress annotation must override the default cluster URL")
			g.Expect(md["query"]).To(Equal(customPromQuery),
				"ome.io/prometheus.query annotation must override the default VLLM query verbatim")
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.It("autoscalerClass switched keda → external mid-flight → ScaledObject deleted", func() {
		// keda_reconciler.go:254-257 — shouldDeleteScaledObject returns
		// true when the current annotation is `external`. So a switch
		// from `keda` to `external` must remove the existing
		// ScaledObject on the next reconcile (and per
		// autoscaler_class_test.go's existing spec, NOT create an HPA
		// either). Pins the cleanup half of the lifecycle — without
		// this an operator changing autoscaling strategies would
		// silently leave a ghost ScaledObject behind.
		prefix := "kswap"
		_, _ = makeKEDAFixtures(prefix)

		minR, maxR := 2, 4
		isvc := v1beta1testing.MakeInferenceService(prefix+"-isvc", ns.Name).
			Model(prefix+"-llama").
			Runtime(prefix+"-runtime").
			DeploymentMode(constants.RawDeployment).
			Annotation(constants.AutoscalerClass, string(constants.AutoscalerClassKEDA)).
			Engine(&v1beta1.EngineSpec{
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
					MinReplicas: &minR,
					MaxReplicas: maxR,
				},
			}).Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())

		ginkgo.By("phase 1: KEDA mode → ScaledObject exists")
		Eventually(func(g Gomega) {
			so := &kedav1.ScaledObject{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{
				Name:      isvcutils.GetScaledObjectName(isvc.Name + "-engine"),
				Namespace: ns.Name,
			}, so)).To(Succeed())
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("phase 2: flip annotation to external — read-modify-write to avoid losing a concurrent status update")
		Eventually(func(g Gomega) {
			latest := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: isvc.Name, Namespace: ns.Name}, latest)).To(Succeed())
			if latest.Annotations == nil {
				latest.Annotations = map[string]string{}
			}
			latest.Annotations[constants.AutoscalerClass] = string(constants.AutoscalerClassExternal)
			g.Expect(k8sClient.Update(suiteCtx, latest)).To(Succeed())
		}).WithTimeout(util.ShortTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("phase 3: ScaledObject is deleted")
		Eventually(func(g Gomega) {
			so := &kedav1.ScaledObject{}
			err := k8sClient.Get(suiteCtx, types.NamespacedName{
				Name:      isvcutils.GetScaledObjectName(isvc.Name + "-engine"),
				Namespace: ns.Name,
			}, so)
			g.Expect(client.IgnoreNotFound(err)).To(Succeed())
			g.Expect(err).To(HaveOccurred(), "ScaledObject must be gone after autoscalerClass=external takes over")
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})
})
