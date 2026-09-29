package inferenceservice

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/basemodel/backends/sharded"
	v1beta1testing "github.com/ome-projects/ome/pkg/utils/testing/v1beta1"

	"github.com/ome-projects/ome/tests/integration/util"
)

// Swapping ISVC.Spec.Model.Name across distribution types is the
// pattern operators use when migrating a model from one storage
// backend to another (e.g. a cluster_cache rollout) without rebuilding
// the ISVC. The reconciler re-resolves the BaseModel reference on
// every pass and emits the appropriate pod spec — Sharded gets
// MODEL_CACHE_* env injection plus MODEL_PATH set to the storage URI
// verbatim, PVC gets a Volume + VolumeMount with
// MODEL_PATH=/opt/ml/model. The
// runtime selector + ValidateRuntime gate every transition; a swap
// to a distribution the runtime doesn't support stalls the rollout.
//
// These specs are coverage for the model-swap surface that the pkg/render
// branches produce, exercising both:
//
//   - the env-injection branch in UpdateEnvVariables / clusterCacheEnvVars
//     (Sharded only; PVC + per-node skip cache envs entirely)
//   - the volume branch in UpdatePodSpecVolumes / UpdateVolumeMounts (PVC
//     adds the PVC volume + mount; Sharded carries no model volume)
//
// Existing distribution_change_test.go in basemodel covers the CBM-side
// branch flip; this is the ISVC-side mirror.
var _ = ginkgo.Describe("InferenceService model swap across distributions", ginkgo.Ordered, func() {
	var ns *corev1.Namespace

	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "isvc-modelswap-")
		Expect(err).NotTo(HaveOccurred())
	})

	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("Sharded → PVC: drops cache envs, gains PVC volume + MODEL_PATH=/opt/ml/model", func() {
		pvcName := "modelswap-fwd-pvc"
		subPath := "models/llama-fwd"
		makeBoundPVC(ns.Name, pvcName)

		shardedURI := "oci://n/test/b/models/o/swap/sharded-fwd"
		shardedCBM := makeShardedCBMReady("modelswap-fwd-sharded", shardedURI)
		ginkgo.DeferCleanup(deleteObj(shardedCBM))

		pvcCBM := v1beta1testing.MakeClusterBaseModel("modelswap-fwd-pvc-cbm").
			StorageURI("pvc://" + ns.Name + ":" + pvcName + "/" + subPath).
			ModelFormat("safetensors").
			ModelArchitecture("LlamaForCausalLM").
			ModelFramework("transformers").Obj()
		Expect(k8sClient.Create(suiteCtx, pvcCBM)).To(Succeed())
		ginkgo.DeferCleanup(deleteObj(pvcCBM))

		csr := dualDistributionRuntime("modelswap-fwd-runtime")
		Expect(k8sClient.Create(suiteCtx, csr)).To(Succeed())
		ginkgo.DeferCleanup(deleteObj(csr))

		ginkgo.By("sanity-checking CSR is readable from apiserver before the ISVC reconciler races to look it up")
		gotCSR := &v1beta1.ClusterServingRuntime{}
		Expect(k8sClient.Get(suiteCtx, client.ObjectKey{Name: csr.Name}, gotCSR)).To(Succeed())
		Expect(gotCSR.Spec.SupportedModelFormats).To(HaveLen(2))

		isvc := v1beta1testing.MakeInferenceService("modelswap-fwd-isvc", ns.Name).
			Model(shardedCBM.Name).
			Runtime(csr.Name).
			DeploymentMode(constants.RawDeployment).
			Engine(&v1beta1.EngineSpec{}).Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())

		ginkgo.By("Deployment reflects Sharded: MODEL_PATH=storage URI, cache envs present, no PVC volume")
		Eventually(func(g Gomega) {
			pod := getISVCPodSpec(g, ns.Name)
			assertModelPath(g, pod, shardedURI)
			assertCacheEnvs(g, pod, true)
			assertPVCVolumeAbsent(g, pod, pvcName)
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("patching ISVC to reference the PVC CBM")
		setISVCModel(isvc, pvcCBM.Name)

		ginkgo.By("Deployment flips to PVC: MODEL_PATH=/opt/ml/model, cache envs gone, PVC volume + mount present")
		Eventually(func(g Gomega) {
			pod := getISVCPodSpec(g, ns.Name)
			assertModelPath(g, pod, constants.ModelDefaultMountPath)
			assertCacheEnvs(g, pod, false)
			assertPVCVolumeMount(g, pod, pvcName, subPath)
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.It("PVC → Sharded: drops PVC volume, gains cache envs + storage-URI MODEL_PATH", func() {
		pvcName := "modelswap-rev-pvc"
		subPath := "models/llama-rev"
		makeBoundPVC(ns.Name, pvcName)

		shardedURI := "oci://n/test/b/models/o/swap/sharded-rev"
		shardedCBM := makeShardedCBMReady("modelswap-rev-sharded", shardedURI)
		ginkgo.DeferCleanup(deleteObj(shardedCBM))

		pvcCBM := v1beta1testing.MakeClusterBaseModel("modelswap-rev-pvc-cbm").
			StorageURI("pvc://" + ns.Name + ":" + pvcName + "/" + subPath).
			ModelFormat("safetensors").
			ModelArchitecture("LlamaForCausalLM").
			ModelFramework("transformers").Obj()
		Expect(k8sClient.Create(suiteCtx, pvcCBM)).To(Succeed())
		ginkgo.DeferCleanup(deleteObj(pvcCBM))

		csr := dualDistributionRuntime("modelswap-rev-runtime")
		Expect(k8sClient.Create(suiteCtx, csr)).To(Succeed())
		ginkgo.DeferCleanup(deleteObj(csr))

		isvc := v1beta1testing.MakeInferenceService("modelswap-rev-isvc", ns.Name).
			Model(pvcCBM.Name).
			Runtime(csr.Name).
			DeploymentMode(constants.RawDeployment).
			Engine(&v1beta1.EngineSpec{}).Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())

		ginkgo.By("Deployment reflects PVC: mount + MODEL_PATH=/opt/ml/model, no cache envs")
		Eventually(func(g Gomega) {
			pod := getISVCPodSpec(g, ns.Name)
			assertModelPath(g, pod, constants.ModelDefaultMountPath)
			assertCacheEnvs(g, pod, false)
			assertPVCVolumeMount(g, pod, pvcName, subPath)
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("patching ISVC to reference the Sharded CBM")
		setISVCModel(isvc, shardedCBM.Name)

		ginkgo.By("Deployment flips to Sharded: MODEL_PATH=storage URI, cache envs present, PVC volume gone")
		Eventually(func(g Gomega) {
			pod := getISVCPodSpec(g, ns.Name)
			assertModelPath(g, pod, shardedURI)
			assertCacheEnvs(g, pod, true)
			assertPVCVolumeAbsent(g, pod, pvcName)
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.It("PVC → Sharded swap stalled when runtime only supports PVC (no cluster_cache provider)", func() {
		pvcName := "modelswap-stall-pvc"
		subPath := "models/llama-stall"
		makeBoundPVC(ns.Name, pvcName)

		shardedURI := "oci://n/test/b/models/o/swap/sharded-stall"
		shardedCBM := makeShardedCBMReady("modelswap-stall-sharded", shardedURI)
		ginkgo.DeferCleanup(deleteObj(shardedCBM))

		pvcCBM := v1beta1testing.MakeClusterBaseModel("modelswap-stall-pvc-cbm").
			StorageURI("pvc://" + ns.Name + ":" + pvcName + "/" + subPath).
			ModelFormat("safetensors").
			ModelArchitecture("LlamaForCausalLM").
			ModelFramework("transformers").Obj()
		Expect(k8sClient.Create(suiteCtx, pvcCBM)).To(Succeed())
		ginkgo.DeferCleanup(deleteObj(pvcCBM))

		ginkgo.By("runtime with a single PVC-only supportedModelFormat — no modelCacheProviders, so Sharded models fail validation")
		csr := v1beta1testing.MakeClusterServingRuntime("modelswap-stall-runtime").
			SupportsModelFormat("safetensors", false, 10).
			WithLastFormatArchitecture("LlamaForCausalLM").
			EngineRunner(corev1.Container{
				Name:  "ome-container",
				Image: "ghcr.io/test/runtime:test",
			}).Obj()
		// Framework name matches the lowercase the modelparser emits
		// (see dualDistributionRuntime for the rationale).
		csr.Spec.SupportedModelFormats[0].ModelFramework = &v1beta1.ModelFrameworkSpec{Name: "transformers"}
		Expect(k8sClient.Create(suiteCtx, csr)).To(Succeed())
		ginkgo.DeferCleanup(deleteObj(csr))

		isvc := v1beta1testing.MakeInferenceService("modelswap-stall-isvc", ns.Name).
			Model(pvcCBM.Name).
			Runtime(csr.Name).
			DeploymentMode(constants.RawDeployment).
			Engine(&v1beta1.EngineSpec{}).Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())

		ginkgo.By("PVC pod spec materializes initially")
		Eventually(func(g Gomega) {
			pod := getISVCPodSpec(g, ns.Name)
			assertModelPath(g, pod, constants.ModelDefaultMountPath)
			assertPVCVolumeMount(g, pod, pvcName, subPath)
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("patching ISVC to reference the Sharded CBM — runtime validation will reject the swap")
		setISVCModel(isvc, shardedCBM.Name)

		ginkgo.By("Deployment never picks up Sharded-shaped env / MODEL_PATH — the controller's ValidateRuntime error keeps the spec frozen")
		Consistently(func(g Gomega) {
			pod := getISVCPodSpec(g, ns.Name)
			// MODEL_PATH must NOT have flipped to the Sharded URI;
			// cache envs must NOT have appeared.
			for _, c := range allContainers(pod) {
				for _, e := range c.Env {
					g.Expect(e.Name).NotTo(Equal(constants.ModelCacheProviderEnvVarKey),
						"cache provider env must not be injected — validation should have stopped the swap")
					if e.Name == constants.ModelPathEnvVarKey {
						g.Expect(e.Value).NotTo(Equal(shardedURI),
							"MODEL_PATH must not flip to the Sharded URI when validation rejects the swap")
					}
				}
			}
		}, util.ShortTimeout, util.PollInterval).Should(Succeed())
	})
})

// makeBoundPVC creates a PVC in the test namespace and force-binds it.
// envtest lacks a volume controller, so PVCs stay Pending unless we
// patch the Status to ClaimBound manually; the ISVC reconciler doesn't
// look at PVC phase but the pattern matches the existing pvc_test.go.
func makeBoundPVC(namespace, name string) {
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")},
			},
		},
	}
	Expect(k8sClient.Create(suiteCtx, pvc)).To(Succeed())
	pvc.Status.Phase = corev1.ClaimBound
	Expect(k8sClient.Status().Update(suiteCtx, pvc)).To(Succeed())
}

// makeShardedCBMReady creates a Sharded ClusterBaseModel and waits for
// the BaseModel reconciler to drive it to Ready=True via the
// suite-level fake mc Client (suite_test.go's CheckSource/InspectSource
// defaults). The ISVC reconciler gates on ShardedBaseModelReady
// (controller.go:220) before generating runtime resources, so the
// swap tests must wait for Ready before patching ISVC spec.
//
// ModelArchitecture / ModelFormat / ModelFramework are left unset on
// Spec — the reconciler populates them by parsing the fake's
// returned config.json (LlamaForCausalLM, safetensors, transformers).
// Tests pair this with a runtime that supports LlamaForCausalLM.
func makeShardedCBMReady(name, storageURI string) *v1beta1.ClusterBaseModel {
	cbm := v1beta1testing.MakeClusterBaseModel(name).
		StorageURI(storageURI).
		Distribution(v1beta1.DistributionSharded).
		ModelFormat("safetensors").Obj()
	Expect(k8sClient.Create(suiteCtx, cbm)).To(Succeed())

	Eventually(func(g Gomega) {
		got := &v1beta1.ClusterBaseModel{}
		g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{Name: name}, got)).To(Succeed())
		ready := meta.FindStatusCondition(got.Status.Conditions, v1beta1.ModelConditionReady)
		g.Expect(ready).NotTo(BeNil(), "Ready condition not yet observed for CBM %s", name)
		g.Expect(ready.Status).To(Equal(metav1.ConditionTrue),
			"CBM %s Ready=%s reason=%s message=%s", name, ready.Status, ready.Reason, ready.Message)
		g.Expect(ready.ObservedGeneration).To(Equal(got.Generation),
			"CBM %s Ready stamped against gen=%d but current gen=%d",
			name, ready.ObservedGeneration, got.Generation)
	}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())

	got := &v1beta1.ClusterBaseModel{}
	Expect(k8sClient.Get(suiteCtx, client.ObjectKey{Name: name}, got)).To(Succeed())
	return got
}

// dualDistributionRuntime builds a CSR with two supportedModelFormat
// entries — one declaring modelCacheProviders=[cluster_cache] (gates
// Sharded models) and one without (matches PVC / per-node delivery).
// Mirrors the dual-entry shape of the PD-disaggregated sample
// ClusterServingRuntime in config/samples/.
//
// Framework name is forced to lowercase "transformers" to match what
// the modelparser emits when the suite's fake mc client returns a
// model_type=llama config.json — the matcher's framework comparison is
// case-sensitive (matcher.go: framework name mismatch).
func dualDistributionRuntime(name string) *v1beta1.ClusterServingRuntime {
	csr := v1beta1testing.MakeClusterServingRuntime(name).
		SupportsModelFormat("safetensors", false, 10).
		WithLastFormatArchitecture("LlamaForCausalLM").
		SupportsModelFormat("safetensors", false, 10).
		WithLastFormatArchitecture("LlamaForCausalLM").
		EngineRunner(corev1.Container{
			Name:  "ome-container",
			Image: "ghcr.io/test/runtime:test",
		}).Obj()
	csr.Spec.SupportedModelFormats[0].ModelCacheProviders = []v1beta1.ModelCacheProvider{
		v1beta1.ModelCacheProviderClusterCache,
	}
	for i := range csr.Spec.SupportedModelFormats {
		csr.Spec.SupportedModelFormats[i].ModelFramework = &v1beta1.ModelFrameworkSpec{
			Name: "transformers",
		}
	}
	return csr
}

// setISVCModel patches the ISVC's Spec.Model.Name with retry-on-conflict
// semantics — the controller is writing status concurrently so a plain
// Update can race.
func setISVCModel(isvc *v1beta1.InferenceService, newModelName string) {
	Eventually(func(g Gomega) {
		got := &v1beta1.InferenceService{}
		g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(isvc), got)).To(Succeed())
		got.Spec.Model.Name = newModelName
		g.Expect(k8sClient.Update(suiteCtx, got)).To(Succeed())
	}).WithTimeout(util.ShortTimeout).WithPolling(util.PollInterval).Should(Succeed())
}

// getISVCPodSpec returns the pod template spec of the (single)
// Deployment the ISVC owns in the namespace. Tests should call this
// inside an Eventually block — the Deployment may not exist yet on the
// first call after ISVC create.
func getISVCPodSpec(g Gomega, namespace string) corev1.PodSpec {
	deps := &appsv1.DeploymentList{}
	g.Expect(k8sClient.List(suiteCtx, deps, client.InNamespace(namespace))).To(Succeed())
	g.Expect(deps.Items).To(HaveLen(1), "expected exactly one ISVC Deployment in %s", namespace)
	return deps.Items[0].Spec.Template.Spec
}

// allContainers flattens regular + init containers — both can carry
// the model env vars / mounts depending on the runtime shape.
func allContainers(pod corev1.PodSpec) []corev1.Container {
	out := append([]corev1.Container{}, pod.Containers...)
	return append(out, pod.InitContainers...)
}

func assertModelPath(g Gomega, pod corev1.PodSpec, want string) {
	for _, c := range allContainers(pod) {
		for _, e := range c.Env {
			if e.Name == constants.ModelPathEnvVarKey {
				g.Expect(e.Value).To(Equal(want),
					"MODEL_PATH on container %q must be %q (got %q)", c.Name, want, e.Value)
				return
			}
		}
	}
	g.Expect(false).To(BeTrue(), "no container carries MODEL_PATH env var")
}

// assertCacheEnvs checks the cluster_cache env trio is present (Sharded)
// or absent (non-Sharded). MODEL_CACHE_PROVIDER is the load-bearing
// signal — modelCacheEnvVars only ever emits it for Sharded models.
func assertCacheEnvs(g Gomega, pod corev1.PodSpec, want bool) {
	var foundProvider, foundEndpoint, foundHeadless bool
	for _, c := range allContainers(pod) {
		for _, e := range c.Env {
			switch e.Name {
			case constants.ModelCacheProviderEnvVarKey:
				foundProvider = true
				if want {
					g.Expect(e.Value).To(Equal(sharded.ProviderClusterCache))
				}
			case constants.ModelCacheEndpointEnvVarKey:
				foundEndpoint = true
			case constants.ClusterCacheHeadlessServiceEnvVarKey:
				foundHeadless = true
			}
		}
	}
	if want {
		g.Expect(foundProvider).To(BeTrue(), "Sharded pod must carry MODEL_CACHE_PROVIDER")
		g.Expect(foundEndpoint).To(BeTrue(), "Sharded pod must carry MODEL_CACHE_ENDPOINT (suite seeded an endpoint)")
		g.Expect(foundHeadless).To(BeTrue(), "Sharded pod must carry CLUSTER_CACHE_HEADLESS_SERVICE when provider=cluster_cache")
	} else {
		g.Expect(foundProvider).To(BeFalse(), "non-Sharded pod must not carry MODEL_CACHE_PROVIDER")
		g.Expect(foundEndpoint).To(BeFalse(), "non-Sharded pod must not carry MODEL_CACHE_ENDPOINT")
		g.Expect(foundHeadless).To(BeFalse(), "non-Sharded pod must not carry CLUSTER_CACHE_HEADLESS_SERVICE")
	}
}

func assertPVCVolumeMount(g Gomega, pod corev1.PodSpec, pvcName, subPath string) {
	var found *corev1.Volume
	for i := range pod.Volumes {
		if pod.Volumes[i].PersistentVolumeClaim != nil &&
			pod.Volumes[i].PersistentVolumeClaim.ClaimName == pvcName {
			found = &pod.Volumes[i]
			break
		}
	}
	g.Expect(found).NotTo(BeNil(), "pod must reference PVC %q", pvcName)
	g.Expect(found.PersistentVolumeClaim.ReadOnly).To(BeTrue(), "model PVC must be ReadOnly")

	var mountFound bool
	for _, c := range allContainers(pod) {
		for _, vm := range c.VolumeMounts {
			if vm.Name == found.Name &&
				vm.MountPath == constants.ModelDefaultMountPath &&
				vm.SubPath == subPath {
				mountFound = true
			}
		}
	}
	g.Expect(mountFound).To(BeTrue(),
		"at least one container must mount the PVC %q at %q with SubPath=%q",
		found.Name, constants.ModelDefaultMountPath, subPath)
}

func assertPVCVolumeAbsent(g Gomega, pod corev1.PodSpec, pvcName string) {
	for _, v := range pod.Volumes {
		if v.PersistentVolumeClaim != nil && v.PersistentVolumeClaim.ClaimName == pvcName {
			g.Expect(false).To(BeTrue(), "pod must not carry PVC volume %q after swap to non-PVC distribution", pvcName)
		}
	}
}

// deleteObj wraps a plain Delete in a cleanup closure that swallows
// NotFound — DeferCleanup runs in LIFO order and a previous spec may
// have already removed an object the next spec re-creates with the
// same name.
func deleteObj(obj client.Object) func() {
	return func() { _ = k8sClient.Delete(suiteCtx, obj) }
}
