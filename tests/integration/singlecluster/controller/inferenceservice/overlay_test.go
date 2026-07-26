package inferenceservice

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	isvcutils "github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/utils"
	v1beta1testing "github.com/ome-projects/ome/pkg/utils/testing/v1beta1"

	"github.com/ome-projects/ome/tests/integration/util"
)

// Overlay end-to-end. Each spec asserts the controller renders the
// expected pod-spec shape per the overlay contract:
//
//   - PVC / PerNode overlay → Volume + VolumeMount at
//     /opt/ml/model-overlays/<name> (ReadOnly)
//   - Sharded overlay       → no mount; cluster_cache envs are added
//     even when the primary is non-Sharded
//   - Every overlay         → OVERLAY_<UPPERCASED_NAME>_MODEL_PATH env
//
// Skipped overlays (disabled BaseModel) do not break the primary and
// surface via status.MountedOverlays exclusion.
var _ = ginkgo.Describe("InferenceService overlays", ginkgo.Ordered, func() {
	var ns *corev1.Namespace

	ginkgo.BeforeEach(func() {
		setSuiteFakeMCDefaults()
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "isvc-overlay-")
		Expect(err).NotTo(HaveOccurred())
	})

	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("Sharded primary + PVC overlay: cache envs + overlay PVC volume + overlay env var", func() {
		pvcName := "overlay-fwd-pvc"
		subPath := "models/llama-overlay"
		makeBoundPVC(ns.Name, pvcName)

		shardedURI := "manifold://test-bucket/tree/overlay/sharded-primary"
		primary := makeShardedCBMReady("overlay-fwd-sharded", shardedURI)
		ginkgo.DeferCleanup(deleteObj(primary))

		overlay := v1beta1testing.MakeClusterBaseModel("overlay-fwd-pvc-cbm").
			StorageURI("pvc://" + ns.Name + ":" + pvcName + "/" + subPath).
			ModelFormat("safetensors").
			ModelArchitecture("LlamaForCausalLM").
			ModelFramework("transformers").Obj()
		Expect(k8sClient.Create(suiteCtx, overlay)).To(Succeed())
		ginkgo.DeferCleanup(deleteObj(overlay))

		csr := dualDistributionRuntime("overlay-fwd-runtime")
		Expect(k8sClient.Create(suiteCtx, csr)).To(Succeed())
		ginkgo.DeferCleanup(deleteObj(csr))

		isvc := v1beta1testing.MakeInferenceService("overlay-fwd-isvc", ns.Name).
			Model(primary.Name).
			Runtime(csr.Name).
			DeploymentMode(constants.RawDeployment).
			Engine(&v1beta1.EngineSpec{}).Obj()
		isvc.Spec.Model.Overlays = []v1beta1.ModelOverlayRef{
			{Name: overlay.Name, Kind: ptr.To("ClusterBaseModel"), APIGroup: ptr.To("ome.io")},
		}
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())

		Eventually(func(g Gomega) {
			pod := getISVCPodSpec(g, ns.Name)
			assertEnvEquals(g, pod, constants.ModelPathEnvVarKey, shardedURI)
			assertEnvPresent(g, pod, constants.ModelCacheProviderEnvVarKey)
			assertEnvEquals(g, pod, isvcutils.OverlayEnvVarName(overlay.Name),
				isvcutils.OverlayMountPath(overlay.Name))
			assertOverlayPVCMounted(g, pod, overlay.Name, pvcName, subPath)
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("status.MountedOverlays reflects the wired overlay")
		Eventually(func(g Gomega) {
			got := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(isvc), got)).To(Succeed())
			g.Expect(got.Status.MountedOverlays).To(HaveLen(1))
			g.Expect(got.Status.MountedOverlays[0].Name).To(Equal(overlay.Name))
			g.Expect(got.Status.MountedOverlays[0].MountPath).To(Equal(isvcutils.OverlayMountPath(overlay.Name)))
			// PVC is a storage backend, not a Distribution value; the
			// overlay's CBM has no Distribution set so the helper
			// defaults to PerNode (the documented default).
			g.Expect(got.Status.MountedOverlays[0].Distribution).To(Equal(string(v1beta1.DistributionPerNode)))
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.It("PVC primary + Sharded overlay: cluster_cache envs injected even though primary isn't Sharded", func() {
		pvcName := "overlay-rev-pvc"
		subPath := "models/llama-primary"
		makeBoundPVC(ns.Name, pvcName)

		primary := v1beta1testing.MakeClusterBaseModel("overlay-rev-pvc-cbm").
			StorageURI("pvc://" + ns.Name + ":" + pvcName + "/" + subPath).
			ModelFormat("safetensors").
			ModelArchitecture("LlamaForCausalLM").
			ModelFramework("transformers").Obj()
		Expect(k8sClient.Create(suiteCtx, primary)).To(Succeed())
		ginkgo.DeferCleanup(deleteObj(primary))

		shardedURI := "manifold://test-bucket/tree/overlay/sharded-overlay"
		overlay := makeShardedCBMReady("overlay-rev-sharded", shardedURI)
		ginkgo.DeferCleanup(deleteObj(overlay))

		csr := dualDistributionRuntime("overlay-rev-runtime")
		Expect(k8sClient.Create(suiteCtx, csr)).To(Succeed())
		ginkgo.DeferCleanup(deleteObj(csr))

		isvc := v1beta1testing.MakeInferenceService("overlay-rev-isvc", ns.Name).
			Model(primary.Name).
			Runtime(csr.Name).
			DeploymentMode(constants.RawDeployment).
			Engine(&v1beta1.EngineSpec{}).Obj()
		isvc.Spec.Model.Overlays = []v1beta1.ModelOverlayRef{
			{Name: overlay.Name, Kind: ptr.To("ClusterBaseModel"), APIGroup: ptr.To("ome.io")},
		}
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())

		Eventually(func(g Gomega) {
			pod := getISVCPodSpec(g, ns.Name)
			// PVC primary → MODEL_PATH points at the mount.
			assertEnvEquals(g, pod, constants.ModelPathEnvVarKey, constants.ModelDefaultMountPath)
			// Sharded overlay → cluster_cache envs MUST appear even though
			// the primary is PVC.
			assertEnvPresent(g, pod, constants.ModelCacheProviderEnvVarKey)
			assertEnvPresent(g, pod, constants.ClusterCacheHeadlessServiceEnvVarKey)
			// Sharded overlay → no mount; env value is the storage URI itself.
			assertEnvEquals(g, pod, isvcutils.OverlayEnvVarName(overlay.Name), shardedURI)
			assertNoVolumeNamed(g, pod, "model-overlay-"+overlay.Name)
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.It("Disabled overlay is skipped: pod spec carries no overlay env / volume", func() {
		shardedURI := "manifold://test-bucket/tree/overlay/skipped-primary"
		primary := makeShardedCBMReady("overlay-skip-sharded", shardedURI)
		ginkgo.DeferCleanup(deleteObj(primary))

		overlay := v1beta1testing.MakeClusterBaseModel("overlay-skip-disabled").
			StorageURI("pvc://" + ns.Name + ":wont-be-mounted/path").
			ModelFormat("safetensors").
			ModelArchitecture("LlamaForCausalLM").
			ModelFramework("transformers").Obj()
		overlay.Spec.Disabled = ptr.To(true)
		Expect(k8sClient.Create(suiteCtx, overlay)).To(Succeed())
		ginkgo.DeferCleanup(deleteObj(overlay))

		csr := dualDistributionRuntime("overlay-skip-runtime")
		Expect(k8sClient.Create(suiteCtx, csr)).To(Succeed())
		ginkgo.DeferCleanup(deleteObj(csr))

		isvc := v1beta1testing.MakeInferenceService("overlay-skip-isvc", ns.Name).
			Model(primary.Name).
			Runtime(csr.Name).
			DeploymentMode(constants.RawDeployment).
			Engine(&v1beta1.EngineSpec{}).Obj()
		isvc.Spec.Model.Overlays = []v1beta1.ModelOverlayRef{
			{Name: overlay.Name, Kind: ptr.To("ClusterBaseModel"), APIGroup: ptr.To("ome.io")},
		}
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())

		Eventually(func(g Gomega) {
			pod := getISVCPodSpec(g, ns.Name)
			// Primary still drives.
			assertEnvEquals(g, pod, constants.ModelPathEnvVarKey, shardedURI)
			// Disabled overlay must produce NO env, NO volume.
			assertEnvAbsent(g, pod, isvcutils.OverlayEnvVarName(overlay.Name))
			assertNoVolumeNamed(g, pod, "model-overlay-"+overlay.Name)
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("status.MountedOverlays excludes the disabled overlay; OverlaysReady=False with skip reason")
		Eventually(func(g Gomega) {
			got := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(isvc), got)).To(Succeed())
			g.Expect(got.Status.MountedOverlays).To(BeEmpty())
			cond := got.Status.GetCondition(v1beta1.OverlaysReady)
			g.Expect(cond).NotTo(BeNil())
			g.Expect(cond.Status).To(Equal(corev1.ConditionFalse))
			g.Expect(cond.Message).To(ContainSubstring("disabled"))
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})
})

// Helpers that are shared with model_swap_test.go (makeBoundPVC,
// makeShardedCBMReady, dualDistributionRuntime, getISVCPodSpec,
// allContainers, deleteObj) live there; this file only defines
// overlay-specific assertions.

func assertEnvEquals(g Gomega, pod corev1.PodSpec, name, want string) {
	for _, c := range allContainers(pod) {
		for _, e := range c.Env {
			if e.Name == name {
				g.Expect(e.Value).To(Equal(want), "env %s on container %s", name, c.Name)
				return
			}
		}
	}
	g.Expect(false).To(BeTrue(), "no container carries env %s", name)
}

func assertEnvPresent(g Gomega, pod corev1.PodSpec, name string) {
	for _, c := range allContainers(pod) {
		for _, e := range c.Env {
			if e.Name == name {
				return
			}
		}
	}
	g.Expect(false).To(BeTrue(), "expected env %s not present on any container", name)
}

func assertEnvAbsent(g Gomega, pod corev1.PodSpec, name string) {
	for _, c := range allContainers(pod) {
		for _, e := range c.Env {
			g.Expect(e.Name).NotTo(Equal(name), "env %s present on container %s, should be absent", name, c.Name)
		}
	}
}

func assertOverlayPVCMounted(g Gomega, pod corev1.PodSpec, modelName, pvcName, subPath string) {
	volName := "model-overlay-" + modelName
	var vol *corev1.Volume
	for i := range pod.Volumes {
		if pod.Volumes[i].Name == volName {
			vol = &pod.Volumes[i]
		}
	}
	g.Expect(vol).NotTo(BeNil(), "no overlay volume named %s", volName)
	g.Expect(vol.PersistentVolumeClaim).NotTo(BeNil())
	g.Expect(vol.PersistentVolumeClaim.ClaimName).To(Equal(pvcName))
	g.Expect(vol.PersistentVolumeClaim.ReadOnly).To(BeTrue())

	var mountFound bool
	for _, c := range allContainers(pod) {
		for _, vm := range c.VolumeMounts {
			if vm.Name == volName &&
				vm.MountPath == isvcutils.OverlayMountPath(modelName) &&
				vm.SubPath == subPath {
				mountFound = true
			}
		}
	}
	g.Expect(mountFound).To(BeTrue(),
		"no container mounts overlay %s at %s SubPath=%s", modelName, isvcutils.OverlayMountPath(modelName), subPath)
}

func assertNoVolumeNamed(g Gomega, pod corev1.PodSpec, name string) {
	for _, v := range pod.Volumes {
		g.Expect(v.Name).NotTo(Equal(name), "volume %s should be absent", name)
	}
}

// =========================================================================
// Additional coverage: multiple overlays, PerNode, NotFound, spec mutation,
// no-overlay regression guard.
// =========================================================================

var _ = ginkgo.Describe("InferenceService overlays — additional coverage", ginkgo.Ordered, func() {
	var ns *corev1.Namespace

	ginkgo.BeforeEach(func() {
		setSuiteFakeMCDefaults()
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "isvc-overlay-extra-")
		Expect(err).NotTo(HaveOccurred())
	})

	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("no overlays declared: pod spec carries no OVERLAY_* envs or overlay volumes (regression guard)", func() {
		shardedURI := "manifold://test-bucket/tree/noovl/primary"
		primary := makeShardedCBMReady("noovl-sharded", shardedURI)
		ginkgo.DeferCleanup(deleteObj(primary))

		csr := dualDistributionRuntime("noovl-runtime")
		Expect(k8sClient.Create(suiteCtx, csr)).To(Succeed())
		ginkgo.DeferCleanup(deleteObj(csr))

		isvc := v1beta1testing.MakeInferenceService("noovl-isvc", ns.Name).
			Model(primary.Name).
			Runtime(csr.Name).
			DeploymentMode(constants.RawDeployment).
			Engine(&v1beta1.EngineSpec{}).Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())

		Eventually(func(g Gomega) {
			pod := getISVCPodSpec(g, ns.Name)
			assertEnvEquals(g, pod, constants.ModelPathEnvVarKey, shardedURI)
			for _, c := range allContainers(pod) {
				for _, e := range c.Env {
					g.Expect(e.Name).NotTo(HavePrefix("OVERLAY_"),
						"container %s carries unexpected overlay env %s", c.Name, e.Name)
				}
			}
			for _, v := range pod.Volumes {
				g.Expect(v.Name).NotTo(HavePrefix("model-overlay-"),
					"unexpected overlay volume %s present when no overlays declared", v.Name)
			}
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("MountedOverlays empty; OverlaysReady condition absent")
		Eventually(func(g Gomega) {
			got := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(isvc), got)).To(Succeed())
			g.Expect(got.Status.MountedOverlays).To(BeEmpty())
			g.Expect(got.Status.GetCondition(v1beta1.OverlaysReady)).To(BeNil())
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.It("two overlays of different distributions: PVC + Sharded both wired on the same pod", func() {
		pvcName := "multi-overlay-pvc"
		subPath := "models/multi"
		makeBoundPVC(ns.Name, pvcName)

		shardedURI := "manifold://test-bucket/tree/multi/primary"
		primary := makeShardedCBMReady("multi-sharded-primary", shardedURI)
		ginkgo.DeferCleanup(deleteObj(primary))

		pvcOv := v1beta1testing.MakeClusterBaseModel("multi-pvc-overlay").
			StorageURI("pvc://" + ns.Name + ":" + pvcName + "/" + subPath).
			ModelFormat("safetensors").
			ModelArchitecture("LlamaForCausalLM").
			ModelFramework("transformers").Obj()
		Expect(k8sClient.Create(suiteCtx, pvcOv)).To(Succeed())
		ginkgo.DeferCleanup(deleteObj(pvcOv))

		shardedOverlayURI := "manifold://test-bucket/tree/multi/sharded-overlay"
		shardedOv := makeShardedCBMReady("multi-sharded-overlay", shardedOverlayURI)
		ginkgo.DeferCleanup(deleteObj(shardedOv))

		csr := dualDistributionRuntime("multi-runtime")
		Expect(k8sClient.Create(suiteCtx, csr)).To(Succeed())
		ginkgo.DeferCleanup(deleteObj(csr))

		isvc := v1beta1testing.MakeInferenceService("multi-isvc", ns.Name).
			Model(primary.Name).
			Runtime(csr.Name).
			DeploymentMode(constants.RawDeployment).
			Engine(&v1beta1.EngineSpec{}).Obj()
		isvc.Spec.Model.Overlays = []v1beta1.ModelOverlayRef{
			{Name: pvcOv.Name, Kind: ptr.To("ClusterBaseModel"), APIGroup: ptr.To("ome.io")},
			{Name: shardedOv.Name, Kind: ptr.To("ClusterBaseModel"), APIGroup: ptr.To("ome.io")},
		}
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())

		Eventually(func(g Gomega) {
			pod := getISVCPodSpec(g, ns.Name)
			// Primary stays Sharded.
			assertEnvEquals(g, pod, constants.ModelPathEnvVarKey, shardedURI)
			assertEnvPresent(g, pod, constants.ModelCacheProviderEnvVarKey)
			// Both overlay envs present, each with the correct value.
			assertEnvEquals(g, pod, isvcutils.OverlayEnvVarName(pvcOv.Name),
				isvcutils.OverlayMountPath(pvcOv.Name))
			assertEnvEquals(g, pod, isvcutils.OverlayEnvVarName(shardedOv.Name), shardedOverlayURI)
			// PVC overlay contributes a volume + mount; Sharded does not.
			assertOverlayPVCMounted(g, pod, pvcOv.Name, pvcName, subPath)
			assertNoVolumeNamed(g, pod, "model-overlay-"+shardedOv.Name)
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("MountedOverlays carries both, OverlaysReady=True")
		Eventually(func(g Gomega) {
			got := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(isvc), got)).To(Succeed())
			g.Expect(got.Status.MountedOverlays).To(HaveLen(2))
			cond := got.Status.GetCondition(v1beta1.OverlaysReady)
			g.Expect(cond).NotTo(BeNil())
			g.Expect(cond.Status).To(Equal(corev1.ConditionTrue))
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.It("PerNode (HostPath) overlay: HostPath volume + mount + env addressable", func() {
		shardedURI := "manifold://test-bucket/tree/pernode/primary"
		primary := makeShardedCBMReady("pernode-sharded-primary", shardedURI)
		ginkgo.DeferCleanup(deleteObj(primary))

		// CRD requires storageUri; controller's HostPath branch fires
		// when Storage.Path is also set (post-agent populated state).
		hostPath := "/var/tmp/test-overlay-pernode"
		overlay := v1beta1testing.MakeClusterBaseModel("pernode-overlay").
			StorageURI("oci://test/repo/models/o/pernode").
			ModelFormat("safetensors").
			ModelArchitecture("LlamaForCausalLM").
			ModelFramework("transformers").Obj()
		overlay.Spec.Storage.Path = &hostPath
		Expect(k8sClient.Create(suiteCtx, overlay)).To(Succeed())
		ginkgo.DeferCleanup(deleteObj(overlay))

		csr := dualDistributionRuntime("pernode-runtime")
		Expect(k8sClient.Create(suiteCtx, csr)).To(Succeed())
		ginkgo.DeferCleanup(deleteObj(csr))

		isvc := v1beta1testing.MakeInferenceService("pernode-isvc", ns.Name).
			Model(primary.Name).
			Runtime(csr.Name).
			DeploymentMode(constants.RawDeployment).
			Engine(&v1beta1.EngineSpec{}).Obj()
		isvc.Spec.Model.Overlays = []v1beta1.ModelOverlayRef{
			{Name: overlay.Name, Kind: ptr.To("ClusterBaseModel"), APIGroup: ptr.To("ome.io")},
		}
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())

		Eventually(func(g Gomega) {
			pod := getISVCPodSpec(g, ns.Name)
			assertEnvEquals(g, pod, isvcutils.OverlayEnvVarName(overlay.Name),
				isvcutils.OverlayMountPath(overlay.Name))

			volName := "model-overlay-" + overlay.Name
			var vol *corev1.Volume
			for i := range pod.Volumes {
				if pod.Volumes[i].Name == volName {
					vol = &pod.Volumes[i]
				}
			}
			g.Expect(vol).NotTo(BeNil())
			g.Expect(vol.HostPath).NotTo(BeNil())
			g.Expect(vol.HostPath.Path).To(Equal(hostPath))
			g.Expect(vol.PersistentVolumeClaim).To(BeNil(),
				"PerNode overlay must produce HostPath, not PVC, volume")
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.It("NotFound overlay: skipped with reason; primary unaffected", func() {
		shardedURI := "manifold://test-bucket/tree/notfound/primary"
		primary := makeShardedCBMReady("notfound-sharded", shardedURI)
		ginkgo.DeferCleanup(deleteObj(primary))

		csr := dualDistributionRuntime("notfound-runtime")
		Expect(k8sClient.Create(suiteCtx, csr)).To(Succeed())
		ginkgo.DeferCleanup(deleteObj(csr))

		// IMPORTANT: bypass the validating webhook (not registered in
		// this suite) by referencing a non-existent overlay. The
		// resolver should treat it as Skipped, not bubble an error.
		isvc := v1beta1testing.MakeInferenceService("notfound-isvc", ns.Name).
			Model(primary.Name).
			Runtime(csr.Name).
			DeploymentMode(constants.RawDeployment).
			Engine(&v1beta1.EngineSpec{}).Obj()
		isvc.Spec.Model.Overlays = []v1beta1.ModelOverlayRef{
			{Name: "does-not-exist", Kind: ptr.To("ClusterBaseModel"), APIGroup: ptr.To("ome.io")},
		}
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())

		Eventually(func(g Gomega) {
			pod := getISVCPodSpec(g, ns.Name)
			assertEnvEquals(g, pod, constants.ModelPathEnvVarKey, shardedURI)
			assertEnvAbsent(g, pod, isvcutils.OverlayEnvVarName("does-not-exist"))
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("OverlaysReady=False with 'not found' in the message")
		Eventually(func(g Gomega) {
			got := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(isvc), got)).To(Succeed())
			g.Expect(got.Status.MountedOverlays).To(BeEmpty())
			cond := got.Status.GetCondition(v1beta1.OverlaysReady)
			g.Expect(cond).NotTo(BeNil())
			g.Expect(cond.Status).To(Equal(corev1.ConditionFalse))
			g.Expect(cond.Message).To(ContainSubstring("not found"))
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.It("spec mutation: removing an overlay drops its env + volume from the pod spec", func() {
		pvcName := "mutate-overlay-pvc"
		subPath := "models/mutate"
		makeBoundPVC(ns.Name, pvcName)

		shardedURI := "manifold://test-bucket/tree/mutate/primary"
		primary := makeShardedCBMReady("mutate-sharded-primary", shardedURI)
		ginkgo.DeferCleanup(deleteObj(primary))

		overlay := v1beta1testing.MakeClusterBaseModel("mutate-pvc-overlay").
			StorageURI("pvc://" + ns.Name + ":" + pvcName + "/" + subPath).
			ModelFormat("safetensors").
			ModelArchitecture("LlamaForCausalLM").
			ModelFramework("transformers").Obj()
		Expect(k8sClient.Create(suiteCtx, overlay)).To(Succeed())
		ginkgo.DeferCleanup(deleteObj(overlay))

		csr := dualDistributionRuntime("mutate-runtime")
		Expect(k8sClient.Create(suiteCtx, csr)).To(Succeed())
		ginkgo.DeferCleanup(deleteObj(csr))

		isvc := v1beta1testing.MakeInferenceService("mutate-isvc", ns.Name).
			Model(primary.Name).
			Runtime(csr.Name).
			DeploymentMode(constants.RawDeployment).
			Engine(&v1beta1.EngineSpec{}).Obj()
		isvc.Spec.Model.Overlays = []v1beta1.ModelOverlayRef{
			{Name: overlay.Name, Kind: ptr.To("ClusterBaseModel"), APIGroup: ptr.To("ome.io")},
		}
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())

		ginkgo.By("overlay env + volume initially present")
		Eventually(func(g Gomega) {
			pod := getISVCPodSpec(g, ns.Name)
			assertOverlayPVCMounted(g, pod, overlay.Name, pvcName, subPath)
			assertEnvEquals(g, pod, isvcutils.OverlayEnvVarName(overlay.Name),
				isvcutils.OverlayMountPath(overlay.Name))
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("patching ISVC to remove the overlay")
		Eventually(func(g Gomega) {
			got := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(isvc), got)).To(Succeed())
			got.Spec.Model.Overlays = nil
			g.Expect(k8sClient.Update(suiteCtx, got)).To(Succeed())
		}).WithTimeout(util.ShortTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("pod spec drops the overlay env + volume")
		Eventually(func(g Gomega) {
			pod := getISVCPodSpec(g, ns.Name)
			assertEnvAbsent(g, pod, isvcutils.OverlayEnvVarName(overlay.Name))
			assertNoVolumeNamed(g, pod, "model-overlay-"+overlay.Name)
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("MountedOverlays clears")
		Eventually(func(g Gomega) {
			got := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(isvc), got)).To(Succeed())
			g.Expect(got.Status.MountedOverlays).To(BeEmpty())
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})
})
